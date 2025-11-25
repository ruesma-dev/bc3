# application/services/phase2_code_mapper.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import os
import re
import json
import csv

import pandas as pd

from utils.text_sanitize import clean_text
from infrastructure.ai.gemini_client import (
    RateLimiter,
    choose_best_code_with_llm,
    choose_best_code_batch_with_llm,
)
from infrastructure.products.catalog_loader import load_catalog

MAX_CODE_LEN = 20
NUM_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")

_PIPE_TAIL_RE = re.compile(r"\|+\s*$")

def _final_trim_trailing_pipes(file_path: Path) -> None:
    """
    Limpia el fichero resultante colapsando '|||' finales en un único '|'
    SOLO al final de cada línea. No toca los '|' internos.

    Reglas:
      - Para líneas que empiezan por '~' (registros BC3), si terminan con
        uno o más '|' (posibles espacios después), se colapsa a un único '|'.
      - No añade '|' si no lo había.
      - Mantiene saltos de línea.
    """
    tmp = file_path.with_suffix(file_path.suffix + ".tmp_clean")
    pat = re.compile(r"\|+\s*$")

    with file_path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:
        for raw in fin:
            if raw.startswith("~"):
                s = raw.rstrip("\n")
                s = pat.sub("|", s)  # '||||  ' -> '|'
                fout.write(s + "\n")
            else:
                # líneas no BC3: solo asegurar salto de línea
                if not raw.endswith("\n"):
                    raw = raw + "\n"
                fout.write(raw)

    # Reemplaza el original por el limpio
    tmp.replace(file_path)
def _fix_d_trailing_backslashes(file_path: Path) -> None:
    """
    En líneas ~D| elimina TODAS las barras invertidas finales inmediatamente
    antes del '|' de cierre. No toca las barras internas ni otras líneas.

    Ej.:  '...\\|' -> '|'
    """
    tmp = file_path.with_suffix(file_path.suffix + ".tmp_dfix")
    pat = re.compile(r"\\+\|\s*$")

    with file_path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:
        for raw in fin:
            if raw.startswith("~D|"):
                s = raw.rstrip("\n")
                s = pat.sub("|", s)
                fout.write(s + "\n")
            else:
                if not raw.endswith("\n"):
                    raw = raw + "\n"
                fout.write(raw)

    tmp.replace(file_path)

def _cleanup_trailing_pipes_file(path: Path) -> None:
    """
    Reescribe el archivo asegurando que las líneas BC3 terminen con
    una única tubería '|' (sin acumular '|||' al final).
    No toca el contenido interno ni las barras invertidas de ~D.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    with path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:
        for line in fin:
            # Mantener exactamente una tubería final
            # (respetando cualquier barra invertida previa)
            if line.rstrip("\n").endswith("|"):
                clean = _PIPE_TAIL_RE.sub("|", line.rstrip("\n")) + "\n"
                fout.write(clean)
            else:
                fout.write(line)
    path.unlink()
    tmp.rename(path)

@dataclass
class Concept:
    code: str
    unidad: str
    desc_short: str
    price_txt: str
    tipo: str
    long_desc: str | None = None


# --------------------------- util numéricas --------------------------------- #
def _to_float(s: str) -> float | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None


# --------------------------- normalización / tokens ------------------------- #
def _normalize_space_lower(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip().lower()


def _tokenize(text: str) -> List[str]:
    text = clean_text(text).lower()
    return re.findall(r"[a-z0-9]+", text)


def _score_token_overlap(a: str, b: str) -> int:
    ta = set(_tokenize(a))
    tb = set(_tokenize(b))
    return len(ta & tb)


# --------------------------- hints (Tipo/Grupo/Familia) --------------------- #
@dataclass
class HintsTree:
    tipos: List[str]
    grupos: List[str]
    familias: List[str]
    # mapas rápidos en minúsculas para comparación
    tipos_l: set[str]
    grupos_l: set[str]
    familias_l: set[str]


def _load_hints_tree_xlsx(path: Path) -> HintsTree:
    """
    Lee un Excel con columnas (case-insensitive) [Tipo, Grupo, Familia].
    Devuelve los valores únicos y normalizados.
    """
    if not path.exists():
        raise FileNotFoundError(f"Hints Excel no encontrado: {path}")

    # Buscamos columnas estándar ignorando mayúsculas
    df = pd.read_excel(path, sheet_name=0, dtype=str).fillna("")
    cols = {c.lower(): c for c in df.columns}

    def pick(*names: str) -> str:
        for n in names:
            if n in cols:
                return cols[n]
        raise KeyError(f"No se encontró ninguna de las columnas {names} en {path.name}")

    col_tipo = pick("tipo", "type")
    col_grupo = pick("grupo", "group")
    col_fam = pick("familia", "family")

    tipos = sorted({str(v).strip() for v in df[col_tipo].tolist() if str(v).strip()})
    grupos = sorted({str(v).strip() for v in df[col_grupo].tolist() if str(v).strip()})
    familias = sorted({str(v).strip() for v in df[col_fam].tolist() if str(v).strip()})

    return HintsTree(
        tipos=tipos,
        grupos=grupos,
        familias=familias,
        tipos_l={_normalize_space_lower(x) for x in tipos},
        grupos_l={_normalize_space_lower(x) for x in grupos},
        familias_l={_normalize_space_lower(x) for x in familias},
    )


def _default_hints() -> HintsTree:
    """
    Fallback si no hay Excel: usa los hints que nos diste para Tipo/Grupo y
    deriva familias desde el catálogo cuando sea posible.
    """
    tipos = ["Coste Directo", "Coste Indirecto"]
    grupos = [
        "MATERIALES",
        "SUBCONTRATA MANO DE OBRA",
        "SUBCONTRATA CON APORTE MATERIALES",
        "MAQUINARIA: ALQUILER",
        "MAQUINARIA: REPUESTOS Y REPARACIONES",
        "MEDIOS AUXILIARES: ALQUILER",
        "MEDIOS AUXILIARES: COMPRA",
        "MANO DE OBRA INDIRECTA",
        "CONSUMOS",
        "INFRAESTRUCTURA",
        "MAQUINARIA AJENA",
        "MAQUINARIA PROPIA",
        "TRABAJOS PROFESIONALES EXTERNOS",
        "OTROS COSTES INDIRECTOS",
    ]
    return HintsTree(
        tipos=tipos,
        grupos=grupos,
        familias=[],
        tipos_l={_normalize_space_lower(x) for x in tipos},
        grupos_l={_normalize_space_lower(x) for x in grupos},
        familias_l=set(),
    )


def _load_hints_from_env_or_arg(hints_tree_xlsx: Optional[Path]) -> HintsTree:
    if hints_tree_xlsx:
        try:
            return _load_hints_tree_xlsx(hints_tree_xlsx)
        except Exception:
            pass
    env_p = os.getenv("PHASE2_HINTS_XLSX", "").strip()
    if env_p:
        try:
            return _load_hints_tree_xlsx(Path(env_p))
        except Exception:
            pass
    # fallback
    return _default_hints()


# --------------------------- parsing de candidato de catálogo --------------- #
def _parse_catalog_desc(desc: str) -> Dict[str, str]:
    """
    La descripción del catálogo viene como:
      'Coste Directo, MATERIALES, ACERO ESTRUCTURAL, ACERO CORRUGADO PARA HORMIGON'
       0: tipo (Coste Directo/Indirecto)
       1: grupo (MATERIALES / SUBCONTRATA ... )
       2: familia (ACERO ESTRUCTURAL ...)
       3: producto (ACERO CORRUGADO ...)
    """
    parts = [p.strip() for p in (desc or "").split(",")]
    return {
        "tipo": parts[0] if len(parts) > 0 else "",
        "grupo": parts[1] if len(parts) > 1 else "",
        "familia": parts[2] if len(parts) > 2 else "",
        "producto": parts[3] if len(parts) > 3 else "",
    }


# --------------------------- reglas lingüísticas grupo ---------------------- #
def _infer_group_from_phrases(text: str) -> Optional[str]:
    """
    Reglas:
      - 'suministro y colocacion|montaje|aplicacion' -> SUBCONTRATA CON APORTE MATERIALES
      - Si menciona 'incluye' y 'mano de obra' y 'material(es)' -> SUBCONTRATA CON APORTE MATERIALES
      - 'suministro' solo -> MATERIALES
      - 'montaje' o 'aplicacion' solo -> SUBCONTRATA MANO DE OBRA
    """
    t = _normalize_space_lower(clean_text(text))
    has_sum = "suministro" in t
    has_mont = ("montaje" in t) or ("colocacion" in t) or ("colocación" in t)
    has_apli = ("aplicacion" in t) or ("aplicación" in t)
    has_incluye = "incluye" in t or "incluido" in t or "incluida" in t or "incluidos" in t
    has_mo = "mano de obra" in t or "elaboracion" in t or "elaboración" in t
    has_mat = "material" in t or "materiales" in t

    # reglas fuertes
    if (has_sum and (has_mont or has_apli)) or (has_incluye and has_mo and has_mat):
        return "SUBCONTRATA CON APORTE MATERIALES"
    if has_sum:
        return "MATERIALES"
    if has_mont or has_apli or (has_incluye and has_mo):
        return "SUBCONTRATA MANO DE OBRA"
    return None


# --------------------------- recogida BC3/contexto -------------------------- #
def _safe_with_suffix(base: str, suffix: str) -> str:
    base = base[: max(0, MAX_CODE_LEN - len(suffix))]
    return (base + suffix)[:MAX_CODE_LEN]


def _letters_suffix(n: int) -> str:
    # 1->a, 2->b, ... 26->z, 27->aa, etc.
    s = ""
    while n > 0:
        n -= 1
        s = chr(97 + (n % 26)) + s
        n //= 26
    return s


def _collect_bc3_info(path: Path) -> Tuple[Dict[str, Concept], Dict[str, str], Dict[str, str], Dict[str, List[str]]]:
    """
    Devuelve:
      concepts: code -> Concept (desc corta, unidad, precio, tipo, long_desc)
      long_map: code -> primera ~T
      parent_of: code -> parent_code (si aparece como hijo en ~D)
      children_of: parent_code -> [child_codes]
    """
    concepts: Dict[str, Concept] = {}
    long_map: Dict[str, str] = {}
    parent_of: Dict[str, str] = {}
    children_of: Dict[str, List[str]] = {}

    with path.open("r", encoding="latin-1", errors="ignore") as fh:
        for raw in fh:
            if raw.startswith("~C|"):
                _, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                while len(parts) < 6:
                    parts.append("")
                code, unidad, desc, price, _date, tipo = parts[:6]
                concepts[code] = Concept(
                    code=code,
                    unidad=unidad or "",
                    desc_short=desc or "",
                    price_txt=price or "",
                    tipo=tipo or "",
                )
            elif raw.startswith("~T|"):
                _, rest = raw.split("|", 1)
                code, txt = rest.rstrip("\n").split("|", 1)
                if code not in long_map:
                    long_map[code] = txt
            elif raw.startswith("~D|"):
                _, rest = raw.split("|", 1)
                parent, child_part = rest.split("|", 1)
                chunks = child_part.rstrip("|\n").split("\\")
                children = []
                for i in range(0, len(chunks), 3):
                    ch = (chunks[i] if i < len(chunks) else "").strip()
                    if ch:
                        parent_of[ch] = parent
                        children.append(ch)
                if children:
                    children_of[parent] = children

    # enganchar long_desc
    for c in concepts.values():
        if c.code in long_map:
            c.long_desc = long_map[c.code]
    return concepts, long_map, parent_of, children_of


def _context_for(code: str, concepts: Dict[str, Concept], parent_of: Dict[str, str]) -> str:
    """
    Construye un contexto textual con cadena de padres (capítulos/partidas) y
    descripciones cortas/largas para dar a Gemini.
    """
    parts: List[str] = []
    cur = code
    chain = []
    seen = set()
    while cur in parent_of and cur not in seen:
        seen.add(cur)
        p = parent_of[cur]
        chain.append(p)
        cur = p
    chain = list(reversed(chain))
    parts.append("Presupuesto de obra en España, elaborado en PRESTO.")
    if chain:
        parts.append("Contexto jerárquico (de mayor a menor):")
        for idx, cc in enumerate(chain, 1):
            c = concepts.get(cc)
            if not c:
                continue
            short = clean_text(c.desc_short)
            longt = clean_text(c.long_desc or "")
            parts.append(f"- [{cc}] {short}. {('Texto: ' + longt) if longt else ''}")
    c0 = concepts.get(code)
    if c0:
        parts.append("Descompuesto a clasificar:")
        parts.append(f"- Código actual: {code}")
        parts.append(f"- Descripción corta: {clean_text(c0.desc_short)}")
        if c0.long_desc:
            parts.append(f"- Descripción larga: {clean_text(c0.long_desc)}")
    return "\n".join(p for p in parts if p)


# --------------------------- few-shots / prompt ----------------------------- #
def _load_fewshots_from_env() -> List[Dict[str, Any]]:
    """
    Si existe PHASE2_FEWSHOTS_PATH (JSON), lo carga.
    Cada ejemplo:
      { "title": "...", "context": "...", "candidates":[{"code":..., "desc":...}], "best_code":"...", "rationale":"..." }
    """
    p = os.getenv("PHASE2_FEWSHOTS_PATH", "").strip()
    if not p:
        return []
    try:
        with open(p, "r", encoding="utf-8") as fh:
            data = json.load(fh)
            if isinstance(data, list):
                return data
    except Exception:
        pass
    return []

def _candidate_as_row(c: Dict[str, str]) -> Dict[str, str]:
    """
    Normaliza campos para la tabla del prompt:
     - Codigo producto
     - Descripcion producto
     - Descripcion familia
     - Descripcion grupo
     - Descripcion tipo
    Admite dos formatos:
      a) c = {"code":..., "desc":"Coste Directo, GRUPO, FAMILIA, PRODUCTO"}
      b) c = {"code":..., "product":..., "family":..., "group":..., "type":...}
         (o claves en español: "producto"/"familia"/"grupo"/"tipo")
    """
    code = c.get("code") or c.get("Codigo producto") or ""
    # intentar el formato (b)
    prod = c.get("product") or c.get("producto") or c.get("Descripcion producto") or ""
    fam = c.get("family") or c.get("familia") or c.get("Descripcion familia") or ""
    grp = c.get("group") or c.get("grupo") or c.get("Descripcion grupo") or ""
    typ = c.get("type") or c.get("tipo") or c.get("Descripcion tipo") or ""

    if not (prod or fam or grp or typ):
        # caer a (a) => desc con comas
        parts = [p.strip() for p in (c.get("desc") or "").split(",")]
        typ = parts[0] if len(parts) > 0 else ""
        grp = parts[1] if len(parts) > 1 else ""
        fam = parts[2] if len(parts) > 2 else ""
        prod = parts[3] if len(parts) > 3 else ""

    return {
        "Codigo producto": code,
        "Descripcion producto": prod,
        "Descripcion familia": fam,
        "Descripcion grupo": grp,
        "Descripcion tipo": typ,
    }

DEFAULT_FEWSHOTS: List[Dict[str, Any]] = [
    {
        "title": "Alicatado porcelánico con suministro y colocación",
        "context": (
            "DESCRIPCION CORTA: ALICATADO PORCELANICO STARK ANTRACITA_REV02\n"
            "DESCRIPCION LARGA: Suministro y colocacion de Alicatado porcelanico de gran formato STARK ANTRACITA "
            "de GRESPANIA, 60x120 cm, 11,5mm, recibido con adhesivo especial porcelanico, sin incluir enfoscado, "
            "incluido cortes, ingletes, piezas especiales y rejuntado; limpieza; según ficha técnica.\n"
            "PADRES: ALICATADOS Y REVESTIMIENTOS"
        ),
        # En few-shots basta con incluir el ganador; los candidatos reales van en el caso actual.
        "candidates": [{"code": "SM3301", "desc": "—"}],
        "best_code": "SM3301",
        "rationale": "suministro+colocación ⇒ subcontrata con aporte; revestimiento cerámico"
    },
    {
        "title": "Losa de cimentación con ferralla (incluye elaboración y montaje)",
        "context": (
            "DESCRIPCION CORTA: Losa de cimentacion.\n"
            "DESCRIPCION LARGA: Losa de cimentacion de hormigon armado HA-30..., precio incluye elaboracion y montaje "
            "de ferralla, vertido con bomba; acabados y curado; no incluye encofrado.\n"
            "PADRES: CIMENTACION"
        ),
        "candidates": [{"code": "SM1002", "desc": "—"}],
        "best_code": "SM1002",
        "rationale": "incluye elaboración y MO ⇒ subcontrata con aporte"
    },
    {
        "title": "Descabezado de pilote (solo mano de obra/maquinaria)",
        "context": (
            "DESCRIPCION CORTA: Descabezado de pilote de hormigon armado de 65cm.\n"
            "DESCRIPCION LARGA: Picado de hormigón de cabeza de pilote con martillo neumático; gestión de escombros.\n"
            "PADRES: CIMENTACION"
        ),
        "candidates": [{"code": "SB1007", "desc": "—"}],
        "best_code": "SB1007",
        "rationale": "servicio sin materiales ⇒ subcontrata mano de obra"
    },
    {
        "title": "Pintura plástica (suministro y aplicación)",
        "context": (
            "DESCRIPCION CORTA: PINTURA PLASTICA LISA MATE COLOR RAL 7016_REV-01B\n"
            "DESCRIPCION LARGA: Suministro y aplicacion de pintura plástica lisa mate, dos manos, con imprimación.\n"
            "PADRES: PINTURAS"
        ),
        "candidates": [{"code": "SM3102", "desc": "—"}],
        "best_code": "SM3102",
        "rationale": "suministro+aplicación ⇒ subcontrata con aporte"
    },
]


def _compose_super_prompt(
    context: str,
    candidates: List[Dict[str, str]],
    fewshots: List[Dict[str, Any]],
    tipo_objetivo: Optional[str],
    grupo_objetivo: Optional[str],
    familias_hints: List[str],
) -> str:
    """
    Construye el prompt EXACTO que pediste, incluyendo la TABLA DE CANDIDATOS.
    """
    # Construimos la tabla legible
    rows = [_candidate_as_row(c) for c in candidates]
    tabla = []
    for r in rows:
        tabla.append(
            f"- Codigo producto: {r['Codigo producto']} | "
            f"Descripcion producto: {r['Descripcion producto']} | "
            f"Descripcion familia: {r['Descripcion familia']} | "
            f"Descripcion grupo: {r['Descripcion grupo']} | "
            f"Descripcion tipo: {r['Descripcion tipo']}"
        )
    tabla_str = "\n".join(tabla)

    # Few-shots (opcionales)
    few = []
    for ex in fewshots or []:
        few.append(f"# DESCRIPCION (contexto ejemplo):\n{ex.get('context','')}\n#CODIGO: {ex.get('best_code','')}")
    fewshots_str = "\n\n".join(few) if few else ""

    # pistas extra (tipo/grupo)
    restr_tipo = ""
    if tipo_objetivo:
        restr_tipo = (
            f"IMPORTANTE: Este descompuesto pertenece a **{tipo_objetivo}**. "
            f"No puedes elegir candidatos de un tipo diferente."
        )
    pista_grupo = f"Pista de grupo sugerido: {grupo_objetivo}." if grupo_objetivo else ""

    # PROMPT final
    prompt = f"""##ROL:Eres un clasificador experto en presupuestos de obra (España) y catálogo de productos.
##OBJETIVO:Tienes que asignar un código de producto a un DESCOMPUESTO de PRESTO.
##CONTEXTO: Los candidatos estan en la tabla de candidatos que adjunto. el candidato es la columna Codigo producto. En la columna descripcion producto esta la descripcion del candidato para darte contexto, es lo que tienes que casar con las descripciones que te dare. en la columna descripcion familia, esta el siguiente nivel, en la descripcion grupo descripcion grupo el siguiente nivel a ese, y en la columna Descripcion tipo tipo, esta el primer nivel. Si en el contexto del producto a clasificar aparece sumnistro y colocacion o montaje, entonces pertenece al grupo SUBCONTRATA CON APORTE MATERIALES, si pone colocacion o montaje unicamente, pertenece al grupo SUBCONTRATA MANO DE OBRA, y si pone suministro es del tipo MATERIAL. Si el precio incluye elaboración y mano de obra significa que es una SUBCONTRATA CON APORTE MATERIALES. Siempre que en la partida se habla de incluir o que forma parte la mano de obra, elaboración o cualquier actividad similar significa que es una SUCONTRATA, si además incluye materiales o habla de estos entonces es con APORTE DE MATERIAL
{restr_tipo}
{pista_grupo}
##INPUT: te voy a dar la descripcion corta y larga del descompuesto, asi como las descripciones de su padres (partidas y capitulos). con ese contexto debes seleccionar entre los candidatos.
##REGLAS: SOLO puedes elegir un código de entre la lista de CANDIDATOS. Si ninguno encaja bien, elige el menos malo, con confianza baja. Da mucha importancia a: Tipo (Coste Directo/Indirecto), Grupo y Familia; también al contexto jerárquico. Prohibido inventar códigos: solo candidatos dados. No puedes elegir candidatos de un nivel distinto (p. ej. si es Coste Directo, evita familias de Coste Indirecto)
##OUTPUT:Responde en una sola línea: {{"best_code":"<code>", "confidence":<0..1>, "rationale":"<≤20 palabras>"}} En el rationale dame la descripcion de producto, y familia que has elegido y la razón de la elección. Cuando tengas el código revisa que la descripción y familia de este coincide con lo que tienes planteado en rationale, después de esta revisión si es necesario cambialo para que se parezca al rationale

### TABLA DE CANDIDATOS
{tabla_str}

### CONTEXTO REAL
{context}

### FEW-SHOTS (EJEMPLOS)
{fewshots_str}
"""
    return prompt


# --------------------------- heurística opcional ---------------------------- #
def _extract_signals_from_context(ctx: str, concepto: Concept, hints: HintsTree) -> Dict[str, Any]:
    """
    Extrae señales desde contexto + texto del descompuesto (desc corta/larga) usando hints del Excel:
      - tipo_sugerido: 'Coste Directo' / 'Coste Indirecto' / None
      - grupo_sugerido: grupo según frases (suministro/montaje/aplicación) si aparece
      - tokens: set de tokens, útil para matching laxo
    """
    full_txt = " ".join([
        ctx or "",
        concepto.desc_short or "",
        concepto.long_desc or ""
    ])
    norm = _normalize_space_lower(clean_text(full_txt))

    # Tipo: buscamos cadenas “coste directo/indirecto” explícitas
    tipo_sugerido = None
    if "coste directo" in norm:
        tipo_sugerido = "Coste Directo"
    elif "coste indirecto" in norm:
        tipo_sugerido = "Coste Indirecto"

    # Grupo: reglas de frases
    grupo_sugerido = _infer_group_from_phrases(full_txt)

    return {
        "tokens": set(_tokenize(full_txt)),
        "tipo_sugerido": tipo_sugerido,
        "grupo_sugerido": grupo_sugerido,
    }


def _prefilter_candidates(
    concept: Concept,
    ctx: str,
    catalog: List[Dict[str, str]],
    topk: int,
    hints: HintsTree,
    use_heuristics: bool,
) -> Tuple[List[Dict[str, str]], Optional[str]]:
    """
    Prefiltra y ranquea candidatos:
      - Si el contexto sugiere Tipo (Directo/Indirecto), se filtra a ese tipo.
      - Se aplican pistas de Grupo (si hay) como peso extra (no filtro duro).
    Devuelve: (candidatos_top, tipo_forzado_o_None)
    """
    sig = _extract_signals_from_context(ctx, concept, hints)
    tipo_obj = sig["tipo_sugerido"]  # puede ser None
    grupo_obj = sig["grupo_sugerido"]

    # Candidatos con parsing
    parsed = []
    for c in catalog:
        parts = _parse_catalog_desc(c["desc"])
        parsed.append((c, parts))

    # Filtro duro por Tipo si hay objetivo
    if tipo_obj:
        parsed = [(c, p) for (c, p) in parsed if _normalize_space_lower(p["tipo"]) == _normalize_space_lower(tipo_obj)]

    # Scoring
    query = f"{concept.desc_short} {concept.long_desc or ''} {ctx}"
    scored: List[Tuple[Dict[str, str], float]] = []

    # Pesos heurísticos
    if use_heuristics:
        w_token = float(os.getenv("HEUR_W_TOKEN", "1.0"))
        w_typ = float(os.getenv("HEUR_W_TIPO", "3.0"))
        w_grp = float(os.getenv("HEUR_W_GRUPO", "2.5"))
        w_fam = float(os.getenv("HEUR_W_FAMILIA", "3.0"))
    else:
        w_token = 1.0
        w_typ = 1.0
        w_grp = 1.0
        w_fam = 1.0

    for c, p in parsed:
        # token overlap
        s = _score_token_overlap(query, c["desc"]) * w_token

        # bonus por Tipo si coincide con sugerido
        if tipo_obj and _normalize_space_lower(p["tipo"]) == _normalize_space_lower(tipo_obj):
            s += w_typ

        # bonus por Grupo si coincide con inferido por frases
        if grupo_obj and _normalize_space_lower(p["grupo"]) == _normalize_space_lower(grupo_obj):
            s += w_grp

        # bonus por familia si aparece en el texto (si hints familia disponibles)
        fam = _normalize_space_lower(p["familia"])
        if fam and (fam in _normalize_space_lower(query)):
            s += w_fam

        scored.append((c, s))

    scored.sort(key=lambda x: x[1], reverse=True)
    top = [c for c, _ in scored[: max(1, topk)]]

    return top, tipo_obj


# --------------------------- builder LLM + heurística ----------------------- #
def _build_replacement_map(
    bc3_path: Path,
    catalog_path: Path,
    topk: int = 20,
    min_conf: float = 0.0,
    use_heuristics: Optional[bool] = None,
    fewshots: Optional[List[Dict[str, Any]]] = None,
    hints_tree_xlsx: Optional[Path] = None,
) -> Tuple[Dict[str, str], List[Tuple[str, str, float, str]]]:
    """
    Calcula old_code -> new_code SOLO para descompuestos (T=3 o T original 1/2/3).
    Devuelve:
      - repl_map: dict old_code -> new_code
      - rows: lista de (old_code, new_code, confidence, method)
    Reglas:
      - Si el código contiene '%', usar **% DESCUENTO#** (método='rule').
      - Prefiltro: simple o heurístico con hints del Excel (Tipo/Grupo/Familia).
      - LLM con few-shots embebidos + restricción de Tipo en el prompt.
    """
    if use_heuristics is None:
        use_heuristics = os.getenv("PHASE2_USE_HEURISTICS", "false").lower() == "true"

    hints = _load_hints_from_env_or_arg(hints_tree_xlsx)

    # Few-shots (ENV tiene prioridad)
    fewshots = _load_fewshots_from_env() or fewshots or DEFAULT_FEWSHOTS

    catalog = load_catalog(catalog_path)  # [{"code":..., "desc":...}, ...]
    concepts, _longs, parent_of, _children = _collect_bc3_info(bc3_path)

    # Rate limit
    rpm = int(os.getenv("GEMINI_RPM", "10") or "10")
    limiter = RateLimiter(rpm=rpm)

    batch_mode = (os.getenv("GEMINI_BATCH_MODE", "false").strip().lower() == "true")
    batch_size = max(1, int(os.getenv("GEMINI_BATCH_SIZE", "10") or "10"))
    min_conf = float(os.getenv("GEMINI_MIN_CONFIDENCE", str(min_conf)) or 0.0)

    assigned: Dict[str, int] = {}
    used: set[str] = set()
    discount_counter = 0  # contador para % DESCUENTO

    def unique_assign(base: str) -> str:
        """Garantiza unicidad y long máx 20. Para '% DESCUENTO' numera 1..N."""
        nonlocal discount_counter
        if base.startswith("% DESCUENTO"):
            discount_counter += 1
            code = f"% DESCUENTO{discount_counter}"
            return code[:MAX_CODE_LEN]
        if base not in assigned:
            assigned[base] = 0
            code = base[:MAX_CODE_LEN]
        else:
            assigned[base] += 1
            suf = _letters_suffix(assigned[base])
            code = _safe_with_suffix(base, suf)
        n = 0
        c0 = code
        while code in used:
            n += 1
            suf = _letters_suffix(n)
            code = _safe_with_suffix(c0, suf)
        used.add(code)
        return code

    repl: Dict[str, str] = {}
    rows: List[Tuple[str, str, float, str]] = []

    # Recolectamos descompuestos
    targets: List[str] = []
    for code, c in concepts.items():
        if "#" in code:
            continue
        if c.tipo not in {"1", "2", "3"}:
            continue
        targets.append(code)

    idx = 0
    while idx < len(targets):
        group = targets[idx: idx + (batch_size if batch_mode else 1)]
        group_payload: List[Dict[str, Any]] = []
        fallbacks: Dict[str, str] = {}
        tipo_obj_map: Dict[str, Optional[str]] = {}

        for code in group:
            c = concepts[code]

            # Regla de descuentos
            if "%" in code:
                newc = unique_assign("% DESCUENTO")
                repl[code] = newc
                rows.append((code, newc, 1.0, "rule"))
                continue

            # Contexto base
            ctx = _context_for(code, concepts, parent_of)

            # Prefiltro (con hints + reglas)
            k = int(os.getenv("PREFILTER_TOPK", str(topk)) or topk)
            top_candidates, tipo_obj = _prefilter_candidates(
                concept=c,
                ctx=ctx,
                catalog=catalog,
                topk=k,
                hints=hints,
                use_heuristics=bool(use_heuristics),
            )
            tipo_obj_map[code] = tipo_obj

            # Super-prompt con few-shots + restricción de Tipo
            super_ctx = _compose_super_prompt(
                context=ctx,
                candidates=top_candidates,
                fewshots=fewshots,
                tipo_objetivo=tipo_obj,
                grupo_objetivo=_infer_group_from_phrases(f"{c.desc_short} {c.long_desc or ''}"),
                familias_hints=hints.familias,
            )

            # Fallback si el LLM falla/devuelve baja confianza
            fallbacks[code] = top_candidates[0]["code"]

            group_payload.append({
                "id": code,
                "context": super_ctx,
                "candidates": [{"code": cc["code"], "desc": cc["desc"]} for cc in top_candidates],
            })

        # Si todo eran descuentos en el grupo, saltamos a siguiente
        if not group_payload:
            idx += len(group)
            continue

        try:
            if batch_mode:
                results = choose_best_code_batch_with_llm(group_payload, limiter=RateLimiter(rpm=rpm))
                by_id = {r.get("id"): r for r in results if isinstance(r, dict)}
                for item in group_payload:
                    code = item["id"]
                    if code in repl:
                        continue
                    r = by_id.get(code) or {}
                    best = (r.get("best_code") or "").strip()
                    conf = float(r.get("confidence", 0.0))

                    # Si hay restricción de tipo, invalidamos best si viola
                    tipo_obj = tipo_obj_map.get(code)
                    if best:
                        parts = _parse_catalog_desc(next((c["desc"] for c in item["candidates"] if c["code"] == best), ""))
                        if tipo_obj and _normalize_space_lower(parts.get("tipo", "")) != _normalize_space_lower(tipo_obj):
                            best = ""  # fuerza fallback

                    if not best or conf < min_conf:
                        best = fallbacks[code]
                        method = "heuristic+fallback" if use_heuristics else "simple+fallback"
                        conf_used = max(conf, 0.5 if use_heuristics else 0.4)
                    else:
                        method = "llm"
                        conf_used = conf
                    newc = unique_assign(best)
                    repl[code] = newc
                    rows.append((code, newc, float(conf_used), method))
            else:
                for item in group_payload:
                    code = item["id"]
                    if code in repl:
                        continue
                    llm = choose_best_code_with_llm(item["context"], item["candidates"], limiter=limiter)
                    best = (llm.get("best_code") or "").strip()
                    conf = float(llm.get("confidence", 0.0))

                    # Validación de tipo tras LLM
                    tipo_obj = tipo_obj_map.get(code)
                    if best:
                        parts = _parse_catalog_desc(next((c["desc"] for c in item["candidates"] if c["code"] == best), ""))
                        if tipo_obj and _normalize_space_lower(parts.get("tipo", "")) != _normalize_space_lower(tipo_obj):
                            best = ""  # fuerza fallback

                    if not best or conf < min_conf:
                        best = fallbacks[code]
                        method = "heuristic+fallback" if use_heuristics else "simple+fallback"
                        conf_used = max(conf, 0.5 if use_heuristics else 0.4)
                    else:
                        method = "llm"
                        conf_used = conf
                    newc = unique_assign(best)
                    repl[code] = newc
                    rows.append((code, newc, float(conf_used), method))
        except Exception:
            # Fallback masivo si hay error con el LLM
            for item in group_payload:
                code = item["id"]
                if code in repl:
                    continue
                best = fallbacks[code]
                method = "heuristic+error" if use_heuristics else "simple+error"
                newc = unique_assign(best)
                repl[code] = newc
                rows.append((code, newc, 0.5 if use_heuristics else 0.4, method))

        idx += len(group)

    return repl, rows


# --------------------------- reescritura BC3 -------------------------------- #
def rewrite_bc3_with_codes(src: Path, dst: Path, repl_map: Dict[str, str]) -> None:
    """
    Reescribe el BC3 aplicando solo el cambio de CÓDIGO (nada más):
      - ~C: cambia el campo 'code' si está en repl_map
      - ~D: cambia únicamente el 'child_code' en los tripletes (child\coef\qty)
            y **preserva exactamente** el nº de barras '\' antes de '|'
      - ~M: cambia únicamente el 'child' en el par <parent>\<child>
    """
    if not repl_map:
        dst.write_text(src.read_text("latin-1", errors="ignore"), "latin-1", errors="ignore")
        return

    with src.open("r", encoding="latin-1", errors="ignore") as fin, \
         dst.open("w", encoding="latin-1", errors="ignore") as fout:
        for raw in fin:
            if raw.startswith("~C|"):
                head, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                while len(parts) < 6:
                    parts.append("")
                code = parts[0]
                if code in repl_map:
                    parts[0] = repl_map[code]
                line = f"{head}|{'|'.join(parts)}|\n"
                fout.write(line)

            elif raw.startswith("~D|"):
                # Preservar número exacto de '\' antes del '|'
                m = re.search(r"(\\+)\|\s*$", raw.rstrip("\n"))
                tail_bslashes = m.group(1) if m else "\\"

                head, rest = raw.split("|", 1)
                parent, child_part = rest.split("|", 1)

                body = child_part.rstrip("\n")
                if body.endswith("|"):
                    body = body[:-1]

                chunks = body.split("\\")
                new_chunks: List[str] = []
                i = 0
                while i < len(chunks):
                    child = chunks[i] if i < len(chunks) else ""
                    coef = chunks[i + 1] if i + 1 < len(chunks) else ""
                    qty = chunks[i + 2] if i + 2 < len(chunks) else ""
                    i += 3
                    if not child:
                        continue
                    if child in repl_map:
                        child = repl_map[child]
                    new_chunks.extend([child, coef, qty])

                rebuilt = "\\".join(new_chunks) + tail_bslashes
                line = f"~D|{parent}|{rebuilt}|\n"
                fout.write(line)

            elif raw.startswith("~M|"):
                # ~M|<parent>\<child>|<meta>|<qty>|...
                try:
                    _tag, after = raw.split("|", 1)
                    pair, tail = after.split("|", 1)
                    if "\\" in pair:
                        parent, child = pair.split("\\", 1)
                        if child in repl_map:
                            child = repl_map[child]
                        pair = f"{parent}\\{child}"
                    line = f"~M|{pair}|{tail}"
                    fout.write(line)
                except Exception:
                    fout.write(raw)

            else:
                fout.write(raw)


# --------------------------- runner (con CSV de mapping) -------------------- #
def _write_mapping_csv(rows: List[Tuple[str, str, float, str]], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh, delimiter=";")
        writer.writerow(["old_code", "new_code", "confidence", "method"])
        for oldc, newc, conf, method in rows:
            writer.writerow([oldc, newc, f"{conf:.3f}", method])


def run_phase2(
    bc3_in: Path | None = None,
    catalog_xlsx: Path | None = None,
    bc3_out: Path | None = None,
    **kwargs,
) -> Path:
    """
    Fase 2: clasifica descompuestos contra catálogo y sustituye códigos.

    Parámetros:
      - bc3_in / input_bc3: BC3 (salida de la fase 1, *_limpio.bc3)
      - catalog_xlsx / catalog_path: Excel con catálogo (2 columnas)
      - bc3_out / output_bc3: salida; si None => <input>_clasificado.bc3
    Opcionales:
      - use_heuristics: bool (o ENV PHASE2_USE_HEURISTICS=true)
      - fewshots: lista de ejemplos (o ENV PHASE2_FEWSHOTS_PATH)
      - hints_tree_xlsx: ruta al Excel con Tipo/Grupo/Familia (o ENV PHASE2_HINTS_XLSX)
      - progress_cb / on_progress / progress_callback / callback / logger
        (callback de progreso: recibe strings con el avance)
    """
    # ---- alias compat GUI ---------------------------------------------------
    if bc3_in is None:
        bc3_in = kwargs.pop("input_bc3", None)
    if catalog_xlsx is None:
        catalog_xlsx = kwargs.pop("catalog_path", None)
    if bc3_out is None:
        bc3_out = kwargs.pop("output_bc3", None)

    if bc3_in is None or catalog_xlsx is None:
        raise ValueError("run_phase2: faltan 'bc3_in' y/o 'catalog_xlsx'.")

    bc3_in = Path(bc3_in)
    catalog_xlsx = Path(catalog_xlsx)
    if bc3_out is None:
        bc3_out = bc3_in.with_name(bc3_in.stem + "_clasificado.bc3")
    else:
        bc3_out = Path(bc3_out)

    # ---- extras opcionales --------------------------------------------------
    use_heuristics = kwargs.pop("use_heuristics", None)
    fewshots = kwargs.pop("fewshots", None)
    hints_tree_xlsx = kwargs.pop("hints_tree_xlsx", None)

    # ---- callback progreso --------------------------------------------------
    progress_cb = None
    for k in ("progress_cb", "on_progress", "progress_callback", "callback", "logger"):
        if k in kwargs and kwargs[k] is not None:
            progress_cb = kwargs[k]
            break

    # ---- conteo estimado de descompuestos ----------------------------------
    if progress_cb:
        try:
            total = 0
            with bc3_in.open("r", encoding="latin-1", errors="ignore") as fh:
                for raw in fh:
                    if raw.startswith("~C|"):
                        parts = raw.rstrip("\n").split("|")
                        if len(parts) >= 7 and parts[6] in {"1", "2", "3"}:
                            total += 1
            progress_cb(f"Detectados {total} descompuestos en el BC3.")
        except Exception:
            pass

    # ---- construir mapeo + lista (old,new,conf,method) ---------------------
    repl_map, rows = _build_replacement_map(
        bc3_in, catalog_xlsx,
        use_heuristics=use_heuristics,
        fewshots=fewshots,
        hints_tree_xlsx=hints_tree_xlsx,
    )

    # Progreso por cada reemplazo (post-cálculo)
    if progress_cb and rows:
        total = len(rows)
        for i, (oldc, newc, conf, method) in enumerate(rows, 1):
            progress_cb(f"{i}/{total} | {oldc} → {newc} ({conf:.2f}, {method})")

    # ---- reescritura y CSV --------------------------------------------------
    rewrite_bc3_with_codes(bc3_in, bc3_out, repl_map)
    _cleanup_trailing_pipes_file(bc3_out)
    map_csv = bc3_out.with_name(bc3_out.stem + "_map.csv")
    _write_mapping_csv(rows, map_csv)

    try:
        _final_trim_trailing_pipes(bc3_out)
    except Exception:
        # si algo va mal, no bloqueamos el flujo principal
        pass

    return bc3_out
