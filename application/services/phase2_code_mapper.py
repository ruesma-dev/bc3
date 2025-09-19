# application/services/phase2_code_mapper.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Any
import os
import re

from utils.text_sanitize import clean_text
from infrastructure.ai.gemini_client import (
    RateLimiter,
    choose_best_code_with_llm,
    choose_best_code_batch_with_llm,
)
from infrastructure.products.catalog_loader import load_catalog

MAX_CODE_LEN = 20
NUM_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")


@dataclass
class Concept:
    code: str
    unidad: str
    desc_short: str
    price_txt: str
    tipo: str
    long_desc: str | None = None


def _to_float(s: str) -> float | None:
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s.replace(",", "."))
    except Exception:
        return None


def _tokenize(text: str) -> List[str]:
    text = clean_text(text).lower()
    return re.findall(r"[a-z0-9]+", text)


def _score(a: str, b: str) -> int:
    ta = set(_tokenize(a))
    tb = set(_tokenize(b))
    return len(ta & tb)


def _prefilter(catalog: List[Dict[str, str]], query: str, topk: int) -> List[Dict[str, str]]:
    scored = [(c, _score(query, c["desc"])) for c in catalog]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [c for c, _ in scored[: max(1, topk)]]


def _letters_suffix(n: int) -> str:
    # 1->a, 2->b, ... 26->z, 27->aa, etc.
    s = ""
    while n > 0:
        n -= 1
        s = chr(97 + (n % 26)) + s
        n //= 26
    return s


def _safe_with_suffix(base: str, suffix: str) -> str:
    base = base[: max(0, MAX_CODE_LEN - len(suffix))]
    return (base + suffix)[:MAX_CODE_LEN]


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


def _build_replacement_map(
    bc3_path: Path,
    catalog_path: Path,
    topk: int = 20,
    min_conf: float = 0.0,
) -> Dict[str, str]:
    """
    Calcula old_code -> new_code SOLO para descompuestos (T=3 o T original 1/2/3).
    Reglas:
      - Si el código contiene '%', usar **% DESCUENTO#** (1,2,3, …).
      - En el resto, usar LLM sobre candidatos del catálogo (prefiltro Top-K).
      - Unicidad: si el mismo código del catálogo se asigna varias veces,
        añadir sufijos a, b, c… para no colisionar (máx 20 chars).
    """
    catalog = load_catalog(catalog_path)
    concepts, _longs, parent_of, _children = _collect_bc3_info(bc3_path)

    # Limitador según rate limits (ajustable por .env)
    rpm = int(os.getenv("GEMINI_RPM", "10") or "10")
    limiter = RateLimiter(rpm=rpm)

    batch_mode = (os.getenv("GEMINI_BATCH_MODE", "false").strip().lower() == "true")
    batch_size = max(1, int(os.getenv("GEMINI_BATCH_SIZE", "10") or "10"))

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

    # Recolectamos descompuestos candidatos
    targets: List[str] = []
    for code, c in concepts.items():
        if "#" in code:
            continue
        if c.tipo not in {"1", "2", "3"}:
            continue
        targets.append(code)

    # Procesamiento
    idx = 0
    while idx < len(targets):
        group = targets[idx : idx + (batch_size if batch_mode else 1)]
        group_payload: List[Dict[str, Any]] = []
        fallbacks: Dict[str, str] = {}

        for code in group:
            c = concepts[code]

            # EXCEPCIÓN: si el código contiene '%', SIEMPRE "% DESCUENTO#"
            if "%" in code:
                repl[code] = unique_assign("% DESCUENTO")
                continue

            # Contexto y prefiltro
            ctx = _context_for(code, concepts, parent_of)
            k = int(os.getenv("PREFILTER_TOPK", str(topk)) or topk)
            top_candidates = _prefilter(catalog, f"{c.desc_short} {c.long_desc or ''} {ctx}", k)
            fallbacks[code] = top_candidates[0]["code"]
            group_payload.append({
                "id": code,
                "context": ctx,
                "candidates": [{"code": cc["code"], "desc": cc["desc"]} for cc in top_candidates],
            })

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
                    if not best or conf < float(os.getenv("GEMINI_MIN_CONFIDENCE", str(min_conf)) or 0.0):
                        best = fallbacks[code]
                    repl[code] = unique_assign(best)
            else:
                for item in group_payload:
                    code = item["id"]
                    if code in repl:
                        continue
                    llm = choose_best_code_with_llm(item["context"], item["candidates"], limiter=limiter)
                    best = (llm.get("best_code") or "").strip() or fallbacks[code]
                    conf = float(llm.get("confidence", 0.0))
                    if conf < float(os.getenv("GEMINI_MIN_CONFIDENCE", str(min_conf)) or 0.0):
                        best = fallbacks[code]
                    repl[code] = unique_assign(best)

        except Exception:
            for item in group_payload:
                code = item["id"]
                if code in repl:
                    continue
                repl[code] = unique_assign(fallbacks[code])

        idx += len(group)

    return repl


def rewrite_bc3_with_codes(src: Path, dst: Path, repl_map: Dict[str, str]) -> None:
    """
    Reescribe el BC3 aplicando solo el cambio de CÓDIGO (nada más):
      - ~C: cambia el campo 'code' si está en repl_map
      - ~D: cambia únicamente el 'child_code' en los tripletes  (child\coef\qty)
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


def run_phase2(
    bc3_in: Path,
    catalog_xlsx: Path,
    bc3_out: Path | None = None,
) -> Path:
    """
    Fase 2: clasifica descompuestos contra catálogo y sustituye códigos.
      - bc3_in: BC3 (recomendado el *_material/_limpio.bc3 de la fase1)
      - catalog_xlsx: Excel con catálogo (2 columnas)
      - bc3_out: si None, crea junto al input con sufijo '_clasificado.bc3'
    """
    if bc3_out is None:
        bc3_out = bc3_in.with_name(bc3_in.stem + "_clasificado.bc3")

    repl_map = _build_replacement_map(bc3_in, catalog_xlsx)
    rewrite_bc3_with_codes(bc3_in, bc3_out, repl_map)
    return bc3_out
