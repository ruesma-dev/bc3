# application/services/product_selection_service.py
from __future__ import annotations

import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from bc3_lib.domain.node import Node
from infrastructure.product_catalog.product_catalog import ProductCatalog
from infrastructure.llm.gemini_client import GeminiClient, GeminiSelection
from config.settings import (
    MAX_CODE_LEN,
    USE_DESTRUCTIVE_RENAME,
    USE_LOCAL_FALLBACK,
    GEMINI_BATCH_SIZE,
    PREFILTER_TOPK,
    GEMINI_MIN_CONFIDENCE,
    MAP_KINDS,
    DISCOUNT_PRODUCT_CODE,
    PRICE_LOCK_MODE
)

# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ProductMatch:
    node_code_old: str
    product_code: str
    confidence: float
    reason: str


def _iter_target_nodes(roots: Iterable[Node]) -> Iterable[Node]:
    """Solo nodos a mapear (por defecto, materiales)."""
    def dfs(n: Node):
        if n.kind in MAP_KINDS:
            yield n
        for ch in n.children:
            yield from dfs(ch)
    for r in roots:
        yield from dfs(r)


def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))


def _tokens(s: str) -> set[str]:
    s = _strip_accents(s or "").lower()
    return set(re.findall(r"[a-z0-9]+", s))


def _prefilter_candidates(query: str, catalog: ProductCatalog, k: int) -> List[dict]:
    qtok = _tokens(query)
    scored: List[tuple[float, str, str]] = []
    for p in catalog.products:
        ptok = _tokens(p.name)
        if not ptok:
            continue
        inter = len(qtok & ptok)
        union = len(qtok | ptok)
        score = inter / union if union else 0.0
        if score > 0.0:
            scored.append((score, p.code, p.name))
    scored.sort(reverse=True)
    top = scored[:k] if k > 0 else scored
    return [{"code": c, "name": n} for _, c, n in top]


def _normalize_bc3_code(raw: str) -> str:
    code = re.sub(r"[^A-Za-z0-9._-]", "", raw.upper())
    return code[:MAX_CODE_LEN]


def _assign_unique(new_code: str, used: set[str]) -> str:
    if new_code not in used:
        used.add(new_code)
        return new_code
    base = new_code[: max(1, MAX_CODE_LEN - 3)]
    i = 1
    while True:
        cand = f"{base}{i}"[:MAX_CODE_LEN]
        if cand not in used:
            used.add(cand)
            return cand
        i += 1


def _index_nodes(roots: List[Node]) -> Dict[str, Node]:
    idx: Dict[str, Node] = {}
    def dfs(n: Node) -> None:
        if n.code not in idx:
            idx[n.code] = n
            for ch in n.children:
                dfs(ch)
    for r in roots:
        dfs(r)
    return idx


def _parent_map(roots: List[Node]) -> Dict[str, str]:
    """child_code -> parent_code (para comprobar hermanos)."""
    pmap: Dict[str, str] = {}
    def dfs(n: Node):
        for ch in n.children:
            pmap[ch.code] = n.code
            dfs(ch)
    for r in roots:
        dfs(r)
    return pmap

def _is_discount_by_unit(unit: str | None) -> bool:
    """Regla DESCUENTO: únicamente si la unidad original es '%'."""
    return (unit or "").strip() == "%"


# ─────────────────────────── Selección INDIVIDUAL ───────────────────────────
def build_product_code_mapping(
    *,
    roots: List[Node],
    catalog: ProductCatalog,
    gemini: GeminiClient,
    min_confidence: float = GEMINI_MIN_CONFIDENCE,
) -> Tuple[Dict[str, str], List[ProductMatch]]:
    """
    Devuelve old_code -> BASE_product_code (sin sufijos globales).
    Regla DESCUENTO: SOLO si la UNIDAD original es '%'.
    Las colisiones entre hermanos se resuelven en rewrite_bc3_with_product_codes().
    """
    mapping: Dict[str, str] = {}
    matches: List[ProductMatch] = []

    products_text = "\n".join(f'- code: "{p.code}" | name: "{p.name}"' for p in catalog.products)
    cache: Dict[tuple[str, str], GeminiSelection] = {}

    for node in _iter_target_nodes(roots):
        raw_code = node.code or ""
        unit = (node.unidad or "").strip()
        short_desc = node.description or ""
        long_desc = node.long_desc or ""
        sig = (short_desc.strip().lower(), long_desc.strip().lower())

        # ---- REGLA DESCUENTO por UNIDAD '%'
        if _is_discount_by_unit(unit):
            pick_code = DISCOUNT_PRODUCT_CODE
            pick_conf = 1.0
            pick_reason = "rule:discount_unit"
        else:
            # LLM (cacheado por firma short/long)
            if sig not in cache:
                cache[sig] = gemini.select_product(
                    short_desc=short_desc,
                    long_desc=long_desc,
                    products_prompt_list=products_text,
                )
            sel = cache[sig]
            pick_code, pick_conf, pick_reason = sel.product_code, sel.confidence, sel.reason

            # Fallback simple
            if (not pick_code or pick_conf < min_confidence) and USE_LOCAL_FALLBACK:
                candidates = _prefilter_candidates(long_desc or short_desc, catalog, k=1)
                if candidates:
                    pick_code = candidates[0]["code"]
                    pick_conf = min(pick_conf, 0.5)
                    pick_reason = f"{pick_reason or ''}|fallback:jaccard".strip("|")

        if not pick_code:
            continue

        base = _normalize_bc3_code(pick_code)  # ← sin unicidad global
        if base != raw_code:
            mapping[raw_code] = base
            matches.append(ProductMatch(raw_code, base, float(pick_conf), pick_reason))

    return mapping, matches


def build_product_code_mapping_batch(
    *,
    roots: List[Node],
    catalog: ProductCatalog,
    gemini: GeminiClient,
    batch_size: int = GEMINI_BATCH_SIZE,
    topk: int = PREFILTER_TOPK,
    min_confidence: float = GEMINI_MIN_CONFIDENCE,
) -> Tuple[Dict[str, str], List[ProductMatch]]:
    """
    Versión batch: devuelve CÓDIGOS BASE sin sufijos globales.
    Regla DESCUENTO: SOLO si la UNIDAD original es '%'.
    """
    mapping: Dict[str, str] = {}
    matches: List[ProductMatch] = []

    # 1) Preparar lotes (dedupe por firma); primero resolvemos los '%' (no van a LLM)
    sig_to_nodes: Dict[str, List[Node]] = {}
    items: List[Dict[str, Any]] = []

    for node in _iter_target_nodes(roots):
        raw_code = node.code or ""
        unit = (node.unidad or "").strip()
        short_desc = (node.description or "").strip()
        long_desc = (node.long_desc or "").strip()

        # DESCUENTO directo por UNIDAD '%'
        if _is_discount_by_unit(unit):
            base = _normalize_bc3_code(DISCOUNT_PRODUCT_CODE)
            if base != raw_code:
                mapping[raw_code] = base
                matches.append(ProductMatch(raw_code, base, 1.0, "rule:discount_unit"))
            continue

        # Para LLM (deduplicado por firma)
        sig = f"{short_desc.lower()}||{long_desc.lower()}"
        if sig not in sig_to_nodes:
            q = long_desc or short_desc
            candidates = _prefilter_candidates(q, catalog, k=topk)
            if not candidates:
                candidates = [{"code": p.code, "name": p.name} for p in catalog.products]
            items.append({"id": sig, "short": short_desc, "long": long_desc, "candidates": candidates})
            sig_to_nodes[sig] = []
        sig_to_nodes[sig].append(node)

    # 2) Llamadas batch al LLM
    for i in range(0, len(items), max(1, batch_size)):
        chunk = items[i : i + batch_size]
        results = gemini.select_products_batch(items=chunk) or []
        rmap = {r.get("id"): r for r in results}

        for it in chunk:
            rid = it["id"]
            r = rmap.get(rid, {})
            pick_code = str(r.get("product_code", "")).strip()
            pick_conf = float(r.get("confidence", 0.0))
            pick_reason = str(r.get("reason", "")).strip()

            if (not pick_code or pick_conf < min_confidence) and USE_LOCAL_FALLBACK:
                cand = (it.get("candidates") or [])
                if cand:
                    pick_code = cand[0]["code"]
                    pick_conf = min(pick_conf, 0.5)
                    pick_reason = f"{pick_reason or ''}|fallback:jaccard".strip("|")

            if not pick_code:
                continue

            base = _normalize_bc3_code(pick_code)
            for node in sig_to_nodes[rid]:
                raw_code = node.code or ""
                if base != raw_code:
                    mapping[raw_code] = base
                    matches.append(ProductMatch(raw_code, base, float(pick_conf), pick_reason))

    return mapping, matches


# ───────────────────────── Reescritura BC3 (con sufijos) ────────────────────
def rewrite_bc3_with_product_codes(path: Path, code_map: Dict[str, str]) -> None:
    r"""
    Reescritura conservando rendimiento y precio UNITARIO del descompuesto original.

    • Para cada descompuesto mapeado se crea una VARIANTE GLOBAL del código base
      según la tupla (base, desc_corta, unidad, precio, desc_larga).
    • La variante copia: descripción corta/larga, UNIDAD y PRECIO del descompuesto original.
    • Las ~D mantienen coeficiente y cantidad ORIGINAL (sin ajustes).
    • En la MISMA ~D, si se repite la variante: clones locales a/b/c... (copian la variante).
    • Solo saneamos ~C de productos. Capítulos/partidas no se tocan.
    • Todos los ~D acaban con barra invertida y tubería («\|»).
    """
    if not code_map:
        return

    MAX_CODE_LEN = 20
    letters = "abcdefghijklmnopqrstuvwxyz"

    def letter_at(idx: int) -> str:
        j = idx - 2
        return letters[j] if 0 <= j < len(letters) else f"x{idx}"

    def with_letter(base: str, idx: int) -> str:
        return (base + letter_at(idx))[:MAX_CODE_LEN]

    def trim_trailing_empty(fields: List[str]) -> List[str]:
        i = len(fields)
        while i > 0 and fields[i - 1] == "":
            i -= 1
        return fields[:i]

    def sanitize_c_parts(parts: List[str], *, for_product: bool) -> List[str]:
        # [code, unidad, desc, precio, fecha, tipo]
        parts = (parts + [""] * 6)[:6]
        if for_product:
            if not parts[1].strip():
                parts[1] = "UD"
            if not str(parts[3]).strip():
                parts[3] = "0"
            if not str(parts[5]).strip():
                parts[5] = "3"
        return parts

    # ───────── Parse ─────────
    lines = path.read_text("latin-1", errors="ignore").splitlines()

    c_parts_by_code: Dict[str, List[str]] = {}     # ~C por código (deduplicado)
    t_text_by_code: Dict[str, str] = {}            # ~T (primera) por código
    c_lines: List[Tuple[str, List[str]]] = []      # (~C, parts) (para conservar orden aproximado)
    t_lines: List[Tuple[str, str]] = []            # (~T, raw)
    d_records: List[Tuple[str, List[str]]] = []    # (parent, chunks)
    other_lines: List[str] = []

    def _tipo(prts: List[str]) -> str:
        return (prts + [""] * 6)[5]

    def _c_priority(prts: List[str]) -> int:
        """
        Priorizamos:
          3) Código con '##' (supercapítulo)
          2) Código con '#'  (capítulo)
          1) Tipo '0' (partida)
          0) Resto (1/2/3/…)
        """
        code = prts[0] if prts else ""
        if "##" in code:
            return 3
        if "#" in code:
            return 2
        if _tipo(prts) == "0":
            return 1
        return 0

    for raw in lines:
        if raw.startswith("~C|"):
            # ~C|code|unidad|desc|precio|fecha|tipo|... (recortamos vacíos al final)
            _, rest = raw.split("|", 1)
            parts = rest.rstrip("\n").split("|")
            # quita trailing vacíos para no confundir el comparador
            i = len(parts)
            while i > 0 and parts[i - 1] == "":
                i -= 1
            parts = parts[:i] if i else parts

            if parts:
                code = parts[0]
                # Si no existe, guardamos la primera aparición
                if code not in c_parts_by_code:
                    c_parts_by_code[code] = parts
                    c_lines.append(("~C", parts))
                else:
                    # Si ya existe, solo sustituimos si la nueva tiene MAYOR prioridad
                    # (pero nunca dejamos que una T=3 pise a una T=0 o capítulo)
                    old = c_parts_by_code[code]
                    if _c_priority(parts) > _c_priority(old):
                        c_parts_by_code[code] = parts
                        # actualizamos también c_lines (sustituimos la última tupla de ese code)
                        for idx in range(len(c_lines) - 1, -1, -1):
                            if c_lines[idx][1][0] == code:
                                c_lines[idx] = ("~C", parts)
                                break
            continue

        if raw.startswith("~T|"):
            _, rest = raw.split("|", 1)
            code, txt = (rest.rstrip("\n").split("|", 1) + [""])[:2]
            # Nos quedamos con la PRIMERA ~T que aparece para ese código
            if code not in t_text_by_code:
                t_text_by_code[code] = txt
            t_lines.append(("~T", raw.rstrip("\n")))
            continue

        if raw.startswith("~D|"):
            _, rest = raw.split("|", 1)
            parent, child_part = rest.split("|", 1)
            chunks = child_part.rstrip("|").split("\\")
            d_records.append((parent, chunks))
            continue

        other_lines.append(raw)

    # ───────── Variantes globales por (base, desc, unidad, precio[, long]) ─────────
    variant_code_by_key: Dict[Tuple[str, str, str, str, str], str] = {}
    variant_parts: Dict[str, List[str]] = {}     # ~C parts de cada variante
    variant_long_text: Dict[str, str] = {}       # ~T de cada variante
    base_variant_counter: Dict[str, int] = {}

    def ensure_variant_for(old_code: str, base: str) -> str:
        # datos originales del descompuesto
        src = c_parts_by_code.get(old_code, [old_code, "UD", "", "0", "", "3"])
        src = sanitize_c_parts(src, for_product=True).copy()
        short = (src[2] or "").strip()
        unit  = (src[1] or "").strip()
        price = (src[3] or "").strip()
        longt = (t_text_by_code.get(old_code, "") or "").strip()
        key = (base, short, unit, price, longt)

        if key in variant_code_by_key:
            return variant_code_by_key[key]

        # Asignar código de variante global
        if base not in base_variant_counter:
            code = base
            base_variant_counter[base] = 1
        else:
            base_variant_counter[base] += 1
            code = with_letter(base, base_variant_counter[base])

        # Construir ~C de variante copiando EXACTAMENTE unidad, desc y precio del original
        vp = ["", "", "", "", "", ""]
        vp[0] = code
        vp[1] = unit or "UD"
        vp[2] = short
        vp[3] = price or "0"
        vp[4] = (src[4] or "")             # fecha si venía
        vp[5] = "3"                        # forzamos material
        variant_parts[code] = vp

        if longt:
            variant_long_text[code] = longt

        variant_code_by_key[key] = code
        return code

    # ───────── Reescritura ~D (sin tocar coef ni qty) ─────────
    used_in_D: set[str] = set()
    clones_needed: Dict[str, str] = {}    # clone -> variant
    new_D_lines: List[str] = []

    for parent, chunks in d_records:
        # preparar triples
        triples: List[Tuple[str, str, str, str]] = []
        for i in range(0, len(chunks), 3):
            old = chunks[i].strip() if i < len(chunks) else ""
            if not old:
                continue
            coef = chunks[i + 1] if i + 1 < len(chunks) else ""
            qty  = chunks[i + 2] if i + 2 < len(chunks) else ""
            base = code_map.get(old, old)
            triples.append((old, base, coef, qty))

        seen_local: Dict[str, int] = {}
        new_chunks: List[str] = []

        for old, base, coef, qty in triples:
            vcode = ensure_variant_for(old, base)

            # evitar colapso entre hermanos en ESTA ~D
            final = vcode
            seen_local[final] = seen_local.get(final, 0) + 1
            if seen_local[final] > 1:
                clone = with_letter(vcode, seen_local[final])
                clones_needed[clone] = vcode
                final = clone

            new_chunks.extend([final, coef, qty])  # coef y qty ORIGINALES
            used_in_D.add(final)

        body = "\\".join(new_chunks) + "\\"
        new_D_lines.append(f"~D|{parent}|{body}|")

    # ───────── Reconstrucción ~C y ~T ─────────
    mapped_olds = set(code_map.keys())
    planned_dest = used_in_D | set(variant_parts.keys()) | set(clones_needed.keys())

    out: List[str] = []
    out.extend(other_lines)
    written_c: set[str] = set()
    written_t: set[str] = set()

    # 1) ~C no afectados (capítulos/partidas…) → no tocar tipo/unidad
    for _tag, parts in c_lines:
        code = parts[0]
        if code in mapped_olds:
            continue
        if code in planned_dest:
            continue
        if code in written_c:
            continue
        p = sanitize_c_parts(parts, for_product=False)
        out.append("~C|" + "|".join(p) + "|")
        written_c.add(code)

    # 2) ~C de variantes (unidad, desc y precio del ORIGINAL)
    for vcode, vparts in sorted(variant_parts.items()):
        if vcode not in used_in_D or vcode in written_c:
            continue
        p = sanitize_c_parts(vparts, for_product=True)
        p[0] = vcode
        out.append("~C|" + "|".join(p) + "|")
        written_c.add(vcode)

    # 3) ~C de clones (copian la variante)
    for clone, vcode in sorted(clones_needed.items()):
        if clone in written_c:
            continue
        base_v = variant_parts.get(vcode)
        if not base_v:
            continue
        p = sanitize_c_parts(base_v, for_product=True)
        p[0] = clone
        out.append("~C|" + "|".join(p) + "|")
        written_c.add(clone)

    # 4) ~T no afectados
    for _tag, raw in t_lines:
        code = raw.split("|", 2)[1]
        if code in mapped_olds:
            continue
        if code in planned_dest:
            continue
        if code in written_t:
            continue
        out.append(raw + "|") if not raw.endswith("|") else out.append(raw)
        written_t.add(code)

    # 5) ~T para variantes y clones (larga original)
    for vcode, txt in sorted(variant_long_text.items()):
        if vcode in used_in_D and vcode not in written_t:
            out.append(f"~T|{vcode}|{txt}")
            written_t.add(vcode)
    for clone, vcode in sorted(clones_needed.items()):
        if vcode in variant_long_text and clone not in written_t:
            out.append(f"~T|{clone}|{variant_long_text[vcode]}")
            written_t.add(clone)

    # 6) ~D reescritos
    out.extend(new_D_lines)

    Path(path).write_text("\n".join(out) + "\n", encoding="latin-1", errors="ignore")

def apply_code_mapping_to_nodes(roots: List[Node], code_map: Dict[str, str]) -> None:
    """
    Aplica en memoria el mapeo old_code -> new_code sobre el árbol de nodos.
    Nota: si durante la reescritura del BC3 se han generado códigos con sufijo
    por colisión entre hermanos (…a, …b, …), esta función no los conoce, porque
    se calculan en la fase de reescritura de ~D. Si necesitas que el árbol
    refleje exactamente los sufijos finales, reparsea el BC3 tras reescribir.
    """
    def dfs(n: Node) -> None:
        if n.code in code_map:
            n.code = code_map[n.code]
        for ch in n.children:
            dfs(ch)

    for r in roots:
        dfs(r)