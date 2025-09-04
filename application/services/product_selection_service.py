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
    if not code_map:
        return

    letters = "abcdefghijklmnopqrstuvwxyz"

    def letter_at(idx: int) -> str:
        j = idx - 2  # 2->a, 3->b...
        return letters[j] if 0 <= j < len(letters) else f"x{idx}"

    def with_letter(base: str, idx: int) -> str:
        return (base + letter_at(idx))[:MAX_CODE_LEN]

    def trim_trailing_empty(fields: List[str]) -> List[str]:
        i = len(fields)
        while i > 0 and fields[i - 1] == "":
            i -= 1
        return fields[:i]

    # 1) Leemos todo y guardamos:
    #   - ~C originales por código (para tomar UD/Precio/Texto del ORIGEN)
    #   - ~D por partida
    #   - resto de líneas tal cual
    lines = path.read_text("latin-1", errors="ignore").splitlines()
    c_parts_by_code: Dict[str, List[str]] = {}      # OLD/otros -> parts (recortados)
    c_lines: List[Tuple[str, List[str]]] = []       # (~C, parts) para re-emisión selectiva
    d_records: List[Tuple[str, List[str]]] = []     # (parent, chunks)
    other_lines: List[str] = []

    for raw in lines:
        if raw.startswith("~C|"):
            _, rest = raw.split("|", 1)
            parts = trim_trailing_empty(rest.rstrip("\n").split("|"))
            if parts:
                code = parts[0]
                c_parts_by_code[code] = parts
                c_lines.append(("~C", parts))
            continue
        if raw.startswith("~D|"):
            _, rest = raw.split("|", 1)
            parent, child_part = rest.split("|", 1)
            chunks = child_part.rstrip("|").split("\\")
            d_records.append((parent, chunks))
            continue
        other_lines.append(raw)

    # 2) Reescribimos ~D por partida aplicando:
    #    - mapping OLD->BASE
    #    - sufijo por hermanos (1º sin sufijo; 2º->a, 3º->b, ...)
    #    - construimos un índice final_code -> parts_origen (para emitir ~C de destino)
    new_D_lines: List[str] = []
    final_code_parts: Dict[str, List[str]] = {}  # código final -> parts del ORIGEN que lo originó
    used_in_D: set[str] = set()

    for parent, chunks in d_records:
        # 2.1 descompone en triples ordenados
        triples: List[Tuple[str, str, str, str]] = []  # (old, base, coef, qty)
        for i in range(0, len(chunks), 3):
            old = chunks[i].strip() if i < len(chunks) else ""
            if not old:
                continue
            coef = chunks[i + 1] if i + 1 < len(chunks) else ""
            qty = chunks[i + 2] if i + 2 < len(chunks) else ""
            base = code_map.get(old, old)
            triples.append((old, base, coef, qty))

        # 2.2 detectar colisiones entre HERMANOS por BASE (en esta partida)
        olds_by_base: Dict[str, List[str]] = {}
        for old, base, _, _ in triples:
            olds_by_base.setdefault(base, []).append(old)

        # 2.3 reescribir preservando el orden; 1º sin sufijo, 2º→a, etc.
        seen_idx_per_base: Dict[str, int] = {}
        new_chunks: List[str] = []

        for old, base, coef, qty in triples:
            final_code = base
            olds_here = olds_by_base.get(base, [])
            if len(set(olds_here)) > 1:
                seen_idx_per_base[base] = seen_idx_per_base.get(base, 0) + 1
                idx = seen_idx_per_base[base]
                if idx > 1:
                    final_code = with_letter(base, idx)

            # apuntar los parts del ORIGEN que definen este código final (price lock)
            parts_old = c_parts_by_code.get(old)
            if parts_old:  # sólo si tenemos ~C origen
                final_code_parts.setdefault(final_code, parts_old)

            new_chunks.extend([final_code, coef, qty])
            used_in_D.add(final_code)

        body = "\\".join(new_chunks) + "\\"
        new_D_lines.append(f"~D|{parent}|{body}|")

    # 3) Reconstruimos ~C:
    #    - NO emitimos ~C de códigos OLD mapeados (se sustituyen)
    #    - NO emitimos ~C de códigos que vamos a volver a escribir como destino
    #    - Emitimos ~C de destino (final_code_parts) heredando los parts del ORIGEN
    mapped_olds = set(code_map.keys())
    planned_dest = used_in_D  # todos los códigos finales citados en ~D

    out: List[str] = []
    out.extend(other_lines)  # el resto tal cual

    written_c: set[str] = set()

    # 3.1 reemitimos ~C no afectados
    for _tag, parts in c_lines:
        code = parts[0]
        if code in mapped_olds:           # OLD mapeado -> lo sustituimos (no reemitir)
            continue
        if code in planned_dest:          # lo vamos a escribir como destino -> evitar duplicado
            continue
        if code in written_c:
            continue
        parts = trim_trailing_empty(parts)
        out.append("~C|" + "|".join(parts) + "|")
        written_c.add(code)

    # 3.2 emitimos ~C de destino con parts del ORIGEN (price lock)
    for final_code, parts_src in final_code_parts.items():
        if final_code in written_c:
            continue
        p = parts_src.copy()
        p[0] = final_code
        p = trim_trailing_empty(p)
        out.append("~C|" + "|".join(p) + "|")
        written_c.add(final_code)

    # 4) Añadimos las ~D nuevas (todas con barra invertida final)
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