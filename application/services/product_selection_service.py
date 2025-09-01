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
    Si el CÓDIGO ORIGINAL contiene '%' o 'DESC' → 'DESCUENTO'.
    Los posibles sufijos a/b/c se aplican MÁS TARDE por partida.
    """
    mapping: Dict[str, str] = {}
    matches: List[ProductMatch] = []

    products_text = "\n".join(f'- code: "{p.code}" | name: "{p.name}"' for p in catalog.products)
    cache: Dict[tuple[str, str], GeminiSelection] = {}

    for node in _iter_target_nodes(roots):
        raw_code = node.code or ""
        short_desc = node.description or ""
        long_desc = node.long_desc or ""
        sig = (short_desc.strip().lower(), long_desc.strip().lower())

        # Regla DESCUENTO por código original
        if "%" in raw_code or "DESC" in raw_code.upper():
            pick_code = DISCOUNT_PRODUCT_CODE
            pick_conf = 1.0
            pick_reason = "rule:discount"
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

        base = _normalize_bc3_code(pick_code)  # ← SOLO normalizamos; NUNCA _assign_unique
        if base != node.code:
            mapping[node.code] = base
            matches.append(ProductMatch(node.code, base, float(pick_conf), pick_reason))

    return mapping, matches



# ────────────────────────────── Selección BATCH ─────────────────────────────
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
    Igual que la individual, pero en batch. Devuelve SIEMPRE códigos BASE.
    La resolución de colisiones entre hermanos la hace rewrite_bc3_with_product_codes().
    """
    mapping: Dict[str, str] = {}
    matches: List[ProductMatch] = []

    # 1) Preparar lotes (aplicar DESCUENTO antes; dedupe por firma)
    sig_to_nodes: Dict[str, List[Node]] = {}
    items: List[Dict[str, Any]] = []

    for node in _iter_target_nodes(roots):
        raw_code = node.code or ""
        short_desc = (node.description or "").strip()
        long_desc = (node.long_desc or "").strip()

        # DESCUENTO directo
        if "%" in raw_code or "DESC" in raw_code.upper():
            base = _normalize_bc3_code(DISCOUNT_PRODUCT_CODE)
            if base != node.code:
                mapping[node.code] = base
                matches.append(ProductMatch(node.code, base, 1.0, "rule:discount"))
            continue

        sig = f"{short_desc.lower()}||{long_desc.lower()}"
        if sig not in sig_to_nodes:
            q = long_desc or short_desc
            candidates = _prefilter_candidates(q, catalog, k=topk)
            if not candidates:
                candidates = [{"code": p.code, "name": p.name} for p in catalog.products]
            items.append({"id": sig, "short": short_desc, "long": long_desc, "candidates": candidates})
            sig_to_nodes[sig] = []
        sig_to_nodes[sig].append(node)

    # 2) Llamadas batch
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

            base = _normalize_bc3_code(pick_code)  # ← SIN _assign_unique
            for node in sig_to_nodes[rid]:
                if base != node.code:
                    mapping[node.code] = base
                    matches.append(ProductMatch(node.code, base, float(pick_conf), pick_reason))

    return mapping, matches


# ───────────────────────── Reescritura BC3 (con sufijos) ────────────────────
def rewrite_bc3_with_product_codes(path: Path, code_map: Dict[str, str]) -> None:
    """
    Renombra códigos (~C y referencias) aplicando:
      • Mapa old_code -> base_new_code (sin sufijos globales)
      • Dentro de cada ~D (por partida): si DOS O MÁS hijos quedan con
        el MISMO base_new y vienen de códigos originales DISTINTOS, añade
        sufijos a,b,c… en el 2º, 3º, … y crea sus ~C clonados.
    """
    if not code_map:
        return

    if not USE_DESTRUCTIVE_RENAME:
        tmp = path.with_suffix(path.suffix + ".tmp")
        with path.open("r", encoding="latin-1", errors="ignore") as fin, \
             tmp.open("w", encoding="latin-1", errors="ignore") as fout:
            for raw in fin:
                if raw.startswith("~C|"):
                    _, rest = raw.split("|", 1)
                    parts = rest.rstrip("\n").split("|")
                    code = parts[0]
                    fout.write(raw)
                    if code in code_map:
                        fout.write(f"~T|{code}|PRD:{code_map[code]}|\n")
                else:
                    fout.write(raw)
        tmp.replace(path)
        return

    MAX_CODE_LEN = 20
    letters = "abcdefghijklmnopqrstuvwxyz"

    def with_letter(base: str, idx: int) -> str:
        # idx=2->a, 3->b, …; si se agota, usa 'xN'
        j = idx - 2
        suf = letters[j] if 0 <= j < len(letters) else f"x{idx}"
        return (base + suf)[:MAX_CODE_LEN]

    lines = path.read_text("latin-1", errors="ignore").splitlines()
    out: list[str] = []

    # Guardaremos el ~C "base" para poder clonar luego
    concept_map: Dict[str, list[str]] = {}
    concept_written: set[str] = set()
    clones_needed: Dict[str, str] = {}  # clone -> base

    for raw in lines:
        if raw.startswith("~C|"):
            head, rest = raw.split("|", 1)
            parts = rest.rstrip("\n").split("|")
            if parts:
                old = parts[0]
                base_new = code_map.get(old, old)
                parts[0] = base_new
                if base_new not in concept_map:
                    concept_map[base_new] = parts.copy()
                if base_new not in concept_written:
                    out.append(f"{head}|{'|'.join(parts)}|")
                    concept_written.add(base_new)
            continue

        if raw.startswith("~D|"):
            # ~D|PADRE|hijo\coef\cant\hijo\...\|
            _, rest = raw.split("|", 1)
            parent_code, child_part = rest.split("|", 1)
            chunks = child_part.rstrip("|").split("\\")

            # 1) Mapeamos cada triple a (old, base, coef, qty)
            triples: list[tuple[str, str, str, str]] = []
            for i in range(0, len(chunks), 3):
                old = chunks[i].strip()
                if not old:
                    continue
                coef = chunks[i + 1] if i + 1 < len(chunks) else ""
                qty = chunks[i + 2] if i + 2 < len(chunks) else ""
                base = code_map.get(old, old)
                triples.append((old, base, coef, qty))

            # 2) Agrupamos por base y contamos solo si provienen de OLD distintos
            group_old: Dict[str, list[str]] = {}
            for old, base, _, _ in triples:
                group_old.setdefault(base, []).append(old)

            # 3) Escribimos aplicando sufijo SOLO si hay >1 OLD distinto
            seen_per_base: Dict[str, int] = {}
            new_chunks: list[str] = []
            for old, base, coef, qty in triples:
                final = base
                olds = group_old.get(base, [])
                need_suffix = len(set(olds)) > 1  # <- clave: solo si distintos códigos originales
                if need_suffix:
                    seen_per_base[base] = seen_per_base.get(base, 0) + 1
                    idx = seen_per_base[base]
                    if idx > 1:
                        final = with_letter(base, idx)
                        clones_needed[final] = base
                new_chunks.extend([final, coef, qty])

            out.append(f"~D|{parent_code}|{'\\'.join(new_chunks)}|")
            continue

        # Resto de líneas: renombrado simple de códigos mapeados
        line = raw
        for old, new in code_map.items():
            line = re.sub(rf"{re.escape(old)}(?=[\\|])", new, line)
        out.append(line)

    # 4) Añadimos ~C clonados a partir del concepto base
    for clone_code, base_code in clones_needed.items():
        base_parts = concept_map.get(base_code)
        if not base_parts:
            continue
        parts = base_parts.copy()
        parts[0] = clone_code
        out.append(f"~C|{'|'.join(parts)}|")

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