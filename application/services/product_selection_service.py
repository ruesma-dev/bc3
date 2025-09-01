# application/services/product_selection_service.py

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

from bc3_lib.domain.node import Node
from infrastructure.product_catalog.product_catalog import ProductCatalog
from infrastructure.llm.gemini_client import GeminiClient, GeminiSelection
from config.settings import (
    MAX_CODE_LEN, USE_DESTRUCTIVE_RENAME, USE_LOCAL_FALLBACK,
    GEMINI_BATCH_SIZE, PREFILTER_TOPK, GEMINI_MIN_CONFIDENCE,
)


@dataclass
class ProductMatch:
    node_code_old: str
    product_code: str
    confidence: float
    reason: str


def _iter_descomposed(nodes: Iterable[Node]) -> Iterable[Node]:
    def dfs(n: Node) -> Iterable[Node]:
        if n.kind.startswith("des_"):
            yield n
        for ch in n.children:
            yield from dfs(ch)
    for r in nodes:
        yield from dfs(r)


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


# ----------- utilidades de similitud (prefiltro + fallback local) -----------
def _strip_accents(s: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", s) if not unicodedata.combining(ch))

def _tokens(s: str) -> set[str]:
    import re as _re
    s = _strip_accents(s or "").lower()
    return set(_re.findall(r"[a-z0-9]+", s))

def _prefilter_candidates(query: str, catalog: ProductCatalog, k: int) -> List[dict]:
    qtok = _tokens(query)
    scored = []
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


# ------------------------ MODO INDIVIDUAL (existente) -----------------------
def build_product_code_mapping(
    *,
    roots: List[Node],
    catalog: ProductCatalog,
    gemini: GeminiClient,
    min_confidence: float = GEMINI_MIN_CONFIDENCE,
) -> Tuple[Dict[str, str], List[ProductMatch]]:
    used: set[str] = {n.code for n in _index_nodes(roots).values()}
    mapping: Dict[str, str] = {}
    matches: List[ProductMatch] = []

    products_text = "\n".join(f'- code: "{p.code}" | name: "{p.name}"' for p in catalog.products)

    cache: Dict[tuple[str, str], GeminiSelection] = {}
    for node in _iter_descomposed(roots):
        short_desc = node.description or ""
        long_desc = node.long_desc or ""
        sig = (short_desc.strip().lower(), long_desc.strip().lower())

        if sig not in cache:
            cache[sig] = gemini.select_product(
                short_desc=short_desc,
                long_desc=long_desc,
                products_prompt_list=products_text,
            )
        sel = cache[sig]

        pick_code, pick_conf, pick_reason = sel.product_code, sel.confidence, sel.reason
        if (not pick_code or pick_conf < min_confidence) and USE_LOCAL_FALLBACK:
            # fallback sencillo: mejor por nombre a todo catálogo
            candidates = _prefilter_candidates(long_desc or short_desc, catalog, k=1)
            if candidates:
                pick_code = candidates[0]["code"]
                pick_conf = min(pick_conf, 0.5)
                pick_reason = f"{pick_reason or ''}|fallback:jaccard".strip("|")

        if not pick_code:
            continue

        target = _normalize_bc3_code(pick_code)
        new_code = _assign_unique(target, used)
        if new_code == node.code:
            continue

        mapping[node.code] = new_code
        matches.append(ProductMatch(node.code, new_code, float(pick_conf), pick_reason))

    return mapping, matches


# ----------------------------- MODO BATCH NUEVO -----------------------------
def build_product_code_mapping_batch(
    *,
    roots: List[Node],
    catalog: ProductCatalog,
    gemini: GeminiClient,
    batch_size: int = GEMINI_BATCH_SIZE,
    topk: int = PREFILTER_TOPK,
    min_confidence: float = GEMINI_MIN_CONFIDENCE,
) -> Tuple[Dict[str, str], List[ProductMatch]]:
    used: set[str] = {n.code for n in _index_nodes(roots).values()}
    mapping: Dict[str, str] = {}
    matches: List[ProductMatch] = []

    # 1) Agrupar descompuestos por firma (dedupe)
    items: List[dict] = []
    sig_to_nodes: Dict[str, List[Node]] = {}
    for node in _iter_descomposed(roots):
        short_desc = (node.description or "").strip()
        long_desc = (node.long_desc or "").strip()
        sig = (short_desc.lower(), long_desc.lower())
        key = f"{short_desc}||{long_desc}"
        if key not in sig_to_nodes:
            sig_to_nodes[key] = []
            # Prefiltro Top-K candidatos
            q = long_desc or short_desc
            candidates = _prefilter_candidates(q, catalog, k=topk)
            # Si no hay candidatos por similitud, mete todo el catálogo (peligro TPM)
            if not candidates:
                candidates = [{"code": p.code, "name": p.name} for p in catalog.products]
            items.append({"id": key, "short": short_desc, "long": long_desc, "candidates": candidates})
        sig_to_nodes[key].append(node)

    # 2) Llamadas en lotes
    for i in range(0, len(items), max(1, batch_size)):
        chunk = items[i : i + batch_size]
        results = gemini.select_products_batch(items=chunk) or []
        result_map = {r.get("id"): r for r in results}

        # 3) Aplicar resultados a todos los nodos de la firma
        for it in chunk:
            rid = it["id"]
            r = result_map.get(rid, {})
            pick_code = str(r.get("product_code", "")).strip()
            pick_conf = float(r.get("confidence", 0.0))
            pick_reason = str(r.get("reason", "")).strip()

            # Fallback local si vacío o baja confianza
            if (not pick_code or pick_conf < min_confidence) and USE_LOCAL_FALLBACK:
                # Usa primer candidato del prefiltro como salvaguarda
                cand = (it.get("candidates") or [])
                if cand:
                    pick_code = cand[0]["code"]
                    pick_conf = min(pick_conf, 0.5)
                    pick_reason = f"{pick_reason or ''}|fallback:jaccard".strip("|")

            if not pick_code:
                continue

            target = _normalize_bc3_code(pick_code)
            new_code = _assign_unique(target, used)

            # Propaga a todos los nodos con esa firma
            for node in sig_to_nodes[rid]:
                if new_code != node.code:
                    mapping[node.code] = new_code
                    matches.append(ProductMatch(node.code, new_code, float(pick_conf), pick_reason))

    return mapping, matches


# ------------------- Reescritura del BC3 + actualización árbol --------------
def apply_code_mapping_to_nodes(roots: List[Node], code_map: Dict[str, str]) -> None:
    def dfs(n: Node) -> None:
        if n.code in code_map:
            n.code = code_map[n.code]
        for ch in n.children:
            dfs(ch)
    for r in roots:
        dfs(r)


def rewrite_bc3_with_product_codes(path: Path, code_map: Dict[str, str]) -> None:
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
                    continue
                fout.write(raw)
        tmp.replace(path)
        return

    # Renombrado integral (~C y referencias ~D)
    pattern = re.compile(
        r"(" + "|".join(re.escape(k) for k in sorted(code_map.keys(), key=len, reverse=True)) + r")(?=[\\|])"
    )
    tmp = path.with_suffix(path.suffix + ".tmp")
    with path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:
        for raw in fin:
            if raw.startswith("~C|"):
                head, rest = raw.split("|", 1)
                parts = rest.rstrip("\n").split("|")
                if parts and parts[0] in code_map:
                    parts[0] = code_map[parts[0]]
                    fout.write(f"{head}|{'|'.join(parts)}|\n")
                    continue
                fout.write(raw)
            else:
                changed = pattern.sub(lambda m: code_map[m.group(1)], raw.rstrip("\n")) + "\n"
                fout.write(changed)
    tmp.replace(path)
