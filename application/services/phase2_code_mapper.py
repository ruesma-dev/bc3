# application/services/phase2_code_mapper.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
import csv
import os
import re
from collections import defaultdict, deque

from application.services.budget_bc3_batch_service import (
    BudgetBc3BatchRequest,
    BudgetBc3BatchService,
)
from domain.bc3.records import (
    ConceptRecord,
    DescomposicionRecord,
    MedicionesRecord,
)
from infrastructure.clients.bc3_classifier_subprocess_client import (
    Bc3ClassifierSubprocessClient,
)
from infrastructure.filesystem.bc_refcru_package_writer import (
    RefCruRow,
    make_refcru_row,
    write_refcru_config_package_xlsx,
)
from utils.text_sanitize import clean_text

MAX_CODE_LEN = 20
NUM_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")
_PIPE_TAIL_RE = re.compile(r"\|+\s*$")


@dataclass
class Concept:
    code: str
    unidad: str
    desc_short: str
    price_txt: str
    tipo: str
    long_desc: str | None = None


def _final_trim_trailing_pipes(file_path: Path) -> None:
    tmp = file_path.with_suffix(file_path.suffix + ".tmp_clean")
    pat = re.compile(r"\|+\s*$")

    with file_path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:
        for raw in fin:
            if raw.startswith("~"):
                s = raw.rstrip("\n")
                s = pat.sub("|", s)
                fout.write(s + "\n")
            else:
                if not raw.endswith("\n"):
                    raw = raw + "\n"
                fout.write(raw)

    tmp.replace(file_path)


def _fix_d_trailing_backslashes(file_path: Path) -> None:
    tmp = file_path.with_suffix(file_path.suffix + ".tmp_dfix")

    with file_path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:
        for raw in fin:
            if raw.startswith("~D|"):
                s = raw.rstrip("\n")
                if s.endswith("|"):
                    core = s[:-1]
                    core = core.rstrip("\\") + "\\"
                    s = core + "|"
                fout.write(s + "\n")
            else:
                if not raw.endswith("\n"):
                    raw = raw + "\n"
                fout.write(raw)

    tmp.replace(file_path)


def _cleanup_trailing_pipes_file(path: Path) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    with path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:
        for line in fin:
            if line.rstrip("\n").endswith("|"):
                clean = _PIPE_TAIL_RE.sub("|", line.rstrip("\n")) + "\n"
                fout.write(clean)
            else:
                fout.write(line)
    path.unlink()
    tmp.rename(path)


def _collect_bc3_info(
    path: Path,
) -> Tuple[
    Dict[str, Concept],
    Dict[str, List[str]],
    Dict[str, List[str]],
]:
    concepts: Dict[str, Concept] = {}
    parents_of_set: Dict[str, set[str]] = {}
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
                if code in concepts and concepts[code].long_desc is None:
                    concepts[code].long_desc = txt

            elif raw.startswith("~D|"):
                _, rest = raw.split("|", 1)
                parent, child_part = rest.split("|", 1)
                chunks = child_part.rstrip("|\n").split("\\")

                children: List[str] = []
                for i in range(0, len(chunks), 3):
                    ch = (chunks[i] if i < len(chunks) else "").strip()
                    if not ch:
                        continue
                    parents_of_set.setdefault(ch, set()).add(parent)
                    children.append(ch)

                if children:
                    children_of.setdefault(parent, [])
                    children_of[parent].extend(children)

    parents_of: Dict[str, List[str]] = {
        ch: sorted(list(parents))
        for ch, parents in parents_of_set.items()
    }
    return concepts, parents_of, children_of


def _closest_partidas_for(
    code: str,
    *,
    concepts: Dict[str, Concept],
    parents_of: Dict[str, List[str]],
) -> set[str]:
    direct = parents_of.get(code, []) or []
    if not direct:
        return {"__ROOT__"}

    partidas: set[str] = set()
    q = deque(direct)
    seen: set[str] = set()

    while q:
        cur = q.popleft()
        if cur in seen:
            continue
        seen.add(cur)

        ccur = concepts.get(cur)
        if ccur and (ccur.tipo or "").strip() == "0":
            partidas.add(cur)
            continue

        for pp in parents_of.get(cur, []) or []:
            if pp not in seen:
                q.append(pp)

    return partidas or {"__ROOT__"}


def _nearest_ancestor_desc(
    start_code: str,
    *,
    concepts: Dict[str, Concept],
    parents_of: Dict[str, List[str]],
    predicate,
) -> Optional[str]:
    q = deque([start_code])
    seen: set[str] = set()

    while q:
        cur = q.popleft()
        for parent in (parents_of.get(cur, []) or []):
            if parent in seen:
                continue
            seen.add(parent)
            if predicate(parent):
                concept = concepts.get(parent)
                if concept:
                    txt = clean_text(concept.desc_short or "") or clean_text(concept.long_desc or "")
                    return txt or None
            q.append(parent)

    return None


def _partida_desc_for(
    old_code: str,
    *,
    concepts: Dict[str, Concept],
    parents_of: Dict[str, List[str]],
) -> Optional[str]:
    pks = sorted(list(_closest_partidas_for(old_code, concepts=concepts, parents_of=parents_of)))
    if not pks or pks == ["__ROOT__"]:
        return None

    descs: List[str] = []
    for pk in pks[:5]:
        concept = concepts.get(pk)
        if not concept:
            continue
        desc = clean_text(concept.desc_short or "") or clean_text(concept.long_desc or "")
        if desc:
            descs.append(desc)

    return " | ".join(descs) if descs else None


def _capitulo_desc_for(
    old_code: str,
    *,
    concepts: Dict[str, Concept],
    parents_of: Dict[str, List[str]],
) -> Optional[str]:
    pks = sorted(list(_closest_partidas_for(old_code, concepts=concepts, parents_of=parents_of)))
    start = pks[0] if pks and pks[0] != "__ROOT__" else old_code

    cap = _nearest_ancestor_desc(
        start,
        concepts=concepts,
        parents_of=parents_of,
        predicate=lambda x: ("#" in x) and ("##" not in x) and (x != "CD#"),
    )
    sub = _nearest_ancestor_desc(
        start,
        concepts=concepts,
        parents_of=parents_of,
        predicate=lambda x: ("##" in x) and (x != "CD#"),
    )
    return cap or sub


def _subcapitulo_desc_for(
    old_code: str,
    *,
    concepts: Dict[str, Concept],
    parents_of: Dict[str, List[str]],
) -> Optional[str]:
    pks = sorted(list(_closest_partidas_for(old_code, concepts=concepts, parents_of=parents_of)))
    start = pks[0] if pks and pks[0] != "__ROOT__" else old_code

    return _nearest_ancestor_desc(
        start,
        concepts=concepts,
        parents_of=parents_of,
        predicate=lambda x: ("##" in x) and (x != "CD#"),
    )


def _extract_best_code_from_result(item: Dict[str, Any]) -> Tuple[str, float]:
    code = str(item.get("codigo_interno") or "").strip()

    raw_conf = (
        item.get("confidence")
        or item.get("confianza")
        or item.get("score")
        or item.get("confianza_pct")
        or item.get("confidence_pct")
        or 0.0
    )

    try:
        conf = float(raw_conf)
    except Exception:
        conf = 0.0

    if conf > 1.0:
        conf = max(0.0, min(1.0, conf / 100.0))
    else:
        conf = max(0.0, min(1.0, conf))

    return code, conf


def _make_code(base: str, k: int) -> str:
    if k <= 0:
        return base[:MAX_CODE_LEN]
    suffix = _letters_suffix(k)
    return _safe_with_suffix(base, suffix)[:MAX_CODE_LEN]


def _safe_with_suffix(base: str, suffix: str) -> str:
    base = base[: max(0, MAX_CODE_LEN - len(suffix))]
    return (base + suffix)[:MAX_CODE_LEN]


def _letters_suffix(n: int) -> str:
    s = ""
    while n > 0:
        n -= 1
        s = chr(97 + (n % 26)) + s
        n //= 26
    return s


def _build_replacement_map(
    bc3_path: Path,
    *,
    progress_cb: Optional[Any] = None,
) -> Tuple[Dict[str, str], List[Tuple[str, str, float, str]]]:
    concepts, parents_of, _children = _collect_bc3_info(bc3_path)

    targets: List[str] = []
    for code, concept in concepts.items():
        if "#" in code:
            continue
        if concept.tipo not in {"1", "2", "3"}:
            continue
        targets.append(code)

    partidas_by_old: Dict[str, set[str]] = {}
    for oldc in targets:
        partidas_by_old[oldc] = _closest_partidas_for(
            oldc,
            concepts=concepts,
            parents_of=parents_of,
        )

    base_choice: Dict[str, str] = {}
    conf_choice: Dict[str, float] = {}
    method_choice: Dict[str, str] = {}

    batch_items: List[Dict[str, Any]] = []
    special_discount_targets: List[str] = []

    for oldc in targets:
        concept = concepts.get(oldc)
        if not concept:
            base_choice[oldc] = "SIN_CODIGO"
            conf_choice[oldc] = 0.0
            method_choice[oldc] = "missing_concept"
            continue

        if "%" in oldc:
            special_discount_targets.append(oldc)
            base_choice[oldc] = "% DESCUENTO"
            conf_choice[oldc] = 1.0
            method_choice[oldc] = "rule"
            if progress_cb:
                progress_cb({"old_code": oldc, "new_code": "% DESCUENTO", "confidence": 1.0})
            continue

        desc_short = clean_text(concept.desc_short or "")
        desc_long = clean_text(concept.long_desc or "")
        descripcion = desc_short
        if desc_long:
            descripcion = (descripcion + " | " + desc_long).strip(" |")

        batch_items.append(
            {
                "id": oldc,
                "codigo_bc3": oldc,
                "descripcion": descripcion or desc_short or oldc,
                "capitulo": _capitulo_desc_for(
                    oldc,
                    concepts=concepts,
                    parents_of=parents_of,
                ),
                "subcapitulo": _subcapitulo_desc_for(
                    oldc,
                    concepts=concepts,
                    parents_of=parents_of,
                ),
                "partida": _partida_desc_for(
                    oldc,
                    concepts=concepts,
                    parents_of=parents_of,
                ),
                "unidad": (concept.unidad or "").strip() or None,
            }
        )

    if batch_items:
        batch_service = BudgetBc3BatchService(
            bc3_client=Bc3ClassifierSubprocessClient.from_env(),
        )
        prompt_key = (os.getenv("BC3_CLASSIFY_PROMPT_KEY") or "bc3_clasificador_es").strip()

        def _on_batch_progress(
            batch_index: int,
            total_batches: int,
            request_items: List[Dict[str, Any]],
            batch_results: List[Dict[str, Any]],
        ) -> None:
            results_by_id = {
                str(item.get("id") or "").strip(): item
                for item in batch_results
                if isinstance(item, dict) and str(item.get("id") or "").strip()
            }

            for request_item in request_items:
                oldc = str(request_item.get("id") or "").strip()
                result_item = results_by_id.get(oldc)
                if result_item is None:
                    raise RuntimeError(
                        f"El servicio BC3 no devolvió resultado para id={oldc}"
                    )

                best_code, conf01 = _extract_best_code_from_result(result_item)
                if not best_code:
                    raise RuntimeError(
                        f"El servicio BC3 devolvió codigo_interno vacío para id={oldc}"
                    )

                base_choice[oldc] = best_code
                conf_choice[oldc] = conf01
                method_choice[oldc] = "ocr_service_batch"

                if progress_cb:
                    progress_cb(
                        {
                            "old_code": oldc,
                            "new_code": best_code,
                            "confidence": conf01,
                        }
                    )

        batch_service.classify_budget(
            BudgetBc3BatchRequest(
                prompt_key=prompt_key,
                bc3_id=bc3_path.stem,
                descompuestos=batch_items,
            ),
            progress_callback=_on_batch_progress,
        )

    repl: Dict[str, str] = {}
    discount_counter = 0
    for oldc in targets:
        if base_choice.get(oldc) == "% DESCUENTO":
            discount_counter += 1
            repl[oldc] = f"% DESCUENTO{discount_counter}"[:MAX_CODE_LEN]

    olds_by_base: Dict[str, List[str]] = defaultdict(list)
    for oldc in targets:
        base = (base_choice.get(oldc) or "").strip() or "SIN_CODIGO"
        if base == "% DESCUENTO":
            continue
        olds_by_base[base].append(oldc)

    suffix_idx: Dict[str, int] = {
        oldc: 0
        for oldc in targets
        if base_choice.get(oldc) != "% DESCUENTO"
    }

    for base, olds in olds_by_base.items():
        adjacency: Dict[str, set[str]] = {oldc: set() for oldc in olds}

        by_partida: Dict[str, List[str]] = defaultdict(list)
        for oldc in olds:
            for partida in partidas_by_old.get(oldc, {"__ROOT__"}):
                by_partida[partida].append(oldc)

        for _partida, codes in by_partida.items():
            if len(codes) <= 1:
                continue
            for i in range(len(codes)):
                for j in range(i + 1, len(codes)):
                    a = codes[i]
                    b = codes[j]
                    adjacency[a].add(b)
                    adjacency[b].add(a)

        order = sorted(olds, key=lambda oldc: (len(adjacency[oldc]), oldc), reverse=True)
        colors: Dict[str, int] = {}

        for oldc in order:
            used = {colors[n] for n in adjacency[oldc] if n in colors}
            color = 0
            while color in used:
                color += 1
            colors[oldc] = color

        for oldc, color in colors.items():
            suffix_idx[oldc] = max(suffix_idx.get(oldc, 0), color)

    for oldc in targets:
        if oldc in repl:
            continue
        base = (base_choice.get(oldc) or "").strip() or "SIN_CODIGO"
        repl[oldc] = _make_code(base, suffix_idx.get(oldc, 0))

    rows: List[Tuple[str, str, float, str]] = []
    for oldc in targets:
        rows.append(
            (
                oldc,
                repl[oldc],
                float(conf_choice.get(oldc, 0.0)),
                str(method_choice.get(oldc, "")),
            )
        )

    return repl, rows


def rewrite_bc3_with_codes(src: Path, dst: Path, repl_map: Dict[str, str]) -> None:
    if not repl_map:
        dst.write_text(src.read_text("latin-1", errors="ignore"), "latin-1", errors="ignore")
        return

    with src.open("r", encoding="latin-1", errors="ignore") as fin, \
         dst.open("w", encoding="latin-1", errors="ignore") as fout:
        for raw in fin:
            if raw.startswith("~C|"):
                try:
                    rec = ConceptRecord.parse(raw)
                    rec.map_code(repl_map)
                    fout.write(rec.to_line())
                except Exception:
                    fout.write(raw)

            elif raw.startswith("~D|"):
                try:
                    rec = DescomposicionRecord.parse(raw)
                    rec.map_child_codes(repl_map)
                    fout.write(rec.to_line())
                except Exception:
                    fout.write(raw)

            elif raw.startswith("~M|"):
                try:
                    rec = MedicionesRecord.parse(raw)
                    rec.map_child_codes(repl_map)
                    fout.write(rec.to_line())
                except Exception:
                    fout.write(raw)

            else:
                fout.write(raw)


def _write_mapping_csv(rows: List[Tuple[str, str, float, str]], csv_path: Path) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", newline="", encoding="utf-8") as fh:
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
    Fase 2: clasifica descompuestos contra el catálogo interno del servicio 2 y sustituye códigos.

    Nota:
    - catalog_xlsx se mantiene solo por compatibilidad retroactiva, pero ya no se usa.
    """
    if bc3_in is None:
        bc3_in = kwargs.pop("input_bc3", None)
    if bc3_out is None:
        bc3_out = kwargs.pop("output_bc3", None)

    if bc3_in is None:
        raise ValueError("run_phase2: falta 'bc3_in'.")

    bc3_in = Path(bc3_in)
    if bc3_out is None:
        bc3_out = bc3_in.with_name(bc3_in.stem + "_clasificado.bc3")
    else:
        bc3_out = Path(bc3_out)

    emit_refcru_xlsx = bool(kwargs.pop("emit_refcru_xlsx", True))
    refcru_template_xlsx = kwargs.pop("refcru_template_xlsx", None) or kwargs.pop("refcru_template", None)
    refcru_out = kwargs.pop("refcru_out", None)

    if refcru_template_xlsx is None:
        env_tpl = os.getenv("PHASE2_REFCRU_TEMPLATE_XLSX", "").strip()
        if env_tpl:
            refcru_template_xlsx = env_tpl

    if refcru_template_xlsx is not None:
        refcru_template_xlsx = Path(refcru_template_xlsx)

    if refcru_out is None:
        refcru_out = bc3_out.with_name(bc3_out.stem + "_REFCRU.xlsx")
    else:
        refcru_out = Path(refcru_out)

    progress_cb = None
    for key in ("progress_cb", "on_progress", "progress_callback", "callback", "logger"):
        if key in kwargs and kwargs[key] is not None:
            progress_cb = kwargs[key]
            break

    repl_map, rows = _build_replacement_map(
        bc3_in,
        progress_cb=progress_cb,
    )

    rewrite_bc3_with_codes(bc3_in, bc3_out, repl_map)
    _cleanup_trailing_pipes_file(bc3_out)

    try:
        _fix_d_trailing_backslashes(bc3_out)
    except Exception:
        pass

    map_csv = bc3_out.with_name(bc3_out.stem + "_map.csv")
    _write_mapping_csv(rows, map_csv)

    try:
        _final_trim_trailing_pipes(bc3_out)
    except Exception:
        pass

    if emit_refcru_xlsx:
        if refcru_template_xlsx is None or not refcru_template_xlsx.exists():
            if progress_cb:
                progress_cb("Aviso: no se encontró plantilla REFCRU. Selecciónala con 'Buscar...'")
        else:
            ref_rows: List[RefCruRow] = []
            for oldc, newc, _conf, _method in rows:
                ref_rows.append(make_refcru_row(item_no=newc, reference_no=oldc))

            write_refcru_config_package_xlsx(
                template_xlsx=refcru_template_xlsx,
                output_xlsx=refcru_out,
                rows=ref_rows,
            )

            if progress_cb:
                progress_cb(f"OK: generado REFCRU importable en BC: {refcru_out.name}")

    return bc3_out
