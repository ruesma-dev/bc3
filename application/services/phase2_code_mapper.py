# application/services/phase2_code_mapper.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple, Any, Optional
import os
import re
import json
import csv
import sys
import subprocess
from collections import deque

from datetime import datetime, timezone
import uuid

import pandas as pd

from utils.text_sanitize import clean_text
from domain.bc3.records import (
    ConceptRecord,
    DescomposicionRecord,
    MedicionesRecord,
)

from infrastructure.filesystem.bc_refcru_package_writer import (
    RefCruRow,
    write_refcru_config_package_xlsx,
    make_refcru_row,
)

MAX_CODE_LEN = 20
NUM_RE = re.compile(r"^-?\d+(?:[.,]\d+)?$")

_PIPE_TAIL_RE = re.compile(r"\|+\s*$")


def _final_trim_trailing_pipes(file_path: Path) -> None:
    """
    Limpia el fichero resultante colapsando '|||' finales en un único '|'
    SOLO al final de cada línea. No toca los '|' internos.
    """
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
    """
    Normaliza las líneas ~D para que terminen en '\\|':
      - si no hay barra antes del último '|', añade UNA '\'
      - si hay varias barras, las reduce a UNA sola
    """
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
    """
    Reescribe el archivo asegurando que las líneas BC3 terminen con
    una única tubería '|' (sin acumular '|||' al final).
    """
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


def _ensure_ud_for_concepts(file_path: Path) -> None:
    """
    Después de la asignación por IA, asegura unidad 'ud' cuando falte en:
      - Descompuestos (tipo '3') y
      - Partidas (tipo '0')
    """
    tmp = file_path.with_suffix(file_path.suffix + ".tmp_units")
    with file_path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:

        for raw in fin:
            if not raw.startswith("~C|"):
                fout.write(raw)
                continue

            head, rest = raw.split("|", 1)
            parts = rest.rstrip("\n")
            fields = parts.split("|")
            while len(fields) < 6:
                fields.append("")

            code = fields[0]
            unidad = fields[1]
            tipo = fields[5]

            if tipo.strip() in {"0", "3"} and "#" not in code and (unidad.strip() == ""):
                fields[1] = "ud"

            line = f"{head}|{'|'.join(fields)}|\n"
            fout.write(line)

    tmp.replace(file_path)


def _normalize_unit_key(u: str) -> str:
    if not u:
        return ""
    s = u.upper().replace(" ", "").replace(".", "")
    s = s.replace("²", "2").replace("³", "3")
    s = s.replace("^2", "2").replace("^3", "3")
    s = s.replace("·", "")
    return s


# ---------------------------- OCR_SERVICE helpers ----------------------------

def _module_rel_candidates(module: str) -> list[Path]:
    parts = [p for p in (module or "").split(".") if p]
    mp = Path(*parts)
    return [
        mp.with_suffix(".py"),
        mp / "__init__.py",
        mp / "__main__.py",
    ]


def _root_has_module(root: Path, module: str) -> bool:
    if not root:
        return False
    for base in (root, root / "src"):
        for rel in _module_rel_candidates(module):
            try:
                if (base / rel).exists():
                    return True
            except Exception:
                continue
    return False


def _guess_repo_root_from_file() -> Optional[Path]:
    """
    Intenta inferir el root del repo actual (bc3) subiendo hasta encontrar 'application/'.
    """
    try:
        here = Path(__file__).resolve()
        for p in [here.parent] + list(here.parents):
            if (p / "application").exists():
                return p
    except Exception:
        pass
    return None


def _resolve_ocr_service_root(module: Optional[str] = None) -> Optional[Path]:
    """
    Carpeta raíz del repo ocr_service para ejecutar el módulo -m con imports correctos.
    """
    module = module or (os.getenv("OCR_SERVICE_MODULE") or "interface_adapters.cli.bc3_classify_stdin").strip()

    env_p = (os.getenv("OCR_SERVICE_ROOT") or os.getenv("OCR_SERVICE_WORKDIR") or "").strip()
    if env_p:
        pp = Path(os.path.expandvars(os.path.expanduser(env_p.strip('"').strip("'"))))
        if pp.exists():
            if _root_has_module(pp, module):
                return pp
            return pp

    bc3_root = _guess_repo_root_from_file()
    if bc3_root:
        cand = bc3_root / "ocr_service"
        if cand.exists() and _root_has_module(cand, module):
            return cand

        sib = bc3_root.parent / "ocr_service"
        if sib.exists() and _root_has_module(sib, module):
            return sib

        try:
            parent = bc3_root.parent
            count = 0
            for d in parent.iterdir():
                if not d.is_dir():
                    continue
                count += 1
                if count > 60:
                    break

                if _root_has_module(d, module):
                    return d
                dd = d / "ocr_service"
                if dd.exists() and _root_has_module(dd, module):
                    return dd
        except Exception:
            pass

    return None


def _resolve_ocr_service_python(ocr_root: Optional[Path] = None) -> str:
    """
    Devuelve el python.exe que ejecutará el ocr_service.

    Orden:
      1) ENV OCR_SERVICE_PYTHON / OCR_SERVICE_PY
      2) Si hay ocr_root, intentar <ocr_root>/.venv/(Scripts|bin)/python
      3) fallback: sys.executable
    """
    p = (os.getenv("OCR_SERVICE_PYTHON") or os.getenv("OCR_SERVICE_PY") or "").strip()
    if p:
        return p.strip('"').strip("'")

    if ocr_root:
        candidates = [
            ocr_root / ".venv" / "Scripts" / "python.exe",
            ocr_root / ".venv" / "bin" / "python",
            ocr_root / "venv" / "Scripts" / "python.exe",
            ocr_root / "venv" / "bin" / "python",
        ]
        for c in candidates:
            try:
                if c.exists():
                    return str(c)
            except Exception:
                continue

    return sys.executable


def _debug_enabled() -> bool:
    """
    DEBUG ON por defecto (para tu fase de depuración).
    Para desactivar:
      PHASE2_DUMP_OCR_IO=0
    """
    v = os.getenv("PHASE2_DUMP_OCR_IO")
    if v is None or v.strip() == "":
        return True
    return v.strip().lower() in {"1", "true", "yes", "y", "on"}


def _debug_mode() -> str:
    return (os.getenv("PHASE2_DUMP_OCR_MODE", "all").strip().lower() or "all")


def _utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S.%fZ")


def _safe_slug(s: str, max_len: int = 120) -> str:
    s = (s or "").strip()
    s = re.sub(r"[^A-Za-z0-9_.-]+", "_", s)
    s = s.strip("._-")
    return (s[:max_len] if s else "item")


def _write_json(path: Path, obj: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as e:
        print(f"[PHASE2 WARN] No pude escribir JSON en {path}: {e}", file=sys.stderr)
        try:
            print(json.dumps(obj, ensure_ascii=False, indent=2), file=sys.stderr)
        except Exception:
            pass


def _write_text(path: Path, txt: str) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(txt or "", encoding="utf-8", errors="ignore")
    except Exception as e:
        print(f"[PHASE2 WARN] No pude escribir TXT en {path}: {e}", file=sys.stderr)
        try:
            print(txt or "", file=sys.stderr)
        except Exception:
            pass


def _resolve_phase2_dump_dir(bc3_path: Path) -> Optional[Path]:
    """
    Reglas pedidas:
      - Primero intenta PHASE2_DUMP_OCR_DIR si existe
      - Si no: carpeta output del proyecto bc3
      - Si no: output junto al bc3
      - Si no: ruta del bc3
      - Si no puede escribir: None (y entonces volcamos a consola)
    """
    if not _debug_enabled():
        return None

    base_env = (os.getenv("PHASE2_DUMP_OCR_DIR", "") or "").strip()
    candidates: list[Path] = []

    if base_env:
        candidates.append(Path(os.path.expandvars(os.path.expanduser(base_env.strip('"').strip("'")))))

    bc3_root = _guess_repo_root_from_file()
    if bc3_root:
        candidates.append(bc3_root / "output")

    candidates.append(bc3_path.parent / "output")
    candidates.append(bc3_path.parent)

    run_tag = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    for base_dir in candidates:
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
            out = base_dir / "phase2_ocr_io" / f"{bc3_path.stem}_{run_tag}"
            out.mkdir(parents=True, exist_ok=True)
            return out
        except Exception:
            continue

    return None


def _parse_json_from_mixed_output(stdout_text: str, stderr_text: str = "") -> Any:
    s = (stdout_text or "").lstrip("\ufeff").strip()
    if not s:
        raise ValueError(f"stdout vacío (stderr: {stderr_text[:500]})")

    try:
        return json.loads(s)
    except Exception:
        pass

    dec = json.JSONDecoder()
    for i, ch in enumerate(s):
        if ch not in "{[":
            continue
        try:
            obj, _end = dec.raw_decode(s[i:])
            return obj
        except Exception:
            continue

    raise ValueError(
        "No se pudo extraer JSON de stdout. "
        f"STDOUT(head): {s[:500]}\nSTDERR(head): {(stderr_text or '')[:500]}"
    )


def _run_ocr_service_bc3_classify(
    payload: Dict[str, Any],
    *,
    dump_dir: Optional[Path] = None,
    dump_mode: str = "all",
) -> Dict[str, Any]:
    """
    Llama al CLI del ocr_service por subprocess enviando payload por stdin.
    Devuelve el envelope dict parseado.
    """
    module = (os.getenv("OCR_SERVICE_MODULE") or "interface_adapters.cli.bc3_classify_stdin").strip()
    cwd = _resolve_ocr_service_root(module)
    py = _resolve_ocr_service_python(cwd)
    timeout_s = int((os.getenv("OCR_SERVICE_TIMEOUT_S") or "240").strip())

    cmd = [py, "-m", module]

    item_id = ""
    try:
        ds0 = (payload.get("descompuestos") or [])[0] or {}
        item_id = str(ds0.get("id") or ds0.get("codigo_bc3") or "")
    except Exception:
        item_id = ""

    trace_id = f"{_utc_stamp()}_{_safe_slug(item_id) if item_id else 'call'}_{uuid.uuid4().hex[:8]}"

    # Si no hay dump_dir y debug está ON: avisamos (y luego volcamos a consola en caso extremo)
    if _debug_enabled() and dump_dir is None:
        print("[PHASE2 DEBUG] Dump ON pero no pude crear carpeta de dump. Fallback: consola.", file=sys.stderr)

    if dump_dir and dump_mode in {"all"}:
        _write_json(dump_dir / f"{trace_id}__request.json", payload)
    elif _debug_enabled() and dump_dir is None and dump_mode in {"all"}:
        print(f"[PHASE2 DEBUG] REQUEST trace_id={trace_id}\n{json.dumps(payload, ensure_ascii=False, indent=2)}", file=sys.stderr)

    # Propagar flags de debug al ocr_service
    env = os.environ.copy()
    if _debug_enabled():
        env.setdefault("PHASE2_DUMP_OCR_IO", "1")
        env.setdefault("PHASE2_DUMP_OCR_MODE", dump_mode or "all")
        # pasar base dir (no el subdir del trace) para que ocr_service cree su subcarpeta
        if dump_dir:
            env.setdefault("PHASE2_DUMP_OCR_DIR", str(dump_dir.parents[1] if len(dump_dir.parents) >= 2 else dump_dir.parent))

    proc = subprocess.run(
        cmd,
        input=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        timeout=timeout_s,
        env=env,
    )

    stdout = proc.stdout.decode("utf-8", errors="replace")
    stderr = proc.stderr.decode("utf-8", errors="replace")

    if proc.returncode != 0:
        if dump_dir and dump_mode in {"all", "errors"}:
            _write_text(dump_dir / f"{trace_id}__stderr.txt", stderr)
            _write_text(dump_dir / f"{trace_id}__stdout.txt", stdout)
            _write_json(
                dump_dir / f"{trace_id}__error.json",
                {
                    "trace_id": trace_id,
                    "cmd": cmd,
                    "cwd": str(cwd) if cwd else None,
                    "returncode": proc.returncode,
                    "request": payload,
                    "stderr": stderr,
                    "stdout": stdout,
                },
            )
        else:
            print(f"[PHASE2 DEBUG] STDERR:\n{stderr}\nSTDOUT:\n{stdout}", file=sys.stderr)

        raise RuntimeError(
            "Fallo llamando a ocr_service.\n"
            f"CMD: {' '.join(cmd)}\n"
            f"CWD: {cwd}\n"
            f"RC: {proc.returncode}\n"
            f"STDERR:\n{stderr}\n"
            f"STDOUT:\n{stdout}\n"
            "Sugerencia: comprueba OCR_SERVICE_ROOT/OCR_SERVICE_PYTHON o que el módulo exista.\n"
        )

    envelope = _parse_json_from_mixed_output(stdout, stderr)
    if not isinstance(envelope, dict):
        if dump_dir and dump_mode in {"all", "errors"}:
            _write_text(dump_dir / f"{trace_id}__stderr.txt", stderr)
            _write_text(dump_dir / f"{trace_id}__stdout.txt", stdout)
            _write_json(
                dump_dir / f"{trace_id}__bad_response.json",
                {
                    "trace_id": trace_id,
                    "cmd": cmd,
                    "cwd": str(cwd) if cwd else None,
                    "returncode": proc.returncode,
                    "request": payload,
                    "stderr": stderr,
                    "stdout": stdout,
                    "parsed_type": str(type(envelope)),
                },
            )
        raise ValueError(f"Respuesta no dict. type={type(envelope)} head={str(envelope)[:200]}")

    if dump_dir and dump_mode in {"all"}:
        _write_text(dump_dir / f"{trace_id}__stderr.txt", stderr)
        _write_json(dump_dir / f"{trace_id}__response.json", envelope)
    elif _debug_enabled() and dump_dir is None and dump_mode in {"all"}:
        print(f"[PHASE2 DEBUG] RESPONSE trace_id={trace_id}\n{json.dumps(envelope, ensure_ascii=False, indent=2)}", file=sys.stderr)

    return envelope


def _extract_best_code_from_envelope(envelope: Dict[str, Any]) -> tuple[str, float]:
    """
    Extrae (best_code, confidence) del envelope devuelto por ocr_service.

    IMPORTANTE:
      - ocr_service devuelve 'codigo_interno' (según tu prompt actual)
      - confianza puede venir como 0..1 o como porcentaje 0..100 ('confianza_pct')
    """
    def _coerce_conf(x: Any) -> float:
        if x is None:
            return 0.0
        try:
            f = float(x)
        except Exception:
            return 0.0
        if f < 0:
            return 0.0
        # si viene 0..100
        if f > 1.0:
            if f <= 100.0:
                return f / 100.0
            return min(1.0, f / 100.0)
        return f

    data = envelope.get("data", envelope)

    # Si viene como lista directa
    if isinstance(data, list) and data:
        first = data[0]
        if isinstance(first, dict):
            code = (
                first.get("codigo_interno")
                or first.get("codigo_producto")
                or first.get("best_code")
                or first.get("code")
                or ""
            )
            conf = (
                first.get("confidence")
                or first.get("confianza")
                or first.get("score")
                or first.get("confianza_pct")
                or first.get("confidence_pct")
                or 0.0
            )
            return str(code).strip(), _coerce_conf(conf)

    if isinstance(data, dict):
        for key in ("clasificaciones", "resultados", "items", "outputs", "descompuestos"):
            v = data.get(key)
            if isinstance(v, list) and v:
                first = v[0]
                if isinstance(first, dict):
                    code = (
                        first.get("codigo_interno")
                        or first.get("codigo_producto")
                        or first.get("best_code")
                        or first.get("code")
                        or ""
                    )
                    conf = (
                        first.get("confidence")
                        or first.get("confianza")
                        or first.get("score")
                        or first.get("confianza_pct")
                        or first.get("confidence_pct")
                        or 0.0
                    )
                    return str(code).strip(), _coerce_conf(conf)

        code = (
            data.get("codigo_interno")
            or data.get("codigo_producto")
            or data.get("best_code")
            or data.get("code")
            or ""
        )
        if isinstance(code, str) and code.strip():
            conf = (
                data.get("confidence")
                or data.get("confianza")
                or data.get("score")
                or data.get("confianza_pct")
                or data.get("confidence_pct")
                or 0.0
            )
            return code.strip(), _coerce_conf(conf)

    raise ValueError(f"No pude extraer best_code de la respuesta. keys={list(envelope.keys())}")


# --------------------------------------------------------------------------- #
# Unificación de unidades (igual que lo tenías)
# --------------------------------------------------------------------------- #

_UNIT_VARIANTS: dict[str, set[str]] = {
    "%": {"%"},
    "CM": {"CM", "CENTIMETRO", "CENTÍMETRO"},
    "H": {"H", "HORA", "H."},
    "HR": {"HR"},
    "KG": {"KG", "K.G", "KGS", "KG.", "KILOGRAMO", "KILOS"},
    "L": {"L", "L.", "LITRO", "LITROS"},
    "M": {"M", "M.", "METRO", "METROS", "M·"},
    "ML": {
        "ML", "M.L", "M L", "M-L", "METROLINEAL", "M LINEAL", "METROS LINEALES",
        "ML.", "M. L."
    },
    "MI": {"MI", "M.I", "M I", "M. I."},
    "M2": {
        "M2", "M2.", "M.2", "M 2", "M^2", "M²", "METROCUADRADO", "METROS CUADRADOS",
        "M.CUADRADO", "M. CUADRADO"
    },
    "M3": {
        "M3", "M3.", "M.3", "M 3", "M^3", "M³", "METROCUBICO", "METROS CUBICOS",
        "M.CUBICO", "M. CUBICO"
    },
    "MES": {"MES", "MESES"},
    "PA": {"PA", "PARTIDA ALZADA", "P.A", "P. A."},
    "PP": {"PP", "P.P", "PARTEPROPORCIONAL", "PARTE PROPORCIONAL"},
    "T": {"T", "TN", "TON", "TONELADA", "TONELADAS"},
    "UD": {"UD", "UD.", "U", "U.", "UNIDAD", "UNIDADES", "UNID", "UNID."},
    "VIV": {"VIV", "VIVIENDA", "VIVIENDAS"},
    "PLANTAS": {"PLANTAS", "PLANTA", "PLANT", "PLANT."},
    "LEGRAND": {"LEGRAND"},
}

_UNIT_LOOKUP: dict[str, str] = {}
for canon, vars_ in _UNIT_VARIANTS.items():
    for v in vars_:
        _UNIT_LOOKUP[_normalize_unit_key(v)] = canon
for canon in _UNIT_VARIANTS.keys():
    _UNIT_LOOKUP[_normalize_unit_key(canon)] = canon


def _unify_units_in_file(file_path: Path) -> None:
    tmp = file_path.with_suffix(file_path.suffix + ".tmp_units_unify")

    with file_path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:

        for raw in fin:
            if not raw.startswith("~C|"):
                fout.write(raw)
                continue

            head, rest = raw.split("|", 1)
            parts = rest.rstrip("\n")
            fields = parts.split("|")
            while len(fields) < 6:
                fields.append("")

            code = fields[0]
            unidad = fields[1]
            tipo = fields[5]

            if tipo.strip() in {"0", "3"} and "#" not in code and unidad.strip():
                key = _normalize_unit_key(unidad)
                canon = _UNIT_LOOKUP.get(key)
                if canon:
                    fields[1] = canon

            line = f"{head}|{'|'.join(fields)}|\n"
            fout.write(line)

    tmp.replace(file_path)


def _ensure_ud_for_materials(file_path: Path) -> None:
    tmp = file_path.with_suffix(file_path.suffix + ".tmp_units")
    with file_path.open("r", encoding="latin-1", errors="ignore") as fin, \
         tmp.open("w", encoding="latin-1", errors="ignore") as fout:

        for raw in fin:
            if not raw.startswith("~C|"):
                fout.write(raw)
                continue

            head, rest = raw.split("|", 1)
            parts = rest.rstrip("\n")
            fields = parts.split("|")
            while len(fields) < 6:
                fields.append("")

            code = fields[0]
            unidad = fields[1]
            tipo = fields[5]

            if tipo.strip() == "3" and "#" not in code and (unidad.strip() == ""):
                fields[1] = "ud"

            line = f"{head}|{'|'.join(fields)}|\n"
            fout.write(line)

    tmp.replace(file_path)


# --------------------------------------------------------------------------- #
# Modelos internos BC3
# --------------------------------------------------------------------------- #

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


def _normalize_space_lower(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip().lower()


def _tokenize(text: str) -> List[str]:
    text = clean_text(text).lower()
    return re.findall(r"[a-z0-9]+", text)


def _score_token_overlap(a: str, b: str) -> int:
    ta = set(_tokenize(a))
    tb = set(_tokenize(b))
    return len(ta & tb)


def _collect_bc3_info(
    path: Path,
) -> Tuple[
    Dict[str, Concept],
    Dict[str, str],
    Dict[str, List[str]],
    Dict[str, List[str]],
]:
    concepts: Dict[str, Concept] = {}
    long_map: Dict[str, str] = {}

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
                if code not in long_map:
                    long_map[code] = txt

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

    for c in concepts.values():
        if c.code in long_map:
            c.long_desc = long_map[c.code]

    parents_of: Dict[str, List[str]] = {ch: sorted(list(ps)) for ch, ps in parents_of_set.items()}
    return concepts, long_map, parents_of, children_of


def _build_replacement_map(
    bc3_path: Path,
    catalog_path: Path,
    topk: int = 20,
    min_conf: float = 0.0,
    use_heuristics: Optional[bool] = None,
    fewshots: Optional[List[Dict[str, Any]]] = None,
    hints_tree_xlsx: Optional[Path] = None,
    progress_cb: Optional[Any] = None,
) -> Tuple[Dict[str, str], List[Tuple[str, str, float, str]]]:
    """
    Calcula old_code -> new_code SOLO para descompuestos (T=3 o T original 1/2/3).
    """
    from collections import defaultdict, deque

    io_dump_dir = _resolve_phase2_dump_dir(bc3_path)
    io_dump_mode = _debug_mode()

    if _debug_enabled():
        if io_dump_dir:
            print(f"[PHASE2 DEBUG] Dump OCR IO en: {io_dump_dir}")
        else:
            print("[PHASE2 DEBUG] Dump ON pero sin carpeta. Se volcará a consola si hace falta.", file=sys.stderr)

    # dump info catálogo (para saber si se puede leer y qué sheets tiene)
    if io_dump_dir:
        try:
            info = {
                "catalog_path": str(catalog_path),
                "exists": bool(catalog_path.exists()),
                "size_bytes": catalog_path.stat().st_size if catalog_path.exists() else None,
                "mtime_utc": datetime.fromtimestamp(catalog_path.stat().st_mtime, tz=timezone.utc).isoformat()
                if catalog_path.exists() else None,
                "sheet_env": (os.getenv("BC3_CATALOG_SHEET") or "").strip() or None,
            }
            if catalog_path.exists():
                try:
                    xls = pd.ExcelFile(catalog_path)
                    info["sheet_names"] = list(xls.sheet_names)
                except Exception as e:
                    info["sheet_names_error"] = str(e)
            _write_json(io_dump_dir / "__catalog_info.json", info)
        except Exception:
            pass

    concepts, _longs, parents_of, _children = _collect_bc3_info(bc3_path)

    def _closest_partidas_for(code: str) -> set[str]:
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

    targets: List[str] = []
    for code, c in concepts.items():
        if "#" in code:
            continue
        if c.tipo not in {"1", "2", "3"}:
            continue
        targets.append(code)

    partidas_by_old: Dict[str, set[str]] = {}
    olds_by_partida: Dict[str, List[str]] = defaultdict(list)

    for oldc in targets:
        pks = _closest_partidas_for(oldc)
        partidas_by_old[oldc] = pks
        for pk in pks:
            olds_by_partida[pk].append(oldc)

    base_choice: Dict[str, str] = {}
    conf_choice: Dict[str, float] = {}
    method_choice: Dict[str, str] = {}

    prompt_key = (os.getenv("BC3_CLASSIFY_PROMPT_KEY") or "bc3_clasificador_es").strip()
    sheet = (os.getenv("BC3_CATALOG_SHEET") or "").strip() or None

    def _nearest_ancestor_desc(start_code: str, predicate) -> Optional[str]:
        q = deque([start_code])
        seen: set[str] = set()
        while q:
            cur = q.popleft()
            for p in (parents_of.get(cur, []) or []):
                if p in seen:
                    continue
                seen.add(p)
                if predicate(p):
                    c = concepts.get(p)
                    if c:
                        txt = clean_text(c.desc_short or "") or clean_text(c.long_desc or "")
                        return txt or None
                q.append(p)
        return None

    def _partida_desc_for(old_code: str) -> Optional[str]:
        pks = sorted(list(_closest_partidas_for(old_code)))
        if not pks or pks == ["__ROOT__"]:
            return None
        descs: List[str] = []
        for pk in pks[:5]:
            c = concepts.get(pk)
            if not c:
                continue
            d = clean_text(c.desc_short or "") or clean_text(c.long_desc or "")
            if d:
                descs.append(d)
        return " | ".join(descs) if descs else None

    def _capitulo_desc_for(old_code: str) -> Optional[str]:
        pks = sorted(list(_closest_partidas_for(old_code)))
        start = pks[0] if pks and pks[0] != "__ROOT__" else old_code

        cap = _nearest_ancestor_desc(
            start,
            lambda x: ("#" in x) and ("##" not in x) and (x != "CD#"),
        )
        sub = _nearest_ancestor_desc(
            start,
            lambda x: ("##" in x) and (x != "CD#"),
        )
        return cap or sub

    def _subcapitulo_desc_for(old_code: str) -> Optional[str]:
        pks = sorted(list(_closest_partidas_for(old_code)))
        start = pks[0] if pks and pks[0] != "__ROOT__" else old_code
        sub = _nearest_ancestor_desc(
            start,
            lambda x: ("##" in x) and (x != "CD#"),
        )
        return sub

    for oldc in targets:
        c = concepts.get(oldc)
        if not c:
            base_choice[oldc] = "SIN_CODIGO"
            conf_choice[oldc] = 0.0
            method_choice[oldc] = "missing_concept"
            continue

        if "%" in oldc:
            base_choice[oldc] = "% DESCUENTO"
            conf_choice[oldc] = 1.0
            method_choice[oldc] = "rule"
            if progress_cb:
                progress_cb({"old_code": oldc, "new_code": "% DESCUENTO", "confidence": 1.0})
            continue

        desc_short = clean_text(c.desc_short or "")
        desc_long = clean_text(c.long_desc or "")
        descripcion = desc_short
        if desc_long:
            descripcion = (descripcion + " | " + desc_long).strip(" |")

        partida_txt = _partida_desc_for(oldc)
        capitulo_txt = _capitulo_desc_for(oldc)
        subcap_txt = _subcapitulo_desc_for(oldc)

        request: Dict[str, Any] = {
            "prompt_key": prompt_key,
            "bc3_id": bc3_path.stem,
            "top_k_candidates": int(topk),
            "catalog_xlsx_path": str(catalog_path),
            "descompuestos": [
                {
                    "id": oldc,
                    "codigo_bc3": oldc,
                    "descripcion": descripcion or desc_short or oldc,
                    "capitulo": capitulo_txt,
                    "subcapitulo": subcap_txt,
                    "partida": partida_txt,
                    "unidad": (c.unidad or "").strip() or None,
                }
            ],
        }
        if sheet:
            request["catalog_sheet"] = sheet

        envelope = _run_ocr_service_bc3_classify(request, dump_dir=io_dump_dir, dump_mode=io_dump_mode)
        best_code, conf01 = _extract_best_code_from_envelope(envelope)

        best_code = (best_code or "").strip()
        if not best_code:
            raise RuntimeError(
                f"ocr_service devolvió best_code vacío para {oldc}. Envelope keys={list(envelope.keys())}"
            )

        base_choice[oldc] = best_code
        conf_choice[oldc] = float(conf01) if conf01 is not None else float(min_conf)
        method_choice[oldc] = "ocr_service"

        if progress_cb:
            progress_cb({"old_code": oldc, "new_code": best_code, "confidence": float(conf_choice[oldc])})

    def _make_code(base: str, k: int) -> str:
        if k <= 0:
            return base[:MAX_CODE_LEN]
        suf = _letters_suffix(k)
        return _safe_with_suffix(base, suf)[:MAX_CODE_LEN]

    repl: Dict[str, str] = {}
    discount_counter = 0
    for oldc in targets:
        if base_choice.get(oldc) == "% DESCUENTO":
            discount_counter += 1
            repl[oldc] = f"% DESCUENTO{discount_counter}"[:MAX_CODE_LEN]

    from collections import defaultdict

    olds_by_base: Dict[str, List[str]] = defaultdict(list)
    for oldc in targets:
        b = (base_choice.get(oldc) or "").strip() or "SIN_CODIGO"
        if b == "% DESCUENTO":
            continue
        olds_by_base[b].append(oldc)

    suffix_idx: Dict[str, int] = {oldc: 0 for oldc in targets if base_choice.get(oldc) != "% DESCUENTO"}

    for base, olds in olds_by_base.items():
        adj: Dict[str, set[str]] = {o: set() for o in olds}

        by_pk: Dict[str, List[str]] = defaultdict(list)
        for o in olds:
            for pk in partidas_by_old.get(o, {"__ROOT__"}):
                by_pk[pk].append(o)

        for pk, lst in by_pk.items():
            if len(lst) <= 1:
                continue
            for i in range(len(lst)):
                for j in range(i + 1, len(lst)):
                    a, b2 = lst[i], lst[j]
                    adj[a].add(b2)
                    adj[b2].add(a)

        order = sorted(olds, key=lambda o: (len(adj[o]), o), reverse=True)
        colors: Dict[str, int] = {}

        for o in order:
            used_colors = {colors[n] for n in adj[o] if n in colors}
            ccol = 0
            while ccol in used_colors:
                ccol += 1
            colors[o] = ccol

        for o, ccol in colors.items():
            suffix_idx[o] = max(suffix_idx.get(o, 0), ccol)

    for oldc in targets:
        if oldc in repl:
            continue
        base = (base_choice.get(oldc) or "").strip() or "SIN_CODIGO"
        repl[oldc] = _make_code(base, suffix_idx.get(oldc, 0))

    def _find_dup_cases() -> List[Tuple[str, str, List[str]]]:
        seen_by_partida: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
        for oldc, newc in repl.items():
            for pk in partidas_by_old.get(oldc, {"__ROOT__"}):
                seen_by_partida[pk][newc].append(oldc)

        dup: List[Tuple[str, str, List[str]]] = []
        for pk, m in seen_by_partida.items():
            for newc, olds in m.items():
                if len(olds) > 1:
                    dup.append((pk, newc, olds))
        return dup

    max_bumps = 5000
    bumps = 0
    while True:
        dup_cases = _find_dup_cases()
        if not dup_cases:
            break
        if bumps >= max_bumps:
            break

        pk, newc, olds = dup_cases[0]
        bump_old = min(olds, key=lambda o: conf_choice.get(o, 0.0))

        if base_choice.get(bump_old) == "% DESCUENTO":
            bumps += 1
            continue

        base = (base_choice.get(bump_old) or "").strip() or "SIN_CODIGO"
        suffix_idx[bump_old] = int(suffix_idx.get(bump_old, 0)) + 1
        repl[bump_old] = _make_code(base, suffix_idx[bump_old])
        bumps += 1

    rows: List[Tuple[str, str, float, str]] = []
    for oldc in targets:
        if oldc not in repl:
            continue
        rows.append((oldc, repl[oldc], float(conf_choice.get(oldc, 0.0)), str(method_choice.get(oldc, ""))))

    dup_cases = _find_dup_cases()
    if not dup_cases:
        print("[PHASE2 OK] Validación de unicidad por partida: sin duplicados de código nuevo.")
        return repl, rows

    print(
        f"[PHASE2 ERROR] Duplicados detectados: código nuevo repetido dentro de una misma partida "
        f"(casos={len(dup_cases)}). Detalle:"
    )
    for pk, newc, olds in dup_cases[:500]:
        print(f"  - Partida {pk}: '{newc}' repetido {len(olds)} veces | old_codes: {', '.join(olds)}")

    raise RuntimeError(
        f"PHASE2: Duplicados de new_code dentro de la misma partida (casos={len(dup_cases)}). "
        f"Revisa asignación/versionado."
    )


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


def rewrite_bc3_with_codes(src: Path, dst: Path, repl_map: Dict[str, str]) -> None:
    """
    Reescribe el BC3 aplicando solo el cambio de CÓDIGO (nada más).
    """
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
    """
    import sys as _sys

    def _app_dir() -> Path:
        if getattr(_sys, "frozen", False):
            return Path(_sys.executable).resolve().parent
        return Path(_sys.argv[0]).resolve().parent

    def _first_existing(candidates: list[Path]) -> Optional[Path]:
        for p in candidates:
            try:
                if p.exists():
                    return p
            except Exception:
                pass
        return None

    base_dir = _app_dir()
    data_dir = base_dir / "data"

    if bc3_in is None:
        bc3_in = kwargs.pop("input_bc3", None)
    if catalog_xlsx is None:
        catalog_xlsx = kwargs.pop("catalog_path", None)
    if bc3_out is None:
        bc3_out = kwargs.pop("output_bc3", None)

    if catalog_xlsx is None:
        env_cat = os.getenv("PHASE2_CATALOG_XLSX", "").strip()
        if env_cat:
            catalog_xlsx = env_cat
        else:
            cand = [
                data_dir / "catalog" / "catalog.xlsx",
                data_dir / "catalog" / "catalogo.xlsx",
                data_dir / "catalog" / "catalogo_productos.xlsx",
                data_dir / "catalog" / "catalogo_productos.xlsm",
                base_dir / "catalog.xlsx",
            ]
            found = _first_existing(cand)
            if found:
                catalog_xlsx = found

    if bc3_in is None or catalog_xlsx is None:
        raise ValueError("run_phase2: faltan 'bc3_in' y/o 'catalog_xlsx'.")

    bc3_in = Path(bc3_in)
    catalog_xlsx = Path(catalog_xlsx)

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
        else:
            cand_tpl = [
                data_dir / "templates" / "REFCRU_template.xlsx",
                data_dir / "templates" / "REFCRU.xlsx",
                base_dir / "templates" / "REFCRU_template.xlsx",
                base_dir / "templates" / "REFCRU.xlsx",
            ]
            found_tpl = _first_existing(cand_tpl)
            if found_tpl:
                refcru_template_xlsx = found_tpl

    if refcru_template_xlsx is not None:
        refcru_template_xlsx = Path(refcru_template_xlsx)

    if refcru_out is None:
        refcru_out = bc3_out.with_name(bc3_out.stem + "_REFCRU.xlsx")
    else:
        refcru_out = Path(refcru_out)

    progress_cb = None
    for k in ("progress_cb", "on_progress", "progress_callback", "callback", "logger"):
        if k in kwargs and kwargs[k] is not None:
            progress_cb = kwargs[k]
            break

    repl_map, rows = _build_replacement_map(
        bc3_in,
        catalog_xlsx,
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
            print("[PHASE2 WARN] No se encontró plantilla REFCRU (XML mapping). No se genera XLSX REFCRU.")
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

            print(f"[PHASE2 OK] Generado REFCRU importable en BC: {refcru_out}")
            if progress_cb:
                progress_cb(f"OK: generado REFCRU importable en BC: {refcru_out.name}")

    return bc3_out