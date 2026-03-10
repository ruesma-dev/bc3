# infrastructure/clients/bc3_classifier_subprocess_client.py
from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Bc3ClassifierSubprocessClientConfig:
    python_executable: str
    working_dir: str
    module_name: str = "interface_adapters.cli.bc3_classify_stdin"
    timeout_s: int = 900
    dump_io: bool = False
    dump_dir: Optional[str] = None


class Bc3ClassifierSubprocessClient:
    """
    Cliente del servicio BC3 por subprocess + stdin/stdout.

    Objetivos de robustez:
    - cargar `.env` del proyecto bc3 aunque el GUI no importe config.settings;
    - aceptar variables nuevas BC3_* y antiguas OCR_SERVICE_*;
    - resolver automáticamente el root de ocr_service si las variables no están cargadas;
    - elegir el python del ocr_service cuando exista su `.venv`.
    """

    def __init__(self, config: Bc3ClassifierSubprocessClientConfig) -> None:
        self._config = config

    @classmethod
    def from_env(cls) -> "Bc3ClassifierSubprocessClient":
        _load_local_dotenv_once()

        module_name = (
            os.getenv("BC3_SERVICE_MODULE")
            or os.getenv("OCR_SERVICE_MODULE")
            or "interface_adapters.cli.bc3_classify_stdin"
        ).strip()

        working_dir = _resolve_working_dir(module_name=module_name)
        python_executable = _resolve_python_executable(working_dir=working_dir)
        timeout_s = _read_first_int_env(
            ["BC3_CLI_TIMEOUT_S", "OCR_SERVICE_TIMEOUT_S"],
            default=900,
        )
        dump_io = _read_first_bool_env(
            ["BC3_SUBPROCESS_DUMP_IO", "PHASE2_DUMP_OCR_IO"],
            default=False,
        )
        dump_dir = (
            os.getenv("BC3_SUBPROCESS_DUMP_DIR")
            or os.getenv("PHASE2_DUMP_OCR_DIR")
            or "logs/bc3_subprocess_io"
        )

        logger.info(
            "Bc3ClassifierSubprocessClient config resuelta. python=%s cwd=%s module=%s timeout_s=%s dump_io=%s dump_dir=%s",
            python_executable,
            working_dir,
            module_name,
            timeout_s,
            dump_io,
            dump_dir,
        )

        return cls(
            config=Bc3ClassifierSubprocessClientConfig(
                python_executable=python_executable,
                working_dir=working_dir,
                module_name=module_name,
                timeout_s=timeout_s,
                dump_io=dump_io,
                dump_dir=dump_dir,
            )
        )

    def classify(
        self,
        payload: Dict[str, Any],
        *,
        batch_index: int,
        total_batches: int,
    ) -> Dict[str, Any]:
        descompuestos = payload.get("descompuestos") or []
        ids = [
            str(item.get("id") or "")
            for item in descompuestos
            if isinstance(item, dict)
        ]
        raw_request = json.dumps(payload, ensure_ascii=False)

        logger.info(
            "Llamada subprocess BC3. batch=%s/%s items=%s ids=%s timeout_s=%s cwd=%s module=%s python=%s",
            batch_index,
            total_batches,
            len(ids),
            ids,
            self._config.timeout_s,
            self._config.working_dir,
            self._config.module_name,
            self._config.python_executable,
        )

        self._dump_text(
            kind="request",
            batch_index=batch_index,
            total_batches=total_batches,
            ids=ids,
            content=raw_request,
        )

        command = [
            self._config.python_executable,
            "-m",
            self._config.module_name,
        ]

        env = os.environ.copy()
        env["PYTHONIOENCODING"] = "utf-8"
        env["PYTHONPATH"] = _build_pythonpath(self._config.working_dir, env.get("PYTHONPATH"))

        try:
            completed = subprocess.run(
                command,
                input=raw_request,
                text=True,
                capture_output=True,
                cwd=self._config.working_dir,
                env=env,
                timeout=self._config.timeout_s,
                check=False,
                encoding="utf-8",
                errors="replace",
            )
        except subprocess.TimeoutExpired as exc:
            stderr_text = (exc.stderr or "") if isinstance(exc.stderr, str) else ""
            stdout_text = (exc.stdout or "") if isinstance(exc.stdout, str) else ""
            self._dump_text(
                kind="timeout_stdout",
                batch_index=batch_index,
                total_batches=total_batches,
                ids=ids,
                content=stdout_text,
            )
            self._dump_text(
                kind="timeout_stderr",
                batch_index=batch_index,
                total_batches=total_batches,
                ids=ids,
                content=stderr_text,
            )
            raise RuntimeError(
                "Timeout llamando al servicio BC3 por subprocess. "
                f"batch={batch_index}/{total_batches} items={len(ids)} ids={ids} "
                f"timeout_s={self._config.timeout_s}"
            ) from exc

        stderr_text = completed.stderr or ""
        stdout_text = completed.stdout or ""

        if stderr_text.strip():
            logger.debug(
                "stderr subprocess BC3 batch=%s/%s\n%s",
                batch_index,
                total_batches,
                stderr_text,
            )

        self._dump_text(
            kind="stdout",
            batch_index=batch_index,
            total_batches=total_batches,
            ids=ids,
            content=stdout_text,
        )
        self._dump_text(
            kind="stderr",
            batch_index=batch_index,
            total_batches=total_batches,
            ids=ids,
            content=stderr_text,
        )

        if completed.returncode != 0:
            raise RuntimeError(
                "El servicio BC3 devolvió error por subprocess. "
                f"batch={batch_index}/{total_batches} items={len(ids)} ids={ids} "
                f"returncode={completed.returncode} stderr={stderr_text[:1500]}"
            )

        parsed = self._parse_stdout_json(
            stdout_text=stdout_text,
            batch_index=batch_index,
            total_batches=total_batches,
            ids=ids,
        )

        logger.info(
            "Respuesta subprocess BC3 OK. batch=%s/%s items=%s",
            batch_index,
            total_batches,
            len((parsed.get("data") or {}).get("resultados") or []),
        )
        return parsed

    def _parse_stdout_json(
        self,
        *,
        stdout_text: str,
        batch_index: int,
        total_batches: int,
        ids: List[str],
    ) -> Dict[str, Any]:
        text = (stdout_text or "").strip()
        if not text:
            raise RuntimeError(
                "El servicio BC3 no devolvió stdout. "
                f"batch={batch_index}/{total_batches} ids={ids}"
            )

        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}")
            if start >= 0 and end > start:
                try:
                    parsed = json.loads(text[start:end + 1])
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        "El stdout del servicio BC3 no es JSON válido. "
                        f"batch={batch_index}/{total_batches} ids={ids} stdout={text[:1500]}"
                    ) from exc
            else:
                raise RuntimeError(
                    "El stdout del servicio BC3 no contiene JSON. "
                    f"batch={batch_index}/{total_batches} ids={ids} stdout={text[:1500]}"
                )

        if not isinstance(parsed, dict):
            raise RuntimeError(
                "La respuesta del servicio BC3 debe ser un objeto JSON. "
                f"batch={batch_index}/{total_batches} ids={ids}"
            )

        return parsed

    def _dump_text(
        self,
        *,
        kind: str,
        batch_index: int,
        total_batches: int,
        ids: List[str],
        content: str,
    ) -> None:
        if not self._config.dump_io:
            return

        dump_dir = Path(self._config.dump_dir or "logs/bc3_subprocess_io")
        dump_dir.mkdir(parents=True, exist_ok=True)

        first_id = ids[0] if ids else "sin_id"
        last_id = ids[-1] if ids else "sin_id"
        safe_first_id = _safe_filename(first_id)
        safe_last_id = _safe_filename(last_id)

        filename = (
            f"{kind}_batch_{batch_index:04d}_of_{total_batches:04d}"
            f"__{safe_first_id}__{safe_last_id}.json"
        )
        (dump_dir / filename).write_text(content or "", encoding="utf-8")


def _load_local_dotenv_once() -> None:
    if getattr(_load_local_dotenv_once, "_done", False):
        return

    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        _load_local_dotenv_once._done = True
        return

    candidates: List[Path] = []
    here = Path(__file__).resolve()
    candidates.append(Path.cwd() / ".env")
    for parent in [here.parent] + list(here.parents):
        candidates.append(parent / ".env")

    seen: set[str] = set()
    for candidate in candidates:
        key = str(candidate)
        if key in seen:
            continue
        seen.add(key)
        if candidate.exists():
            load_dotenv(dotenv_path=candidate, override=False)
            logger.info("Cargado .env para subprocess BC3: %s", candidate)
            _load_local_dotenv_once._done = True
            return

    load_dotenv(override=False)
    _load_local_dotenv_once._done = True


def _resolve_working_dir(*, module_name: str) -> str:
    env_candidates = [
        os.getenv("BC3_SERVICE_WORKDIR"),
        os.getenv("OCR_SERVICE_WORKDIR"),
        os.getenv("OCR_SERVICE_ROOT"),
    ]
    for raw in env_candidates:
        path = _clean_env_path(raw)
        if not path:
            continue
        if path.exists() and _root_has_module(path, module_name):
            return str(path)

    guessed = _guess_ocr_service_root(module_name=module_name)
    if guessed is not None:
        return str(guessed)

    return os.getcwd()


def _resolve_python_executable(*, working_dir: str) -> str:
    env_candidates = [
        os.getenv("BC3_SERVICE_PYTHON_EXE"),
        os.getenv("OCR_SERVICE_PYTHON"),
        os.getenv("OCR_SERVICE_PY"),
    ]
    for raw in env_candidates:
        path = _clean_env_path(raw)
        if path and path.exists():
            return str(path)

    wd = Path(working_dir)
    for candidate in [
        wd / ".venv" / "Scripts" / "python.exe",
        wd / ".venv" / "bin" / "python",
        wd / "venv" / "Scripts" / "python.exe",
        wd / "venv" / "bin" / "python",
    ]:
        if candidate.exists():
            return str(candidate)

    return sys.executable


def _guess_ocr_service_root(*, module_name: str) -> Optional[Path]:
    here = Path(__file__).resolve()
    for parent in [here.parent] + list(here.parents):
        if _root_has_module(parent, module_name):
            return parent

        sibling = parent / "ocr_service"
        if sibling.exists() and _root_has_module(sibling, module_name):
            return sibling

    cwd = Path.cwd().resolve()
    for candidate in [cwd, cwd / "ocr_service", cwd.parent / "ocr_service"]:
        if candidate.exists() and _root_has_module(candidate, module_name):
            return candidate

    return None


def _root_has_module(root: Path, module_name: str) -> bool:
    parts = [part for part in module_name.split(".") if part]
    if not parts:
        return False

    rel_py = Path(*parts).with_suffix(".py")
    rel_pkg_init = Path(*parts) / "__init__.py"
    rel_pkg_main = Path(*parts) / "__main__.py"

    for base in [root, root / "src"]:
        for rel in [rel_py, rel_pkg_init, rel_pkg_main]:
            if (base / rel).exists():
                return True
    return False


def _build_pythonpath(working_dir: str, existing_value: Optional[str]) -> str:
    paths: List[str] = []
    wd = Path(working_dir)
    for candidate in [wd, wd / "src"]:
        if candidate.exists():
            paths.append(str(candidate))

    if existing_value:
        paths.append(existing_value)

    return os.pathsep.join(paths)


def _clean_env_path(raw: Optional[str]) -> Optional[Path]:
    if raw is None:
        return None
    value = str(raw).strip().strip('"').strip("'")
    if not value:
        return None
    return Path(os.path.expandvars(os.path.expanduser(value)))


def _read_int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        value = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return max(1, value)


def _read_first_int_env(names: List[str], default: int) -> int:
    for name in names:
        raw = os.getenv(name)
        if raw is None or not str(raw).strip():
            continue
        try:
            value = int(str(raw).strip())
        except (TypeError, ValueError):
            continue
        return max(1, value)
    return default


def _read_bool_env(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}


def _read_first_bool_env(names: List[str], default: bool) -> bool:
    for name in names:
        raw = os.getenv(name)
        if raw is None or not str(raw).strip():
            continue
        return str(raw).strip().lower() in {"1", "true", "yes", "y", "on"}
    return default


def _safe_filename(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)
    cleaned = cleaned.strip("_")
    return cleaned or "sin_id"