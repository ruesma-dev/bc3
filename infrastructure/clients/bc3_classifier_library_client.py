# infrastructure/clients/bc3_classifier_library_client.py
from __future__ import annotations

import importlib
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Type

logger = logging.getLogger(__name__)

_IMPORT_ERROR: Optional[Exception] = None
_LIBRARY_CLASS: Optional[Type[Any]] = None
_LIBRARY_CONFIG_CLASS: Optional[Type[Any]] = None


def _resolve_library_symbols() -> tuple[Optional[Type[Any]], Optional[Type[Any]]]:
    global _IMPORT_ERROR, _LIBRARY_CLASS, _LIBRARY_CONFIG_CLASS

    if _LIBRARY_CLASS is not None and _LIBRARY_CONFIG_CLASS is not None:
        return _LIBRARY_CLASS, _LIBRARY_CONFIG_CLASS

    candidate_modules = [
        "ruesma_ocr_service.bc3_library",
    ]
    candidate_library_names = [
        "Bc3ClassifierLibrary",
        "Bc3Library",
        "Bc3Classifier",
    ]
    candidate_config_names = [
        "Bc3ClassifierLibraryConfig",
        "Bc3LibraryConfig",
        "Bc3ClassifierConfig",
    ]

    last_error: Optional[Exception] = None

    for module_name in candidate_modules:
        try:
            module = importlib.import_module(module_name)
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            continue

        library_class = None
        config_class = None

        for attr_name in candidate_library_names:
            if hasattr(module, attr_name):
                library_class = getattr(module, attr_name)
                break

        for attr_name in candidate_config_names:
            if hasattr(module, attr_name):
                config_class = getattr(module, attr_name)
                break

        if library_class is not None and config_class is not None:
            _LIBRARY_CLASS = library_class
            _LIBRARY_CONFIG_CLASS = config_class
            _IMPORT_ERROR = None
            return _LIBRARY_CLASS, _LIBRARY_CONFIG_CLASS

        available = sorted(name for name in dir(module) if not name.startswith("_"))
        last_error = RuntimeError(
            f"El módulo '{module_name}' se importó, pero no expone una combinación válida "
            f"de clases de librería/configuración. Exporta: {available}"
        )

    _IMPORT_ERROR = last_error
    return None, None


def _load_local_dotenv_once() -> None:
    if getattr(_load_local_dotenv_once, "_done", False):
        return

    try:
        from dotenv import load_dotenv  # type: ignore
    except Exception:
        _load_local_dotenv_once._done = True
        return

    candidates: list[Path] = []
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
            logger.info("Cargado .env para cliente librería BC3: %s", candidate)
            _load_local_dotenv_once._done = True
            return

    load_dotenv(override=False)
    _load_local_dotenv_once._done = True


def _read_first_int_env(names: list[str], default: int) -> int:
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


@dataclass(frozen=True)
class Bc3ClassifierLibraryClientConfig:
    model_name: str
    llm_batch_size: int
    top_k_candidates: int


class Bc3ClassifierLibraryClient:
    def __init__(self, config: Bc3ClassifierLibraryClientConfig) -> None:
        self._config = config

        library_class, config_class = _resolve_library_symbols()
        if library_class is None or config_class is None:
            detail = ""
            if _IMPORT_ERROR is not None:
                detail = (
                    f" Detalle real: {type(_IMPORT_ERROR).__name__}: "
                    f"{_IMPORT_ERROR}"
                )
            raise RuntimeError(
                "No se pudo cargar la librería del servicio 2 "
                "empaquetada dentro de la aplicación."
                + detail
            ) from _IMPORT_ERROR

        library_config = config_class(
            model_name=config.model_name,
            llm_batch_size=config.llm_batch_size,
            top_k_candidates=config.top_k_candidates,
        )
        self._library = library_class(config=library_config)

    @classmethod
    def from_env(cls) -> "Bc3ClassifierLibraryClient":
        _load_local_dotenv_once()

        model_name = (
            os.getenv("OPENAI_MODEL_NAME")
            or os.getenv("OPENAI_MODEL")
            or "gpt-5.2"
        ).strip()

        llm_batch_size = _read_first_int_env(
            [
                "BC3_REQUEST_BATCH_SIZE",
                "BC3_LLM_BATCH_SIZE",
                "GEMINI_BATCH_SIZE",
            ],
            default=5,
        )

        top_k_candidates = _read_first_int_env(
            [
                "BC3_DEFAULT_TOP_K",
                "BC3_TOP_K_CANDIDATES",
            ],
            default=20,
        )

        logger.info(
            "Bc3ClassifierLibraryClient config resuelta. model=%s llm_batch_size=%s top_k_candidates=%s",
            model_name,
            llm_batch_size,
            top_k_candidates,
        )

        return cls(
            Bc3ClassifierLibraryClientConfig(
                model_name=model_name,
                llm_batch_size=llm_batch_size,
                top_k_candidates=top_k_candidates,
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

        logger.info(
            "Llamada librería BC3. batch=%s/%s items=%s ids=%s",
            batch_index,
            total_batches,
            len(ids),
            ids,
        )

        return self._library.classify(payload)