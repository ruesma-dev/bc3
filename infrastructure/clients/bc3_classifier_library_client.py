# infrastructure/clients/bc3_classifier_library_client.py
from __future__ import annotations

import logging
import os
import threading
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_IMPORT_ERROR: Optional[Exception] = None
Bc3ClassifierLibrary = None
build_default_classifier = None

try:
    from ruesma_ocr_service.bc3_library import (  # type: ignore
        Bc3ClassifierLibrary,
        build_default_classifier,
    )
except Exception as exc:  # pragma: no cover
    _IMPORT_ERROR = exc


@dataclass(frozen=True)
class Bc3ClassifierLibraryClientConfig:
    model_name: str
    llm_batch_size: int
    top_k_candidates: int


class Bc3ClassifierLibraryClient:
    """
    Adaptador para usar el servicio 2 como librería local, manteniendo
    la misma interfaz pública que esperaba BudgetBc3BatchService.
    """

    _instances_by_model: Dict[str, Any] = {}
    _lock = threading.Lock()

    def __init__(self, config: Bc3ClassifierLibraryClientConfig) -> None:
        self._config = config

    @classmethod
    def from_env(cls) -> "Bc3ClassifierLibraryClient":
        model_name = (
            os.getenv("OPENAI_MODEL_NAME")
            or os.getenv("OPENAI_MODEL")
            or "gpt-5.2"
        ).strip()

        raw_batch = (
            os.getenv("BC3_REQUEST_BATCH_SIZE")
            or os.getenv("BC3_LLM_BATCH_SIZE")
            or os.getenv("GEMINI_BATCH_SIZE")
            or "5"
        )
        try:
            llm_batch_size = max(1, int(str(raw_batch).strip()))
        except Exception:
            llm_batch_size = 5

        raw_top_k = os.getenv("BC3_TOP_K_CANDIDATES") or "20"
        try:
            top_k_candidates = max(1, int(str(raw_top_k).strip()))
        except Exception:
            top_k_candidates = 20

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

        library = self._get_library()

        effective_payload = dict(payload)
        effective_payload.setdefault("llm_batch_size", self._config.llm_batch_size)
        effective_payload.setdefault("top_k_candidates", self._config.top_k_candidates)

        response = library.classify(effective_payload)

        logger.info(
            "Respuesta librería BC3 OK. batch=%s/%s items=%s",
            batch_index,
            total_batches,
            len(((response.get("data") or {}).get("resultados") or [])),
        )
        return response

    def _get_library(self) -> Any:
        if Bc3ClassifierLibrary is None and build_default_classifier is None:
            detail = ""
            if _IMPORT_ERROR is not None:
                detail = f" Detalle real: {type(_IMPORT_ERROR).__name__}: {_IMPORT_ERROR}"
            raise RuntimeError(
                "No se pudo importar la librería del servicio 2. "
                "Asegúrate de que 'ruesma_ocr_service' está instalado en este entorno."
                + detail
            ) from _IMPORT_ERROR

        model_key = self._config.model_name.strip() or "gpt-5.2"

        with self._lock:
            cached = self._instances_by_model.get(model_key)
            if cached is not None:
                return cached

            os.environ["OPENAI_MODEL_NAME"] = model_key
            os.environ["OPENAI_MODEL"] = model_key

            if Bc3ClassifierLibrary is not None and hasattr(Bc3ClassifierLibrary, "from_env"):
                instance = Bc3ClassifierLibrary.from_env()
            elif callable(build_default_classifier):
                instance = build_default_classifier()
            elif Bc3ClassifierLibrary is not None:
                instance = Bc3ClassifierLibrary()
            else:
                raise RuntimeError(
                    "La librería del servicio 2 está importada, pero no expone "
                    "una API válida para inicializar el clasificador."
                )

            self._instances_by_model[model_key] = instance
            return instance