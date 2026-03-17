# application/services/budget_bc3_batch_service.py
from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Protocol, Sequence

logger = logging.getLogger(__name__)

ProgressCallback = Callable[
    [int, int, List[Dict[str, Any]], List[Dict[str, Any]]],
    None,
]


class Bc3ClassifierClient(Protocol):
    def classify(
        self,
        payload: Dict[str, Any],
        *,
        batch_index: int,
        total_batches: int,
    ) -> Dict[str, Any]:
        ...


@dataclass(frozen=True)
class BudgetBc3BatchRequest:
    prompt_key: str
    bc3_id: str
    descompuestos: List[Dict[str, Any]]
    batch_size: int | None = None
    top_k_candidates: int = 20


class BudgetBc3BatchService:
    """
    Servicio 1 -> servicio 2 por lotes.

    La dependencia externa queda abstraída por un cliente Python, de forma que
    la GUI ya no conoce si el servicio 2 está implementado como script, API o
    librería local.
    """

    def __init__(self, bc3_client: Bc3ClassifierClient) -> None:
        self._bc3_client = bc3_client

    def classify_budget(
        self,
        request: BudgetBc3BatchRequest,
        *,
        progress_callback: ProgressCallback | None = None,
    ) -> Dict[str, Any]:
        if not request.descompuestos:
            raise ValueError("No hay descompuestos para clasificar.")

        batch_size = self._resolve_batch_size(request.batch_size)
        total_items = len(request.descompuestos)
        total_batches = max(1, (total_items + batch_size - 1) // batch_size)

        logger.info(
            "Inicio clasificación BC3 por lotes. bc3_id=%s total_items=%s batch_size=%s total_batches=%s",
            request.bc3_id,
            total_items,
            batch_size,
            total_batches,
        )

        aggregated_results: List[Dict[str, Any]] = []
        batch_meta: List[Dict[str, Any]] = []
        input_order = {
            str(item.get("id") or ""): index
            for index, item in enumerate(request.descompuestos)
        }

        for batch_index, batch_items in enumerate(
            self._chunk(request.descompuestos, batch_size),
            start=1,
        ):
            payload = self._build_batch_payload(
                request=request,
                batch_items=batch_items,
                batch_size=batch_size,
            )
            batch_ids = [str(item.get("id") or "") for item in batch_items]

            logger.info(
                "Preparado lote %s/%s. items=%s ids=%s",
                batch_index,
                total_batches,
                len(batch_items),
                batch_ids,
            )

            response = self._bc3_client.classify(
                payload,
                batch_index=batch_index,
                total_batches=total_batches,
            )
            batch_results = self._extract_results(response)

            logger.info(
                "Lote %s/%s completado. items_in=%s items_out=%s",
                batch_index,
                total_batches,
                len(batch_items),
                len(batch_results),
            )

            aggregated_results.extend(batch_results)
            batch_meta.append(
                {
                    "batch_index": batch_index,
                    "items": len(batch_items),
                    "ids": batch_ids,
                }
            )

            if progress_callback is not None:
                progress_callback(
                    batch_index,
                    total_batches,
                    batch_items,
                    batch_results,
                )

        aggregated_results.sort(
            key=lambda item: input_order.get(str(item.get("id") or ""), 10**9)
        )

        return {
            "meta": {
                "prompt_key": request.prompt_key,
                "schema": "bc3_clasificacion_resultado",
                "source_filename": f"{request.bc3_id}.json",
                "source_mime_type": "application/json",
                "source_sha256": _sha256_obj(
                    {
                        "prompt_key": request.prompt_key,
                        "bc3_id": request.bc3_id,
                        "top_k_candidates": request.top_k_candidates,
                        "batch_size": batch_size,
                        "descompuestos": request.descompuestos,
                    }
                ),
                "processed_at_utc": _utc_iso(),
                "context": {
                    "batch_size": batch_size,
                    "total_batches": total_batches,
                    "descompuestos_count": total_items,
                    "batches": batch_meta,
                },
            },
            "data": {
                "resultados": aggregated_results,
            },
        }

    @staticmethod
    def _build_batch_payload(
        *,
        request: BudgetBc3BatchRequest,
        batch_items: List[Dict[str, Any]],
        batch_size: int,
    ) -> Dict[str, Any]:
        return {
            "prompt_key": request.prompt_key,
            "bc3_id": request.bc3_id,
            "top_k_candidates": request.top_k_candidates,
            "llm_batch_size": batch_size,
            "descompuestos": batch_items,
        }

    @staticmethod
    def _extract_results(response: Dict[str, Any]) -> List[Dict[str, Any]]:
        data = response.get("data") or {}
        resultados = data.get("resultados") or []
        if not isinstance(resultados, list):
            raise RuntimeError(
                "La respuesta del servicio BC3 no contiene 'data.resultados' como lista."
            )

        output: List[Dict[str, Any]] = []
        for item in resultados:
            if isinstance(item, dict):
                output.append(item)
        return output

    @staticmethod
    def _chunk(
        items: Sequence[Dict[str, Any]],
        chunk_size: int,
    ) -> Iterator[List[Dict[str, Any]]]:
        size = max(1, int(chunk_size))
        for start in range(0, len(items), size):
            yield list(items[start:start + size])

    @staticmethod
    def _resolve_batch_size(explicit_value: int | None) -> int:
        _load_local_dotenv_once()

        if explicit_value is not None:
            return max(1, int(explicit_value))

        raw = (
            os.getenv("BC3_REQUEST_BATCH_SIZE")
            or os.getenv("BC3_LLM_BATCH_SIZE")
            or os.getenv("GEMINI_BATCH_SIZE")
            or "5"
        )
        try:
            value = int(str(raw).strip())
        except (TypeError, ValueError):
            value = 5
        return max(1, value)


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
            logger.info("Cargado .env para batch BC3: %s", candidate)
            _load_local_dotenv_once._done = True
            return

    load_dotenv(override=False)
    _load_local_dotenv_once._done = True


def _utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _sha256_obj(obj: Dict[str, Any]) -> str:
    raw = json.dumps(
        obj,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()
