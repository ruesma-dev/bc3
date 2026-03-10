# infrastructure/clients/bc3_classifier_api_client.py
from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any, Dict

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Bc3ClassifierApiClientConfig:
    base_url: str
    timeout_s: int = 180


class Bc3ClassifierApiClient:
    def __init__(self, config: Bc3ClassifierApiClientConfig) -> None:
        self._config = config

    def classify(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = self._build_url("/v1/bc3/classify")
        raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        request = urllib.request.Request(
            url=url,
            data=raw,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )

        try:
            with urllib.request.urlopen(request, timeout=self._config.timeout_s) as response:
                body = response.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(
                f"Error HTTP llamando a BC3 service. status={exc.code} detail={detail}"
            ) from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(
                f"No se pudo conectar con BC3 service en {url}: {exc}"
            ) from exc

        try:
            parsed = json.loads(body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                "La respuesta del BC3 service no es JSON válido."
            ) from exc

        if not isinstance(parsed, dict):
            raise RuntimeError("La respuesta del BC3 service debe ser un objeto JSON.")

        logger.debug(
            "Respuesta BC3 recibida. url=%s keys=%s",
            url,
            sorted(parsed.keys()),
        )
        return parsed

    def _build_url(self, path: str) -> str:
        base = self._config.base_url.rstrip("/")
        suffix = path if path.startswith("/") else f"/{path}"
        return f"{base}{suffix}"
