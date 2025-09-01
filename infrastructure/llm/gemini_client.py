# infrastructure/llm/gemini_client.py

from __future__ import annotations

import json
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional, List, Dict, Any

import google.generativeai as genai
from google.api_core.exceptions import ResourceExhausted, GoogleAPIError


@dataclass
class GeminiSelection:
    product_code: str
    confidence: float
    reason: str


class _RateLimiter:
    def __init__(self, rpm: int) -> None:
        self.rpm = max(1, rpm)
        self._times: deque[float] = deque()

    def wait(self) -> None:
        now = time.monotonic()
        window = 60.0
        while self._times and now - self._times[0] > window:
            self._times.popleft()
        if len(self._times) >= self.rpm:
            sleep_for = window - (now - self._times[0])
            if sleep_for > 0:
                time.sleep(sleep_for)
        self._times.append(time.monotonic())


class GeminiClient:
    def __init__(
        self,
        api_key: str,
        model_name: str = "gemini-2.5-flash-lite",
        rpm: int = 10,
        max_retries: int = 2,
        on_429: str = "fallback",  # "wait" | "fallback"
    ) -> None:
        if not api_key:
            raise ValueError("GEMINI_API_KEY no configurada")
        genai.configure(api_key=api_key)
        self._model = genai.GenerativeModel(model_name)
        self._limiter = _RateLimiter(rpm)
        self._max_retries = max(0, max_retries)
        self._on_429 = on_429.lower()

    # --------- Modo 1: item individual (ya existente) ----------
    def select_product(
        self,
        *,
        short_desc: str,
        long_desc: Optional[str],
        products_prompt_list: str,
    ) -> GeminiSelection:
        system = (
            "Eres un asistente experto en construcción para una empresa constructora, "
            "especializada en edificación. Tu tarea es seleccionar el producto más adecuado "
            "EXCLUSIVAMENTE de la lista proporcionada."
        )
        user = f"""
[LISTA DE PRODUCTOS]
{products_prompt_list}

[DESCOMPUESTO A CLASIFICAR]
descripcion_larga (PRIORITARIA): {(long_desc or '').strip()}
descripcion_corta: {(short_desc or '').strip()}

[INSTRUCCIONES]
- Prioriza la descripcion_larga si existe.
- No elijas productos fuera de la lista.
- Si no hay candidato claro: product_code="" y confidence=0.0.
- Responde SOLO en JSON válido: product_code (string), confidence (float 0..1), reason (string).
"""
        return self._call_llm_single(system, user)

    def _call_llm_single(self, system: str, user: str) -> GeminiSelection:
        attempt = 0
        while True:
            self._limiter.wait()
            try:
                resp = self._model.generate_content([system, user])
                text = (resp.text or "").strip()
                data = self._parse_json(text)
                return GeminiSelection(
                    product_code=str(data.get("product_code", "")).strip(),
                    confidence=float(data.get("confidence", 0.0)),
                    reason=str(data.get("reason", "")).strip(),
                )
            except ResourceExhausted as e:
                attempt += 1
                if self._on_429 == "wait" and attempt <= self._max_retries:
                    retry_sec = getattr(getattr(e, "retry_delay", None), "seconds", None)
                    time.sleep(float(retry_sec or 30))
                    continue
                return GeminiSelection(product_code="", confidence=0.0, reason="rate_limit")
            except GoogleAPIError as e:
                attempt += 1
                if attempt <= self._max_retries:
                    time.sleep(min(30, 2 ** attempt))
                    continue
                return GeminiSelection(product_code="", confidence=0.0, reason=f"api_error:{e.__class__.__name__}")
            except Exception as e:  # noqa: BLE001
                return GeminiSelection(product_code="", confidence=0.0, reason=f"error:{e.__class__.__name__}")

    # --------- Modo 2: batch (varios ítems por request) ----------
    def select_products_batch(
        self,
        *,
        items: List[Dict[str, Any]],  # [{"id": str, "short": str, "long": str, "candidates": [{"code","name"},...]}, ...]
    ) -> List[Dict[str, Any]]:
        """
        Devuelve una lista de dicts:
        [{"id": "...", "product_code": "...", "confidence": 0..1, "reason": "..."}, ...]
        """
        system = (
            "Eres un asistente experto en construcción para una constructora especializada en edificación. "
            "Debes asignar, para CADA ítem, el mejor producto EXCLUSIVAMENTE de su lista de candidatos."
        )
        # Compactamos el payload para reducir tokens
        items_json = json.dumps(items, ensure_ascii=False)

        user = f"""
[INSTRUCCIONES]
- Para cada ítem, elige SOLO entre los códigos listados en 'candidates'.
- Prioriza 'long' (descripcion_larga) si existe.
- Si no hay candidato claro: product_code="" y confidence=0.0.
- Responde SOLO en JSON válido como lista con este esquema:
  [
    {{"id": "...", "product_code": "...", "confidence": 0..1, "reason": "..."}},
    ...
  ]

[ITEMS]
{items_json}
"""
        attempt = 0
        while True:
            self._limiter.wait()
            try:
                resp = self._model.generate_content([system, user])
                text = (resp.text or "").strip()
                data = self._parse_json(text)
                if isinstance(data, dict):  # por si responde objeto con 'results'
                    data = data.get("results") or data.get("items") or []
                if not isinstance(data, list):
                    raise ValueError("Respuesta no es lista JSON")
                # Normalización de tipos
                out: List[Dict[str, Any]] = []
                for r in data:
                    out.append({
                        "id": str(r.get("id", "")).strip(),
                        "product_code": str(r.get("product_code", "")).strip(),
                        "confidence": float(r.get("confidence", 0.0)),
                        "reason": str(r.get("reason", "")).strip(),
                    })
                return out
            except ResourceExhausted as e:
                attempt += 1
                if self._on_429 == "wait" and attempt <= self._max_retries:
                    retry_sec = getattr(getattr(e, "retry_delay", None), "seconds", None)
                    time.sleep(float(retry_sec or 30))
                    continue
                return []  # fallback lo gestionará arriba
            except GoogleAPIError as e:
                attempt += 1
                if attempt <= self._max_retries:
                    time.sleep(min(30, 2 ** attempt))
                    continue
                return []
            except Exception:
                return []

    @staticmethod
    def _parse_json(text: str) -> dict | list:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            start, end = text.find("{"), text.rfind("}")
            if start != -1 and end != -1 and end > start:
                return json.loads(text[start:end + 1])
            start, end = text.find("["), text.rfind("]")
            if start != -1 and end != -1 and end > start:
                return json.loads(text[start:end + 1])
            raise
