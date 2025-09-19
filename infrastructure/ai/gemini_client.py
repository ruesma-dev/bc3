# infrastructure/ai/gemini_client.py
from __future__ import annotations
"""
Cliente Gemini con:
- Rate limiting por RPM (tokens no se controlan aquí; los maneja el servicio).
- Reintentos con backoff en 429/503 (configurable).
- Modo batch opcional (una llamada procesa varios ítems y devuelve lista JSON).
"""

import json
import os
import random
import time
from typing import Any, Dict, List, Optional

# SDK oficial (si no está instalado, lanzamos en ejecución)
try:
    import google.generativeai as genai  # type: ignore
except Exception as _e:  # pragma: no cover
    genai = None


# ----------------------------- Rate Limiter ---------------------------------
class RateLimiter:
    """Limita a ~N requests por minuto (RPM). Sencillo y suficiente para uso secuencial."""

    def __init__(self, rpm: int = 10) -> None:
        rpm = max(1, int(rpm or 10))
        self.min_interval = 60.0 / float(rpm)
        self._last = 0.0

    def wait(self) -> None:
        now = time.time()
        delay = self.min_interval - (now - self._last)
        if delay > 0:
            # pequeño jitter para evitar sincronía exacta
            time.sleep(delay + random.uniform(0.0, 0.05))
        self._last = time.time()


def _configure() -> tuple[Optional[str], str]:
    api_key = os.getenv("GEMINI_API_KEY")
    model_name = os.getenv("GEMINI_MODEL_NAME", "gemini-2.5-flash")
    if genai and api_key:
        genai.configure(api_key=api_key)
    return api_key, model_name


def _should_retry(msg: str) -> bool:
    m = (msg or "").lower()
    return ("429" in m) or ("rate" in m) or ("quota" in m) or ("exceeded" in m) or ("unavailable" in m) or ("503" in m)


def _gen_model(model_name: str):
    return genai.GenerativeModel(
        model_name,
        generation_config={
            "response_mime_type": "application/json",
            "temperature": float(os.getenv("GEMINI_TEMPERATURE", "0.2")),
        },
    )


# ----------------------------- Single choice --------------------------------
def choose_best_code_with_llm(
    context_text: str,
    candidates: List[Dict[str, str]],
    limiter: Optional[RateLimiter] = None,
) -> Dict[str, Any]:
    """
    Elige el mejor 'code' de 'candidates' para el contexto dado.
    Devuelve: {"best_code": str, "confidence": float, "reason": str}

    Reglas de reintento:
      GEMINI_ON_429 = "wait" (default): backoff y reintenta hasta 3 veces.
                        "fallback": lanza RuntimeError para que el caller haga fallback.
    """
    if not genai:
        raise RuntimeError("Gemini SDK no disponible.")
    api_key, model_name = _configure()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY no configurada.")

    on429 = os.getenv("GEMINI_ON_429", "wait").lower().strip() or "wait"
    model = _gen_model(model_name)

    system = (
        "Eres un experto en presupuestos de obra en España (software PRESTO). "
        "Vas a clasificar el código de un descompuesto (material) escogiendo un producto del catálogo. "
        "Responde SOLO JSON con: {\"best_code\":\"...\",\"confidence\":0..1,\"reason\":\"...\"}."
    )

    payload = {
        "contexto": context_text,
        "catalogo_topk": [{"code": c.get("code", ""), "desc": c.get("desc", "")} for c in candidates],
        "instrucciones": [
            "Elige el code del catálogo que mejor casa con el descompuesto.",
            "Considera primero tipo y grupo, después familia, luego producto.",
            "Responde solo JSON."
        ],
    }

    attempts = 0
    while True:
        attempts += 1
        if limiter:
            limiter.wait()
        try:
            resp = model.generate_content([system, {"text": json.dumps(payload, ensure_ascii=False)}])
            if not resp or not resp.text:
                raise RuntimeError("Respuesta vacía del modelo.")
            data = json.loads(resp.text)
            best = (data.get("best_code") or "").strip()
            conf = float(data.get("confidence", 0.0))
            reason = data.get("reason", "")
            if not best:
                raise RuntimeError("JSON sin best_code.")
            return {"best_code": best, "confidence": conf, "reason": reason}
        except Exception as e:
            msg = str(e)
            if _should_retry(msg) and on429 == "wait" and attempts < 4:
                # backoff exponencial con jitter
                sleep_s = min(10.0, (2 ** (attempts - 1))) + random.uniform(0.0, 0.25)
                time.sleep(sleep_s)
                continue
            raise RuntimeError(f"Fallo Gemini: {e}")


# ----------------------------- Batch choice ---------------------------------
def choose_best_code_batch_with_llm(
    items: List[Dict[str, Any]],
    limiter: Optional[RateLimiter] = None,
) -> List[Dict[str, Any]]:
    """
    items = [
      {
        "id": "VERTEDEROT",
        "context": "...",
        "candidates": [{"code": "...", "desc": "..."}, ...]
      }, ...
    ]
    Devuelve lista de objetos: [{"id": "...", "best_code": "...", "confidence": 0.0, "reason": "..."}]
    """
    if not genai:
        raise RuntimeError("Gemini SDK no disponible.")
    api_key, model_name = _configure()
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY no configurada.")
    on429 = os.getenv("GEMINI_ON_429", "wait").lower().strip() or "wait"

    system = (
        "Eres un experto en presupuestos de obra en España (PRESTO). "
        "Para cada ítem de entrada elige un 'code' del catálogo adjunto a ese ítem. "
        "Responde SOLO JSON como lista: "
        "[{\"id\":\"...\",\"best_code\":\"...\",\"confidence\":0..1,\"reason\":\"...\"}, ...]"
    )

    # Compactamos prompt
    prompt = {"items": items}

    attempts = 0
    while True:
        attempts += 1
        if limiter:
            limiter.wait()
        try:
            model = _gen_model(model_name)
            resp = model.generate_content([system, {"text": json.dumps(prompt, ensure_ascii=False)}])
            if not resp or not resp.text:
                raise RuntimeError("Respuesta vacía del modelo.")
            data = json.loads(resp.text)
            if not isinstance(data, list):
                raise RuntimeError("Esperaba lista JSON.")
            return data
        except Exception as e:
            msg = str(e)
            if _should_retry(msg) and on429 == "wait" and attempts < 4:
                time.sleep(min(10.0, (2 ** (attempts - 1))) + random.uniform(0.0, 0.25))
                continue
            raise RuntimeError(f"Fallo Gemini(batch): {e}")
