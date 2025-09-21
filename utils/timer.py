# utils/timer.py
from __future__ import annotations

import time
from contextlib import contextmanager
from typing import Callable, List, Tuple


def _fmt(sec: float) -> str:
    """Formatea segundos con 3 decimales."""
    return f"{sec:.3f}s"


@contextmanager
def timer(label: str, logger: Callable[[str], None] = print):
    """
    Context manager rápido para medir un bloque:
        with timer("mi tarea"):
            do_work()
    """
    t0 = time.perf_counter()
    try:
        yield
    finally:
        dt = time.perf_counter() - t0
        logger(f"{label}: {_fmt(dt)}")


class Stopwatch:
    """
    Cronómetro por secciones con informe final.
        sw = Stopwatch()
        with sw.section("paso1"): ...
        with sw.section("paso2"): ...
        print(sw.report())
    """
    def __init__(self, logger: Callable[[str], None] = print) -> None:
        self._sections: List[Tuple[str, float]] = []
        self._logger = logger

    @contextmanager
    def section(self, name: str):
        t0 = time.perf_counter()
        try:
            yield
        finally:
            dt = time.perf_counter() - t0
            self._sections.append((name, dt))
            # logging inmediato por sección (opcional: comenta si no quieres ruido)
            self._logger(f"[tiempo] {name}: {_fmt(dt)}")

    def report(self, title: str = "Resumen de tiempos") -> str:
        total = sum(dt for _, dt in self._sections)
        lines = [f"--- {title} ---"]
        for name, dt in self._sections:
            lines.append(f"  {name:<22} {_fmt(dt)}")
        lines.append(f"  {'TOTAL':<22} {_fmt(total)}")
        return "\n".join(lines)
