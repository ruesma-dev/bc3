# utils/timer.py
from __future__ import annotations

import time
from dataclasses import dataclass, field


@dataclass
class Stopwatch:
    started_at: float = field(default_factory=time.perf_counter)

    def report(self, title: str = "Tiempos") -> str:
        elapsed = time.perf_counter() - self.started_at
        return f"{title}: {elapsed:.3f}s"
