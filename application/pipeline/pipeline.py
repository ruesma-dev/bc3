# application/pipeline/pipeline.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Protocol

from application.services.build_tree_service import Node
from config.settings import Settings


@dataclass
class ETLContext:
    settings: Settings
    original_path: Optional[Path] = None
    modified_path: Optional[Path] = None
    roots: Optional[List[Node]] = None
    csv_path: Optional[Path] = None


class Step(Protocol):
    def run(self, ctx: ETLContext) -> None: ...


class Pipeline:
    def __init__(self, steps: List[Step] | None = None) -> None:
        self.steps = steps or []

    def add(self, step: Step) -> "Pipeline":
        self.steps.append(step)
        return self

    def run(self, ctx: ETLContext) -> ETLContext:
        for step in self.steps:
            step.run(ctx)
        return ctx
