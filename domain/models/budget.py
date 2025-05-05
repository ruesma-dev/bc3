# domain/models/budget.py
from dataclasses import dataclass, field
from typing import List

@dataclass
class Budget:
    code: str
    description: str
    chapters: List["Chapter"] = field(default_factory=list)

    def add_chapter(self, chapter: "Chapter") -> None:
        self.chapters.append(chapter)
