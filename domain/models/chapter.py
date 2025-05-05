# domain/models/chapter.py
from dataclasses import dataclass, field
from typing import List

@dataclass
class Chapter:
    code: str
    description: str
    subchapters: List["SubChapter"] = field(default_factory=list)

    def add_subchapter(self, subchapter: "SubChapter") -> None:
        self.subchapters.append(subchapter)
