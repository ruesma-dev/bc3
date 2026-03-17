# domain/models/subchapter.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List


@dataclass
class SubChapter:
    code: str
    description: str
    items: List["Item"] = field(default_factory=list)

    def add_item(self, item: "Item") -> None:
        self.items.append(item)
