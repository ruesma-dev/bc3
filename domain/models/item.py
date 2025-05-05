# domain/models/item.py
from dataclasses import dataclass, field
from typing import List

@dataclass
class Breakdown:
    code: str
    description: str
    unit: str
    quantity: float
    price: float
    breakdown_type: str  # Mano de obra, Material, Maquinaria…

@dataclass
class Item:
    code: str
    description: str
    unit: str
    quantity: float
    price: float
    breakdowns: List[Breakdown] = field(default_factory=list)

    def add_breakdown(self, breakdown: Breakdown) -> None:
        self.breakdowns.append(breakdown)
