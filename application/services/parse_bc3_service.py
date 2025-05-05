# application/services/parse_bc3_service.py
from pathlib import Path
from typing import Dict, Optional

from domain.models.budget import Budget
from domain.models.chapter import Chapter
from domain.models.subchapter import SubChapter
from domain.models.item import Item, Breakdown
from infrastructure.bc3.bc3_reader import read_bc3

class ParseBC3Service:
    """
    Transforma un .bc3 en el árbol de entidades de dominio.
    """
    def __init__(self, file_path: Path):
        self.file_path = file_path

    def execute(self) -> Budget:
        budget: Optional[Budget] = None
        current_chapter: Optional[Chapter] = None
        current_subchapter: Optional[SubChapter] = None
        current_item: Optional[Item] = None

        for section, payload in read_bc3(self.file_path):
            if section == "CAB":  # Cabecera de presupuesto
                code, description = payload.split(";", 1)
                budget = Budget(code=code, description=description)
            elif section == "CAP":  # Capítulo
                code, description = payload.split(";", 1)
                current_chapter = Chapter(code=code, description=description)
                budget.add_chapter(current_chapter)
            elif section == "SUB":  # Subcapítulo
                code, description = payload.split(";", 1)
                current_subchapter = SubChapter(code=code, description=description)
                current_chapter.add_subchapter(current_subchapter)
            elif section == "PAR":  # Partida
                parts = payload.split(";")
                code, description, unit, qty, price = parts[:5]
                current_item = Item(
                    code=code,
                    description=description,
                    unit=unit,
                    quantity=float(qty.replace(",", ".")),
                    price=float(price.replace(",", "."))
                )
                current_subchapter.add_item(current_item)
            elif section == "DEC":  # Descompuesto
                parts = payload.split(";")
                code, description, unit, qty, price, btype = parts[:6]
                breakdown = Breakdown(
                    code=code,
                    description=description,
                    unit=unit,
                    quantity=float(qty.replace(",", ".")),
                    price=float(price.replace(",", ".")),
                    breakdown_type=btype.strip()
                )
                current_item.add_breakdown(breakdown)

        return budget
