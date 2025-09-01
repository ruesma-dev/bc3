# infrastructure/product_catalog/product_catalog.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd
import yaml


@dataclass(frozen=True)
class Product:
    code: str
    name: str


class ProductCatalog:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._products: list[Product] = []

    def load(self) -> None:
        if not self.path.exists():
            raise FileNotFoundError(f"Catálogo no encontrado: {self.path}")

        suffix = self.path.suffix.lower()
        if suffix in {".yaml", ".yml"}:
            data = yaml.safe_load(self.path.read_text(encoding="utf-8")) or {}
            items = data.get("products", [])
            df = pd.DataFrame(items)
        elif suffix == ".csv":
            df = pd.read_csv(self.path, encoding="utf-8")
        elif suffix in {".xlsx", ".xls"}:
            df = pd.read_excel(self.path)
        else:
            raise ValueError(f"Extensión no soportada para catálogo: {suffix}")

        # Normalización de columnas mínimas: code, name
        # Si viniera “descripcion”, la mapeamos a name.
        cols = {c.lower(): c for c in df.columns}
        code_col = cols.get("code") or cols.get("codigo") or list(df.columns)[0]
        name_col = cols.get("name") or cols.get("descripcion") or list(df.columns)[1]

        df = df[[code_col, name_col]].copy()
        df.columns = ["code", "name"]
        df["code"] = df["code"].astype(str).str.strip()
        df["name"] = df["name"].astype(str).str.strip()
        df = df[(df["code"] != "") & (df["name"] != "")].drop_duplicates("code")

        self._products = [Product(code=row["code"], name=row["name"]) for _, row in df.iterrows()]

    @property
    def products(self) -> List[Product]:
        return self._products

    def to_prompt_list(self) -> str:
        # Lista compacta para el prompt
        return "\n".join(f'- code: "{p.code}" | name: "{p.name}"' for p in self._products)

    def by_code(self, code: str) -> Optional[Product]:
        return next((p for p in self._products if p.code == code), None)
