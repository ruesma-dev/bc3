# interface_adapters/controllers/etl_controller.py

from __future__ import annotations
from pathlib import Path

import pandas as pd
from bc3_lib.infra.reader import build_tree as lib_build_tree
from bc3_lib.app.flatten import nodes_to_rows

from infrastructure.bc3.bc3_modifier import convert_to_material
from application.services.missing_clones_service import (
    add_missing_clones,
    rewrite_bc3_with_clones,
)


def _print_tree(node, indent: int = 0) -> None:
    spacer = " " * indent
    print(
        f"{spacer}- [{node.kind.upper():12}] "
        f"{node.code:<15} "
        f"{(node.unidad or '').ljust(5)} "
        f"{node.description}"
    )
    for child in sorted(node.children, key=lambda n: n.code):
        _print_tree(child, indent + 4)


def run_etl(input_filename: str = "presupuesto.bc3") -> None:
    original = Path("input") / input_filename
    if not original.exists():
        raise FileNotFoundError(original)

    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    mod_file = output_dir / "presupuesto_material.bc3"

    # 0) Preproceso/limpieza (R1–R5)
    convert_to_material(original, mod_file)
    print(f"BC3 modificado  →  {mod_file.resolve()}")

    # 1) Árbol (librería)
    roots = lib_build_tree(mod_file)

    # 2) Pasada extra (clones .1) + reescritura BC3
    created = add_missing_clones(roots)
    if created:
        rewrite_bc3_with_clones(mod_file, roots)

    # 3) Impresión jerárquica (opcional)
    print("\n=== ÁRBOL DE CONCEPTOS ===")
    for root in roots:
        _print_tree(root)
    print("=== FIN DEL ÁRBOL ===\n")

    # 4) Export CSV desde el árbol ya clonado (sin reparsear)
    rows = nodes_to_rows(roots)
    df = pd.DataFrame.from_records(rows, columns=[
        "tipo",
        "codigo",
        "descripcion_corta",
        "descripcion_larga",
        "unidad",
        "precio",
        "cantidad_pres",
        "importe_pres",
        "hijos",
        "mediciones",
    ])
    csv_path = output_dir / "presupuesto_tree.csv"
    df.to_csv(csv_path, sep=";", index=False, encoding="utf-8")
    print(f"CSV generado    →  {csv_path.resolve()}\n")
