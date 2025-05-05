# interface_adapters/controllers/etl_controller.py
"""
Controlador principal:
  1) Construye el árbol lógico del BC3
  2) Muestra el árbol por consola
  3) Exporta a CSV (output/presupuesto_tree.csv)
"""

from pathlib import Path

from application.services.build_tree_service import build_tree, Node
from application.services.export_csv_service import export_to_csv


# ---------------------- helpers de impresión ---------------------------------
def _print_tree(node: Node, indent: int = 0) -> None:
    spacer = " " * indent
    print(
        f"{spacer}- [{node.kind.upper():12}] "
        f"{node.code:<15} "
        f"{(node.unidad or '').ljust(5)} "
        f"{node.description}"
    )
    for child in sorted(node.children, key=lambda n: n.code):
        _print_tree(child, indent + 4)


# ---------------------- función pública --------------------------------------
def run_etl(input_filename: str = "presupuesto.bc3") -> None:
    file_path = Path("input") / input_filename
    if not file_path.exists():
        raise FileNotFoundError(file_path)

    # 1) Parseo
    roots = build_tree(file_path)

    # 2) Impresión jerárquica
    print("\n=== ÁRBOL DE CONCEPTOS ===")
    for root in roots:
        _print_tree(root)
    print("=== FIN DEL ÁRBOL ===\n")

    # 3) CSV
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    csv_path = output_dir / "presupuesto_tree.csv"
    export_to_csv(roots, csv_path)

    print(f"CSV generado → {csv_path.resolve()}\n")
