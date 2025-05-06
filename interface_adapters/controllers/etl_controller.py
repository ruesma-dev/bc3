# interface_adapters/controllers/etl_controller.py
"""
Controlador principal:
  0) Crea una copia del BC3 en output/ con:
        • Todos los ... DESC  → Tipo Material (T=1)
        • Todos los conceptos con T=4 (Otros) → T=1
  1) Construye el árbol lógico sobre la COPIA modificada
  2) Muestra el árbol por consola
  3) Exporta a CSV (output/presupuesto_tree.csv)
"""

from pathlib import Path

from application.services.build_tree_service import build_tree, Node
from application.services.export_csv_service import export_to_csv
from infrastructure.bc3.bc3_modifier import convert_to_material


# ---------------- helpers de impresión --------------------------------------
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


# ---------------- función pública -------------------------------------------
def run_etl(input_filename: str = "presupuesto.bc3") -> None:
    original = Path("input") / input_filename
    if not original.exists():
        raise FileNotFoundError(original)

    # 0) Copia modificada en output/
    output_dir = Path("output")
    output_dir.mkdir(exist_ok=True)
    mod_file = output_dir / "presupuesto_material.bc3"
    convert_to_material(original, mod_file)
    print(f"BC3 modificado  →  {mod_file.resolve()}")

    # 1) Parseo del BC3 ya corregido
    roots = build_tree(mod_file)

    # 2) Impresión jerárquica
    print("\n=== ÁRBOL DE CONCEPTOS ===")
    for root in roots:
        _print_tree(root)
    print("=== FIN DEL ÁRBOL ===\n")

    # 3) Exportar a CSV
    csv_path = output_dir / "presupuesto_tree.csv"
    export_to_csv(roots, csv_path)
    print(f"CSV generado    →  {csv_path.resolve()}\n")
