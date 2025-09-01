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

from config.settings import (
    CSV_DEFAULT_PATH, CSV_ENCODING, CSV_SEP,
    PRODUCTS_PATH,
    GEMINI_API_KEY, GEMINI_MODEL_NAME,
    GEMINI_RPM, GEMINI_MAX_RETRIES, GEMINI_ON_429,
    GEMINI_BATCH_MODE, GEMINI_BATCH_SIZE, PREFILTER_TOPK, GEMINI_MIN_CONFIDENCE,
)
from infrastructure.product_catalog.product_catalog import ProductCatalog
from infrastructure.llm.gemini_client import GeminiClient
from application.services.product_selection_service import (
    build_product_code_mapping,
    build_product_code_mapping_batch,
    apply_code_mapping_to_nodes,
    rewrite_bc3_with_product_codes,
)
from application.services.audit_service import export_product_matches_audit


def _print_tree(node, indent: int = 0) -> None:
    spacer = " " * indent
    print(
        f"{spacer}- [{node.kind.upper():12}] "
        f"{node.code:<20} "
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
    output_dir.mkdir(exist_ok=True, parents=True)
    mod_file = output_dir / "presupuesto_material.bc3"

    # 0) Preproceso
    convert_to_material(original, mod_file)
    print(f"BC3 modificado  →  {mod_file.resolve()}")

    # 1) Árbol
    roots = lib_build_tree(mod_file)

    # 2) Clones
    created = add_missing_clones(roots)
    if created:
        rewrite_bc3_with_clones(mod_file, roots)
        print(f"[Clones] Añadidos {len(created)} clones '.1'.")

    # 3) Selección de producto
    catalog = ProductCatalog(PRODUCTS_PATH)
    catalog.load()

    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY no configurada en .env")

    print(
        f"[Gemini] Modelo={GEMINI_MODEL_NAME} RPM={GEMINI_RPM} "
        f"batch_mode={GEMINI_BATCH_MODE} batch_size={GEMINI_BATCH_SIZE} topk={PREFILTER_TOPK}"
    )

    gemini = GeminiClient(
        api_key=GEMINI_API_KEY,
        model_name=GEMINI_MODEL_NAME,
        rpm=GEMINI_RPM,
        max_retries=GEMINI_MAX_RETRIES,
        on_429=GEMINI_ON_429,
    )

    if GEMINI_BATCH_MODE:
        code_map, matches = build_product_code_mapping_batch(
            roots=roots,
            catalog=catalog,
            gemini=gemini,
            batch_size=GEMINI_BATCH_SIZE,
            topk=PREFILTER_TOPK,
            min_confidence=GEMINI_MIN_CONFIDENCE,
        )
    else:
        code_map, matches = build_product_code_mapping(
            roots=roots,
            catalog=catalog,
            gemini=gemini,
            min_confidence=GEMINI_MIN_CONFIDENCE,
        )

    # ⬇️ 3.1 Auditoría JSON/CSV ANTES del renombrado de BC3/árbol
    audit_json = output_dir / "product_mapping_audit.json"
    audit_csv = output_dir / "product_mapping_audit.csv"
    export_product_matches_audit(roots, matches, audit_json, audit_csv)
    print(f"[Audit] JSON → {audit_json.resolve()}")
    print(f"[Audit] CSV  → {audit_csv.resolve()}")

    if code_map:
        # 4) Reescritura BC3 y sincronización del árbol
        rewrite_bc3_with_product_codes(mod_file, code_map)
        apply_code_mapping_to_nodes(roots, code_map)
        print(f"[Product Mapping] Renombrados {len(code_map)} descompuestos.")
    else:
        print("[Product Mapping] No se encontraron asignaciones de producto.")

    # 5) Árbol (opcional)
    print("\n=== ÁRBOL DE CONCEPTOS ===")
    for root in roots:
        _print_tree(root)
    print("=== FIN DEL ÁRBOL ===\n")

    # 6) Export CSV final
    rows = nodes_to_rows(roots)
    df = pd.DataFrame.from_records(
        rows,
        columns=[
            "tipo", "codigo", "descripcion_corta", "descripcion_larga",
            "unidad", "precio", "cantidad_pres", "importe_pres",
            "hijos", "mediciones",
        ],
    )
    csv_path = CSV_DEFAULT_PATH
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, sep=CSV_SEP, index=False, encoding=CSV_ENCODING)
    print(f"CSV generado    →  {csv_path.resolve()}\n")
