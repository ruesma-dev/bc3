# application/pipeline/steps.py
from __future__ import annotations

from dataclasses import dataclass

from application.pipeline.pipeline import ETLContext, Step
from application.services.build_tree_service import build_tree
from application.services.export_csv_service import export_to_csv
from infrastructure.bc3.bc3_modifier import convert_to_material


@dataclass
class ResolveInputStep(Step):
    def run(self, ctx: ETLContext) -> None:
        file_path = ctx.settings.input_dir / ctx.settings.input_filename
        if not file_path.exists():
            raise FileNotFoundError(file_path)
        ctx.original_path = file_path


@dataclass
class TransformBC3Step(Step):
    def run(self, ctx: ETLContext) -> None:
        assert ctx.original_path is not None
        out_dir = ctx.settings.output_dir
        out_dir.mkdir(parents=True, exist_ok=True)
        mod_file = out_dir / "presupuesto_material.bc3"

        try:
            convert_to_material(
                src=ctx.original_path,
                dst=mod_file,
                max_code_len=ctx.settings.max_code_len,
                fill_unit_ud=ctx.settings.fill_unit_ud,
                force_material=ctx.settings.force_material,
                encoding=ctx.settings.encoding,
            )
        except TypeError:
            convert_to_material(ctx.original_path, mod_file)

        ctx.modified_path = mod_file
        print(f"BC3 modificado  →  {mod_file.resolve()}")


@dataclass
class BuildTreeStep(Step):
    def run(self, ctx: ETLContext) -> None:
        assert ctx.modified_path is not None
        try:
            roots = build_tree(
                ctx.modified_path,
                create_clones=ctx.settings.create_clones,
                rewrite_bc3=ctx.settings.rewrite_bc3,
                encoding=ctx.settings.encoding,
            )
        except TypeError:
            roots = build_tree(ctx.modified_path)
        ctx.roots = roots


@dataclass
class PrintTreeStep(Step):
    def run(self, ctx: ETLContext) -> None:
        assert ctx.roots is not None
        print("\n=== ÁRBOL DE CONCEPTOS ===")
        for root in ctx.roots:
            self._print_tree(root)
        print("=== FIN DEL ÁRBOL ===\n")

    def _print_tree(self, node, indent: int = 0) -> None:
        spacer = " " * indent
        print(
            f"{spacer}- [{node.kind.upper():12}] "
            f"{node.code:<15} "
            f"{(node.unidad or '').ljust(5)} "
            f"{node.description}"
        )
        for child in sorted(node.children, key=lambda n: n.code):
            self._print_tree(child, indent + 4)


@dataclass
class ExportCsvStep(Step):
    def run(self, ctx: ETLContext) -> None:
        assert ctx.roots is not None
        csv_path = ctx.settings.output_dir / ctx.settings.csv_filename
        try:
            export_to_csv(ctx.roots, csv_path, sep=ctx.settings.csv_sep)
        except TypeError:
            export_to_csv(ctx.roots, csv_path)
        ctx.csv_path = csv_path
        print(f"CSV generado    →  {csv_path.resolve()}\n")
