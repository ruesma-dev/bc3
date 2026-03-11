# infrastructure/filesystem/bc_refcru_package_writer.py
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Optional, Tuple
import re
import zipfile
import xml.etree.ElementTree as ET

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS = {"m": NS_MAIN}

ET.register_namespace("", NS_MAIN)

_REF_RE = re.compile(r"^(?P<col>[A-Z]+)(?P<row>\d+)$")
_RANGE_RE = re.compile(
    r"^(?P<start_col>[A-Z]+)(?P<start_row>\d+):(?P<end_col>[A-Z]+)(?P<end_row>\d+)$"
)


@dataclass(frozen=True)
class RefCruRow:
    """
    Una fila del paquete REFCRU:
      - new_code => N.º artículo (col A)
      - old_code => N.º referencia (col F)
    """

    new_code: str
    old_code: str


def make_refcru_row(
    *,
    new_code: str | None = None,
    old_code: str | None = None,
    item_no: str | None = None,
    reference_no: str | None = None,
    **_: Any,
) -> RefCruRow:
    """
    Factory retrocompatible para construir RefCruRow desde distintos nombres de campo.

    Soporta:
      - make_refcru_row(new_code="SB...", old_code="VCIM")
      - make_refcru_row(item_no="SB...", reference_no="VCIM")
    """
    nc = (new_code if new_code is not None else item_no) or ""
    oc = (old_code if old_code is not None else reference_no) or ""
    return RefCruRow(new_code=str(nc), old_code=str(oc))


def write_refcru_config_package_xlsx(
    template_xlsx: Path,
    output_xlsx: Path | None = None,
    rows: Iterable[RefCruRow] = (),
    *,
    out_xlsx: Path | None = None,
) -> Path:
    """
    Genera un Excel IMPORTABLE directamente por Business Central (Paquete Config),
    preservando la asignación XML (XML map) del template exportado por BC.

    Fix principal respecto a la versión anterior:
      - no recrea las filas desde cero de forma "mínima";
      - clona la fila plantilla del propio Excel y solo cambia A/F;
      - recalcula correctamente sharedStrings.count y uniqueCount;
      - conserva mejor estilos, estructura de tabla y mapeos XML.
    """
    template_xlsx = Path(template_xlsx)

    if output_xlsx is None and out_xlsx is not None:
        output_xlsx = out_xlsx
    if output_xlsx is None:
        raise TypeError(
            "write_refcru_config_package_xlsx: falta 'output_xlsx' (o alias 'out_xlsx')."
        )

    output_xlsx = Path(output_xlsx)
    row_list = list(rows)

    with zipfile.ZipFile(template_xlsx, "r") as zin:
        names = zin.namelist()
        files = {name: zin.read(name) for name in names}

    required = [
        "xl/xmlMaps.xml",
        "xl/sharedStrings.xml",
        "xl/worksheets/sheet1.xml",
        "xl/tables/table1.xml",
    ]
    missing = [p for p in required if p not in files]
    if missing:
        raise ValueError(
            f"Template inválido para BC Config Package (faltan partes {missing}). "
            "Usa el Excel exportado desde Business Central."
        )

    ss_root = ET.fromstring(files["xl/sharedStrings.xml"])
    sheet_root = ET.fromstring(files["xl/worksheets/sheet1.xml"])
    table_root = ET.fromstring(files["xl/tables/table1.xml"])

    def _si_text(si: ET.Element) -> str:
        return "".join(si.itertext())

    si_nodes = ss_root.findall("m:si", NS)
    ss_index = {_si_text(si): i for i, si in enumerate(si_nodes)}

    def _append_si(text: str) -> int:
        si = ET.Element(f"{{{NS_MAIN}}}si")
        t = ET.SubElement(si, f"{{{NS_MAIN}}}t")
        if text and (text[0].isspace() or text[-1].isspace() or "  " in text):
            t.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
        t.text = text
        ss_root.append(si)
        idx = len(ss_index)
        ss_index[text] = idx
        return idx

    def _get_ssi(text: str) -> int:
        value = str(text or "")
        if value in ss_index:
            return ss_index[value]
        return _append_si(value)

    table_ref = table_root.get("ref")
    if not table_ref:
        raise ValueError("Template inválido: table1.xml no contiene atributo 'ref'.")

    start_col, header_row, end_col, current_last_row = _parse_range_ref(table_ref)
    first_data_row = header_row + 1

    sheet_data = sheet_root.find("m:sheetData", NS)
    if sheet_data is None:
        raise ValueError("Template inválido: sheetData no encontrado en xl/worksheets/sheet1.xml")

    template_row = sheet_data.find(f"m:row[@r='{first_data_row}']", NS)
    template_row_attrib = dict(template_row.attrib) if template_row is not None else {}

    rows_to_remove: list[ET.Element] = []
    preserved_rows: list[ET.Element] = []
    for row in list(sheet_data):
        row_num = _get_row_number(row)
        if row_num is None:
            preserved_rows.append(row)
            continue

        if first_data_row <= row_num <= current_last_row:
            rows_to_remove.append(row)
        else:
            preserved_rows.append(row)

    for row in rows_to_remove:
        sheet_data.remove(row)

    new_rows: list[ET.Element] = []
    for offset, ref_row in enumerate(row_list, start=0):
        rn = first_data_row + offset
        new_row = _build_data_row(
            row_number=rn,
            new_code=ref_row.new_code,
            old_code=ref_row.old_code,
            shared_string_getter=_get_ssi,
            template_row=template_row,
            template_row_attrib=template_row_attrib,
            start_col=start_col,
            end_col=end_col,
        )
        new_rows.append(new_row)

    all_rows = preserved_rows + new_rows
    all_rows.sort(key=lambda r: _get_row_number(r) or 0)

    for row in list(sheet_data):
        sheet_data.remove(row)
    for row in all_rows:
        sheet_data.append(row)

    new_last_row = first_data_row + len(row_list) - 1 if row_list else header_row
    new_table_ref = f"{start_col}{header_row}:{end_col}{max(new_last_row, header_row)}"
    table_root.set("ref", new_table_ref)
    auto = table_root.find("m:autoFilter", NS)
    if auto is not None:
        auto.set("ref", new_table_ref)

    dim = sheet_root.find("m:dimension", NS)
    if dim is not None:
        max_existing_row = max((_get_row_number(r) or 0) for r in all_rows) if all_rows else header_row
        dim.set("ref", f"A1:{end_col}{max(max_existing_row, header_row)}")

    files["xl/sharedStrings.xml"] = _serialize_xml(ss_root)
    files["xl/worksheets/sheet1.xml"] = _serialize_xml(sheet_root)
    files["xl/tables/table1.xml"] = _serialize_xml(table_root)

    total_shared_string_refs = _count_shared_string_refs(files)
    total_unique_strings = len(ss_root.findall("m:si", NS))
    ss_root.set("count", str(total_shared_string_refs))
    ss_root.set("uniqueCount", str(total_unique_strings))
    files["xl/sharedStrings.xml"] = _serialize_xml(ss_root)

    output_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_xlsx, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name in names:
            zout.writestr(name, files[name])

    return output_xlsx


def _build_data_row(
    *,
    row_number: int,
    new_code: str,
    old_code: str,
    shared_string_getter,
    template_row: Optional[ET.Element],
    template_row_attrib: dict[str, str],
    start_col: str,
    end_col: str,
) -> ET.Element:
    if template_row is not None:
        row_el = deepcopy(template_row)
        row_el.set("r", str(row_number))
        if "spans" in template_row_attrib:
            row_el.set("spans", template_row_attrib["spans"])
    else:
        row_el = ET.Element(f"{{{NS_MAIN}}}row")
        row_el.set("r", str(row_number))
        row_el.set("spans", f"{_col_to_index(start_col)}:{_col_to_index(end_col)}")

    template_cells_by_col = _cells_by_col(template_row) if template_row is not None else {}
    row_cells_by_col = _cells_by_col(row_el)

    for col, cell in list(row_cells_by_col.items()):
        cell.set("r", f"{col}{row_number}")
        if col == "A":
            _set_cell_shared_string(cell, shared_string_getter(str(new_code or "")))
        elif col == "F":
            _set_cell_shared_string(cell, shared_string_getter(str(old_code or "")))
        else:
            _blank_cell(cell)

    for required_col, value in (("A", str(new_code or "")), ("F", str(old_code or ""))):
        if required_col not in row_cells_by_col:
            cell = _make_cell_from_template(
                template_cells_by_col=template_cells_by_col,
                col=required_col,
                row_number=row_number,
            )
            _set_cell_shared_string(cell, shared_string_getter(value))
            row_cells_by_col[required_col] = cell

    _rebuild_row_cell_order(row_el=row_el, cells_by_col=row_cells_by_col)
    return row_el


def _make_cell_from_template(
    *,
    template_cells_by_col: dict[str, ET.Element],
    col: str,
    row_number: int,
) -> ET.Element:
    source = template_cells_by_col.get(col)
    if source is None and template_cells_by_col:
        source = next(iter(template_cells_by_col.values()))

    if source is not None:
        cell = deepcopy(source)
    else:
        cell = ET.Element(f"{{{NS_MAIN}}}c")

    cell.set("r", f"{col}{row_number}")
    _blank_cell(cell)
    return cell


def _rebuild_row_cell_order(
    *,
    row_el: ET.Element,
    cells_by_col: dict[str, ET.Element],
) -> None:
    non_cells = [child for child in list(row_el) if _local_name(child.tag) != "c"]
    for child in list(row_el):
        row_el.remove(child)

    for child in non_cells:
        row_el.append(child)

    ordered_cols = sorted(cells_by_col.keys(), key=_col_to_index)
    for col in ordered_cols:
        row_el.append(cells_by_col[col])


def _cells_by_col(row_el: Optional[ET.Element]) -> dict[str, ET.Element]:
    result: dict[str, ET.Element] = {}
    if row_el is None:
        return result

    for cell in row_el.findall("m:c", NS):
        ref = cell.get("r", "")
        col, _row = _parse_cell_ref(ref)
        if col:
            result[col] = cell
    return result


def _set_cell_shared_string(cell: ET.Element, ssi: int) -> None:
    _blank_cell(cell)
    cell.set("t", "s")
    value_el = ET.SubElement(cell, f"{{{NS_MAIN}}}v")
    value_el.text = str(ssi)


def _blank_cell(cell: ET.Element) -> None:
    if "t" in cell.attrib:
        del cell.attrib["t"]

    for child in list(cell):
        if _local_name(child.tag) in {"v", "is", "f"}:
            cell.remove(child)


def _count_shared_string_refs(files: dict[str, bytes]) -> int:
    total = 0
    for name, content in files.items():
        if not name.startswith("xl/worksheets/") or not name.endswith(".xml"):
            continue
        try:
            root = ET.fromstring(content)
        except Exception:
            continue
        for cell in root.findall(".//m:c", NS):
            if cell.get("t") == "s":
                total += 1
    return total


def _serialize_xml(root: ET.Element) -> bytes:
    return ET.tostring(root, encoding="utf-8", xml_declaration=True)


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _get_row_number(row_el: ET.Element) -> Optional[int]:
    raw = row_el.get("r")
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _parse_cell_ref(ref: str) -> Tuple[Optional[str], Optional[int]]:
    match = _REF_RE.match(ref or "")
    if not match:
        return None, None
    return match.group("col"), int(match.group("row"))


def _parse_range_ref(ref: str) -> Tuple[str, int, str, int]:
    match = _RANGE_RE.match(ref or "")
    if not match:
        raise ValueError(f"Rango Excel inválido: {ref!r}")
    return (
        match.group("start_col"),
        int(match.group("start_row")),
        match.group("end_col"),
        int(match.group("end_row")),
    )


def _col_to_index(col: str) -> int:
    value = 0
    for ch in col:
        value = value * 26 + (ord(ch) - ord("A") + 1)
    return value