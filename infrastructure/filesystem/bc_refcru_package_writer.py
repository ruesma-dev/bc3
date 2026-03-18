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
ET.register_namespace(
    "r",
    "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
)
ET.register_namespace(
    "xdr",
    "http://schemas.openxmlformats.org/drawingml/2006/spreadsheetDrawing",
)
ET.register_namespace(
    "x14",
    "http://schemas.microsoft.com/office/spreadsheetml/2009/9/main",
)
ET.register_namespace(
    "mc",
    "http://schemas.openxmlformats.org/markup-compatibility/2006",
)
ET.register_namespace(
    "x14ac",
    "http://schemas.microsoft.com/office/spreadsheetml/2009/9/ac",
)
ET.register_namespace(
    "xr",
    "http://schemas.microsoft.com/office/spreadsheetml/2014/revision",
)
ET.register_namespace(
    "xr2",
    "http://schemas.microsoft.com/office/spreadsheetml/2015/revision2",
)
ET.register_namespace(
    "xr3",
    "http://schemas.microsoft.com/office/spreadsheetml/2016/revision3",
)
ET.register_namespace(
    "xr6",
    "http://schemas.microsoft.com/office/spreadsheetml/2016/revision6",
)

_REF_RE = re.compile(r"^(?P<col>[A-Z]+)(?P<row>\d+)$")
_RANGE_RE = re.compile(
    r"^(?P<start_col>[A-Z]+)(?P<start_row>\d+):(?P<end_col>[A-Z]+)(?P<end_row>\d+)$"
)
_XMLNS_RE = re.compile(r'\s(xmlns(?::[A-Za-z_][\w.-]*)?)="([^"]*)"')
_XML_DECL_RE = re.compile(r"^\s*(<\?xml[^>]+\?>)\s*", re.DOTALL)


@dataclass(frozen=True)
class RefCruRow:
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
    template_xlsx = Path(template_xlsx)

    if output_xlsx is None and out_xlsx is not None:
        output_xlsx = out_xlsx
    if output_xlsx is None:
        raise TypeError(
            "write_refcru_config_package_xlsx: falta 'output_xlsx' "
            "(o alias 'out_xlsx')."
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
    missing = [path for path in required if path not in files]
    if missing:
        raise ValueError(
            f"Template inválido para BC Config Package (faltan partes {missing}). "
            "Usa el Excel exportado desde Business Central."
        )

    original_shared_strings = files["xl/sharedStrings.xml"]
    original_sheet_xml = files["xl/worksheets/sheet1.xml"]
    original_table_xml = files["xl/tables/table1.xml"]

    ss_root = ET.fromstring(original_shared_strings)
    sheet_root = ET.fromstring(original_sheet_xml)
    table_root = ET.fromstring(original_table_xml)

    def _si_text(si: ET.Element) -> str:
        return "".join(si.itertext())

    si_nodes = ss_root.findall("m:si", NS)
    ss_index = {_si_text(si): i for i, si in enumerate(si_nodes)}

    def _append_si(text: str) -> int:
        si = ET.Element(f"{{{NS_MAIN}}}si")
        t = ET.SubElement(si, f"{{{NS_MAIN}}}t")
        if text and (
            text[0].isspace() or text[-1].isspace() or "  " in text
        ):
            t.set(
                "{http://www.w3.org/XML/1998/namespace}space",
                "preserve",
            )
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
        raise ValueError(
            "Template inválido: table1.xml no contiene atributo 'ref'."
        )

    start_col, header_row, end_col, current_last_row = _parse_range_ref(table_ref)
    first_data_row = header_row + 1

    sheet_data = sheet_root.find("m:sheetData", NS)
    if sheet_data is None:
        raise ValueError(
            "Template inválido: sheetData no encontrado en "
            "xl/worksheets/sheet1.xml"
        )

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
        row_number = first_data_row + offset
        new_row = _build_data_row(
            row_number=row_number,
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
    all_rows.sort(key=lambda row: _get_row_number(row) or 0)

    for row in list(sheet_data):
        sheet_data.remove(row)
    for row in all_rows:
        sheet_data.append(row)

    new_last_row = first_data_row + len(row_list) - 1 if row_list else header_row
    new_table_ref = f"{start_col}{header_row}:{end_col}{max(new_last_row, header_row)}"
    table_root.set("ref", new_table_ref)

    auto_filter = table_root.find("m:autoFilter", NS)
    if auto_filter is not None:
        auto_filter.set("ref", new_table_ref)

    dimension = sheet_root.find("m:dimension", NS)
    if dimension is not None:
        max_existing_row = (
            max((_get_row_number(row) or 0) for row in all_rows)
            if all_rows
            else header_row
        )
        dimension.set(
            "ref",
            f"A1:{end_col}{max(max_existing_row, header_row)}",
        )

    selection = sheet_root.find("./m:sheetViews/m:sheetView/m:selection", NS)
    if selection is not None:
        if row_list:
            selection_ref = (
                f"{start_col}{first_data_row}:{end_col}"
                f"{max(new_last_row, first_data_row)}"
            )
            selection.set("activeCell", f"{start_col}{first_data_row}")
            selection.set("sqref", selection_ref)
        else:
            selection.set("activeCell", f"{start_col}{header_row}")
            selection.set("sqref", f"{start_col}{header_row}")

    files["xl/sharedStrings.xml"] = _serialize_xml(
        ss_root,
        original_shared_strings,
    )
    files["xl/worksheets/sheet1.xml"] = _serialize_sheet_xml_preserving_root(
        original_sheet_xml,
        sheet_root,
    )
    files["xl/tables/table1.xml"] = _serialize_xml(
        table_root,
        original_table_xml,
    )

    total_shared_string_refs = _count_shared_string_refs(files)
    total_unique_strings = len(ss_root.findall("m:si", NS))
    ss_root.set("count", str(total_shared_string_refs))
    ss_root.set("uniqueCount", str(total_unique_strings))
    files["xl/sharedStrings.xml"] = _serialize_xml(
        ss_root,
        original_shared_strings,
    )

    output_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(
        output_xlsx,
        "w",
        compression=zipfile.ZIP_DEFLATED,
    ) as zout:
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
        row_el.set(
            "spans",
            f"{_col_to_index(start_col)}:{_col_to_index(end_col)}",
        )

    template_cells_by_col = _cells_by_col(template_row) if template_row is not None else {}
    row_cells_by_col = _cells_by_col(row_el)

    for col in _iter_cols(start_col, end_col):
        if col not in row_cells_by_col:
            row_cells_by_col[col] = _make_cell_from_template(
                template_cells_by_col=template_cells_by_col,
                col=col,
                row_number=row_number,
            )

    for col, cell in list(row_cells_by_col.items()):
        cell.set("r", f"{col}{row_number}")
        if col == "A":
            _set_cell_shared_string(
                cell,
                shared_string_getter(str(new_code or "")),
            )
        elif col == "F":
            _set_cell_shared_string(
                cell,
                shared_string_getter(str(old_code or "")),
            )
        else:
            _blank_cell(cell)

    _rebuild_row_cell_order(
        row_el=row_el,
        cells_by_col=row_cells_by_col,
    )
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
    non_cells = [
        child for child in list(row_el)
        if _local_name(child.tag) != "c"
    ]
    for child in list(row_el):
        row_el.remove(child)

    for col in sorted(cells_by_col.keys(), key=_col_to_index):
        row_el.append(cells_by_col[col])

    for child in non_cells:
        row_el.append(child)


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


def _serialize_xml(root: ET.Element, original_xml: bytes) -> bytes:
    serialized = ET.tostring(
        root,
        encoding="utf-8",
        xml_declaration=True,
    )
    return _inject_missing_root_namespaces(serialized, original_xml)


def _serialize_sheet_xml_preserving_root(
    original_xml: bytes,
    root: ET.Element,
) -> bytes:
    """
    Serializa `sheet1.xml` preservando EXACTAMENTE la declaración XML y el
    start-tag original del worksheet del template.

    Esto evita que Excel repare la hoja cuando el template exportado por
    Business Central es sensible al encabezado/namespaces del nodo raíz.
    """
    serialized = ET.tostring(
        root,
        encoding="utf-8",
        xml_declaration=True,
    )

    try:
        original_text = original_xml.decode("utf-8")
        serialized_text = serialized.decode("utf-8")
    except Exception:
        return _serialize_xml(root, original_xml)

    original_decl = _extract_xml_declaration(original_text)
    if not original_decl:
        original_decl = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'

    original_start = _extract_root_start_tag(original_text)
    serialized_start = _extract_root_start_tag(serialized_text)

    if not original_start or not serialized_start:
        return _serialize_xml(root, original_xml)

    serialized_body = _strip_xml_declaration(serialized_text).lstrip()
    if not serialized_body.startswith(serialized_start):
        return _serialize_xml(root, original_xml)

    patched_body = original_start + serialized_body[len(serialized_start):]
    return (original_decl + "\n" + patched_body).encode("utf-8")


def _inject_missing_root_namespaces(
    serialized_xml: bytes,
    original_xml: bytes,
) -> bytes:
    try:
        original_text = original_xml.decode("utf-8")
        serialized_text = serialized_xml.decode("utf-8")
    except Exception:
        return serialized_xml

    original_start = _extract_root_start_tag(original_text)
    serialized_start = _extract_root_start_tag(serialized_text)
    if not original_start or not serialized_start:
        return serialized_xml

    original_ns = _XMLNS_RE.findall(original_start)
    if not original_ns:
        return serialized_xml

    serialized_decl_names = {
        name for name, _uri in _XMLNS_RE.findall(serialized_start)
    }
    missing_decls = [
        f'{name}="{uri}"'
        for name, uri in original_ns
        if name not in serialized_decl_names
    ]
    if not missing_decls:
        return serialized_xml

    insert_at = serialized_text.find(
        ">",
        serialized_text.find("<", serialized_text.find("?>") + 2),
    )
    if insert_at < 0:
        return serialized_xml

    patched = (
        serialized_text[:insert_at]
        + " "
        + " ".join(missing_decls)
        + serialized_text[insert_at:]
    )
    return patched.encode("utf-8")


def _extract_xml_declaration(xml_text: str) -> str:
    match = _XML_DECL_RE.match(xml_text)
    if not match:
        return ""
    return match.group(1)


def _strip_xml_declaration(xml_text: str) -> str:
    return _XML_DECL_RE.sub("", xml_text, count=1)


def _extract_root_start_tag(xml_text: str) -> str:
    body = _strip_xml_declaration(xml_text).lstrip()
    lt = body.find("<")
    gt = body.find(">", lt + 1) if lt >= 0 else -1
    if lt < 0 or gt < 0:
        return ""
    return body[lt:gt + 1]


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


def _index_to_col(index: int) -> str:
    if index <= 0:
        raise ValueError("index debe ser >= 1")
    out = ""
    n = index
    while n > 0:
        n, rem = divmod(n - 1, 26)
        out = chr(65 + rem) + out
    return out


def _iter_cols(start_col: str, end_col: str):
    start = _col_to_index(start_col)
    end = _col_to_index(end_col)
    for idx in range(start, end + 1):
        yield _index_to_col(idx)