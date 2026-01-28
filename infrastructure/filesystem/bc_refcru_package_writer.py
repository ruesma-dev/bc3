# infrastructure/filesystem/bc_refcru_package_writer.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Tuple, Any
import zipfile
import xml.etree.ElementTree as ET

NS_MAIN = "http://schemas.openxmlformats.org/spreadsheetml/2006/main"
NS = {"m": NS_MAIN}

# Para que ElementTree escriba el namespace principal sin prefijos raros
ET.register_namespace("", NS_MAIN)


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

    Nota:
      - Si se proporcionan ambos (new_code e item_no), tiene prioridad new_code.
      - Si se proporcionan ambos (old_code y reference_no), tiene prioridad old_code.
    """
    nc = (new_code if new_code is not None else item_no) or ""
    oc = (old_code if old_code is not None else reference_no) or ""
    return RefCruRow(new_code=str(nc), old_code=str(oc))


def write_refcru_config_package_xlsx(
    template_xlsx: Path,
    output_xlsx: Path | None = None,
    rows: Iterable[RefCruRow] = (),
    *,
    out_xlsx: Path | None = None,   # alias retrocompatible
) -> Path:
    """
    Genera un Excel IMPORTABLE directamente por Business Central (Paquete Config),
    preservando la asignación XML (XML map) del template exportado por BC.
    """
    template_xlsx = Path(template_xlsx)

    # aceptar alias out_xlsx
    if output_xlsx is None and out_xlsx is not None:
        output_xlsx = out_xlsx
    if output_xlsx is None:
        raise TypeError("write_refcru_config_package_xlsx: falta 'output_xlsx' (o alias 'out_xlsx').")

    output_xlsx = Path(output_xlsx)

    row_list = list(rows)

    # --- leer ZIP completo ---
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
            f"Usa el Excel exportado desde Business Central."
        )

    # --- sharedStrings: indexar y añadir strings nuevos ---
    ss_root = ET.fromstring(files["xl/sharedStrings.xml"])

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
        text = str(text or "")
        if text in ss_index:
            return ss_index[text]
        return _append_si(text)

    # --- sheet1.xml: borrar filas >=4 y recrearlas ---
    sheet_root = ET.fromstring(files["xl/worksheets/sheet1.xml"])
    sheet_data = sheet_root.find("m:sheetData", NS)
    if sheet_data is None:
        raise ValueError("Template inválido: sheetData no encontrado en xl/worksheets/sheet1.xml")

    template_row4 = sheet_data.find("m:row[@r='4']", NS)
    row4_attrib = dict(template_row4.attrib) if template_row4 is not None else {"spans": "1:6"}

    for row in list(sheet_data):
        r = row.attrib.get("r")
        if not r:
            continue
        try:
            rn = int(r)
        except ValueError:
            continue
        if rn >= 4:
            sheet_data.remove(row)

    start_row = 4
    for i, rr in enumerate(row_list):
        rn = start_row + i

        row_el = ET.Element(f"{{{NS_MAIN}}}row", {k: v for k, v in row4_attrib.items() if k != "r"})
        row_el.set("r", str(rn))

        cA = ET.SubElement(row_el, f"{{{NS_MAIN}}}c", {"r": f"A{rn}", "t": "s"})
        vA = ET.SubElement(cA, f"{{{NS_MAIN}}}v")
        vA.text = str(_get_ssi(rr.new_code))

        cF = ET.SubElement(row_el, f"{{{NS_MAIN}}}c", {"r": f"F{rn}", "t": "s"})
        vF = ET.SubElement(cF, f"{{{NS_MAIN}}}v")
        vF.text = str(_get_ssi(rr.old_code))

        sheet_data.append(row_el)

    last_row = 3 + len(row_list)

    dim = sheet_root.find("m:dimension", NS)
    if dim is not None:
        dim.set("ref", f"A1:F{last_row}")

    table_root = ET.fromstring(files["xl/tables/table1.xml"])
    table_root.set("ref", f"A3:F{last_row}")
    auto = table_root.find("m:autoFilter", NS)
    if auto is not None:
        auto.set("ref", f"A3:F{last_row}")

    total_si = len(ss_root.findall("m:si", NS))
    ss_root.set("uniqueCount", str(total_si))
    ss_root.set("count", str(total_si))

    files["xl/sharedStrings.xml"] = ET.tostring(ss_root, encoding="utf-8", xml_declaration=True)
    files["xl/worksheets/sheet1.xml"] = ET.tostring(sheet_root, encoding="utf-8", xml_declaration=True)
    files["xl/tables/table1.xml"] = ET.tostring(table_root, encoding="utf-8", xml_declaration=True)

    output_xlsx.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output_xlsx, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for name in names:
            zout.writestr(name, files[name])

    return output_xlsx
