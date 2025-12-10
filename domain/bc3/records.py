# domain/bc3/records.py
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Dict, Optional
import re


# Regex para capturar las barras invertidas justo antes del '|' final en ~D
_TAIL_BACKSLASHES_RE = re.compile(r"(\\+)\|\s*$")


@dataclass
class BC3RecordBase:
    """
    Base mínima para registros BC3 tipados.
    Solo define la interfaz común parse() / to_line().
    """
    tag: str

    @classmethod
    def parse(cls, line: str) -> "BC3RecordBase":  # pragma: no cover - interfaz
        raise NotImplementedError

    def to_line(self) -> str:  # pragma: no cover - interfaz
        raise NotImplementedError


# --------------------------------------------------------------------------- #
#  ~C  ConceptRecord                                                          #
# --------------------------------------------------------------------------- #
@dataclass
class ConceptRecord(BC3RecordBase):
    """
    Representa un registro ~C|...|
    Mantiene todos los campos tal cual en 'fields', sin truncar.
    """
    fields: List[str] = field(default_factory=list)

    @property
    def code(self) -> str:
        return self.fields[0] if self.fields else ""

    @code.setter
    def code(self, value: str) -> None:
        if not self.fields:
            self.fields = [value]
        else:
            self.fields[0] = value

    @classmethod
    def parse(cls, line: str) -> "ConceptRecord":
        if not line.startswith("~C|"):
            raise ValueError("ConceptRecord.parse: línea no empieza por '~C|'")
        # Imitamos exactamente la lógica anterior:
        # head, rest = raw.split("|", 1)
        head, rest = line.split("|", 1)
        parts = rest.rstrip("\n").split("|")
        # En rewrite_bc3_with_codes se garantizaban al menos 6 campos
        while len(parts) < 6:
            parts.append("")
        return cls(tag=head, fields=parts)

    def to_line(self) -> str:
        # Misma reconstrucción que antes: '~C|' + join(fields) + '|\\n'
        return f"{self.tag}|{'|'.join(self.fields)}|\n"

    def map_code(self, repl_map: Dict[str, str]) -> None:
        """
        Si el código está en repl_map, lo sustituye.
        """
        current = self.code
        if current in repl_map:
            self.code = repl_map[current]


# --------------------------------------------------------------------------- #
#  ~D  DescomposicionRecord                                                   #
# --------------------------------------------------------------------------- #
@dataclass
class DescompLineaSimple:
    """
    Triplete simple de descomposición:
      child_code \ coef \ qty
    (equivalente al manejo actual en rewrite_bc3_with_codes).
    """
    child_code: str
    coef: str
    qty: str


@dataclass
class DescomposicionRecord(BC3RecordBase):
    """
    Representa un registro ~D, en el mismo modelo simplificado que usa
    actualmente el código:
      ~D|PADRE|child\coef\qty\child\coef\qty...<barras_finales>|
    No distingue aún entre campo 2 y campo 3 (eso lo haremos más adelante).
    """
    parent: str
    triplets: List[DescompLineaSimple] = field(default_factory=list)
    tail_backslashes: str = "\\"  # barras invertidas antes del '|' final

    @classmethod
    def parse(cls, line: str) -> "DescomposicionRecord":
        if not line.startswith("~D|"):
            raise ValueError("DescomposicionRecord.parse: línea no empieza por '~D|'")

        # 1) Capturamos nº de '\' antes del '|' final (igual que antes)
        stripped = line.rstrip("\n")
        m = _TAIL_BACKSLASHES_RE.search(stripped)
        tail_bslashes = m.group(1) if m else "\\"

        # 2) head, rest = raw.split("|", 1)
        head, rest = line.split("|", 1)
        # parent, child_part = rest.split("|", 1)
        parent, child_part = rest.split("|", 1)

        # 3) body = child_part sin '\n'
        body = child_part.rstrip("\n")
        # Si termina en '|', lo quitamos (mismo patrón que antes)
        if body.endswith("|"):
            body = body[:-1]

        # 4) Descomponemos en chunks separados por '\'
        chunks = body.split("\\") if body else []
        triplets: List[DescompLineaSimple] = []

        i = 0
        while i < len(chunks):
            child = chunks[i] if i < len(chunks) else ""
            coef = chunks[i + 1] if i + 1 < len(chunks) else ""
            qty = chunks[i + 2] if i + 2 < len(chunks) else ""
            i += 3
            if not child:
                continue
            triplets.append(
                DescompLineaSimple(child_code=child, coef=coef, qty=qty)
            )

        return cls(tag=head, parent=parent, triplets=triplets, tail_backslashes=tail_bslashes)

    def to_line(self) -> str:
        """
        Reconstruye la línea con la misma estructura que el código actual:
          ~D|PADRE|child\coef\qty\...<tail_backslashes>|
        """
        new_chunks: List[str] = []
        for t in self.triplets:
            if not t.child_code:
                continue
            new_chunks.extend([t.child_code, t.coef, t.qty])

        body = "\\".join(new_chunks) + self.tail_backslashes
        return f"{self.tag}|{self.parent}|{body}|\n"

    def map_child_codes(self, repl_map: Dict[str, str]) -> None:
        """
        Aplica repl_map sobre cada child_code, replicando la lógica actual:
        solo cambia el código hijo, mantiene coef y qty.
        """
        for t in self.triplets:
            if t.child_code in repl_map:
                t.child_code = repl_map[t.child_code]


# --------------------------------------------------------------------------- #
#  ~M  MedicionesRecord                                                       #
# --------------------------------------------------------------------------- #
@dataclass
class MedicionesRecord(BC3RecordBase):
    """
    Representa un registro ~M|<parent>\<child>|<tail>
    Solo nos interesa poder cambiar el 'child' cuando exista.
    'tail' incluye el resto de la línea (campos y salto de línea).
    """
    raw_pair: str
    parent: Optional[str]
    child: Optional[str]
    tail: str

    @classmethod
    def parse(cls, line: str) -> "MedicionesRecord":
        if not line.startswith("~M|"):
            raise ValueError("MedicionesRecord.parse: línea no empieza por '~M|'")
        # _tag, after = raw.split("|", 1)
        _tag, after = line.split("|", 1)
        # pair, tail = after.split("|", 1)
        pair, tail = after.split("|", 1)

        parent: Optional[str] = None
        child: Optional[str] = None
        if "\\" in pair:
            parent, child = pair.split("\\", 1)

        return cls(tag="~M", raw_pair=pair, parent=parent, child=child, tail=tail)

    def to_line(self) -> str:
        """
        Reconstruye la línea en el mismo formato que antes:
          ~M|<pair>|<tail>
        donde <tail> ya contiene el salto de línea original.
        """
        if self.parent is not None and self.child is not None:
            pair = f"{self.parent}\\{self.child}"
        else:
            pair = self.raw_pair
        return f"{self.tag}|{pair}|{self.tail}"

    def map_child_codes(self, repl_map: Dict[str, str]) -> None:
        """
        Si hay child y está en repl_map, lo sustituye.
        """
        if self.child and self.child in repl_map:
            self.child = repl_map[self.child]


# --------------------------------------------------------------------------- #
#  ~T  TextoRecord (de momento solo lectura / helper)                         #
# --------------------------------------------------------------------------- #
@dataclass
class TextoRecord(BC3RecordBase):
    """
    Representa un registro ~T|CODIGO|TEXTO|
    Ahora mismo no se usa para reescritura, pero lo dejamos preparado.
    """
    code: str
    text: str

    @classmethod
    def parse(cls, line: str) -> "TextoRecord":
        if not line.startswith("~T|"):
            raise ValueError("TextoRecord.parse: línea no empieza por '~T|'")
        stripped = line.rstrip("\n")
        head, rest = stripped.split("|", 1)
        code, txt = rest.split("|", 1)
        # Si el texto termina en '|', lo quitamos (forma más limpia)
        if txt.endswith("|"):
            txt = txt[:-1]
        return cls(tag=head, code=code, text=txt)

    def to_line(self) -> str:
        return f"{self.tag}|{self.code}|{self.text}|\n"
