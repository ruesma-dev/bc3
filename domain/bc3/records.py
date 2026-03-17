# domain/bc3/records.py
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Dict, List, Optional

_TAIL_BACKSLASHES_RE = re.compile(r"(\\+)\|\s*$")


@dataclass
class BC3RecordBase:
    tag: str

    @classmethod
    def parse(cls, line: str) -> "BC3RecordBase":
        raise NotImplementedError

    def to_line(self) -> str:
        raise NotImplementedError


@dataclass
class ConceptRecord(BC3RecordBase):
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
        head, rest = line.split("|", 1)
        parts = rest.rstrip("\n").split("|")
        while len(parts) < 6:
            parts.append("")
        return cls(tag=head, fields=parts)

    def to_line(self) -> str:
        return f"{self.tag}|{'|'.join(self.fields)}|\n"

    def map_code(self, repl_map: Dict[str, str]) -> None:
        current = self.code
        if current in repl_map:
            self.code = repl_map[current]


@dataclass
class DescompLineaSimple:
    child_code: str
    coef: str
    qty: str


@dataclass
class DescomposicionRecord(BC3RecordBase):
    parent: str
    triplets: List[DescompLineaSimple] = field(default_factory=list)
    tail_backslashes: str = "\\"

    @classmethod
    def parse(cls, line: str) -> "DescomposicionRecord":
        if not line.startswith("~D|"):
            raise ValueError("DescomposicionRecord.parse: línea no empieza por '~D|'")

        stripped = line.rstrip("\n")
        match = _TAIL_BACKSLASHES_RE.search(stripped)
        tail_bslashes = match.group(1) if match else "\\"

        head, rest = line.split("|", 1)
        parent, child_part = rest.split("|", 1)

        body = child_part.rstrip("\n")
        if body.endswith("|"):
            body = body[:-1]

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
                DescompLineaSimple(
                    child_code=child,
                    coef=coef,
                    qty=qty,
                )
            )

        return cls(
            tag=head,
            parent=parent,
            triplets=triplets,
            tail_backslashes=tail_bslashes,
        )

    def to_line(self) -> str:
        new_chunks: List[str] = []
        for triplet in self.triplets:
            if not triplet.child_code:
                continue
            new_chunks.extend(
                [triplet.child_code, triplet.coef, triplet.qty]
            )

        body = "\\".join(new_chunks) + self.tail_backslashes
        return f"{self.tag}|{self.parent}|{body}|\n"

    def map_child_codes(self, repl_map: Dict[str, str]) -> None:
        for triplet in self.triplets:
            if triplet.child_code in repl_map:
                triplet.child_code = repl_map[triplet.child_code]


@dataclass
class MedicionesRecord(BC3RecordBase):
    raw_pair: str
    parent: Optional[str]
    child: Optional[str]
    tail: str

    @classmethod
    def parse(cls, line: str) -> "MedicionesRecord":
        if not line.startswith("~M|"):
            raise ValueError("MedicionesRecord.parse: línea no empieza por '~M|'")
        _tag, after = line.split("|", 1)
        pair, tail = after.split("|", 1)

        parent: Optional[str] = None
        child: Optional[str] = None
        if "\\" in pair:
            parent, child = pair.split("\\", 1)

        return cls(
            tag="~M",
            raw_pair=pair,
            parent=parent,
            child=child,
            tail=tail,
        )

    def to_line(self) -> str:
        if self.parent is not None and self.child is not None:
            pair = f"{self.parent}\\{self.child}"
        else:
            pair = self.raw_pair
        return f"{self.tag}|{pair}|{self.tail}"

    def map_child_codes(self, repl_map: Dict[str, str]) -> None:
        if self.child and self.child in repl_map:
            self.child = repl_map[self.child]


@dataclass
class TextoRecord(BC3RecordBase):
    code: str
    text: str

    @classmethod
    def parse(cls, line: str) -> "TextoRecord":
        if not line.startswith("~T|"):
            raise ValueError("TextoRecord.parse: línea no empieza por '~T|'")
        stripped = line.rstrip("\n")
        head, rest = stripped.split("|", 1)
        code, txt = rest.split("|", 1)
        if txt.endswith("|"):
            txt = txt[:-1]
        return cls(tag=head, code=code, text=txt)

    def to_line(self) -> str:
        return f"{self.tag}|{self.code}|{self.text}|\n"
