"""Minimal Schema model for MVP."""
from dataclasses import dataclass, asdict
from typing import List, Any, Dict


@dataclass
class Field:
    name: str
    type: str
    nullable: bool = False
    default: Any = None

    def to_dict(self) -> Dict[str, Any]:
        t = self.type
        if self.nullable:
            t = ["null", t]
        d = {"name": self.name, "type": t}
        if self.default is not None:
            d["default"] = self.default
        return d


@dataclass
class Schema:
    name: str
    fields: List[Field]

    def to_dict(self) -> Dict[str, Any]:
        return {"name": self.name, "type": "record", "fields": [f.to_dict() for f in self.fields]}

    def to_json(self) -> Dict[str, Any]:
        return self.to_dict()

