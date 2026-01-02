"""JSONL adapter for MVP: streams lines and yields a sample chunk of parsed JSON objects."""
from typing import Iterator, List, Dict, Any
import json

from .base import BaseAdapter


class JSONLAdapter(BaseAdapter):
    def __init__(self, source: str, sample_size: int = 1000, encoding: str = "utf-8"):
        super().__init__(source)
        self.sample_size = sample_size
        self.encoding = encoding

    def iter_tables(self) -> Iterator[List[Dict[str, Any]]]:
        rows: List[Dict[str, Any]] = []
        with open(self.source, "r", encoding=self.encoding, errors="replace") as f:
            for i, line in enumerate(f):
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except json.JSONDecodeError:
                    # skip bad lines for MVP
                    continue
                if isinstance(obj, dict):
                    rows.append(obj)
                else:
                    # non-object top-level; store under a synthetic field
                    rows.append({"_value": obj})
                if i + 1 >= self.sample_size:
                    break
        yield rows

