"""A simple CSV adapter that yields chunks of rows (as dicts).

This intentionally uses only the Python stdlib and pandas if available. For MVP
we'll attempt a lightweight implementation using the csv module and streaming.
"""
from typing import Iterator, List
import csv

from .base import BaseAdapter


class CSVAdapter(BaseAdapter):
    def __init__(self, source: str, sample_size: int = 1000, encoding: str = "utf-8"):
        super().__init__(source)
        self.sample_size = sample_size
        self.encoding = encoding

    def iter_tables(self) -> Iterator[List[dict]]:
        """Yield a single chunk as a list of dicts containing up to sample_size rows.

        This MVP reads up to sample_size rows for inference.
        """
        rows = []
        with open(self.source, "r", encoding=self.encoding, errors="replace") as f:
            reader = csv.DictReader(f)
            for i, r in enumerate(reader):
                rows.append(r)
                if i + 1 >= self.sample_size:
                    break
        yield rows

