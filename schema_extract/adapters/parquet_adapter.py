"""Parquet adapter for MVP.

This adapter uses pyarrow.parquet if available. For the test suite we skip
parquet tests when pyarrow is not installed.

It yields a single chunk (list of dict rows) for simplicity.
"""
from typing import Iterator, List, Dict, Any

from .base import BaseAdapter


class ParquetAdapter(BaseAdapter):
    def __init__(self, source: str):
        super().__init__(source)
        # lazy import to avoid hard dependency at package import time
        try:
            import pyarrow.parquet as pq  # type: ignore
            import pyarrow as pa  # type: ignore
            self._pq = pq
            self._pa = pa
        except Exception:
            self._pq = None
            self._pa = None

    def iter_tables(self) -> Iterator[List[Dict[str, Any]]]:
        if self._pq is None or self._pa is None:
            raise ImportError("pyarrow is required for ParquetAdapter")
        # read the parquet file into a pyarrow Table
        table = self._pq.read_table(self.source)
        # Yield the pyarrow Table directly so the inference engine can use
        # the pyarrow schema without converting to Python objects.
        yield table
