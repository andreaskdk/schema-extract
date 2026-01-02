"""Base adapter interfaces."""
from typing import Iterator


class BaseAdapter:
    """Minimal adapter interface: yields chunks as list-of-dicts or simple tables.

    For MVP we'll use list-of-dicts rows and let infer convert them.
    """

    def __init__(self, source: str):
        self.source = source

    def iter_tables(self) -> Iterator[list]:
        """Yield iterable chunks (list of row dicts)."""
        raise NotImplementedError

