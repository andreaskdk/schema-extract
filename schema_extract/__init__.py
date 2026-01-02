"""schema-extract minimal package exports for MVP.

Avoid importing CLI at package import time to prevent side-effects when the
package is imported in scripts or tests.
"""
from .infer import infer_from_tables
from .schema_model import Schema

__all__ = ["infer_from_tables", "Schema"]
