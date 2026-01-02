"""Simple inference engine for MVP.

Takes iterable chunks (list-of-dict rows) and produces a minimal Schema object.
"""
from typing import Iterable, List, Dict, Any

from .schema_model import Schema, Field

# try to import pyarrow (optional)
try:
    import pyarrow as pa  # type: ignore
    PA_AVAILABLE = True
except Exception:
    pa = None  # type: ignore
    PA_AVAILABLE = False


def _infer_value_type(val: Any) -> str:
    """Return a simple type name for a single value."""
    if val is None:
        return "null"
    if isinstance(val, bool):
        return "boolean"
    # numbers
    try:
        # prefer int when possible
        if isinstance(val, int):
            return "int64"
        # try parse string numbers
        if isinstance(val, str):
            s = val.strip()
            if s == "":
                return "null"
            # int?
            if s.lstrip("+-").isdigit():
                return "int64"
            # float?
            try:
                float(s)
                return "float64"
            except Exception:
                pass
            # otherwise string
            return "string"
    except Exception:
        pass
    return "string"


def _merge_type(a: str, b: str) -> str:
    """Merge two primitive types with simple precedence."""
    precedence = ["null", "boolean", "int64", "float64", "string"]
    if a == b:
        return a
    for t in precedence[::-1]:
        if a == t or b == t:
            # pick the "larger" type (string > float > int > boolean > null)
            # simpler: if either is string -> string; elif either is float -> float; elif either is int -> int; else boolean/null
            break
    # explicit rules:
    if a == "string" or b == "string":
        return "string"
    if a == "float64" or b == "float64":
        return "float64"
    if a == "int64" or b == "int64":
        return "int64"
    if a == "boolean" or b == "boolean":
        return "boolean"
    return "string"


def _pa_type_to_simple(pa_type) -> str:
    """Map a pyarrow DataType to our simple string type names.

    This intentionally keeps mappings conservative for the MVP.
    """
    # guard if pyarrow isn't available
    if not PA_AVAILABLE:
        return "string"

    try:
        import pyarrow.types as pat
    except Exception:
        return "string"

    if pat.is_null(pa_type):
        return "null"
    if pat.is_boolean(pa_type):
        return "boolean"
    if pat.is_integer(pa_type):
        return "int64"
    if pat.is_floating(pa_type):
        return "float64"
    if pat.is_decimal(pa_type):
        return "float64"
    if pat.is_string(pa_type) or pat.is_large_string(pa_type) or pat.is_binary(pa_type) or pat.is_large_binary(pa_type):
        return "string"
    if pat.is_timestamp(pa_type) or pat.is_date(pa_type) or pat.is_time(pa_type) or pat.is_duration(pa_type):
        # represent temporal types as strings for MVP
        return "string"
    # lists, structs, maps, and other complex types -> string fallback
    return "string"


def _infer_from_pyarrow_schema(pa_schema) -> Schema:
    """Build a Schema from a pyarrow.Schema without materializing rows."""
    fields = []
    for f in pa_schema:
        t = _pa_type_to_simple(f.type)
        nullable = getattr(f, "nullable", False)
        fields.append(Field(name=f.name, type=t, nullable=nullable))
    return Schema(name="inferred", fields=fields)


def infer_from_tables(tables: Iterable[List[Dict[str, Any]]]) -> Schema:
    """Infer a schema from iterable chunks (list-of-dict rows).

    Strategy: scan rows, collect per-column observed types and nullability.
    """
    # First, check whether any chunk is a pyarrow Table/RecordBatch and use
    # the pyarrow schema directly if available. This avoids converting large
    # columns back into Python objects and preserves type information.
    if PA_AVAILABLE:
        for chunk in tables:
            # pyarrow Table or RecordBatch
            if isinstance(chunk, getattr(pa, "Table", object)) or isinstance(chunk, getattr(pa, "RecordBatch", object)):
                return _infer_from_pyarrow_schema(chunk.schema)

    # Fallback: previous row-wise inference (list-of-dicts)
    col_types = {}  # name -> type str
    col_nullable = {}  # name -> bool

    for chunk in tables:
        # skip None/empty chunks
        if not chunk:
            continue
        for row in chunk:
            for k, v in row.items():
                t = _infer_value_type(v)
                if k not in col_types:
                    col_types[k] = t
                    col_nullable[k] = (t == "null")
                else:
                    merged = _merge_type(col_types[k], t)
                    col_types[k] = merged
                    if t == "null":
                        col_nullable[k] = True

    fields = []
    for name, t in col_types.items():
        nullable = col_nullable.get(name, False)
        f = Field(name=name, type=t, nullable=nullable)
        fields.append(f)

    return Schema(name="inferred", fields=fields)

