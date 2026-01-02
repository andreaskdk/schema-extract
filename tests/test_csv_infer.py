import io
import tempfile
import json
from pathlib import Path

from schema_extract.adapters.csv_adapter import CSVAdapter
from schema_extract.infer import infer_from_tables


def test_csv_infer_ints_and_nulls(tmp_path):
    csv = """a,b\n1,2\n3,\n,5\n"""
    p = tmp_path / "t.csv"
    p.write_text(csv)
    adapter = CSVAdapter(str(p), sample_size=1000)
    tables = list(adapter.iter_tables())
    schema = infer_from_tables(tables)
    d = schema.to_json()
    # expect fields a and b present
    names = [f["name"] for f in d["fields"]]
    assert "a" in names and "b" in names


def test_csv_infer_mixed_float_int(tmp_path):
    csv = """x\n1\n2.5\n3\n"""
    p = tmp_path / "t2.csv"
    p.write_text(csv)
    adapter = CSVAdapter(str(p), sample_size=1000)
    schema = infer_from_tables(list(adapter.iter_tables()))
    d = schema.to_json()
    t = {f["name"]: f["type"] for f in d["fields"]}
    assert t["x"] == "float64" or (isinstance(t["x"], list) and "float64" in t["x"])

