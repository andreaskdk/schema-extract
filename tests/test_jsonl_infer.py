import json
from pathlib import Path

from schema_extract.adapters.jsonl_adapter import JSONLAdapter
from schema_extract.infer import infer_from_tables


def test_jsonl_infer_simple(tmp_path):
    data = '{"id":1,"name":"a"}\n{"id":2,"name":"b"}\n'
    p = tmp_path / "t.jsonl"
    p.write_text(data)
    adapter = JSONLAdapter(str(p), sample_size=1000)
    schema = infer_from_tables(list(adapter.iter_tables()))
    d = schema.to_json()
    names = [f["name"] for f in d["fields"]]
    assert "id" in names and "name" in names
    types = {f["name"]: f["type"] for f in d["fields"]}
    assert types["id"] == "int64" or (isinstance(types["id"], list) and "int64" in types["id"])

