import pytest
from pathlib import Path

from schema_extract.adapters.parquet_adapter import ParquetAdapter
from schema_extract.infer import infer_from_tables


@pytest.mark.skipif(
    __import__('importlib').util.find_spec('pyarrow') is None,
    reason='pyarrow not installed - skipping parquet tests',
)
def test_parquet_infer_roundtrip(tmp_path):
    # create a small parquet file using pyarrow if available
    import pyarrow as pa
    import pyarrow.parquet as pq

    tbl = pa.table({'a': pa.array([1, 2, None], type=pa.int64()), 'b': pa.array(['x', 'y', 'z'])})
    p = tmp_path / 't.parquet'
    pq.write_table(tbl, str(p))

    adapter = ParquetAdapter(str(p))
    tables = list(adapter.iter_tables())
    schema = infer_from_tables(tables)
    d = schema.to_json()
    names = [f['name'] for f in d['fields']]
    assert 'a' in names and 'b' in names

