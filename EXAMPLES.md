# Examples — Quickstart and common workflows

This file contains short, copy-pasteable examples showing common ways to use the
`schema-extract` tool and Python API for quick experimentation.

The examples assume you have the `schema-extract` package installed (or you are
running from the repo). Some features (Parquet reading/writing) require
`pyarrow` to be installed.

---

## Quick CLI: infer a schema from a CSV

Create an Avro-style schema (JSON) from a CSV sample. This is the fastest way to
get a starting schema you can inspect and edit.

```bash
# Infer a schema from a CSV and write it to schema.avsc
schema-extract infer sample_mvp.csv -o schema.avsc
```

What happens:
- The tool reads a sample (up to an internal sample size) and detects fields,
  types, and nullability.
- The output is an Avro-like JSON schema file you can open and edit.

---

## Quick CLI: infer from JSONL

```bash
# JSONL (newline-delimited JSON)
schema-extract infer events.jsonl -o events_schema.avsc
```

The CLI treats each JSON line as an object (or stores scalars under a
`_value` synthetic field) and infers field names and types.

---

## Parquet: prefer pyarrow for accurate fast inference

If `pyarrow` is installed, the `Parquet` adapter yields an Arrow table and the
inference engine will use the Arrow schema directly. That preserves numeric and
temporal types and avoids expensive conversion to Python objects.

```bash
# This requires pyarrow to be available in the environment
schema-extract infer data.parquet -o data_schema.avsc
```

If `pyarrow` is not installed, Parquet support will be skipped (parquet tests
are typically configured to skip when pyarrow is missing).

---

## CLI: conform input data to a schema

Once you have a schema you like (for example `schema.avsc`) you can apply it
and write an output dataset that conforms to the schema:

```bash
schema-extract conform input.json --schema schema.avsc -o output.parquet
```

Behavior:
- Fields are mapped and coerced to the target types.
- Defaults from the schema are applied for missing values.
- Output is guaranteed to comply with the provided schema or the run fails
  with a report.

---

## Python API: infer programmatically (CSV example)

You can use the adapters and the inference engine directly from Python.

```python
from schema_extract.adapters.csv_adapter import CSVAdapter
from schema_extract.infer import infer_from_tables

# Create an adapter and get the sample tables (list-of-dicts chunks)
adapter = CSVAdapter('sample_mvp.csv', sample_size=1000)
tables = list(adapter.iter_tables())

# Infer a Schema object
schema = infer_from_tables(tables)

# Get JSON/Avro-style dict
schema_json = schema.to_json()
print(schema_json)
```

---

## Python API: Parquet + pyarrow fast-path

If `pyarrow` is installed, the `ParquetAdapter` yields a `pyarrow.Table`.
`infer_from_tables` detects `pyarrow.Table`/`RecordBatch` objects and will
build a schema from the Arrow schema directly (no materialization of rows).

```python
from schema_extract.adapters.parquet_adapter import ParquetAdapter
from schema_extract.infer import infer_from_tables

adapter = ParquetAdapter('data.parquet')
tables = list(adapter.iter_tables())

# When pyarrow is present, tables[0] is a pyarrow.Table and inference uses its schema
schema = infer_from_tables(tables)
print(schema.to_json())
```

Notes:
- This path preserves precise Arrow types (ints, floats, timestamps) where
  possible and is faster for large datasets.
- If pyarrow is not installed, the adapter will raise ImportError. Tests in the
  repo commonly skip parquet tests when pyarrow is absent.

---

## Minimal end-to-end example (infer -> edit -> conform)

1. Infer a schema from a CSV:

```bash
schema-extract infer january.csv -o january_schema.avsc
```

2. Edit `january_schema.avsc` as needed (rename fields, add defaults, mark
   required/optional).

3. Conform all monthly files to the same schema:

```bash
schema-extract conform january.csv   --schema january_schema.avsc -o january.parquet
schema-extract conform february.csv  --schema january_schema.avsc -o february.parquet
schema-extract conform march.jsonl   --schema january_schema.avsc -o march.parquet
```

The output Parquet files are schema-compliant and can be combined downstream.

---

## Troubleshooting

- If a CLI command fails with a message about `pyarrow`, either install
  `pyarrow` (`pip install pyarrow`) or use a non-parquet input format.
- If inference produces many `string` types where you expected numeric types,
  consider increasing the sample size or converting a file to Parquet and
  re-running inference with `pyarrow` available.

---

## Where to go next

- Inspect and tweak the generated Avro-style schema files.
- Add aliases or defaults to make conformance more robust.
- Use the Python API to build automated ETL pipelines with deterministic
  schema enforcement.

Happy extracting! If you'd like, I can add a short CI workflow that runs tests
with and without `pyarrow` to ensure Parquet support stays exercised.

