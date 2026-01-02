# schema-extract

**schema-extract** is a Python library and CLI tool that makes a best effort to extract structured data from almost any source and guarantees that the resulting dataset conforms to a user-defined schema.

It is designed for **analysts, OSINT investigators, and ML practitioners** who need a *first usable dataset* quickly, even when the input data is messy, inconsistent, or semi-structured.

---

## Why schema-extract?

Real-world data is rarely clean:

* HTML tables scraped from the web
* JSON from APIs with drifting fields
* Logs with inconsistent structure
* CSVs and spreadsheets with mixed types

`schema-extract` minimizes the effort required to turn these sources into **reliable, schema-compliant datasets** that can be reused across workflows.

---

## Core guarantees

* **Best-effort extraction** from supported formats
* **Deterministic output**
* **Guaranteed schema compliance**
  If a dataset is produced, it *always* conforms to the schema
* **Clear reporting** explaining how data was mapped and transformed

If schema compliance cannot be guaranteed, the run fails with an explicit report.

---

## Core workflows

### 1. Infer a schema from data

```bash
schema-extract infer input.html -o schema.avsc
```

This:

* Detects structure and fields
* Infers an Avro-style schema
* Preserves nested records
* Marks fields as nullable when appropriate

---

### 2. Adjust and reuse the schema

Schemas are meant to be reviewed and edited by users.

You can:

* Rename fields
* Add aliases
* Set defaults
* Make fields required or optional

The same schema can then be reused across multiple files and formats.

---

### 3. Extract data that conforms to the schema

```bash
schema-extract conform input.json \
  --schema schema.avsc \
  -o output.parquet
```

During conformance, `schema-extract`:

* Maps input fields to schema fields
* Coerces values to target types
* Applies defaults defined in the schema
* Preserves nested structures

The output dataset is **guaranteed to comply with the schema**.

---

### 4. Use the same schema across heterogeneous inputs

```bash
schema-extract conform january.csv   --schema schema.avsc
schema-extract conform february.xlsx --schema schema.avsc
schema-extract conform march.json    --schema schema.avsc
```

This makes it easy to normalize heterogeneous sources into a single dataset.

---

## Supported formats (v0.1)

### Input

* CSV / TSV
* JSON / JSONL
* Excel (XLSX)
* Parquet
* Avro data files
* SQLite
* XML
* HTML tables
* Line-based log files

### Output

* CSV
* JSONL
* Parquet
* Avro
* Arrow IPC
* SQLite

Internally, all data is represented as **Apache Arrow tables**.

---

## Schema model

`schema-extract` uses an **Avro-inspired schema model** with a deliberate restriction to keep extraction predictable and safe.

### Supported schema features

* `record` (nested structs)
* `array`
* `bytes`
* Defaults
* Field aliases

### Union restriction

Only **nullable unions** are supported:

```json
["null", "string"]
```

Arbitrary unions such as `["int", "string"]` are intentionally not supported.

This restriction ensures:

* Deterministic type coercion
* Clear error handling
* Reliable downstream datasets

---

## Nested data support

Nested records are **preserved end-to-end**, following Parquet / Arrow semantics.

Example schema:

```json
{
  "name": "event",
  "type": "record",
  "fields": [
    {
      "name": "user",
      "type": {
        "name": "user",
        "type": "record",
        "fields": [
          {"name": "id", "type": "string"},
          {"name": "email", "type": ["null", "string"], "default": null}
        ]
      }
    },
    {"name": "timestamp", "type": "long"}
  ]
}
```

Nested structures are written natively when the output format supports them (e.g. Parquet, Avro).

---

## Field mapping and coercion

Field mapping is **configurable** and may include:

* Exact name matches
* Case-insensitive matches
* Schema-defined aliases

Type coercion is also configurable, with policies such as:

* Fail on error
* Set invalid values to `null`
* Drop invalid rows

The default behavior prioritizes **schema compliance and transparency**.

---

## Defaults and missing values

If a field defines a `default` value in the schema, it will be applied when:

* The input field is missing
* The value cannot be safely coerced

This allows schemas to act as both **contracts** and **repair guides**.

---

## Reports and explainability

Every run can emit a structured report describing:

* Field mappings
* Type conversions
* Defaults applied
* Validation failures and examples

An **explain / dry-run mode** shows what would happen without producing output.

---

## Performance and scope

* Designed for datasets with **thousands of rows**
* In-memory execution (no streaming yet)
* Built on Apache Arrow
* Python 3.12+
* MIT licensed

---

## Example use cases

* Extracting tables from Wikipedia HTML pages
* Normalizing JSON API responses
* Turning line-based logs into structured datasets
* Producing schema-compliant Parquet files for analytics or ML

---

## Roadmap

* Derived / computed fields
* Better constraint inference
* Streaming execution
* Interactive schema editing
* Optional AI-assisted extraction for difficult sources

---

## Installation

```bash
pip install schema-extract
```

---

## License

MIT

