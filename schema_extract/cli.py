"""Simple CLI entrypoint for MVP using click."""
import sys
import json
from pathlib import Path

import click

from .adapters.csv_adapter import CSVAdapter
from .infer import infer_from_tables
from .schema_model import Schema


@click.group()
def cli():
    pass


@cli.command()
@click.argument("input", type=click.Path(exists=True))
@click.option("--output", "output_path", type=click.Path(), default=None, help="Write schema JSON to path")
@click.option("--format", "fmt", type=click.Choice(["csv", "jsonl", "parquet"], case_sensitive=False), default="csv")
@click.option("--sample-size", type=int, default=1000)
def infer(input, output_path, fmt, sample_size):
    """Infer a schema from INPUT and write to OUTPUT (or stdout)."""
    src = Path(input)
    if fmt == "csv":
        adapter = CSVAdapter(str(src), sample_size=sample_size)
    elif fmt == "jsonl":
        from .adapters.jsonl_adapter import JSONLAdapter
        adapter = JSONLAdapter(str(src), sample_size=sample_size)
    elif fmt == "parquet":
        from .adapters.parquet_adapter import ParquetAdapter
        adapter = ParquetAdapter(str(src))
    else:
        click.echo(f"Unsupported format: {fmt}", err=True)
        sys.exit(5)

    tables = list(adapter.iter_tables())
    schema = infer_from_tables(tables)
    schema_json = schema.to_json()

    if output_path:
        Path(output_path).write_text(json.dumps(schema_json, indent=2))
        click.echo(f"Wrote schema to {output_path}")
    else:
        click.echo(json.dumps(schema_json, indent=2))


def main(argv=None):
    """Entrypoint for programmatic invocation. If argv is provided it should be a
    list of arguments (excluding program name).
    """
    return cli(args=argv)


if __name__ == "__main__":
    cli()
