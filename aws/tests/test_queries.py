"""Athena queries: run them locally against real pipeline output.

The Trino/Athena SQL is transpiled to DuckDB with sqlglot and executed on the
Parquet/NDJSON files produced by the Lambdas (moto S3 -> local directory),
laid out exactly like the S3 prefixes Glue partition projection points at.
"""

import random
import runpy
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

from conftest import BUCKET, ROOT, kinesis_event

duckdb = pytest.importorskip("duckdb")
sqlglot = pytest.importorskip("sqlglot")

QUERIES = sorted((ROOT / "queries").glob("*.sql"))
QUERY_MODULE = runpy.run_path(str(ROOT / "scripts/query.py"))


@pytest.fixture
def lake(tmp_path, simulator, processor, aggregator):
    with mock_aws():
        s3 = boto3.client("s3", region_name="eu-west-3")
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-west-3"})
        rng = random.Random(99)
        fleet = simulator.make_fleet(3, rng)
        payloads = [e for _ in range(40) for e in simulator.generate_tick(fleet, 1.0, 0.1, 0.1, rng)]
        for i in range(0, len(payloads), 100):
            processor.handler(kinesis_event(payloads[i:i + 100], first_seq=10**50 + i))
        aggregator.handler({"hours_back": 1})
        for obj in s3.list_objects_v2(Bucket=BUCKET)["Contents"]:
            target = tmp_path / obj["Key"]
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read())

    con = duckdb.connect()
    hive = "hive_partitioning=true, hive_types={'dt': 'VARCHAR', 'hour': 'VARCHAR'}"
    con.execute(f"CREATE VIEW silver_events AS SELECT * FROM read_parquet('{tmp_path}/silver/events/*/*/*.parquet', {hive})")
    con.execute(f"CREATE VIEW gold_device_hourly AS SELECT * FROM read_parquet('{tmp_path}/gold/device_hourly/*/*/*.parquet', {hive})")
    for table, prefix in [("bronze_events", "bronze/events"), ("quarantine_events", "quarantine/events")]:
        con.execute(f"CREATE VIEW {table} AS SELECT * FROM read_json_auto('{tmp_path}/{prefix}/*/*/*.json', "
                    f"format='newline_delimited', {hive})")
    return con


@pytest.mark.parametrize("path", QUERIES, ids=lambda p: p.name)
def test_query_runs_and_returns_rows(lake, path: Path):
    sql = sqlglot.transpile(path.read_text(), read="trino", write="duckdb")[0]
    rows = lake.execute(sql).fetchall()
    if path.name.startswith("06"):  # depends on random battery/SpO2 state
        return
    assert rows, f"{path.name} returned no rows"


def test_every_query_is_valid_trino_sql():
    for path in QUERIES:
        assert len(sqlglot.parse(path.read_text(), read="trino")) == 1, path.name


def test_format_table_and_cost():
    table = QUERY_MODULE["format_table"]([["a", "bb"], ["1", "x" * 50]], max_width=10)
    assert table.splitlines()[0].startswith("a ")
    assert "…" in table
    assert QUERY_MODULE["format_table"]([]) == "(no rows)"
    # Athena bills a 10 MB minimum per query at $5/TB
    assert QUERY_MODULE["estimated_cost"](0) == pytest.approx(10 / 1024 ** 2 * 5)


class FakeAthena:
    def __init__(self, state="SUCCEEDED"):
        self.state = state

    def start_query_execution(self, **kwargs):
        assert kwargs["WorkGroup"] == "wg" and kwargs["QueryExecutionContext"] == {"Database": "db"}
        return {"QueryExecutionId": "q1"}

    def get_query_execution(self, QueryExecutionId):
        return {"QueryExecution": {"Status": {"State": self.state, "StateChangeReason": "boom"},
                                   "Statistics": {"DataScannedInBytes": 42}}}

    def get_paginator(self, name):
        class P:
            def paginate(self, **_):
                yield {"ResultSet": {"Rows": [{"Data": [{"VarCharValue": "n"}]}, {"Data": [{"VarCharValue": "1"}]}]}}
        return P()


def test_run_query_success_and_failure():
    rows, stats = QUERY_MODULE["run_query"](FakeAthena(), "SELECT 1", "wg", "db")
    assert rows == [["n"], ["1"]] and stats["DataScannedInBytes"] == 42
    with pytest.raises(RuntimeError, match="boom"):
        QUERY_MODULE["run_query"](FakeAthena("FAILED"), "SELECT 1", "wg", "db")
