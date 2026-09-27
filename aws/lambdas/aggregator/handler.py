"""Silver -> Gold hourly aggregator.

Reads every Silver Parquet file of an hour partition, de-duplicates events on
event_id (Kinesis delivery is at-least-once), computes per-device metrics and
writes ONE Parquet file per hour to a fixed key. Re-running an hour simply
overwrites it, so the job is idempotent and safe to schedule frequently.

Invocation payload (all optional):
  {"hours_back": 2}                        current hour + previous one (default)
  {"dt": "2026-09-27", "hour": "14"}       one specific hour
"""

from __future__ import annotations

import io
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

BUCKET = os.environ.get("DATA_LAKE_BUCKET", "")
SILVER_PREFIX = "silver/events"
GOLD_PREFIX = "gold/device_hourly"
GOLD_FILE = "part-00000.parquet"
MAX_HOURS_BACK = 24 * 7

HIVE_TO_ARROW = {
    "string": pa.string(),
    "int": pa.int32(),
    "bigint": pa.int64(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("ms"),
}

GOLD_SCHEMA = pa.schema([
    pa.field(c["name"], HIVE_TO_ARROW[c["type"]])
    for c in json.loads(Path(__file__).with_name("schema_gold.json").read_text())
])

_s3 = None


def s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


def hours_to_process(event: dict, now: datetime) -> list[tuple[str, str]]:
    if event.get("dt") and event.get("hour") is not None:
        dt = datetime.strptime(event["dt"], "%Y-%m-%d")
        hour = int(event["hour"])
        if not 0 <= hour <= 23:
            raise ValueError("hour must be between 0 and 23")
        return [(dt.strftime("%Y-%m-%d"), f"{hour:02d}")]
    hours_back = int(event.get("hours_back", 2))
    if not 1 <= hours_back <= MAX_HOURS_BACK:
        raise ValueError(f"hours_back must be between 1 and {MAX_HOURS_BACK}")
    top = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    return [((top - timedelta(hours=i)).strftime("%Y-%m-%d"), (top - timedelta(hours=i)).strftime("%H"))
            for i in range(hours_back)]


def read_silver_hour(dt: str, hour: str) -> pa.Table | None:
    prefix = f"{SILVER_PREFIX}/dt={dt}/hour={hour}/"
    tables = []
    for page in s3().get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].endswith(".parquet"):
                body = s3().get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
                tables.append(pq.read_table(io.BytesIO(body)))
    if not tables:
        return None
    return pa.concat_tables(tables, promote_options="default")


def dedupe(table: pa.Table) -> pa.Table:
    """Keep the first occurrence of each event_id."""
    seen, keep = set(), []
    for i, event_id in enumerate(table.column("event_id").to_pylist()):
        if event_id not in seen:
            seen.add(event_id)
            keep.append(i)
    return table.take(keep) if len(keep) != table.num_rows else table


def aggregate(silver: pa.Table, aggregated_at: datetime) -> pa.Table:
    table = dedupe(silver).sort_by([("device_id", "ascending"), ("event_time", "ascending")])
    table = table.append_column("abnormal_int", pc.cast(table.column("is_abnormal_hr"), pa.int64()))
    # use_threads=False keeps input order, which makes "last" deterministic
    grouped = table.group_by("device_id", use_threads=False).aggregate([
        ("event_id", "count"),
        ("heart_rate", "mean"),
        ("heart_rate", "min"),
        ("heart_rate", "max"),
        ("abnormal_int", "sum"),
        ("steps", "sum"),
        ("spo2", "mean"),
        ("spo2", "min"),
        ("battery", "last"),
        ("event_time", "min"),
        ("event_time", "max"),
    ])
    n = grouped.num_rows
    out = {
        "device_id": grouped.column("device_id"),
        "event_count": grouped.column("event_id_count"),
        "avg_heart_rate": pc.round(grouped.column("heart_rate_mean"), 2),
        "min_heart_rate": grouped.column("heart_rate_min"),
        "max_heart_rate": grouped.column("heart_rate_max"),
        "abnormal_hr_count": grouped.column("abnormal_int_sum"),
        "total_steps": grouped.column("steps_sum"),
        "avg_spo2": pc.round(grouped.column("spo2_mean"), 2),
        "min_spo2": grouped.column("spo2_min"),
        "last_battery": grouped.column("battery_last"),
        "first_event_time": grouped.column("event_time_min"),
        "last_event_time": grouped.column("event_time_max"),
        "aggregated_at": pa.array([aggregated_at.astimezone(timezone.utc).replace(tzinfo=None)] * n,
                                  type=pa.timestamp("ms")),
    }
    return pa.table({name: out[name].cast(field.type) for name, field in zip(GOLD_SCHEMA.names, GOLD_SCHEMA)},
                    schema=GOLD_SCHEMA).sort_by("device_id")


def handler(event, context=None, now: datetime | None = None):
    if not BUCKET:
        raise RuntimeError("DATA_LAKE_BUCKET environment variable is not set")
    event = event or {}
    now = now or datetime.now(timezone.utc)
    results = []
    for dt, hour in hours_to_process(event, now):
        silver = read_silver_hour(dt, hour)
        if silver is None or silver.num_rows == 0:
            results.append({"dt": dt, "hour": hour, "devices": 0, "events": 0})
            continue
        gold = aggregate(silver, now)
        buf = io.BytesIO()
        pq.write_table(gold, buf, compression="snappy")
        key = f"{GOLD_PREFIX}/dt={dt}/hour={hour}/{GOLD_FILE}"
        s3().put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue(), ContentType="application/vnd.apache.parquet")
        results.append({"dt": dt, "hour": hour, "devices": gold.num_rows,
                        "events": int(pc.sum(gold.column("event_count")).as_py())})
    print(json.dumps({"type": "GOLD_AGGREGATION", "results": results}))
    return {"results": results}
