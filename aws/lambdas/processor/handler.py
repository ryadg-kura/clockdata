"""Kinesis -> S3 stream processor (Bronze / Silver / Quarantine).

For every batch delivered by the Kinesis event source mapping:

  1. Bronze     every record, untouched, as NDJSON envelopes
                (payload + Kinesis metadata), partitioned by arrival hour.
  2. Validate   schema, types and physiological ranges.
  3. Silver     valid events, enriched (heart-rate zone, abnormal flags,
                latency...) and written as Parquet, partitioned by event hour.
  4. Quarantine invalid events with the list of reasons, as NDJSON.
  5. Metrics    CloudWatch Embedded Metric Format (EMF) log line: no
                PutMetricData call, no extra IAM permission.

Object keys are derived from the Kinesis sequence numbers of the batch, so a
retried batch overwrites the same objects instead of duplicating them.

Any exception (S3 unavailable, ...) is re-raised: Lambda retries the batch,
bisects it, and finally sends its metadata to the SQS dead-letter queue.
"""

from __future__ import annotations

import base64
import io
import json
import os
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

BUCKET = os.environ.get("DATA_LAKE_BUCKET", "")
HR_LOW = int(os.environ.get("HR_LOW_THRESHOLD", "40"))
HR_HIGH = int(os.environ.get("HR_HIGH_THRESHOLD", "180"))
METRIC_NAMESPACE = os.environ.get("METRIC_NAMESPACE", "ClockData")
PIPELINE_NAME = os.environ.get("PIPELINE_NAME", "clockdata")

BRONZE_PREFIX = "bronze/events"
SILVER_PREFIX = "silver/events"
QUARANTINE_PREFIX = "quarantine/events"

MAX_FUTURE_SKEW = timedelta(minutes=5)
MIN_EVENT_TIME = datetime(2020, 1, 1, tzinfo=timezone.utc)
DEVICE_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")
EMF_MAX_VALUES = 100  # EMF accepts at most 100 values per metric per log line

HIVE_TO_ARROW = {
    "string": pa.string(),
    "int": pa.int32(),
    "bigint": pa.int64(),
    "double": pa.float64(),
    "boolean": pa.bool_(),
    "timestamp": pa.timestamp("ms"),
}


def load_schema(path: Path) -> pa.Schema:
    """Build the Arrow schema from the JSON file Terraform also uses for Glue."""
    columns = json.loads(path.read_text())
    return pa.schema([pa.field(c["name"], HIVE_TO_ARROW[c["type"]]) for c in columns])


SILVER_SCHEMA = load_schema(Path(__file__).with_name("schema_silver.json"))

_s3 = None


def s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3")
    return _s3


# --------------------------------------------------------------------------- #
# Validation & enrichment
# --------------------------------------------------------------------------- #

def parse_timestamp(value) -> datetime | None:
    """Accept ISO 8601 strings, or epoch seconds / milliseconds."""
    if isinstance(value, bool):
        return None
    try:
        if isinstance(value, (int, float)):
            seconds = value / 1000 if value > 1e11 else value
            return datetime.fromtimestamp(seconds, tz=timezone.utc)
        if isinstance(value, str):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None
    return None


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v) -> bool:
    return isinstance(v, (int, float)) and not isinstance(v, bool)


def validate(event, arrival: datetime) -> tuple[dict | None, list[str]]:
    """Return (normalized event, []) or (None, [reasons])."""
    if not isinstance(event, dict):
        return None, ["payload is not a JSON object"]

    errors: list[str] = []
    event_id = event.get("event_id")
    if not isinstance(event_id, str) or not event_id:
        errors.append("event_id missing or not a string")

    device_id = event.get("device_id")
    if not isinstance(device_id, str) or not DEVICE_ID_RE.match(device_id):
        errors.append("device_id missing or invalid")

    ts = parse_timestamp(event.get("timestamp"))
    if ts is None:
        errors.append("timestamp missing or unparseable")
    elif ts < MIN_EVENT_TIME or ts > arrival + MAX_FUTURE_SKEW:
        errors.append("timestamp out of accepted range")

    hr = event.get("heart_rate")
    if not _is_int(hr) or not 20 <= hr <= 250:
        errors.append("heart_rate must be an integer in [20, 250]")

    steps = event.get("steps")
    if not _is_int(steps) or not 0 <= steps <= 10_000:
        errors.append("steps must be an integer in [0, 10000]")

    spo2 = event.get("spo2")
    if not _is_number(spo2) or not 50 <= spo2 <= 100:
        errors.append("spo2 must be a number in [50, 100]")

    battery = event.get("battery")
    if not _is_int(battery) or not 0 <= battery <= 100:
        errors.append("battery must be an integer in [0, 100]")

    if errors:
        return None, errors
    return {
        "event_id": event_id,
        "device_id": device_id,
        "event_time": ts,
        "heart_rate": hr,
        "steps": steps,
        "spo2": float(spo2),
        "battery": battery,
    }, []


def heart_rate_zone(hr: int) -> str:
    if hr < HR_LOW:
        return "bradycardia"
    if hr > HR_HIGH:
        return "tachycardia"
    if hr < 100:
        return "resting"
    if hr < 130:
        return "light"
    if hr < 160:
        return "moderate"
    return "vigorous"


def spo2_status(spo2: float) -> str:
    if spo2 < 90:
        return "critical"
    if spo2 < 95:
        return "low"
    return "normal"


def enrich(event: dict, arrival: datetime, processed_at: datetime, sequence_number: str) -> dict:
    zone = heart_rate_zone(event["heart_rate"])
    latency_ms = int((processed_at - event["event_time"]).total_seconds() * 1000)
    return {
        **event,
        "heart_rate_zone": zone,
        "is_abnormal_hr": zone in ("bradycardia", "tachycardia"),
        "spo2_status": spo2_status(event["spo2"]),
        "battery_low": event["battery"] < 15,
        "arrival_time": arrival,
        "processed_at": processed_at,
        "end_to_end_latency_ms": max(0, latency_ms),
        "kinesis_sequence_number": sequence_number,
    }


# --------------------------------------------------------------------------- #
# S3 writers
# --------------------------------------------------------------------------- #

def partition(ts: datetime) -> str:
    ts = ts.astimezone(timezone.utc)
    return f"dt={ts:%Y-%m-%d}/hour={ts:%H}"


def batch_key(prefix: str, part: str, seqs: list[str], ext: str) -> str:
    # Sequence numbers are strictly increasing within a shard, so the
    # (first, last) pair identifies the content of the file deterministically.
    return f"{prefix}/{part}/{min(seqs, key=int)}-{max(seqs, key=int)}.{ext}"


def naive_utc(ts: datetime) -> datetime:
    """Parquet/Athena timestamps are stored without zone, always UTC here."""
    return ts.astimezone(timezone.utc).replace(tzinfo=None)


def to_parquet(rows: list[dict]) -> bytes:
    columns = {f.name: [] for f in SILVER_SCHEMA}
    for row in rows:
        for name in columns:
            value = row[name]
            columns[name].append(naive_utc(value) if isinstance(value, datetime) else value)
    table = pa.table(columns, schema=SILVER_SCHEMA)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


def put_ndjson(key: str, lines: list[dict]) -> None:
    body = "\n".join(json.dumps(line, separators=(",", ":"), ensure_ascii=False) for line in lines) + "\n"
    s3().put_object(Bucket=BUCKET, Key=key, Body=body.encode("utf-8"), ContentType="application/x-ndjson")


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #

def emit_metrics(valid: int, invalid: int, abnormal: int, latencies: list[int], now: datetime) -> dict:
    metrics = [
        {"Name": "ValidEvents", "Unit": "Count"},
        {"Name": "InvalidEvents", "Unit": "Count"},
        {"Name": "AbnormalHeartRate", "Unit": "Count"},
    ]
    doc = {
        "_aws": {
            "Timestamp": int(now.timestamp() * 1000),
            "CloudWatchMetrics": [{
                "Namespace": METRIC_NAMESPACE,
                "Dimensions": [["Pipeline"]],
                "Metrics": metrics,
            }],
        },
        "Pipeline": PIPELINE_NAME,
        "ValidEvents": valid,
        "InvalidEvents": invalid,
        "AbnormalHeartRate": abnormal,
    }
    if latencies:
        metrics.append({"Name": "EndToEndLatency", "Unit": "Milliseconds"})
        doc["EndToEndLatency"] = latencies[:EMF_MAX_VALUES]
    print(json.dumps(doc))
    return doc


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #

def decode(record: dict) -> tuple[str, str]:
    raw = base64.b64decode(record["kinesis"]["data"])
    return raw.decode("utf-8", errors="replace"), record["kinesis"]["sequenceNumber"]


def handler(event, context=None, now: datetime | None = None):
    records = event.get("Records", [])
    if not records:
        return {"valid": 0, "invalid": 0, "abnormal": 0}
    if not BUCKET:
        raise RuntimeError("DATA_LAKE_BUCKET environment variable is not set")

    processed_at = now or datetime.now(timezone.utc)
    bronze: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    silver: dict[str, list[dict]] = defaultdict(list)
    quarantine: dict[str, list[tuple[str, dict]]] = defaultdict(list)
    latencies: list[int] = []
    abnormal = 0

    for record in records:
        payload, seq = decode(record)
        arrival = datetime.fromtimestamp(record["kinesis"]["approximateArrivalTimestamp"], tz=timezone.utc)
        envelope = {
            "payload": payload,
            "partition_key": record["kinesis"].get("partitionKey"),
            "sequence_number": seq,
            "shard_id": record.get("eventID", "").split(":")[0],
            "arrival_time": arrival.isoformat(),
        }
        bronze[partition(arrival)].append((seq, envelope))

        try:
            clean, errors = validate(json.loads(payload), arrival)
        except json.JSONDecodeError:
            clean, errors = None, ["payload is not valid JSON"]

        if errors:
            quarantine[partition(arrival)].append((seq, {**envelope, "errors": errors}))
            continue

        row = enrich(clean, arrival, processed_at, seq)
        silver[partition(row["event_time"])].append(row)
        latencies.append(row["end_to_end_latency_ms"])
        if row["is_abnormal_hr"]:
            abnormal += 1
            # Structured line, easy to find with CloudWatch Logs Insights
            print(json.dumps({
                "level": "WARN", "type": "ABNORMAL_HEART_RATE", "device_id": row["device_id"],
                "heart_rate": row["heart_rate"], "zone": row["heart_rate_zone"],
                "event_time": row["event_time"].isoformat(),
            }))

    for part, items in bronze.items():
        put_ndjson(batch_key(BRONZE_PREFIX, part, [s for s, _ in items], "json"), [e for _, e in items])
    for part, items in quarantine.items():
        put_ndjson(batch_key(QUARANTINE_PREFIX, part, [s for s, _ in items], "json"), [e for _, e in items])
    for part, rows in silver.items():
        key = batch_key(SILVER_PREFIX, part, [r["kinesis_sequence_number"] for r in rows], "parquet")
        s3().put_object(Bucket=BUCKET, Key=key, Body=to_parquet(rows), ContentType="application/vnd.apache.parquet")

    valid = sum(len(rows) for rows in silver.values())
    invalid = sum(len(items) for items in quarantine.values())
    emit_metrics(valid, invalid, abnormal, latencies, processed_at)
    return {"valid": valid, "invalid": invalid, "abnormal": abnormal}
