import io
import json
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest

from conftest import BUCKET, kinesis_event, list_keys

NOW = datetime(2026, 9, 27, 14, 30, 0, tzinfo=timezone.utc)


def good_event(**overrides):
    event = {
        "event_id": "11111111-1111-4111-8111-111111111111",
        "device_id": "watch-0001",
        "timestamp": "2026-09-27T14:29:59.500Z",
        "heart_rate": 72,
        "steps": 3,
        "spo2": 98.1,
        "battery": 80,
    }
    event.update(overrides)
    return event


# ------------------------------------------------------------------ validation

def test_valid_event_is_normalized(processor):
    clean, errors = processor.validate(good_event(), NOW)
    assert errors == []
    assert clean["event_time"] == datetime(2026, 9, 27, 14, 29, 59, 500000, tzinfo=timezone.utc)
    assert clean["spo2"] == 98.1


@pytest.mark.parametrize("overrides, reason", [
    ({"heart_rate": 400}, "heart_rate"),
    ({"heart_rate": "72"}, "heart_rate"),
    ({"heart_rate": True}, "heart_rate"),
    ({"steps": -1}, "steps"),
    ({"spo2": 101.0}, "spo2"),
    ({"spo2": float("nan")}, "spo2"),
    ({"battery": 120}, "battery"),
    ({"device_id": ""}, "device_id"),
    ({"device_id": "bad id with spaces"}, "device_id"),
    ({"event_id": None}, "event_id"),
    ({"timestamp": "yesterday-ish"}, "timestamp"),
    ({"timestamp": "2026-09-27T15:00:00Z"}, "timestamp out of accepted range"),  # 30 min in the future
    ({"timestamp": "2019-01-01T00:00:00Z"}, "timestamp out of accepted range"),
])
def test_invalid_events_are_rejected(processor, overrides, reason):
    clean, errors = processor.validate(good_event(**overrides), NOW)
    assert clean is None
    assert any(reason in e for e in errors), errors


def test_missing_field_is_rejected(processor):
    event = good_event()
    del event["battery"]
    assert processor.validate(event, NOW)[0] is None


def test_non_object_payload(processor):
    assert processor.validate([1, 2], NOW) == (None, ["payload is not a JSON object"])
    assert processor.validate(None, NOW)[0] is None


def test_epoch_timestamps_are_accepted(processor):
    ts = int(datetime(2026, 9, 27, 14, 0, tzinfo=timezone.utc).timestamp())
    assert processor.validate(good_event(timestamp=ts), NOW)[0]["event_time"].hour == 14
    assert processor.validate(good_event(timestamp=ts * 1000), NOW)[0]["event_time"].hour == 14


@pytest.mark.parametrize("hr, zone", [
    (30, "bradycardia"), (39, "bradycardia"), (40, "resting"), (99, "resting"),
    (100, "light"), (130, "moderate"), (160, "vigorous"), (180, "vigorous"), (181, "tachycardia"),
])
def test_heart_rate_zones(processor, hr, zone):
    assert processor.heart_rate_zone(hr) == zone


@pytest.mark.parametrize("spo2, status", [(99.0, "normal"), (95.0, "normal"), (94.9, "low"), (89.9, "critical")])
def test_spo2_status(processor, spo2, status):
    assert processor.spo2_status(spo2) == status


# ------------------------------------------------------------------ handler

def test_handler_writes_bronze_silver_quarantine(processor, s3_bucket, capsys):
    payloads = [
        good_event(),
        good_event(event_id="22222222-2222-4222-8222-222222222222", heart_rate=35),   # abnormal
        good_event(event_id="33333333-3333-4333-8333-333333333333", heart_rate=999),  # invalid
        "\x00not-json{",                                                              # garbage
    ]
    result = processor.handler(kinesis_event(payloads, arrival=NOW), now=NOW + timedelta(seconds=2))
    assert result == {"valid": 2, "invalid": 2, "abnormal": 1}

    bronze = list_keys(s3_bucket, "bronze/")
    silver = list_keys(s3_bucket, "silver/")
    quarantine = list_keys(s3_bucket, "quarantine/")
    assert len(bronze) == len(silver) == len(quarantine) == 1
    assert bronze[0].startswith("bronze/events/dt=2026-09-27/hour=14/")
    assert silver[0].startswith("silver/events/dt=2026-09-27/hour=14/") and silver[0].endswith(".parquet")

    # Bronze keeps every record, byte for byte
    lines = s3_bucket.get_object(Bucket=BUCKET, Key=bronze[0])["Body"].read().decode().splitlines()
    assert [json.loads(line)["payload"] for line in lines][3] == "\x00not-json{"
    assert len(lines) == 4

    # Quarantine explains why
    rejected = [json.loads(line) for line in
                s3_bucket.get_object(Bucket=BUCKET, Key=quarantine[0])["Body"].read().decode().splitlines()]
    assert any("heart_rate" in e for e in rejected[0]["errors"])
    assert rejected[1]["errors"] == ["payload is not valid JSON"]

    # Silver is typed Parquet matching the Glue schema file
    table = pq.read_table(io.BytesIO(s3_bucket.get_object(Bucket=BUCKET, Key=silver[0])["Body"].read()))
    assert table.schema.equals(processor.SILVER_SCHEMA)
    rows = table.to_pylist()
    assert [r["heart_rate_zone"] for r in rows] == ["resting", "bradycardia"]
    assert [r["is_abnormal_hr"] for r in rows] == [False, True]
    assert rows[0]["end_to_end_latency_ms"] == 2500

    # EMF metrics + structured alert line in the logs
    out = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    emf = next(o for o in out if "_aws" in o)
    assert (emf["ValidEvents"], emf["InvalidEvents"], emf["AbnormalHeartRate"]) == (2, 2, 1)
    assert emf["_aws"]["CloudWatchMetrics"][0]["Dimensions"] == [["Pipeline"]]
    assert any(o.get("type") == "ABNORMAL_HEART_RATE" and o["heart_rate"] == 35 for o in out)


def test_silver_partition_follows_event_time_not_arrival(processor, s3_bucket):
    late = good_event(timestamp="2026-09-27T13:59:58Z")
    processor.handler(kinesis_event([late], arrival=NOW), now=NOW)
    assert list_keys(s3_bucket, "silver/")[0].startswith("silver/events/dt=2026-09-27/hour=13/")
    assert list_keys(s3_bucket, "bronze/")[0].startswith("bronze/events/dt=2026-09-27/hour=14/")


def test_retried_batch_overwrites_same_objects(processor, s3_bucket):
    event = kinesis_event([good_event(), good_event(event_id="x-2")], arrival=NOW)
    processor.handler(event, now=NOW)
    first = list_keys(s3_bucket, "")
    processor.handler(event, now=NOW)
    assert list_keys(s3_bucket, "") == first


def test_empty_batch_is_a_noop(processor, s3_bucket):
    assert processor.handler({"Records": []}) == {"valid": 0, "invalid": 0, "abnormal": 0}
    assert list_keys(s3_bucket, "") == []


def test_s3_failure_propagates_for_lambda_retry(processor, s3_bucket):
    processor.BUCKET = "bucket-that-does-not-exist"
    with pytest.raises(Exception):
        processor.handler(kinesis_event([good_event()], arrival=NOW), now=NOW)


def test_emf_latency_values_are_capped(processor, capsys):
    doc = processor.emit_metrics(1, 0, 0, list(range(250)), NOW)
    assert len(doc["EndToEndLatency"]) == processor.EMF_MAX_VALUES
