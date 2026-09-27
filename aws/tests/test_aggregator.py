import io
from datetime import datetime, timedelta, timezone

import pyarrow.parquet as pq
import pytest

from conftest import BUCKET, kinesis_event, list_keys

NOW = datetime(2026, 9, 27, 14, 50, 0, tzinfo=timezone.utc)


def event(i, device, hr, minute, steps=10, spo2=97.0, battery=50):
    return {
        "event_id": f"evt-{device}-{i}",
        "device_id": device,
        "timestamp": f"2026-09-27T14:{minute:02d}:00Z",
        "heart_rate": hr,
        "steps": steps,
        "spo2": spo2,
        "battery": battery,
    }


def read_gold(client, dt="2026-09-27", hour="14"):
    key = f"gold/device_hourly/dt={dt}/hour={hour}/part-00000.parquet"
    return pq.read_table(io.BytesIO(client.get_object(Bucket=BUCKET, Key=key)["Body"].read()))


def test_hours_to_process(aggregator):
    assert aggregator.hours_to_process({}, NOW) == [("2026-09-27", "14"), ("2026-09-27", "13")]
    assert aggregator.hours_to_process({"hours_back": 1}, NOW) == [("2026-09-27", "14")]
    assert aggregator.hours_to_process({"dt": "2026-09-01", "hour": 3}, NOW) == [("2026-09-01", "03")]
    midnight = datetime(2026, 9, 27, 0, 10, tzinfo=timezone.utc)
    assert aggregator.hours_to_process({}, midnight)[1] == ("2026-09-26", "23")
    with pytest.raises(ValueError):
        aggregator.hours_to_process({"hours_back": 0}, NOW)
    with pytest.raises(ValueError):
        aggregator.hours_to_process({"dt": "2026-09-01", "hour": 24}, NOW)


def test_aggregates_per_device_and_dedupes(processor, aggregator, s3_bucket):
    a = [event(1, "watch-a", 60, 1, steps=5, battery=90),
         event(2, "watch-a", 80, 2, steps=7, spo2=95.0, battery=89),
         event(3, "watch-a", 35, 3, steps=0, battery=88)]           # abnormal
    b = [event(1, "watch-b", 120, 5, steps=30, battery=40)]
    arrival = NOW - timedelta(minutes=1)
    processor.handler(kinesis_event(a + b, arrival=arrival), now=NOW)
    # Kinesis at-least-once: the same event delivered again in another batch
    processor.handler(kinesis_event([a[0]], arrival=arrival, first_seq=10**55), now=NOW)
    assert len(list_keys(s3_bucket, "silver/")) == 2

    result = aggregator.handler({"hours_back": 1}, now=NOW)
    assert result["results"] == [{"dt": "2026-09-27", "hour": "14", "devices": 2, "events": 4}]

    gold = read_gold(s3_bucket)
    assert gold.schema.equals(aggregator.GOLD_SCHEMA)
    rows = {r["device_id"]: r for r in gold.to_pylist()}
    wa = rows["watch-a"]
    assert wa["event_count"] == 3
    assert wa["avg_heart_rate"] == pytest.approx(58.33, abs=0.01)
    assert (wa["min_heart_rate"], wa["max_heart_rate"]) == (35, 80)
    assert wa["abnormal_hr_count"] == 1
    assert wa["total_steps"] == 12
    assert wa["min_spo2"] == 95.0
    assert wa["last_battery"] == 88
    assert wa["first_event_time"] == datetime(2026, 9, 27, 14, 1)
    assert wa["last_event_time"] == datetime(2026, 9, 27, 14, 3)
    assert rows["watch-b"]["total_steps"] == 30


def test_rerun_overwrites_single_gold_file(processor, aggregator, s3_bucket):
    processor.handler(kinesis_event([event(1, "watch-a", 70, 1)], arrival=NOW), now=NOW)
    aggregator.handler({"hours_back": 1}, now=NOW)
    processor.handler(kinesis_event([event(2, "watch-a", 90, 2)], arrival=NOW, first_seq=10**55), now=NOW)
    aggregator.handler({"hours_back": 1}, now=NOW)
    assert list_keys(s3_bucket, "gold/") == ["gold/device_hourly/dt=2026-09-27/hour=14/part-00000.parquet"]
    assert read_gold(s3_bucket).to_pylist()[0]["event_count"] == 2


def test_empty_hour_writes_nothing(aggregator, s3_bucket):
    assert aggregator.handler({}, now=NOW)["results"][0]["devices"] == 0
    assert list_keys(s3_bucket, "gold/") == []
