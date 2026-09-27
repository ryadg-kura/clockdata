"""Simulator unit tests + a local end-to-end run of the whole pipeline.

The end-to-end test uses moto's in-memory Kinesis and S3:
simulator -> Kinesis -> processor Lambda -> S3 Silver -> aggregator -> Gold.
"""

import base64
import io
import random
from datetime import datetime, timezone

import boto3
import pyarrow.parquet as pq
from moto import mock_aws

from conftest import BUCKET, list_keys


def test_generated_events_pass_lambda_validation(simulator, processor):
    rng = random.Random(42)
    fleet = simulator.make_fleet(10, rng)
    now = datetime.now(timezone.utc)
    for _ in range(200):
        for e in simulator.generate_tick(fleet, 1.0, anomaly_rate=0.05, invalid_rate=0.0, rng=rng):
            clean, errors = processor.validate(e, now)
            assert errors == [], (e, errors)


def test_anomalies_are_generated(simulator, processor):
    rng = random.Random(7)
    fleet = simulator.make_fleet(5, rng)
    events = [e for _ in range(300) for e in simulator.generate_tick(fleet, 1.0, 0.05, 0.0, rng)]
    zones = {processor.heart_rate_zone(e["heart_rate"]) for e in events}
    assert {"bradycardia", "tachycardia", "resting"} <= zones
    assert any(e["spo2"] < 90 for e in events)
    assert all(0 <= e["battery"] <= 100 for e in events)


def test_invalid_events_are_rejected_by_lambda(simulator, processor):
    rng = random.Random(3)
    fleet = simulator.make_fleet(5, rng)
    events = [e for _ in range(50) for e in simulator.generate_tick(fleet, 1.0, 0.0, 1.0, rng)]
    now = datetime.now(timezone.utc)
    for e in events:
        if isinstance(e, str):
            continue  # garbage payload, rejected by the JSON parser
        assert processor.validate(e, now)[0] is None, e


def test_same_seed_same_events(simulator):
    def run():
        rng = random.Random(123)
        fleet = simulator.make_fleet(3, rng)
        return [{k: v for k, v in e.items() if k != "timestamp"}
                for _ in range(20) for e in simulator.generate_tick(fleet, 1.0, 0.1, 0.1, rng)
                if isinstance(e, dict)]
    assert run() == run()


def test_cli_requires_stream_unless_dry_run(simulator, capsys):
    import pytest
    with pytest.raises(SystemExit):
        simulator.parse_args([])
    assert simulator.parse_args(["--dry-run"]).dry_run


def test_dry_run_prints_json(simulator, capsys):
    assert simulator.main(["--dry-run", "--count", "2", "--devices", "3", "--seed", "1", "--invalid-rate", "0"]) == 0
    assert len(capsys.readouterr().out.strip().splitlines()) == 6


class FlakyKinesis:
    """put_records that throttles the first record once."""

    def __init__(self):
        self.calls = []

    def put_records(self, StreamName, Records):
        self.calls.append(len(Records))
        if len(self.calls) == 1:
            return {"FailedRecordCount": 1,
                    "Records": [{"ErrorCode": "ProvisionedThroughputExceededException"}]
                    + [{"SequenceNumber": "1"}] * (len(Records) - 1)}
        return {"FailedRecordCount": 0, "Records": [{"SequenceNumber": "1"}] * len(Records)}


def test_sink_retries_only_failed_records(simulator):
    client = FlakyKinesis()
    sink = simulator.KinesisSink(client, "s")
    sink.send([{"device_id": f"d{i}"} for i in range(3)])
    assert client.calls == [3, 1]
    assert (sink.sent, sink.failed) == (3, 0)


def test_sink_splits_large_batches(simulator):
    client = FlakyKinesis()
    client.calls.append(0)  # skip the throttling behaviour
    sink = simulator.KinesisSink(client, "s")
    sink.send([{"device_id": "d"}] * 1201)
    assert client.calls[1:] == [500, 500, 201]


def test_end_to_end_local_pipeline(simulator, processor, aggregator):
    with mock_aws():
        region = "eu-west-3"
        s3 = boto3.client("s3", region_name=region)
        s3.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": region})
        kinesis = boto3.client("kinesis", region_name=region)
        kinesis.create_stream(StreamName="clockdata-test-events", ShardCount=1)

        # 1. Simulator -> Kinesis
        rng = random.Random(2026)
        fleet = simulator.make_fleet(4, rng)
        sink = simulator.KinesisSink(kinesis, "clockdata-test-events")
        for _ in range(30):
            sink.send(simulator.generate_tick(fleet, 1.0, 0.05, 0.05, rng))
        assert sink.sent == 120

        # 2. Kinesis -> processor Lambda (what the event source mapping does)
        shard = kinesis.describe_stream(StreamName="clockdata-test-events")["StreamDescription"]["Shards"][0]
        iterator = kinesis.get_shard_iterator(StreamName="clockdata-test-events", ShardId=shard["ShardId"],
                                              ShardIteratorType="TRIM_HORIZON")["ShardIterator"]
        records = kinesis.get_records(ShardIterator=iterator, Limit=1000)["Records"]
        assert len(records) == 120
        totals = {"valid": 0, "invalid": 0, "abnormal": 0}
        for start in range(0, len(records), 100):  # batch_size = 100 in Terraform
            batch = {"Records": [{
                "eventID": f"{shard['ShardId']}:{r['SequenceNumber']}",
                "kinesis": {
                    "data": base64.b64encode(r["Data"]).decode(),
                    "sequenceNumber": r["SequenceNumber"],
                    "partitionKey": r["PartitionKey"],
                    "approximateArrivalTimestamp": r["ApproximateArrivalTimestamp"].timestamp(),
                },
            } for r in records[start:start + 100]]}
            for k, v in processor.handler(batch).items():
                totals[k] += v
        assert totals["valid"] + totals["invalid"] == 120
        assert totals["invalid"] > 0 and totals["abnormal"] > 0

        # 3. Silver -> Gold
        result = aggregator.handler({"hours_back": 2})
        assert sum(r["events"] for r in result["results"]) == totals["valid"]
        gold_keys = list_keys(s3, "gold/")
        assert gold_keys
        devices = set()
        for key in gold_keys:
            table = pq.read_table(io.BytesIO(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()))
            devices |= set(table.column("device_id").to_pylist())
        assert devices == {"watch-0001", "watch-0002", "watch-0003", "watch-0004"}
