import base64
import importlib.util
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

ROOT = Path(__file__).resolve().parents[1]
BUCKET = "clockdata-test-datalake"


def _load(name: str, path: Path):
    """Both Lambdas are called handler.py, so load them under distinct names."""
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(autouse=True)
def aws_env(monkeypatch):
    # Fake credentials: nothing in the test suite can ever reach real AWS.
    for key, value in {
        "AWS_ACCESS_KEY_ID": "testing",
        "AWS_SECRET_ACCESS_KEY": "testing",
        "AWS_SESSION_TOKEN": "testing",
        "AWS_DEFAULT_REGION": "eu-west-3",
        "DATA_LAKE_BUCKET": BUCKET,
    }.items():
        monkeypatch.setenv(key, value)


@pytest.fixture
def processor():
    mod = _load("clockdata_processor", ROOT / "lambdas/processor/handler.py")
    mod.BUCKET, mod._s3 = BUCKET, None
    return mod


@pytest.fixture
def aggregator():
    mod = _load("clockdata_aggregator", ROOT / "lambdas/aggregator/handler.py")
    mod.BUCKET, mod._s3 = BUCKET, None
    return mod


@pytest.fixture
def simulator():
    return _load("clockdata_simulator", ROOT / "simulator/simulator.py")


@pytest.fixture
def s3_bucket():
    with mock_aws():
        client = boto3.client("s3", region_name="eu-west-3")
        client.create_bucket(Bucket=BUCKET, CreateBucketConfiguration={"LocationConstraint": "eu-west-3"})
        yield client


def kinesis_event(payloads, arrival: datetime | None = None, first_seq: int = 49600000000000000000000000000000000000000000000000000001):
    """Build the event Lambda receives from a Kinesis event source mapping."""
    arrival = arrival or datetime.now(timezone.utc)
    records = []
    for i, payload in enumerate(payloads):
        data = payload if isinstance(payload, (bytes, str)) else json.dumps(payload)
        if isinstance(data, str):
            data = data.encode("utf-8")
        seq = str(first_seq + i)
        records.append({
            "eventSource": "aws:kinesis",
            "eventID": f"shardId-000000000000:{seq}",
            "kinesis": {
                "data": base64.b64encode(data).decode(),
                "sequenceNumber": seq,
                "partitionKey": payload.get("device_id", "unknown") if isinstance(payload, dict) else "unknown",
                "approximateArrivalTimestamp": arrival.timestamp(),
            },
        })
    return {"Records": records}


def list_keys(client, prefix: str) -> list[str]:
    resp = client.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return sorted(o["Key"] for o in resp.get("Contents", []))
