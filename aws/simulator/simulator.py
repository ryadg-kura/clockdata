"""ClockData smartwatch simulator for the AWS pipeline.

Generates realistic smartwatch telemetry (heart rate, steps, SpO2, battery)
and publishes it to an Amazon Kinesis Data Stream, one record per event,
partitioned by device_id.

Each simulated watch follows a small activity state machine
(sleeping -> resting -> walking -> running) so heart rate, step count and
battery drain move together the way they do on a real wrist. On top of that
you can inject:

  * anomalies  (--anomaly-rate): bradycardia, tachycardia at rest, SpO2 drops
  * bad events (--invalid-rate): missing fields, impossible values, garbage
    payloads, used to exercise the Lambda validation + quarantine path

Examples
--------
  # Print 10 events to stdout, no AWS call at all
  python simulator.py --dry-run --count 10

  # Send 5 watches, one event per second each, for 5 minutes
  python simulator.py --stream clockdata-dev-events --duration 300
"""

from __future__ import annotations

import argparse
import json
import random
import signal
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

# Kinesis PutRecords accepts at most 500 records per call.
MAX_RECORDS_PER_PUT = 500

# state -> (target heart-rate offset above resting, steps per second range,
#           battery drain in % per hour)
ACTIVITY_PROFILES = {
    "sleeping": (-8, (0, 0), 0.8),
    "resting": (0, (0, 1), 1.0),
    "walking": (35, (1, 2), 1.8),
    "running": (85, (2, 3), 4.0),
}

# Battery time runs faster than wall-clock time so a 5-minute demo shows
# drain and recharge cycles.
BATTERY_TIME_SCALE = 30
CHARGE_PER_HOUR = 60.0

# Markov chain of activity transitions, evaluated once per tick.
TRANSITIONS = {
    "sleeping": {"sleeping": 0.97, "resting": 0.03},
    "resting": {"resting": 0.90, "walking": 0.08, "running": 0.01, "sleeping": 0.01},
    "walking": {"walking": 0.88, "resting": 0.09, "running": 0.03},
    "running": {"running": 0.90, "walking": 0.10},
}


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(ts: datetime) -> str:
    """ISO 8601 with millisecond precision and a Z suffix."""
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"


@dataclass
class Watch:
    device_id: str
    rng: random.Random
    resting_hr: int = 0
    heart_rate: float = 0.0
    spo2: float = 98.0
    battery: float = 100.0
    state: str = "resting"
    charging: bool = False
    _pending_anomaly: int = field(default=0, repr=False)
    _anomaly_kind: str = field(default="", repr=False)

    def __post_init__(self) -> None:
        self.resting_hr = self.rng.randint(55, 75)
        self.heart_rate = float(self.resting_hr + self.rng.randint(-3, 3))
        self.spo2 = round(self.rng.uniform(96.5, 99.0), 1)
        self.battery = float(self.rng.randint(40, 100))

    def _next_state(self) -> str:
        roll, acc = self.rng.random(), 0.0
        for state, prob in TRANSITIONS[self.state].items():
            acc += prob
            if roll < acc:
                return state
        return self.state

    def _start_anomaly(self) -> None:
        # Anomalies last a few ticks so they look like a real episode.
        self._anomaly_kind = self.rng.choice(["bradycardia", "tachycardia", "hypoxemia"])
        self._pending_anomaly = self.rng.randint(2, 5)

    def tick(self, interval_s: float, anomaly_rate: float) -> dict:
        """Advance the watch by one interval and return the resulting event."""
        self.state = self._next_state()
        hr_offset, steps_rate, drain_per_hour = ACTIVITY_PROFILES[self.state]

        if self._pending_anomaly == 0 and anomaly_rate and self.rng.random() < anomaly_rate:
            self._start_anomaly()

        # Heart rate relaxes toward the activity target (exponential smoothing)
        target = self.resting_hr + hr_offset
        self.heart_rate += (target - self.heart_rate) * 0.3 + self.rng.gauss(0, 1.5)
        spo2_target = 97.0 if self.state == "sleeping" else 98.0
        self.spo2 += (spo2_target - self.spo2) * 0.2 + self.rng.gauss(0, 0.3)

        hr_out, spo2_out = self.heart_rate, self.spo2
        if self._pending_anomaly:
            self._pending_anomaly -= 1
            if self._anomaly_kind == "bradycardia":
                hr_out = self.rng.randint(28, 38)
            elif self._anomaly_kind == "tachycardia":
                hr_out = self.rng.randint(185, 215)
            else:
                spo2_out = self.rng.uniform(82.0, 89.0)

        # Steps accumulated during the interval
        lo, hi = steps_rate
        steps = int(round(self.rng.uniform(lo, hi) * interval_s))

        # Battery: drain, or charge back to 100% once it gets low
        if self.charging:
            self.battery = min(100.0, self.battery + CHARGE_PER_HOUR * interval_s / 3600 * BATTERY_TIME_SCALE)
            if self.battery >= 100.0:
                self.charging = False
        else:
            self.battery = max(0.0, self.battery - drain_per_hour * interval_s / 3600 * BATTERY_TIME_SCALE)
            if self.battery <= 5.0:
                self.charging = True

        return {
            "event_id": str(uuid.UUID(int=self.rng.getrandbits(128), version=4)),
            "device_id": self.device_id,
            "timestamp": iso_utc(utc_now()),
            "heart_rate": int(round(max(20, min(250, hr_out)))),
            "steps": steps,
            "spo2": round(max(50.0, min(100.0, spo2_out)), 1),
            "battery": int(round(self.battery)),
        }


def corrupt(event: dict, rng: random.Random) -> dict | str:
    """Turn a good event into one of the bad shapes a real fleet produces."""
    kind = rng.choice(["missing_field", "impossible_hr", "bad_timestamp", "garbage", "negative_steps"])
    bad = dict(event)
    if kind == "missing_field":
        bad.pop(rng.choice(["heart_rate", "device_id", "spo2", "battery"]))
    elif kind == "impossible_hr":
        bad["heart_rate"] = rng.choice([0, 400, -12])
    elif kind == "bad_timestamp":
        bad["timestamp"] = "yesterday-ish"
    elif kind == "negative_steps":
        bad["steps"] = -rng.randint(1, 100)
    else:
        return "\x00not-json{" + uuid.UUID(int=rng.getrandbits(128)).hex
    return bad


def encode(event: dict | str) -> bytes:
    if isinstance(event, str):
        return event.encode("utf-8")
    return json.dumps(event, separators=(",", ":")).encode("utf-8")


def partition_key(event: dict | str) -> str:
    if isinstance(event, dict) and isinstance(event.get("device_id"), str) and event["device_id"]:
        return event["device_id"]
    return "unknown"


class KinesisSink:
    """Batches events into PutRecords calls and retries throttled records."""

    def __init__(self, client, stream_name: str, max_retries: int = 5):
        self.client = client
        self.stream_name = stream_name
        self.max_retries = max_retries
        self.sent = 0
        self.failed = 0

    def send(self, events: list[dict | str]) -> None:
        records = [{"Data": encode(e), "PartitionKey": partition_key(e)} for e in events]
        for start in range(0, len(records), MAX_RECORDS_PER_PUT):
            self._put_with_retry(records[start:start + MAX_RECORDS_PER_PUT])

    def _put_with_retry(self, records: list[dict]) -> None:
        pending = records
        for attempt in range(self.max_retries + 1):
            resp = self.client.put_records(StreamName=self.stream_name, Records=pending)
            if resp.get("FailedRecordCount", 0) == 0:
                self.sent += len(pending)
                return
            # Keep only the records that failed (throttling, internal errors)
            retry = [rec for rec, res in zip(pending, resp["Records"]) if "ErrorCode" in res]
            self.sent += len(pending) - len(retry)
            pending = retry
            if attempt < self.max_retries:
                time.sleep(min(2.0, 0.1 * 2 ** attempt))
        self.failed += len(pending)
        print(f"[WARN] {len(pending)} record(s) dropped after {self.max_retries} retries", file=sys.stderr)


class StdoutSink:
    def __init__(self) -> None:
        self.sent = 0
        self.failed = 0

    def send(self, events: list[dict | str]) -> None:
        for e in events:
            print(e if isinstance(e, str) else json.dumps(e, separators=(",", ":")))
        self.sent += len(events)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="ClockData smartwatch simulator (Kinesis producer)")
    p.add_argument("--stream", help="Kinesis stream name (required unless --dry-run)")
    p.add_argument("--region", default=None, help="AWS region (default: from your AWS config)")
    p.add_argument("--devices", type=int, default=5, help="number of simulated watches (default: 5)")
    p.add_argument("--interval", type=float, default=1.0, help="seconds between two events of a watch (default: 1)")
    p.add_argument("--duration", type=float, default=300, help="stop after N seconds (default: 300, 0 = forever)")
    p.add_argument("--count", type=int, default=0, help="stop after N ticks instead of --duration")
    p.add_argument("--anomaly-rate", type=float, default=0.02, help="probability to start an anomaly episode per tick")
    p.add_argument("--invalid-rate", type=float, default=0.01, help="probability to send a malformed event")
    p.add_argument("--seed", type=int, default=None, help="random seed for reproducible runs")
    p.add_argument("--dry-run", action="store_true", help="print events to stdout, do not call AWS")
    args = p.parse_args(argv)
    if not args.dry_run and not args.stream:
        p.error("--stream is required unless --dry-run is set")
    if args.devices < 1 or args.interval <= 0:
        p.error("--devices must be >= 1 and --interval > 0")
    return args


def make_fleet(n: int, rng: random.Random) -> list[Watch]:
    return [Watch(device_id=f"watch-{i:04d}", rng=random.Random(rng.getrandbits(64))) for i in range(1, n + 1)]


def generate_tick(fleet: list[Watch], interval_s: float, anomaly_rate: float,
                  invalid_rate: float, rng: random.Random) -> list[dict | str]:
    events: list[dict | str] = []
    for watch in fleet:
        event = watch.tick(interval_s, anomaly_rate)
        events.append(corrupt(event, rng) if invalid_rate and rng.random() < invalid_rate else event)
    return events


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    rng = random.Random(args.seed)
    fleet = make_fleet(args.devices, rng)

    if args.dry_run:
        sink = StdoutSink()
    else:
        import boto3  # imported lazily so --dry-run works without boto3

        sink = KinesisSink(boto3.client("kinesis", region_name=args.region), args.stream)
        print(f"[INFO] Sending {args.devices} watch(es) to '{args.stream}' every {args.interval}s. Ctrl+C to stop.",
              file=sys.stderr)

    running = True

    def stop(*_):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, stop)
    signal.signal(signal.SIGTERM, stop)

    started, ticks = time.monotonic(), 0
    while running:
        tick_start = time.monotonic()
        sink.send(generate_tick(fleet, args.interval, args.anomaly_rate, args.invalid_rate, rng))
        ticks += 1
        if args.count and ticks >= args.count:
            break
        if not args.count and args.duration and time.monotonic() - started >= args.duration:
            break
        if not args.dry_run:
            if ticks % 10 == 0:
                print(f"[INFO] {sink.sent} events sent, {sink.failed} failed", file=sys.stderr)
            time.sleep(max(0.0, args.interval - (time.monotonic() - tick_start)))

    print(f"[INFO] Done: {sink.sent} events sent, {sink.failed} failed.", file=sys.stderr)
    return 0 if sink.failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
