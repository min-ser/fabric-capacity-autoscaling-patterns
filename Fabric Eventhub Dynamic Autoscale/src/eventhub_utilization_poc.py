
#!/usr/bin/env python3
"""Fabric Capacity Analytics CLI.

A read-only reference implementation for analyzing Microsoft Fabric Capacity
Summary events from Azure Event Hubs or local JSONL files.

Commands:
    sample  - Generate synthetic Capacity Summary events.
    collect - Read Capacity Summary events from Azure Event Hubs.
    analyze - Validate data quality and evaluate rolling utilization.

This tool never modifies a Fabric Capacity SKU.
"""

import argparse
import json
import math
import os
import sys
import threading
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

UTC = timezone.utc
EVENT_TYPE = "Microsoft.Fabric.Capacity.Summary"


@dataclass(frozen=True)
class CapacityEvent:
    capacity: str
    start: datetime
    end: datetime
    consumed_cu_ms: float
    base_units: float
    sku: str

    @property
    def duration_seconds(self) -> float:
        return (self.end - self.start).total_seconds()

    @property
    def budget_cu_ms(self) -> float:
        return self.base_units * self.duration_seconds * 1000

    @property
    def utilization(self) -> float:
        return self.consumed_cu_ms / self.budget_cu_ms * 100


@dataclass(frozen=True)
class AnalysisConfig:
    window_minutes: int = 10
    step_minutes: int = 1
    required_steps: int = 5
    scale_up_threshold: float = 80.0
    scale_down_threshold: float = 40.0
    min_coverage: float = 0.95
    max_gap_seconds: float = 60.0
    aggregation: str = "arithmetic"

    @property
    def lookback_minutes(self) -> int:
        return self.window_minutes + (
            self.required_steps - 1
        ) * self.step_minutes

    def validate(self) -> None:
        if min(
            self.window_minutes,
            self.step_minutes,
            self.required_steps,
        ) <= 0:
            raise ValueError("Window, step and required steps must be positive")
        if not 0 < self.min_coverage <= 1:
            raise ValueError("min_coverage must be in (0, 1]")
        if self.max_gap_seconds < 0:
            raise ValueError("max_gap_seconds cannot be negative")
        if self.scale_down_threshold >= self.scale_up_threshold:
            raise ValueError("Scale-down threshold must be below scale-up")
        if self.aggregation not in ("arithmetic", "weighted"):
            raise ValueError("Unsupported aggregation method")


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include timezone information")
    return parsed.astimezone(UTC)


def iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def parse_capacity_event(payload: dict[str, Any]) -> CapacityEvent:
    if payload.get("type") == EVENT_TYPE:
        data = payload.get("data")
    else:
        data = payload

    if not isinstance(data, dict):
        raise ValueError("Invalid event payload")

    start = parse_time(data["windowStartTime"])
    end = parse_time(data["windowEndTime"])
    consumed = float(data["capacityUnitMs"])
    units = float(data["baseCapacityUnits"])

    if end <= start:
        raise ValueError("Invalid event time interval")
    if not all(math.isfinite(v) for v in (consumed, units)):
        raise ValueError("Non-finite capacity metric")
    if consumed < 0 or units <= 0:
        raise ValueError("Invalid capacity metric")

    return CapacityEvent(
        capacity=str(data["capacityName"]),
        start=start,
        end=end,
        consumed_cu_ms=consumed,
        base_units=units,
        sku=str(data.get("capacitySku", "")),
    )


def load_jsonl(path: Path) -> tuple[list[CapacityEvent], dict[str, int]]:
    events = []
    stats = {"records": 0, "invalid": 0}

    with path.open("r", encoding="utf-8") as stream:
        for line in stream:
            if not line.strip():
                continue
            stats["records"] += 1
            try:
                payload = json.loads(line)
                events.append(parse_capacity_event(payload))
            except (ValueError, TypeError, KeyError, json.JSONDecodeError):
                stats["invalid"] += 1

    return events, stats


def deduplicate(events: list[CapacityEvent]) -> list[CapacityEvent]:
    unique = {}

    for event in events:
        key = (event.capacity, event.start, event.end)
        unique[key] = event

    return sorted(
        unique.values(),
        key=lambda item: (item.start, item.end),
    )


def calculate_coverage(
    events: list[CapacityEvent],
    start: datetime,
    end: datetime,
) -> tuple[float, float]:
    intervals = []

    for event in events:
        left = max(event.start, start)
        right = min(event.end, end)
        if right > left:
            intervals.append((left, right))

    intervals.sort()
    cursor = start
    covered = 0.0
    largest_gap = 0.0

    for left, right in intervals:
        if left > cursor:
            largest_gap = max(
                largest_gap,
                (left - cursor).total_seconds(),
            )

        segment_start = max(left, cursor)
        if right > segment_start:
            covered += (right - segment_start).total_seconds()

        cursor = max(cursor, right)

    if cursor < end:
        largest_gap = max(
            largest_gap,
            (end - cursor).total_seconds(),
        )

    duration = (end - start).total_seconds()
    return covered / duration if duration else 0.0, largest_gap


def calculate_average(
    events: list[CapacityEvent],
    method: str,
) -> float | None:
    if not events:
        return None

    if method == "weighted":
        consumed = sum(event.consumed_cu_ms for event in events)
        budget = sum(event.budget_cu_ms for event in events)
        return consumed / budget * 100 if budget else None

    return sum(event.utilization for event in events) / len(events)


def evaluate_window(
    events: list[CapacityEvent],
    start: datetime,
    end: datetime,
    config: AnalysisConfig,
) -> dict[str, Any]:
    selected = [
        event for event in events
        if start < event.end <= end
    ]

    coverage, max_gap = calculate_coverage(selected, start, end)
    average = calculate_average(selected, config.aggregation)

    valid = (
        bool(selected)
        and coverage >= config.min_coverage
        and max_gap <= config.max_gap_seconds
    )

    return {
        "start": iso(start),
        "end": iso(end),
        "event_count": len(selected),
        "coverage_pct": round(coverage * 100, 2),
        "max_gap_seconds": round(max_gap, 2),
        "average_utilization_pct": (
            round(average, 4) if average is not None else None
        ),
        "valid": valid,
    }


def analyze(
    events: list[CapacityEvent],
    capacity: str,
    evaluation_end: datetime,
    config: AnalysisConfig,
) -> dict[str, Any]:
    config.validate()

    filtered = deduplicate([
        event for event in events
        if event.capacity == capacity
    ])

    windows = []

    for index in range(config.required_steps):
        offset = (
            config.required_steps - index - 1
        ) * config.step_minutes

        end = evaluation_end - timedelta(minutes=offset)
        start = end - timedelta(minutes=config.window_minutes)

        window = evaluate_window(filtered, start, end, config)
        window["step"] = index + 1
        windows.append(window)

    if not all(window["valid"] for window in windows):
        decision = "SKIP"
    elif all(
        window["average_utilization_pct"] > config.scale_up_threshold
        for window in windows
    ):
        decision = "SCALE_UP_CANDIDATE"
    elif all(
        window["average_utilization_pct"] < config.scale_down_threshold
        for window in windows
    ):
        decision = "SCALE_DOWN_CANDIDATE"
    else:
        decision = "MAINTAIN"

    return {
        "capacity": capacity,
        "evaluation_end": iso(evaluation_end),
        "decision": decision,
        "read_only": True,
        "config": asdict(config),
        "unique_events": len(filtered),
        "windows": windows,
    }


def make_sample(
    output: Path,
    capacity: str,
    utilization: float,
    duration_minutes: int,
    interval_seconds: int,
) -> None:
    if not 0 <= utilization <= 1000:
        raise ValueError("Sample utilization must be between 0 and 1000")
    if duration_minutes <= 0 or interval_seconds <= 0:
        raise ValueError("Sample duration and interval must be positive")

    end = datetime.now(UTC).replace(second=0, microsecond=0)
    start = end - timedelta(minutes=duration_minutes)
    current = start
    count = 0

    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8") as stream:
        while current < end:
            next_time = min(
                current + timedelta(seconds=interval_seconds),
                end,
            )
            seconds = (next_time - current).total_seconds()
            units = 16
            budget = units * seconds * 1000

            payload = {
                "type": EVENT_TYPE,
                "data": {
                    "capacityName": capacity,
                    "capacitySku": "F16",
                    "baseCapacityUnits": units,
                    "capacityUnitMs": budget * utilization / 100,
                    "windowStartTime": iso(current),
                    "windowEndTime": iso(next_time),
                },
            }

            stream.write(json.dumps(payload) + "\n")
            current = next_time
            count += 1

    print(f"Generated {count} synthetic events: {output}")


def collect_eventhub(
    output: Path,
    capacity: str,
    lookback_minutes: int,
    timeout_seconds: int,
) -> None:
    from azure.eventhub import EventHubConsumerClient, TransportType
    from azure.identity import DefaultAzureCredential

    namespace = os.environ["EVENTHUB_NAMESPACE"]
    eventhub_name = os.environ["EVENTHUB_NAME"]
    consumer_group = os.getenv("EVENTHUB_CONSUMER_GROUP", "$Default")

    end = datetime.now(UTC).replace(second=0, microsecond=0)
    start = end - timedelta(minutes=lookback_minutes)
    collected = []
    receive_errors = []

    credential = DefaultAzureCredential()
    client = EventHubConsumerClient(
        fully_qualified_namespace=namespace,
        eventhub_name=eventhub_name,
        consumer_group=consumer_group,
        credential=credential,
        transport_type=TransportType.AmqpOverWebsocket,
    )

    def on_batch(partition_context, batch):
        for event in batch:
            try:
                payload = event.body_as_json()
                if isinstance(payload, str):
                    payload = json.loads(payload)

                record = parse_capacity_event(payload)

                if (
                    record.capacity == capacity
                    and start < record.end <= end
                ):
                    collected.append(payload)
            except (ValueError, TypeError, KeyError):
                continue

    def on_error(partition_context, error):
        receive_errors.append(str(error))

    timer = threading.Timer(timeout_seconds, client.close)
    timer.daemon = True

    try:
        timer.start()

        with client:
            client.receive_batch(
                on_event_batch=on_batch,
                starting_position=start,
                starting_position_inclusive=True,
                max_batch_size=100,
                max_wait_time=5,
                on_error=on_error,
            )

    except Exception:
        if timer.is_alive():
            raise
    finally:
        timer.cancel()
        client.close()
        credential.close()

    if receive_errors:
        raise RuntimeError("Event Hub collection reported errors")

    output.parent.mkdir(parents=True, exist_ok=True)

    with output.open("w", encoding="utf-8") as stream:
        for payload in collected:
            stream.write(json.dumps(payload) + "\n")

    print(f"Collected {len(collected)} events: {output}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Read-only Microsoft Fabric Capacity analytics"
    )
    commands = parser.add_subparsers(dest="command", required=True)

    sample = commands.add_parser("sample")
    sample.add_argument("--output", type=Path, default=Path("sample.jsonl"))
    sample.add_argument("--capacity", default="demo-capacity")
    sample.add_argument("--utilization", type=float, default=85.0)
    sample.add_argument("--minutes", type=int, default=14)
    sample.add_argument("--interval-seconds", type=int, default=30)

    collect = commands.add_parser("collect")
    collect.add_argument("--output", type=Path, default=Path("events.jsonl"))
    collect.add_argument("--capacity", required=True)
    collect.add_argument("--lookback-minutes", type=int, default=14)
    collect.add_argument("--timeout-seconds", type=int, default=30)

    analysis = commands.add_parser("analyze")
    analysis.add_argument("--input", type=Path, required=True)
    analysis.add_argument("--capacity", required=True)
    analysis.add_argument("--output", type=Path)
    analysis.add_argument("--evaluation-end")
    analysis.add_argument("--window-minutes", type=int, default=10)
    analysis.add_argument("--step-minutes", type=int, default=1)
    analysis.add_argument("--required-steps", type=int, default=5)
    analysis.add_argument("--scale-up", type=float, default=80.0)
    analysis.add_argument("--scale-down", type=float, default=40.0)
    analysis.add_argument("--min-coverage", type=float, default=0.95)
    analysis.add_argument("--max-gap-seconds", type=float, default=60.0)
    analysis.add_argument(
        "--aggregation",
        choices=["arithmetic", "weighted"],
        default="arithmetic",
    )

    return parser


def main() -> int:
    args = build_parser().parse_args()

    try:
        if args.command == "sample":
            make_sample(
                args.output,
                args.capacity,
                args.utilization,
                args.minutes,
                args.interval_seconds,
            )
            return 0

        if args.command == "collect":
            collect_eventhub(
                args.output,
                args.capacity,
                args.lookback_minutes,
                args.timeout_seconds,
            )
            return 0

        if args.command == "analyze":
            config = AnalysisConfig(
                window_minutes=args.window_minutes,
                step_minutes=args.step_minutes,
                required_steps=args.required_steps,
                scale_up_threshold=args.scale_up,
                scale_down_threshold=args.scale_down,
                min_coverage=args.min_coverage,
                max_gap_seconds=args.max_gap_seconds,
                aggregation=args.aggregation,
            )

            events, stats = load_jsonl(args.input)

            evaluation_end = (
                parse_time(args.evaluation_end)
                if args.evaluation_end
                else datetime.now(UTC).replace(second=0, microsecond=0)
            )

            result = analyze(
                events,
                args.capacity,
                evaluation_end,
                config,
            )

            result["input_stats"] = stats
            output_json = json.dumps(result, indent=2, ensure_ascii=False)

            print(output_json)

            if args.output:
                args.output.parent.mkdir(parents=True, exist_ok=True)
                args.output.write_text(output_json + "\n", encoding="utf-8")

            return 0

        return 1

    except Exception as exc:
        print(f"ERROR: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())