
#!/usr/bin/env python3
"""Configurable Fabric Capacity policy controller (read-only by default).

Requires: azure-identity, azure-eventhub, requests
Configuration: JSON file; see example in accompanying response.
"""
import argparse
import json
import logging
import math
import os
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from azure.eventhub import EventHubConsumerClient, TransportType
from azure.identity import DefaultAzureCredential

LOG = logging.getLogger("fabric-policy")
UTC = timezone.utc
EVENT_TYPE = "Microsoft.Fabric.Capacity.Summary"
ARM_SCOPE = "https://management.azure.com/.default"
ARM_API_VERSION = "2023-11-01"


@dataclass(frozen=True)
class Settings:
    subscription_id: str
    resource_group: str
    capacity_name: str
    eventhub_namespace: str
    eventhub_name: str
    consumer_group: str
    timezone_name: str
    weekdays: tuple[int, ...]
    start_time: str
    end_time: str
    window_minutes: int
    step_minutes: int
    required_steps: int
    scale_up_threshold: float
    scale_down_threshold: float
    min_coverage: float
    max_gap_seconds: float
    min_events_per_window: int
    eventhub_receive_seconds: int
    sku_ladder: tuple[str, ...]
    minimum_sku: str
    maximum_sku: str
    webhook_url_env: str
    verify_timeout_seconds: int
    verify_interval_seconds: int

    @property
    def lookback_minutes(self) -> int:
        return self.window_minutes + (self.required_steps - 1) * self.step_minutes

    @classmethod
    def load(cls, path: Path) -> "Settings":
        raw = json.loads(path.read_text(encoding="utf-8"))
        azure = raw["azure"]
        eventhub = raw["eventhub"]
        schedule = raw["schedule"]
        rule = raw["rule"]
        policy = raw["sku_policy"]
        notify = raw.get("notifications", {})
        settings = cls(
            subscription_id=os.getenv("AZURE_SUBSCRIPTION_ID", azure.get("subscription_id", "")),
            resource_group=os.getenv("FABRIC_RESOURCE_GROUP", azure.get("resource_group", "")),
            capacity_name=os.getenv("FABRIC_CAPACITY_NAME", azure.get("capacity_name", "")),
            eventhub_namespace=os.getenv("EVENTHUB_NAMESPACE", eventhub.get("namespace", "")),
            eventhub_name=os.getenv("EVENTHUB_NAME", eventhub.get("name", "")),
            consumer_group=os.getenv("EVENTHUB_CONSUMER_GROUP", eventhub.get("consumer_group", "$Default")),
            timezone_name=schedule.get("timezone", "Asia/Seoul"),
            weekdays=tuple(schedule.get("weekdays", [0, 1, 2, 3, 4])),
            start_time=schedule.get("start", "08:15"),
            end_time=schedule.get("end", "17:45"),
            window_minutes=int(rule.get("window_minutes", 10)),
            step_minutes=int(rule.get("step_minutes", 1)),
            required_steps=int(rule.get("required_steps", 5)),
            scale_up_threshold=float(rule.get("scale_up_threshold", 80)),
            scale_down_threshold=float(rule.get("scale_down_threshold", 40)),
            min_coverage=float(rule.get("min_coverage", 0.95)),
            max_gap_seconds=float(rule.get("max_gap_seconds", 60)),
            min_events_per_window=int(rule.get("min_events_per_window", 20)),
            eventhub_receive_seconds=int(eventhub.get("receive_seconds", 30)),
            sku_ladder=tuple(policy.get("ladder", ["F2", "F4", "F8", "F16", "F32", "F64"])),
            minimum_sku=policy["minimum"],
            maximum_sku=policy["maximum"],
            webhook_url_env=notify.get("webhook_url_env", "TEAMS_WEBHOOK_URL"),
            verify_timeout_seconds=int(raw.get("verify", {}).get("timeout_seconds", 300)),
            verify_interval_seconds=int(raw.get("verify", {}).get("interval_seconds", 15)),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not all((self.subscription_id, self.resource_group, self.capacity_name,
                    self.eventhub_namespace, self.eventhub_name, self.consumer_group)):
            raise ValueError("Missing Azure or Event Hub configuration")
        if self.window_minutes < 1 or self.step_minutes < 1 or self.required_steps < 1:
            raise ValueError("Window, step and evaluation count must be positive")
        if self.scale_down_threshold >= self.scale_up_threshold:
            raise ValueError("Scale-down threshold must be below scale-up threshold")
        if not 0 < self.min_coverage <= 1 or self.max_gap_seconds < 0:
            raise ValueError("Invalid data quality thresholds")
        if self.min_events_per_window < 1 or self.eventhub_receive_seconds < 1:
            raise ValueError("Invalid event collection settings")
        if len(set(self.sku_ladder)) != len(self.sku_ladder):
            raise ValueError("SKU ladder contains duplicates")
        if self.minimum_sku not in self.sku_ladder or self.maximum_sku not in self.sku_ladder:
            raise ValueError("SKU policy must refer to entries in the ladder")
        if self.sku_ladder.index(self.minimum_sku) > self.sku_ladder.index(self.maximum_sku):
            raise ValueError("Minimum SKU exceeds maximum SKU")
        if any(day not in range(7) for day in self.weekdays):
            raise ValueError("Invalid weekday")
        ZoneInfo(self.timezone_name)
        start = datetime.strptime(self.start_time, "%H:%M").time()
        end = datetime.strptime(self.end_time, "%H:%M").time()
        if start >= end:
            raise ValueError("Schedule must be within a single day")
        if self.verify_timeout_seconds < 1 or self.verify_interval_seconds < 1:
            raise ValueError("Invalid verification settings")


@dataclass(frozen=True)
class UsageEvent:
    start: datetime
    end: datetime
    usage: float


def parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("Event timestamp has no timezone")
    return parsed.astimezone(UTC)


def decode_event(payload: Any, capacity_name: str) -> UsageEvent | None:
    if not isinstance(payload, dict) or payload.get("type") != EVENT_TYPE:
        return None
    data = payload.get("data")
    if not isinstance(data, dict) or data.get("capacityName") != capacity_name:
        return None
    try:
        start = parse_time(data["windowStartTime"])
        end = parse_time(data["windowEndTime"])
        units = float(data["baseCapacityUnits"])
        consumed = float(data["capacityUnitMs"])
        duration = (end - start).total_seconds()
        if not all(map(math.isfinite, (units, consumed, duration))):
            return None
        if units <= 0 or consumed < 0 or duration <= 0:
            return None
        return UsageEvent(start, end, consumed / (units * duration * 1000) * 100)
    except (KeyError, TypeError, ValueError, OverflowError):
        return None


def in_schedule(settings: Settings, instant: datetime) -> bool:
    local = instant.astimezone(ZoneInfo(settings.timezone_name))
    return (local.weekday() in settings.weekdays
            and settings.start_time <= local.strftime("%H:%M") < settings.end_time)


def load_events_file(path: Path, capacity_name: str) -> list[UsageEvent]:
    result = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            event = decode_event(json.loads(line), capacity_name)
        except (ValueError, TypeError):
            continue
        if event:
            result.append(event)
    return result


def collect_eventhub(settings: Settings, credential: DefaultAzureCredential,
                     evaluation_end: datetime) -> list[UsageEvent]:
    """Bounded Event Hubs read using an SDK callback with an explicit deadline.

    A receiver is closed after its deadline, including on idle partitions.
    Partition errors are surfaced instead of being interpreted as low usage.
    """
    from threading import Event, Thread
    start = evaluation_end - timedelta(minutes=settings.lookback_minutes)
    unique: dict[tuple[datetime, datetime], UsageEvent] = {}
    errors: list[str] = []
    stopped = Event()
    client = EventHubConsumerClient(
        fully_qualified_namespace=settings.eventhub_namespace,
        eventhub_name=settings.eventhub_name,
        consumer_group=settings.consumer_group,
        credential=credential,
        transport_type=TransportType.AmqpOverWebsocket,
    )

    def on_batch(partition_context: Any, batch: list[Any]) -> None:
        for item in batch:
            try:
                payload = item.body_as_json()
                if isinstance(payload, str):
                    payload = json.loads(payload)
                event = decode_event(payload, settings.capacity_name)
                if event and start < event.end <= evaluation_end:
                    unique[(event.start, event.end)] = event
            except (ValueError, TypeError, UnicodeDecodeError):
                continue

    def on_error(partition_context: Any, error: Exception) -> None:
        if not stopped.is_set():
            errors.append(f"Event Hub receive error: {type(error).__name__}")

    def close_at_deadline() -> None:
        if not stopped.wait(settings.eventhub_receive_seconds):
            stopped.set()
            try:
                client.close()
            except Exception:
                pass

    closer = Thread(target=close_at_deadline, daemon=True)
    closer.start()
    try:
        with client:
            client.receive_batch(
                on_event_batch=on_batch,
                starting_position=start,
                starting_position_inclusive=True,
                max_batch_size=100,
                max_wait_time=5,
                on_error=on_error,
            )
    except Exception as exc:
        if not stopped.is_set():
            raise RuntimeError("Event Hub collection failed") from exc
    finally:
        stopped.set()
        client.close()
        closer.join(timeout=2)
    if errors:
        raise RuntimeError("; ".join(errors))
    return sorted(unique.values(), key=lambda item: item.end)


def coverage_and_gap(events: list[UsageEvent], start: datetime,
                     end: datetime) -> tuple[float, float]:
    intervals = sorted((max(item.start, start), min(item.end, end))
                       for item in events if item.end > start and item.start < end)
    cursor = start
    covered = 0.0
    largest_gap = 0.0
    for left, right in intervals:
        if left > cursor:
            largest_gap = max(largest_gap, (left - cursor).total_seconds())
        if right > cursor:
            covered += (right - max(left, cursor)).total_seconds()
            cursor = right
    largest_gap = max(largest_gap, (end - cursor).total_seconds())
    return covered / (end - start).total_seconds(), largest_gap


def analyze_windows(settings: Settings, events: list[UsageEvent],
                    evaluation_end: datetime) -> tuple[str, list[dict[str, Any]]]:
    unique = {(item.start, item.end): item for item in events}
    ordered = sorted(unique.values(), key=lambda item: item.end)
    windows = []
    for step in range(settings.required_steps):
        end = evaluation_end - timedelta(minutes=(settings.required_steps - step - 1) * settings.step_minutes)
        start = end - timedelta(minutes=settings.window_minutes)
        selected = [item for item in ordered if start < item.end <= end]
        coverage, gap = coverage_and_gap(selected, start, end)
        average = sum(item.usage for item in selected) / len(selected) if selected else None
        valid = (len(selected) >= settings.min_events_per_window
                 and coverage >= settings.min_coverage
                 and gap <= settings.max_gap_seconds)
        windows.append({
            "step": step + 1,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "events": len(selected),
            "coverage_percent": round(coverage * 100, 2),
            "largest_gap_seconds": round(gap, 2),
            "average_percent": round(average, 4) if average is not None else None,
            "valid": valid,
        })
    if not all(item["valid"] for item in windows):
        return "SKIP", windows
    if all(item["average_percent"] > settings.scale_up_threshold for item in windows):
        return "UP", windows
    if all(item["average_percent"] < settings.scale_down_threshold for item in windows):
        return "DOWN", windows
    return "HOLD", windows


def select_target(settings: Settings, current: str, signal: str) -> tuple[str, str]:
    ladder = settings.sku_ladder
    if current not in ladder:
        return "SKIP", current
    index = ladder.index(current)
    lower = ladder.index(settings.minimum_sku)
    upper = ladder.index(settings.maximum_sku)
    if signal == "UP" and index < upper:
        return "SCALE_UP", ladder[index + 1]
    if signal == "DOWN" and index > lower:
        return "SCALE_DOWN", ladder[index - 1]
    return ("SKIP" if signal == "SKIP" else "MAINTAIN"), current


class ArmCapacity:
    def __init__(self, settings: Settings, credential: DefaultAzureCredential):
        self.settings = settings
        self.credential = credential
        self.url = (
            f"https://management.azure.com/subscriptions/{settings.subscription_id}"
            f"/resourceGroups/{settings.resource_group}/providers/Microsoft.Fabric"
            f"/capacities/{settings.capacity_name}?api-version={ARM_API_VERSION}"
        )
        self.session = requests.Session()

    def request(self, method: str, *, payload: dict | None = None) -> requests.Response:
        token = self.credential.get_token(ARM_SCOPE).token
        response = self.session.request(
            method, self.url,
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=payload,
            timeout=45,
        )
        response.raise_for_status()
        return response

    def status(self) -> tuple[str, str, str]:
        body = self.request("GET").json()
        return (
            str(body.get("sku", {}).get("name", "")).upper(),
            str(body.get("properties", {}).get("state", "Unknown")),
            str(body.get("properties", {}).get("provisioningState", "Unknown")),
        )

    def change(self, target: str) -> int:
        response = self.request("PATCH", payload={"sku": {"name": target, "tier": "Fabric"}})
        if response.status_code not in (200, 201, 202):
            raise RuntimeError(f"Unexpected ARM status {response.status_code}")
        return response.status_code

    def verify(self, target: str) -> bool:
        deadline = time.monotonic() + self.settings.verify_timeout_seconds
        while time.monotonic() < deadline:
            sku, state, provisioning = self.status()
            if sku == target and state.lower() == "active" and provisioning.lower() == "succeeded":
                return True
            time.sleep(min(self.settings.verify_interval_seconds,
                           max(0, deadline - time.monotonic())))
        return False

    def close(self) -> None:
        self.session.close()


def notify(settings: Settings, outcome: dict[str, Any]) -> None:
    url = os.getenv(settings.webhook_url_env, "")
    if not url:
        return
    # MessageCard is supported by some legacy incoming webhooks; confirm
    # payload type for your Teams Workflow before enabling notifications.
    card = {"text": "Fabric Capacity policy controller: " + json.dumps(outcome, ensure_ascii=False)}
    try:
        response = requests.post(url, json=card, timeout=15)
        response.raise_for_status()
    except requests.RequestException:
        LOG.warning("Notification delivery failed")


def run(settings: Settings, *, apply: bool, offline: Path | None,
        ignore_schedule: bool, evaluation_end: datetime | None) -> dict[str, Any]:
    instant = datetime.now(UTC)
    end = evaluation_end or instant.replace(second=0, microsecond=0)
    outcome: dict[str, Any] = {
        "mode": "APPLY" if apply else "DRY_RUN",
        "evaluation_end": end.isoformat(),
        "capacity": settings.capacity_name,
    }
    if not ignore_schedule and not in_schedule(settings, instant):
        return {**outcome, "action": "SKIP", "reason": "Outside operating schedule"}
    if apply and (offline is not None or ignore_schedule or evaluation_end is not None):
        raise ValueError("Apply mode cannot use offline data or bypass schedule/current time")
    credential = DefaultAzureCredential()
    arm = ArmCapacity(settings, credential)
    try:
        current, state, provisioning = arm.status()
        outcome.update({"current_sku": current, "state": state, "provisioning_state": provisioning})
        if state.lower() != "active" or provisioning.lower() not in ("succeeded", "unknown"):
            return {**outcome, "action": "SKIP", "reason": "Capacity is not ready"}
        if current not in settings.sku_ladder:
            return {**outcome, "action": "SKIP", "reason": "Unsupported SKU"}
        events = (load_events_file(offline, settings.capacity_name) if offline
                  else collect_eventhub(settings, credential, end))
        signal, windows = analyze_windows(settings, events, end)
        action, target = select_target(settings, current, signal)
        outcome.update({"signal": signal, "windows": windows, "action": action, "target_sku": target})
        if action not in ("SCALE_UP", "SCALE_DOWN"):
            return outcome
        if not apply:
            return {**outcome, "reason": "Dry run; no PATCH sent"}
        if not in_schedule(settings, datetime.now(UTC)):
            return {**outcome, "action": "SKIP", "reason": "Operating window ended before PATCH"}
        latest_sku, latest_state, latest_provisioning = arm.status()
        if (latest_sku != current or latest_state.lower() != "active"
                or latest_provisioning.lower() not in ("succeeded", "unknown")):
            return {**outcome, "action": "SKIP", "reason": "Capacity changed before PATCH"}
        status = arm.change(target)
        outcome["arm_http_status"] = status
        outcome["verified"] = arm.verify(target)
        outcome["result"] = "COMPLETED" if outcome["verified"] else "NOT_CONFIRMED"
        notify(settings, outcome)
        return outcome
    finally:
        arm.close()
        credential.close()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--apply", action="store_true", help="Enable real ARM PATCH (default: dry-run)")
    parser.add_argument("--offline-events", type=Path, help="JSONL events for analysis testing")
    parser.add_argument("--ignore-schedule", action="store_true", help="Dry-run testing only")
    parser.add_argument("--evaluation-end", help="ISO 8601 UTC time for dry-run testing")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    try:
        settings = Settings.load(args.config)
        end = parse_time(args.evaluation_end) if args.evaluation_end else None
        result = run(settings, apply=args.apply, offline=args.offline_events,
                     ignore_schedule=args.ignore_schedule, evaluation_end=end)
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 1 if result.get("result") == "NOT_CONFIRMED" else 0
    except Exception as exc:
        LOG.error("Controller failed: %s: %s", type(exc).__name__, exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
