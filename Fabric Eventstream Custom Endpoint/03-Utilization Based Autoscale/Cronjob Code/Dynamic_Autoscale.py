#!/usr/bin/env python3
"""
Microsoft Fabric Capacity utilization-based dynamic autoscaler.

Purpose
-------
Fabric Eventstream Custom Endpoint를 통해 Capacity Summary Event를 조회하고,
Historical Event 기반 Rolling Average를 계산하여 업무시간 중
Fabric Capacity SKU를 동적으로 조정한다.

Dynamic Control Window:
    Monday ~ Friday
    08:15 ~ 17:45 KST

Dynamic SKU Range:
    STG : F8  <-> F16
    PRD : F16 <-> F32

Activator-compatible Rule:
    Summarization : Average
    Window Size   : 10 minutes
    Step Size     : 1 minute
    Time Elapsed  : 5 minutes

Scale Up:
    10-minute Average Utilization > 80%
    continuously for 5 minutes

Scale Down:
    10-minute Average Utilization < 40%
    continuously for 5 minutes

Authentication:
    AKS Workload Identity
    DefaultAzureCredential

Transport:
    AMQP over WebSocket / TCP 443

Author : 김민서 (MEGAZONECLOUD)
Created: 2026-07-30
"""

import json
import os
import ssl
import sys
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any

from azure.eventhub import EventHubConsumerClient, TransportType
from azure.identity import DefaultAzureCredential


# ============================================================
# 1. Configuration
# ============================================================

ENV_NAME = os.getenv("ENV", "").strip().lower()
SUBSCRIPTION_ID = os.getenv("subscriptionId")
RESOURCE_GROUP = os.getenv("resourceGroup")
CAPACITY_NAME = os.getenv("capacityName")

EVENTHUB_NAMESPACE = os.getenv("Fabric-Event-Hub-namespace-endpoint")
EVENTHUB_NAME = os.getenv("Fabric-Event-Hub-name")
CONSUMER_GROUP = os.getenv("Fabric-Event-Hub-Consumer-group")

TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")

ARM_API_VERSION = "2023-11-01"
KST = timezone(timedelta(hours=9))


# ============================================================
# 2. Dynamic Autoscale Window
# ============================================================

DYNAMIC_START_HOUR = 8
DYNAMIC_START_MINUTE = 15
DYNAMIC_END_HOUR = 17
DYNAMIC_END_MINUTE = 45


# ============================================================
# 3. Activator-compatible Rule
# ============================================================

WINDOW_MINUTES = 10
STEP_MINUTES = 1
TIME_ELAPSED_MINUTES = 5

UTIL_UP_THRESHOLD = 80.0
UTIL_DOWN_THRESHOLD = 40.0

# Capacity Summary Event 기본 Window
EVENT_WINDOW_SECONDS = 30

# 10분 Rolling Window + 5분 지속조건 계산을 위해
# 여유를 포함하여 최근 20분 Event를 조회
LOOKBACK_MINUTES = 20

# Event Hub backlog 소진 여부 확인 대기시간
MAX_WAIT_SECONDS = 10


# ============================================================
# 4. Dynamic SKU Policy
# ============================================================

DYNAMIC_SKU_POLICY = {
    "stg": {"min": "F8", "max": "F16"},
    "prd": {"min": "F16", "max": "F32"},
}


# ============================================================
# 5. Common Helpers
# ============================================================

def print_section(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        # Fabric timestamp 예:
        # 2026-07-30T04:48:00.0000000Z
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"

        return datetime.fromisoformat(value)

    except ValueError:
        return None


# ============================================================
# 6. Configuration Validation
# ============================================================

def validate_config() -> None:
    required = {
        "ENV": ENV_NAME,
        "subscriptionId": SUBSCRIPTION_ID,
        "resourceGroup": RESOURCE_GROUP,
        "capacityName": CAPACITY_NAME,
        "Fabric-Event-Hub-namespace-endpoint": EVENTHUB_NAMESPACE,
        "Fabric-Event-Hub-name": EVENTHUB_NAME,
        "Fabric-Event-Hub-Consumer-group": CONSUMER_GROUP,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing)
        )

    if ENV_NAME not in DYNAMIC_SKU_POLICY:
        raise RuntimeError(
            "Dynamic Autoscale supports only STG and PRD environments. "
            f"Current ENV={ENV_NAME!r}"
        )

    if UTIL_DOWN_THRESHOLD >= UTIL_UP_THRESHOLD:
        raise RuntimeError(
            "UTIL_DOWN_THRESHOLD must be lower than UTIL_UP_THRESHOLD."
        )


# ============================================================
# 7. Dynamic Window
# ============================================================

def is_dynamic_autoscale_window(now_kst: datetime) -> bool:
    # Monday(0) ~ Friday(4)
    if now_kst.weekday() >= 5:
        return False

    dynamic_start = now_kst.replace(
        hour=DYNAMIC_START_HOUR,
        minute=DYNAMIC_START_MINUTE,
        second=0,
        microsecond=0,
    )

    dynamic_end = now_kst.replace(
        hour=DYNAMIC_END_HOUR,
        minute=DYNAMIC_END_MINUTE,
        second=0,
        microsecond=0,
    )

    return dynamic_start <= now_kst < dynamic_end


# ============================================================
# 8. Utilization
# ============================================================

def calculate_utilization(data: dict) -> float:
    """
    Utilization (%)

                      capacityUnitMs
    -------------------------------------------------- x 100
    baseCapacityUnits * 1000 * Event Window Seconds
    """

    capacity_unit_ms = float(data.get("capacityUnitMs", 0))
    base_capacity_units = float(data.get("baseCapacityUnits", 0))

    capacity_budget_ms = (
        base_capacity_units
        * 1000
        * EVENT_WINDOW_SECONDS
    )

    if capacity_budget_ms <= 0:
        raise ValueError(
            "Capacity budget must be greater than zero."
        )

    return (
        capacity_unit_ms
        / capacity_budget_ms
        * 100
    )


# ============================================================
# 9. ARM Helpers
# ============================================================

def build_arm_url() -> str:
    resource_id = (
        f"/subscriptions/{SUBSCRIPTION_ID}"
        f"/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/Microsoft.Fabric"
        f"/capacities/{CAPACITY_NAME}"
    )

    return (
        f"https://management.azure.com"
        f"{resource_id}"
        f"?api-version={ARM_API_VERSION}"
    )


def http_get(url: str, headers: dict) -> dict:
    request = urllib.request.Request(
        url,
        headers=headers,
        method="GET",
    )

    with urllib.request.urlopen(
        request,
        timeout=30,
    ) as response:
        return json.loads(
            response.read().decode()
        )


def http_patch(
    url: str,
    headers: dict,
    body: dict,
):
    data = json.dumps(body).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method="PATCH",
    )

    try:
        with urllib.request.urlopen(
            request,
            timeout=60,
        ) as response:
            return (
                response.status,
                response.read().decode(),
            )

    except urllib.error.HTTPError as exc:
        return (
            exc.code,
            exc.read().decode(),
        )


# ============================================================
# 10. Teams Notification
# ============================================================

def send_teams_notification(
    message: str,
    event_type: str,
) -> None:
    if not TEAMS_WEBHOOK_URL:
        return

    payload = {
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": [
            {
                "type": "TextBlock",
                "text": "Fabric Capacity Dynamic Autoscale Notification",
                "weight": "bolder",
                "size": "medium",
            },
            {
                "type": "FactSet",
                "facts": [
                    {"title": "Environment", "value": ENV_NAME.upper()},
                    {"title": "Capacity", "value": CAPACITY_NAME},
                    {"title": "Event", "value": event_type},
                    {"title": "Message", "value": message},
                    {
                        "title": "Timestamp",
                        "value": datetime.now(KST).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                    },
                ],
            },
        ],
        "$schema": "https://adaptivecards.io/schemas/adaptive-card.json",
    }

    request = urllib.request.Request(
        TEAMS_WEBHOOK_URL,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Content-Type": "application/json",
            "User-Agent": "fabric-dynamic-autoscale/1.0",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(
            request,
            context=ssl.create_default_context(),
            timeout=30,
        ) as response:
            print(
                f"[Teams] Notification HTTP "
                f"{response.status}"
            )

    except Exception as exc:
        print(
            f"[WARN] Teams notification failed: "
            f"{exc}"
        )


# ============================================================
# 11. Historical Event Collection
# ============================================================

def collect_historical_events(
    credential: DefaultAzureCredential,
    now_utc: datetime,
) -> list[dict[str, Any]]:

    collected_events: list[dict[str, Any]] = []
    process_error = None

    lookback_start = (
        now_utc
        - timedelta(minutes=LOOKBACK_MINUTES)
    )

    client = EventHubConsumerClient(
        fully_qualified_namespace=EVENTHUB_NAMESPACE,
        eventhub_name=EVENTHUB_NAME,
        consumer_group=CONSUMER_GROUP,
        credential=credential,
        transport_type=TransportType.AmqpOverWebsocket,
    )

    def on_event_batch(
        partition_context,
        events,
    ) -> None:
        nonlocal process_error

        # max_wait_time 동안 추가 Event가 없으면
        # Historical backlog를 모두 읽은 것으로 판단
        if not events:
            client.close()
            return

        try:
            for event in events:
                body = event.body_as_json()

                if not isinstance(body, dict):
                    continue

                if (
                    body.get("type")
                    != "Microsoft.Fabric.Capacity.Summary"
                ):
                    continue

                data = body.get("data")

                if not isinstance(data, dict):
                    continue

                if (
                    data.get("capacityName")
                    != CAPACITY_NAME
                ):
                    continue

                window_start = parse_datetime(
                    data.get("windowStartTime")
                )

                window_end = parse_datetime(
                    data.get("windowEndTime")
                )

                if (
                    window_start is None
                    or window_end is None
                ):
                    continue

                # Event Hub Enqueued Time뿐 아니라
                # 실제 Capacity Summary Window 기준으로 다시 필터링
                if (
                    window_end < lookback_start
                    or window_end > now_utc
                ):
                    continue

                collected_events.append(
                    {
                        "partition":
                            partition_context.partition_id,

                        "sequence":
                            event.sequence_number,

                        "window_start":
                            window_start,

                        "window_end":
                            window_end,

                        "utilization":
                            calculate_utilization(data),

                        "sku":
                            str(
                                data.get(
                                    "capacitySku",
                                    "",
                                )
                            ).upper(),
                    }
                )

        except Exception as exc:
            process_error = exc
            client.close()

    def on_error(
        partition_context,
        error,
    ) -> None:
        nonlocal process_error

        process_error = error

        if partition_context:
            print(
                f"[ERROR] Partition "
                f"{partition_context.partition_id}: "
                f"{error}"
            )
        else:
            print(
                f"[ERROR] Event Hub: "
                f"{error}"
            )

        client.close()

    print_section(
        "Historical Event Collection"
    )

    print(
        f"Lookback Start : "
        f"{lookback_start.strftime('%Y-%m-%d %H:%M:%S')} UTC"
    )

    print(
        f"Lookback End   : "
        f"{now_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC"
    )

    print(
        f"Lookback       : "
        f"{LOOKBACK_MINUTES} minutes"
    )

    try:
        with client:
            client.receive_batch(
                on_event_batch=on_event_batch,
                max_batch_size=100,

                # datetime을 전달하면 해당 UTC Enqueued Time부터
                # Event를 읽기 시작한다.
                starting_position=lookback_start,
                starting_position_inclusive=True,

                max_wait_time=MAX_WAIT_SECONDS,
                on_error=on_error,
            )

    except Exception as exc:
        if process_error is None:
            process_error = exc

    finally:
        try:
            client.close()
        except Exception:
            pass

    if process_error is not None:
        raise RuntimeError(
            "Fabric Eventstream collection failed: "
            f"{process_error}"
        )

    # --------------------------------------------------------
    # Duplicate Event 제거
    #
    # Capacity Summary는 동일 Window가 중복 전달될 가능성에 대비하여
    # windowStartTime + windowEndTime 기준으로 중복 제거
    # --------------------------------------------------------

    unique_events: dict[
        tuple[datetime, datetime],
        dict[str, Any],
    ] = {}

    for item in collected_events:
        key = (
            item["window_start"],
            item["window_end"],
        )

        unique_events[key] = item

    result = sorted(
        unique_events.values(),
        key=lambda item: item["window_end"],
    )

    print(
        f"Raw Events     : "
        f"{len(collected_events)}"
    )

    print(
        f"Unique Events  : "
        f"{len(result)}"
    )

    return result


# ============================================================
# 12. Rolling Window Evaluation
# ============================================================

def build_rolling_evaluations(
    events: list[dict[str, Any]],
    now_utc: datetime,
) -> list[dict[str, Any]]:

    if not events:
        return []

    evaluations: list[
        dict[str, Any]
    ] = []

    # 현재 시간을 직전 1분 Step으로 정렬
    current_step = now_utc.replace(
        second=0,
        microsecond=0,
    )

    required_steps = (
        TIME_ELAPSED_MINUTES
        // STEP_MINUTES
    )

    first_step = (
        current_step
        - timedelta(
            minutes=(
                required_steps - 1
            )
            * STEP_MINUTES
        )
    )

    for index in range(required_steps):
        evaluation_time = (
            first_step
            + timedelta(
                minutes=(
                    index
                    * STEP_MINUTES
                )
            )
        )

        window_start = (
            evaluation_time
            - timedelta(
                minutes=WINDOW_MINUTES
            )
        )

        window_events = [
            item
            for item in events
            if (
                window_start
                < item["window_end"]
                <= evaluation_time
            )
        ]

        if not window_events:
            continue

        utilizations = [
            float(
                item["utilization"]
            )
            for item in window_events
        ]

        evaluations.append(
            {
                "evaluation_time":
                    evaluation_time,

                "window_start":
                    window_start,

                "window_end":
                    evaluation_time,

                "event_count":
                    len(window_events),

                "average":
                    sum(utilizations)
                    / len(utilizations),

                "minimum":
                    min(utilizations),

                "maximum":
                    max(utilizations),
            }
        )

    return evaluations


# ============================================================
# 13. Dynamic Autoscale Decision
# ============================================================

def determine_action(
    current_sku: str,
    evaluations: list[dict[str, Any]],
) -> tuple[str, str, str]:

    policy = DYNAMIC_SKU_POLICY[
        ENV_NAME
    ]

    min_sku = policy["min"]
    max_sku = policy["max"]

    # Dynamic 허용 범위 밖 SKU 보호
    if current_sku not in {
        min_sku,
        max_sku,
    }:
        return (
            "SKIP",
            current_sku,
            (
                "Current SKU is outside the "
                "dynamic autoscale range "
                f"({min_sku} <-> {max_sku})."
            ),
        )

    required_steps = (
        TIME_ELAPSED_MINUTES
        // STEP_MINUTES
    )

    if len(evaluations) < required_steps:
        return (
            "SKIP",
            current_sku,
            (
                "Insufficient rolling evaluation data. "
                f"Required={required_steps}, "
                f"Actual={len(evaluations)}."
            ),
        )

    recent = evaluations[
        -required_steps:
    ]

    # --------------------------------------------------------
    # Scale Up
    #
    # Activator:
    # Average > 0.8
    # When it has been true for 5 minutes
    # --------------------------------------------------------

    upscale_true = all(
        item["average"]
        > UTIL_UP_THRESHOLD
        for item in recent
    )

    # --------------------------------------------------------
    # Scale Down
    #
    # Activator:
    # Average < 0.4
    # When it has been true for 5 minutes
    # --------------------------------------------------------

    downscale_true = all(
        item["average"]
        < UTIL_DOWN_THRESHOLD
        for item in recent
    )

    latest_avg = float(
        recent[-1]["average"]
    )

    if upscale_true:
        if current_sku == max_sku:
            return (
                "MAINTAIN",
                current_sku,
                (
                    "Scale-up condition sustained, "
                    "but Capacity is already at "
                    "dynamic maximum SKU."
                ),
            )

        return (
            "SCALE_UP",
            max_sku,
            (
                f"10-minute AVG > "
                f"{UTIL_UP_THRESHOLD:.2f}% "
                f"for {TIME_ELAPSED_MINUTES} "
                f"consecutive 1-minute evaluations. "
                f"Latest AVG={latest_avg:.2f}%."
            ),
        )

    if downscale_true:
        if current_sku == min_sku:
            return (
                "MAINTAIN",
                current_sku,
                (
                    "Scale-down condition sustained, "
                    "but Capacity is already at "
                    "dynamic minimum SKU."
                ),
            )

        return (
            "SCALE_DOWN",
            min_sku,
            (
                f"10-minute AVG < "
                f"{UTIL_DOWN_THRESHOLD:.2f}% "
                f"for {TIME_ELAPSED_MINUTES} "
                f"consecutive 1-minute evaluations. "
                f"Latest AVG={latest_avg:.2f}%."
            ),
        )

    return (
        "MAINTAIN",
        current_sku,
        (
            "Neither scale-up nor scale-down condition "
            f"has remained true for "
            f"{TIME_ELAPSED_MINUTES} minutes."
        ),
    )


# ============================================================
# 14. Main
# ============================================================

def main() -> int:
    try:
        validate_config()

    except RuntimeError as exc:
        print(
            f"[ERROR] {exc}"
        )
        return 1

    now_utc = datetime.now(
        timezone.utc
    )

    now_kst = now_utc.astimezone(
        KST
    )

    # --------------------------------------------------------
    # Dynamic Window Guard
    # --------------------------------------------------------

    if not is_dynamic_autoscale_window(
        now_kst
    ):
        print_section(
            "Dynamic Autoscale Skipped"
        )

        print(
            f"Environment : "
            f"{ENV_NAME.upper()}"
        )

        print(
            f"Current KST : "
            f"{now_kst.strftime('%Y-%m-%d %H:%M:%S (%A)')}"
        )

        print(
            "Time Window : "
            "Monday-Friday / "
            "08:15 - 17:45 KST"
        )

        print(
            "Reason      : "
            "Outside dynamic autoscale window."
        )

        return 0

    policy = DYNAMIC_SKU_POLICY[
        ENV_NAME
    ]

    print_section(
        "Fabric Capacity Dynamic Autoscale"
    )

    print(
        f"Environment       : "
        f"{ENV_NAME.upper()}"
    )

    print(
        f"Capacity          : "
        f"{CAPACITY_NAME}"
    )

    print(
        f"Current KST       : "
        f"{now_kst.strftime('%Y-%m-%d %H:%M:%S (%A)')}"
    )

    print(
        "Time Window       : "
        "Monday-Friday / "
        "08:15 - 17:45 KST"
    )

    print(
        f"Dynamic SKU Range : "
        f"{policy['min']} "
        f"<-> "
        f"{policy['max']}"
    )

    print(
        f"Rule Window       : "
        f"{WINDOW_MINUTES} minutes"
    )

    print(
        f"Rule Step         : "
        f"{STEP_MINUTES} minute"
    )

    print(
        f"Time Elapsed      : "
        f"{TIME_ELAPSED_MINUTES} minutes"
    )

    print(
        f"Scale Up          : "
        f"10m AVG > "
        f"{UTIL_UP_THRESHOLD:.2f}% "
        f"for "
        f"{TIME_ELAPSED_MINUTES} minutes"
    )

    print(
        f"Scale Down        : "
        f"10m AVG < "
        f"{UTIL_DOWN_THRESHOLD:.2f}% "
        f"for "
        f"{TIME_ELAPSED_MINUTES} minutes"
    )

    credential = (
        DefaultAzureCredential()
    )

    try:
        # ----------------------------------------------------
        # ARM Authentication
        # ----------------------------------------------------

        arm_token = (
            credential.get_token(
                "https://management.azure.com/.default"
            ).token
        )

        arm_headers = {
            "Authorization":
                f"Bearer {arm_token}",

            "Content-Type":
                "application/json",
        }

        arm_url = build_arm_url()

        # ----------------------------------------------------
        # Current Capacity
        # ----------------------------------------------------

        capacity = http_get(
            arm_url,
            arm_headers,
        )

        current_sku = str(
            capacity.get(
                "sku",
                {},
            ).get(
                "name",
                "",
            )
        ).upper()

        state = str(
            capacity.get(
                "properties",
                {},
            ).get(
                "state",
                "Unknown",
            )
        )

        print_section(
            "Current Fabric Capacity"
        )

        print(
            f"Current SKU : "
            f"{current_sku}"
        )

        print(
            f"State       : "
            f"{state}"
        )

        # ----------------------------------------------------
        # Active Guard
        # ----------------------------------------------------

        if state.lower() != "active":
            print(
                "[SKIP] Capacity is not Active. "
                "Dynamic Autoscale will not "
                "resume the Capacity."
            )

            return 0

        # ----------------------------------------------------
        # Manual F64 Guard
        # ----------------------------------------------------

        if current_sku == "F64":
            message = (
                "Capacity is currently F64. "
                "Dynamic autoscale is bypassed "
                "to protect manual "
                "high-capacity operation."
            )

            print(
                f"[SKIP] {message}"
            )

            send_teams_notification(
                message,
                "Dynamic-Scale-Bypassed",
            )

            return 0

        # ----------------------------------------------------
        # Historical Event Collection
        # ----------------------------------------------------

        events = collect_historical_events(
            credential,
            now_utc,
        )

        if not events:
            print(
                "[WARN] No historical "
                "Capacity Summary events."
            )

            print(
                "[SKIP] Capacity SKU "
                "will not be changed."
            )

            return 0

        # ----------------------------------------------------
        # Rolling Evaluation
        # ----------------------------------------------------

        evaluations = (
            build_rolling_evaluations(
                events,
                now_utc,
            )
        )

        print_section(
            "Activator-compatible Rolling Evaluation"
        )

        print(
            "Rule : "
            "10-minute Average / "
            "1-minute Step"
        )

        for index, item in enumerate(
            evaluations,
            start=1,
        ):
            evaluation_kst = (
                item[
                    "evaluation_time"
                ].astimezone(
                    KST
                )
            )

            up_true = (
                item["average"]
                > UTIL_UP_THRESHOLD
            )

            down_true = (
                item["average"]
                < UTIL_DOWN_THRESHOLD
            )

            print(
                f"[Step {index}] "
                f"{evaluation_kst.strftime('%H:%M')} | "
                f"Events={item['event_count']:2d} | "
                f"AVG={item['average']:.2f}% | "
                f"MIN={item['minimum']:.2f}% | "
                f"MAX={item['maximum']:.2f}% | "
                f"UP={up_true} | "
                f"DOWN={down_true}"
            )

        # ----------------------------------------------------
        # Decision
        # ----------------------------------------------------

        (
            action,
            target_sku,
            reason,
        ) = determine_action(
            current_sku,
            evaluations,
        )

        print_section(
            "Dynamic Autoscale Decision"
        )

        print(
            f"Action      : "
            f"{action}"
        )

        print(
            f"Current SKU : "
            f"{current_sku}"
        )

        print(
            f"Target SKU  : "
            f"{target_sku}"
        )

        print(
            f"Reason      : "
            f"{reason}"
        )

        # ----------------------------------------------------
        # No Change
        # ----------------------------------------------------

        if action in {
            "MAINTAIN",
            "SKIP",
        }:
            print(
                "[INFO] No Fabric Capacity "
                "SKU change required."
            )

            return 0

        # ----------------------------------------------------
        # ARM PATCH
        # ----------------------------------------------------

        print()

        print(
            "Triggering dynamic "
            "scale transition: "
            f"{current_sku} -> "
            f"{target_sku}"
        )

        patch_body = {
            "sku": {
                "name":
                    target_sku,

                "tier":
                    "Fabric",
            }
        }

        status, text = http_patch(
            arm_url,
            arm_headers,
            patch_body,
        )

        if status in {
            200,
            201,
            202,
        }:
            latest_average = (
                evaluations[
                    -1
                ][
                    "average"
                ]
            )

            message = (
                f"{action}: "
                f"{current_sku} -> "
                f"{target_sku} | "
                f"10m AVG="
                f"{latest_average:.2f}% | "
                f"Condition sustained "
                f"{TIME_ELAPSED_MINUTES} minutes"
            )

            print(
                f"[SUCCESS] {message}"
            )

            send_teams_notification(
                message,
                "Dynamic-Autoscale",
            )

            return 0

        print(
            f"[ERROR] Dynamic scale failed "
            f"(HTTP {status})"
        )

        print(
            text[:2000]
        )

        return 1

    except urllib.error.HTTPError as exc:
        body = (
            exc.read().decode(
                errors="replace"
            )
        )

        print(
            "[ERROR] ARM request failed."
        )

        print(
            f"HTTP Status : "
            f"{exc.code}"
        )

        print(
            f"Response    : "
            f"{body[:2000]}"
        )

        return 1

    except Exception as exc:
        print(
            "[ERROR] Dynamic Autoscale "
            "execution failed."
        )

        print(
            f"Exception : "
            f"{type(exc).__name__}"
        )

        print(
            f"Message   : "
            f"{exc}"
        )

        return 1

    finally:
        credential.close()


if __name__ == "__main__":
    exit_code = main()

    print()
    print("=" * 72)
    print("Script Information")
    print("=" * 72)
    print("Author  : 김민서 (MEGAZONECLOUD)")
    print("Created : 2026-07-30")
    print(
        f"Ended   : "
        f"{datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}"
    )
    print("=" * 72)

    sys.exit(exit_code)