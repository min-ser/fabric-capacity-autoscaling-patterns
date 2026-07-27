#!/usr/bin/env python3

import os
import sys

from azure.eventhub import (
    EventHubConsumerClient,
    TransportType,
)
from azure.identity import DefaultAzureCredential


# ============================================================
# Configuration
# ============================================================

EVENTHUB_NAMESPACE      = os.getenv("Fabric-Event-Hub-namespace-endpoint")
EVENTHUB_NAME           = os.getenv("Fabric-Event-Hub-name")
CONSUMER_GROUP          = os.getenv("Fabric-Event-Hub-Consumer-group")
TARGET_CAPACITY_NAME    = os.getenv("capacityName")

WINDOW_SECONDS = 30

# CronJob 1회 실행 시 최대 대기 시간
MAX_WAIT_SECONDS = 60


# ============================================================
# Config Validation
# ============================================================

def validate_config() -> None:
    required = {
        "Fabric-Event-Hub-namespace-endpoint":
            EVENTHUB_NAMESPACE,
        "Fabric-Event-Hub-name":
            EVENTHUB_NAME,
        "Fabric-Event-Hub-Consumer-group":
            CONSUMER_GROUP,
        "capacityName":
            TARGET_CAPACITY_NAME,
    }

    missing = [
        name
        for name, value in required.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing)
        )


# ============================================================
# Utilization
# ============================================================

def calculate_utilization(data: dict) -> float:
    capacity_unit_ms = float(
        data.get(
            "capacityUnitMs",
            0,
        )
    )

    base_capacity_units = float(
        data.get(
            "baseCapacityUnits",
            0,
        )
    )

    capacity_budget_ms = (
        base_capacity_units
        * 1000
        * WINDOW_SECONDS
    )

    if capacity_budget_ms <= 0:
        raise ValueError(
            "baseCapacityUnits must be greater than zero."
        )

    return (
        capacity_unit_ms
        / capacity_budget_ms
        * 100
    )


# ============================================================
# Event Processing
# ============================================================

def print_capacity_summary(
    partition_context,
    event,
    body: dict,
) -> None:
    data = body["data"]

    utilization = calculate_utilization(
        data
    )

    capacity_name = data.get(
        "capacityName",
        "UNKNOWN",
    )

    capacity_sku = data.get(
        "capacitySku",
        "UNKNOWN",
    )

    capacity_region = data.get(
        "capacityRegion",
        "UNKNOWN",
    )

    window_start = data.get(
        "windowStartTime",
        "UNKNOWN",
    )

    window_end = data.get(
        "windowEndTime",
        "UNKNOWN",
    )

    base_capacity_units = float(
        data.get(
            "baseCapacityUnits",
            0,
        )
    )

    capacity_unit_ms = float(
        data.get(
            "capacityUnitMs",
            0,
        )
    )

    interactive_delay = float(
        data.get(
            "interactiveDelayThresholdPercentage",
            0,
        )
    )

    interactive_reject = float(
        data.get(
            "interactiveRejectionThresholdPercentage",
            0,
        )
    )

    background_reject = float(
        data.get(
            "backgroundRejectionThresholdPercentage",
            0,
        )
    )

    overage_active = bool(
        data.get(
            "overageBillingProtectionActive",
            False,
        )
    )

    print()
    print("=" * 72)
    print(" Fabric Capacity Summary")
    print("=" * 72)

    print(
        f"Partition / Seq     : "
        f"{partition_context.partition_id}"
        f" / "
        f"{event.sequence_number}"
    )

    print("-" * 72)

    print(
        f"Capacity Name       : "
        f"{capacity_name}"
    )

    print(
        f"Capacity SKU        : "
        f"{capacity_sku}"
    )

    print(
        f"Region              : "
        f"{capacity_region}"
    )

    print("-" * 72)

    print(
        f"Window Start        : "
        f"{window_start}"
    )

    print(
        f"Window End          : "
        f"{window_end}"
    )

    print("-" * 72)

    print(
        f"Base Capacity Units : "
        f"{base_capacity_units:g}"
    )

    print(
        f"Capacity Unit Ms    : "
        f"{capacity_unit_ms:,.2f}"
    )

    print(
        f"Utilization         : "
        f"{utilization:.2f}%"
    )

    print("-" * 72)

    print(
        f"Interactive Delay   : "
        f"{interactive_delay:.2f}%"
    )

    print(
        f"Interactive Reject  : "
        f"{interactive_reject:.2f}%"
    )

    print(
        f"Background Reject   : "
        f"{background_reject:.2f}%"
    )

    print(
        f"Overage Active      : "
        f"{overage_active}"
    )

    print("=" * 72)


# ============================================================
# Main
# ============================================================

def main() -> int:
    try:
        validate_config()

    except RuntimeError as exc:
        print(
            f"[ERROR] {exc}"
        )
        return 1

    print("=" * 72)
    print(
        " Fabric Eventstream CronJob One-Shot"
    )
    print("=" * 72)

    print(
        f"Environment     : "
        f"{os.getenv('ENV', 'UNKNOWN')}"
    )

    print(
        f"Target Capacity : "
        f"{TARGET_CAPACITY_NAME}"
    )

    print(
        "Authentication  : "
        "Microsoft Entra ID"
    )

    print(
        "Transport       : "
        "AMQP over WebSocket / TCP 443"
    )

    print(
        "Execution Mode  : "
        "ONE SHOT"
    )

    print(
        f"Max Wait        : "
        f"{MAX_WAIT_SECONDS} seconds"
    )

    print("=" * 72)

    credential = DefaultAzureCredential()

    client = EventHubConsumerClient(
        fully_qualified_namespace=(
            EVENTHUB_NAMESPACE
        ),
        eventhub_name=(
            EVENTHUB_NAME
        ),
        consumer_group=(
            CONSUMER_GROUP
        ),
        credential=credential,
        transport_type=(
            TransportType.AmqpOverWebsocket
        ),
    )

    processed = False
    process_error = None

    # ========================================================
    # Batch Handler
    # ========================================================

    def on_event_batch(
        partition_context,
        events,
    ) -> None:
        nonlocal processed
        nonlocal process_error

        # max_wait_time 동안 이벤트가 없으면
        # empty list로 callback 호출됨
        if not events:
            return

        try:
            for event in events:
                if processed:
                    break

                body = event.body_as_json()

                if not isinstance(
                    body,
                    dict,
                ):
                    continue

                if (
                    body.get("type")
                    != "Microsoft.Fabric.Capacity.Summary"
                ):
                    continue

                data = body.get(
                    "data"
                )

                if not isinstance(
                    data,
                    dict,
                ):
                    continue

                capacity_name = data.get(
                    "capacityName"
                )

                if (
                    capacity_name
                    != TARGET_CAPACITY_NAME
                ):
                    continue

                # --------------------------------------------
                # 정상 이벤트 딱 1건만 처리
                # --------------------------------------------

                print_capacity_summary(
                    partition_context,
                    event,
                    body,
                )

                processed = True

                # Consumer 종료
                client.close()

                break

        except Exception as exc:
            process_error = exc
            client.close()

    # ========================================================
    # Error Handler
    # ========================================================

    def on_error(
        partition_context,
        error,
    ) -> None:
        nonlocal process_error

        process_error = error

        if partition_context:

            print(
                "[ERROR] Partition "
                f"{partition_context.partition_id}: "
                f"{error}"
            )

        else:

            print(
                f"[ERROR] Consumer: {error}"
            )

        client.close()

    # ========================================================
    # Receive
    # ========================================================

    try:
        print()
        print(
            "Waiting for one "
            "Capacity Summary event..."
        )

        with client:

            client.receive_batch(
                on_event_batch=on_event_batch,

                # 한 callback에 최대 10건
                max_batch_size=10,

                # 실행 이후 신규 이벤트부터
                starting_position="@latest",

                # 이벤트가 없더라도 60초 후
                # 빈 batch callback 발생
                max_wait_time=(
                    MAX_WAIT_SECONDS
                ),

                on_error=on_error,
            )

    except Exception as exc:
        if (
            process_error is None
            and not processed
        ):
            process_error = exc

    finally:
        credential.close()

        try:
            client.close()
        except Exception:
            pass

    # ========================================================
    # Result
    # ========================================================

    print()

    if processed:
        print(
            "[SUCCESS] "
            "One Capacity Summary event processed."
        )

        print(
            "[SUCCESS] "
            "CronJob execution completed."
        )

        return 0

    if process_error is not None:
        print(
            "[ERROR] "
            f"{type(process_error).__name__}: "
            f"{process_error}"
        )

        return 1

    print(
        "[WARN] No matching Capacity Summary "
        f"event received within "
        f"{MAX_WAIT_SECONDS} seconds."
    )

    return 0


if __name__ == "__main__":
    sys.exit(
        main()
    )