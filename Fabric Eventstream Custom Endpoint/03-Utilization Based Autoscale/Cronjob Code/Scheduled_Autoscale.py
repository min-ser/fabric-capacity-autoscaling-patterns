#!/usr/bin/env python3
"""
Microsoft Fabric Capacity scheduled autoscaler.

Purpose
-------
Fabric Capacity의 업무 시작/종료 및 비업무시간 Baseline SKU를
Schedule 기반으로 관리한다.

Dynamic Autoscale Integration:
    08:00          : Scheduled Baseline 적용
    08:15 ~ 17:45  : Dynamic_Autoscale.py에 SKU 제어권 위임
    17:45          : Scheduled Control 복귀
    18:00          : Off-hours SKU 적용

Schedule Policy:
    DEV
        Fixed : F2

    STG
        Weekday 08:00 ~ 18:00 : F16
        Other Time            : F4

    PRD Weekday
        00:00 ~ 18:00 : F32
        18:00 ~ 24:00 : F16

    PRD Weekend
        00:00 ~ 08:00 : F32
        08:00 ~ 24:00 : F8

Authentication:
    AKS Workload Identity
    DefaultAzureCredential

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

from azure.identity import DefaultAzureCredential


# ============================================================
# 1. Configuration
# ============================================================

ENV_NAME = os.getenv("ENV", "").strip().lower()
SUBSCRIPTION_ID = os.getenv("subscriptionId")
RESOURCE_GROUP = os.getenv("resourceGroup")
CAPACITY_NAME = os.getenv("capacityName")
TEAMS_WEBHOOK_URL = os.getenv("TEAMS_WEBHOOK_URL")

ARM_API_VERSION = "2023-11-01"
KST = timezone(timedelta(hours=9))

SKU_CU_MAP = {
    "F2": 2,
    "F4": 4,
    "F8": 8,
    "F16": 16,
    "F32": 32,
    "F64": 64,
}


# ============================================================
# 2. Environment SKU Boundary
# ============================================================

DEFAULT_SKU_BOUNDARY = {
    "dev": {"min": "F2", "max": "F8"},
    "develop": {"min": "F2", "max": "F8"},
    "stg": {"min": "F4", "max": "F64"},
    "prd": {"min": "F8", "max": "F64"},
}

boundary = DEFAULT_SKU_BOUNDARY.get(
    ENV_NAME,
    DEFAULT_SKU_BOUNDARY["dev"],
)

SKU_MIN = os.getenv("skuMin", boundary["min"])
SKU_MAX = os.getenv("skuMax", boundary["max"])


# ============================================================
# 3. Dynamic Autoscale Control Window
# ============================================================

DYNAMIC_START_HOUR = 8
DYNAMIC_START_MINUTE = 15

DYNAMIC_END_HOUR = 17
DYNAMIC_END_MINUTE = 45


# ============================================================
# 4. Common Helpers
# ============================================================

def print_section(title: str) -> None:
    print()
    print("=" * 72)
    print(title)
    print("=" * 72)


def validate_config() -> None:
    required = {
        "ENV": ENV_NAME,
        "subscriptionId": SUBSCRIPTION_ID,
        "resourceGroup": RESOURCE_GROUP,
        "capacityName": CAPACITY_NAME,
    }

    missing = [name for name, value in required.items() if not value]

    if missing:
        raise RuntimeError(
            "Missing environment variables: "
            + ", ".join(missing)
        )

    if ENV_NAME not in {"dev", "develop", "stg", "prd"}:
        raise RuntimeError(
            f"Unsupported environment: {ENV_NAME!r}"
        )

    if SKU_MIN not in SKU_CU_MAP:
        raise RuntimeError(f"Invalid SKU_MIN: {SKU_MIN}")

    if SKU_MAX not in SKU_CU_MAP:
        raise RuntimeError(f"Invalid SKU_MAX: {SKU_MAX}")

    if SKU_CU_MAP[SKU_MIN] > SKU_CU_MAP[SKU_MAX]:
        raise RuntimeError(
            f"SKU_MIN ({SKU_MIN}) cannot be greater than "
            f"SKU_MAX ({SKU_MAX})."
        )


# ============================================================
# 5. ARM Helpers
# ============================================================

def build_resource_id() -> str:
    return (
        f"/subscriptions/{SUBSCRIPTION_ID}"
        f"/resourceGroups/{RESOURCE_GROUP}"
        f"/providers/Microsoft.Fabric"
        f"/capacities/{CAPACITY_NAME}"
    )


def build_arm_url() -> str:
    return (
        f"https://management.azure.com"
        f"{build_resource_id()}"
        f"?api-version={ARM_API_VERSION}"
    )


def http_get(url: str, headers: dict) -> dict:
    request = urllib.request.Request(
        url,
        headers=headers,
        method="GET",
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode())


def http_post(url: str, headers: dict, body: dict):
    data = json.dumps(body).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read().decode()

    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def http_patch(url: str, headers: dict, body: dict):
    data = json.dumps(body).encode("utf-8")

    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method="PATCH",
    )

    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.status, response.read().decode()

    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


# ============================================================
# 6. Teams Notification
# ============================================================

def send_teams_notification(message: str, event_type: str) -> None:
    if not TEAMS_WEBHOOK_URL:
        return

    payload = {
        "type": "AdaptiveCard",
        "version": "1.4",
        "body": [
            {
                "type": "TextBlock",
                "text": "Fabric Capacity Scheduled Autoscale Notification",
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
            "User-Agent": "fabric-scheduled-autoscale/1.0",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(
            request,
            context=ssl.create_default_context(),
            timeout=30,
        ) as response:
            print(f"[Teams] Notification HTTP {response.status}")

    except Exception as exc:
        print(f"[WARN] Teams notification failed: {exc}")


# ============================================================
# 7. Dynamic Autoscale Delegation
# ============================================================

def is_dynamic_autoscale_window(now_kst: datetime) -> bool:
    """
    평일 08:15 ~ 17:45 동안 STG/PRD의 Scheduled SKU 제어를 중단하고
    Dynamic_Autoscale.py에 제어권을 위임한다.
    """

    if ENV_NAME not in {"stg", "prd"}:
        return False

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
# 8. Scheduled SKU Decision
# ============================================================

def determine_scheduled_sku(
    now_kst: datetime,
) -> tuple[str, str]:

    is_weekday = now_kst.weekday() < 5
    hour = now_kst.hour

    # --------------------------------------------------------
    # DEV
    # --------------------------------------------------------

    if ENV_NAME in {"dev", "develop"}:
        return (
            "F2",
            "DEV environment uses fixed baseline SKU F2.",
        )

    # --------------------------------------------------------
    # STG
    # --------------------------------------------------------

    if ENV_NAME == "stg":
        if is_weekday and 8 <= hour < 18:
            return (
                "F16",
                "STG weekday business hours (08:00 - 18:00).",
            )

        return (
            "F4",
            "STG off-hours baseline.",
        )

    # --------------------------------------------------------
    # PRD Weekday
    # --------------------------------------------------------

    if is_weekday:
        if 18 <= hour < 24:
            return (
                "F16",
                "PRD weekday evening (18:00 - 24:00).",
            )

        return (
            "F32",
            "PRD weekday business/batch hours (00:00 - 18:00).",
        )

    # --------------------------------------------------------
    # PRD Weekend
    # --------------------------------------------------------

    if 0 <= hour < 8:
        return (
            "F32",
            "PRD weekend batch hours (00:00 - 08:00).",
        )

    return (
        "F8",
        "PRD weekend daytime/off-peak hours (08:00 - 24:00).",
    )


# ============================================================
# 9. SKU Boundary Guard
# ============================================================

def apply_sku_boundary(
    target_sku: str,
    reason: str,
) -> tuple[str, str]:

    target_cu = SKU_CU_MAP[target_sku]
    min_cu = SKU_CU_MAP[SKU_MIN]
    max_cu = SKU_CU_MAP[SKU_MAX]

    if target_cu < min_cu:
        return (
            SKU_MIN,
            f"{reason} Target overridden by SKU_MIN={SKU_MIN}.",
        )

    if target_cu > max_cu:
        return (
            SKU_MAX,
            f"{reason} Target overridden by SKU_MAX={SKU_MAX}.",
        )

    return target_sku, reason


# ============================================================
# 10. Main
# ============================================================

def main() -> int:
    try:
        validate_config()

    except RuntimeError as exc:
        print(f"[ERROR] {exc}")
        return 1

    now_kst = datetime.now(timezone.utc).astimezone(KST)

    print_section("Fabric Capacity Scheduled Autoscale")

    print(f"Environment       : {ENV_NAME.upper()}")
    print(f"Capacity          : {CAPACITY_NAME}")
    print(f"Current KST       : {now_kst.strftime('%Y-%m-%d %H:%M:%S (%A)')}")
    print(f"Configured Range  : {SKU_MIN} -> {SKU_MAX}")
    print("Dynamic Window    : Monday-Friday / 08:15 - 17:45 KST")

    credential = DefaultAzureCredential()

    try:
        # ----------------------------------------------------
        # ARM Authentication
        # ----------------------------------------------------

        arm_token = credential.get_token(
            "https://management.azure.com/.default"
        ).token

        arm_headers = {
            "Authorization": f"Bearer {arm_token}",
            "Content-Type": "application/json",
        }

        arm_url = build_arm_url()

        # ----------------------------------------------------
        # Current Capacity
        # ----------------------------------------------------

        capacity = http_get(arm_url, arm_headers)

        current_sku = str(
            capacity.get("sku", {}).get("name", "")
        ).upper()

        state = str(
            capacity.get("properties", {}).get("state", "Unknown")
        )

        print_section("Current Fabric Capacity")
        print(f"Current SKU : {current_sku}")
        print(f"State       : {state}")

        if current_sku not in SKU_CU_MAP:
            raise RuntimeError(
                f"Unsupported current Fabric SKU: {current_sku}"
            )

        # ----------------------------------------------------
        # Capacity State
        # ----------------------------------------------------

        if state.lower() != "active":
            if ENV_NAME == "prd":
                print(
                    "[INFO] PRD Capacity is not Active. "
                    "Attempting automatic resume."
                )

                resume_url = (
                    f"https://management.azure.com"
                    f"{build_resource_id()}"
                    f"/resume?api-version={ARM_API_VERSION}"
                )

                status, text = http_post(
                    resume_url,
                    arm_headers,
                    {},
                )

                if status not in {200, 201, 202}:
                    print(
                        f"[ERROR] PRD Capacity resume failed "
                        f"(HTTP {status})"
                    )
                    print(text[:2000])
                    return 1

                print(
                    "[SUCCESS] PRD Capacity resume request accepted."
                )

            else:
                print(
                    "[SKIP] Capacity is not Active. "
                    "Scheduled Autoscale will not resume DEV/STG."
                )
                return 0

        # ----------------------------------------------------
        # Dynamic Autoscale Delegation
        # ----------------------------------------------------

        if is_dynamic_autoscale_window(now_kst):
            policy = DYNAMIC_SKU_POLICY_DISPLAY[ENV_NAME]

            print_section("Dynamic Autoscale Delegation")
            print(f"Environment      : {ENV_NAME.upper()}")
            print(f"Current SKU      : {current_sku}")
            print("Time Window      : 08:15 - 17:45 KST")
            print(f"Dynamic Range    : {policy}")
            print("Schedule Control : SKIPPED")
            print(
                "Control Owner     : Dynamic_Autoscale.py"
            )

            return 0

        # ----------------------------------------------------
        # Scheduled SKU Decision
        # ----------------------------------------------------

        target_sku, reason = determine_scheduled_sku(now_kst)
        target_sku, reason = apply_sku_boundary(
            target_sku,
            reason,
        )

        print_section("Scheduled Autoscale Decision")
        print(f"Current SKU : {current_sku}")
        print(f"Target SKU  : {target_sku}")
        print(f"Reason      : {reason}")

        # ----------------------------------------------------
        # No Change
        # ----------------------------------------------------

        if current_sku == target_sku:
            print(
                "[INFO] Capacity is already at the scheduled target SKU."
            )
            return 0

        # ----------------------------------------------------
        # F64 Safety Guard
        # ----------------------------------------------------

        if (
            current_sku == "F64"
            and SKU_CU_MAP[target_sku] < SKU_CU_MAP[current_sku]
        ):
            message = (
                f"Capacity is currently running on F64. "
                f"Scheduled downscale to {target_sku} was bypassed "
                "to protect manual high-capacity operation."
            )

            print(f"[SKIP] {message}")

            send_teams_notification(
                message,
                "Scheduled-Scale-Bypassed",
            )

            return 0

        # ----------------------------------------------------
        # ARM PATCH
        # ----------------------------------------------------

        print()
        print(
            f"Triggering scheduled scale transition: "
            f"{current_sku} -> {target_sku}"
        )

        patch_body = {
            "sku": {
                "name": target_sku,
                "tier": "Fabric",
            }
        }

        status, text = http_patch(
            arm_url,
            arm_headers,
            patch_body,
        )

        if status in {200, 201, 202}:
            message = (
                f"SCHEDULED_SCALE: "
                f"{current_sku} -> {target_sku} | "
                f"{reason}"
            )

            print(f"[SUCCESS] {message}")

            send_teams_notification(
                message,
                "Scheduled-Autoscale",
            )

            return 0

        print(
            f"[ERROR] Scheduled scale failed "
            f"(HTTP {status})"
        )

        print(text[:2000])

        return 1

    except urllib.error.HTTPError as exc:
        body = exc.read().decode(errors="replace")

        print("[ERROR] ARM request failed.")
        print(f"HTTP Status : {exc.code}")
        print(f"Response    : {body[:2000]}")

        return 1

    except Exception as exc:
        print("[ERROR] Scheduled Autoscale execution failed.")
        print(f"Exception : {type(exc).__name__}")
        print(f"Message   : {exc}")

        return 1

    finally:
        credential.close()


# Dynamic Autoscale Range 출력 전용
DYNAMIC_SKU_POLICY_DISPLAY = {
    "stg": "F8 <-> F16",
    "prd": "F16 <-> F32",
}


if __name__ == "__main__":
    exit_code = main()

    print()
    print("=" * 72)
    print("Script Information")
    print("=" * 72)
    print("Author  : 김민서 (MEGAZONECLOUD)")
    print("Created : 2026-07-30")
    print(f"Ended   : {datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S KST')}")
    print("=" * 72)

    sys.exit(exit_code)