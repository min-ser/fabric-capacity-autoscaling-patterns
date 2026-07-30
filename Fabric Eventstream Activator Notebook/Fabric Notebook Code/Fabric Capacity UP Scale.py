import json
import os
import time
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import requests
from azure.identity import ClientSecretCredential
from notebookutils import mssparkutils


# ============================================================
# 0. Run ID
# ============================================================

RUN_ID = str(uuid.uuid4())

print("=" * 70)
print(f"RUN ID : {RUN_ID}")
print("=" * 70)


# ============================================================
# 1. Entra ID Authentication
# ============================================================
#
# Do NOT hard-code credentials in source code.
#
# Required environment variables:
#
# AZURE_TENANT_ID
# AZURE_CLIENT_ID
# AZURE_CLIENT_SECRET
#

TENANT_ID = os.getenv("AZURE_TENANT_ID")
CLIENT_ID = os.getenv("AZURE_CLIENT_ID")
CLIENT_SECRET = os.getenv("AZURE_CLIENT_SECRET")


# ============================================================
# 2. Fabric Capacity Configuration
# ============================================================
#
# Required environment variables:
#
# AZURE_SUBSCRIPTION_ID
# FABRIC_RESOURCE_GROUP
# FABRIC_CAPACITY_NAME
#

SUBSCRIPTION_ID = os.getenv("AZURE_SUBSCRIPTION_ID")
RESOURCE_GROUP = os.getenv("FABRIC_RESOURCE_GROUP")
CAPACITY_NAME = os.getenv("FABRIC_CAPACITY_NAME")

API_VERSION = "2023-11-01"


# ============================================================
# 3. Scale-up Policy
# ============================================================

SOURCE_SKU = os.getenv(
    "FABRIC_SOURCE_SKU",
    "F16",
)

TARGET_SKU = os.getenv(
    "FABRIC_TARGET_SKU",
    "F32",
)

MAX_RETRY = 30
RETRY_INTERVAL_SECONDS = 10

HTTP_GET_TIMEOUT_SECONDS = 30
HTTP_PATCH_TIMEOUT_SECONDS = 60


# ============================================================
# 4. Common Configuration
# ============================================================

KST = timezone(
    timedelta(hours=9)
)


# ============================================================
# 5. Configuration Validation
# ============================================================

def validate_configuration() -> None:
    required = {
        "AZURE_TENANT_ID": TENANT_ID,
        "AZURE_CLIENT_ID": CLIENT_ID,
        "AZURE_CLIENT_SECRET": CLIENT_SECRET,
        "AZURE_SUBSCRIPTION_ID": SUBSCRIPTION_ID,
        "FABRIC_RESOURCE_GROUP": RESOURCE_GROUP,
        "FABRIC_CAPACITY_NAME": CAPACITY_NAME,
    }

    missing = [
        name
        for name, value in required.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Missing required configuration: "
            + ", ".join(missing)
        )


validate_configuration()


# ============================================================
# 6. Fabric Capacity ARM URL
# ============================================================

CAPACITY_URL = (
    "https://management.azure.com"
    f"/subscriptions/{SUBSCRIPTION_ID}"
    f"/resourceGroups/{RESOURCE_GROUP}"
    f"/providers/Microsoft.Fabric"
    f"/capacities/{CAPACITY_NAME}"
    f"?api-version={API_VERSION}"
)


# ============================================================
# 7. Common Helpers
# ============================================================

def now_utc() -> str:
    return datetime.now(
        timezone.utc
    ).isoformat()


def now_kst() -> str:
    return datetime.now(
        KST
    ).isoformat()


def print_section(
    title: str,
) -> None:
    print()
    print("=" * 70)
    print(title)
    print("=" * 70)


# ============================================================
# 8. Activator Utilization Parameter
# ============================================================

def convert_utilization(
    raw_value: Any,
) -> Optional[float]:
    """
    Convert the utilization parameter received from
    Fabric Activator into a float.

    Examples:
        "0.8234"
        "82.34%"

    Fabric Activator Test action:
        "Sample Value"

    Returns:
        float: normalized utilization ratio
        None : Activator Test action
    """

    normalized = str(
        raw_value or ""
    ).strip()

    if not normalized:
        raise ValueError(
            "Activator utilization value is empty."
        )

    if normalized.lower() == "sample value":
        return None

    try:
        if normalized.endswith("%"):
            numeric_part = (
                normalized[:-1]
                .strip()
            )

            if not numeric_part:
                raise ValueError(
                    "Percentage utilization "
                    "contains no numeric value."
                )

            result = (
                float(numeric_part)
                / 100.0
            )

        else:
            result = float(
                normalized
            )

    except (TypeError, ValueError) as exc:
        raise ValueError(
            "Failed to convert Activator utilization "
            "parameter to float. "
            f"raw_value={raw_value!r}"
        ) from exc

    if result < 0:
        raise ValueError(
            "Utilization cannot be negative. "
            f"value={result}"
        )

    return result


# ============================================================
# 9. Activator Parameter
# ============================================================
#
# 'utilization' is expected to be provided as a Fabric Notebook
# parameter by Fabric Activator.
#

utilization_raw = utilization


print_section(
    "Activator Parameter"
)

print(
    "Parameter Name      : utilization"
)

print(
    f"Parameter Value     : "
    f"{utilization_raw}"
)

print(
    f"Parameter repr      : "
    f"{utilization_raw!r}"
)

print(
    f"Python Type         : "
    f"{type(utilization_raw).__name__}"
)

print(
    f"Is None             : "
    f"{utilization_raw is None}"
)

if isinstance(
    utilization_raw,
    str,
):
    print(
        f"String Length       : "
        f"{len(utilization_raw)}"
    )

    print(
        f"Trimmed Value       : "
        f"{utilization_raw.strip()!r}"
    )


utilization_value = convert_utilization(
    utilization_raw
)


# ============================================================
# 10. Fabric Activator Test Action
# ============================================================

if utilization_value is None:
    print_section(
        "Fabric Capacity Scale-up Test"
    )

    print(
        f"Execution Time UTC : "
        f"{now_utc()}"
    )

    print(
        f"Execution Time KST : "
        f"{now_kst()}"
    )

    print(
        "Trigger Source     : "
        "Fabric Activator Test action"
    )

    print(
        f"Raw Utilization    : "
        f"{utilization_raw!r}"
    )

    print(
        "Result             : "
        "Activator -> Notebook invocation succeeded"
    )

    print(
        "SKU Change         : Not executed"
    )

    print(
        f"Target Capacity    : "
        f"{CAPACITY_NAME}"
    )

    print(
        f"Scale Range        : "
        f"{SOURCE_SKU} -> {TARGET_SKU}"
    )

    mssparkutils.notebook.exit(
        json.dumps(
            {
                "status": "test_action_succeeded",
                "trigger": "fabric_activator_test_action",
                "action": "scale_up",
                "run_id": RUN_ID,
                "raw_utilization": str(
                    utilization_raw
                ),
                "capacity": CAPACITY_NAME,
                "source_sku": SOURCE_SKU,
                "target_sku": TARGET_SKU,
                "sku_changed": False,
                "completed_at_kst": now_kst(),
            },
            ensure_ascii=False,
        )
    )


# ============================================================
# 11. Scale-up Execution Log
# ============================================================

print_section(
    "Fabric Capacity Scale-up Notebook"
)

print(
    f"Execution Time UTC : "
    f"{now_utc()}"
)

print(
    f"Execution Time KST : "
    f"{now_kst()}"
)

print(
    "Trigger Source     : Fabric Activator"
)

print(
    f"Raw Utilization    : "
    f"{utilization_raw!r}"
)

print(
    f"Utilization        : "
    f"{utilization_value:.6f}"
)

print(
    f"Utilization Percent: "
    f"{utilization_value * 100:.2f}%"
)

print(
    f"Target Capacity    : "
    f"{CAPACITY_NAME}"
)

print(
    f"Scale Range        : "
    f"{SOURCE_SKU} -> {TARGET_SKU}"
)


# ============================================================
# 12. Azure ARM Authentication
# ============================================================

credential = ClientSecretCredential(
    tenant_id=TENANT_ID,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
)

access_token = credential.get_token(
    "https://management.azure.com/.default"
).token

HEADERS = {
    "Authorization":
        f"Bearer {access_token}",

    "Content-Type":
        "application/json",

    "Accept":
        "application/json",
}


print(
    "Azure ARM authentication succeeded."
)


# ============================================================
# 13. Fabric Capacity Functions
# ============================================================

def get_capacity() -> dict:
    response = requests.get(
        CAPACITY_URL,
        headers=HEADERS,
        timeout=HTTP_GET_TIMEOUT_SECONDS,
    )

    if response.status_code != 200:
        raise RuntimeError(
            "Failed to query Fabric Capacity.\n"
            f"HTTP Status: "
            f"{response.status_code}\n"
            f"Response: "
            f"{response.text[:2000]}"
        )

    return response.json()


def extract_capacity_state(
    capacity: dict,
) -> dict:
    properties = capacity.get(
        "properties",
        {},
    )

    sku = capacity.get(
        "sku",
        {},
    )

    return {
        "name":
            capacity.get("name"),

        "location":
            capacity.get("location"),

        "sku":
            str(
                sku.get(
                    "name",
                    "",
                )
            ).upper(),

        "tier":
            sku.get("tier"),

        "state":
            properties.get("state"),

        "provisioningState":
            properties.get(
                "provisioningState"
            ),
    }


# ============================================================
# 14. Current Fabric Capacity State
# ============================================================

current_capacity = get_capacity()

current_state = extract_capacity_state(
    current_capacity
)

current_sku = current_state[
    "sku"
]


print_section(
    "Current Fabric Capacity State"
)

print(
    json.dumps(
        current_state,
        indent=2,
        ensure_ascii=False,
    )
)


# ============================================================
# 15. Scale-up Decision
# ============================================================

if current_sku == TARGET_SKU:
    print_section(
        "Scale-up Skip"
    )

    print(
        f"Trigger Utilization : "
        f"{utilization_value:.6f}"
    )

    print(
        f"Utilization Percent : "
        f"{utilization_value * 100:.2f}%"
    )

    print(
        f"Current SKU         : "
        f"{current_sku}"
    )

    print(
        f"Target SKU          : "
        f"{TARGET_SKU}"
    )

    print(
        "Capacity is already running "
        "on the target SKU. "
        "No scale-up is required."
    )


elif current_sku != SOURCE_SKU:
    raise RuntimeError(
        "Current SKU is outside the "
        "configured scale-up range.\n"
        f"Allowed SKU: "
        f"{SOURCE_SKU}, {TARGET_SKU}\n"
        f"Current SKU: "
        f"{current_sku}\n"
        f"Trigger Utilization: "
        f"{utilization_value:.6f}"
    )


else:
    # ========================================================
    # 16. Execute Scale-up
    # ========================================================

    print_section(
        "Scale-up Execution"
    )

    print(
        f"Trigger Utilization : "
        f"{utilization_value:.6f}"
    )

    print(
        f"Utilization Percent : "
        f"{utilization_value * 100:.2f}%"
    )

    print(
        f"Current SKU         : "
        f"{current_sku}"
    )

    print(
        f"Target SKU          : "
        f"{TARGET_SKU}"
    )

    payload = {
        "sku": {
            "name": TARGET_SKU,
            "tier": "Fabric",
        }
    }

    update_response = requests.patch(
        CAPACITY_URL,
        headers=HEADERS,
        json=payload,
        timeout=HTTP_PATCH_TIMEOUT_SECONDS,
    )

    print()

    print(
        f"PATCH HTTP Status: "
        f"{update_response.status_code}"
    )

    if update_response.text:
        try:
            print(
                json.dumps(
                    update_response.json(),
                    indent=2,
                    ensure_ascii=False,
                )
            )

        except ValueError:
            print(
                update_response.text[:2000]
            )

    if (
        update_response.status_code
        not in {200, 201, 202}
    ):
        raise RuntimeError(
            "Fabric Capacity scale-up "
            "request failed.\n"
            f"HTTP Status: "
            f"{update_response.status_code}\n"
            f"Response: "
            f"{update_response.text[:2000]}"
        )

    print(
        "Fabric Capacity scale-up "
        "request accepted: "
        f"{current_sku} -> {TARGET_SKU}"
    )


    # ========================================================
    # 17. Verify Scale-up
    # ========================================================

    for retry_count in range(
        1,
        MAX_RETRY + 1,
    ):
        time.sleep(
            RETRY_INTERVAL_SECONDS
        )

        checked_capacity = (
            get_capacity()
        )

        checked_state = (
            extract_capacity_state(
                checked_capacity
            )
        )

        checked_sku = checked_state[
            "sku"
        ]

        checked_state_value = (
            checked_state.get(
                "state"
            )
        )

        checked_provisioning_state = str(
            checked_state.get(
                "provisioningState"
            )
            or ""
        )

        print(
            f"[{retry_count}/{MAX_RETRY}] "
            f"SKU={checked_sku}, "
            f"State={checked_state_value}, "
            f"ProvisioningState="
            f"{checked_provisioning_state}"
        )

        if (
            checked_sku
            == TARGET_SKU
            and checked_provisioning_state.lower()
            == "succeeded"
        ):
            print_section(
                "Fabric Capacity Scale-up Completed"
            )

            print(
                f"Trigger Utilization : "
                f"{utilization_value:.6f}"
            )

            print(
                f"Utilization Percent : "
                f"{utilization_value * 100:.2f}%"
            )

            print(
                f"Previous SKU        : "
                f"{current_sku}"
            )

            print(
                f"Current SKU         : "
                f"{checked_sku}"
            )

            print(
                f"State               : "
                f"{checked_state_value}"
            )

            print(
                f"Completed Time KST  : "
                f"{now_kst()}"
            )

            break

    else:
        raise TimeoutError(
            "Scale-up request was accepted, "
            "but the target SKU was not confirmed "
            "within the configured timeout."
        )