# Microsoft Fabric Capacity Autoscaling Patterns

This repository contains reference architectures and proof-of-concept implementations for automatically scaling **Microsoft Fabric Capacity SKUs** based on Fabric Capacity utilization metrics.

The project explores two different autoscaling patterns built around Microsoft Fabric Real-Time Intelligence:

1. **Eventstream + Activator + Notebook**
2. **Eventstream Custom Endpoint + AKS CronJob**

Both approaches use Fabric Capacity events as the source of utilization metrics, but differ in where the autoscaling decision and execution logic runs.

---

## Overview

Microsoft Fabric Capacity exposes real-time capacity usage information through Fabric Capacity events.

The following event type is used as the primary metric source:

```text
Microsoft.Fabric.Capacity.Summary
```

The event contains information such as:

```text
capacityName
capacitySku
capacityRegion
baseCapacityUnits
capacityUnitMs
windowStartTime
windowEndTime
utilizationBackground
utilizationInteractive
interactiveDelayThresholdPercentage
interactiveRejectionThresholdPercentage
backgroundRejectionThresholdPercentage
overageTotalCapacityUnitMs
```

These values can be used to calculate Capacity utilization and determine whether the current Fabric SKU should be scaled up or down.

---

# Architecture Patterns

This repository contains two autoscaling patterns.

```text
Microsoft Fabric Capacity
        ↓
Capacity Summary Events
        ↓
Fabric Eventstream
        ↓
        ├─────────────────────────────────────┐
        │                                     │
        ▼                                     ▼
Pattern 1                               Pattern 2
Activator                              Custom Endpoint
        ↓                                     ↓
Fabric Notebook                        AKS CronJob
        ↓                                     ↓
Azure ARM API                          Azure ARM API
        ↓                                     ↓
Fabric Capacity SKU                    Fabric Capacity SKU
Scale Up / Down                        Scale Up / Down
```

---

# Pattern 1 — Eventstream + Activator + Notebook

📁 [`eventstream-activator-notebook`](./eventstream-activator-notebook)

This pattern keeps the autoscaling control flow primarily inside Microsoft Fabric.

```text
Fabric Capacity
        ↓
Capacity Summary Event
        ↓
Fabric Eventstream
        ↓
Stream Query
        ↓
Utilization
        ↓
Fabric Activator
        ↓
Threshold Rule
        ↓
Fabric Notebook
        ↓
Azure ARM API
        ↓
Fabric Capacity SKU
```

The Eventstream processes `Microsoft.Fabric.Capacity.Summary` events and calculates utilization.

Fabric Activator evaluates the metric using configurable rules such as:

```text
Window Size  : 10 minutes
Step Size    : 1 minute
Time Elapsed : 5 minutes
```

For example:

```text
Scale Up
Utilization > 60%

Scale Down
Utilization < 30%
```

When the rule remains satisfied for the configured duration, Activator triggers a Fabric Notebook.

The Notebook then:

```text
1. Receives the Activator parameter
2. Validates utilization
3. Authenticates to Azure
4. Reads the current Fabric Capacity SKU
5. Determines the target SKU
6. Validates SKU_MIN / SKU_MAX
7. Calls Azure Resource Manager
8. Updates the Fabric Capacity SKU
```

### Characteristics

- Fabric-native autoscaling flow
- Event-driven rule evaluation
- Activator provides Window / Step / Time Elapsed logic
- No external event consumer required
- Autoscaling control runs inside Fabric
- Fabric Notebook session initialization is required before scaling

---

# Pattern 2 — Eventstream Custom Endpoint + AKS CronJob

📁 [`eventstream-custom-endpoint`](./eventstream-custom-endpoint)

This pattern separates the autoscaling control plane from Fabric.

Fabric Eventstream is responsible only for providing Capacity events, while an AKS CronJob consumes the Eventstream Custom Endpoint and performs the autoscaling logic.

```text
Fabric Capacity
        ↓
Capacity Summary Event
        ↓
Fabric Eventstream
        ↓
Custom Endpoint
(Event Hubs-compatible)
        ↓
AMQP over WebSocket
TCP 443
        ↓
AKS CronJob
        ↓
Python Autoscaler
        ↓
Utilization Calculation
        ↓
Autoscaling Decision
        ↓
Azure ARM API
        ↓
Fabric Capacity SKU
```

The Custom Endpoint exposes an Event Hubs-compatible interface containing:

```text
Event Hub Namespace Endpoint
Event Hub Name
Consumer Group
```

The AKS application connects using the Azure Event Hubs Python SDK.

Example:

```python
client = EventHubConsumerClient(
    fully_qualified_namespace=EVENTHUB_NAMESPACE,
    eventhub_name=EVENTHUB_NAME,
    consumer_group=CONSUMER_GROUP,
    credential=DefaultAzureCredential(),
    transport_type=TransportType.AmqpOverWebsocket,
)
```

The CronJob runs as a **one-shot batch process**:

```text
CronJob Schedule
      ↓
Job
      ↓
Pod
      ↓
Connect to Eventstream
      ↓
Receive Capacity Summary
      ↓
Calculate Utilization
      ↓
Evaluate Autoscale Policy
      ↓
Call Azure ARM API
      ↓
Exit
      ↓
Pod Completed
```

### Characteristics

- Autoscaling control plane runs outside Fabric
- Reuses existing AKS batch/CronJob infrastructure
- No Fabric Notebook session initialization
- Microsoft Entra ID / AKS Workload Identity authentication
- Event Hubs-compatible Custom Endpoint
- AMQP over WebSocket on TCP 443
- Requires outbound network access from AKS to the Eventstream endpoint

---

# Pattern Comparison

| Category | Activator + Notebook | Custom Endpoint + AKS CronJob |
|---|---|---|
| Metric Source | Fabric Capacity Events | Fabric Capacity Events |
| Event Processing | Fabric Eventstream | Fabric Eventstream |
| Utilization Calculation | Eventstream Query | Python |
| Threshold Evaluation | Fabric Activator | Python Autoscaler |
| Execution Trigger | Activator Rule | Kubernetes CronJob |
| Scale Execution | Fabric Notebook | AKS Python Application |
| ARM API Caller | Fabric Notebook | AKS |
| Notebook Session | Required | Not required |
| External Event Endpoint | Not required | Required |
| Eventstream Network Access | Internal Fabric flow | AKS outbound access required |
| Authentication | Fabric execution identity | Entra ID / Workload Identity |
| Control Plane | Microsoft Fabric | AKS |
| Runtime Model | Event-driven | Scheduled one-shot |
| Rule Window Management | Activator | Application logic |
| Operational Integration | Fabric-native | Kubernetes-native |

---

# Capacity Utilization

Both patterns are based on the same Fabric Capacity Summary metrics.

For a summary window:

```text
Capacity Budget (CUms)
    =
    baseCapacityUnits
    × Window Duration (ms)
```

Utilization is calculated as:

```text
Utilization (%)
    =
    capacityUnitMs
    / Capacity Budget
    × 100
```

For a 30-second Capacity Summary window:

```text
Utilization (%)
    =
    capacityUnitMs
    / (baseCapacityUnits × 30,000)
    × 100
```

Example:

```text
baseCapacityUnits = 32
capacityUnitMs    = 465,000
window            = 30 seconds
```

```text
Capacity Budget
= 32 × 30,000
= 960,000 CUms
```

```text
Utilization
= 465,000 / 960,000
= 0.484375
= 48.44%
```

---

# SKU Ladder

Autoscaling can be configured using the Fabric SKU ladder.

```text
F2
 ↓
F4
 ↓
F8
 ↓
F16
 ↓
F32
 ↓
F64
```

Environment-specific minimum and maximum limits should be applied.

Example:

```text
DEV : F2  ~ F8
STG : F4  ~ F64
PRD : F16 ~ F64
```

These values are examples only and should be adjusted based on actual workload and operational requirements.

---

# Authentication

Both implementations should avoid hard-coded credentials.

Recommended authentication:

```text
Local Development
        ↓
DefaultAzureCredential
        ↓
Azure CLI / Developer Credential
```

```text
AKS
        ↓
Kubernetes ServiceAccount
        ↓
AKS Workload Identity
        ↓
Microsoft Entra ID
```

Sensitive values such as the following should not be committed to source control:

```text
Client Secret
Access Token
SAS Key
Event Hub Connection String
Webhook URL
Tenant-specific credentials
```

---

# Network Requirements

The Custom Endpoint pattern requires AKS outbound connectivity to the Fabric Eventstream endpoint.

Recommended configuration:

```text
Direction   : Outbound
Destination : *.servicebus.windows.net
Protocol    : AMQP over WebSocket / TLS
Port        : TCP 443
```

Using:

```python
TransportType.AmqpOverWebsocket
```

allows the Event Hubs-compatible connection to operate through TCP 443 rather than requiring native AMQP TCP 5671.

The Activator + Notebook pattern does not require this AKS-to-Eventstream network path.

---

# Repository Structure

```text
fabric-capacity-autoscaling-patterns/
│
├── README.md
├── README.ko.md
│
├── eventstream-activator-notebook/
│   ├── README.md
│   ├── architecture.png
│   ├── upscale/
│   └── downscale/
│
└── eventstream-custom-endpoint/
    ├── README.md
    ├── architecture.png
    ├── samples/
    └── cronjob/
```

---

# When to Use Each Pattern

## Eventstream + Activator + Notebook

Recommended when:

- A Fabric-native architecture is preferred
- Autoscaling logic should stay within Fabric
- Activator Window / Step / Time Elapsed features are useful
- External Kubernetes infrastructure should not be required
- Notebook startup latency is acceptable

## Eventstream Custom Endpoint + AKS CronJob

Recommended when:

- AKS is already used for batch/application workloads
- Autoscaling logic should be managed outside Fabric
- Faster execution without Notebook session initialization is preferred
- Existing Kubernetes deployment, logging, and scheduling standards should be reused
- More control over Python autoscaling logic is required

---

# Proof of Concept Results

The following areas were validated during the POC.

### Activator + Notebook

```text
Capacity Summary Event ingestion
Eventstream utilization calculation
Activator rule evaluation
Window / Step / Time Elapsed configuration
Activator parameter delivery
Scale Up Notebook
Scale Down Notebook
Azure ARM Capacity SKU update
```

### Custom Endpoint + AKS

```text
Fabric Eventstream Custom Endpoint
Event Hubs-compatible connection
Microsoft Entra ID authentication
AKS Workload Identity
AMQP over WebSocket / TCP 443
Capacity Summary event consumption
Utilization calculation
One-shot CronJob execution
Azure ARM Capacity integration
```

---

# Security Considerations

This repository is intended to contain architecture patterns and sample implementations.

Do not commit production-specific information such as:

```text
Subscription ID
Tenant ID
Client ID
Client Secret
Capacity Resource ID
Actual Eventstream endpoint
Consumer Group
Webhook URL
Connection String
Private IP address
```

Use environment variables, Kubernetes Secrets, Azure Key Vault, or an equivalent secret-management mechanism.

---

# Disclaimer

The autoscaling thresholds, SKU ranges, rule durations, and schedules included in this repository are reference examples.

Production values should be determined based on:

- Fabric workload behavior
- Business requirements
- Capacity performance
- Cost constraints
- Scale transition characteristics
- Operational policies

Always validate autoscaling policies in a non-production environment before enabling automatic SKU changes in production.

---

## Maintainer

**Minseo Kim**  
MegazoneCloud