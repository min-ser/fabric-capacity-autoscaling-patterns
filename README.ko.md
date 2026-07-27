# Microsoft Fabric Capacity Autoscaling Patterns

본 Repository는 **Microsoft Fabric Capacity의 사용량을 기반으로 Fabric SKU를 자동 조정하는 Autoscaling 아키텍처 및 POC 구현 패턴**을 정리한 프로젝트입니다.

Microsoft Fabric Real-Time Intelligence의 Capacity Event를 기반으로 다음 두 가지 방식을 구성하고 검증합니다.

1. **Eventstream + Activator + Notebook 방식**
2. **Eventstream Custom Endpoint + AKS CronJob 방식**

두 방식 모두 Fabric Capacity Event를 사용량 지표의 Source로 사용하지만, **Autoscaling 조건을 판단하고 실제 SKU 변경을 수행하는 Control Plane의 위치가 다릅니다.**

---

# Overview

Microsoft Fabric Capacity에서는 Capacity 사용량과 상태 정보를 실시간 Event 형태로 제공할 수 있습니다.

본 프로젝트에서는 다음 Event Type을 주요 Autoscaling Metric Source로 사용합니다.

```text
Microsoft.Fabric.Capacity.Summary
```

해당 Event에서는 다음과 같은 Capacity 정보를 확인할 수 있습니다.

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

해당 정보를 이용하여 Fabric Capacity Utilization을 계산하고 현재 Capacity SKU를 Scale Up 또는 Scale Down할지 판단할 수 있습니다.

---

# Architecture Patterns

본 Repository에서는 동일한 Fabric Capacity Event를 기반으로 두 가지 Autoscaling 구조를 비교합니다.

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

Fabric 내부의 Real-Time Intelligence 기능을 중심으로 Autoscaling을 구성하는 방식입니다.

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

Eventstream에서 `Microsoft.Fabric.Capacity.Summary` Event를 처리하고 Utilization을 계산합니다.

Activator에서는 다음과 같은 Window 기반 Rule을 사용할 수 있습니다.

```text
Window Size  : 10분
Step Size    : 1분
Time Elapsed : 5분
```

예:

```text
Scale Up
Utilization > 60%

Scale Down
Utilization < 30%
```

조건이 지정된 시간 동안 지속되면 Activator가 Fabric Notebook을 실행합니다.

Notebook은 다음 작업을 수행합니다.

```text
1. Activator Parameter 수신
2. Utilization 검증
3. Azure 인증
4. 현재 Fabric Capacity 조회
5. 현재 SKU 확인
6. Target SKU 결정
7. SKU_MIN / SKU_MAX 검증
8. Azure ARM API 호출
9. Capacity SKU 변경
```

### 특징

- Fabric 내부 서비스 중심의 Autoscaling
- Event-driven 방식의 Rule 평가
- Activator Window / Step / Time Elapsed 기능 활용
- 별도 Event Consumer Application 불필요
- Autoscaling Control Logic이 Fabric 내부에서 실행
- Scale 실행 시 Fabric Notebook Session 초기화 필요

---

# Pattern 2 — Eventstream Custom Endpoint + AKS CronJob

📁 [`eventstream-custom-endpoint`](./eventstream-custom-endpoint)

Autoscaling Control Plane을 Fabric 외부의 AKS로 분리하는 방식입니다.

Fabric Eventstream은 Capacity Event를 제공하는 역할만 수행하며, AKS CronJob에서 Custom Endpoint를 통해 Event를 수신하고 Autoscaling 로직을 수행합니다.

```text
Fabric Capacity
        ↓
Capacity Summary Event
        ↓
Fabric Eventstream
        ↓
Custom Endpoint
(Event Hubs Compatible)
        ↓
AMQP over WebSocket
TCP 443
        ↓
AKS CronJob
        ↓
Python Autoscaler
        ↓
Utilization 계산
        ↓
Autoscaling 판단
        ↓
Azure ARM API
        ↓
Fabric Capacity SKU
```

Custom Endpoint에서는 다음 연결정보를 제공합니다.

```text
Event Hub Namespace Endpoint
Event Hub Name
Consumer Group
```

AKS에서는 Azure Event Hubs Python SDK를 이용하여 연결할 수 있습니다.

```python
client = EventHubConsumerClient(
    fully_qualified_namespace=EVENTHUB_NAMESPACE,
    eventhub_name=EVENTHUB_NAME,
    consumer_group=CONSUMER_GROUP,
    credential=DefaultAzureCredential(),
    transport_type=TransportType.AmqpOverWebsocket,
)
```

CronJob은 상시 실행 Consumer가 아니라 **One-Shot Batch 방식**으로 동작합니다.

```text
CronJob Schedule
      ↓
Job 생성
      ↓
Pod 생성
      ↓
Eventstream 연결
      ↓
Capacity Summary 수신
      ↓
Utilization 계산
      ↓
Autoscale Policy 판단
      ↓
Azure ARM API 호출
      ↓
Process 종료
      ↓
Pod Completed
```

### 특징

- Autoscaling Control Plane을 Fabric 외부로 분리
- 기존 AKS Batch / CronJob 운영체계 활용
- Fabric Notebook Session 불필요
- Microsoft Entra ID / AKS Workload Identity 인증
- Event Hubs-compatible Custom Endpoint 사용
- AMQP over WebSocket / TCP 443
- AKS에서 Eventstream Endpoint로 Outbound 통신 필요

---

# 두 방식 비교

| 구분 | Activator + Notebook | Custom Endpoint + AKS CronJob |
|---|---|---|
| Metric Source | Fabric Capacity Event | Fabric Capacity Event |
| Event Processing | Fabric Eventstream | Fabric Eventstream |
| Utilization 계산 | Eventstream Query | Python |
| Threshold 판단 | Fabric Activator | Python Autoscaler |
| 실행 Trigger | Activator Rule | Kubernetes CronJob |
| Scale 실행 | Fabric Notebook | AKS Python Application |
| ARM API 호출 | Notebook | AKS |
| Notebook Session | 필요 | 불필요 |
| 외부 Event Endpoint | 불필요 | 필요 |
| Eventstream Network | Fabric 내부 | AKS Outbound 필요 |
| 인증 | Fabric 실행 Identity | Entra ID / Workload Identity |
| Control Plane | Microsoft Fabric | AKS |
| 실행 모델 | Event-driven | Scheduled One-Shot |
| Window 상태관리 | Activator | Application Logic |
| 운영체계 | Fabric 중심 | Kubernetes 중심 |

---

# Capacity Utilization 계산

두 방식 모두 동일한 Fabric Capacity Summary Metric을 사용합니다.

Capacity Summary Window에서 사용 가능한 총 Capacity Budget은 다음과 같습니다.

```text
Capacity Budget (CUms)
    =
    baseCapacityUnits
    × Window Duration (ms)
```

Utilization:

```text
Utilization (%)
    =
    capacityUnitMs
    / Capacity Budget
    × 100
```

30초 Window라면:

```text
Utilization (%)
    =
    capacityUnitMs
    / (baseCapacityUnits × 30,000)
    × 100
```

예:

```text
baseCapacityUnits = 32
capacityUnitMs    = 465,000
Window            = 30 seconds
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

Fabric Capacity Autoscaling은 환경별 허용 SKU 범위 내에서 수행하는 것을 권장합니다.

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

예:

```text
DEV : F2  ~ F8
STG : F4  ~ F64
PRD : F16 ~ F64
```

실제 SKU 범위는 각 환경의 Workload 및 운영 정책에 따라 설정해야 합니다.

---

# Authentication

Credential을 Source Code에 직접 저장하지 않는 것을 권장합니다.

로컬 개발 환경:

```text
DefaultAzureCredential
        ↓
Azure CLI / Developer Credential
```

AKS:

```text
Kubernetes ServiceAccount
        ↓
AKS Workload Identity
        ↓
Microsoft Entra ID
```

다음 정보는 Repository에 Commit하지 않습니다.

```text
Client Secret
Access Token
SAS Key
Event Hub Connection String
Webhook URL
실제 운영 Credential
```

---

# Network Requirements

Custom Endpoint 방식에서는 AKS에서 Fabric Eventstream Endpoint로의 Outbound 통신이 필요합니다.

```text
Direction   : Outbound
Destination : *.servicebus.windows.net
Protocol    : AMQP over WebSocket / TLS
Port        : TCP 443
```

Python SDK에서는 다음 Transport를 사용합니다.

```python
TransportType.AmqpOverWebsocket
```

이를 통해 기본 AMQP 5671 대신 TCP 443을 사용할 수 있습니다.

Activator + Notebook 방식에서는 AKS가 Eventstream Endpoint에 접근하지 않으므로 해당 Outbound 구성이 필요하지 않습니다.

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

# 어떤 방식을 선택할 것인가

## Eventstream + Activator + Notebook

다음 상황에 적합합니다.

- Fabric Native Architecture를 우선하는 경우
- Autoscaling Logic을 Fabric 내부에서 관리하려는 경우
- Activator의 Window / Step / Time Elapsed 기능을 활용하려는 경우
- 별도 Kubernetes Consumer를 운영하지 않으려는 경우
- Notebook Session 초기화 시간이 허용 가능한 경우

## Eventstream Custom Endpoint + AKS CronJob

다음 상황에 적합합니다.

- 기존 AKS Batch 운영체계를 활용하려는 경우
- Autoscaling Control Logic을 Fabric 외부에서 관리하려는 경우
- Notebook Session 초기화 시간을 제거하려는 경우
- Kubernetes 기반 배포/로그/스케줄 관리체계를 재사용하려는 경우
- Python 기반 Autoscaling Policy를 세밀하게 제어하려는 경우

---

# POC 검증 항목

## Activator + Notebook

```text
Capacity Summary Event 수집
Eventstream Utilization 계산
Activator Rule 평가
Window / Step / Time Elapsed 구성
Activator Parameter 전달
Scale Up Notebook
Scale Down Notebook
Azure ARM Capacity SKU 변경
```

## Custom Endpoint + AKS

```text
Fabric Eventstream Custom Endpoint
Event Hubs-compatible Connection
Microsoft Entra ID 인증
AKS Workload Identity
AMQP over WebSocket / TCP 443
Capacity Summary Event 수신
Utilization 계산
One-Shot CronJob 실행
Azure ARM Capacity 연계
```

---

# Security Considerations

본 Repository는 Architecture Pattern 및 Sample 구현을 목적으로 합니다.

Public Repository에는 다음 정보를 포함하지 않습니다.

```text
Subscription ID
Tenant ID
실제 Client ID
Client Secret
Capacity Resource ID
실제 Eventstream Endpoint
Consumer Group
Webhook URL
Connection String
Private IP
```

환경별 값은 다음과 같은 방식으로 관리하는 것을 권장합니다.

```text
Environment Variables
Kubernetes Secret
Azure Key Vault
Secret Management System
```

---

# Disclaimer

본 Repository의 Autoscaling Threshold, SKU 범위, Rule Duration 및 Schedule은 POC 및 Reference 예시입니다.

Production 적용 시 다음 항목을 기반으로 별도 검증이 필요합니다.

- 실제 Fabric Workload 특성
- Capacity 사용 패턴
- Scale Up / Down 빈도
- 업무 시간대
- Cost 정책
- Capacity 전환 영향
- 운영 정책

Production에서 자동 SKU 변경을 활성화하기 전에 DEV / STG 환경에서 충분한 검증을 수행하는 것을 권장합니다.

---

# Summary

두 Architecture는 동일한 Fabric Capacity Event를 사용하지만 Autoscaling Control Plane의 위치가 다릅니다.

```text
Pattern 1

Fabric Capacity
    ↓
Eventstream
    ↓
Activator
    ↓
Notebook
    ↓
ARM API
```

```text
Pattern 2

Fabric Capacity
    ↓
Eventstream
    ↓
Custom Endpoint
    ↓
AKS CronJob
    ↓
ARM API
```

Pattern 1은 **Fabric-native Event-driven Autoscaling**에 가깝고,

Pattern 2는 **Event-driven Metrics + Kubernetes Scheduled Control** 구조입니다.

운영 환경에서는 조직의 Platform Architecture, Network Policy, 실행 지연 요구사항 및 운영체계를 고려하여 적합한 방식을 선택할 수 있습니다.

---

## Maintainer

**김민서**  
MegazoneCloud