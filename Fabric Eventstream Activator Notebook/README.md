# Fabric Eventstream Activator + Notebook 기반 Capacity Autoscaling

Microsoft Fabric Capacity에서 발생하는 실시간 Capacity Summary 이벤트를 Fabric Eventstream으로 수집하고,  
Eventstream의 이벤트를 기반으로 Activator Rule을 평가하여 Fabric Notebook을 실행한 뒤 Azure ARM API를 통해 Fabric Capacity SKU를 자동으로 조정하는 Autoscaling 구조입니다.

![Architecture](architecture.png)

---

## 1. Overview

본 방식은 Microsoft Fabric의 Real-Time Intelligence 기능을 활용하여 Fabric 내부에서 Capacity 사용량을 감지하고 Autoscaling을 수행하기 위한 구조입니다.

Fabric Capacity에서 발생하는 `Microsoft.Fabric.Capacity.Summary` 이벤트를 Eventstream으로 수집하고, Stream에서 계산된 Utilization을 Activator에서 지속적으로 평가합니다.

설정된 Threshold 조건이 일정 시간 이상 충족되면 Activator가 Fabric Notebook을 실행하며, Notebook에서는 Azure ARM API를 호출하여 대상 Fabric Capacity의 SKU를 Scale Up 또는 Scale Down합니다.

전체 Autoscaling Control Flow를 Fabric 내부 구성요소를 중심으로 구현할 수 있다는 것이 이 방식의 주요 특징입니다.

### 주요 특징

- Fabric Capacity Summary Event 기반 사용률 확인
- Fabric Eventstream을 통한 실시간 이벤트 수집
- Stream Query를 통한 Utilization 계산
- Fabric Activator 기반 Threshold Rule 구성
- Window / Step / Time elapsed 기반 지속 조건 판단
- Activator → Fabric Notebook 실행
- Notebook에서 Azure ARM API 호출
- Scale Up / Scale Down Notebook 분리 가능
- Fabric 내부 서비스 중심의 Event-driven Autoscaling
- 별도의 상시 실행 Autoscaler Application 불필요

---

# 2. Architecture

```text
┌─────────────────────────────────────────────┐
│          Microsoft Fabric Capacity          │
│                                             │
│        F2 / F4 / F8 / F16 / F32 ...        │
└──────────────────────┬──────────────────────┘
                       │
                       │ Capacity Metrics Event
                       ▼
┌─────────────────────────────────────────────┐
│              Fabric Eventstream             │
│                                             │
│ Microsoft.Fabric.Capacity.Summary           │
│                                             │
│ - capacityUnitMs                            │
│ - baseCapacityUnits                         │
│ - windowStartTime                           │
│ - windowEndTime                             │
│ - capacitySku                               │
└──────────────────────┬──────────────────────┘
                       │
                       │ Stream Processing
                       ▼
┌─────────────────────────────────────────────┐
│             Eventstream Query               │
│                                             │
│ Utilization 계산                            │
│                                             │
│ capacityUnitMs                              │
│ ───────────────────────────── × 100         │
│ baseCapacityUnits × Window(ms)              │
└──────────────────────┬──────────────────────┘
                       │
                       │ Utilization Event
                       ▼
┌─────────────────────────────────────────────┐
│              Fabric Activator               │
│                                             │
│ Window Size  : 10 minutes                   │
│ Step Size    : 1 minute                     │
│ Time Elapsed : 5 minutes                    │
│                                             │
│ Scale Up Rule / Scale Down Rule             │
└──────────────────────┬──────────────────────┘
                       │
                       │ Rule Trigger
                       ▼
┌─────────────────────────────────────────────┐
│               Fabric Notebook               │
│                                             │
│  Scale Up Notebook                          │
│  Scale Down Notebook                        │
│                                             │
│  1. Activator Parameter 수신                │
│  2. 현재 Capacity/SKU 조회                  │
│  3. Scale Policy 검증                       │
│  4. ARM API 호출                            │
└──────────────────────┬──────────────────────┘
                       │
                       │ Azure ARM API
                       ▼
┌─────────────────────────────────────────────┐
│             Azure Resource Manager          │
│                                             │
│       Microsoft.Fabric/capacities           │
└──────────────────────┬──────────────────────┘
                       │
                       │ SKU Update
                       ▼
┌─────────────────────────────────────────────┐
│          Microsoft Fabric Capacity          │
│                                             │
│             Scale Up / Down                 │
└─────────────────────────────────────────────┘
```

전체 데이터 흐름은 다음과 같습니다.

```text
Fabric Capacity Metrics Event
        ↓
Fabric Eventstream
        ↓
Microsoft.Fabric.Capacity.Summary
        ↓
Stream Query
        ↓
Utilization 계산
        ↓
Fabric Activator
        ↓
Window / Threshold / Time elapsed 평가
        ↓
Activator Rule Trigger
        ↓
Fabric Notebook
        ↓
Azure ARM API
        ↓
Fabric Capacity SKU
Scale Up / Scale Down
```

---

# 3. Fabric Capacity Eventstream 구성

## 3.1 Fabric Capacity Events 연결

Fabric Workspace에서 Capacity 관련 이벤트를 Source로 사용하는 Eventstream을 구성합니다.

본 Autoscaling에서 사용하는 주요 Event Type은 다음과 같습니다.

```text
Microsoft.Fabric.Capacity.Summary
```

Capacity Summary 이벤트에는 일정 Window 단위의 Capacity 사용량 및 상태 정보가 포함됩니다.

대표적인 데이터는 다음과 같습니다.

| Field | Description |
|---|---|
| `capacityId` | Fabric Capacity ID |
| `capacityName` | Fabric Capacity 이름 |
| `capacitySku` | 현재 Capacity SKU |
| `capacityRegion` | Capacity Region |
| `windowStartTime` | 측정 Window 시작 시간 |
| `windowEndTime` | 측정 Window 종료 시간 |
| `baseCapacityUnits` | 현재 Capacity의 기본 CU |
| `capacityUnitMs` | Window 내 사용된 Capacity Unit Milliseconds |
| `utilizationBackground` | Background Workload 사용량 |
| `utilizationInteractive` | Interactive Workload 사용량 |
| `interactiveDelayThresholdPercentage` | Interactive Delay 관련 지표 |
| `interactiveRejectionThresholdPercentage` | Interactive Rejection 관련 지표 |
| `backgroundRejectionThresholdPercentage` | Background Rejection 관련 지표 |
| `overageTotalCapacityUnitMs` | Overage 사용량 |
| `overageBillingProtectionActive` | Overage Billing Protection 상태 |

Eventstream 구성을 완료한 후 반드시 Publish하여 이벤트가 정상적으로 전달되는지 확인합니다.

---

# 4. Utilization 계산

## 4.1 기본 계산 방식

Capacity Summary에서 제공하는 `capacityUnitMs`와 `baseCapacityUnits`를 이용하여 해당 Window의 Capacity Utilization을 계산합니다.

기본 개념은 다음과 같습니다.

```text
Utilization
    =
    실제 사용된 Capacity Unit Time
    ──────────────────────────────
    해당 Window에서 사용 가능한 전체 Capacity Unit Time
```

30초 Window를 예로 들면:

```text
Capacity Budget
    = baseCapacityUnits
      × 30,000 ms
```

따라서:

```text
Utilization
    = capacityUnitMs
      / (baseCapacityUnits × 30,000)
```

백분율로 사용하는 경우:

```text
Utilization (%)
    = capacityUnitMs
      / (baseCapacityUnits × 30,000)
      × 100
```

---

## 4.2 Stream Query

Eventstream에서 Capacity Summary Event를 처리하여 Activator에서 사용할 `utilization` 값을 생성합니다.

개념적인 계산식은 다음과 같습니다.

```sql
capacityUnitMs
/
(
    baseCapacityUnits
    *
    DATEDIFF(
        millisecond,
        windowStartTime,
        windowEndTime
    )
)
```

예를 들어 다음 이벤트가 발생했다고 가정합니다.

```text
baseCapacityUnits : 32
Window            : 30 seconds
capacityUnitMs    : 465,000
```

계산 결과:

```text
465,000
────────────────
32 × 30,000

= 0.484375
```

즉 Activator로 전달되는 값은:

```text
utilization = 0.484375
```

이며 백분율로 해석하면:

```text
48.44%
```

입니다.

> Activator Rule에서 `0.6`을 60%로 사용할 것인지, Stream Query에서 미리 `× 100`하여 `60`을 사용할 것인지 구현 시 일관성을 유지해야 합니다.

---

# 5. Fabric Activator 구성

## 5.1 Activator 역할

Activator는 Eventstream에서 전달되는 Utilization을 지속적으로 평가하고 설정된 조건이 만족되는 경우 Notebook을 실행하는 역할을 담당합니다.

단순히 특정 이벤트 한 건의 값만 보고 Scale을 수행하는 것이 아니라 일정 Window 동안의 상태를 평가하도록 구성할 수 있습니다.

POC에서는 다음과 같은 Rule 조건을 검증하였습니다.

```text
Window Size  : 10 minutes
Step Size    : 1 minute
Time Elapsed : 5 minutes
```

각 항목의 의미는 다음과 같습니다.

| 설정 | 의미 |
|---|---|
| Window Size | 최근 10분 데이터를 기준으로 집계 |
| Step Size | 1분마다 Window 조건을 다시 평가 |
| Time Elapsed | 조건이 5분 동안 지속되는 경우 Trigger |

한 문장으로 표현하면:

> **최근 10분간의 Utilization을 1분마다 평가하고, 설정된 Threshold 조건이 5분 동안 지속될 경우 Autoscaling을 실행합니다.**

---

# 6. Autoscaling Rule

Scale Up과 Scale Down은 각각 독립적인 Activator Rule로 구성할 수 있습니다.

```text
                    Utilization
                         │
             ┌───────────┴───────────┐
             │                       │
             ▼                       ▼
      Scale Up Rule           Scale Down Rule
             │                       │
             ▼                       ▼
      Upscale Notebook       Downscale Notebook
```

## 6.1 Scale Up

예:

```text
Utilization > 60%
```

조건이 설정된 Time elapsed 동안 지속될 경우 Scale Up Notebook을 실행합니다.

```text
Utilization
     ↓
> 60%
     ↓
5분 지속
     ↓
Activator Trigger
     ↓
Scale Up Notebook
     ↓
F16 → F32
```

---

## 6.2 Scale Down

예:

```text
Utilization < 30%
```

조건이 설정된 Time elapsed 동안 지속될 경우 Scale Down Notebook을 실행합니다.

```text
Utilization
     ↓
< 30%
     ↓
5분 지속
     ↓
Activator Trigger
     ↓
Scale Down Notebook
     ↓
F32 → F16
```

Scale Up / Scale Down Threshold 사이에 충분한 간격을 두어 Capacity가 경계값 부근에서 반복적으로 변경되는 현상을 방지하는 것을 권장합니다.

예:

```text
Utilization

100% ┤
     │
 60% ┼──────────── Scale Up
     │
     │    Maintain
     │
 30% ┼──────────── Scale Down
     │
  0% ┤
```

이러한 Threshold 차이는 Autoscaling에서 일종의 Hysteresis 역할을 합니다.

---

# 7. Activator → Notebook 호출

Activator Rule 조건이 충족되면 Fabric Notebook을 Action으로 실행합니다.

Notebook은 Activator에서 전달한 값을 Parameter로 받을 수 있습니다.

예:

```text
utilization
```

POC 과정에서는 Activator에서 전달된 값이 다음과 같이 문자열 형태로 전달되는 경우를 확인하였습니다.

```text
Parameter Name  : utilization
Parameter Value : 0.484638237838223
Python Type     : str
```

따라서 Notebook에서는 명시적으로 숫자 변환을 수행하는 것이 안전합니다.

```python
utilization = float(
    str(utilization).strip()
)
```

이렇게 하면 다음과 같이 처리됩니다.

```text
Activator
   ↓
"0.484638237838223 "
   ↓
strip()
   ↓
"0.484638237838223"
   ↓
float()
   ↓
0.484638237838223
```

---

# 8. Fabric Notebook

Notebook은 Activator가 Autoscaling 조건을 감지한 이후 실제 Capacity Control을 수행합니다.

주요 역할은 다음과 같습니다.

```text
Activator Trigger
        ↓
Notebook Start
        ↓
Parameter Validation
        ↓
Azure Authentication
        ↓
현재 Capacity 조회
        ↓
현재 SKU 확인
        ↓
Target SKU 결정
        ↓
SKU Min / Max 검증
        ↓
ARM API 호출
        ↓
Scale 완료
```

Scale Up / Scale Down을 하나의 Notebook에서 처리할 수도 있지만, Activator Rule과 Action을 명확하게 분리하기 위해 별도의 Notebook으로 구성할 수도 있습니다.

예:

```text
fabric_capacity_upscale
fabric_capacity_downscale
```

---

# 9. Azure Authentication

Notebook에서 Fabric Capacity를 변경하려면 Azure Resource Manager Access Token이 필요합니다.

개념적인 인증 흐름은 다음과 같습니다.

```text
Fabric Notebook
        ↓
Microsoft Entra ID
        ↓
Azure Management Token
        ↓
Azure Resource Manager
        ↓
Microsoft.Fabric/capacities
```

대상 Capacity를 변경할 수 있는 적절한 Azure RBAC 권한이 실행 Identity에 부여되어 있어야 합니다.

---

# 10. Azure ARM API

대상 Azure Resource Provider는 다음과 같습니다.

```text
Microsoft.Fabric/capacities
```

개념적인 Resource URI는 다음과 같습니다.

```text
/subscriptions/<subscription-id>
/resourceGroups/<resource-group>
/providers/Microsoft.Fabric/capacities/<capacity-name>
```

현재 Capacity 정보를 조회하여 SKU를 확인하고, Autoscaling 정책에 따라 Target SKU를 결정합니다.

예:

```text
Current SKU : F16
Utilization : 72%
Threshold   : 60%

Decision
   ↓
Scale Up

Target SKU
   ↓
F32
```

이후 ARM API를 통해 SKU 변경을 요청합니다.

---

# 11. SKU Ladder

Autoscaling 대상 SKU는 환경 및 운영 정책에 따라 제한합니다.

예:

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

Notebook에서는 최소/최대 SKU를 설정하여 허용 범위를 벗어나는 변경을 방지하는 것이 좋습니다.

예:

```text
SKU_MIN = F16
SKU_MAX = F64
```

현재 F64인 경우:

```text
Scale Up
→ Maximum SKU
→ Maintain F64
```

현재 F16인 경우:

```text
Scale Down
→ Minimum SKU
→ Maintain F16
```

---

# 12. Scale Up / Down Flow

## Scale Up

```text
Capacity Summary
        ↓
Eventstream
        ↓
Utilization 계산
        ↓
Activator
        ↓
Utilization > UP Threshold
        ↓
조건 지속시간 확인
        ↓
Upscale Notebook
        ↓
현재 SKU 조회
        ↓
Next SKU
        ↓
ARM PATCH
        ↓
Scale Up
```

## Scale Down

```text
Capacity Summary
        ↓
Eventstream
        ↓
Utilization 계산
        ↓
Activator
        ↓
Utilization < DOWN Threshold
        ↓
조건 지속시간 확인
        ↓
Downscale Notebook
        ↓
현재 SKU 조회
        ↓
Previous SKU
        ↓
ARM PATCH
        ↓
Scale Down
```

---

# 13. POC Configuration

POC에서 검증한 주요 설정 예시는 다음과 같습니다.

| Item | Value |
|---|---|
| Event Type | `Microsoft.Fabric.Capacity.Summary` |
| Capacity Event Window | 30 seconds |
| Activator Window Size | 10 minutes |
| Activator Step Size | 1 minute |
| Activator Time Elapsed | 5 minutes |
| Scale Up Threshold | 60% |
| Scale Down Threshold | 30% |
| Scale Control | Azure ARM API |
| Execution | Fabric Notebook |

> Threshold 및 SKU 범위는 POC 예시이며 실제 운영환경에서는 Capacity Workload 특성을 분석한 후 조정해야 합니다.

---

# 14. 장점

본 방식은 Autoscaling 구성 대부분을 Fabric 내부에서 구현할 수 있다는 장점이 있습니다.

```text
Capacity Event
      ↓
Eventstream
      ↓
Activator
      ↓
Notebook
```

별도의 Kubernetes Consumer나 외부 Autoscaling Application 없이 Fabric Real-Time Intelligence 기능을 활용하여 Autoscaling Workflow를 구성할 수 있습니다.

또한 Activator의 Window / Step / Time elapsed 기능을 활용하면 Python Application에서 별도의 시계열 상태 관리 Logic을 구현하지 않고도 지속적인 Threshold 조건을 평가할 수 있습니다.

---

# 15. 고려사항

Activator + Notebook 방식에서는 Autoscaling Action이 발생할 때 Fabric Notebook이 실행됩니다.

따라서 다음과 같은 특성을 고려해야 합니다.

### Notebook Session 초기화

Notebook이 실행될 때 Fabric Notebook Session이 준비되어야 하므로 실제 Scale Action까지 추가적인 초기화 시간이 발생할 수 있습니다.

```text
Activator Trigger
        ↓
Notebook 요청
        ↓
Session 준비
        ↓
Notebook 실행
        ↓
ARM API
```

따라서 즉각적인 Capacity 변경이 필요한 경우에는 Session 준비 시간이 Autoscaling 반응속도에 영향을 줄 수 있습니다.

### Fabric Resource 의존성

Autoscaling Control Logic 자체가 Fabric Notebook에서 실행되므로 Capacity 부하가 높은 상황에서도 Autoscaling Workflow가 Fabric 실행환경에 의존합니다.

### Notebook 실행 비용/Resource

Capacity를 제어하기 위한 Autoscaling Logic 역시 Fabric Notebook Resource를 사용합니다.

이러한 특성으로 인해 후속 POC에서는 Autoscaling Control Plane을 AKS로 분리하는 `Eventstream Custom Endpoint + AKS CronJob` 방식도 검토하였습니다.

---

# 16. AS-IS / TO-BE 비교

본 방식과 Eventstream Custom Endpoint 방식의 차이는 다음과 같습니다.

| 구분 | Activator + Notebook | Custom Endpoint + AKS CronJob |
|---|---|---|
| Metrics Source | Fabric Capacity Event | Fabric Capacity Event |
| Event 수집 | Eventstream | Eventstream |
| Utilization 처리 | Eventstream Query | AKS Python |
| Threshold 평가 | Fabric Activator | Python Autoscaler |
| 실행 Trigger | Activator | Kubernetes CronJob |
| Scale 실행 | Fabric Notebook | AKS Python |
| ARM API 호출 | Notebook | AKS |
| Notebook Session | 필요 | 불필요 |
| 외부 Event Endpoint | 불필요 | Custom Endpoint 필요 |
| AKS Eventstream 통신 | 불필요 | 필요 |
| Network Firewall | 상대적으로 단순 | TCP 443 Outbound 필요 |
| 상태 평가 | Activator 기능 활용 | Application Logic 필요 |
| Control Plane | Fabric | AKS |
| Scale 반응시간 | Notebook Session 영향 | Event 수신 후 즉시 제어 가능 |
| 운영 구조 | Fabric 중심 | AKS Batch 중심 |

두 방식의 가장 큰 차이는 **Capacity Metrics의 Source가 아니라 Autoscaling Control Logic을 어디에서 실행하느냐**입니다.

```text
Activator + Notebook

Fabric
 ├─ Event
 ├─ Eventstream
 ├─ Threshold 판단
 └─ Notebook Scale Control


Custom Endpoint + AKS

Fabric
 ├─ Event
 └─ Eventstream
       │
       └──── Custom Endpoint ────→ AKS
                                  ├─ Threshold 판단
                                  └─ Scale Control
```

---

# 17. Troubleshooting

## 17.1 Activator Parameter Type

Activator에서 Notebook으로 전달된 Parameter가 문자열로 처리될 수 있습니다.

예:

```text
Parameter Name     : utilization
Parameter Value    : 0.484638237838223
Python Type        : str
Python Full Type   : <class 'str'>
```

Notebook에서는 다음과 같이 명시적인 변환을 권장합니다.

```python
utilization = float(
    str(utilization).strip()
)
```

---

## 17.2 Notebook Session Error

Notebook 실행 과정에서 다음과 같은 오류가 발생할 수 있습니다.

```text
InvalidHttpRequest

Submission failed due to error content =
["requirement failed: Session isn't active."]

HTTP status code: 400
```

Notebook Session 상태를 확인하고 필요한 경우 Session을 재시작한 후 다시 테스트합니다.

---

## 17.3 Activator Rule이 Notebook을 호출하지 않는 경우

다음 항목을 확인합니다.

```text
1. Eventstream Publish 상태
2. Capacity Summary Event 유입 여부
3. Stream Query 결과
4. utilization 데이터 타입
5. Activator Object 연결 상태
6. Rule 활성화 상태
7. Window Size
8. Step Size
9. Time Elapsed
10. Notebook Action 연결 상태
```

특히 Eventstream 또는 Activator 구성을 변경한 이후 Rule과 Object 연결 상태가 정상인지 확인합니다.

---

# 18. Security Considerations

Repository에 다음 정보를 직접 저장하지 않는 것을 권장합니다.

```text
Subscription ID
Tenant-specific Credential
Client Secret
Access Token
Webhook URL
Connection String
Capacity Resource Identifier
```

환경별 값은 Notebook Parameter, Workspace 설정 또는 별도의 Secret Management 방식을 사용하여 관리합니다.

또한 Notebook에서 ARM API를 호출하는 Identity에는 필요한 범위의 Azure RBAC 권한만 부여하는 것을 권장합니다.

---

# 19. Summary

본 구조는 Microsoft Fabric Real-Time Intelligence 기능을 이용하여 **Capacity 이벤트 수집부터 Threshold 감지 및 Autoscaling 실행까지 Fabric 내부에서 처리**하는 방식입니다.

```text
Microsoft Fabric Capacity
        ↓
Capacity Summary Event
        ↓
Fabric Eventstream
        ↓
Utilization
        ↓
Fabric Activator
        ↓
Threshold / Duration Rule
        ↓
Fabric Notebook
        ↓
Azure ARM API
        ↓
Fabric Capacity SKU
Scale Up / Scale Down
```

Activator의 Window 기반 Rule을 이용할 수 있어 Autoscaling 조건을 선언적으로 구성할 수 있으며 별도의 외부 Consumer Application 없이 POC를 빠르게 구현할 수 있습니다.

반면 Notebook Session 초기화 시간과 Fabric Resource 의존성이 존재하므로, 보다 빠른 반응과 Autoscaling Control Plane 분리가 필요한 환경에서는 `Eventstream Custom Endpoint + AKS CronJob` 방식을 대안으로 사용할 수 있습니다.

---

## Maintainer

**김민서**  
MegazoneCloud