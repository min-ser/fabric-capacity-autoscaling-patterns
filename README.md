# Fabric Eventstream Autoscaler

> Event-driven Microsoft Fabric Capacity Autoscaling using Eventstream, Activator, and Notebook.

## Overview

Fabric Eventstream Autoscaler is an event-driven autoscaling solution for Microsoft Fabric Capacity.

This project monitors Fabric Capacity utilization in near real-time using **Fabric Eventstream**, evaluates scaling thresholds through **Activator**, and automatically scales Capacity up or down by executing a **Fabric Notebook**.

Unlike traditional scheduled autoscaling, this architecture reacts to utilization events as they occur, enabling faster response and improved cost efficiency.

---

## Architecture

```
Fabric Capacity Metrics
          │
          ▼
   Eventstream
          │
          ▼
     Stream SQL
(Utilization Calculation)
          │
          ▼
      Activator
 (Threshold Detection)
          │
          ▼
     Fabric Notebook
          │
          ▼
 Azure Management API
          │
          ▼
 Fabric Capacity Scale
```

---

## Features

- Near real-time Capacity Monitoring
- Event-driven Autoscaling
- Automatic Scale Up / Scale Down
- Configurable Threshold Rules
- Microsoft Fabric Native Components
- Azure Management REST API Integration

---

## Autoscaling Flow

1. Eventstream receives Fabric Capacity metrics.
2. Stream SQL calculates utilization.
3. Activator detects threshold conditions.
4. Notebook executes automatically.
5. Notebook authenticates with Azure.
6. Azure Management API updates Fabric Capacity SKU.
7. Scaling result is logged.

---

## Scaling Logic

### Scale Up

| Condition | Action |
|-----------|--------|
| Utilization >= 60% | Scale Up |

### Scale Down

| Condition | Action |
|-----------|--------|
| Utilization <= 30% | Scale Down |

Threshold values can be customized.

---

## Technologies

- Microsoft Fabric
- Eventstream
- Activator
- Notebook
- Azure Resource Manager REST API
- Python
- Managed Identity

---

## Repository Structure

```
/
├── notebook/
│   └── autoscale.ipynb
│
├── images/
│   └── architecture.png
│
├── docs/
│   ├── architecture.md
│   └── scaling-flow.md
│
├── README.md
└── LICENSE
```

---

## Example Event Flow

```
Capacity Utilization

      │
      ▼

 Eventstream

      │
      ▼

 Stream SQL
 AVG(Utilization)

      │
      ▼

 Activator Rule

      │
      ▼

 Notebook

      │
      ▼

 Azure REST API

      │
      ▼

 Scale Capacity
```

---

## Benefits

- Reduce Fabric Capacity Cost
- Automatic Performance Optimization
- Event-driven Architecture
- Native Microsoft Fabric Components
- No External Scheduler Required

---

## Future Improvements

- Multiple Capacity Support
- Dynamic Threshold
- Teams Notification
- Capacity History Dashboard
- Automatic Cooldown Period
- ML-based Predictive Autoscaling

---

## References

- Microsoft Fabric
- Microsoft Fabric Eventstream
- Microsoft Fabric Activator
- Azure Resource Manager REST API

---

## License

MIT License
