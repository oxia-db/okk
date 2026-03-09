# okk-pilot: Code-Driven Continuous Verification for Oxia

**Status**: Implemented
**Authors**: @mattison
**Date**: 2026-03-09

## Overview

okk-pilot is a deterministic, code-driven continuous verification pilot for the [Oxia](https://github.com/oxia-db/oxia) distributed key-value store. It replaces the earlier AI-agent approach (`okk-agent`) with programmatic event handling, chaos injection, scale testing, and invariant checking — while optionally using AI only for log analysis.

### Why not AI-driven?

The original `okk-agent` relied on an LLM (via tool calling) to decide what actions to take. This had fundamental problems:
- Small models (Qwen 2.5:7b) couldn't reliably use function calling
- Larger models (GPT-4o) had rate limits that made continuous operation impractical
- Non-deterministic behavior: the same event could produce different (sometimes wrong) actions
- Debugging was difficult — "why did the agent do X?" had no clear answer

okk-pilot moves all decision-making into code. Every event type has a deterministic handler. AI is optional and scoped to a single task: analyzing error logs for root cause hints.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    okk-pilot                         │
│                                                      │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐ │
│  │ Event Sources │  │    Pilot     │  │   Tools    │ │
│  │              │  │  (handlers)  │  │            │ │
│  │ • K8s Watch  │──│ startup     │──│ observe    │ │
│  │ • Cron       │  │ health_check│  │ act        │ │
│  │ • Webhook    │  │ chaos_round │  │ report     │ │
│  │ • GH Poller  │  │ scale_event │  │ state      │ │
│  │              │  │ daily_report│  │ invariants │ │
│  └──────────────┘  │ gh_comment  │  └────────────┘ │
│                    │ pod_event   │                  │
│                    │ k8s_warning │  ┌────────────┐ │
│                    └──────────────┘  │ AI (opt.)  │ │
│                                      │ log anlysis│ │
│                                      └────────────┘ │
└─────────────────────────────────────────────────────┘
         │                    │                │
    K8s API            Coordinator        GitHub API
         │                    │                │
   ┌─────┴─────┐      ┌──────┴──────┐   ┌────┴────┐
   │ Oxia      │      │ okk-coord   │   │ oxia-db │
   │ Cluster   │      │ + worker    │   │ /okk    │
   └───────────┘      └─────────────┘   └─────────┘
```

### Components

| Component | Language | Role |
|-----------|----------|------|
| **okk-coordinator** | Go | Generates test operations, streams to workers, tracks assertions |
| **okk-jvm-worker** | Java | Executes operations against Oxia, validates assertions |
| **okk-pilot** | Python | Orchestrates verification: chaos, scale, invariant checks, reporting |
| **Oxia cluster** | Go | The system under test (distributed KV store) |

## Event System

okk-pilot uses an event-driven architecture. Events come from four sources and are dispatched to deterministic handlers via a `ThreadPoolExecutor` (4 workers).

### Event Sources

| Source | Events | Mechanism |
|--------|--------|-----------|
| **CronScheduler** | `health_check` (5m), `periodic_summary` (4h), `chaos_round` (2h), `scale_event` (6h), `daily_report` (midnight UTC) | APScheduler |
| **K8sWatcher** | `pod_restart`, `crash_loop`, `k8s_warning` | K8s Watch API (2 threads) |
| **Webhook** | `github_comment`, `new_tag` | FastAPI POST `/webhook/github` |
| **GitHubPoller** | `github_comment` | Polls open issues every 2 min |

### Event Handlers

| Event | Handler | Behavior |
|-------|---------|----------|
| `startup` | `_handle_startup` | Check invariants, ensure testcases running, post stats |
| `health_check` | `_handle_health_check` | Check invariants; only post to GitHub if violations found; use AI for log analysis if available |
| `periodic_summary` | `_handle_periodic_summary` | Gather snapshot, post stats to daily issue |
| `chaos_round` | `_handle_chaos_round` | Pre-check → cleanup stale chaos → inject (round-robin) → wait → post-check → report |
| `scale_event` | `_handle_scale_event` | Pre-check → scale down → wait → check → scale up → wait → check → report |
| `daily_report` | `_handle_daily_report` | Post end-of-day summary, close daily issue |
| `github_comment` | `_handle_github_comment` | Parse command: `status`, `chaos`, `stop`, `start`, `scale N` |
| `pod_restart` / `crash_loop` | `_handle_pod_event` | Only report if restart_count >= 3 or OOMKilled |
| `k8s_warning` | `_handle_k8s_warning` | Filter transient warnings; only report significant ones |

### Event Deduplication

- 300-second cooldown on identical event signatures (`type:summary[:80]`)
- Scheduled and interactive events bypass dedup (startup, health_check, chaos_round, etc.)

## Invariant Checking

Three-tier invariant system checking correctness, availability, and performance:

### Safety Tier (Correctness)

| Check | Condition | Source |
|-------|-----------|--------|
| `safety.coordinator_reachable` | HTTP GET `/testcases` succeeds | Coordinator API |
| `safety.testcases_exist` | At least one testcase running | Coordinator API |
| `safety.no_assertion_failures.{name}` | `assertions_failed == 0` per testcase | Coordinator API |
| `safety.testcase_running.{name}` | `state == "running"` per testcase | Coordinator API |

### Liveness Tier (Availability)

For each pod group (`oxia_servers`, `oxia_coordinator`, `okk_worker`, `okk_coordinator`):

| Check | Condition | Source |
|-------|-----------|--------|
| `liveness.pods_exist.{group}` | Pods found matching selector | K8s API |
| `liveness.pods_healthy.{group}` | All containers Ready | K8s API |
| `liveness.restarts.{group}` | Total restarts <= 10 | K8s API |

### Performance Tier (Efficiency)

| Check | Condition | Source |
|-------|-----------|--------|
| `performance.p99_latency` | p99 <= 500ms | Prometheus |
| `performance.throughput` | ops/s > 0 | Prometheus |

### Verdict

- Per-tier: PASS if all checks in that tier pass
- Overall: PASS if all three tiers pass
- Failed checks produce: `"VIOLATION in {tiers}: {messages}"`

## Chaos Injection

Automated fault injection using [Chaos Mesh](https://chaos-mesh.org/) CRDs.

### Round-Robin Cycle

```
pod-kill → network-delay → pod-failure → cpu-stress → clock-skew → (repeat)
```

The current index is persisted in a K8s ConfigMap (`okk-pilot-state`) so it survives pod restarts.

### Chaos Round Flow

```
1. Pre-check: all invariants must pass (skip if unhealthy)
2. Cleanup: delete any stuck chaos experiments (pilot-* or agent-*)
3. Select: next chaos type via round-robin index
4. Inject: create Chaos Mesh CRD (default: 30s duration, target: Oxia server pods)
5. Wait: poll until chaos experiment deleted (up to 90s)
6. Post-check: verify invariants again
7. Report: post results to daily GitHub issue
```

### Supported Chaos Types

| Type | Chaos Mesh Kind | Effect |
|------|----------------|--------|
| `pod-kill` | PodChaos | Kill one pod |
| `pod-failure` | PodChaos | Make one pod unavailable |
| `network-delay` | NetworkChaos | 10ms latency + 5ms jitter |
| `network-partition` | NetworkChaos | Bidirectional network partition |
| `cpu-stress` | StressChaos | 100% CPU on 1 core |
| `memory-stress` | StressChaos | 256MB memory allocation |
| `clock-skew` | TimeChaos | +10s clock offset |

## Scale Testing

Validates that Oxia handles scaling events while maintaining invariants.

### Scale Event Flow

```
1. Pre-check: all invariants pass + no active chaos
2. Scale down: replicas N → N-1
3. Wait 30s, check invariants at reduced capacity
4. Scale up: replicas N-1 → N
5. Wait 30s, check invariants after recovery
6. Report results
```

## GitHub Reporting

### Daily Issue Tracking

- One issue per UTC day: `"🤖 okk-pilot daily report — YYYY-MM-DD"`
- Labeled `daily-report`
- All events post stats comments to the daily issue
- Closed automatically at end of day

### Stats Comment Format

```
🤖 health_check | ops: 1,234,567 | assertions: 1,234,567 passed, 0 failed | p99: 2.3ms | throughput: 100.0 ops/s
basic-kv-test: 800,000 ops, 800,000✓ 0✗
streaming-seq-test: 434,567 ops, 434,567✓ 0✗
All invariants hold.
```

### Interactive Commands

Comment on a daily issue mentioning `@okk-pilot`:

| Command | Action |
|---------|--------|
| `@okk-pilot status` | Reply with current stats snapshot |
| `@okk-pilot chaos` | Trigger immediate chaos round |
| `@okk-pilot stop` | Delete all running testcases |
| `@okk-pilot start` | Ensure default testcases are running |
| `@okk-pilot scale N` | Scale Oxia to N replicas |

## AI Analysis (Optional)

When `AI_ENABLED=true`, the pilot uses an OpenAI-compatible API (e.g., Ollama) for **log analysis only**.

### When AI is Used

- After a health check finds invariant violations
- After a chaos round detects recovery issues
- When a pod restarts with >= 3 restarts or OOMKilled

### How It Works

1. Gather recent pod logs (5 min, 200 lines)
2. Check for error keywords: `error`, `panic`, `fatal`, `oom`, `timeout`
3. If found, send to AI with context and ask for 1-2 sentence root cause
4. Append `"AI analysis: ..."` to the report if the answer isn't `"NORMAL"`

### Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `AI_ENABLED` | `false` | Enable AI log analysis |
| `AI_URL` | `http://host.docker.internal:11434` | Ollama/OpenAI-compatible server |
| `AI_MODEL` | `qwen2.5:14b` | Model name |

## Configuration

All configuration via environment variables:

| Env Var | Default | Description |
|---------|---------|-------------|
| `GITHUB_TOKEN` | (empty) | GitHub API token (optional — disables reporting if empty) |
| `GITHUB_REPO` | `oxia-db/okk` | Target GitHub repo |
| `OKK_NAMESPACE` | `okk` | Kubernetes namespace |
| `IN_CLUSTER` | `true` | Use in-cluster K8s config |
| `PROMETHEUS_URL` | `http://prometheus...:9090` | Prometheus endpoint |
| `WEBHOOK_PORT` | `8080` | Webhook server port |
| `DAILY_REPORT_HOUR` | `0` | UTC hour for daily report |
| `COORDINATOR_URL` | `http://okk-coordinator:8080` | OKK coordinator endpoint |
| `OXIA_REPLICAS` | `3` | Expected Oxia replica count |
| `CHAOS_DURATION` | `30s` | Chaos experiment duration |
| `CHAOS_TARGET` | `app.kubernetes.io/name=oxia-cluster,...` | Chaos target label selector |

## Deployment

### Helm Chart

```yaml
pilot:
  enabled: true
  image: okk-pilot:local
  replicas: 1
  port: 8080
  env:
    OKK_NAMESPACE: okk
    COORDINATOR_URL: "http://okk-coordinator:8080"
    PROMETHEUS_URL: "http://prometheus-operated:9090"
    AI_ENABLED: "false"
```

### RBAC

The pilot ServiceAccount requires:
- **Chaos Mesh**: get, list, watch, create, delete on podchaos/networkchaos/stresschaos/timechaos
- **Core**: get, list, watch, create, update, patch on pods, pods/log, events, configmaps, services
- **Apps**: get, list, watch, update, patch on deployments, statefulsets, statefulsets/scale

### Docker Image

```dockerfile
FROM python:3.13-slim
WORKDIR /app
COPY pyproject.toml .
COPY src/ src/
RUN pip install --no-cache-dir .
ENTRYPOINT ["okk-pilot"]
```

## Dependencies

```
kubernetes >= 31.0.0    # K8s API
PyGithub >= 2.5.0       # GitHub API
fastapi >= 0.115.0      # Webhook server
uvicorn >= 0.34.0       # ASGI server
pydantic >= 2.10.0      # Data validation
apscheduler >= 3.10.0   # Cron/interval scheduling
openai >= 1.50.0        # Optional AI client
```

## File Structure

```
pilot/
├── Dockerfile
├── pyproject.toml
└── src/okk_pilot/
    ├── __init__.py
    ├── config.py              # Environment-based configuration
    ├── main.py                # Application entry point, wires everything
    ├── pilot.py               # Core: Event dataclass, Pilot handler class
    ├── events/
    │   ├── cron.py            # APScheduler-based periodic events
    │   ├── github_poller.py   # Polls GitHub issues for @okk-pilot mentions
    │   ├── k8s.py             # K8s pod watcher + event watcher
    │   └── webhook.py         # FastAPI webhook + trigger endpoints
    └── tools/
        ├── act.py             # Mutations: testcase CRUD, chaos injection, scaling
        ├── invariants.py      # Three-tier invariant checker
        ├── observe.py         # Prometheus queries, pod logs/status, testcase status
        ├── report.py          # GitHub issues, comments, PRs
        └── state.py           # ConfigMap-backed persistent state
```

## Future Work

- [ ] Add more chaos types (IO fault, DNS error)
- [ ] Configurable invariant thresholds per environment
- [ ] Webhook for tag push → auto-upgrade Oxia image and re-verify
- [ ] Multi-cluster support (run pilot against multiple Oxia deployments)
- [ ] Grafana dashboard integration for pilot metrics
- [ ] Long-running soak test mode (extended duration with periodic snapshots)
- [ ] Notification channels beyond GitHub (Slack, PagerDuty)
