from __future__ import annotations

import json
import logging
import urllib.request
import urllib.parse
from dataclasses import dataclass, asdict
from typing import Any

from kubernetes import client as k8s_client

from okk_agent.config import Config

logger = logging.getLogger(__name__)


@dataclass
class CheckResult:
    name: str
    tier: str  # "safety", "liveness", "performance"
    passed: bool
    message: str
    value: Any = None


@dataclass
class InvariantVerdict:
    passed: bool
    tier_results: dict[str, bool]
    checks: list[CheckResult]
    summary: str


class InvariantChecker:
    def __init__(self, config: Config, k8s_core: k8s_client.CoreV1Api):
        self.config = config
        self.k8s_core = k8s_core
        self._coordinator_url = config.coordinator_url.rstrip("/")
        self._prometheus_url = config.prometheus_url
        self._namespace = config.namespace

    def check_invariants(self, p99_threshold_ms: float = 500.0) -> str:
        checks: list[CheckResult] = []
        checks.extend(self._check_safety())
        checks.extend(self._check_liveness())
        checks.extend(self._check_performance(p99_threshold_ms))

        tier_results = {}
        for tier in ("safety", "liveness", "performance"):
            tier_checks = [c for c in checks if c.tier == tier]
            tier_results[tier] = all(c.passed for c in tier_checks) if tier_checks else True

        all_passed = all(tier_results.values())
        failed = [c for c in checks if not c.passed]

        if all_passed:
            summary = "All invariants hold."
        else:
            failed_tiers = [t for t, ok in tier_results.items() if not ok]
            summary = f"VIOLATION in {', '.join(failed_tiers)}: " + "; ".join(
                c.message for c in failed[:3]
            )

        verdict = InvariantVerdict(
            passed=all_passed,
            tier_results=tier_results,
            checks=checks,
            summary=summary,
        )
        return json.dumps(asdict(verdict), indent=2)

    # ── Safety ──────────────────────────────────────────────

    def _check_safety(self) -> list[CheckResult]:
        results: list[CheckResult] = []
        try:
            url = f"{self._coordinator_url}/testcases"
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
        except Exception as e:
            return [CheckResult(
                name="safety.coordinator_reachable",
                tier="safety",
                passed=False,
                message=f"Cannot reach coordinator: {e}",
            )]

        testcases = data.get("testcases", [])
        if not testcases:
            results.append(CheckResult(
                name="safety.testcases_exist",
                tier="safety",
                passed=False,
                message="No testcases running",
            ))
            return results

        for tc in testcases:
            name = tc["name"]
            failures = tc.get("assertions_failed", 0)
            state = tc.get("state", "unknown")

            results.append(CheckResult(
                name=f"safety.no_assertion_failures.{name}",
                tier="safety",
                passed=failures == 0,
                message=f"{failures} assertion failures in {name}" if failures > 0
                        else f"Zero assertion failures in {name}",
                value=failures,
            ))

            results.append(CheckResult(
                name=f"safety.testcase_running.{name}",
                tier="safety",
                passed=state == "running",
                message=f"{name} is {state}" if state != "running"
                        else f"{name} is running",
                value=state,
            ))

        return results

    # ── Liveness ────────────────────────────────────────────

    def _check_liveness(self) -> list[CheckResult]:
        results: list[CheckResult] = []

        pod_groups = [
            ("oxia_servers", "oxia_cluster=oxia,app.kubernetes.io/component=server"),
            ("oxia_coordinator", "oxia_cluster=oxia,app.kubernetes.io/component=coordinator"),
            ("okk_worker", "app=okk-jvm-worker"),
            ("okk_coordinator", "app=okk-coordinator"),
        ]

        for group_name, selector in pod_groups:
            try:
                pods = self.k8s_core.list_namespaced_pod(
                    namespace=self._namespace,
                    label_selector=selector,
                )
                if not pods.items:
                    results.append(CheckResult(
                        name=f"liveness.pods_exist.{group_name}",
                        tier="liveness",
                        passed=False,
                        message=f"No {group_name} pods found",
                    ))
                    continue

                all_ready = True
                total_restarts = 0
                for pod in pods.items:
                    for cs in (pod.status.container_statuses or []):
                        total_restarts += cs.restart_count
                        if not cs.ready:
                            all_ready = False

                count = len(pods.items)
                results.append(CheckResult(
                    name=f"liveness.pods_healthy.{group_name}",
                    tier="liveness",
                    passed=all_ready,
                    message=f"All {count} {group_name} pods healthy" if all_ready
                            else f"Some {group_name} pods not ready",
                    value=count,
                ))

                results.append(CheckResult(
                    name=f"liveness.restarts.{group_name}",
                    tier="liveness",
                    passed=total_restarts <= 10,
                    message=f"{group_name}: {total_restarts} restarts" + (
                        " (excessive)" if total_restarts > 10 else ""
                    ),
                    value=total_restarts,
                ))

            except Exception as e:
                results.append(CheckResult(
                    name=f"liveness.pods_check.{group_name}",
                    tier="liveness",
                    passed=False,
                    message=f"Failed to check {group_name}: {e}",
                ))

        return results

    # ── Performance ─────────────────────────────────────────

    def _check_performance(self, p99_threshold_ms: float) -> list[CheckResult]:
        results: list[CheckResult] = []

        # P99 latency
        p99_query = 'histogram_quantile(0.99, sum(rate(task_operation_duration_seconds_bucket[5m])) by (le))'
        p99_value = self._query_prometheus_instant(p99_query)

        if p99_value is not None:
            p99_ms = p99_value * 1000
            threshold = p99_threshold_ms
            results.append(CheckResult(
                name="performance.p99_latency",
                tier="performance",
                passed=p99_ms <= threshold,
                message=f"p99 latency {p99_ms:.1f}ms" + (
                    f" > {threshold}ms threshold" if p99_ms > threshold
                    else f" <= {threshold}ms threshold"
                ),
                value=round(p99_ms, 1),
            ))
        else:
            results.append(CheckResult(
                name="performance.p99_latency",
                tier="performance",
                passed=True,
                message="No latency data yet (Prometheus unavailable or no data)",
            ))

        # Throughput > 0
        throughput_query = 'sum(rate(task_operation_duration_seconds_count[5m]))'
        throughput = self._query_prometheus_instant(throughput_query)

        if throughput is not None:
            results.append(CheckResult(
                name="performance.throughput",
                tier="performance",
                passed=throughput > 0,
                message=f"Throughput: {throughput:.1f} ops/s" + (
                    "" if throughput > 0 else " (zero — tests may be stalled)"
                ),
                value=round(throughput, 1),
            ))
        else:
            results.append(CheckResult(
                name="performance.throughput",
                tier="performance",
                passed=True,
                message="No throughput data yet (Prometheus unavailable or no data)",
            ))

        return results

    def _query_prometheus_instant(self, query: str) -> float | None:
        try:
            params = urllib.parse.urlencode({"query": query})
            url = f"{self._prometheus_url}/api/v1/query?{params}"
            with urllib.request.urlopen(url, timeout=10) as resp:
                data = json.loads(resp.read())
                if data.get("status") == "success":
                    results = data.get("data", {}).get("result", [])
                    if results and "value" in results[0]:
                        val = float(results[0]["value"][1])
                        if val != float("inf") and val != float("nan"):
                            return val
        except Exception as e:
            logger.debug("Prometheus query failed: %s", e)
        return None
