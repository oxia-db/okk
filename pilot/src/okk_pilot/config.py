from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass
class Config:
    # GitHub
    github_token: str = ""
    github_repo: str = "oxia-db/okk"

    # Kubernetes
    namespace: str = "okk"
    in_cluster: bool = True

    # Prometheus
    prometheus_url: str = "http://prometheus.monitoring.svc.cluster.local:9090"

    # Webhook server
    webhook_host: str = "0.0.0.0"
    webhook_port: int = 8080

    # Daily report schedule (hour in UTC)
    daily_report_hour: int = 0

    # Oxia cluster defaults
    oxia_image: str = "oxia/oxia:latest"
    oxia_shards: int = 4
    oxia_replicas: int = 3

    # okk defaults
    okk_worker_image: str = "oxia/okk-jvm-worker:local"
    okk_op_rate: int = 100
    okk_key_space: int = 10000

    # Coordinator
    coordinator_url: str = "http://okk-coordinator:8080"

    # Pilot state
    state_configmap: str = "okk-pilot-state"

    # Chaos defaults
    chaos_duration: str = "30s"
    chaos_target: str = "app.kubernetes.io/name=oxia-cluster,app.kubernetes.io/component=server"

    # AI analysis (optional — pilot works without it)
    ai_enabled: bool = False
    ai_url: str = "http://host.docker.internal:11434"
    ai_model: str = "qwen2.5:14b"

    @classmethod
    def from_env(cls) -> Config:
        return cls(
            github_token=os.environ.get("GITHUB_TOKEN", ""),
            github_repo=os.environ.get("GITHUB_REPO", cls.github_repo),
            namespace=os.environ.get("OKK_NAMESPACE", cls.namespace),
            in_cluster=os.environ.get("IN_CLUSTER", "true").lower() == "true",
            prometheus_url=os.environ.get("PROMETHEUS_URL", cls.prometheus_url),
            webhook_host=os.environ.get("WEBHOOK_HOST", cls.webhook_host),
            webhook_port=int(os.environ.get("WEBHOOK_PORT", cls.webhook_port)),
            daily_report_hour=int(os.environ.get("DAILY_REPORT_HOUR", cls.daily_report_hour)),
            oxia_image=os.environ.get("OXIA_IMAGE", cls.oxia_image),
            oxia_replicas=int(os.environ.get("OXIA_REPLICAS", cls.oxia_replicas)),
            okk_worker_image=os.environ.get("OKK_WORKER_IMAGE", cls.okk_worker_image),
            coordinator_url=os.environ.get("COORDINATOR_URL", cls.coordinator_url),
            chaos_duration=os.environ.get("CHAOS_DURATION", cls.chaos_duration),
            chaos_target=os.environ.get("CHAOS_TARGET", cls.chaos_target),
            ai_enabled=os.environ.get("AI_ENABLED", "false").lower() == "true",
            ai_url=os.environ.get("AI_URL", cls.ai_url),
            ai_model=os.environ.get("AI_MODEL", cls.ai_model),
        )
