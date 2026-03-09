from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import uvicorn
from kubernetes import client as k8s_client, config as k8s_config

from okk_pilot.pilot import Pilot, Event
from okk_pilot.config import Config
from okk_pilot.pipeline import load_pipeline
from okk_pilot.events.cron import CronScheduler
from okk_pilot.events.github_poller import GitHubPoller
from okk_pilot.events.k8s import K8sWatcher
from okk_pilot.events.webhook import create_webhook_app
from okk_pilot.tools.observe import ObserveTools
from okk_pilot.tools.act import ActTools
from okk_pilot.tools.report import ReportTools
from okk_pilot.tools.state import StateTools
from okk_pilot.tools.invariants import InvariantChecker

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("okk-pilot")

PIPELINE_PATH = Path(__file__).parent.parent.parent / "pipeline.yaml"


class OkkPilot:
    """Main application that wires event sources to the pilot."""

    def __init__(self, config: Config):
        self.config = config
        self._shutdown = threading.Event()

        # Load pipeline
        pipeline_path = os.environ.get("OKK_PIPELINE_PATH", str(PIPELINE_PATH))
        self.pipeline = load_pipeline(pipeline_path)
        logger.info("Pipeline loaded: %d actions, %d chaos types, %d invariants",
                     len(self.pipeline.actions), len(self.pipeline.chaos_types),
                     len(self.pipeline.invariants))

        # Init Kubernetes clients
        if config.in_cluster:
            k8s_config.load_incluster_config()
        else:
            k8s_config.load_kube_config()

        k8s_custom = k8s_client.CustomObjectsApi()
        k8s_core = k8s_client.CoreV1Api()

        # Init tool providers
        observe = ObserveTools(config, k8s_core)
        act = ActTools(config, k8s_custom)
        report = ReportTools(config) if config.github_token else None
        state = StateTools(config, k8s_core)
        invariants = InvariantChecker(config, k8s_core)

        # Init the pilot
        self.pilot = Pilot(config, observe, act, report, state, invariants, self.pipeline)

        # Event processing
        self._executor = ThreadPoolExecutor(max_workers=4)
        self._event_lock = threading.Lock()

        # Init event sources
        self.k8s_watcher = K8sWatcher(k8s_custom, k8s_core, config.namespace, self._on_event)
        self.cron = CronScheduler(self._on_event, config.daily_report_hour)

        # GitHub comment poller
        self.github_poller = GitHubPoller(config, self._on_event) if config.github_token else None

        self.webhook_app = create_webhook_app(
            self._on_event,
            webhook_secret=os.environ.get("GITHUB_WEBHOOK_SECRET"),
            pipeline=self.pipeline,
        )

    def _on_event(self, event: Event):
        self._executor.submit(self._handle_event, event)

    def _handle_event(self, event: Event):
        with self._event_lock:
            try:
                result = self.pilot.handle(event)
                if result:
                    logger.info("Pilot result for %s: %s", event.type, result[:300])
            except Exception:
                logger.exception("Failed to handle event: %s", event.type)

    def _run_continuous_chaos(self):
        """Run chaos in a continuous loop: inject one, wait, repeat."""
        logger.info("Starting continuous chaos loop")
        while not self._shutdown.is_set():
            event = Event(type="inject_chaos", summary="Continuous chaos injection", details={})
            self._handle_event(event)
            # Wait between chaos rounds (chaos duration + recovery buffer)
            self._shutdown.wait(timeout=120)

    def run(self):
        logger.info("Starting okk-pilot")

        self.k8s_watcher.start()
        self.cron.start()

        # Schedule actions from pipeline
        for action in self.pipeline.actions:
            if action.schedule == "always":
                # Run in a dedicated background thread
                t = threading.Thread(target=self._run_continuous_chaos, daemon=True)
                t.start()
                logger.info("Started continuous: %s", action.action)
            elif action.interval_seconds:
                trigger_fn = self._make_trigger(action.action)
                self.cron.add_interval_job(
                    action.action, trigger_fn,
                    seconds=action.interval_seconds,
                )
                logger.info("Scheduled %s: every %ds", action.action, action.interval_seconds)
            elif action.cron_hour is not None:
                # Daily actions are handled by the cron scheduler
                self.cron.add_daily_job(action.action,
                                        self._make_trigger(action.action),
                                        hour=action.cron_hour)
                logger.info("Scheduled %s: daily at %02d:00", action.action, action.cron_hour)

        # GitHub comment polling (every 2 minutes)
        if self.github_poller:
            self.cron.add_interval_job("github_poll", self.github_poller.poll, seconds=120)

        # Fire startup event
        self._on_event(Event(
            type="startup",
            summary="okk-pilot started. Checking cluster state and ensuring tests are running.",
            details={
                "namespace": self.config.namespace,
                "oxia_image": self.config.oxia_image,
                "okk_worker_image": self.config.okk_worker_image,
            },
        ))

        # Start webhook server (blocks)
        uvicorn.run(
            self.webhook_app,
            host=self.config.webhook_host,
            port=self.config.webhook_port,
            log_level="info",
        )

    def _make_trigger(self, action: str):
        """Create a trigger function for a given action name."""
        def trigger():
            self._on_event(Event(
                type=action,
                summary=f"Scheduled {action}",
                details={},
            ))
        return trigger

    def shutdown(self):
        logger.info("Shutting down okk-pilot")
        self._shutdown.set()
        self.k8s_watcher.stop()
        self.cron.stop()
        self._executor.shutdown(wait=False)


def main():
    config = Config.from_env()

    if not config.github_token:
        logger.warning("GITHUB_TOKEN not set — GitHub reporting disabled")
    if config.ai_enabled:
        logger.info("AI analysis enabled: %s", config.ai_model)

    app = OkkPilot(config)

    def handle_signal(signum, frame):
        app.shutdown()
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    app.run()


if __name__ == "__main__":
    main()
