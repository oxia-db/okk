from __future__ import annotations

import logging
import threading
import time
from typing import Callable

from kubernetes import client as k8s_client, watch

from okk_pilot.pilot import Event

logger = logging.getLogger(__name__)


class K8sWatcher:
    """Watches Kubernetes events relevant to okk and Oxia."""

    def __init__(
        self, k8s_custom: k8s_client.CustomObjectsApi,
        k8s_core: k8s_client.CoreV1Api,
        namespace: str, on_event: Callable[[Event], None],
    ):
        self.k8s_custom = k8s_custom
        self.k8s_core = k8s_core
        self.namespace = namespace
        self.on_event = on_event
        self._threads: list[threading.Thread] = []
        self._stop = threading.Event()
        self._start_time = time.time()

    def start(self):
        watchers = [
            ("pod-watcher", self._watch_pods),
            ("event-watcher", self._watch_events),
        ]
        for name, func in watchers:
            t = threading.Thread(target=func, name=name, daemon=True)
            t.start()
            self._threads.append(t)
            logger.info("Started %s", name)

    def stop(self):
        self._stop.set()

    def _watch_pods(self):
        w = watch.Watch()
        seen_restarts: dict[str, int] = {}
        initial_list_done = False
        while not self._stop.is_set():
            try:
                for event in w.stream(
                    self.k8s_core.list_namespaced_pod,
                    namespace=self.namespace,
                    timeout_seconds=300,
                ):
                    if self._stop.is_set():
                        break

                    event_type = event["type"]
                    pod = event["object"]
                    pod_name = pod.metadata.name

                    if not initial_list_done and event_type in ("ADDED",):
                        if pod.status.container_statuses:
                            for cs in pod.status.container_statuses:
                                seen_restarts[f"{pod_name}/{cs.name}"] = cs.restart_count
                        continue

                    if event_type == "BOOKMARK":
                        initial_list_done = True
                        continue

                    if event_type == "MODIFIED" and pod.status.container_statuses:
                        initial_list_done = True
                        for cs in pod.status.container_statuses:
                            key = f"{pod_name}/{cs.name}"
                            prev_count = seen_restarts.get(key, 0)
                            seen_restarts[key] = cs.restart_count
                            if cs.restart_count > prev_count and cs.last_state and cs.last_state.terminated:
                                reason = cs.last_state.terminated.reason or "Unknown"
                                self.on_event(Event(
                                    type="pod_restart",
                                    summary=f"Pod {pod_name} container {cs.name} restarted (reason: {reason}, count: {cs.restart_count})",
                                    details={
                                        "pod": pod_name,
                                        "container": cs.name,
                                        "restart_count": cs.restart_count,
                                        "reason": reason,
                                        "exit_code": cs.last_state.terminated.exit_code,
                                    },
                                ))

                    if event_type == "MODIFIED" and pod.status.container_statuses:
                        for cs in pod.status.container_statuses:
                            if cs.state and cs.state.waiting and cs.state.waiting.reason == "CrashLoopBackOff":
                                self.on_event(Event(
                                    type="crash_loop",
                                    summary=f"Pod {pod_name} is in CrashLoopBackOff",
                                    details={
                                        "pod": pod_name,
                                        "container": cs.name,
                                        "restart_count": cs.restart_count,
                                    },
                                ))
            except Exception:
                if not self._stop.is_set():
                    logger.exception("Pod watcher error, restarting")

    def _watch_events(self):
        w = watch.Watch()
        while not self._stop.is_set():
            try:
                for event in w.stream(
                    self.k8s_core.list_namespaced_event,
                    namespace=self.namespace,
                    timeout_seconds=300,
                ):
                    if self._stop.is_set():
                        break

                    k8s_event = event["object"]
                    if k8s_event.last_timestamp:
                        if k8s_event.last_timestamp.timestamp() < self._start_time:
                            continue
                    elif k8s_event.event_time:
                        if k8s_event.event_time.timestamp() < self._start_time:
                            continue

                    if k8s_event.type == "Warning":
                        self.on_event(Event(
                            type="k8s_warning",
                            summary=f"K8s warning on {k8s_event.involved_object.name}: {k8s_event.reason} — {k8s_event.message}",
                            details={
                                "object": k8s_event.involved_object.name,
                                "kind": k8s_event.involved_object.kind,
                                "reason": k8s_event.reason,
                                "message": k8s_event.message,
                                "count": k8s_event.count,
                            },
                        ))
            except Exception:
                if not self._stop.is_set():
                    logger.exception("Event watcher error, restarting")
