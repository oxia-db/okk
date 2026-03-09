from __future__ import annotations

import logging
from typing import Callable

from apscheduler.schedulers.background import BackgroundScheduler

from okk_pilot.pilot import Event

logger = logging.getLogger(__name__)


class CronScheduler:
    """Schedules periodic events."""

    def __init__(self, on_event: Callable[[Event], None], daily_report_hour: int = 0):
        self.on_event = on_event
        self.daily_report_hour = daily_report_hour
        self._scheduler = BackgroundScheduler()

    def start(self):
        self._scheduler.add_job(
            self._trigger_daily_report,
            trigger="cron",
            hour=self.daily_report_hour,
            minute=0,
            id="daily_report",
        )
        self._scheduler.start()
        logger.info("Cron scheduler started (daily report at %02d:00 UTC)", self.daily_report_hour)

    def stop(self):
        self._scheduler.shutdown(wait=False)

    def _trigger_daily_report(self):
        self.on_event(Event(type="daily_report", summary="Daily verification report is due", details={}))

    def _trigger_health_check(self):
        self.on_event(Event(type="health_check", summary="Periodic health check", details={}))

    def _trigger_periodic_summary(self):
        self.on_event(Event(type="periodic_summary", summary="Periodic summary", details={}))

    def _trigger_chaos_round(self):
        self.on_event(Event(type="chaos_round", summary="Scheduled chaos injection round", details={}))

    def _trigger_scale_event(self):
        self.on_event(Event(type="scale_event", summary="Scheduled scale test", details={}))

    def add_interval_job(self, job_id: str, func, **kwargs):
        self._scheduler.add_job(func, trigger="interval", id=job_id, **kwargs)
        logger.info("Added interval job: %s (%s)", job_id, kwargs)

    def add_daily_job(self, job_id: str, func, hour: int = 0, minute: int = 0):
        self._scheduler.add_job(func, trigger="cron", hour=hour, minute=minute, id=job_id)
        logger.info("Added daily job: %s at %02d:%02d UTC", job_id, hour, minute)
