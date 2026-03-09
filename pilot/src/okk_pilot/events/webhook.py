from __future__ import annotations

import hashlib
import hmac
import json
import logging
from typing import Callable

from fastapi import FastAPI, Request, HTTPException

from okk_pilot.pilot import Event
from okk_pilot.pipeline import PipelineConfig

logger = logging.getLogger(__name__)


def create_webhook_app(
    on_event: Callable[[Event], None],
    webhook_secret: str | None = None,
    pipeline: PipelineConfig | None = None,
) -> FastAPI:
    app = FastAPI(title="okk-pilot")

    @app.post("/webhook/github")
    async def github_webhook(request: Request):
        body = await request.body()

        if webhook_secret:
            signature = request.headers.get("X-Hub-Signature-256", "")
            expected = "sha256=" + hmac.new(
                webhook_secret.encode(), body, hashlib.sha256,
            ).hexdigest()
            if not hmac.compare_digest(signature, expected):
                raise HTTPException(status_code=403, detail="Invalid signature")

        event_type = request.headers.get("X-GitHub-Event", "")
        payload = json.loads(body)

        if event_type == "push":
            ref = payload.get("ref", "")
            if ref.startswith("refs/tags/v"):
                tag = ref.removeprefix("refs/tags/")
                on_event(Event(
                    type="new_tag",
                    summary=f"New tag pushed: {tag}",
                    details={"tag": tag, "ref": ref},
                ))

        elif event_type == "issue_comment":
            action = payload.get("action")
            comment = payload.get("comment", {})
            issue = payload.get("issue", {})
            commenter = comment.get("user", {}).get("login", "")
            sender_association = comment.get("author_association", "")
            is_trusted = sender_association in ("OWNER", "MEMBER", "COLLABORATOR")

            body_text = comment.get("body", "")
            mentions_pilot = "@okk-pilot" in body_text.lower() or "@okk-agent" in body_text.lower()

            if action == "created" and is_trusted and "🤖" not in body_text and mentions_pilot:
                on_event(Event(
                    type="github_comment",
                    summary=f"Comment on #{issue['number']} by @{commenter}: {body_text[:100]}",
                    details={
                        "issue_number": issue["number"],
                        "issue_title": issue.get("title", ""),
                        "comment_body": body_text,
                        "commenter": commenter,
                    },
                ))

        return {"status": "ok"}

    @app.get("/healthz")
    async def health():
        return {"status": "healthy"}

    @app.get("/pipeline")
    async def get_pipeline():
        if not pipeline:
            return {"error": "no pipeline loaded"}
        return pipeline.to_display()

    @app.get("/pipeline/detail")
    async def get_pipeline_detail():
        if not pipeline:
            return {"error": "no pipeline loaded"}
        return pipeline.to_dict()

    @app.post("/trigger/{event_type}")
    async def trigger_event(event_type: str):
        valid = {
            "check_invariants", "inject_chaos", "test_scaling",
            "post_report", "daily_report",
        }
        if event_type not in valid:
            return {"error": f"Unknown event type: {event_type}. Available: {', '.join(sorted(valid))}"}
        on_event(Event(type=event_type, summary=f"Manual {event_type}", details={}))
        return {"status": "triggered", "type": event_type}

    return app
