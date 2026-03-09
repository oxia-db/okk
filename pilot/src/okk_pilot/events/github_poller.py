from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Callable

from github import Github, GithubException

from okk_pilot.pilot import Event
from okk_pilot.config import Config

logger = logging.getLogger(__name__)

ALLOWED_ORG = "oxia-db"


class GitHubPoller:
    """Polls GitHub for new issue comments mentioning @okk-pilot."""

    def __init__(self, config: Config, on_event: Callable[[Event], None]):
        self.config = config
        self.on_event = on_event
        self._gh = Github(config.github_token)
        self._repo = self._gh.get_repo(config.github_repo)
        self._last_checked = datetime.now(timezone.utc)
        self._allowed_users_cache: set[str] = set()
        self._org_cache_time: float = 0

    def _is_org_member(self, username: str) -> bool:
        import time
        now = time.time()
        if now - self._org_cache_time > 3600:
            try:
                org = self._gh.get_organization(ALLOWED_ORG)
                self._allowed_users_cache = {m.login for m in org.get_members()}
                self._org_cache_time = now
            except GithubException:
                try:
                    self._allowed_users_cache = {c.login for c in self._repo.get_collaborators()}
                    self._org_cache_time = now
                except GithubException:
                    pass
        return username in self._allowed_users_cache

    def poll(self):
        try:
            issues = self._repo.get_issues(state="open", labels=["daily-report"], sort="updated", direction="desc")
            now = datetime.now(timezone.utc)

            for issue in issues[:5]:
                for comment in issue.get_comments(since=self._last_checked):
                    commenter = comment.user.login if comment.user else ""
                    body = comment.body or ""

                    if "🤖" in body:
                        continue

                    mentions_pilot = "@okk-pilot" in body.lower() or "@okk-agent" in body.lower()
                    if not mentions_pilot:
                        continue

                    if not self._is_org_member(commenter):
                        logger.warning("Ignoring comment from non-org user @%s on #%d", commenter, issue.number)
                        continue

                    logger.info("New comment on #%d by @%s", issue.number, commenter)
                    self.on_event(Event(
                        type="github_comment",
                        summary=f"Comment on #{issue.number} by @{commenter}: {body[:100]}",
                        details={
                            "issue_number": issue.number,
                            "issue_title": issue.title,
                            "comment_body": body,
                            "commenter": commenter,
                        },
                    ))

            self._last_checked = now
        except GithubException as e:
            logger.warning("GitHub polling failed: %s", e)
        except Exception:
            logger.exception("GitHub polling error")
