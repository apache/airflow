# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Display and formatting utilities for PR triage output."""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from airflow_breeze.utils.console import get_console

if TYPE_CHECKING:
    from airflow_breeze.utils.pr_models import PRData


def human_readable_age(iso_date: str) -> str:
    """Convert an ISO date string to a human-readable relative age (e.g. '2 years, 3 months ago')."""
    from datetime import datetime, timezone

    try:
        created = datetime.fromisoformat(iso_date.replace("Z", "+00:00"))
        now = datetime.now(timezone.utc)
        delta = now - created
        total_days = delta.days

        years = total_days // 365
        remaining_days = total_days % 365
        months = remaining_days // 30

        parts: list[str] = []
        if years:
            parts.append(f"{years} year{'s' if years != 1 else ''}")
        if months:
            parts.append(f"{months} month{'s' if months != 1 else ''}")
        if not parts:
            if total_days > 0:
                parts.append(f"{total_days} day{'s' if total_days != 1 else ''}")
            else:
                return "today"
        return ", ".join(parts) + " ago"
    except (ValueError, TypeError):
        return "unknown"


def fmt_duration(seconds: float) -> str:
    """Format a duration in seconds to a human-readable string like '2m 30s' or '1h 05m 00s'."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds) // 60
    secs = int(seconds) % 60
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins:02d}m {secs:02d}s"


def linkify_commit_hashes(text: str, github_repository: str = "apache/airflow") -> str:
    """Convert commit hashes in text to inline GitHub links."""
    repo_url = f"https://github.com/{github_repository}"

    def _replace(m: re.Match) -> str:
        sha = m.group(0)
        start = m.start()
        if start > 0 and text[start - 1] == "/":
            return sha
        short = sha[:10]
        return f"[{short}]({repo_url}/commit/{sha})"

    return re.sub(r"\b[0-9a-f]{40}\b", _replace, text)


def pr_link(pr: PRData) -> str:
    """Return a Rich-formatted clickable link for a PR."""
    return f"[link={pr.url}]#{pr.number}[/link]"


def print_pr_header(pr: PRData, index: int | None = None, total: int | None = None) -> None:
    """Print a header line for a PR being reviewed."""
    console = get_console()
    position = f" ({index}/{total})" if index is not None and total is not None else ""
    console.rule(f"[bold cyan]PR #{pr.number}{position}[/]", style="cyan")
    console.print(f"  [bold]{pr.title}[/]")
    console.print(f"  [dim]{pr.url}[/]")
    console.print(f"  Author: [bold]{pr.author_login}[/] ({pr.author_association})")
    if pr.review_decisions:
        maintainer_assocs = {"COLLABORATOR", "MEMBER", "OWNER"}
        maintainer_reviews = [r for r in pr.review_decisions if r.reviewer_association in maintainer_assocs]
        if maintainer_reviews:
            approved = [r for r in maintainer_reviews if r.state == "APPROVED"]
            changes_requested = [r for r in maintainer_reviews if r.state == "CHANGES_REQUESTED"]
            parts: list[str] = []
            if approved:
                names = ", ".join(r.reviewer_login for r in approved)
                parts.append(f"[green]{len(approved)} approved[/] ({names})")
            if changes_requested:
                names = ", ".join(r.reviewer_login for r in changes_requested)
                parts.append(f"[red]{len(changes_requested)} changes requested[/] ({names})")
            console.print(f"  Maintainer reviews: {' | '.join(parts)}")
    console.print()
