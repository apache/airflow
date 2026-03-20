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
"""Data models shared across PR triage modules."""

from __future__ import annotations

from concurrent.futures import Future
from dataclasses import dataclass, field

# Grace period constants used by PRData and triage logic
CHECK_FAILURE_GRACE_PERIOD_HOURS = 24
CHECK_FAILURE_GRACE_PERIOD_WITH_ENGAGEMENT_HOURS = 96

# Associations considered "collaborator" (not flagged for triage)
COLLABORATOR_ASSOCIATIONS = {"COLLABORATOR", "MEMBER", "OWNER"}

# Label / comment constants shared across modules
READY_FOR_REVIEW_LABEL = "ready for maintainer review"
CLOSED_QUALITY_LABEL = "closed:quality"
SUSPICIOUS_CHANGES_LABEL = "suspicious-changes"
TRIAGE_COMMENT_MARKER = "<!-- auto-triage-marker -->"

QUALITY_CRITERIA_LINK = (
    "https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst#pr-quality-criteria"
)


@dataclass
class ExistingComment:
    """An existing review comment already posted on a PR."""

    path: str
    line: int | None
    body: str
    user_login: str


@dataclass
class UnresolvedThread:
    """Detail about a single unresolved review thread from a maintainer."""

    reviewer_login: str
    reviewer_association: str  # COLLABORATOR, MEMBER, OWNER
    comment_body: str  # the reviewer's original comment (first in thread)
    comment_url: str  # permalink to the thread
    author_last_reply: str  # the PR author's most recent reply in that thread (empty if none)

    @property
    def reviewer_display(self) -> str:
        """Human-readable reviewer identifier like 'login (MEMBER)'."""
        return f"{self.reviewer_login} ({self.reviewer_association})"


@dataclass
class RecentPRFailureInfo:
    """Preloaded information about CI failures in recently merged PRs."""

    failing_checks: dict[str, list[dict]]
    failing_check_names: set[str]
    prs_examined: int

    def find_matching_failures(self, pr_failed_checks: list[str]) -> list[str]:
        """Return PR check names that also fail in recent PRs (case-insensitive substring match)."""
        matched = []
        for check in pr_failed_checks:
            lower = check.lower()
            for recent_check in self.failing_check_names:
                if (
                    lower == recent_check.lower()
                    or lower in recent_check.lower()
                    or recent_check.lower() in lower
                ):
                    matched.append(check)
                    break
        return matched


@dataclass
class LogSnippetInfo:
    """Log snippet from a failed CI check, with a link to the full log."""

    snippet: str
    job_url: str


@dataclass
class PRData:
    """PR data fetched from GraphQL."""

    number: int
    title: str
    body: str
    url: str
    created_at: str
    updated_at: str
    node_id: str
    author_login: str
    author_association: str
    head_sha: str
    base_ref: str
    check_summary: str
    checks_state: str
    failed_checks: list[str]
    commits_behind: int
    is_draft: bool
    mergeable: str
    labels: list[str]
    unresolved_threads: list[UnresolvedThread]
    has_collaborator_review: bool = False

    @property
    def unresolved_review_comments(self) -> int:
        """Count of unresolved review threads from maintainers."""
        return len(self.unresolved_threads)

    @property
    def ci_grace_period_hours(self) -> int:
        """Return the appropriate CI failure grace period based on collaborator engagement."""
        if self.has_collaborator_review:
            return CHECK_FAILURE_GRACE_PERIOD_WITH_ENGAGEMENT_HOURS
        return CHECK_FAILURE_GRACE_PERIOD_HOURS


@dataclass
class StaleReviewInfo:
    """Info about a CHANGES_REQUESTED review that may need a follow-up nudge."""

    reviewer_login: str
    review_date: str
    author_pinged_reviewer: bool


@dataclass
class ReviewComment:
    """A single line-level review comment proposed by the LLM."""

    path: str
    line: int
    body: str
    category: str


@dataclass
class CodeReview:
    """Full code review result from an LLM."""

    summary: str
    overall_assessment: str
    overall_comment: str
    comments: list[ReviewComment]
    error: bool = False
    error_message: str = ""


@dataclass
class TriageStats:
    """Mutable counters for triage actions taken during auto-triage."""

    total_converted: int = 0
    total_commented: int = 0
    total_closed: int = 0
    total_ready: int = 0
    total_rebased: int = 0
    total_rerun: int = 0
    total_review_nudges: int = 0
    total_reviews_submitted: int = 0
    total_review_comments: int = 0
    total_skipped_action: int = 0
    total_workflows_approved: int = 0
    quit_early: bool = False


@dataclass
class TriageContext:
    """Shared context passed to all triage review phases."""

    token: str
    github_repository: str
    dry_run: bool
    answer_triage: str | None
    stats: TriageStats
    author_flagged_count: dict[str, int]
    llm_future_to_pr: dict
    llm_assessments: dict
    llm_completed: list
    llm_errors: list[int]
    llm_passing: list
    main_failures: RecentPRFailureInfo | None = None
    log_futures: dict[int, Future[dict[str, LogSnippetInfo]]] = field(default_factory=dict)
