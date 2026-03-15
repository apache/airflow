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
from __future__ import annotations

import json
import re
import sys
import time
from collections import Counter
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING

import click
from rich.panel import Panel
from rich.table import Table

from airflow_breeze.commands.common_options import (
    option_dry_run,
    option_github_repository,
    option_github_token,
    option_llm_model,
    option_verbose,
)
from airflow_breeze.utils.click_utils import BreezeGroup
from airflow_breeze.utils.confirm import Answer, TriageAction, prompt_triage_action, user_confirm
from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.custom_param_types import HiddenChoiceWithCompletion, NotVerifiedBetterChoice
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose

if TYPE_CHECKING:
    from airflow_breeze.utils.github import PRAssessment

QUALITY_CRITERIA_LINK = (
    "[Pull Request quality criteria](https://github.com/apache/airflow/blob/main/"
    "contributing-docs/05_pull_requests.rst#pull-request-quality-criteria)"
)

# authorAssociation values that indicate the author has write access
_COLLABORATOR_ASSOCIATIONS = {"COLLABORATOR", "MEMBER", "OWNER"}

# Label applied when a maintainer marks a flagged PR as ready for review
_READY_FOR_REVIEW_LABEL = "ready for maintainer review"

# Label applied when a PR is closed due to multiple quality violations
_CLOSED_QUALITY_LABEL = "closed because of multiple quality violations"

# Label applied when a PR is closed due to suspicious changes
_SUSPICIOUS_CHANGES_LABEL = "suspicious changes detected"

# GitHub accounts that should be auto-skipped during triage
_BOT_ACCOUNT_LOGINS = {"dependabot", "dependabot[bot]", "renovate[bot]", "github-actions[bot]"}

# Marker used to identify comments posted by the auto-triage process
_TRIAGE_COMMENT_MARKER = "Pull Request quality criteria"

# Proximity threshold for showing "nearby" existing comments (lines)
_NEARBY_LINE_THRESHOLD = 5


def _get_review_cache_dir(github_repository: str) -> Path:
    """Return the directory for storing cached LLM review results."""
    from airflow_breeze.utils.path_utils import BUILD_CACHE_PATH

    safe_name = github_repository.replace("/", "_")
    cache_dir = Path(BUILD_CACHE_PATH) / "review_cache" / safe_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _get_cached_review(github_repository: str, pr_number: int, head_sha: str) -> dict | None:
    """Load a cached LLM review result if it exists and matches the current commit hash."""
    cache_file = _get_review_cache_dir(github_repository) / f"pr_{pr_number}.json"
    if not cache_file.exists():
        return None
    try:
        data = json.loads(cache_file.read_text())
        if data.get("head_sha") == head_sha:
            return data.get("review")
        return None
    except (json.JSONDecodeError, KeyError):
        return None


def _save_review_cache(github_repository: str, pr_number: int, head_sha: str, review: dict) -> None:
    """Save an LLM review result to the cache."""
    cache_file = _get_review_cache_dir(github_repository) / f"pr_{pr_number}.json"
    cache_file.write_text(json.dumps({"head_sha": head_sha, "review": review}, indent=2))


def _get_triage_cache_dir(github_repository: str) -> Path:
    """Return the directory for storing cached LLM triage assessment results."""
    from airflow_breeze.utils.path_utils import BUILD_CACHE_PATH

    safe_name = github_repository.replace("/", "_")
    cache_dir = Path(BUILD_CACHE_PATH) / "triage_cache" / safe_name
    cache_dir.mkdir(parents=True, exist_ok=True)
    return cache_dir


def _get_cached_assessment(github_repository: str, pr_number: int, head_sha: str) -> dict | None:
    """Load a cached LLM triage assessment if it exists and matches the current commit hash."""
    cache_file = _get_triage_cache_dir(github_repository) / f"pr_{pr_number}.json"
    if not cache_file.exists():
        return None
    try:
        data = json.loads(cache_file.read_text())
        if data.get("head_sha") == head_sha:
            return data.get("assessment")
        return None
    except (json.JSONDecodeError, KeyError):
        return None


def _save_assessment_cache(github_repository: str, pr_number: int, head_sha: str, assessment: dict) -> None:
    """Save an LLM triage assessment result to the cache."""
    cache_file = _get_triage_cache_dir(github_repository) / f"pr_{pr_number}.json"
    cache_file.write_text(json.dumps({"head_sha": head_sha, "assessment": assessment}, indent=2))


def _cached_assess_pr(
    github_repository: str,
    head_sha: str,
    pr_number: int,
    pr_title: str,
    pr_body: str,
    check_status_summary: str,
    llm_model: str,
) -> PRAssessment:
    """Run assess_pr with caching keyed by PR number + commit hash.

    Returns cached PRAssessment when the commit hash matches, avoiding redundant LLM calls.
    """
    from airflow_breeze.utils.github import PRAssessment, Violation
    from airflow_breeze.utils.llm_utils import assess_pr

    cached = _get_cached_assessment(github_repository, pr_number, head_sha)
    if cached is not None:
        violations = [
            Violation(
                category=v.get("category", "unknown"),
                explanation=v.get("explanation", ""),
                severity=v.get("severity", "warning"),
                details=v.get("details", ""),
            )
            for v in cached.get("violations", [])
        ]
        result = PRAssessment(
            should_flag=cached.get("should_flag", False),
            should_report=cached.get("should_report", False),
            violations=violations,
            summary=cached.get("summary", ""),
            error=cached.get("error", False),
            error_debug_file=cached.get("error_debug_file", ""),
        )
        result._from_cache = True  # type: ignore[attr-defined]
        return result

    result = assess_pr(
        pr_number=pr_number,
        pr_title=pr_title,
        pr_body=pr_body,
        check_status_summary=check_status_summary,
        llm_model=llm_model,
    )

    # Cache successful results (not errors)
    if not result.error:
        assessment_dict = {
            "should_flag": result.should_flag,
            "should_report": result.should_report,
            "violations": [
                {
                    "category": v.category,
                    "explanation": v.explanation,
                    "severity": v.severity,
                    "details": v.details,
                }
                for v in result.violations
            ],
            "summary": result.summary,
        }
        _save_assessment_cache(github_repository, pr_number, head_sha, assessment_dict)

    return result


@dataclass
class ExistingComment:
    """An existing review comment already posted on a PR."""

    path: str
    line: int | None
    body: str
    user_login: str


def _fetch_existing_review_comments(
    token: str, github_repository: str, pr_number: int
) -> list[ExistingComment]:
    """Fetch all existing review comments on a PR via REST API."""
    import requests

    comments: list[ExistingComment] = []
    url = f"https://api.github.com/repos/{github_repository}/pulls/{pr_number}/comments"
    page = 1
    while True:
        response = requests.get(
            url,
            params={"per_page": 100, "page": page},
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=30,
        )
        if response.status_code != 200:
            break
        items = response.json()
        if not items:
            break
        for item in items:
            comments.append(
                ExistingComment(
                    path=item.get("path", ""),
                    line=item.get("line") or item.get("original_line"),
                    body=item.get("body", ""),
                    user_login=item.get("user", {}).get("login", ""),
                )
            )
        page += 1
    return comments


_SEARCH_PRS_QUERY = """
query($query: String!, $first: Int!, $after: String) {
  search(query: $query, type: ISSUE, first: $first, after: $after) {
    issueCount
    pageInfo {
      hasNextPage
      endCursor
    }
    nodes {
      ... on PullRequest {
        number
        title
        body
        url
        createdAt
        updatedAt
        id
        author { login }
        authorAssociation
        baseRefName
        isDraft
        mergeable
        labels(first: 20) {
          nodes { name }
        }
        commits(last: 1) {
          nodes {
            commit {
              oid
              statusCheckRollup {
                state
              }
            }
          }
        }
      }
    }
  }
}
"""

_FETCH_SINGLE_PR_QUERY = """
query($owner: String!, $repo: String!, $number: Int!) {
  repository(owner: $owner, name: $repo) {
    pullRequest(number: $number) {
      number
      title
      body
      url
      createdAt
      updatedAt
      id
      author { login }
      authorAssociation
      baseRefName
      isDraft
      mergeable
      labels(first: 20) {
        nodes { name }
      }
      commits(last: 1) {
        nodes {
          commit {
            oid
            statusCheckRollup {
              state
            }
          }
        }
      }
    }
  }
}
"""

_CHECK_CONTEXTS_QUERY = """
query($owner: String!, $repo: String!, $oid: GitObjectID!, $first: Int!, $after: String) {
  repository(owner: $owner, name: $repo) {
    object(oid: $oid) {
      ... on Commit {
        statusCheckRollup {
          contexts(first: $first, after: $after) {
            totalCount
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              ... on CheckRun {
                __typename
                name
                conclusion
                status
              }
              ... on StatusContext {
                __typename
                context
                state
              }
            }
          }
        }
      }
    }
  }
}
"""

_AUTHOR_PROFILE_QUERY = """
query(
  $login: String!,
  $repoAll: String!, $repoMerged: String!, $repoClosed: String!,
  $globalAll: String!, $globalMerged: String!, $globalClosed: String!
) {
  user(login: $login) {
    createdAt
    repositoriesContributedTo(
      first: 10,
      contributionTypes: [COMMIT, PULL_REQUEST],
      orderBy: {field: STARGAZERS, direction: DESC}
    ) {
      totalCount
      nodes {
        nameWithOwner
        url
        stargazerCount
        isPrivate
      }
    }
  }
  repoAll: search(query: $repoAll, type: ISSUE) { issueCount }
  repoMerged: search(query: $repoMerged, type: ISSUE) { issueCount }
  repoClosed: search(query: $repoClosed, type: ISSUE) { issueCount }
  globalAll: search(query: $globalAll, type: ISSUE) { issueCount }
  globalMerged: search(query: $globalMerged, type: ISSUE) { issueCount }
  globalClosed: search(query: $globalClosed, type: ISSUE) { issueCount }
}
"""

_CONVERT_TO_DRAFT_MUTATION = """
mutation($prId: ID!) {
  convertPullRequestToDraft(input: {pullRequestId: $prId}) {
    pullRequest { id }
  }
}
"""

_MARK_READY_FOR_REVIEW_MUTATION = """
mutation($prId: ID!) {
  markPullRequestReadyForReview(input: {pullRequestId: $prId}) {
    pullRequest { id }
  }
}
"""

_ADD_COMMENT_MUTATION = """
mutation($subjectId: ID!, $body: String!) {
  addComment(input: {subjectId: $subjectId, body: $body}) {
    commentEdge { node { id } }
  }
}
"""

_ADD_LABELS_MUTATION = """
mutation($labelableId: ID!, $labelIds: [ID!]!) {
  addLabelsToLabelable(input: {labelableId: $labelableId, labelIds: $labelIds}) {
    labelable { ... on PullRequest { id } }
  }
}
"""

_REMOVE_LABELS_MUTATION = """
mutation($labelableId: ID!, $labelIds: [ID!]!) {
  removeLabelsFromLabelable(input: {labelableId: $labelableId, labelIds: $labelIds}) {
    labelable { ... on PullRequest { id } }
  }
}
"""

_GET_LABEL_ID_QUERY = """
query($owner: String!, $repo: String!, $name: String!) {
  repository(owner: $owner, name: $repo) {
    label(name: $name) { id }
  }
}
"""

_CLOSE_PR_MUTATION = """
mutation($prId: ID!) {
  closePullRequest(input: {pullRequestId: $prId}) {
    pullRequest { id }
  }
}
"""


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
    """Preloaded information about CI failures in recently merged PRs.

    Used to detect consistent failure patterns: when a PR's failures also
    appear in other recently merged PRs targeting the same branch, this
    suggests the failures are systemic rather than caused by the PR's changes.
    """

    # Map from failing check name -> list of PR dicts (with number, title, url, html_url, etc.)
    failing_checks: dict[str, list[dict]]
    # Set of check names failing across recent PRs
    failing_check_names: set[str]
    # How many recent PRs were examined
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
    base_ref: str  # e.g. "main"
    check_summary: str
    checks_state: str  # statusCheckRollup.state: SUCCESS, FAILURE, PENDING, etc.
    failed_checks: list[str]  # best-effort list of individual failing check names
    commits_behind: int  # how many commits behind the base branch
    is_draft: bool  # whether the PR is currently a draft
    mergeable: str  # MERGEABLE, CONFLICTING, or UNKNOWN
    labels: list[str]  # label names attached to this PR
    unresolved_threads: list[UnresolvedThread]  # detailed unresolved review threads from maintainers
    has_collaborator_review: bool = False  # whether a collaborator/member/owner has reviewed or commented

    @property
    def unresolved_review_comments(self) -> int:
        """Count of unresolved review threads from maintainers."""
        return len(self.unresolved_threads)

    @property
    def ci_grace_period_hours(self) -> int:
        """Return the appropriate CI failure grace period based on collaborator engagement."""
        if self.has_collaborator_review:
            return _CHECK_FAILURE_GRACE_PERIOD_WITH_ENGAGEMENT_HOURS
        return _CHECK_FAILURE_GRACE_PERIOD_HOURS


@dataclass
class StaleReviewInfo:
    """Info about a CHANGES_REQUESTED review that may need a follow-up nudge."""

    reviewer_login: str
    review_date: str  # ISO 8601
    author_pinged_reviewer: bool  # whether the author mentioned the reviewer after the review


@dataclass
class ReviewComment:
    """A single line-level review comment proposed by the LLM."""

    path: str
    line: int
    body: str
    category: str  # bug, style, performance, security, suggestion, etc.


@dataclass
class CodeReview:
    """Full code review result from an LLM."""

    summary: str
    overall_assessment: str  # APPROVE, REQUEST_CHANGES, COMMENT
    overall_comment: str  # body text for the overall review
    comments: list[ReviewComment]
    error: bool = False
    error_message: str = ""


@click.group(cls=BreezeGroup, name="pr", help="Tools for managing GitHub pull requests.")
def pr_group():
    pass


_TRUSTED_REPOSITORIES = {"apache/airflow"}

# answer-triage values that auto-confirm destructive actions without user review
_DANGEROUS_ANSWER_VALUES = {"d", "c", "y"}


def _validate_llm_safety(github_repository: str, answer_triage: str | None) -> None:
    """Verify safety preconditions before starting LLM assessment threads.

    LLM assessments feed directly into triage actions (draft, close, comment) that
    modify PRs on GitHub. To prevent accidental damage we require:
    1. The target repository is a trusted repository.
    2. No --answer-triage value is set that would auto-confirm destructive actions.
    """
    console = get_console()

    if github_repository not in _TRUSTED_REPOSITORIES:
        console.print(
            f"[error]LLM assessment refused: repository '{github_repository}' is not trusted.\n"
            f"Trusted repositories: {', '.join(sorted(_TRUSTED_REPOSITORIES))}.\n"
            f"Use --github-repository apache/airflow or run without LLM "
            f"(--check-mode api).[/]"
        )
        sys.exit(1)

    if answer_triage and answer_triage.lower() in _DANGEROUS_ANSWER_VALUES:
        label = {"d": "draft", "c": "close", "y": "yes (auto-confirm)"}
        console.print(
            f"[error]LLM assessment refused: --answer-triage={answer_triage} "
            f"({label.get(answer_triage.lower(), answer_triage)}) would auto-confirm "
            f"destructive actions on PRs based on LLM output without user review.\n"
            f"Remove --answer-triage or use a safe value (s=skip, q=quit, n=no) "
            f"to proceed with LLM assessment.[/]"
        )
        sys.exit(1)


def _resolve_github_token(github_token: str | None) -> str | None:
    """Resolve GitHub token from option, environment, or gh CLI."""
    if github_token:
        return github_token
    gh_token_result = run_command(
        ["gh", "auth", "token"],
        capture_output=True,
        text=True,
        check=False,
        dry_run_override=False,
    )
    if gh_token_result.returncode == 0:
        return gh_token_result.stdout.strip()
    return None


_VIEWER_QUERY = """
query { viewer { login } }
"""


def _resolve_viewer_login(token: str) -> str:
    """Resolve the GitHub login of the authenticated user via the viewer query."""
    data = _graphql_request(token, _VIEWER_QUERY, {})
    return data["viewer"]["login"]


def _get_collaborators_cache_path(github_repository: str) -> Path:
    """Return the path to the local collaborators cache file."""
    from airflow_breeze.utils.path_utils import BUILD_CACHE_PATH

    safe_name = github_repository.replace("/", "_")
    return Path(BUILD_CACHE_PATH) / f".collaborators_{safe_name}.json"


def _fetch_collaborators_from_api(token: str, github_repository: str) -> list[str]:
    """Fetch the list of collaborators from the GitHub REST API."""
    import requests

    collaborators: list[str] = []
    page = 1
    while True:
        response = requests.get(
            f"https://api.github.com/repos/{github_repository}/collaborators",
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            params={"per_page": 100, "page": page},
        )
        if response.status_code != 200:
            break
        data = response.json()
        if not data:
            break
        for user in data:
            login = user.get("login")
            if login:
                collaborators.append(login)
        page += 1
    return sorted(collaborators)


def _load_collaborators_cache(github_repository: str) -> list[str]:
    """Load collaborators from local cache file. Returns empty list if no cache."""
    cache_path = _get_collaborators_cache_path(github_repository)
    if cache_path.exists():
        try:
            return json.loads(cache_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return []


def _save_collaborators_cache(github_repository: str, collaborators: list[str]) -> None:
    """Save collaborators list to local cache file."""
    cache_path = _get_collaborators_cache_path(github_repository)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_text(json.dumps(collaborators))


def _refresh_collaborators_cache_in_background(token: str, github_repository: str) -> None:
    """Fetch collaborators from API and update the cache in a background thread."""

    def _refresh():
        collaborators = _fetch_collaborators_from_api(token, github_repository)
        if collaborators:
            _save_collaborators_cache(github_repository, collaborators)

    thread = Thread(target=_refresh, daemon=True)
    thread.start()


def _graphql_request(token: str, query: str, variables: dict) -> dict:
    """Execute a GitHub GraphQL request. Returns the 'data' dict or exits on error."""
    import requests

    response = requests.post(
        "https://api.github.com/graphql",
        json={"query": query, "variables": variables},
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        timeout=60,
    )
    if response.status_code != 200:
        console_print(f"[error]GraphQL request failed: {response.status_code} {response.text}[/]")
        sys.exit(1)
    result = response.json()
    if "errors" in result:
        console_print(f"[error]GraphQL errors: {result['errors']}[/]")
        sys.exit(1)
    return result["data"]


_CHECK_FAILURE_CONCLUSIONS = {"FAILURE", "TIMED_OUT", "ACTION_REQUIRED"}
_STATUS_FAILURE_STATES = {"FAILURE", "ERROR"}

# Number of hours after which a CI failure is considered "stale" and should be flagged.
# Default grace period for new PRs without collaborator engagement.
_CHECK_FAILURE_GRACE_PERIOD_HOURS = 24

# Extended grace period (in hours) when a collaborator/member/owner has left
# a review or comment on the PR — gives contributors more time to respond.
_CHECK_FAILURE_GRACE_PERIOD_WITH_ENGAGEMENT_HOURS = 96

# Batch sizes for chunked GraphQL queries to avoid GitHub timeout errors
_CHECK_DETAIL_BATCH_SIZE = 10
_COMMITS_BEHIND_BATCH_SIZE = 20

# Substrings that indicate a check is from a CI test workflow (not just labelers/bots)
_TEST_WORKFLOW_PATTERNS = [
    "test",
    "static check",
    "build",
    "ci image",
    "prod image",
    "helm",
    "k8s",
    "basic",
    "unit",
    "integration",
    "provider",
    "mypy",
    "pre-commit",
    "docs",
]


def _are_failures_recent(
    token: str,
    github_repository: str,
    head_sha: str,
    grace_hours: int = _CHECK_FAILURE_GRACE_PERIOD_HOURS,
) -> bool:
    """Check if all completed failed workflow runs for a commit finished within the grace period.

    Returns True if ALL failures are recent (within *grace_hours*),
    meaning we should NOT nag the author yet. Returns False if any failure is older
    or if we can't determine the age.

    The default grace period is _CHECK_FAILURE_GRACE_PERIOD_HOURS (24 h).  When a
    collaborator has already reviewed or commented on the PR, callers should pass
    _CHECK_FAILURE_GRACE_PERIOD_WITH_ENGAGEMENT_HOURS (96 h) instead.
    """
    from datetime import datetime, timezone

    runs = _find_workflow_runs_by_status(token, github_repository, head_sha, "completed")
    if not runs:
        return False

    now = datetime.now(timezone.utc)
    failed_runs = [r for r in runs if r.get("conclusion") == "failure"]
    if not failed_runs:
        return False

    for run in failed_runs:
        completed_at = run.get("completed_at", "")
        if not completed_at:
            return False  # Can't determine age — be safe and flag it
        try:
            completed_dt = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
            age_hours = (now - completed_dt).total_seconds() / 3600
            if age_hours > grace_hours:
                return False  # At least one failure is old enough to flag
        except (ValueError, TypeError):
            return False
    return True  # All failures are recent


def _is_test_check(name: str) -> bool:
    """Return True if the check name looks like a CI test workflow (not just a bot/labeler)."""
    lower = name.lower()
    return any(p in lower for p in _TEST_WORKFLOW_PATTERNS)


def _extract_basic_check_info(pr_node: dict) -> tuple[str, str]:
    """Extract basic check info from a lightweight GraphQL PR node (no contexts).

    Returns (head_sha, rollup_state).
    """
    commits = pr_node.get("commits", {}).get("nodes", [])
    if not commits:
        return "", "UNKNOWN"
    commit = commits[0].get("commit", {})
    head_sha = commit.get("oid", "")
    rollup = commit.get("statusCheckRollup")
    if not rollup:
        return head_sha, "UNKNOWN"
    return head_sha, rollup.get("state", "UNKNOWN")


def _process_check_contexts(contexts: list[dict], total_count: int) -> tuple[str, list[str], bool]:
    """Process check context nodes into summary text, failed names, and test-check presence.

    Returns (summary_text, failed_check_names, has_test_checks).
    """
    lines: list[str] = []
    failed: list[str] = []
    has_test_checks = False
    for ctx in contexts:
        typename = ctx.get("__typename")
        if typename == "CheckRun":
            name = ctx.get("name", "unknown")
            conclusion = ctx.get("conclusion") or ctx.get("status") or "unknown"
            lines.append(f"  {name}: {conclusion}")
            if _is_test_check(name):
                has_test_checks = True
            if conclusion.upper() in _CHECK_FAILURE_CONCLUSIONS:
                failed.append(name)
        elif typename == "StatusContext":
            name = ctx.get("context", "unknown")
            state = ctx.get("state", "unknown")
            lines.append(f"  {name}: {state}")
            if _is_test_check(name):
                has_test_checks = True
            if state.upper() in _STATUS_FAILURE_STATES:
                failed.append(name)
    if total_count > len(contexts):
        extra = total_count - len(contexts)
        lines.append(f"  ... ({extra} more {'checks' if extra != 1 else 'check'} not shown)")
    summary = "\n".join(lines) if lines else "No check runs found."
    return summary, failed, has_test_checks


def _fetch_check_status_counts(token: str, github_repository: str, head_sha: str) -> dict[str, int]:
    """Fetch counts of checks by status for a commit. Returns a dict like {"SUCCESS": 5, "FAILURE": 2, ...}.

    Also includes an "IN_PROGRESS" key for checks still running.
    """
    owner, repo = github_repository.split("/", 1)
    counts: dict[str, int] = {}
    cursor: str | None = None

    while True:
        variables: dict = {"owner": owner, "repo": repo, "oid": head_sha, "first": 100}
        if cursor:
            variables["after"] = cursor

        data = _graphql_request(token, _CHECK_CONTEXTS_QUERY, variables)
        rollup = (data.get("repository", {}).get("object", {}) or {}).get("statusCheckRollup")
        if not rollup:
            break

        contexts_data = rollup.get("contexts", {})
        for ctx in contexts_data.get("nodes", []):
            typename = ctx.get("__typename")
            if typename == "CheckRun":
                status = ctx.get("status", "")
                if status.upper() in ("IN_PROGRESS", "QUEUED"):
                    key = status.upper()
                else:
                    conclusion = ctx.get("conclusion") or status or "UNKNOWN"
                    key = conclusion.upper()
            elif typename == "StatusContext":
                state = ctx.get("state", "UNKNOWN")
                key = state.upper() if state.upper() != "PENDING" else "PENDING"
            else:
                continue
            counts[key] = counts.get(key, 0) + 1

        page_info = contexts_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return counts


def _format_check_status_counts(counts: dict[str, int]) -> str:
    """Format check status counts with Rich color markup."""
    _STATUS_COLORS = {
        "SUCCESS": "green",
        "FAILURE": "red",
        "TIMED_OUT": "red",
        "ACTION_REQUIRED": "yellow",
        "CANCELLED": "dim",
        "SKIPPED": "dim",
        "NEUTRAL": "dim",
        "STALE": "dim",
        "STARTUP_FAILURE": "red",
        "IN_PROGRESS": "bright_cyan",
        "QUEUED": "bright_cyan",
        "PENDING": "yellow",
        "ERROR": "red",
        "EXPECTED": "green",
    }
    if not counts:
        return "[dim]No checks found[/]"
    parts = []
    # Show in a consistent order: failures first, then in-progress, then success, then others
    order = [
        "FAILURE",
        "TIMED_OUT",
        "ERROR",
        "STARTUP_FAILURE",
        "ACTION_REQUIRED",
        "IN_PROGRESS",
        "QUEUED",
        "PENDING",
        "SUCCESS",
        "EXPECTED",
        "CANCELLED",
        "SKIPPED",
        "NEUTRAL",
        "STALE",
    ]
    shown = set()
    for status in order:
        if status in counts:
            color = _STATUS_COLORS.get(status, "white")
            parts.append(f"[{color}]{counts[status]} {status.lower()}[/]")
            shown.add(status)
    for status, count in sorted(counts.items()):
        if status not in shown:
            parts.append(f"{count} {status.lower()}")
    total = sum(counts.values())
    return f"{total} checks: " + ", ".join(parts)


def _has_running_checks(counts: dict[str, int]) -> bool:
    """Return True if any checks are still running (in_progress, queued, or pending)."""
    return any(counts.get(s, 0) > 0 for s in ("IN_PROGRESS", "QUEUED", "PENDING"))


def _fetch_failed_checks(token: str, github_repository: str, head_sha: str) -> list[str]:
    """Fetch all failing check names for a commit by paginating through check contexts."""
    owner, repo = github_repository.split("/", 1)
    failed: list[str] = []
    cursor: str | None = None

    while True:
        variables: dict = {"owner": owner, "repo": repo, "oid": head_sha, "first": 100}
        if cursor:
            variables["after"] = cursor

        data = _graphql_request(token, _CHECK_CONTEXTS_QUERY, variables)
        rollup = (data.get("repository", {}).get("object", {}) or {}).get("statusCheckRollup")
        if not rollup:
            break

        contexts_data = rollup.get("contexts", {})
        for ctx in contexts_data.get("nodes", []):
            typename = ctx.get("__typename")
            if typename == "CheckRun":
                conclusion = ctx.get("conclusion") or ctx.get("status") or "unknown"
                if conclusion.upper() in _CHECK_FAILURE_CONCLUSIONS:
                    failed.append(ctx.get("name", "unknown"))
            elif typename == "StatusContext":
                state = ctx.get("state", "unknown")
                if state.upper() in _STATUS_FAILURE_STATES:
                    failed.append(ctx.get("context", "unknown"))

        page_info = contexts_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")

    return failed


def _fetch_check_details_batch(token: str, github_repository: str, prs: list[PRData]) -> None:
    """Fetch detailed check contexts for PRs in chunked GraphQL queries.

    Updates each PR's check_summary, checks_state, and failed_checks in-place.
    Processes in chunks of _CHECK_DETAIL_BATCH_SIZE to avoid GitHub timeout errors.
    """
    owner, repo = github_repository.split("/", 1)
    eligible = [pr for pr in prs if pr.head_sha]
    if not eligible:
        return

    for chunk_start in range(0, len(eligible), _CHECK_DETAIL_BATCH_SIZE):
        chunk = eligible[chunk_start : chunk_start + _CHECK_DETAIL_BATCH_SIZE]

        object_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            object_fields.append(
                f'    {alias}: object(oid: "{pr.head_sha}") {{\n'
                f"      ... on Commit {{\n"
                f"        statusCheckRollup {{\n"
                f"          state\n"
                f"          contexts(first: 100) {{\n"
                f"            totalCount\n"
                f"            nodes {{\n"
                f"              ... on CheckRun {{ __typename name conclusion status }}\n"
                f"              ... on StatusContext {{ __typename context state }}\n"
                f"            }}\n"
                f"          }}\n"
                f"        }}\n"
                f"      }}\n"
                f"    }}"
            )

        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(object_fields)
            + "\n  }\n}"
        )

        try:
            data = _graphql_request(token, query, {})
        except SystemExit:
            continue

        repo_data = data.get("repository", {})
        for pr in chunk:
            alias = f"pr{pr.number}"
            commit_data = repo_data.get(alias) or {}
            rollup = commit_data.get("statusCheckRollup")
            if not rollup:
                continue

            rollup_state = rollup.get("state", "UNKNOWN")
            contexts_data = rollup.get("contexts", {})
            total_count = contexts_data.get("totalCount", 0)
            contexts = contexts_data.get("nodes", [])

            summary, failed, has_test_checks = _process_check_contexts(contexts, total_count)

            if contexts and not has_test_checks:
                rollup_state = "NOT_RUN"

            pr.checks_state = rollup_state
            pr.check_summary = summary
            pr.failed_checks = failed


_REVIEW_THREADS_BATCH_SIZE = 10


def _fetch_unresolved_comments_batch(token: str, github_repository: str, prs: list[PRData]) -> None:
    """Fetch unresolved review thread details for PRs in chunked GraphQL queries.

    Fetches only threads started by collaborators/members/owners (maintainers).
    For each thread, captures the reviewer info, their comment body, permalink,
    and the PR author's most recent reply.
    Updates each PR's unresolved_threads list in-place.
    """
    owner, repo = github_repository.split("/", 1)
    if not prs:
        return

    for chunk_start in range(0, len(prs), _REVIEW_THREADS_BATCH_SIZE):
        chunk = prs[chunk_start : chunk_start + _REVIEW_THREADS_BATCH_SIZE]

        pr_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            # Fetch review threads (for unresolved comments) and latestReviews
            # (to detect collaborator engagement for extended grace period).
            pr_fields.append(
                f"    {alias}: pullRequest(number: {pr.number}) {{\n"
                f"      reviewThreads(first: 100) {{\n"
                f"        nodes {{\n"
                f"          isResolved\n"
                f"          comments(first: 20) {{\n"
                f"            nodes {{\n"
                f"              author {{ login }}\n"
                f"              authorAssociation\n"
                f"              body\n"
                f"              url\n"
                f"            }}\n"
                f"          }}\n"
                f"        }}\n"
                f"      }}\n"
                f"      latestReviews(first: 20) {{\n"
                f"        nodes {{\n"
                f"          author {{ login }}\n"
                f"          authorAssociation\n"
                f"          state\n"
                f"        }}\n"
                f"      }}\n"
                f"    }}"
            )

        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(pr_fields)
            + "\n  }\n}"
        )

        try:
            data = _graphql_request(token, query, {})
        except SystemExit:
            continue

        repo_data = data.get("repository", {})
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_data = repo_data.get(alias) or {}
            threads = pr_data.get("reviewThreads", {}).get("nodes", [])
            unresolved: list[UnresolvedThread] = []
            for thread in threads:
                if thread.get("isResolved"):
                    continue
                comments = thread.get("comments", {}).get("nodes", [])
                if not comments:
                    continue
                first = comments[0]
                assoc = first.get("authorAssociation", "NONE")
                if assoc not in _COLLABORATOR_ASSOCIATIONS:
                    continue
                reviewer_login = (first.get("author") or {}).get("login", "unknown")
                comment_body = first.get("body", "")
                comment_url = first.get("url", "")

                # Find the PR author's most recent reply in this thread
                author_last_reply = ""
                for c in reversed(comments[1:]):
                    c_login = (c.get("author") or {}).get("login", "")
                    if c_login == pr.author_login:
                        author_last_reply = c.get("body", "")
                        break

                unresolved.append(
                    UnresolvedThread(
                        reviewer_login=reviewer_login,
                        reviewer_association=assoc,
                        comment_body=comment_body,
                        comment_url=comment_url,
                        author_last_reply=author_last_reply,
                    )
                )
            pr.unresolved_threads = unresolved

            # Detect collaborator engagement from reviews
            reviews = pr_data.get("latestReviews", {}).get("nodes", [])
            for review in reviews:
                assoc = review.get("authorAssociation", "NONE")
                if assoc in _COLLABORATOR_ASSOCIATIONS:
                    pr.has_collaborator_review = True
                    break
            # Also count unresolved threads from collaborators as engagement
            if unresolved:
                pr.has_collaborator_review = True


def _fetch_commits_behind_batch(token: str, github_repository: str, prs: list[PRData]) -> dict[int, int]:
    """Fetch how many commits each PR is behind its base branch in chunked GraphQL queries.

    Uses aliased ref.compare fields batched into chunks of _COMMITS_BEHIND_BATCH_SIZE
    to avoid GitHub timeout errors.
    Returns a dict mapping PR number to commits behind count.
    """
    owner, repo = github_repository.split("/", 1)
    eligible = [(i, pr) for i, pr in enumerate(prs) if pr.head_sha]
    if not eligible:
        return {}

    result: dict[int, int] = {}
    for chunk_start in range(0, len(eligible), _COMMITS_BEHIND_BATCH_SIZE):
        chunk = eligible[chunk_start : chunk_start + _COMMITS_BEHIND_BATCH_SIZE]

        compare_fields = []
        for _i, pr in chunk:
            alias = f"pr{pr.number}"
            compare_fields.append(
                f'    {alias}: ref(qualifiedName: "refs/heads/{pr.base_ref}") {{\n'
                f'      compare(headRef: "{pr.head_sha}") {{ behindBy }}\n'
                f"    }}"
            )

        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(compare_fields)
            + "\n  }\n}"
        )

        try:
            data = _graphql_request(token, query, {})
        except SystemExit:
            continue

        repo_data = data.get("repository", {})
        for _i, pr in chunk:
            alias = f"pr{pr.number}"
            ref_data = repo_data.get(alias) or {}
            compare = ref_data.get("compare") or {}
            result[pr.number] = compare.get("behindBy", 0)

    return result


def _find_already_triaged_prs(
    token: str,
    github_repository: str,
    prs: list[PRData],
    viewer_login: str,
    *,
    require_marker: bool = True,
) -> set[int]:
    """Find PRs that already have a triage comment posted after the last commit.

    Returns a set of PR numbers that should be skipped because the triage process
    already commented and no new commits were pushed since.

    :param require_marker: if True, only match comments containing _TRIAGE_COMMENT_MARKER.
        If False, match any comment from the viewer (useful for workflow approval PRs
        where a rebase comment may have been posted instead of a full triage comment).
    """
    result = _classify_already_triaged_prs(
        token, github_repository, prs, viewer_login, require_marker=require_marker
    )
    return result["waiting"] | result["responded"]


def _classify_already_triaged_prs(
    token: str,
    github_repository: str,
    prs: list[PRData],
    viewer_login: str,
    *,
    require_marker: bool = True,
) -> dict[str, set[int]]:
    """Classify already-triaged PRs into waiting vs responded.

    Returns a dict with keys:
    - "waiting": PR numbers where we commented but author has not responded
    - "responded": PR numbers where author responded after our triage comment

    :param require_marker: if True, only match comments containing _TRIAGE_COMMENT_MARKER.
        If False, match any comment from the viewer (useful for workflow approval PRs
        where a rebase comment may have been posted instead of a full triage comment).
    """
    result: dict[str, set[int]] = {"waiting": set(), "responded": set()}
    if not prs:
        return result

    owner, repo = github_repository.split("/", 1)

    # Batch fetch last 10 comments + last commit date for each PR
    for chunk_start in range(0, len(prs), _COMMITS_BEHIND_BATCH_SIZE):
        chunk = prs[chunk_start : chunk_start + _COMMITS_BEHIND_BATCH_SIZE]

        pr_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_fields.append(
                f"    {alias}: pullRequest(number: {pr.number}) {{\n"
                f"      comments(last: 10) {{\n"
                f"        nodes {{ author {{ login }} body createdAt }}\n"
                f"      }}\n"
                f"      commits(last: 1) {{\n"
                f"        nodes {{ commit {{ committedDate }} }}\n"
                f"      }}\n"
                f"    }}"
            )

        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(pr_fields)
            + "\n  }\n}"
        )

        try:
            data = _graphql_request(token, query, {})
        except SystemExit:
            continue

        repo_data = data.get("repository", {})
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_data = repo_data.get(alias) or {}

            # Get last commit date
            commits = pr_data.get("commits", {}).get("nodes", [])
            last_commit_date = ""
            if commits:
                last_commit_date = commits[0].get("commit", {}).get("committedDate", "")

            # Check if any comment is from the viewer and contains the triage marker
            comments = pr_data.get("comments", {}).get("nodes", [])
            triage_comment_date = ""
            for comment in reversed(comments):
                author = (comment.get("author") or {}).get("login", "")
                body = comment.get("body", "")
                comment_date = comment.get("createdAt", "")

                if (
                    author == viewer_login
                    and (not require_marker or _TRIAGE_COMMENT_MARKER in body)
                    and comment_date >= last_commit_date
                ):
                    triage_comment_date = comment_date
                    break

            if not triage_comment_date:
                continue

            # Check if the PR author responded after our triage comment
            author_responded = False
            for comment in comments:
                comment_author = (comment.get("author") or {}).get("login", "")
                comment_date = comment.get("createdAt", "")
                if comment_author == pr.author_login and comment_date > triage_comment_date:
                    author_responded = True
                    break

            if author_responded:
                result["responded"].add(pr.number)
            else:
                result["waiting"].add(pr.number)

    return result


_STALE_REVIEW_BATCH_SIZE = 10


def _fetch_stale_review_data_batch(
    token: str, github_repository: str, prs: list[PRData]
) -> dict[int, list[StaleReviewInfo]]:
    """Fetch review data for PRs to detect stale CHANGES_REQUESTED reviews.

    For each PR, fetches the latest review per reviewer, the last commit date,
    and recent comments. Returns a mapping from PR number to a list of
    StaleReviewInfo for reviewers whose latest review is CHANGES_REQUESTED
    and the author has pushed commits since that review.
    """
    if not prs:
        return {}

    owner, repo = github_repository.split("/", 1)
    result: dict[int, list[StaleReviewInfo]] = {}

    for chunk_start in range(0, len(prs), _STALE_REVIEW_BATCH_SIZE):
        chunk = prs[chunk_start : chunk_start + _STALE_REVIEW_BATCH_SIZE]

        pr_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_fields.append(
                f"    {alias}: pullRequest(number: {pr.number}) {{\n"
                f"      latestReviews(first: 20) {{\n"
                f"        nodes {{\n"
                f"          author {{ login }}\n"
                f"          state\n"
                f"          submittedAt\n"
                f"        }}\n"
                f"      }}\n"
                f"      commits(last: 1) {{\n"
                f"        nodes {{ commit {{ committedDate }} }}\n"
                f"      }}\n"
                f"      comments(last: 30) {{\n"
                f"        nodes {{ author {{ login }} body createdAt }}\n"
                f"      }}\n"
                f"    }}"
            )

        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(pr_fields)
            + "\n  }\n}"
        )

        try:
            data = _graphql_request(token, query, {})
        except SystemExit:
            continue

        repo_data = data.get("repository", {})
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_data = repo_data.get(alias) or {}

            # Get last commit date
            commits = pr_data.get("commits", {}).get("nodes", [])
            last_commit_date = ""
            if commits:
                last_commit_date = commits[0].get("commit", {}).get("committedDate", "")

            # Find reviewers whose latest review is CHANGES_REQUESTED
            reviews = pr_data.get("latestReviews", {}).get("nodes", [])
            changes_requested_reviews: list[tuple[str, str]] = []  # (login, submittedAt)
            for review in reviews:
                if review.get("state") != "CHANGES_REQUESTED":
                    continue
                reviewer = (review.get("author") or {}).get("login", "")
                submitted_at = review.get("submittedAt", "")
                if reviewer and submitted_at:
                    changes_requested_reviews.append((reviewer, submitted_at))

            if not changes_requested_reviews:
                continue

            # Check if there are commits after each review
            stale_reviews: list[StaleReviewInfo] = []
            comments = pr_data.get("comments", {}).get("nodes", [])

            for reviewer_login, review_date in changes_requested_reviews:
                # Only flag if the author pushed commits after the review
                if not last_commit_date or last_commit_date <= review_date:
                    continue

                # Check if the author pinged the reviewer in comments after the review
                author_pinged = False
                for comment in comments:
                    comment_author = (comment.get("author") or {}).get("login", "")
                    comment_date = comment.get("createdAt", "")
                    comment_body = comment.get("body", "")
                    if (
                        comment_author == pr.author_login
                        and comment_date > review_date
                        and f"@{reviewer_login}" in comment_body
                    ):
                        author_pinged = True
                        break

                stale_reviews.append(
                    StaleReviewInfo(
                        reviewer_login=reviewer_login,
                        review_date=review_date,
                        author_pinged_reviewer=author_pinged,
                    )
                )

            if stale_reviews:
                result[pr.number] = stale_reviews

    return result


def _build_review_nudge_comment(pr_author: str, stale_reviews: list[StaleReviewInfo]) -> str:
    """Build a comment to nudge review follow-up on a PR with stale CHANGES_REQUESTED reviews.

    If the author has pinged the reviewer(s), tag both and ask the reviewer(s) to re-check.
    If not, tag only the author and suggest they ping the reviewer(s).
    """
    # Separate reviews by whether the author pinged
    pinged = [r for r in stale_reviews if r.author_pinged_reviewer]
    not_pinged = [r for r in stale_reviews if not r.author_pinged_reviewer]

    parts: list[str] = []

    if pinged:
        reviewer_tags = " ".join(f"@{r.reviewer_login}" for r in pinged)
        parts.append(
            f"@{pr_author} {reviewer_tags} — This PR has new commits since the last review "
            "requesting changes, and it looks like the author has followed up. "
            "Could you take another look when you have a chance to see if the review "
            "comments have been addressed? Thanks!"
        )

    if not_pinged:
        reviewer_names = ", ".join(f"**{r.reviewer_login}**" for r in not_pinged)
        parts.append(
            f"@{pr_author} — This PR has new commits since the last review requesting changes "
            f"from {reviewer_names}. If you believe you've addressed the feedback, "
            "please ping the reviewer(s) to request a re-review. Thanks!"
        )

    return "\n\n".join(parts)


def _fetch_prs_graphql(
    token: str,
    github_repository: str,
    labels: tuple[str, ...],
    exclude_labels: tuple[str, ...],
    filter_user: str | None,
    sort: str,
    batch_size: int,
    created_after: str | None = None,
    created_before: str | None = None,
    updated_after: str | None = None,
    updated_before: str | None = None,
    review_requested: str | None = None,
    reviewed_by: str | None = None,
    after_cursor: str | None = None,
) -> tuple[list[PRData], bool, str | None]:
    """Fetch a single batch of matching PRs via GraphQL.

    Returns (prs, has_next_page, end_cursor).
    """
    query_parts = [f"repo:{github_repository}", "type:pr", "is:open"]
    if filter_user:
        query_parts.append(f"author:{filter_user}")
    if review_requested:
        query_parts.append(f"review-requested:{review_requested}")
    if reviewed_by:
        query_parts.append(f"reviewed-by:{reviewed_by}")
    for label in labels:
        query_parts.append(f'label:"{label}"')
    for label in exclude_labels:
        query_parts.append(f'-label:"{label}"')
    # Date range filters — GitHub search supports created: and updated: qualifiers
    if created_after and created_before:
        query_parts.append(f"created:{created_after}..{created_before}")
    elif created_after:
        query_parts.append(f"created:>={created_after}")
    elif created_before:
        query_parts.append(f"created:<={created_before}")
    if updated_after and updated_before:
        query_parts.append(f"updated:{updated_after}..{updated_before}")
    elif updated_after:
        query_parts.append(f"updated:>={updated_after}")
    elif updated_before:
        query_parts.append(f"updated:<={updated_before}")
    search_query = " ".join(query_parts)

    sort_field, sort_direction = sort.rsplit("-", 1)
    search_query += f" sort:{sort_field}-{sort_direction}"

    if not after_cursor:
        console_print(f"[info]Searching PRs: {search_query}[/]")

    variables: dict = {"query": search_query, "first": batch_size}
    if after_cursor:
        variables["after"] = after_cursor

    data = _graphql_request(token, _SEARCH_PRS_QUERY, variables)
    search_data = data["search"]
    page_info = search_data.get("pageInfo", {})
    has_next_page = page_info.get("hasNextPage", False)
    end_cursor = page_info.get("endCursor")

    console_print(
        f"[info]Found {search_data['issueCount']} matching "
        f"{'PRs' if search_data['issueCount'] != 1 else 'PR'}, "
        f"fetched {len(search_data['nodes'])}"
        f"{' (more available)' if has_next_page else ''}.[/]"
    )

    prs: list[PRData] = []
    for node in search_data["nodes"]:
        if not node:
            continue
        author = node.get("author") or {}
        head_sha, checks_state = _extract_basic_check_info(node)
        prs.append(
            PRData(
                number=node["number"],
                title=node["title"],
                body=node.get("body") or "",
                url=node["url"],
                created_at=node["createdAt"],
                updated_at=node["updatedAt"],
                node_id=node["id"],
                author_login=author.get("login", "ghost"),
                author_association=node.get("authorAssociation", "NONE"),
                head_sha=head_sha,
                base_ref=node.get("baseRefName", "main"),
                check_summary="",
                checks_state=checks_state,
                failed_checks=[],
                commits_behind=0,
                is_draft=bool(node.get("isDraft", False)),
                mergeable=node.get("mergeable", "UNKNOWN"),
                labels=[lbl["name"] for lbl in (node.get("labels") or {}).get("nodes", []) if lbl],
                unresolved_threads=[],
            )
        )

    return prs, has_next_page, end_cursor


def _fetch_single_pr_graphql(token: str, github_repository: str, pr_number: int) -> PRData:
    """Fetch a single PR by number via GraphQL."""
    owner, repo = github_repository.split("/", 1)
    data = _graphql_request(
        token, _FETCH_SINGLE_PR_QUERY, {"owner": owner, "repo": repo, "number": pr_number}
    )
    node = data.get("repository", {}).get("pullRequest")
    if not node:
        console_print(f"[error]PR #{pr_number} not found in {github_repository}.[/]")
        sys.exit(1)

    author = node.get("author") or {}
    head_sha, checks_state = _extract_basic_check_info(node)
    return PRData(
        number=node["number"],
        title=node["title"],
        body=node.get("body") or "",
        url=node["url"],
        created_at=node["createdAt"],
        updated_at=node["updatedAt"],
        node_id=node["id"],
        author_login=author.get("login", "ghost"),
        author_association=node.get("authorAssociation", "NONE"),
        head_sha=head_sha,
        base_ref=node.get("baseRefName", "main"),
        check_summary="",
        checks_state=checks_state,
        failed_checks=[],
        commits_behind=0,
        is_draft=bool(node.get("isDraft", False)),
        mergeable=node.get("mergeable", "UNKNOWN"),
        labels=[lbl["name"] for lbl in (node.get("labels") or {}).get("nodes", []) if lbl],
        unresolved_threads=[],
    )


def _human_readable_age(iso_date: str) -> str:
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


_author_profile_cache: dict[str, dict] = {}


def _fetch_author_profile(token: str, login: str, github_repository: str) -> dict:
    """Fetch author profile info via GraphQL: account age, PR counts, contributed repos.

    Results are cached per login so the same author is only queried once.
    """
    if login in _author_profile_cache:
        return _author_profile_cache[login]

    repo_prefix = f"repo:{github_repository} type:pr author:{login}"
    global_prefix = f"type:pr author:{login}"
    try:
        data = _graphql_request(
            token,
            _AUTHOR_PROFILE_QUERY,
            {
                "login": login,
                "repoAll": repo_prefix,
                "repoMerged": f"{repo_prefix} is:merged",
                "repoClosed": f"{repo_prefix} is:closed is:unmerged",
                "globalAll": global_prefix,
                "globalMerged": f"{global_prefix} is:merged",
                "globalClosed": f"{global_prefix} is:closed is:unmerged",
            },
        )
    except SystemExit:
        # Bot accounts (e.g. dependabot) cannot be resolved as Users
        profile = {
            "login": login,
            "account_age": "unknown (bot account)",
            "repo_total_prs": 0,
            "repo_merged_prs": 0,
            "repo_closed_prs": 0,
            "global_total_prs": 0,
            "global_merged_prs": 0,
            "global_closed_prs": 0,
            "contributed_repos": [],
            "contributed_repos_total": 0,
        }
        _author_profile_cache[login] = profile
        return profile
    user_data = data.get("user") or {}
    created_at = user_data.get("createdAt", "unknown")
    account_age = _human_readable_age(created_at) if created_at != "unknown" else "unknown"

    # Extract contributed repos (public only)
    contrib_data = user_data.get("repositoriesContributedTo", {})
    contrib_total = contrib_data.get("totalCount", 0)
    contributed_repos = []
    for repo_node in contrib_data.get("nodes", []):
        if repo_node and not repo_node.get("isPrivate", False):
            contributed_repos.append(
                {
                    "name": repo_node["nameWithOwner"],
                    "url": repo_node["url"],
                    "stars": repo_node.get("stargazerCount", 0),
                }
            )

    profile = {
        "login": login,
        "account_age": account_age,
        "created_at": created_at,
        "repo_total_prs": data.get("repoAll", {}).get("issueCount", 0),
        "repo_merged_prs": data.get("repoMerged", {}).get("issueCount", 0),
        "repo_closed_prs": data.get("repoClosed", {}).get("issueCount", 0),
        "global_total_prs": data.get("globalAll", {}).get("issueCount", 0),
        "global_merged_prs": data.get("globalMerged", {}).get("issueCount", 0),
        "global_closed_prs": data.get("globalClosed", {}).get("issueCount", 0),
        "contributed_repos": contributed_repos,
        "contributed_repos_total": contrib_total,
    }
    _author_profile_cache[login] = profile
    return profile


def _is_bot_account(login: str) -> bool:
    """Check if a GitHub login belongs to a bot account."""
    return login.lower() in _BOT_ACCOUNT_LOGINS or login.endswith("[bot]")


def _convert_pr_to_draft(token: str, node_id: str) -> bool:
    """Convert a PR to draft using GitHub GraphQL API."""
    try:
        _graphql_request(token, _CONVERT_TO_DRAFT_MUTATION, {"prId": node_id})
        return True
    except SystemExit:
        return False


def _mark_pr_ready_for_review(token: str, node_id: str) -> bool:
    """Mark a draft PR as ready for review using GitHub GraphQL API."""
    try:
        _graphql_request(token, _MARK_READY_FOR_REVIEW_MUTATION, {"prId": node_id})
        return True
    except SystemExit:
        return False


def _close_pr(token: str, node_id: str) -> bool:
    """Close a PR using GitHub GraphQL API."""
    try:
        _graphql_request(token, _CLOSE_PR_MUTATION, {"prId": node_id})
        return True
    except SystemExit:
        return False


def _post_comment(token: str, node_id: str, body: str) -> bool:
    """Post a comment on a PR using GitHub GraphQL API."""
    try:
        _graphql_request(token, _ADD_COMMENT_MUTATION, {"subjectId": node_id, "body": body})
        return True
    except SystemExit:
        return False


def _post_review_comment(
    token: str,
    github_repository: str,
    pr_number: int,
    commit_id: str,
    path: str,
    line: int,
    body: str,
) -> bool:
    """Post a single review comment on a specific line of a PR via REST API."""
    import requests

    owner, repo = github_repository.split("/", 1)
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/comments"
    payload = {
        "commit_id": commit_id,
        "path": path,
        "line": line,
        "side": "RIGHT",
        "body": body,
    }
    response = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    if response.status_code in (200, 201):
        return True
    get_console().print(
        f"[warning]Failed to post review comment on {path}:{line}: "
        f"{response.status_code} {response.text[:200]}[/]"
    )
    return False


def _submit_pr_review(
    token: str,
    github_repository: str,
    pr_number: int,
    commit_id: str,
    body: str,
    event: str = "COMMENT",
) -> bool:
    """Submit an overall PR review (APPROVE, REQUEST_CHANGES, or COMMENT) via REST API."""
    import requests

    owner, repo = github_repository.split("/", 1)
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/reviews"
    payload = {
        "commit_id": commit_id,
        "body": body,
        "event": event,
    }
    response = requests.post(
        url,
        json=payload,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    if response.status_code in (200, 201):
        return True
    get_console().print(f"[warning]Failed to submit review: {response.status_code} {response.text[:200]}[/]")
    return False


def _update_pr_branch(token: str, github_repository: str, pr_number: int) -> bool:
    """Update (rebase) a PR branch to the latest base branch via GitHub REST API."""
    import requests

    owner, repo = github_repository.split("/", 1)
    url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/update-branch"
    response = requests.put(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    if response.status_code in (200, 202):
        return True
    get_console().print(
        f"[warning]Failed to update PR branch: {response.status_code} {response.text[:200]}[/]"
    )
    return False


_label_id_cache: dict[str, str | None] = {}


def _resolve_label_id(token: str, github_repository: str, label_name: str) -> str | None:
    """Resolve a label name to its node ID via GraphQL. Cached per label name."""
    if label_name in _label_id_cache:
        return _label_id_cache[label_name]
    owner, repo = github_repository.split("/", 1)
    try:
        data = _graphql_request(
            token, _GET_LABEL_ID_QUERY, {"owner": owner, "repo": repo, "name": label_name}
        )
        label_data = (data.get("repository") or {}).get("label")
        label_id = label_data["id"] if label_data else None
    except (SystemExit, KeyError):
        label_id = None
    _label_id_cache[label_name] = label_id
    return label_id


def _add_label(token: str, github_repository: str, pr_node_id: str, label_name: str) -> bool:
    """Add a label to a PR. Returns True on success."""
    label_id = _resolve_label_id(token, github_repository, label_name)
    if not label_id:
        console_print(f"[warning]Label '{label_name}' not found in {github_repository}. Skipping label.[/]")
        return False
    try:
        _graphql_request(token, _ADD_LABELS_MUTATION, {"labelableId": pr_node_id, "labelIds": [label_id]})
        return True
    except SystemExit:
        return False


def _remove_label(token: str, github_repository: str, pr_node_id: str, label_name: str) -> bool:
    """Remove a label from a PR. Returns True on success."""
    label_id = _resolve_label_id(token, github_repository, label_name)
    if not label_id:
        get_console().print(
            f"[warning]Label '{label_name}' not found in {github_repository}. Skipping label removal.[/]"
        )
        return False
    try:
        _graphql_request(token, _REMOVE_LABELS_MUTATION, {"labelableId": pr_node_id, "labelIds": [label_id]})
        return True
    except SystemExit:
        return False


def _load_labels_from_boring_cyborg() -> list[str]:
    """Read labels from .github/boring-cyborg.yml."""
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

    boring_cyborg_path = AIRFLOW_ROOT_PATH / ".github" / "boring-cyborg.yml"
    if not boring_cyborg_path.exists():
        console_print("[warning]boring-cyborg.yml not found, label validation disabled.[/]")
        return []

    import yaml

    with open(boring_cyborg_path) as f:
        data = yaml.safe_load(f)

    labels: list[str] = []
    label_pr_section = data.get("labelPRBasedOnFilePath", {})
    for label_name in label_pr_section:
        labels.append(label_name)
    return sorted(labels)


def _load_what_to_do_next() -> str:
    """Load 'what to do next' instructions from contributing docs."""
    from airflow_breeze.utils.llm_utils import _read_file_section
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

    # Read the "What happens when a PR is converted to draft?" section
    section = _read_file_section(
        AIRFLOW_ROOT_PATH,
        "contributing-docs/05_pull_requests.rst",
        "What happens when a PR is converted to draft",
        "Converting a PR to draft is",
    )
    if section:
        # Convert RST list items to markdown
        lines = []
        for line in section.splitlines():
            stripped = line.strip()
            if stripped.startswith("-"):
                lines.append(stripped.replace("-  ", "- ", 1))
            elif stripped:
                lines.append(stripped)
        return "\n".join(lines)
    return (
        "- Fix each issue listed above.\n"
        "- Make sure static checks pass locally (`prek run --from-ref main --stage pre-commit`).\n"
        '- Mark the PR as "Ready for review" when you\'re done.'
    )


def _build_collaborator_comment(
    pr_author: str,
    violations: list,
    commits_behind: int,
    base_ref: str,
    comment_only: bool = False,
) -> str:
    """Build a simplified comment for PRs authored by collaborators.

    Collaborators know the project well — just state the issues directly without
    instructions, links, or encouragement.
    """
    violation_lines = []
    for v in violations:
        line = f"- **{v.category}**: {v.explanation}"
        if v.details:
            line += f" {v.details}"
        violation_lines.append(line)
    violations_text = "\n".join(violation_lines)

    rebase_note = ""
    if commits_behind > 0:
        rebase_note = (
            f"\n\nBranch is {commits_behind} "
            f"commit{'s' if commits_behind != 1 else ''} behind `{base_ref}` "
            "-- some failures may be from the base branch."
        )

    if comment_only:
        return f"@{pr_author} Issues found:\n{violations_text}{rebase_note}"

    return f"@{pr_author} Converted to draft. Issues found:\n{violations_text}{rebase_note}"


def _build_comment(
    pr_author: str,
    violations: list,
    pr_number: int,
    commits_behind: int,
    base_ref: str,
    comment_only: bool = False,
    is_collaborator: bool = False,
) -> str:
    """Build the comment to post on a flagged PR.

    When comment_only is True, the comment just lists findings without
    mentioning draft conversion.
    When is_collaborator is True, produces a simplified comment without instructions.
    """
    if is_collaborator:
        return _build_collaborator_comment(
            pr_author, violations, commits_behind, base_ref, comment_only=comment_only
        )

    violation_lines = []
    for v in violations:
        icon = "x" if v.severity == "error" else "warning"
        line = f"- :{icon}: **{v.category}**: {v.explanation}"
        if v.details:
            line += f" {v.details}"
        violation_lines.append(line)
    violations_text = "\n".join(violation_lines)

    what_to_do = _load_what_to_do_next()

    rebase_note = ""
    if commits_behind > 0:
        rebase_note = (
            f"\n\n> **Note:** Your branch is **{commits_behind} "
            f"commit{'s' if commits_behind != 1 else ''} behind `{base_ref}`**. "
            "Some check failures may be caused by changes in the base branch rather than by your PR. "
            "Please rebase your branch and push again to get up-to-date CI results."
        )

    no_rush = (
        "There is no rush — take your time and work at your own pace. "
        "We appreciate your contribution and are happy to wait for updates."
    )

    if comment_only:
        return (
            f"@{pr_author} This PR has a few issues that need to be addressed before it can be "
            f"reviewed — please see our {QUALITY_CRITERIA_LINK}.\n\n"
            f"**Issues found:**\n{violations_text}{rebase_note}\n\n"
            f"**What to do next:**\n{what_to_do}\n\n"
            f"{no_rush} "
            "If you have questions, feel free to ask on the "
            "[Airflow Slack](https://s.apache.org/airflow-slack)."
        )

    return (
        f"@{pr_author} This PR has been converted to **draft** because it does not yet meet "
        f"our {QUALITY_CRITERIA_LINK}.\n\n"
        f"**Issues found:**\n{violations_text}{rebase_note}\n\n"
        f"**What to do next:**\n{what_to_do}\n\n"
        "Converting a PR to draft is **not** a rejection — it is an invitation to bring the PR up to "
        "the project's standards so that maintainer review time is spent productively. "
        f"{no_rush} "
        "If you have questions, feel free to ask on the "
        "[Airflow Slack](https://s.apache.org/airflow-slack)."
    )


def _build_close_comment(
    pr_author: str,
    violations: list,
    pr_number: int,
    author_flagged_count: int,
    is_collaborator: bool = False,
) -> str:
    """Build the comment to post on a PR being closed due to quality issues."""
    if is_collaborator:
        violation_lines = []
        for v in violations:
            line = f"- **{v.category}**: {v.explanation}"
            if v.details:
                line += f" {v.details}"
            violation_lines.append(line)
        violations_text = "\n".join(violation_lines)
        return f"@{pr_author} Closing due to quality issues:\n{violations_text}"

    violation_lines = []
    for v in violations:
        icon = "x" if v.severity == "error" else "warning"
        line = f"- :{icon}: **{v.category}**: {v.explanation}"
        if v.details:
            line += f" {v.details}"
        violation_lines.append(line)
    if author_flagged_count > 3:
        violation_lines.append(
            f"- :x: **Multiple flagged PRs**: You currently have **{author_flagged_count} "
            f"{'PRs' if author_flagged_count != 1 else 'PR'}** "
            "flagged for quality issues in this repository. We recommend focusing on improving "
            f"your existing {'PRs' if author_flagged_count != 1 else 'PR'} before opening new ones."
        )
    violations_text = "\n".join(violation_lines)

    return (
        f"@{pr_author} This PR has been **closed because of multiple quality violations** "
        f"— it does not meet our {QUALITY_CRITERIA_LINK}.\n\n"
        f"**Issues found:**\n{violations_text}\n\n"
        "You are welcome to open a new PR that addresses the issues listed above. "
        "There is no rush — take your time and work at your own pace. "
        "If you have questions, feel free to ask on the "
        "[Airflow Slack](https://s.apache.org/airflow-slack)."
    )


def _compute_default_action(
    pr: PRData,
    assessment,
    author_flagged_count: dict[str, int],
    main_failures: RecentPRFailureInfo | None = None,
) -> tuple[TriageAction, str]:
    """Compute the suggested default triage action and reason for a flagged PR."""
    # If LLM potentially flagged for reporting, default to skip
    # so the user can review the report details before taking action manually
    if getattr(assessment, "should_report", False):
        reason = "Potentially flagged for reporting to GitHub"
        if assessment.summary:
            reason += f" — {assessment.summary.lower()}"
        return TriageAction.SKIP, f"{reason} — review details before deciding"

    reason_parts: list[str] = []

    has_conflicts = pr.mergeable == "CONFLICTING"
    if has_conflicts:
        reason_parts.append("has merge conflicts")

    failed_count = len(pr.failed_checks)
    has_ci_failures = failed_count > 0
    if has_ci_failures:
        reason_parts.append(f"{failed_count} CI failure{'s' if failed_count != 1 else ''}")

    # Check if failures match main branch failures
    main_matching: list[str] = []
    if has_ci_failures and main_failures:
        main_matching = main_failures.find_matching_failures(pr.failed_checks)

    has_unresolved_comments = pr.unresolved_review_comments > 0
    if has_unresolved_comments and not any("unresolved" in p for p in reason_parts):
        reviewer_logins = sorted({t.reviewer_login for t in pr.unresolved_threads})
        if reviewer_logins:
            names = ", ".join(f"@{r}" for r in reviewer_logins)
            reason_parts.append(
                f"{pr.unresolved_review_comments} unresolved review "
                f"comment{'s' if pr.unresolved_review_comments != 1 else ''} "
                f"from {names}"
            )
        else:
            reason_parts.append(
                f"{pr.unresolved_review_comments} unresolved review "
                f"comment{'s' if pr.unresolved_review_comments != 1 else ''}"
            )

    if assessment.summary:
        reason_parts.append(assessment.summary.lower())

    count = author_flagged_count.get(pr.author_login, 0)
    if count > 3:
        reason_parts.append(f"author has {count} flagged {'PRs' if count != 1 else 'PR'}")
        action = TriageAction.CLOSE
    elif pr.checks_state == "UNKNOWN" and not has_conflicts:
        # No checks at all — suggest rebase so CI workflows get triggered
        reason_parts.append("no CI checks found — needs rebase")
        action = TriageAction.DRAFT
    elif main_matching and len(main_matching) == failed_count:
        # ALL failures match recent PRs — consistent pattern, suggest rerun not draft
        reason_parts.append("all CI failures also appear in other recent PRs — likely systemic")
        action = TriageAction.RERUN
    elif main_matching:
        # Some failures match recent PRs — consistent pattern
        reason_parts.append(
            f"{len(main_matching)}/{failed_count} CI failures also appear in other recent PRs — likely systemic"
        )
        action = TriageAction.RERUN
    elif (
        not has_conflicts
        and has_ci_failures
        and failed_count <= 2
        and not has_unresolved_comments
        and pr.commits_behind <= 50
    ):
        # Only 1-2 CI failures, no conflicts, no unresolved comments, not too far behind — suggest rerun
        action = TriageAction.RERUN
    elif not has_ci_failures and not has_conflicts and has_unresolved_comments:
        # CI passes, no conflicts, no LLM issues — only unresolved review comments; just add a comment
        action = TriageAction.COMMENT
    else:
        action = TriageAction.DRAFT

    reason = "; ".join(reason_parts) if reason_parts else "flagged for quality issues"
    reason = reason[0].upper() + reason[1:]
    action_label = {
        TriageAction.DRAFT: "draft",
        TriageAction.COMMENT: "comment",
        TriageAction.CLOSE: "close",
        TriageAction.RERUN: "rerun checks",
        TriageAction.REBASE: "rebase",
    }.get(action, str(action))
    return action, f"{reason} — suggesting {action_label}"


def _fmt_duration(seconds: float) -> str:
    """Format a duration in seconds to a human-friendly string like '2m 05s' or '3.2s'."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    minutes = int(seconds) // 60
    secs = int(seconds) % 60
    if minutes < 60:
        return f"{minutes}m {secs:02d}s"
    hours = minutes // 60
    mins = minutes % 60
    return f"{hours}h {mins:02d}m {secs:02d}s"


def _select_violations(violations: list) -> list | None:
    """Prompt the user to select which violations to include in the comment.

    Returns the selected subset, or the original list if the user accepts all.
    Returns None if there are no violations to select from.
    """
    if len(violations) <= 1:
        return violations

    console = get_console()
    console.print("\n  [bold]Select which issues to include in the comment:[/]")
    for i, v in enumerate(violations, 1):
        color = "red" if v.severity == "error" else "yellow"
        console.print(f"    [{color}]{i}.[/{color}] {v.category}: {v.explanation}")

    console.print("    [dim]Enter numbers (e.g. 1,3), 'a' for all, or 's' to skip comment[/]")
    user_input = input("  Include [A/numbers/s]: ").strip()

    if not user_input or user_input.lower() == "a":
        return violations
    if user_input.lower() == "s":
        return []

    selected = []
    for raw_num in user_input.split(","):
        stripped = raw_num.strip()
        if stripped.isdigit():
            idx = int(stripped) - 1
            if 0 <= idx < len(violations):
                selected.append(violations[idx])
    return selected if selected else violations


def _linkify_commit_hashes(text: str, github_repository: str = "apache/airflow") -> str:
    """Convert commit hashes in text to inline GitHub links.

    Replaces full 40-char and abbreviated 7+ char hex hashes with clickable
    Markdown-style links pointing to the commit on GitHub.  Hashes that are
    already part of a URL are left untouched.
    """
    repo_url = f"https://github.com/{github_repository}"

    def _replace(m: re.Match) -> str:
        sha = m.group(0)
        # Skip if the hash is already inside a URL (preceded by '/').
        start = m.start()
        if start > 0 and text[start - 1] == "/":
            return sha
        short = sha[:10]
        return f"[{short}]({repo_url}/commit/{sha})"

    # Match 7-40 hex-char sequences on word boundaries
    return re.sub(r"\b[0-9a-f]{7,40}\b", _replace, text)


def _pr_link(pr: PRData) -> str:
    """Return a Rich-markup clickable link for a PR: [link=url]#number[/link]."""
    return f"[link={pr.url}]#{pr.number}[/link]"


def _print_pr_header(pr: PRData, index: int | None = None, total: int | None = None) -> None:
    """Print a highly visible separator and header for a PR section."""
    console = get_console()
    console.print()
    console.print()
    counter = f" ({index}/{total})" if index is not None and total is not None else ""
    console.rule(
        f"[bold cyan]PR #{pr.number}[/] — {pr.title[:70]} — by [bold]{pr.author_login}[/]{counter}",
        style="cyan",
    )
    console.print()


def _display_pr_info_panels(pr: PRData, author_profile: dict | None, *, open_in_browser: bool = False):
    """Display PR info and author panels (shared by flagged-PR and workflow-approval flows)."""
    import webbrowser

    from rich.markdown import Markdown

    console = get_console()
    console.print()
    console.rule(style="dim")
    console.print()

    status_info = ""
    if pr.is_draft:
        status_info += "\n[yellow]Draft PR[/]"
    if pr.commits_behind > 0:
        status_info += (
            f"\n[yellow]{pr.commits_behind} commit{'s' if pr.commits_behind != 1 else ''} "
            f"behind {pr.base_ref}[/]"
        )
    if pr.mergeable == "CONFLICTING":
        status_info += "\n[red]Has merge conflicts[/]"
    pr_info = (
        f"[link={pr.url}][cyan]#{pr.number}[/][/link] {pr.title}\n"
        f"Author: [link=https://github.com/{pr.author_login}][bold]{pr.author_login}[/][/link]  |  "
        f"Created: {_human_readable_age(pr.created_at)}  |  "
        f"Updated: {_human_readable_age(pr.updated_at)}"
        f"{status_info}"
    )
    console.print(Panel(pr_info, title="Pull Request", border_style="cyan"))

    # Display PR description as rendered markdown
    if pr.body and pr.body.strip():
        # Truncate very long descriptions
        body_text = pr.body.strip()
        if len(body_text) > 3000:
            body_text = body_text[:3000] + "\n\n*... (truncated)*"
        body_text = _linkify_commit_hashes(body_text)
        console.print(Panel(Markdown(body_text), title="PR Description", border_style="dim"))

    # Auto-open PR "Files changed" view in browser when requested
    if open_in_browser:
        files_url = f"{pr.url}/files"
        webbrowser.open(files_url)
        console.print(f"  [info]Opened {files_url} in browser.[/]")

    if author_profile:
        login = author_profile["login"]

        # Color account age: red if < 1 week, yellow if < 1 month
        account_age_text = author_profile["account_age"]
        created_at_raw = author_profile.get("created_at", "unknown")
        if created_at_raw != "unknown":
            from datetime import datetime, timezone

            try:
                created_dt = datetime.fromisoformat(created_at_raw.replace("Z", "+00:00"))
                age_days = (datetime.now(timezone.utc) - created_dt).days
                if age_days < 7:
                    account_age_text = f"[red]{account_age_text}[/]"
                elif age_days < 30:
                    account_age_text = f"[yellow]{account_age_text}[/]"
                elif age_days >= 60 and author_profile.get("repo_merged_prs", 0) > 0:
                    account_age_text = f"[green]{account_age_text}[/]"
            except (ValueError, TypeError):
                pass

        def _pr_line(label: str, total: int, merged: int, closed: int) -> str:
            """Format a PR stats line with colors."""
            merged_text = f"[green]{merged} merged[/]"
            closed_text = f"{closed} closed (unmerged)"
            line = f"{label}: {total} total, {merged_text}, {closed_text}"
            if closed > merged:
                line = f"[red]{label}: {total} total, {merged} merged, {closed_text}[/]"
            return line

        lines = [
            f"Account created: {account_age_text}",
            _pr_line(
                "PRs in this repo",
                author_profile["repo_total_prs"],
                author_profile["repo_merged_prs"],
                author_profile["repo_closed_prs"],
            ),
            _pr_line(
                "PRs across GitHub",
                author_profile["global_total_prs"],
                author_profile["global_merged_prs"],
                author_profile["global_closed_prs"],
            ),
        ]

        contributed_repos = author_profile.get("contributed_repos", [])
        contrib_total = author_profile.get("contributed_repos_total", 0)
        if contributed_repos:
            repo_items = []
            for repo in contributed_repos:
                repo_link = f"[link={repo['url']}/pulls?q=author%3A{login}]{repo['name']}[/link]"
                repo_items.append(repo_link)
            repos_text = ", ".join(repo_items)
            if contrib_total > len(contributed_repos):
                repos_text += f" (+{contrib_total - len(contributed_repos)} more)"
            lines.append(f"Contributed to: {repos_text}")
        elif contrib_total > 0:
            lines.append(f"Contributed to: {contrib_total} repos (private or not shown)")
        else:
            lines.append("Contributed to: no public repos found")

        author_link = f"[link=https://github.com/{login}]{login}[/link]"
        console.print(Panel("\n".join(lines), title=f"Author: {author_link}", border_style="yellow"))


def _display_unresolved_threads_panel(pr: PRData) -> None:
    """Display a panel showing unresolved review thread details with reviewer and author replies."""
    if not pr.unresolved_threads:
        return
    console = get_console()
    lines = []
    for i, t in enumerate(pr.unresolved_threads, 1):
        reviewer_body = t.comment_body[:300]
        if len(t.comment_body) > 300:
            reviewer_body += "..."
        reviewer_body = _linkify_commit_hashes(reviewer_body)
        url_link = f"[link={t.comment_url}]view[/link]" if t.comment_url else ""
        lines.append(
            f"  [bold]{i}. @{t.reviewer_login}[/] ({t.reviewer_association}) {url_link}\n"
            f"     [dim]Comment:[/] {reviewer_body}"
        )
        if t.author_last_reply:
            reply = t.author_last_reply[:200]
            if len(t.author_last_reply) > 200:
                reply += "..."
            reply = _linkify_commit_hashes(reply)
            lines.append(f"     [dim]Author reply:[/] {reply}")
        else:
            lines.append("     [dim]Author reply:[/] [yellow]— no reply —[/]")
    console.print(
        Panel(
            "\n".join(lines),
            title=f"Unresolved Review Threads ({len(pr.unresolved_threads)})",
            border_style="yellow",
        )
    )


def _display_pr_panel(pr: PRData, author_profile: dict | None, assessment):
    """Display Rich panels with PR details, author info, and violations."""
    console = get_console()
    _display_pr_info_panels(pr, author_profile)

    violation_lines = []
    if getattr(assessment, "should_report", False):
        violation_lines.append(
            "[yellow]*** POTENTIALLY FLAGGED — This PR may warrant reporting to GitHub "
            "(possible prompt injection, automated spam, or ToS violation). "
            "Please review carefully before deciding. ***[/yellow]\n"
        )
    for v in assessment.violations:
        color = "red" if v.severity == "error" else "yellow"
        violation_lines.append(f"[{color}][{v.severity.upper()}][/{color}] {v.category}: {v.explanation}")
    border_style = "bold yellow" if getattr(assessment, "should_report", False) else "red"
    console.print(
        Panel(
            "\n".join(violation_lines), title=f"Assessment: {assessment.summary}", border_style=border_style
        )
    )

    # Show cache indicator if assessment came from cache
    if getattr(assessment, "_from_cache", False):
        console.print("[dim]Using cached LLM assessment (same commit hash as previous run).[/]")

    # Show deferred JSON parse-fix info if present
    fix_info = getattr(assessment, "json_fix_info", None)
    if fix_info:
        _display_json_fix_info(fix_info)

    # Show unresolved thread details when relevant
    _display_unresolved_threads_panel(pr)


def _display_log_snippets_panel(log_snippets: dict[str, str]) -> None:
    """Display Rich panel(s) with log snippets from failed CI checks."""
    console = get_console()
    for check_name, snippet in log_snippets.items():
        # Truncate very long snippets for display
        display_snippet = snippet
        if len(display_snippet) > 2000:
            display_snippet = display_snippet[:2000] + "\n... (truncated)"
        console.print(
            Panel(
                display_snippet,
                title=f"Failed check logs: {check_name}",
                border_style="red",
                expand=False,
            )
        )


def _launch_background_log_fetching(
    token: str,
    github_repository: str,
    prs: list,
    llm_concurrency: int,
) -> dict[int, Future[dict[str, str]]]:
    """Launch background CI log fetching for all PRs with failed checks.

    Returns a dict mapping PR number -> Future[dict[str, str]].
    Uses the same concurrency level as LLM assessments.
    """
    log_futures: dict[int, Future[dict[str, str]]] = {}
    prs_with_failures = [pr for pr in prs if pr.failed_checks and pr.head_sha]
    if not prs_with_failures:
        return log_futures

    log_executor = ThreadPoolExecutor(max_workers=llm_concurrency)
    for pr in prs_with_failures:
        log_futures[pr.number] = log_executor.submit(
            _fetch_failed_job_log_snippets,
            token,
            github_repository,
            pr.head_sha,
            pr.failed_checks,
        )

    pr_word = "PRs" if len(prs_with_failures) != 1 else "PR"
    get_console().print(
        f"[info]Launched CI log fetching for {len(prs_with_failures)} {pr_word} "
        f"with failures in background (concurrency: {llm_concurrency}).[/]"
    )
    return log_futures


def _display_workflow_approval_panel(
    pr: PRData,
    author_profile: dict | None,
    pending_runs: list[dict],
    check_counts: dict[str, int] | None = None,
):
    """Display Rich panels for a PR needing workflow approval."""
    console = get_console()
    _display_pr_info_panels(pr, author_profile)

    run_lines = []
    if pending_runs:
        for run in pending_runs:
            name = run.get("name", "unknown")
            run_url = run.get("html_url", "")
            if run_url:
                run_lines.append(f"  [link={run_url}]{name}[/link]")
            else:
                run_lines.append(f"  {name}")
        runs_text = "\n".join(run_lines)
        info_text = (
            f"[bright_cyan]{len(pending_runs)} workflow "
            f"{'runs' if len(pending_runs) != 1 else 'run'} awaiting approval:[/]\n{runs_text}\n\n"
        )
    else:
        info_text = "[bright_cyan]No test workflows have run on this PR.[/]\n\n"

    if check_counts:
        info_text += f"Check status: {_format_check_status_counts(check_counts)}\n\n"

    if pr.is_draft:
        info_text += "[yellow]This PR is a draft.[/]\n"
    info_text += (
        "Please review the PR changes before approving:\n"
        f"  [link={pr.url}/files]View changes on GitHub[/link]"
    )
    console.print(Panel(info_text, title="Workflow Approval Needed", border_style="bright_cyan"))


def _resolve_unknown_mergeable(token: str, github_repository: str, prs: list[PRData]) -> int:
    """Resolve UNKNOWN mergeable status for PRs via REST API.

    GitHub's GraphQL API computes mergeability lazily and often returns UNKNOWN.
    The REST API triggers computation and returns the result. Sometimes the first
    call still returns null, so we retry once after a short delay.

    Returns the number of PRs whose status was resolved.
    """
    import time

    import requests

    unknown_prs = [pr for pr in prs if pr.mergeable == "UNKNOWN"]
    if not unknown_prs:
        return 0

    resolved = 0
    still_unknown: list[PRData] = []

    for pr in unknown_prs:
        url = f"https://api.github.com/repos/{github_repository}/pulls/{pr.number}"
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3+json"},
            timeout=30,
        )
        if response.status_code == 200:
            data = response.json()
            mergeable = data.get("mergeable")
            if mergeable is True:
                pr.mergeable = "MERGEABLE"
                resolved += 1
            elif mergeable is False:
                pr.mergeable = "CONFLICTING"
                resolved += 1
            else:
                # null means GitHub hasn't computed it yet — retry later
                still_unknown.append(pr)
        else:
            still_unknown.append(pr)

    if still_unknown:
        # Give GitHub a moment to compute mergeability, then retry
        time.sleep(2)
        for pr in still_unknown:
            url = f"https://api.github.com/repos/{github_repository}/pulls/{pr.number}"
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3+json"},
                timeout=30,
            )
            if response.status_code == 200:
                data = response.json()
                mergeable = data.get("mergeable")
                if mergeable is True:
                    pr.mergeable = "MERGEABLE"
                    resolved += 1
                elif mergeable is False:
                    pr.mergeable = "CONFLICTING"
                    resolved += 1

    return resolved


def _llm_progress_status(completed: int, total: int, flagged: int, errors: int) -> str:
    """Build a one-line LLM assessment progress string for display between prompts."""
    if total == 0:
        return ""
    remaining = total - completed
    parts = [f"{completed}/{total} done"]
    if flagged:
        parts.append(f"{flagged} flagged")
    if errors:
        parts.append(f"{errors} errors")
    if remaining:
        parts.append(f"{remaining} in progress")
    return f"[dim]LLM assessment: {', '.join(parts)}[/]"


def _collect_llm_results(
    future_to_pr: dict,
    llm_assessments: dict,
    llm_completed: list[int],
    llm_errors: list[int],
    llm_passing: list,
    block: bool = False,
) -> None:
    """Collect completed LLM futures. If block=False, only collect already-done futures."""
    from concurrent.futures import wait

    if not future_to_pr:
        return

    if block:
        # Wait for all remaining futures
        pending = [f for f in future_to_pr if not f.done()]
        if pending:
            wait(pending)

    done_futures = [f for f in future_to_pr if f.done() and f not in llm_completed]
    for future in done_futures:
        llm_completed.append(future)
        pr = future_to_pr[future]
        assessment = future.result()
        if assessment.error:
            llm_errors.append(pr.number)
            msg = f"  [warning]PR {_pr_link(pr)} LLM assessment failed ({assessment.summary})."
            if assessment.error_debug_file:
                msg += f" Raw response saved to {assessment.error_debug_file}"
            console_print(f"{msg}[/]")
            continue
        if not assessment.should_flag:
            llm_passing.append(pr)
            console_print(f"  [success]PR {_pr_link(pr)} passes LLM quality check.[/]")
            continue
        llm_assessments[pr.number] = assessment
        if assessment.should_report:
            console_print(
                f"  [yellow]PR {_pr_link(pr)} potentially flagged for reporting to GitHub.[/yellow]"
            )


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
    # LLM background state
    llm_future_to_pr: dict
    llm_assessments: dict
    llm_completed: list
    llm_errors: list[int]
    llm_passing: list
    # Main branch failure info (optional)
    main_failures: RecentPRFailureInfo | None = None
    # Background CI log fetching: PR number -> Future[dict[str, str]]
    log_futures: dict[int, Future[dict[str, str]]] = field(default_factory=dict)

    def collect_llm_progress(self) -> None:
        """Collect completed LLM results and print progress status."""
        if not self.llm_future_to_pr:
            return
        _collect_llm_results(
            self.llm_future_to_pr,
            self.llm_assessments,
            self.llm_completed,
            self.llm_errors,
            self.llm_passing,
        )
        progress = _llm_progress_status(
            len(self.llm_completed),
            len(self.llm_future_to_pr),
            len(self.llm_assessments),
            len(self.llm_errors),
        )
        if progress:
            console_print(progress)


def _confirm_action(pr: PRData, description: str, forced_answer: str | None = None) -> bool:
    """Ask for final confirmation before modifying a PR. Returns True if confirmed.

    Uses single-keypress input (no Enter required) for a snappy workflow.
    """
    import os

    from airflow_breeze.utils.confirm import _read_char
    from airflow_breeze.utils.shared_options import get_forced_answer

    force = forced_answer or get_forced_answer() or os.environ.get("ANSWER")
    if force:
        print(f"Forced answer for confirm '{description}': {force}")
        return force.upper() in ("Y", "YES")

    prompt = f"  Confirm: {description} on PR {_pr_link(pr)}? [Y/n] "
    console_print(prompt, end="")

    try:
        ch = _read_char()
    except (KeyboardInterrupt, EOFError):
        console_print()
        console_print(f"  [info]Cancelled — no changes made to PR {_pr_link(pr)}.[/]")
        return False

    # Ignore multi-byte escape sequences (arrow keys, etc.)
    if len(ch) > 1:
        console_print()
        console_print(f"  [info]Cancelled — no changes made to PR {_pr_link(pr)}.[/]")
        return False

    # Echo the character and move to next line
    console_print(ch)

    if ch.upper() in ("Y", "\r", "\n", ""):
        return True
    console_print(f"  [info]Cancelled — no changes made to PR {_pr_link(pr)}.[/]")
    return False


def _execute_triage_action(
    ctx: TriageContext,
    pr: PRData,
    action: TriageAction,
    *,
    draft_comment: str,
    close_comment: str,
    comment_only_text: str | None = None,
) -> None:
    """Execute a single triage action on a PR. Mutates ctx.stats."""
    stats = ctx.stats

    if action == TriageAction.SKIP:
        console_print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
        stats.total_skipped_action += 1
        return

    if action == TriageAction.READY:
        if not _confirm_action(pr, "Add 'ready for maintainer review' label", ctx.answer_triage):
            stats.total_skipped_action += 1
            return
        console_print(
            f"  [info]Marking PR {_pr_link(pr)} as ready — adding '{_READY_FOR_REVIEW_LABEL}' label.[/]"
        )
        if _add_label(ctx.token, ctx.github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
            console_print(f"  [success]Label '{_READY_FOR_REVIEW_LABEL}' added to PR {_pr_link(pr)}.[/]")
            stats.total_ready += 1
        else:
            console_print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
        return

    if action == TriageAction.RERUN:
        if pr.head_sha and pr.failed_checks:
            console_print(
                f"  Rerunning {len(pr.failed_checks)} failed "
                f"{'checks' if len(pr.failed_checks) != 1 else 'check'} for PR {_pr_link(pr)}..."
            )
            rerun_count = _rerun_failed_workflow_runs(
                ctx.token, ctx.github_repository, pr.head_sha, pr.failed_checks
            )
            if rerun_count:
                console_print(
                    f"  [success]Rerun triggered for {rerun_count} workflow "
                    f"{'runs' if rerun_count != 1 else 'run'} on PR {_pr_link(pr)}.[/]"
                )
                stats.total_rerun += 1
            else:
                # No completed failed runs to rerun — check if workflows are still running
                console_print(
                    f"  [warning]No completed failed runs found for PR {_pr_link(pr)}. "
                    f"Checking for in-progress workflows...[/]"
                )
                restarted = _cancel_and_rerun_in_progress_workflows(
                    ctx.token, ctx.github_repository, pr.head_sha
                )
                if restarted:
                    console_print(
                        f"  [success]Cancelled and restarted {restarted} workflow "
                        f"{'runs' if restarted != 1 else 'run'} on PR {_pr_link(pr)}.[/]"
                    )
                    stats.total_rerun += 1
                else:
                    console_print(f"  [warning]Could not rerun any workflow runs for PR {_pr_link(pr)}.[/]")
        else:
            console_print(f"  [warning]No failed checks to rerun for PR {_pr_link(pr)}.[/]")
        return

    if action == TriageAction.REBASE:
        if not _confirm_action(pr, "Update (rebase) PR branch", ctx.answer_triage):
            stats.total_skipped_action += 1
            return

        get_console().print(f"  Updating branch for PR {_pr_link(pr)}...")
        if _update_pr_branch(ctx.token, ctx.github_repository, pr.number):
            get_console().print(f"  [success]Branch updated for PR {_pr_link(pr)}.[/]")
            stats.total_rebased += 1
        else:
            get_console().print(f"  [error]Failed to update branch for PR {_pr_link(pr)}.[/]")
        return

    if action == TriageAction.COMMENT:
        if not _confirm_action(pr, "Post comment", ctx.answer_triage):
            stats.total_skipped_action += 1
            return
        text = comment_only_text or draft_comment
        console_print(f"  Posting comment on PR {_pr_link(pr)}...")
        if _post_comment(ctx.token, pr.node_id, text):
            console_print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
            stats.total_commented += 1
        else:
            console_print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
        return

    if action == TriageAction.DRAFT:
        if not _confirm_action(pr, "Convert to draft and post comment", ctx.answer_triage):
            stats.total_skipped_action += 1
            return
        console_print(f"  Converting PR {_pr_link(pr)} to draft...")
        if _convert_pr_to_draft(ctx.token, pr.node_id):
            console_print(f"  [success]PR {_pr_link(pr)} converted to draft.[/]")
        else:
            console_print(f"  [error]Failed to convert PR {_pr_link(pr)} to draft.[/]")
            return
        console_print(f"  Posting comment on PR {_pr_link(pr)}...")
        if _post_comment(ctx.token, pr.node_id, draft_comment):
            console_print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
            stats.total_converted += 1
        else:
            console_print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
        return

    if action == TriageAction.PING:
        if not pr.unresolved_threads:
            get_console().print(
                f"  [warning]No unresolved threads to ping reviewers about on PR {_pr_link(pr)}.[/]"
            )
            return
        reviewer_logins = sorted({t.reviewer_login for t in pr.unresolved_threads})
        mentions = ", ".join(f"@{login}" for login in reviewer_logins)
        ping_body = (
            f"{mentions} — Could you please check whether your review feedback on this PR "
            f"has been addressed? @{pr.author_login} appears to have responded to your comments. "
            f"@{pr.author_login}, do you believe the reviewer's concerns have been resolved?\n\n"
            f"If the concerns are resolved, please resolve the conversation threads. Thank you!"
        )
        if not _confirm_action(pr, f"Ping reviewer(s): {mentions}", ctx.answer_triage):
            stats.total_skipped_action += 1
            return
        get_console().print(f"  Pinging reviewer(s) on PR {_pr_link(pr)}...")
        if _post_comment(ctx.token, pr.node_id, ping_body):
            get_console().print(f"  [success]Ping comment posted on PR {_pr_link(pr)}.[/]")
            stats.total_commented += 1
        else:
            get_console().print(f"  [error]Failed to post ping comment on PR {_pr_link(pr)}.[/]")
        return

    if action == TriageAction.CLOSE:
        if not _confirm_action(pr, "Close PR and post comment", ctx.answer_triage):
            stats.total_skipped_action += 1
            return
        console_print(f"  Closing PR {_pr_link(pr)}...")
        if _close_pr(ctx.token, pr.node_id):
            console_print(f"  [success]PR {_pr_link(pr)} closed.[/]")
        else:
            console_print(f"  [error]Failed to close PR {_pr_link(pr)}.[/]")
            return
        if _add_label(ctx.token, ctx.github_repository, pr.node_id, _CLOSED_QUALITY_LABEL):
            console_print(f"  [success]Label '{_CLOSED_QUALITY_LABEL}' added to PR {_pr_link(pr)}.[/]")
        else:
            console_print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
        console_print(f"  Posting comment on PR {_pr_link(pr)}...")
        if _post_comment(ctx.token, pr.node_id, close_comment):
            console_print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
            stats.total_closed += 1
        else:
            console_print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")


def _prompt_and_execute_flagged_pr(
    ctx: TriageContext,
    pr: PRData,
    assessment,
    *,
    comment_only_text: str | None = None,
) -> None:
    """Display a flagged PR panel, prompt user for action, and execute it. Mutates ctx.stats."""
    author_profile = _fetch_author_profile(ctx.token, pr.author_login, ctx.github_repository)

    _display_pr_panel(pr, author_profile, assessment)

    # Display log snippets from failed CI checks (fetched in background)
    if pr.failed_checks and pr.head_sha:
        log_future = ctx.log_futures.get(pr.number)
        if log_future is not None:
            if log_future.done():
                log_snippets = log_future.result()
                if log_snippets:
                    _display_log_snippets_panel(log_snippets)
            else:
                # Logs are still being fetched — offer user to wait or cancel
                from airflow_breeze.utils.confirm import _read_char

                get_console().print("  [dim]CI failure logs are still being fetched in the background...[/]")
                get_console().print(
                    "  Press any key to [bold]wait[/] or [bold]\\[c]ancel[/] log retrieval for this PR: ",
                    end="",
                )
                try:
                    ch = _read_char()
                except (KeyboardInterrupt, EOFError):
                    ch = "c"
                get_console().print(ch if len(ch) == 1 else "")

                if ch.lower() != "c":
                    get_console().print("  [dim]Waiting for CI logs...[/]")
                    try:
                        log_snippets = log_future.result(timeout=120)
                        if log_snippets:
                            _display_log_snippets_panel(log_snippets)
                    except TimeoutError:
                        get_console().print("  [warning]CI log retrieval timed out.[/]")
                    except Exception:
                        get_console().print("  [warning]CI log retrieval failed.[/]")
                else:
                    get_console().print("  [dim]Skipping CI log display for this PR.[/]")
        else:
            # No background future — fetch inline as fallback
            log_snippets = _fetch_failed_job_log_snippets(
                ctx.token, ctx.github_repository, pr.head_sha, pr.failed_checks
            )
            if log_snippets:
                _display_log_snippets_panel(log_snippets)

    # Check if PR failures match main branch failures
    main_matching: list[str] = []
    if ctx.main_failures and pr.failed_checks:
        main_matching = ctx.main_failures.find_matching_failures(pr.failed_checks)
        if main_matching:
            _display_recent_pr_failure_panel(pr, ctx.main_failures, main_matching)

    default_action, reason = _compute_default_action(
        pr, assessment, ctx.author_flagged_count, ctx.main_failures
    )
    # If PR is already a draft, don't offer converting to draft — use comment instead
    exclude_actions: set[TriageAction] = set()
    if pr.is_draft and default_action == TriageAction.DRAFT:
        default_action = TriageAction.COMMENT
        reason = reason.replace("draft", "comment (already draft)")
        exclude_actions.add(TriageAction.DRAFT)
    elif pr.is_draft:
        exclude_actions.add(TriageAction.DRAFT)
    # Only offer PING when there are unresolved review threads
    if not pr.unresolved_threads:
        exclude_actions.add(TriageAction.PING)

    console_print(f"  [bold]{reason}[/]")

    if ctx.dry_run:
        action_label = {
            TriageAction.DRAFT: "draft",
            TriageAction.COMMENT: "comment",
            TriageAction.CLOSE: "close",
            TriageAction.RERUN: "rerun checks",
            TriageAction.REBASE: "rebase",
            TriageAction.PING: "ping reviewer",
            TriageAction.READY: "mark as ready",
            TriageAction.SKIP: "skip",
        }.get(default_action, str(default_action))
        console_print(f"[warning]Dry run — would default to: {action_label}[/]")
        return

    action = prompt_triage_action(
        f"Action for PR {_pr_link(pr)}?",
        default=default_action,
        forced_answer=ctx.answer_triage,
        exclude=exclude_actions,
        pr_url=pr.url,
        token=ctx.token,
        github_repository=ctx.github_repository,
        pr_number=pr.number,
    )

    if action == TriageAction.QUIT:
        console_print("[warning]Quitting.[/]")
        ctx.stats.quit_early = True
        return

    # If user takes action on a should_report PR (anything other than skip),
    # downgrade it from "report" to regular "flagged" — user has reviewed and decided.
    if action != TriageAction.SKIP and getattr(assessment, "should_report", False):
        assessment.should_report = False
        console_print("  [info]Report status cleared — PR marked as flagged.[/]")

    # For actions that post comments, let the user select violations and preview the comment
    draft_comment = ""
    close_comment = ""
    if action in (TriageAction.DRAFT, TriageAction.COMMENT, TriageAction.CLOSE):
        selected = _select_violations(assessment.violations)
        if selected is not None and len(selected) == 0:
            console_print("  [info]No violations selected — skipping.[/]")
            action = TriageAction.SKIP
        else:
            violations = selected if selected is not None else assessment.violations
            is_collab = pr.author_association in _COLLABORATOR_ASSOCIATIONS
            draft_comment = _build_comment(
                pr.author_login,
                violations,
                pr.number,
                pr.commits_behind,
                pr.base_ref,
                is_collaborator=is_collab,
            )
            close_comment = _build_close_comment(
                pr.author_login,
                violations,
                pr.number,
                ctx.author_flagged_count.get(pr.author_login, 0),
                is_collaborator=is_collab,
            )
            comment_only_text = _build_comment(
                pr.author_login,
                violations,
                pr.number,
                pr.commits_behind,
                pr.base_ref,
                comment_only=True,
                is_collaborator=is_collab,
            )
            # Show the final comment that will be posted
            if action == TriageAction.CLOSE:
                console_print(Panel(close_comment, title="Comment to be posted", border_style="red"))
            elif action == TriageAction.COMMENT:
                console_print(Panel(comment_only_text, title="Comment to be posted", border_style="green"))
            else:
                console_print(Panel(draft_comment, title="Comment to be posted", border_style="green"))

    _execute_triage_action(
        ctx,
        pr,
        action,
        draft_comment=draft_comment,
        close_comment=close_comment,
        comment_only_text=comment_only_text,
    )


def _display_pr_overview_table(
    all_prs: list[PRData],
    *,
    triaged_waiting_nums: set[int] | None = None,
    triaged_responded_nums: set[int] | None = None,
) -> None:
    """Display a Rich table overview of non-collaborator PRs."""
    commented = triaged_waiting_nums or set()
    responded = triaged_responded_nums or set()
    non_collab_prs = [pr for pr in all_prs if pr.author_association not in _COLLABORATOR_ASSOCIATIONS]
    collab_count = len(all_prs) - len(non_collab_prs)
    pr_table = Table(title=f"Fetched PRs ({len(non_collab_prs)} non-collaborator)")
    pr_table.add_column("PR", style="cyan", no_wrap=True)
    pr_table.add_column("Triage", no_wrap=True)
    pr_table.add_column("Status")
    pr_table.add_column("Title", max_width=50)
    pr_table.add_column("Author")
    pr_table.add_column("Behind", justify="right")
    pr_table.add_column("Conflicts")
    pr_table.add_column("CI Status")
    pr_table.add_column("Workflows")
    for pr in non_collab_prs:
        if pr.checks_state == "FAILURE":
            ci_status = "[red]Failing[/]"
        elif pr.checks_state == "PENDING":
            ci_status = "[yellow]Pending[/]"
        elif pr.checks_state in ("UNKNOWN", "NOT_RUN"):
            ci_status = "[yellow]Not run[/]"
        else:
            ci_status = f"[green]{pr.checks_state.capitalize()}[/]"
        behind_text = f"[yellow]{pr.commits_behind}[/]" if pr.commits_behind > 0 else "[green]0[/]"
        if pr.mergeable == "CONFLICTING":
            conflicts_text = "[red]Yes[/]"
        elif pr.mergeable == "UNKNOWN":
            conflicts_text = "[dim]?[/]"
        else:
            conflicts_text = "[green]No[/]"

        # Workflow status
        if pr.checks_state == "NOT_RUN":
            workflows_text = "[yellow]Needs run[/]"
        elif pr.checks_state == "PENDING":
            workflows_text = "[yellow]Running[/]"
        else:
            workflows_text = "[green]Done[/]"

        has_issues = pr.checks_state == "FAILURE" or pr.mergeable == "CONFLICTING"
        if pr.is_draft:
            overall = "[yellow]Draft[/]"
        elif has_issues:
            overall = "[red]Flag[/]"
        else:
            overall = "[green]OK[/]"

        # Triage status column
        if _READY_FOR_REVIEW_LABEL in pr.labels:
            triage_status = "[green]Ready for review[/]"
        elif pr.number in commented or pr.is_draft:
            triage_status = "[yellow]Waiting for Author[/]"
        elif pr.number in responded:
            triage_status = "[bright_cyan]Responded[/]"
        else:
            triage_status = "[blue]-[/]"

        pr_table.add_row(
            _pr_link(pr),
            triage_status,
            overall,
            pr.title[:50],
            pr.author_login,
            behind_text,
            conflicts_text,
            ci_status,
            workflows_text,
        )
    console_print(pr_table)
    console_print(
        "  Triage: [green]Ready for review[/] = ready for maintainer review  "
        "[yellow]Waiting for Author[/] = triaged, no response  "
        "[bright_cyan]Responded[/] = author replied  "
        "[blue]-[/] = not yet triaged"
    )
    if collab_count:
        console_print(
            f"  [dim]({collab_count} collaborator/member {'PRs' if collab_count != 1 else 'PR'} not shown)[/]"
        )
    console_print()


def _filter_candidate_prs(
    all_prs: list[PRData],
    *,
    include_collaborators: bool,
    include_drafts: bool,
    checks_state: str,
    min_commits_behind: int,
    max_num: int,
    also_accepted: set[int] | None = None,
) -> tuple[list[PRData], list[PRData], int, int, int]:
    """Filter PRs to candidates. Returns (candidates, accepted_prs, skipped_collaborator, skipped_bot, skipped_accepted).

    :param also_accepted: additional PR numbers to treat as accepted (e.g. PRs reviewed by the user).
    """
    candidate_prs: list[PRData] = []
    accepted_prs: list[PRData] = []
    total_skipped_collaborator = 0
    total_skipped_bot = 0
    total_skipped_accepted = 0
    total_skipped_checks_state = 0
    total_skipped_commits_behind = 0
    total_skipped_drafts = 0
    verbose = get_verbose()
    for pr in all_prs:
        if not include_drafts and pr.is_draft:
            total_skipped_drafts += 1
            if verbose:
                console_print(f"  [dim]Skipping PR {_pr_link(pr)} — draft PR[/]")
        elif not include_collaborators and pr.author_association in _COLLABORATOR_ASSOCIATIONS:
            total_skipped_collaborator += 1
            if verbose:
                console_print(
                    f"  [dim]Skipping PR {_pr_link(pr)} by "
                    f"{pr.author_association.lower()} {pr.author_login}[/]"
                )
        elif _is_bot_account(pr.author_login):
            total_skipped_bot += 1
            if verbose:
                console_print(f"  [dim]Skipping PR {_pr_link(pr)} — bot account {pr.author_login}[/]")
        elif _READY_FOR_REVIEW_LABEL in pr.labels or (also_accepted and pr.number in also_accepted):
            total_skipped_accepted += 1
            accepted_prs.append(pr)
            if verbose:
                reason = (
                    f"already has '{_READY_FOR_REVIEW_LABEL}' label"
                    if _READY_FOR_REVIEW_LABEL in pr.labels
                    else "previously reviewed by reviewer"
                )
                console_print(f"  [dim]Skipping PR {_pr_link(pr)} — {reason}[/]")
        elif (
            checks_state != "any"
            and pr.checks_state not in ("NOT_RUN",)
            and pr.checks_state.lower() != checks_state
        ):
            total_skipped_checks_state += 1
            if verbose:
                console_print(
                    f"  [dim]Skipping PR {_pr_link(pr)} — checks state {pr.checks_state} != {checks_state}[/]"
                )
        elif min_commits_behind > 0 and pr.commits_behind < min_commits_behind:
            total_skipped_commits_behind += 1
            if verbose:
                console_print(
                    f"  [dim]Skipping PR {_pr_link(pr)} — only "
                    f"{pr.commits_behind} commits behind (min: {min_commits_behind})[/]"
                )
        else:
            candidate_prs.append(pr)

    if max_num and len(candidate_prs) > max_num:
        candidate_prs = candidate_prs[:max_num]

    skipped_parts: list[str] = []
    if total_skipped_collaborator:
        skipped_parts.append(
            f"{total_skipped_collaborator} "
            f"{'collaborators' if total_skipped_collaborator != 1 else 'collaborator'}"
        )
    if total_skipped_drafts:
        skipped_parts.append(f"{total_skipped_drafts} {'drafts' if total_skipped_drafts != 1 else 'draft'}")
    if total_skipped_bot:
        skipped_parts.append(f"{total_skipped_bot} {'bots' if total_skipped_bot != 1 else 'bot'}")
    if total_skipped_accepted:
        skipped_parts.append(f"{total_skipped_accepted} ready-for-review")
    if total_skipped_checks_state:
        skipped_parts.append(f"{total_skipped_checks_state} checks-state mismatch")
    if total_skipped_commits_behind:
        skipped_parts.append(f"{total_skipped_commits_behind} below min-commits-behind")
    skipped_text = f"skipped {', '.join(skipped_parts)}, " if skipped_parts else ""
    console_print(
        f"\n[info]Fetched {len(all_prs)} {'PRs' if len(all_prs) != 1 else 'PR'}, "
        f"{skipped_text}"
        f"assessing {len(candidate_prs)} {'PRs' if len(candidate_prs) != 1 else 'PR'}"
        f"{f' (capped at {max_num})' if max_num else ''}...[/]\n"
    )
    return candidate_prs, accepted_prs, total_skipped_collaborator, total_skipped_bot, total_skipped_accepted


def _enrich_candidate_details(
    token: str, github_repository: str, candidate_prs: list[PRData], *, run_api: bool
) -> None:
    """Fetch check details, resolve unknown mergeable status, and fetch review comments."""
    if not candidate_prs:
        return

    console_print(
        f"[info]Fetching check details for {len(candidate_prs)} "
        f"candidate {'PRs' if len(candidate_prs) != 1 else 'PR'}...[/]"
    )
    _fetch_check_details_batch(token, github_repository, candidate_prs)

    for pr in candidate_prs:
        if pr.checks_state == "FAILURE" and not pr.failed_checks and pr.head_sha:
            console_print(
                f"  [dim]Fetching full check details for PR {_pr_link(pr)} "
                f"(failures beyond first 100 checks)...[/]"
            )
            pr.failed_checks = _fetch_failed_checks(token, github_repository, pr.head_sha)

    unknown_count = sum(1 for pr in candidate_prs if pr.mergeable == "UNKNOWN")
    if unknown_count:
        console_print(
            f"[info]Resolving merge conflict status for {unknown_count} "
            f"{'PRs' if unknown_count != 1 else 'PR'} with unknown status...[/]"
        )
        resolved = _resolve_unknown_mergeable(token, github_repository, candidate_prs)
        remaining = unknown_count - resolved
        if remaining:
            console_print(
                f"  [dim]{resolved} resolved, {remaining} still unknown "
                f"(GitHub hasn't computed mergeability yet).[/]"
            )
        else:
            console_print(f"  [dim]All {resolved} resolved.[/]")

    if run_api:
        console_print(
            f"[info]Fetching review thread details for {len(candidate_prs)} "
            f"candidate {'PRs' if len(candidate_prs) != 1 else 'PR'}...[/]"
        )
        _fetch_unresolved_comments_batch(token, github_repository, candidate_prs)


def _review_workflow_approval_prs(ctx: TriageContext, pending_approval: list[PRData]) -> None:
    """Present NOT_RUN PRs for workflow approval. Mutates ctx.stats."""
    if ctx.stats.quit_early or not pending_approval:
        return

    pending_approval.sort(key=lambda p: (p.author_login.lower(), p.number))
    draft_count = sum(1 for pr in pending_approval if pr.is_draft)
    draft_note = f" ({draft_count} draft)" if draft_count else ""
    console_print(
        f"\n[info]{len(pending_approval)} {'PRs' if len(pending_approval) != 1 else 'PR'}{draft_note} "
        f"need workflow approval — review and approve workflow runs"
        f"{' (LLM assessments running in background)' if ctx.llm_future_to_pr else ''}:[/]\n"
    )

    for pr in pending_approval:
        if ctx.stats.quit_early:
            return
        ctx.collect_llm_progress()

        author_profile = _fetch_author_profile(ctx.token, pr.author_login, ctx.github_repository)
        pending_runs = _find_pending_workflow_runs(ctx.token, ctx.github_repository, pr.head_sha)

        # Fetch check status counts for display and running-checks detection
        check_counts: dict[str, int] = {}
        if pr.head_sha:
            check_counts = _fetch_check_status_counts(ctx.token, ctx.github_repository, pr.head_sha)
            if _has_running_checks(check_counts):
                console_print(
                    f"  [dim]Skipping PR {_pr_link(pr)} — checks still running "
                    f"({_format_check_status_counts(check_counts)})[/]"
                )
                continue

        _display_workflow_approval_panel(pr, author_profile, pending_runs, check_counts)

        # If author exceeds the close threshold, suggest closing instead of approving
        author_count = ctx.author_flagged_count.get(pr.author_login, 0)
        if author_count > 3:
            console_print(
                f"  [bold red]Author {pr.author_login} has {author_count} flagged "
                f"{'PRs' if author_count != 1 else 'PR'} "
                f"— suggesting close instead of workflow approval.[/]"
            )
            close_comment = _build_close_comment(
                pr.author_login,
                [],
                pr.number,
                author_count,
                is_collaborator=pr.author_association in _COLLABORATOR_ASSOCIATIONS,
            )
            console_print(Panel(close_comment, title="Proposed close comment", border_style="red"))

            if ctx.dry_run:
                console_print("[warning]Dry run — would default to: close[/]")
                continue

            action = prompt_triage_action(
                f"Action for PR {_pr_link(pr)}?",
                default=TriageAction.CLOSE,
                forced_answer=ctx.answer_triage,
                exclude={TriageAction.DRAFT} if pr.is_draft else None,
                pr_url=pr.url,
                token=ctx.token,
                github_repository=ctx.github_repository,
                pr_number=pr.number,
            )
            if action == TriageAction.QUIT:
                console_print("[warning]Quitting.[/]")
                ctx.stats.quit_early = True
                return
            if action == TriageAction.SKIP:
                console_print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
                continue
            if action == TriageAction.CLOSE:
                _execute_triage_action(
                    ctx, pr, TriageAction.CLOSE, draft_comment="", close_comment=close_comment
                )
                continue
            # For DRAFT or READY, fall through to normal workflow approval

        if ctx.dry_run:
            console_print("[warning]Dry run — skipping workflow approval.[/]")
            continue

        if not pending_runs:
            # No pending workflow runs — try to rerun completed workflows first.
            # If no workflows exist at all, fall back to rebase (or close/reopen).
            console_print(
                f"  [info]No pending workflow runs for PR {_pr_link(pr)}. "
                f"Attempting to rerun completed workflows...[/]"
            )
            default_action = TriageAction.RERUN
            console_print("  [bold]No pending runs — suggesting rerun checks[/]")

            action = prompt_triage_action(
                f"Action for PR {_pr_link(pr)}?",
                default=default_action,
                forced_answer=ctx.answer_triage,
                exclude={TriageAction.DRAFT} if pr.is_draft else None,
                pr_url=pr.url,
                token=ctx.token,
                github_repository=ctx.github_repository,
                pr_number=pr.number,
            )
            if action == TriageAction.QUIT:
                console_print("[warning]Quitting.[/]")
                ctx.stats.quit_early = True
                return
            if action == TriageAction.SKIP:
                console_print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
                continue

            if action == TriageAction.RERUN:
                # Try to find and rerun any completed workflow runs for this SHA
                rerun_count = 0
                if pr.head_sha:
                    completed_runs = _find_workflow_runs_by_status(
                        ctx.token, ctx.github_repository, pr.head_sha, "completed"
                    )
                    if completed_runs:
                        for run in completed_runs:
                            if _rerun_workflow_run(ctx.token, ctx.github_repository, run):
                                console_print(
                                    f"  [success]Rerun triggered for: {run.get('name', run['id'])}[/]"
                                )
                                rerun_count += 1

                if rerun_count:
                    console_print(
                        f"  [success]Rerun triggered for {rerun_count} workflow "
                        f"{'runs' if rerun_count != 1 else 'run'} on PR {_pr_link(pr)}.[/]"
                    )
                    ctx.stats.total_rerun += 1
                else:
                    # No workflows to rerun — need rebase or close/reopen to trigger CI
                    console_print(f"  [warning]No workflow runs found to rerun for PR {_pr_link(pr)}.[/]")
                    if pr.mergeable == "CONFLICTING":
                        console_print("  [warning]PR has merge conflicts — suggesting close/reopen.[/]")
                        rebase_comment = (
                            f"@{pr.author_login} This PR has no workflow runs and has "
                            f"**merge conflicts**.\n\n"
                            "Please close this PR, resolve the conflicts by rebasing onto "
                            f"the latest `{pr.base_ref}` branch, and reopen.\n\n"
                            "If you need help rebasing, see our "
                            "[contributor guide](https://github.com/apache/airflow/blob/main/"
                            "contributing-docs/05_pull_requests.rst)."
                        )
                    else:
                        console_print("  [info]Suggesting rebase to trigger CI workflows.[/]")
                        rebase_comment = f"@{pr.author_login} This PR has no workflow runs."
                        if pr.commits_behind > 0:
                            rebase_comment += (
                                f" The PR is **{pr.commits_behind} "
                                f"commit{'s' if pr.commits_behind != 1 else ''} "
                                f"behind `{pr.base_ref}`**."
                            )
                        rebase_comment += (
                            "\n\nPlease **rebase** your branch onto the latest base branch "
                            "and push again so that CI workflows can run.\n\n"
                            "If you need help rebasing, see our "
                            "[contributor guide](https://github.com/apache/airflow/blob/main/"
                            "contributing-docs/05_pull_requests.rst)."
                        )
                    console_print(
                        Panel(rebase_comment, title="Proposed rebase comment", border_style="yellow")
                    )
                    fallback_action = TriageAction.DRAFT if not pr.is_draft else TriageAction.COMMENT
                    fallback = prompt_triage_action(
                        f"Rerun failed — action for PR {_pr_link(pr)}?",
                        default=fallback_action,
                        forced_answer=ctx.answer_triage,
                        exclude={TriageAction.DRAFT} if pr.is_draft else None,
                        pr_url=pr.url,
                        token=ctx.token,
                        github_repository=ctx.github_repository,
                        pr_number=pr.number,
                    )
                    if fallback == TriageAction.QUIT:
                        console_print("[warning]Quitting.[/]")
                        ctx.stats.quit_early = True
                        return
                    if fallback == TriageAction.SKIP:
                        console_print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
                    elif fallback == TriageAction.CLOSE:
                        close_comment = _build_close_comment(
                            pr.author_login,
                            [],
                            pr.number,
                            0,
                            is_collaborator=pr.author_association in _COLLABORATOR_ASSOCIATIONS,
                        )
                        _execute_triage_action(
                            ctx, pr, TriageAction.CLOSE, draft_comment="", close_comment=close_comment
                        )
                    elif fallback == TriageAction.DRAFT:
                        draft_comment = rebase_comment + (
                            "\n\nConverting this PR to **draft** until it is rebased."
                        )
                        _execute_triage_action(
                            ctx, pr, TriageAction.DRAFT, draft_comment=draft_comment, close_comment=""
                        )
                    elif fallback == TriageAction.COMMENT:
                        _execute_triage_action(
                            ctx,
                            pr,
                            TriageAction.COMMENT,
                            draft_comment="",
                            close_comment="",
                            comment_only_text=rebase_comment,
                        )
            elif action == TriageAction.CLOSE:
                close_comment = _build_close_comment(
                    pr.author_login,
                    [],
                    pr.number,
                    0,
                    is_collaborator=pr.author_association in _COLLABORATOR_ASSOCIATIONS,
                )
                _execute_triage_action(
                    ctx, pr, TriageAction.CLOSE, draft_comment="", close_comment=close_comment
                )
            elif action == TriageAction.DRAFT:
                rebase_comment = f"@{pr.author_login} This PR has no workflow runs."
                if pr.commits_behind > 0:
                    rebase_comment += (
                        f" The PR is **{pr.commits_behind} "
                        f"commit{'s' if pr.commits_behind != 1 else ''} "
                        f"behind `{pr.base_ref}`**."
                    )
                rebase_comment += (
                    "\n\nPlease **rebase** your branch onto the latest base branch "
                    "and push again so that CI workflows can run.\n\n"
                    "If you need help rebasing, see our "
                    "[contributor guide](https://github.com/apache/airflow/blob/main/"
                    "contributing-docs/05_pull_requests.rst).\n\n"
                    "Converting this PR to **draft** until it is rebased."
                )
                _execute_triage_action(
                    ctx, pr, TriageAction.DRAFT, draft_comment=rebase_comment, close_comment=""
                )
            elif action == TriageAction.COMMENT:
                rebase_comment = f"@{pr.author_login} This PR has no workflow runs."
                if pr.commits_behind > 0:
                    rebase_comment += (
                        f" The PR is **{pr.commits_behind} "
                        f"commit{'s' if pr.commits_behind != 1 else ''} "
                        f"behind `{pr.base_ref}`**."
                    )
                rebase_comment += (
                    "\n\nPlease **rebase** your branch onto the latest base branch "
                    "and push again so that CI workflows can run.\n\n"
                    "If you need help rebasing, see our "
                    "[contributor guide](https://github.com/apache/airflow/blob/main/"
                    "contributing-docs/05_pull_requests.rst)."
                )
                _execute_triage_action(
                    ctx,
                    pr,
                    TriageAction.COMMENT,
                    draft_comment="",
                    close_comment="",
                    comment_only_text=rebase_comment,
                )
            continue

        answer = user_confirm(
            f"Review diff for PR {_pr_link(pr)} before approving workflows?",
            default_answer=Answer.YES,
            forced_answer=ctx.answer_triage,
        )
        if answer == Answer.QUIT:
            console_print("[warning]Quitting.[/]")
            ctx.stats.quit_early = True
            return
        if answer == Answer.NO:
            console_print(f"  [info]Skipping workflow approval for PR {_pr_link(pr)}.[/]")
            continue

        has_sensitive_changes = False
        console_print(f"  Fetching diff for PR {_pr_link(pr)}...")
        diff_text = _fetch_pr_diff(ctx.token, ctx.github_repository, pr.number)
        if diff_text:
            from rich.syntax import Syntax

            console_print(
                Panel(
                    Syntax(diff_text, "diff", theme="monokai", word_wrap=True),
                    title=f"Diff for PR {_pr_link(pr)}",
                    border_style="bright_cyan",
                )
            )

            # Warn about changes to sensitive directories (.github/, scripts/)
            sensitive_files = _detect_sensitive_file_changes(diff_text)
            if sensitive_files:
                has_sensitive_changes = True
                console_print()
                console_print(
                    "[bold red]WARNING: This PR contains changes to sensitive files "
                    "— please review carefully![/]"
                )
                for f in sensitive_files:
                    console_print(f"  [bold red]  - {f}[/]")
                console_print()
        else:
            console_print(
                f"  [warning]Could not fetch diff for PR {_pr_link(pr)}. "
                f"Review manually at: {pr.url}/files[/]"
            )

        approve_default = Answer.NO if has_sensitive_changes else Answer.YES
        answer = user_confirm(
            f"No suspicious changes found in PR {_pr_link(pr)}? "
            f"Approve {len(pending_runs)} workflow {'runs' if len(pending_runs) != 1 else 'run'}?",
            default_answer=approve_default,
            forced_answer=ctx.answer_triage,
        )
        if answer == Answer.QUIT:
            console_print("[warning]Quitting.[/]")
            ctx.stats.quit_early = True
            return
        if answer == Answer.NO:
            console_print(
                f"\n  [bold red]Suspicious changes detected in PR {_pr_link(pr)} by {pr.author_login}.[/]"
            )
            console_print(f"  Fetching all open PRs by {pr.author_login}...")
            author_prs = _fetch_author_open_prs(ctx.token, ctx.github_repository, pr.author_login)
            if not author_prs:
                console_print(f"  [dim]No open PRs found for {pr.author_login}.[/]")
                continue

            console_print()
            console_print(
                f"  [bold red]The following {len(author_prs)} "
                f"{'PRs' if len(author_prs) != 1 else 'PR'} by "
                f"{pr.author_login} will be closed, labeled "
                f"'{_SUSPICIOUS_CHANGES_LABEL}', and commented:[/]"
            )
            for pr_info in author_prs:
                console_print(f"    - [link={pr_info['url']}]#{pr_info['number']}[/link] {pr_info['title']}")
            console_print()

            confirm = user_confirm(
                f"Close all {len(author_prs)} {'PRs' if len(author_prs) != 1 else 'PR'} "
                f"by {pr.author_login} and label as suspicious?",
                forced_answer=ctx.answer_triage,
            )
            if confirm == Answer.QUIT:
                console_print("[warning]Quitting.[/]")
                ctx.stats.quit_early = True
                return
            if confirm == Answer.NO:
                console_print(f"  [info]Skipping — no PRs closed for {pr.author_login}.[/]")
                continue

            closed, commented = _close_suspicious_prs(ctx.token, ctx.github_repository, author_prs, pr.number)
            console_print(
                f"  [success]Closed {closed}/{len(author_prs)} "
                f"{'PRs' if len(author_prs) != 1 else 'PR'}, commented on {commented}.[/]"
            )
            ctx.stats.total_closed += closed
            continue

        approved = _approve_workflow_runs(ctx.token, ctx.github_repository, pending_runs)
        if approved:
            console_print(
                f"  [success]Approved {approved}/{len(pending_runs)} workflow "
                f"{'runs' if len(pending_runs) != 1 else 'run'} for PR "
                f"{_pr_link(pr)}.[/]"
            )
            ctx.stats.total_workflows_approved += 1
        else:
            console_print(f"  [error]Failed to approve workflow runs for PR {_pr_link(pr)}.[/]")
            # Approval failed (likely 403 — runs are too old). Suggest converting to draft
            # with a rebase comment so the author pushes fresh commits.
            if pr.is_draft:
                # Already a draft — just add a comment, no need to convert
                rebase_comment = (
                    f"@{pr.author_login} This PR has workflow runs awaiting approval that could "
                    f"not be approved (they are likely too old). The PR is "
                    f"**{pr.commits_behind} commits behind `{pr.base_ref}`**.\n\n"
                    "Please **rebase** your branch onto the latest base branch and push again "
                    "so that fresh CI workflows can run.\n\n"
                    "If you need help rebasing, see our "
                    "[contributor guide](https://github.com/apache/airflow/blob/main/"
                    "contributing-docs/05_pull_requests.rst)."
                )
                default_action = TriageAction.COMMENT
                exclude_actions = {TriageAction.DRAFT}
            else:
                rebase_comment = (
                    f"@{pr.author_login} This PR has workflow runs awaiting approval that could "
                    f"not be approved (they are likely too old). The PR is "
                    f"**{pr.commits_behind} commits behind `{pr.base_ref}`**.\n\n"
                    "Please **rebase** your branch onto the latest base branch and push again "
                    "so that fresh CI workflows can run.\n\n"
                    "If you need help rebasing, see our "
                    "[contributor guide](https://github.com/apache/airflow/blob/main/"
                    "contributing-docs/05_pull_requests.rst).\n\n"
                    "Converting this PR to **draft** until it is rebased."
                )
                default_action = TriageAction.DRAFT
                exclude_actions = None
            console_print(Panel(rebase_comment, title="Proposed rebase comment", border_style="yellow"))
            action = prompt_triage_action(
                f"Action for PR {_pr_link(pr)}?",
                default=default_action,
                forced_answer=ctx.answer_triage,
                exclude=exclude_actions,
                pr_url=pr.url,
                token=ctx.token,
                github_repository=ctx.github_repository,
                pr_number=pr.number,
            )
            if action == TriageAction.QUIT:
                console_print("[warning]Quitting.[/]")
                ctx.stats.quit_early = True
                return
            if action == TriageAction.SKIP:
                _execute_triage_action(ctx, pr, TriageAction.SKIP, draft_comment="", close_comment="")
            elif action == TriageAction.COMMENT:
                _execute_triage_action(
                    ctx,
                    pr,
                    TriageAction.COMMENT,
                    draft_comment="",
                    close_comment="",
                    comment_only_text=rebase_comment,
                )
            elif action == TriageAction.DRAFT:
                _execute_triage_action(
                    ctx, pr, TriageAction.DRAFT, draft_comment=rebase_comment, close_comment=""
                )
            elif action == TriageAction.CLOSE:
                close_comment = _build_close_comment(
                    pr.author_login,
                    [],
                    pr.number,
                    0,
                    is_collaborator=pr.author_association in _COLLABORATOR_ASSOCIATIONS,
                )
                _execute_triage_action(
                    ctx, pr, TriageAction.CLOSE, draft_comment="", close_comment=close_comment
                )


def _review_deterministic_flagged_prs(
    ctx: TriageContext, det_flagged_prs: list[tuple[PRData, PRAssessment]]
) -> None:
    """Present deterministically flagged PRs for interactive review. Mutates ctx.stats."""
    if not det_flagged_prs:
        return

    console_print(
        f"\n[info]Reviewing {len(det_flagged_prs)} deterministically flagged "
        f"{'PRs' if len(det_flagged_prs) != 1 else 'PR'}"
        f"{' (LLM assessments running in background)' if ctx.llm_future_to_pr else ''}...[/]\n"
    )

    current_author: str | None = None
    for pr, assessment in det_flagged_prs:
        if ctx.stats.quit_early:
            break

        ctx.collect_llm_progress()

        if pr.author_login != current_author:
            current_author = pr.author_login
            count = ctx.author_flagged_count[current_author]
            console_print()
            get_console().rule(
                f"[bold]Author: {current_author}[/] ({count} flagged PR{'s' if count != 1 else ''})",
                style="cyan",
            )

        _prompt_and_execute_flagged_pr(ctx, pr, assessment)


def _review_llm_flagged_prs(ctx: TriageContext, llm_candidates: list[PRData]) -> None:
    """Present LLM-flagged PRs as they become ready, without blocking. Mutates ctx.stats."""
    if ctx.stats.quit_early or not ctx.llm_future_to_pr:
        return

    _collect_llm_results(
        ctx.llm_future_to_pr, ctx.llm_assessments, ctx.llm_completed, ctx.llm_errors, ctx.llm_passing
    )

    llm_presented: set[int] = set()

    while not ctx.stats.quit_early:
        new_flagged = [
            (pr, ctx.llm_assessments[pr.number])
            for pr in llm_candidates
            if pr.number in ctx.llm_assessments and pr.number not in llm_presented
        ]
        # should_report PRs first (0 sorts before 1), then by author, then PR number
        new_flagged.sort(
            key=lambda pair: (
                0 if pair[1].should_report else 1,
                pair[0].author_login.lower(),
                pair[0].number,
            )
        )

        if new_flagged:
            remaining = len(ctx.llm_future_to_pr) - len(ctx.llm_completed)
            status_parts = [f"{len(ctx.llm_completed)}/{len(ctx.llm_future_to_pr)} done"]
            if remaining:
                status_parts.append(f"{remaining} still running")
            console_print(
                f"\n[info]{len(new_flagged)} new LLM-flagged "
                f"{'PRs' if len(new_flagged) != 1 else 'PR'} ready for review "
                f"({', '.join(status_parts)}):[/]\n"
            )

            for pr, assessment in new_flagged:
                if ctx.stats.quit_early:
                    break
                llm_presented.add(pr.number)
                ctx.author_flagged_count[pr.author_login] = (
                    ctx.author_flagged_count.get(pr.author_login, 0) + 1
                )

                _prompt_and_execute_flagged_pr(ctx, pr, assessment)

                # While user was deciding, more results may have arrived
                _collect_llm_results(
                    ctx.llm_future_to_pr,
                    ctx.llm_assessments,
                    ctx.llm_completed,
                    ctx.llm_errors,
                    ctx.llm_passing,
                )

        if len(ctx.llm_completed) >= len(ctx.llm_future_to_pr):
            break

        console_print(
            f"[dim]Waiting for {len(ctx.llm_future_to_pr) - len(ctx.llm_completed)} "
            f"remaining LLM "
            f"{'assessments' if len(ctx.llm_future_to_pr) - len(ctx.llm_completed) != 1 else 'assessment'}"
            f"...[/]"
        )
        time.sleep(2)
        _collect_llm_results(
            ctx.llm_future_to_pr, ctx.llm_assessments, ctx.llm_completed, ctx.llm_errors, ctx.llm_passing
        )

    console_print(
        f"\n[info]LLM assessment complete: {len(ctx.llm_assessments)} flagged, "
        f"{len(ctx.llm_passing)} passed, {len(ctx.llm_errors)} errors "
        f"(out of {len(ctx.llm_future_to_pr)} assessed).[/]\n"
    )


def _review_passing_prs(ctx: TriageContext, passing_prs: list[PRData]) -> None:
    """Present passing PRs for optional ready-for-review marking. Mutates ctx.stats."""
    if ctx.stats.quit_early or not passing_prs:
        return

    passing_prs.sort(key=lambda p: (p.author_login.lower(), p.number))
    console_print(
        f"\n[info]{len(passing_prs)} {'PRs pass' if len(passing_prs) != 1 else 'PR passes'} "
        f"all checks — review to mark as ready:[/]\n"
    )
    for pr in passing_prs:
        author_profile = _fetch_author_profile(ctx.token, pr.author_login, ctx.github_repository)
        _display_pr_info_panels(pr, author_profile)
        console_print("[success]This looks like a PR that is ready for review.[/]")

        if ctx.dry_run:
            console_print("[warning]Dry run — skipping.[/]")
            continue

        action = prompt_triage_action(
            f"Action for PR {_pr_link(pr)}?",
            default=TriageAction.READY,
            forced_answer=ctx.answer_triage,
            exclude={TriageAction.DRAFT} if pr.is_draft else None,
            pr_url=pr.url,
            token=ctx.token,
            github_repository=ctx.github_repository,
            pr_number=pr.number,
        )

        if action == TriageAction.QUIT:
            console_print("[warning]Quitting.[/]")
            ctx.stats.quit_early = True
            return

        if action == TriageAction.READY:
            if pr.is_draft:
                confirm_desc = "Mark as ready for review (undraft + label + comment)"
            else:
                confirm_desc = "Add 'ready for maintainer review' label"
            if _confirm_action(pr, confirm_desc, ctx.answer_triage):
                if pr.is_draft:
                    # Undraft the PR first
                    if _mark_pr_ready_for_review(ctx.token, pr.node_id):
                        console_print(
                            f"  [success]PR {_pr_link(pr)} marked as ready for review (undrafted).[/]"
                        )
                    else:
                        console_print(f"  [warning]Failed to undraft PR {_pr_link(pr)}.[/]")
                    # Post a polite comment
                    comment = (
                        "This PR looks like it's ready for review, so I'm marking it as such "
                        "and adding the `ready for maintainer review` label.\n\n"
                        "As a friendly reminder — next time please mark your PR as "
                        "ready for review yourself when you're done working on it. "
                        "This helps maintainers find PRs that need attention more quickly. "
                        "Thank you! \U0001f64f"
                    )
                    _post_comment(ctx.token, pr.node_id, comment)
                console_print(f"  [info]Adding '{_READY_FOR_REVIEW_LABEL}' label to PR {_pr_link(pr)}.[/]")
                if _add_label(ctx.token, ctx.github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
                    console_print(
                        f"  [success]Label '{_READY_FOR_REVIEW_LABEL}' added to PR {_pr_link(pr)}.[/]"
                    )
                    ctx.stats.total_ready += 1
                else:
                    console_print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
            else:
                ctx.stats.total_skipped_action += 1
        else:
            console_print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
            ctx.stats.total_skipped_action += 1


def _review_stale_review_requests(
    ctx: TriageContext,
    accepted_prs: list[PRData],
) -> None:
    """Present accepted PRs with stale CHANGES_REQUESTED reviews for follow-up nudging.

    Finds PRs that have the 'ready for maintainer review' label, pass all checks,
    and have CHANGES_REQUESTED reviews with commits pushed after the review.
    Proposes a comment to nudge the author or reviewer to follow up.
    """
    if ctx.stats.quit_early or not accepted_prs:
        return

    # Only consider PRs that pass all checks
    passing_accepted = [pr for pr in accepted_prs if pr.checks_state == "SUCCESS"]
    if not passing_accepted:
        return

    console_print(
        f"[info]Checking {len(passing_accepted)} accepted "
        f"{'PRs' if len(passing_accepted) != 1 else 'PR'} for stale review requests...[/]"
    )
    stale_data = _fetch_stale_review_data_batch(ctx.token, ctx.github_repository, passing_accepted)
    if not stale_data:
        console_print("  [dim]No stale review requests found.[/]")
        return

    stale_prs = [pr for pr in passing_accepted if pr.number in stale_data]
    stale_prs.sort(key=lambda p: (p.author_login.lower(), p.number))

    console_print(
        f"\n[info]{len(stale_prs)} accepted "
        f"{'PRs have' if len(stale_prs) != 1 else 'PR has'} stale review requests "
        f"with commits pushed after the review — review to nudge follow-up:[/]\n"
    )

    for pr in stale_prs:
        if ctx.stats.quit_early:
            break

        reviews = stale_data[pr.number]
        author_profile = _fetch_author_profile(ctx.token, pr.author_login, ctx.github_repository)
        _display_pr_info_panels(pr, author_profile)

        # Show review info
        console = get_console()
        review_lines = []
        for r in reviews:
            ping_status = (
                "[green]author pinged reviewer[/]"
                if r.author_pinged_reviewer
                else "[yellow]no follow-up ping from author[/]"
            )
            review_lines.append(
                f"  Reviewer: [bold]{r.reviewer_login}[/] — "
                f"requested changes: {_human_readable_age(r.review_date)} — {ping_status}"
            )
        console.print(
            Panel(
                "\n".join(review_lines),
                title="Stale Review Requests",
                border_style="yellow",
            )
        )

        # Build and show proposed comment
        comment = _build_review_nudge_comment(pr.author_login, reviews)
        console.print(Panel(comment, title="Proposed comment", border_style="green"))

        if ctx.dry_run:
            console_print("[warning]Dry run — skipping.[/]")
            continue

        action = prompt_triage_action(
            f"Action for PR {_pr_link(pr)}?",
            default=TriageAction.COMMENT,
            forced_answer=ctx.answer_triage,
            exclude={TriageAction.DRAFT, TriageAction.CLOSE, TriageAction.RERUN},
            pr_url=pr.url,
            token=ctx.token,
            github_repository=ctx.github_repository,
            pr_number=pr.number,
        )

        if action == TriageAction.QUIT:
            console_print("[warning]Quitting.[/]")
            ctx.stats.quit_early = True
            return

        if action == TriageAction.COMMENT:
            if _confirm_action(pr, "Post review nudge comment", ctx.answer_triage):
                if _post_comment(ctx.token, pr.node_id, comment):
                    console_print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
                    ctx.stats.total_review_nudges += 1
                else:
                    console_print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
            else:
                ctx.stats.total_skipped_action += 1
        elif action == TriageAction.READY:
            if _confirm_action(pr, "Add 'ready for maintainer review' label", ctx.answer_triage):
                if _add_label(ctx.token, ctx.github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
                    console_print(
                        f"  [success]Label '{_READY_FOR_REVIEW_LABEL}' added to PR {_pr_link(pr)}.[/]"
                    )
                    ctx.stats.total_ready += 1
                else:
                    console_print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
            else:
                ctx.stats.total_skipped_action += 1
        else:
            console_print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
            ctx.stats.total_skipped_action += 1


def _display_json_fix_info(fix_info: dict) -> None:
    """Display deferred LLM JSON parse-fix debug info."""
    console = get_console()
    if fix_info.get("fixed"):
        console.print(
            Panel(
                f"[yellow]Original parse error:[/] {fix_info['error']}\n"
                f"[green]LLM successfully repaired the JSON.[/]\n\n"
                f"[dim]Original response: {fix_info['before_file']}[/]\n"
                f"[dim]Extracted JSON: {fix_info['extracted_json_file']}[/]",
                title="JSON Parse Recovery",
                border_style="yellow",
            )
        )
    else:
        console.print(
            Panel(
                f"[red]Original parse error:[/] {fix_info['error']}\n"
                f"[red]LLM fix also failed:[/] {fix_info.get('fix_error', 'unknown')}\n\n"
                f"[dim]Original response: {fix_info['before_file']}[/]\n"
                f"[dim]Extracted JSON: {fix_info['extracted_json_file']}[/]\n"
                f"[dim]Fix attempt: {fix_info.get('after_file', 'N/A')}[/]",
                title="JSON Parse Failure",
                border_style="red",
            )
        )


def _prompt_review_comment_action(comment_summary: str, *, with_open: bool = False) -> str:
    """Prompt user for action on a single review comment: [p]ost, [e]dit, [o]pen, [s]kip.

    Returns 'p' for post, 'e' for edit, 'o' for open, 's' for skip, 'q' for quit.
    """
    from airflow_breeze.utils.confirm import _read_char

    get_console().print(f"\n  {comment_summary}")
    if with_open:
        get_console().print("  \\[P]ost / \\[e]dit / \\[o]pen / \\[s]kip / \\[q]uit: ", end="")
    else:
        get_console().print("  \\[P]ost / \\[e]dit / \\[s]kip / \\[q]uit: ", end="")
    try:
        ch = _read_char()
    except (KeyboardInterrupt, EOFError):
        get_console().print()
        return "q"
    if len(ch) > 1:
        get_console().print()
        return "p"  # default: post
    get_console().print(ch)
    valid = ("p", "e", "o", "s", "q") if with_open else ("p", "e", "s", "q")
    return ch.lower() if ch.lower() in valid else "p"


def _edit_comment_body(original: str) -> str:
    """Let the user edit a comment body in their $EDITOR. Returns the edited text."""
    import click

    edited = click.edit(original, extension=".md")
    if edited is not None:
        edited = edited.strip()
        if edited:
            return edited
    get_console().print("  [dim]No changes made — keeping original.[/]")
    return original


def _interactive_review_comments(
    ctx: TriageContext,
    pr: PRData,
    review: CodeReview,
    *,
    json_fix_info: dict | None = None,
    from_cache: bool = False,
) -> None:
    """Walk through LLM-proposed review comments one-by-one and post/skip/edit each.

    Then propose the overall review comment at the end.
    """
    import webbrowser

    from rich.markdown import Markdown

    console = get_console()

    # Show deferred JSON parse-fix info if present
    if json_fix_info:
        _display_json_fix_info(json_fix_info)

    if from_cache:
        console.print("[dim]Using cached LLM review (same commit hash as previous run).[/]")

    # Display summary and overall assessment
    assessment_colors = {
        "APPROVE": "green",
        "REQUEST_CHANGES": "red",
        "COMMENT": "yellow",
    }
    color = assessment_colors.get(review.overall_assessment, "yellow")
    console.print(
        Panel(
            Markdown(f"**Assessment:** {review.overall_assessment}\n\n{review.summary}"),
            title="Code Review Summary",
            border_style=color,
        )
    )

    # Fetch existing review comments to check for duplicates and nearby comments
    existing_comments = _fetch_existing_review_comments(ctx.token, ctx.github_repository, pr.number)

    # List summary of all comments
    if review.comments:
        console.print(
            f"\n  [bold]{len(review.comments)} review comment{'s' if len(review.comments) != 1 else ''}:[/]"
        )
        for i, c in enumerate(review.comments, 1):
            cat_color = "red" if c.category in ("bug", "security") else "yellow"
            # Strip markdown for the one-line summary
            plain_preview = c.body.replace("`", "").replace("*", "")[:80]
            console.print(
                f"    {i}. [{cat_color}]{c.category}[/{cat_color}] "
                f"[dim]{c.path}:{c.line}[/] — {plain_preview}{'...' if len(c.body) > 80 else ''}"
            )
    else:
        console.print("\n  [dim]No line-level comments proposed.[/]")

    if ctx.dry_run:
        console.print("[warning]Dry run — skipping comment submission.[/]")
        return

    # Build URL base for opening files in browser
    # PR files URL: https://github.com/owner/repo/pull/N/files
    pr_files_base = f"{pr.url}/files"

    # Go through comments one-by-one
    submitted_count = 0
    skipped_count = 0
    duplicate_count = 0
    for i, comment in enumerate(review.comments, 1):
        if ctx.stats.quit_early:
            break

        # Check if this comment was already posted (duplicate detection)
        is_duplicate = any(
            ec.path == comment.path and ec.line == comment.line and ec.body.strip() == comment.body.strip()
            for ec in existing_comments
        )
        if is_duplicate:
            console.print(
                f"  [dim]Comment {i}/{len(review.comments)} on {comment.path}:{comment.line} "
                f"already posted — skipping.[/]"
            )
            duplicate_count += 1
            continue

        # Show nearby existing comments on the same file
        nearby = [
            ec
            for ec in existing_comments
            if ec.path == comment.path
            and ec.line is not None
            and abs(ec.line - comment.line) <= _NEARBY_LINE_THRESHOLD
        ]
        if nearby:
            nearby_lines = []
            for ec in nearby:
                preview = _linkify_commit_hashes(ec.body.replace("\n", " ")[:100])
                nearby_lines.append(f"  [dim]@{ec.user_login} on line {ec.line}: {preview}[/]")
            console.print(
                Panel(
                    "\n".join(nearby_lines),
                    title=f"Existing comments near {comment.path}:{comment.line}",
                    border_style="dim",
                )
            )

        console.print(
            Panel(
                Markdown(comment.body),
                title=f"Comment {i}/{len(review.comments)} — {comment.path}:{comment.line} [{comment.category}]",
                border_style="cyan",
                subtitle=f"[dim]{comment.path}:{comment.line}[/]",
            )
        )

        while True:
            action = _prompt_review_comment_action(
                f"[bold]Comment {i}/{len(review.comments)}[/] on [dim]{comment.path}:{comment.line}[/]",
                with_open=True,
            )

            if action == "o":
                file_url = f"{pr_files_base}#diff-{comment.path.replace('/', '-')}"
                webbrowser.open(file_url)
                console.print(f"  [info]Opened {pr_files_base} in browser.[/]")
                continue  # re-prompt after opening
            if action == "e":
                comment.body = _edit_comment_body(comment.body)
                console.print(Panel(Markdown(comment.body), title="Edited comment", border_style="green"))
                continue  # re-prompt so user can submit, edit again, or skip
            break

        if action == "q":
            console.print("[warning]Quitting review.[/]")
            ctx.stats.quit_early = True
            return
        if action == "s":
            console.print("  [dim]Skipped.[/]")
            skipped_count += 1
            continue

        # Post the comment
        if _post_review_comment(
            ctx.token,
            ctx.github_repository,
            pr.number,
            pr.head_sha,
            comment.path,
            comment.line,
            comment.body,
        ):
            console.print(f"  [success]Comment posted on {comment.path}:{comment.line}.[/]")
            submitted_count += 1
            ctx.stats.total_review_comments += 1
            # Track the newly posted comment to avoid duplicates within this session
            existing_comments.append(
                ExistingComment(path=comment.path, line=comment.line, body=comment.body, user_login="(self)")
            )
        else:
            console.print(f"  [error]Failed to post comment on {comment.path}:{comment.line}.[/]")

    if not ctx.stats.quit_early:
        parts = [f"{submitted_count} posted", f"{skipped_count} skipped"]
        if duplicate_count:
            parts.append(f"{duplicate_count} already posted")
        console.print(f"\n  [info]Comments: {', '.join(parts)}.[/]")

    # Overall review comment
    if ctx.stats.quit_early or not review.overall_comment:
        return

    console.print(
        Panel(
            Markdown(f"**Assessment: {review.overall_assessment}**\n\n{review.overall_comment}"),
            title="Overall Review Comment",
            border_style=color,
        )
    )

    while True:
        action = _prompt_review_comment_action("[bold]Overall review comment[/]")

        if action == "q":
            console.print("[warning]Quitting review.[/]")
            ctx.stats.quit_early = True
            return
        if action == "s":
            console.print("  [dim]Skipped overall review.[/]")
            return
        if action == "e":
            review.overall_comment = _edit_comment_body(review.overall_comment)
            console.print(
                Panel(
                    Markdown(f"**Assessment: {review.overall_assessment}**\n\n{review.overall_comment}"),
                    title="Edited overall comment",
                    border_style="green",
                )
            )
            # Also let user change the assessment event
            console.print("  Assessment: \\[A]pprovE / \\[R]equest changes / \\[C]omment: ", end="")
            from airflow_breeze.utils.confirm import _read_char

            try:
                ch = _read_char()
            except (KeyboardInterrupt, EOFError):
                ch = "c"
            if len(ch) > 1:
                ch = "c"
            console.print(ch)
            event_map = {"a": "APPROVE", "r": "REQUEST_CHANGES", "c": "COMMENT"}
            review.overall_assessment = event_map.get(ch.lower(), review.overall_assessment)
            continue  # re-prompt so user can submit, edit again, or skip
        break  # action == "s" — proceed to submit

    if _submit_pr_review(
        ctx.token,
        ctx.github_repository,
        pr.number,
        pr.head_sha,
        review.overall_comment,
        event=review.overall_assessment,
    ):
        console.print(f"  [success]Review submitted: {review.overall_assessment} on PR {_pr_link(pr)}.[/]")
        ctx.stats.total_reviews_submitted += 1
    else:
        console.print(f"  [error]Failed to submit review on PR {_pr_link(pr)}.[/]")


def _fetch_diff_and_review_pr(
    token: str,
    github_repository: str,
    pr_number: int,
    pr_title: str,
    pr_body: str,
    head_sha: str,
    llm_model: str,
) -> dict:
    """Background task: fetch diff and run LLM code review for a single PR.

    Uses a cache keyed by PR number + commit hash to avoid redundant LLM calls.
    Returns the review_pr() result dict (with added 'error' key on diff failure).
    """
    from airflow_breeze.utils.llm_utils import review_pr as _review_pr_llm

    # Check cache first
    cached = _get_cached_review(github_repository, pr_number, head_sha)
    if cached is not None:
        cached["_from_cache"] = True
        return cached

    diff_text = _fetch_pr_diff(token, github_repository, pr_number)
    if not diff_text:
        return {
            "summary": "",
            "overall_assessment": "COMMENT",
            "overall_comment": "",
            "comments": [],
            "error": True,
            "error_message": f"Could not fetch diff for PR #{pr_number}",
        }
    result = _review_pr_llm(
        pr_number=pr_number,
        pr_title=pr_title,
        pr_body=pr_body,
        diff_text=diff_text,
        llm_model=llm_model,
    )

    # Cache the result if it's not an error
    if not result.get("error"):
        _save_review_cache(github_repository, pr_number, head_sha, result)

    return result


def _review_ready_prs_review_mode(
    ctx: TriageContext,
    accepted_prs: list[PRData],
    *,
    run_api: bool,
    run_llm: bool,
    llm_model: str,
    llm_concurrency: int,
) -> tuple[dict[int, float], dict[int, str]]:
    """Review mode: assess PRs with 'ready for maintainer review' label.

    1. Launch LLM code reviews for ALL accepted PRs in the background immediately.
    2. Run deterministic checks; PRs with in-progress CI are treated as passing.
    3. Present deterministic failures first (while LLM runs in background).
    4. Present LLM review results as they complete for PRs that passed deterministic checks.

    Returns (pr_timings, pr_actions) where:
      - pr_timings: PR number -> time spent (seconds) on that PR during interactive review
      - pr_actions: PR number -> action taken (e.g. 'reviewed', 'drafted', 'skipped', 'llm-error')
    """
    from airflow_breeze.utils.github import (
        PRAssessment,
        assess_pr_checks,
        assess_pr_conflicts,
        assess_pr_unresolved_comments,
    )

    pr_timings: dict[int, float] = {}
    pr_actions: dict[int, str] = {}

    if ctx.stats.quit_early or not accepted_prs:
        return pr_timings, pr_actions

    console = get_console()
    accepted_prs.sort(key=lambda p: (p.author_login.lower(), p.number))

    console.print(
        f"\n[info]Review mode: assessing {len(accepted_prs)} "
        f"{'PRs' if len(accepted_prs) != 1 else 'PR'} "
        f"with '{_READY_FOR_REVIEW_LABEL}' label...[/]\n"
    )

    # --- Launch LLM reviews for ALL PRs in background immediately ---
    # Start before enrichment so LLM fetches diffs concurrently with check/merge details.
    llm_executor = None
    llm_future_to_pr: dict = {}
    if run_llm:
        llm_executor = ThreadPoolExecutor(max_workers=llm_concurrency)
        llm_future_to_pr = {
            llm_executor.submit(
                _fetch_diff_and_review_pr,
                token=ctx.token,
                github_repository=ctx.github_repository,
                pr_number=pr.number,
                pr_title=pr.title,
                pr_body=pr.body,
                head_sha=pr.head_sha,
                llm_model=llm_model,
            ): pr
            for pr in accepted_prs
        }
        pr_word = "PRs" if len(accepted_prs) != 1 else "PR"
        console.print(
            f"[info]Launched LLM code reviews for {len(accepted_prs)} {pr_word} "
            f"in background (concurrency: {llm_concurrency}).[/]"
        )

    # Enrich candidates with check details (runs while LLM reviews are in progress)
    _enrich_candidate_details(ctx.token, ctx.github_repository, accepted_prs, run_api=run_api)

    # Launch background CI log fetching for PRs with failed checks
    ctx.log_futures = _launch_background_log_fetching(
        ctx.token, ctx.github_repository, accepted_prs, llm_concurrency
    )

    if llm_future_to_pr:
        done_count = sum(1 for f in llm_future_to_pr if f.done())
        total = len(llm_future_to_pr)
        console.print(f"[info]LLM progress: {done_count}/{total} reviews completed so far.[/]\n")

    try:
        # --- Phase 1: Deterministic checks — present failures first ---
        det_flagged: list[tuple[PRData, PRAssessment]] = []
        det_passing: list[PRData] = []
        recent_failures_skipped: list[PRData] = []

        for pr in accepted_prs:
            if not run_api:
                det_passing.append(pr)
                continue

            ci_assessment = assess_pr_checks(pr.number, pr.checks_state, pr.failed_checks)

            # In review mode, PRs with in-progress CI are treated as passing
            # (the checks are still running — don't penalise).
            if (
                ci_assessment
                and pr.head_sha
                and _has_in_progress_workflows(ctx.token, ctx.github_repository, pr.head_sha)
            ):
                ci_assessment = None

            # Grace period: don't flag CI failures within the grace window.
            # Default 24 h; extended to 96 h when a collaborator has already engaged.
            if (
                ci_assessment
                and pr.head_sha
                and _are_failures_recent(
                    ctx.token, ctx.github_repository, pr.head_sha, pr.ci_grace_period_hours
                )
            ):
                ci_assessment = None
                # If CI is the only issue, skip this PR entirely
                conflict_check = assess_pr_conflicts(pr.number, pr.mergeable, pr.base_ref, pr.commits_behind)
                comments_check = assess_pr_unresolved_comments(
                    pr.number, pr.unresolved_review_comments, pr.unresolved_threads
                )
                if not conflict_check and not comments_check:
                    recent_failures_skipped.append(pr)
                    continue

            conflict_assessment = assess_pr_conflicts(pr.number, pr.mergeable, pr.base_ref, pr.commits_behind)
            comments_assessment = assess_pr_unresolved_comments(
                pr.number, pr.unresolved_review_comments, pr.unresolved_threads
            )

            if ci_assessment or conflict_assessment or comments_assessment:
                violations = []
                summaries = []
                if conflict_assessment:
                    violations.extend(conflict_assessment.violations)
                    summaries.append(conflict_assessment.summary)
                if ci_assessment:
                    violations.extend(ci_assessment.violations)
                    summaries.append(ci_assessment.summary)
                if comments_assessment:
                    violations.extend(comments_assessment.violations)
                    summaries.append(comments_assessment.summary)

                assessment = PRAssessment(
                    should_flag=True,
                    violations=violations,
                    summary=" ".join(summaries),
                )
                det_flagged.append((pr, assessment))
            else:
                det_passing.append(pr)

        # Display PRs skipped due to recent CI failures
        if recent_failures_skipped:
            pr_word = "PRs" if len(recent_failures_skipped) != 1 else "PR"
            console.print(
                f"\n[info]Skipped {len(recent_failures_skipped)} {pr_word} with recent CI failures "
                f"(within grace period — giving authors time to address at their own pace):[/]"
            )
            for pr in recent_failures_skipped:
                console.print(f"  [dim]{_pr_link(pr)} — {pr.title[:70]}[/]")

        # Track det_flagged PRs that also got LLM review, so Phase 3 skips them
        det_flagged_with_llm: set[int] = set()

        # Present deterministic failures (while LLM runs in background)
        if det_flagged:
            console.print(
                f"\n[info]{len(det_flagged)} "
                f"{'PRs have' if len(det_flagged) != 1 else 'PR has'} quality issues"
                f"{' (LLM reviews running in background)' if llm_future_to_pr else ''}:[/]\n"
            )
            for det_i, (pr, assessment) in enumerate(det_flagged, 1):
                if ctx.stats.quit_early:
                    break

                t_pr_start = time.monotonic()
                _print_pr_header(pr, index=det_i, total=len(det_flagged))

                ctx.author_flagged_count[pr.author_login] = (
                    ctx.author_flagged_count.get(pr.author_login, 0) + 1
                )
                console.print(
                    f"[yellow]Quality issues detected despite '{_READY_FOR_REVIEW_LABEL}' label:[/]"
                )

                before_converted = ctx.stats.total_converted
                before_closed = ctx.stats.total_closed
                before_commented = ctx.stats.total_commented

                _prompt_and_execute_flagged_pr(ctx, pr, assessment)

                # Determine what action was taken
                if ctx.stats.total_converted > before_converted:
                    pr_actions[pr.number] = "drafted"
                elif ctx.stats.total_closed > before_closed:
                    pr_actions[pr.number] = "closed"
                elif ctx.stats.total_commented > before_commented:
                    pr_actions[pr.number] = "commented"
                else:
                    pr_actions[pr.number] = "skipped"

                action_taken = (
                    ctx.stats.total_converted > before_converted
                    or ctx.stats.total_closed > before_closed
                    or ctx.stats.total_commented > before_commented
                )
                if not ctx.stats.quit_early and action_taken:
                    if _remove_label(ctx.token, ctx.github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
                        console.print(
                            f"  [info]Removed '{_READY_FOR_REVIEW_LABEL}' label from PR {_pr_link(pr)}.[/]"
                        )

                # Also present LLM review if available for this PR
                if not ctx.stats.quit_early and run_llm:
                    llm_future = next((f for f, p in llm_future_to_pr.items() if p.number == pr.number), None)
                    if llm_future is not None:
                        if not llm_future.done():
                            console.print(f"  [dim]Waiting for LLM review of PR {_pr_link(pr)}...[/]")
                        try:
                            review_result = llm_future.result(timeout=300)
                        except Exception as e:
                            console.print(f"  [warning]LLM review failed for PR {_pr_link(pr)}: {e}[/]")
                            review_result = None

                        if review_result and not review_result.get("error") and review_result.get("summary"):
                            review = CodeReview(
                                summary=review_result.get("summary", ""),
                                overall_assessment=review_result.get("overall_assessment", "COMMENT"),
                                overall_comment=review_result.get("overall_comment", ""),
                                comments=[
                                    ReviewComment(
                                        path=c.get("path", ""),
                                        line=c.get("line", 0),
                                        body=c.get("body", ""),
                                        category=c.get("category", "suggestion"),
                                    )
                                    for c in review_result.get("comments", [])
                                    if c.get("path") and c.get("line") and c.get("body")
                                ],
                            )
                            _interactive_review_comments(
                                ctx,
                                pr,
                                review,
                                json_fix_info=review_result.get("json_fix_info"),
                                from_cache=bool(review_result.get("_from_cache")),
                            )
                            pr_actions[pr.number] = "reviewed"
                        elif review_result and (
                            review_result.get("error") or not review_result.get("summary")
                        ):
                            error_msg = review_result.get("error_message", "empty response")
                            console.print(
                                f"  [warning]LLM review error for PR {_pr_link(pr)}: {error_msg}[/]"
                            )
                        # Mark as presented so Phase 3 skips it
                        det_flagged_with_llm.add(pr.number)

                pr_timings[pr.number] = time.monotonic() - t_pr_start

        # --- Phase 2: PRs that passed deterministic checks ---
        if ctx.stats.quit_early or not det_passing:
            return pr_timings, pr_actions

        if not run_llm:
            # No LLM — show passing PRs for manual review
            console.print(
                f"\n[info]{len(det_passing)} "
                f"{'PRs pass' if len(det_passing) != 1 else 'PR passes'} "
                f"deterministic checks. No LLM review configured.[/]\n"
            )
            for pass_i, pr in enumerate(det_passing, 1):
                if ctx.stats.quit_early:
                    break
                t_pr_start = time.monotonic()
                _print_pr_header(pr, index=pass_i, total=len(det_passing))
                author_profile = _fetch_author_profile(ctx.token, pr.author_login, ctx.github_repository)
                _display_pr_info_panels(pr, author_profile, open_in_browser=True)
                console.print("[success]Deterministic checks pass.[/]")

                action = prompt_triage_action(
                    f"Action for PR {_pr_link(pr)}?",
                    default=TriageAction.SKIP,
                    forced_answer=ctx.answer_triage,
                    exclude={TriageAction.RERUN},
                    pr_url=pr.url,
                    token=ctx.token,
                    github_repository=ctx.github_repository,
                    pr_number=pr.number,
                )
                pr_timings[pr.number] = time.monotonic() - t_pr_start
                if action == TriageAction.QUIT:
                    ctx.stats.quit_early = True
                    pr_actions[pr.number] = "quit"
                    return pr_timings, pr_actions
                if action == TriageAction.SKIP:
                    ctx.stats.total_skipped_action += 1
                    pr_actions[pr.number] = "skipped"
                else:
                    pr_actions[pr.number] = action.value
            return pr_timings, pr_actions

        # --- Phase 3: Present LLM review results as they complete ---
        det_flagged_nums = {pr.number for pr, _ in det_flagged}
        # Exclude PRs already presented with LLM review in Phase 1
        already_reviewed = det_flagged_nums | det_flagged_with_llm
        review_prs = [pr for pr in det_passing if pr.number not in already_reviewed]

        if not review_prs:
            return pr_timings, pr_actions

        console.print(
            f"\n[info]Waiting for LLM code reviews for {len(review_prs)} "
            f"{'PRs' if len(review_prs) != 1 else 'PR'} that passed deterministic checks...[/]\n"
        )

        # Build a lookup from PR number to future
        pr_to_future: dict[int, Future] = {}
        for future, pr in llm_future_to_pr.items():
            pr_to_future[pr.number] = future

        presented: set[int] = set()
        review_pr_map = {pr.number: pr for pr in review_prs}

        while not ctx.stats.quit_early:
            # Check for newly completed futures
            newly_done = []
            for pr_num, future in pr_to_future.items():
                if pr_num in presented or pr_num not in review_pr_map:
                    continue
                if future.done():
                    newly_done.append(pr_num)

            for pr_num in newly_done:
                if ctx.stats.quit_early:
                    break

                t_pr_start = time.monotonic()

                pr = review_pr_map[pr_num]
                future = pr_to_future[pr_num]
                presented.add(pr_num)

                llm_idx = len(presented)
                _print_pr_header(pr, index=llm_idx, total=len(review_prs))

                try:
                    review_result = future.result()
                except Exception as e:
                    console.print(f"[warning]LLM review failed for PR {_pr_link(pr)}: {e}[/]")
                    ctx.stats.total_skipped_action += 1
                    pr_timings[pr.number] = time.monotonic() - t_pr_start
                    pr_actions[pr.number] = "llm-error"
                    continue

                if review_result.get("error") or not review_result.get("summary"):
                    error_msg = review_result.get("error_message", "empty LLM response")
                    console.print(f"[warning]LLM review failed for PR {_pr_link(pr)}: {error_msg}[/]")
                    ctx.stats.total_skipped_action += 1
                    pr_timings[pr.number] = time.monotonic() - t_pr_start
                    pr_actions[pr.number] = "llm-error"
                    continue

                author_profile = _fetch_author_profile(ctx.token, pr.author_login, ctx.github_repository)
                _display_pr_info_panels(pr, author_profile, open_in_browser=True)

                remaining = len(review_prs) - len(presented)
                if remaining:
                    console.print(
                        f"[dim]{remaining} LLM "
                        f"{'reviews' if remaining != 1 else 'review'} still running...[/]"
                    )

                review = CodeReview(
                    summary=review_result.get("summary", ""),
                    overall_assessment=review_result.get("overall_assessment", "COMMENT"),
                    overall_comment=review_result.get("overall_comment", ""),
                    comments=[
                        ReviewComment(
                            path=c.get("path", ""),
                            line=c.get("line", 0),
                            body=c.get("body", ""),
                            category=c.get("category", "suggestion"),
                        )
                        for c in review_result.get("comments", [])
                        if c.get("path") and c.get("line") and c.get("body")
                    ],
                )

                _interactive_review_comments(
                    ctx,
                    pr,
                    review,
                    json_fix_info=review_result.get("json_fix_info"),
                    from_cache=bool(review_result.get("_from_cache")),
                )

                pr_timings[pr.number] = time.monotonic() - t_pr_start
                pr_actions[pr.number] = "reviewed"

            # All done?
            if len(presented) >= len(review_prs):
                break

            # Wait briefly for more results
            time.sleep(2)
            remaining = len(review_prs) - len(presented)
            console.print(
                f"[dim]Waiting for {remaining} remaining LLM "
                f"{'reviews' if remaining != 1 else 'review'}...[/]"
            )

    finally:
        if llm_executor is not None:
            llm_executor.shutdown(wait=False, cancel_futures=True)

    return pr_timings, pr_actions


def _display_triage_summary(
    all_prs: list[PRData],
    candidate_prs: list[PRData],
    passing_prs: list[PRData],
    pending_approval: list[PRData],
    workflows_in_progress: list[PRData],
    skipped_drafts: list[PRData],
    recent_failures_skipped: list[PRData],
    already_triaged: list[PRData],
    stats: TriageStats,
    *,
    total_deterministic_flags: int,
    total_llm_flagged: int,
    total_llm_errors: int,
    total_llm_report: int,
    total_skipped_collaborator: int,
    total_skipped_bot: int,
    total_skipped_accepted: int,
    triaged_waiting_count: int = 0,
    triaged_responded_count: int = 0,
) -> None:
    """Print the final triage summary table."""
    total_flagged = total_deterministic_flags + total_llm_flagged
    verbose = get_verbose()

    console_print(
        f"\n[info]Assessment complete: {total_flagged} {'PRs' if total_flagged != 1 else 'PR'} "
        f"flagged ({total_deterministic_flags} CI/conflicts/comments, "
        f"{total_llm_flagged} LLM-flagged"
        f"{f', {total_llm_errors} LLM errors' if total_llm_errors else ''}"
        f"{f', {len(pending_approval)} awaiting workflow approval' if pending_approval else ''}"
        f"{f', {len(workflows_in_progress)} workflows in progress' if workflows_in_progress else ''}"
        f"{f', {len(skipped_drafts)} drafts with issues skipped' if skipped_drafts else ''}"
        f"{f', {len(recent_failures_skipped)} recent CI failures skipped' if recent_failures_skipped else ''}"
        f"{f', {len(already_triaged)} already triaged' if already_triaged else ''}"
        f").[/]\n"
    )

    summary_table = Table(title="Summary")
    summary_table.add_column("Metric", style="bold")
    summary_table.add_column("Count", justify="right")
    total_skipped = total_skipped_collaborator + total_skipped_bot + total_skipped_accepted
    summary_table.add_row("PRs fetched", str(len(all_prs)))
    if verbose:
        summary_table.add_row("Collaborators skipped", str(total_skipped_collaborator))
        summary_table.add_row("Bots skipped", str(total_skipped_bot))
        summary_table.add_row("Ready-for-review skipped", str(total_skipped_accepted))
    summary_table.add_row("PRs skipped (filtered)", str(total_skipped))
    summary_table.add_row("Already triaged (skipped)", str(len(already_triaged)))
    if already_triaged:
        summary_table.add_row("  Commented (no response)", str(triaged_waiting_count))
        summary_table.add_row("  Triaged (author responded)", str(triaged_responded_count))
    summary_table.add_row("PRs assessed", str(len(candidate_prs)))
    summary_table.add_row("Flagged by CI/conflicts/comments", str(total_deterministic_flags))
    summary_table.add_row("Flagged by LLM", str(total_llm_flagged))
    if total_llm_report:
        summary_table.add_row("[red]Potentially flagged for report[/red]", f"[red]{total_llm_report}[/red]")
    summary_table.add_row("LLM errors (skipped)", str(total_llm_errors))
    summary_table.add_row("Total flagged", str(total_flagged))
    summary_table.add_row("PRs passing all checks", str(len(passing_prs)))
    summary_table.add_row("Drafts with issues (skipped)", str(len(skipped_drafts)))
    summary_table.add_row(
        "Recent CI failures within grace period (skipped)",
        str(len(recent_failures_skipped)),
    )
    summary_table.add_row("PRs converted to draft", str(stats.total_converted))
    summary_table.add_row("PRs commented (not drafted)", str(stats.total_commented))
    summary_table.add_row("PRs closed", str(stats.total_closed))
    summary_table.add_row("PRs with checks rerun", str(stats.total_rerun))
    summary_table.add_row("PRs rebased", str(stats.total_rebased))
    summary_table.add_row("Review follow-up nudges", str(stats.total_review_nudges))
    summary_table.add_row("PRs marked ready for review", str(stats.total_ready))
    summary_table.add_row("PRs skipped (no action)", str(stats.total_skipped_action))
    summary_table.add_row("Awaiting workflow approval", str(len(pending_approval)))
    summary_table.add_row("Workflows in progress (skipped)", str(len(workflows_in_progress)))
    summary_table.add_row("PRs with workflows approved", str(stats.total_workflows_approved))
    console_print(summary_table)


def _fetch_pr_diff(token: str, github_repository: str, pr_number: int) -> str | None:
    """Fetch the diff for a PR via GitHub REST API. Returns the diff text or None on failure."""
    import requests

    url = f"https://api.github.com/repos/{github_repository}/pulls/{pr_number}"
    response = requests.get(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3.diff"},
        timeout=60,
    )
    if response.status_code != 200:
        return None
    return response.text


def _detect_sensitive_file_changes(diff_text: str) -> list[str]:
    """Parse a unified diff and return paths under .github/ or scripts/ that were changed."""
    import re

    sensitive_paths: list[str] = []
    seen: set[str] = set()
    for match in re.finditer(r"^diff --git a/(\S+) b/(\S+)", diff_text, re.MULTILINE):
        path = match.group(2)
        if path in seen:
            continue
        seen.add(path)
        if path.startswith((".github/", "scripts/")):
            sensitive_paths.append(path)
    return sensitive_paths


def _fetch_author_open_prs(token: str, github_repository: str, author_login: str) -> list[dict]:
    """Fetch all open PRs by a given author. Returns list of dicts with number, url, title, node_id."""
    search_query = f"repo:{github_repository} type:pr is:open author:{author_login}"
    data = _graphql_request(token, _SEARCH_PRS_QUERY, {"query": search_query, "first": 100})
    results = []
    for node in data["search"]["nodes"]:
        if not node:
            continue
        results.append(
            {
                "number": node["number"],
                "url": node["url"],
                "title": node["title"],
                "node_id": node["id"],
            }
        )
    return results


def _close_suspicious_prs(
    token: str,
    github_repository: str,
    author_prs: list[dict],
    flagged_pr_number: int,
) -> tuple[int, int]:
    """Close PRs flagged as suspicious, add label and comment. Returns (closed, commented) counts."""
    closed = 0
    commented = 0
    for pr_info in author_prs:
        pr_num = pr_info["number"]
        node_id = pr_info["node_id"]

        if _close_pr(token, node_id):
            console_print(f"  [success]PR [link={pr_info['url']}]#{pr_num}[/link] closed.[/]")
            closed += 1
        else:
            console_print(f"  [error]Failed to close PR [link={pr_info['url']}]#{pr_num}[/link].[/]")
            continue

        _add_label(token, github_repository, node_id, _SUSPICIOUS_CHANGES_LABEL)

        comment = (
            f"This PR has been **closed** because suspicious changes were detected "
            f"in [PR #{flagged_pr_number}]"
            f"(https://github.com/{github_repository}/pull/{flagged_pr_number}) "
            f"by the same author.\n\n"
            "All open PRs by this author have been closed as a precaution. "
            "If you believe this was done in error, please reach out on the "
            "[Airflow Slack](https://s.apache.org/airflow-slack)."
        )
        if _post_comment(token, node_id, comment):
            commented += 1

    return closed, commented


def _find_workflow_runs_by_status(
    token: str, github_repository: str, head_sha: str, status: str
) -> list[dict]:
    """Find workflow runs with a given status for a commit SHA.

    Common statuses: ``action_required``, ``in_progress``, ``queued``.
    """
    import requests

    url = f"https://api.github.com/repos/{github_repository}/actions/runs"
    response = requests.get(
        url,
        params={"head_sha": head_sha, "status": status, "per_page": "50"},
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    if response.status_code != 200:
        return []
    return response.json().get("workflow_runs", [])


def _find_pending_workflow_runs(token: str, github_repository: str, head_sha: str) -> list[dict]:
    """Find workflow runs awaiting approval for a given commit SHA."""
    return _find_workflow_runs_by_status(token, github_repository, head_sha, "action_required")


def _has_in_progress_workflows(token: str, github_repository: str, head_sha: str) -> bool:
    """Check whether a PR has any workflow runs currently in progress or queued."""
    for status in ("in_progress", "queued"):
        if _find_workflow_runs_by_status(token, github_repository, head_sha, status):
            return True
    return False


def _approve_workflow_runs(token: str, github_repository: str, pending_runs: list[dict]) -> int:
    """Approve pending workflow runs. Returns number successfully approved."""
    import requests

    approved = 0
    for run in pending_runs:
        run_id = run["id"]
        url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/approve"
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=30,
        )
        if response.status_code in (201, 204):
            approved += 1
        else:
            console_print(
                f"  [warning]Failed to approve run {run.get('name', run_id)}: {response.status_code}[/]"
            )
    return approved


def _cancel_workflow_run(token: str, github_repository: str, run: dict) -> bool:
    """Cancel a single workflow run. Returns True if successful."""
    import requests

    run_id = run["id"]
    url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/cancel"
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    return response.status_code in (202, 204)


def _rerun_workflow_run(token: str, github_repository: str, run: dict) -> bool:
    """Rerun a complete workflow run (all jobs). Returns True if successful."""
    import requests

    run_id = run["id"]
    url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/rerun"
    response = requests.post(
        url,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        timeout=30,
    )
    return response.status_code in (201, 204)


def _rerun_failed_workflow_runs(
    token: str, github_repository: str, head_sha: str, failed_check_names: list[str]
) -> int:
    """Rerun failed workflow runs for a commit. Returns number of runs rerun."""
    import requests

    # Find completed (failed) workflow runs for this SHA
    runs = _find_workflow_runs_by_status(token, github_repository, head_sha, "completed")
    if not runs:
        return 0

    # Filter to runs whose names match failed checks
    failed_set = set(failed_check_names)
    failed_runs = [r for r in runs if r.get("name") in failed_set or r.get("conclusion") == "failure"]

    rerun_count = 0
    for run in failed_runs:
        run_id = run["id"]
        url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/rerun-failed-jobs"
        response = requests.post(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=30,
        )
        if response.status_code in (201, 204):
            console_print(f"  [success]Rerun triggered for: {run.get('name', run_id)}[/]")
            rerun_count += 1
        else:
            console_print(f"  [warning]Failed to rerun {run.get('name', run_id)}: {response.status_code}[/]")
    return rerun_count


# Maximum number of log lines to extract per failed job
_LOG_SNIPPET_MAX_LINES = 30
# Maximum total log size to download (in bytes) before giving up
_LOG_DOWNLOAD_MAX_BYTES = 5 * 1024 * 1024  # 5 MB


def _fetch_failed_job_log_snippets(
    token: str, github_repository: str, head_sha: str, failed_check_names: list[str]
) -> dict[str, str]:
    """Fetch short log snippets from failed GitHub Actions jobs for a commit.

    Returns a dict mapping failed check name -> log snippet (last N lines of the failed step).
    Only fetches logs for checks in ``failed_check_names`` to limit API calls.
    """
    import io
    import zipfile

    import requests

    if not failed_check_names:
        return {}

    # Find completed (failed) workflow runs for this SHA
    runs = _find_workflow_runs_by_status(token, github_repository, head_sha, "completed")
    if not runs:
        return {}

    failed_runs = [r for r in runs if r.get("conclusion") == "failure"]
    if not failed_runs:
        return {}

    snippets: dict[str, str] = {}
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}

    for run in failed_runs:
        run_id = run["id"]

        # List jobs for this workflow run
        jobs_url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/jobs"
        try:
            jobs_resp = requests.get(jobs_url, headers=headers, params={"per_page": 100}, timeout=30)
        except requests.RequestException:
            continue
        if jobs_resp.status_code != 200:
            continue

        jobs = jobs_resp.json().get("jobs", [])
        failed_jobs = [j for j in jobs if j.get("conclusion") == "failure"]
        if not failed_jobs:
            continue

        # Match failed jobs to the check names we care about
        matched_jobs = []
        for job in failed_jobs:
            job_name = job.get("name", "")
            for check_name in failed_check_names:
                if check_name in snippets:
                    continue
                # Match by exact name or substring (job names often include matrix info)
                if job_name == check_name or check_name.lower() in job_name.lower():
                    matched_jobs.append((job, check_name))
                    break

        if not matched_jobs:
            # If no specific match, try to get the first failed job from this run
            run_name = run.get("name", "")
            for check_name in failed_check_names:
                if check_name in snippets:
                    continue
                if run_name == check_name or check_name.lower() in run_name.lower():
                    matched_jobs.append((failed_jobs[0], check_name))
                    break

        if not matched_jobs:
            continue

        # Download logs for this workflow run (zip archive)
        logs_url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/logs"
        try:
            logs_resp = requests.get(
                logs_url,
                headers=headers,
                timeout=60,
                stream=True,
            )
        except requests.RequestException:
            continue
        if logs_resp.status_code != 200:
            continue

        # Read the zip content with a size limit
        content = b""
        for chunk in logs_resp.iter_content(chunk_size=64 * 1024):
            content += chunk
            if len(content) > _LOG_DOWNLOAD_MAX_BYTES:
                break

        try:
            zf = zipfile.ZipFile(io.BytesIO(content))
        except (zipfile.BadZipFile, OSError):
            continue

        for job, check_name in matched_jobs:
            if check_name in snippets:
                continue
            job_name = job.get("name", "")
            snippet = _extract_failed_step_snippet(zf, job_name, job)
            if snippet:
                snippets[check_name] = snippet

        zf.close()

        # Stop if we have snippets for all checks
        if all(name in snippets for name in failed_check_names):
            break

    return snippets


def _extract_failed_step_snippet(zf, job_name: str, job: dict) -> str:
    """Extract a log snippet from the failed step of a job within a zip log archive.

    The zip archive has files named like ``<job_name>/<step_number>_<step_name>.txt``.
    We find the failed step and extract the last N error lines.
    """
    # Find the failed step(s) from job metadata
    failed_steps = []
    for step in job.get("steps", []):
        if step.get("conclusion") == "failure":
            failed_steps.append(step)

    # Try to find the log file for the failed step
    # Log files in the zip are named: "<job_name>/<step_number>_<step_name>.txt"
    zip_names = zf.namelist()

    for step in failed_steps:
        step_number = step.get("number", 0)
        step_name = step.get("name", "")

        # Try to find matching log file
        candidate_files = []
        for zname in zip_names:
            # Match by job name prefix and step number
            if f"{step_number}_" in zname and (
                zname.startswith(f"{job_name}/") or job_name.lower() in zname.lower()
            ):
                candidate_files.append(zname)

        if not candidate_files:
            # Fallback: try any file containing the step name
            for zname in zip_names:
                if step_name and step_name.lower() in zname.lower():
                    candidate_files.append(zname)

        for log_file in candidate_files:
            try:
                log_content = zf.read(log_file).decode("utf-8", errors="replace")
            except (KeyError, OSError):
                continue

            snippet = _extract_error_lines(log_content, step_name)
            if snippet:
                return snippet

    # Fallback: try to find any log file for this job and extract errors
    for zname in zip_names:
        if job_name in zname or job_name.replace("/", "_") in zname:
            try:
                log_content = zf.read(zname).decode("utf-8", errors="replace")
            except (KeyError, OSError):
                continue
            snippet = _extract_error_lines(log_content, "")
            if snippet:
                return snippet

    return ""


def _extract_error_lines(log_content: str, step_name: str) -> str:
    """Extract relevant error lines from a log file.

    Looks for error markers, then takes surrounding context.
    Falls back to the last N lines if no error markers are found.
    """
    lines = log_content.splitlines()
    if not lines:
        return ""

    # Look for error markers in the log
    error_markers = [
        "error:",
        "Error:",
        "ERROR:",
        "FAILED",
        "FAILURE",
        "fatal:",
        "Fatal:",
        "AssertionError",
        "Exception:",
        "Traceback (most recent call last)",
        "✗",
        "❌",
    ]

    # Find lines with errors and collect context
    error_indices: list[int] = []
    for i, line in enumerate(lines):
        # Strip GitHub Actions timestamp prefix (e.g. "2024-01-01T00:00:00.0000000Z ")
        stripped = line.lstrip()
        if any(marker in stripped for marker in error_markers):
            error_indices.append(i)

    if error_indices:
        # Take a window around the first error cluster
        first_error = error_indices[0]
        last_error = error_indices[-1]

        # If errors are spread across the file, focus on the first cluster
        for i in range(1, len(error_indices)):
            if error_indices[i] - error_indices[i - 1] > 20:
                last_error = error_indices[i - 1]
                break

        start = max(0, first_error - 3)
        end = min(len(lines), last_error + _LOG_SNIPPET_MAX_LINES - (last_error - first_error))
        snippet_lines = lines[start:end]
    else:
        # No error markers found — take the last N lines
        snippet_lines = lines[-_LOG_SNIPPET_MAX_LINES:]

    # Clean up GitHub Actions timestamp prefixes for readability
    cleaned = []
    for raw_line in snippet_lines:
        # Remove ISO timestamp prefix that GitHub Actions adds
        if len(raw_line) > 28 and raw_line[4] == "-" and raw_line[7] == "-" and raw_line[10] == "T":
            cleaned.append(raw_line[28:].lstrip())
        else:
            cleaned.append(raw_line)

    header = f"[Failed step: {step_name}]" if step_name else ""
    snippet = "\n".join(cleaned).strip()
    if len(snippet) > 3000:
        snippet = snippet[:3000] + "\n... (truncated)"

    if header:
        return f"{header}\n{snippet}"
    return snippet


def _cancel_and_rerun_in_progress_workflows(token: str, github_repository: str, head_sha: str) -> int:
    """Cancel in-progress/queued workflow runs and rerun them. Returns number rerun."""
    import time as time_mod

    in_progress = []
    for status in ("in_progress", "queued"):
        in_progress.extend(_find_workflow_runs_by_status(token, github_repository, head_sha, status))
    if not in_progress:
        return 0

    # Cancel all in-progress runs
    cancelled = 0
    for run in in_progress:
        name = run.get("name", run["id"])
        if _cancel_workflow_run(token, github_repository, run):
            console_print(f"  [info]Cancelled workflow run: {name}[/]")
            cancelled += 1
        else:
            console_print(f"  [warning]Failed to cancel: {name}[/]")

    if not cancelled:
        return 0

    # Brief pause to let GitHub process the cancellations
    console_print("  [dim]Waiting for cancellations to complete...[/]")
    time_mod.sleep(3)

    # Rerun the cancelled runs
    rerun_count = 0
    for run in in_progress:
        name = run.get("name", run["id"])
        if _rerun_workflow_run(token, github_repository, run):
            console_print(f"  [success]Rerun triggered for: {name}[/]")
            rerun_count += 1
        else:
            console_print(f"  [warning]Failed to rerun: {name}[/]")
    return rerun_count


def _fetch_main_canary_builds(
    token: str, github_repository: str, *, branch: str = "main", count: int = 4
) -> list[dict]:
    """Fetch the most recent scheduled Tests workflow runs on the main branch.

    Only returns runs from the "Tests" workflow.
    Returns a list of workflow run dicts (most recent first), up to ``count`` entries.
    Each dict contains keys like ``id``, ``name``, ``status``, ``conclusion``,
    ``created_at``, ``updated_at``, ``html_url``, ``run_started_at``, etc.
    """
    import requests

    url = f"https://api.github.com/repos/{github_repository}/actions/runs"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    # Fetch more than needed since we filter by name
    try:
        response = requests.get(
            url,
            params={"branch": branch, "event": "schedule", "per_page": str(count * 3)},
            headers=headers,
            timeout=30,
        )
    except requests.RequestException:
        return []
    if response.status_code != 200:
        return []
    runs = response.json().get("workflow_runs", [])
    return [r for r in runs if r.get("name") == "Tests"][:count]


def _platform_from_labels(labels: list[str]) -> str:
    """Determine platform (ARM/AMD) from GitHub Actions job runner labels."""
    for label in labels:
        if "arm" in label.lower() or "aarch64" in label.lower():
            return "ARM"
    return "AMD"


def _fetch_failed_jobs_for_run(token: str, github_repository: str, run_id: int) -> list[dict]:
    """Fetch failed jobs for a workflow run, returning job name, platform, and URL."""
    import requests

    url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/jobs"
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    all_jobs: list[dict] = []
    page = 1
    while True:
        try:
            response = requests.get(
                url,
                headers=headers,
                params={"per_page": "100", "filter": "latest", "page": str(page)},
                timeout=30,
            )
        except requests.RequestException:
            break
        if response.status_code != 200:
            break
        jobs = response.json().get("jobs", [])
        if not jobs:
            break
        all_jobs.extend(jobs)
        if len(jobs) < 100:
            break
        page += 1
    failed = []
    for job in all_jobs:
        if job.get("conclusion") == "failure":
            failed.append(
                {
                    "name": job.get("name", "unknown"),
                    "platform": _platform_from_labels(job.get("labels") or []),
                    "html_url": job.get("html_url", ""),
                }
            )
    return failed


def _display_canary_builds_status(builds: list[dict], token: str, github_repository: str) -> None:
    """Display a Rich table showing the status of recent scheduled Tests builds."""
    from rich.table import Table

    console = get_console()

    if not builds:
        console.print("[dim]No scheduled Tests builds found on main branch.[/]")
        return

    table = Table(title="Main Branch Tests Builds (scheduled)", expand=False)
    table.add_column("Status", justify="center")
    table.add_column("Started", justify="right")
    table.add_column("Failed Jobs", style="red")
    table.add_column("Link", style="dim")

    for build in builds:
        status = build.get("status", "unknown")
        conclusion = build.get("conclusion")
        started = build.get("run_started_at") or build.get("created_at", "")
        html_url = build.get("html_url", "")
        run_id = build.get("id", "")

        # Format status with colour
        if status == "completed":
            if conclusion == "success":
                status_display = "[green]success[/]"
            elif conclusion == "failure":
                status_display = "[red]failure[/]"
            elif conclusion == "cancelled":
                status_display = "[yellow]cancelled[/]"
            elif conclusion == "timed_out":
                status_display = "[red]timed_out[/]"
            else:
                status_display = f"[yellow]{conclusion or status}[/]"
        elif status in ("in_progress", "queued"):
            status_display = f"[cyan]{status}[/]"
        else:
            status_display = f"[dim]{status}[/]"

        # Format age
        age = _human_readable_age(started) if started else "unknown"

        # Clickable link to the workflow run page
        link = f"[link={html_url}]checks[/link]" if html_url else str(run_id)

        # Fetch failed jobs for failed builds
        failed_jobs_display = ""
        if conclusion == "failure" and run_id:
            failed_jobs = _fetch_failed_jobs_for_run(token, github_repository, run_id)
            if failed_jobs:
                parts = []
                for fj in failed_jobs:
                    parts.append(f"{fj['name']} ({fj['platform']})")
                failed_jobs_display = "\n".join(parts)

        table.add_row(status_display, age, failed_jobs_display, link)

    console.print(table)
    console.print()


def _fetch_recent_pr_failures(
    token: str, github_repository: str, *, branch: str = "main", hours: int = 4, max_prs: int = 10
) -> RecentPRFailureInfo:
    """Fetch CI failures from recently updated PRs targeting the same branch.

    Looks at PRs updated in the last ``hours`` hours to detect consistent
    failure patterns. When the same check fails across multiple PRs, the
    failure is likely systemic rather than caused by any individual PR.
    """
    from datetime import datetime, timezone

    import requests

    console = get_console()
    console.print(f"[info]Checking recent PRs to {branch} for consistent CI failure patterns...[/]")

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}
    now = datetime.now(timezone.utc)
    cutoff = now.timestamp() - hours * 3600

    # Search for recently updated PRs targeting this branch
    owner, repo = github_repository.split("/", 1)
    failing_checks: dict[str, list[dict]] = {}
    prs_examined = 0

    try:
        url = f"https://api.github.com/repos/{github_repository}/pulls"
        response = requests.get(
            url,
            params={
                "base": branch,
                "state": "all",
                "sort": "updated",
                "direction": "desc",
                "per_page": str(max_prs),
            },
            headers=headers,
            timeout=30,
        )
        if response.status_code != 200:
            console.print("  [dim]Could not fetch recent PRs.[/]")
            return RecentPRFailureInfo(failing_checks={}, failing_check_names=set(), prs_examined=0)

        prs = response.json()

        for pr_data in prs:
            updated_at = pr_data.get("updated_at", "")
            if not updated_at:
                continue
            try:
                updated_dt = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
                if updated_dt.timestamp() < cutoff:
                    continue
            except (ValueError, TypeError):
                continue

            pr_number = pr_data.get("number", 0)
            pr_title = pr_data.get("title", "")
            pr_url = pr_data.get("html_url", "")
            head_sha = pr_data.get("head", {}).get("sha", "")
            if not head_sha:
                continue

            prs_examined += 1

            # Fetch check runs for this PR's head commit
            checks_url = f"https://api.github.com/repos/{github_repository}/commits/{head_sha}/check-runs"
            try:
                checks_resp = requests.get(
                    checks_url,
                    params={"per_page": "100"},
                    headers=headers,
                    timeout=30,
                )
            except requests.RequestException:
                continue
            if checks_resp.status_code != 200:
                continue

            for check_run in checks_resp.json().get("check_runs", []):
                conclusion = check_run.get("conclusion", "")
                if conclusion in ("failure", "timed_out"):
                    name = check_run.get("name", "unknown")
                    if name not in failing_checks:
                        failing_checks[name] = []
                    failing_checks[name].append(
                        {
                            "pr_number": pr_number,
                            "pr_title": pr_title,
                            "pr_url": pr_url,
                            "check_name": name,
                            "check_url": check_run.get("html_url", ""),
                            "completed_at": check_run.get("completed_at", ""),
                        }
                    )
    except (requests.RequestException, ValueError):
        console.print("  [dim]Could not fetch recent PR failures.[/]")

    # Only keep checks that fail in at least 2 PRs — that's the "consistent pattern" signal
    consistent_failures: dict[str, list[dict]] = {
        name: prs_list for name, prs_list in failing_checks.items() if len(prs_list) >= 2
    }

    failing_check_names = set(consistent_failures.keys())
    if failing_check_names:
        console.print(
            f"  [warning]{len(failing_check_names)} "
            f"{'checks' if len(failing_check_names) != 1 else 'check'} "
            f"failing consistently across recent PRs: "
            f"{', '.join(sorted(failing_check_names)[:5])}"
            f"{'...' if len(failing_check_names) > 5 else ''}[/]"
        )
    else:
        console.print(f"  [success]No consistent failure patterns in recent PRs to {branch}.[/]")

    return RecentPRFailureInfo(
        failing_checks=consistent_failures,
        failing_check_names=failing_check_names,
        prs_examined=prs_examined,
    )


def _display_recent_pr_failure_panel(
    pr: PRData,
    recent_failures: RecentPRFailureInfo,
    matching_checks: list[str],
) -> None:
    """Display a Rich panel explaining that PR failures are a consistent pattern across recent PRs."""
    console = get_console()

    lines = [
        "[yellow]Some failing checks in this PR also fail in other recent PRs — "
        "likely a systemic issue, not caused by this PR.[/]",
        "",
        f"[bold]Consistent failures (across {recent_failures.prs_examined} recent PRs):[/]",
    ]
    for check in matching_checks[:10]:
        pr_entries = recent_failures.failing_checks.get(check, [])
        if pr_entries:
            # Show which other PRs have the same failure — link to their checks tab
            other_prs = [e for e in pr_entries if e.get("pr_number") != pr.number][:3]
            pr_refs = ", ".join(
                f"[link={e['pr_url']}/checks]#{e['pr_number']}[/link]"
                if e.get("pr_url")
                else f"#{e['pr_number']}"
                for e in other_prs
            )
            lines.append(f"  - {check} (also fails in {pr_refs})")
        else:
            lines.append(f"  - {check}")

    lines.append("")
    lines.append(
        "[dim]These failures are likely not caused by this PR's changes. "
        "Consider skipping or rerunning checks.[/]"
    )

    console.print(
        Panel(
            "\n".join(lines),
            title="Consistent CI Failure Pattern Detected",
            border_style="yellow",
        )
    )


@pr_group.command(
    name="auto-triage",
    help="Find open PRs from non-collaborators that don't meet quality criteria and convert to draft.",
)
@option_github_token
@option_github_repository
# --- Target selection ---
@click.option(
    "--pr",
    "pr_number",
    type=int,
    default=None,
    help="Triage a specific PR by number instead of searching.",
)
# --- Select people ---
@click.option(
    "--author",
    "filter_user",
    default=None,
    help="Filter PRs to a specific author.",
)
@click.option(
    "--include-collaborators",
    is_flag=True,
    default=False,
    help="Include PRs from collaborators/members/owners (normally skipped).",
)
@click.option(
    "--reviews-for-me",
    "my_reviews",
    is_flag=True,
    default=False,
    help="Only show PRs where review is requested for the authenticated user.",
)
@click.option(
    "--reviews-for",
    "reviewers",
    type=HiddenChoiceWithCompletion(_load_collaborators_cache("apache/airflow")),
    multiple=True,
    default=(),
    help="Only show PRs where review is requested for this user. Can be repeated.",
)
# --- Filter options ---
@click.option(
    "--label",
    "labels",
    type=NotVerifiedBetterChoice(_load_labels_from_boring_cyborg()),
    multiple=True,
    help="Filter PRs by label. Supports wildcards (e.g. 'area:*', 'provider:amazon*'). Can be repeated.",
)
@click.option(
    "--exclude-label",
    "exclude_labels",
    type=str,
    multiple=True,
    help="Exclude PRs with this label. Supports wildcards. Can be repeated.",
)
@click.option(
    "--created-after",
    default=None,
    help="Only PRs created on or after this date (YYYY-MM-DD).",
)
@click.option(
    "--created-before",
    default=None,
    help="Only PRs created on or before this date (YYYY-MM-DD).",
)
@click.option(
    "--updated-after",
    default=None,
    help="Only PRs updated on or after this date (YYYY-MM-DD).",
)
@click.option(
    "--updated-before",
    default=None,
    help="Only PRs updated on or before this date (YYYY-MM-DD).",
)
@click.option(
    "--include-drafts",
    is_flag=True,
    default=False,
    help="Include draft PRs in triage (normally skipped). Passing drafts can be marked as ready for review.",
)
@click.option(
    "--pending-approval-only",
    is_flag=True,
    default=False,
    help="Only show PRs with workflow runs awaiting approval.",
)
@click.option(
    "--mode",
    "triage_mode",
    type=click.Choice(["triage", "review"]),
    default="triage",
    show_default=True,
    help="Operating mode. 'triage': assess PRs for quality issues (default). "
    "'review': only select PRs with 'ready for maintainer review' label, "
    "run deterministic checks first, then LLM code review with line-level comments.",
)
@click.option(
    "--checks-state",
    type=click.Choice(["failure", "success", "pending", "any"]),
    default="any",
    show_default=True,
    help="Only assess PRs with this CI checks state.",
)
@click.option(
    "--min-commits-behind",
    type=int,
    default=0,
    show_default=True,
    help="Only assess PRs that are at least this many commits behind base branch.",
)
# --- Pagination and sorting ---
@click.option(
    "--batch-size",
    type=int,
    default=50,
    show_default=True,
    help="Number of PRs to fetch per GraphQL page.",
)
@click.option(
    "--max-num",
    type=int,
    default=0,
    show_default=True,
    help="Maximum number of non-collaborator PRs to assess (0 = no limit).",
)
@click.option(
    "--sort",
    type=click.Choice(["created-asc", "created-desc", "updated-asc", "updated-desc"]),
    default="created-desc",
    show_default=True,
    help="Sort order for PR search results.",
)
# --- Assessment options ---
@click.option(
    "--check-mode",
    type=click.Choice(["both", "api", "llm"]),
    default="both",
    show_default=True,
    help="Which checks to run: 'both' (API + LLM), 'api' (deterministic only), 'llm' (LLM only).",
)
@click.option(
    "--llm-concurrency",
    type=int,
    default=4,
    show_default=True,
    help="Number of concurrent LLM assessment calls.",
)
@option_llm_model
@click.option(
    "--clear-llm-cache",
    is_flag=True,
    default=False,
    help="Clear the LLM review and triage caches before running.",
)
# --- Action options ---
@click.option(
    "--answer-triage",
    type=click.Choice(["d", "c", "r", "s", "q", "y", "n"], case_sensitive=False),
    default=None,
    help="Force answer to triage prompts: [d]raft, [c]lose, [r]eady, [s]kip, [q]uit, [y]es, [n]o.",
)
@option_dry_run
@option_verbose
def auto_triage(
    github_token: str | None,
    github_repository: str,
    pr_number: int | None,
    labels: tuple[str, ...],
    exclude_labels: tuple[str, ...],
    batch_size: int,
    max_num: int,
    sort: str,
    filter_user: str | None,
    created_after: str | None,
    created_before: str | None,
    updated_after: str | None,
    updated_before: str | None,
    include_collaborators: bool,
    include_drafts: bool,
    pending_approval_only: bool,
    triage_mode: str,
    checks_state: str,
    min_commits_behind: int,
    my_reviews: bool,
    reviewers: tuple[str, ...],
    check_mode: str,
    llm_concurrency: int,
    llm_model: str,
    clear_llm_cache: bool,
    answer_triage: str | None,
):
    from airflow_breeze.utils.github import (
        PRAssessment,
        assess_pr_checks,
        assess_pr_conflicts,
        assess_pr_unresolved_comments,
    )
    from airflow_breeze.utils.llm_utils import (
        _check_cli_available,
        _resolve_cli_provider,
        check_llm_cli_safety,
    )

    token = _resolve_github_token(github_token)
    if not token:
        console_print(
            "[error]GitHub token not found. Provide --github-token, "
            "set GITHUB_TOKEN, or authenticate with `gh auth login`.[/]"
        )
        sys.exit(1)

    run_api = check_mode in ("both", "api")
    run_llm = check_mode in ("both", "llm")

    console = get_console()

    if clear_llm_cache:
        import shutil

        for label, get_dir in [
            ("review", _get_review_cache_dir),
            ("triage", _get_triage_cache_dir),
        ]:
            cache_dir = get_dir(github_repository)
            if cache_dir.exists():
                count = len(list(cache_dir.glob("*.json")))
                shutil.rmtree(cache_dir)
                console.print(f"[info]Cleared LLM {label} cache ({count} entries) at {cache_dir}.[/]")
            else:
                console.print(f"[info]LLM {label} cache is already empty.[/]")
    mode_desc = {"both": "API + LLM", "api": "API only", "llm": "LLM only"}
    console.print(
        f"[info]Check mode: [bold]{check_mode}[/bold] ({mode_desc.get(check_mode, check_mode)}). "
        f"Change with --check-mode (api|llm|both).[/]"
    )
    review_mode = triage_mode == "review"
    if review_mode:
        console.print(
            f"[info]Mode: [bold]review[/bold] — selecting PRs with '{_READY_FOR_REVIEW_LABEL}' label"
            f"{' and PRs with your previous reviews' if my_reviews or reviewers else ''}. "
            f"Deterministic checks run first; passing PRs get LLM code review if enabled.[/]"
        )

    # Validate CLI tool is available and safe early (only when LLM checks are enabled)
    if run_llm:
        provider, model = _resolve_cli_provider(llm_model)
        _check_cli_available(provider)
        if not check_llm_cli_safety(provider, model):
            run_llm = False
        else:
            _validate_llm_safety(github_repository, answer_triage)

    dry_run = get_dry_run()

    # Validate --reviews-for-me and --reviews-for are mutually exclusive
    if my_reviews and reviewers:
        console_print("[error]--reviews-for-me and --reviews-for are mutually exclusive.[/]")
        sys.exit(1)

    # Resolve the authenticated user login (used for --reviews-for-me and triage comment detection)
    viewer_login = _resolve_viewer_login(token)

    # Refresh collaborators cache in the background on every run
    _refresh_collaborators_cache_in_background(token, github_repository)

    # Preload main branch CI failure information
    main_failures = _fetch_recent_pr_failures(token, github_repository)

    # Show status of recent scheduled (canary) builds on main branch
    canary_builds = _fetch_main_canary_builds(token, github_repository)
    _display_canary_builds_status(canary_builds, token, github_repository)

    # Resolve review-requested filter: --reviews-for-me uses authenticated user, --reviews-for uses specified users
    review_requested_user: str | None = None
    review_requested_users: list[str] = []
    if my_reviews:
        review_requested_user = viewer_login
        review_requested_users = [viewer_login]
        console_print(f"[info]Filtering PRs with review requested for: {review_requested_user}[/]")
    elif reviewers:
        review_requested_users = list(reviewers)
        review_requested_user = reviewers[0]
        console_print(f"[info]Filtering PRs with review requested for: {', '.join(reviewers)}[/]")

    # Phase 1: Fetch PRs via GraphQL
    from fnmatch import fnmatch

    # In review mode, force-include the "ready for maintainer review" label in the search
    # (unless we also fetch reviewed-by PRs below, in which case the label search is one of two queries)
    effective_labels = labels
    if review_mode and _READY_FOR_REVIEW_LABEL not in labels:
        effective_labels = (*labels, _READY_FOR_REVIEW_LABEL)

    exact_labels = tuple(lbl for lbl in effective_labels if "*" not in lbl and "?" not in lbl)
    wildcard_labels = [lbl for lbl in effective_labels if "*" in lbl or "?" in lbl]
    exact_exclude_labels = tuple(lbl for lbl in exclude_labels if "*" not in lbl and "?" not in lbl)
    wildcard_exclude_labels = [lbl for lbl in exclude_labels if "*" in lbl or "?" in lbl]

    t_total_start = time.monotonic()

    # Phase 1: Lightweight fetch of PRs via GraphQL (no check contexts — fast)
    t_phase1_start = time.monotonic()
    has_next_page = False
    next_cursor: str | None = None
    if pr_number:
        console_print(f"[info]Fetching PR #{pr_number} via GraphQL...[/]")
        all_prs = [_fetch_single_pr_graphql(token, github_repository, pr_number)]
    elif len(review_requested_users) > 1:
        # Multiple reviewers: fetch PRs for each reviewer and merge (deduplicate)
        console_print("[info]Fetching PRs via GraphQL for multiple reviewers...[/]")
        seen_numbers: set[int] = set()
        all_prs = []
        for reviewer in review_requested_users:
            batch_prs, _, _ = _fetch_prs_graphql(
                token,
                github_repository,
                labels=exact_labels,
                exclude_labels=exact_exclude_labels,
                filter_user=filter_user,
                sort=sort,
                batch_size=batch_size,
                created_after=created_after,
                created_before=created_before,
                updated_after=updated_after,
                updated_before=updated_before,
                review_requested=reviewer,
            )
            for pr in batch_prs:
                if pr.number not in seen_numbers:
                    seen_numbers.add(pr.number)
                    all_prs.append(pr)
        # Disable pagination for multi-reviewer queries
        has_next_page = False
    else:
        console_print("[info]Fetching PRs via GraphQL...[/]")
        all_prs, has_next_page, next_cursor = _fetch_prs_graphql(
            token,
            github_repository,
            labels=exact_labels,
            exclude_labels=exact_exclude_labels,
            filter_user=filter_user,
            sort=sort,
            batch_size=batch_size,
            created_after=created_after,
            created_before=created_before,
            updated_after=updated_after,
            updated_before=updated_before,
            review_requested=review_requested_user,
        )

    # Apply wildcard label filters client-side
    if wildcard_labels:
        all_prs = [
            pr for pr in all_prs if any(fnmatch(lbl, pat) for pat in wildcard_labels for lbl in pr.labels)
        ]
    if wildcard_exclude_labels:
        all_prs = [
            pr
            for pr in all_prs
            if not any(fnmatch(lbl, pat) for pat in wildcard_exclude_labels for lbl in pr.labels)
        ]

    # In review mode with a reviewer filter, also fetch PRs where the reviewer has already
    # submitted a review (e.g. CHANGES_REQUESTED). These won't have the "ready for maintainer
    # review" label but should still appear in review mode so the reviewer can follow up.
    reviewed_by_prs: set[int] = set()
    if review_mode and review_requested_users:
        seen_numbers = {pr.number for pr in all_prs}
        # Use base labels (without the ready-for-review label) for the reviewed-by query
        base_exact_labels = tuple(lbl for lbl in labels if "*" not in lbl and "?" not in lbl)
        for reviewer in review_requested_users:
            batch_prs, _, _ = _fetch_prs_graphql(
                token,
                github_repository,
                labels=base_exact_labels,
                exclude_labels=exact_exclude_labels,
                filter_user=filter_user,
                sort=sort,
                batch_size=batch_size,
                created_after=created_after,
                created_before=created_before,
                updated_after=updated_after,
                updated_before=updated_before,
                reviewed_by=reviewer,
            )
            for pr in batch_prs:
                if pr.number not in seen_numbers:
                    seen_numbers.add(pr.number)
                    all_prs.append(pr)
                    reviewed_by_prs.add(pr.number)
        if reviewed_by_prs:
            console.print(
                f"[info]Also found {len(reviewed_by_prs)} "
                f"{'PRs' if len(reviewed_by_prs) != 1 else 'PR'} "
                f"previously reviewed by {', '.join(review_requested_users)}.[/]"
            )

    # Resolve how far behind base branch each PR is
    console_print("[info]Checking how far behind base branch each PR is...[/]")
    behind_map = _fetch_commits_behind_batch(token, github_repository, all_prs)
    for pr in all_prs:
        pr.commits_behind = behind_map.get(pr.number, 0)

    # Resolve UNKNOWN mergeable status before displaying the overview table
    unknown_count = sum(1 for pr in all_prs if pr.mergeable == "UNKNOWN")
    if unknown_count:
        console_print(
            f"[info]Resolving merge conflict status for {unknown_count} "
            f"{'PRs' if unknown_count != 1 else 'PR'} with unknown status...[/]"
        )
        resolved = _resolve_unknown_mergeable(token, github_repository, all_prs)
        remaining = unknown_count - resolved
        if remaining:
            console_print(
                f"  [dim]{resolved} resolved, {remaining} still unknown "
                f"(GitHub hasn't computed mergeability yet).[/]"
            )
        else:
            console_print(f"  [dim]All {resolved} resolved.[/]")

    # Detect PRs whose rollup state is SUCCESS but only have bot/labeler checks (no real CI).
    # These need to be reclassified as NOT_RUN so they get routed to workflow approval.
    non_collab_success = [
        pr
        for pr in all_prs
        if pr.checks_state == "SUCCESS"
        and pr.author_association not in _COLLABORATOR_ASSOCIATIONS
        and not _is_bot_account(pr.author_login)
    ]
    if non_collab_success:
        console_print(
            f"[info]Verifying CI status for {len(non_collab_success)} "
            f"{'PRs' if len(non_collab_success) != 1 else 'PR'} "
            f"showing SUCCESS (checking for real test checks)...[/]"
        )
        _fetch_check_details_batch(token, github_repository, non_collab_success)
        reclassified = sum(1 for pr in non_collab_success if pr.checks_state == "NOT_RUN")
        if reclassified:
            console_print(
                f"  [warning]{reclassified} {'PRs' if reclassified != 1 else 'PR'} "
                f"reclassified to NOT_RUN (only bot/labeler checks, no real CI).[/]"
            )

    # Filter candidates first
    candidate_prs, accepted_prs, total_skipped_collaborator, total_skipped_bot, total_skipped_accepted = (
        _filter_candidate_prs(
            all_prs,
            include_collaborators=include_collaborators,
            include_drafts=include_drafts,
            checks_state=checks_state,
            min_commits_behind=min_commits_behind,
            max_num=max_num,
            also_accepted=reviewed_by_prs if review_mode else None,
        )
    )

    # Exclude PRs that already have a triage comment posted after the last commit
    console_print("[info]Checking for PRs already triaged (no new commits since last triage comment)...[/]")
    triaged_classification = _classify_already_triaged_prs(
        token, github_repository, candidate_prs, viewer_login
    )
    already_triaged_nums = triaged_classification["waiting"] | triaged_classification["responded"]
    triaged_waiting_count = len(triaged_classification["waiting"])
    triaged_responded_count = len(triaged_classification["responded"])
    already_triaged: list[PRData] = []
    if already_triaged_nums:
        already_triaged = [pr for pr in candidate_prs if pr.number in already_triaged_nums]
        candidate_prs = [pr for pr in candidate_prs if pr.number not in already_triaged_nums]
        console_print(
            f"[info]Skipped {len(already_triaged)} already-triaged "
            f"{'PRs' if len(already_triaged) != 1 else 'PR'} "
            f"({triaged_waiting_count} commented, "
            f"{triaged_responded_count} author responded).[/]"
        )
    else:
        console_print("  [dim]None found.[/]")

    # Display overview table (after triaged detection so we can mark actionable PRs)
    _display_pr_overview_table(
        all_prs,
        triaged_waiting_nums=triaged_classification["waiting"],
        triaged_responded_nums=triaged_classification["responded"],
    )

    t_phase1_end = time.monotonic()

    # --- Review mode: early exit into review flow for accepted PRs ---
    if review_mode:
        stats = TriageStats()
        ctx = TriageContext(
            token=token,
            github_repository=github_repository,
            dry_run=dry_run,
            answer_triage=answer_triage,
            stats=stats,
            author_flagged_count={},
            main_failures=main_failures,
            llm_future_to_pr={},
            llm_assessments={},
            llm_completed=[],
            llm_errors=[],
            llm_passing=[],
        )

        pr_timings: dict[int, float] = {}
        pr_actions: dict[int, str] = {}
        if not accepted_prs:
            get_console().print(
                f"[info]No PRs with '{_READY_FOR_REVIEW_LABEL}' label found matching the filters.[/]"
            )
        else:
            try:
                pr_timings, pr_actions = _review_ready_prs_review_mode(
                    ctx,
                    accepted_prs,
                    run_api=run_api,
                    run_llm=run_llm,
                    llm_model=llm_model,
                    llm_concurrency=llm_concurrency,
                )
            except KeyboardInterrupt:
                get_console().print("\n[warning]Interrupted — shutting down.[/]")
                stats.quit_early = True

        t_total_end = time.monotonic()
        total_elapsed = t_total_end - t_total_start
        console.print(f"\n[info]Review mode complete in {_fmt_duration(total_elapsed)}.[/]")

        # Simple summary for review mode
        summary_table = Table(title="Review Mode Summary")
        summary_table.add_column("Metric", style="bold")
        summary_table.add_column("Count", justify="right")
        summary_table.add_row("PRs assessed", str(len(accepted_prs)))
        summary_table.add_row("PRs converted to draft", str(stats.total_converted))
        summary_table.add_row("PRs commented (triage)", str(stats.total_commented))
        summary_table.add_row("PRs closed", str(stats.total_closed))
        summary_table.add_row("PRs rebased", str(stats.total_rebased))
        summary_table.add_row("Review comments posted", str(stats.total_review_comments))
        summary_table.add_row("Overall reviews submitted", str(stats.total_reviews_submitted))
        summary_table.add_row("PRs skipped", str(stats.total_skipped_action))
        console.print(summary_table)

        # Timing summary
        timing_table = Table(title="Timing Summary")
        timing_table.add_column("Phase", style="bold")
        timing_table.add_column("Total", justify="right")
        timing_table.add_column("PRs", justify="right")
        timing_table.add_column("Avg/PR", justify="right")
        timing_table.add_column("Min/PR", justify="right")
        timing_table.add_column("Max/PR", justify="right")

        phase1_total = t_phase1_end - t_total_start
        num_all = len(all_prs) or 1
        timing_table.add_row(
            "Fetch PRs + overview",
            _fmt_duration(phase1_total),
            str(len(all_prs)),
            _fmt_duration(phase1_total / num_all),
            "[dim]—[/]",
            "[dim]—[/]",
        )

        interactive_total = t_total_end - t_phase1_end
        timing_table.add_row(
            "Interactive review",
            _fmt_duration(interactive_total),
            str(len(accepted_prs)),
            _fmt_duration(interactive_total / len(accepted_prs)) if accepted_prs else "[dim]—[/]",
            "[dim]—[/]",
            "[dim]—[/]",
        )

        timing_table.add_row("", "", "", "", "", "")
        prs_with_timing = len(pr_timings)
        avg_per_pr = _fmt_duration(total_elapsed / prs_with_timing) if prs_with_timing else "[dim]—[/]"
        timing_table.add_row(
            "[bold]Total[/]",
            f"[bold]{_fmt_duration(total_elapsed)}[/]",
            str(prs_with_timing) if prs_with_timing else "",
            f"[bold]{avg_per_pr}[/]" if prs_with_timing else "",
            "",
            "",
        )
        console.print(timing_table)

        # Per-PR timing table
        if pr_timings:
            action_styles = {
                "reviewed": "[success]reviewed[/]",
                "drafted": "[yellow]drafted[/]",
                "commented": "[yellow]commented[/]",
                "closed": "[red]closed[/]",
                "skipped": "[dim]skipped[/]",
                "llm-error": "[red]llm-error[/]",
                "quit": "[dim]quit[/]",
            }
            pr_map = {pr.number: pr for pr in accepted_prs}
            pr_timing_table = Table(title="Per-PR Timing")
            pr_timing_table.add_column("PR", style="bold")
            pr_timing_table.add_column("Title")
            pr_timing_table.add_column("Action")
            pr_timing_table.add_column("Time", justify="right")

            for pr_num in sorted(pr_timings, key=lambda n: pr_timings[n], reverse=True):
                pr_entry = pr_map.get(pr_num)
                title = (pr_entry.title[:60] if pr_entry else "") or ""
                action_raw = pr_actions.get(pr_num, "")
                action_display = action_styles.get(action_raw, f"[dim]{action_raw or '—'}[/]")
                pr_timing_table.add_row(
                    f"#{pr_num}",
                    title,
                    action_display,
                    _fmt_duration(pr_timings[pr_num]),
                )
            console.print(pr_timing_table)

        return

    # Enrich candidate PRs with check details, mergeable status, and review comments
    t_enrich_start = time.monotonic()
    _enrich_candidate_details(token, github_repository, candidate_prs, run_api=run_api)
    t_enrich_end = time.monotonic()

    # Phase 3: Deterministic checks + categorize PRs
    assessments: dict[int, PRAssessment] = {}
    llm_candidates: list[PRData] = []
    passing_prs: list[PRData] = []
    pending_approval: list[PRData] = []
    workflows_in_progress: list[PRData] = []
    skipped_drafts: list[PRData] = []  # Draft PRs skipped because they have other issues
    recent_failures_skipped: list[PRData] = []  # PRs skipped because CI failures are < 24h old
    total_deterministic_flags = 0
    deterministic_timings: dict[int, float] = {}  # PR number -> deterministic triage duration

    def _categorize_pr(pr: PRData) -> None:
        """Route a PR to pending_approval, workflows_in_progress, or llm_candidates."""
        if pr.checks_state == "NOT_RUN":
            # No workflow runs yet — needs workflow approval.
            # But first check if workflows are already running — skip those.
            if pr.head_sha and _has_in_progress_workflows(token, github_repository, pr.head_sha):
                workflows_in_progress.append(pr)
            else:
                pending_approval.append(pr)
        elif (
            pr.is_draft and pr.head_sha and _has_in_progress_workflows(token, github_repository, pr.head_sha)
        ):
            # Draft PRs with workflows still running — author is still iterating
            workflows_in_progress.append(pr)
        else:
            llm_candidates.append(pr)

    if run_api:
        for pr in candidate_prs:
            t_det_start = time.monotonic()
            ci_assessment = assess_pr_checks(pr.number, pr.checks_state, pr.failed_checks)

            # Race condition: checks_state is FAILURE but workflows are currently running.
            # The rollup state may be stale from previous runs while new runs are in progress.
            # In that case, skip the CI failure and treat as workflows in progress.
            if (
                ci_assessment
                and pr.head_sha
                and _has_in_progress_workflows(token, github_repository, pr.head_sha)
            ):
                workflows_in_progress.append(pr)
                deterministic_timings[pr.number] = time.monotonic() - t_det_start
                continue

            # Grace period: don't flag CI failures within the grace window.
            # Default 24 h; extended to 96 h when a collaborator has already engaged.
            if (
                ci_assessment
                and pr.head_sha
                and _are_failures_recent(token, github_repository, pr.head_sha, pr.ci_grace_period_hours)
            ):
                ci_assessment = None
                # If CI is the only issue, skip this PR entirely
                conflict_check = assess_pr_conflicts(pr.number, pr.mergeable, pr.base_ref, pr.commits_behind)
                comments_check = assess_pr_unresolved_comments(
                    pr.number, pr.unresolved_review_comments, pr.unresolved_threads
                )
                if not conflict_check and not comments_check:
                    recent_failures_skipped.append(pr)
                    deterministic_timings[pr.number] = time.monotonic() - t_det_start
                    continue

            conflict_assessment = assess_pr_conflicts(pr.number, pr.mergeable, pr.base_ref, pr.commits_behind)
            comments_assessment = assess_pr_unresolved_comments(
                pr.number, pr.unresolved_review_comments, pr.unresolved_threads
            )

            if ci_assessment or conflict_assessment or comments_assessment:
                if pr.is_draft:
                    # Draft PRs with issues are skipped — the author is still working on them
                    skipped_drafts.append(pr)
                else:
                    total_deterministic_flags += 1
                    violations = []
                    summaries = []
                    if conflict_assessment:
                        violations.extend(conflict_assessment.violations)
                        summaries.append(conflict_assessment.summary)
                    if ci_assessment:
                        violations.extend(ci_assessment.violations)
                        summaries.append(ci_assessment.summary)
                    if comments_assessment:
                        violations.extend(comments_assessment.violations)
                        summaries.append(comments_assessment.summary)
                    assessments[pr.number] = PRAssessment(
                        should_flag=True,
                        violations=violations,
                        summary=" ".join(summaries),
                    )
            else:
                _categorize_pr(pr)
            deterministic_timings[pr.number] = time.monotonic() - t_det_start
    else:
        for pr in candidate_prs:
            _categorize_pr(pr)

    if skipped_drafts:
        console_print(
            f"[info]Skipped {len(skipped_drafts)} draft "
            f"{'PRs' if len(skipped_drafts) != 1 else 'PR'} "
            f"with existing issues (CI failures, conflicts, or unresolved comments).[/]"
        )
    if workflows_in_progress:
        console_print(
            f"[info]Excluded {len(workflows_in_progress)} "
            f"{'PRs' if len(workflows_in_progress) != 1 else 'PR'} "
            f"with workflows already in progress.[/]"
        )
    if recent_failures_skipped:
        get_console().print(
            f"[info]Skipped {len(recent_failures_skipped)} "
            f"{'PRs' if len(recent_failures_skipped) != 1 else 'PR'} "
            f"with recent CI failures within grace period "
            f"(giving authors time to address at their own pace):[/]"
        )
        for pr in recent_failures_skipped:
            grace = pr.ci_grace_period_hours
            engaged = " (collaborator engaged)" if pr.has_collaborator_review else ""
            get_console().print(f"  [dim]{_pr_link(pr)} — {pr.title[:70]} [<{grace}h{engaged}][/]")

    # Filter out pending_approval PRs that already have a comment from the viewer
    # (triage or rebase comment) with no new commits since — no point re-approving
    if pending_approval and viewer_login:
        already_commented_nums = _find_already_triaged_prs(
            token, github_repository, pending_approval, viewer_login, require_marker=False
        )
        if already_commented_nums:
            already_triaged.extend(pr for pr in pending_approval if pr.number in already_commented_nums)
            pending_approval = [pr for pr in pending_approval if pr.number not in already_commented_nums]
            console_print(
                f"[info]Skipped {len(already_commented_nums)} workflow-approval "
                f"{'PRs' if len(already_commented_nums) != 1 else 'PR'} "
                f"already commented on (no new commits since).[/]"
            )

    # If --pending-approval-only, discard all assessment results and only keep pending_approval
    if pending_approval_only:
        assessments = {}
        llm_candidates = []
        total_deterministic_flags = 0
        if not pending_approval:
            console_print("[success]No PRs with pending workflow approvals found.[/]")
            sys.exit(0)
        console_print(
            f"[info]--pending-approval-only: found {len(pending_approval)} "
            f"{'PRs' if len(pending_approval) != 1 else 'PR'} awaiting workflow approval.[/]\n"
        )

    # Phase 4: Start LLM assessments in background (non-blocking)
    llm_future_to_pr: dict = {}
    llm_assessments: dict[int, PRAssessment] = {}
    llm_completed: list = []
    llm_errors: list[int] = []
    llm_passing: list[PRData] = []
    llm_executor = None

    if not run_llm:
        if llm_candidates:
            console_print(
                f"\n[info]--check-mode=api: skipping LLM assessment for {len(llm_candidates)} "
                f"{'PRs' if len(llm_candidates) != 1 else 'PR'}.[/]\n"
            )
            passing_prs.extend(llm_candidates)
    elif llm_candidates:
        skipped_detail = f"{total_deterministic_flags} CI/conflicts/comments"
        if skipped_drafts:
            skipped_detail += f", {len(skipped_drafts)} drafts with issues"
        if recent_failures_skipped:
            skipped_detail += f", {len(recent_failures_skipped)} recent CI failures"
        if already_triaged:
            skipped_detail += f", {len(already_triaged)} already triaged"
        if pending_approval:
            skipped_detail += f", {len(pending_approval)} awaiting workflow approval"
        if workflows_in_progress:
            skipped_detail += f", {len(workflows_in_progress)} workflows in progress"
        console_print(
            f"\n[info]Starting LLM assessment for {len(llm_candidates)} "
            f"{'PRs' if len(llm_candidates) != 1 else 'PR'} in background "
            f"(skipped {skipped_detail})...[/]\n"
        )
        llm_executor = ThreadPoolExecutor(max_workers=llm_concurrency)
        llm_future_to_pr = {
            llm_executor.submit(
                _cached_assess_pr,
                github_repository=github_repository,
                head_sha=pr.head_sha,
                pr_number=pr.number,
                pr_title=pr.title,
                pr_body=pr.body,
                check_status_summary=pr.check_summary,
                llm_model=llm_model,
            ): pr
            for pr in llm_candidates
        }

    # Launch background CI log fetching for PRs with failed checks
    log_futures = _launch_background_log_fetching(token, github_repository, candidate_prs, llm_concurrency)

    # Build shared triage context and stats
    pr_actions = {}  # PR number -> action taken by user

    author_flagged_count: dict[str, int] = dict(
        Counter(pr.author_login for pr in candidate_prs if pr.number in assessments)
    )
    stats = TriageStats()
    ctx = TriageContext(
        token=token,
        github_repository=github_repository,
        dry_run=dry_run,
        answer_triage=answer_triage,
        stats=stats,
        author_flagged_count=author_flagged_count,
        main_failures=main_failures,
        llm_future_to_pr=llm_future_to_pr,
        llm_assessments=llm_assessments,
        llm_completed=llm_completed,
        llm_errors=llm_errors,
        llm_passing=llm_passing,
        log_futures=log_futures,
    )

    try:
        # Phase 4b: Present NOT_RUN PRs for workflow approval (LLM runs in background)
        _review_workflow_approval_prs(ctx, pending_approval)

        # Phase 5a: Present deterministically flagged PRs
        det_flagged_prs = [(pr, assessments[pr.number]) for pr in candidate_prs if pr.number in assessments]
        det_flagged_prs.sort(key=lambda pair: (pair[0].author_login.lower(), pair[0].number))
        _review_deterministic_flagged_prs(ctx, det_flagged_prs)

        # Phase 5b: Present LLM-flagged PRs as they become ready (streaming)
        _review_llm_flagged_prs(ctx, llm_candidates)

        # Add LLM passing PRs to the passing list
        passing_prs.extend(llm_passing)

        # Phase 5c: Present passing PRs for optional ready-for-review marking
        _review_passing_prs(ctx, passing_prs)

        # Phase 5d: Check accepted PRs for stale CHANGES_REQUESTED reviews
        _review_stale_review_requests(ctx, accepted_prs)
    except KeyboardInterrupt:
        console_print("\n[warning]Interrupted — shutting down.[/]")
        stats.quit_early = True
    finally:
        # Shut down LLM executor if it was started
        if llm_executor is not None:
            llm_executor.shutdown(wait=False, cancel_futures=True)

    # Fetch and process next batch if available and user hasn't quit
    while has_next_page and not stats.quit_early and not pr_number:
        batch_num = getattr(stats, "_batch_count", 1) + 1
        stats._batch_count = batch_num  # type: ignore[attr-defined]
        console_print(f"\n[info]Batch complete. Fetching next batch (page {batch_num})...[/]\n")
        all_prs, has_next_page, next_cursor = _fetch_prs_graphql(
            token,
            github_repository,
            labels=exact_labels,
            exclude_labels=exact_exclude_labels,
            filter_user=filter_user,
            sort=sort,
            batch_size=batch_size,
            created_after=created_after,
            created_before=created_before,
            updated_after=updated_after,
            updated_before=updated_before,
            review_requested=review_requested_user,
            after_cursor=next_cursor,
        )
        if not all_prs:
            console_print("[info]No more PRs to process.[/]")
            break

        # Apply wildcard label filters client-side
        if wildcard_labels:
            all_prs = [
                pr for pr in all_prs if any(fnmatch(lbl, pat) for pat in wildcard_labels for lbl in pr.labels)
            ]
        if wildcard_exclude_labels:
            all_prs = [
                pr
                for pr in all_prs
                if not any(fnmatch(lbl, pat) for pat in wildcard_exclude_labels for lbl in pr.labels)
            ]

        # Enrich: commits behind, mergeable status
        behind_map = _fetch_commits_behind_batch(token, github_repository, all_prs)
        for pr in all_prs:
            pr.commits_behind = behind_map.get(pr.number, 0)
        unknown_count = sum(1 for pr in all_prs if pr.mergeable == "UNKNOWN")
        if unknown_count:
            _resolve_unknown_mergeable(token, github_repository, all_prs)

        # Detect PRs whose rollup state is SUCCESS but only have bot/labeler checks
        batch_non_collab_success = [
            pr
            for pr in all_prs
            if pr.checks_state == "SUCCESS"
            and pr.author_association not in _COLLABORATOR_ASSOCIATIONS
            and not _is_bot_account(pr.author_login)
        ]
        if batch_non_collab_success:
            console_print(
                f"[info]Verifying CI status for {len(batch_non_collab_success)} "
                f"{'PRs' if len(batch_non_collab_success) != 1 else 'PR'} "
                f"showing SUCCESS...[/]"
            )
            _fetch_check_details_batch(token, github_repository, batch_non_collab_success)
            reclassified = sum(1 for pr in batch_non_collab_success if pr.checks_state == "NOT_RUN")
            if reclassified:
                console_print(
                    f"  [warning]{reclassified} {'PRs' if reclassified != 1 else 'PR'} "
                    f"reclassified to NOT_RUN (only bot/labeler checks).[/]"
                )

        (
            candidate_prs,
            batch_accepted,
            _,
            _,
            _,
        ) = _filter_candidate_prs(
            all_prs,
            include_collaborators=include_collaborators,
            include_drafts=include_drafts,
            checks_state=checks_state,
            min_commits_behind=min_commits_behind,
            max_num=max_num,
        )
        accepted_prs.extend(batch_accepted)

        if not candidate_prs:
            console_print("[info]No candidates in this batch.[/]")
            _display_pr_overview_table(all_prs)
            continue

        # Check already-triaged
        batch_triaged_cls = _classify_already_triaged_prs(
            token, github_repository, candidate_prs, viewer_login
        )
        batch_triaged_nums = batch_triaged_cls["waiting"] | batch_triaged_cls["responded"]
        if batch_triaged_nums:
            candidate_prs = [pr for pr in candidate_prs if pr.number not in batch_triaged_nums]

        _display_pr_overview_table(
            all_prs,
            triaged_waiting_nums=batch_triaged_cls["waiting"],
            triaged_responded_nums=batch_triaged_cls["responded"],
        )

        if not candidate_prs:
            console_print("[info]All PRs in this batch already triaged.[/]")
            continue

        # Enrich and assess
        _enrich_candidate_details(token, github_repository, candidate_prs, run_api=run_api)

        batch_assessments: dict[int, PRAssessment] = {}
        batch_llm_candidates: list[PRData] = []
        batch_passing: list[PRData] = []
        batch_pending: list[PRData] = []

        if run_api:
            for pr in candidate_prs:
                ci_assessment = assess_pr_checks(pr.number, pr.checks_state, pr.failed_checks)
                if (
                    ci_assessment
                    and pr.head_sha
                    and _has_in_progress_workflows(token, github_repository, pr.head_sha)
                ):
                    continue
                # Grace period: don't flag CI failures within the grace window.
                # Default 24 h; extended to 96 h when a collaborator has already engaged.
                if (
                    ci_assessment
                    and pr.head_sha
                    and _are_failures_recent(token, github_repository, pr.head_sha, pr.ci_grace_period_hours)
                ):
                    ci_assessment = None
                    # If CI is the only issue, skip this PR entirely
                    conflict_check = assess_pr_conflicts(
                        pr.number, pr.mergeable, pr.base_ref, pr.commits_behind
                    )
                    comments_check = assess_pr_unresolved_comments(
                        pr.number, pr.unresolved_review_comments, pr.unresolved_threads
                    )
                    if not conflict_check and not comments_check:
                        get_console().print(
                            f"  [dim]Skipped {_pr_link(pr)} — CI failures less than "
                            f"{pr.ci_grace_period_hours}h old"
                            f"{' (collaborator engaged)' if pr.has_collaborator_review else ''}[/]"
                        )
                        continue
                conflict_assessment = assess_pr_conflicts(
                    pr.number, pr.mergeable, pr.base_ref, pr.commits_behind
                )
                comments_assessment = assess_pr_unresolved_comments(
                    pr.number, pr.unresolved_review_comments, pr.unresolved_threads
                )
                if ci_assessment or conflict_assessment or comments_assessment:
                    if pr.is_draft:
                        continue
                    violations = []
                    summaries = []
                    if conflict_assessment:
                        violations.extend(conflict_assessment.violations)
                        summaries.append(conflict_assessment.summary)
                    if ci_assessment:
                        violations.extend(ci_assessment.violations)
                        summaries.append(ci_assessment.summary)
                    if comments_assessment:
                        violations.extend(comments_assessment.violations)
                        summaries.append(comments_assessment.summary)
                    batch_assessments[pr.number] = PRAssessment(
                        should_flag=True,
                        violations=violations,
                        summary=" ".join(summaries),
                    )
                elif pr.checks_state == "NOT_RUN":
                    batch_pending.append(pr)
                else:
                    batch_llm_candidates.append(pr)
        else:
            for pr in candidate_prs:
                if pr.checks_state == "NOT_RUN":
                    batch_pending.append(pr)
                else:
                    batch_llm_candidates.append(pr)

        # LLM assessment for this batch
        batch_llm_future_to_pr: dict = {}
        batch_llm_assessments: dict[int, PRAssessment] = {}
        batch_llm_completed: list = []
        batch_llm_errors: list[int] = []
        batch_llm_passing: list[PRData] = []
        batch_executor = None

        if not run_llm:
            batch_passing.extend(batch_llm_candidates)
        elif batch_llm_candidates:
            batch_executor = ThreadPoolExecutor(max_workers=llm_concurrency)
            batch_llm_future_to_pr = {
                batch_executor.submit(
                    _cached_assess_pr,
                    github_repository=github_repository,
                    head_sha=pr.head_sha,
                    pr_number=pr.number,
                    pr_title=pr.title,
                    pr_body=pr.body,
                    check_status_summary=pr.check_summary,
                    llm_model=llm_model,
                ): pr
                for pr in batch_llm_candidates
            }

        # Launch background CI log fetching for this batch
        batch_log_futures = _launch_background_log_fetching(
            token, github_repository, candidate_prs, llm_concurrency
        )

        batch_author_flagged_count: dict[str, int] = dict(
            Counter(pr.author_login for pr in candidate_prs if pr.number in batch_assessments)
        )
        batch_ctx = TriageContext(
            token=token,
            github_repository=github_repository,
            dry_run=dry_run,
            answer_triage=answer_triage,
            stats=stats,
            author_flagged_count=batch_author_flagged_count,
            main_failures=main_failures,
            llm_future_to_pr=batch_llm_future_to_pr,
            llm_assessments=batch_llm_assessments,
            llm_completed=batch_llm_completed,
            llm_errors=batch_llm_errors,
            llm_passing=batch_llm_passing,
            log_futures=batch_log_futures,
        )

        try:
            _review_workflow_approval_prs(batch_ctx, batch_pending)

            det_flagged = [
                (pr, batch_assessments[pr.number]) for pr in candidate_prs if pr.number in batch_assessments
            ]
            det_flagged.sort(key=lambda pair: (pair[0].author_login.lower(), pair[0].number))
            _review_deterministic_flagged_prs(batch_ctx, det_flagged)

            _review_llm_flagged_prs(batch_ctx, batch_llm_candidates)
            batch_passing.extend(batch_llm_passing)

            _review_passing_prs(batch_ctx, batch_passing)
            _review_stale_review_requests(batch_ctx, batch_accepted)
        except KeyboardInterrupt:
            console_print("\n[warning]Interrupted — shutting down.[/]")
            stats.quit_early = True
        finally:
            if batch_executor is not None:
                batch_executor.shutdown(wait=False, cancel_futures=True)

    # Display summary
    _display_triage_summary(
        all_prs,
        candidate_prs,
        passing_prs,
        pending_approval,
        workflows_in_progress,
        skipped_drafts,
        recent_failures_skipped,
        already_triaged,
        stats,
        total_deterministic_flags=total_deterministic_flags,
        total_llm_flagged=len(llm_assessments),
        total_llm_errors=len(llm_errors),
        total_llm_report=sum(1 for a in llm_assessments.values() if a.should_report),
        total_skipped_collaborator=total_skipped_collaborator,
        total_skipped_bot=total_skipped_bot,
        total_skipped_accepted=total_skipped_accepted,
        triaged_waiting_count=triaged_waiting_count,
        triaged_responded_count=triaged_responded_count,
    )

    # Timing summary
    t_total_end = time.monotonic()
    console_print()
    timing_table = Table(title="Timing Summary")
    timing_table.add_column("Phase", style="bold")
    timing_table.add_column("Total", justify="right")
    timing_table.add_column("PRs", justify="right")
    timing_table.add_column("Avg/PR", justify="right")
    timing_table.add_column("Min/PR", justify="right")
    timing_table.add_column("Max/PR", justify="right")

    num_all = len(all_prs) or 1
    num_candidates = len(candidate_prs) or 1

    phase1_total = t_phase1_end - t_phase1_start
    timing_table.add_row(
        "Fetch PRs + commits behind",
        _fmt_duration(phase1_total),
        str(len(all_prs)),
        _fmt_duration(phase1_total / num_all),
        "[dim]—[/]",
        "[dim]—[/]",
    )

    enrich_total = t_enrich_end - t_enrich_start
    timing_table.add_row(
        "Enrich candidates (checks + mergeability + comments)",
        _fmt_duration(enrich_total),
        str(len(candidate_prs)),
        _fmt_duration(enrich_total / num_candidates),
        "[dim]—[/]",
        "[dim]—[/]",
    )

    if deterministic_timings:
        det_values = list(deterministic_timings.values())
        det_total = sum(det_values)
        timing_table.add_row(
            "Deterministic triage",
            _fmt_duration(det_total),
            str(len(det_values)),
            _fmt_duration(det_total / len(det_values)),
            _fmt_duration(min(det_values)),
            _fmt_duration(max(det_values)),
        )
    else:
        timing_table.add_row("Deterministic triage", "[dim]—[/]", "0", "[dim]—[/]", "[dim]—[/]", "[dim]—[/]")

    llm_count = len(llm_completed)
    llm_wall_time = t_total_end - t_enrich_end  # LLM runs in background across all interactive phases
    if llm_count:
        timing_table.add_row(
            "LLM assessment (background)",
            _fmt_duration(llm_wall_time),
            str(llm_count),
            "[dim]—[/]",
            "[dim]—[/]",
            "[dim]—[/]",
        )
    else:
        timing_table.add_row(
            "LLM assessment (background)", "[dim]—[/]", "0", "[dim]—[/]", "[dim]—[/]", "[dim]—[/]"
        )

    timing_table.add_row(
        "Interactive review (overlaps LLM)",
        _fmt_duration(t_total_end - t_enrich_end),
        "",
        "",
        "",
        "",
    )
    timing_table.add_row("", "", "", "", "", "")
    total_elapsed = t_total_end - t_total_start
    prs_with_action = len(pr_actions)
    avg_per_actioned = _fmt_duration(total_elapsed / prs_with_action) if prs_with_action else "[dim]—[/]"
    timing_table.add_row(
        "[bold]Total[/]",
        f"[bold]{_fmt_duration(total_elapsed)}[/]",
        str(prs_with_action) if prs_with_action else "",
        f"[bold]{avg_per_actioned}[/]" if prs_with_action else "",
        "",
        "",
    )
    console_print(timing_table)

    if deterministic_timings:
        pr_titles = {pr.number: pr.title for pr in candidate_prs}
        pr_urls = {pr.number: pr.url for pr in candidate_prs}
        # Amortize batch fetch time evenly across candidate PRs
        num_candidates = len(candidate_prs) or 1
        fetch_per_pr = enrich_total / num_candidates

        action_styles = {
            "drafted": "[yellow]drafted[/]",
            "commented": "[yellow]commented[/]",
            "closed": "[red]closed[/]",
            "ready": "[success]ready[/]",
            "skipped": "[dim]skipped[/]",
            "approved": "[success]approved[/]",
            "suspicious": "[red]suspicious[/]",
        }

        pr_timing_table = Table(title="Per-PR Phase Timing")
        pr_timing_table.add_column("PR", style="bold")
        pr_timing_table.add_column("Title")
        pr_timing_table.add_column("Result")
        pr_timing_table.add_column("Action")
        pr_timing_table.add_column("Fetch (avg)", justify="right")
        pr_timing_table.add_column("Deterministic", justify="right")
        pr_timing_table.add_column("Total", justify="right")

        all_pr_numbers = sorted(
            deterministic_timings.keys(),
            key=lambda n: deterministic_timings.get(n, 0) + fetch_per_pr,
            reverse=True,
        )
        for pr_num in all_pr_numbers:
            title = pr_titles.get(pr_num, "")[:60]
            det_time = deterministic_timings.get(pr_num, 0)
            total_time = fetch_per_pr + det_time

            if any(pr.number == pr_num for pr in skipped_drafts):
                result = "[dim]draft-skipped[/]"
            elif pr_num in assessments or pr_num in llm_assessments:
                result = "[red]flagged[/]"
            elif any(pr.number == pr_num for pr in passing_prs) or any(
                pr.number == pr_num for pr in llm_passing
            ):
                result = "[success]passed[/]"
            elif any(pr.number == pr_num for pr in pending_approval):
                result = "[dim]pending[/]"
            elif any(pr.number == pr_num for pr in workflows_in_progress):
                result = "[cyan]running[/]"
            else:
                result = "[yellow]error[/]"

            action_raw = pr_actions.get(pr_num, "")
            action_display = action_styles.get(action_raw, f"[dim]{action_raw or '—'}[/]")

            pr_timing_table.add_row(
                f"[link={pr_urls.get(pr_num, '')}]#{pr_num}[/link]",
                title,
                result,
                action_display,
                _fmt_duration(fetch_per_pr),
                _fmt_duration(det_time) if det_time else "[dim]—[/]",
                _fmt_duration(total_time),
            )
        console_print(pr_timing_table)
