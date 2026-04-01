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
import os
import re
import sys
import threading
import time
from collections import Counter
from collections.abc import Callable
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from threading import Thread
from typing import TYPE_CHECKING, Any

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
from airflow_breeze.utils.confirm import (
    Answer,
    ContinueAction,
    TriageAction,
    _has_tty,
    prompt_space_continue,
    prompt_triage_action,
    user_confirm,
)
from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.custom_param_types import HiddenChoiceWithCompletion, NotVerifiedBetterChoice
from airflow_breeze.utils.pr_cache import (
    classification_cache as _classification_cache,
    get_cached_assessment as _get_cached_assessment,
    get_cached_classification as _get_cached_classification,
    get_cached_review as _get_cached_review,
    get_cached_status as _get_cached_status,
    review_cache as _review_cache,
    save_assessment_cache as _save_assessment_cache,
    save_classification_cache as _save_classification_cache,
    save_review_cache as _save_review_cache,
    save_status_cache as _save_status_cache,
    status_cache as _status_cache,
    triage_cache as _triage_cache,
)
from airflow_breeze.utils.pr_comments import (
    build_close_comment as _build_close_comment,
    build_comment as _build_comment,
    build_review_nudge_comment as _build_review_nudge_comment,
)
from airflow_breeze.utils.pr_display import (
    fmt_duration as _fmt_duration,
    human_readable_age as _human_readable_age,
    linkify_commit_hashes as _linkify_commit_hashes,
    pr_link as _pr_link,
    print_pr_header as _print_pr_header,
)
from airflow_breeze.utils.pr_models import (
    CHECK_FAILURE_GRACE_PERIOD_HOURS,
    PRData,
    UnresolvedThread,
)
from airflow_breeze.utils.recording import generating_command_images
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.shared_options import get_dry_run, get_verbose

if TYPE_CHECKING:
    from airflow_breeze.utils.github import PRAssessment
    from airflow_breeze.utils.tui_display import PRListEntry, TriageTUI, TUIAction

QUALITY_CRITERIA_LINK = (
    "[Pull Request quality criteria](https://github.com/apache/airflow/blob/main/"
    "contributing-docs/05_pull_requests.rst#pull-request-quality-criteria)"
)

# authorAssociation values that indicate the author has write access
_COLLABORATOR_ASSOCIATIONS = {"COLLABORATOR", "MEMBER", "OWNER"}


class AuthorFilter(Enum):
    """Which PR authors to include in triage."""

    CONTRIBUTORS = "contributors"  # non-collaborators only (default)
    COLLABORATORS = "collaborators"  # collaborators/members/owners only
    ALL = "all"  # everyone

    def should_include(self, author_association: str) -> bool:
        """Return True if a PR with this author_association should be included."""
        is_collab = author_association in _COLLABORATOR_ASSOCIATIONS
        if self == AuthorFilter.CONTRIBUTORS:
            return not is_collab
        if self == AuthorFilter.COLLABORATORS:
            return is_collab
        return True  # ALL


# Label applied when a maintainer marks a flagged PR as ready for review
_READY_FOR_REVIEW_LABEL = "ready for maintainer review"

# Label applied when a PR is closed due to multiple quality violations
_CLOSED_QUALITY_LABEL = "closed because of multiple quality violations"

# Label applied when a PR is closed due to suspicious changes
_SUSPICIOUS_CHANGES_LABEL = "suspicious changes detected"

# Marker used to identify comments posted by the auto-triage process
_TRIAGE_COMMENT_MARKER = "Pull Request quality criteria"

# GitHub accounts that should be auto-skipped during triage
_BOT_ACCOUNT_LOGINS = {"dependabot", "dependabot[bot]", "renovate[bot]", "github-actions[bot]"}

# Proximity threshold for showing "nearby" existing comments (lines)
_NEARBY_LINE_THRESHOLD = 5


def _get_review_cache_dir(github_repository: str) -> Path:
    return _review_cache.cache_dir(github_repository)


def _get_triage_cache_dir(github_repository: str) -> Path:
    return _triage_cache.cache_dir(github_repository)


def _get_status_cache_dir(github_repository: str) -> Path:
    return _status_cache.cache_dir(github_repository)


def _get_classification_cache_dir(github_repository: str) -> Path:
    return _classification_cache.cache_dir(github_repository)


def _cached_fetch_recent_pr_failures(
    token: str, github_repository: str, *, branch: str = "main", hours: int = 4, max_prs: int = 10
) -> RecentPRFailureInfo:
    """Return cached recent-PR failure info, fetching fresh data when the cache expires."""
    cache_key = f"recent_pr_failures_{branch}"
    cached = _get_cached_status(github_repository, cache_key)
    if cached is not None:
        get_console().print("[dim]Using cached recent-PR failure data (expires after 4 h).[/]")
        return RecentPRFailureInfo(
            failing_checks=cached["failing_checks"],
            failing_check_names=set(cached["failing_check_names"]),
            prs_examined=cached["prs_examined"],
        )
    result = _fetch_recent_pr_failures(token, github_repository, branch=branch, hours=hours, max_prs=max_prs)
    _save_status_cache(
        github_repository,
        cache_key,
        {
            "failing_checks": result.failing_checks,
            "failing_check_names": list(result.failing_check_names),
            "prs_examined": result.prs_examined,
        },
    )
    return result


def _cached_fetch_main_canary_builds(
    token: str, github_repository: str, *, branch: str = "main", count: int = 4
) -> list[dict]:
    """Return cached canary build data, fetching fresh data when the cache expires."""
    cache_key = f"canary_builds_{branch}"
    cached = _get_cached_status(github_repository, cache_key)
    if cached is not None:
        get_console().print("[dim]Using cached canary build data (expires after 4 h).[/]")
        return cached
    result = _fetch_main_canary_builds(token, github_repository, branch=branch, count=count)
    _save_status_cache(github_repository, cache_key, result)
    return result


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
            duration=cached.get("duration", 0.0),
            attempts=cached.get("attempts", 1),
        )
        result._from_cache = True  # type: ignore[attr-defined]
        return result

    t_start = time.monotonic()
    last_err: Exception | None = None
    attempts_made = 0
    for _attempt in range(3):
        attempts_made = _attempt + 1
        try:
            result = assess_pr(
                pr_number=pr_number,
                pr_title=pr_title,
                pr_body=pr_body,
                check_status_summary=check_status_summary,
                llm_model=llm_model,
            )
            if not result.error:
                break
            # LLM returned an error response — retry
            last_err = RuntimeError(result.summary or "LLM error response")
        except Exception as exc:
            last_err = exc
        if _attempt < 2:
            time.sleep(2 * (_attempt + 1))
    else:
        # All retries exhausted
        result = PRAssessment(
            should_flag=False,
            error=True,
            summary=f"LLM failed after 3 attempts: {last_err}",
        )
    result.duration = time.monotonic() - t_start
    result.attempts = attempts_made

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
            "duration": result.duration,
            "attempts": result.attempts,
        }
        _save_assessment_cache(github_repository, pr_number, head_sha, assessment_dict)

    return result


def _review_pr_as_assessment(
    token: str,
    github_repository: str,
    head_sha: str,
    pr_number: int,
    pr_title: str,
    pr_body: str,
    llm_model: str,
) -> PRAssessment:
    """Run review_pr (detailed code review) and wrap the result as a PRAssessment.

    This allows the review mode LLM action to use the same pipeline (collect_llm_progress)
    as the triage mode assess_pr action. The raw review dict is stored on the returned
    PRAssessment as ``_review_result`` for later use when presenting review comments.
    """
    from airflow_breeze.utils.github import PRAssessment, Violation

    t_start = time.monotonic()
    review_result = _fetch_diff_and_review_pr(
        token=token,
        github_repository=github_repository,
        pr_number=pr_number,
        pr_title=pr_title,
        pr_body=pr_body,
        head_sha=head_sha,
        llm_model=llm_model,
    )
    duration = time.monotonic() - t_start

    is_error = bool(review_result.get("error")) or not review_result.get("summary")
    if is_error:
        error_msg = review_result.get("error_message", "empty LLM response")
        result = PRAssessment(
            should_flag=False,
            error=True,
            summary=f"LLM review failed: {error_msg}",
            duration=duration,
        )
        result._review_result = review_result  # type: ignore[attr-defined]
        return result

    # Map overall_assessment to should_flag: REQUEST_CHANGES → flag, APPROVE/COMMENT → pass
    overall = review_result.get("overall_assessment", "COMMENT").upper()
    has_comments = bool(review_result.get("comments"))
    should_flag = overall == "REQUEST_CHANGES" or has_comments

    violations = []
    for c in review_result.get("comments", []):
        if c.get("body"):
            violations.append(
                Violation(
                    category=c.get("category", "review"),
                    explanation=c.get("body", ""),
                    severity="error" if overall == "REQUEST_CHANGES" else "warning",
                )
            )

    result = PRAssessment(
        should_flag=should_flag,
        should_report=False,
        violations=violations,
        summary=review_result.get("summary", ""),
        duration=duration,
    )
    if review_result.get("_from_cache"):
        result._from_cache = True  # type: ignore[attr-defined]
    result._review_result = review_result  # type: ignore[attr-defined]
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
        try:
            response = requests.get(
                url,
                params={"per_page": 100, "page": page},
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
                timeout=(10, 20),
            )
        except (requests.ConnectionError, requests.Timeout, OSError):
            break
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
class LogSnippetInfo:
    """Log snippet from a failed CI check, with a link to the full log."""

    snippet: str
    job_url: str  # html_url of the failed job (for clickable link)


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
            f"(--llm-use api).[/]"
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
        try:
            response = requests.get(
                f"https://api.github.com/repos/{github_repository}/collaborators",
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
                params={"per_page": 100, "page": page},
            )
        except (requests.ConnectionError, requests.Timeout, OSError):
            break
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
    from airflow_breeze.utils.recording import generating_command_images

    if generating_command_images():
        return []
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


def _graphql_request(token: str, query: str, variables: dict, *, max_retries: int = 3) -> dict:
    """Execute a GitHub GraphQL request with automatic retry on transient errors.

    Returns the 'data' dict or exits on persistent error.
    """
    import requests

    last_exc: Exception | None = None
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(
                "https://api.github.com/graphql",
                json={"query": query, "variables": variables},
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                timeout=(10, 30),
            )
        except (requests.ConnectionError, requests.Timeout) as exc:
            last_exc = exc
            if attempt < max_retries:
                time.sleep(2 * attempt)
                continue
            console_print(f"[error]GraphQL request failed after {max_retries} attempts: {exc}[/]")
            sys.exit(1)

        if response.status_code == 502 or response.status_code == 503:
            if attempt < max_retries:
                time.sleep(2 * attempt)
                continue
        if response.status_code != 200:
            console_print(f"[error]GraphQL request failed: {response.status_code} {response.text}[/]")
            sys.exit(1)
        result = response.json()
        if "errors" in result:
            console_print(f"[error]GraphQL errors: {result['errors']}[/]")
            sys.exit(1)
        return result["data"]

    # Should not reach here, but just in case
    console_print(f"[error]GraphQL request failed after {max_retries} attempts: {last_exc}[/]")
    sys.exit(1)


_CHECK_FAILURE_CONCLUSIONS = {"FAILURE", "TIMED_OUT", "ACTION_REQUIRED", "CANCELLED", "STARTUP_FAILURE"}
_STATUS_FAILURE_STATES = {"FAILURE", "ERROR"}

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
    grace_hours: int = CHECK_FAILURE_GRACE_PERIOD_HOURS,
) -> bool:
    """Check if all completed failed workflow runs for a commit finished within the grace period.

    Returns True if ALL failures are recent (within *grace_hours*),
    meaning we should NOT nag the author yet. Returns False if any failure is older
    or if we can't determine the age.

    The default grace period is CHECK_FAILURE_GRACE_PERIOD_HOURS (24 h).  When a
    collaborator has already reviewed or commented on the PR, callers should pass
    CHECK_FAILURE_GRACE_PERIOD_WITH_ENGAGEMENT_HOURS (96 h) instead.
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


def _fetch_check_details_batch(
    token: str,
    github_repository: str,
    prs: list[PRData],
    on_progress: Callable[[int, int], None] | None = None,
) -> None:
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
        if on_progress:
            on_progress(min(chunk_start + len(chunk), len(eligible)), len(eligible))


_REVIEW_THREADS_BATCH_SIZE = 10


def _fetch_unresolved_comments_batch(
    token: str,
    github_repository: str,
    prs: list[PRData],
    on_progress: Callable[[int, int], None] | None = None,
) -> None:
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
        if on_progress:
            on_progress(min(chunk_start + len(chunk), len(prs)), len(prs))


def _fetch_commits_behind_batch(
    token: str,
    github_repository: str,
    prs: list[PRData],
    on_progress: Callable[[int, int], None] | None = None,
) -> dict[int, int]:
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
        if on_progress:
            on_progress(min(chunk_start + len(chunk), len(eligible)), len(eligible))

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
    on_progress: Callable[[int, int], None] | None = None,
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

    # Check cache first — only fetch uncached PRs
    uncached_prs: list[PRData] = []
    for pr in prs:
        if pr.head_sha and require_marker:
            cached_cls, _cached_act = _get_cached_classification(
                github_repository, pr.number, pr.head_sha, viewer_login
            )
            if cached_cls in ("waiting", "responded", "acted"):
                result[cached_cls if cached_cls != "acted" else "waiting"].add(pr.number)
                continue
            if cached_cls == "none":
                continue  # Known not-triaged, skip API call
        uncached_prs.append(pr)

    if on_progress and uncached_prs:
        # Report cache hits as progress
        cache_hits = len(prs) - len(uncached_prs)
        if cache_hits:
            on_progress(cache_hits, len(prs))

    # Batch fetch last 10 comments + last commit date for uncached PRs
    for chunk_start in range(0, len(uncached_prs), _COMMITS_BEHIND_BATCH_SIZE):
        chunk = uncached_prs[chunk_start : chunk_start + _COMMITS_BEHIND_BATCH_SIZE]

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
                # Not triaged — cache this negative result too (avoids re-fetching)
                if pr.head_sha and require_marker:
                    _save_classification_cache(
                        github_repository, pr.number, pr.head_sha, viewer_login, "none"
                    )
                continue

            # Check if the PR author responded after our triage comment
            author_responded = False
            for comment in comments:
                comment_author = (comment.get("author") or {}).get("login", "")
                comment_date = comment.get("createdAt", "")
                if comment_author == pr.author_login and comment_date > triage_comment_date:
                    author_responded = True
                    break

            classification = "responded" if author_responded else "waiting"
            result[classification].add(pr.number)
            # Cache the classification result (keyed by head_sha — invalidated on new commits)
            if pr.head_sha and require_marker:
                _save_classification_cache(
                    github_repository, pr.number, pr.head_sha, viewer_login, classification
                )
        if on_progress:
            cache_hits = len(prs) - len(uncached_prs)
            on_progress(min(cache_hits + chunk_start + len(chunk), len(prs)), len(prs))

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
    quiet: bool = False,
) -> tuple[list[PRData], bool, str | None, int]:
    """Fetch a single batch of matching PRs via GraphQL.

    Returns (prs, has_next_page, end_cursor, total_issue_count).
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

    if not after_cursor and not quiet:
        console_print(f"[info]Searching PRs: {search_query}[/]")

    variables: dict = {"query": search_query, "first": batch_size}
    if after_cursor:
        variables["after"] = after_cursor

    data = _graphql_request(token, _SEARCH_PRS_QUERY, variables)
    search_data = data["search"]
    page_info = search_data.get("pageInfo", {})
    has_next_page = page_info.get("hasNextPage", False)
    end_cursor = page_info.get("endCursor")

    if not quiet:
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

    return prs, has_next_page, end_cursor, search_data["issueCount"]


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


def _graphql_mutation(token: str, mutation: str, variables: dict) -> bool:
    """Execute a GraphQL mutation, returning True on success, False on failure."""
    try:
        _graphql_request(token, mutation, variables)
        return True
    except SystemExit:
        return False


def _convert_pr_to_draft(token: str, node_id: str) -> bool:
    """Convert a PR to draft using GitHub GraphQL API."""
    return _graphql_mutation(token, _CONVERT_TO_DRAFT_MUTATION, {"prId": node_id})


def _mark_pr_ready_for_review(token: str, node_id: str) -> bool:
    """Mark a draft PR as ready for review using GitHub GraphQL API."""
    return _graphql_mutation(token, _MARK_READY_FOR_REVIEW_MUTATION, {"prId": node_id})


def _close_pr(token: str, node_id: str) -> bool:
    """Close a PR using GitHub GraphQL API."""
    return _graphql_mutation(token, _CLOSE_PR_MUTATION, {"prId": node_id})


def _post_comment(token: str, node_id: str, body: str) -> bool:
    """Post a comment on a PR using GitHub GraphQL API."""
    return _graphql_mutation(token, _ADD_COMMENT_MUTATION, {"subjectId": node_id, "body": body})


def _github_rest(
    token: str,
    method: str,
    url: str,
    *,
    json_payload: dict | None = None,
    ok_codes: tuple[int, ...] = (200, 201),
    fail_message: str = "",
) -> bool:
    """Make a GitHub REST API request, returning True on success.

    Consolidates the repeated pattern of POST/PUT with auth headers, status check,
    and optional failure message.
    """
    import requests

    fn = getattr(requests, method.lower())
    kwargs: dict[str, Any] = {
        "headers": {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
        "timeout": (10, 20),
    }
    if json_payload is not None:
        kwargs["json"] = json_payload
    for _attempt in range(3):
        try:
            response = fn(url, **kwargs)
            break
        except Exception:
            if _attempt == 2:
                if fail_message:
                    get_console().print(f"[warning]{fail_message}: connection failed after 3 attempts[/]")
                return False
            time.sleep(2 * (_attempt + 1))
            continue
    else:
        return False
    if response.status_code in ok_codes:
        return True
    if fail_message:
        get_console().print(f"[warning]{fail_message}: {response.status_code} {response.text[:200]}[/]")
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
    owner, repo = github_repository.split("/", 1)
    return _github_rest(
        token,
        "post",
        f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/comments",
        json_payload={"commit_id": commit_id, "path": path, "line": line, "side": "RIGHT", "body": body},
        fail_message=f"Failed to post review comment on {path}:{line}",
    )


def _submit_pr_review(
    token: str,
    github_repository: str,
    pr_number: int,
    commit_id: str,
    body: str,
    event: str = "COMMENT",
) -> bool:
    """Submit an overall PR review (APPROVE, REQUEST_CHANGES, or COMMENT) via REST API."""
    owner, repo = github_repository.split("/", 1)
    return _github_rest(
        token,
        "post",
        f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/reviews",
        json_payload={"commit_id": commit_id, "body": body, "event": event},
        fail_message="Failed to submit review",
    )


def _update_pr_branch(token: str, github_repository: str, pr_number: int) -> bool:
    """Update (rebase) a PR branch to the latest base branch via GitHub REST API."""
    owner, repo = github_repository.split("/", 1)
    return _github_rest(
        token,
        "put",
        f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/update-branch",
        ok_codes=(200, 202),
        fail_message="Failed to update PR branch",
    )


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


# Check name patterns that indicate static/lint checks (deterministic, not flaky)
_STATIC_CHECK_PATTERNS = (
    "static check",
    "pre-commit",
    "prek",
    "lint",
    "mypy",
    "ruff",
    "black",
    "flake8",
    "pylint",
    "isort",
    "bandit",
    "codespell",
    "yamllint",
    "shellcheck",
)


def _are_only_static_check_failures(failed_checks: list[str]) -> bool:
    """Return True if all failed checks are static/lint checks (deterministic, not flaky)."""
    if not failed_checks:
        return False
    return all(any(pattern in check.lower() for pattern in _STATIC_CHECK_PATTERNS) for check in failed_checks)


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
    elif has_ci_failures and _are_only_static_check_failures(pr.failed_checks):
        # Only static checks failed — these are deterministic (not flaky), suggest comment
        reason_parts.append("only static check failures — likely needs code fix, not rerun")
        action = TriageAction.COMMENT
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


def _display_pr_info_panels(
    pr: PRData,
    author_profile: dict | None,
    *,
    open_in_browser: bool = False,
    compact: bool = False,
    classification: str = "",
    llm_status: str = "",
    llm_duration: float = 0.0,
    llm_queue_time: float = 0.0,
    llm_submit_time: float = 0.0,
    llm_attempts: int = 0,
):
    """Display PR info and author panels (shared by flagged-PR and workflow-approval flows).

    When compact=True, the PR description is truncated more aggressively and the
    author panel is condensed (used when advancing through PRs in TUI mode).
    """
    import webbrowser

    from rich.markdown import Markdown

    console = get_console()
    console.print()
    console.rule(style="dim")
    console.print()

    status_parts_inline = []
    if pr.is_draft:
        status_parts_inline.append("[yellow]Draft[/]")
    if pr.commits_behind > 0:
        status_parts_inline.append(f"[yellow]{pr.commits_behind} behind {pr.base_ref}[/]")
    if pr.mergeable == "CONFLICTING":
        status_parts_inline.append("[red]Conflicts[/]")
    status_info = ("  |  " + " | ".join(status_parts_inline)) if status_parts_inline else ""

    # Classification and LLM status line
    extra_info = ""
    if classification:
        extra_info += f"\nPR Classification: [bold]{classification}[/]"
    if llm_status:
        if llm_status == "in_progress" and llm_submit_time > 0:
            elapsed = time.monotonic() - llm_submit_time
            queue_wait = llm_submit_time - llm_queue_time if llm_queue_time > 0 else 0
            parts = ["[yellow]Running[/]"]
            if elapsed > 0:
                parts.append(f"for {_fmt_duration(elapsed)}")
            if queue_wait > 1:
                parts.append(f"queued {_fmt_duration(queue_wait)}")
            llm_label = " — ".join(parts)
        elif llm_status == "pending" and llm_queue_time > 0:
            queued_for = time.monotonic() - llm_queue_time
            llm_label = f"[dim]Queued — waiting for LLM thread ({_fmt_duration(queued_for)})[/]"
        else:
            _llm_labels = {
                "in_progress": "[yellow]In progress[/]",
                "passed": "[green]Passed — no issues found[/]",
                "flagged": "[red]Flagged — issues found[/]",
                "error": "[red]Error — LLM assessment failed[/]",
                "disabled": "[dim]Disabled[/]",
                "pending": "[dim]Pending[/]",
            }
            llm_label = _llm_labels.get(llm_status, f"[dim]{llm_status}[/]")
        if llm_status in ("passed", "flagged", "error"):
            timing_parts = []
            if llm_duration > 0:
                timing_parts.append(f"took {_fmt_duration(llm_duration)}")
            if llm_submit_time > 0 and llm_queue_time > 0 and llm_submit_time > llm_queue_time:
                queue_wait = llm_submit_time - llm_queue_time
                if queue_wait > 1:
                    timing_parts.append(f"queued {_fmt_duration(queue_wait)}")
            if llm_attempts > 1:
                timing_parts.append(f"{llm_attempts} attempts")
            if timing_parts:
                llm_label += f" [dim]({', '.join(timing_parts)})[/]"
        extra_info += f"\nLLM Review: {llm_label}"

    pr_info = (
        f"[link={pr.url}][cyan]#{pr.number}[/][/link] {pr.title}\n"
        f"Author: [link=https://github.com/{pr.author_login}][bold]{pr.author_login}[/][/link]  |  "
        f"Created: {_human_readable_age(pr.created_at)}  |  "
        f"Updated: {_human_readable_age(pr.updated_at)}"
        f"{status_info}"
        f"{extra_info}"
    )
    console.print(Panel(pr_info, title="Pull Request", border_style="cyan"))

    # Display PR description as rendered markdown
    if pr.body and pr.body.strip():
        body_text = pr.body.strip()
        max_body = 1000 if compact else 3000
        if len(body_text) > max_body:
            body_text = body_text[:max_body] + "\n\n*... (truncated)*"
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


def _display_pr_panel(
    pr: PRData,
    author_profile: dict | None,
    assessment,
    *,
    compact: bool = False,
    classification: str = "",
    llm_status: str = "",
    llm_duration: float = 0.0,
    llm_queue_time: float = 0.0,
    llm_submit_time: float = 0.0,
    llm_attempts: int = 0,
):
    """Display Rich panels with PR details, author info, and violations."""
    console = get_console()
    _display_pr_info_panels(
        pr,
        author_profile,
        compact=compact,
        classification=classification,
        llm_status=llm_status,
        llm_duration=llm_duration,
        llm_queue_time=llm_queue_time,
        llm_submit_time=llm_submit_time,
        llm_attempts=llm_attempts,
    )

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


def _display_log_snippets_panel(
    log_snippets: dict[str, LogSnippetInfo], pr: PRData | None = None, *, compact: bool = False
) -> None:
    """Display Rich panel(s) with log snippets from failed CI checks."""
    from rich.console import Group
    from rich.text import Text

    console = get_console()
    for check_name, info in log_snippets.items():
        # Truncate very long snippets for display
        display_snippet = info.snippet
        max_len = 800 if compact else 2000
        if len(display_snippet) > max_len:
            display_snippet = display_snippet[:max_len] + "\n... (truncated)"

        # Build content: clickable links header + pre-formatted log text
        renderables: list[Any] = []
        link_parts: list[str] = []
        if pr:
            link_parts.append(f"PR: [link={pr.url}]#{pr.number}[/link]")
        if info.job_url:
            link_parts.append(f"Log: [link={info.job_url}]{info.job_url}[/link]")
        if link_parts:
            renderables.append("  ".join(link_parts))

        # Use Text object to preserve exact whitespace/indentation in log output
        renderables.append(Text(display_snippet))

        console.print(
            Panel(
                Group(*renderables),
                title=f"Failed check logs: {check_name}",
                border_style="red",
                expand=True,
            )
        )


def _launch_background_log_fetching(
    token: str,
    github_repository: str,
    prs: list,
    llm_concurrency: int,
) -> dict[int, Future[dict[str, LogSnippetInfo]]]:
    """Launch background CI log fetching for all PRs with failed checks.

    Returns a dict mapping PR number -> Future[dict[str, LogSnippetInfo]].
    Uses the same concurrency level as LLM assessments.
    """
    log_futures: dict[int, Future[dict[str, LogSnippetInfo]]] = {}
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
    *,
    compact: bool = False,
):
    """Display Rich panels for a PR needing workflow approval."""
    console = get_console()
    _display_pr_info_panels(pr, author_profile, compact=compact, classification="WF Approval")

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


def _resolve_unknown_mergeable(
    token: str,
    github_repository: str,
    prs: list[PRData],
    on_progress: Callable[[int, int], None] | None = None,
) -> int:
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

    for idx, pr in enumerate(unknown_prs):
        url = f"https://api.github.com/repos/{github_repository}/pulls/{pr.number}"
        try:
            response = requests.get(
                url,
                headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3+json"},
                timeout=(10, 20),
            )
        except (requests.ConnectionError, requests.Timeout, OSError):
            still_unknown.append(pr)
            if on_progress:
                on_progress(idx + 1, len(unknown_prs))
            continue
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
        if on_progress:
            on_progress(idx + 1, len(unknown_prs))

    if still_unknown:
        # Give GitHub a moment to compute mergeability, then retry
        time.sleep(2)
        for pr in still_unknown:
            url = f"https://api.github.com/repos/{github_repository}/pulls/{pr.number}"
            try:
                response = requests.get(
                    url,
                    headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3+json"},
                    timeout=(10, 20),
                )
            except (requests.ConnectionError, requests.Timeout, OSError):
                continue
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
        parts.append(f"{flagged} issues found")
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
    quiet: bool = False,
    llm_durations: dict[int, float] | None = None,
    llm_attempts: dict[int, int] | None = None,
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
        try:
            assessment = future.result()
        except Exception as exc:
            # Future raised an exception — treat as LLM error
            llm_errors.append(pr.number)
            if not quiet:
                console_print(f"  [warning]PR {_pr_link(pr)} LLM assessment crashed: {exc}[/]")
            continue
        # Record actual LLM execution duration and attempts
        if llm_durations is not None and hasattr(assessment, "duration") and assessment.duration > 0:
            llm_durations[pr.number] = assessment.duration
        if llm_attempts is not None and hasattr(assessment, "attempts") and assessment.attempts > 0:
            llm_attempts[pr.number] = assessment.attempts
        if assessment.error:
            llm_errors.append(pr.number)
            if not quiet:
                msg = f"  [warning]PR {_pr_link(pr)} LLM assessment failed ({assessment.summary})."
                if assessment.error_debug_file:
                    msg += f" Raw response saved to {assessment.error_debug_file}"
                console_print(f"{msg}[/]")
            continue
        if not assessment.should_flag:
            llm_passing.append(pr)
            if not quiet:
                console_print(f"  [success]PR {_pr_link(pr)} passes LLM quality check.[/]")
            continue
        llm_assessments[pr.number] = assessment
        if assessment.should_report and not quiet:
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

    # Cumulative PR counts across all batches/pages (for summary)
    cumulative_all_prs: int = 0
    cumulative_candidates: int = 0
    cumulative_passing: int = 0
    cumulative_pending_approval: int = 0
    cumulative_workflows_in_progress: int = 0
    cumulative_skipped_drafts: int = 0
    cumulative_recent_failures_skipped: int = 0
    cumulative_already_triaged: int = 0
    cumulative_deterministic_flags: int = 0
    cumulative_llm_flagged: int = 0
    cumulative_llm_errors: int = 0
    cumulative_llm_report: int = 0
    cumulative_skipped_collaborator: int = 0
    cumulative_skipped_bot: int = 0
    cumulative_skipped_accepted: int = 0
    cumulative_triaged_waiting: int = 0
    cumulative_triaged_responded: int = 0


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
    # LLM durations: PR number → actual execution time in seconds
    llm_durations: dict[int, float] = field(default_factory=dict)
    # LLM attempts: PR number → number of attempts
    llm_attempts: dict[int, int] = field(default_factory=dict)
    # Main branch failure info (optional)
    main_failures: RecentPRFailureInfo | None = None
    # Background CI log fetching: PR number -> Future[dict[str, LogSnippetInfo]]
    log_futures: dict[int, Future[dict[str, LogSnippetInfo]]] = field(default_factory=dict)

    def collect_llm_progress(self, quiet: bool = False) -> str:
        """Collect completed LLM results and optionally print progress status.

        Returns the progress status string (useful for TUI status panel).
        """
        if not self.llm_future_to_pr:
            return ""
        _collect_llm_results(
            self.llm_future_to_pr,
            self.llm_assessments,
            self.llm_completed,
            self.llm_errors,
            self.llm_passing,
            quiet=quiet,
            llm_durations=self.llm_durations,
            llm_attempts=self.llm_attempts,
        )
        progress = _llm_progress_status(
            len(self.llm_completed),
            len(self.llm_future_to_pr),
            len(self.llm_assessments),
            len(self.llm_errors),
        )
        if progress and not quiet:
            console_print(progress)
        return progress


@dataclass
class PRStateSnapshot:
    """Snapshot of key PR fields for staleness detection."""

    head_sha: str
    updated_at: str
    is_draft: bool
    mergeable: str


def _snapshot_pr_state(pr: PRData) -> PRStateSnapshot:
    """Capture the current PR state for later comparison."""
    return PRStateSnapshot(
        head_sha=pr.head_sha,
        updated_at=pr.updated_at,
        is_draft=pr.is_draft,
        mergeable=pr.mergeable,
    )


def _refresh_pr_if_stale(
    pr: PRData,
    snapshot: PRStateSnapshot,
    *,
    token: str,
    github_repository: str,
    run_api: bool = True,
) -> DeterministicResult | None:
    """Re-fetch PR from GitHub and check if state changed since snapshot.

    If the PR state has not changed, returns None (proceed with original action).
    If it changed, updates the PRData in place with fresh data, re-enriches it,
    runs deterministic checks, and returns the new DeterministicResult so the
    caller can re-evaluate the appropriate action.
    """
    fresh = _fetch_single_pr_graphql(token, github_repository, pr.number)

    # Compare key state fields
    changed_fields: list[str] = []
    if fresh.head_sha != snapshot.head_sha:
        changed_fields.append(f"new commits ({snapshot.head_sha[:8]} → {fresh.head_sha[:8]})")
    if fresh.updated_at != snapshot.updated_at:
        # updated_at changes on any PR mutation (comments, labels, reviews, etc.)
        if not changed_fields:
            changed_fields.append("PR updated since last check")
    if fresh.is_draft != snapshot.is_draft:
        changed_fields.append(f"draft status changed ({'draft' if fresh.is_draft else 'ready'})")
    if fresh.mergeable != snapshot.mergeable:
        changed_fields.append(f"merge status changed ({snapshot.mergeable} → {fresh.mergeable})")

    if not changed_fields:
        return None

    # State changed — update the PR in place and re-evaluate
    console_print(
        f"  [warning]PR #{pr.number} state changed: {'; '.join(changed_fields)}. Re-evaluating...[/]"
    )
    # Update mutable fields on the existing PRData object
    pr.head_sha = fresh.head_sha
    pr.updated_at = fresh.updated_at
    pr.is_draft = fresh.is_draft
    pr.mergeable = fresh.mergeable
    pr.title = fresh.title
    pr.body = fresh.body
    pr.labels = fresh.labels
    pr.checks_state = fresh.checks_state
    pr.node_id = fresh.node_id

    # Re-enrich with fresh check details, merge status, and review comments
    return _reevaluate_triaged_pr(
        pr,
        token=token,
        github_repository=github_repository,
        run_api=run_api,
    )


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
    snapshot: PRStateSnapshot | None = None,
    run_api: bool = True,
) -> DeterministicResult | None:
    """Execute a single triage action on a PR. Mutates ctx.stats.

    If ``snapshot`` is provided, checks whether the PR state has changed since the
    snapshot was taken. When a change is detected the PR is re-enriched and
    re-evaluated; the new ``DeterministicResult`` is returned so the caller can
    present updated information. Returns ``None`` when the action completed (or was
    skipped) normally.
    """
    stats = ctx.stats

    if action == TriageAction.SKIP:
        console_print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
        stats.total_skipped_action += 1
        return None

    # Optimistic locking: verify PR state hasn't changed before mutating
    if snapshot is not None:
        stale_result = _refresh_pr_if_stale(
            pr, snapshot, token=ctx.token, github_repository=ctx.github_repository, run_api=run_api
        )
        if stale_result is not None:
            return stale_result

    if action == TriageAction.READY:
        if not _confirm_action(pr, "Add 'ready for maintainer review' label", ctx.answer_triage):
            stats.total_skipped_action += 1
            return None
        console_print(
            f"  [info]Marking PR {_pr_link(pr)} as ready — adding '{_READY_FOR_REVIEW_LABEL}' label.[/]"
        )
        if _add_label(ctx.token, ctx.github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
            console_print(f"  [success]Label '{_READY_FOR_REVIEW_LABEL}' added to PR {_pr_link(pr)}.[/]")
            stats.total_ready += 1
        else:
            console_print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
        return None

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
        return None

    if action == TriageAction.REBASE:
        if not _confirm_action(pr, "Update (rebase) PR branch", ctx.answer_triage):
            stats.total_skipped_action += 1
            return None

        get_console().print(f"  Updating branch for PR {_pr_link(pr)}...")
        if _update_pr_branch(ctx.token, ctx.github_repository, pr.number):
            get_console().print(f"  [success]Branch updated for PR {_pr_link(pr)}.[/]")
            stats.total_rebased += 1
        else:
            get_console().print(f"  [error]Failed to update branch for PR {_pr_link(pr)}.[/]")
        return None

    if action == TriageAction.COMMENT:
        if not _confirm_action(pr, "Post comment", ctx.answer_triage):
            stats.total_skipped_action += 1
            return None
        text = comment_only_text or draft_comment
        console_print(f"  Posting comment on PR {_pr_link(pr)}...")
        if _post_comment(ctx.token, pr.node_id, text):
            console_print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
            stats.total_commented += 1
        else:
            console_print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
        return None

    if action == TriageAction.DRAFT:
        if not _confirm_action(pr, "Convert to draft and post comment", ctx.answer_triage):
            stats.total_skipped_action += 1
            return None
        console_print(f"  Converting PR {_pr_link(pr)} to draft...")
        if _convert_pr_to_draft(ctx.token, pr.node_id):
            console_print(f"  [success]PR {_pr_link(pr)} converted to draft.[/]")
        else:
            console_print(f"  [error]Failed to convert PR {_pr_link(pr)} to draft.[/]")
            return None
        console_print(f"  Posting comment on PR {_pr_link(pr)}...")
        if _post_comment(ctx.token, pr.node_id, draft_comment):
            console_print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
            stats.total_converted += 1
        else:
            console_print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
        return None

    if action == TriageAction.PING:
        if not pr.unresolved_threads:
            get_console().print(
                f"  [warning]No unresolved threads to ping reviewers about on PR {_pr_link(pr)}.[/]"
            )
            return None
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
            return None
        get_console().print(f"  Pinging reviewer(s) on PR {_pr_link(pr)}...")
        if _post_comment(ctx.token, pr.node_id, ping_body):
            get_console().print(f"  [success]Ping comment posted on PR {_pr_link(pr)}.[/]")
            stats.total_commented += 1
        else:
            get_console().print(f"  [error]Failed to post ping comment on PR {_pr_link(pr)}.[/]")
        return None

    if action == TriageAction.CLOSE:
        if not _confirm_action(pr, "Close PR and post comment", ctx.answer_triage):
            stats.total_skipped_action += 1
            return None
        console_print(f"  Closing PR {_pr_link(pr)}...")
        if _close_pr(ctx.token, pr.node_id):
            console_print(f"  [success]PR {_pr_link(pr)} closed.[/]")
        else:
            console_print(f"  [error]Failed to close PR {_pr_link(pr)}.[/]")
            return None
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
    return None


def _prompt_and_execute_flagged_pr(
    ctx: TriageContext,
    pr: PRData,
    assessment,
    *,
    comment_only_text: str | None = None,
    allow_back: bool = False,
    compact: bool = False,
    default_skip: bool = False,
    run_api: bool = True,
    classification: str = "",
    llm_status: str = "",
    llm_duration: float = 0.0,
    llm_queue_time: float = 0.0,
    llm_submit_time: float = 0.0,
    llm_attempts: int = 0,
) -> bool | DeterministicResult:
    """Display a flagged PR panel, prompt user for action, and execute it. Mutates ctx.stats.

    When default_skip=True, the default action is overridden to SKIP (used when
    re-opening a previously skipped PR — all actions remain available).

    Returns True if the user chose to go back to the TUI without taking action.
    Returns False if the action was executed normally.
    Returns a DeterministicResult if the PR state changed (optimistic lock failure)
    and the caller should re-evaluate with the new result.
    """
    snapshot = _snapshot_pr_state(pr)
    author_profile = _fetch_author_profile(ctx.token, pr.author_login, ctx.github_repository)

    _display_pr_panel(
        pr,
        author_profile,
        assessment,
        compact=compact,
        classification=classification,
        llm_status=llm_status,
        llm_duration=llm_duration,
        llm_queue_time=llm_queue_time,
        llm_submit_time=llm_submit_time,
        llm_attempts=llm_attempts,
    )

    # Display log snippets from failed CI checks (fetched in background)
    if pr.failed_checks and pr.head_sha:
        log_future = ctx.log_futures.get(pr.number)
        if log_future is not None:
            if log_future.done():
                try:
                    log_snippets = log_future.result()
                except Exception:
                    get_console().print("  [warning]CI log retrieval failed (connection error).[/]")
                    log_snippets = None
                if log_snippets:
                    _display_log_snippets_panel(log_snippets, pr=pr, compact=compact)
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
                            _display_log_snippets_panel(log_snippets, pr=pr, compact=compact)
                    except TimeoutError:
                        get_console().print("  [warning]CI log retrieval timed out.[/]")
                    except Exception:
                        get_console().print("  [warning]CI log retrieval failed.[/]")
                else:
                    get_console().print("  [dim]Skipping CI log display for this PR.[/]")
        else:
            # No background future — fetch inline as fallback
            try:
                log_snippets = _fetch_failed_job_log_snippets(
                    ctx.token, ctx.github_repository, pr.head_sha, pr.failed_checks
                )
            except Exception:
                get_console().print("  [warning]CI log retrieval failed (connection error).[/]")
                log_snippets = None
            if log_snippets:
                _display_log_snippets_panel(log_snippets, pr=pr, compact=compact)

    # Check if PR failures match main branch failures
    main_matching: list[str] = []
    if ctx.main_failures and pr.failed_checks:
        main_matching = ctx.main_failures.find_matching_failures(pr.failed_checks)
        if main_matching:
            _display_recent_pr_failure_panel(pr, ctx.main_failures, main_matching)

    default_action, reason = _compute_default_action(
        pr, assessment, ctx.author_flagged_count, ctx.main_failures
    )
    if default_skip:
        default_action = TriageAction.SKIP
        reason = f"Previously skipped (original suggestion: {reason})"
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
        return False

    action = prompt_triage_action(
        f"Action for PR {_pr_link(pr)}?",
        default=default_action,
        forced_answer=ctx.answer_triage,
        exclude=exclude_actions,
        pr_url=pr.url,
        token=ctx.token,
        github_repository=ctx.github_repository,
        pr_number=pr.number,
        allow_back=allow_back,
    )

    if action == TriageAction.BACK:
        return True

    if action == TriageAction.QUIT:
        console_print("[warning]Quitting.[/]")
        ctx.stats.quit_early = True
        return False

    # If user takes action on a should_report PR (anything other than skip),
    # downgrade it from "report" to regular "flagged" — user has reviewed and decided.
    if action != TriageAction.SKIP and getattr(assessment, "should_report", False):
        assessment.should_report = False
        console_print("  [info]Report status cleared — PR marked as issues found.[/]")

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

    stale_result = _execute_triage_action(
        ctx,
        pr,
        action,
        draft_comment=draft_comment,
        close_comment=close_comment,
        comment_only_text=comment_only_text,
        snapshot=snapshot,
        run_api=run_api,
    )
    if stale_result is not None:
        return stale_result
    return False


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


def _execute_tui_direct_action(
    ctx: TriageContext,
    tui: TriageTUI,
    entry: PRListEntry,
    action: TUIAction,
    assessment_map: dict[int, PRAssessment],
    on_action: Callable[[PRData, str], None] | None = None,
) -> None:
    """Execute a direct triage action from the TUI without entering detailed review.

    Maps TUIAction.ACTION_* to TriageAction and executes it, building any
    required comments from the assessment automatically.
    """
    from airflow_breeze.utils.tui_display import PRCategory, TUIAction

    # Handle flag-as-suspicious directly (not a regular triage action)
    if action == TUIAction.ACTION_FLAG:
        pr = entry.pr
        if ctx.dry_run:
            get_console().print("\n" * 25)
            get_console().clear()
            console_print(f"[warning]Dry run — would flag PR #{pr.number} as suspicious[/]")
            from airflow_breeze.utils.confirm import _read_char

            console_print("[dim]Press any key to return...[/]")
            _read_char()
            return
        get_console().print("\n" * 25)
        get_console().clear()
        get_console().rule(f"[cyan]PR {_pr_link(pr)}[/]", style="dim")
        console_print(f"  [bold]{pr.title}[/] by {pr.author_login}\n")
        console_print(f"  [bold red]Flagging PR {_pr_link(pr)} as suspicious.[/]")
        console_print(f"  Fetching all open PRs by {pr.author_login}...")
        author_prs = _fetch_author_open_prs(ctx.token, ctx.github_repository, pr.author_login)
        if not author_prs:
            console_print(f"  [dim]No open PRs found for {pr.author_login}.[/]")
        else:
            console_print(
                f"\n  [bold red]{len(author_prs)} "
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
            if confirm == Answer.YES:
                closed, commented = _close_suspicious_prs(
                    ctx.token, ctx.github_repository, author_prs, pr.number
                )
                console_print(
                    f"  [success]Closed {closed}/{len(author_prs)} "
                    f"{'PRs' if len(author_prs) != 1 else 'PR'}, commented on {commented}.[/]"
                )
                ctx.stats.total_closed += closed
            elif confirm == Answer.QUIT:
                ctx.stats.quit_early = True
            else:
                console_print(f"  [info]Skipping — no PRs closed for {pr.author_login}.[/]")
        entry.action_taken = "suspicious"
        if on_action:
            on_action(pr, "suspicious")
        from airflow_breeze.utils.confirm import _read_char

        console_print("\n[dim]Press any key to return to TUI...[/]")
        _read_char()
        return

    # Handle LLM review trigger
    if action == TUIAction.ACTION_LLM:
        pr = entry.pr
        if entry.llm_status in ("in_progress", "pending"):
            return  # already running
        if not hasattr(ctx, "_llm_model") or not hasattr(ctx, "_llm_executor"):
            return  # LLM not available
        llm_executor = ctx._llm_executor  # type: ignore[attr-defined]
        llm_model = ctx._llm_model  # type: ignore[attr-defined]
        if llm_executor is None:
            return
        # Discard previous LLM results for this PR
        ctx.llm_assessments.pop(pr.number, None)
        ctx.llm_durations.pop(pr.number, None)
        if pr.number in ctx.llm_errors:
            ctx.llm_errors.remove(pr.number)
        ctx.llm_passing[:] = [p for p in ctx.llm_passing if p.number != pr.number]
        # Invalidate cached assessment so LLM re-runs from scratch
        from airflow_breeze.utils.pr_cache import triage_cache as _triage_cache

        cache_file = _triage_cache._file(ctx.github_repository, f"pr_{pr.number}")
        if cache_file.exists():
            cache_file.unlink(missing_ok=True)
        # Reset entry state — clear all LLM timing (main loop will promote to in_progress)
        entry.llm_status = "pending"
        entry.llm_queue_time = time.monotonic()
        entry.llm_submit_time = 0.0
        entry.llm_duration = 0.0
        # Reset category to PASSING so it shows as "Wait for LLM"
        entry.category = PRCategory.PASSING
        entry.action_taken = ""
        # Invalidate detail cache
        if tui._detail_pr_number == pr.number:
            tui._detail_pr_number = None
        if tui.review_mode:
            fut = llm_executor.submit(
                _review_pr_as_assessment,
                token=ctx.token,
                github_repository=ctx.github_repository,
                head_sha=pr.head_sha,
                pr_number=pr.number,
                pr_title=pr.title,
                pr_body=pr.body,
                llm_model=llm_model,
            )
        else:
            fut = llm_executor.submit(
                _cached_assess_pr,
                github_repository=ctx.github_repository,
                head_sha=pr.head_sha,
                pr_number=pr.number,
                pr_title=pr.title,
                pr_body=pr.body,
                check_status_summary=pr.check_summary,
                llm_model=llm_model,
            )
        ctx.llm_future_to_pr[fut] = pr
        return

    _TUI_TO_TRIAGE: dict[TUIAction, TriageAction] = {
        TUIAction.ACTION_DRAFT: TriageAction.DRAFT,
        TUIAction.ACTION_COMMENT: TriageAction.COMMENT,
        TUIAction.ACTION_CLOSE: TriageAction.CLOSE,
        TUIAction.ACTION_RERUN: TriageAction.RERUN,
        TUIAction.ACTION_REBASE: TriageAction.REBASE,
        TUIAction.ACTION_READY: TriageAction.READY,
    }

    triage_action = _TUI_TO_TRIAGE.get(action)
    if triage_action is None:
        return

    pr = entry.pr

    if ctx.dry_run:
        get_console().print("\n" * 25)
        get_console().clear()
        console_print(f"[warning]Dry run — would execute: {triage_action.name} on PR #{pr.number}[/]")
        from airflow_breeze.utils.confirm import _read_char

        console_print("[dim]Press any key to return...[/]")
        _read_char()
        return

    # For workflow approval PRs, RERUN means show diff first, then approve pending workflows
    if entry.category == PRCategory.WORKFLOW_APPROVAL and triage_action == TriageAction.RERUN:
        get_console().print("\n" * 25)
        get_console().clear()

        pending_runs = _find_pending_workflow_runs(ctx.token, ctx.github_repository, pr.head_sha)
        if pending_runs:
            get_console().rule(f"[cyan]PR {_pr_link(pr)}[/]", style="dim")
            console_print(f"  [bold]{pr.title}[/] by {pr.author_login}\n")
            confirm = _show_diff_and_confirm(
                ctx.token,
                ctx.github_repository,
                pr,
                forced_answer=ctx.answer_triage,
                confirm_message="Press Enter to approve, \\[f] to flag as suspicious, \\[q] to quit",
            )
            if confirm == ContinueAction.QUIT:
                ctx.stats.quit_early = True
                return
            if confirm == ContinueAction.FLAG:
                console_print(f"  [bold red]Flagged PR {_pr_link(pr)} as suspicious — skipping approval.[/]")
                entry.action_taken = "suspicious"
                if on_action:
                    on_action(pr, "suspicious")
            else:
                approved = _approve_workflow_runs(ctx.token, ctx.github_repository, pending_runs)
                if approved:
                    console_print(
                        f"  [success]Approved {approved} workflow "
                        f"{'runs' if approved != 1 else 'run'} for PR {_pr_link(pr)}.[/]"
                    )
                    ctx.stats.total_workflows_approved += 1
                    entry.action_taken = "workflow approved"
                    if on_action:
                        on_action(pr, "workflow approved")
                else:
                    console_print(f"  [error]Failed to approve workflow runs for PR {_pr_link(pr)}.[/]")
        else:
            # No pending runs — try rerunning completed runs (no diff needed)
            console_print(f"\n[info]Rerunning workflows for PR {_pr_link(pr)}...[/]")
            rerun_count = 0
            if pr.head_sha:
                completed_runs = _find_workflow_runs_by_status(
                    ctx.token, ctx.github_repository, pr.head_sha, "completed"
                )
                if completed_runs:
                    for run in completed_runs:
                        if _rerun_workflow_run(ctx.token, ctx.github_repository, run):
                            rerun_count += 1
            if rerun_count:
                console_print(
                    f"  [success]Rerun {rerun_count} workflow "
                    f"{'runs' if rerun_count != 1 else 'run'} for PR {_pr_link(pr)}.[/]"
                )
                ctx.stats.total_rerun += 1
                entry.action_taken = "rerun"
                if on_action:
                    on_action(pr, "rerun")
            else:
                console_print(f"  [warning]No workflow runs found for PR {_pr_link(pr)}.[/]")
        from airflow_breeze.utils.confirm import _read_char

        console_print("[dim]Press any key to return...[/]")
        _read_char()
        if entry.action_taken:
            tui.move_cursor(1)
        return

    # Build comments for actions that need them (draft, comment, close)
    draft_comment = ""
    close_comment = ""
    comment_only_text: str | None = None
    assessment = assessment_map.get(pr.number)

    if triage_action in (TriageAction.DRAFT, TriageAction.COMMENT, TriageAction.CLOSE):
        violations = assessment.violations if assessment else []
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

    # Show a brief confirmation before executing
    get_console().print("\n" * 25)
    get_console().clear()
    action_label = {
        TriageAction.DRAFT: "Convert to draft",
        TriageAction.COMMENT: "Post comment",
        TriageAction.CLOSE: "Close PR",
        TriageAction.RERUN: "Rerun checks",
        TriageAction.REBASE: "Update/rebase branch",
        TriageAction.READY: "Mark ready for review",
    }.get(triage_action, str(triage_action))
    console_print(f"\n[bold]{action_label}[/] for PR {_pr_link(pr)} — [bold]{pr.title}[/]")

    # Show comment preview for comment-posting actions
    if triage_action == TriageAction.CLOSE and close_comment:
        console_print(Panel(close_comment, title="Comment to be posted", border_style="red"))
    elif triage_action == TriageAction.COMMENT and comment_only_text:
        console_print(Panel(comment_only_text, title="Comment to be posted", border_style="green"))
    elif triage_action == TriageAction.DRAFT and draft_comment:
        console_print(Panel(draft_comment, title="Comment to be posted", border_style="green"))

    _execute_triage_action(
        ctx,
        pr,
        triage_action,
        draft_comment=draft_comment,
        close_comment=close_comment,
        comment_only_text=comment_only_text,
    )

    # Map triage action to action_taken label
    _ACTION_LABELS: dict[TriageAction, str] = {
        TriageAction.DRAFT: "drafted",
        TriageAction.COMMENT: "commented",
        TriageAction.CLOSE: "closed",
        TriageAction.RERUN: "rerun",
        TriageAction.REBASE: "rebased",
        TriageAction.READY: "ready",
        TriageAction.SKIP: "skipped",
    }
    entry.action_taken = _ACTION_LABELS.get(triage_action, triage_action.name.lower())
    if on_action and entry.action_taken:
        on_action(entry.pr, entry.action_taken)

    from airflow_breeze.utils.confirm import _read_char

    console_print("[dim]Press any key to return to TUI...[/]")
    _read_char()
    tui.move_cursor(1)


# ---------------------------------------------------------------------------
# Background PR status refresh manager
# ---------------------------------------------------------------------------

_REFRESH_INITIAL_DELAY = 5.0  # seconds after action before first refresh
_REFRESH_POLL_INTERVAL = 60.0  # seconds between polls for pending CI


@dataclass
class _RefreshResult:
    """Result of a background PR refresh."""

    pr_number: int
    pr_data: PRData | None  # None if PR was deleted/inaccessible
    category: str  # deterministic category string from _assess_pr_deterministic
    assessment: PRAssessment | None  # set when category == "flagged"
    has_pending_ci: bool  # True if CI is still in-progress


class _PRRefreshManager:
    """Background manager that re-fetches PR status after actions and polls pending CI.

    Designed to be used from the TUI event loop: the caller checks ``collect()``
    periodically (e.g. on input timeout) and applies the results to the entry list.
    """

    def __init__(self, token: str, github_repository: str, *, run_api: bool = True):
        self._token = token
        self._repo = github_repository
        self._run_api = run_api
        self._executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="pr-refresh")
        self._pending: dict[int, Future] = {}  # pr_number → in-flight future
        self._poll_set: dict[int, float] = {}  # pr_number → next poll time (monotonic)
        self._lock = threading.Lock()

    # -- public API --

    def schedule_refresh(self, pr_number: int, *, delay: float = _REFRESH_INITIAL_DELAY) -> None:
        """Schedule a PR for refresh after *delay* seconds."""
        with self._lock:
            if pr_number in self._pending:
                return  # already scheduled
            self._pending[pr_number] = self._executor.submit(self._delayed_fetch, pr_number, delay)

    def schedule_poll(self, pr_number: int) -> None:
        """Add a PR to the periodic poll set (checked every ~60s)."""
        with self._lock:
            if pr_number not in self._poll_set:
                self._poll_set[pr_number] = time.monotonic() + _REFRESH_POLL_INTERVAL

    def remove_from_poll(self, pr_number: int) -> None:
        """Remove a PR from the periodic poll set."""
        with self._lock:
            self._poll_set.pop(pr_number, None)

    def collect(self) -> list[_RefreshResult]:
        """Collect completed refresh results (non-blocking). Also triggers periodic polls."""
        results: list[_RefreshResult] = []

        # Collect completed futures
        with self._lock:
            done_nums = [n for n, f in self._pending.items() if f.done()]
            for n in done_nums:
                fut = self._pending.pop(n)
                try:
                    result = fut.result()
                    if result is not None:
                        results.append(result)
                except Exception:
                    pass  # silently ignore fetch errors

        # Check if any polled PRs are due
        now = time.monotonic()
        with self._lock:
            due = [n for n, t in self._poll_set.items() if now >= t and n not in self._pending]
            for n in due:
                self._poll_set[n] = now + _REFRESH_POLL_INTERVAL
                self._pending[n] = self._executor.submit(self._do_fetch, n)

        return results

    @property
    def active_count(self) -> int:
        """Number of PRs being monitored (in-flight + polling)."""
        with self._lock:
            return len(self._pending) + len(self._poll_set)

    @property
    def polling_count(self) -> int:
        """Number of PRs in the periodic poll set."""
        with self._lock:
            return len(self._poll_set)

    def shutdown(self) -> None:
        """Stop all background refresh threads."""
        with self._lock:
            self._poll_set.clear()
        self._executor.shutdown(wait=False, cancel_futures=True)

    # -- internal --

    def _delayed_fetch(self, pr_number: int, delay: float) -> _RefreshResult | None:
        """Wait *delay* seconds, then fetch."""
        time.sleep(delay)
        return self._do_fetch(pr_number)

    def _do_fetch(self, pr_number: int) -> _RefreshResult | None:
        """Re-fetch a single PR and run deterministic categorization."""
        try:
            pr_data = _fetch_single_pr_graphql(self._token, self._repo, pr_number)
        except Exception:
            return None

        # Enrich: resolve commits behind
        try:
            behind_map = _fetch_commits_behind_batch(self._token, self._repo, [pr_data])
            pr_data.commits_behind = behind_map.get(pr_number, 0)
        except Exception:
            pass

        # Run deterministic checks
        det = _assess_pr_deterministic(pr_data, token=self._token, github_repository=self._repo)

        has_pending = det.category in ("in_progress", "pending_approval")
        return _RefreshResult(
            pr_number=pr_number,
            pr_data=pr_data,
            category=det.category,
            assessment=det.assessment if det.category == "flagged" else None,
            has_pending_ci=has_pending,
        )


def _run_tui_triage(
    ctx: TriageContext,
    all_prs: list[PRData],
    *,
    pending_approval: list[PRData],
    det_flagged_prs: list[tuple[PRData, PRAssessment]],
    llm_candidates: list[PRData],
    passing_prs: list[PRData],
    accepted_prs: list[PRData],
    already_triaged_nums: set[int],
    run_api: bool = True,
    run_llm: bool = True,
    llm_model: str = "",
    llm_executor: ThreadPoolExecutor | None = None,
    author_filter: AuthorFilter = AuthorFilter.CONTRIBUTORS,
    include_drafts: bool = False,
    mode_desc: str = "",
    selection_criteria: str = "",
    total_matching_prs: int = 0,
    viewer_login: str = "",
    load_more_fn: Callable[
        [],
        tuple[list[PRData], list[tuple[PRData, PRAssessment]], list[PRData], list[PRData], set[int], bool]
        | None,
    ]
    | None = None,
) -> tuple[list[dict], float]:
    """Run the full-screen TUI triage interface.

    Displays all PRs in a full-screen view. When the user selects a PR,
    drops into the existing review flow for that PR, then returns to the TUI.
    """
    import webbrowser

    from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn, TimeElapsedColumn

    from airflow_breeze.utils.confirm import _read_char
    from airflow_breeze.utils.tui_display import (
        PRCategory,
        PRListEntry,
        TriageTUI,
        TUIAction,
    )

    _tui_progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=20),
        TextColumn("[dim]{task.fields[status]}"),
        TimeElapsedColumn(),
        console=get_console(),
    )
    _tui_steps = [
        ("build_list", "Build PR list"),
        ("sort", "Sort entries"),
        ("fill_pages", "Fill initial pages"),
        ("init_tui", "Initialize TUI"),
        ("fetch_diff", "Fetch first diff"),
        ("prefetch", "Prefetch diffs"),
    ]
    _tui_tasks: dict[str, TaskID] = {}
    for key, desc in _tui_steps:
        _tui_tasks[key] = _tui_progress.add_task(desc, total=1, status="waiting")
    _tui_progress.start()

    def _tui_step_start(key: str, status: str = "running") -> None:
        _tui_progress.update(_tui_tasks[key], status=status)

    def _tui_step_done(key: str, status: str = "done") -> None:
        _tui_progress.update(_tui_tasks[key], completed=1, status=status)

    # Build categorization sets
    _tui_step_start("build_list")
    pending_nums = {pr.number for pr in pending_approval}
    flagged_nums = {pr.number for pr, _ in det_flagged_prs}
    # LLM flagged will be populated as they arrive
    llm_flagged_nums: set[int] = set()
    passing_nums = {pr.number for pr in passing_prs}

    # Map PR number to assessment for flagged PRs
    assessment_map: dict[int, PRAssessment] = {pr.number: asmt for pr, asmt in det_flagged_prs}

    # Build entries
    entries: list[PRListEntry] = []
    pr_map: dict[int, PRData] = {}
    # Also build set of LLM candidate PR numbers for status tracking
    _llm_candidate_nums = {pr.number for pr in llm_candidates}

    for pr in all_prs:
        if not author_filter.should_include(pr.author_association):
            continue
        if not include_drafts and pr.is_draft:
            continue
        pr_map[pr.number] = pr

        if pr.number in flagged_nums:
            cat = PRCategory.NON_LLM_ISSUES
        elif pr.number in pending_nums:
            cat = PRCategory.WORKFLOW_APPROVAL
        elif pr.number in passing_nums:
            cat = PRCategory.PASSING
        elif pr.number in already_triaged_nums:
            cat = PRCategory.ALREADY_TRIAGED
        else:
            # LLM candidates — show as passing with "waiting for LLM" status
            cat = PRCategory.PASSING
        entry = PRListEntry(pr, cat)
        # Restore cached action for already-triaged PRs
        if cat == PRCategory.ALREADY_TRIAGED and viewer_login and pr.head_sha:
            _cached_cls, cached_act = _get_cached_classification(
                ctx.github_repository, pr.number, pr.head_sha, viewer_login
            )
            if cached_act:
                entry.action_taken = cached_act
        entries.append(entry)

    _tui_step_done("build_list", f"{len(entries)} entries")
    _tui_step_start("sort")

    # Sort: page first (keep batch order), then category, then author, PR#
    _ORDER = {
        PRCategory.WORKFLOW_APPROVAL: 0,
        PRCategory.NON_LLM_ISSUES: 1,
        PRCategory.LLM_ERRORS: 2,
        PRCategory.LLM_FLAGGED: 3,
        PRCategory.PASSING: 4,
        PRCategory.STALE_REVIEW: 5,
        PRCategory.ALREADY_TRIAGED: 6,
        PRCategory.SKIPPED: 7,
    }
    entries.sort(
        key=lambda e: (
            e.page,
            _ORDER.get(e.category, 99),
            e.pr.author_login.lower(),
            e.pr.number,
        )
    )
    has_more_batches = load_more_fn is not None

    # Pre-fill: keep loading batches until we have enough visible entries
    # to fill the first page. Estimate visible rows from terminal size.
    _tui_step_start("fill_pages")
    if has_more_batches:
        import shutil as _shutil

        _term_h = _shutil.get_terminal_size().lines
        _est_header = 6 if selection_criteria else 5
        _est_avail = _term_h - _est_header - 5 - 2  # header + footer + borders
        _est_bottom = max(5, _est_avail // 2)
        _est_list = max(5, _est_avail - _est_bottom)
        _est_visible = _est_list - 3
        _target = _est_visible  # fill first page only
        _pages_loaded = 0

        while has_more_batches and len(entries) < _target and load_more_fn is not None:
            result = load_more_fn()
            if result is None:
                has_more_batches = False
                break
            new_all_prs, new_det_flagged, new_pending, new_passing, new_triaged_nums, more_available = result
            has_more_batches = more_available
            if not new_all_prs:
                break

            # Accumulate page counts into stats
            ctx.stats.cumulative_all_prs += len(new_all_prs)
            ctx.stats.cumulative_pending_approval += len(new_pending)
            ctx.stats.cumulative_passing += len(new_passing)
            ctx.stats.cumulative_deterministic_flags += len(new_det_flagged)
            ctx.stats.cumulative_already_triaged += len(new_triaged_nums)

            new_flagged_nums = {pr.number for pr, _ in new_det_flagged}
            new_pending_nums = {pr.number for pr in new_pending}
            new_passing_nums = {pr.number for pr in new_passing}
            existing_nums = {e.pr.number for e in entries}

            for pr in new_all_prs:
                if pr.number in existing_nums:
                    continue
                if not author_filter.should_include(pr.author_association):
                    continue
                if not include_drafts and pr.is_draft:
                    continue
                if pr.number in new_flagged_nums:
                    cat = PRCategory.NON_LLM_ISSUES
                elif pr.number in new_pending_nums:
                    cat = PRCategory.WORKFLOW_APPROVAL
                elif pr.number in new_passing_nums:
                    cat = PRCategory.PASSING
                elif pr.number in new_triaged_nums:
                    cat = PRCategory.ALREADY_TRIAGED
                else:
                    cat = PRCategory.PASSING
                entries.append(PRListEntry(pr, cat, page=_pages_loaded + 1))

            for pr, asmt in new_det_flagged:
                assessment_map[pr.number] = asmt

            _pages_loaded += 1

        if _pages_loaded > 0:
            entries.sort(
                key=lambda e: (
                    e.page,
                    _ORDER.get(e.category, 99),
                    e.pr.author_login.lower(),
                    e.pr.number,
                )
            )
        _tui_step_done("fill_pages", f"{_pages_loaded} extra pages, {len(entries)} visible")
    else:
        _pages_loaded = 0
        _tui_step_done("fill_pages", "skipped")

    _action_timestamps: dict[int, float] = {}  # pr_number → monotonic time of action
    _prev_action_time: list[float] = [0.0]  # mutable ref for last action timestamp

    def _cache_action(pr: PRData, action: str) -> None:
        """Update the classification cache when a triage action is taken on a PR."""
        if viewer_login and pr.head_sha and action and action != "skipped":
            _save_classification_cache(
                ctx.github_repository, pr.number, pr.head_sha, viewer_login, "acted", action=action
            )
        # Track action timing
        if action and action != "skipped":
            _action_timestamps[pr.number] = time.monotonic()
            refresh_mgr.schedule_refresh(pr.number)

    def _apply_refresh_results() -> bool:
        """Collect background refresh results and update entries.

        Returns True if any entries were updated.
        """
        results = refresh_mgr.collect()
        if not results:
            return False

        changed = False
        for result in results:
            if result.pr_data is None:
                continue

            pr = result.pr_data
            pr_map[pr.number] = pr

            # Determine new category
            cat_map = {
                "flagged": PRCategory.NON_LLM_ISSUES,
                "pending_approval": PRCategory.WORKFLOW_APPROVAL,
                "in_progress": PRCategory.WORKFLOW_APPROVAL,
                "llm_candidate": PRCategory.PASSING,
                "draft_skipped": PRCategory.SKIPPED,
                "grace_period": PRCategory.SKIPPED,
            }
            new_cat = cat_map.get(result.category, PRCategory.SKIPPED)

            # Update assessment if flagged
            if result.assessment:
                assessment_map[pr.number] = result.assessment

            # Find and update existing entry
            for entry in entries:
                if entry.pr.number == pr.number:
                    old_cat = entry.category
                    entry.pr = pr
                    entry.category = new_cat
                    # Clear action_taken so the PR can be acted on again
                    entry.action_taken = ""
                    # Invalidate cached detail/diff for this PR
                    if tui._detail_pr_number == pr.number:
                        tui._detail_pr_number = None
                    if pr.number in diff_cache:
                        del diff_cache[pr.number]
                    if old_cat != new_cat:
                        changed = True
                    break

            # Manage polling: if CI is still pending, keep polling; otherwise stop
            if result.has_pending_ci:
                refresh_mgr.schedule_poll(pr.number)
            else:
                refresh_mgr.remove_from_poll(pr.number)

        if changed:
            entries.sort(
                key=lambda e: (
                    e.page,
                    _ORDER.get(e.category, 99),
                    e.pr.author_login.lower(),
                    e.pr.number,
                )
            )
            tui.update_entries(entries)
            tui.set_assessments(assessment_map)

        return changed

    # Background page loading state
    _next_page_num = _pages_loaded + 1 if has_more_batches else 1
    _page_load_future: Future | None = None

    def _start_loading_next_page() -> bool:
        """Start loading the next page of PRs in the background.

        Returns True if a load was started, False if already loading or no more data.
        """
        nonlocal _page_load_future
        if _page_load_future is not None:
            return False  # Already loading
        if not load_more_fn or not has_more_batches:
            return False
        _page_load_future = diff_executor.submit(load_more_fn)
        return True

    def _check_page_load_complete() -> bool:
        """Check if background page load is done. If so, merge results into entries.

        Returns True if new entries were merged, False otherwise.
        """
        nonlocal _page_load_future, has_more_batches, _next_page_num
        if _page_load_future is None or not _page_load_future.done():
            return False

        fut = _page_load_future
        _page_load_future = None

        try:
            result = fut.result()
        except Exception:
            has_more_batches = False
            tui.has_more_pages = False
            return False

        if result is None:
            has_more_batches = False
            tui.has_more_pages = False
            return False

        new_all_prs, new_det_flagged, new_pending, new_passing, new_triaged_nums, more_available = result
        has_more_batches = more_available
        tui.has_more_pages = more_available

        if not new_all_prs:
            return False

        # Accumulate page counts into stats
        ctx.stats.cumulative_all_prs += len(new_all_prs)
        ctx.stats.cumulative_pending_approval += len(new_pending)
        ctx.stats.cumulative_passing += len(new_passing)
        ctx.stats.cumulative_deterministic_flags += len(new_det_flagged)
        ctx.stats.cumulative_already_triaged += len(new_triaged_nums)

        new_flagged_nums = {pr.number for pr, _ in new_det_flagged}
        new_pending_nums = {pr.number for pr in new_pending}
        new_passing_nums = {pr.number for pr in new_passing}
        existing_nums = {e.pr.number for e in entries}

        new_entries: list[PRListEntry] = []
        for pr in new_all_prs:
            if pr.number in existing_nums:
                continue
            if not author_filter.should_include(pr.author_association):
                continue
            if not include_drafts and pr.is_draft:
                continue

            if pr.number in new_flagged_nums:
                cat = PRCategory.NON_LLM_ISSUES
            elif pr.number in new_pending_nums:
                cat = PRCategory.WORKFLOW_APPROVAL
            elif pr.number in new_passing_nums:
                cat = PRCategory.PASSING
            elif pr.number in new_triaged_nums:
                cat = PRCategory.ALREADY_TRIAGED
            else:
                cat = PRCategory.PASSING
            new_entries.append(PRListEntry(pr, cat, page=_next_page_num))

        # Update assessment map for new flagged PRs
        for pr, asmt in new_det_flagged:
            assessment_map[pr.number] = asmt

        # Set LLM status for new entries
        _llm_future_pr_nums = {pr.number for pr in ctx.llm_future_to_pr.values()}
        _now_page = time.monotonic()
        for entry in new_entries:
            n = entry.pr.number
            if n in ctx.llm_assessments:
                entry.llm_status = "flagged"
                entry.llm_duration = ctx.llm_durations.get(n, 0.0)
                entry.llm_attempts = ctx.llm_attempts.get(n, 0)
            elif n in {p.number for p in ctx.llm_passing}:
                entry.llm_status = "passed"
                entry.llm_duration = ctx.llm_durations.get(n, 0.0)
                entry.llm_attempts = ctx.llm_attempts.get(n, 0)
            elif n in ctx.llm_errors:
                entry.llm_status = "error"
                entry.llm_duration = ctx.llm_durations.get(n, 0.0)
                entry.llm_attempts = ctx.llm_attempts.get(n, 0)
            elif _is_bot_account(entry.pr.author_login):
                pass  # No LLM for bot PRs
            elif n in _llm_future_pr_nums:
                entry.llm_status = "pending"
                entry.llm_queue_time = _now_page
                entry.llm_submit_time = _now_page
            elif tui.llm_enabled and entry.category == PRCategory.PASSING:
                entry.llm_status = "pending"
                entry.llm_queue_time = _now_page
                entry.llm_submit_time = _now_page
            elif not tui.llm_enabled:
                entry.llm_status = "disabled"

        _next_page_num += 1
        tui.total_loaded_prs = len(entries) + len(new_entries)
        if new_entries:
            entries.extend(new_entries)
            entries.sort(
                key=lambda e: (
                    e.page,
                    _ORDER.get(e.category, 99),
                    e.pr.author_login.lower(),
                    e.pr.number,
                )
            )
            return True
        return False

    _tui_step_done("sort", "sorted")
    _tui_step_start("init_tui")

    _is_review = "review" in mode_desc.lower() if mode_desc else False
    tui = TriageTUI(
        title="Review" if _is_review else "Auto-Triage",
        mode_desc=mode_desc,
        github_repository=ctx.github_repository,
        selection_criteria=selection_criteria,
        total_matching_prs=total_matching_prs,
    )
    if _is_review:
        tui.set_review_mode(True)
    tui.has_more_pages = has_more_batches
    tui.total_loaded_prs = len(entries)
    tui.llm_enabled = run_llm and llm_executor is not None
    # Store LLM executor/model on context for on-demand LLM triggers
    ctx._llm_executor = llm_executor  # type: ignore[attr-defined]
    ctx._llm_model = llm_model  # type: ignore[attr-defined]

    # Track LLM submit times per PR (monotonic, for in-progress elapsed display)
    _llm_submit_times: dict[int, float] = {}  # pr_number → monotonic submit time

    # Set initial LLM status on entries
    _llm_in_progress_nums = {pr.number for pr in ctx.llm_future_to_pr.values()}
    _now = time.monotonic()
    for pr_num in _llm_in_progress_nums:
        _llm_submit_times[pr_num] = _now  # approximate: submitted just before TUI init

    for entry in entries:
        if entry.pr.number in ctx.llm_assessments:
            entry.llm_status = "flagged"
            entry.llm_duration = ctx.llm_durations.get(entry.pr.number, 0.0)
            entry.llm_attempts = ctx.llm_attempts.get(entry.pr.number, 0)
        elif entry.pr.number in {p.number for p in ctx.llm_passing}:
            entry.llm_status = "passed"
            entry.llm_duration = ctx.llm_durations.get(entry.pr.number, 0.0)
            entry.llm_attempts = ctx.llm_attempts.get(entry.pr.number, 0)
        elif entry.pr.number in ctx.llm_errors:
            entry.llm_status = "error"
            entry.llm_duration = ctx.llm_durations.get(entry.pr.number, 0.0)
            entry.llm_attempts = ctx.llm_attempts.get(entry.pr.number, 0)
        elif entry.pr.number in _llm_in_progress_nums:
            entry.llm_status = "pending"
            entry.llm_queue_time = _now
            entry.llm_submit_time = _now
        elif _is_bot_account(entry.pr.author_login):
            pass  # No LLM for bot PRs
        elif entry.pr.number in _llm_candidate_nums:
            entry.llm_status = "pending"
            entry.llm_queue_time = _now
            entry.llm_submit_time = _now
        elif tui.llm_enabled and entry.category == PRCategory.PASSING and llm_executor:
            # PR passed deterministic checks but not yet submitted for LLM — submit now
            entry.llm_status = "pending"
            entry.llm_queue_time = _now
            entry.llm_submit_time = _now
            if _is_review:
                _fut = llm_executor.submit(
                    _review_pr_as_assessment,
                    token=ctx.token,
                    github_repository=ctx.github_repository,
                    head_sha=entry.pr.head_sha,
                    pr_number=entry.pr.number,
                    pr_title=entry.pr.title,
                    pr_body=entry.pr.body,
                    llm_model=llm_model,
                )
            else:
                _fut = llm_executor.submit(
                    _cached_assess_pr,
                    github_repository=ctx.github_repository,
                    head_sha=entry.pr.head_sha,
                    pr_number=entry.pr.number,
                    pr_title=entry.pr.title,
                    pr_body=entry.pr.body,
                    check_status_summary=entry.pr.check_summary,
                    llm_model=llm_model,
                )
            ctx.llm_future_to_pr[_fut] = entry.pr
        elif not tui.llm_enabled:
            entry.llm_status = "disabled"

    tui.set_entries(entries)
    tui.set_assessments(assessment_map)

    # --- Background PR status refresh ---
    refresh_mgr = _PRRefreshManager(ctx.token, ctx.github_repository, run_api=run_api)

    # --- Diff cache and background fetching ---
    diff_cache: dict[int, str] = {}  # pr_number → diff text (or error message)
    diff_pending: dict[int, Future] = {}  # pr_number → in-flight Future
    diff_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="diff-fetch")

    def _submit_diff_fetch(pr_number: int, pr_url: str) -> None:
        """Submit a background diff fetch if not already cached or in-flight."""
        if pr_number in diff_cache or pr_number in diff_pending:
            return
        diff_pending[pr_number] = diff_executor.submit(
            _fetch_pr_diff, ctx.token, ctx.github_repository, pr_number
        )

    def _collect_diff_results(tui_ref: TriageTUI) -> None:
        """Move completed diff futures into the cache and update the TUI if relevant."""
        done = [num for num, fut in diff_pending.items() if fut.done()]
        for num in done:
            fut = diff_pending.pop(num)
            try:
                result = fut.result()
            except Exception:
                result = None
            if result:
                diff_cache[num] = result
            else:
                # Look up the PR URL for the error message
                pr_entry = pr_map.get(num)
                fallback_url = pr_entry.url if pr_entry else ""
                diff_cache[num] = f"Could not fetch diff. Review at: {fallback_url}/files"
            # Update TUI if this is the currently viewed PR
            cur = tui_ref.get_selected_entry()
            if cur and cur.pr.number == num:
                tui_ref.set_diff(num, diff_cache[num])

    def _ensure_diff_for_pr(tui_ref: TriageTUI, pr_number: int, pr_url: str) -> None:
        """Ensure a diff is available (from cache or pending). Update TUI immediately if cached."""
        if pr_number in diff_cache:
            tui_ref.set_diff(pr_number, diff_cache[pr_number])
        else:
            _submit_diff_fetch(pr_number, pr_url)

    _tui_step_done("init_tui", "ready")

    # Prefetch diff for first PR (blocking, since user sees it immediately)
    if entries:
        first_pr = entries[0].pr
        _tui_step_start("fetch_diff", f"PR #{first_pr.number}")
        diff_text = _fetch_pr_diff(ctx.token, ctx.github_repository, first_pr.number)
        if diff_text:
            diff_cache[first_pr.number] = diff_text
            tui.set_diff(first_pr.number, diff_text)
        else:
            fallback = f"Could not fetch diff. Review at: {first_pr.url}/files"
            diff_cache[first_pr.number] = fallback
            tui.set_diff(first_pr.number, fallback)
        _tui_step_done("fetch_diff", "loaded")
        # Prefetch diffs for next few PRs in background
        _tui_step_start("prefetch")
        for prefetch_entry in entries[1:4]:
            _submit_diff_fetch(prefetch_entry.pr.number, prefetch_entry.pr.url)
        _tui_step_done("prefetch", f"{min(3, len(entries) - 1)} queued")
    else:
        _tui_step_done("fetch_diff", "no PRs")
        _tui_step_done("prefetch", "skipped")

    _tui_progress.stop()

    # Print whitespace before TUI takes over, so scrolling back
    # shows a clear boundary between the startup output and the TUI
    import shutil

    term_lines = shutil.get_terminal_size().lines
    get_console().print("\n" * (term_lines * 3))

    t_tui_start = time.monotonic()
    _prev_action_time[0] = t_tui_start

    # Start background loading of second page if we only filled the first
    if has_more_batches and _page_load_future is None:
        _start_loading_next_page()

    # Pre-build LLM passing set (updated incrementally)
    llm_passing_nums: set[int] = {p.number for p in ctx.llm_passing}

    while not ctx.stats.quit_early:
        # Check if background page load completed — merge results
        if _check_page_load_complete():
            tui.update_entries(entries)
            tui.set_assessments(assessment_map)

        # Collect background PR refresh results
        _apply_refresh_results()

        # Collect LLM results if available (quiet in TUI — shown in status panel)
        llm_status = ctx.collect_llm_progress(quiet=True)
        if llm_status:
            tui.llm_progress = llm_status

        # Update LLM running/queued status based on actual future state
        # Don't rely on Future.running() alone — it's unreliable in CPython.
        # Instead, count actually-running futures and cap at executor's max_workers.
        _llm_pr_to_future: dict[int, Future] = {pr.number: fut for fut, pr in ctx.llm_future_to_pr.items()}
        _llm_completed_pr_nums = {pr.number for fut, pr in ctx.llm_future_to_pr.items() if fut.done()}
        _llm_result_pr_nums = (
            set(ctx.llm_assessments.keys()) | {p.number for p in ctx.llm_passing} | set(ctx.llm_errors)
        )
        # Collect undone futures and sort by queue time to determine which are truly running
        _undone_entries: list[PRListEntry] = []
        for entry in entries:
            if entry.llm_status in ("in_progress", "pending"):
                n = entry.pr.number
                fut = _llm_pr_to_future.get(n)
                if fut is not None and not fut.done():
                    _undone_entries.append(entry)
                elif n in _llm_completed_pr_nums and n not in _llm_result_pr_nums:
                    entry.llm_status = "error"
                    entry.llm_duration = 0.0

        # Sort by queue time — earliest queued are most likely running
        _undone_entries.sort(key=lambda e: e.llm_queue_time if e.llm_queue_time > 0 else float("inf"))
        _max_running = llm_executor._max_workers if llm_executor else 4
        for i, entry in enumerate(_undone_entries):
            if i < _max_running:
                if entry.llm_status != "in_progress":
                    # Transitioning from pending to in_progress — record start time
                    entry.llm_submit_time = time.monotonic()
                entry.llm_status = "in_progress"
            else:
                entry.llm_status = "pending"

        # Collect any completed diff fetches
        _collect_diff_results(tui)

        # Update LLM-flagged entries
        # Refresh the passing set incrementally
        for p in ctx.llm_passing:
            llm_passing_nums.add(p.number)

        # Update LLM status and re-categorize entries when LLM results arrive
        _needs_resort = False
        for entry in entries:
            n = entry.pr.number
            if n in ctx.llm_assessments and entry.llm_status not in ("flagged",):
                assessment = ctx.llm_assessments[n]
                if getattr(assessment, "should_report", False):
                    entry.category = PRCategory.LLM_ERRORS
                else:
                    entry.category = PRCategory.LLM_FLAGGED
                entry.llm_status = "flagged"
                entry.llm_duration = ctx.llm_durations.get(n, 0.0)
                entry.llm_attempts = ctx.llm_attempts.get(n, 0)
                llm_flagged_nums.add(n)
                assessment_map[n] = assessment
                if tui._detail_pr_number == n:
                    tui._detail_pr_number = None
                _needs_resort = True
            elif n in llm_passing_nums and entry.llm_status not in ("passed",):
                entry.llm_status = "passed"
                entry.llm_duration = ctx.llm_durations.get(n, 0.0)
                entry.llm_attempts = ctx.llm_attempts.get(n, 0)
                passing_nums.add(n)
            elif n in ctx.llm_errors and entry.llm_status not in ("error",):
                entry.llm_status = "error"
                entry.llm_duration = ctx.llm_durations.get(n, 0.0)
                entry.llm_attempts = ctx.llm_attempts.get(n, 0)
        if _needs_resort:
            entries.sort(
                key=lambda e: (
                    e.page,
                    _ORDER.get(e.category, 99),
                    e.pr.author_login.lower(),
                    e.pr.number,
                )
            )
            tui.update_entries(entries)
            tui.set_assessments(assessment_map)

        # Update background activity indicators for the status panel
        _bg: dict[str, int | str] = {}
        _n_diffs = len(diff_pending)
        if _n_diffs:
            _bg["diffs"] = _n_diffs
        if _page_load_future is not None and not _page_load_future.done():
            _bg["paging"] = 1
        _n_refresh = refresh_mgr.active_count
        if _n_refresh:
            _bg["refresh"] = _n_refresh
        _llm_pending = [f for f in ctx.llm_future_to_pr if not f.done()]
        if _llm_pending:
            _llm_running = sum(1 for f in _llm_pending if f.running())
            _llm_queued = len(_llm_pending) - _llm_running
            _llm_parts = []
            if _llm_running:
                _llm_parts.append(f"{_llm_running} running")
            if _llm_queued:
                _llm_parts.append(f"{_llm_queued} queued")
            _bg["LLM"] = ", ".join(_llm_parts)
        tui._bg_activity = _bg

        # Use timeout when background work is active so we can pick up results
        _poll_timeout = 1.0 if _bg else None
        entry, action = tui.run_interactive(timeout=_poll_timeout)

        # Timeout — no input; check for background updates and re-render
        if action is None:
            _apply_refresh_results()
            continue

        # Auto-fetch diff when cursor moves to a different PR (non-blocking)
        if tui.cursor_changed() and tui.needs_diff_fetch():
            current_entry = tui.get_selected_entry()
            if current_entry:
                _ensure_diff_for_pr(tui, current_entry.pr.number, current_entry.pr.url)
                # Prefetch a few PRs ahead of the cursor
                for i in range(tui.cursor + 1, min(tui.cursor + 4, len(entries))):
                    _submit_diff_fetch(entries[i].pr.number, entries[i].pr.url)

        if action == TUIAction.QUIT:
            tui.disable_mouse()
            ctx.stats.quit_early = True
            break

        if action in (
            TUIAction.UP,
            TUIAction.DOWN,
            TUIAction.PAGE_UP,
            TUIAction.PAGE_DOWN,
            TUIAction.NEXT_PAGE,
            TUIAction.PREV_PAGE,
            TUIAction.NEXT_SECTION,
            TUIAction.TOGGLE_SELECT,
            TUIAction.SHOW_DIFF,
        ):
            # Check if background page load completed — merge results
            if _check_page_load_complete():
                tui.update_entries(entries)
                tui.set_assessments(assessment_map)

            # Lazy pagination: start loading next page in the background when
            # remaining entries can't fill current + next page
            if has_more_batches and _page_load_future is None and tui._visible_rows > 0:
                active_below = sum(1 for e in entries[tui.cursor :] if not e.action_taken)
                if active_below < tui._visible_rows * 2:
                    _start_loading_next_page()
            continue

        if action == TUIAction.BATCH_ACTION:
            selected_entries = tui.get_selected_entries()
            if not selected_entries:
                continue
            # Batch approve selected workflow PRs — show diffs first
            tui.disable_mouse()
            get_console().print("\n" * 25)
            get_console().clear()
            get_console().rule("[bold green]Batch workflow approval — diff review[/]", style="green")
            get_console().print(
                f"\n[info]Reviewing diffs for {len(selected_entries)} "
                f"{'PRs' if len(selected_entries) != 1 else 'PR'} before approval.[/]"
            )
            get_console().print(
                "[info]Press Enter to approve, \\[f] to flag as suspicious, \\[q] to quit.[/]\n"
            )

            if ctx.dry_run:
                get_console().print("[warning]Dry run — skipping batch approval.[/]")
                for sel_entry in selected_entries:
                    sel_entry.selected = False
            else:
                # Phase 1: Show diffs and collect approve/flag decisions
                to_approve: list[tuple[PRListEntry, list[dict]]] = []
                flagged_entries: list[PRListEntry] = []
                for sel_entry in selected_entries:
                    if ctx.stats.quit_early:
                        break
                    sel_pr = sel_entry.pr
                    pending_runs = _find_pending_workflow_runs(
                        ctx.token, ctx.github_repository, sel_pr.head_sha
                    )
                    if not pending_runs:
                        # No pending runs — rerun completed (no diff review needed)
                        if sel_pr.head_sha:
                            completed_runs = _find_workflow_runs_by_status(
                                ctx.token, ctx.github_repository, sel_pr.head_sha, "completed"
                            )
                            rerun_count = 0
                            if completed_runs:
                                for run in completed_runs:
                                    if _rerun_workflow_run(ctx.token, ctx.github_repository, run):
                                        rerun_count += 1
                            if rerun_count:
                                get_console().print(
                                    f"  [success]Rerun {rerun_count} workflow "
                                    f"{'runs' if rerun_count != 1 else 'run'} for "
                                    f"PR {_pr_link(sel_pr)}.[/]"
                                )
                                ctx.stats.total_rerun += 1
                                sel_entry.action_taken = "rerun"
                                _cache_action(sel_pr, "rerun")
                            else:
                                get_console().print(
                                    f"  [warning]No workflow runs found for PR {_pr_link(sel_pr)}.[/]"
                                )
                        sel_entry.selected = False
                        continue

                    # Show diff for this PR
                    get_console().rule(f"[cyan]PR {_pr_link(sel_pr)}[/]", style="dim")
                    get_console().print(f"  [bold]{sel_pr.title}[/] by {sel_pr.author_login}\n")
                    confirm = _show_diff_and_confirm(
                        ctx.token,
                        ctx.github_repository,
                        sel_pr,
                        forced_answer=ctx.answer_triage,
                        confirm_message="Press Enter to approve, \\[f] to flag as suspicious, \\[q] to quit",
                    )
                    if confirm == ContinueAction.QUIT:
                        ctx.stats.quit_early = True
                        break
                    if confirm == ContinueAction.FLAG:
                        get_console().print(f"  [bold red]Flagged PR {_pr_link(sel_pr)} as suspicious.[/]")
                        sel_entry.action_taken = "suspicious"
                        _cache_action(sel_pr, "suspicious")
                        flagged_entries.append(sel_entry)
                        sel_entry.selected = False
                    else:
                        to_approve.append((sel_entry, pending_runs))

                # Phase 2: Batch approve all confirmed PRs
                if to_approve and not ctx.stats.quit_early:
                    get_console().print()
                    get_console().rule("[bold green]Approving workflows[/]", style="green")
                    for sel_entry, pending_runs in to_approve:
                        sel_pr = sel_entry.pr
                        approved = _approve_workflow_runs(ctx.token, ctx.github_repository, pending_runs)
                        if approved:
                            get_console().print(
                                f"  [success]Approved {approved} workflow "
                                f"{'runs' if approved != 1 else 'run'} for "
                                f"PR {_pr_link(sel_pr)}.[/]"
                            )
                            ctx.stats.total_workflows_approved += 1
                            sel_entry.action_taken = "workflow approved"
                            _cache_action(sel_pr, "workflow approved")
                        else:
                            get_console().print(
                                f"  [error]Failed to approve workflows for PR {_pr_link(sel_pr)}.[/]"
                            )
                        sel_entry.selected = False

                # Deselect any remaining entries
                for sel_entry in selected_entries:
                    sel_entry.selected = False

            get_console().print("\n[dim]Press any key to return to TUI...[/]")
            _read_char()
            continue

        if entry is None:
            continue

        pr = entry.pr

        if action == TUIAction.OPEN:
            webbrowser.open(pr.url)
            continue

        if action == TUIAction.SKIP:
            # Already marked as skipped by the TUI
            ctx.stats.total_skipped_action += 1
            continue

        # Direct triage actions from TUI (without entering detailed review)
        if isinstance(action, TUIAction) and action.name.startswith("ACTION_"):
            tui.disable_mouse()
            _execute_tui_direct_action(ctx, tui, entry, action, assessment_map, on_action=_cache_action)
            if ctx.stats.quit_early:
                break
            continue

        if action == TUIAction.SELECT:
            # Drop into detailed review — disable mouse while outside TUI
            tui.disable_mouse()
            while not ctx.stats.quit_early:
                cur_entry = tui.get_selected_entry()
                if cur_entry is None:
                    break
                cur_pr = cur_entry.pr

                # Print whitespace so each PR review is visually separated when scrolling up
                get_console().print("\n" * 25)
                get_console().clear()

                go_back = False
                was_skipped = cur_entry.action_taken == "skipped"
                if cur_entry.category in (
                    PRCategory.NON_LLM_ISSUES,
                    PRCategory.LLM_FLAGGED,
                    PRCategory.LLM_ERRORS,
                ):
                    assessment = assessment_map.get(cur_pr.number)
                    if assessment:
                        action_result = _prompt_and_execute_flagged_pr(
                            ctx,
                            cur_pr,
                            assessment,
                            allow_back=True,
                            compact=True,
                            default_skip=was_skipped,
                            run_api=run_api,
                            classification=cur_entry.category.value,
                            llm_status=cur_entry.llm_status,
                            llm_duration=cur_entry.llm_duration,
                            llm_queue_time=cur_entry.llm_queue_time,
                            llm_submit_time=cur_entry.llm_submit_time,
                            llm_attempts=cur_entry.llm_attempts,
                        )
                        if isinstance(action_result, DeterministicResult):
                            # PR state changed — re-categorize and notify user
                            console_print(
                                f"[warning]PR #{cur_pr.number} was re-evaluated due to state change. "
                                f"Returning to list.[/]"
                            )
                            if action_result.category == "flagged" and action_result.assessment:
                                assessment_map[cur_pr.number] = action_result.assessment
                                tui.set_assessments(assessment_map)
                            elif action_result.category == "llm_candidate":
                                cur_entry.category = PRCategory.PASSING
                                cur_entry.action_taken = ""
                            elif action_result.category == "pending_approval":
                                cur_entry.category = PRCategory.WORKFLOW_APPROVAL
                                cur_entry.action_taken = ""
                            tui.update_entries(entries)
                            get_console().print("[dim]Press any key to continue...[/]")
                            _read_char()
                            go_back = True
                        elif action_result is True:
                            go_back = True
                        else:
                            cur_entry.action_taken = _infer_last_action(ctx.stats)
                            _cache_action(cur_pr, cur_entry.action_taken)
                    else:
                        get_console().print(f"[warning]No assessment available for PR #{cur_pr.number}[/]")
                        get_console().print("[dim]Press any key to continue...[/]")
                        _read_char()
                elif cur_entry.category == PRCategory.WORKFLOW_APPROVAL:
                    go_back = _review_single_workflow_pr(
                        ctx, cur_pr, allow_back=True, compact=True, default_skip=was_skipped
                    )
                    if not go_back:
                        cur_entry.action_taken = _infer_last_action(ctx.stats)
                        _cache_action(cur_pr, cur_entry.action_taken)
                elif cur_entry.category == PRCategory.PASSING:
                    _passing_snapshot = _snapshot_pr_state(cur_pr)
                    author_profile = _fetch_author_profile(
                        ctx.token, cur_pr.author_login, ctx.github_repository
                    )
                    _display_pr_info_panels(
                        cur_pr,
                        author_profile,
                        compact=True,
                        classification=cur_entry.category.value,
                        llm_status=cur_entry.llm_status,
                        llm_duration=cur_entry.llm_duration,
                    )
                    if cur_entry.llm_status in ("in_progress", "pending"):
                        console_print("[info]LLM review is still in progress for this PR.[/]")
                    else:
                        console_print("[success]This looks like a PR that is ready for review.[/]")

                    if not ctx.dry_run:
                        if cur_entry.llm_status in ("in_progress", "pending"):
                            console_print("[dim]LLM still running — press any key to go back to list...[/]")
                            _read_char()
                            go_back = True
                        if not go_back:
                            if was_skipped:
                                passing_default = TriageAction.SKIP
                            else:
                                passing_default = TriageAction.READY
                        if not go_back:
                            act = prompt_triage_action(
                                f"Action for PR {_pr_link(cur_pr)}?",
                                default=passing_default,
                                forced_answer=ctx.answer_triage,
                                exclude={TriageAction.DRAFT} if cur_pr.is_draft else None,
                                pr_url=cur_pr.url,
                                token=ctx.token,
                                github_repository=ctx.github_repository,
                                pr_number=cur_pr.number,
                                allow_back=True,
                            )
                            if act == TriageAction.BACK:
                                go_back = True
                            elif act == TriageAction.QUIT:
                                ctx.stats.quit_early = True
                            elif act == TriageAction.SKIP:
                                cur_entry.action_taken = "skipped"
                            else:
                                stale = _execute_triage_action(
                                    ctx,
                                    cur_pr,
                                    act,
                                    draft_comment="",
                                    close_comment="",
                                    snapshot=_passing_snapshot,
                                    run_api=run_api,
                                )
                                if stale is not None:
                                    console_print(
                                        f"[warning]PR #{cur_pr.number} state changed. Returning to list.[/]"
                                    )
                                    if stale.category == "flagged" and stale.assessment:
                                        cur_entry.category = PRCategory.NON_LLM_ISSUES
                                        assessment_map[cur_pr.number] = stale.assessment
                                        tui.set_assessments(assessment_map)
                                    tui.update_entries(entries)
                                    get_console().print("[dim]Press any key to continue...[/]")
                                    _read_char()
                                    go_back = True
                                else:
                                    cur_entry.action_taken = (
                                        "ready" if act == TriageAction.READY else act.value
                                    )
                                    _cache_action(cur_pr, cur_entry.action_taken)
                elif cur_entry.category == PRCategory.ALREADY_TRIAGED:
                    # Re-evaluate: enrich with fresh data and run deterministic checks
                    get_console().print(f"[info]Re-evaluating PR #{cur_pr.number}...[/]")
                    det = _reevaluate_triaged_pr(
                        cur_pr,
                        token=ctx.token,
                        github_repository=ctx.github_repository,
                        run_api=run_api,
                    )
                    if det.category == "flagged" and det.assessment:
                        # Deterministic issues found — show flagged review
                        cur_entry.category = PRCategory.NON_LLM_ISSUES
                        assessment_map[cur_pr.number] = det.assessment
                        tui.set_assessments(assessment_map)
                        action_result = _prompt_and_execute_flagged_pr(
                            ctx,
                            cur_pr,
                            det.assessment,
                            allow_back=True,
                            compact=True,
                            run_api=run_api,
                            classification=cur_entry.category.value,
                            llm_status=cur_entry.llm_status,
                            llm_duration=cur_entry.llm_duration,
                            llm_queue_time=cur_entry.llm_queue_time,
                            llm_submit_time=cur_entry.llm_submit_time,
                            llm_attempts=cur_entry.llm_attempts,
                        )
                        if isinstance(action_result, DeterministicResult):
                            go_back = True  # state changed again, return to list
                        elif action_result is True:
                            go_back = True
                        else:
                            cur_entry.action_taken = _infer_last_action(ctx.stats)
                            _cache_action(cur_pr, cur_entry.action_taken)
                    elif det.category == "llm_candidate":
                        # No deterministic issues — submit for LLM in background
                        if run_llm and llm_executor:
                            if _is_review:
                                fut = llm_executor.submit(
                                    _review_pr_as_assessment,
                                    token=ctx.token,
                                    github_repository=ctx.github_repository,
                                    head_sha=cur_pr.head_sha,
                                    pr_number=cur_pr.number,
                                    pr_title=cur_pr.title,
                                    pr_body=cur_pr.body,
                                    llm_model=llm_model,
                                )
                            else:
                                fut = llm_executor.submit(
                                    _cached_assess_pr,
                                    github_repository=ctx.github_repository,
                                    head_sha=cur_pr.head_sha,
                                    pr_number=cur_pr.number,
                                    pr_title=cur_pr.title,
                                    pr_body=cur_pr.body,
                                    check_status_summary=cur_pr.check_summary,
                                    llm_model=llm_model,
                                )
                            ctx.llm_future_to_pr[fut] = cur_pr
                            # Keep as PASSING with LLM in progress
                            cur_entry.category = PRCategory.PASSING
                            cur_entry.action_taken = ""
                            cur_entry.llm_status = "pending"
                            cur_entry.llm_queue_time = time.monotonic()
                            cur_entry.llm_submit_time = 0.0
                            tui.update_entries(entries)
                            get_console().print(
                                f"[info]PR #{cur_pr.number} passes deterministic checks. "
                                f"LLM assessment submitted — status will update automatically.[/]"
                            )
                            get_console().print("[dim]Press any key to continue...[/]")
                            _read_char()
                        else:
                            # No LLM — treat as passing
                            cur_entry.category = PRCategory.PASSING
                            passing_nums.add(cur_pr.number)
                            tui.update_entries(entries)
                            # Show passing review
                            author_profile = _fetch_author_profile(
                                ctx.token, cur_pr.author_login, ctx.github_repository
                            )
                            _display_pr_info_panels(
                                cur_pr,
                                author_profile,
                                compact=True,
                                classification=cur_entry.category.value,
                                llm_status=cur_entry.llm_status,
                                llm_duration=cur_entry.llm_duration,
                                llm_queue_time=cur_entry.llm_queue_time,
                                llm_submit_time=cur_entry.llm_submit_time,
                                llm_attempts=cur_entry.llm_attempts,
                            )
                            console_print("[success]Re-evaluation: this PR passes all checks.[/]")
                            if not ctx.dry_run:
                                act = prompt_triage_action(
                                    f"Action for PR {_pr_link(cur_pr)}?",
                                    default=TriageAction.READY,
                                    forced_answer=ctx.answer_triage,
                                    exclude={TriageAction.DRAFT} if cur_pr.is_draft else None,
                                    pr_url=cur_pr.url,
                                    token=ctx.token,
                                    github_repository=ctx.github_repository,
                                    pr_number=cur_pr.number,
                                    allow_back=True,
                                )
                                if act == TriageAction.BACK:
                                    go_back = True
                                elif act == TriageAction.QUIT:
                                    ctx.stats.quit_early = True
                                elif act == TriageAction.SKIP:
                                    cur_entry.action_taken = "skipped"
                                else:
                                    _execute_triage_action(
                                        ctx, cur_pr, act, draft_comment="", close_comment=""
                                    )
                                    cur_entry.action_taken = (
                                        "ready" if act == TriageAction.READY else act.value
                                    )
                                    _cache_action(cur_pr, cur_entry.action_taken)
                    elif det.category == "pending_approval":
                        cur_entry.category = PRCategory.WORKFLOW_APPROVAL
                        tui.update_entries(entries)
                        go_back = _review_single_workflow_pr(
                            ctx,
                            cur_pr,
                            allow_back=True,
                            compact=True,
                        )
                        if not go_back:
                            cur_entry.action_taken = _infer_last_action(ctx.stats)
                            _cache_action(cur_pr, cur_entry.action_taken)
                    elif det.category == "in_progress":
                        get_console().print(
                            f"[info]PR #{cur_pr.number} has workflows in progress. "
                            f"Press Esc to go back or any key to continue...[/]"
                        )
                        ch = _read_char()
                        if ch == "\x1b":
                            go_back = True
                    else:
                        # grace_period, draft_skipped, etc.
                        get_console().print(
                            f"[dim]PR #{cur_pr.number} — {det.category.replace('_', ' ')}. "
                            f"Press Esc to go back or any key to continue...[/]"
                        )
                        ch = _read_char()
                        if ch == "\x1b":
                            go_back = True
                elif cur_entry.category == PRCategory.SKIPPED:
                    get_console().print(
                        f"[dim]PR #{cur_pr.number} — no action available. "
                        f"Press Esc to go back or any key to continue...[/]"
                    )
                    ch = _read_char()
                    if ch == "\x1b":
                        go_back = True
                else:
                    get_console().print(
                        f"[dim]PR #{cur_pr.number} — press Esc to go back or any key to continue...[/]"
                    )
                    ch = _read_char()
                    if ch == "\x1b":
                        go_back = True

                if go_back or ctx.stats.quit_early:
                    break

                # Print the action taken so it's visible when scrolling back
                if cur_entry.action_taken:
                    get_console().print(
                        f"\n[bold]>>> PR #{cur_pr.number}: {cur_entry.action_taken.upper()} <<<[/]\n"
                    )

                # Advance to next PR
                old_cursor = tui.cursor
                tui.move_cursor(1)

                # If cursor didn't move, we're at the end of the list
                if tui.cursor == old_cursor:
                    break

    # Cleanup background threads with countdown
    tui.disable_mouse()
    console = get_console()
    console.clear()

    # Count active background jobs
    _active_jobs: list[str] = []
    _n_diffs = sum(1 for f in diff_pending.values() if not f.done())
    if _n_diffs:
        _active_jobs.append(f"{_n_diffs} diff fetch{'es' if _n_diffs != 1 else ''}")
    if _page_load_future is not None and not _page_load_future.done():
        _active_jobs.append("1 page load")
    _n_refresh_active = refresh_mgr.active_count
    if _n_refresh_active:
        _active_jobs.append(f"{_n_refresh_active} PR refresh{'es' if _n_refresh_active != 1 else ''}")
    _llm_active = [f for f in ctx.llm_future_to_pr if not f.done()]
    if _llm_active:
        _active_jobs.append(f"{len(_llm_active)} LLM review{'s' if len(_llm_active) != 1 else ''}")

    # Collect action summary BEFORE shutdown (entries may become inaccessible)
    acted_entries = []
    # Sort by action timestamp to show in chronological order
    acted = [
        (e, _action_timestamps.get(e.pr.number, 0.0))
        for e in entries
        if e.action_taken and e.action_taken != "skipped" and e.pr.number in _action_timestamps
    ]
    acted.sort(key=lambda x: x[1])

    prev_t = t_tui_start
    for e, action_t in acted:
        duration = action_t - prev_t if action_t > 0 else 0.0
        acted_entries.append(
            {
                "number": e.pr.number,
                "title": e.pr.title,
                "url": e.pr.url,
                "author": e.pr.author_login,
                "suggested": tui.get_suggested_action(e),
                "action": e.action_taken,
                "llm_status": e.llm_status,
                "duration": duration,
            }
        )
        prev_t = action_t

    # Shutdown all background threads — cancel immediately, then wait with timeout
    all_futures: list[Future] = list(diff_pending.values()) + _llm_active
    if _page_load_future is not None:
        all_futures.append(_page_load_future)
    for f in all_futures:
        f.cancel()
    refresh_mgr.shutdown()
    diff_executor.shutdown(wait=False, cancel_futures=True)

    if _active_jobs:
        _total_jobs = (
            _n_diffs
            + (1 if _page_load_future and not _page_load_future.done() else 0)
            + _n_refresh_active
            + len(_llm_active)
        )
        console.print(
            f"\n[warning]Stopping {_total_jobs} background "
            f"{'jobs' if _total_jobs != 1 else 'job'}: {', '.join(_active_jobs)}[/]"
        )
        console.print("[info]Waiting up to 10 seconds for graceful shutdown...[/]")

        for remaining in range(10, 0, -1):
            still_pending = [f for f in all_futures if not f.done()]
            if not still_pending:
                console.print("[success]All background jobs stopped.[/]")
                break
            console.print(
                f"  [dim]{len(still_pending)} "
                f"job{'s' if len(still_pending) != 1 else ''} remaining... {remaining}s[/]"
            )
            time.sleep(1)
        else:
            still_pending = [f for f in all_futures if not f.done()]
            if still_pending:
                console.print(
                    f"[error]Timed out waiting for {len(still_pending)} "
                    f"{'jobs' if len(still_pending) != 1 else 'job'}. "
                    f"Will force exit after session summary.[/]"
                )

    return acted_entries, t_tui_start


def _infer_last_action(stats: TriageStats) -> str:
    """Try to infer the last action from stats changes.

    This is a heuristic — we check which stat counter was last incremented.
    """
    # We can't perfectly determine this, but we can use the stats object fields
    # Just return a generic marker
    if stats.total_converted > getattr(stats, "_prev_converted", 0):
        stats._prev_converted = stats.total_converted  # type: ignore[attr-defined]
        return "drafted"
    if stats.total_commented > getattr(stats, "_prev_commented", 0):
        stats._prev_commented = stats.total_commented  # type: ignore[attr-defined]
        return "commented"
    if stats.total_closed > getattr(stats, "_prev_closed", 0):
        stats._prev_closed = stats.total_closed  # type: ignore[attr-defined]
        return "closed"
    if stats.total_rebased > getattr(stats, "_prev_rebased", 0):
        stats._prev_rebased = stats.total_rebased  # type: ignore[attr-defined]
        return "rebased"
    if stats.total_rerun > getattr(stats, "_prev_rerun", 0):
        stats._prev_rerun = stats.total_rerun  # type: ignore[attr-defined]
        return "rerun"
    if stats.total_ready > getattr(stats, "_prev_ready", 0):
        stats._prev_ready = stats.total_ready  # type: ignore[attr-defined]
        return "ready"
    if stats.total_skipped_action > getattr(stats, "_prev_skipped", 0):
        stats._prev_skipped = stats.total_skipped_action  # type: ignore[attr-defined]
        return "skipped"
    return ""


def _review_single_workflow_pr(
    ctx: TriageContext,
    pr: PRData,
    *,
    allow_back: bool = False,
    compact: bool = False,
    default_skip: bool = False,
) -> bool:
    """Review a single PR that needs workflow approval (used by TUI mode).

    Returns True if the user chose to go back to the TUI without taking action.
    """
    author_profile = _fetch_author_profile(ctx.token, pr.author_login, ctx.github_repository)
    pending_runs = _find_pending_workflow_runs(ctx.token, ctx.github_repository, pr.head_sha)

    check_counts: dict[str, int] = {}
    if pr.head_sha:
        check_counts = _fetch_check_status_counts(ctx.token, ctx.github_repository, pr.head_sha)

    _display_workflow_approval_panel(pr, author_profile, pending_runs, check_counts, compact=compact)

    if ctx.dry_run:
        console_print("[warning]Dry run — skipping workflow approval.[/]")
        return False

    if default_skip:
        default_action = TriageAction.SKIP
    elif not pending_runs:
        console_print(
            f"  [info]No pending workflow runs for PR {_pr_link(pr)}. "
            f"Attempting to rerun completed workflows...[/]"
        )
        default_action = TriageAction.RERUN
    else:
        default_action = TriageAction.RERUN

    action = prompt_triage_action(
        f"Action for PR {_pr_link(pr)}?",
        default=default_action,
        forced_answer=ctx.answer_triage,
        exclude={TriageAction.DRAFT} if pr.is_draft else None,
        pr_url=pr.url,
        token=ctx.token,
        github_repository=ctx.github_repository,
        pr_number=pr.number,
        allow_back=allow_back,
        label_overrides={TriageAction.RERUN: "review and approve workflows"},
    )

    if action == TriageAction.BACK:
        return True

    if action == TriageAction.QUIT:
        ctx.stats.quit_early = True
        return False
    if action == TriageAction.SKIP:
        console_print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
        return False

    if action == TriageAction.RERUN:
        if pending_runs:
            approved = _approve_workflow_runs(ctx.token, ctx.github_repository, pending_runs)
            if approved:
                console_print(
                    f"  [success]Approved {approved} workflow "
                    f"{'runs' if approved != 1 else 'run'} for PR {_pr_link(pr)}.[/]"
                )
                ctx.stats.total_workflows_approved += 1
        else:
            # Try rerunning completed runs
            if pr.head_sha:
                completed_runs = _find_workflow_runs_by_status(
                    ctx.token, ctx.github_repository, pr.head_sha, "completed"
                )
                rerun_count = 0
                if completed_runs:
                    for run in completed_runs:
                        if _rerun_workflow_run(ctx.token, ctx.github_repository, run):
                            console_print(f"  [success]Rerun triggered for: {run.get('name', run['id'])}[/]")
                            rerun_count += 1
                if rerun_count:
                    ctx.stats.total_rerun += 1
                else:
                    console_print(f"  [warning]No workflow runs found to rerun for PR {_pr_link(pr)}.[/]")
    else:
        _execute_triage_action(ctx, pr, action, draft_comment="", close_comment="")
    return False


def _filter_candidate_prs(
    all_prs: list[PRData],
    *,
    author_filter: AuthorFilter = AuthorFilter.CONTRIBUTORS,
    include_drafts: bool,
    checks_state: str,
    min_commits_behind: int,
    max_num: int,
    also_accepted: set[int] | None = None,
    quiet: bool = False,
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
        elif not author_filter.should_include(pr.author_association):
            total_skipped_collaborator += 1
            if verbose:
                console_print(
                    f"  [dim]Skipping PR {_pr_link(pr)} by "
                    f"{pr.author_association.lower()} {pr.author_login} "
                    f"(filter: {author_filter.value})[/]"
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
    if not quiet:
        console_print(
            f"\n[info]Fetched {len(all_prs)} {'PRs' if len(all_prs) != 1 else 'PR'}, "
            f"{skipped_text}"
            f"assessing {len(candidate_prs)} {'PRs' if len(candidate_prs) != 1 else 'PR'}"
            f"{f' (capped at {max_num})' if max_num else ''}...[/]\n"
        )
    return candidate_prs, accepted_prs, total_skipped_collaborator, total_skipped_bot, total_skipped_accepted


def _enrich_candidate_details(
    token: str,
    github_repository: str,
    candidate_prs: list[PRData],
    *,
    run_api: bool,
    quiet: bool = False,
    on_check_progress: Callable[[int, int], None] | None = None,
    on_merge_progress: Callable[[int, int], None] | None = None,
    on_review_progress: Callable[[int, int], None] | None = None,
) -> None:
    """Fetch check details, resolve unknown mergeable status, and fetch review comments."""
    if not candidate_prs:
        return

    if not quiet:
        console_print(
            f"[info]Fetching check details for {len(candidate_prs)} "
            f"{'PRs' if len(candidate_prs) != 1 else 'PR'}...[/]"
        )
    _fetch_check_details_batch(token, github_repository, candidate_prs, on_progress=on_check_progress)

    for pr in candidate_prs:
        if pr.checks_state == "FAILURE" and not pr.failed_checks and pr.head_sha:
            if not quiet:
                console_print(
                    f"  [dim]Fetching full check details for PR {_pr_link(pr)} "
                    f"(failures beyond first 100 checks)...[/]"
                )
            pr.failed_checks = _fetch_failed_checks(token, github_repository, pr.head_sha)

    unknown_count = sum(1 for pr in candidate_prs if pr.mergeable == "UNKNOWN")
    if unknown_count:
        if not quiet:
            console_print(
                f"[info]Resolving merge conflict status for {unknown_count} "
                f"{'PRs' if unknown_count != 1 else 'PR'} with unknown status...[/]"
            )
        resolved = _resolve_unknown_mergeable(
            token, github_repository, candidate_prs, on_progress=on_merge_progress
        )
        remaining = unknown_count - resolved
        if not quiet:
            if remaining:
                console_print(
                    f"  [dim]{resolved} resolved, {remaining} still unknown "
                    f"(GitHub hasn't computed mergeability yet).[/]"
                )
            else:
                console_print(f"  [dim]All {resolved} resolved.[/]")

    if run_api:
        if not quiet:
            console_print(
                f"[info]Fetching review thread details for {len(candidate_prs)} "
                f"{'PRs' if len(candidate_prs) != 1 else 'PR'}...[/]"
            )
        _fetch_unresolved_comments_batch(
            token, github_repository, candidate_prs, on_progress=on_review_progress
        )


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

    # Collect PRs with pending runs for batched review
    batch_approvable: list[tuple[PRData, list[dict]]] = []

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
                f"  [bold red]Author {pr.author_login} has {author_count} "
                f"{'PRs' if author_count != 1 else 'PR'} with issues "
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
                label_overrides={TriageAction.RERUN: "review and approve workflows"},
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

        # Normal workflow approval — collect for batched review
        batch_approvable.append((pr, pending_runs))

    # --- Batched workflow approval flow ---
    # Group PRs that have pending runs, show titles first, then diffs one-by-one,
    # then batch approve all non-flagged PRs at once.
    if not batch_approvable or ctx.stats.quit_early:
        return

    console_print()
    get_console().rule("[bold bright_cyan]Workflow approval — batch review[/]", style="bright_cyan")
    console_print(
        f"\n[info]{len(batch_approvable)} "
        f"{'PRs' if len(batch_approvable) != 1 else 'PR'} "
        f"with pending workflow runs to review:[/]\n"
    )
    for pr, runs in batch_approvable:
        draft_tag = " [yellow](draft)[/]" if pr.is_draft else ""
        console_print(
            f"  {_pr_link(pr)} {pr.title}{draft_tag}  "
            f"[dim]by {pr.author_login} — {len(runs)} pending "
            f"{'runs' if len(runs) != 1 else 'run'}[/]"
        )

    if ctx.dry_run:
        console_print("\n[warning]Dry run — skipping batch workflow approval.[/]")
        return

    console_print(
        "\n[info]Showing diffs one-by-one. Press Enter to continue, "
        "\\[f] to flag as suspicious, \\[q] to quit.[/]\n"
    )

    # Track which PRs to approve vs flagged as suspicious
    prs_to_approve: list[tuple[PRData, list[dict]]] = []
    flagged_suspicious: list[PRData] = []

    for pr, runs in batch_approvable:
        if ctx.stats.quit_early:
            break

        get_console().rule(f"[cyan]PR {_pr_link(pr)}[/]", style="dim")
        console_print(f"  [bold]{pr.title}[/] by {pr.author_login}\n")

        action = _show_diff_and_confirm(
            ctx.token,
            ctx.github_repository,
            pr,
            forced_answer=ctx.answer_triage,
            confirm_message="Press Enter to approve, \\[f] to flag as suspicious, \\[q] to quit",
        )
        if action == ContinueAction.QUIT:
            console_print("[warning]Quitting.[/]")
            ctx.stats.quit_early = True
            break
        if action == ContinueAction.FLAG:
            console_print(f"  [bold red]Flagged PR {_pr_link(pr)} by {pr.author_login} as suspicious.[/]")
            flagged_suspicious.append(pr)
        else:
            prs_to_approve.append((pr, runs))

    if ctx.stats.quit_early:
        return

    # Handle flagged suspicious PRs
    for pr in flagged_suspicious:
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

    if ctx.stats.quit_early:
        return

    # Batch approve all non-flagged PRs
    if not prs_to_approve:
        console_print("\n[info]No PRs to approve (all were flagged or skipped).[/]")
        return

    console_print()
    get_console().rule("[bold green]Batch approval[/]", style="green")
    console_print(
        f"\n[info]Approving workflows for {len(prs_to_approve)} "
        f"{'PRs' if len(prs_to_approve) != 1 else 'PR'}:[/]"
    )
    for pr, runs in prs_to_approve:
        console_print(
            f"  {_pr_link(pr)} {pr.title} [dim]({len(runs)} {'runs' if len(runs) != 1 else 'run'})[/]"
        )

    answer = user_confirm(
        f"Approve workflow runs for all {len(prs_to_approve)} {'PRs' if len(prs_to_approve) != 1 else 'PR'}?",
        default_answer=Answer.YES,
        forced_answer=ctx.answer_triage,
    )
    if answer == Answer.QUIT:
        console_print("[warning]Quitting.[/]")
        ctx.stats.quit_early = True
        return
    if answer == Answer.NO:
        console_print("[info]Skipping batch approval — no workflows approved.[/]")
        return

    for pr, runs in prs_to_approve:
        approved = _approve_workflow_runs(ctx.token, ctx.github_repository, runs)
        if approved:
            console_print(
                f"  [success]Approved {approved}/{len(runs)} workflow "
                f"{'runs' if len(runs) != 1 else 'run'} for PR "
                f"{_pr_link(pr)}.[/]"
            )
            ctx.stats.total_workflows_approved += 1
        else:
            console_print(f"  [error]Failed to approve workflow runs for PR {_pr_link(pr)}.[/]")
            # Approval failed (likely 403 — runs are too old). Suggest converting to draft
            # with a rebase comment so the author pushes fresh commits.
            if pr.is_draft:
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
        f"\n[info]Reviewing {len(det_flagged_prs)} {'PRs' if len(det_flagged_prs) != 1 else 'PR'} "
        f"with issues found"
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
                f"[bold]Author: {current_author}[/] ({count} PR{'s' if count != 1 else ''} with issues)",
                style="cyan",
            )

        _prompt_and_execute_flagged_pr(ctx, pr, assessment, classification="Non-LLM Issues")


def _review_llm_flagged_prs(ctx: TriageContext, llm_candidates: list[PRData]) -> None:
    """Present LLM-flagged PRs as they become ready, without blocking. Mutates ctx.stats."""
    if ctx.stats.quit_early or not ctx.llm_future_to_pr:
        return

    _collect_llm_results(
        ctx.llm_future_to_pr,
        ctx.llm_assessments,
        ctx.llm_completed,
        ctx.llm_errors,
        ctx.llm_passing,
        llm_durations=ctx.llm_durations,
        llm_attempts=ctx.llm_attempts,
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
                f"\n[info]{len(new_flagged)} new "
                f"{'PRs' if len(new_flagged) != 1 else 'PR'} with LLM issues found, ready for review "
                f"({', '.join(status_parts)}):[/]\n"
            )

            for pr, assessment in new_flagged:
                if ctx.stats.quit_early:
                    break
                llm_presented.add(pr.number)
                ctx.author_flagged_count[pr.author_login] = (
                    ctx.author_flagged_count.get(pr.author_login, 0) + 1
                )

                _llm_cls = "LLM Errors" if getattr(assessment, "should_report", False) else "LLM Warnings"
                _prompt_and_execute_flagged_pr(
                    ctx,
                    pr,
                    assessment,
                    classification=_llm_cls,
                    llm_status="flagged",
                    llm_duration=ctx.llm_durations.get(pr.number, 0.0),
                )

                # While user was deciding, more results may have arrived
                _collect_llm_results(
                    ctx.llm_future_to_pr,
                    ctx.llm_assessments,
                    ctx.llm_completed,
                    ctx.llm_errors,
                    ctx.llm_passing,
                    llm_durations=ctx.llm_durations,
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
            ctx.llm_future_to_pr,
            ctx.llm_assessments,
            ctx.llm_completed,
            ctx.llm_errors,
            ctx.llm_passing,
            llm_durations=ctx.llm_durations,
        )

    console_print(
        f"\n[info]LLM assessment complete: {len(ctx.llm_assessments)} issues found, "
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
        _llm_st = "passed" if pr.number in {p.number for p in ctx.llm_passing} else ""
        _display_pr_info_panels(pr, author_profile, classification="All passed", llm_status=_llm_st)
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
        _display_pr_info_panels(pr, author_profile, classification="Stale Review")

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


def _review_already_triaged_prs(
    ctx: TriageContext,
    already_triaged: list[PRData],
    *,
    run_api: bool,
) -> None:
    """Present already-triaged PRs for optional re-evaluation in sequential mode."""
    if ctx.stats.quit_early or not already_triaged:
        return

    already_triaged.sort(key=lambda p: (p.author_login.lower(), p.number))
    console_print(
        f"\n[info]{len(already_triaged)} already-triaged "
        f"{'PRs' if len(already_triaged) != 1 else 'PR'} "
        f"— press Enter to re-evaluate or s to skip:[/]\n"
    )
    for pr in already_triaged:
        if ctx.stats.quit_early:
            break
        _print_pr_header(pr)
        console_print("[dim]Previously triaged — re-evaluate to check for changes.[/]")
        action = prompt_triage_action(
            f"Re-evaluate PR {_pr_link(pr)}?",
            default=TriageAction.SKIP,
            forced_answer=ctx.answer_triage,
            pr_url=pr.url,
            token=ctx.token,
            github_repository=ctx.github_repository,
            pr_number=pr.number,
        )
        if action == TriageAction.QUIT:
            ctx.stats.quit_early = True
            break
        if action == TriageAction.SKIP:
            ctx.stats.total_skipped_action += 1
            continue

        # Re-evaluate
        console_print(f"[info]Re-evaluating PR #{pr.number}...[/]")
        det = _reevaluate_triaged_pr(
            pr,
            token=ctx.token,
            github_repository=ctx.github_repository,
            run_api=run_api,
        )
        if det.category == "flagged" and det.assessment:
            _prompt_and_execute_flagged_pr(ctx, pr, det.assessment, classification="Non-LLM Issues")
        elif det.category == "llm_candidate":
            console_print("[success]Re-evaluation: this PR passes all deterministic checks.[/]")
            pass_action = prompt_triage_action(
                f"Action for PR {_pr_link(pr)}?",
                default=TriageAction.READY,
                forced_answer=ctx.answer_triage,
                exclude={TriageAction.DRAFT} if pr.is_draft else None,
                pr_url=pr.url,
                token=ctx.token,
                github_repository=ctx.github_repository,
                pr_number=pr.number,
            )
            if pass_action == TriageAction.QUIT:
                ctx.stats.quit_early = True
            elif pass_action != TriageAction.SKIP:
                _execute_triage_action(ctx, pr, pass_action, draft_comment="", close_comment="")
            else:
                ctx.stats.total_skipped_action += 1
        elif det.category == "pending_approval":
            console_print(f"[info]PR #{pr.number} needs workflow approval.[/]")
        elif det.category == "in_progress":
            console_print(f"[info]PR #{pr.number} has workflows in progress.[/]")
        else:
            console_print(f"[dim]PR #{pr.number} — {det.category.replace('_', ' ')}.[/]")


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
                _display_pr_info_panels(pr, author_profile, open_in_browser=True, classification="All passed")
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
                _display_pr_info_panels(pr, author_profile, open_in_browser=True, classification="LLM Review")

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
        f"with issues found ({total_deterministic_flags} CI/conflicts/comments, "
        f"{total_llm_flagged} LLM"
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
    total_fetched = max(len(all_prs), stats.cumulative_all_prs)
    summary_table.add_row("PRs fetched", str(total_fetched))
    if verbose:
        summary_table.add_row("Collaborators skipped", str(total_skipped_collaborator))
        summary_table.add_row("Bots skipped", str(total_skipped_bot))
        summary_table.add_row("Ready-for-review skipped", str(total_skipped_accepted))
    summary_table.add_row("PRs skipped (filtered)", str(total_skipped))
    total_triaged = max(len(already_triaged), stats.cumulative_already_triaged)
    summary_table.add_row("Already triaged (skipped)", str(total_triaged))
    if total_triaged:
        summary_table.add_row("  Commented (no response)", str(triaged_waiting_count))
        summary_table.add_row("  Triaged (author responded)", str(triaged_responded_count))
    total_assessed = max(len(candidate_prs), stats.cumulative_candidates)
    summary_table.add_row("PRs assessed", str(total_assessed))
    summary_table.add_row("Issues found by CI/conflicts/comments", str(total_deterministic_flags))
    summary_table.add_row("Issues found by LLM", str(total_llm_flagged))
    if total_llm_report:
        summary_table.add_row("[red]Potentially flagged for report[/red]", f"[red]{total_llm_report}[/red]")
    summary_table.add_row("LLM errors (skipped)", str(total_llm_errors))
    summary_table.add_row("Total issues found", str(total_flagged))
    total_passing = max(len(passing_prs), stats.cumulative_passing)
    summary_table.add_row("PRs passing all checks", str(total_passing))
    total_drafts_skipped = max(len(skipped_drafts), stats.cumulative_skipped_drafts)
    summary_table.add_row("Drafts with issues (skipped)", str(total_drafts_skipped))
    total_grace = max(len(recent_failures_skipped), stats.cumulative_recent_failures_skipped)
    summary_table.add_row(
        "Recent CI failures within grace period (skipped)",
        str(total_grace),
    )
    summary_table.add_row("PRs converted to draft", str(stats.total_converted))
    summary_table.add_row("PRs commented (not drafted)", str(stats.total_commented))
    summary_table.add_row("PRs closed", str(stats.total_closed))
    summary_table.add_row("PRs with checks rerun", str(stats.total_rerun))
    summary_table.add_row("PRs rebased", str(stats.total_rebased))
    summary_table.add_row("Review follow-up nudges", str(stats.total_review_nudges))
    summary_table.add_row("PRs marked ready for review", str(stats.total_ready))
    summary_table.add_row("PRs skipped (no action)", str(stats.total_skipped_action))
    total_pending = max(len(pending_approval), stats.cumulative_pending_approval)
    summary_table.add_row("Awaiting workflow approval", str(total_pending))
    total_wip = max(len(workflows_in_progress), stats.cumulative_workflows_in_progress)
    summary_table.add_row("Workflows in progress (skipped)", str(total_wip))
    summary_table.add_row("PRs with workflows approved", str(stats.total_workflows_approved))
    console_print(summary_table)


def _fetch_pr_diff(token: str, github_repository: str, pr_number: int) -> str | None:
    """Fetch the diff for a PR via GitHub REST API. Returns the diff text or None on failure."""
    import requests

    url = f"https://api.github.com/repos/{github_repository}/pulls/{pr_number}"
    try:
        response = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github.v3.diff"},
            timeout=(10, 30),
        )
    except (requests.ConnectionError, requests.Timeout, OSError):
        return None
    if response.status_code != 200:
        return None
    return response.text


def _show_diff_and_confirm(
    token: str,
    github_repository: str,
    pr: PRData,
    *,
    forced_answer: str | None = None,
    confirm_message: str | None = None,
) -> ContinueAction:
    """Fetch and display a PR diff, warn about sensitive files, and prompt for confirmation.

    Returns ContinueAction.CONTINUE to approve, FLAG to flag as suspicious, QUIT to quit.
    """
    console_print(f"  Fetching diff for PR {_pr_link(pr)}...")
    diff_text = _fetch_pr_diff(token, github_repository, pr.number)
    if diff_text:
        from rich.syntax import Syntax

        console_print(
            Panel(
                Syntax(diff_text, "diff", theme="monokai", word_wrap=True),
                title=f"Diff for PR {_pr_link(pr)}",
                border_style="bright_cyan",
            )
        )
        sensitive_files = _detect_sensitive_file_changes(diff_text)
        if sensitive_files:
            console_print()
            console_print(
                "[bold red]WARNING: This PR contains changes to sensitive files — please review carefully![/]"
            )
            for f in sensitive_files:
                console_print(f"  [bold red]  - {f}[/]")
            console_print()
    else:
        console_print(
            f"  [warning]Could not fetch diff for PR {_pr_link(pr)}. Review manually at: {pr.url}/files[/]"
        )
    msg = confirm_message or "Press Enter to continue, \\[f] to flag as suspicious, \\[q] to quit"
    return prompt_space_continue(message=msg, forced_answer=forced_answer)


def _detect_sensitive_file_changes(diff_text: str) -> list[str]:
    """Parse a unified diff and return paths under .github/ or scripts/ that were changed."""

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
    try:
        response = requests.get(
            url,
            params={"head_sha": head_sha, "status": status, "per_page": "50"},
            headers={"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"},
            timeout=(10, 20),
        )
    except (requests.ConnectionError, requests.Timeout, OSError):
        return []
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


def _post_workflow_run_action(token: str, github_repository: str, run: dict, action: str) -> bool:
    """POST to a workflow run endpoint (approve/cancel/rerun). Returns True on success."""
    run_id = run["id"]
    url = f"https://api.github.com/repos/{github_repository}/actions/runs/{run_id}/{action}"
    return _github_rest(token, "post", url, ok_codes=(201, 202, 204))


def _approve_workflow_runs(token: str, github_repository: str, pending_runs: list[dict]) -> int:
    """Approve pending workflow runs. Returns number successfully approved."""
    approved = 0
    for run in pending_runs:
        if _post_workflow_run_action(token, github_repository, run, "approve"):
            approved += 1
        else:
            console_print(
                f"  [warning]Failed to approve run {run.get('name', run['id'])}: check permissions[/]"
            )
    return approved


def _cancel_workflow_run(token: str, github_repository: str, run: dict) -> bool:
    """Cancel a single workflow run. Returns True if successful."""
    return _post_workflow_run_action(token, github_repository, run, "cancel")


def _rerun_workflow_run(token: str, github_repository: str, run: dict) -> bool:
    """Rerun a complete workflow run (all jobs). Returns True if successful."""
    return _post_workflow_run_action(token, github_repository, run, "rerun")


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
            timeout=(10, 20),
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
) -> dict[str, LogSnippetInfo]:
    """Fetch short log snippets from failed GitHub Actions jobs for a commit.

    Returns a dict mapping failed check name -> LogSnippetInfo (snippet + job URL).
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

    snippets: dict[str, LogSnippetInfo] = {}
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
                timeout=(10, 30),
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
                snippets[check_name] = LogSnippetInfo(snippet=snippet, job_url=job.get("html_url", ""))

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

    # Normalize job name for matching: GitHub replaces "/" with " _ " in zip filenames
    job_name_normalized = job_name.replace("/", "_").lower()

    for step in failed_steps:
        step_number = step.get("number", 0)
        step_name = step.get("name", "")

        # Try to find matching log file by job name (normalized) and step number
        candidate_files = []
        for zname in zip_names:
            zname_lower = zname.lower()
            if f"{step_number}_" in zname and (
                zname.startswith(f"{job_name}/") or job_name_normalized in zname_lower
            ):
                candidate_files.append(zname)

        if not candidate_files:
            # Try matching by job name without step number (zip numbering may differ)
            for zname in zip_names:
                if job_name_normalized in zname.lower():
                    candidate_files.append(zname)

        if not candidate_files:
            # Fallback: try any file containing the step name
            for zname in zip_names:
                if step_name and step_name.lower() in zname.lower():
                    candidate_files.append(zname)

        # For static check logs, read the full file — errors appear in the middle,
        # not at the tail (the tail is usually TRACE output from subsequent hooks).
        _is_static = step_name and any(
            kw in step_name.lower() for kw in ("static check", "pre-commit", "prek")
        )
        for log_file in candidate_files:
            log_tail = _read_log_tail(zf, log_file, full=_is_static)
            if log_tail is None:
                continue

            snippet = _extract_error_lines(log_tail, step_name)
            if snippet:
                return snippet

    # Fallback: try to find any log file for this job and extract errors
    for zname in zip_names:
        if job_name_normalized in zname.lower():
            log_tail = _read_log_tail(zf, zname)
            if log_tail is None:
                continue
            snippet = _extract_error_lines(log_tail, "")
            if snippet:
                return snippet

    return ""


_LOG_TAIL_BYTES = 128 * 1024  # 128 KB — enough for ~1000 log lines
_LOG_TAIL_LINES = 1000


def _read_log_tail(zf, log_file: str, *, full: bool = False) -> str | None:
    """Read the tail (or full content) of a log file from a zip archive.

    When ``full=True``, reads the entire file (for static check logs where errors
    appear in the middle, not at the tail).  Otherwise reads only the last ~1000 lines.
    Returns the text as a string, or None if the file cannot be read.
    """
    try:
        raw = zf.read(log_file)
    except (KeyError, OSError):
        return None
    if not full:
        # Only decode the tail to save memory on large log files
        if len(raw) > _LOG_TAIL_BYTES:
            raw = raw[-_LOG_TAIL_BYTES:]
            # Drop the first (likely partial) line
            nl = raw.find(b"\n")
            if nl != -1:
                raw = raw[nl + 1 :]
    text = raw.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if not full and len(lines) > _LOG_TAIL_LINES:
        lines = lines[-_LOG_TAIL_LINES:]
    return "\n".join(lines)


def _extract_error_lines(log_content: str, step_name: str) -> str:
    """Extract relevant error lines from a log file.

    Looks for error markers, then takes surrounding context.
    Falls back to the last N lines if no error markers are found.
    The input is expected to already be truncated to the last ~1000 lines.
    """
    lines = log_content.splitlines()
    if not lines:
        return ""

    # For static check / prek jobs, use specialised extraction that filters out
    # verbose TRACE output and focuses on hook failure summaries.
    # When the specialised extractor finds nothing, return empty so the caller
    # can try the next candidate log file instead of falling through to generic
    # extraction (which picks up irrelevant markers like ❌ from setup steps).
    _is_static_checks = step_name and any(
        kw in step_name.lower() for kw in ("static check", "pre-commit", "prek")
    )
    if _is_static_checks:
        snippet_lines = _extract_static_check_errors(lines)
        return _format_snippet(snippet_lines, step_name) if snippet_lines else ""

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
        # Take a window around the last error cluster (most relevant failure info)
        last_error = error_indices[-1]
        first_error = error_indices[0]

        # Walk backwards to find the start of the last cluster
        for i in range(len(error_indices) - 1, 0, -1):
            if error_indices[i] - error_indices[i - 1] > 20:
                first_error = error_indices[i]
                break

        start = max(0, first_error - 3)
        end = min(len(lines), last_error + _LOG_SNIPPET_MAX_LINES - (last_error - first_error))
        snippet_lines = lines[start:end]
    else:
        # No error markers found — take the last N lines
        snippet_lines = lines[-_LOG_SNIPPET_MAX_LINES:]

    return _format_snippet(snippet_lines, step_name)


def _format_snippet(snippet_lines: list[str], step_name: str) -> str:
    """Clean up and format a snippet for display."""
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


def _extract_static_check_errors(lines: list[str]) -> list[str]:
    """Extract meaningful error output from static check / prek logs.

    Static check logs are very verbose with TRACE-level prek output (resolved commands,
    shebangs, interpreter paths, etc.).  This function collects output per hook, then
    retroactively includes output from hooks that failed.

    The key insight is that hooks print their output BEFORE the result line
    (``hook_name...Failed``), so we buffer each hook's output and flush it when
    we see the result.
    """
    import re

    # Patterns to skip — verbose prek / pre-commit noise
    _NOISE_PATTERNS = (
        " TRACE ",
        " DEBUG ",
        " INFO ",
        "Resolved command:",
        "Found shebang:",
        "Resolved interpreter:",
        "Running ",
        "Executing `cd ",
        "hook returned exit code",
        "(no files to check)",
        "##[group]",
        "##[endgroup]",
        "##[error]",
        "- hook id:",
        "- duration:",
        "- exit code:",
        "- files were modified by this hook",
        "shell: /",
        "env:",
        "PYTHON_MAJOR_MINOR",
        "UPGRADE_TO_NEWER",
        "GITHUB_TOKEN:",
        "pythonLocation:",
        "PKG_CONFIG_PATH:",
        "Python_ROOT_DIR:",
        "Python2_ROOT_DIR:",
        "Python3_ROOT_DIR:",
        "LD_LIBRARY_PATH:",
        "tar-restored=",
        "Cache tarball",
        "Restored files",
        "du ~/",
    )

    # ANSI escape code pattern (e.g. \x1b[41m)
    _ansi_re = re.compile(r"\x1b\[[0-9;]*m")

    # Pattern for hook result lines: "hook_name...Passed" or "hook_name...Failed"
    # The status may be wrapped in ANSI color codes in CI logs.
    _hook_result_re = re.compile(r"^(.+?)\.\.\.*\s*(Passed|Failed)\s*$")

    # Metadata lines that appear after a Failed hook — include as context
    _POST_FAIL_PATTERNS = ("- files were modified by this hook", "- exit code:")

    result: list[str] = []
    failed_hooks: list[str] = []
    # Buffer non-noise output between hook result lines
    current_hook_output: list[str] = []
    # Whether we just saw a Failed line and should capture post-fail metadata
    _capture_post_fail = False

    for line in lines:
        # Strip timestamp if present
        stripped = line
        if len(line) > 28 and line[4] == "-" and line[7] == "-" and line[10] == "T":
            stripped = line[28:].lstrip()

        # Strip ANSI escape codes for pattern matching
        clean = _ansi_re.sub("", stripped)

        # Check for hook result summary lines
        hook_match = _hook_result_re.search(clean)
        if hook_match:
            hook_name = hook_match.group(1).strip().rstrip(".")
            status = hook_match.group(2)
            if status == "Failed":
                # Include the buffered output from this hook — that's the actual error
                result.append(f"--- {hook_name}: FAILED ---")
                result.extend(current_hook_output)
                failed_hooks.append(hook_name)
                _capture_post_fail = True
            else:
                _capture_post_fail = False
            # Reset buffer for next hook
            current_hook_output = []
            continue

        # Capture post-failure metadata lines (e.g. "- files were modified by this hook")
        if _capture_post_fail:
            if any(pat in clean for pat in _POST_FAIL_PATTERNS):
                result.append(clean.strip().lstrip("- "))
                continue
            # Stop capturing after non-metadata line
            if clean.strip() and "- hook id:" not in clean and "- duration:" not in clean:
                _capture_post_fail = False

        # Skip "Skipped" lines
        if clean.endswith("Skipped"):
            current_hook_output = []
            continue

        # Skip noise lines — don't buffer them
        if any(noise in stripped for noise in _NOISE_PATTERNS):
            continue

        # Skip empty lines in the buffer
        if not stripped.strip():
            continue

        # Buffer this line — it's potential hook output
        current_hook_output.append(stripped)

    # If we found failed hooks but no output, add a summary
    if failed_hooks and len(result) <= len(failed_hooks):
        result.insert(0, f"Failed hooks: {', '.join(failed_hooks)}")

    # Cap the result
    if len(result) > _LOG_SNIPPET_MAX_LINES * 2:
        result = result[: _LOG_SNIPPET_MAX_LINES * 2]
        result.append("... (truncated)")

    return result


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
            timeout=(10, 20),
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
                timeout=(10, 20),
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
            timeout=(10, 20),
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
                    timeout=(10, 20),
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


def _clear_triage_caches(github_repository: str) -> None:
    """Clear all triage-related caches."""
    import shutil

    console = get_console()
    for label, get_dir in [
        ("review", _get_review_cache_dir),
        ("triage", _get_triage_cache_dir),
        ("status", _get_status_cache_dir),
        ("classification", _get_classification_cache_dir),
    ]:
        cache_dir = get_dir(github_repository)
        if cache_dir.exists():
            count = len(list(cache_dir.glob("*.json")))
            shutil.rmtree(cache_dir)
            console.print(f"[info]Cleared LLM {label} cache ({count} entries) at {cache_dir}.[/]")
        else:
            console.print(f"[info]LLM {label} cache is already empty.[/]")


def _validate_and_refresh_caches(
    token: str,
    github_repository: str,
    *,
    quiet: bool = False,
) -> tuple[int, int, int]:
    """Check cached PRs against GitHub and invalidate entries whose head_sha changed.

    Returns (total_cached, stale_count, removed_count).
    """
    from airflow_breeze.utils.pr_cache import invalidate_stale_caches, scan_cached_pr_numbers

    cached_prs = scan_cached_pr_numbers(github_repository)
    if not cached_prs:
        return 0, 0, 0

    # Batch-fetch current head SHAs using GraphQL (up to 100 per query via aliases)
    owner, name = github_repository.split("/", 1)
    current_shas: dict[int, str | None] = {}
    pr_numbers = sorted(cached_prs.keys())

    for i in range(0, len(pr_numbers), 100):
        batch = pr_numbers[i : i + 100]
        aliases = "\n".join(f"pr_{n}: pullRequest(number: {n}) {{ headRefOid }}" for n in batch)
        query = f"""
        query {{
            repository(owner: "{owner}", name: "{name}") {{
                {aliases}
            }}
        }}
        """
        data = _graphql_request(token, query, {})
        repo_data = data.get("repository", {})
        for n in batch:
            pr_data = repo_data.get(f"pr_{n}")
            current_shas[n] = pr_data["headRefOid"] if pr_data else None

    # Find stale PRs: cached head_sha differs from current, or PR no longer exists
    stale: dict[int, str] = {}
    for pr_num, cache_entries in cached_prs.items():
        current_sha = current_shas.get(pr_num)
        if current_sha is None:
            # PR was deleted/closed and not found — mark as stale with empty sha to force mismatch
            stale[pr_num] = ""
            continue
        for _cache_name, cached_sha in cache_entries.items():
            if cached_sha != current_sha:
                stale[pr_num] = current_sha
                break

    removed = 0
    if stale:
        removed = invalidate_stale_caches(github_repository, stale)

    if not quiet:
        console = get_console()
        if stale:
            stale_nums = ", ".join(f"#{n}" for n in sorted(stale.keys()))
            console.print(
                f"[info]Cache: {len(cached_prs)} cached PRs, "
                f"{len(stale)} stale ({removed} entries removed): {stale_nums}[/]"
            )
        else:
            console.print(f"[info]Cache: {len(cached_prs)} cached PRs, all up to date[/]")

    return len(cached_prs), len(stale), removed


def _split_label_filters(
    labels: tuple[str, ...], exclude_labels: tuple[str, ...]
) -> tuple[tuple[str, ...], list[str], tuple[str, ...], list[str]]:
    """Split labels into (exact_labels, wildcard_labels, exact_exclude, wildcard_exclude)."""
    exact_labels = tuple(lbl for lbl in labels if "*" not in lbl and "?" not in lbl)
    wildcard_labels = [lbl for lbl in labels if "*" in lbl or "?" in lbl]
    exact_exclude_labels = tuple(lbl for lbl in exclude_labels if "*" not in lbl and "?" not in lbl)
    wildcard_exclude_labels = [lbl for lbl in exclude_labels if "*" in lbl or "?" in lbl]
    return exact_labels, wildcard_labels, exact_exclude_labels, wildcard_exclude_labels


def _apply_wildcard_label_filters(
    prs: list[PRData], wildcard_labels: list[str], wildcard_exclude_labels: list[str]
) -> list[PRData]:
    """Filter PRs by wildcard include/exclude label patterns (client-side fnmatch)."""
    from fnmatch import fnmatch

    if wildcard_labels:
        prs = [pr for pr in prs if any(fnmatch(lbl, pat) for pat in wildcard_labels for lbl in pr.labels)]
    if wildcard_exclude_labels:
        prs = [
            pr
            for pr in prs
            if not any(fnmatch(lbl, pat) for pat in wildcard_exclude_labels for lbl in pr.labels)
        ]
    return prs


def _merge_pr_assessments(
    *assessments: PRAssessment | None,
) -> PRAssessment | None:
    """Merge multiple optional PRAssessments into one combined result. Returns None if all are None."""
    violations: list = []
    summaries: list[str] = []
    for a in assessments:
        if a:
            violations.extend(a.violations)
            summaries.append(a.summary)
    if not violations and not summaries:
        return None
    from airflow_breeze.utils.github import PRAssessment as _PRAssessment

    return _PRAssessment(
        should_flag=True,
        violations=violations,
        summary=" ".join(summaries),
    )


@dataclass
class DeterministicResult:
    """Result of deterministic PR assessment."""

    category: (
        str  # "flagged", "draft_skipped", "grace_period", "in_progress", "pending_approval", "llm_candidate"
    )
    assessment: PRAssessment | None = None


def _assess_pr_deterministic(
    pr: PRData,
    *,
    token: str,
    github_repository: str,
) -> DeterministicResult:
    """Run deterministic checks on a single PR and return its categorization."""
    from airflow_breeze.utils.github import (
        assess_pr_checks,
        assess_pr_conflicts,
        assess_pr_unresolved_comments,
    )

    ci_assessment = assess_pr_checks(pr.number, pr.checks_state, pr.failed_checks)

    # Race condition: checks_state is FAILURE but workflows are currently running.
    if ci_assessment and pr.head_sha and _has_in_progress_workflows(token, github_repository, pr.head_sha):
        return DeterministicResult(category="in_progress")

    # Grace period: don't flag CI failures within the grace window.
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
            return DeterministicResult(category="grace_period")

    conflict_assessment = assess_pr_conflicts(pr.number, pr.mergeable, pr.base_ref, pr.commits_behind)
    comments_assessment = assess_pr_unresolved_comments(
        pr.number, pr.unresolved_review_comments, pr.unresolved_threads
    )

    if ci_assessment or conflict_assessment or comments_assessment:
        if pr.is_draft:
            return DeterministicResult(category="draft_skipped")
        merged = _merge_pr_assessments(conflict_assessment, ci_assessment, comments_assessment)
        return DeterministicResult(category="flagged", assessment=merged)

    # No deterministic issues found — needs further categorization
    if pr.checks_state == "NOT_RUN":
        if pr.head_sha and _has_in_progress_workflows(token, github_repository, pr.head_sha):
            return DeterministicResult(category="in_progress")
        return DeterministicResult(category="pending_approval")
    if pr.is_draft and pr.head_sha and _has_in_progress_workflows(token, github_repository, pr.head_sha):
        return DeterministicResult(category="in_progress")
    return DeterministicResult(category="llm_candidate")


@dataclass
class CategorizationResult:
    """Result of categorizing all candidate PRs."""

    assessments: dict[int, PRAssessment] = field(default_factory=dict)
    llm_candidates: list[PRData] = field(default_factory=list)
    pending_approval: list[PRData] = field(default_factory=list)
    workflows_in_progress: list[PRData] = field(default_factory=list)
    skipped_drafts: list[PRData] = field(default_factory=list)
    recent_failures_skipped: list[PRData] = field(default_factory=list)
    deterministic_timings: dict[int, float] = field(default_factory=dict)
    total_flagged: int = 0


def _categorize_all_candidates(
    candidate_prs: list[PRData],
    *,
    token: str,
    github_repository: str,
    run_api: bool,
    progress_cb: Callable | None = None,
) -> CategorizationResult:
    """Iterate over candidates and sort them into deterministic buckets."""
    result = CategorizationResult()

    if run_api:
        for pr_idx, pr in enumerate(candidate_prs):
            if progress_cb:
                progress_cb(pr_idx, len(candidate_prs))
            t_det_start = time.monotonic()
            det = _assess_pr_deterministic(pr, token=token, github_repository=github_repository)
            result.deterministic_timings[pr.number] = time.monotonic() - t_det_start
            if det.category == "flagged" and det.assessment:
                result.total_flagged += 1
                result.assessments[pr.number] = det.assessment
            elif det.category == "draft_skipped":
                result.skipped_drafts.append(pr)
            elif det.category == "grace_period":
                result.recent_failures_skipped.append(pr)
            elif det.category == "in_progress":
                result.workflows_in_progress.append(pr)
            elif det.category == "pending_approval":
                result.pending_approval.append(pr)
            elif det.category == "llm_candidate":
                result.llm_candidates.append(pr)
    else:
        for pr in candidate_prs:
            if pr.checks_state == "NOT_RUN":
                if pr.head_sha and _has_in_progress_workflows(token, github_repository, pr.head_sha):
                    result.workflows_in_progress.append(pr)
                else:
                    result.pending_approval.append(pr)
            elif (
                pr.is_draft
                and pr.head_sha
                and _has_in_progress_workflows(token, github_repository, pr.head_sha)
            ):
                result.workflows_in_progress.append(pr)
            else:
                result.llm_candidates.append(pr)

    return result


def _build_triage_timing_table(
    *,
    phase1_total: float,
    enrich_total: float,
    num_all: int,
    num_candidates: int,
    deterministic_timings: dict[int, float],
    llm_count: int,
    llm_wall_time: float,
    interactive_time: float,
    total_elapsed: float,
    prs_with_action: int,
) -> Table:
    """Build the timing summary table for triage."""
    timing_table = Table(title="Timing Summary")
    timing_table.add_column("Phase", style="bold")
    timing_table.add_column("Total", justify="right")
    timing_table.add_column("PRs", justify="right")
    timing_table.add_column("Avg/PR", justify="right")
    timing_table.add_column("Min/PR", justify="right")
    timing_table.add_column("Max/PR", justify="right")

    safe_num_all = num_all or 1
    safe_num_candidates = num_candidates or 1

    timing_table.add_row(
        "Fetch PRs + commits behind",
        _fmt_duration(phase1_total),
        str(num_all),
        _fmt_duration(phase1_total / safe_num_all),
        "[dim]—[/]",
        "[dim]—[/]",
    )

    timing_table.add_row(
        "Enrich PRs (checks + mergeability + comments)",
        _fmt_duration(enrich_total),
        str(num_candidates),
        _fmt_duration(enrich_total / safe_num_candidates),
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
        _fmt_duration(interactive_time),
        "",
        "",
        "",
        "",
    )
    timing_table.add_row("", "", "", "", "", "")
    avg_per_actioned = _fmt_duration(total_elapsed / prs_with_action) if prs_with_action else "[dim]—[/]"
    timing_table.add_row(
        "[bold]Total[/]",
        f"[bold]{_fmt_duration(total_elapsed)}[/]",
        str(prs_with_action) if prs_with_action else "",
        f"[bold]{avg_per_actioned}[/]" if prs_with_action else "",
        "",
        "",
    )
    return timing_table


def _build_pr_timing_table(
    *,
    deterministic_timings: dict[int, float],
    enrich_total: float,
    num_candidates: int,
    pr_titles: dict[int, str],
    pr_urls: dict[int, str],
    pr_actions: dict[int, str],
    assessments: dict[int, PRAssessment],
    llm_assessments: dict[int, PRAssessment],
    skipped_drafts: list[PRData],
    passing_prs: list[PRData],
    llm_passing: list[PRData],
    pending_approval: list[PRData],
    workflows_in_progress: list[PRData],
) -> Table:
    """Build the per-PR timing table for triage."""
    safe_num_candidates = num_candidates or 1
    fetch_per_pr = enrich_total / safe_num_candidates

    action_styles = {
        "drafted": "[yellow]drafted[/]",
        "commented": "[yellow]commented[/]",
        "closed": "[red]closed[/]",
        "ready": "[success]ready[/]",
        "skipped": "[dim]skipped[/]",
        "workflow approved": "[success]workflow approved[/]",
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

    skipped_draft_nums = {pr.number for pr in skipped_drafts}
    passing_nums = {pr.number for pr in passing_prs}
    llm_passing_nums = {pr.number for pr in llm_passing}
    pending_nums = {pr.number for pr in pending_approval}
    in_progress_nums = {pr.number for pr in workflows_in_progress}

    all_pr_numbers = sorted(
        deterministic_timings.keys(),
        key=lambda n: deterministic_timings.get(n, 0) + fetch_per_pr,
        reverse=True,
    )
    for pr_num in all_pr_numbers:
        title = pr_titles.get(pr_num, "")[:60]
        det_time = deterministic_timings.get(pr_num, 0)
        total_time = fetch_per_pr + det_time

        if pr_num in skipped_draft_nums:
            result = "[dim]draft-skipped[/]"
        elif pr_num in assessments or pr_num in llm_assessments:
            action_for_result = pr_actions.get(pr_num, "")
            if action_for_result == "drafted":
                result = "[yellow]drafted[/]"
            elif action_for_result == "commented":
                result = "[yellow]commented[/]"
            elif action_for_result == "closed":
                result = "[red]closed[/]"
            elif action_for_result == "skipped":
                result = "[dim]skipped[/]"
            else:
                result = "[yellow]issues found[/]"
        elif pr_num in passing_nums or pr_num in llm_passing_nums:
            result = "[success]passed[/]"
        elif pr_num in pending_nums:
            result = "[dim]pending[/]"
        elif pr_num in in_progress_nums:
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
    return pr_timing_table


@dataclass
class BatchEnrichResult:
    """Result of enriching and filtering a batch of PRs."""

    all_prs: list[PRData]
    candidate_prs: list[PRData]
    accepted_prs: list[PRData]
    already_triaged: list[PRData]
    already_triaged_nums: set[int]
    triaged_classification: dict[str, set[int]]
    skipped_collaborator: int
    skipped_bot: int
    skipped_accepted: int


def _enrich_and_filter_batch(
    token: str,
    github_repository: str,
    all_prs: list[PRData],
    *,
    wildcard_labels: list[str],
    wildcard_exclude_labels: list[str],
    author_filter: AuthorFilter,
    include_drafts: bool,
    checks_state: str,
    min_commits_behind: int,
    max_num: int,
    viewer_login: str,
    also_accepted: set[int] | None = None,
    quiet: bool = False,
    on_branch_progress: Callable[[int, int], None] | None = None,
    on_merge_progress: Callable[[int, int], None] | None = None,
    on_ci_progress: Callable[[int, int], None] | None = None,
) -> BatchEnrichResult:
    """Enrich, filter, and classify already-triaged PRs in a single batch.

    This is the common pipeline shared by the initial batch, _load_more_batch, and pagination.
    """
    # Wildcard label filters
    all_prs = _apply_wildcard_label_filters(all_prs, wildcard_labels, wildcard_exclude_labels)

    # Branch status
    behind_map = _fetch_commits_behind_batch(
        token, github_repository, all_prs, on_progress=on_branch_progress
    )
    for pr in all_prs:
        pr.commits_behind = behind_map.get(pr.number, 0)

    # Merge status
    unknown_count = sum(1 for pr in all_prs if pr.mergeable == "UNKNOWN")
    if unknown_count:
        _resolve_unknown_mergeable(token, github_repository, all_prs, on_progress=on_merge_progress)

    # Verify CI for non-collab SUCCESS PRs
    non_collab_success = [
        pr
        for pr in all_prs
        if pr.checks_state == "SUCCESS"
        and pr.author_association not in _COLLABORATOR_ASSOCIATIONS
        and not _is_bot_account(pr.author_login)
    ]
    if non_collab_success:
        _fetch_check_details_batch(token, github_repository, non_collab_success, on_progress=on_ci_progress)

    # Filter candidates
    candidate_prs, accepted_prs, skipped_collab, skipped_bot, skipped_accepted = _filter_candidate_prs(
        all_prs,
        author_filter=author_filter,
        include_drafts=include_drafts,
        checks_state=checks_state,
        min_commits_behind=min_commits_behind,
        max_num=max_num,
        also_accepted=also_accepted,
        quiet=quiet,
    )

    # Check already triaged
    triaged_cls = _classify_already_triaged_prs(token, github_repository, candidate_prs, viewer_login)
    triaged_nums = triaged_cls["waiting"] | triaged_cls["responded"]
    already_triaged = [pr for pr in candidate_prs if pr.number in triaged_nums]
    candidate_prs = [pr for pr in candidate_prs if pr.number not in triaged_nums]

    return BatchEnrichResult(
        all_prs=all_prs,
        candidate_prs=candidate_prs,
        accepted_prs=accepted_prs,
        already_triaged=already_triaged,
        already_triaged_nums=triaged_nums,
        triaged_classification=triaged_cls,
        skipped_collaborator=skipped_collab,
        skipped_bot=skipped_bot,
        skipped_accepted=skipped_accepted,
    )


def _launch_llm_and_build_context(
    *,
    token: str,
    github_repository: str,
    dry_run: bool,
    answer_triage: str | None,
    main_failures: RecentPRFailureInfo | None,
    candidate_prs: list[PRData],
    assessments: dict[int, PRAssessment],
    llm_candidates: list[PRData],
    run_llm: bool,
    llm_model: str,
    llm_concurrency: int,
    stats: TriageStats,
    review_mode: bool = False,
) -> tuple[TriageContext, ThreadPoolExecutor | None]:
    """Launch LLM executor for candidates and build a TriageContext."""
    llm_future_to_pr: dict = {}
    llm_assessments: dict[int, PRAssessment] = {}
    llm_completed: list = []
    llm_errors: list[int] = []
    llm_passing: list[PRData] = []
    llm_executor = None

    if run_llm and llm_candidates:
        llm_executor = ThreadPoolExecutor(max_workers=llm_concurrency)
        if review_mode:
            llm_future_to_pr = {
                llm_executor.submit(
                    _review_pr_as_assessment,
                    token=token,
                    github_repository=github_repository,
                    head_sha=pr.head_sha,
                    pr_number=pr.number,
                    pr_title=pr.title,
                    pr_body=pr.body,
                    llm_model=llm_model,
                ): pr
                for pr in llm_candidates
            }
        else:
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

    log_futures = _launch_background_log_fetching(token, github_repository, candidate_prs, llm_concurrency)

    author_flagged_count: dict[str, int] = dict(
        Counter(pr.author_login for pr in candidate_prs if pr.number in assessments)
    )
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
    return ctx, llm_executor


def _process_pagination_batch(
    *,
    token: str,
    github_repository: str,
    exact_labels: tuple[str, ...],
    exact_exclude_labels: tuple[str, ...],
    filter_user: str | None,
    sort: str,
    batch_size: int,
    created_after: str | None,
    created_before: str | None,
    updated_after: str | None,
    updated_before: str | None,
    review_requested: str | None,
    next_cursor: str | None,
    wildcard_labels: list[str],
    wildcard_exclude_labels: list[str],
    author_filter: AuthorFilter,
    include_drafts: bool,
    checks_state: str,
    min_commits_behind: int,
    max_num: int,
    viewer_login: str,
    run_api: bool,
    run_llm: bool,
    llm_model: str,
    llm_concurrency: int,
    dry_run: bool,
    answer_triage: str | None,
    main_failures: RecentPRFailureInfo | None,
    stats: TriageStats,
    accepted_prs: list[PRData],
) -> tuple[bool, str | None]:
    """Process one pagination batch: fetch, enrich, categorize, review. Returns (has_next, cursor)."""
    all_prs, has_next_page, new_cursor, _ = _fetch_prs_graphql(
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
        review_requested=review_requested,
        after_cursor=next_cursor,
    )
    if not all_prs:
        console_print("[info]No more PRs to process.[/]")
        return False, new_cursor

    batch_result = _enrich_and_filter_batch(
        token,
        github_repository,
        all_prs,
        wildcard_labels=wildcard_labels,
        wildcard_exclude_labels=wildcard_exclude_labels,
        author_filter=author_filter,
        include_drafts=include_drafts,
        checks_state=checks_state,
        min_commits_behind=min_commits_behind,
        max_num=max_num,
        viewer_login=viewer_login,
    )
    all_prs = batch_result.all_prs
    candidate_prs = batch_result.candidate_prs
    accepted_prs.extend(batch_result.accepted_prs)

    # Reclassification logging
    non_collab_success = [
        pr
        for pr in all_prs
        if pr.checks_state == "SUCCESS"
        and pr.author_association not in _COLLABORATOR_ASSOCIATIONS
        and not _is_bot_account(pr.author_login)
    ]
    if non_collab_success:
        reclassified = sum(1 for pr in non_collab_success if pr.checks_state == "NOT_RUN")
        if reclassified:
            console_print(
                f"  [warning]{reclassified} {'PRs' if reclassified != 1 else 'PR'} "
                f"reclassified to NOT_RUN (only bot/labeler checks).[/]"
            )

    if not candidate_prs:
        console_print("[info]No PRs to assess in this batch.[/]")
        _display_pr_overview_table(all_prs)
        return has_next_page, new_cursor

    _display_pr_overview_table(
        all_prs,
        triaged_waiting_nums=batch_result.triaged_classification["waiting"],
        triaged_responded_nums=batch_result.triaged_classification["responded"],
    )

    if not candidate_prs:
        console_print("[info]All PRs in this batch already triaged.[/]")
        return has_next_page, new_cursor

    # Enrich and assess
    _enrich_candidate_details(token, github_repository, candidate_prs, run_api=run_api)

    batch_passing: list[PRData] = []
    if run_api:
        batch_assessments: dict[int, PRAssessment] = {}
        batch_llm_candidates: list[PRData] = []
        batch_pending: list[PRData] = []
        for pr in candidate_prs:
            det = _assess_pr_deterministic(pr, token=token, github_repository=github_repository)
            if det.category == "flagged" and det.assessment:
                batch_assessments[pr.number] = det.assessment
            elif det.category == "grace_period":
                get_console().print(
                    f"  [dim]Skipped {_pr_link(pr)} — CI failures less than "
                    f"{pr.ci_grace_period_hours}h old"
                    f"{' (collaborator engaged)' if pr.has_collaborator_review else ''}[/]"
                )
            elif det.category in ("in_progress", "draft_skipped"):
                pass
            elif det.category == "pending_approval":
                batch_pending.append(pr)
            elif det.category == "llm_candidate":
                batch_llm_candidates.append(pr)
    else:
        batch_assessments = {}
        batch_llm_candidates = []
        batch_pending = []
        for pr in candidate_prs:
            if pr.checks_state == "NOT_RUN":
                batch_pending.append(pr)
            else:
                batch_llm_candidates.append(pr)

    if not run_llm:
        batch_passing.extend(batch_llm_candidates)
        batch_llm_candidates_for_llm: list[PRData] = []
    else:
        batch_llm_candidates_for_llm = batch_llm_candidates

    batch_ctx, batch_executor = _launch_llm_and_build_context(
        token=token,
        github_repository=github_repository,
        dry_run=dry_run,
        answer_triage=answer_triage,
        main_failures=main_failures,
        candidate_prs=candidate_prs,
        assessments=batch_assessments,
        llm_candidates=batch_llm_candidates_for_llm,
        run_llm=run_llm,
        llm_model=llm_model,
        llm_concurrency=llm_concurrency,
        stats=stats,
    )

    try:
        _review_workflow_approval_prs(batch_ctx, batch_pending)

        det_flagged = [
            (pr, batch_assessments[pr.number]) for pr in candidate_prs if pr.number in batch_assessments
        ]
        det_flagged.sort(key=lambda pair: (pair[0].author_login.lower(), pair[0].number))
        _review_deterministic_flagged_prs(batch_ctx, det_flagged)

        _review_llm_flagged_prs(batch_ctx, batch_llm_candidates)
        batch_passing.extend(batch_ctx.llm_passing)

        _review_passing_prs(batch_ctx, batch_passing)
        _review_stale_review_requests(batch_ctx, batch_result.accepted_prs)
    except KeyboardInterrupt:
        console_print("\n[warning]Interrupted — shutting down.[/]")
        stats.quit_early = True
    finally:
        if batch_executor is not None:
            batch_executor.shutdown(wait=False, cancel_futures=True)

    # Accumulate batch counts into cumulative stats
    stats.cumulative_all_prs += len(all_prs)
    stats.cumulative_candidates += len(candidate_prs)
    stats.cumulative_passing += len(batch_passing)
    stats.cumulative_pending_approval += len(batch_pending)
    stats.cumulative_deterministic_flags += len(det_flagged)
    stats.cumulative_llm_flagged += len(batch_ctx.llm_assessments)
    stats.cumulative_llm_errors += len(batch_ctx.llm_errors)
    stats.cumulative_llm_report += sum(1 for a in batch_ctx.llm_assessments.values() if a.should_report)

    return has_next_page, new_cursor


def _build_load_more_fn(
    *,
    token: str,
    github_repository: str,
    exact_labels: tuple[str, ...],
    exact_exclude_labels: tuple[str, ...],
    filter_user: str | None,
    sort: str,
    batch_size: int,
    created_after: str | None,
    created_before: str | None,
    updated_after: str | None,
    updated_before: str | None,
    review_requested: str | None,
    initial_cursor: str | None,
    initial_has_more: bool,
    author_filter: AuthorFilter,
    include_drafts: bool,
    checks_state: str,
    min_commits_behind: int,
    viewer_login: str,
    run_api: bool,
    run_llm: bool,
    llm_model: str,
    llm_executor: ThreadPoolExecutor | None,
    ctx: TriageContext,
    review_mode: bool = False,
) -> Callable:
    """Build a closure that fetches + enriches + categorizes the next page of PRs for the TUI."""
    _batch_cursor: list[str | None] = [initial_cursor]
    _batch_has_more: list[bool] = [initial_has_more]

    def _load_more_batch():
        if not _batch_has_more[0] or not _batch_cursor[0]:
            return None

        new_prs, more, cursor, _ = _fetch_prs_graphql(
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
            review_requested=review_requested,
            after_cursor=_batch_cursor[0],
            quiet=True,
        )
        _batch_cursor[0] = cursor
        _batch_has_more[0] = more

        if not new_prs:
            return None

        # Enrich: branch status + CI verification
        behind_map = _fetch_commits_behind_batch(token, github_repository, new_prs)
        for pr in new_prs:
            pr.commits_behind = behind_map.get(pr.number, 0)

        non_collab_success = [
            pr
            for pr in new_prs
            if pr.checks_state == "SUCCESS"
            and pr.author_association not in _COLLABORATOR_ASSOCIATIONS
            and not _is_bot_account(pr.author_login)
        ]
        if non_collab_success:
            _fetch_check_details_batch(token, github_repository, non_collab_success)

        # Filter candidates
        new_candidates, new_accepted, _, _, _ = _filter_candidate_prs(
            new_prs,
            author_filter=author_filter,
            include_drafts=include_drafts,
            checks_state=checks_state,
            min_commits_behind=min_commits_behind,
            max_num=0,
            quiet=True,
        )

        # Check already triaged
        new_triaged_class = _classify_already_triaged_prs(
            token, github_repository, new_candidates, viewer_login
        )
        new_triaged_nums = new_triaged_class["waiting"] | new_triaged_class["responded"]
        new_candidates = [pr for pr in new_candidates if pr.number not in new_triaged_nums]

        # Enrich candidate details
        if new_candidates:
            _enrich_candidate_details(token, github_repository, new_candidates, run_api=run_api, quiet=True)

        # Categorize
        new_assessments_map: dict[int, PRAssessment] = {}
        new_pending: list[PRData] = []
        new_passing: list[PRData] = []
        new_det_flagged: list[tuple[PRData, PRAssessment]] = []

        if run_api:
            for pr in new_candidates:
                det = _assess_pr_deterministic(pr, token=token, github_repository=github_repository)
                if det.category == "flagged" and det.assessment:
                    new_assessments_map[pr.number] = det.assessment
                    new_det_flagged.append((pr, det.assessment))
                elif det.category in ("in_progress", "draft_skipped", "grace_period"):
                    continue
                elif det.category == "pending_approval":
                    new_pending.append(pr)
                elif det.category == "llm_candidate":
                    if run_llm and llm_executor:
                        if review_mode:
                            fut = llm_executor.submit(
                                _review_pr_as_assessment,
                                token=token,
                                github_repository=github_repository,
                                head_sha=pr.head_sha,
                                pr_number=pr.number,
                                pr_title=pr.title,
                                pr_body=pr.body,
                                llm_model=llm_model,
                            )
                        else:
                            fut = llm_executor.submit(
                                _cached_assess_pr,
                                github_repository=github_repository,
                                head_sha=pr.head_sha,
                                pr_number=pr.number,
                                pr_title=pr.title,
                                pr_body=pr.body,
                                check_status_summary=pr.check_summary,
                                llm_model=llm_model,
                            )
                        ctx.llm_future_to_pr[fut] = pr
                    else:
                        new_passing.append(pr)
        else:
            for pr in new_candidates:
                if pr.checks_state == "NOT_RUN":
                    new_pending.append(pr)
                else:
                    new_passing.append(pr)

        return (
            new_prs,
            new_det_flagged,
            new_pending,
            new_passing,
            new_triaged_nums,
            more,
        )

    return _load_more_batch


@dataclass
class FetchResult:
    """Result of the initial PR fetching phase."""

    all_prs: list[PRData]
    has_next_page: bool
    next_cursor: str | None
    total_matching_prs: int
    reviewed_by_prs: set[int]


def _fetch_initial_prs(
    *,
    token: str,
    github_repository: str,
    pr_number: int | None,
    exact_labels: tuple[str, ...],
    exact_exclude_labels: tuple[str, ...],
    wildcard_labels: list[str],
    wildcard_exclude_labels: list[str],
    filter_user: str | None,
    sort: str,
    batch_size: int,
    created_after: str | None,
    created_before: str | None,
    updated_after: str | None,
    updated_before: str | None,
    review_requested_user: str | None,
    review_requested_users: list[str],
    review_mode: bool,
    labels: tuple[str, ...],
    quiet: bool = False,
) -> FetchResult:
    """Phase 1: Fetch PRs via GraphQL, apply wildcard filters, fetch reviewed-by PRs."""
    console = get_console()
    has_next_page = False
    next_cursor: str | None = None
    total_matching_prs = 0

    # In review mode, don't pass review_requested to the initial fetch — the label filter
    # is sufficient.  The reviewed-by secondary fetch below brings in PRs the user already
    # reviewed.  Mixing review_requested AND label in one query would AND them, missing PRs
    # that have the label but aren't explicitly requested for review.
    _initial_review_requested_user: str | None = None if review_mode else review_requested_user

    if pr_number:
        if not quiet:
            console_print(f"[info]Fetching PR #{pr_number} via GraphQL...[/]")
        all_prs = [_fetch_single_pr_graphql(token, github_repository, pr_number)]
        total_matching_prs = 1
    elif len(review_requested_users) > 1 and not review_mode:
        if not quiet:
            console_print("[info]Fetching PRs via GraphQL for multiple reviewers...[/]")
        seen_numbers: set[int] = set()
        all_prs = list[PRData]()
        for reviewer in review_requested_users:
            batch_prs, _, _, _ = _fetch_prs_graphql(
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
                quiet=quiet,
            )
            for pr in batch_prs:
                if pr.number not in seen_numbers:
                    seen_numbers.add(pr.number)
                    all_prs.append(pr)
        has_next_page = False
        total_matching_prs = len(all_prs)
    else:
        if not quiet:
            console_print("[info]Fetching PRs via GraphQL...[/]")
        all_prs, has_next_page, next_cursor, total_matching_prs = _fetch_prs_graphql(
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
            review_requested=_initial_review_requested_user,
            quiet=quiet,
        )

    # Apply wildcard label filters client-side
    all_prs = _apply_wildcard_label_filters(all_prs, wildcard_labels, wildcard_exclude_labels)

    # In review mode, also fetch PRs where the reviewer has already submitted a review
    reviewed_by_prs: set[int] = set()
    if review_mode and review_requested_users:
        seen_numbers = {pr.number for pr in all_prs}
        base_exact_labels = tuple(lbl for lbl in labels if "*" not in lbl and "?" not in lbl)
        for reviewer in review_requested_users:
            batch_prs, _, _, _ = _fetch_prs_graphql(
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
                quiet=quiet,
            )
            for pr in batch_prs:
                if pr.number not in seen_numbers:
                    seen_numbers.add(pr.number)
                    all_prs.append(pr)
                    reviewed_by_prs.add(pr.number)
        if reviewed_by_prs and not quiet:
            console.print(
                f"[info]Also found {len(reviewed_by_prs)} "
                f"{'PRs' if len(reviewed_by_prs) != 1 else 'PR'} "
                f"previously reviewed by {', '.join(review_requested_users)}.[/]"
            )

    return FetchResult(
        all_prs=all_prs,
        has_next_page=has_next_page,
        next_cursor=next_cursor,
        total_matching_prs=total_matching_prs,
        reviewed_by_prs=reviewed_by_prs,
    )


@dataclass
class StartupEnrichResult:
    """Result of the startup enrichment phase (branch status, merge, CI verify, filter, triage)."""

    candidate_prs: list[PRData]
    accepted_prs: list[PRData]
    already_triaged: list[PRData]
    already_triaged_nums: set[int]
    triaged_classification: dict[str, set[int]]
    triaged_waiting_count: int
    triaged_responded_count: int
    total_skipped_collaborator: int
    total_skipped_bot: int
    total_skipped_accepted: int


def _run_startup_enrichment(
    token: str,
    github_repository: str,
    all_prs: list[PRData],
    *,
    author_filter: AuthorFilter,
    include_drafts: bool,
    checks_state: str,
    min_commits_behind: int,
    max_num: int,
    viewer_login: str,
    review_mode: bool,
    reviewed_by_prs: set[int],
    quiet: bool = False,
    on_branch_progress: Callable[[int, int], None] | None = None,
    on_merge_progress: Callable[[int, int], None] | None = None,
    on_ci_progress: Callable[[int, int], None] | None = None,
    on_triage_progress: Callable[[int, int], None] | None = None,
) -> StartupEnrichResult:
    """Phase 2: Enrich PRs with branch/merge/CI data, filter candidates, check triage status."""
    console = get_console()

    # Branch status
    if not quiet:
        console.print()
        console.rule("[bold]Phase 2: Enrich PR data[/]")
        console_print("[info]Checking how far behind base branch each PR is...[/]")
    t_step = time.monotonic()
    behind_map = _fetch_commits_behind_batch(
        token, github_repository, all_prs, on_progress=on_branch_progress
    )
    for pr in all_prs:
        pr.commits_behind = behind_map.get(pr.number, 0)
    behind_count = sum(1 for pr in all_prs if pr.commits_behind > 0)
    if not quiet:
        console.print(
            f"  [dim]{behind_count} PRs behind base branch ({_fmt_duration(time.monotonic() - t_step)})[/]"
        )

    # Merge status
    unknown_count = sum(1 for pr in all_prs if pr.mergeable == "UNKNOWN")
    if on_merge_progress:
        if unknown_count:
            on_merge_progress(0, unknown_count)
        else:
            on_merge_progress(1, 1)  # no unknowns — mark as done immediately
    if unknown_count:
        t_step = time.monotonic()
        if not quiet:
            console_print(
                f"[info]Resolving merge conflict status for {unknown_count} "
                f"{'PRs' if unknown_count != 1 else 'PR'} with unknown status...[/]"
            )
        resolved = _resolve_unknown_mergeable(
            token, github_repository, all_prs, on_progress=on_merge_progress
        )
        remaining = unknown_count - resolved
        if not quiet:
            if remaining:
                console_print(
                    f"  [dim]{resolved} resolved, {remaining} still unknown "
                    f"(GitHub hasn't computed mergeability yet) "
                    f"({_fmt_duration(time.monotonic() - t_step)})[/]"
                )
            else:
                console_print(
                    f"  [dim]All {resolved} resolved ({_fmt_duration(time.monotonic() - t_step)})[/]"
                )
    conflict_count = sum(1 for pr in all_prs if pr.mergeable == "CONFLICTING")
    if conflict_count and not quiet:
        console.print(f"  [dim]{conflict_count} PRs have merge conflicts[/]")

    # CI verification
    non_collab_success = [
        pr
        for pr in all_prs
        if pr.checks_state == "SUCCESS"
        and pr.author_association not in _COLLABORATOR_ASSOCIATIONS
        and not _is_bot_account(pr.author_login)
    ]
    if on_ci_progress:
        if non_collab_success:
            on_ci_progress(0, len(non_collab_success))
        else:
            on_ci_progress(1, 1)
    if non_collab_success:
        t_step = time.monotonic()
        if not quiet:
            console_print(
                f"[info]Verifying CI status for {len(non_collab_success)} "
                f"{'PRs' if len(non_collab_success) != 1 else 'PR'} "
                f"showing SUCCESS (checking for real test checks)...[/]"
            )
        _fetch_check_details_batch(token, github_repository, non_collab_success, on_progress=on_ci_progress)
        reclassified = sum(1 for pr in non_collab_success if pr.checks_state == "NOT_RUN")
        if not quiet:
            if reclassified:
                console_print(
                    f"  [warning]{reclassified} {'PRs' if reclassified != 1 else 'PR'} "
                    f"reclassified to NOT_RUN (only bot/labeler checks, no real CI) "
                    f"({_fmt_duration(time.monotonic() - t_step)})[/]"
                )
            else:
                console.print(
                    f"  [dim]All verified as real CI ({_fmt_duration(time.monotonic() - t_step)})[/]"
                )

    # Filter candidates
    if not quiet:
        console.print()
        console.rule("[bold]Phase 3: Filter & classify[/]")
    candidate_prs, accepted_prs, total_skipped_collaborator, total_skipped_bot, total_skipped_accepted = (
        _filter_candidate_prs(
            all_prs,
            author_filter=author_filter,
            include_drafts=include_drafts,
            checks_state=checks_state,
            min_commits_behind=min_commits_behind,
            max_num=max_num,
            also_accepted=reviewed_by_prs if review_mode else None,
            quiet=quiet,
        )
    )
    if not quiet:
        console.print(
            f"[info]Candidates: [bold]{len(candidate_prs)}[/bold] "
            f"(skipped {total_skipped_collaborator} collaborator, "
            f"{total_skipped_bot} bot"
            f"{f', {total_skipped_accepted} accepted' if total_skipped_accepted else ''})[/]"
        )

    # Triage check
    t_step = time.monotonic()
    if on_triage_progress:
        if candidate_prs:
            on_triage_progress(0, len(candidate_prs))
        else:
            on_triage_progress(1, 1)
    if not quiet:
        console_print(
            "[info]Checking for PRs already triaged (no new commits since last triage comment)...[/]"
        )
    triaged_classification = _classify_already_triaged_prs(
        token, github_repository, candidate_prs, viewer_login, on_progress=on_triage_progress
    )
    already_triaged_nums = triaged_classification["waiting"] | triaged_classification["responded"]
    triaged_waiting_count = len(triaged_classification["waiting"])
    triaged_responded_count = len(triaged_classification["responded"])
    already_triaged: list[PRData] = []
    if already_triaged_nums:
        already_triaged = [pr for pr in candidate_prs if pr.number in already_triaged_nums]
        candidate_prs = [pr for pr in candidate_prs if pr.number not in already_triaged_nums]
        if not quiet:
            console_print(
                f"[info]Skipped {len(already_triaged)} already-triaged "
                f"{'PRs' if len(already_triaged) != 1 else 'PR'} "
                f"({triaged_waiting_count} commented, "
                f"{triaged_responded_count} author responded) "
                f"({_fmt_duration(time.monotonic() - t_step)})[/]"
            )
    elif not quiet:
        console_print(f"  [dim]None found ({_fmt_duration(time.monotonic() - t_step)})[/]")

    # Overview table
    if not quiet:
        _display_pr_overview_table(
            all_prs,
            triaged_waiting_nums=triaged_classification["waiting"],
            triaged_responded_nums=triaged_classification["responded"],
        )

    return StartupEnrichResult(
        candidate_prs=candidate_prs,
        accepted_prs=accepted_prs,
        already_triaged=already_triaged,
        already_triaged_nums=already_triaged_nums,
        triaged_classification=triaged_classification,
        triaged_waiting_count=triaged_waiting_count,
        triaged_responded_count=triaged_responded_count,
        total_skipped_collaborator=total_skipped_collaborator,
        total_skipped_bot=total_skipped_bot,
        total_skipped_accepted=total_skipped_accepted,
    )


def _reevaluate_triaged_pr(
    pr: PRData,
    *,
    token: str,
    github_repository: str,
    run_api: bool,
) -> DeterministicResult:
    """Re-evaluate a previously triaged PR: enrich it and run deterministic checks.

    Enriches the PR with fresh check details, merge status, and review comments,
    then runs deterministic assessment. Returns the DeterministicResult so the
    caller can re-categorize the entry.
    """
    # Enrich with latest data
    _enrich_candidate_details(token, github_repository, [pr], run_api=run_api, quiet=True)
    # Run deterministic checks
    if run_api:
        return _assess_pr_deterministic(pr, token=token, github_repository=github_repository)
    # Without API checks, categorize by check state
    if pr.checks_state == "NOT_RUN":
        return DeterministicResult(category="pending_approval")
    return DeterministicResult(category="llm_candidate")


def _enrich_and_categorize_candidates(
    token: str,
    github_repository: str,
    candidate_prs: list[PRData],
    *,
    run_api: bool,
    quiet: bool = False,
    on_check_progress: Callable[[int, int], None] | None = None,
    on_merge_progress: Callable[[int, int], None] | None = None,
    on_review_progress: Callable[[int, int], None] | None = None,
    on_classify_progress: Callable[[int, int], None] | None = None,
) -> tuple[CategorizationResult, float, float]:
    """Enrich candidate PRs and run deterministic categorization.

    Returns (categorization_result, enrich_start_time, enrich_end_time).
    """
    t_enrich_start = time.monotonic()
    _enrich_candidate_details(
        token,
        github_repository,
        candidate_prs,
        run_api=run_api,
        quiet=quiet,
        on_check_progress=on_check_progress,
        on_merge_progress=on_merge_progress,
        on_review_progress=on_review_progress,
    )
    t_enrich_end = time.monotonic()

    cat_result = _categorize_all_candidates(
        candidate_prs,
        token=token,
        github_repository=github_repository,
        run_api=run_api,
        progress_cb=on_classify_progress,
    )

    if not quiet:
        if cat_result.skipped_drafts:
            console_print(
                f"[info]Skipped {len(cat_result.skipped_drafts)} draft "
                f"{'PRs' if len(cat_result.skipped_drafts) != 1 else 'PR'} "
                f"with existing issues (CI failures, conflicts, or unresolved comments).[/]"
            )
        if cat_result.workflows_in_progress:
            console_print(
                f"[info]Excluded {len(cat_result.workflows_in_progress)} "
                f"{'PRs' if len(cat_result.workflows_in_progress) != 1 else 'PR'} "
                f"with workflows already in progress.[/]"
            )
        if cat_result.recent_failures_skipped:
            get_console().print(
                f"[info]Skipped {len(cat_result.recent_failures_skipped)} "
                f"{'PRs' if len(cat_result.recent_failures_skipped) != 1 else 'PR'} "
                f"with recent CI failures within grace period "
                f"(giving authors time to address at their own pace):[/]"
            )
            for pr in cat_result.recent_failures_skipped:
                grace = pr.ci_grace_period_hours
                engaged = " (collaborator engaged)" if pr.has_collaborator_review else ""
                get_console().print(f"  [dim]{_pr_link(pr)} — {pr.title[:70]} [<{grace}h{engaged}][/]")

    return cat_result, t_enrich_start, t_enrich_end


def _enrich_pr_batch(
    token: str,
    github_repository: str,
    all_prs: list[PRData],
    *,
    use_tui: bool = False,
    on_branch_progress: Callable | None = None,
    on_merge_progress: Callable | None = None,
    on_ci_progress: Callable | None = None,
) -> None:
    """Enrich PRs with branch status, merge status, and CI verification (Phase 2)."""
    # Branch status
    behind_map = _fetch_commits_behind_batch(
        token, github_repository, all_prs, on_progress=on_branch_progress
    )
    for pr in all_prs:
        pr.commits_behind = behind_map.get(pr.number, 0)

    # Merge status
    unknown_count = sum(1 for pr in all_prs if pr.mergeable == "UNKNOWN")
    if unknown_count:
        _resolve_unknown_mergeable(token, github_repository, all_prs, on_progress=on_merge_progress)

    # Verify CI for non-collab SUCCESS PRs (detect bot-only checks)
    non_collab_success = [
        pr
        for pr in all_prs
        if pr.checks_state == "SUCCESS"
        and pr.author_association not in _COLLABORATOR_ASSOCIATIONS
        and not _is_bot_account(pr.author_login)
    ]
    if non_collab_success:
        _fetch_check_details_batch(token, github_repository, non_collab_success, on_progress=on_ci_progress)


def _build_selection_criteria(
    *,
    pr_number: int | None,
    labels: tuple[str, ...],
    exclude_labels: tuple[str, ...],
    filter_user: str | None,
    review_requested_users: list[str],
    created_after: str | None,
    created_before: str | None,
    updated_after: str | None,
    updated_before: str | None,
    checks_state: str,
    min_commits_behind: int,
    include_drafts: bool,
    author_filter: AuthorFilter,
    sort: str,
    batch_size: int,
    triage_mode: str,
) -> str:
    """Build a human-readable selection criteria string for the TUI header."""
    parts: list[str] = []
    if pr_number:
        parts.append(f"PR #{pr_number}")
    if labels:
        parts.append(f"labels={','.join(labels)}")
    if exclude_labels:
        parts.append(f"exclude={','.join(exclude_labels)}")
    if filter_user:
        parts.append(f"user={filter_user}")
    if review_requested_users:
        parts.append(f"reviewer={','.join(review_requested_users)}")
    if created_after:
        parts.append(f"created>={created_after}")
    if created_before:
        parts.append(f"created<={created_before}")
    if updated_after:
        parts.append(f"updated>={updated_after}")
    if updated_before:
        parts.append(f"updated<={updated_before}")
    if checks_state != "all":
        parts.append(f"checks={checks_state}")
    if min_commits_behind > 0:
        parts.append(f"behind>={min_commits_behind}")
    if include_drafts:
        parts.append("include_drafts")
    if author_filter != AuthorFilter.CONTRIBUTORS:
        parts.append(f"authors={author_filter.value}")
    if sort != "created":
        parts.append(f"sort={sort}")
    parts.append(f"batch={batch_size}")
    if triage_mode != "triage":
        parts.append(f"mode={triage_mode}")
    return " | ".join(parts) if parts else "defaults"


def _display_review_mode_summary(
    stats: TriageStats,
    accepted_prs: list[PRData],
    pr_timings: dict[int, float],
    pr_actions: dict[int, str],
    total_elapsed: float,
    t_phase1_elapsed: float,
    all_prs_count: int,
) -> None:
    """Display the summary tables for review mode."""
    console = get_console()

    console.print(f"\n[info]Review mode complete in {_fmt_duration(total_elapsed)}.[/]")

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

    timing_table = Table(title="Timing Summary")
    timing_table.add_column("Phase", style="bold")
    timing_table.add_column("Total", justify="right")
    timing_table.add_column("PRs", justify="right")
    timing_table.add_column("Avg/PR", justify="right")

    num_all = all_prs_count or 1
    timing_table.add_row(
        "Fetch PRs + overview",
        _fmt_duration(t_phase1_elapsed),
        str(all_prs_count),
        _fmt_duration(t_phase1_elapsed / num_all),
    )

    interactive_total = total_elapsed - t_phase1_elapsed
    timing_table.add_row(
        "Interactive review",
        _fmt_duration(interactive_total),
        str(len(accepted_prs)),
        _fmt_duration(interactive_total / len(accepted_prs)) if accepted_prs else "[dim]—[/]",
    )

    timing_table.add_row("", "", "", "")
    prs_with_timing = len(pr_timings)
    avg_per_pr = _fmt_duration(total_elapsed / prs_with_timing) if prs_with_timing else "[dim]—[/]"
    timing_table.add_row(
        "[bold]Total[/]",
        f"[bold]{_fmt_duration(total_elapsed)}[/]",
        str(prs_with_timing) if prs_with_timing else "",
        f"[bold]{avg_per_pr}[/]" if prs_with_timing else "",
    )
    console.print(timing_table)

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
    "--authors",
    type=click.Choice(["contributors", "collaborators", "all"]),
    default="contributors",
    help="Which authors to include: 'contributors' (non-collaborators, default), "
    "'collaborators' (collaborators/members/owners only), 'all' (everyone).",
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
    "--tui/--no-tui",
    default=False,
    show_default=True,
    help="Use full-screen TUI mode (requires TTY). Default: --no-tui (sequential mode).",
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
    "--llm-use",
    "llm_use",
    type=click.Choice(["both", "api", "llm"]),
    default="both",
    show_default=True,
    help="Which checks to run: 'both' (API + LLM), 'api' (deterministic only), 'llm' (LLM only).",
)
@click.option(
    "--llm-concurrency",
    type=int,
    default=4 if generating_command_images() else min(os.cpu_count() or 4, 8),
    show_default=True,
    help="Number of concurrent LLM assessment calls (default: CPU count, max 8).",
)
@option_llm_model
@click.option(
    "--clear-cache",
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
    authors: str,
    include_drafts: bool,
    pending_approval_only: bool,
    triage_mode: str,
    tui: bool,
    checks_state: str,
    min_commits_behind: int,
    my_reviews: bool,
    reviewers: tuple[str, ...],
    llm_use: str,
    llm_concurrency: int,
    llm_model: str,
    clear_cache: bool,
    answer_triage: str | None,
):

    from airflow_breeze.utils.llm_utils import (
        _check_cli_available,
        _resolve_cli_provider,
        check_llm_cli_safety,
    )

    author_filter = AuthorFilter(authors)

    token = _resolve_github_token(github_token)
    if not token:
        console_print(
            "[error]GitHub token not found. Provide --github-token, "
            "set GITHUB_TOKEN, or authenticate with `gh auth login`.[/]"
        )
        sys.exit(1)

    run_api = llm_use in ("both", "api")
    run_llm = llm_use in ("both", "llm")

    console = get_console()

    if clear_cache:
        _clear_triage_caches(github_repository)
    dry_run = get_dry_run()

    # Determine TUI mode: --tui enables it (requires TTY), --no-tui (default) skips it
    use_tui = tui and _has_tty() and not dry_run and not answer_triage

    mode_desc = {"both": "API + LLM", "api": "API only", "llm": "LLM only"}
    review_mode = triage_mode == "review"
    if not use_tui:
        console.print(
            f"[info]LLM use: [bold]{llm_use}[/bold] ({mode_desc.get(llm_use, llm_use)}). "
            f"Change with --llm-use (api|llm|both).[/]"
        )
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

    # Validate mutual exclusivity
    if filter_user and authors != "contributors":
        console_print("[error]--author and --authors are mutually exclusive.[/]")
        sys.exit(1)
    if my_reviews and reviewers:
        console_print("[error]--reviews-for-me and --reviews-for are mutually exclusive.[/]")
        sys.exit(1)

    if not use_tui:
        console.print(f"[info]Repository: [bold]{github_repository}[/bold][/]")
        console.print("[info]Display mode: [bold]sequential[/bold][/]")

    # Progress bar for startup phases (shown in both TUI and sequential mode)
    from rich.progress import BarColumn, Progress, SpinnerColumn, TaskID, TextColumn, TimeElapsedColumn

    _progress_tasks: dict[str, TaskID] = {}
    _progress = Progress(
        SpinnerColumn(),
        TextColumn("[bold]{task.description}"),
        BarColumn(bar_width=20),
        TextColumn("[dim]{task.fields[status]}"),
        TimeElapsedColumn(),
        console=console,
    )
    _step_names = [
        ("auth", "Authenticate"),
        ("cache_check", "Validate cache"),
        ("ci_failures", "Load CI failures"),
        ("fetch_prs", "Fetch PRs"),
        ("branch_status", "Check branch status"),
        ("merge_status", "Resolve merge status"),
        ("verify_ci", "Verify CI status"),
        ("classify", "Filter & classify"),
        ("triage_check", "Check prior triage"),
    ]
    for key, desc in _step_names:
        _progress_tasks[key] = _progress.add_task(desc, total=1, status="waiting")
    _progress.start()

    def _step_start(key: str, status: str = "running", total: int = 1) -> None:
        if _progress and key in _progress_tasks:
            _progress.update(_progress_tasks[key], total=total, status=status)

    def _step_done(key: str, status: str = "done") -> None:
        if _progress and key in _progress_tasks:
            task = _progress_tasks[key]
            # Set completed to total to fill the bar
            _progress.update(task, completed=_progress.tasks[task].total, status=status)

    def _make_progress_cb(key: str) -> Callable[[int, int], None] | None:
        """Create a callback that updates the progress bar for a step with real item counts."""
        if not _progress or key not in _progress_tasks:
            return None
        progress = _progress
        task_id = _progress_tasks[key]

        def _cb(completed: int, total: int) -> None:
            progress.update(task_id, completed=completed, total=total, status=f"{completed}/{total}")

        return _cb

    # Resolve the authenticated user login (used for --reviews-for-me and triage comment detection)
    _step_start("auth")
    t_step = time.monotonic()
    viewer_login = _resolve_viewer_login(token)
    _step_done("auth", f"as {viewer_login}")
    if not use_tui:
        console.print(
            f"[info]Authenticated as: [bold]{viewer_login}[/bold] "
            f"({_fmt_duration(time.monotonic() - t_step)})[/]"
        )

    # Refresh collaborators cache in the background on every run
    _refresh_collaborators_cache_in_background(token, github_repository)

    # Validate cached PR data — remove stale entries where head_sha has changed
    _step_start("cache_check")
    t_step = time.monotonic()
    total_cached, stale_count, removed_count = _validate_and_refresh_caches(
        token, github_repository, quiet=use_tui
    )
    if total_cached:
        _step_done(
            "cache_check",
            f"{total_cached} cached, {stale_count} stale" if stale_count else f"{total_cached} up to date",
        )
    else:
        _step_done("cache_check", "empty")

    # Preload main branch CI failure information (cached for 4 hours)
    _step_start("ci_failures")
    t_step = time.monotonic()
    main_failures = _cached_fetch_recent_pr_failures(token, github_repository)
    _step_done("ci_failures", f"{len(main_failures.failing_check_names)} known failures")
    if not use_tui:
        console.print(
            f"[info]Main branch CI failures: "
            f"[bold]{len(main_failures.failing_check_names)}[/bold] known failing checks "
            f"({_fmt_duration(time.monotonic() - t_step)})[/]"
        )

    # Show status of recent scheduled (canary) builds on main branch (cached for 4 hours)
    canary_builds = _cached_fetch_main_canary_builds(token, github_repository)
    if not use_tui:
        _display_canary_builds_status(canary_builds, token, github_repository)

    # Resolve review-requested filter: --reviews-for-me uses authenticated user, --reviews-for uses specified users
    review_requested_user: str | None = None
    review_requested_users: list[str] = []
    if my_reviews:
        review_requested_user = viewer_login
        review_requested_users = [viewer_login]
        if not use_tui:
            console_print(f"[info]Filtering PRs with review requested for: {review_requested_user}[/]")
    elif reviewers:
        review_requested_users = list(reviewers)
        review_requested_user = reviewers[0]
        if not use_tui:
            console_print(f"[info]Filtering PRs with review requested for: {', '.join(reviewers)}[/]")

    # Phase 1: Fetch PRs via GraphQL
    # In review mode, force-include the "ready for maintainer review" label in the search
    # (unless we also fetch reviewed-by PRs below, in which case the label search is one of two queries)
    effective_labels = labels
    if review_mode and _READY_FOR_REVIEW_LABEL not in labels:
        effective_labels = (*labels, _READY_FOR_REVIEW_LABEL)

    exact_labels, wildcard_labels, exact_exclude_labels, wildcard_exclude_labels = _split_label_filters(
        effective_labels, exclude_labels
    )

    t_total_start = time.monotonic()

    # Phase 1: Lightweight fetch of PRs via GraphQL (no check contexts — fast)
    _step_start("fetch_prs")
    if not use_tui:
        console.print()
        console.rule("[bold]Phase 1: Fetch PRs[/]")
    t_phase1_start = time.monotonic()
    fetch_result = _fetch_initial_prs(
        token=token,
        github_repository=github_repository,
        pr_number=pr_number,
        exact_labels=exact_labels,
        exact_exclude_labels=exact_exclude_labels,
        wildcard_labels=wildcard_labels,
        wildcard_exclude_labels=wildcard_exclude_labels,
        filter_user=filter_user,
        sort=sort,
        batch_size=batch_size,
        created_after=created_after,
        created_before=created_before,
        updated_after=updated_after,
        updated_before=updated_before,
        review_requested_user=review_requested_user,
        review_requested_users=review_requested_users,
        review_mode=review_mode,
        labels=labels,
        quiet=use_tui,
    )
    all_prs = fetch_result.all_prs
    has_next_page = fetch_result.has_next_page
    next_cursor = fetch_result.next_cursor
    total_matching_prs = fetch_result.total_matching_prs
    reviewed_by_prs = fetch_result.reviewed_by_prs

    t_fetch = time.monotonic() - t_phase1_start
    collab_count = sum(1 for pr in all_prs if pr.author_association in _COLLABORATOR_ASSOCIATIONS)
    non_collab_count = len(all_prs) - collab_count
    _step_done("fetch_prs", f"{len(all_prs)} PRs")
    if not use_tui:
        console.print(
            f"[info]Fetched [bold]{len(all_prs)}[/bold] PRs "
            f"({non_collab_count} non-collaborator, {collab_count} collaborator) "
            f"in {_fmt_duration(t_fetch)}[/]"
        )
        if has_next_page:
            console.print(f"[info]More pages available (batch size: {batch_size})[/]")

    # Phase 2: Enrich PR data (branch status, merge, CI verify, filter, triage check)
    _step_start("branch_status", total=len(all_prs))
    _step_start("merge_status")
    _step_start("verify_ci")
    _step_start("classify")
    _step_start("triage_check")
    enrich_result = _run_startup_enrichment(
        token,
        github_repository,
        all_prs,
        author_filter=author_filter,
        include_drafts=include_drafts,
        checks_state=checks_state,
        min_commits_behind=min_commits_behind,
        max_num=max_num,
        viewer_login=viewer_login,
        review_mode=review_mode,
        reviewed_by_prs=reviewed_by_prs,
        quiet=use_tui,
        on_branch_progress=_make_progress_cb("branch_status"),
        on_merge_progress=_make_progress_cb("merge_status"),
        on_ci_progress=_make_progress_cb("verify_ci"),
        on_triage_progress=_make_progress_cb("triage_check"),
    )
    candidate_prs = enrich_result.candidate_prs
    accepted_prs = enrich_result.accepted_prs
    already_triaged = enrich_result.already_triaged
    already_triaged_nums = enrich_result.already_triaged_nums
    total_skipped_collaborator = enrich_result.total_skipped_collaborator
    total_skipped_bot = enrich_result.total_skipped_bot
    total_skipped_accepted = enrich_result.total_skipped_accepted
    triaged_waiting_count = enrich_result.triaged_waiting_count
    triaged_responded_count = enrich_result.triaged_responded_count
    # Update progress bar steps with final status
    behind_count = sum(1 for pr in all_prs if pr.commits_behind > 0)
    _step_done("branch_status", f"{behind_count} behind")
    conflict_count = sum(1 for pr in all_prs if pr.mergeable == "CONFLICTING")
    unknown_count = sum(1 for pr in all_prs if pr.mergeable == "UNKNOWN")
    _step_done(
        "merge_status", f"{conflict_count} conflicts" if unknown_count or conflict_count else "none unknown"
    )
    reclassified = sum(
        1
        for pr in all_prs
        if pr.checks_state == "NOT_RUN"
        and pr.author_association not in _COLLABORATOR_ASSOCIATIONS
        and not _is_bot_account(pr.author_login)
    )
    _step_done("verify_ci", f"{reclassified} reclassified" if reclassified else "none to verify")
    _step_done("classify", f"{len(candidate_prs)} candidates")
    triage_status = f"{len(already_triaged)} skipped" if already_triaged_nums else "none found"
    _step_done("triage_check", triage_status)

    t_phase1_end = time.monotonic()
    if _progress:
        _progress.stop()
        _progress = None  # type: ignore[assignment]
    if not use_tui:
        console.print(
            f"\n[info]Startup complete in [bold]{_fmt_duration(t_phase1_end - t_total_start)}[/bold].[/]"
        )

    # --- Review mode: early exit into review flow for accepted PRs ---
    if review_mode:
        if not accepted_prs:
            get_console().print(
                f"[info]No PRs with '{_READY_FOR_REVIEW_LABEL}' label"
                f" or reviewer reviews found matching the filters.[/]"
            )
            return

        if use_tui:
            # TUI review mode: only show accepted PRs (ready-for-review label + reviewer-reviewed)
            candidate_prs = list(accepted_prs)
            all_prs = list(accepted_prs)
        else:
            # Sequential review mode (existing flow)
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
            _display_review_mode_summary(
                stats,
                accepted_prs,
                pr_timings,
                pr_actions,
                total_elapsed=t_total_end - t_total_start,
                t_phase1_elapsed=t_phase1_end - t_total_start,
                all_prs_count=len(all_prs),
            )
            return

        # TUI review mode continues below — enrich, categorize, and use TUI
        # Override variables so the enrichment + TUI path works with accepted PRs
        already_triaged = []
        already_triaged_nums = set()
        total_skipped_collaborator = 0
        total_skipped_bot = 0
        total_skipped_accepted = 0
        triaged_waiting_count = 0
        triaged_responded_count = 0
        # (falls through to the same enrichment + TUI path as triage mode)

    # Enrich candidate PRs with check details, mergeable status, and review comments
    _enrich_progress = None
    _enrich_tasks: dict[str, TaskID] = {}
    if candidate_prs:
        _enrich_progress = Progress(
            SpinnerColumn(),
            TextColumn("[bold]{task.description}"),
            BarColumn(bar_width=20),
            TextColumn("[dim]{task.fields[status]}"),
            TimeElapsedColumn(),
            console=console,
        )
        _enrich_steps = [
            ("enrich_checks", "Fetch check details"),
            ("enrich_merge", "Resolve merge status"),
            ("enrich_reviews", "Fetch review threads"),
            ("enrich_classify", "Classify PRs"),
        ]
        for key, desc in _enrich_steps:
            _enrich_tasks[key] = _enrich_progress.add_task(desc, total=len(candidate_prs), status="waiting")
        _enrich_progress.start()

    def _enrich_cb(key: str) -> Callable[[int, int], None] | None:
        if not _enrich_progress or key not in _enrich_tasks:
            return None
        progress = _enrich_progress
        task_id = _enrich_tasks[key]

        def _cb(completed: int, total: int) -> None:
            progress.update(task_id, completed=completed, total=total, status=f"{completed}/{total}")

        return _cb

    def _enrich_done(key: str, status: str = "done") -> None:
        if _enrich_progress and key in _enrich_tasks:
            task = _enrich_tasks[key]
            _enrich_progress.update(task, completed=_enrich_progress.tasks[task].total, status=status)

    cat_result, t_enrich_start, t_enrich_end = _enrich_and_categorize_candidates(
        token,
        github_repository,
        candidate_prs,
        run_api=run_api,
        quiet=use_tui,
        on_check_progress=_enrich_cb("enrich_checks"),
        on_merge_progress=_enrich_cb("enrich_merge"),
        on_review_progress=_enrich_cb("enrich_reviews"),
        on_classify_progress=_enrich_cb("enrich_classify"),
    )
    _enrich_done("enrich_checks")
    _enrich_done("enrich_merge")
    _enrich_done("enrich_reviews")
    _enrich_done("enrich_classify", f"{cat_result.total_flagged} flagged")
    if _enrich_progress:
        _enrich_progress.stop()

    passing_prs: list[PRData] = []
    assessments = cat_result.assessments
    llm_candidates = cat_result.llm_candidates
    pending_approval = cat_result.pending_approval
    workflows_in_progress = cat_result.workflows_in_progress
    skipped_drafts = cat_result.skipped_drafts
    recent_failures_skipped = cat_result.recent_failures_skipped
    total_deterministic_flags = cat_result.total_flagged

    # Filter out pending_approval PRs that already have a comment from the viewer
    # (triage or rebase comment) with no new commits since — no point re-approving
    if pending_approval and viewer_login:
        already_commented_nums = _find_already_triaged_prs(
            token, github_repository, pending_approval, viewer_login, require_marker=False
        )
        if already_commented_nums:
            already_triaged.extend(pr for pr in pending_approval if pr.number in already_commented_nums)
            pending_approval = [pr for pr in pending_approval if pr.number not in already_commented_nums]
            if not use_tui:
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
    if not run_llm:
        if llm_candidates:
            if not use_tui:
                console_print(
                    f"\n[info]--llm-use=api: skipping LLM assessment for {len(llm_candidates)} "
                    f"{'PRs' if len(llm_candidates) != 1 else 'PR'}.[/]\n"
                )
            passing_prs.extend(llm_candidates)
        llm_candidates_for_llm: list[PRData] = []
    else:
        if llm_candidates and not use_tui:
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
        llm_candidates_for_llm = llm_candidates

    pr_actions = {}  # PR number -> action taken by user
    stats = TriageStats()
    # Initialize cumulative counts from the first batch
    stats.cumulative_all_prs = len(all_prs)
    stats.cumulative_candidates = len(candidate_prs)
    stats.cumulative_passing = len(passing_prs)
    stats.cumulative_pending_approval = len(pending_approval)
    stats.cumulative_workflows_in_progress = len(workflows_in_progress)
    stats.cumulative_skipped_drafts = len(skipped_drafts)
    stats.cumulative_recent_failures_skipped = len(recent_failures_skipped)
    stats.cumulative_already_triaged = len(already_triaged)
    stats.cumulative_deterministic_flags = total_deterministic_flags
    stats.cumulative_skipped_collaborator = total_skipped_collaborator
    stats.cumulative_skipped_bot = total_skipped_bot
    stats.cumulative_skipped_accepted = total_skipped_accepted
    stats.cumulative_triaged_waiting = triaged_waiting_count
    stats.cumulative_triaged_responded = triaged_responded_count
    ctx, llm_executor = _launch_llm_and_build_context(
        token=token,
        github_repository=github_repository,
        dry_run=dry_run,
        answer_triage=answer_triage,
        main_failures=main_failures,
        candidate_prs=candidate_prs,
        assessments=assessments,
        llm_candidates=llm_candidates_for_llm,
        run_llm=run_llm,
        llm_model=llm_model,
        llm_concurrency=llm_concurrency,
        stats=stats,
        review_mode=review_mode,
    )
    llm_passing = ctx.llm_passing

    det_flagged_prs = [(pr, assessments[pr.number]) for pr in candidate_prs if pr.number in assessments]
    det_flagged_prs.sort(key=lambda pair: (pair[0].author_login.lower(), pair[0].number))

    _tui_acted_entries: list[dict] = []
    _t_tui_start: float = 0.0
    try:
        if use_tui:
            # Full-screen TUI mode: show all PRs in an interactive full-screen view
            selection_criteria = _build_selection_criteria(
                pr_number=pr_number,
                labels=labels,
                exclude_labels=exclude_labels,
                filter_user=filter_user,
                review_requested_users=review_requested_users,
                created_after=created_after,
                created_before=created_before,
                updated_after=updated_after,
                updated_before=updated_before,
                checks_state=checks_state,
                min_commits_behind=min_commits_behind,
                include_drafts=include_drafts,
                author_filter=author_filter,
                sort=sort,
                batch_size=batch_size,
                triage_mode=triage_mode,
            )

            _load_more_batch = _build_load_more_fn(
                token=token,
                github_repository=github_repository,
                exact_labels=exact_labels,
                exact_exclude_labels=exact_exclude_labels,
                filter_user=filter_user,
                sort=sort,
                batch_size=batch_size,
                created_after=created_after,
                created_before=created_before,
                updated_after=updated_after,
                updated_before=updated_before,
                review_requested=review_requested_user,
                initial_cursor=next_cursor,
                initial_has_more=has_next_page,
                author_filter=author_filter,
                include_drafts=include_drafts,
                checks_state=checks_state,
                min_commits_behind=min_commits_behind,
                viewer_login=viewer_login,
                run_api=run_api,
                run_llm=run_llm,
                llm_model=llm_model,
                llm_executor=llm_executor,
                ctx=ctx,
                review_mode=review_mode,
            )

            _tui_acted_entries, _t_tui_start = _run_tui_triage(
                ctx,
                all_prs,
                pending_approval=pending_approval,
                det_flagged_prs=det_flagged_prs,
                llm_candidates=llm_candidates,
                passing_prs=passing_prs,
                accepted_prs=accepted_prs,
                already_triaged_nums=already_triaged_nums,
                run_api=run_api,
                run_llm=run_llm,
                llm_model=llm_model,
                llm_executor=llm_executor,
                author_filter=author_filter,
                include_drafts=include_drafts,
                mode_desc=f"{'Review' if review_mode else 'Triage'} | {mode_desc.get(llm_use, llm_use)}",
                selection_criteria=selection_criteria,
                total_matching_prs=total_matching_prs,
                viewer_login=viewer_login,
                load_more_fn=_load_more_batch if has_next_page else None,
            )
        else:
            # Sequential mode (CI / forced answer / no TTY)
            # Phase 4b: Present NOT_RUN PRs for workflow approval (LLM runs in background)
            _review_workflow_approval_prs(ctx, pending_approval)

            # Phase 5a: Present deterministically flagged PRs
            _review_deterministic_flagged_prs(ctx, det_flagged_prs)

            # Phase 5b: Present LLM-flagged PRs as they become ready (streaming)
            _review_llm_flagged_prs(ctx, llm_candidates)

            # Add LLM passing PRs to the passing list
            passing_prs.extend(llm_passing)

            # Phase 5c: Present passing PRs for optional ready-for-review marking
            _review_passing_prs(ctx, passing_prs)

            # Phase 5d: Check accepted PRs for stale CHANGES_REQUESTED reviews
            _review_stale_review_requests(ctx, accepted_prs)

            # Phase 5e: Present already-triaged PRs for optional re-evaluation
            _review_already_triaged_prs(ctx, already_triaged, run_api=run_api)
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
        has_next_page, next_cursor = _process_pagination_batch(
            token=token,
            github_repository=github_repository,
            exact_labels=exact_labels,
            exact_exclude_labels=exact_exclude_labels,
            filter_user=filter_user,
            sort=sort,
            batch_size=batch_size,
            created_after=created_after,
            created_before=created_before,
            updated_after=updated_after,
            updated_before=updated_before,
            review_requested=review_requested_user,
            next_cursor=next_cursor,
            wildcard_labels=wildcard_labels,
            wildcard_exclude_labels=wildcard_exclude_labels,
            author_filter=author_filter,
            include_drafts=include_drafts,
            checks_state=checks_state,
            min_commits_behind=min_commits_behind,
            max_num=max_num,
            viewer_login=viewer_login,
            run_api=run_api,
            run_llm=run_llm,
            llm_model=llm_model,
            llm_concurrency=llm_concurrency,
            dry_run=dry_run,
            answer_triage=answer_triage,
            main_failures=main_failures,
            stats=stats,
            accepted_prs=accepted_prs,
        )

    # Session summary
    t_total_end = time.monotonic()
    total_elapsed = t_total_end - t_total_start
    startup_elapsed = (_t_tui_start - t_total_start) if _t_tui_start > 0 else (t_enrich_end - t_total_start)
    interactive_elapsed = total_elapsed - startup_elapsed

    # Collect acted PRs from TUI or sequential mode
    acted_entries: list[dict] = []
    if use_tui:
        acted_entries = _tui_acted_entries if _tui_acted_entries else []
    else:
        # For sequential mode, build acted list from stats
        for pr in all_prs:
            act = pr_actions.get(pr.number)
            if act and act != "skipped":
                acted_entries.append(
                    {
                        "number": pr.number,
                        "title": pr.title,
                        "url": pr.url,
                        "author": pr.author_login,
                        "suggested": "",
                        "action": act,
                        "llm_status": "",
                        "duration": 0.0,
                    }
                )

    console = get_console()
    console.print()
    if acted_entries:
        import re as _re_summary

        action_table = Table(title="Session Summary — PRs with actions performed")
        action_table.add_column("#", style="cyan", width=7, no_wrap=True)
        action_table.add_column("Title", ratio=1, no_wrap=True)
        action_table.add_column("Author", width=16, no_wrap=True)
        action_table.add_column("Suggested", width=14, no_wrap=True)
        action_table.add_column("Performed", width=14, no_wrap=True)
        action_table.add_column("Time", width=8, justify="right", no_wrap=True)
        for entry in acted_entries:
            suggested_clean = _re_summary.sub(r"\[/?[^\]]*\]", "", entry.get("suggested", ""))
            pr_url = entry["url"]
            author = entry["author"]
            author_url = f"https://github.com/{github_repository}/pulls/{author}"
            duration = entry.get("duration", 0.0)
            duration_text = _fmt_duration(duration) if duration > 0 else "[dim]—[/]"
            action_table.add_row(
                f"[link={pr_url}]#{entry['number']}[/link]",
                entry["title"][:55] + ("..." if len(entry["title"]) > 55 else ""),
                f"[link={author_url}]{author[:16]}[/link]",
                suggested_clean,
                entry["action"],
                duration_text,
            )
        console.print(action_table)
    else:
        console.print("[info]No actions were performed in this session.[/]")

    # Session statistics
    total_acted = len(acted_entries)
    elapsed_hr = total_elapsed / 3600

    # Compute LLM wall time: time from first submission to last completion
    llm_wall_time = 0.0
    llm_total_completed = len(ctx.llm_completed)
    llm_total_submitted = len(ctx.llm_future_to_pr)
    if llm_total_submitted > 0 and llm_total_completed > 0:
        # Approximate: LLM ran concurrently during interactive time
        # Use the time from enrich_end (when LLM submissions start) to now,
        # but cap at interactive_elapsed since that's when LLM actually ran
        llm_wall_time = min(interactive_elapsed, t_total_end - t_enrich_end)

    console.print()
    console.print(f"[bold]Startup time:[/]        {_fmt_duration(startup_elapsed)}")
    console.print(f"[bold]Interactive time:[/]    {_fmt_duration(interactive_elapsed)}")
    if llm_total_submitted > 0:
        console.print(
            f"[bold]LLM time:[/]            {_fmt_duration(llm_wall_time)} "
            f"({llm_total_completed}/{llm_total_submitted} completed, "
            f"{len(ctx.llm_errors)} errors, {len(ctx.llm_passing)} passed)"
        )
        # Average and median LLM execution time from actual durations
        llm_dur_values = [d for d in ctx.llm_durations.values() if d > 0]
        if llm_dur_values:
            avg_llm = sum(llm_dur_values) / len(llm_dur_values)
            sorted_durs = sorted(llm_dur_values)
            n = len(sorted_durs)
            median_llm = (sorted_durs[n // 2] + sorted_durs[(n - 1) // 2]) / 2
            console.print(
                f"[bold]LLM avg/median:[/]      {_fmt_duration(avg_llm)} avg, "
                f"{_fmt_duration(median_llm)} median ({len(llm_dur_values)} PRs)"
            )
    console.print(f"[bold]Total session:[/]       {_fmt_duration(total_elapsed)}")
    console.print(f"[bold]PRs triaged:[/]         {total_acted}")
    if total_acted > 0:
        velocity_hr = total_acted / elapsed_hr if elapsed_hr > 0 else 0
        avg_time = interactive_elapsed / total_acted
        console.print(f"[bold]Avg time per PR:[/]     {_fmt_duration(avg_time)}")
        console.print(f"[bold]Velocity:[/]            {velocity_hr:.1f} PRs/hr")
    console.print()

    # Force exit — background threads (GitHub API, LLM) can hang indefinitely
    import os as _os

    _os._exit(0)
