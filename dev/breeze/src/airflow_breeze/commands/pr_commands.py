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
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
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
from airflow_breeze.utils.console import get_console
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
    unresolved_review_comments: int  # count of unresolved review threads from maintainers


@dataclass
class StaleReviewInfo:
    """Info about a CHANGES_REQUESTED review that may need a follow-up nudge."""

    reviewer_login: str
    review_date: str  # ISO 8601
    author_pinged_reviewer: bool  # whether the author mentioned the reviewer after the review


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
        get_console().print(f"[error]GraphQL request failed: {response.status_code} {response.text}[/]")
        sys.exit(1)
    result = response.json()
    if "errors" in result:
        get_console().print(f"[error]GraphQL errors: {result['errors']}[/]")
        sys.exit(1)
    return result["data"]


_CHECK_FAILURE_CONCLUSIONS = {"FAILURE", "TIMED_OUT", "ACTION_REQUIRED"}
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
    """Fetch unresolved review thread counts for PRs in chunked GraphQL queries.

    Counts only threads started by collaborators/members/owners (i.e. maintainers).
    Updates each PR's unresolved_review_comments in-place.
    """
    owner, repo = github_repository.split("/", 1)
    if not prs:
        return

    for chunk_start in range(0, len(prs), _REVIEW_THREADS_BATCH_SIZE):
        chunk = prs[chunk_start : chunk_start + _REVIEW_THREADS_BATCH_SIZE]

        pr_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            pr_fields.append(
                f"    {alias}: pullRequest(number: {pr.number}) {{\n"
                f"      reviewThreads(first: 100) {{\n"
                f"        nodes {{\n"
                f"          isResolved\n"
                f"          comments(first: 1) {{\n"
                f"            nodes {{\n"
                f"              author {{ login }}\n"
                f"              authorAssociation\n"
                f"            }}\n"
                f"          }}\n"
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
            unresolved = 0
            for thread in threads:
                if thread.get("isResolved"):
                    continue
                # Only count threads started by maintainers (collaborators/members/owners)
                comments = thread.get("comments", {}).get("nodes", [])
                if comments:
                    assoc = comments[0].get("authorAssociation", "NONE")
                    if assoc in _COLLABORATOR_ASSOCIATIONS:
                        unresolved += 1
            pr.unresolved_review_comments = unresolved


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
        get_console().print(f"[info]Searching PRs: {search_query}[/]")

    variables: dict = {"query": search_query, "first": batch_size}
    if after_cursor:
        variables["after"] = after_cursor

    data = _graphql_request(token, _SEARCH_PRS_QUERY, variables)
    search_data = data["search"]
    page_info = search_data.get("pageInfo", {})
    has_next_page = page_info.get("hasNextPage", False)
    end_cursor = page_info.get("endCursor")

    get_console().print(
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
                unresolved_review_comments=0,
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
        get_console().print(f"[error]PR #{pr_number} not found in {github_repository}.[/]")
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
        unresolved_review_comments=0,
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
        get_console().print(
            f"[warning]Label '{label_name}' not found in {github_repository}. Skipping label.[/]"
        )
        return False
    try:
        _graphql_request(token, _ADD_LABELS_MUTATION, {"labelableId": pr_node_id, "labelIds": [label_id]})
        return True
    except SystemExit:
        return False


def _load_labels_from_boring_cyborg() -> list[str]:
    """Read labels from .github/boring-cyborg.yml."""
    from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

    boring_cyborg_path = AIRFLOW_ROOT_PATH / ".github" / "boring-cyborg.yml"
    if not boring_cyborg_path.exists():
        get_console().print("[warning]boring-cyborg.yml not found, label validation disabled.[/]")
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

    if comment_only:
        return (
            f"@{pr_author} This PR has a few issues that need to be addressed before it can be "
            f"reviewed — please see our {QUALITY_CRITERIA_LINK}.\n\n"
            f"**Issues found:**\n{violations_text}{rebase_note}\n\n"
            f"**What to do next:**\n{what_to_do}\n\n"
            "Please address the issues above and push again. "
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
        "If you have questions, feel free to ask on the "
        "[Airflow Slack](https://s.apache.org/airflow-slack)."
    )


def _compute_default_action(
    pr: PRData, assessment, author_flagged_count: dict[str, int]
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

    has_unresolved_comments = pr.unresolved_review_comments > 0
    if has_unresolved_comments and not any("unresolved" in p for p in reason_parts):
        reason_parts.append(
            f"{pr.unresolved_review_comments} unresolved review comment{'s' if pr.unresolved_review_comments != 1 else ''}"
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


def _pr_link(pr: PRData) -> str:
    """Return a Rich-markup clickable link for a PR: [link=url]#number[/link]."""
    return f"[link={pr.url}]#{pr.number}[/link]"


def _display_pr_info_panels(pr: PRData, author_profile: dict | None):
    """Display PR info and author panels (shared by flagged-PR and workflow-approval flows)."""
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
            get_console().print(f"{msg}[/]")
            continue
        if not assessment.should_flag:
            llm_passing.append(pr)
            get_console().print(f"  [success]PR {_pr_link(pr)} passes LLM quality check.[/]")
            continue
        llm_assessments[pr.number] = assessment
        if assessment.should_report:
            get_console().print(
                f"  [yellow]PR {_pr_link(pr)} potentially flagged for reporting to GitHub.[/yellow]"
            )


@dataclass
class TriageStats:
    """Mutable counters for triage actions taken during auto-triage."""

    total_converted: int = 0
    total_commented: int = 0
    total_closed: int = 0
    total_ready: int = 0
    total_rerun: int = 0
    total_review_nudges: int = 0
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
            get_console().print(progress)


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
    get_console().print(prompt, end="")

    try:
        ch = _read_char()
    except (KeyboardInterrupt, EOFError):
        get_console().print()
        get_console().print(f"  [info]Cancelled — no changes made to PR {_pr_link(pr)}.[/]")
        return False

    # Ignore multi-byte escape sequences (arrow keys, etc.)
    if len(ch) > 1:
        get_console().print()
        get_console().print(f"  [info]Cancelled — no changes made to PR {_pr_link(pr)}.[/]")
        return False

    # Echo the character and move to next line
    get_console().print(ch)

    if ch.upper() in ("Y", "\r", "\n", ""):
        return True
    get_console().print(f"  [info]Cancelled — no changes made to PR {_pr_link(pr)}.[/]")
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
        get_console().print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
        stats.total_skipped_action += 1
        return

    if action == TriageAction.READY:
        if not _confirm_action(pr, "Add 'ready for maintainer review' label", ctx.answer_triage):
            stats.total_skipped_action += 1
            return
        get_console().print(
            f"  [info]Marking PR {_pr_link(pr)} as ready — adding '{_READY_FOR_REVIEW_LABEL}' label.[/]"
        )
        if _add_label(ctx.token, ctx.github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
            get_console().print(
                f"  [success]Label '{_READY_FOR_REVIEW_LABEL}' added to PR {_pr_link(pr)}.[/]"
            )
            stats.total_ready += 1
        else:
            get_console().print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
        return

    if action == TriageAction.RERUN:
        if pr.head_sha and pr.failed_checks:
            get_console().print(
                f"  Rerunning {len(pr.failed_checks)} failed "
                f"{'checks' if len(pr.failed_checks) != 1 else 'check'} for PR {_pr_link(pr)}..."
            )
            rerun_count = _rerun_failed_workflow_runs(
                ctx.token, ctx.github_repository, pr.head_sha, pr.failed_checks
            )
            if rerun_count:
                get_console().print(
                    f"  [success]Rerun triggered for {rerun_count} workflow "
                    f"{'runs' if rerun_count != 1 else 'run'} on PR {_pr_link(pr)}.[/]"
                )
                stats.total_rerun += 1
            else:
                # No completed failed runs to rerun — check if workflows are still running
                get_console().print(
                    f"  [warning]No completed failed runs found for PR {_pr_link(pr)}. "
                    f"Checking for in-progress workflows...[/]"
                )
                restarted = _cancel_and_rerun_in_progress_workflows(
                    ctx.token, ctx.github_repository, pr.head_sha
                )
                if restarted:
                    get_console().print(
                        f"  [success]Cancelled and restarted {restarted} workflow "
                        f"{'runs' if restarted != 1 else 'run'} on PR {_pr_link(pr)}.[/]"
                    )
                    stats.total_rerun += 1
                else:
                    get_console().print(
                        f"  [warning]Could not rerun any workflow runs for PR {_pr_link(pr)}.[/]"
                    )
        else:
            get_console().print(f"  [warning]No failed checks to rerun for PR {_pr_link(pr)}.[/]")
        return

    if action == TriageAction.COMMENT:
        if not _confirm_action(pr, "Post comment", ctx.answer_triage):
            stats.total_skipped_action += 1
            return
        text = comment_only_text or draft_comment
        get_console().print(f"  Posting comment on PR {_pr_link(pr)}...")
        if _post_comment(ctx.token, pr.node_id, text):
            get_console().print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
            stats.total_commented += 1
        else:
            get_console().print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
        return

    if action == TriageAction.DRAFT:
        if not _confirm_action(pr, "Convert to draft and post comment", ctx.answer_triage):
            stats.total_skipped_action += 1
            return
        get_console().print(f"  Converting PR {_pr_link(pr)} to draft...")
        if _convert_pr_to_draft(ctx.token, pr.node_id):
            get_console().print(f"  [success]PR {_pr_link(pr)} converted to draft.[/]")
        else:
            get_console().print(f"  [error]Failed to convert PR {_pr_link(pr)} to draft.[/]")
            return
        get_console().print(f"  Posting comment on PR {_pr_link(pr)}...")
        if _post_comment(ctx.token, pr.node_id, draft_comment):
            get_console().print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
            stats.total_converted += 1
        else:
            get_console().print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
        return

    if action == TriageAction.CLOSE:
        if not _confirm_action(pr, "Close PR and post comment", ctx.answer_triage):
            stats.total_skipped_action += 1
            return
        get_console().print(f"  Closing PR {_pr_link(pr)}...")
        if _close_pr(ctx.token, pr.node_id):
            get_console().print(f"  [success]PR {_pr_link(pr)} closed.[/]")
        else:
            get_console().print(f"  [error]Failed to close PR {_pr_link(pr)}.[/]")
            return
        if _add_label(ctx.token, ctx.github_repository, pr.node_id, _CLOSED_QUALITY_LABEL):
            get_console().print(f"  [success]Label '{_CLOSED_QUALITY_LABEL}' added to PR {_pr_link(pr)}.[/]")
        else:
            get_console().print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
        get_console().print(f"  Posting comment on PR {_pr_link(pr)}...")
        if _post_comment(ctx.token, pr.node_id, close_comment):
            get_console().print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
            stats.total_closed += 1
        else:
            get_console().print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")


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

    default_action, reason = _compute_default_action(pr, assessment, ctx.author_flagged_count)
    # If PR is already a draft, don't offer converting to draft — use comment instead
    exclude_actions: set[TriageAction] | None = None
    if pr.is_draft and default_action == TriageAction.DRAFT:
        default_action = TriageAction.COMMENT
        reason = reason.replace("draft", "comment (already draft)")
        exclude_actions = {TriageAction.DRAFT}
    elif pr.is_draft:
        exclude_actions = {TriageAction.DRAFT}

    get_console().print(f"  [bold]{reason}[/]")

    if ctx.dry_run:
        action_label = {
            TriageAction.DRAFT: "draft",
            TriageAction.COMMENT: "comment",
            TriageAction.CLOSE: "close",
            TriageAction.RERUN: "rerun checks",
            TriageAction.READY: "mark as ready",
            TriageAction.SKIP: "skip",
        }.get(default_action, str(default_action))
        get_console().print(f"[warning]Dry run — would default to: {action_label}[/]")
        return

    action = prompt_triage_action(
        f"Action for PR {_pr_link(pr)}?",
        default=default_action,
        forced_answer=ctx.answer_triage,
        exclude=exclude_actions,
        pr_url=pr.url,
    )

    if action == TriageAction.QUIT:
        get_console().print("[warning]Quitting.[/]")
        ctx.stats.quit_early = True
        return

    # If user takes action on a should_report PR (anything other than skip),
    # downgrade it from "report" to regular "flagged" — user has reviewed and decided.
    if action != TriageAction.SKIP and getattr(assessment, "should_report", False):
        assessment.should_report = False
        get_console().print("  [info]Report status cleared — PR marked as flagged.[/]")

    # For actions that post comments, let the user select violations and preview the comment
    draft_comment = ""
    close_comment = ""
    if action in (TriageAction.DRAFT, TriageAction.COMMENT, TriageAction.CLOSE):
        selected = _select_violations(assessment.violations)
        if selected is not None and len(selected) == 0:
            get_console().print("  [info]No violations selected — skipping.[/]")
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
                get_console().print(Panel(close_comment, title="Comment to be posted", border_style="red"))
            elif action == TriageAction.COMMENT:
                get_console().print(
                    Panel(comment_only_text, title="Comment to be posted", border_style="green")
                )
            else:
                get_console().print(Panel(draft_comment, title="Comment to be posted", border_style="green"))

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
    get_console().print(pr_table)
    get_console().print(
        "  Triage: [green]Ready for review[/] = ready for maintainer review  "
        "[yellow]Waiting for Author[/] = triaged, no response  "
        "[bright_cyan]Responded[/] = author replied  "
        "[blue]-[/] = not yet triaged"
    )
    if collab_count:
        get_console().print(
            f"  [dim]({collab_count} collaborator/member {'PRs' if collab_count != 1 else 'PR'} not shown)[/]"
        )
    get_console().print()


def _filter_candidate_prs(
    all_prs: list[PRData],
    *,
    include_collaborators: bool,
    include_drafts: bool,
    checks_state: str,
    min_commits_behind: int,
    max_num: int,
) -> tuple[list[PRData], list[PRData], int, int, int]:
    """Filter PRs to candidates. Returns (candidates, accepted_prs, skipped_collaborator, skipped_bot, skipped_accepted)."""
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
                get_console().print(f"  [dim]Skipping PR {_pr_link(pr)} — draft PR[/]")
        elif not include_collaborators and pr.author_association in _COLLABORATOR_ASSOCIATIONS:
            total_skipped_collaborator += 1
            if verbose:
                get_console().print(
                    f"  [dim]Skipping PR {_pr_link(pr)} by "
                    f"{pr.author_association.lower()} {pr.author_login}[/]"
                )
        elif _is_bot_account(pr.author_login):
            total_skipped_bot += 1
            if verbose:
                get_console().print(f"  [dim]Skipping PR {_pr_link(pr)} — bot account {pr.author_login}[/]")
        elif _READY_FOR_REVIEW_LABEL in pr.labels:
            total_skipped_accepted += 1
            accepted_prs.append(pr)
            if verbose:
                get_console().print(
                    f"  [dim]Skipping PR {_pr_link(pr)} — already has '{_READY_FOR_REVIEW_LABEL}' label[/]"
                )
        elif (
            checks_state != "any"
            and pr.checks_state not in ("NOT_RUN",)
            and pr.checks_state.lower() != checks_state
        ):
            total_skipped_checks_state += 1
            if verbose:
                get_console().print(
                    f"  [dim]Skipping PR {_pr_link(pr)} — checks state {pr.checks_state} != {checks_state}[/]"
                )
        elif min_commits_behind > 0 and pr.commits_behind < min_commits_behind:
            total_skipped_commits_behind += 1
            if verbose:
                get_console().print(
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
    get_console().print(
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

    get_console().print(
        f"[info]Fetching check details for {len(candidate_prs)} "
        f"candidate {'PRs' if len(candidate_prs) != 1 else 'PR'}...[/]"
    )
    _fetch_check_details_batch(token, github_repository, candidate_prs)

    for pr in candidate_prs:
        if pr.checks_state == "FAILURE" and not pr.failed_checks and pr.head_sha:
            get_console().print(
                f"  [dim]Fetching full check details for PR {_pr_link(pr)} "
                f"(failures beyond first 100 checks)...[/]"
            )
            pr.failed_checks = _fetch_failed_checks(token, github_repository, pr.head_sha)

    unknown_count = sum(1 for pr in candidate_prs if pr.mergeable == "UNKNOWN")
    if unknown_count:
        get_console().print(
            f"[info]Resolving merge conflict status for {unknown_count} "
            f"{'PRs' if unknown_count != 1 else 'PR'} with unknown status...[/]"
        )
        resolved = _resolve_unknown_mergeable(token, github_repository, candidate_prs)
        remaining = unknown_count - resolved
        if remaining:
            get_console().print(
                f"  [dim]{resolved} resolved, {remaining} still unknown "
                f"(GitHub hasn't computed mergeability yet).[/]"
            )
        else:
            get_console().print(f"  [dim]All {resolved} resolved.[/]")

    if run_api:
        get_console().print(
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
    get_console().print(
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
                get_console().print(
                    f"  [dim]Skipping PR {_pr_link(pr)} — checks still running "
                    f"({_format_check_status_counts(check_counts)})[/]"
                )
                continue

        _display_workflow_approval_panel(pr, author_profile, pending_runs, check_counts)

        # If author exceeds the close threshold, suggest closing instead of approving
        author_count = ctx.author_flagged_count.get(pr.author_login, 0)
        if author_count > 3:
            get_console().print(
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
            get_console().print(Panel(close_comment, title="Proposed close comment", border_style="red"))

            if ctx.dry_run:
                get_console().print("[warning]Dry run — would default to: close[/]")
                continue

            action = prompt_triage_action(
                f"Action for PR {_pr_link(pr)}?",
                default=TriageAction.CLOSE,
                forced_answer=ctx.answer_triage,
                exclude={TriageAction.DRAFT} if pr.is_draft else None,
                pr_url=pr.url,
            )
            if action == TriageAction.QUIT:
                get_console().print("[warning]Quitting.[/]")
                ctx.stats.quit_early = True
                return
            if action == TriageAction.SKIP:
                get_console().print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
                continue
            if action == TriageAction.CLOSE:
                _execute_triage_action(
                    ctx, pr, TriageAction.CLOSE, draft_comment="", close_comment=close_comment
                )
                continue
            # For DRAFT or READY, fall through to normal workflow approval

        if ctx.dry_run:
            get_console().print("[warning]Dry run — skipping workflow approval.[/]")
            continue

        if not pending_runs:
            # No pending workflow runs — try to rerun completed workflows first.
            # If no workflows exist at all, fall back to rebase (or close/reopen).
            get_console().print(
                f"  [info]No pending workflow runs for PR {_pr_link(pr)}. "
                f"Attempting to rerun completed workflows...[/]"
            )
            default_action = TriageAction.RERUN
            get_console().print("  [bold]No pending runs — suggesting rerun checks[/]")

            action = prompt_triage_action(
                f"Action for PR {_pr_link(pr)}?",
                default=default_action,
                forced_answer=ctx.answer_triage,
                exclude={TriageAction.DRAFT} if pr.is_draft else None,
                pr_url=pr.url,
            )
            if action == TriageAction.QUIT:
                get_console().print("[warning]Quitting.[/]")
                ctx.stats.quit_early = True
                return
            if action == TriageAction.SKIP:
                get_console().print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
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
                                get_console().print(
                                    f"  [success]Rerun triggered for: {run.get('name', run['id'])}[/]"
                                )
                                rerun_count += 1

                if rerun_count:
                    get_console().print(
                        f"  [success]Rerun triggered for {rerun_count} workflow "
                        f"{'runs' if rerun_count != 1 else 'run'} on PR {_pr_link(pr)}.[/]"
                    )
                    ctx.stats.total_rerun += 1
                else:
                    # No workflows to rerun — need rebase or close/reopen to trigger CI
                    get_console().print(
                        f"  [warning]No workflow runs found to rerun for PR {_pr_link(pr)}.[/]"
                    )
                    if pr.mergeable == "CONFLICTING":
                        get_console().print("  [warning]PR has merge conflicts — suggesting close/reopen.[/]")
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
                        get_console().print("  [info]Suggesting rebase to trigger CI workflows.[/]")
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
                    get_console().print(
                        Panel(rebase_comment, title="Proposed rebase comment", border_style="yellow")
                    )
                    fallback_action = TriageAction.DRAFT if not pr.is_draft else TriageAction.COMMENT
                    fallback = prompt_triage_action(
                        f"Rerun failed — action for PR {_pr_link(pr)}?",
                        default=fallback_action,
                        forced_answer=ctx.answer_triage,
                        exclude={TriageAction.DRAFT} if pr.is_draft else None,
                        pr_url=pr.url,
                    )
                    if fallback == TriageAction.QUIT:
                        get_console().print("[warning]Quitting.[/]")
                        ctx.stats.quit_early = True
                        return
                    if fallback == TriageAction.SKIP:
                        get_console().print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
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
            get_console().print("[warning]Quitting.[/]")
            ctx.stats.quit_early = True
            return
        if answer == Answer.NO:
            get_console().print(f"  [info]Skipping workflow approval for PR {_pr_link(pr)}.[/]")
            continue

        has_sensitive_changes = False
        get_console().print(f"  Fetching diff for PR {_pr_link(pr)}...")
        diff_text = _fetch_pr_diff(ctx.token, ctx.github_repository, pr.number)
        if diff_text:
            from rich.syntax import Syntax

            get_console().print(
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
                get_console().print()
                get_console().print(
                    "[bold red]WARNING: This PR contains changes to sensitive files "
                    "— please review carefully![/]"
                )
                for f in sensitive_files:
                    get_console().print(f"  [bold red]  - {f}[/]")
                get_console().print()
        else:
            get_console().print(
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
            get_console().print("[warning]Quitting.[/]")
            ctx.stats.quit_early = True
            return
        if answer == Answer.NO:
            get_console().print(
                f"\n  [bold red]Suspicious changes detected in PR {_pr_link(pr)} by {pr.author_login}.[/]"
            )
            get_console().print(f"  Fetching all open PRs by {pr.author_login}...")
            author_prs = _fetch_author_open_prs(ctx.token, ctx.github_repository, pr.author_login)
            if not author_prs:
                get_console().print(f"  [dim]No open PRs found for {pr.author_login}.[/]")
                continue

            get_console().print()
            get_console().print(
                f"  [bold red]The following {len(author_prs)} "
                f"{'PRs' if len(author_prs) != 1 else 'PR'} by "
                f"{pr.author_login} will be closed, labeled "
                f"'{_SUSPICIOUS_CHANGES_LABEL}', and commented:[/]"
            )
            for pr_info in author_prs:
                get_console().print(
                    f"    - [link={pr_info['url']}]#{pr_info['number']}[/link] {pr_info['title']}"
                )
            get_console().print()

            confirm = user_confirm(
                f"Close all {len(author_prs)} {'PRs' if len(author_prs) != 1 else 'PR'} "
                f"by {pr.author_login} and label as suspicious?",
                forced_answer=ctx.answer_triage,
            )
            if confirm == Answer.QUIT:
                get_console().print("[warning]Quitting.[/]")
                ctx.stats.quit_early = True
                return
            if confirm == Answer.NO:
                get_console().print(f"  [info]Skipping — no PRs closed for {pr.author_login}.[/]")
                continue

            closed, commented = _close_suspicious_prs(ctx.token, ctx.github_repository, author_prs, pr.number)
            get_console().print(
                f"  [success]Closed {closed}/{len(author_prs)} "
                f"{'PRs' if len(author_prs) != 1 else 'PR'}, commented on {commented}.[/]"
            )
            ctx.stats.total_closed += closed
            continue

        approved = _approve_workflow_runs(ctx.token, ctx.github_repository, pending_runs)
        if approved:
            get_console().print(
                f"  [success]Approved {approved}/{len(pending_runs)} workflow "
                f"{'runs' if len(pending_runs) != 1 else 'run'} for PR "
                f"{_pr_link(pr)}.[/]"
            )
            ctx.stats.total_workflows_approved += 1
        else:
            get_console().print(f"  [error]Failed to approve workflow runs for PR {_pr_link(pr)}.[/]")
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
            get_console().print(Panel(rebase_comment, title="Proposed rebase comment", border_style="yellow"))
            action = prompt_triage_action(
                f"Action for PR {_pr_link(pr)}?",
                default=default_action,
                forced_answer=ctx.answer_triage,
                exclude=exclude_actions,
                pr_url=pr.url,
            )
            if action == TriageAction.QUIT:
                get_console().print("[warning]Quitting.[/]")
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

    get_console().print(
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
            get_console().print()
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
            get_console().print(
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

        get_console().print(
            f"[dim]Waiting for {len(ctx.llm_future_to_pr) - len(ctx.llm_completed)} "
            f"remaining LLM "
            f"{'assessments' if len(ctx.llm_future_to_pr) - len(ctx.llm_completed) != 1 else 'assessment'}"
            f"...[/]"
        )
        time.sleep(2)
        _collect_llm_results(
            ctx.llm_future_to_pr, ctx.llm_assessments, ctx.llm_completed, ctx.llm_errors, ctx.llm_passing
        )

    get_console().print(
        f"\n[info]LLM assessment complete: {len(ctx.llm_assessments)} flagged, "
        f"{len(ctx.llm_passing)} passed, {len(ctx.llm_errors)} errors "
        f"(out of {len(ctx.llm_future_to_pr)} assessed).[/]\n"
    )


def _review_passing_prs(ctx: TriageContext, passing_prs: list[PRData]) -> None:
    """Present passing PRs for optional ready-for-review marking. Mutates ctx.stats."""
    if ctx.stats.quit_early or not passing_prs:
        return

    passing_prs.sort(key=lambda p: (p.author_login.lower(), p.number))
    get_console().print(
        f"\n[info]{len(passing_prs)} {'PRs pass' if len(passing_prs) != 1 else 'PR passes'} "
        f"all checks — review to mark as ready:[/]\n"
    )
    for pr in passing_prs:
        author_profile = _fetch_author_profile(ctx.token, pr.author_login, ctx.github_repository)
        _display_pr_info_panels(pr, author_profile)
        get_console().print("[success]This looks like a PR that is ready for review.[/]")

        if ctx.dry_run:
            get_console().print("[warning]Dry run — skipping.[/]")
            continue

        action = prompt_triage_action(
            f"Action for PR {_pr_link(pr)}?",
            default=TriageAction.READY,
            forced_answer=ctx.answer_triage,
            exclude={TriageAction.DRAFT} if pr.is_draft else None,
            pr_url=pr.url,
        )

        if action == TriageAction.QUIT:
            get_console().print("[warning]Quitting.[/]")
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
                        get_console().print(
                            f"  [success]PR {_pr_link(pr)} marked as ready for review (undrafted).[/]"
                        )
                    else:
                        get_console().print(f"  [warning]Failed to undraft PR {_pr_link(pr)}.[/]")
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
                get_console().print(
                    f"  [info]Adding '{_READY_FOR_REVIEW_LABEL}' label to PR {_pr_link(pr)}.[/]"
                )
                if _add_label(ctx.token, ctx.github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
                    get_console().print(
                        f"  [success]Label '{_READY_FOR_REVIEW_LABEL}' added to PR {_pr_link(pr)}.[/]"
                    )
                    ctx.stats.total_ready += 1
                else:
                    get_console().print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
            else:
                ctx.stats.total_skipped_action += 1
        else:
            get_console().print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
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

    get_console().print(
        f"[info]Checking {len(passing_accepted)} accepted "
        f"{'PRs' if len(passing_accepted) != 1 else 'PR'} for stale review requests...[/]"
    )
    stale_data = _fetch_stale_review_data_batch(ctx.token, ctx.github_repository, passing_accepted)
    if not stale_data:
        get_console().print("  [dim]No stale review requests found.[/]")
        return

    stale_prs = [pr for pr in passing_accepted if pr.number in stale_data]
    stale_prs.sort(key=lambda p: (p.author_login.lower(), p.number))

    get_console().print(
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
            get_console().print("[warning]Dry run — skipping.[/]")
            continue

        action = prompt_triage_action(
            f"Action for PR {_pr_link(pr)}?",
            default=TriageAction.COMMENT,
            forced_answer=ctx.answer_triage,
            exclude={TriageAction.DRAFT, TriageAction.CLOSE, TriageAction.RERUN},
            pr_url=pr.url,
        )

        if action == TriageAction.QUIT:
            get_console().print("[warning]Quitting.[/]")
            ctx.stats.quit_early = True
            return

        if action == TriageAction.COMMENT:
            if _confirm_action(pr, "Post review nudge comment", ctx.answer_triage):
                if _post_comment(ctx.token, pr.node_id, comment):
                    get_console().print(f"  [success]Comment posted on PR {_pr_link(pr)}.[/]")
                    ctx.stats.total_review_nudges += 1
                else:
                    get_console().print(f"  [error]Failed to post comment on PR {_pr_link(pr)}.[/]")
            else:
                ctx.stats.total_skipped_action += 1
        elif action == TriageAction.READY:
            if _confirm_action(pr, "Add 'ready for maintainer review' label", ctx.answer_triage):
                if _add_label(ctx.token, ctx.github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
                    get_console().print(
                        f"  [success]Label '{_READY_FOR_REVIEW_LABEL}' added to PR {_pr_link(pr)}.[/]"
                    )
                    ctx.stats.total_ready += 1
                else:
                    get_console().print(f"  [warning]Failed to add label to PR {_pr_link(pr)}.[/]")
            else:
                ctx.stats.total_skipped_action += 1
        else:
            get_console().print(f"  [info]Skipping PR {_pr_link(pr)} — no action taken.[/]")
            ctx.stats.total_skipped_action += 1


def _display_triage_summary(
    all_prs: list[PRData],
    candidate_prs: list[PRData],
    passing_prs: list[PRData],
    pending_approval: list[PRData],
    workflows_in_progress: list[PRData],
    skipped_drafts: list[PRData],
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

    get_console().print(
        f"\n[info]Assessment complete: {total_flagged} {'PRs' if total_flagged != 1 else 'PR'} "
        f"flagged ({total_deterministic_flags} CI/conflicts/comments, "
        f"{total_llm_flagged} LLM-flagged"
        f"{f', {total_llm_errors} LLM errors' if total_llm_errors else ''}"
        f"{f', {len(pending_approval)} awaiting workflow approval' if pending_approval else ''}"
        f"{f', {len(workflows_in_progress)} workflows in progress' if workflows_in_progress else ''}"
        f"{f', {len(skipped_drafts)} drafts with issues skipped' if skipped_drafts else ''}"
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
    summary_table.add_row("PRs converted to draft", str(stats.total_converted))
    summary_table.add_row("PRs commented (not drafted)", str(stats.total_commented))
    summary_table.add_row("PRs closed", str(stats.total_closed))
    summary_table.add_row("PRs with checks rerun", str(stats.total_rerun))
    summary_table.add_row("Review follow-up nudges", str(stats.total_review_nudges))
    summary_table.add_row("PRs marked ready for review", str(stats.total_ready))
    summary_table.add_row("PRs skipped (no action)", str(stats.total_skipped_action))
    summary_table.add_row("Awaiting workflow approval", str(len(pending_approval)))
    summary_table.add_row("Workflows in progress (skipped)", str(len(workflows_in_progress)))
    summary_table.add_row("PRs with workflows approved", str(stats.total_workflows_approved))
    get_console().print(summary_table)


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
            get_console().print(f"  [success]PR [link={pr_info['url']}]#{pr_num}[/link] closed.[/]")
            closed += 1
        else:
            get_console().print(f"  [error]Failed to close PR [link={pr_info['url']}]#{pr_num}[/link].[/]")
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
            get_console().print(
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
            get_console().print(f"  [success]Rerun triggered for: {run.get('name', run_id)}[/]")
            rerun_count += 1
        else:
            get_console().print(
                f"  [warning]Failed to rerun {run.get('name', run_id)}: {response.status_code}[/]"
            )
    return rerun_count


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
            get_console().print(f"  [info]Cancelled workflow run: {name}[/]")
            cancelled += 1
        else:
            get_console().print(f"  [warning]Failed to cancel: {name}[/]")

    if not cancelled:
        return 0

    # Brief pause to let GitHub process the cancellations
    get_console().print("  [dim]Waiting for cancellations to complete...[/]")
    time_mod.sleep(3)

    # Rerun the cancelled runs
    rerun_count = 0
    for run in in_progress:
        name = run.get("name", run["id"])
        if _rerun_workflow_run(token, github_repository, run):
            get_console().print(f"  [success]Rerun triggered for: {name}[/]")
            rerun_count += 1
        else:
            get_console().print(f"  [warning]Failed to rerun: {name}[/]")
    return rerun_count


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
    checks_state: str,
    min_commits_behind: int,
    my_reviews: bool,
    reviewers: tuple[str, ...],
    check_mode: str,
    llm_concurrency: int,
    llm_model: str,
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
        assess_pr,
        check_llm_cli_safety,
    )

    token = _resolve_github_token(github_token)
    if not token:
        get_console().print(
            "[error]GitHub token not found. Provide --github-token, "
            "set GITHUB_TOKEN, or authenticate with `gh auth login`.[/]"
        )
        sys.exit(1)

    run_api = check_mode in ("both", "api")
    run_llm = check_mode in ("both", "llm")

    console = get_console()
    mode_desc = {"both": "API + LLM", "api": "API only", "llm": "LLM only"}
    console.print(
        f"[info]Check mode: [bold]{check_mode}[/bold] ({mode_desc.get(check_mode, check_mode)}). "
        f"Change with --check-mode (api|llm|both).[/]"
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
        get_console().print("[error]--reviews-for-me and --reviews-for are mutually exclusive.[/]")
        sys.exit(1)

    # Resolve the authenticated user login (used for --reviews-for-me and triage comment detection)
    viewer_login = _resolve_viewer_login(token)

    # Refresh collaborators cache in the background on every run
    _refresh_collaborators_cache_in_background(token, github_repository)

    # Resolve review-requested filter: --reviews-for-me uses authenticated user, --reviews-for uses specified users
    review_requested_user: str | None = None
    review_requested_users: list[str] = []
    if my_reviews:
        review_requested_user = viewer_login
        review_requested_users = [viewer_login]
        get_console().print(f"[info]Filtering PRs with review requested for: {review_requested_user}[/]")
    elif reviewers:
        review_requested_users = list(reviewers)
        review_requested_user = reviewers[0]
        get_console().print(f"[info]Filtering PRs with review requested for: {', '.join(reviewers)}[/]")

    # Phase 1: Fetch PRs via GraphQL
    from fnmatch import fnmatch

    exact_labels = tuple(lbl for lbl in labels if "*" not in lbl and "?" not in lbl)
    wildcard_labels = [lbl for lbl in labels if "*" in lbl or "?" in lbl]
    exact_exclude_labels = tuple(lbl for lbl in exclude_labels if "*" not in lbl and "?" not in lbl)
    wildcard_exclude_labels = [lbl for lbl in exclude_labels if "*" in lbl or "?" in lbl]

    t_total_start = time.monotonic()

    # Phase 1: Lightweight fetch of PRs via GraphQL (no check contexts — fast)
    t_phase1_start = time.monotonic()
    has_next_page = False
    next_cursor: str | None = None
    if pr_number:
        get_console().print(f"[info]Fetching PR #{pr_number} via GraphQL...[/]")
        all_prs = [_fetch_single_pr_graphql(token, github_repository, pr_number)]
    elif len(review_requested_users) > 1:
        # Multiple reviewers: fetch PRs for each reviewer and merge (deduplicate)
        get_console().print("[info]Fetching PRs via GraphQL for multiple reviewers...[/]")
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
        get_console().print("[info]Fetching PRs via GraphQL...[/]")
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

    # Resolve how far behind base branch each PR is
    get_console().print("[info]Checking how far behind base branch each PR is...[/]")
    behind_map = _fetch_commits_behind_batch(token, github_repository, all_prs)
    for pr in all_prs:
        pr.commits_behind = behind_map.get(pr.number, 0)

    # Resolve UNKNOWN mergeable status before displaying the overview table
    unknown_count = sum(1 for pr in all_prs if pr.mergeable == "UNKNOWN")
    if unknown_count:
        get_console().print(
            f"[info]Resolving merge conflict status for {unknown_count} "
            f"{'PRs' if unknown_count != 1 else 'PR'} with unknown status...[/]"
        )
        resolved = _resolve_unknown_mergeable(token, github_repository, all_prs)
        remaining = unknown_count - resolved
        if remaining:
            get_console().print(
                f"  [dim]{resolved} resolved, {remaining} still unknown "
                f"(GitHub hasn't computed mergeability yet).[/]"
            )
        else:
            get_console().print(f"  [dim]All {resolved} resolved.[/]")

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
        get_console().print(
            f"[info]Verifying CI status for {len(non_collab_success)} "
            f"{'PRs' if len(non_collab_success) != 1 else 'PR'} "
            f"showing SUCCESS (checking for real test checks)...[/]"
        )
        _fetch_check_details_batch(token, github_repository, non_collab_success)
        reclassified = sum(1 for pr in non_collab_success if pr.checks_state == "NOT_RUN")
        if reclassified:
            get_console().print(
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
        )
    )

    # Exclude PRs that already have a triage comment posted after the last commit
    get_console().print(
        "[info]Checking for PRs already triaged (no new commits since last triage comment)...[/]"
    )
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
        get_console().print(
            f"[info]Skipped {len(already_triaged)} already-triaged "
            f"{'PRs' if len(already_triaged) != 1 else 'PR'} "
            f"({triaged_waiting_count} commented, "
            f"{triaged_responded_count} author responded).[/]"
        )
    else:
        get_console().print("  [dim]None found.[/]")

    # Display overview table (after triaged detection so we can mark actionable PRs)
    _display_pr_overview_table(
        all_prs,
        triaged_waiting_nums=triaged_classification["waiting"],
        triaged_responded_nums=triaged_classification["responded"],
    )

    t_phase1_end = time.monotonic()

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

            conflict_assessment = assess_pr_conflicts(pr.number, pr.mergeable, pr.base_ref, pr.commits_behind)
            comments_assessment = assess_pr_unresolved_comments(pr.number, pr.unresolved_review_comments)

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
        get_console().print(
            f"[info]Skipped {len(skipped_drafts)} draft "
            f"{'PRs' if len(skipped_drafts) != 1 else 'PR'} "
            f"with existing issues (CI failures, conflicts, or unresolved comments).[/]"
        )
    if workflows_in_progress:
        get_console().print(
            f"[info]Excluded {len(workflows_in_progress)} "
            f"{'PRs' if len(workflows_in_progress) != 1 else 'PR'} "
            f"with workflows already in progress.[/]"
        )

    # Filter out pending_approval PRs that already have a comment from the viewer
    # (triage or rebase comment) with no new commits since — no point re-approving
    if pending_approval and viewer_login:
        already_commented_nums = _find_already_triaged_prs(
            token, github_repository, pending_approval, viewer_login, require_marker=False
        )
        if already_commented_nums:
            already_triaged.extend(pr for pr in pending_approval if pr.number in already_commented_nums)
            pending_approval = [pr for pr in pending_approval if pr.number not in already_commented_nums]
            get_console().print(
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
            get_console().print("[success]No PRs with pending workflow approvals found.[/]")
            sys.exit(0)
        get_console().print(
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
            get_console().print(
                f"\n[info]--check-mode=api: skipping LLM assessment for {len(llm_candidates)} "
                f"{'PRs' if len(llm_candidates) != 1 else 'PR'}.[/]\n"
            )
            passing_prs.extend(llm_candidates)
    elif llm_candidates:
        skipped_detail = f"{total_deterministic_flags} CI/conflicts/comments"
        if skipped_drafts:
            skipped_detail += f", {len(skipped_drafts)} drafts with issues"
        if already_triaged:
            skipped_detail += f", {len(already_triaged)} already triaged"
        if pending_approval:
            skipped_detail += f", {len(pending_approval)} awaiting workflow approval"
        if workflows_in_progress:
            skipped_detail += f", {len(workflows_in_progress)} workflows in progress"
        get_console().print(
            f"\n[info]Starting LLM assessment for {len(llm_candidates)} "
            f"{'PRs' if len(llm_candidates) != 1 else 'PR'} in background "
            f"(skipped {skipped_detail})...[/]\n"
        )
        llm_executor = ThreadPoolExecutor(max_workers=llm_concurrency)
        llm_future_to_pr = {
            llm_executor.submit(
                assess_pr,
                pr_number=pr.number,
                pr_title=pr.title,
                pr_body=pr.body,
                check_status_summary=pr.check_summary,
                llm_model=llm_model,
            ): pr
            for pr in llm_candidates
        }

    # Build shared triage context and stats
    pr_actions: dict[int, str] = {}  # PR number -> action taken by user

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
        llm_future_to_pr=llm_future_to_pr,
        llm_assessments=llm_assessments,
        llm_completed=llm_completed,
        llm_errors=llm_errors,
        llm_passing=llm_passing,
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
        get_console().print("\n[warning]Interrupted — shutting down.[/]")
        stats.quit_early = True
    finally:
        # Shut down LLM executor if it was started
        if llm_executor is not None:
            llm_executor.shutdown(wait=False, cancel_futures=True)

    # Fetch and process next batch if available and user hasn't quit
    while has_next_page and not stats.quit_early and not pr_number:
        batch_num = getattr(stats, "_batch_count", 1) + 1
        stats._batch_count = batch_num  # type: ignore[attr-defined]
        get_console().print(f"\n[info]Batch complete. Fetching next batch (page {batch_num})...[/]\n")
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
            get_console().print("[info]No more PRs to process.[/]")
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
            get_console().print(
                f"[info]Verifying CI status for {len(batch_non_collab_success)} "
                f"{'PRs' if len(batch_non_collab_success) != 1 else 'PR'} "
                f"showing SUCCESS...[/]"
            )
            _fetch_check_details_batch(token, github_repository, batch_non_collab_success)
            reclassified = sum(1 for pr in batch_non_collab_success if pr.checks_state == "NOT_RUN")
            if reclassified:
                get_console().print(
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
            get_console().print("[info]No candidates in this batch.[/]")
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
            get_console().print("[info]All PRs in this batch already triaged.[/]")
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
                conflict_assessment = assess_pr_conflicts(
                    pr.number, pr.mergeable, pr.base_ref, pr.commits_behind
                )
                comments_assessment = assess_pr_unresolved_comments(pr.number, pr.unresolved_review_comments)
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
                    assess_pr,
                    pr_number=pr.number,
                    pr_title=pr.title,
                    pr_body=pr.body,
                    check_status_summary=pr.check_summary,
                    llm_model=llm_model,
                ): pr
                for pr in batch_llm_candidates
            }

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
            llm_future_to_pr=batch_llm_future_to_pr,
            llm_assessments=batch_llm_assessments,
            llm_completed=batch_llm_completed,
            llm_errors=batch_llm_errors,
            llm_passing=batch_llm_passing,
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
            get_console().print("\n[warning]Interrupted — shutting down.[/]")
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
    get_console().print()
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
    get_console().print(timing_table)

    if deterministic_timings:
        pr_titles = {pr.number: pr.title for pr in candidate_prs}
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
                f"#{pr_num}",
                title,
                result,
                action_display,
                _fmt_duration(fetch_per_pr),
                _fmt_duration(det_time) if det_time else "[dim]—[/]",
                _fmt_duration(total_time),
            )
        get_console().print(pr_timing_table)
