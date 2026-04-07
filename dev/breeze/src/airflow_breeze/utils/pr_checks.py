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
"""CI check analysis, log parsing, and failure detection for PR triage."""
from __future__ import annotations

import re
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass, field

from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.pr_display import human_readable_age
from airflow_breeze.utils.pr_github import graphql_request
from airflow_breeze.utils.pr_models import PRData, ReviewDecision

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHECK_FAILURE_CONCLUSIONS = {"FAILURE", "TIMED_OUT", "ACTION_REQUIRED", "CANCELLED", "STARTUP_FAILURE"}
STATUS_FAILURE_STATES = {"FAILURE", "ERROR"}
CHECK_DETAIL_BATCH_SIZE = 10
COMMITS_BEHIND_BATCH_SIZE = 20

TEST_WORKFLOW_PATTERNS = [
    "test", "static check", "build", "ci image", "prod image", "helm",
    "k8s", "basic", "unit", "integration", "provider", "mypy", "pre-commit", "docs",
]

STATIC_CHECK_PATTERNS = (
    "static check", "pre-commit", "prek", "lint", "mypy", "ruff", "black",
    "flake8", "pylint", "isort", "bandit", "codespell", "yamllint", "shellcheck",
)

LOG_SNIPPET_MAX_LINES = 30
LOG_DOWNLOAD_MAX_BYTES = 5 * 1024 * 1024  # 5 MB
LOG_TAIL_BYTES = 128 * 1024  # 128 KB
LOG_TAIL_LINES = 1000

CHECK_CONTEXTS_QUERY = """
query(: String!, : String!, : GitObjectID!, : Int!, : String) {
  repository(owner: , name: ) {
    object(oid: ) {
      ... on Commit {
        statusCheckRollup {
          contexts(first: , after: ) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes {
              ... on CheckRun { __typename name conclusion status }
              ... on StatusContext { __typename context state }
            }
          }
        }
      }
    }
  }
}
"""


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------

@dataclass
class RecentPRFailureInfo:
    """Preloaded information about CI failures in recently merged PRs."""
    failing_checks: dict[str, list[dict]]
    failing_check_names: set[str]
    prs_examined: int

    def find_matching_failures(self, pr_failed_checks: list[str]) -> list[str]:
        matched = []
        for check in pr_failed_checks:
            lower = check.lower()
            for recent_check in self.failing_check_names:
                if lower == recent_check.lower() or lower in recent_check.lower() or recent_check.lower() in lower:
                    matched.append(check)
                    break
        return matched


@dataclass
class LogSnippetInfo:
    """Log snippet from a failed CI check, with a link to the full log."""
    snippet: str
    job_url: str


# ---------------------------------------------------------------------------
# Check analysis functions
# ---------------------------------------------------------------------------

def is_test_check(name: str) -> bool:
    lower = name.lower()
    return any(p in lower for p in TEST_WORKFLOW_PATTERNS)


def extract_basic_check_info(pr_node: dict) -> tuple[str, str]:
    commits = pr_node.get("commits", {}).get("nodes", [])
    if not commits:
        return "", "UNKNOWN"
    commit = commits[0].get("commit", {})
    head_sha = commit.get("oid", "")
    rollup = commit.get("statusCheckRollup")
    if not rollup:
        return head_sha, "UNKNOWN"
    return head_sha, rollup.get("state", "UNKNOWN")


def extract_review_decisions(pr_node: dict) -> list[ReviewDecision]:
    reviews_nodes = (pr_node.get("reviews") or {}).get("nodes", [])
    latest: dict[str, tuple[str, str]] = {}
    for review in reviews_nodes:
        author = (review.get("author") or {}).get("login", "")
        state = review.get("state", "")
        association = review.get("authorAssociation", "")
        if author and state in ("APPROVED", "CHANGES_REQUESTED"):
            latest[author] = (state, association)
    return [
        ReviewDecision(reviewer_login=login, state=state, reviewer_association=association)
        for login, (state, association) in latest.items()
    ]


def process_check_contexts(contexts: list[dict], total_count: int) -> tuple[str, list[str], bool]:
    lines: list[str] = []
    failed: list[str] = []
    has_test_checks = False
    for ctx in contexts:
        typename = ctx.get("__typename")
        if typename == "CheckRun":
            name = ctx.get("name", "unknown")
            conclusion = ctx.get("conclusion") or ctx.get("status") or "unknown"
            lines.append(f"  {name}: {conclusion}")
            if is_test_check(name):
                has_test_checks = True
            if conclusion.upper() in CHECK_FAILURE_CONCLUSIONS:
                failed.append(name)
        elif typename == "StatusContext":
            name = ctx.get("context", "unknown")
            state = ctx.get("state", "unknown")
            lines.append(f"  {name}: {state}")
            if is_test_check(name):
                has_test_checks = True
            if state.upper() in STATUS_FAILURE_STATES:
                failed.append(name)
    if total_count > len(contexts):
        extra = total_count - len(contexts)
        lines.append(f"  ... ({extra} more {'checks' if extra != 1 else 'check'} not shown)")
    summary = chr(10).join(lines) if lines else "No check runs found."
    return summary, failed, has_test_checks


def are_only_static_check_failures(failed_checks: list[str]) -> bool:
    if not failed_checks:
        return False
    return all(any(pattern in check.lower() for pattern in STATIC_CHECK_PATTERNS) for check in failed_checks)


def format_check_status_counts(counts: dict[str, int]) -> str:
    _STATUS_COLORS = {
        "SUCCESS": "green", "FAILURE": "red", "TIMED_OUT": "red",
        "ACTION_REQUIRED": "yellow", "CANCELLED": "dim", "SKIPPED": "dim",
        "NEUTRAL": "dim", "STALE": "dim", "STARTUP_FAILURE": "red",
        "IN_PROGRESS": "bright_cyan", "QUEUED": "bright_cyan",
        "PENDING": "yellow", "ERROR": "red", "EXPECTED": "green",
    }
    if not counts:
        return "[dim]No checks found[/]"
    parts = []
    order = ["FAILURE", "TIMED_OUT", "ERROR", "STARTUP_FAILURE", "ACTION_REQUIRED",
             "IN_PROGRESS", "QUEUED", "PENDING", "SUCCESS", "EXPECTED",
             "CANCELLED", "SKIPPED", "NEUTRAL", "STALE"]
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


def has_running_checks(counts: dict[str, int]) -> bool:
    return any(counts.get(s, 0) > 0 for s in ("IN_PROGRESS", "QUEUED", "PENDING"))


def fetch_check_status_counts(token: str, github_repository: str, head_sha: str) -> dict[str, int]:
    owner, repo = github_repository.split("/", 1)
    counts: dict[str, int] = {}
    cursor = None
    while True:
        variables: dict = {"owner": owner, "repo": repo, "oid": head_sha, "first": 100}
        if cursor:
            variables["after"] = cursor
        data = graphql_request(token, CHECK_CONTEXTS_QUERY, variables)
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


def fetch_failed_checks(token: str, github_repository: str, head_sha: str) -> list[str]:
    owner, repo = github_repository.split("/", 1)
    failed: list[str] = []
    cursor = None
    while True:
        variables: dict = {"owner": owner, "repo": repo, "oid": head_sha, "first": 100}
        if cursor:
            variables["after"] = cursor
        data = graphql_request(token, CHECK_CONTEXTS_QUERY, variables)
        rollup = (data.get("repository", {}).get("object", {}) or {}).get("statusCheckRollup")
        if not rollup:
            break
        contexts_data = rollup.get("contexts", {})
        for ctx in contexts_data.get("nodes", []):
            typename = ctx.get("__typename")
            if typename == "CheckRun":
                conclusion = ctx.get("conclusion") or ctx.get("status") or "unknown"
                if conclusion.upper() in CHECK_FAILURE_CONCLUSIONS:
                    failed.append(ctx.get("name", "unknown"))
            elif typename == "StatusContext":
                state = ctx.get("state", "unknown")
                if state.upper() in STATUS_FAILURE_STATES:
                    failed.append(ctx.get("context", "unknown"))
        page_info = contexts_data.get("pageInfo", {})
        if not page_info.get("hasNextPage"):
            break
        cursor = page_info.get("endCursor")
    return failed


def fetch_check_details_batch(
    token: str, github_repository: str, prs: list[PRData],
    on_progress: Callable[[int, int], None] | None = None,
) -> None:
    owner, repo = github_repository.split("/", 1)
    eligible = [pr for pr in prs if pr.head_sha]
    if not eligible:
        return
    for chunk_start in range(0, len(eligible), CHECK_DETAIL_BATCH_SIZE):
        chunk = eligible[chunk_start : chunk_start + CHECK_DETAIL_BATCH_SIZE]
        object_fields = []
        for pr in chunk:
            alias = f"pr{pr.number}"
            field = (
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
            object_fields.append(field)
        query = (
            f'query {{\n  repository(owner: "{owner}", name: "{repo}") {{\n'
            + "\n".join(object_fields)
            + "\n  }\n}"
        )
        try:
            data = graphql_request(token, query, {})
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
            summary, failed, has_tests = process_check_contexts(contexts, total_count)
            if contexts and not has_tests:
                rollup_state = "NOT_RUN"
            pr.checks_state = rollup_state
            pr.check_summary = summary
            pr.failed_checks = failed
        if on_progress:
            on_progress(min(chunk_start + len(chunk), len(eligible)), len(eligible))

def are_failures_recent(
    token: str, github_repository: str, head_sha: str,
    grace_hours: int = 24,
) -> bool:
    from datetime import datetime, timezone
    from airflow_breeze.utils.pr_workflows import find_workflow_runs_by_status
    runs = find_workflow_runs_by_status(token, github_repository, head_sha, "completed")
    if not runs:
        return False
    now = datetime.now(timezone.utc)
    failed_runs = [r for r in runs if r.get("conclusion") == "failure"]
    if not failed_runs:
        return False
    for run in failed_runs:
        completed_at = run.get("completed_at", "")
        if not completed_at:
            return False
        try:
            completed_dt = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
            age_hours = (now - completed_dt).total_seconds() / 3600
            if age_hours > grace_hours:
                return False
        except (ValueError, TypeError):
            return False
    return True


def platform_from_labels(labels: list[str]) -> str:
    for label in labels:
        if "arm" in label.lower() or "aarch64" in label.lower():
            return "ARM"
    return "AMD"


# ---------------------------------------------------------------------------
# Log parsing functions
# ---------------------------------------------------------------------------

def extract_error_lines(log_content: str, step_name: str) -> str:
    lines = log_content.splitlines()
    if not lines:
        return ""
    _is_static_checks = step_name and any(
        kw in step_name.lower() for kw in ("static check", "pre-commit", "prek")
    )
    if _is_static_checks:
        snippet_lines = extract_static_check_errors(lines)
        return format_snippet(snippet_lines, step_name) if snippet_lines else ""
    error_markers = [
        "error:", "Error:", "ERROR:", "FAILED", "FAILURE", "fatal:", "Fatal:",
        "AssertionError", "Exception:", "Traceback (most recent call last)",
    ]
    error_indices: list[int] = []
    for i, line in enumerate(lines):
        stripped = line.lstrip()
        if any(marker in stripped for marker in error_markers):
            error_indices.append(i)
    if error_indices:
        last_error = error_indices[-1]
        first_error = error_indices[0]
        for i in range(len(error_indices) - 1, 0, -1):
            if error_indices[i] - error_indices[i - 1] > 20:
                first_error = error_indices[i]
                break
        start = max(0, first_error - 3)
        end = min(len(lines), last_error + LOG_SNIPPET_MAX_LINES - (last_error - first_error))
        snippet_lines = lines[start:end]
    else:
        snippet_lines = lines[-LOG_SNIPPET_MAX_LINES:]
    return format_snippet(snippet_lines, step_name)


def format_snippet(snippet_lines: list[str], step_name: str) -> str:
    cleaned = []
    for raw_line in snippet_lines:
        if len(raw_line) > 28 and raw_line[4] == "-" and raw_line[7] == "-" and raw_line[10] == "T":
            cleaned.append(raw_line[28:].lstrip())
        else:
            cleaned.append(raw_line)
    header = f"[Failed step: {step_name}]" if step_name else ""
    snippet = chr(10).join(cleaned).strip()
    if len(snippet) > 3000:
        snippet = snippet[:3000] + chr(10) + "... (truncated)"
    if header:
        return f"{header}\n{snippet}"
    return snippet


def extract_static_check_errors(lines: list[str]) -> list[str]:
    _ansi_re = re.compile(r"\[[0-9;]*m")
    _hook_result_re = re.compile(r"^(.+?)\.\.\..*\s*(Passed|Failed)\s*$")
    _NOISE_PATTERNS = (
        " TRACE ", " DEBUG ", " INFO ", "Resolved command:", "Found shebang:",
        "Resolved interpreter:", "Running ", "Executing `cd ", "hook returned exit code",
        "(no files to check)", "##[group]", "##[endgroup]", "##[error]",
        "- hook id:", "- duration:", "- exit code:", "- files were modified by this hook",
        "shell: /", "env:", "PYTHON_MAJOR_MINOR", "UPGRADE_TO_NEWER", "GITHUB_TOKEN:",
        "pythonLocation:", "PKG_CONFIG_PATH:", "Python_ROOT_DIR:", "Python2_ROOT_DIR:",
        "Python3_ROOT_DIR:", "LD_LIBRARY_PATH:", "tar-restored=", "Cache tarball",
        "Restored files", "du ~/",
    )
    _POST_FAIL_PATTERNS = ("- files were modified by this hook", "- exit code:")
    result: list[str] = []
    failed_hooks: list[str] = []
    current_hook_output: list[str] = []
    _capture_post_fail = False
    for line in lines:
        stripped = line
        if len(line) > 28 and line[4] == "-" and line[7] == "-" and line[10] == "T":
            stripped = line[28:].lstrip()
        clean = _ansi_re.sub("", stripped)
        hook_match = _hook_result_re.search(clean)
        if hook_match:
            hook_name = hook_match.group(1).strip().rstrip(".")
            status = hook_match.group(2)
            if status == "Failed":
                result.append(f"--- {hook_name}: FAILED ---")
                result.extend(current_hook_output)
                failed_hooks.append(hook_name)
                _capture_post_fail = True
            else:
                _capture_post_fail = False
            current_hook_output = []
            continue
        if _capture_post_fail:
            if any(pat in clean for pat in _POST_FAIL_PATTERNS):
                result.append(clean.strip().lstrip("- "))
                continue
            if clean.strip() and "- hook id:" not in clean and "- duration:" not in clean:
                _capture_post_fail = False
        if clean.endswith("Skipped"):
            current_hook_output = []
            continue
        if any(noise in stripped for noise in _NOISE_PATTERNS):
            continue
        if not stripped.strip():
            continue
        current_hook_output.append(stripped)
    if failed_hooks and len(result) <= len(failed_hooks):
        result.insert(0, f"Failed hooks: {', '.join(failed_hooks)}")
    if len(result) > LOG_SNIPPET_MAX_LINES * 2:
        result = result[: LOG_SNIPPET_MAX_LINES * 2]
        result.append("... (truncated)")
    return result
