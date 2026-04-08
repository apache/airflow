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
"""Vault storage for PR triage data — persist across sessions to reduce API calls."""

from __future__ import annotations

import re

from airflow_breeze.utils.pr_cache import CacheStore

# ── PR metadata vault ────────────────────────────────────────────
# 4-hour TTL: PR metadata can change (labels, checks, mergeable status)
# but re-fetching every run is wasteful for PRs that haven't been updated.
_pr_vault = CacheStore("pr_vault", ttl_seconds=4 * 3600)

# Fields from PRData that are safe to serialize to JSON.
_VAULT_FIELDS = (
    "number",
    "title",
    "body",
    "url",
    "created_at",
    "updated_at",
    "node_id",
    "author_login",
    "author_association",
    "head_sha",
    "base_ref",
    "check_summary",
    "checks_state",
    "failed_checks",
    "commits_behind",
    "is_draft",
    "mergeable",
    "labels",
)


def save_pr(github_repository: str, pr) -> None:
    """Persist a PRData instance to the vault."""
    data = {field: getattr(pr, field) for field in _VAULT_FIELDS}
    _pr_vault.save(github_repository, f"pr_{pr.number}", data)


def load_pr(github_repository: str, pr_number: int, *, head_sha: str | None = None) -> dict | None:
    """Load a PR from the vault. Returns None when not cached, expired, or SHA mismatch."""
    match = {"head_sha": head_sha} if head_sha else None
    data = _pr_vault.get(github_repository, f"pr_{pr_number}", match=match)
    if data is not None:
        data.pop("cached_at", None)
    return data


def save_prs_batch(github_repository: str, prs) -> int:
    """Persist a batch of PRData instances. Returns count saved."""
    for pr in prs:
        save_pr(github_repository, pr)
    return len(prs)


# ── Check status vault ───────────────────────────────────────────
# Keyed by head_sha. Only caches fully-completed check results (no
# IN_PROGRESS or QUEUED). Completed results never change for the same SHA.
_check_vault = CacheStore("check_vault")

# Statuses that indicate checks are still running
_INCOMPLETE_STATUSES = {"IN_PROGRESS", "QUEUED", "PENDING"}


def save_check_status(github_repository: str, head_sha: str, counts: dict[str, int]) -> None:
    """Persist check status counts for a commit.

    Skips caching when any checks are still in progress — partial results
    would cause future sessions to see an incomplete picture.
    """
    if _INCOMPLETE_STATUSES & set(counts.keys()):
        return
    _check_vault.save(github_repository, f"checks_{head_sha}", {"head_sha": head_sha, "counts": counts})


def load_check_status(github_repository: str, head_sha: str) -> dict[str, int] | None:
    """Load cached check status counts for a commit. Returns None if not cached."""
    data = _check_vault.get(github_repository, f"checks_{head_sha}", match={"head_sha": head_sha})
    return data.get("counts") if data else None


# ── Workflow runs vault ──────────────────────────────────────────
# 10-minute TTL: workflow status changes frequently but not instantly.
_workflow_vault = CacheStore("workflow_vault", ttl_seconds=600)


def save_workflow_runs(github_repository: str, head_sha: str, status: str, runs: list[dict]) -> None:
    """Persist workflow runs for a commit + status combination."""
    _workflow_vault.save(
        github_repository,
        f"wf_{head_sha}_{status}",
        {"head_sha": head_sha, "status": status, "runs": runs},
    )


def load_workflow_runs(github_repository: str, head_sha: str, status: str) -> list[dict] | None:
    """Load cached workflow runs. Returns None if not cached or expired."""
    data = _workflow_vault.get(
        github_repository,
        f"wf_{head_sha}_{status}",
        match={"head_sha": head_sha, "status": status},
    )
    return data.get("runs") if data else None


# ── Directed review questions ────────────────────────────────────


def generate_review_questions(diff_text: str, pr_body: str) -> list[str]:
    """Generate verification questions from a PR diff and body.

    These are deterministic checks that don't require an LLM. They can be
    appended to the LLM prompt to focus the assessment on concrete issues.
    """
    questions: list[str] = []

    if not diff_text:
        return questions

    # Extract only added lines for content analysis (avoids false positives
    # from removed lines that contain keywords like "deprecated").
    added_lines = "\n".join(
        line[1:] for line in diff_text.splitlines() if line.startswith("+") and not line.startswith("+++")
    )

    # Count changes
    added = len(re.findall(r"^\+[^+]", diff_text, re.MULTILINE))
    removed = len(re.findall(r"^-[^-]", diff_text, re.MULTILINE))
    total = added + removed

    # Large PR warning
    if total > 500:
        questions.append(
            f"LARGE PR: {total} changed lines (+{added}/-{removed}). "
            f"Should this be split into smaller, focused PRs?"
        )

    # Source files without test changes
    src_files: set[str] = set()
    test_files: set[str] = set()
    for match in re.finditer(r"^diff --git a/(.+?) b/", diff_text, re.MULTILINE):
        path = match.group(1)
        if "test" in path.lower():
            test_files.add(path)
        elif path.endswith((".py", ".js", ".ts", ".java", ".go", ".rs")):
            src_files.add(path)
    if src_files and not test_files:
        questions.append(
            f"TEST COVERAGE: {len(src_files)} source file(s) modified but no test files changed. "
            f"Is test coverage needed?"
        )

    # Version fields referencing already-released versions (only in added lines)
    version_matches = re.findall(r"version_added:\s*[\"']?(\d+\.\d+\.\d+)", added_lines)
    if version_matches:
        questions.append(
            f"VERSION CHECK: version_added references {', '.join(set(version_matches))}. "
            f"Verify these are unreleased versions."
        )

    # Breaking change indicators (only in added lines to avoid false positives
    # from removed deprecation notices)
    breaking_signals = [
        "breaking",
        "backward",
        "deprecat",
        "behaviour change",
        "behavior change",
        "BREAKING CHANGE",
        "incompatible",
    ]
    added_lower = added_lines.lower()
    found_signals = [s for s in breaking_signals if s in added_lower]
    if found_signals:
        questions.append(
            "BREAKING CHANGE: This diff contains breaking change indicators "
            f"({', '.join(found_signals)}). Has this been discussed in an issue or "
            "on the mailing list?"
        )

    # Multiple exception types (only in added lines)
    exceptions = re.findall(r"raise (\w+(?:Error|Exception))\(", added_lines)
    unique_exceptions = set(exceptions)
    if len(unique_exceptions) > 3:
        questions.append(
            f"CONSISTENCY: {len(unique_exceptions)} different exception types raised "
            f"({', '.join(sorted(unique_exceptions)[:5])}). Should these be consolidated?"
        )

    return questions
