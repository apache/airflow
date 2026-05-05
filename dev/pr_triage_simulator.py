#!/usr/bin/env python3
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
"""
pr-triage decision-table validation simulator.

Implements two pure-Python classifiers:

  * `classify_old` — walks the old two-file specification
    (`classify.md` + `suggested-actions.md`) in document order:
    C1 → C2b (override) → C2 → C3 → C4 → C5, then applies the
    suggested-actions table, then applies refusal checks
    (viewer-is-author, too-fresh, data anomaly).

  * `classify_new` — walks the new ordered decision table
    (`classify-and-act.md`) row 1 → row 22, first-match-wins.

Both classifiers consume the same `Derived` state object so any
divergence is in the rule logic, not in upstream parsing.

Read-only. Stdlib only. Uses `gh` CLI for GraphQL + REST.

Usage:
    python3 dev/pr_triage_simulator.py --repo apache/airflow --first 30
"""
from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

# ---------------------------------------------------------------------
# GraphQL queries
# ---------------------------------------------------------------------

PR_LIST_QUERY = """
query($searchQuery: String!, $batchSize: Int!) {
  search(query: $searchQuery, type: ISSUE, first: $batchSize) {
    issueCount
    nodes {
      ... on PullRequest {
        number
        title
        url
        createdAt
        updatedAt
        isDraft
        mergeable
        baseRefName
        author { login }
        authorAssociation
        labels(first: 30) { nodes { name } }
        commits(last: 1) {
          nodes {
            commit {
              oid
              committedDate
              statusCheckRollup {
                state
                contexts(first: 50) {
                  nodes {
                    __typename
                    ... on CheckRun {
                      name conclusion status startedAt completedAt
                    }
                    ... on StatusContext { context state createdAt }
                  }
                }
              }
            }
          }
        }
        reviewThreads(first: 30) {
          totalCount
          nodes {
            isResolved
            comments(first: 5) {
              nodes {
                author { login }
                authorAssociation
                createdAt
              }
            }
          }
        }
        latestReviews(first: 20) {
          nodes {
            state
            author { login }
            authorAssociation
            submittedAt
          }
        }
        comments(last: 10) {
          nodes {
            author { login }
            authorAssociation
            createdAt
            bodyText
          }
        }
      }
    }
  }
}
"""

RECENT_MAIN_QUERY = """
query($owner: String!, $repo: String!) {
  repository(owner: $owner, name: $repo) {
    pullRequests(states: MERGED, orderBy: {field: UPDATED_AT, direction: DESC}, first: 10) {
      nodes {
        number
        commits(last: 1) {
          nodes {
            commit {
              statusCheckRollup {
                contexts(first: 50) {
                  nodes {
                    __typename
                    ... on CheckRun     { name conclusion }
                    ... on StatusContext { context state }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

# ---------------------------------------------------------------------
# Constants — identical to the spec
# ---------------------------------------------------------------------

STATIC_CHECK_PATTERNS = (
    "static check", "pre-commit", "prek", "lint", "mypy", "ruff",
    "black", "flake8", "pylint", "isort", "bandit", "codespell",
    "yamllint", "shellcheck",
)
REAL_CI_PATTERNS = (
    "tests", "static checks", "pre-commit", "ruff", "mypy",
    "build ci image", "build prod image", "helm tests", "k8s tests",
    "docs build", "codeql", "check newsfragment",
)
BOT_LOGINS = (
    "dependabot", "dependabot[bot]", "renovate[bot]",
    "github-actions", "github-actions[bot]",
)
COPILOT_PATTERNS = ("copilot", "github-copilot")
COLLAB_ASSOCS = {"OWNER", "MEMBER", "COLLABORATOR"}
TRIAGE_MARKER = "Pull Request quality criteria"
COMMITS_BEHIND_THRESHOLD = 50

# ---------------------------------------------------------------------
# Subprocess + fetch helpers
# ---------------------------------------------------------------------


def gh(*args: str) -> str:
    result = subprocess.run(
        ["gh", *args], capture_output=True, text=True, check=False
    )
    if result.returncode != 0:
        raise RuntimeError(f"gh {' '.join(args)} failed: {result.stderr}")
    return result.stdout


def fetch_prs(repo: str, count: int, sort: str = "updated-asc") -> list[dict[str, Any]]:
    search_query = f"is:pr is:open repo:{repo} sort:{sort}"
    raw = gh(
        "api", "graphql",
        "-F", f"searchQuery={search_query}",
        "-F", f"batchSize={count}",
        "-f", f"query={PR_LIST_QUERY}",
    )
    data = json.loads(raw)
    return data["data"]["search"]["nodes"]


def fetch_action_required_index(repo: str) -> set[str]:
    try:
        raw = gh(
            "api",
            f"repos/{repo}/actions/runs?event=pull_request&status=action_required&per_page=100",
        )
        data = json.loads(raw)
        return {run["head_sha"] for run in data.get("workflow_runs", [])}
    except Exception as exc:
        print(f"WARN: action_required index fetch failed: {exc}", file=sys.stderr)
        return set()


def fetch_recent_main_failures(repo: str) -> set[str]:
    """Return the set of CheckRun names that failed in ≥2 of the
    last 10 merged PRs on the repo. Used by `classify-and-act.md`
    rows 10-11 / old `suggested-actions.md` 'systemic failure'
    rules."""
    owner, name = repo.split("/", 1)
    try:
        raw = gh(
            "api", "graphql",
            "-F", f"owner={owner}",
            "-F", f"repo={name}",
            "-f", f"query={RECENT_MAIN_QUERY}",
        )
    except Exception as exc:
        print(f"WARN: recent_main_failures fetch failed: {exc}", file=sys.stderr)
        return set()
    data = json.loads(raw)
    nodes = (
        data.get("data", {})
        .get("repository", {})
        .get("pullRequests", {})
        .get("nodes", [])
    )
    counts: dict[str, int] = {}
    for pr in nodes:
        seen: set[str] = set()
        commits = pr.get("commits", {}).get("nodes", []) or []
        if not commits:
            continue
        rollup = (commits[0].get("commit") or {}).get("statusCheckRollup") or {}
        for ctx in (rollup.get("contexts") or {}).get("nodes", []):
            if ctx["__typename"] == "CheckRun" and ctx.get("conclusion") in ("FAILURE", "TIMED_OUT"):
                seen.add(ctx.get("name") or "")
            elif ctx["__typename"] == "StatusContext" and ctx.get("state") == "FAILURE":
                seen.add(ctx.get("context") or "")
        for n in seen:
            counts[n] = counts.get(n, 0) + 1
    return {name for name, c in counts.items() if c >= 2}


def fetch_commits_behind(repo: str, base: str, head_sha: str) -> int | None:
    """REST compare endpoint. Returns commits-behind count or
    None on failure / unknown."""
    try:
        raw = gh("api", f"repos/{repo}/compare/{base}...{head_sha}")
    except Exception:
        return None
    try:
        data = json.loads(raw)
        return data.get("behind_by")
    except Exception:
        return None


# ---------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _parse(ts: str | None) -> datetime | None:
    if ts is None:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


# ---------------------------------------------------------------------
# Derived state — fields populated once, consumed by both classifiers
# ---------------------------------------------------------------------


@dataclass
class Derived:
    head_sha: str
    is_draft: bool
    mergeable: str
    rollup_state: str
    rollup_contexts: list[dict[str, Any]]
    failed_checks: list[str]
    has_real_ci: bool
    last_failed_started_at: datetime | None
    unresolved_threads: list[dict[str, Any]]
    unresolved_collab_threads: list[dict[str, Any]]
    last_commit_at: datetime | None
    author_login: str
    author_assoc: str
    labels: set[str]
    has_ready_label: bool
    last_collab_comment: dict[str, Any] | None
    viewer_triage_comment_at: datetime | None
    author_responded_after_triage: bool
    age_minutes: float
    in_action_required_index: bool
    is_first_time: bool
    copilot_stale: bool
    base_ref: str
    commits_behind: int | None
    data_anomaly: bool
    follow_up_ping: bool
    stale_changes_requested_reviews: list[dict[str, Any]]


def _is_bot(login: str) -> bool:
    low = login.lower()
    return low.endswith("[bot]") or low in BOT_LOGINS


def _is_real_ci_name(name: str) -> bool:
    low = name.lower()
    return any(p in low for p in REAL_CI_PATTERNS)


def derive(
    pr: dict[str, Any],
    viewer: str,
    ar_index: set[str],
    repo: str,
) -> Derived:
    nodes = pr.get("commits", {}).get("nodes") or []
    first_node = (nodes[0] if nodes else None) or {}
    commit_node = first_node.get("commit")
    rollup = commit_node["statusCheckRollup"] if commit_node else None
    rollup_state = (rollup or {}).get("state") or ""
    rollup_contexts = (rollup or {}).get("contexts", {}).get("nodes", []) or []
    head_sha = (commit_node or {}).get("oid", "")
    last_commit_at = _parse((commit_node or {}).get("committedDate"))
    base_ref = pr.get("baseRefName") or "main"

    failed_checks: list[str] = []
    has_real_ci = False
    last_failed_started_at: datetime | None = None
    for ctx in rollup_contexts:
        name = ""
        failed = False
        started: datetime | None = None
        if ctx["__typename"] == "CheckRun":
            name = ctx.get("name") or ""
            conclusion = ctx.get("conclusion") or ""
            if conclusion in ("FAILURE", "TIMED_OUT"):
                failed = True
            started = _parse(ctx.get("startedAt") or ctx.get("completedAt"))
        elif ctx["__typename"] == "StatusContext":
            name = ctx.get("context") or ""
            if (ctx.get("state") or "") == "FAILURE":
                failed = True
            started = _parse(ctx.get("createdAt"))
        if failed:
            failed_checks.append(name)
            if started and (last_failed_started_at is None or started > last_failed_started_at):
                last_failed_started_at = started
        if name and _is_real_ci_name(name):
            has_real_ci = True

    unresolved = [t for t in pr["reviewThreads"]["nodes"] if not t["isResolved"]]
    unresolved_collab = [
        t for t in unresolved
        if t["comments"]["nodes"]
        and t["comments"]["nodes"][0].get("authorAssociation") in COLLAB_ASSOCS
    ]

    labels = {l["name"] for l in pr["labels"]["nodes"]}
    has_ready_label = "ready for maintainer review" in labels

    comments = pr["comments"]["nodes"]
    collab_comments = [c for c in comments if c.get("authorAssociation") in COLLAB_ASSOCS]
    last_collab = collab_comments[-1] if collab_comments else None

    # Most-recent triage marker comment by viewer, after last commit.
    viewer_triage_comment_at: datetime | None = None
    for c in reversed(comments):
        login = (c.get("author") or {}).get("login")
        if login != viewer:
            continue
        if TRIAGE_MARKER not in (c.get("bodyText") or ""):
            continue
        ts = _parse(c["createdAt"])
        if not ts:
            continue
        if last_commit_at and ts <= last_commit_at:
            continue
        viewer_triage_comment_at = ts
        break

    author_login = (pr.get("author") or {}).get("login") or ""
    author_responded_after_triage = False
    if viewer_triage_comment_at:
        for c in comments:
            if (c.get("author") or {}).get("login") != author_login:
                continue
            ts = _parse(c["createdAt"])
            if ts and ts > viewer_triage_comment_at:
                author_responded_after_triage = True
                break

    created = _parse(pr["createdAt"])
    age_minutes = (_now() - created).total_seconds() / 60 if created else 0

    assoc = pr.get("authorAssociation") or ""
    is_first_time = assoc in ("FIRST_TIME_CONTRIBUTOR", "FIRST_TIMER")

    copilot_stale = False
    cutoff = _now() - timedelta(days=7)
    for thread in unresolved:
        first = (thread["comments"]["nodes"] or [None])[0]
        if not first:
            continue
        login = (first.get("author") or {}).get("login") or ""
        login_low = login.lower()
        is_copilot = (
            login_low.endswith("[bot]")
            and any(login_low.startswith(p) for p in COPILOT_PATTERNS)
        )
        first_at = _parse(first.get("createdAt"))
        if not (is_copilot and first_at and first_at < cutoff):
            continue
        author_replied_in_thread = False
        for tc in thread["comments"]["nodes"]:
            if (tc.get("author") or {}).get("login") == author_login:
                tc_at = _parse(tc.get("createdAt"))
                if tc_at and tc_at > first_at:
                    author_replied_in_thread = True
                    break
        if not author_replied_in_thread:
            copilot_stale = True
            break

    # Stale CHANGES_REQUESTED reviews — author committed after.
    stale_cr_reviews: list[dict[str, Any]] = []
    for review in pr["latestReviews"]["nodes"]:
        if review.get("state") != "CHANGES_REQUESTED":
            continue
        rts = _parse(review["submittedAt"])
        if last_commit_at and rts and last_commit_at > rts:
            stale_cr_reviews.append(review)

    # follow_up_ping — defined per glossary in classify-and-act.md.
    follow_up_ping = False
    if stale_cr_reviews:
        most_recent_cr = max(
            stale_cr_reviews,
            key=lambda r: _parse(r["submittedAt"]) or _now(),
        )
        cr_at = _parse(most_recent_cr["submittedAt"])
        cr_login = (most_recent_cr.get("author") or {}).get("login")
        # Author comment after CR mentioning reviewer login.
        for c in comments:
            ats = _parse(c["createdAt"])
            if (
                (c.get("author") or {}).get("login") == author_login
                and ats and cr_at and ats > cr_at
                and cr_login
                and re.search(rf"@{re.escape(cr_login)}\b", c.get("bodyText") or "")
            ):
                follow_up_ping = True
                break
        # Reviewer comment after the most recent author commit.
        if not follow_up_ping and last_commit_at:
            for c in comments:
                cts = _parse(c["createdAt"])
                if (
                    (c.get("author") or {}).get("login") == cr_login
                    and cts and cts > last_commit_at
                ):
                    follow_up_ping = True
                    break

    # Commits-behind via REST compare. Cheap one-shot per PR; the
    # production skill batches via aliased GraphQL but for the
    # simulator's correctness goal a per-PR call is acceptable.
    commits_behind = (
        fetch_commits_behind(repo, base_ref, head_sha) if head_sha else None
    )

    # Data anomaly — rollup SUCCESS but failed_checks non-empty.
    data_anomaly = rollup_state == "SUCCESS" and bool(failed_checks)

    return Derived(
        head_sha=head_sha,
        is_draft=pr["isDraft"],
        mergeable=pr["mergeable"],
        rollup_state=rollup_state,
        rollup_contexts=rollup_contexts,
        failed_checks=failed_checks,
        has_real_ci=has_real_ci,
        last_failed_started_at=last_failed_started_at,
        unresolved_threads=unresolved,
        unresolved_collab_threads=unresolved_collab,
        last_commit_at=last_commit_at,
        author_login=author_login,
        author_assoc=assoc,
        labels=labels,
        has_ready_label=has_ready_label,
        last_collab_comment=last_collab,
        viewer_triage_comment_at=viewer_triage_comment_at,
        author_responded_after_triage=author_responded_after_triage,
        age_minutes=age_minutes,
        in_action_required_index=head_sha in ar_index,
        is_first_time=is_first_time,
        copilot_stale=copilot_stale,
        base_ref=base_ref,
        commits_behind=commits_behind,
        data_anomaly=data_anomaly,
        follow_up_ping=follow_up_ping,
        stale_changes_requested_reviews=stale_cr_reviews,
    )


# ---------------------------------------------------------------------
# Pre-filters (shared)
# ---------------------------------------------------------------------


def pre_filter(pr: dict[str, Any], d: Derived) -> str | None:
    if d.author_assoc in COLLAB_ASSOCS:
        return "F1: collaborator/member/owner"
    if _is_bot(d.author_login):
        return "F2: bot"
    updated = _parse(pr["updatedAt"])
    if d.is_draft and updated and (_now() - updated) < timedelta(days=14):
        return "F3: fresh draft"
    if d.has_ready_label:
        regressed = (
            d.rollup_state == "FAILURE"
            or d.mergeable == "CONFLICTING"
            or bool(d.unresolved_collab_threads)
        )
        if not regressed:
            return "F4: already marked ready, no regression"
    if d.last_collab_comment and d.last_commit_at:
        ts = _parse(d.last_collab_comment["createdAt"])
        if ts and (_now() - ts) < timedelta(hours=72) and ts > d.last_commit_at:
            return "F5a: recent collab comment <72h"
    if d.last_collab_comment:
        body = d.last_collab_comment.get("bodyText") or ""
        mentions = set(re.findall(r"@([A-Za-z0-9-]+)", body))
        mentions.discard(d.author_login)
        ts = _parse(d.last_collab_comment["createdAt"])
        if mentions and ts:
            answered = False
            for c in pr["comments"]["nodes"]:
                login = (c.get("author") or {}).get("login")
                cts = _parse(c["createdAt"])
                if login in mentions and cts and cts > ts:
                    answered = True
                    break
            if not answered:
                return "F5b: maintainer-to-maintainer ping unanswered"
    return None


# ---------------------------------------------------------------------
# Compound preconditions
# ---------------------------------------------------------------------


def grace_window(pr: dict[str, Any], d: Derived) -> timedelta:
    has_collab_engagement = bool(d.last_collab_comment) or any(
        r.get("authorAssociation") in COLLAB_ASSOCS
        for r in pr["latestReviews"]["nodes"]
    )
    return timedelta(hours=96) if has_collab_engagement else timedelta(hours=24)


def grace_anchor(pr: dict[str, Any], d: Derived) -> datetime:
    if d.last_failed_started_at:
        return d.last_failed_started_at
    return _parse(pr["updatedAt"]) or _now()


def ci_failure_past_grace(pr: dict[str, Any], d: Derived) -> bool:
    if d.rollup_state != "FAILURE":
        return False
    return (_now() - grace_anchor(pr, d)) > grace_window(pr, d)


def has_deterministic_signal(pr: dict[str, Any], d: Derived) -> bool:
    return (
        d.mergeable == "CONFLICTING"
        or ci_failure_past_grace(pr, d)
        or bool(d.unresolved_collab_threads)
    )


def ci_failures_only(pr: dict[str, Any], d: Derived) -> bool:
    return (
        ci_failure_past_grace(pr, d)
        and d.mergeable != "CONFLICTING"
        and not d.unresolved_collab_threads
    )


def unresolved_threads_only(pr: dict[str, Any], d: Derived) -> bool:
    return (
        bool(d.unresolved_collab_threads)
        and d.rollup_state == "SUCCESS"
        and d.mergeable != "CONFLICTING"
    )


def unresolved_threads_only_likely_addressed(pr: dict[str, Any], d: Derived) -> bool:
    if not unresolved_threads_only(pr, d):
        return False
    first_comment_times = [
        ts
        for ts in (
            _parse(t["comments"]["nodes"][0]["createdAt"])
            for t in d.unresolved_collab_threads
            if t["comments"]["nodes"]
        )
        if ts is not None
    ]
    most_recent_first = max(first_comment_times, default=None)
    if not (d.last_commit_at and most_recent_first and d.last_commit_at > most_recent_first):
        return False
    for thread in d.unresolved_collab_threads:
        first = thread["comments"]["nodes"][0]
        first_at = _parse(first["createdAt"])
        author_replied = any(
            (c.get("author") or {}).get("login") == d.author_login
            and (_parse(c["createdAt"]) or _now()) > (first_at or _now())
            for c in thread["comments"]["nodes"]
        )
        commit_after = d.last_commit_at and first_at and d.last_commit_at > first_at
        if not (author_replied or commit_after):
            return False
    return True


def all_static_checks(failed: list[str]) -> bool:
    return bool(failed) and all(
        any(p in name.lower() for p in STATIC_CHECK_PATTERNS) for name in failed
    )


def first_time_no_real_ci(d: Derived) -> bool:
    if not d.is_first_time:
        return False
    if d.rollup_state in ("EXPECTED", ""):
        return True
    if d.rollup_state == "SUCCESS" and not d.has_real_ci:
        return True
    return False


def failures_match_main(d: Derived, recent_main: set[str]) -> tuple[int, int]:
    if not d.failed_checks or not recent_main:
        return (0, len(d.failed_checks))
    matches = sum(1 for f in d.failed_checks if any(
        m.lower() in f.lower() or f.lower() in m.lower() for m in recent_main
    ))
    return (matches, len(d.failed_checks))


# ---------------------------------------------------------------------
# OLD spec — classify in document order, then suggest action,
# then apply refusal checks.
# ---------------------------------------------------------------------


def _classify_old_only(pr: dict[str, Any], d: Derived) -> str:
    """Return the old-spec classification (C1..C5) following
    `classify.md` document order, with C2b evaluated before C2
    per its own ordering note."""
    # C1
    if d.in_action_required_index:
        return "pending_workflow_approval"
    if first_time_no_real_ci(d):
        return "pending_workflow_approval"
    # C2b (old spec note: evaluate before C2)
    if d.copilot_stale:
        return "stale_copilot_review"
    # C2
    if has_deterministic_signal(pr, d):
        return "deterministic_flag"
    # C3 — old spec classify.md L289-302 requires no follow-up ping.
    if d.stale_changes_requested_reviews and not d.follow_up_ping:
        return "stale_review"
    # C4
    if d.viewer_triage_comment_at:
        return "already_triaged"
    # C5 — passing requires Real-CI guard (real-CI present)
    if (
        d.rollup_state == "SUCCESS"
        and d.mergeable != "CONFLICTING"
        and not d.unresolved_collab_threads
        and d.has_real_ci
    ):
        return "passing"
    # Fallback — Real-CI guard reclassification path: no real CI
    # ran, mergeable, not first-time → deterministic_flag with
    # rebase suggestion. Old spec wording "rollup empty / only
    # bot contexts" covers FAILURE on bot-only checks too.
    if (
        d.mergeable != "CONFLICTING"
        and not d.has_real_ci
        and not d.is_first_time
    ):
        return "deterministic_flag_no_ci"
    return "unclassified"


def _suggest_old(
    pr: dict[str, Any],
    d: Derived,
    classification: str,
    recent_main: set[str],
) -> tuple[str, str]:
    """Apply suggested-actions.md per-classification table."""
    if classification == "pending_workflow_approval":
        return ("pending_workflow_approval", "approve-workflow")
    if classification == "stale_copilot_review":
        return ("stale_copilot_review", "draft")
    if classification == "deterministic_flag":
        if d.mergeable == "CONFLICTING":
            return ("deterministic_flag", "draft")
        if ci_failures_only(pr, d):
            matches, total = failures_match_main(d, recent_main)
            if total > 0 and matches == total:
                return ("deterministic_flag", "rerun")
            if matches > 0:
                return ("deterministic_flag", "rerun")
            if all_static_checks(d.failed_checks):
                return ("deterministic_flag", "comment")
            if (
                len(d.failed_checks) <= 2
                and (d.commits_behind is None or d.commits_behind <= COMMITS_BEHIND_THRESHOLD)
            ):
                return ("deterministic_flag", "rerun")
        if unresolved_threads_only(pr, d):
            if unresolved_threads_only_likely_addressed(pr, d):
                return ("deterministic_flag", "mark-ready-with-ping")
            return ("deterministic_flag", "ping")
        return ("deterministic_flag", "draft")
    if classification == "deterministic_flag_no_ci":
        return ("deterministic_flag", "rebase")
    if classification == "stale_review":
        return ("stale_review", "ping")
    if classification == "already_triaged":
        if d.author_responded_after_triage:
            return ("already_triaged", "skip")
        if d.is_draft and d.viewer_triage_comment_at:
            age = (_now() - d.viewer_triage_comment_at).days
            if age >= 7:
                return ("stale_draft", "defer-to-sweep")
        return ("already_triaged", "skip")
    if classification == "passing":
        if d.has_ready_label:
            return ("passing", "skip")
        return ("passing", "mark-ready")
    return ("skip", "no-rule-fired")


def _refusal_old(d: Derived, viewer: str) -> tuple[str, str] | None:
    """suggested-actions.md 'When to refuse to suggest' overrides."""
    if d.author_login and d.author_login == viewer:
        return ("skip", "viewer-is-author")
    if d.age_minutes < 30:
        return ("skip", "too-fresh")
    if d.data_anomaly:
        return ("skip", "data-anomaly")
    return None


def classify_old(
    pr: dict[str, Any],
    d: Derived,
    recent_main: set[str],
    viewer: str,
) -> tuple[str, str]:
    refusal = _refusal_old(d, viewer)
    if refusal:
        return refusal
    classification = _classify_old_only(pr, d)
    return _suggest_old(pr, d, classification, recent_main)


# ---------------------------------------------------------------------
# NEW spec — single ordered decision table, first match wins.
# ---------------------------------------------------------------------


def classify_new(
    pr: dict[str, Any],
    d: Derived,
    recent_main: set[str],
    viewer: str,
) -> tuple[str, str]:
    # Row 1
    if d.in_action_required_index or first_time_no_real_ci(d):
        return ("pending_workflow_approval", "approve-workflow")
    # Row 2
    if d.copilot_stale:
        return ("stale_copilot_review", "draft")
    # Rows 3-5 (already_triaged ladder + draft → stale_draft handoff)
    if d.viewer_triage_comment_at:
        days_since = (_now() - d.viewer_triage_comment_at).days
        if d.is_draft and days_since >= 7 and not d.author_responded_after_triage:
            return ("stale_draft", "defer-to-sweep")
        if d.author_responded_after_triage:
            return ("already_triaged", "skip")
        if days_since < 7:
            return ("already_triaged", "skip")
    # Row 6
    if d.author_login and d.author_login == viewer:
        return ("skip", "viewer-is-author")
    # Row 7
    if d.age_minutes < 30:
        return ("skip", "too-fresh")
    # Row 22 — data anomaly (placed early so SUCCESS+failed_checks
    # combination cannot fall into rows 19/20).
    if d.data_anomaly:
        return ("skip", "data-anomaly")
    # Rows 8-17 — deterministic_flag ladder
    if has_deterministic_signal(pr, d):
        # Row 9
        if d.mergeable == "CONFLICTING":
            return ("deterministic_flag", "draft")
        # Rows 10-13
        if ci_failures_only(pr, d):
            matches, total = failures_match_main(d, recent_main)
            if total > 0 and matches == total:
                return ("deterministic_flag", "rerun")
            if matches > 0:
                return ("deterministic_flag", "rerun")
            if all_static_checks(d.failed_checks):
                return ("deterministic_flag", "comment")
            if (
                len(d.failed_checks) <= 2
                and (d.commits_behind is None or d.commits_behind <= COMMITS_BEHIND_THRESHOLD)
            ):
                return ("deterministic_flag", "rerun")
        # Row 14
        if unresolved_threads_only(pr, d) and unresolved_threads_only_likely_addressed(pr, d):
            return ("deterministic_flag", "mark-ready-with-ping")
        # Row 15
        if unresolved_threads_only(pr, d):
            return ("deterministic_flag", "ping")
        # Row 16 — placed inside the deterministic_flag block,
        # before the row 17 fallback. Its preconditions remain
        # independent of has_deterministic_signal in the spec but
        # only matter (and only fire) here for PRs that already
        # have a deterministic signal yet no real CI ran.
        if (
            d.mergeable != "CONFLICTING"
            and not d.has_real_ci
            and not d.is_first_time
        ):
            return ("deterministic_flag", "rebase")
        # Row 17 fallback
        return ("deterministic_flag", "draft")
    # Row 16 — outside the deterministic_flag block: PRs with no
    # deterministic signal but no real CI ran (e.g. only bot
    # contexts present) are reclassified per the Real-CI guard.
    if (
        d.mergeable != "CONFLICTING"
        and not d.has_real_ci
        and not d.is_first_time
    ):
        return ("deterministic_flag", "rebase")
    # Row 18 — stale_review (unaddressed)
    if d.stale_changes_requested_reviews and not d.follow_up_ping:
        return ("stale_review", "ping")
    # Rows 19-20
    if (
        d.rollup_state == "SUCCESS"
        and d.mergeable != "CONFLICTING"
        and not d.unresolved_collab_threads
        and d.has_real_ci
    ):
        if d.has_ready_label:
            return ("passing", "skip")
        return ("passing", "mark-ready")
    return ("skip", "no-rule-fired")


# ---------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------


_VIEWER: str | None = None


def _viewer_cache() -> str:
    global _VIEWER
    if _VIEWER is None:
        _VIEWER = gh("api", "user", "--jq", ".login").strip()
    return _VIEWER


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", default="apache/airflow")
    parser.add_argument("--first", type=int, default=30)
    parser.add_argument(
        "--sort", default="updated-asc",
        help="GitHub search sort (e.g. updated-asc, updated-desc, created-desc)",
    )
    args = parser.parse_args()

    viewer = _viewer_cache()
    print(f"viewer: {viewer}")
    print(f"repo:   {args.repo}")
    print(f"first:  {args.first}")

    ar_index = fetch_action_required_index(args.repo)
    print(f"action_required SHAs in index: {len(ar_index)}")

    recent_main = fetch_recent_main_failures(args.repo)
    print(f"recent_main_failures (≥2 of last 10 merged): {sorted(recent_main)}")

    prs = fetch_prs(args.repo, args.first, args.sort)
    print(f"fetched: {len(prs)} PRs")
    print()

    divergences: list[dict[str, Any]] = []
    rows = []
    for pr in prs:
        if not pr:
            continue
        d = derive(pr, viewer, ar_index, args.repo)
        skip = pre_filter(pr, d)
        if skip:
            rows.append((pr["number"], "PRE", skip, "PRE", skip, False))
            continue
        cls_o, act_o = classify_old(pr, d, recent_main, viewer)
        cls_n, act_n = classify_new(pr, d, recent_main, viewer)
        diverged = (cls_o, act_o) != (cls_n, act_n)
        if diverged:
            divergences.append({
                "pr": pr["number"], "url": pr["url"],
                "old": (cls_o, act_o), "new": (cls_n, act_n),
                "head_sha": d.head_sha,
            })
        rows.append((pr["number"], cls_o, act_o, cls_n, act_n, diverged))

    print(f"{'#':>6}  {'classification_old':<28} {'action_old':<22}  "
          f"{'classification_new':<28} {'action_new':<22}  diverge")
    print("-" * 134)
    for r in rows:
        n, co, ao, cn, an, dv = r
        marker = "  *" if dv else ""
        print(f"{n:>6}  {co:<28} {ao:<22}  {cn:<28} {an:<22}{marker}")

    print()
    print(f"total PRs: {len(rows)}")
    print(f"divergences: {len(divergences)}")
    if divergences:
        print()
        print("DIVERGENCE DETAIL:")
        for div in divergences:
            print(f"  #{div['pr']:>6} {div['url']}")
            print(f"    OLD: {div['old']}")
            print(f"    NEW: {div['new']}")
    return 0 if not divergences else 2


if __name__ == "__main__":
    sys.exit(main())
