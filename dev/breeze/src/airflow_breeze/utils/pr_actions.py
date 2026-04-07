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
"""Triage action recommendation and execution for PR auto-triage.

This is the core decision-making module. It determines what action to suggest
for a given PR state and executes the chosen action. All magic numbers and
thresholds are defined as named constants at the top.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from airflow_breeze.utils.confirm import TriageAction
from airflow_breeze.utils.console import console_print, get_console
from airflow_breeze.utils.pr_display import pr_link
from airflow_breeze.utils.pr_models import PRData

if TYPE_CHECKING:
    from airflow_breeze.utils.github import PRAssessment

# ---------------------------------------------------------------------------
# Thresholds — all magic numbers in one place
# ---------------------------------------------------------------------------

# If an author has more than this many flagged PRs, suggest closing
AUTHOR_FLAGGED_CLOSE_THRESHOLD = 3

# Maximum number of CI failures to still suggest rerun (instead of draft)
MAX_CI_FAILURES_FOR_RERUN = 2

# Maximum commits behind base to still suggest rerun (instead of draft)
MAX_COMMITS_BEHIND_FOR_RERUN = 50


# ---------------------------------------------------------------------------
# PR state snapshot for optimistic locking
# ---------------------------------------------------------------------------

@dataclass
class PRStateSnapshot:
    """Snapshot of key PR fields for staleness detection."""

    head_sha: str
    updated_at: str
    is_draft: bool


def snapshot_pr_state(pr: PRData) -> PRStateSnapshot:
    """Capture the current PR state for later comparison."""
    return PRStateSnapshot(
        head_sha=pr.head_sha,
        updated_at=pr.updated_at,
        is_draft=pr.is_draft,
    )


# ---------------------------------------------------------------------------
# Static check detection
# ---------------------------------------------------------------------------

# Check name patterns that indicate static/lint checks (deterministic, not flaky)
STATIC_CHECK_PATTERNS = (
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


def are_only_static_check_failures(failed_checks: list[str]) -> bool:
    """Return True if all failed checks are static/lint checks (deterministic, not flaky)."""
    if not failed_checks:
        return False
    return all(
        any(pattern in check.lower() for pattern in STATIC_CHECK_PATTERNS)
        for check in failed_checks
    )


# ---------------------------------------------------------------------------
# Action recommendation — the core decision engine
# ---------------------------------------------------------------------------


def compute_default_action(
    pr: PRData,
    assessment: PRAssessment,
    author_flagged_count: dict[str, int],
    main_failures=None,
) -> tuple[TriageAction, str]:
    """Compute the suggested default triage action and reason for a flagged PR.

    This is pure logic with no I/O — fully testable with mock objects.

    Args:
        pr: The PR data.
        assessment: The assessment result (deterministic or LLM).
        author_flagged_count: Map of author login -> number of flagged PRs.
        main_failures: Optional RecentPRFailureInfo for matching systemic failures.

    Returns:
        Tuple of (suggested_action, human_readable_reason).
    """
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
    if count > AUTHOR_FLAGGED_CLOSE_THRESHOLD:
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
    elif has_ci_failures and are_only_static_check_failures(pr.failed_checks):
        # Only static checks failed — these are deterministic (not flaky), suggest comment
        reason_parts.append("only static check failures — likely needs code fix, not rerun")
        action = TriageAction.COMMENT
    elif (
        not has_conflicts
        and has_ci_failures
        and failed_count <= MAX_CI_FAILURES_FOR_RERUN
        and not has_unresolved_comments
        and pr.commits_behind <= MAX_COMMITS_BEHIND_FOR_RERUN
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


# ---------------------------------------------------------------------------
# Violation selection
# ---------------------------------------------------------------------------


def select_violations(violations: list) -> list | None:
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


# ---------------------------------------------------------------------------
# Action confirmation
# ---------------------------------------------------------------------------


def confirm_action(
    pr: PRData,
    description: str,
    forced_answer: str | None = None,
    stats=None,
) -> bool:
    """Ask for final confirmation before modifying a PR. Returns True if confirmed.

    Uses single-keypress input (no Enter required) for a snappy workflow.
    Pressing 'q' sets stats.quit_early if stats is provided and returns False.
    """
    import os

    from airflow_breeze.utils.confirm import _read_char
    from airflow_breeze.utils.shared_options import get_forced_answer

    force = forced_answer or get_forced_answer() or os.environ.get("ANSWER")
    if force:
        print(f"Forced answer for confirm '{description}': {force}")
        return force.upper() in ("Y", "YES")

    prompt = f"  Confirm: {description} on PR {pr_link(pr)}? [Y/n/q] "
    console_print(prompt, end="")

    try:
        ch = _read_char()
    except (KeyboardInterrupt, EOFError):
        console_print()
        console_print(f"  [info]Cancelled — no changes made to PR {pr_link(pr)}.[/]")
        return False

    # Ignore multi-byte escape sequences (arrow keys, etc.)
    if len(ch) > 1:
        console_print()
        console_print(f"  [info]Cancelled — no changes made to PR {pr_link(pr)}.[/]")
        return False

    # Echo the character and move to next line
    console_print(ch)

    if ch.upper() == "Q":
        console_print("  [warning]Quitting...[/]")
        if stats:
            stats.quit_early = True
        return False
    if ch.upper() in ("Y", "\r", "\n", ""):
        return True
    console_print(f"  [info]Cancelled — no changes made to PR {pr_link(pr)}.[/]")
    return False


# ---------------------------------------------------------------------------
# Triage action execution
# ---------------------------------------------------------------------------


def execute_triage_action(
    ctx,
    pr: PRData,
    action: TriageAction,
    *,
    draft_comment: str,
    close_comment: str,
    comment_only_text: str | None = None,
    snapshot: PRStateSnapshot | None = None,
    run_api: bool = True,
):
    """Execute a single triage action on a PR. Mutates ctx.stats.

    If ``snapshot`` is provided, checks whether the PR state has changed since the
    snapshot was taken. When a change is detected the PR is re-enriched and
    re-evaluated; the new result is returned so the caller can present updated
    information. Returns ``None`` when the action completed (or was skipped) normally.
    """
    from airflow_breeze.utils.pr_github import (
        add_label,
        close_pr,
        convert_pr_to_draft,
        post_comment,
        update_pr_branch,
    )

    stats = ctx.stats

    if action == TriageAction.SKIP:
        console_print(f"  [info]Skipping PR {pr_link(pr)} — no action taken.[/]")
        stats.total_skipped_action += 1
        return None

    # Optimistic locking: verify PR state hasn't changed before mutating
    if snapshot is not None:
        stale_result = refresh_pr_if_stale(
            pr, snapshot, token=ctx.token, github_repository=ctx.github_repository, run_api=run_api
        )
        if stale_result is not None:
            return stale_result

    # Label applied when a maintainer marks a flagged PR as ready for review
    _READY_FOR_REVIEW_LABEL = "ready for maintainer review"
    # Label applied when a PR is closed due to multiple quality violations
    _CLOSED_QUALITY_LABEL = "closed because of multiple quality violations"

    if action == TriageAction.READY:
        if not confirm_action(pr, "Add 'ready for maintainer review' label", ctx.answer_triage, stats=stats):
            stats.total_skipped_action += 1
            return None
        console_print(
            f"  [info]Marking PR {pr_link(pr)} as ready — adding '{_READY_FOR_REVIEW_LABEL}' label.[/]"
        )
        if add_label(ctx.token, ctx.github_repository, pr.node_id, _READY_FOR_REVIEW_LABEL):
            console_print(f"  [success]Label '{_READY_FOR_REVIEW_LABEL}' added to PR {pr_link(pr)}.[/]")
            stats.total_ready += 1
        else:
            console_print(f"  [warning]Failed to add label to PR {pr_link(pr)}.[/]")
        return None

    if action == TriageAction.RERUN:
        if pr.head_sha and pr.failed_checks:
            from airflow_breeze.utils.pr_workflows import (
                cancel_and_rerun_in_progress_workflows,
                rerun_failed_workflow_runs,
            )

            console_print(
                f"  Rerunning {len(pr.failed_checks)} failed "
                f"{'checks' if len(pr.failed_checks) != 1 else 'check'} for PR {pr_link(pr)}..."
            )
            rerun_count = rerun_failed_workflow_runs(
                ctx.token, ctx.github_repository, pr.head_sha, pr.failed_checks
            )
            if rerun_count:
                console_print(
                    f"  [success]Rerun triggered for {rerun_count} workflow "
                    f"{'runs' if rerun_count != 1 else 'run'} on PR {pr_link(pr)}.[/]"
                )
                stats.total_rerun += 1
            else:
                # No completed failed runs to rerun — check if workflows are still running
                console_print(
                    f"  [warning]No completed failed runs found for PR {pr_link(pr)}. "
                    f"Checking for in-progress workflows...[/]"
                )
                restarted = cancel_and_rerun_in_progress_workflows(
                    ctx.token, ctx.github_repository, pr.head_sha
                )
                if restarted:
                    console_print(
                        f"  [success]Cancelled and restarted {restarted} workflow "
                        f"{'runs' if restarted != 1 else 'run'} on PR {pr_link(pr)}.[/]"
                    )
                    stats.total_rerun += 1
                else:
                    console_print(
                        f"  [warning]Could not rerun any workflow runs for PR {pr_link(pr)}.[/]"
                    )
        else:
            console_print(f"  [warning]No failed checks to rerun for PR {pr_link(pr)}.[/]")
        return None

    if action == TriageAction.REBASE:
        if not confirm_action(pr, "Update (rebase) PR branch", ctx.answer_triage, stats=stats):
            stats.total_skipped_action += 1
            return None

        get_console().print(f"  Updating branch for PR {pr_link(pr)}...")
        if update_pr_branch(ctx.token, ctx.github_repository, pr.number):
            get_console().print(f"  [success]Branch updated for PR {pr_link(pr)}.[/]")
            stats.total_rebased += 1
        else:
            get_console().print(f"  [error]Failed to update branch for PR {pr_link(pr)}.[/]")
        return None

    if action == TriageAction.COMMENT:
        if not confirm_action(pr, "Post comment", ctx.answer_triage, stats=stats):
            stats.total_skipped_action += 1
            return None
        text = comment_only_text or draft_comment
        console_print(f"  Posting comment on PR {pr_link(pr)}...")
        if post_comment(ctx.token, pr.node_id, text):
            console_print(f"  [success]Comment posted on PR {pr_link(pr)}.[/]")
            stats.total_commented += 1
        else:
            console_print(f"  [error]Failed to post comment on PR {pr_link(pr)}.[/]")
        return None

    if action == TriageAction.DRAFT:
        if not confirm_action(pr, "Convert to draft and post comment", ctx.answer_triage, stats=stats):
            stats.total_skipped_action += 1
            return None
        console_print(f"  Converting PR {pr_link(pr)} to draft...")
        if convert_pr_to_draft(ctx.token, pr.node_id):
            console_print(f"  [success]PR {pr_link(pr)} converted to draft.[/]")
        else:
            console_print(f"  [error]Failed to convert PR {pr_link(pr)} to draft.[/]")
            return None
        console_print(f"  Posting comment on PR {pr_link(pr)}...")
        if post_comment(ctx.token, pr.node_id, draft_comment):
            console_print(f"  [success]Comment posted on PR {pr_link(pr)}.[/]")
            stats.total_converted += 1
        else:
            console_print(f"  [error]Failed to post comment on PR {pr_link(pr)}.[/]")
        return None

    if action == TriageAction.PING:
        if not pr.unresolved_threads:
            get_console().print(
                f"  [warning]No unresolved threads to ping reviewers about on PR {pr_link(pr)}.[/]"
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
        if not confirm_action(pr, f"Ping reviewer(s): {mentions}", ctx.answer_triage, stats=stats):
            stats.total_skipped_action += 1
            return None
        get_console().print(f"  Pinging reviewer(s) on PR {pr_link(pr)}...")
        if post_comment(ctx.token, pr.node_id, ping_body):
            get_console().print(f"  [success]Ping comment posted on PR {pr_link(pr)}.[/]")
            stats.total_commented += 1
        else:
            get_console().print(f"  [error]Failed to post ping comment on PR {pr_link(pr)}.[/]")
        return None

    if action == TriageAction.CLOSE:
        if not confirm_action(pr, "Close PR and post comment", ctx.answer_triage, stats=stats):
            stats.total_skipped_action += 1
            return None
        console_print(f"  Closing PR {pr_link(pr)}...")
        if close_pr(ctx.token, pr.node_id):
            console_print(f"  [success]PR {pr_link(pr)} closed.[/]")
        else:
            console_print(f"  [error]Failed to close PR {pr_link(pr)}.[/]")
            return None
        if add_label(ctx.token, ctx.github_repository, pr.node_id, _CLOSED_QUALITY_LABEL):
            console_print(f"  [success]Label '{_CLOSED_QUALITY_LABEL}' added to PR {pr_link(pr)}.[/]")
        else:
            console_print(f"  [warning]Failed to add label to PR {pr_link(pr)}.[/]")
        console_print(f"  Posting comment on PR {pr_link(pr)}...")
        if post_comment(ctx.token, pr.node_id, close_comment):
            console_print(f"  [success]Comment posted on PR {pr_link(pr)}.[/]")
            stats.total_closed += 1
        else:
            console_print(f"  [error]Failed to post comment on PR {pr_link(pr)}.[/]")
    return None


# ---------------------------------------------------------------------------
# Staleness detection and refresh
# ---------------------------------------------------------------------------


def refresh_pr_if_stale(
    pr: PRData,
    snapshot: PRStateSnapshot,
    *,
    token: str,
    github_repository: str,
    run_api: bool = True,
):
    """Re-fetch PR from GitHub and check if state changed since snapshot.

    If the PR state has not changed, returns None (proceed with original action).
    If it changed, updates the PRData in place with fresh data, re-enriches it,
    runs deterministic checks, and returns the new result so the caller can
    re-evaluate the appropriate action.
    """
    from airflow_breeze.utils.pr_fetching import fetch_single_pr_graphql

    fresh = fetch_single_pr_graphql(token, github_repository, pr.number)

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
    from airflow_breeze.utils.pr_enrichment import reevaluate_triaged_pr

    return reevaluate_triaged_pr(
        pr,
        token=token,
        github_repository=github_repository,
        run_api=run_api,
    )
