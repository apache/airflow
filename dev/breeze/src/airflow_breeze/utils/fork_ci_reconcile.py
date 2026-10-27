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
Scheduled reconciler for pull requests gated to fork CI (AIP-120).

``classify`` is a pure function from what GitHub recorded about one pull request to the mutations
that bring it to the desired state; ``run_reconcile`` gathers that data and applies the result.
See "Reconciler" in dev/breeze/doc/ci/08_fork_ci_for_external_contributors.md.

Part of the fork CI gate: a pull request whose CI runs in the contributor's fork and changes
this file is closed by the fork CI monitor, see "Gate protection" in the same document.
"""

from __future__ import annotations

import base64
import functools
import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml

from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.fork_ci_gate import (
    GATED_REASONS,
    LABEL_FORK_CI_REQUIRED,
    LABEL_NEEDS_PROJECT_CI,
    LABEL_READY_FOR_REVIEW,
    LABEL_USE_FORK_CI,
    LABEL_USE_PROJECT_CI,
    TAMPER_CHECKED_REASONS,
    GateConfig,
    GateEvent,
    decide,
    parse_config,
    parse_github_datetime,
)
from airflow_breeze.utils.fork_ci_github import GitHubClient, GitHubError
from airflow_breeze.utils.fork_ci_messages import (
    CI_PASSED_CONTEXT,
    CI_PASSED_DESCRIPTION,
    CI_PASSED_REVOKED_DESCRIPTION,
    JOB_STATUS_CONTEXT_PREFIX,
    JOB_STATUSES,
    STALE_JOB_STATUS,
    STATUS_CONTEXT,
    STATUS_STATES,
    Marker,
    fork_actions_url,
    gate_modified_close_comment,
    inactivity_close_comment,
    job_status_context,
    nudge_comment,
    parse_marker,
    released_comment,
    state_comment,
    status_description,
    tamper_close_comment,
    ungated_comment,
    workflow_disabled_comment,
)

CI_WORKFLOW_FILE = "ci-amd.yml"
# The files the gate job of ci-amd.yml reads from a pull request's base branch.
GATE_CONFIG_PATH = ".github/fork-ci-gate.toml"
GATE_SCRIPT_PATH = "dev/breeze/src/airflow_breeze/utils/fork_ci_gate.py"
CI_WORKFLOW_PATH = f".github/workflows/{CI_WORKFLOW_FILE}"
# Files of the fork CI gate that a pull request whose CI runs in a fork must not change at all.
# The gate configuration and the gate job in ci-amd.yml are checked separately, since the first
# may carry the author's own pilot entry and the second shares its file with the CI pipeline.
GATE_PROTECTED_PATHS = frozenset(
    {
        GATE_SCRIPT_PATH,
        "dev/breeze/src/airflow_breeze/utils/fork_ci_github.py",
        "dev/breeze/src/airflow_breeze/utils/fork_ci_messages.py",
        "dev/breeze/src/airflow_breeze/utils/fork_ci_reconcile.py",
        ".github/workflows/fork-ci-monitor.yml",
    }
)
GATE_JOB_NAME = "Fork CI gate"
# Jobs that also run, without needing the gate, in a run the gate left to fork CI: main's
# check-run-needed (no steps outside schedule and push runs) and the "CI passed" gate job of the
# merge ruleset, which reports as "CI passed (project CI not run)" when project CI was skipped and
# as "CI passed (run cancelled)" when a newer push cancelled the run before build-info started --
# "CI passed" in a workflow from before that name. Like the gate, each must finish within
# GATE_MAX_DURATION, so project CI work moved under one of these names counts. Each runs once, so
# the names of one job are grouped: repeating short jobs under them counts too.
GATED_RUN_JOBS = (
    frozenset({"Check if the run is needed"}),
    frozenset({"CI passed (project CI not run)", "CI passed (run cancelled)", "CI passed"}),
)
GATED_RUN_JOB_NAMES = frozenset[str]().union(*GATED_RUN_JOBS)
CHECK_RUN_NEEDED_JOB_NAME = "Check if the run is needed"
PROJECT_CI_NOT_RUN_JOB_NAME = "CI passed (project CI not run)"
BOT_LOGINS = frozenset({"github-actions", "github-actions[bot]"})
DISCOVERY_WINDOW = timedelta(hours=24)
PASS_TWO_PAGE_SIZE = 25
FORK_BATCH_SIZE = 50
# Project CI runs created this long ago or later are checked for gate tampering. Several hours
# overlap the 15-minute cadence, so a tampered run is seen even after the pull request was
# pushed again with the gate restored.
TAMPER_WINDOW = timedelta(hours=6)
# A ci-amd.yml run in which only the gate executed finishes in about a minute; only longer runs
# can have done project CI work and are worth listing jobs for.
SUSPICIOUS_RUN_DURATION = timedelta(minutes=5)
GATE_MAX_DURATION = timedelta(minutes=5)
# Paths whose content defines what fork CI runs. A pull request changing them controls its own
# fork CI result, so a green fork run cannot be trusted for it.
CI_DEFINING_PREFIXES = (
    ".github/",
    "dev/breeze/",
    "scripts/ci/",
    "scripts/docker/",
    "scripts/in_container/",
    "Dockerfile",
)
CI_DEFINING_SUFFIXES = (".pre-commit-config.yaml",)
PULL_REQUEST_FILES_CAP = 3000
COMPARE_FILES_CAP = 300
SLACK_SECTION_LIMIT = 3000
SLACK_MAX_BLOCKS = 50
SLACK_ITEM_LIMIT = 500
# Events whose runs use the workflow files of the pull request's merge commit, so a pull request
# can change what they run. pull_request_target and issue_comment use the base branch's files.
MERGE_REF_EVENTS = ("pull_request", "pull_request_review", "pull_request_review_comment")

# Jobs of every fork run whose outcome the `Fork CI / Tests (AMD)` status already shows.
UNREPORTED_JOB_NAMES = GATED_RUN_JOB_NAMES | {GATE_JOB_NAME, "Platform: AMD"}
RUNNING_JOB_STATUSES = frozenset({"queued", "waiting", "pending", "requested", "in_progress"})
JOB_STATE_RANKS = {"failure": 0, "pending": 1, "success": 2}
# Commit statuses count towards GitHub's limit of 500 content-creating requests per hour, shared by
# every workflow of the repository. At four runs an hour this keeps job statuses to at most about
# half of it; the rest follow on the next run, failures first. See "Job statuses" in the design doc.
MAX_JOB_STATUSES_PER_RUN = 60
# Fixed text, so the note is posted to Slack once while deferrals persist; the run log lists the pull
# requests.
JOB_STATUSES_DEFERRED_NOTE = (
    f"job statuses did not fit in the budget of {MAX_JOB_STATUSES_PER_RUN} writes per run and are "
    "posted by later runs; if this persists, set `job_statuses = false` in .github/fork-ci-gate.toml "
    "(see dev/AGENTS.md)"
)

RUNNING_STATUSES = frozenset({"QUEUED", "IN_PROGRESS", "PENDING", "WAITING", "REQUESTED"})
FAILED_CONCLUSIONS = frozenset({"FAILURE", "CANCELLED", "TIMED_OUT", "STARTUP_FAILURE", "ACTION_REQUIRED"})
SETUP_WORKFLOW_STATES = frozenset({"disabled_fork", "disabled_inactivity", "disabled_manually", "missing"})


class ForkState(str, Enum):
    SETUP_REQUIRED = "setup_required"
    AWAITING_PUSH = "awaiting_push"
    RUNNING = "running"
    FAILED = "failed"
    GREEN = "green"
    GREEN_UNVERIFIED = "green_unverified"
    GATE_BYPASSED = "gate_bypassed"
    CLOSED = "closed"
    RELEASED = "released"
    UNGATED = "ungated"
    GATE_MODIFIED = "gate_modified"


CLOSABLE_STATES = frozenset({ForkState.SETUP_REQUIRED, ForkState.AWAITING_PUSH})


@dataclass(frozen=True)
class WorkflowRun:
    """A run of a workflow in apache/airflow triggered by a pull_request event."""

    id: int
    path: str
    html_url: str
    head_sha: str
    head_repo: str
    head_branch: str
    created_at: datetime
    run_started_at: datetime
    updated_at: datetime
    status: str
    event: str = "pull_request"
    run_attempt: int = 1

    @property
    def workflow_file(self) -> str:
        return self.path.rsplit("/", 1)[-1]

    def is_long_running(self, now: datetime) -> bool:
        # Time spent waiting for a runner is not work, so a run that has not started is never
        # long. A running one is judged as it goes: one tampered to outlast the tamper window
        # would otherwise never be judged at all.
        if self.status == "completed":
            return self.updated_at - self.run_started_at >= SUSPICIOUS_RUN_DURATION
        return self.status == "in_progress" and now - self.run_started_at >= SUSPICIOUS_RUN_DURATION


@dataclass(frozen=True)
class PullRequestSnapshot:
    number: int
    node_id: str
    is_draft: bool
    labels: tuple[str, ...]
    author_login: str
    author_association: str
    created_at: datetime
    head_sha: str
    head_repo: str | None
    base_ref: str
    head_ref: str = ""
    last_draft_actor: str | None = None
    last_project_ci_unlabel_at: datetime | None = None
    last_reopened_at: datetime | None = None
    last_reopened_by_anyone_at: datetime | None = None
    last_base_change_at: datetime | None = None
    # (changed at, previous base, new base) for each retarget, oldest first.
    base_changes: tuple[tuple[datetime, str, str], ...] = ()
    current_status: tuple[str, str, str] | None = None
    status_created_at: datetime | None = None
    ci_passed_state: str | None = None
    marker: Marker | None = None
    marker_comment_id: int | None = None
    marker_comment_body: str | None = None
    marker_error: str | None = None
    # Set when the state comment was not among the comments fetched with the pull request and
    # more comments follow; the reconciler pages from here before deciding anything.
    comments_cursor: str | None = None
    # Set when the pull request's last listed commit is not its head: GitHub orders the commit list
    # by date, so after a rebase that keeps old dates the statuses read with it belong to another
    # commit. The reconciler reads the head's statuses before deciding anything.
    head_status_unread: bool = False


@dataclass(frozen=True)
class ForkObservation:
    suite_status: str | None = None
    suite_conclusion: str | None = None
    run_url: str | None = None
    run_id: int | None = None
    run_attempt: int | None = None
    workflow_state: str | None = None
    error: str | None = None


@dataclass
class Plan:
    state: ForkState | None = None
    add_labels: list[str] = field(default_factory=list)
    remove_labels: list[str] = field(default_factory=list)
    draft: bool | None = None
    status: tuple[str, str, str] | None = None
    # (state, target URL, description) of the `CI passed` status.
    ci_passed: tuple[str, str, str] | None = None
    marker_comment: str | None = None
    new_comments: list[str] = field(default_factory=list)
    close: bool = False
    attention: list[str] = field(default_factory=list)
    unknown: list[str] = field(default_factory=list)


def _set_label(plan: Plan, labels: set[str], label: str, *, present: bool) -> None:
    if present and label not in labels:
        plan.add_labels.append(label)
    elif not present and label in labels:
        plan.remove_labels.append(label)


def _workflow_file(resource_path: str | None) -> str:
    return (resource_path or "").rsplit("/", 1)[-1]


def _is_bot(login: str | None) -> bool:
    return login in BOT_LOGINS


def observe_fork_state(fork: ForkObservation, marker: Marker | None) -> tuple[ForkState | None, str | None]:
    """Map what the fork shows for the head commit to a state, or return the reason it cannot."""
    if fork.error:
        return None, fork.error
    if fork.suite_status is not None:
        if fork.suite_status in RUNNING_STATUSES:
            return ForkState.RUNNING, None
        if fork.suite_status != "COMPLETED":
            return None, f"unexpected fork check suite status {fork.suite_status!r}"
        if fork.suite_conclusion == "SUCCESS":
            return ForkState.GREEN, None
        if fork.suite_conclusion in FAILED_CONCLUSIONS:
            return ForkState.FAILED, None
        return None, f"unexpected fork check suite conclusion {fork.suite_conclusion!r}"
    if fork.workflow_state is None:
        return None, "no fork run and the fork workflow state was not read"
    if fork.workflow_state in SETUP_WORKFLOW_STATES:
        return ForkState.SETUP_REQUIRED, None
    if fork.workflow_state == "active":
        return ForkState.AWAITING_PUSH, None
    return None, f"unexpected fork workflow state {fork.workflow_state!r}"


def has_job_started(job: dict[str, Any]) -> bool:
    # A job that never reached a runner -- queued, or cancelled while queued -- has no steps and a
    # `runner_id` of null or 0, but its `status` can be "completed" and its `started_at` set.
    return bool(job.get("runner_id")) or bool(job.get("steps"))


def jobs_show_project_ci(jobs: Iterable[dict[str, Any]], now: datetime) -> bool:
    """
    Whether the jobs of a ci-amd.yml run show more than the gate deciding to skip.

    Any started job other than the gate and the ``GATED_RUN_JOB_NAMES`` jobs that was not skipped
    counts, and so does a gate job that is missing or duplicated, one of those jobs duplicated, or
    one of them running for
    longer than they ever need, which is how a renamed job doing project CI work under their name
    shows up. Jobs that no runner picked up do not count: a queued job reports its creation as
    ``started_at``, and a run cancelled by a newer push marks its never-started jobs completed.
    """
    jobs = list(jobs)
    gate_jobs = [job for job in jobs if job["name"] == GATE_JOB_NAME]
    if len(gate_jobs) != 1:
        return True
    if any(sum(job["name"] in names for job in jobs) > 1 for names in GATED_RUN_JOBS):
        return True
    expected_names = GATED_RUN_JOB_NAMES | {GATE_JOB_NAME}
    for job in jobs:
        if job["name"] not in expected_names or not has_job_started(job):
            continue
        started = parse_github_datetime(job.get("started_at") or "")
        completed = parse_github_datetime(job.get("completed_at") or "") or now
        if started is not None and completed - started > GATE_MAX_DURATION:
            return True
    return any(
        job["name"] not in expected_names and has_job_started(job) and job.get("conclusion") != "skipped"
        for job in jobs
    )


def is_gated_skip_run(jobs: Iterable[dict[str, Any]], now: datetime) -> bool:
    """
    Whether a completed ci-amd.yml run is only the gate leaving the pull request to fork CI.

    Such a run lists every project CI job as a skipped check on the pull request, next to the
    fork's result in the `Fork CI / Tests (AMD)` status. The gate and the check-run-needed job
    succeeded, CI passed reported as not run, and every other job was skipped.
    """
    jobs = list(jobs)
    if jobs_show_project_ci(jobs, now):
        return False
    succeeded = {GATE_JOB_NAME, CHECK_RUN_NEEDED_JOB_NAME, PROJECT_CI_NOT_RUN_JOB_NAME}
    if sorted(job["name"] for job in jobs if job["name"] in succeeded) != sorted(succeeded):
        return False
    return all(
        job.get("conclusion") == ("success" if job["name"] in succeeded else "skipped") for job in jobs
    )


def touches(files: Iterable[str], prefixes: tuple[str, ...]) -> bool:
    return any(path.startswith(prefixes) for path in files)


def changed_paths(entries: Iterable[dict[str, Any]]) -> list[str]:
    """
    Every path a changed-file listing touches.

    A rename lists only its new path as ``filename``; moving a file out of a CI-defining directory
    changes that directory too, so the old path counts as well.
    """
    paths = []
    for entry in entries:
        paths.append(entry["filename"])
        if entry.get("previous_filename"):
            paths.append(entry["previous_filename"])
    return paths


def changes_ci_definition(files: Sequence[str], *, file_count: int | None = None) -> bool:
    """
    Whether a pull request's changed files can alter what its fork CI runs.

    :param file_count: entries in the listing, when ``files`` also holds the old paths of renames.
    """
    # GitHub lists at most 3000 files of a pull request; a longer one is not trusted.
    return (
        (len(files) if file_count is None else file_count) >= PULL_REQUEST_FILES_CAP
        or touches(files, CI_DEFINING_PREFIXES)
        or any(path.endswith(CI_DEFINING_SUFFIXES) for path in files)
    )


def _gate_event(pr: PullRequestSnapshot, repo: str) -> GateEvent:
    return GateEvent(
        event_name="pull_request",
        repository=repo,
        ref="",
        head_repository=pr.head_repo or "",
        author_login=pr.author_login,
        author_association=pr.author_association,
        created_at=pr.created_at,
        labels=pr.labels,
    )


def status_state(status: tuple[str, str, str] | None) -> str | None:
    """The state the reconciler recorded in a commit status it posted, if it posted it."""
    if status is None:
        return None
    return next(
        (
            state
            for state, github_state in STATUS_STATES.items()
            if (github_state, status_description(state)) == (status[0], status[2])
        ),
        None,
    )


def _set_status(
    plan: Plan, pr: PullRequestSnapshot, state: str, target_url: str, *, force: bool = False
) -> None:
    desired = (STATUS_STATES[state], target_url, status_description(state))
    if force or desired != pr.current_status:
        plan.status = desired


def _set_ci_passed(plan: Plan, pr: PullRequestSnapshot, *, passed: bool, target_url: str) -> None:
    """
    Report fork CI to the `CI passed` check the merge ruleset requires.

    Only success is ever posted: a gated pull request without the status cannot merge, which is
    what a pending or failing one would say. A success that no longer holds for the head commit,
    such as a failed re-run in the fork, is overwritten so the pull request cannot merge on it.
    """
    if passed and pr.ci_passed_state != "success":
        plan.ci_passed = ("success", target_url, CI_PASSED_DESCRIPTION)
    elif not passed and pr.ci_passed_state == "success":
        plan.ci_passed = ("pending", target_url, CI_PASSED_REVOKED_DESCRIPTION)


def _set_marker_comment(plan: Plan, pr: PullRequestSnapshot, body: str) -> None:
    if body != pr.marker_comment_body:
        plan.marker_comment = body


def classify(
    pr: PullRequestSnapshot,
    fork: ForkObservation,
    *,
    config: GateConfig | None,
    now: datetime,
    repo: str,
    find_bypass_run: Callable[[], str | None],
    changes_ci_files: Callable[[], bool | None],
    count_behind: Callable[[], int | None] = lambda: None,
    jobs_reported: str | None = None,
    head_shared_across_bases: bool = False,
    find_gate_change: Callable[[], str | None] = lambda: None,
) -> Plan:
    """
    Decide the mutations for one gated pull request.

    :param config: the gate configuration of the pull request's base branch; None when the base
        branch has no gate, so its gate job runs project CI.
    :param find_bypass_run: returns the project CI run that bypassed the gate, if any; only
        called for pull requests that the gate tampering check applies to.
    :param changes_ci_files: whether the pull request changes CI-defining paths, None when that
        cannot be told for the snapshot's head commit; only called when a new head commit turns
        green in the fork. On None nothing is changed, since the decision would be recorded on a
        commit the answer may not belong to.
    :param count_behind: how many commits the head commit is behind the base branch, None when
        unknown; called at the same time as ``changes_ci_files``.
    :param jobs_reported: the completed fork run whose jobs are all reported as statuses, recorded
        in the marker so that later runs do not read its jobs again.
    :param head_shared_across_bases: another gated pull request has the same head commit and a
        different base, so their changed files differ but they share one commit status.
    :param find_gate_change: describes how the pull request changes the fork CI gate, or returns
        None; only called for pull requests that the gate tampering check applies to, once per
        head commit.
    """
    plan = Plan()
    labels = set(pr.labels)
    marker = pr.marker
    if pr.marker_error:
        plan.unknown.append(pr.marker_error)
        marker = None

    def release(state: ForkState, comment: Callable[[Marker], str]) -> Plan:
        plan.state = state
        plan.remove_labels.append(LABEL_FORK_CI_REQUIRED)
        _set_label(plan, labels, LABEL_NEEDS_PROJECT_CI, present=False)
        if pr.is_draft and _is_bot(pr.last_draft_actor):
            plan.draft = False
        since = marker.since if marker and marker.state == state.value else now
        _set_marker_comment(plan, pr, comment(Marker(state.value, pr.head_sha, since)))
        _set_status(plan, pr, state.value, f"https://github.com/{repo}/pull/{pr.number}/checks")
        return plan

    if LABEL_USE_PROJECT_CI in labels:
        if LABEL_USE_FORK_CI in labels:
            plan.attention.append("carries both `use project ci` and `use fork ci`; project CI wins")
        return release(ForkState.RELEASED, released_comment)

    if config is None:
        return release(ForkState.UNGATED, functools.partial(ungated_comment, base_ref=pr.base_ref))

    if pr.head_repo is None:
        plan.unknown.append("head repository no longer exists")
        return plan

    reason = decide(_gate_event(pr, repo), config).reason
    if reason in TAMPER_CHECKED_REASONS:
        bypass_run = find_bypass_run()
        if bypass_run is not None:
            plan.state = ForkState.GATE_BYPASSED
            if not pr.is_draft:
                plan.draft = True
            if LABEL_READY_FOR_REVIEW in labels:
                plan.remove_labels.append(LABEL_READY_FOR_REVIEW)
            _set_label(plan, labels, LABEL_NEEDS_PROJECT_CI, present=False)
            _set_status(plan, pr, ForkState.GATE_BYPASSED.value, bypass_run)
            _set_marker_comment(
                plan,
                pr,
                state_comment(
                    Marker(ForkState.GATE_BYPASSED.value, pr.head_sha, now),
                    fork=pr.head_repo,
                    run_url=bypass_run,
                ),
            )
            plan.new_comments.append(tamper_close_comment(run_url=bypass_run))
            plan.close = True
            plan.attention.append(f"closed for gate tampering (author @{pr.author_login}, run {bypass_run})")
            return plan

    # A closing state in the marker of an open pull request that was not reopened since means that
    # posting the explanation or closing failed on an earlier run: finish the closure. Only a
    # maintainer's reopen lifts a closure for gate tampering, as for the tampering check itself.
    reopened_since_closing = {
        ForkState.CLOSED.value: pr.last_reopened_by_anyone_at,
        ForkState.GATE_BYPASSED.value: pr.last_reopened_at,
        ForkState.GATE_MODIFIED.value: pr.last_reopened_at,
    }
    if marker and marker.state in reopened_since_closing:
        reopened = reopened_since_closing[marker.state]
        if reopened is None or reopened <= marker.since:
            plan.state = ForkState(marker.state)
            _set_label(plan, labels, LABEL_NEEDS_PROJECT_CI, present=False)
            if plan.state == ForkState.CLOSED:
                comment = inactivity_close_comment(days=config.close_after_days, fork=pr.head_repo)
            elif plan.state == ForkState.GATE_MODIFIED:
                comment = gate_modified_close_comment(change=None)
            else:
                # The status written with the marker links the run that bypassed the gate.
                bypass_url = (
                    pr.current_status[1]
                    if pr.current_status and status_state(pr.current_status) == ForkState.GATE_BYPASSED.value
                    else f"https://github.com/{repo}/pull/{pr.number}/checks"
                )
                comment = tamper_close_comment(run_url=bypass_url)
            plan.new_comments.append(comment)
            plan.close = True
            return plan

    # A pull request whose CI runs in a fork must not change the gate that sent it there. Checked
    # once per head commit: the marker records a head commit as checked only when the check ran
    # for it (not while `use fork ci` exempted it), and a retarget since the state began changes
    # what the pull request changes. A
    # maintainer's reopen of one closed for it leaves the marker at gate_modified, so reopening
    # without restoring the gate closes it again; `use project ci` is the way to accept the change.
    gate_checked = reason in TAMPER_CHECKED_REASONS
    already_checked = (
        marker is not None
        and marker.gate_checked
        and marker.sha == pr.head_sha
        and (pr.last_base_change_at is None or pr.last_base_change_at < marker.since)
    )
    if gate_checked and not already_checked:
        change = find_gate_change()
        if change is not None:
            plan.state = ForkState.GATE_MODIFIED
            if not pr.is_draft:
                plan.draft = True
            _set_label(plan, labels, LABEL_READY_FOR_REVIEW, present=False)
            _set_label(plan, labels, LABEL_NEEDS_PROJECT_CI, present=False)
            _set_status(
                plan, pr, ForkState.GATE_MODIFIED.value, f"https://github.com/{repo}/pull/{pr.number}/files"
            )
            _set_marker_comment(
                plan,
                pr,
                state_comment(
                    Marker(ForkState.GATE_MODIFIED.value, pr.head_sha, now), fork=pr.head_repo, run_url=None
                ),
            )
            plan.new_comments.append(gate_modified_close_comment(change=change))
            plan.close = True
            plan.attention.append(
                f"closed for changing the fork CI gate (author @{pr.author_login}): {change}"
            )
            return plan

    state, unknown_reason = observe_fork_state(fork, marker)
    if state is None:
        plan.unknown.append(unknown_reason or "unknown fork state")
        return plan
    refresh_status = False
    behind = marker.behind if marker and marker.sha == pr.head_sha else None
    if state == ForkState.GREEN:
        # The decision is read back from the commit status on the head commit, which only
        # apache/airflow's own tokens can write, never from a comment. The changed files depend
        # on the base too, so a status written before the pull request was retargeted is stale.
        previous = status_state(pr.current_status)
        decided = previous in (ForkState.GREEN.value, ForkState.GREEN_UNVERIFIED.value)
        stale = pr.last_base_change_at is not None and (
            pr.status_created_at is None or pr.status_created_at <= pr.last_base_change_at
        )
        if head_shared_across_bases:
            # One commit status cannot hold a decision per base, and trusting the one written for
            # the other pull request could skip the check; both wait for a maintainer instead.
            unverified = True
            if previous != ForkState.GREEN_UNVERIFIED.value:
                plan.attention.append(
                    "fork CI is green, but another gated pull request to a different base has the "
                    "same head commit, so its CI-defining changes cannot be told apart; review and add "
                    "`use project ci` to verify"
                )
        elif decided and not stale:
            unverified = previous == ForkState.GREEN_UNVERIFIED.value
        else:
            # Writing the status again, even unchanged, records when this decision was made.
            refresh_status = decided
            # The `needs project ci` label lists these pull requests for maintainers.
            changes = changes_ci_files()
            if changes is None:
                return Plan()
            unverified = changes
            behind = count_behind()
        if unverified:
            state = ForkState.GREEN_UNVERIFIED
    plan.state = state
    since = marker.since if marker and marker.state == state.value else now

    if marker and marker.state == ForkState.SETUP_REQUIRED.value and state == ForkState.AWAITING_PUSH:
        plan.new_comments.append(nudge_comment(fork=pr.head_repo))
    # A new comment, because the edited state comment would not notify the author that fork CI they
    # had working stopped running.
    if (
        marker
        and marker.state not in (ForkState.SETUP_REQUIRED.value, ForkState.CLOSED.value)
        and state == ForkState.SETUP_REQUIRED
    ):
        plan.new_comments.append(
            workflow_disabled_comment(fork=pr.head_repo, workflow_state=fork.workflow_state)
        )

    if state in CLOSABLE_STATES and now - since >= timedelta(days=config.close_after_days):
        plan.state = ForkState.CLOSED
        _set_label(plan, labels, LABEL_NEEDS_PROJECT_CI, present=False)
        _set_marker_comment(
            plan,
            pr,
            state_comment(
                Marker(ForkState.CLOSED.value, pr.head_sha, now, gate_checked=gate_checked),
                fork=pr.head_repo,
                run_url=None,
            ),
        )
        plan.new_comments.append(inactivity_close_comment(days=config.close_after_days, fork=pr.head_repo))
        plan.close = True
        return plan

    if state == ForkState.GREEN:
        if pr.is_draft and _is_bot(pr.last_draft_actor):
            plan.draft = False
        ready = not pr.is_draft or plan.draft is False
        if ready and LABEL_READY_FOR_REVIEW not in labels:
            plan.add_labels.append(LABEL_READY_FOR_REVIEW)
    else:
        if not pr.is_draft:
            plan.draft = True
        if LABEL_READY_FOR_REVIEW in labels:
            plan.remove_labels.append(LABEL_READY_FOR_REVIEW)
    # Lets maintainers list the pull requests that only wait for them to run project CI, which
    # otherwise look like any other draft with a pending fork CI status.
    _set_label(plan, labels, LABEL_NEEDS_PROJECT_CI, present=state == ForkState.GREEN_UNVERIFIED)

    target_url = fork.run_url or fork_actions_url(pr.head_repo)
    _set_status(plan, pr, state.value, target_url, force=refresh_status)
    _set_ci_passed(plan, pr, passed=state == ForkState.GREEN, target_url=target_url)
    green = state in (ForkState.GREEN, ForkState.GREEN_UNVERIFIED)
    _set_marker_comment(
        plan,
        pr,
        state_comment(
            Marker(
                state.value,
                pr.head_sha,
                since,
                behind=behind if green else None,
                jobs=jobs_reported,
                gate_checked=gate_checked,
            ),
            fork=pr.head_repo,
            run_url=fork.run_url,
            base_ref=pr.base_ref,
        ),
    )
    return plan


# ---------------------------------------------------------------------------------------------
# GitHub data gathering
# ---------------------------------------------------------------------------------------------

DISCOVERY_QUERY = """
query($q: String!, $cursor: String) {
  search(type: ISSUE, first: 100, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes { ... on PullRequest {
      number createdAt authorAssociation baseRefName
      author { login }
      headRepository { nameWithOwner }
      labels(first: 50) { nodes { name } }
    } }
  }
}
"""

PASS_TWO_QUERY = """
query($q: String!, $cursor: String) {
  search(type: ISSUE, first: %(page)d, after: $cursor, query: $q) {
    pageInfo { hasNextPage endCursor }
    nodes { ... on PullRequest {
      number id isDraft createdAt authorAssociation baseRefName headRefName headRefOid
      author { login }
      headRepository { nameWithOwner }
      labels(first: 50) { nodes { name } }
      timelineItems(
        last: 100,
        itemTypes: [CONVERT_TO_DRAFT_EVENT, UNLABELED_EVENT, REOPENED_EVENT, BASE_REF_CHANGED_EVENT]
      ) {
        nodes {
          __typename
          ... on ConvertToDraftEvent { createdAt actor { login } }
          ... on UnlabeledEvent { createdAt label { name } }
          ... on ReopenedEvent { createdAt actor { login } }
          ... on BaseRefChangedEvent { createdAt previousRefName currentRefName }
        }
      }
      commits(last: 1) { nodes { commit {
        oid
        status {
          context(name: "%(context)s") { state targetUrl description createdAt }
          ciPassed: context(name: "%(ci_passed)s") { state }
        }
      } } }
      comments(first: 100) {
        pageInfo { hasNextPage endCursor }
        nodes { databaseId body author { login } }
      }
    } }
  }
}
"""


def _labels(node: dict[str, Any]) -> tuple[str, ...]:
    return tuple(label["name"] for label in (node.get("labels") or {}).get("nodes") or [])


def _login(node: dict[str, Any] | None) -> str:
    return (node or {}).get("login") or ""


def find_marker_comment(
    comments: Iterable[dict[str, Any]],
) -> tuple[Marker | None, int, str, str | None] | None:
    """
    Return ``(marker, comment id, body, parse error)`` of the state comment, None if not among these.

    The state comment is the earliest bot comment with a marker: the reconciler creates it on
    first contact and only edits it afterwards. Later ones may come from other workflows that echo
    pull-request-controlled text, so they are never trusted.
    """
    for comment in comments:
        if not _is_bot(_login(comment.get("author"))):
            continue
        try:
            parsed = parse_marker(comment["body"])
        except ValueError as e:
            return None, comment["databaseId"], comment["body"], str(e)
        if parsed is not None:
            return parsed, comment["databaseId"], comment["body"], None
    return None


def parse_pull_request(node: dict[str, Any]) -> PullRequestSnapshot:
    last_draft_actor = None
    last_unlabel = None
    last_reopened = None
    last_reopened_by_anyone = None
    last_base_change = None
    base_changes = []
    for item in (node.get("timelineItems") or {}).get("nodes") or []:
        if item["__typename"] == "ConvertToDraftEvent":
            last_draft_actor = _login(item.get("actor"))
        elif item["__typename"] == "BaseRefChangedEvent":
            last_base_change = parse_github_datetime(item["createdAt"])
            base_changes.append(
                (last_base_change, item.get("previousRefName") or "", item.get("currentRefName") or "")
            )
        elif item["__typename"] == "ReopenedEvent":
            last_reopened_by_anyone = parse_github_datetime(item["createdAt"])
            # Only a reopen by someone else counts for the gate tampering check: an author closing
            # and reopening their own pull request must not hide a tampered run from it.
            if _login(item.get("actor")).casefold() != _login(node.get("author")).casefold():
                last_reopened = last_reopened_by_anyone
        elif item["__typename"] == "UnlabeledEvent":
            label = (item.get("label") or {}).get("name")
            if label == LABEL_USE_PROJECT_CI:
                last_unlabel = parse_github_datetime(item["createdAt"])

    current_status = None
    status_created_at = None
    ci_passed_state = None
    commits = (node.get("commits") or {}).get("nodes") or []
    head_status_unread = bool(commits) and commits[0]["commit"].get("oid") != node["headRefOid"]
    if commits and not head_status_unread:
        status = commits[0]["commit"].get("status") or {}
        if status.get("ciPassed"):
            ci_passed_state = status["ciPassed"]["state"].lower()
        context = status.get("context")
        if context:
            current_status = (
                context["state"].lower(),
                context["targetUrl"] or "",
                context["description"] or "",
            )
            status_created_at = parse_github_datetime(context.get("createdAt") or "")

    comments = node.get("comments") or {}
    found = find_marker_comment(comments.get("nodes") or [])
    page_info = comments.get("pageInfo") or {}
    comments_cursor = page_info.get("endCursor") if found is None and page_info.get("hasNextPage") else None
    marker = marker_id = marker_body = marker_error = None
    if found is not None:
        marker, marker_id, marker_body, marker_error = found

    head_repo = (node.get("headRepository") or {}).get("nameWithOwner")
    return PullRequestSnapshot(
        number=node["number"],
        node_id=node["id"],
        is_draft=node["isDraft"],
        labels=_labels(node),
        author_login=_login(node.get("author")),
        author_association=node["authorAssociation"],
        created_at=parse_github_datetime(node["createdAt"]),  # type: ignore[arg-type]
        head_sha=node["headRefOid"],
        head_repo=head_repo,
        base_ref=node["baseRefName"],
        head_ref=node["headRefName"],
        last_draft_actor=last_draft_actor,
        last_project_ci_unlabel_at=last_unlabel,
        last_reopened_at=last_reopened,
        last_reopened_by_anyone_at=last_reopened_by_anyone,
        last_base_change_at=last_base_change,
        base_changes=tuple(base_changes),
        current_status=current_status,
        status_created_at=status_created_at,
        ci_passed_state=ci_passed_state,
        marker=marker,
        marker_comment_id=marker_id,
        marker_comment_body=marker_body,
        marker_error=marker_error,
        comments_cursor=comments_cursor,
        head_status_unread=head_status_unread,
    )


def _search(client: GitHubClient, query: str, search_query: str) -> Iterable[dict[str, Any]]:
    cursor = None
    while True:
        result = client.graphql(query, {"q": search_query, "cursor": cursor})["search"]
        yield from (node for node in result["nodes"] if node)
        if not result["pageInfo"]["hasNextPage"]:
            return
        cursor = result["pageInfo"]["endCursor"]


def _is_not_found(error: GitHubError) -> bool:
    return "404" in str(error) or "Not Found" in str(error)


def read_file(client: GitHubClient, repo: str, path: str, ref: str) -> str | None:
    """Return a file's content at a ref, or None when it does not exist there."""
    try:
        content = client.rest_get(f"repos/{repo}/contents/{path}?ref={quote(ref, safe='')}")
    except GitHubError as e:
        if _is_not_found(e):
            return None
        raise
    return base64.b64decode(content["content"]).decode()


class BaseGateConfigs:
    """
    The gate configuration of each base branch, as the gate job of a pull request against it reads it.

    The default branch's configuration comes from the reconciler's own checkout; any other base
    branch's is read from GitHub once per run.
    """

    def __init__(self, client: GitHubClient, *, repo: str, default_branch: str, default_config: GateConfig):
        self._client = client
        self._repo = repo
        self._configs: dict[str, GateConfig | None] = {default_branch: default_config}

    def _read(self, path: str, base_ref: str) -> str | None:
        return read_file(self._client, self._repo, path, base_ref)

    def get(self, base_ref: str) -> GateConfig | None:
        """Return the base branch's configuration, or None when the base branch has no gate."""
        if base_ref not in self._configs:
            config_text = self._read(GATE_CONFIG_PATH, base_ref)
            has_script = self._read(GATE_SCRIPT_PATH, base_ref) is not None
            if config_text is None and not has_script:
                self._configs[base_ref] = None
            elif config_text is None or not has_script:
                # The gate job fails for these pull requests instead of deciding; so does this.
                raise ValueError(
                    f"base branch {base_ref} has only one of {GATE_CONFIG_PATH} and {GATE_SCRIPT_PATH}"
                )
            else:
                self._configs[base_ref] = parse_config(config_text)
        return self._configs[base_ref]


def discover(
    client: GitHubClient,
    configs: BaseGateConfigs,
    *,
    repo: str,
    now: datetime,
    unknown: list[tuple[int, str]] | None = None,
) -> list[int]:
    """
    Label newly gated pull requests with `fork ci required` and return their numbers.

    :param unknown: collects the pull requests whose base branch configuration could not be read.
    """
    since = (now - DISCOVERY_WINDOW).strftime("%Y-%m-%dT%H:%M:%SZ")
    queries = (
        f'repo:{repo} is:pr is:open updated:>={since} -label:"{LABEL_FORK_CI_REQUIRED}"',
        f'repo:{repo} is:pr is:open label:"{LABEL_USE_FORK_CI}" -label:"{LABEL_FORK_CI_REQUIRED}"',
    )
    labelled: list[int] = []
    for search_query in queries:
        for node in _search(client, DISCOVERY_QUERY, search_query):
            if node["number"] in labelled:
                continue
            try:
                config = configs.get(node["baseRefName"])
            except (GitHubError, ValueError) as e:
                if unknown is not None:
                    unknown.append((node["number"], f"{type(e).__name__}: {e}"))
                continue
            if config is None:
                continue
            event = GateEvent(
                event_name="pull_request",
                repository=repo,
                ref="",
                head_repository=(node.get("headRepository") or {}).get("nameWithOwner") or "",
                author_login=_login(node.get("author")),
                author_association=node["authorAssociation"],
                created_at=parse_github_datetime(node["createdAt"]),
                labels=_labels(node),
            )
            if decide(event, config).reason in GATED_REASONS:
                client.add_labels(node["number"], [LABEL_FORK_CI_REQUIRED])
                labelled.append(node["number"])
    return labelled


def fetch_fork_observations(
    client: GitHubClient, prs: list[PullRequestSnapshot]
) -> dict[int, ForkObservation]:
    observations: dict[int, ForkObservation] = {}
    with_fork = [pr for pr in prs if pr.head_repo]
    for start in range(0, len(with_fork), FORK_BATCH_SIZE):
        batch = with_fork[start : start + FORK_BATCH_SIZE]
        declarations, fields, variables = [], [], {}
        for index, pr in enumerate(batch):
            owner, name = pr.head_repo.split("/", 1)  # type: ignore[union-attr]
            declarations.append(f"$o{index}: String!, $n{index}: String!, $s{index}: GitObjectID!")
            fields.append(
                f"pr{index}: repository(owner: $o{index}, name: $n{index}) {{ object(oid: $s{index}) {{ "
                "... on Commit { checkSuites(first: 30) { nodes { createdAt status conclusion "
                "workflowRun { databaseId runAttempt url event workflow { resourcePath } } } } } } }"
            )
            variables.update({f"o{index}": owner, f"n{index}": name, f"s{index}": pr.head_sha})
        query = f"query({', '.join(declarations)}) {{ {' '.join(fields)} }}"
        data = client.graphql(query, variables, allow_partial=True)
        for index, pr in enumerate(batch):
            repository = data.get(f"pr{index}")
            if repository is None:
                observations[pr.number] = ForkObservation(error=f"fork {pr.head_repo} is not readable")
                continue
            commit = repository.get("object") or {}
            # Only a push run executes the workflow from the head commit itself. A pull_request
            # run inside the fork (into the fork's own main) runs a merge the contributor controls
            # and could be made to pass trivially.
            suites = [
                suite
                for suite in (commit.get("checkSuites") or {}).get("nodes") or []
                if (run := suite.get("workflowRun") or {}).get("event") == "push"
                and _workflow_file((run.get("workflow") or {}).get("resourcePath")) == CI_WORKFLOW_FILE
            ]
            if not suites:
                observations[pr.number] = ForkObservation()
                continue
            latest = max(suites, key=lambda suite: suite["createdAt"])
            observations[pr.number] = ForkObservation(
                suite_status=latest["status"],
                suite_conclusion=latest["conclusion"],
                run_url=latest["workflowRun"]["url"],
                run_id=latest["workflowRun"].get("databaseId"),
                run_attempt=latest["workflowRun"].get("runAttempt"),
            )
    return observations


def build_job_statuses(jobs: Iterable[dict[str, Any]]) -> dict[str, tuple[str, str, str]]:
    """The ``(state, target URL, description)`` to report for each queued or started fork job, by context."""
    statuses: dict[str, tuple[str, str, str]] = {}
    # GitHub compares contexts case-insensitively, so differently capitalised names are one context.
    contexts: dict[str, str] = {}
    for job in jobs:
        if job["name"] in UNREPORTED_JOB_NAMES:
            continue
        if job.get("status") in RUNNING_JOB_STATUSES:
            outcome = "running"
        else:
            outcome = job.get("conclusion") or ""
        if outcome not in JOB_STATUSES:
            # Skipped and neutral jobs did no work worth a check on the pull request.
            continue
        state, description = JOB_STATUSES[outcome]
        context = contexts.setdefault(
            job_status_context(job["name"]).casefold(), job_status_context(job["name"])
        )
        # Jobs can share a name, or two names can be the same once shortened; the worst one counts.
        reported = statuses.get(context)
        if reported is None or JOB_STATE_RANKS[state] < JOB_STATE_RANKS[reported[0]]:
            statuses[context] = (state, job["html_url"], description)
    return statuses


class JobStatusReporter:
    """Reports each job of a pull request's fork run as a commit status, within a per-run budget."""

    def __init__(self, client: GitHubClient, *, repo: str, budget: int = MAX_JOB_STATUSES_PER_RUN):
        self._client = client
        self._repo = repo
        self.remaining = budget
        self.deferred: list[int] = []

    def report(self, pr: PullRequestSnapshot, fork: ForkObservation) -> str | None:
        """
        Post the job statuses that changed; return the completed run's key once all are reported.

        The key is recorded in the marker, so a completed run's jobs are read once. A run in
        progress, or one whose statuses did not all fit in the budget, is read again next time. The
        key holds the run attempt, so a re-run in the fork is read again even when it ends with the
        same conclusion.
        """
        if fork.run_id is None or not pr.head_repo:
            return None
        key = (
            f"{fork.run_id}:{fork.run_attempt}:{fork.suite_conclusion}"
            if fork.suite_status == "COMPLETED"
            else None
        )
        recorded = pr.marker.jobs if pr.marker is not None and pr.marker.sha == pr.head_sha else None
        if key is not None and key == recorded:
            return key
        # Without writes left, the reads are spent for nothing: a backlog of hundreds of pull requests
        # would otherwise cost two reads each on every run.
        if self.remaining == 0:
            self.deferred.append(pr.number)
            return None
        jobs = self._client.rest_get(
            f"repos/{pr.head_repo}/actions/runs/{fork.run_id}/jobs?filter=latest&per_page=100",
            paginate=True,
            items_key="jobs",
        )
        # By the case-folded context, as GitHub compares them.
        current = {
            status["context"].casefold(): (
                status["context"],
                (status["state"], status.get("target_url") or "", status.get("description") or ""),
            )
            for status in self._client.rest_get(
                f"repos/{self._repo}/commits/{pr.head_sha}/status?per_page=100",
                paginate=True,
                items_key="statuses",
            )
        }
        desired = build_job_statuses(jobs)
        if key is not None:
            # Jobs of an earlier run of this commit that the completed run did not run, or skipped.
            wanted = {context.casefold() for context in desired}
            for folded, (context, (_, target_url, _)) in current.items():
                if context.startswith(JOB_STATUS_CONTEXT_PREFIX) and folded not in wanted:
                    desired[context] = (STALE_JOB_STATUS[0], target_url, STALE_JOB_STATUS[1])
        changes = sorted(
            (
                (context, status)
                for context, status in desired.items()
                if current.get(context.casefold(), ("", None))[1] != status
            ),
            key=lambda change: (JOB_STATE_RANKS[change[1][0]], change[0]),
        )
        for context, (state, target_url, description) in changes:
            if self.remaining == 0:
                self.deferred.append(pr.number)
                return None
            self._client.set_status(pr.number, pr.head_sha, state, target_url, description, context=context)
            # Charged per write, so writes made before a later one fails still count.
            self.remaining -= 1
        return key


def fetch_workflow_state(client: GitHubClient, fork: str) -> str:
    try:
        return client.rest_get(f"repos/{fork}/actions/workflows/{CI_WORKFLOW_FILE}")["state"]
    except GitHubError as e:
        if "404" in str(e) or "Not Found" in str(e):
            return "missing"
        raise


def parse_workflow_run(run: dict[str, Any]) -> WorkflowRun:
    return WorkflowRun(
        id=run["id"],
        path=run["path"],
        html_url=run["html_url"],
        head_sha=run["head_sha"],
        head_repo=(run.get("head_repository") or {}).get("full_name") or "",
        head_branch=run.get("head_branch") or "",
        created_at=parse_github_datetime(run["created_at"]),  # type: ignore[arg-type]
        run_started_at=parse_github_datetime(run.get("run_started_at") or run["created_at"]),  # type: ignore[arg-type]
        updated_at=parse_github_datetime(run["updated_at"]),  # type: ignore[arg-type]
        status=run["status"],
        event=run["event"],
        run_attempt=run.get("run_attempt") or 1,
    )


def get_workflow_triggers(workflow: dict[Any, Any]) -> frozenset[str]:
    """The events a parsed workflow file is triggered by."""
    # PyYAML reads the bare `on:` key as the boolean True.
    on = workflow.get("on", workflow.get(True))
    if isinstance(on, str):
        return frozenset({on})
    if isinstance(on, list):
        return frozenset(on)
    if isinstance(on, dict):
        return frozenset(on)
    return frozenset()


def load_workflow_triggers(workflows_dir: Path) -> dict[str, frozenset[str]]:
    """Map each workflow file name of a checkout to the events that trigger it."""
    triggers = {}
    for path in sorted(workflows_dir.glob("*.y*ml")):
        workflow = yaml.safe_load(path.read_text()) or {}
        triggers[path.name] = get_workflow_triggers(workflow if isinstance(workflow, dict) else {})
    return triggers


def base_at(pr: PullRequestSnapshot, when: datetime) -> str:
    """The base branch the pull request had at ``when``, from its retargets."""
    base = pr.base_ref
    for changed_at, previous, _ in reversed(pr.base_changes):
        if when < changed_at:
            base = previous
    return base


class BypassFinder:
    """
    Finds project CI runs that bypassed the gate, from the runs apache/airflow recorded.

    It looks at every recent run of a pull request's branch for the events that run workflow
    files from the pull request's merge commit, not only the runs of its head commit, so pushing a
    tampered commit and then one that restores the gate does not hide the tampered run. Both
    signals of the design must hold for a run: it did project CI work, and its own commit changes
    `.github/workflows/`. A run did project CI work when the base branch's version of its
    workflow is not triggered by the run's event (an injected workflow, or a trigger the pull
    request added to an existing one, such as `pull_request` on ci-arm.yml) and a job of it ran,
    or when it is a long ci-amd.yml run whose jobs show more than the gate.
    """

    def __init__(
        self,
        client: GitHubClient,
        *,
        repo: str,
        now: datetime,
        base_triggers: dict[str, frozenset[str]],
        is_gated_base: Callable[[str], bool] = lambda base_ref: True,
    ):
        """:param is_gated_base: whether a base branch has the gate; runs against one without it are project CI by design."""
        self._client = client
        self._repo = repo
        self._now = now
        self._base_triggers = base_triggers
        self._is_gated_base = is_gated_base
        # Pull requests whose matching run could belong to another pull request from the same
        # branch; reported instead of closed.
        self.ambiguous: list[tuple[int, str]] = []
        since = (now - TAMPER_WINDOW).strftime("%Y-%m-%dT%H:%M:%SZ")
        self._runs_by_branch: dict[tuple[str, str], list[WorkflowRun]] = {}
        incomplete = []
        for event in MERGE_REF_EVENTS:
            pages = client.rest_get_pages(
                f"repos/{repo}/actions/runs?event={event}&per_page=100&created=%3E%3D{since}"
            )
            runs = [run for page in pages for run in page["workflow_runs"]]
            total = pages[0]["total_count"] if pages else 0
            # GitHub lists at most 1000 runs for a filtered query; anything beyond is not checked.
            if total > len(runs):
                incomplete.append(f"only {len(runs)} of {total} {event} runs")
            for run in map(parse_workflow_run, runs):
                self._runs_by_branch.setdefault((run.head_repo.casefold(), run.head_branch), []).append(run)
        self.incomplete = (
            f"{', '.join(incomplete)} of the last {TAMPER_WINDOW} could be listed, so the gate tampering "
            "check did not see the rest"
            if incomplete
            else None
        )
        self._did_project_ci: dict[int, bool] = {}
        self._changes_workflows: dict[tuple[str, str], bool | None] = {}
        self._jobs: dict[int, list[dict[str, Any]]] = {}

    def _get_jobs(self, run: WorkflowRun) -> list[dict[str, Any]]:
        if run.id not in self._jobs:
            self._jobs[run.id] = self._client.rest_get(
                f"repos/{self._repo}/actions/runs/{run.id}/jobs?filter=latest&per_page=100",
                paginate=True,
                items_key="jobs",
            )
        return self._jobs[run.id]

    def _did_run_project_ci(self, run: WorkflowRun) -> bool:
        if run.id not in self._did_project_ci:
            if run.event not in self._base_triggers.get(run.workflow_file, frozenset()):
                # A workflow or trigger the base branch does not have, for example a fork-only
                # workflow the pull request adds, did no work in apache/airflow if its jobs all
                # skipped there. Jobs that cannot be listed count as work.
                try:
                    jobs = self._get_jobs(run)
                except GitHubError:
                    result = True
                else:
                    result = (
                        jobs_show_project_ci(jobs, self._now)
                        if run.workflow_file == CI_WORKFLOW_FILE
                        else any(has_job_started(job) and job.get("conclusion") != "skipped" for job in jobs)
                    )
            elif run.workflow_file != CI_WORKFLOW_FILE or not run.is_long_running(self._now):
                result = False
            else:
                result = jobs_show_project_ci(self._get_jobs(run), self._now)
            self._did_project_ci[run.id] = result
        return self._did_project_ci[run.id]

    def find_gated_runs(self, pr: PullRequestSnapshot) -> list[WorkflowRun]:
        """
        Completed ci-amd.yml pull_request runs of the pull request's branch that only gated it.

        A commit that changes workflows could do project CI work under the gate's name within its
        time limit, so its runs are kept as evidence, as are runs whose change set is too large
        to check. The jobs read are those of the latest attempt, and deleting a run deletes every
        attempt, so a re-run is kept: an earlier attempt may have run project CI.
        """
        if not pr.head_repo:
            return []
        return [
            run
            for run in self._runs_by_branch.get((pr.head_repo.casefold(), pr.head_ref), [])
            if run.workflow_file == CI_WORKFLOW_FILE
            and run.event == "pull_request"
            and run.status == "completed"
            and run.run_attempt == 1
            and not self._did_run_project_ci(run)
            and is_gated_skip_run(self._get_jobs(run), self._now)
            and self._commit_changes_workflows(run, pr.base_ref) is False
        ]

    def _commit_changes_workflows(self, run: WorkflowRun, base_ref: str) -> bool | None:
        """Whether the run's commit changes workflows against the base; None when not knowable."""
        key = (run.head_sha, base_ref)
        if key not in self._changes_workflows:
            owner = run.head_repo.split("/", 1)[0]
            # The compare API returns `files` only on its first page; later pages list commits.
            files = self._client.rest_get(
                f"repos/{self._repo}/compare/{base_ref}...{owner}:{run.head_sha}?per_page=1"
            ).get("files", [])
            if touches(changed_paths(files), (".github/workflows/",)):
                self._changes_workflows[key] = True
            else:
                # The compare API lists at most 300 files; a workflow change could be beyond them.
                self._changes_workflows[key] = None if len(files) >= COMPARE_FILES_CAP else False
        return self._changes_workflows[key]

    def _other_pull_requests(self, pr: PullRequestSnapshot) -> list[int]:
        """Other pull requests from the same branch that were open within the tampering window."""
        owner = pr.head_repo.split("/", 1)[0]  # type: ignore[union-attr]
        pulls = self._client.rest_get(
            f"repos/{self._repo}/pulls?head={quote(owner, safe='')}:{quote(pr.head_ref, safe='')}"
            "&state=all&per_page=100",
            paginate=True,
        )
        window_start = self._now - TAMPER_WINDOW
        return [
            pull["number"]
            for pull in pulls
            if pull["number"] != pr.number
            and (pull.get("closed_at") is None or parse_github_datetime(pull["closed_at"]) >= window_start)
        ]

    def find(self, pr: PullRequestSnapshot) -> str | None:
        if not pr.head_repo:
            return None
        # Runs are listed per fork branch: GitHub records no pull request on runs from forks.
        runs = self._runs_by_branch.get((pr.head_repo.casefold(), pr.head_ref), [])
        # Runs before `use project ci` was removed were legitimate. Runs before a maintainer
        # reopened the pull request were already acted on; reopening without restoring the gate
        # triggers a new run, which is caught again.
        not_before = max(
            (t for t in (pr.last_project_ci_unlabel_at, pr.last_reopened_at) if t is not None), default=None
        )
        for run in sorted(runs, key=lambda r: r.created_at, reverse=True):
            if not_before is not None and run.created_at <= not_before:
                continue
            base_ref = base_at(pr, run.created_at)
            # Project CI against a base branch without the gate is what its gate job decides.
            if not self._is_gated_base(base_ref):
                continue
            if not self._did_run_project_ci(run):
                continue
            # A change set too large to list fails closed: padding a tampered commit past the
            # compare API's cap must not hide the workflow change.
            if self._commit_changes_workflows(run, base_ref) is not False:
                others = self._other_pull_requests(pr)
                if others:
                    self.ambiguous.append(
                        (
                            pr.number,
                            f"project CI run {run.html_url} may have bypassed the gate, but it could also "
                            f"belong to {', '.join(f'#{n}' for n in others)} from the same branch; not closed, "
                            "check it",
                        )
                    )
                    return None
                return run.html_url
        return None


def get_gate_wiring(workflow_text: str | None) -> Any:
    """
    The parts of a ci-amd.yml that make up the gate: its job, how build-info depends on it, the
    jobs that do not depend on it (a job freed from the gate runs whatever it decides), and the
    workflow-level triggers and settings that every job, the gate included, runs with.

    A file that is not a workflow mapping yields an object equal to nothing, so a pull request
    breaking the workflow counts as changing the gate rather than escaping the check.
    """
    if workflow_text is None:
        return None
    try:
        workflow = yaml.safe_load(workflow_text)
    except yaml.YAMLError:
        return object()
    jobs = workflow.get("jobs") if isinstance(workflow, dict) else None
    if not isinstance(jobs, dict):
        return object()
    build_info = jobs.get("build-info")
    build_info = build_info if isinstance(build_info, dict) else {}
    return (
        jobs.get("gate"),
        build_info.get("needs"),
        build_info.get("if"),
        find_jobs_outside_gate(jobs),
        workflow.get("on", workflow.get(True)),
        workflow.get("defaults"),
        workflow.get("env"),
        workflow.get("permissions"),
        workflow.get("concurrency"),
    )


def find_jobs_outside_gate(jobs: dict[str, Any]) -> frozenset[str]:
    """Jobs that do not depend on the gate job, directly or through other jobs."""

    def get_needs(job_id: str) -> list[str]:
        job = jobs.get(job_id)
        needs = job.get("needs") if isinstance(job, dict) else None
        if isinstance(needs, str):
            return [needs]
        return [need for need in needs if isinstance(need, str)] if isinstance(needs, list) else []

    def depends_on_gate(job_id: str, seen: frozenset[str] = frozenset()) -> bool:
        # `seen` stops a dependency cycle, which GitHub rejects anyway.
        return any(
            need == "gate" or (need not in seen and depends_on_gate(need, seen | {job_id}))
            for need in get_needs(job_id)
        )

    return frozenset(job_id for job_id in jobs if job_id != "gate" and not depends_on_gate(job_id))


def is_own_pilot_change(base_text: str | None, head_text: str | None, author_login: str) -> bool:
    """Whether a gate configuration differs from the base only in the author's own pilot entry."""
    if base_text is None or head_text is None:
        return False
    try:
        base = parse_config(base_text)
        head = parse_config(head_text)
    except (ValueError, TypeError, AttributeError):
        # Valid TOML of the wrong shape, such as `pilot = []`, is no permitted change either.
        return False
    base_pilot = {login.casefold(): joined for login, joined in base.pilot.items()}
    head_pilot = {login.casefold(): joined for login, joined in head.pilot.items()}
    changed = {
        login
        for login in base_pilot.keys() | head_pilot.keys()
        if base_pilot.get(login) != head_pilot.get(login)
    }
    return replace(head, pilot=base.pilot) == base and changed <= {author_login.casefold()}


class HeadMovedError(RuntimeError):
    """The pull request's head moved while it was checked; the next run checks the new head."""


class GateChangeFinder:
    """Finds how a pull request changes the fork CI gate, from its changed files."""

    def __init__(self, client: GitHubClient, *, repo: str):
        self._client = client
        self._repo = repo

    def _merge_base(self, pr: PullRequestSnapshot) -> str:
        # The pull request's changes are against the merge base, not the base branch's tip, which
        # may have moved on: comparing with the tip would count others' later changes as its own.
        owner = (pr.head_repo or "").split("/", 1)[0]
        compare = self._client.rest_get(
            f"repos/{self._repo}/compare/{quote(pr.base_ref, safe='')}...{owner}:{pr.head_sha}?per_page=1"
        )
        return compare["merge_base_commit"]["sha"]

    def find(self, pr: PullRequestSnapshot) -> str | None:
        files = self._client.rest_get(
            f"repos/{self._repo}/pulls/{pr.number}/files?per_page=100", paginate=True
        )
        # The listing is of the pull request as it is now, while the head commit recorded as checked
        # is the one read at the start of the run: a push in between would mark a commit checked
        # by another one's files.
        current_head = self._client.rest_get(f"repos/{self._repo}/pulls/{pr.number}")["head"]["sha"]
        if current_head != pr.head_sha:
            raise HeadMovedError(
                f"head moved from {pr.head_sha[:8]} to {current_head[:8]} during the gate check"
            )
        # A listing at GitHub's cap may leave out a gate file, so it is not trusted.
        if len(files) >= PULL_REQUEST_FILES_CAP:
            return f"it changes {len(files)} or more files, more than GitHub lists, so the gate cannot be checked"
        paths = set(changed_paths(files))
        for path in sorted(GATE_PROTECTED_PATHS & paths):
            return f"it changes `{path}`"
        if not paths & {GATE_CONFIG_PATH, CI_WORKFLOW_PATH}:
            return None
        head_repo = pr.head_repo or ""
        merge_base = self._merge_base(pr)
        if GATE_CONFIG_PATH in paths and not is_own_pilot_change(
            read_file(self._client, self._repo, GATE_CONFIG_PATH, merge_base),
            read_file(self._client, head_repo, GATE_CONFIG_PATH, pr.head_sha),
            pr.author_login,
        ):
            return f"it changes `{GATE_CONFIG_PATH}` beyond the author's own `[pilot]` entry"
        if CI_WORKFLOW_PATH in paths and get_gate_wiring(
            read_file(self._client, self._repo, CI_WORKFLOW_PATH, merge_base)
        ) != get_gate_wiring(read_file(self._client, head_repo, CI_WORKFLOW_PATH, pr.head_sha)):
            return (
                f"it changes the `gate` job of `{CI_WORKFLOW_PATH}`, which jobs depend on it, or the "
                "workflow-level triggers or settings it runs with"
            )
        return None


COMMENTS_QUERY = """
query($owner: String!, $name: String!, $number: Int!, $cursor: String) {
  repository(owner: $owner, name: $name) { pullRequest(number: $number) {
    comments(first: 100, after: $cursor) {
      pageInfo { hasNextPage endCursor }
      nodes { databaseId body author { login } }
    }
  } }
}
"""


def find_state_comment(client: GitHubClient, repo: str, pr: PullRequestSnapshot) -> PullRequestSnapshot:
    """Page through the remaining comments of a long pull request for its state comment."""
    owner, name = repo.split("/", 1)
    cursor = pr.comments_cursor
    while cursor is not None:
        comments = client.graphql(
            COMMENTS_QUERY, {"owner": owner, "name": name, "number": pr.number, "cursor": cursor}
        )["repository"]["pullRequest"]["comments"]
        found = find_marker_comment(comments["nodes"])
        if found is not None:
            marker, comment_id, body, error = found
            return replace(
                pr,
                marker=marker,
                marker_comment_id=comment_id,
                marker_comment_body=body,
                marker_error=error,
                comments_cursor=None,
            )
        cursor = comments["pageInfo"]["endCursor"] if comments["pageInfo"]["hasNextPage"] else None
    return replace(pr, comments_cursor=None)


def read_head_status(client: GitHubClient, repo: str, pr: PullRequestSnapshot) -> PullRequestSnapshot:
    """Read the reconciler's statuses from the head commit, for a snapshot that could not."""
    # Every page: with job statuses a commit can carry more than 100 contexts, and a missed `CI passed`
    # success would not be revoked.
    statuses = {
        status["context"]: status
        for status in client.rest_get(
            f"repos/{repo}/commits/{pr.head_sha}/status?per_page=100", paginate=True, items_key="statuses"
        )
    }
    status = statuses.get(STATUS_CONTEXT)
    ci_passed = statuses.get(CI_PASSED_CONTEXT)
    return replace(
        pr,
        current_status=(
            (status["state"], status.get("target_url") or "", status.get("description") or "")
            if status
            else None
        ),
        status_created_at=parse_github_datetime(status.get("created_at") or "") if status else None,
        ci_passed_state=ci_passed["state"] if ci_passed else None,
        head_status_unread=False,
    )


def pull_request_changes_ci_files(client: GitHubClient, repo: str, number: int, head_sha: str) -> bool | None:
    """
    Whether the pull request at ``head_sha`` changes CI-defining paths; None when it cannot tell.

    The files endpoint lists the current head, which a push may have moved since the snapshot.
    When the head moved while the files were read, the listing may belong to another commit, so
    no answer is given; the next run decides for the new head.
    """
    files = client.rest_get(f"repos/{repo}/pulls/{number}/files?per_page=100", paginate=True)
    if client.rest_get(f"repos/{repo}/pulls/{number}")["head"]["sha"] != head_sha:
        return None
    return changes_ci_definition(changed_paths(files), file_count=len(files))


def count_commits_behind(client: GitHubClient, repo: str, pr: PullRequestSnapshot) -> int | None:
    """Commits on the base branch that the head commit does not have; None when not readable."""
    owner = (pr.head_repo or "").split("/", 1)[0]
    try:
        compare = client.rest_get(
            f"repos/{repo}/compare/{quote(pr.base_ref, safe='')}...{owner}:{pr.head_sha}?per_page=1"
        )
    except GitHubError as e:
        console_print(f"[warning]#{pr.number}: could not count commits behind {pr.base_ref}: {e}[/]")
        return None
    return compare.get("behind_by")


def find_heads_shared_across_bases(prs: Iterable[PullRequestSnapshot]) -> set[str]:
    """Head commits of more than one pull request whose bases differ."""
    bases_by_head: dict[str, set[str]] = {}
    for pr in prs:
        bases_by_head.setdefault(pr.head_sha, set()).add(pr.base_ref)
    return {head for head, bases in bases_by_head.items() if len(bases) > 1}


@dataclass(frozen=True)
class DraftChange:
    """A draft state change, applied by the monitor's draft-state job rather than the reconciler."""

    number: int
    node_id: str
    draft: bool
    # Remove `fork ci required` once the pull request is undrafted.
    release: bool = False


def apply_plan(client: GitHubClient, pr: PullRequestSnapshot, plan: Plan) -> DraftChange | None:
    """
    Apply the plan, except for the draft state change, which is returned.

    GitHub authorises draft changes against `contents: write`, which the reconciler, running
    Breeze and its dependencies, does not get; a separate job with no other code applies them.
    """
    draft_change = None
    if plan.draft is not None:
        verb = "convert to draft" if plan.draft else "mark ready for review"
        console_print(f"[info]#{pr.number}: {verb} (left to the draft-state job)[/]")
        draft_change = DraftChange(pr.number, pr.node_id, plan.draft)
    if plan.add_labels:
        client.add_labels(pr.number, plan.add_labels)
    for label in plan.remove_labels:
        if label != LABEL_FORK_CI_REQUIRED:
            client.remove_label(pr.number, label)
    # New comments go before the state comment records the new state: the state recorded there is
    # what the next run compares with, so a failure on the way posts them again rather than never.
    # They carry no marker, so the state comment stays the earliest bot comment with one.
    for comment in plan.new_comments:
        client.create_comment(pr.number, comment)
    if plan.marker_comment is not None:
        if pr.marker_comment_id is not None:
            client.update_comment(pr.number, pr.marker_comment_id, plan.marker_comment)
        else:
            client.create_comment(pr.number, plan.marker_comment)
    if plan.status is not None:
        client.set_status(pr.number, pr.head_sha, *plan.status)
    if plan.ci_passed is not None:
        client.set_status(pr.number, pr.head_sha, *plan.ci_passed, context=CI_PASSED_CONTEXT)
    if plan.close:
        client.close(pr.number)
    # Last, because the label is what brings the pull request back to the next run: when an
    # earlier mutation fails, the next run retries the release instead of losing track of it.
    # A release that undrafts leaves it to the draft-state job, for the same reason.
    if LABEL_FORK_CI_REQUIRED in plan.remove_labels:
        if draft_change is not None and not draft_change.draft:
            draft_change = replace(draft_change, release=True)
        else:
            client.remove_label(pr.number, LABEL_FORK_CI_REQUIRED)
    return draft_change


def _escape_slack(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def build_slack_payload(
    *,
    unknown: list[tuple[int, str]],
    attention: list[tuple[int, str]],
    notes: Sequence[str] = (),
    repo: str,
    run_url: str,
    channel: str,
) -> dict[str, Any]:
    def shorten(text: str) -> str:
        return text if len(text) <= SLACK_ITEM_LIMIT else f"{text[: SLACK_ITEM_LIMIT - 1]}…"

    def item(number: int, reason: str) -> str:
        return f"• <https://github.com/{repo}/pull/{number}|#{number}>: {_escape_slack(shorten(reason))}"

    # Slack rejects a whole message with a section over 3000 characters or more than 50 blocks.
    texts = [f"*Fork CI monitor* (<{run_url}|run>)"]
    texts.extend(f"*Incomplete:* {_escape_slack(shorten(note))}" for note in notes)
    for heading, items in (
        ("*Could not classify or update:*", unknown),
        ("*Attention (expected, for a maintainer to see):*", attention),
    ):
        if not items:
            continue
        text = heading
        for number, reason in items:
            line = item(number, reason)
            if len(text) + 1 + len(line) > SLACK_SECTION_LIMIT:
                texts.append(text)
                text = line
            else:
                text = f"{text}\n{line}"
        texts.append(text)
    if len(texts) > SLACK_MAX_BLOCKS:
        hidden = len(texts) - (SLACK_MAX_BLOCKS - 1)
        texts = [*texts[: SLACK_MAX_BLOCKS - 1], f"_{hidden} more sections are in the <{run_url}|run log>._"]
    return {
        "channel": channel,
        "text": "Fork CI monitor needs attention",
        "blocks": [{"type": "section", "text": {"type": "mrkdwn", "text": text}} for text in texts],
    }


def drop_reported(
    reported_file: Path,
    *,
    unknown: list[tuple[int, str]],
    attention: list[tuple[int, str]],
    notes: list[str],
    record: bool = True,
) -> tuple[list[tuple[int, str]], list[tuple[int, str]], list[str]]:
    """
    Leave out the items the previous run already reported, and record this run's items.

    Only the previous run's items are kept, so an item that went away and comes back is posted
    again. A missing or unreadable file reports everything.
    """
    current: dict[str, list[Any]] = {
        "unknown": [list(item) for item in unknown],
        "attention": [list(item) for item in attention],
        "notes": notes,
    }
    try:
        previous = json.loads(reported_file.read_text())
    except (OSError, ValueError):
        previous = {}
    if not isinstance(previous, dict):
        previous = {}
    if record:
        reported_file.write_text(json.dumps(current, indent=2))

    def drop(key: str, items: list[Any]) -> list[Any]:
        seen = previous.get(key) or []
        return [item for item, stored in zip(items, current[key]) if stored not in seen]

    return drop("unknown", unknown), drop("attention", attention), drop("notes", notes)


def run_reconcile(
    client: GitHubClient,
    config: GateConfig,
    *,
    now: datetime,
    run_url: str,
    output_file: Path,
    workflows_dir: Path,
    reported_file: Path | None = None,
    draft_changes_file: Path | None = None,
    channel: str = "internal-airflow-ci-cd",
    default_branch: str = "main",
) -> bool:
    """
    Reconcile every gated pull request once; return whether there are items for Slack.

    :param config: the gate configuration of the default branch's checkout.
    :param workflows_dir: ``.github/workflows`` of a checkout of the default branch, whose
        triggers decide which recorded runs were legitimate.
    :param reported_file: the items of the previous run, replaced by this run's. Items in it are
        left out of the Slack message, so an item that persists is posted once. A dry run reads
        it but leaves it unchanged, because it posts nothing.
    :param draft_changes_file: where the draft state changes for the draft-state job are written,
        as a JSON list; always written, empty when there are none.
    """
    repo = client.repo
    unknown: list[tuple[int, str]] = []
    attention: list[tuple[int, str]] = []
    draft_changes: list[DraftChange] = []
    configs = BaseGateConfigs(client, repo=repo, default_branch=default_branch, default_config=config)

    discover(client, configs, repo=repo, now=now, unknown=unknown)

    prs = [
        parse_pull_request(node)
        for node in _search(
            client,
            PASS_TWO_QUERY
            % {"page": PASS_TWO_PAGE_SIZE, "context": STATUS_CONTEXT, "ci_passed": CI_PASSED_CONTEXT},
            f'repo:{repo} is:pr is:open label:"{LABEL_FORK_CI_REQUIRED}"',
        )
    ]
    observations = fetch_fork_observations(client, prs)

    def is_gated_base(base_ref: str) -> bool:
        # A base branch whose configuration cannot be read is treated as gated.
        try:
            return configs.get(base_ref) is not None
        except (GitHubError, ValueError):
            return True

    bypass_finder = BypassFinder(
        client,
        repo=repo,
        now=now,
        base_triggers=load_workflow_triggers(workflows_dir),
        is_gated_base=is_gated_base,
    )
    job_reporter = JobStatusReporter(client, repo=repo)

    shared_heads = find_heads_shared_across_bases(prs)
    gate_change_finder = GateChangeFinder(client, repo=repo)
    for listed in prs:
        try:
            pr = listed if listed.comments_cursor is None else find_state_comment(client, repo, listed)
            if pr.head_status_unread:
                pr = read_head_status(client, repo, pr)
            fork = observations.get(pr.number, ForkObservation())
            # Read on every run, not only until the workflow is first seen enabled: it can be
            # disabled later, and a pull request waiting for a push would then wait forever.
            if pr.head_repo and fork.suite_status is None and fork.error is None:
                fork = ForkObservation(workflow_state=fetch_workflow_state(client, pr.head_repo))
            jobs_reported = None
            if config.job_statuses:
                # Job statuses are informational; failing to post them must not keep the pull request
                # from being reconciled, such as a `CI passed` that no longer holds being revoked.
                try:
                    jobs_reported = job_reporter.report(pr, fork)
                except Exception as e:
                    console_print(f"[warning]#{pr.number}: reporting job statuses failed: {e}[/]")
                    unknown.append((pr.number, f"reporting job statuses failed: {type(e).__name__}: {e}"))

            plan = classify(
                pr,
                fork,
                config=configs.get(pr.base_ref),
                now=now,
                repo=repo,
                find_bypass_run=functools.partial(bypass_finder.find, pr),
                changes_ci_files=functools.partial(
                    pull_request_changes_ci_files, client, repo, pr.number, pr.head_sha
                ),
                count_behind=functools.partial(count_commits_behind, client, repo, pr),
                jobs_reported=jobs_reported,
                head_shared_across_bases=pr.head_sha in shared_heads,
                find_gate_change=functools.partial(gate_change_finder.find, pr),
            )
            draft_change = apply_plan(client, pr, plan)
            if draft_change is not None:
                draft_changes.append(draft_change)
            # The fork's result is in the `Fork CI / Tests (AMD)` status; the gated run's skipped
            # checks only clutter the pull request. Runs with any project CI work stay for the
            # tampering check.
            if plan.state != ForkState.GATE_BYPASSED:
                for run in bypass_finder.find_gated_runs(pr):
                    client.delete_run(pr.number, run.id)
            unknown.extend((pr.number, reason) for reason in plan.unknown)
            attention.extend((pr.number, item) for item in plan.attention)
        except HeadMovedError as e:
            console_print(f"[info]#{listed.number}: {e}; checked again on the next run[/]")
        except Exception as e:
            console_print(f"[error]#{listed.number}: {e}[/]")
            unknown.append((listed.number, f"{type(e).__name__}: {e}"))

    attention.extend(bypass_finder.ambiguous)
    notes = [bypass_finder.incomplete] if bypass_finder.incomplete else []
    if job_reporter.deferred:
        console_print(
            f"[info]Job statuses of {len(job_reporter.deferred)} pull requests left for the next run: "
            f"{job_reporter.deferred}[/]"
        )
        notes.append(JOB_STATUSES_DEFERRED_NOTE)
    # The full list for the run log; Slack may show only part of a very long one.
    for number, reason in [*unknown, *attention]:
        console_print(f"[warning]#{number}: {reason}[/]")
    console_print(f"[info]Reconciled {len(prs)} pull requests; GitHub calls: {dict(client.calls)}[/]")
    if draft_changes_file is not None:
        draft_changes_file.write_text(json.dumps([asdict(change) for change in draft_changes]))
    if reported_file is not None:
        unknown, attention, notes = drop_reported(
            reported_file, unknown=unknown, attention=attention, notes=notes, record=not client.dry_run
        )
    has_alerts = bool(unknown or attention or notes)
    if has_alerts:
        payload = build_slack_payload(
            unknown=unknown, attention=attention, notes=notes, repo=repo, run_url=run_url, channel=channel
        )
        output_file.write_text(json.dumps(payload, indent=2))
    return has_alerts
