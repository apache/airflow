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
"""

from __future__ import annotations

import base64
import functools
import json
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field, replace
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
    Reason,
    decide,
    parse_config,
    parse_github_datetime,
)
from airflow_breeze.utils.fork_ci_github import GitHubClient, GitHubError
from airflow_breeze.utils.fork_ci_messages import (
    STATUS_STATES,
    Marker,
    fork_actions_url,
    inactivity_close_comment,
    nudge_comment,
    parse_marker,
    released_comment,
    state_comment,
    status_description,
    tamper_close_comment,
    ungated_comment,
)

CI_WORKFLOW_FILE = "ci-amd.yml"
# The files the gate job of ci-amd.yml reads from a pull request's base branch.
GATE_CONFIG_PATH = ".github/fork-ci-gate.toml"
GATE_SCRIPT_PATH = "dev/breeze/src/airflow_breeze/utils/fork_ci_gate.py"
GATE_JOB_NAME = "Fork CI gate"
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

RUNNING_STATUSES = frozenset({"QUEUED", "IN_PROGRESS", "PENDING", "WAITING", "REQUESTED"})
FAILED_CONCLUSIONS = frozenset({"FAILURE", "CANCELLED", "TIMED_OUT", "STARTUP_FAILURE", "ACTION_REQUIRED"})
SETUP_WORKFLOW_STATES = frozenset({"disabled_fork", "disabled_manually", "missing"})


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

    @property
    def workflow_file(self) -> str:
        return self.path.rsplit("/", 1)[-1]

    def is_long_running(self, now: datetime) -> bool:
        end = self.updated_at if self.status == "completed" else now
        return end - self.run_started_at >= SUSPICIOUS_RUN_DURATION


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
    current_status: tuple[str, str, str] | None = None
    status_created_at: datetime | None = None
    marker: Marker | None = None
    marker_comment_id: int | None = None
    marker_comment_body: str | None = None
    marker_error: str | None = None
    # Set when the state comment was not among the comments fetched with the pull request and
    # more comments follow; the reconciler pages from here before deciding anything.
    comments_cursor: str | None = None


@dataclass(frozen=True)
class ForkObservation:
    suite_status: str | None = None
    suite_conclusion: str | None = None
    run_url: str | None = None
    workflow_state: str | None = None
    error: str | None = None


@dataclass
class Plan:
    state: ForkState | None = None
    add_labels: list[str] = field(default_factory=list)
    remove_labels: list[str] = field(default_factory=list)
    draft: bool | None = None
    status: tuple[str, str, str] | None = None
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
        if marker is not None and marker.state == ForkState.AWAITING_PUSH.value:
            return ForkState.AWAITING_PUSH, None
        return None, "no fork run and the fork workflow state was not read"
    if fork.workflow_state in SETUP_WORKFLOW_STATES:
        return ForkState.SETUP_REQUIRED, None
    if fork.workflow_state == "active":
        return ForkState.AWAITING_PUSH, None
    return None, f"unexpected fork workflow state {fork.workflow_state!r}"


def needs_workflow_state(marker: Marker | None) -> bool:
    return marker is None or marker.state != ForkState.AWAITING_PUSH.value


def jobs_show_project_ci(jobs: Iterable[dict[str, Any]], now: datetime) -> bool:
    """
    Whether the jobs of a ci-amd.yml run show more than the gate deciding to skip.

    Any job other than the gate that was not skipped counts, and so does a gate job that is
    missing, duplicated or ran for longer than the gate ever needs, which is how a renamed job
    doing project CI work under the gate's name shows up.
    """
    jobs = list(jobs)
    gate_jobs = [job for job in jobs if job["name"] == GATE_JOB_NAME]
    if len(gate_jobs) != 1:
        return True
    started = parse_github_datetime(gate_jobs[0].get("started_at") or "")
    completed = parse_github_datetime(gate_jobs[0].get("completed_at") or "") or now
    if started is not None and completed - started > GATE_MAX_DURATION:
        return True
    return any(job["name"] != GATE_JOB_NAME and job.get("conclusion") != "skipped" for job in jobs)


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
    changes_ci_files: Callable[[], bool],
) -> Plan:
    """
    Decide the mutations for one gated pull request.

    :param config: the gate configuration of the pull request's base branch; None when the base
        branch has no gate, so its gate job runs project CI.
    :param find_bypass_run: returns the project CI run that bypassed the gate, if any; only
        called for pull requests that the gate tampering check applies to.
    :param changes_ci_files: whether the pull request changes CI-defining paths; only called
        when a new head commit turns green in the fork.
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
    }
    if marker and marker.state in reopened_since_closing:
        reopened = reopened_since_closing[marker.state]
        if reopened is None or reopened <= marker.since:
            plan.state = ForkState(marker.state)
            _set_label(plan, labels, LABEL_NEEDS_PROJECT_CI, present=False)
            if plan.state == ForkState.CLOSED:
                comment = inactivity_close_comment(days=config.close_after_days, fork=pr.head_repo)
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

    state, unknown_reason = observe_fork_state(fork, marker)
    if state is None:
        plan.unknown.append(unknown_reason or "unknown fork state")
        return plan
    refresh_status = False
    if state == ForkState.GREEN:
        # The decision is read back from the commit status on the head commit, which only
        # apache/airflow's own tokens can write, never from a comment. The changed files depend
        # on the base too, so a status written before the pull request was retargeted is stale.
        # The status is shared by every pull request with this head commit; scoping it to one is
        # tracked at https://github.com/apache/airflow/issues/74907
        previous = status_state(pr.current_status)
        decided = previous in (ForkState.GREEN.value, ForkState.GREEN_UNVERIFIED.value)
        stale = pr.last_base_change_at is not None and (
            pr.status_created_at is None or pr.status_created_at <= pr.last_base_change_at
        )
        if decided and not stale:
            unverified = previous == ForkState.GREEN_UNVERIFIED.value
        else:
            # Writing the status again, even unchanged, records when this decision was made.
            refresh_status = decided
            unverified = changes_ci_files()
            if unverified:
                plan.attention.append(
                    "fork CI is green, but the pull request changes CI-defining files that the fork run "
                    "itself executed; review them and add `use project ci` to verify"
                )
        if unverified:
            state = ForkState.GREEN_UNVERIFIED
    plan.state = state
    since = marker.since if marker and marker.state == state.value else now

    # The marker records awaiting_push before the nudge is posted, so a failed post is not retried;
    # tracked at https://github.com/apache/airflow/issues/74908
    if marker and marker.state == ForkState.SETUP_REQUIRED.value and state == ForkState.AWAITING_PUSH:
        plan.new_comments.append(nudge_comment(fork=pr.head_repo))

    if state in CLOSABLE_STATES and now - since >= timedelta(days=config.close_after_days):
        plan.state = ForkState.CLOSED
        _set_label(plan, labels, LABEL_NEEDS_PROJECT_CI, present=False)
        _set_marker_comment(
            plan,
            pr,
            state_comment(Marker(ForkState.CLOSED.value, pr.head_sha, now), fork=pr.head_repo, run_url=None),
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

    _set_status(plan, pr, state.value, fork.run_url or fork_actions_url(pr.head_repo), force=refresh_status)
    _set_marker_comment(
        plan,
        pr,
        state_comment(Marker(state.value, pr.head_sha, since), fork=pr.head_repo, run_url=fork.run_url),
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
          ... on BaseRefChangedEvent { createdAt }
        }
      }
      commits(last: 1) { nodes { commit {
        status { context(name: "%(context)s") { state targetUrl description createdAt } }
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
    for item in (node.get("timelineItems") or {}).get("nodes") or []:
        if item["__typename"] == "ConvertToDraftEvent":
            last_draft_actor = _login(item.get("actor"))
        elif item["__typename"] == "BaseRefChangedEvent":
            last_base_change = parse_github_datetime(item["createdAt"])
        elif item["__typename"] == "ReopenedEvent":
            last_reopened_by_anyone = parse_github_datetime(item["createdAt"])
            # Only a reopen by someone else counts for the gate tampering check: an author closing
            # and reopening their own pull request must not hide a tampered run from it.
            if _login(item.get("actor")).casefold() != _login(node.get("author")).casefold():
                last_reopened = last_reopened_by_anyone
        elif (
            item["__typename"] == "UnlabeledEvent"
            and (item.get("label") or {}).get("name") == LABEL_USE_PROJECT_CI
        ):
            last_unlabel = parse_github_datetime(item["createdAt"])

    current_status = None
    status_created_at = None
    commits = (node.get("commits") or {}).get("nodes") or []
    if commits:
        context = (commits[0]["commit"].get("status") or {}).get("context")
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
        current_status=current_status,
        status_created_at=status_created_at,
        marker=marker,
        marker_comment_id=marker_id,
        marker_comment_body=marker_body,
        marker_error=marker_error,
        comments_cursor=comments_cursor,
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
        try:
            content = self._client.rest_get(
                f"repos/{self._repo}/contents/{path}?ref={quote(base_ref, safe='')}"
            )
        except GitHubError as e:
            if _is_not_found(e):
                return None
            raise
        return base64.b64decode(content["content"]).decode()

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


COMMITTER_PERMISSIONS = frozenset({"admin", "maintain", "write"})


def has_write_access(client: GitHubClient, repo: str, login: str) -> bool:
    """
    Whether the author can push to the repository; False when that cannot be read.

    GitHub reports an author whose membership of the organisation is private as CONTRIBUTOR, so a
    committer could look external.
    """
    try:
        permission = client.rest_get(f"repos/{repo}/collaborators/{quote(login, safe='')}/permission")
    except GitHubError:
        return False
    return (permission or {}).get("permission") in COMMITTER_PERMISSIONS


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
    # An author with many open pull requests is looked up once.
    write_access = functools.cache(functools.partial(has_write_access, client, repo))
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
            reason = decide(event, config).reason
            if reason == Reason.EXTERNAL and write_access(event.author_login):
                continue
            if reason in GATED_REASONS:
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
                "workflowRun { url event workflow { resourcePath } } } } } } }"
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
            )
    return observations


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


class BypassFinder:
    """
    Finds project CI runs that bypassed the gate, from the runs apache/airflow recorded.

    It looks at every recent run of a pull request's branch for the events that run workflow
    files from the pull request's merge commit, not only the runs of its head commit, so pushing a
    tampered commit and then one that restores the gate does not hide the tampered run. Both
    signals of the design must hold for a run: it did project CI work, and its own commit changes
    `.github/workflows/`. A run did project CI work when the base branch's version of its
    workflow is not triggered by the run's event (an injected workflow, or a trigger the pull
    request added to an existing one, such as `pull_request` on ci-arm.yml), or when it is a long
    ci-amd.yml run whose jobs show more than the gate.
    """

    def __init__(
        self, client: GitHubClient, *, repo: str, now: datetime, base_triggers: dict[str, frozenset[str]]
    ):
        self._client = client
        self._repo = repo
        self._now = now
        self._base_triggers = base_triggers
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

    def _did_run_project_ci(self, run: WorkflowRun) -> bool:
        if run.id not in self._did_project_ci:
            if run.event not in self._base_triggers.get(run.workflow_file, frozenset()):
                # Counts even when every job skipped; tracked at
                # https://github.com/apache/airflow/issues/74905
                result = True
            elif run.workflow_file != CI_WORKFLOW_FILE or not run.is_long_running(self._now):
                result = False
            else:
                jobs = self._client.rest_get(
                    f"repos/{self._repo}/actions/runs/{run.id}/jobs?filter=latest&per_page=100",
                    paginate=True,
                    items_key="jobs",
                )
                result = jobs_show_project_ci(jobs, self._now)
            self._did_project_ci[run.id] = result
        return self._did_project_ci[run.id]

    def _commit_changes_workflows(self, run: WorkflowRun, base_ref: str) -> bool | None:
        """Whether the run's commit changes workflows against the base; None when not knowable."""
        key = (run.head_sha, base_ref)
        if key not in self._changes_workflows:
            owner = run.head_repo.split("/", 1)[0]
            files = self._client.rest_get(
                f"repos/{self._repo}/compare/{base_ref}...{owner}:{run.head_sha}?per_page=100",
                paginate=True,
                items_key="files",
            )
            if touches(changed_paths(files), (".github/workflows/",)):
                self._changes_workflows[key] = True
            else:
                # The compare API lists at most 300 files; a workflow change could be beyond them.
                self._changes_workflows[key] = None if len(files) >= COMPARE_FILES_CAP else False
        return self._changes_workflows[key]

    def find(self, pr: PullRequestSnapshot) -> str | None:
        if not pr.head_repo:
            return None
        # Matching by fork branch alone also picks up runs of another pull request from that branch
        # or from before a retarget; tracked at https://github.com/apache/airflow/issues/74906
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
            if not self._did_run_project_ci(run):
                continue
            # A change set too large to list fails closed: padding a tampered commit past the
            # compare API's cap must not hide the workflow change.
            if self._commit_changes_workflows(run, pr.base_ref) is not False:
                return run.html_url
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


def pull_request_changes_ci_files(client: GitHubClient, repo: str, number: int) -> bool:
    files = client.rest_get(f"repos/{repo}/pulls/{number}/files?per_page=100", paginate=True)
    return changes_ci_definition(changed_paths(files), file_count=len(files))


def apply_plan(client: GitHubClient, pr: PullRequestSnapshot, plan: Plan) -> None:
    if plan.draft is True:
        client.convert_to_draft(pr.number, pr.node_id)
    if plan.add_labels:
        client.add_labels(pr.number, plan.add_labels)
    for label in plan.remove_labels:
        if label != LABEL_FORK_CI_REQUIRED:
            client.remove_label(pr.number, label)
    if plan.marker_comment is not None:
        if pr.marker_comment_id is not None:
            client.update_comment(pr.number, pr.marker_comment_id, plan.marker_comment)
        else:
            client.create_comment(pr.number, plan.marker_comment)
    if plan.status is not None:
        client.set_status(pr.number, pr.head_sha, *plan.status)
    for comment in plan.new_comments:
        client.create_comment(pr.number, comment)
    if plan.draft is False:
        client.mark_ready(pr.number, pr.node_id)
    if plan.close:
        client.close(pr.number)
    # Last, because the label is what brings the pull request back to the next run: when an
    # earlier mutation fails, the next run retries the release instead of losing track of it.
    if LABEL_FORK_CI_REQUIRED in plan.remove_labels:
        client.remove_label(pr.number, LABEL_FORK_CI_REQUIRED)


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


def run_reconcile(
    client: GitHubClient,
    config: GateConfig,
    *,
    now: datetime,
    run_url: str,
    output_file: Path,
    workflows_dir: Path,
    channel: str = "internal-airflow-ci-cd",
    default_branch: str = "main",
) -> bool:
    """
    Reconcile every gated pull request once; return whether there are items for Slack.

    :param config: the gate configuration of the default branch's checkout.
    :param workflows_dir: ``.github/workflows`` of a checkout of the default branch, whose
        triggers decide which recorded runs were legitimate.
    """
    repo = client.repo
    unknown: list[tuple[int, str]] = []
    attention: list[tuple[int, str]] = []
    configs = BaseGateConfigs(client, repo=repo, default_branch=default_branch, default_config=config)

    discover(client, configs, repo=repo, now=now, unknown=unknown)

    prs = [
        parse_pull_request(node)
        for node in _search(
            client,
            PASS_TWO_QUERY % {"page": PASS_TWO_PAGE_SIZE, "context": "Fork CI / Tests (AMD)"},
            f'repo:{repo} is:pr is:open label:"{LABEL_FORK_CI_REQUIRED}"',
        )
    ]
    observations = fetch_fork_observations(client, prs)
    bypass_finder = BypassFinder(
        client, repo=repo, now=now, base_triggers=load_workflow_triggers(workflows_dir)
    )

    for listed in prs:
        try:
            pr = listed if listed.comments_cursor is None else find_state_comment(client, repo, listed)
            fork = observations.get(pr.number, ForkObservation())
            if (
                pr.head_repo
                and fork.suite_status is None
                and fork.error is None
                and needs_workflow_state(pr.marker)
            ):
                fork = ForkObservation(workflow_state=fetch_workflow_state(client, pr.head_repo))

            plan = classify(
                pr,
                fork,
                config=configs.get(pr.base_ref),
                now=now,
                repo=repo,
                find_bypass_run=functools.partial(bypass_finder.find, pr),
                changes_ci_files=functools.partial(pull_request_changes_ci_files, client, repo, pr.number),
            )
            apply_plan(client, pr, plan)
            unknown.extend((pr.number, reason) for reason in plan.unknown)
            attention.extend((pr.number, item) for item in plan.attention)
        except Exception as e:
            console_print(f"[error]#{listed.number}: {e}[/]")
            unknown.append((listed.number, f"{type(e).__name__}: {e}"))

    notes = [bypass_finder.incomplete] if bypass_finder.incomplete else []
    # The full list for the run log; Slack may show only part of a very long one.
    for number, reason in [*unknown, *attention]:
        console_print(f"[warning]#{number}: {reason}[/]")
    console_print(f"[info]Reconciled {len(prs)} pull requests; GitHub calls: {dict(client.calls)}[/]")
    has_alerts = bool(unknown or attention or notes)
    if has_alerts:
        payload = build_slack_payload(
            unknown=unknown, attention=attention, notes=notes, repo=repo, run_url=run_url, channel=channel
        )
        output_file.write_text(json.dumps(payload, indent=2))
    return has_alerts
