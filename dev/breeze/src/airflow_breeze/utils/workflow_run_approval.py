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
Automatic approval of workflow runs from external contributors' fork PRs.

GitHub holds the workflow runs of a fork PR in ``action_required`` until someone with write access
approves them. This module approves the runs of PRs to ``main`` that do not touch anything that
defines CI or chooses runners, do not need the full set of tests or a dependency upgrade, and whose
jobs, as selected by selective checks, are estimated to take a limited number of job-minutes.
Everything else gets the ``need workflow approval`` label and waits for a maintainer, and a pending
``Workflow approval`` commit status that tells the author why, without notifying anyone. When the
runs start, ``mark-workflow-runs-approved.yml`` turns the status green and removes the label.

A PR whose CI runs in the contributor's fork (AIP-120) is decided by one rule instead: its project
CI run only executes the fork CI gate, so it is approved whatever its size, unless it changes files
that define CI, in which case it waits for a maintainer and the author gets a comment saying why.

The runs of PRs from the repositories of trusted stakeholders (Amazon, Google, Astronomer) are
always approved.

Only API metadata and git objects of the PR are read. The PR's code is never checked out or run:
selective checks come from the trusted ``main`` checkout and only see the list of changed files
and the PR's ``pyproject.toml`` contents through ``git show``.
"""

from __future__ import annotations

import os
import re
import subprocess
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import requests
from rich.markup import escape

from airflow_breeze.global_constants import GithubEvents
from airflow_breeze.utils.ci_minutes_estimate import CiMinutesEstimate, estimate_ci_minutes
from airflow_breeze.utils.console import console_print
from airflow_breeze.utils.fork_ci_gate import (
    GATED_REASONS,
    GateConfig,
    GateEvent,
    decide,
    load_config,
    parse_github_datetime,
)
from airflow_breeze.utils.fork_ci_messages import CI_CHANGE_MARKER, ci_change_approval_comment
from airflow_breeze.utils.fork_ci_reconcile import changes_ci_definition
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH
from airflow_breeze.utils.run_utils import run_command
from airflow_breeze.utils.selective_checks import (
    ALL_VERSIONS_LABEL,
    AREA_E2E_TESTS_LABEL,
    AREA_KUBERNETES_TESTS_LABEL,
    CANARY_LABEL,
    DEFAULT_VERSIONS_ONLY_LABEL,
    FULL_TESTS_NEEDED_LABEL,
    LATEST_VERSIONS_ONLY_LABEL,
    UPGRADE_TO_NEWER_DEPENDENCIES_LABEL,
)

# The label and the status context are also used by .github/workflows/mark-workflow-runs-approved.yml.
NEED_WORKFLOW_APPROVAL_LABEL = "need workflow approval"

APPROVAL_STATUS_CONTEXT = "Workflow approval"
APPROVAL_STATUS_URL = (
    "https://github.com/apache/airflow/blob/main/contributing-docs/05_pull_requests.rst"
    "#ci-workflow-runs-of-external-contributors"
)
# GitHub rejects longer commit status descriptions.
MAX_STATUS_DESCRIPTION_LENGTH = 140

# Only PRs to main are approved automatically. PRs to release branches (``v3-4-test``,
# ``airflow-ctl/v0-1-test``, ...) run those branches' own, older workflows and always need a maintainer.
AUTO_APPROVABLE_BASE_BRANCH = "main"

MAX_ERROR_TEXT_LENGTH = 200

# Dependency manifests and lock files. Selective checks only switch to dependency-upgrade mode for
# pyproject.toml, but a changed lock file or JS / Go / Java manifest also changes what CI installs.
DEPENDENCY_FILE_PATTERN = re.compile(
    r"(^|/)(pyproject\.toml|uv\.lock|requirements[^/]*\.txt|package\.json|pnpm-lock\.yaml"
    r"|package-lock\.json|yarn\.lock|go\.mod|go\.sum|build\.gradle(\.kts)?|settings\.gradle(\.kts)?"
    r"|gradle\.lockfile|libs\.versions\.toml|pom\.xml"
    r"|gradle-wrapper\.(properties|jar)|maven-wrapper\.(properties|jar)|gradlew(\.bat)?|mvnw(\.cmd)?)$"
)

# About 23% of a full-tests run (~1330 job-minutes on public runners), just above the cluster of
# single core areas and the Helm chart (274-301). Changes to large providers (amazon 364, google 374)
# and anything bigger wait for a maintainer, to keep the load on the shared public runners down.
MAX_AUTO_APPROVED_JOB_MINUTES = 310

# Larger changes to non-test code are features or behaviour changes rather than fixes, and get a
# maintainer's look even when their CI is cheap. 100 lines is the 90th percentile of the PRs that
# pass the other rules; above it the distribution thins out into feature-sized changes.
MAX_AUTO_APPROVED_SOURCE_LINES = 100

# Files that are not counted as source: tests (including e2e tests), docs, newsfragments,
# translations, lock files and images.
NON_SOURCE_FILE_PATTERN = re.compile(
    r"(^|/)(tests?|test_utils|testing|[^/]*e2e[^/]*)/"
    r"|(^|/)test_[^/]*$|_test\.(py|go)$|\.test\.(ts|tsx|js)$|\.spec\.(ts|tsx)$|Test\.java$"
    r"|\.(md|rst|txt)$|(^|/)newsfragments/|(^|/)docs?/"
    r"|/i18n/locales/|uv\.lock$|pnpm-lock\.yaml$|package-lock\.json$|go\.sum$"
    r"|\.(svg|png|jpg|snap)$"
)

GITHUB_API_URL = "https://api.github.com"

# Labels with which selective checks select a different set of tests or versions. Selective checks
# read the labels of the run's event, so a run started before one was added or removed still runs
# the selection of the labels it started with, which the current labels do not show.
TEST_SELECTION_LABELS = frozenset(
    {
        ALL_VERSIONS_LABEL,
        AREA_E2E_TESTS_LABEL,
        AREA_KUBERNETES_TESTS_LABEL,
        CANARY_LABEL,
        DEFAULT_VERSIONS_ONLY_LABEL,
        FULL_TESTS_NEEDED_LABEL,
        LATEST_VERSIONS_ONLY_LABEL,
        UPGRADE_TO_NEWER_DEPENDENCIES_LABEL,
    }
)

# Repositories of stakeholders that maintain large parts of Airflow, with the stakeholder's display
# name. Only people with write access to such a repository can push the commits of its PRs, so their
# runs are always approved, whatever the PR changes or targets. Keyed by repository id rather than
# name: after a rename or deletion someone else could register the name. Find the id with
# ``gh api repos/<owner>/<repo> --jq .id``.
TRUSTED_STAKEHOLDER_REPOSITORIES: dict[int, tuple[str, str]] = {
    65323404: ("astronomer/airflow", "Astronomer"),
    484591340: ("aws-mwaa/upstream-to-airflow", "Amazon"),
    733432432: ("VladaZakharova/airflow", "Google"),
}

# Read from the trusted main checkout, which is also the base branch whose gate job decides the
# PRs that are approved automatically.
FORK_CI_GATE_CONFIG_PATH = AIRFLOW_ROOT_PATH / ".github" / "fork-ci-gate.toml"


@dataclass
class ChangedFile:
    path: str
    changed_lines: int


@dataclass
class ApprovalDecision:
    approve: bool
    reasons: list[str] = field(default_factory=list)
    estimate: CiMinutesEstimate | None = None
    # Posted on the PR once, when the runs are left for a maintainer.
    comment: str | None = None
    # The labels the decision depends on: runs started before one of them changed are not approved.
    decision_labels: frozenset[str] = TEST_SELECTION_LABELS


def count_source_lines(changed_files: list[ChangedFile]) -> int:
    return sum(file.changed_lines for file in changed_files if not NON_SOURCE_FILE_PATTERN.search(file.path))


def find_dependency_files(files: list[str]) -> list[str]:
    return [file for file in files if DEPENDENCY_FILE_PATTERN.search(file)]


def find_new_provider_files(files: list[str]) -> list[str]:
    """
    Return provider.yaml files that the trusted checkout does not have.

    Selective checks run from main's tree and do not know a provider the PR adds, so they cannot
    estimate its tests.
    """
    return [
        file
        for file in files
        if file.startswith("providers/")
        and file.endswith("/provider.yaml")
        and not (AIRFLOW_ROOT_PATH / file).exists()
    ]


def is_gated_to_fork_ci(*, pr: dict[str, Any], github_repository: str, config: GateConfig) -> bool:
    """Whether the PR's ``Tests (AMD)`` run only executes the fork CI gate (AIP-120)."""
    event = GateEvent(
        event_name="pull_request",
        repository=github_repository,
        ref="",
        head_repository=pr["head"]["repo"]["full_name"] if pr["head"].get("repo") else "",
        author_login=(pr.get("user") or {}).get("login") or "",
        author_association=pr.get("author_association") or "",
        created_at=parse_github_datetime(pr["created_at"]),
        labels=tuple(label["name"] for label in pr.get("labels", [])),
    )
    return decide(event, config).reason in GATED_REASONS


def find_trusted_stakeholder(run: dict[str, Any]) -> str | None:
    repository_id = (run.get("head_repository") or {}).get("id")
    trusted = TRUSTED_STAKEHOLDER_REPOSITORIES.get(repository_id) if isinstance(repository_id, int) else None
    return trusted[1] if trusted else None


def decide_trusted_approval(*, stakeholder: str) -> ApprovalDecision:
    return ApprovalDecision(
        approve=True,
        reasons=[f"it comes from a repository of {stakeholder}, a trusted stakeholder"],
        decision_labels=frozenset(),
    )


def find_fork_ci_definition_files(files: list[str]) -> list[str]:
    """The files the fork CI reconciler counts as defining CI, so both decide alike."""
    return [file for file in files if changes_ci_definition([file], file_count=1)]


def decide_gated_approval(*, changed_files: list[ChangedFile]) -> ApprovalDecision:
    """
    Decide on the runs of a PR whose CI runs in the contributor's fork.

    Its project CI run only executes the gate, so its cost and size do not matter. A change to the CI
    definition could remove the gate or add a workflow that does run, so it still needs a maintainer,
    and the author is told why.
    """
    ci_files = find_fork_ci_definition_files([file.path for file in changed_files])
    if ci_files:
        return ApprovalDecision(
            approve=False,
            reasons=[f"its CI runs in the fork, but it changes CI definition files: {', '.join(ci_files)}"],
            comment=ci_change_approval_comment(ci_files=ci_files),
        )
    return ApprovalDecision(
        approve=True,
        reasons=["its CI runs in the contributor's fork, so its project CI run only executes the gate"],
        # The gate reads `use fork ci` and `use project ci` when the run starts and fails when it
        # cannot, so runs started before one of them changed are approved too.
        decision_labels=frozenset(),
    )


def decide_approval(
    *,
    base_branch: str,
    changed_files: list[ChangedFile],
    selective_checks: Any,
) -> ApprovalDecision:
    """Decide whether the workflow runs of a fork PR commit can be approved without a maintainer."""
    try:
        full_tests_needed = selective_checks.full_tests_needed
        all_versions = selective_checks.all_versions
        is_canary_run = selective_checks._is_canary_run()
        upgrade_to_newer_dependencies = selective_checks.upgrade_to_newer_dependencies
        estimate = estimate_ci_minutes(selective_checks)
    except (SystemExit, Exception):
        # Selective checks exit when a PR changes a suspended provider without the label allowing it,
        # and raise on malformed PR content such as an invalid pyproject.toml. Either way, fail closed.
        return ApprovalDecision(approve=False, reasons=["selective checks fail for this change"])
    if base_branch != AUTO_APPROVABLE_BASE_BRANCH:
        return ApprovalDecision(
            approve=False,
            reasons=[f"it targets {base_branch}, not {AUTO_APPROVABLE_BASE_BRANCH}"],
            estimate=estimate,
        )
    paths = [file.path for file in changed_files]
    # CI definition files can change which runner a job uses (``runs-on:``, ``runner-type``
    # computed by selective checks, the scripts build-info runs) or what the jobs run, so they
    # always need a maintainer's look.
    ci_files = find_fork_ci_definition_files(paths)
    if ci_files:
        return ApprovalDecision(
            approve=False,
            reasons=[f"it changes CI definition files: {', '.join(ci_files)}"],
            estimate=estimate,
        )
    dependency_files = find_dependency_files(paths)
    if dependency_files:
        return ApprovalDecision(
            approve=False,
            reasons=[f"it changes dependency files: {', '.join(dependency_files)}"],
            estimate=estimate,
        )
    new_provider_files = find_new_provider_files(paths)
    if new_provider_files:
        return ApprovalDecision(
            approve=False,
            reasons=[f"it adds a new provider: {', '.join(new_provider_files)}"],
            estimate=estimate,
        )
    if full_tests_needed:
        return ApprovalDecision(approve=False, reasons=["it needs the full set of tests"], estimate=estimate)
    if is_canary_run:
        # The canary label adds special tests and integration jobs that the estimate does not model.
        return ApprovalDecision(approve=False, reasons=["it runs as a canary build"], estimate=estimate)
    if all_versions:
        # The estimate models the default Python and database versions only.
        return ApprovalDecision(
            approve=False, reasons=["it runs all Python and database versions"], estimate=estimate
        )
    if upgrade_to_newer_dependencies:
        return ApprovalDecision(approve=False, reasons=["it changes dependencies"], estimate=estimate)
    if estimate.job_minutes > MAX_AUTO_APPROVED_JOB_MINUTES:
        return ApprovalDecision(
            approve=False,
            reasons=[
                f"its jobs take an estimated {estimate.job_minutes} job-minutes "
                f"(more than {MAX_AUTO_APPROVED_JOB_MINUTES})"
            ],
            estimate=estimate,
        )
    source_lines = count_source_lines(changed_files)
    if source_lines > MAX_AUTO_APPROVED_SOURCE_LINES:
        return ApprovalDecision(
            approve=False,
            reasons=[
                f"it changes {source_lines} lines of source code "
                f"(more than {MAX_AUTO_APPROVED_SOURCE_LINES}, tests and docs not counted)"
            ],
            estimate=estimate,
        )
    return ApprovalDecision(
        approve=True,
        reasons=[
            f"its jobs take an estimated {estimate.job_minutes} job-minutes "
            f"(at most {MAX_AUTO_APPROVED_JOB_MINUTES}) and it changes {source_lines} lines of source code"
        ],
        estimate=estimate,
    )


def parse_github_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


class GitHubApi:
    def __init__(self, *, github_repository: str, github_token: str, report_only: bool):
        self.github_repository = github_repository
        self.report_only = report_only
        self._timelines: dict[int, list[dict[str, Any]]] = {}
        self._session = requests.Session()
        self._session.headers.update(
            {
                "Accept": "application/vnd.github+json",
                "Authorization": f"Bearer {github_token}",
                "X-GitHub-Api-Version": "2022-11-28",
            }
        )

    def _get_paginated(self, path: str, *, item_key: str | None = None) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        url: str | None = f"{GITHUB_API_URL}/repos/{self.github_repository}/{path}"
        while url:
            response = self._session.get(url, timeout=60)
            response.raise_for_status()
            payload = response.json()
            items.extend(payload[item_key] if item_key else payload)
            url = response.links.get("next", {}).get("url")
        return items

    def get_runs_waiting_for_approval(self, created_since: datetime) -> list[dict[str, Any]]:
        created = created_since.strftime("%Y-%m-%dT%H:%M:%SZ")
        return self._get_paginated(
            f"actions/runs?status=action_required&event=pull_request&per_page=100&created=%3E%3D{created}",
            item_key="workflow_runs",
        )

    def get_open_pull_requests(self) -> list[dict[str, Any]]:
        return self._get_paginated("pulls?state=open&per_page=100")

    def approve_run(self, run_id: int) -> None:
        if self.report_only:
            console_print(f"[info]Report only: would approve workflow run {run_id}[/]")
            return
        response = self._session.post(
            f"{GITHUB_API_URL}/repos/{self.github_repository}/actions/runs/{run_id}/approve", timeout=60
        )
        response.raise_for_status()

    def add_label(self, pr_number: int, label: str) -> None:
        if self.report_only:
            console_print(f"[info]Report only: would add label '{label}' to PR #{pr_number}[/]")
            return
        response = self._session.post(
            f"{GITHUB_API_URL}/repos/{self.github_repository}/issues/{pr_number}/labels",
            json={"labels": [label]},
            timeout=60,
        )
        response.raise_for_status()

    def has_bot_comment(self, pr_number: int, marker: str) -> bool:
        comments = self._get_paginated(f"issues/{pr_number}/comments?per_page=100")
        # Only the workflow's own comments count; anyone can post a comment containing the marker.
        return any(
            (comment.get("user") or {}).get("login") == "github-actions[bot]" and marker in comment["body"]
            for comment in comments
        )

    def create_comment(self, pr_number: int, body: str) -> None:
        if self.report_only:
            console_print(f"[info]Report only: would comment on PR #{pr_number}[/]")
            return
        response = self._session.post(
            f"{GITHUB_API_URL}/repos/{self.github_repository}/issues/{pr_number}/comments",
            json={"body": body},
            timeout=60,
        )
        response.raise_for_status()

    def get_approval_status(self, sha: str) -> dict[str, Any] | None:
        """The latest ``Workflow approval`` status of the commit."""
        # Paginated: the fork CI reconciler posts a status per job, so a commit can have many contexts.
        statuses = self._get_paginated(f"commits/{sha}/status?per_page=100", item_key="statuses")
        return next((status for status in statuses if status["context"] == APPROVAL_STATUS_CONTEXT), None)

    def is_run_waiting_for_approval(self, run_id: int) -> bool:
        response = self._session.get(
            f"{GITHUB_API_URL}/repos/{self.github_repository}/actions/runs/{run_id}", timeout=60
        )
        response.raise_for_status()
        run = response.json()
        # A run waiting for approval is reported as completed with the action_required conclusion.
        return "action_required" in (run.get("status"), run.get("conclusion"))

    def set_approval_status(self, sha: str, state: str, description: str) -> None:
        if self.report_only:
            console_print(
                f"[info]Report only: would set status {state} on {sha[:10]}: {escape(description)}[/]"
            )
            return
        response = self._session.post(
            f"{GITHUB_API_URL}/repos/{self.github_repository}/statuses/{sha}",
            json={
                "state": state,
                "target_url": APPROVAL_STATUS_URL,
                "description": description,
                "context": APPROVAL_STATUS_CONTEXT,
            },
            timeout=60,
        )
        response.raise_for_status()

    def _get_timeline(self, pr_number: int) -> list[dict[str, Any]]:
        if pr_number not in self._timelines:
            self._timelines[pr_number] = self._get_paginated(f"issues/{pr_number}/timeline?per_page=100")
        return self._timelines[pr_number]

    def get_last_base_change(self, pr_number: int) -> datetime | None:
        changes = [
            parse_github_time(event["created_at"])
            for event in self._get_timeline(pr_number)
            if event.get("event") == "base_ref_changed"
        ]
        return max(changes, default=None)

    def get_last_label_change(self, pr_number: int, labels: frozenset[str]) -> datetime | None:
        """When one of ``labels`` was last added to or removed from the PR."""
        changes = [
            parse_github_time(event["created_at"])
            for event in self._get_timeline(pr_number)
            if event.get("event") in ("labeled", "unlabeled")
            and (event.get("label") or {}).get("name") in labels
        ]
        return max(changes, default=None)

    def remove_label(self, pr_number: int, label: str) -> None:
        if self.report_only:
            console_print(f"[info]Report only: would remove label '{label}' from PR #{pr_number}[/]")
            return
        response = self._session.delete(
            f"{GITHUB_API_URL}/repos/{self.github_repository}/issues/{pr_number}/labels/{label}",
            timeout=60,
        )
        if response.status_code != 404:
            response.raise_for_status()


def is_shallow_repository() -> bool:
    result = run_command(
        ["git", "rev-parse", "--is-shallow-repository"],
        check=False,
        capture_output=True,
        text=True,
        dry_run_override=False,
    )
    return result.stdout.strip() == "true"


def fetch_merge_commit(*, github_repository: str, pr_number: int, head_sha: str) -> str | None:
    """
    Fetch the PR's merge commit (what the CI run tests) and return its sha.

    Return None when there is no merge commit or it was not built from ``head_sha``, so the result
    is always tied to the commit whose runs get approved.
    """
    ref = f"refs/remotes/auto-approve/pull/{pr_number}/merge"
    # Only the merge commit and its parents are needed. Limit the depth only in an already shallow
    # checkout (CI): in a full clone, --depth would record shallow boundaries and truncate history.
    depth = ["--depth=2"] if is_shallow_repository() else []
    result = run_command(
        [
            "git",
            "fetch",
            "--no-tags",
            *depth,
            f"https://github.com/{github_repository}.git",
            f"+refs/pull/{pr_number}/merge:{ref}",
        ],
        check=False,
        capture_output=True,
        text=True,
        dry_run_override=False,
    )
    if result.returncode != 0:
        return None
    parents = run_command(
        ["git", "rev-list", "--parents", "-n", "1", ref],
        check=False,
        capture_output=True,
        text=True,
        dry_run_override=False,
    )
    if parents.returncode != 0:
        return None
    merge_sha, *parent_shas = parents.stdout.split()
    if len(parent_shas) != 2 or parent_shas[1] != head_sha:
        return None
    return merge_sha


NUMSTAT_RECORD = re.compile(r"(\d+|-)\t(\d+|-)\t(.+)", re.DOTALL)


def get_changed_files(merge_sha: str) -> list[ChangedFile]:
    """
    List the files the merge commit changes, with their changed line counts.

    Paths must be literal for the CI-definition and source checks: without ``-z`` git C-quotes unusual
    file names, and rename detection reports ``old => new`` instead of both paths.
    """
    result = run_command(
        [
            "git",
            "-c",
            "core.quotePath=false",
            "diff",
            "--no-renames",
            "--numstat",
            "-z",
            f"{merge_sha}^1",
            merge_sha,
        ],
        check=True,
        capture_output=True,
        text=True,
        dry_run_override=False,
    )
    changed_files = []
    for record in result.stdout.split("\0"):
        if not record:
            continue
        match = NUMSTAT_RECORD.fullmatch(record)
        if match is None:
            raise ValueError(f"Unexpected git numstat record: {record!r}")
        added, deleted, path = match.groups()
        # Binary files report "-" for both counts; count them over the limit so they never pass.
        changed_lines = int(added) + int(deleted) if added != "-" else MAX_AUTO_APPROVED_SOURCE_LINES + 1
        changed_files.append(ChangedFile(path=path, changed_lines=changed_lines))
    return changed_files


def build_selective_checks(
    *, merge_sha: str, changed_files: list[ChangedFile], pr_labels: tuple[str, ...], base_branch: str
):
    from airflow_breeze.utils.selective_checks import SelectiveChecks

    # Release-branch PRs select different jobs (no provider tests, for example), so selective checks
    # must see the branch the PR targets.
    return SelectiveChecks(
        files=tuple(file.path for file in changed_files),
        default_branch=base_branch,
        commit_ref=merge_sha,
        pr_labels=pr_labels,
        github_event=GithubEvents.PULL_REQUEST,
    )


@dataclass
class PullRequestResult:
    pr_number: int
    head_sha: str
    run_ids: list[int]
    decision: ApprovalDecision
    failed: bool = False


def process_waiting_runs(api: GitHubApi, *, created_since: datetime) -> list[PullRequestResult]:
    """
    Decide on the runs created since ``created_since``.

    Older waiting runs are left alone, so the workflow never approves a backlog of stale runs at once.
    """
    runs = api.get_runs_waiting_for_approval(created_since)
    if not runs:
        console_print(
            f"[info]No workflow runs created since {created_since:%Y-%m-%d %H:%M} UTC wait for approval.[/]"
        )
        return []
    runs_by_head: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for run in runs:
        head_repository = (run.get("head_repository") or {}).get("full_name", "")
        if head_repository == api.github_repository:
            continue
        runs_by_head.setdefault((head_repository, run["head_branch"], run["head_sha"]), []).append(run)
    # The runs of fork PRs do not list their pull request, so match them by head repository, branch
    # and commit.
    pull_requests: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for pr in api.get_open_pull_requests():
        head_repository = pr["head"]["repo"]["full_name"] if pr["head"].get("repo") else ""
        pull_requests.setdefault((head_repository, pr["head"]["ref"], pr["head"]["sha"]), []).append(pr)
    results = []
    for (head_repository, head_branch, head_sha), head_runs in runs_by_head.items():
        run_ids = [run["id"] for run in head_runs]
        matching_prs = pull_requests.get((head_repository, head_branch, head_sha), [])
        if not matching_prs:
            console_print(
                f"[warning]No open PR has head {escape(head_repository)}:{escape(head_branch)}@{head_sha}; "
                f"leaving runs {run_ids} alone[/]"
            )
            continue
        # The runs of a group share their head repository.
        stakeholder = find_trusted_stakeholder(head_runs[0])
        # Approving a run again fails, and the PRs that share a trusted head share its runs.
        approved_run_ids: set[int] = set()
        for pr in matching_prs:
            failed = False
            decision: ApprovalDecision | None = None
            try:
                if stakeholder is not None:
                    decision = decide_trusted_approval(stakeholder=stakeholder)
                elif len(matching_prs) > 1:
                    # The runs cannot be told apart by PR, so one PR's decision must not approve them all.
                    decision = ApprovalDecision(
                        approve=False, reasons=["its head commit is the head of several open PRs"]
                    )
                else:
                    decision = decide_for_pull_request(api=api, pr=pr)
                pr_run_ids = run_ids
                # A trusted repository's runs do not depend on the base branch or labels they started with.
                if decision.approve and stakeholder is None:
                    pr_run_ids = find_runs_started_for_current_pr(
                        api=api, pr=pr, runs=head_runs, decision_labels=decision.decision_labels
                    )
                    if not pr_run_ids:
                        decision = ApprovalDecision(
                            approve=False,
                            reasons=[
                                "its waiting runs started before the PR was opened, retargeted "
                                "or changed a label its decision depends on"
                            ],
                            estimate=decision.estimate,
                        )
                        pr_run_ids = run_ids
                if decision.approve:
                    pr_run_ids = [run_id for run_id in pr_run_ids if run_id not in approved_run_ids]
                apply_decision(api=api, pr=pr, run_ids=pr_run_ids, decision=decision)
                if decision.approve:
                    approved_run_ids.update(pr_run_ids)
            except (requests.RequestException, subprocess.CalledProcessError) as e:
                # One failing PR must not stop the others; the failure is reported through ``failed``.
                failed = True
                console_print(f"[warning]PR #{pr['number']}: processing failed: {escape(str(e))}[/]")
                pr_run_ids = run_ids
                # Without the label a PR that keeps failing drops out of the query window unnoticed.
                try:
                    has_label = has_need_workflow_approval_label(pr)
                    if not has_label:
                        api.add_label(pr["number"], NEED_WORKFLOW_APPROVAL_LABEL)
                    # The error itself is only in the job summary: it can show the workflow's internals.
                    status_changed = update_approval_status(
                        api,
                        sha=head_sha,
                        state="pending",
                        description="Waits for a maintainer: the automatic check failed",
                    )
                    if not has_label or status_changed:
                        clear_if_runs_were_approved(api, pr=pr, run_ids=pr_run_ids)
                except requests.RequestException as label_error:
                    console_print(
                        f"[warning]PR #{pr['number']}: adding the label failed: {escape(str(label_error))}[/]"
                    )
                decision = ApprovalDecision(
                    approve=False,
                    reasons=[build_failure_reason(e)],
                    estimate=decision.estimate if decision else None,
                )
            results.append(
                PullRequestResult(
                    pr_number=pr["number"],
                    head_sha=head_sha,
                    run_ids=pr_run_ids,
                    decision=decision,
                    failed=failed,
                )
            )
    return results


def find_runs_started_for_current_pr(
    *,
    api: GitHubApi,
    pr: dict[str, Any],
    runs: list[dict[str, Any]],
    decision_labels: frozenset[str] = TEST_SELECTION_LABELS,
) -> list[int]:
    """
    Return the runs started after the PR was opened and after its base branch last changed.

    Retargeting a PR does not start new runs, and a closed PR can leave runs with the same head behind.
    Such runs were started for another base branch, so the decision about the current PR must not
    approve them.

    :param decision_labels: the labels the decision depends on. It is made from the current labels,
        while a run can act on those of its event: with ``full tests needed`` removed, or
        ``default versions only`` added to a run of all versions, the estimate is smaller than the
        run.
    """
    started_after = parse_github_time(pr["created_at"])
    changes = [
        api.get_last_base_change(pr["number"]),
        api.get_last_label_change(pr["number"], decision_labels),
    ]
    started_after = max([started_after, *(change for change in changes if change is not None)])
    return [run["id"] for run in runs if parse_github_time(run["created_at"]) >= started_after]


def decide_for_pull_request(*, api: GitHubApi, pr: dict[str, Any]) -> ApprovalDecision:
    merge_sha = fetch_merge_commit(
        github_repository=api.github_repository, pr_number=pr["number"], head_sha=pr["head"]["sha"]
    )
    if merge_sha is None:
        return ApprovalDecision(
            approve=False, reasons=["its merge commit is missing or was not built from the PR's head commit"]
        )
    try:
        changed_files = get_changed_files(merge_sha)
    except ValueError:
        return ApprovalDecision(approve=False, reasons=["its changed files could not be listed"])
    if pr["base"]["ref"] == AUTO_APPROVABLE_BASE_BRANCH and is_gated_to_fork_ci(
        pr=pr, github_repository=api.github_repository, config=load_config(FORK_CI_GATE_CONFIG_PATH)
    ):
        return decide_gated_approval(changed_files=changed_files)
    pr_labels = tuple(label["name"] for label in pr.get("labels", []))
    return decide_approval(
        base_branch=pr["base"]["ref"],
        changed_files=changed_files,
        selective_checks=build_selective_checks(
            merge_sha=merge_sha,
            changed_files=changed_files,
            pr_labels=pr_labels,
            base_branch=pr["base"]["ref"],
        ),
    )


def build_failure_reason(error: Exception) -> str:
    text = " ".join(str(error).split())
    if len(text) > MAX_ERROR_TEXT_LENGTH:
        # The end of a failed git command holds its exit status, so keep both ends.
        half = (MAX_ERROR_TEXT_LENGTH - len("...")) // 2
        text = f"{text[:half]}...{text[-half:]}"
    reason = f"processing failed with {type(error).__name__}"
    return f"{reason}: {text}" if text else reason


def build_status_description(prefix: str, reasons: list[str]) -> str:
    description = f"{prefix}: {'; '.join(reasons)}"
    if len(description) > MAX_STATUS_DESCRIPTION_LENGTH:
        description = f"{description[: MAX_STATUS_DESCRIPTION_LENGTH - len('...')]}..."
    return description


def update_approval_status(api: GitHubApi, *, sha: str, state: str, description: str) -> bool:
    """
    Set the commit's ``Workflow approval`` status unless it already has this one; return whether it did.

    Every tick decides again on the runs of the last 24 hours, and repeating the same status would
    only spend the API's stricter write limit.
    """
    current = api.get_approval_status(sha)
    if current is not None and (current["state"], current["description"]) == (state, description):
        return False
    api.set_approval_status(sha, state, description)
    return True


def clear_if_runs_were_approved(api: GitHubApi, *, pr: dict[str, Any], run_ids: list[int]) -> bool:
    """
    Undo the label and pending status just written if no run waits any more; return whether it did.

    A maintainer may have approved the runs since they were listed, and the run that started may
    already have cleared the label and status that were just written. An approval after this check
    starts its run after the writes, so mark-workflow-runs-approved.yml clears them.
    """
    if any(api.is_run_waiting_for_approval(run_id) for run_id in run_ids):
        return False
    console_print(f"[info]PR #{pr['number']}: its runs were approved meanwhile[/]")
    api.set_approval_status(pr["head"]["sha"], "success", "Workflow runs approved")
    api.remove_label(pr["number"], NEED_WORKFLOW_APPROVAL_LABEL)
    return True


def has_need_workflow_approval_label(pr: dict[str, Any]) -> bool:
    return any(label["name"] == NEED_WORKFLOW_APPROVAL_LABEL for label in pr.get("labels", []))


def apply_decision(
    *, api: GitHubApi, pr: dict[str, Any], run_ids: list[int], decision: ApprovalDecision
) -> None:
    pr_number = pr["number"]
    has_label = has_need_workflow_approval_label(pr)
    console_print(f"[info]PR #{pr_number}: {format_estimate(decision.estimate)}[/]")
    if decision.approve:
        console_print(
            f"[success]Approving runs {run_ids} of PR #{pr_number}: {escape('; '.join(decision.reasons))}[/]"
        )
        for run_id in run_ids:
            api.approve_run(run_id)
        if has_label:
            api.remove_label(pr_number, NEED_WORKFLOW_APPROVAL_LABEL)
        return
    # Reasons can contain contributor-chosen file names, which must not be read as Rich markup.
    console_print(f"[warning]PR #{pr_number} needs a maintainer: {escape('; '.join(decision.reasons))}[/]")
    if not has_label:
        api.add_label(pr_number, NEED_WORKFLOW_APPROVAL_LABEL)
    status_changed = update_approval_status(
        api,
        sha=pr["head"]["sha"],
        state="pending",
        description=build_status_description("Waits for a maintainer", decision.reasons),
    )
    if (not has_label or status_changed) and clear_if_runs_were_approved(api, pr=pr, run_ids=run_ids):
        return
    # Once per PR: every later push would otherwise repeat it while the change stays.
    if decision.comment is not None and not api.has_bot_comment(pr_number, CI_CHANGE_MARKER):
        api.create_comment(pr_number, decision.comment)


MARKDOWN_SPECIAL_CHARACTERS = re.compile(r"([\\`*_{}\[\]()<>#+!|~])")


def escape_markdown(text: str) -> str:
    """Escape Markdown in contributor-controlled text, such as file names, for the job summary."""
    return MARKDOWN_SPECIAL_CHARACTERS.sub(r"\\\1", text)


def format_estimate(estimate: CiMinutesEstimate | None) -> str:
    if estimate is None:
        return "n/a"
    return f"~{estimate.job_minutes} job-min, ~{estimate.wall_clock_minutes} min wall clock"


def write_step_summary(results: list[PullRequestResult], github_repository: str) -> None:
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if not summary_path:
        return
    lines = [
        "## Workflow run approval",
        "",
        "Estimates are for `Tests (AMD)` on public runners.",
        "",
        "| PR | Head | Decision | Estimated CI time | Reasons |",
        "|---|---|---|---|---|",
    ]
    for result in results:
        if result.failed:
            decision = "failed"
        elif result.decision.approve:
            decision = "approved"
        else:
            decision = f"`{NEED_WORKFLOW_APPROVAL_LABEL}`"
        lines.append(
            f"| [#{result.pr_number}](https://github.com/{github_repository}/pull/{result.pr_number}) "
            f"| `{result.head_sha[:10]}` | {decision} | {format_estimate(result.decision.estimate)} "
            f"| {escape_markdown('; '.join(result.decision.reasons))} |"
        )
    for result in results:
        estimate = result.decision.estimate
        if estimate is None:
            continue
        lines.extend(
            [
                "",
                f"### #{result.pr_number}: expected jobs",
                "",
                "| Job group | Estimated job-minutes | Critical path (min) |",
                "|---|---|---|",
                *(
                    f"| {group.name} | {group.job_minutes} | {group.critical_path_minutes} |"
                    for group in estimate.job_groups
                ),
                f"| **Total** | **{estimate.job_minutes}** | **~{estimate.wall_clock_minutes} wall clock** |",
            ]
        )
    with open(summary_path, "a") as summary_file:
        summary_file.write("\n".join(lines) + "\n")
