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

import base64
import dataclasses
import json
from datetime import UTC, datetime, timedelta
from unittest import mock

import pytest
from click.testing import CliRunner

from airflow_breeze.commands.ci_commands import fork_ci_reconcile
from airflow_breeze.utils.fork_ci_gate import GateConfig
from airflow_breeze.utils.fork_ci_github import GitHubClient, GitHubError
from airflow_breeze.utils.fork_ci_messages import Marker, parse_marker, render_marker
from airflow_breeze.utils.fork_ci_reconcile import (
    BaseGateConfigs,
    BypassFinder,
    ForkObservation,
    ForkState,
    Plan,
    PullRequestSnapshot,
    apply_plan,
    build_slack_payload,
    changes_ci_definition,
    classify,
    count_commits_behind,
    discover,
    fetch_fork_observations,
    fetch_workflow_state,
    find_state_comment,
    jobs_show_project_ci,
    load_workflow_triggers,
    parse_pull_request,
    pull_request_changes_ci_files,
    run_reconcile,
)
from airflow_breeze.utils.path_utils import AIRFLOW_ROOT_PATH

REPO = "apache/airflow"
NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)
CREATED = datetime(2026, 11, 1, tzinfo=UTC)
CONFIG = GateConfig(enabled=True, enabled_since=datetime(2026, 10, 1, tzinfo=UTC), close_after_days=7)
RUN_URL = "https://github.com/someone/airflow/actions/runs/1"
PROJECT_RUN_URL = "https://github.com/apache/airflow/actions/runs/2"
GREEN = ForkObservation(suite_status="COMPLETED", suite_conclusion="SUCCESS", run_url=RUN_URL)


def _pr(**overrides) -> PullRequestSnapshot:
    values = {
        "number": 42,
        "node_id": "PR_node",
        "is_draft": True,
        "labels": ("fork ci required",),
        "author_login": "someone",
        "author_association": "CONTRIBUTOR",
        "created_at": CREATED,
        "head_sha": "abc123",
        "head_repo": "someone/airflow",
        "base_ref": "main",
        "head_ref": "my-change",
        "last_draft_actor": "github-actions",
    }
    values.update(overrides)
    return PullRequestSnapshot(**values)


def _marker(state: str, *, days_ago: float = 0, sha: str = "abc123") -> Marker:
    return Marker(state=state, sha=sha, since=NOW - timedelta(days=days_ago))


def _classify(pr, fork=None, *, bypass_run=None, changes_ci_files=False, config=CONFIG, behind=None):
    find_bypass = mock.Mock(return_value=bypass_run)
    changes = mock.Mock(return_value=changes_ci_files)
    plan = classify(
        pr,
        fork or ForkObservation(),
        config=config,
        now=NOW,
        repo=REPO,
        find_bypass_run=find_bypass,
        changes_ci_files=changes,
        count_behind=mock.Mock(return_value=behind),
    )
    return plan, find_bypass, changes


@pytest.mark.parametrize(
    ("fork", "marker", "expected_state", "expected_status"),
    [
        pytest.param(
            ForkObservation(workflow_state="disabled_fork"),
            None,
            ForkState.SETUP_REQUIRED,
            "error",
            id="workflow-disabled",
        ),
        pytest.param(
            ForkObservation(workflow_state="missing"),
            None,
            ForkState.SETUP_REQUIRED,
            "error",
            id="workflow-missing",
        ),
        pytest.param(
            ForkObservation(workflow_state="active"),
            None,
            ForkState.AWAITING_PUSH,
            "pending",
            id="enabled-no-run",
        ),
        pytest.param(
            ForkObservation(),
            _marker("awaiting_push"),
            ForkState.AWAITING_PUSH,
            "pending",
            id="still-awaiting",
        ),
        pytest.param(
            ForkObservation(suite_status="IN_PROGRESS", run_url=RUN_URL),
            None,
            ForkState.RUNNING,
            "pending",
            id="running",
        ),
        pytest.param(
            ForkObservation(suite_status="COMPLETED", suite_conclusion="FAILURE", run_url=RUN_URL),
            None,
            ForkState.FAILED,
            "failure",
            id="failed",
        ),
        pytest.param(
            ForkObservation(suite_status="COMPLETED", suite_conclusion="CANCELLED", run_url=RUN_URL),
            None,
            ForkState.FAILED,
            "failure",
            id="cancelled",
        ),
        pytest.param(GREEN, None, ForkState.GREEN, "success", id="green"),
    ],
)
def test_state_table(fork, marker, expected_state, expected_status):
    plan, _, _ = _classify(_pr(marker=marker), fork)

    assert plan.state == expected_state
    assert plan.status is not None
    assert plan.status[0] == expected_status
    assert plan.status[1] == (
        fork.run_url or "https://github.com/someone/airflow/actions/workflows/ci-amd.yml"
    )
    assert f'"state": "{expected_state.value}"' in plan.marker_comment
    assert not plan.close
    assert not plan.unknown


def test_setup_required_comment_gives_the_command():
    plan, _, _ = _classify(_pr(), ForkObservation(workflow_state="disabled_fork"))

    assert "gh workflow enable ci-amd.yml -R someone/airflow" in plan.marker_comment


def test_non_green_drafts_and_removes_review_label():
    pr = _pr(is_draft=False, labels=("fork ci required", "ready for maintainer review"))

    plan, _, _ = _classify(pr, ForkObservation(suite_status="IN_PROGRESS", run_url=RUN_URL))

    assert plan.draft is True
    assert plan.remove_labels == ["ready for maintainer review"]


def test_green_undrafts_what_the_bot_drafted():
    plan, _, _ = _classify(_pr(last_draft_actor="github-actions"), GREEN)

    assert plan.draft is False
    assert plan.add_labels == ["ready for maintainer review"]


def test_green_keeps_an_author_draft():
    plan, _, _ = _classify(_pr(last_draft_actor="someone"), GREEN)

    assert plan.draft is None
    assert plan.add_labels == []


def test_green_labels_a_ready_pull_request():
    plan, _, _ = _classify(_pr(is_draft=False), GREEN)

    assert plan.draft is None
    assert plan.add_labels == ["ready for maintainer review"]


def test_green_is_not_trusted_when_ci_files_change():
    pr = _pr(is_draft=False, labels=("fork ci required", "ready for maintainer review"))

    plan, _, changes = _classify(pr, GREEN, changes_ci_files=True)

    assert plan.state == ForkState.GREEN_UNVERIFIED
    assert plan.draft is True
    assert plan.add_labels == ["needs project ci"]
    assert plan.remove_labels == ["ready for maintainer review"]
    assert plan.status == (
        "pending",
        RUN_URL,
        "Fork CI passed, but CI files changed - a maintainer must run project CI",
    )
    assert plan.attention == []
    assert plan.ci_passed is None
    changes.assert_called_once_with()


LABELLED = ("fork ci required", "needs project ci")


@pytest.mark.parametrize(
    ("pr_overrides", "fork", "bypass_run", "expected_state"),
    [
        pytest.param(
            {},
            ForkObservation(suite_status="IN_PROGRESS", run_url=RUN_URL),
            None,
            ForkState.RUNNING,
            id="new-commit-running",
        ),
        pytest.param({}, GREEN, None, ForkState.GREEN, id="green"),
        pytest.param(
            {"labels": (*LABELLED, "use project ci")}, GREEN, None, ForkState.RELEASED, id="released"
        ),
        pytest.param({}, GREEN, PROJECT_RUN_URL, ForkState.GATE_BYPASSED, id="gate-bypassed"),
        pytest.param(
            {"marker": _marker("setup_required", days_ago=7)},
            ForkObservation(workflow_state="disabled_fork"),
            None,
            ForkState.CLOSED,
            id="closed-inactive",
        ),
    ],
)
def test_needs_project_ci_label_is_removed_outside_green_unverified(
    pr_overrides, fork, bypass_run, expected_state
):
    plan, _, _ = _classify(_pr(**{"labels": LABELLED, **pr_overrides}), fork, bypass_run=bypass_run)

    assert plan.state == expected_state
    assert "needs project ci" in plan.remove_labels
    assert "needs project ci" not in plan.add_labels


GREEN_STATUS = ("success", RUN_URL, "Fork CI passed - see Details")
UNVERIFIED_STATUS = (
    "pending",
    RUN_URL,
    "Fork CI passed, but CI files changed - a maintainer must run project CI",
)


CI_PASSED = ("success", RUN_URL, "Fork CI passed for this commit")
CI_PASSED_REVOKED = ("pending", RUN_URL, "Fork CI result no longer trusted for this commit")


@pytest.mark.parametrize(
    ("fork", "changes_ci_files", "ci_passed_state", "expected"),
    [
        pytest.param(GREEN, False, None, CI_PASSED, id="green-posts-success"),
        pytest.param(GREEN, False, "success", None, id="green-already-posted"),
        pytest.param(GREEN, False, "pending", CI_PASSED, id="green-after-revoke"),
        pytest.param(GREEN, True, None, None, id="unverified-gets-none"),
        pytest.param(
            ForkObservation(suite_status="COMPLETED", suite_conclusion="FAILURE", run_url=RUN_URL),
            False,
            "success",
            CI_PASSED_REVOKED,
            id="failed-rerun-revokes",
        ),
        pytest.param(GREEN, True, "success", CI_PASSED_REVOKED, id="unverified-after-retarget-revokes"),
        pytest.param(
            ForkObservation(suite_status="IN_PROGRESS", run_url=RUN_URL),
            False,
            None,
            None,
            id="running-gets-none",
        ),
    ],
)
def test_ci_passed_reports_trusted_green_only(fork, changes_ci_files, ci_passed_state, expected):
    plan, _, _ = _classify(_pr(ci_passed_state=ci_passed_state), fork, changes_ci_files=changes_ci_files)

    assert plan.ci_passed == expected


@pytest.mark.parametrize(
    ("pr_overrides", "behind", "expected_behind", "warned"),
    [
        pytest.param({}, 250, 250, True, id="counted-on-decision"),
        pytest.param({}, 3, 3, False, id="close-to-base"),
        pytest.param(
            {"current_status": GREEN_STATUS, "marker": Marker("green", "abc123", NOW, behind=300)},
            None,
            300,
            True,
            id="kept-from-the-marker",
        ),
        pytest.param(
            {"current_status": GREEN_STATUS, "marker": Marker("green", "old-sha", NOW, behind=300)},
            None,
            None,
            False,
            id="not-kept-for-another-commit",
        ),
    ],
)
def test_green_records_how_far_behind_the_base_branch(pr_overrides, behind, expected_behind, warned):
    plan, _, _ = _classify(_pr(**pr_overrides), GREEN, behind=behind)

    assert parse_marker(plan.marker_comment).behind == expected_behind
    assert ("commits behind `main`" in plan.marker_comment) is warned


@pytest.mark.parametrize(
    ("current_status", "marker", "expected_state", "lists_files"),
    [
        pytest.param(GREEN_STATUS, None, ForkState.GREEN, False, id="status-green"),
        pytest.param(UNVERIFIED_STATUS, None, ForkState.GREEN_UNVERIFIED, False, id="status-unverified"),
        pytest.param(None, None, ForkState.GREEN, True, id="no-status-on-new-commit"),
        pytest.param(
            ("pending", RUN_URL, "Fork CI running - see Details"),
            None,
            ForkState.GREEN,
            True,
            id="newly-green",
        ),
        pytest.param(None, _marker("green"), ForkState.GREEN, True, id="marker-is-not-trusted"),
        pytest.param(("success", RUN_URL, "forged"), None, ForkState.GREEN, True, id="unknown-status"),
    ],
)
def test_green_trust_is_decided_once_per_commit(current_status, marker, expected_state, lists_files):
    plan, _, changes = _classify(
        _pr(current_status=current_status, marker=marker), GREEN, changes_ci_files=False
    )

    assert plan.state == expected_state
    assert changes.called is lists_files
    assert plan.attention == []


@pytest.mark.parametrize(
    ("status_created_at", "lists_files", "reposts_status"),
    [
        pytest.param(NOW - timedelta(hours=2), True, True, id="status-before-retarget"),
        pytest.param(None, True, True, id="status-time-unknown"),
    ],
)
def test_green_trust_is_decided_again_after_retargeting(status_created_at, lists_files, reposts_status):
    pr = _pr(
        current_status=GREEN_STATUS,
        status_created_at=status_created_at,
        last_base_change_at=NOW - timedelta(hours=1),
    )

    plan, _, changes = _classify(pr, GREEN, changes_ci_files=False)

    assert plan.state == ForkState.GREEN
    assert changes.called is lists_files
    assert (plan.status == GREEN_STATUS) is reposts_status


@pytest.mark.parametrize(
    ("files", "expected"),
    [
        pytest.param(["airflow-core/src/airflow/models/dag.py"], False, id="code"),
        pytest.param([".github/workflows/ci-amd.yml"], True, id="workflow"),
        pytest.param([".github/actions/breeze/action.yml"], True, id="action"),
        pytest.param(["dev/breeze/src/airflow_breeze/utils/selective_checks.py"], True, id="breeze"),
        pytest.param(["scripts/ci/prek/check_x.py"], True, id="ci-scripts"),
        pytest.param(["Dockerfile.ci"], True, id="dockerfile"),
        pytest.param(["scripts/in_container/run_ci_tests.sh"], True, id="in-container-scripts"),
        pytest.param([".pre-commit-config.yaml"], True, id="root-prek-config"),
        pytest.param(["dev/.pre-commit-config.yaml"], True, id="nested-prek-config"),
        pytest.param([f"airflow-core/f{i}.py" for i in range(3000)], True, id="too-many-files-to-list"),
    ],
)
def test_changes_ci_definition(files, expected):
    assert changes_ci_definition(files) is expected


@pytest.mark.parametrize(
    ("entries", "expected"),
    [
        pytest.param(
            [{"filename": "docs/old.yml", "previous_filename": ".github/workflows/codeql-analysis.yml"}],
            True,
            id="moved-out-of-ci-path",
        ),
    ],
)
def test_pull_request_changes_ci_files_follows_renames(client, entries, expected):
    client.rest_get.side_effect = [entries, {"head": {"sha": "abc123"}}]

    assert pull_request_changes_ci_files(client, REPO, 42, "abc123") is expected


def test_pull_request_changes_ci_files_gives_no_answer_when_the_head_moved(client):
    client.rest_get.side_effect = [[{"filename": "README.md"}], {"head": {"sha": "pushed-meanwhile"}}]

    assert pull_request_changes_ci_files(client, REPO, 42, "abc123") is None


def test_green_decision_waits_when_the_head_moved():
    plan, _, changes = _classify(_pr(), GREEN, changes_ci_files=None)

    assert plan == Plan()
    changes.assert_called_once_with()


def test_nudge_on_transition_from_setup_required():
    plan, _, _ = _classify(_pr(marker=_marker("setup_required")), ForkObservation(workflow_state="active"))

    assert plan.state == ForkState.AWAITING_PUSH
    assert len(plan.new_comments) == 1
    assert "Push this branch again" in plan.new_comments[0]


@pytest.mark.parametrize(
    ("marker", "should_close"),
    [
        pytest.param(_marker("setup_required", days_ago=7), True, id="seven-days"),
        pytest.param(_marker("setup_required", days_ago=6.9), False, id="under-seven-days"),
        pytest.param(_marker("closed", days_ago=30), False, id="reopened-after-close"),
        pytest.param(_marker("running", days_ago=30), False, id="state-changed"),
    ],
)
def test_close_clock(marker, should_close):
    pr = _pr(marker=marker, last_reopened_by_anyone_at=NOW - timedelta(days=1))
    plan, _, _ = _classify(pr, ForkObservation(workflow_state="disabled_fork"))

    assert plan.close is should_close
    if should_close:
        assert plan.state == ForkState.CLOSED
        assert '"state": "closed"' in plan.marker_comment
        assert "Reopening restarts" in plan.new_comments[0]
        assert plan.status is None
    else:
        assert plan.state == ForkState.SETUP_REQUIRED


@pytest.mark.parametrize(
    "reopened_at",
    [pytest.param(None, id="never-reopened"), pytest.param(NOW - timedelta(days=31), id="reopened-before")],
)
def test_unfinished_closure_is_retried(reopened_at):
    pr = _pr(
        marker=_marker("closed", days_ago=30),
        labels=("fork ci required", "needs project ci"),
        last_reopened_by_anyone_at=reopened_at,
    )

    plan, _, _ = _classify(pr, ForkObservation(workflow_state="disabled_fork"))

    assert plan.state == ForkState.CLOSED
    assert plan.close is True
    assert plan.marker_comment is None
    assert "Reopening restarts" in plan.new_comments[0]
    assert plan.remove_labels == ["needs project ci"]


BYPASSED_STATUS = ("error", PROJECT_RUN_URL, "Project CI ran for a gated pull request - see Details")


@pytest.mark.parametrize(
    ("pr_overrides", "expected_link"),
    [
        pytest.param({"current_status": BYPASSED_STATUS}, PROJECT_RUN_URL, id="never-reopened"),
        pytest.param(
            {"current_status": BYPASSED_STATUS, "last_reopened_by_anyone_at": NOW},
            PROJECT_RUN_URL,
            id="author-reopened",
        ),
        pytest.param({}, "https://github.com/apache/airflow/pull/42/checks", id="status-missing"),
    ],
)
def test_unfinished_tampering_closure_is_retried(pr_overrides, expected_link):
    plan, _, _ = _classify(_pr(marker=_marker("gate_bypassed", days_ago=1), **pr_overrides), GREEN)

    assert plan.state == ForkState.GATE_BYPASSED
    assert plan.close is True
    assert expected_link in plan.new_comments[0]
    assert plan.marker_comment is None


def test_since_is_kept_while_state_is_unchanged():
    marker = Marker("setup_required", "abc123", NOW - timedelta(days=2))

    plan, _, _ = _classify(_pr(marker=marker), ForkObservation(workflow_state="disabled_fork"))

    assert render_marker(marker) in plan.marker_comment


def test_unchanged_status_and_comment_are_not_reposted():
    first, _, _ = _classify(_pr(), GREEN)
    pr = _pr(
        current_status=first.status,
        marker_comment_body=first.marker_comment,
        marker_comment_id=7,
        marker=_marker("green"),
    )

    plan, _, _ = _classify(pr, GREEN)

    assert plan.status is None
    assert plan.marker_comment is None


def test_release_with_use_project_ci():
    plan, find_bypass, _ = _classify(_pr(labels=("fork ci required", "use project ci")), GREEN)

    assert plan.state == ForkState.RELEASED
    assert plan.remove_labels == ["fork ci required"]
    assert plan.draft is False
    assert plan.status == (
        "success",
        "https://github.com/apache/airflow/pull/42/checks",
        "Project CI in use (label `use project ci`) - see the checks",
    )
    assert plan.attention == []
    find_bypass.assert_not_called()


def test_base_branch_without_gate_releases():
    plan, find_bypass, _ = _classify(_pr(base_ref="v2-10-test"), GREEN, config=None)

    assert plan.state == ForkState.UNGATED
    assert plan.remove_labels == ["fork ci required"]
    assert plan.draft is False
    assert plan.status == (
        "success",
        "https://github.com/apache/airflow/pull/42/checks",
        "Project CI in use (base branch without fork CI) - see the checks",
    )
    assert "`v2-10-test` does not use fork CI" in plan.marker_comment
    find_bypass.assert_not_called()


def test_both_override_labels_are_reported():
    plan, _, _ = _classify(_pr(labels=("fork ci required", "use project ci", "use fork ci")), GREEN)

    assert plan.state == ForkState.RELEASED
    assert len(plan.attention) == 1


def test_deleted_head_repository_is_unknown():
    plan, _, _ = _classify(_pr(head_repo=None), GREEN)

    assert plan.unknown == ["head repository no longer exists"]
    assert plan.status is None
    assert plan.draft is None


def test_malformed_marker_is_reported_and_rewritten():
    plan, _, _ = _classify(_pr(marker_error="Malformed fork CI marker", marker_comment_id=3), GREEN)

    assert plan.unknown == ["Malformed fork CI marker"]
    assert plan.state == ForkState.GREEN
    assert plan.marker_comment is not None


@pytest.mark.parametrize(
    "fork",
    [
        pytest.param(ForkObservation(suite_status="COMPLETED", suite_conclusion="NEUTRAL"), id="neutral"),
        pytest.param(ForkObservation(suite_status="WEIRD"), id="status"),
        pytest.param(ForkObservation(workflow_state="deleted"), id="workflow-state"),
        pytest.param(ForkObservation(error="fork someone/airflow is not readable"), id="unreadable"),
    ],
)
def test_unexpected_fork_states_are_unknown(fork):
    plan, _, _ = _classify(_pr(), fork)

    assert plan.state is None
    assert len(plan.unknown) == 1
    assert plan.status is None


@pytest.mark.parametrize(
    ("pr_overrides", "checked"),
    [
        pytest.param({}, True, id="external"),
        pytest.param({"author_login": "pilot-user"}, True, id="contributor-pilot"),
        pytest.param(
            {"author_association": "MEMBER", "labels": ("fork ci required", "use fork ci")},
            False,
            id="committer-pilot-exempt",
        ),
    ],
)
def test_gate_tampering_closes(pr_overrides, checked):
    config = dataclasses.replace(CONFIG, pilot={"pilot-user": CREATED - timedelta(days=1)})

    plan, find_bypass, _ = _classify(
        _pr(is_draft=False, **pr_overrides), GREEN, bypass_run=PROJECT_RUN_URL, config=config
    )

    assert find_bypass.called is checked
    assert plan.close is checked
    if checked:
        assert plan.state == ForkState.GATE_BYPASSED
        assert plan.draft is True
        assert plan.status == (
            "error",
            PROJECT_RUN_URL,
            "Project CI ran for a gated pull request - see Details",
        )
        assert PROJECT_RUN_URL in plan.new_comments[0]
        assert len(plan.attention) == 1
    else:
        assert plan.state == ForkState.GREEN


# ---------------------------------------------------------------------------------------------
# Gate tampering detection from recorded runs
# ---------------------------------------------------------------------------------------------


def _job(
    name: str, conclusion: str | None = "skipped", minutes: float = 0.2, status: str = "completed"
) -> dict:
    start = NOW - timedelta(hours=1)
    return {
        "name": name,
        "status": status,
        "conclusion": conclusion,
        "started_at": start.isoformat(),
        "completed_at": (start + timedelta(minutes=minutes)).isoformat() if status == "completed" else None,
    }


@pytest.mark.parametrize(
    ("jobs", "expected"),
    [
        pytest.param([_job("Fork CI gate", "success"), _job("Build info")], False, id="gate-only"),
        pytest.param(
            [_job("Fork CI gate", "success"), _job("Build info", "success")], True, id="build-info-ran"
        ),
        pytest.param(
            [_job("Fork CI gate", "success"), _job("Tests", None, status="in_progress")],
            True,
            id="job-in-progress",
        ),
        pytest.param(
            [_job("Fork CI gate", "success"), _job("Tests", None, status="queued")],
            False,
            id="queued-job-is-not-counted",
        ),
        pytest.param([_job("Fork CI gate", None, status="queued")], False, id="gate-waiting-for-a-runner"),
        pytest.param([_job("Fork CI gate", None, status="in_progress")], True, id="gate-running-for-an-hour"),
        pytest.param([_job("Build info", "success")], True, id="gate-removed"),
        pytest.param(
            [_job("Fork CI gate", "success"), _job("Fork CI gate", "success")], True, id="gate-twice"
        ),
        pytest.param([_job("Fork CI gate", "success", minutes=30)], True, id="work-under-gate-name"),
    ],
)
def test_jobs_show_project_ci(jobs, expected):
    assert jobs_show_project_ci(jobs, NOW) is expected


def _run(
    run_id: int,
    *,
    path=".github/workflows/ci-amd.yml",
    minutes=20.0,
    sha="sha-old",
    age_hours=1.0,
    event="pull_request",
    status="completed",
):
    created = NOW - timedelta(hours=age_hours)
    return {
        "id": run_id,
        "path": path,
        "html_url": f"https://github.com/apache/airflow/actions/runs/{run_id}",
        "head_sha": sha,
        "head_repository": {"full_name": "someone/airflow"},
        "head_branch": "my-change",
        "created_at": created.isoformat(),
        "run_started_at": created.isoformat(),
        "updated_at": (created + timedelta(minutes=minutes)).isoformat(),
        "status": status,
        "event": event,
    }


BASE_TRIGGERS = {
    "ci-amd.yml": frozenset({"schedule", "pull_request", "push", "workflow_dispatch"}),
    "ci-arm.yml": frozenset({"schedule", "push", "workflow_dispatch"}),
    "codeql-analysis.yml": frozenset({"pull_request", "push", "schedule"}),
}


def _finder(
    client,
    runs,
    *,
    jobs=(),
    compare_files=(".github/workflows/ci-amd.yml",),
    total_count=None,
):
    def rest_get_pages(path):
        event = path.split("event=", 1)[1].split("&", 1)[0]
        event_runs = [run for run in runs if run["event"] == event]
        count = len(event_runs) if total_count is None else total_count
        return [{"total_count": count, "workflow_runs": event_runs}]

    def rest_get(path, paginate=False, items_key=None):
        if path.endswith("jobs?filter=latest&per_page=100"):
            return list(jobs)
        if "/compare/" in path:
            assert not paginate, "the compare API returns files only on its first page"
            return {"files": [{"filename": name} for name in compare_files]}
        raise AssertionError(path)

    client.rest_get_pages.side_effect = rest_get_pages
    client.rest_get.side_effect = rest_get
    return BypassFinder(client, repo=REPO, now=NOW, base_triggers=BASE_TRIGGERS)


@pytest.fixture
def client():
    github = mock.create_autospec(GitHubClient, instance=True)
    github.repo = REPO
    github.calls = {}
    return github


TAMPERED_JOBS = [_job("Fork CI gate", "success"), _job("Build info", "success")]


def test_finder_catches_a_tampered_run_still_in_progress(client):
    """A tampered run that outlasts the tamper window must be caught while it runs."""
    finder = _finder(client, [_run(7, status="in_progress", age_hours=5.5)], jobs=TAMPERED_JOBS)

    assert finder.find(_pr()) == "https://github.com/apache/airflow/actions/runs/7"


def test_finder_catches_a_tampered_run_on_an_earlier_commit(client):
    finder = _finder(client, [_run(7, sha="tampered")], jobs=TAMPERED_JOBS)

    assert finder.find(_pr(head_sha="restored")) == "https://github.com/apache/airflow/actions/runs/7"


@pytest.mark.parametrize(
    ("runs", "jobs", "compare_files", "pr_overrides"),
    [
        pytest.param([_run(7, minutes=1)], TAMPERED_JOBS, (".github/workflows/x.yml",), {}, id="short-run"),
        pytest.param(
            [_run(7, status="queued")],
            TAMPERED_JOBS,
            (".github/workflows/x.yml",),
            {},
            id="queued-run-waiting-for-a-runner",
        ),
        pytest.param(
            [_run(7, status="in_progress", age_hours=0.05)],
            TAMPERED_JOBS,
            (".github/workflows/x.yml",),
            {},
            id="run-started-moments-ago",
        ),
        pytest.param(
            [_run(7)], [_job("Fork CI gate", "success")], (".github/workflows/x.yml",), {}, id="gate-held"
        ),
        pytest.param([_run(7)], TAMPERED_JOBS, ("airflow-core/x.py",), {}, id="no-workflow-change"),
        pytest.param(
            [_run(7)],
            TAMPERED_JOBS,
            (".github/workflows/x.yml",),
            {"last_project_ci_unlabel_at": NOW - timedelta(minutes=30)},
            id="ran-while-use-project-ci",
        ),
        pytest.param(
            [_run(7)],
            TAMPERED_JOBS,
            (".github/workflows/x.yml",),
            {"last_reopened_at": NOW - timedelta(minutes=30)},
            id="before-maintainer-reopened",
        ),
        pytest.param(
            [_run(7, path=".github/workflows/codeql-analysis.yml")],
            (),
            (".github/workflows/x.yml",),
            {},
            id="base-workflow",
        ),
        pytest.param(
            [{**_run(7), "head_branch": "other"}],
            TAMPERED_JOBS,
            (".github/workflows/x.yml",),
            {},
            id="other-branch",
        ),
    ],
)
def test_finder_ignores_legitimate_runs(client, runs, jobs, compare_files, pr_overrides):
    finder = _finder(client, runs, jobs=jobs, compare_files=compare_files)

    assert finder.find(_pr(**pr_overrides)) is None


@pytest.mark.parametrize(
    "run",
    [
        pytest.param(_run(9, path=".github/workflows/injected.yml", minutes=1), id="injected-workflow"),
        pytest.param(
            _run(9, path=".github/workflows/ci-arm.yml", minutes=1), id="pull-request-added-to-ci-arm"
        ),
        pytest.param(
            _run(9, path=".github/workflows/ci-amd.yml", minutes=1, event="pull_request_review"),
            id="review-trigger-added",
        ),
        pytest.param(
            _run(9, path=".github/workflows/codeql-analysis.yml", event="pull_request_review_comment"),
            id="review-comment-trigger-added",
        ),
    ],
)
def test_finder_catches_triggers_the_base_branch_does_not_have(client, run):
    finder = _finder(client, [run])

    assert finder.find(_pr()) == "https://github.com/apache/airflow/actions/runs/9"


def test_load_workflow_triggers(tmp_path):
    (tmp_path / "bare-on.yml").write_text("on:\n  pull_request:\n  push:\n")
    (tmp_path / "string.yaml").write_text("on: push\n")
    (tmp_path / "list.yml").write_text("'on': [schedule, workflow_dispatch]\n")
    (tmp_path / "empty.yml").write_text("")
    (tmp_path / "notes.txt").write_text("on: push\n")

    assert load_workflow_triggers(tmp_path) == {
        "bare-on.yml": frozenset({"pull_request", "push"}),
        "empty.yml": frozenset(),
        "list.yml": frozenset({"schedule", "workflow_dispatch"}),
        "string.yaml": frozenset({"push"}),
    }


def test_repository_workflow_triggers_are_readable():
    triggers = load_workflow_triggers(AIRFLOW_ROOT_PATH / ".github" / "workflows")

    assert "pull_request" in triggers["ci-amd.yml"]
    assert "pull_request" not in triggers["ci-arm.yml"]


def test_finder_fails_closed_on_a_change_set_it_cannot_check(client):
    finder = _finder(client, [_run(7)], jobs=TAMPERED_JOBS, compare_files=[f"f{i}.py" for i in range(300)])

    assert finder.find(_pr()) == "https://github.com/apache/airflow/actions/runs/7"


def test_finder_catches_a_workflow_moved_away(client):
    finder = _finder(client, [_run(7)], jobs=TAMPERED_JOBS)
    client.rest_get.side_effect = lambda path, paginate=False, items_key=None: (
        TAMPERED_JOBS
        if path.endswith("jobs?filter=latest&per_page=100")
        else {"files": [{"filename": "docs/x.yml", "previous_filename": ".github/workflows/x.yml"}]}
    )

    assert finder.find(_pr()) == "https://github.com/apache/airflow/actions/runs/7"


@pytest.mark.parametrize(
    ("total_count", "incomplete"),
    [
        pytest.param(None, False, id="complete"),
        pytest.param(1500, True, id="listing-truncated"),
    ],
)
def test_finder_reports_an_incomplete_run_listing(client, total_count, incomplete):
    finder = _finder(client, [_run(7)], total_count=total_count)

    assert (finder.incomplete is not None) is incomplete


# ---------------------------------------------------------------------------------------------
# GitHub data gathering and the run loop
# ---------------------------------------------------------------------------------------------


def _pr_node(number: int, **overrides) -> dict:
    node = {
        "number": number,
        "id": f"PR_{number}",
        "isDraft": True,
        "createdAt": "2026-11-01T00:00:00Z",
        "authorAssociation": "CONTRIBUTOR",
        "baseRefName": "main",
        "headRefName": f"branch-{number}",
        "headRefOid": f"sha{number}",
        "author": {"login": "someone"},
        "headRepository": {"nameWithOwner": "someone/my-airflow"},
        "labels": {"nodes": [{"name": "fork ci required"}]},
        "timelineItems": {"nodes": []},
        "commits": {"nodes": [{"commit": {"status": None}}]},
        "comments": {"nodes": []},
    }
    node.update(overrides)
    return node


def _search_result(*nodes: dict) -> dict:
    return {"search": {"pageInfo": {"hasNextPage": False, "endCursor": None}, "nodes": list(nodes)}}


def test_parse_pull_request_reads_timeline_status_and_marker():
    marker = _marker("running")
    node = _pr_node(
        1,
        timelineItems={
            "nodes": [
                {
                    "__typename": "ConvertToDraftEvent",
                    "createdAt": "2026-11-02T00:00:00Z",
                    "actor": {"login": "x"},
                },
                {
                    "__typename": "UnlabeledEvent",
                    "createdAt": "2026-11-03T00:00:00Z",
                    "label": {"name": "use project ci"},
                },
                {
                    "__typename": "ConvertToDraftEvent",
                    "createdAt": "2026-11-04T00:00:00Z",
                    "actor": {"login": "github-actions"},
                },
                {
                    "__typename": "ReopenedEvent",
                    "createdAt": "2026-11-05T00:00:00Z",
                    "actor": {"login": "maintainer"},
                },
                {
                    "__typename": "ReopenedEvent",
                    "createdAt": "2026-11-06T00:00:00Z",
                    "actor": {"login": "someone"},
                },
                {"__typename": "BaseRefChangedEvent", "createdAt": "2026-11-07T00:00:00Z"},
            ]
        },
        commits={
            "nodes": [
                {
                    "commit": {
                        "status": {
                            "context": {
                                "state": "PENDING",
                                "targetUrl": "u",
                                "description": "d",
                                "createdAt": "2026-11-08T00:00:00Z",
                            },
                            "ciPassed": {"state": "SUCCESS"},
                        }
                    }
                }
            ]
        },
        comments={
            "nodes": [
                {"databaseId": 5, "body": render_marker(marker), "author": {"login": "github-actions"}},
                {"databaseId": 6, "body": render_marker(_marker("green")), "author": {"login": "someone"}},
                {
                    "databaseId": 7,
                    "body": f"Echoed title: {render_marker(_marker('green'))}",
                    "author": {"login": "github-actions"},
                },
            ]
        },
    )

    pr = parse_pull_request(node)

    assert pr.last_draft_actor == "github-actions"
    assert pr.last_project_ci_unlabel_at == datetime(2026, 11, 3, tzinfo=UTC)
    assert pr.last_reopened_at == datetime(2026, 11, 5, tzinfo=UTC)
    assert pr.last_reopened_by_anyone_at == datetime(2026, 11, 6, tzinfo=UTC)
    assert pr.last_base_change_at == datetime(2026, 11, 7, tzinfo=UTC)
    assert pr.current_status == ("pending", "u", "d")
    assert pr.status_created_at == datetime(2026, 11, 8, tzinfo=UTC)
    assert pr.ci_passed_state == "success"
    assert pr.marker == marker
    assert pr.marker_comment_id == 5
    assert pr.head_repo == "someone/my-airflow"
    assert pr.head_ref == "branch-1"


def test_discover_labels_pilot_pull_requests_while_disabled(client):
    config = GateConfig(enabled=False, pilot={"pilot-user": datetime(2026, 11, 1, tzinfo=UTC)})
    new = {**_pr_node(1), "author": {"login": "pilot-user"}, "labels": {"nodes": []}}
    old = {**_pr_node(2), "author": {"login": "pilot-user"}, "createdAt": "2026-10-20T00:00:00Z"}
    other = {**_pr_node(3), "labels": {"nodes": []}}
    client.graphql.side_effect = [_search_result(new, old, other), _search_result()]

    configs = BaseGateConfigs(client, repo=REPO, default_branch="main", default_config=config)

    labelled = discover(client, configs, repo=REPO, now=NOW)

    assert labelled == [1]
    client.add_labels.assert_called_once_with(1, ["fork ci required"])


def _contents(files: dict[tuple[str, str], str]):
    def rest_get(path, paginate=False, items_key=None):
        name, ref = path.removeprefix(f"repos/{REPO}/contents/").split("?ref=")
        if (name, ref) not in files:
            raise GitHubError("gh api failed: HTTP 404: Not Found")
        return {"content": base64.b64encode(files[name, ref].encode()).decode()}

    return rest_get


GATE_FILES = (".github/fork-ci-gate.toml", "dev/breeze/src/airflow_breeze/utils/fork_ci_gate.py")


@pytest.mark.parametrize(
    ("files", "expected"),
    [
        pytest.param({}, None, id="no-gate"),
        pytest.param(
            {(GATE_FILES[0], "v3-1-test"): "enabled = false\n", (GATE_FILES[1], "v3-1-test"): "# gate\n"},
            GateConfig(enabled=False),
            id="gate",
        ),
    ],
)
def test_base_gate_configs_read_the_base_branch(client, files, expected):
    client.rest_get.side_effect = _contents(files)
    configs = BaseGateConfigs(client, repo=REPO, default_branch="main", default_config=CONFIG)

    assert configs.get("main") is CONFIG
    assert configs.get("v3-1-test") == expected
    assert configs.get("v3-1-test") == expected
    assert client.rest_get.call_count == 2


def test_base_gate_configs_reject_a_half_gate(client):
    client.rest_get.side_effect = _contents({(GATE_FILES[1], "v3-1-test"): "# gate\n"})
    configs = BaseGateConfigs(client, repo=REPO, default_branch="main", default_config=CONFIG)

    with pytest.raises(ValueError, match="only one of"):
        configs.get("v3-1-test")


def test_discover_skips_base_branches_without_the_gate(client):
    ungated = _pr_node(1, baseRefName="v2-10-test", labels={"nodes": []})
    half_gated = _pr_node(2, baseRefName="v3-1-test", labels={"nodes": []})
    client.graphql.side_effect = [_search_result(ungated, half_gated), _search_result()]
    client.rest_get.side_effect = _contents({(GATE_FILES[1], "v3-1-test"): "# gate\n"})
    configs = BaseGateConfigs(client, repo=REPO, default_branch="main", default_config=CONFIG)
    unknown: list[tuple[int, str]] = []

    assert discover(client, configs, repo=REPO, now=NOW, unknown=unknown) == []
    client.add_labels.assert_not_called()
    assert [number for number, _ in unknown] == [2]


def test_fork_observations_use_the_fork_name_and_latest_suite(client):
    pr = parse_pull_request(_pr_node(1))
    client.graphql.return_value = {
        "pr0": {
            "object": {
                "checkSuites": {
                    "nodes": [
                        {
                            "createdAt": "2026-11-01T00:00:00Z",
                            "status": "COMPLETED",
                            "conclusion": "FAILURE",
                            "workflowRun": {
                                "url": "old",
                                "event": "push",
                                "workflow": {"resourcePath": "/x/ci-amd.yml"},
                            },
                        },
                        {
                            "createdAt": "2026-11-02T00:00:00Z",
                            "status": "COMPLETED",
                            "conclusion": "SUCCESS",
                            "workflowRun": {
                                "url": "new",
                                "event": "push",
                                "workflow": {"resourcePath": "/x/ci-amd.yml"},
                            },
                        },
                        {
                            "createdAt": "2026-11-03T00:00:00Z",
                            "status": "COMPLETED",
                            "conclusion": "FAILURE",
                            "workflowRun": {
                                "url": "other",
                                "event": "push",
                                "workflow": {"resourcePath": "/x/codeql.yml"},
                            },
                        },
                        {
                            "createdAt": "2026-11-04T00:00:00Z",
                            "status": "COMPLETED",
                            "conclusion": "SUCCESS",
                            "workflowRun": {
                                "url": "fork-internal-pull-request",
                                "event": "pull_request",
                                "workflow": {"resourcePath": "/x/ci-amd.yml"},
                            },
                        },
                    ]
                }
            }
        }
    }

    observations = fetch_fork_observations(client, [pr])

    assert observations[1] == ForkObservation(
        suite_status="COMPLETED", suite_conclusion="SUCCESS", run_url="new"
    )
    assert client.graphql.call_args.args[1] == {"o0": "someone", "n0": "my-airflow", "s0": "sha1"}


def test_state_comment_is_found_beyond_the_first_page(client):
    marker = _marker("running")
    first_page = {
        "pageInfo": {"hasNextPage": True, "endCursor": "c1"},
        "nodes": [{"databaseId": 1, "body": "hello", "author": {"login": "someone"}}],
    }
    pr = parse_pull_request(_pr_node(1, comments=first_page))
    assert pr.comments_cursor == "c1"
    client.graphql.side_effect = [
        {
            "repository": {
                "pullRequest": {
                    "comments": {
                        "pageInfo": {"hasNextPage": True, "endCursor": "c2"},
                        "nodes": [{"databaseId": 2, "body": "more", "author": {"login": "other"}}],
                    }
                }
            }
        },
        {
            "repository": {
                "pullRequest": {
                    "comments": {
                        "pageInfo": {"hasNextPage": False, "endCursor": None},
                        "nodes": [
                            {
                                "databaseId": 3,
                                "body": render_marker(marker),
                                "author": {"login": "github-actions"},
                            }
                        ],
                    }
                }
            }
        },
    ]

    found = find_state_comment(client, REPO, pr)

    assert found.marker == marker
    assert found.marker_comment_id == 3
    assert found.comments_cursor is None
    assert [c.args[1]["cursor"] for c in client.graphql.call_args_list] == ["c1", "c2"]


def test_unreadable_fork_is_an_error_observation(client):
    client.graphql.return_value = {"pr0": None}

    observations = fetch_fork_observations(client, [parse_pull_request(_pr_node(1))])

    assert observations[1].error == "fork someone/my-airflow is not readable"


@pytest.mark.parametrize(
    ("side_effect", "expected"),
    [
        pytest.param([{"state": "disabled_fork"}], "disabled_fork", id="state"),
        pytest.param(GitHubError("gh api failed: HTTP 404: Not Found"), "missing", id="missing"),
    ],
)
def test_fetch_workflow_state(client, side_effect, expected):
    client.rest_get.side_effect = side_effect

    assert fetch_workflow_state(client, "someone/airflow") == expected


def test_run_reconcile_isolates_failures_and_reports_them(client, tmp_path):
    output = tmp_path / "slack-message.json"
    client.graphql.side_effect = [
        _search_result(),
        _search_result(),
        _search_result(_pr_node(1), _pr_node(2)),
        {"pr0": {"object": None}, "pr1": {"object": None}},
    ]
    workflow_state_calls = []
    client.rest_get_pages.return_value = [{"total_count": 0, "workflow_runs": []}]

    def rest_get(path, paginate=False, items_key=None):
        if path == "repos/someone/my-airflow/actions/workflows/ci-amd.yml":
            workflow_state_calls.append(path)
            if len(workflow_state_calls) == 1:
                raise GitHubError("gh api failed: HTTP 500")
            return {"state": "disabled_fork"}
        raise AssertionError(path)

    client.rest_get.side_effect = rest_get

    has_alerts = run_reconcile(
        client, CONFIG, now=NOW, run_url="https://run", output_file=output, workflows_dir=tmp_path
    )

    assert has_alerts is True
    payload = json.loads(output.read_text())
    assert payload["channel"] == "internal-airflow-ci-cd"
    text = "\n".join(block["text"]["text"] for block in payload["blocks"])
    assert "|#1>: GitHubError: gh api failed: HTTP 500" in text
    assert "#2" not in text
    client.set_status.assert_called_once()
    assert client.set_status.call_args.args[:3] == (2, "sha2", "error")


@mock.patch("airflow_breeze.utils.fork_ci_reconcile.run_reconcile", autospec=True, return_value=True)
def test_command_writes_has_alerts_output(mock_run, tmp_path, monkeypatch):
    github_output = tmp_path / "github_output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(github_output))

    result = CliRunner().invoke(fork_ci_reconcile, ["--dry-run", "--output-file", str(tmp_path / "s.json")])

    assert result.exit_code == 0, result.output
    assert github_output.read_text() == "has-alerts=true\n"
    github_client = mock_run.call_args.args[0]
    assert github_client.dry_run is True
    assert github_client.repo == "apache/airflow"


@pytest.mark.parametrize(
    ("total_count", "has_alerts"),
    [
        pytest.param(0, False, id="nothing-to-do"),
        pytest.param(1500, True, id="run-listing-truncated"),
    ],
)
def test_run_reconcile_without_gated_pull_requests(client, tmp_path, total_count, has_alerts):
    output = tmp_path / "slack-message.json"
    client.graphql.side_effect = [_search_result(), _search_result(), _search_result()]
    client.rest_get_pages.return_value = [{"total_count": total_count, "workflow_runs": []}]

    assert (
        run_reconcile(
            client, CONFIG, now=NOW, run_url="https://run", output_file=output, workflows_dir=tmp_path
        )
        is has_alerts
    )
    assert output.exists() is has_alerts
    if has_alerts:
        assert "*Incomplete:* only 0 of 1500" in output.read_text()


@pytest.mark.parametrize(
    "fails", [pytest.param(False, id="all-succeed"), pytest.param(True, id="status-fails")]
)
def test_fork_ci_required_is_removed_after_everything_else(client, fails):
    plan, _, _ = _classify(_pr(labels=("fork ci required", "use project ci")), GREEN)
    calls = mock.Mock()
    client.mark_ready.side_effect = lambda *args: calls("mark_ready")
    client.remove_label.side_effect = lambda pr, label: calls("remove_label", label)
    client.set_status.side_effect = GitHubError("gh api failed: HTTP 500") if fails else None

    if fails:
        with pytest.raises(GitHubError):
            apply_plan(client, _pr(), plan)
        client.remove_label.assert_not_called()
    else:
        apply_plan(client, _pr(), plan)
        assert calls.call_args_list[-1] == mock.call("remove_label", "fork ci required")


def test_slack_payload_stays_within_slack_limits():
    attention = [(n, "x" * 2000) for n in range(500)]

    payload = build_slack_payload(
        unknown=[], attention=attention, repo=REPO, run_url="https://run", channel="c"
    )

    texts = [block["text"]["text"] for block in payload["blocks"]]
    assert len(texts) == 50
    assert all(len(text) <= 3000 for text in texts)
    assert "…" in texts[1]
    assert texts[-1].startswith("_")
    assert "more sections are in the <https://run|run log>" in texts[-1]


def test_apply_plan_posts_ci_passed_under_its_own_context(client):
    plan, _, _ = _classify(_pr(), GREEN)

    apply_plan(client, _pr(), plan)

    assert mock.call(42, "abc123", *CI_PASSED, context="CI passed") in client.set_status.call_args_list


@pytest.mark.parametrize(
    ("side_effect", "expected"),
    [
        pytest.param([{"behind_by": 17}], 17, id="counted"),
        pytest.param(GitHubError("gh api failed: HTTP 500"), None, id="unreadable"),
    ],
)
def test_count_commits_behind(client, side_effect, expected):
    client.rest_get.side_effect = side_effect

    assert count_commits_behind(client, REPO, _pr(base_ref="v3-1-test")) == expected
    assert client.rest_get.call_args.args[0] == f"repos/{REPO}/compare/v3-1-test...someone:abc123?per_page=1"
