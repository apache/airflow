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

import subprocess
from datetime import UTC, datetime
from unittest import mock

import pytest
import requests
from click.testing import CliRunner

from airflow_breeze.commands.ci_commands import approve_workflow_runs
from airflow_breeze.utils.ci_minutes_estimate import CiMinutesEstimate, EstimatedJobGroup
from airflow_breeze.utils.fork_ci_gate import GateConfig
from airflow_breeze.utils.fork_ci_messages import CI_CHANGE_MARKER
from airflow_breeze.utils.selective_checks import SelectiveChecks
from airflow_breeze.utils.shared_options import set_dry_run
from airflow_breeze.utils.workflow_run_approval import (
    MAX_AUTO_APPROVED_JOB_MINUTES,
    MAX_AUTO_APPROVED_SOURCE_LINES,
    MAX_ERROR_TEXT_LENGTH,
    NEED_WORKFLOW_APPROVAL_LABEL,
    TEST_SELECTION_LABELS,
    ApprovalDecision,
    ChangedFile,
    GitHubApi,
    PullRequestResult,
    apply_decision,
    build_failure_reason,
    build_selective_checks,
    count_source_lines,
    decide_approval,
    decide_for_pull_request,
    escape_markdown,
    fetch_merge_commit,
    get_changed_files,
    process_waiting_runs,
    write_step_summary,
)

MODULE = "airflow_breeze.utils.workflow_run_approval"
CREATED_SINCE = datetime(2026, 10, 22, 8, 0, tzinfo=UTC)
CORE_CHANGE = [ChangedFile("airflow-core/src/airflow/models/dag.py", 10)]
PROVIDER_CHANGE = [ChangedFile(f"providers/http/src/airflow/providers/http/f_{i}.py", 10) for i in range(8)]
LARGE_SOURCE_CHANGE = [
    ChangedFile("airflow-core/src/airflow/models/dag.py", MAX_AUTO_APPROVED_SOURCE_LINES + 1)
]
LARGE_TEST_CHANGE = [
    ChangedFile("airflow-core/src/airflow/models/dag.py", 10),
    ChangedFile("airflow-core/tests/unit/models/test_dag.py", 500),
]


def _selective_checks(
    *, full_tests_needed=False, upgrade_to_newer_dependencies=False, all_versions=False, canary=False
) -> mock.MagicMock:
    selective_checks = mock.MagicMock(spec=SelectiveChecks)
    selective_checks.full_tests_needed = full_tests_needed
    selective_checks.upgrade_to_newer_dependencies = upgrade_to_newer_dependencies
    selective_checks.all_versions = all_versions
    selective_checks._is_canary_run.return_value = canary
    return selective_checks


def _estimate(job_minutes: int) -> CiMinutesEstimate:
    return CiMinutesEstimate([EstimatedJobGroup("Tests", job_minutes, 20)])


@pytest.fixture
def mock_estimate():
    with mock.patch(f"{MODULE}.estimate_ci_minutes", autospec=True) as estimate_ci_minutes:
        estimate_ci_minutes.return_value = _estimate(100)
        yield estimate_ci_minutes


@pytest.mark.parametrize(
    ("changed_files", "selective_checks", "estimated_minutes", "approve", "reason"),
    [
        pytest.param(PROVIDER_CHANGE, _selective_checks(), 100, True, "at most", id="few-job-minutes"),
        pytest.param(
            CORE_CHANGE,
            _selective_checks(),
            MAX_AUTO_APPROVED_JOB_MINUTES,
            True,
            "at most",
            id="at-the-threshold",
        ),
        pytest.param(
            CORE_CHANGE,
            _selective_checks(),
            MAX_AUTO_APPROVED_JOB_MINUTES + 1,
            False,
            "more than",
            id="too-many-job-minutes",
        ),
        pytest.param(
            LARGE_SOURCE_CHANGE,
            _selective_checks(),
            100,
            False,
            "lines of source code",
            id="too-many-source-lines",
        ),
        pytest.param(LARGE_TEST_CHANGE, _selective_checks(), 100, True, "at most", id="large-test-change"),
        pytest.param(
            [ChangedFile(".github/workflows/ci-amd.yml", 1)],
            _selective_checks(),
            100,
            False,
            "CI definition files",
            id="workflow-change",
        ),
        pytest.param(
            [ChangedFile("dev/breeze/src/airflow_breeze/utils/selective_checks.py", 1)],
            _selective_checks(),
            100,
            False,
            "CI definition files",
            id="selective-checks-change",
        ),
        *(
            pytest.param(
                [ChangedFile(path, 1)],
                _selective_checks(),
                100,
                False,
                f"CI definition files: {path}",
                id=f"fork-ci-defining-{path}",
            )
            for path in (
                ".pre-commit-config.yaml",
                "airflow-core/.pre-commit-config.yaml",
                "Dockerfile.ci",
                "scripts/docker/entrypoint_ci.sh",
                "scripts/in_container/run_ci_tests.sh",
            )
        ),
        pytest.param(
            CORE_CHANGE,
            _selective_checks(full_tests_needed=True),
            100,
            False,
            "full set of tests",
            id="full-tests",
        ),
        pytest.param(
            [ChangedFile("uv.lock", 4)],
            _selective_checks(),
            100,
            False,
            "dependency files: uv.lock",
            id="uv-lock-change",
        ),
        pytest.param(
            [ChangedFile("airflow-core/src/airflow/ui/package.json", 2)],
            _selective_checks(),
            100,
            False,
            "dependency files",
            id="package-json-change",
        ),
        pytest.param(
            [ChangedFile("java-sdk/gradle/wrapper/gradle-wrapper.properties", 2)],
            _selective_checks(),
            100,
            False,
            "dependency files",
            id="gradle-wrapper-change",
        ),
        pytest.param(
            [ChangedFile("go-sdk/go.mod", 2)],
            _selective_checks(),
            100,
            False,
            "dependency files",
            id="go-mod-change",
        ),
        pytest.param(
            [ChangedFile("providers/brand_new/provider.yaml", 20)],
            _selective_checks(),
            100,
            False,
            "adds a new provider",
            id="new-provider",
        ),
        pytest.param(
            [ChangedFile("providers/http/provider.yaml", 2)],
            _selective_checks(),
            100,
            True,
            "at most",
            id="existing-provider-yaml",
        ),
        pytest.param(
            CORE_CHANGE,
            _selective_checks(canary=True),
            100,
            False,
            "canary build",
            id="canary",
        ),
        pytest.param(
            CORE_CHANGE,
            _selective_checks(all_versions=True),
            100,
            False,
            "all Python and database versions",
            id="all-versions",
        ),
        pytest.param(
            CORE_CHANGE,
            _selective_checks(upgrade_to_newer_dependencies=True),
            100,
            False,
            "dependencies",
            id="dependencies",
        ),
    ],
)
def test_decide_approval(mock_estimate, changed_files, selective_checks, estimated_minutes, approve, reason):
    mock_estimate.return_value = _estimate(estimated_minutes)

    decision = decide_approval(
        base_branch="main",
        changed_files=changed_files,
        selective_checks=selective_checks,
    )

    assert decision.approve is approve
    assert reason in "; ".join(decision.reasons)
    assert decision.estimate == mock_estimate.return_value


@pytest.mark.parametrize("error", [SystemExit(1), ValueError("Invalid pyproject.toml")])
def test_decide_approval_when_selective_checks_fail(mock_estimate, error):
    mock_estimate.side_effect = error

    decision = decide_approval(
        base_branch="main", changed_files=CORE_CHANGE, selective_checks=_selective_checks()
    )

    assert decision == ApprovalDecision(approve=False, reasons=["selective checks fail for this change"])


def _completed(stdout: str = "", returncode: int = 0) -> subprocess.CompletedProcess:
    return subprocess.CompletedProcess(args=[], returncode=returncode, stdout=stdout, stderr="")


@pytest.mark.parametrize(
    ("fetch_returncode", "parents", "expected"),
    [
        pytest.param(0, "merge base head\n", "merge", id="built-from-head"),
        pytest.param(0, "merge base other\n", None, id="built-from-other-commit"),
        pytest.param(0, "merge head\n", None, id="not-a-merge-commit"),
        pytest.param(128, "", None, id="no-merge-ref"),
    ],
)
@mock.patch(f"{MODULE}.is_shallow_repository", autospec=True, return_value=True)
@mock.patch(f"{MODULE}.run_command", autospec=True)
def test_fetch_merge_commit(mock_run_command, mock_is_shallow, fetch_returncode, parents, expected):
    mock_run_command.side_effect = [_completed(returncode=fetch_returncode), _completed(parents)]

    assert fetch_merge_commit(github_repository="apache/airflow", pr_number=1, head_sha="head") == expected


@pytest.mark.parametrize(("shallow", "expect_depth"), [("true", True), ("false", False)])
@mock.patch(f"{MODULE}.run_command", autospec=True)
def test_fetch_merge_commit_limits_depth_only_in_shallow_checkouts(mock_run_command, shallow, expect_depth):
    mock_run_command.side_effect = [_completed(f"{shallow}\n"), _completed(), _completed("merge base head\n")]

    fetch_merge_commit(github_repository="apache/airflow", pr_number=1, head_sha="head")

    fetch_command = mock_run_command.call_args_list[1].args[0]
    assert ("--depth=2" in fetch_command) is expect_depth


@mock.patch(f"{MODULE}.run_command", autospec=True)
def test_get_changed_files_reads_literal_paths(mock_run_command):
    mock_run_command.return_value = _completed(
        '3\t2\tREADME.md\x00-\t-\tdata.bin\x001\t0\t.github/workflows/check-\u00e9 "x".yml\x00'
    )

    changed_files = get_changed_files("merge")

    assert changed_files == [
        ChangedFile("README.md", 5),
        ChangedFile("data.bin", MAX_AUTO_APPROVED_SOURCE_LINES + 1),
        ChangedFile('.github/workflows/check-\u00e9 "x".yml', 1),
    ]
    command = mock_run_command.call_args.args[0]
    assert command[:3] == ["git", "-c", "core.quotePath=false"]
    assert {"--no-renames", "--numstat", "-z"} <= set(command)


@mock.patch(f"{MODULE}.run_command", autospec=True)
def test_get_changed_files_rejects_unexpected_output(mock_run_command):
    mock_run_command.return_value = _completed("README.md => docs/README.md\x00")

    with pytest.raises(ValueError, match="Unexpected git numstat record"):
        get_changed_files("merge")


@mock.patch(f"{MODULE}.get_changed_files", autospec=True, side_effect=ValueError("bad"))
@mock.patch(f"{MODULE}.fetch_merge_commit", autospec=True, return_value="merge")
def test_decide_for_pull_request_fails_closed_when_files_cannot_be_listed(mock_fetch, mock_get_changed_files):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"

    decision = decide_for_pull_request(api=api, pr=_pr(10, "aaa"))

    assert decision == ApprovalDecision(approve=False, reasons=["its changed files could not be listed"])


@pytest.mark.parametrize(
    ("path", "is_source"),
    [
        ("airflow-core/src/airflow/models/dag.py", True),
        ("task-sdk/src/airflow/sdk/definitions/dag.py", True),
        ("chart/values.yaml", True),
        ("providers/amazon/tests/unit/aws/hooks/test_s3.py", False),
        ("airflow-e2e-tests/tests/airflow_e2e_tests/test_basic.py", False),
        ("java-sdk/e2e/src/main/java/example/Dags.java", False),
        ("airflow-core/src/airflow/utils/foo_e2e_helper.py", True),
        ("go-sdk/pkg/worker/worker_test.go", False),
        ("airflow-core/src/airflow/ui/src/pages/Dag.test.tsx", False),
        ("airflow-core/src/airflow/ui/public/i18n/locales/de/common.json", False),
        ("airflow-core/newsfragments/12345.significant.rst", False),
        ("airflow-core/docs/howto/index.rst", False),
        ("uv.lock", False),
    ],
)
def test_count_source_lines(path, is_source):
    assert count_source_lines([ChangedFile(path, 7)]) == (7 if is_source else 0)


def _pr(number: int, head_sha: str, labels=(), head_repo="contributor/airflow", head_ref="fix") -> dict:
    return {
        "number": number,
        "head": {"sha": head_sha, "ref": head_ref, "repo": {"full_name": head_repo}},
        "created_at": "2026-10-22T09:00:00Z",
        "labels": [{"name": label} for label in labels],
    }


def _run(
    run_id: int,
    head_sha: str,
    head_repo="contributor/airflow",
    head_branch="fix",
    created_at="2026-10-22T10:00:00Z",
    head_repo_id=1,
) -> dict:
    return {
        "id": run_id,
        "head_sha": head_sha,
        "head_branch": head_branch,
        "created_at": created_at,
        "head_repository": {"full_name": head_repo, "id": head_repo_id},
    }


@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_groups_runs_by_pr_commit(mock_decide):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_last_base_change.return_value = None
    api.get_last_label_change.return_value = None
    api.get_runs_waiting_for_approval.return_value = [
        _run(1, "aaa"),
        _run(2, "aaa"),
        _run(3, "bbb"),
        _run(4, "ccc", head_repo="apache/airflow"),
        _run(5, "ddd"),
    ]
    api.get_open_pull_requests.return_value = [
        _pr(10, "aaa"),
        _pr(11, "bbb", labels=[NEED_WORKFLOW_APPROVAL_LABEL]),
    ]
    mock_decide.side_effect = [
        ApprovalDecision(approve=True, reasons=["localized"]),
        ApprovalDecision(approve=True, reasons=["cheap"]),
    ]

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    api.get_runs_waiting_for_approval.assert_called_once_with(CREATED_SINCE)

    assert [(r.pr_number, r.run_ids) for r in results] == [(10, [1, 2]), (11, [3])]
    assert api.approve_run.call_args_list == [mock.call(1), mock.call(2), mock.call(3)]
    api.remove_label.assert_called_once_with(11, NEED_WORKFLOW_APPROVAL_LABEL)


@pytest.mark.parametrize(
    ("labels", "expected_add_label_calls"),
    [((), [mock.call(10, NEED_WORKFLOW_APPROVAL_LABEL)]), ((NEED_WORKFLOW_APPROVAL_LABEL,), [])],
)
def test_apply_decision_labels_prs_that_need_a_maintainer(labels, expected_add_label_calls):
    api = mock.MagicMock(spec=GitHubApi)

    apply_decision(
        api=api,
        pr=_pr(10, "aaa", labels=labels),
        run_ids=[1],
        decision=ApprovalDecision(approve=False, reasons=["it needs the full set of tests"]),
    )

    api.approve_run.assert_not_called()
    assert api.add_label.call_args_list == expected_add_label_calls


@pytest.mark.parametrize("method", ["approve_run", "add_label", "remove_label", "create_comment"])
def test_report_only_api_does_not_change_anything(method):
    api = GitHubApi(github_repository="apache/airflow", github_token="token", report_only=True)
    with mock.patch.object(api, "_session", autospec=True) as session:
        args = (1,) if method == "approve_run" else (1, NEED_WORKFLOW_APPROVAL_LABEL)
        getattr(api, method)(*args)

    assert session.method_calls == []


def test_write_step_summary_lists_expected_jobs_and_time(tmp_path, monkeypatch):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))
    estimate = CiMinutesEstimate(
        [
            EstimatedJobGroup("CI image, basic tests and static checks", 44, 17),
            EstimatedJobGroup("Provider tests (1 providers, 74 test files)", 110, 16),
        ]
    )

    write_step_summary(
        [
            PullRequestResult(
                pr_number=10,
                head_sha="abcdef0123456789",
                run_ids=[1],
                decision=ApprovalDecision(approve=True, reasons=["localized"], estimate=estimate),
            )
        ],
        "apache/airflow",
    )

    text = summary.read_text()
    assert "| [#10](https://github.com/apache/airflow/pull/10) | `abcdef0123` | approved |" in text
    assert "~154 job-min, ~35 min wall clock" in text
    assert "| Provider tests (1 providers, 74 test files) | 110 | 16 |" in text


def test_write_step_summary_shows_failed_pull_requests_as_failed(tmp_path, monkeypatch):
    summary = tmp_path / "summary.md"
    monkeypatch.setenv("GITHUB_STEP_SUMMARY", str(summary))

    write_step_summary(
        [
            PullRequestResult(
                pr_number=10,
                head_sha="abcdef0123456789",
                run_ids=[1],
                decision=ApprovalDecision(approve=False, reasons=["processing failed"]),
                failed=True,
            )
        ],
        "apache/airflow",
    )

    text = summary.read_text()
    assert "| `abcdef0123` | failed |" in text
    assert NEED_WORKFLOW_APPROVAL_LABEL not in text


@pytest.mark.parametrize(
    ("paths", "approve"),
    [
        pytest.param(
            ["providers/discord/src/airflow/providers/discord/hooks/discord_webhook.py"],
            True,
            id="small-provider-change",
        ),
        pytest.param(
            ["providers/amazon/src/airflow/providers/amazon/aws/hooks/s3.py"],
            False,
            id="large-provider-change",
        ),
        pytest.param(["airflow-core/src/airflow/models/dagrun.py"], True, id="core-change"),
        pytest.param(["task-sdk/src/airflow/sdk/definitions/dag.py"], False, id="task-sdk-change"),
    ],
)
def test_decide_approval_with_real_selective_checks(paths, approve):
    changed_files = [ChangedFile(path, 10) for path in paths]
    selective_checks = SelectiveChecks(
        files=tuple(paths), default_branch="main", commit_ref="HEAD", pr_labels=()
    )

    decision = decide_approval(
        base_branch="main", changed_files=changed_files, selective_checks=selective_checks
    )

    assert decision.approve is approve, decision.reasons
    assert decision.estimate.job_minutes > 0


@pytest.mark.parametrize(("base_branch", "runs_provider_tests"), [("main", True), ("v3-4-test", False)])
def test_build_selective_checks_uses_the_pr_base_branch(base_branch, runs_provider_tests):
    selective_checks = build_selective_checks(
        merge_sha="HEAD",
        changed_files=[ChangedFile("providers/http/src/airflow/providers/http/hooks/http.py", 10)],
        pr_labels=(),
        base_branch=base_branch,
    )

    assert selective_checks.skip_providers_tests is not runs_provider_tests


@pytest.mark.parametrize("base_branch", ["v3-4-test", "airflow-ctl/v0-1-test"])
def test_decide_approval_never_approves_prs_to_other_branches(mock_estimate, base_branch):
    decision = decide_approval(
        base_branch=base_branch, changed_files=CORE_CHANGE, selective_checks=_selective_checks()
    )

    assert decision.approve is False
    assert decision.reasons == [f"it targets {base_branch}, not main"]
    assert decision.estimate == mock_estimate.return_value


def test_get_runs_waiting_for_approval_only_lists_runs_created_since():
    api = GitHubApi(github_repository="apache/airflow", github_token="token", report_only=True)
    with mock.patch.object(api, "_get_paginated", autospec=True, return_value=[]) as get_paginated:
        api.get_runs_waiting_for_approval(CREATED_SINCE)

    assert "&created=%3E%3D2026-10-22T08:00:00Z" in get_paginated.call_args.args[0]


def test_process_waiting_runs_without_runs():
    api = mock.MagicMock(spec=GitHubApi)
    api.get_runs_waiting_for_approval.return_value = []

    assert process_waiting_runs(api, created_since=CREATED_SINCE) == []
    api.get_open_pull_requests.assert_not_called()


@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_does_not_approve_a_commit_shared_by_several_prs(mock_decide):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_runs_waiting_for_approval.return_value = [_run(1, "aaa"), _run(2, "aaa")]
    api.get_open_pull_requests.return_value = [_pr(10, "aaa"), _pr(11, "aaa")]

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    assert [r.pr_number for r in results] == [10, 11]
    assert all(not r.decision.approve for r in results)
    mock_decide.assert_not_called()
    api.approve_run.assert_not_called()
    assert api.add_label.call_args_list == [
        mock.call(10, NEED_WORKFLOW_APPROVAL_LABEL),
        mock.call(11, NEED_WORKFLOW_APPROVAL_LABEL),
    ]


@pytest.mark.parametrize(
    ("args", "report_only"),
    [([], False), (["--report-only"], True), (["--dry-run"], True)],
)
@mock.patch(f"{MODULE}.write_step_summary", autospec=True)
@mock.patch(f"{MODULE}.process_waiting_runs", autospec=True, return_value=[])
@mock.patch(f"{MODULE}.GitHubApi", autospec=True)
def test_approve_workflow_runs_command_never_writes_in_dry_run(
    mock_api, mock_process, mock_summary, args, report_only
):
    try:
        result = CliRunner().invoke(approve_workflow_runs, ["--github-token", "token", *args])
    finally:
        set_dry_run(False)

    assert result.exit_code == 0, result.output
    assert mock_api.call_args.kwargs["report_only"] is report_only


@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_requires_the_run_branch_to_match_the_pr(mock_decide):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_runs_waiting_for_approval.return_value = [_run(1, "aaa", head_branch="other")]
    api.get_open_pull_requests.return_value = [_pr(10, "aaa", head_ref="fix")]

    assert process_waiting_runs(api, created_since=CREATED_SINCE) == []
    mock_decide.assert_not_called()
    api.approve_run.assert_not_called()


def test_apply_decision_prints_contributor_paths_without_markup():
    api = mock.MagicMock(spec=GitHubApi)

    apply_decision(
        api=api,
        pr=_pr(10, "aaa"),
        run_ids=[1],
        decision=ApprovalDecision(
            approve=False, reasons=["it changes CI definition files: .github/a[/bogus].txt"]
        ),
    )

    api.add_label.assert_called_once_with(10, NEED_WORKFLOW_APPROVAL_LABEL)


@pytest.mark.parametrize(
    ("last_base_change", "approved_runs"),
    [
        pytest.param(None, [2, 3], id="no-retarget"),
        pytest.param(datetime(2026, 10, 22, 10, 30, tzinfo=UTC), [3], id="retargeted-after-first-run"),
        pytest.param(datetime(2026, 10, 22, 12, 0, tzinfo=UTC), [], id="retargeted-after-all-runs"),
    ],
)
@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_only_approves_runs_started_for_the_current_pr(
    mock_decide, last_base_change, approved_runs
):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_runs_waiting_for_approval.return_value = [
        _run(1, "aaa", created_at="2026-10-22T08:00:00Z"),
        _run(2, "aaa", created_at="2026-10-22T10:00:00Z"),
        _run(3, "aaa", created_at="2026-10-22T11:00:00Z"),
    ]
    api.get_open_pull_requests.return_value = [_pr(10, "aaa")]
    api.get_last_base_change.return_value = last_base_change
    api.get_last_label_change.return_value = None
    mock_decide.return_value = ApprovalDecision(approve=True, reasons=["cheap"])

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    assert api.approve_run.call_args_list == [mock.call(run_id) for run_id in approved_runs]
    assert results[0].decision.approve is bool(approved_runs)


def test_get_last_base_change():
    api = GitHubApi(github_repository="apache/airflow", github_token="token", report_only=True)
    events = [
        {"event": "labeled", "created_at": "2026-10-22T12:00:00Z"},
        {"event": "base_ref_changed", "created_at": "2026-10-22T09:00:00Z"},
        {"event": "base_ref_changed", "created_at": "2026-10-22T10:00:00Z"},
    ]
    with mock.patch.object(api, "_get_paginated", autospec=True, return_value=events):
        assert api.get_last_base_change(10) == datetime(2026, 10, 22, 10, 0, tzinfo=UTC)


def test_escape_markdown():
    assert escape_markdown("it changes .github/[x](http://evil)|`y`") == (
        "it changes .github/\\[x\\]\\(http://evil\\)\\|\\`y\\`"
    )


@pytest.mark.parametrize(
    "error",
    [
        pytest.param(requests.HTTPError("502 Server Error"), id="http-error"),
        pytest.param(requests.ConnectionError("reset"), id="connection-error"),
        pytest.param(subprocess.CalledProcessError(128, ["git", "diff"]), id="git-error"),
    ],
)
@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_continues_after_a_pull_request_fails(mock_decide, error):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_last_base_change.return_value = None
    api.get_last_label_change.return_value = None
    api.get_runs_waiting_for_approval.return_value = [_run(1, "aaa"), _run(2, "bbb")]
    api.get_open_pull_requests.return_value = [_pr(10, "aaa"), _pr(11, "bbb")]
    mock_decide.side_effect = [error, ApprovalDecision(approve=True, reasons=["cheap"])]

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    assert [(r.pr_number, r.decision.approve, r.failed) for r in results] == [
        (10, False, True),
        (11, True, False),
    ]
    assert type(error).__name__ in results[0].decision.reasons[0]
    assert api.approve_run.call_args_list == [mock.call(2)]


@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_records_a_failure_while_approving_as_not_approved(mock_decide):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_last_base_change.return_value = None
    api.get_last_label_change.return_value = None
    api.get_runs_waiting_for_approval.return_value = [_run(1, "aaa"), _run(2, "aaa")]
    api.get_open_pull_requests.return_value = [_pr(10, "aaa")]
    api.approve_run.side_effect = [None, requests.HTTPError("409 Conflict")]
    mock_decide.return_value = ApprovalDecision(approve=True, reasons=["cheap"])

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    assert [(r.pr_number, r.run_ids, r.decision.approve, r.failed) for r in results] == [
        (10, [1, 2], False, True)
    ]
    assert api.approve_run.call_args_list == [mock.call(1), mock.call(2)]
    api.add_label.assert_called_once_with(10, NEED_WORKFLOW_APPROVAL_LABEL)


def _api_with_prs(prs, runs):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_last_base_change.return_value = None
    api.get_last_label_change.return_value = None
    api.get_runs_waiting_for_approval.return_value = runs
    api.get_open_pull_requests.return_value = prs
    return api


@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_labels_failed_pull_requests_that_lack_the_label_without_approving(mock_decide):
    api = _api_with_prs(
        [_pr(10, "aaa"), _pr(11, "bbb", labels=[NEED_WORKFLOW_APPROVAL_LABEL])],
        [_run(1, "aaa"), _run(2, "bbb")],
    )
    mock_decide.side_effect = requests.HTTPError("502 Server Error")

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    assert [(r.failed, r.decision.approve) for r in results] == [(True, False), (True, False)]
    assert api.add_label.call_args_list == [mock.call(10, NEED_WORKFLOW_APPROVAL_LABEL)]
    api.approve_run.assert_not_called()


@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_survives_a_failure_to_label_a_failed_pull_request(mock_decide):
    api = _api_with_prs([_pr(10, "aaa"), _pr(11, "bbb")], [_run(1, "aaa"), _run(2, "bbb")])
    api.add_label.side_effect = requests.ConnectionError("reset")
    mock_decide.side_effect = [
        requests.HTTPError("502 Server Error"),
        ApprovalDecision(approve=True, reasons=["cheap"]),
    ]

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    assert [(r.pr_number, r.failed) for r in results] == [(10, True), (11, False)]
    assert api.add_label.call_args_list == [mock.call(10, NEED_WORKFLOW_APPROVAL_LABEL)]
    assert api.approve_run.call_args_list == [mock.call(2)]


@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_keeps_each_failed_pull_requests_own_estimate(mock_decide):
    api = _api_with_prs([_pr(10, "aaa"), _pr(11, "bbb")], [_run(1, "aaa"), _run(2, "bbb")])
    api.approve_run.side_effect = requests.HTTPError("409 Conflict")
    estimate = CiMinutesEstimate(job_groups=[])
    mock_decide.side_effect = [
        ApprovalDecision(approve=True, reasons=["cheap"], estimate=estimate),
        requests.HTTPError("502 Server Error"),
    ]

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    assert [(r.pr_number, r.failed) for r in results] == [(10, True), (11, True)]
    assert results[0].decision.estimate is estimate
    assert results[1].decision.estimate is None


@pytest.mark.parametrize(
    ("error", "expected_reason"),
    [
        pytest.param(
            requests.HTTPError("502 Server\nError"),
            "processing failed with HTTPError: 502 Server Error",
            id="whitespace-collapsed",
        ),
        pytest.param(
            requests.ConnectionError(),
            "processing failed with ConnectionError",
            id="empty-message",
        ),
    ],
)
@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_describes_the_error_in_the_failure_reason(mock_decide, error, expected_reason):
    api = _api_with_prs([_pr(10, "aaa")], [_run(1, "aaa")])
    mock_decide.side_effect = error

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    assert results[0].decision.reasons == [expected_reason]


def test_build_failure_reason_keeps_the_end_of_a_long_error():
    error = subprocess.CalledProcessError(128, ["git", "diff", "x" * 1000])

    reason = build_failure_reason(error)

    assert reason.startswith("processing failed with CalledProcessError: Command '['git', 'diff'")
    assert reason.endswith("returned non-zero exit status 128.")
    assert "..." in reason
    assert len(reason) == len("processing failed with CalledProcessError: ") + MAX_ERROR_TEXT_LENGTH - 1


@pytest.mark.parametrize(("failed", "exit_code"), [(False, 0), (True, 1)])
@mock.patch(f"{MODULE}.write_step_summary", autospec=True)
@mock.patch(f"{MODULE}.process_waiting_runs", autospec=True)
@mock.patch(f"{MODULE}.GitHubApi", autospec=True)
def test_approve_workflow_runs_command_exits_non_zero_after_summary_when_a_pr_failed(
    mock_api, mock_process, mock_summary, failed, exit_code
):
    results = [
        PullRequestResult(
            pr_number=10,
            head_sha="abc",
            run_ids=[1],
            decision=ApprovalDecision(approve=False, reasons=["processing failed"]),
            failed=failed,
        )
    ]
    mock_process.return_value = results

    result = CliRunner().invoke(approve_workflow_runs, ["--github-token", "token"])

    assert result.exit_code == exit_code, result.output
    mock_summary.assert_called_once_with(results, mock.ANY)


GATE_ENABLED = GateConfig(enabled=True, enabled_since=datetime(2026, 1, 1, tzinfo=UTC))


def _gated_pr(*, base_ref="main", author_association="CONTRIBUTOR", labels=()) -> dict:
    return {
        **_pr(10, "aaa", labels=labels),
        "base": {"ref": base_ref},
        "user": {"login": "contributor"},
        "author_association": author_association,
    }


@pytest.mark.parametrize(
    ("changed_files", "approve", "ci_files"),
    [
        pytest.param(LARGE_SOURCE_CHANGE, True, [], id="large change approved without size checks"),
        pytest.param([ChangedFile("airflow-core/pyproject.toml", 3)], True, [], id="dependency change"),
        pytest.param(
            [ChangedFile(".github/workflows/ci-amd.yml", 1), *CORE_CHANGE],
            False,
            [".github/workflows/ci-amd.yml"],
            id="workflow change",
        ),
        pytest.param(
            [ChangedFile("scripts/docker/entrypoint_ci.sh", 1)],
            False,
            ["scripts/docker/entrypoint_ci.sh"],
            id="file the fork CI reconciler counts as CI",
        ),
    ],
)
@mock.patch(f"{MODULE}.build_selective_checks", autospec=True)
@mock.patch(f"{MODULE}.load_config", autospec=True, return_value=GATE_ENABLED)
@mock.patch(f"{MODULE}.get_changed_files", autospec=True)
@mock.patch(f"{MODULE}.fetch_merge_commit", autospec=True, return_value="merge")
def test_decide_for_pull_request_gated_to_fork_ci(
    mock_fetch,
    mock_get_changed_files,
    mock_load_config,
    mock_selective_checks,
    changed_files,
    approve,
    ci_files,
):
    mock_get_changed_files.return_value = changed_files
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"

    decision = decide_for_pull_request(api=api, pr=_gated_pr())

    assert decision.approve is approve
    assert decision.estimate is None
    mock_selective_checks.assert_not_called()
    if approve:
        assert decision.comment is None
    else:
        assert decision.comment is not None
        assert all(f"`{path}`" in decision.comment for path in ci_files)


@pytest.mark.parametrize(
    ("config", "pr"),
    [
        pytest.param(GateConfig(), _gated_pr(), id="gate disabled"),
        pytest.param(GATE_ENABLED, _gated_pr(author_association="MEMBER"), id="committer"),
        pytest.param(GATE_ENABLED, _gated_pr(labels=["use project ci"]), id="use project ci"),
        pytest.param(GATE_ENABLED, _gated_pr(base_ref="v3-4-test"), id="release branch"),
    ],
)
@mock.patch(f"{MODULE}.decide_approval", autospec=True)
@mock.patch(f"{MODULE}.build_selective_checks", autospec=True)
@mock.patch(f"{MODULE}.load_config", autospec=True)
@mock.patch(f"{MODULE}.get_changed_files", autospec=True, return_value=LARGE_SOURCE_CHANGE)
@mock.patch(f"{MODULE}.fetch_merge_commit", autospec=True, return_value="merge")
def test_decide_for_pull_request_running_project_ci_keeps_the_size_rules(
    mock_fetch,
    mock_get_changed_files,
    mock_load_config,
    mock_selective_checks,
    mock_decide_approval,
    config,
    pr,
):
    mock_load_config.return_value = config
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"

    decision = decide_for_pull_request(api=api, pr=pr)

    assert decision is mock_decide_approval.return_value


@pytest.mark.parametrize(("already_commented", "expect_comment"), [(False, True), (True, False)])
def test_apply_decision_comments_once_on_a_ci_change(already_commented, expect_comment):
    api = mock.MagicMock(spec=GitHubApi)
    api.has_bot_comment.return_value = already_commented
    decision = ApprovalDecision(approve=False, reasons=["it changes CI"], comment="explanation")

    apply_decision(api=api, pr=_pr(10, "aaa"), run_ids=[1], decision=decision)

    api.approve_run.assert_not_called()
    api.has_bot_comment.assert_called_once_with(10, CI_CHANGE_MARKER)
    assert api.create_comment.call_args_list == ([mock.call(10, "explanation")] if expect_comment else [])


def test_apply_decision_without_a_comment_does_not_read_comments():
    api = mock.MagicMock(spec=GitHubApi)

    apply_decision(
        api=api, pr=_pr(10, "aaa"), run_ids=[1], decision=ApprovalDecision(approve=False, reasons=["big"])
    )

    api.has_bot_comment.assert_not_called()
    api.create_comment.assert_not_called()


@pytest.mark.parametrize(("login", "expected"), [("github-actions[bot]", True), ("contributor", False)])
def test_has_bot_comment_only_counts_the_workflow_comments(login, expected):
    api = GitHubApi(github_repository="apache/airflow", github_token="token", report_only=False)
    comments = [{"user": {"login": login}, "body": f"{CI_CHANGE_MARKER}\n\ntext"}]
    with mock.patch.object(api, "_get_paginated", autospec=True, return_value=comments):
        assert api.has_bot_comment(10, CI_CHANGE_MARKER) is expected


@pytest.mark.parametrize(
    ("decision_labels", "approved_runs"),
    [
        pytest.param(frozenset({"full tests needed"}), [3], id="runs started before the change"),
        pytest.param(frozenset(), [2, 3], id="no change of the decision's labels"),
    ],
)
@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_skips_runs_started_before_a_test_selection_label_changed(
    mock_decide, decision_labels, approved_runs
):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_runs_waiting_for_approval.return_value = [
        _run(2, "aaa", created_at="2026-10-22T10:00:00Z"),
        _run(3, "aaa", created_at="2026-10-22T11:00:00Z"),
    ]
    api.get_open_pull_requests.return_value = [_pr(10, "aaa")]
    api.get_last_base_change.return_value = None
    api.get_last_label_change.side_effect = lambda number, labels: (
        datetime(2026, 10, 22, 10, 30, tzinfo=UTC) if labels else None
    )
    mock_decide.return_value = ApprovalDecision(
        approve=True, reasons=["cheap"], decision_labels=decision_labels
    )

    process_waiting_runs(api, created_since=CREATED_SINCE)

    assert api.approve_run.call_args_list == [mock.call(run_id) for run_id in approved_runs]


def test_get_last_label_change_ignores_other_labels_and_reads_the_timeline_once():
    api = GitHubApi(github_repository="apache/airflow", github_token="token", report_only=True)
    events = [
        {"event": "unlabeled", "created_at": "2026-10-22T09:00:00Z", "label": {"name": "full tests needed"}},
        {
            "event": "unlabeled",
            "created_at": "2026-10-22T11:00:00Z",
            "label": {"name": NEED_WORKFLOW_APPROVAL_LABEL},
        },
        {"event": "labeled", "created_at": "2026-10-22T11:30:00Z", "label": {"name": "area:UI"}},
        {
            "event": "unlabeled",
            "created_at": "2026-10-22T11:45:00Z",
            "label": {"name": "use public runners"},
        },
        {"event": "base_ref_changed", "created_at": "2026-10-22T08:00:00Z"},
    ]
    with mock.patch.object(api, "_get_paginated", autospec=True, return_value=events) as get_paginated:
        assert api.get_last_label_change(10, TEST_SELECTION_LABELS) == datetime(
            2026, 10, 22, 9, 0, tzinfo=UTC
        )
        assert api.get_last_base_change(10) == datetime(2026, 10, 22, 8, 0, tzinfo=UTC)
    get_paginated.assert_called_once()


@mock.patch(f"{MODULE}.load_config", autospec=True, return_value=GATE_ENABLED)
@mock.patch(f"{MODULE}.get_changed_files", autospec=True, return_value=CORE_CHANGE)
@mock.patch(f"{MODULE}.fetch_merge_commit", autospec=True, return_value="merge")
def test_gated_approval_depends_on_no_label(mock_fetch, mock_get_changed_files, mock_load_config):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"

    assert decide_for_pull_request(api=api, pr=_gated_pr()).decision_labels == frozenset()


@pytest.mark.parametrize("event", ["labeled", "unlabeled"])
@pytest.mark.parametrize("label", sorted(TEST_SELECTION_LABELS))
def test_get_last_label_change_counts_every_test_selection_label(label, event):
    api = GitHubApi(github_repository="apache/airflow", github_token="token", report_only=True)
    events = [{"event": event, "created_at": "2026-10-22T09:00:00Z", "label": {"name": label}}]
    with mock.patch.object(api, "_get_paginated", autospec=True, return_value=events):
        assert api.get_last_label_change(10, TEST_SELECTION_LABELS) == datetime(
            2026, 10, 22, 9, 0, tzinfo=UTC
        )


@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_approves_gated_runs_started_before_use_fork_ci_was_added(mock_decide):
    api = GitHubApi(github_repository="apache/airflow", github_token="token", report_only=False)
    timeline = [
        {"event": "labeled", "created_at": "2026-10-22T10:30:00Z", "label": {"name": "use fork ci"}},
        {"event": "unlabeled", "created_at": "2026-10-22T10:40:00Z", "label": {"name": "full tests needed"}},
    ]
    mock_decide.return_value = ApprovalDecision(
        approve=True, reasons=["fork CI"], decision_labels=frozenset()
    )
    with (
        mock.patch.object(
            api,
            "get_runs_waiting_for_approval",
            autospec=True,
            return_value=[
                _run(2, "aaa", created_at="2026-10-22T10:00:00Z"),
                _run(3, "aaa", created_at="2026-10-22T11:00:00Z"),
            ],
        ),
        mock.patch.object(api, "get_open_pull_requests", autospec=True, return_value=[_pr(10, "aaa")]),
        mock.patch.object(api, "_get_paginated", autospec=True, return_value=timeline),
        mock.patch.object(api, "approve_run", autospec=True) as approve_run,
    ):
        process_waiting_runs(api, created_since=CREATED_SINCE)

    assert approve_run.call_args_list == [mock.call(2), mock.call(3)]


@pytest.mark.parametrize(
    ("head_repo", "head_repo_id", "stakeholder"),
    [
        ("astronomer/airflow", 65323404, "Astronomer"),
        ("aws-mwaa/upstream-to-airflow", 484591340, "Amazon"),
        ("VladaZakharova/airflow", 733432432, "Google"),
    ],
)
@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_always_approves_trusted_stakeholder_repositories(
    mock_decide, head_repo, head_repo_id, stakeholder
):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_runs_waiting_for_approval.return_value = [
        _run(1, "aaa", head_repo=head_repo, head_repo_id=head_repo_id, created_at="2026-10-22T08:30:00Z"),
        _run(2, "aaa", head_repo=head_repo, head_repo_id=head_repo_id),
    ]
    # Several PRs share the head and a run started before the PRs were opened.
    api.get_open_pull_requests.return_value = [
        _pr(10, "aaa", head_repo=head_repo, labels=[NEED_WORKFLOW_APPROVAL_LABEL]),
        _pr(11, "aaa", head_repo=head_repo, labels=[NEED_WORKFLOW_APPROVAL_LABEL]),
    ]

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    mock_decide.assert_not_called()
    assert [(r.pr_number, r.run_ids, r.decision.approve) for r in results] == [
        (10, [1, 2], True),
        (11, [], True),
    ]
    assert stakeholder in results[0].decision.reasons[0]
    assert api.approve_run.call_args_list == [mock.call(1), mock.call(2)]
    api.add_label.assert_not_called()
    assert api.remove_label.call_args_list == [
        mock.call(10, NEED_WORKFLOW_APPROVAL_LABEL),
        mock.call(11, NEED_WORKFLOW_APPROVAL_LABEL),
    ]


@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_does_not_trust_a_repository_by_its_name(mock_decide):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_last_base_change.return_value = None
    api.get_last_label_change.return_value = None
    api.get_runs_waiting_for_approval.return_value = [_run(1, "aaa", head_repo="astronomer/airflow")]
    api.get_open_pull_requests.return_value = [_pr(10, "aaa", head_repo="astronomer/airflow")]
    mock_decide.return_value = ApprovalDecision(approve=False, reasons=["too big"])

    results = process_waiting_runs(api, created_since=CREATED_SINCE)

    mock_decide.assert_called_once()
    assert not results[0].decision.approve
    api.approve_run.assert_not_called()
