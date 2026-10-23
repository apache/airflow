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

from airflow_breeze.utils.ci_minutes_estimate import CiMinutesEstimate, EstimatedJobGroup
from airflow_breeze.utils.selective_checks import SelectiveChecks
from airflow_breeze.utils.workflow_run_approval import (
    MAX_AUTO_APPROVED_JOB_MINUTES,
    MAX_AUTO_APPROVED_SOURCE_LINES,
    NEED_WORKFLOW_APPROVAL_LABEL,
    ApprovalDecision,
    ChangedFile,
    GitHubApi,
    PullRequestResult,
    apply_decision,
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
) -> dict:
    return {
        "id": run_id,
        "head_sha": head_sha,
        "head_branch": head_branch,
        "created_at": created_at,
        "head_repository": {"full_name": head_repo},
    }


@mock.patch(f"{MODULE}.decide_for_pull_request", autospec=True)
def test_process_waiting_runs_groups_runs_by_pr_commit(mock_decide):
    api = mock.MagicMock(spec=GitHubApi)
    api.github_repository = "apache/airflow"
    api.get_last_base_change.return_value = None
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


@pytest.mark.parametrize("method", ["approve_run", "add_label", "remove_label"])
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
    selective_checks = SelectiveChecks(files=tuple(paths), commit_ref="HEAD", pr_labels=())

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
    from click.testing import CliRunner

    from airflow_breeze.commands.ci_commands import approve_workflow_runs
    from airflow_breeze.utils.shared_options import set_dry_run

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
