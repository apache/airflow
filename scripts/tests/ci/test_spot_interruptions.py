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

import importlib.util
import json
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from unittest import mock

import pytest

MODULE_PATH = Path(__file__).resolve().parents[3] / "scripts" / "ci" / "spot_interruptions.py"
RUN_URL = "https://github.com/apache/airflow/actions/runs/7"


@pytest.fixture
def spot():
    module_name = "test_spot_interruptions_module"
    sys.modules.pop(module_name, None)
    spec = importlib.util.spec_from_file_location(module_name, MODULE_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _job(spot, name, *, interrupted):
    """A failed job; `interrupted` is True for a spot interruption, or a conclusion other than failure."""
    if isinstance(interrupted, str):
        return spot.FailedJob(hash(name), name, f"https://job/{name}", spot=False, conclusion=interrupted)
    return spot.FailedJob(hash(name), name, f"https://job/{name}", spot=interrupted)


def test_get_failed_jobs_marks_spot_interruptions_by_annotation(spot):
    responses = {
        "jobs": "\n".join(
            json.dumps(job)
            for job in [
                {"id": 1, "name": "Tests A", "html_url": "https://job/1", "conclusion": "failure"},
                {"id": 2, "name": "Tests B", "html_url": "https://job/2", "conclusion": "failure"},
                {"id": 3, "name": "Tests C", "html_url": "https://job/3", "conclusion": "cancelled"},
                {"id": 4, "name": "Tests D", "html_url": "https://job/4", "conclusion": "success"},
                # A pull request's own workflow could add the annotation on a GitHub-hosted runner.
                {"id": 5, "name": "Tests E", "html_url": "https://job/5", "conclusion": "failure"},
            ]
        ),
        # Titles of all pages of annotations, one per line.
        "repos/apache/airflow/check-runs/1/annotations?per_page=100": "\nProcess completed\nEC2 Spot interruption",
        "repos/apache/airflow/check-runs/2/annotations?per_page=100": "Process completed",
    }

    runners = {1: "runs-on--i-0123--abc", 2: "runs-on--i-0456--def", 5: "GitHub Actions 1000"}
    responses["jobs"] = "\n".join(
        json.dumps({**json.loads(line), "runner_name": runners.get(json.loads(line)["id"])})
        for line in responses["jobs"].splitlines()
    )

    def gh_api(*args):
        return responses["jobs"] if "/jobs?" in args[1] else responses[args[1]]

    with mock.patch.object(spot, "gh_api", side_effect=gh_api):
        jobs = spot.get_failed_jobs("apache/airflow", "7")

    # A cancelled job is a failure, but was not interrupted, so its annotations are not read.
    assert [(job.name, job.spot, job.conclusion) for job in jobs] == [
        ("Tests A", True, "failure"),
        ("Tests B", False, "failure"),
        ("Tests C", False, "cancelled"),
        ("Tests E", False, "failure"),
    ]


SPOT_ONLY = [("Tests A", True), ("CI passed", False)]
BOT = "runsonairflow[bot]"


@pytest.mark.parametrize(
    ("failed", "attempt", "actor", "expected_skip"),
    [
        pytest.param(SPOT_ONLY, 1, "github-actions", True, id="spot-only-retried"),
        pytest.param(SPOT_ONLY, 2, BOT, True, id="spot-only-retried-again"),
        pytest.param(SPOT_ONLY, 3, BOT, False, id="runs-on-gave-up"),
        pytest.param(
            [("Tests A", True), ("Tests B", False), ("CI passed", False)],
            1,
            "github-actions",
            False,
            id="real-failure-too",
        ),
        pytest.param([("CI passed", False)], 1, "github-actions", False, id="ci-passed-alone-is-real"),
        pytest.param(
            [("Tests A", True), ("Tests C", "cancelled"), ("CI passed", False)],
            1,
            "github-actions",
            False,
            id="spot-and-timed-out-job",
        ),
        pytest.param([], 1, "github-actions", False, id="no-failures"),
        pytest.param([], 2, BOT, False, id="runs-on-re-run-recovered"),
        pytest.param([("Tests B", False)], 2, "a-maintainer", True, id="maintainer-re-run"),
    ],
)
def test_should_skip_slack(spot, failed, attempt, actor, expected_skip):
    jobs = [_job(spot, name, interrupted=interrupted) for name, interrupted in failed]

    assert spot.should_skip_slack(jobs, attempt, actor) is expected_skip


def test_format_failed_jobs_marks_spot_and_cancelled_jobs(spot):
    jobs = [
        _job(spot, "Tests B", interrupted=False),
        _job(spot, "Tests A", interrupted=True),
        _job(spot, "Tests C", interrupted="timed_out"),
    ]

    assert spot.format_failed_jobs(jobs) == "Tests A (spot instance reclaimed)\nTests B\nTests C (timed out)"


@pytest.mark.parametrize(
    ("failed", "attempt", "conclusion", "expected"),
    [
        pytest.param(
            [("Tests A", True)], 1, "failure", ["attempt 1", "Tests A", "re-runs them"], id="retried"
        ),
        pytest.param([("Tests A", True)], 3, "failure", ["attempt 3", "does not re-run"], id="gave-up"),
        pytest.param([], 2, "success", ["attempt 2", "passed"], id="re-run-passed"),
        pytest.param([("Tests B", False)], 2, "failure", ["attempt 2", "failed"], id="re-run-failed"),
        pytest.param([("Tests B", False)], 1, "failure", None, id="no-spot-first-attempt"),
    ],
)
def test_build_comment(spot, failed, attempt, conclusion, expected):
    jobs = [_job(spot, name, interrupted=interrupted) for name, interrupted in failed]

    body = spot.build_comment(jobs, run_id=7, attempt=attempt, conclusion=conclusion, run_url=RUN_URL)

    if expected is None:
        assert body is None
    else:
        assert body.startswith("<!-- spot-interruption-notice run=7 -->\n")
        assert all(text in body for text in expected)
        assert f"{RUN_URL}/attempts/{attempt}" in body


def _run(
    status="in_progress",
    attempt=1,
    conclusion=None,
    branch="my-change",
    created="2026-10-25T10:00:00Z",
    updated="2026-10-25T15:30:00Z",
):
    return {
        "updated_at": updated,
        "head_sha": "abc123",
        "pr_numbers": [],
        "id": 7,
        "run_attempt": attempt,
        "status": status,
        "conclusion": conclusion,
        "html_url": RUN_URL,
        "head_branch": branch,
        "head_owner": "someone",
        "created_at": created,
    }


@pytest.mark.parametrize(
    ("run", "failed", "existing_body", "expected_write"),
    [
        pytest.param(_run(), [("Tests A", True)], None, "POST", id="interrupted-while-running"),
        pytest.param(_run(), [("Tests A", True)], "same", None, id="notice-unchanged"),
        pytest.param(_run(), [("Tests B", False)], None, None, id="running-without-interruptions"),
        pytest.param(
            _run("completed", 1, "failure"), [("Tests B", False)], None, None, id="first-attempt-real-failure"
        ),
        pytest.param(_run("in_progress", 2), [], "old", None, id="re-run-in-progress"),
        pytest.param(_run("completed", 2, "success"), [], "old", "PATCH", id="re-run-finished"),
        pytest.param(_run("completed", 2, "success"), [], None, None, id="re-run-without-notice"),
        pytest.param(
            _run("completed", 2, "failure"),
            [("Tests B", False)],
            "other-run",
            None,
            id="notice-of-another-run",
        ),
    ],
)
def test_update_notice(spot, run, failed, existing_body, expected_write):
    jobs = [_job(spot, name, interrupted=interrupted) for name, interrupted in failed]
    expected_body = spot.build_comment(
        jobs, run_id=7, attempt=run["run_attempt"], conclusion=run["conclusion"] or "", run_url=RUN_URL
    )
    bodies = {
        "same": expected_body,
        "old": "<!-- spot-interruption-notice run=7 -->\nold",
        "other-run": "<!-- spot-interruption-notice run=6 -->\nold",
    }
    existing = spot.Comment(99, bodies[existing_body]) if existing_body else None

    with (
        mock.patch.object(spot, "get_failed_jobs", autospec=True, return_value=jobs),
        mock.patch.object(spot, "find_pull_requests", autospec=True, return_value=[42]),
        mock.patch.object(spot, "find_comment", autospec=True, return_value=existing),
        mock.patch.object(spot, "gh_api", autospec=True) as mock_gh_api,
    ):
        spot.update_notice("apache/airflow", run)

    if expected_write is None:
        mock_gh_api.assert_not_called()
    else:
        path = (
            "repos/apache/airflow/issues/42/comments"
            if expected_write == "POST"
            else "repos/apache/airflow/issues/comments/99"
        )
        mock_gh_api.assert_called_once_with("-X", expected_write, path, "-f", f"body={expected_body}")


def test_list_pull_request_runs_keeps_the_newest_recent_run_per_pull_request(spot):
    in_progress = [
        {**_run(created="2026-10-25T12:00:00Z"), "id": 1},
        {**_run(branch="other"), "id": 2},
    ]
    completed = [
        {**_run("completed", created="2026-10-25T11:00:00Z"), "id": 3},
        # Created days ago, but RunsOn's re-run just finished: its outcome is still to report.
        {
            **_run("completed", branch="old", created="2026-10-23T10:00:00Z", updated="2026-10-25T15:00:00Z"),
            "id": 4,
        },
        {**_run("completed", branch="done", updated="2026-10-25T13:00:00Z"), "id": 5},
    ]

    def gh_api(*args):
        if "status=in_progress" in args[1]:
            runs = in_progress
        else:
            runs = completed if "created=2026-10-24T16" in args[1] else []
        return "\n".join(json.dumps(run) for run in runs)

    with mock.patch.object(spot, "gh_api", side_effect=gh_api) as mock_gh_api:
        runs = spot.list_pull_request_runs("apache/airflow", datetime(2026, 10, 25, 16, tzinfo=UTC))

    assert sorted((run["head_branch"], run["created_at"]) for run in runs) == [
        ("my-change", "2026-10-25T12:00:00Z"),
        ("old", "2026-10-23T10:00:00Z"),
        ("other", "2026-10-25T10:00:00Z"),
    ]
    # A listing per day of the 7, so that none reaches GitHub's limit of 1000 runs.
    created = [call.args[1].split("created=")[1] for call in mock_gh_api.call_args_list[1:]]
    assert created[0] == "2026-10-24T16%3A00%3A00Z..2026-10-25T16%3A00%3A00Z"
    assert created[-1] == "2026-10-18T16%3A00%3A00Z..2026-10-19T16%3A00%3A00Z"
    assert len(created) == 7


def test_list_runs_warns_when_reaching_the_listing_limit(spot, capsys):
    runs = "\n".join(json.dumps(_run(branch=str(n))) for n in range(1000))

    with mock.patch.object(spot, "gh_api", autospec=True, return_value=runs):
        assert len(spot.list_runs("apache/airflow", "status=completed")) == 1000

    assert "reached GitHub's limit" in capsys.readouterr().err


def test_find_comment_ignores_comments_of_others(spot):
    marker = spot.COMMENT_MARKER
    comments = [
        {"id": 1, "body": f"{marker}\nnotice", "author": "github-actions[bot]"},
        {"id": 2, "body": f"{marker}\nlook-alike", "author": "someone"},
        {"id": 3, "body": "unrelated", "author": "github-actions[bot]"},
    ]

    with mock.patch.object(
        spot, "gh_api", autospec=True, return_value="\n".join(json.dumps(c) for c in comments)
    ):
        assert spot.find_comment("apache/airflow", 42) == spot.Comment(1, f"{marker}\nnotice")


SERVER_ERROR = subprocess.CalledProcessError(1, ["gh"], stderr="gh: Server Error (HTTP 502)\n")
NOT_FOUND_ERROR = subprocess.CalledProcessError(1, ["gh"], stderr="gh: Not Found (HTTP 404)\n")
STREAM_ERROR = subprocess.CalledProcessError(
    1, ["gh"], stderr="stream error: stream ID 3; CANCEL; received from peer\n"
)


@pytest.mark.parametrize(
    ("error", "expected_ok"), [(SERVER_ERROR, True), (STREAM_ERROR, True), (NOT_FOUND_ERROR, False)]
)
def test_run_pr_comments_carries_on_past_a_failing_run(spot, capsys, error, expected_ok):
    runs = [_run(branch="a"), _run(branch="b")]

    with (
        mock.patch.object(spot, "list_pull_request_runs", autospec=True, return_value=runs),
        mock.patch.object(spot, "update_notice", autospec=True, side_effect=[error, None]) as mock_update,
    ):
        ok = spot.run_pr_comments("apache/airflow", datetime(2026, 10, 25, tzinfo=UTC))

    assert ok is expected_ok
    assert mock_update.call_count == 2
    assert capsys.readouterr().out.startswith("::warning::") is expected_ok


@pytest.mark.parametrize(("error", "expected_ok"), [(SERVER_ERROR, True), (NOT_FOUND_ERROR, None)])
def test_run_pr_comments_when_listing_the_runs_fails(spot, error, expected_ok):
    with mock.patch.object(spot, "list_pull_request_runs", autospec=True, side_effect=error):
        if expected_ok is None:
            with pytest.raises(subprocess.CalledProcessError):
                spot.run_pr_comments("apache/airflow", datetime(2026, 10, 25, tzinfo=UTC))
        else:
            assert spot.run_pr_comments("apache/airflow", datetime(2026, 10, 25, tzinfo=UTC)) is expected_ok


@pytest.mark.parametrize(
    ("args", "errors", "expected_calls", "expected_error"),
    [
        pytest.param(("repos/x",), [SERVER_ERROR, SERVER_ERROR], 3, None, id="retries-server-errors"),
        pytest.param(("repos/x",), [SERVER_ERROR] * 3, 3, SERVER_ERROR, id="gives-up"),
        pytest.param(("repos/x",), [STREAM_ERROR], 2, None, id="retries-dropped-connections"),
        pytest.param(("repos/x",), [NOT_FOUND_ERROR], 1, NOT_FOUND_ERROR, id="no-retry-on-client-error"),
        pytest.param(("-X", "POST", "repos/x"), [SERVER_ERROR], 1, SERVER_ERROR, id="no-retry-on-post"),
        pytest.param(("-X", "PATCH", "repos/x"), [SERVER_ERROR], 2, None, id="retries-patch"),
    ],
)
@mock.patch("time.sleep", autospec=True)
@mock.patch("subprocess.run", autospec=True)
def test_gh_api_retries_server_errors(
    mock_run, mock_sleep, spot, args, errors, expected_calls, expected_error
):
    mock_run.side_effect = [*errors, subprocess.CompletedProcess([], 0, stdout="out")]

    if expected_error is None:
        assert spot.gh_api(*args) == "out"
    else:
        with pytest.raises(subprocess.CalledProcessError) as raised:
            spot.gh_api(*args)
        assert raised.value is expected_error
    assert mock_run.call_count == expected_calls


def test_run_slack_writes_outputs(spot, monkeypatch, tmp_path):
    output = tmp_path / "output"
    monkeypatch.setenv("GITHUB_OUTPUT", str(output))
    jobs = [_job(spot, "Tests A", interrupted=True), _job(spot, "CI passed", interrupted=False)]

    with mock.patch.object(spot, "get_failed_jobs", autospec=True, return_value=jobs):
        spot.run_slack("apache/airflow", "7", 1)

    assert output.read_text() == (
        "failed-jobs<<EOF\nCI passed\nTests A (spot instance reclaimed)\nEOF\n"
        "failed-jobs-text<<EOF\nCI passed\\nTests A (spot instance reclaimed)\nEOF\n"
        "has-failures<<EOF\ntrue\nEOF\n"
        "skip<<EOF\ntrue\nEOF\n"
    )


def test_build_comment_neutralises_job_names(spot):
    job = spot.FailedJob(1, "`x` @someone\n\n# [link](https://evil)", "https://job/1", spot=True)

    body = spot.build_comment([job], run_id=7, attempt=1, conclusion="failure", run_url=RUN_URL)

    assert "- [`x @someone # [link](https://evil)`](https://job/1)" in body


@pytest.mark.parametrize(
    ("actor", "previous", "expected_attempt_in_notice"),
    [
        pytest.param("runsonairflow[bot]", [("Tests A", True)], 1, id="interruption-before-runs-on-re-run"),
        pytest.param("runsonairflow[bot]", [("Tests B", False)], None, id="runs-on-re-run-of-real-failure"),
        pytest.param("a-maintainer", [("Tests A", True)], None, id="maintainer-re-run"),
    ],
)
def test_update_notice_finds_the_interruption_runs_on_is_re_running(
    spot, actor, previous, expected_attempt_in_notice
):
    run = {**_run("in_progress", 2), "triggering_actor": actor}
    previous_jobs = [_job(spot, name, interrupted=interrupted) for name, interrupted in previous]

    with (
        mock.patch.object(spot, "get_failed_jobs", autospec=True, side_effect=[[], previous_jobs]),
        mock.patch.object(spot, "find_pull_requests", autospec=True, return_value=[42]),
        mock.patch.object(spot, "find_comment", autospec=True, return_value=None),
        mock.patch.object(spot, "write_comment", autospec=True) as mock_write,
    ):
        spot.update_notice("apache/airflow", run)

    if expected_attempt_in_notice is None:
        mock_write.assert_not_called()
    else:
        body = mock_write.call_args.args[3]
        assert f"[attempt {expected_attempt_in_notice}]" in body
        assert "RunsOn re-runs them" in body


@pytest.mark.parametrize(
    ("pr_numbers", "pulls", "expected"),
    [
        pytest.param([42], None, [42], id="attached-to-the-run"),
        pytest.param(
            [],
            [{"number": 42, "sha": "abc123"}, {"number": 43, "sha": "def456"}],
            [42],
            id="fork-branch-with-two-pull-requests",
        ),
        pytest.param(
            [],
            [{"number": 42, "sha": "abc123"}, {"number": 43, "sha": "abc123"}],
            [],
            id="fork-branch-with-two-pull-requests-of-one-commit",
        ),
    ],
)
def test_find_pull_requests(spot, pr_numbers, pulls, expected):
    run = {**_run(), "pr_numbers": pr_numbers}
    output = "\n".join(json.dumps(pull) for pull in pulls or [])

    with mock.patch.object(spot, "gh_api", autospec=True, return_value=output) as mock_gh_api:
        assert spot.find_pull_requests("apache/airflow", run) == expected

    assert mock_gh_api.called is (pulls is not None)


def test_list_pull_request_runs_keeps_the_newest_run_of_each_attached_pull_request(spot):
    runs = [
        {**_run(created="2026-10-25T11:00:00Z"), "id": 1, "pr_numbers": [42]},
        {**_run(created="2026-10-25T12:00:00Z"), "id": 2, "pr_numbers": [43]},
        {**_run(created="2026-10-25T10:00:00Z"), "id": 3, "pr_numbers": [42]},
    ]

    with mock.patch.object(spot, "list_runs", autospec=True, side_effect=[runs] + [[]] * 7):
        found = spot.list_pull_request_runs("apache/airflow", datetime(2026, 10, 25, 16, tzinfo=UTC))

    assert sorted(run["id"] for run in found) == [1, 2]
