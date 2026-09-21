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

import contextlib
import subprocess
from unittest import mock

import pytest

from airflow_breeze.utils.gh_workflow_utils import (
    NEW_RUN_TIMEOUT_SECONDS,
    get_latest_workflow_run_id,
    monitor_workflow_run,
    trigger_workflow_and_monitor,
    wait_for_new_workflow_run,
)
from airflow_breeze.utils.shared_options import set_dry_run


@mock.patch("airflow_breeze.utils.gh_workflow_utils.monitor_workflow_run")
@mock.patch("airflow_breeze.utils.gh_workflow_utils.make_sure_gh_is_installed")
def test_trigger_workflow_and_monitor_stops_after_the_dispatch_in_dry_run(_, mock_monitor):
    """A dry run dispatches nothing, so the empty `gh run list` must not read as a missing run."""
    set_dry_run(True)
    try:
        trigger_workflow_and_monitor(
            workflow_name="release-constraints.yml",
            repo="apache/airflow",
            version="3.2.0rc1",
            ref="v3-2-stable",
        )
    finally:
        set_dry_run(False)

    mock_monitor.assert_not_called()


@mock.patch("airflow_breeze.utils.gh_workflow_utils.run_gh_command")
@mock.patch("airflow_breeze.utils.gh_workflow_utils.make_sure_gh_is_installed")
def test_get_latest_workflow_run_id_only_looks_at_dispatched_runs(_, mock_run_gh_command):
    """Push and pull_request runs of the same workflow must not be mistaken for the dispatched one."""
    mock_run_gh_command.return_value = subprocess.CompletedProcess(
        args=[], returncode=0, stdout='[{"databaseId": 123}]', stderr=""
    )

    assert get_latest_workflow_run_id("build.yml", "apache/airflow-site") == 123

    command = mock_run_gh_command.call_args.args[0]
    assert command[command.index("--event") + 1] == "workflow_dispatch"


@pytest.mark.parametrize(
    ("exit_on_error", "expectation"),
    [(True, pytest.raises(SystemExit)), (False, contextlib.nullcontext())],
)
@mock.patch("airflow_breeze.utils.gh_workflow_utils.run_gh_command")
@mock.patch("airflow_breeze.utils.gh_workflow_utils.make_sure_gh_is_installed")
def test_get_latest_workflow_run_id_lets_a_polling_caller_survive_a_failed_lookup(
    _, mock_run_gh_command, exit_on_error, expectation
):
    mock_run_gh_command.return_value = subprocess.CompletedProcess(
        args=[], returncode=1, stdout="", stderr="could not connect to api.github.com"
    )

    with expectation:
        assert get_latest_workflow_run_id("build.yml", "apache/airflow", exit_on_error=exit_on_error) is None


@mock.patch("airflow_breeze.utils.gh_workflow_utils.time.sleep")
@mock.patch("airflow_breeze.utils.gh_workflow_utils.get_latest_workflow_run_id")
def test_wait_for_new_workflow_run_retries_after_a_failed_lookup(mock_latest, _):
    """A transient `gh` failure while polling must not abort the whole publishing chain."""
    mock_latest.side_effect = [None, 222]

    assert wait_for_new_workflow_run("build.yml", "apache/airflow-site", previous_run_id=111) == 222
    assert all(call.kwargs["exit_on_error"] is False for call in mock_latest.call_args_list)


@mock.patch("airflow_breeze.utils.gh_workflow_utils.time.sleep")
@mock.patch("airflow_breeze.utils.gh_workflow_utils.get_latest_workflow_run_id")
def test_wait_for_new_workflow_run_ignores_the_run_that_predates_the_dispatch(mock_latest, _):
    mock_latest.side_effect = [111, 111, 222]

    assert wait_for_new_workflow_run("publish-docs-to-s3.yml", "apache/airflow", previous_run_id=111) == 222


@mock.patch("airflow_breeze.utils.gh_workflow_utils.time.sleep")
@mock.patch("airflow_breeze.utils.gh_workflow_utils.get_latest_workflow_run_id")
def test_wait_for_new_workflow_run_accepts_the_first_ever_run(mock_latest, _):
    mock_latest.return_value = 42

    assert wait_for_new_workflow_run("build.yml", "apache/airflow-site", previous_run_id=None) == 42


@mock.patch("airflow_breeze.utils.gh_workflow_utils.time.monotonic")
@mock.patch("airflow_breeze.utils.gh_workflow_utils.time.sleep")
@mock.patch("airflow_breeze.utils.gh_workflow_utils.get_latest_workflow_run_id")
def test_wait_for_new_workflow_run_gives_up_when_no_new_run_appears(mock_latest, _, mock_monotonic):
    mock_latest.return_value = 111
    mock_monotonic.side_effect = [0, 0, NEW_RUN_TIMEOUT_SECONDS]

    with pytest.raises(SystemExit) as exc_info:
        wait_for_new_workflow_run("publish-docs-to-s3.yml", "apache/airflow", previous_run_id=111)

    assert exc_info.value.code == 1


@pytest.mark.parametrize("conclusion", ["failure", "cancelled", "timed_out", "action_required"])
@mock.patch("airflow_breeze.utils.gh_workflow_utils.get_workflow_run_info")
def test_monitor_workflow_run_fails_on_any_unsuccessful_conclusion(mock_info, conclusion):
    mock_info.side_effect = [
        {"jobs": []},
        {"status": "completed", "conclusion": conclusion, "name": "Publish Docs to S3"},
    ]

    with pytest.raises(SystemExit) as exc_info:
        monitor_workflow_run(run_id="123", repo="apache/airflow")

    assert exc_info.value.code == 1
