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

from unittest import mock

import httpx
import pytest

from airflow.api_fastapi.execution_api.datamodels.job import TerminalJobState
from airflow.dag_processing.api_client import (
    DagProcessorAPIClient,
    DagProcessorJobAlreadyRunning,
    DagProcessorRegistrationRetired,
)
from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.jobs.dag_processor_job_runner import DagProcessorHeartbeatTimeout, DagProcessorJobRunner
from airflow.jobs.job import Job, JobState

from tests_common.test_utils.config import conf_vars


@pytest.fixture
def client():
    client = mock.create_autospec(DagProcessorAPIClient, instance=True)
    client.register_job.return_value = 42
    client.restart_required = False
    client.heartbeat.return_value = JobState.RUNNING
    return client


@pytest.fixture
def runner():
    return DagProcessorJobRunner(
        job=Job(heartrate=5), processor=mock.create_autospec(DagFileProcessorManager, instance=True)
    )


@pytest.fixture
def clock():
    with mock.patch(
        "airflow.jobs.dag_processor_job_runner.monotonic", autospec=True, return_value=0
    ) as clock:
        yield clock


@pytest.mark.parametrize(
    ("error", "outcome"),
    [
        (None, TerminalJobState.SUCCESS),
        (SystemExit(0), TerminalJobState.SUCCESS),
        (ValueError("parse failed"), TerminalJobState.FAILED),
        (KeyboardInterrupt(), TerminalJobState.FAILED),
    ],
)
@mock.patch.object(Job, "prepare_for_execution", autospec=True)
@mock.patch.object(Job, "complete_execution", autospec=True)
def test_api_lifecycle_cleans_up_before_completion(complete, prepare, runner, client, error, outcome):
    events = []
    runner.processor.run.side_effect = error
    runner.processor.terminate.side_effect = lambda: events.append("terminate")
    runner.processor.end.side_effect = lambda: events.append("end")
    client.complete_job.side_effect = lambda state: events.append(state)

    if outcome == TerminalJobState.FAILED:
        with pytest.raises(type(error)):
            runner.run_with_api(client)
    else:
        runner.run_with_api(client)

    assert runner.job.id == 42
    assert runner.job.state == outcome.value
    assert runner.processor.api_client is client
    runner.processor.sync_bundles.assert_not_called()
    client.register_job.assert_called_once_with()
    assert events == ["terminate", "end", outcome]
    prepare.assert_not_called()
    complete.assert_not_called()


def test_failed_registration_never_starts_parsing(runner, client):
    client.register_job.side_effect = httpx.ConnectError("unavailable")
    with pytest.raises(httpx.ConnectError):
        runner.run_with_api(client)
    runner.processor.run.assert_not_called()
    client.complete_job.assert_not_called()


@pytest.mark.parametrize("available_at", [12, 15, None])
@conf_vars({("dag_processor", "health_check_threshold"): "10"})
@mock.patch("airflow.jobs.dag_processor_job_runner.sleep", autospec=True)
def test_registration_waits_for_a_crashed_job_with_a_bounded_backoff(
    sleep, runner, client, clock, available_at
):
    def wait(seconds):
        clock.return_value += seconds

    def register():
        runner.processor.run.assert_not_called()
        if available_at is None or clock.return_value < available_at:
            raise DagProcessorJobAlreadyRunning("Still alive")
        return 42

    sleep.side_effect = wait
    client.register_job.side_effect = register

    if available_at is None:
        with pytest.raises(DagProcessorJobAlreadyRunning):
            runner.run_with_api(client)
        runner.processor.run.assert_not_called()
        client.complete_job.assert_not_called()
    else:
        runner.run_with_api(client)
        runner.processor.run.assert_called_once_with()
        assert runner.job.id == 42

    assert sleep.call_args_list == [mock.call(1), mock.call(2), mock.call(4), mock.call(5)] + (
        [mock.call(3)] if available_at != 12 else []
    )
    assert clock.return_value == (available_at or 15)
    assert client.register_job.call_count == sleep.call_count + 1


@pytest.mark.parametrize("reason", ["registration_conflict", "registration_retired", None])
@mock.patch("airflow.jobs.dag_processor_job_runner.sleep", autospec=True)
def test_registration_does_not_wait_on_other_conflicts(sleep, runner, client, reason):
    error = httpx.HTTPStatusError(
        "conflict",
        request=httpx.Request("POST", "http://api/jobs"),
        response=httpx.Response(409, json={"detail": {"reason": reason}}),
    )
    client.register_job.side_effect = error

    with pytest.raises(httpx.HTTPStatusError):
        runner.run_with_api(client)

    sleep.assert_not_called()
    client.register_job.assert_called_once_with()
    runner.processor.run.assert_not_called()


@mock.patch("airflow.jobs.dag_processor_job_runner.get_listener_manager", autospec=True)
def test_shutdown_listener_failure_does_not_prevent_completion(listeners, runner, client):
    listeners.return_value.hook.before_stopping.side_effect = ValueError("listener failed")
    runner.run_with_api(client)
    listeners.return_value.hook.before_stopping.assert_called_once_with(component=runner.job)
    client.complete_job.assert_called_once_with(TerminalJobState.SUCCESS)


@pytest.mark.parametrize("parse_error", [None, ValueError("original error")])
def test_completion_failure_preserves_the_original_error(runner, client, parse_error):
    runner.processor.run.side_effect = parse_error
    client.complete_job.side_effect = httpx.ConnectError("completion unavailable")
    with pytest.raises(type(parse_error) if parse_error else httpx.ConnectError) as raised:
        runner.run_with_api(client)
    if parse_error:
        assert raised.value is parse_error


def test_heartbeats_are_throttled_and_use_the_api(runner, client, clock):
    def run():
        runner.processor.heartbeat()
        client.heartbeat.assert_not_called()
        clock.return_value = 5
        runner.processor.heartbeat()
        runner.processor.heartbeat()
        client.heartbeat.assert_called_once_with()
        clock.return_value = 10
        runner.processor.heartbeat()
        assert client.heartbeat.call_count == 2

    runner.processor.run.side_effect = run
    runner.run_with_api(client)


@pytest.mark.parametrize("restart", ["already_retired", "renewal_retired", "server_restart", "job_closed"])
def test_restart_stops_parsing_and_releases_children(runner, client, clock, restart):
    def heartbeat():
        if restart == "job_closed":
            raise DagProcessorRegistrationRetired("replaced")
        if restart == "renewal_retired":
            client.restart_required = True
        return JobState.RESTARTING if restart == "server_restart" else JobState.RUNNING

    def run():
        client.restart_required = restart == "already_retired"
        clock.return_value = 1 if client.restart_required else 5
        runner.processor.heartbeat()
        pytest.fail("Parsing continued after a restart request")

    client.heartbeat.side_effect = heartbeat
    runner.processor.run.side_effect = run
    with pytest.raises(DagProcessorRegistrationRetired):
        runner.run_with_api(client)
    runner.processor.terminate.assert_called_once_with()
    runner.processor.end.assert_called_once_with()
    client.complete_job.assert_called_once_with(TerminalJobState.FAILED)


@pytest.mark.parametrize("failure", ["transport", 503, 403])
@conf_vars({("dag_processor", "job_heartbeat_timeout"): "10"})
def test_heartbeat_failure_is_bounded(runner, client, clock, failure):
    error = (
        httpx.ConnectError("unavailable")
        if failure == "transport"
        else httpx.HTTPStatusError(
            "refused",
            request=httpx.Request("POST", "http://api/jobs/42/heartbeat"),
            response=httpx.Response(failure),
        )
    )
    client.heartbeat.side_effect = error

    def run():
        clock.return_value = 5
        runner.processor.heartbeat()
        clock.return_value = 10
        runner.processor.heartbeat()

    runner.processor.run.side_effect = run
    with pytest.raises(httpx.HTTPStatusError if failure == 403 else DagProcessorHeartbeatTimeout):
        runner.run_with_api(client)
    assert client.heartbeat.call_count == (1 if failure == 403 else 2)


@conf_vars({("dag_processor", "job_heartbeat_timeout"): "10"})
def test_recovered_heartbeat_resets_the_lease(runner, client, clock):
    client.heartbeat.side_effect = [
        httpx.ConnectError("offline"),
        JobState.RUNNING,
        httpx.ConnectError("offline"),
    ]

    def run():
        for tick in (5, 10, 15):
            clock.return_value = tick
            runner.processor.heartbeat()

    runner.processor.run.side_effect = run
    runner.run_with_api(client)
    client.complete_job.assert_called_once_with(TerminalJobState.SUCCESS)


@conf_vars(
    {
        ("dag_processor", "health_check_threshold"): "30",
        ("dag_processor", "job_heartbeat_timeout"): "300",
    }
)
def test_a_stall_past_the_health_threshold_keeps_the_lease(runner, client, clock):
    client.heartbeat.side_effect = [httpx.ConnectError("offline"), JobState.RUNNING]

    def run():
        for tick in (120, 125):
            clock.return_value = tick
            runner.processor.heartbeat()

    runner.processor.run.side_effect = run
    runner.run_with_api(client)
    assert client.heartbeat.call_count == 2
    client.complete_job.assert_called_once_with(TerminalJobState.SUCCESS)
