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

import inspect
import json
import logging
import os
import re
import selectors
import signal
import socket
import sys
import time
from io import BytesIO
from operator import attrgetter
from time import sleep
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import httpx
import psutil
import pytest
from pytest_unordered import unordered
from task_sdk import FAKE_BUNDLE, make_client
from uuid6 import uuid7

from airflow.executors.workloads import BundleInfo
from airflow.sdk.api import client as sdk_client
from airflow.sdk.api.client import ServerResponseError
from airflow.sdk.api.datamodels._generated import (
    AssetEventResponse,
    AssetResponse,
    DagRunState,
    TaskInstance,
    TerminalTIState,
)
from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
from airflow.sdk.execution_time.comms import (
    AssetEventsResult,
    AssetResult,
    ConnectionResult,
    DagRunStateResult,
    DeferTask,
    DeleteVariable,
    DeleteXCom,
    DRCount,
    ErrorResponse,
    GetAssetByName,
    GetAssetByUri,
    GetAssetEventByAsset,
    GetAssetEventByAssetAlias,
    GetConnection,
    GetDagRunState,
    GetDRCount,
    GetPrevSuccessfulDagRun,
    GetTaskRescheduleStartDate,
    GetTaskStates,
    GetTICount,
    GetVariable,
    GetXCom,
    OKResponse,
    PrevSuccessfulDagRunResult,
    PutVariable,
    RescheduleTask,
    RetryTask,
    SetRenderedFields,
    SetXCom,
    SucceedTask,
    TaskRescheduleStartDate,
    TaskState,
    TaskStatesResult,
    TICount,
    TriggerDagRun,
    VariableResult,
    XComResult,
)
from airflow.sdk.execution_time.secrets_masker import SecretsMasker
from airflow.sdk.execution_time.supervisor import BUFFER_SIZE, ActivitySubprocess, mkpipe, supervise
from airflow.sdk.execution_time.task_runner import CommsDecoder
from airflow.utils import timezone, timezone as tz

if TYPE_CHECKING:
    import kgb

log = logging.getLogger(__name__)
TI_ID = uuid7()


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


def local_dag_bundle_cfg(path, name="my-bundle"):
    return {
        "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST": json.dumps(
            [
                {
                    "name": name,
                    "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                    "kwargs": {"path": str(path), "refresh_interval": 1},
                }
            ]
        )
    }


@pytest.mark.usefixtures("disable_capturing")
class TestWatchedSubprocess:
    @pytest.fixture(autouse=True)
    def disable_log_upload(self, spy_agency):
        spy_agency.spy_on(ActivitySubprocess._upload_logs, call_original=False)

    def test_reading_from_pipes(self, captured_logs, time_machine):
        def subprocess_main():
            # This is run in the subprocess!

            # Ensure we follow the "protocol" and get the startup message before we do anything
            sys.stdin.readline()

            import logging
            import warnings

            print("I'm a short message")
            sys.stdout.write("Message ")
            print("stderr message", file=sys.stderr)
            # We need a short sleep for the main process to process things. I worry this timing will be
            # fragile, but I can't think of a better way. This lets the stdout be read (partial line) and the
            # stderr full line be read
            sleep(0.1)
            sys.stdout.write("split across two writes\n")

            logging.getLogger("airflow.foobar").error("An error message")

            warnings.warn("Warning should be captured too", stacklevel=1)

        line = lineno() - 2  # Line the error should be on

        instant = tz.datetime(2024, 11, 7, 12, 34, 56, 78901)
        time_machine.move_to(instant, tick=False)

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id="4d828a62-a417-4936-a7a6-2b3fabacecab",
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
            ),
            client=MagicMock(spec=sdk_client.Client),
            target=subprocess_main,
        )

        rc = proc.wait()

        assert rc == 0
        assert captured_logs == unordered(
            [
                {
                    "chan": "stdout",
                    "event": "I'm a short message",
                    "level": "info",
                    "logger": "task",
                    "timestamp": "2024-11-07T12:34:56.078901Z",
                },
                {
                    "chan": "stderr",
                    "event": "stderr message",
                    "level": "error",
                    "logger": "task",
                    "timestamp": "2024-11-07T12:34:56.078901Z",
                },
                {
                    "chan": "stdout",
                    "event": "Message split across two writes",
                    "level": "info",
                    "logger": "task",
                    "timestamp": "2024-11-07T12:34:56.078901Z",
                },
                {
                    "event": "An error message",
                    "level": "error",
                    "logger": "airflow.foobar",
                    "timestamp": instant.replace(tzinfo=None),
                },
                {
                    "category": "UserWarning",
                    "event": "Warning should be captured too",
                    "filename": __file__,
                    "level": "warning",
                    "lineno": line,
                    "logger": "py.warnings",
                    "timestamp": instant.replace(tzinfo=None),
                },
            ]
        )

    def test_subprocess_sigkilled(self):
        main_pid = os.getpid()

        def subprocess_main():
            # Ensure we follow the "protocol" and get the startup message before we do anything
            sys.stdin.readline()

            assert os.getpid() != main_pid
            os.kill(os.getpid(), signal.SIGKILL)

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id="4d828a62-a417-4936-a7a6-2b3fabacecab",
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
            ),
            client=MagicMock(spec=sdk_client.Client),
            target=subprocess_main,
        )

        rc = proc.wait()

        assert rc == -9

    def test_last_chance_exception_handling(self, capfd):
        def subprocess_main():
            # The real main() in task_runner catches exceptions! This is what would happen if we had a syntax
            # or import error for instance - a very early exception
            raise RuntimeError("Fake syntax error")

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(id=uuid7(), task_id="b", dag_id="c", run_id="d", try_number=1),
            client=MagicMock(spec=sdk_client.Client),
            target=subprocess_main,
        )

        rc = proc.wait()

        assert rc == 126

        captured = capfd.readouterr()
        assert "Last chance exception handler" in captured.err
        assert "RuntimeError: Fake syntax error" in captured.err

    def test_regular_heartbeat(self, spy_agency: kgb.SpyAgency, monkeypatch, mocker, make_ti_context):
        """Test that the WatchedSubprocess class regularly sends heartbeat requests, up to a certain frequency"""
        import airflow.sdk.execution_time.supervisor

        monkeypatch.setattr(airflow.sdk.execution_time.supervisor, "MIN_HEARTBEAT_INTERVAL", 0.1)

        def subprocess_main():
            sys.stdin.readline()

            for _ in range(5):
                print("output", flush=True)
                sleep(0.05)

        ti_id = uuid7()
        _ = mocker.patch.object(sdk_client.TaskInstanceOperations, "start", return_value=make_ti_context())

        spy = spy_agency.spy_on(sdk_client.TaskInstanceOperations.heartbeat)
        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(id=ti_id, task_id="b", dag_id="c", run_id="d", try_number=1),
            client=sdk_client.Client(base_url="", dry_run=True, token=""),
            target=subprocess_main,
        )
        assert proc.wait() == 0
        assert spy.called_with(ti_id, pid=proc.pid)  # noqa: PGH005
        # The exact number we get will depend on timing behaviour, so be a little lenient
        assert 1 <= len(spy.calls) <= 4

    def test_no_heartbeat_in_overtime(self, spy_agency: kgb.SpyAgency, monkeypatch, mocker, make_ti_context):
        """Test that we don't try and send heartbeats for task that are in "overtime"."""
        import airflow.sdk.execution_time.supervisor

        monkeypatch.setattr(airflow.sdk.execution_time.supervisor, "MIN_HEARTBEAT_INTERVAL", 0.1)

        def subprocess_main():
            sys.stdin.readline()

            for _ in range(5):
                print("output", flush=True)
                sleep(0.05)

        ti_id = uuid7()
        _ = mocker.patch.object(sdk_client.TaskInstanceOperations, "start", return_value=make_ti_context())

        @spy_agency.spy_for(ActivitySubprocess._on_child_started)
        def _on_child_started(self, *args, **kwargs):
            # Set it up so we are in overtime straight away
            self._terminal_state = TerminalTIState.SUCCESS
            ActivitySubprocess._on_child_started.call_original(self, *args, **kwargs)

        heartbeat_spy = spy_agency.spy_on(sdk_client.TaskInstanceOperations.heartbeat)
        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(id=ti_id, task_id="b", dag_id="c", run_id="d", try_number=1),
            client=sdk_client.Client(base_url="", dry_run=True, token=""),
            target=subprocess_main,
        )
        assert proc.wait() == 0
        spy_agency.assert_spy_not_called(heartbeat_spy)

    def test_run_simple_dag(self, test_dags_dir, captured_logs, time_machine, mocker, make_ti_context):
        """Test running a simple DAG in a subprocess and capturing the output."""

        instant = tz.datetime(2024, 11, 7, 12, 34, 56, 78901)
        time_machine.move_to(instant, tick=False)

        dagfile_path = test_dags_dir
        ti = TaskInstance(
            id=uuid7(),
            task_id="hello",
            dag_id="super_basic_run",
            run_id="c",
            try_number=1,
        )

        # Create a mock client to assert calls to the client
        # We assume the implementation of the client is correct and only need to check the calls
        mock_client = mocker.Mock(spec=sdk_client.Client)
        mock_client.task_instances.start.return_value = make_ti_context()

        bundle_info = BundleInfo(name="my-bundle", version=None)
        with patch.dict(os.environ, local_dag_bundle_cfg(test_dags_dir, bundle_info.name)):
            exit_code = supervise(
                ti=ti,
                dag_rel_path=dagfile_path,
                token="",
                server="",
                dry_run=True,
                client=mock_client,
                bundle_info=bundle_info,
            )
            assert exit_code == 0, captured_logs

        # We should have a log from the task!
        assert {
            "chan": "stdout",
            "event": "Hello World hello!",
            "level": "info",
            "logger": "task",
            "timestamp": "2024-11-07T12:34:56.078901Z",
        } in captured_logs

    def test_supervise_handles_deferred_task(
        self, test_dags_dir, captured_logs, time_machine, mocker, make_ti_context
    ):
        """
        Test that the supervisor handles a deferred task correctly.

        This includes ensuring the task starts and executes successfully, and that the task is deferred (via
        the API client) with the expected parameters.
        """
        instant = tz.datetime(2024, 11, 7, 12, 34, 56, 0)

        ti = TaskInstance(
            id=uuid7(),
            task_id="async",
            dag_id="super_basic_deferred_run",
            run_id="d",
            try_number=1,
        )

        # Create a mock client to assert calls to the client
        # We assume the implementation of the client is correct and only need to check the calls
        mock_client = mocker.Mock(spec=sdk_client.Client)
        mock_client.task_instances.start.return_value = make_ti_context()

        time_machine.move_to(instant, tick=False)

        bundle_info = BundleInfo(name="my-bundle", version=None)
        with patch.dict(os.environ, local_dag_bundle_cfg(test_dags_dir, bundle_info.name)):
            exit_code = supervise(
                ti=ti,
                dag_rel_path="super_basic_deferred_run.py",
                token="",
                client=mock_client,
                bundle_info=bundle_info,
            )
        assert exit_code == 0, captured_logs

        # Validate calls to the client
        mock_client.task_instances.start.assert_called_once_with(ti.id, mocker.ANY, mocker.ANY)
        mock_client.task_instances.heartbeat.assert_called_once_with(ti.id, pid=mocker.ANY)
        mock_client.task_instances.defer.assert_called_once_with(
            ti.id,
            # Since the message as serialized in the client upon sending, we expect it to be already encoded
            DeferTask(
                classpath="airflow.providers.standard.triggers.temporal.DateTimeTrigger",
                next_method="execute_complete",
                trigger_kwargs={
                    "__type": "dict",
                    "__var": {
                        "moment": {"__type": "datetime", "__var": 1730982899.0},
                        "end_from_trigger": False,
                    },
                },
                next_kwargs={"__type": "dict", "__var": {}},
            ),
        )

        # We are asserting the log messages here to ensure the task ran successfully
        # and mainly to get the final state of the task matches one in the DB.
        assert {
            "exit_code": 0,
            "duration": 0.0,
            "final_state": "deferred",
            "event": "Task finished",
            "timestamp": mocker.ANY,
            "level": "info",
            "logger": "supervisor",
        } in captured_logs

    def test_supervisor_handles_already_running_task(self):
        """Test that Supervisor prevents starting a Task Instance that is already running."""
        ti = TaskInstance(id=uuid7(), task_id="b", dag_id="c", run_id="d", try_number=1)

        # Mock API Server response indicating the TI is already running
        # The API Server would return a 409 Conflict status code if the TI is not
        # in a "queued" state.
        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti.id}/run":
                return httpx.Response(
                    409,
                    json={
                        "reason": "invalid_state",
                        "message": "TI was not in a state where it could be marked as running",
                        "previous_state": "running",
                    },
                )

            return httpx.Response(status_code=204)

        client = make_client(transport=httpx.MockTransport(handle_request))

        with pytest.raises(ServerResponseError, match="Server returned error") as err:
            ActivitySubprocess.start(dag_rel_path=os.devnull, bundle_info=FAKE_BUNDLE, what=ti, client=client)

        assert err.value.response.status_code == 409
        assert err.value.detail == {
            "reason": "invalid_state",
            "message": "TI was not in a state where it could be marked as running",
            "previous_state": "running",
        }

    @pytest.mark.parametrize("captured_logs", [logging.ERROR], indirect=True, ids=["log_level=error"])
    def test_state_conflict_on_heartbeat(self, captured_logs, monkeypatch, mocker, make_ti_context_dict):
        """
        Test that ensures that the Supervisor does not cause the task to fail if the Task Instance is no longer
        in the running state. Instead, it logs the error and terminates the task process if it
        might be running in a different state or has already completed -- or running on a different worker.

        Also verifies that the supervisor does not try to send the finish request (update_state) to the API server.
        """
        import airflow.sdk.execution_time.supervisor

        # Heartbeat every time around the loop
        monkeypatch.setattr(airflow.sdk.execution_time.supervisor, "MIN_HEARTBEAT_INTERVAL", 0.0)

        def subprocess_main():
            sys.stdin.readline()
            sleep(5)
            # Shouldn't get here
            exit(5)

        ti_id = uuid7()

        # Track the number of requests to simulate mixed responses
        request_count = {"count": 0}

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/heartbeat":
                request_count["count"] += 1
                if request_count["count"] == 1:
                    # First request succeeds
                    return httpx.Response(status_code=204)
                # Second request returns a conflict status code
                return httpx.Response(
                    409,
                    json={
                        "reason": "not_running",
                        "message": "TI is no longer in the 'running' state. Task state might be externally set and task should terminate",
                        "current_state": "success",
                    },
                )
            if request.url.path == f"/task-instances/{ti_id}/run":
                return httpx.Response(200, json=make_ti_context_dict())
            if request.url.path == f"/task-instances/{ti_id}/state":
                pytest.fail("Should not have sent a state update request")
            # Return a 204 for all other requests
            return httpx.Response(status_code=204)

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            what=TaskInstance(id=ti_id, task_id="b", dag_id="c", run_id="d", try_number=1),
            client=make_client(transport=httpx.MockTransport(handle_request)),
            target=subprocess_main,
            bundle_info=FAKE_BUNDLE,
        )

        # Wait for the subprocess to finish -- it should have been terminated with SIGTERM
        assert proc.wait() == -signal.SIGTERM
        assert proc._exit_code == -signal.SIGTERM
        assert proc.final_state == "SERVER_TERMINATED"

        assert request_count["count"] == 2
        # Verify the error was logged
        assert captured_logs == [
            {
                "detail": {
                    "reason": "not_running",
                    "message": "TI is no longer in the 'running' state. Task state might be externally set and task should terminate",
                    "current_state": "success",
                },
                "event": "Server indicated the task shouldn't be running anymore",
                "level": "error",
                "status_code": 409,
                "logger": "supervisor",
                "timestamp": mocker.ANY,
                "ti_id": ti_id,
            },
            {
                "detail": {
                    "current_state": "success",
                    "message": "TI is no longer in the 'running' state. Task state might be externally set and task should terminate",
                    "reason": "not_running",
                },
                "event": "Server indicated the task shouldn't be running anymore. Terminating process",
                "level": "error",
                "logger": "task",
                "timestamp": mocker.ANY,
            },
            {
                "event": "Task killed!",
                "level": "error",
                "logger": "task",
                "timestamp": mocker.ANY,
            },
        ]

    @pytest.mark.parametrize("captured_logs", [logging.WARNING], indirect=True)
    def test_heartbeat_failures_handling(self, monkeypatch, mocker, captured_logs, time_machine):
        """
        Test that ensures the WatchedSubprocess kills the process after
        MAX_FAILED_HEARTBEATS are exceeded.
        """
        max_failed_heartbeats = 3
        min_heartbeat_interval = 5
        monkeypatch.setattr(
            "airflow.sdk.execution_time.supervisor.MAX_FAILED_HEARTBEATS", max_failed_heartbeats
        )
        monkeypatch.setattr(
            "airflow.sdk.execution_time.supervisor.MIN_HEARTBEAT_INTERVAL", min_heartbeat_interval
        )

        mock_process = mocker.Mock()
        mock_process.pid = 12345

        # Mock the client heartbeat method to raise an exception
        mock_client_heartbeat = mocker.Mock(side_effect=Exception("Simulated heartbeat failure"))
        client = mocker.Mock()
        client.task_instances.heartbeat = mock_client_heartbeat

        # Patch the kill method at the class level so we can assert it was called with the correct signal
        mock_kill = mocker.patch("airflow.sdk.execution_time.supervisor.WatchedSubprocess.kill")

        proc = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=mock_process.pid,
            stdin=mocker.MagicMock(),
            client=client,
            process=mock_process,
            requests_fd=-1,
        )

        time_now = tz.datetime(2024, 11, 28, 12, 0, 0)
        time_machine.move_to(time_now, tick=False)

        # Simulate sending heartbeats and ensure the process gets killed after max retries
        for i in range(1, max_failed_heartbeats):
            proc._send_heartbeat_if_needed()
            assert proc.failed_heartbeats == i  # Increment happens after failure
            mock_client_heartbeat.assert_called_with(TI_ID, pid=mock_process.pid)

            # Ensure the retry log is present
            expected_log = {
                "event": "Failed to send heartbeat. Will be retried",
                "failed_heartbeats": i,
                "ti_id": TI_ID,
                "max_retries": max_failed_heartbeats,
                "level": "warning",
                "logger": "supervisor",
                "timestamp": mocker.ANY,
                "exception": mocker.ANY,
            }

            assert expected_log in captured_logs

            # Advance time by `min_heartbeat_interval` to allow the next heartbeat
            time_machine.shift(min_heartbeat_interval)

        # On the final failure, the process should be killed
        proc._send_heartbeat_if_needed()

        assert proc.failed_heartbeats == max_failed_heartbeats
        mock_kill.assert_called_once_with(signal.SIGTERM, force=True)
        mock_client_heartbeat.assert_called_with(TI_ID, pid=mock_process.pid)
        assert {
            "event": "Too many failed heartbeats; terminating process",
            "level": "error",
            "failed_heartbeats": max_failed_heartbeats,
            "logger": "supervisor",
            "timestamp": mocker.ANY,
        } in captured_logs

    @pytest.mark.parametrize(
        ["terminal_state", "task_end_time_monotonic", "overtime_threshold", "expected_kill"],
        [
            pytest.param(
                None,
                15.0,
                10,
                False,
                id="no_terminal_state",
            ),
            pytest.param(TerminalTIState.SUCCESS, 15.0, 10, False, id="below_threshold"),
            pytest.param(TerminalTIState.SUCCESS, 9.0, 10, True, id="above_threshold"),
            pytest.param(TerminalTIState.FAILED, 9.0, 10, True, id="above_threshold_failed_state"),
            pytest.param(TerminalTIState.SKIPPED, 9.0, 10, True, id="above_threshold_skipped_state"),
            pytest.param(TerminalTIState.SUCCESS, None, 20, False, id="task_end_datetime_none"),
        ],
    )
    def test_overtime_handling(
        self,
        mocker,
        terminal_state,
        task_end_time_monotonic,
        overtime_threshold,
        expected_kill,
        monkeypatch,
    ):
        """Test handling of overtime under various conditions."""
        # Mocking logger since we are only interested that it is called with the expected message
        # and not the actual log output
        mock_logger = mocker.patch("airflow.sdk.execution_time.supervisor.log")

        # Mock the kill method at the class level so we can assert it was called with the correct signal
        mock_kill = mocker.patch("airflow.sdk.execution_time.supervisor.WatchedSubprocess.kill")

        # Mock the current monotonic time
        mocker.patch("time.monotonic", return_value=20.0)

        # Patch the task overtime threshold
        monkeypatch.setattr(ActivitySubprocess, "TASK_OVERTIME_THRESHOLD", overtime_threshold)

        mock_watched_subprocess = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=12345,
            stdin=mocker.Mock(),
            process=mocker.Mock(),
            client=mocker.Mock(),
            requests_fd=-1,
        )

        # Set the terminal state and task end datetime
        mock_watched_subprocess._terminal_state = terminal_state
        mock_watched_subprocess._task_end_time_monotonic = task_end_time_monotonic

        # Call `wait` to trigger the overtime handling
        # This will call the `kill` method if the task has been running for too long
        mock_watched_subprocess._handle_process_overtime_if_needed()

        # Validate process kill behavior and log messages
        if expected_kill:
            mock_kill.assert_called_once_with(signal.SIGTERM, force=True)
            mock_logger.warning.assert_called_once_with(
                "Workload success overtime reached; terminating process",
                ti_id=TI_ID,
            )
        else:
            mock_kill.assert_not_called()
            mock_logger.warning.assert_not_called()

    @pytest.mark.parametrize(
        ["signal_to_raise", "log_pattern"],
        (
            pytest.param(
                signal.SIGKILL,
                re.compile(r"Process terminated by signal. For more information, see"),
                id="kill",
            ),
            pytest.param(
                signal.SIGSEGV,
                re.compile(r".*SIGSEGV \(Segmentation Violation\) signal indicates", re.DOTALL),
                id="segv",
            ),
        ),
    )
    def test_exit_by_signal(self, monkeypatch, signal_to_raise, log_pattern, cap_structlog):
        def subprocess_main():
            import faulthandler
            import os

            # Disable pytest fault handler
            if faulthandler.is_enabled():
                faulthandler.disable()

            # Ensure we follow the "protocol" and get the startup message before we do anything
            sys.stdin.readline()

            os.kill(os.getpid(), signal_to_raise)

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id="4d828a62-a417-4936-a7a6-2b3fabacecab",
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
            ),
            client=MagicMock(spec=sdk_client.Client),
            target=subprocess_main,
        )

        rc = proc.wait()

        assert {
            "log_level": "critical",
            "event": log_pattern,
        } in cap_structlog
        assert rc == -signal_to_raise


class TestWatchedSubprocessKill:
    @pytest.fixture
    def mock_process(self, mocker):
        process = mocker.Mock(spec=psutil.Process)
        process.pid = 12345
        return process

    @pytest.fixture
    def watched_subprocess(self, mocker, mock_process):
        proc = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=12345,
            stdin=mocker.Mock(),
            client=mocker.Mock(),
            process=mock_process,
            requests_fd=-1,
        )
        # Mock the selector
        mock_selector = mocker.Mock(spec=selectors.DefaultSelector)
        mock_selector.select.return_value = []

        # Set the selector on the process
        proc.selector = mock_selector
        return proc

    def test_kill_process_already_exited(self, watched_subprocess, mock_process):
        """Test behavior when the process has already exited."""
        mock_process.wait.side_effect = psutil.NoSuchProcess(pid=1234)
        watched_subprocess.kill(signal.SIGINT, force=True)

        mock_process.send_signal.assert_called_once_with(signal.SIGINT)
        mock_process.wait.assert_called_once()
        assert watched_subprocess._exit_code == -1

    def test_kill_process_custom_signal(self, watched_subprocess, mock_process):
        """Test that the process is killed with the correct signal."""
        mock_process.wait.return_value = 0

        signal_to_send = signal.SIGUSR1
        watched_subprocess.kill(signal_to_send, force=False)

        mock_process.send_signal.assert_called_once_with(signal_to_send)
        mock_process.wait.assert_called_once_with(timeout=0)

    @pytest.mark.parametrize(
        ["signal_to_send", "exit_after"],
        [
            pytest.param(
                signal.SIGINT,
                signal.SIGINT,
                id="SIGINT-success-without-escalation",
            ),
            pytest.param(
                signal.SIGINT,
                signal.SIGTERM,
                id="SIGINT-escalates-to-SIGTERM",
            ),
            pytest.param(
                signal.SIGINT,
                None,
                id="SIGINT-escalates-to-SIGTERM-then-SIGKILL",
            ),
            pytest.param(
                signal.SIGTERM,
                None,
                id="SIGTERM-escalates-to-SIGKILL",
            ),
            pytest.param(
                signal.SIGKILL,
                None,
                id="SIGKILL-success-without-escalation",
            ),
        ],
    )
    def test_kill_escalation_path(self, signal_to_send, exit_after, mocker, captured_logs, monkeypatch):
        def subprocess_main():
            import signal

            def _handler(sig, frame):
                print(f"Signal {sig} received", file=sys.stderr)
                if exit_after == sig:
                    sleep(0.1)
                    # We exit 0 as that's what task_runner.py tries hard to do. The only difference if we exit
                    # with non-zero is extra logs
                    exit(0)
                sleep(5)
                print("Should not get here")

            signal.signal(signal.SIGINT, _handler)
            signal.signal(signal.SIGTERM, _handler)
            try:
                sys.stdin.readline()
                print("Ready")
                sleep(10)
            except Exception as e:
                print(e)
            # Shouldn't get here
            exit(5)

        ti_id = uuid7()

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(id=ti_id, task_id="b", dag_id="c", run_id="d", try_number=1),
            client=MagicMock(spec=sdk_client.Client),
            target=subprocess_main,
        )

        # Ensure we get one normal run, to give the proc time to register it's custom sighandler
        time.sleep(0.1)
        proc._service_subprocess(max_wait_time=1)
        proc.kill(signal_to_send=signal_to_send, escalation_delay=0.5, force=True)

        # Wait for the subprocess to finish
        assert proc.wait() == exit_after or -signal.SIGKILL
        exit_after = exit_after or signal.SIGKILL

        logs = [{"event": m["event"], "chan": m.get("chan"), "logger": m["logger"]} for m in captured_logs]
        expected_logs = [
            {"chan": "stdout", "event": "Ready", "logger": "task"},
        ]
        # Work out what logs we expect to see
        if signal_to_send == signal.SIGINT:
            expected_logs.append({"chan": "stderr", "event": "Signal 2 received", "logger": "task"})
        if signal_to_send == signal.SIGTERM or (
            signal_to_send == signal.SIGINT and exit_after != signal.SIGINT
        ):
            if signal_to_send == signal.SIGINT:
                expected_logs.append(
                    {
                        "chan": None,
                        "event": "Process did not terminate in time; escalating",
                        "logger": "supervisor",
                    }
                )
            expected_logs.append({"chan": "stderr", "event": "Signal 15 received", "logger": "task"})
        if exit_after == signal.SIGKILL:
            if signal_to_send in {signal.SIGINT, signal.SIGTERM}:
                expected_logs.append(
                    {
                        "chan": None,
                        "event": "Process did not terminate in time; escalating",
                        "logger": "supervisor",
                    }
                )

        expected_logs.extend(({"chan": None, "event": "Process exited", "logger": "supervisor"},))
        assert logs == expected_logs

    def test_service_subprocess(self, watched_subprocess, mock_process, mocker):
        """Test `_service_subprocess` processes selector events and handles subprocess exit."""
        ## Given

        # Mock file objects and handlers
        mock_stdout = mocker.Mock()
        mock_stderr = mocker.Mock()

        # Handlers for stdout and stderr
        mock_stdout_handler = mocker.Mock(return_value=False)  # Simulate EOF for stdout
        mock_stderr_handler = mocker.Mock(return_value=True)  # Continue processing for stderr

        # Mock selector to return events
        mock_key_stdout = mocker.Mock(fileobj=mock_stdout, data=mock_stdout_handler)
        mock_key_stderr = mocker.Mock(fileobj=mock_stderr, data=mock_stderr_handler)
        watched_subprocess.selector.select.return_value = [(mock_key_stdout, None), (mock_key_stderr, None)]

        # Mock to simulate process exited successfully
        mock_process.wait.return_value = 0

        ## Our actual test
        watched_subprocess._service_subprocess(max_wait_time=1.0)

        ## Validations!
        # Validate selector interactions
        watched_subprocess.selector.select.assert_called_once_with(timeout=1.0)

        # Validate handler calls
        mock_stdout_handler.assert_called_once_with(mock_stdout)
        mock_stderr_handler.assert_called_once_with(mock_stderr)

        # Validate unregistering and closing of EOF file object
        watched_subprocess.selector.unregister.assert_called_once_with(mock_stdout)
        mock_stdout.close.assert_called_once()

        # Validate that `_check_subprocess_exit` is called
        mock_process.wait.assert_called_once_with(timeout=0)


class TestHandleRequest:
    @pytest.fixture
    def watched_subprocess(self, mocker):
        read_end, write_end = mkpipe(remote_read=True)

        subprocess = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=12345,
            stdin=write_end,  # this is the writer side
            client=mocker.Mock(),
            process=mocker.Mock(),
            requests_fd=-1,
        )

        return subprocess, read_end

    @patch("airflow.sdk.execution_time.secrets_masker._secrets_masker")
    @pytest.mark.parametrize(
        ["message", "expected_buffer", "client_attr_path", "method_arg", "method_kwarg", "mock_response"],
        [
            pytest.param(
                GetConnection(conn_id="test_conn"),
                b'{"conn_id":"test_conn","conn_type":"mysql","type":"ConnectionResult"}\n',
                "connections.get",
                ("test_conn",),
                {},
                ConnectionResult(conn_id="test_conn", conn_type="mysql"),
                id="get_connection",
            ),
            pytest.param(
                GetConnection(conn_id="test_conn"),
                b'{"conn_id":"test_conn","conn_type":"mysql","schema":"mysql","type":"ConnectionResult"}\n',
                "connections.get",
                ("test_conn",),
                {},
                ConnectionResult(conn_id="test_conn", conn_type="mysql", schema="mysql"),  # type: ignore[call-arg]
                id="get_connection_with_alias",
            ),
            pytest.param(
                GetVariable(key="test_key"),
                b'{"key":"test_key","value":"test_value","type":"VariableResult"}\n',
                "variables.get",
                ("test_key",),
                {},
                VariableResult(key="test_key", value="test_value"),
                id="get_variable",
            ),
            pytest.param(
                PutVariable(key="test_key", value="test_value", description="test_description"),
                b"",
                "variables.set",
                ("test_key", "test_value", "test_description"),
                {},
                OKResponse(ok=True),
                id="set_variable",
            ),
            pytest.param(
                DeleteVariable(key="test_key"),
                b'{"ok":true,"type":"OKResponse"}\n',
                "variables.delete",
                ("test_key",),
                {},
                OKResponse(ok=True),
                id="delete_variable",
            ),
            pytest.param(
                DeferTask(next_method="execute_callback", classpath="my-classpath"),
                b"",
                "task_instances.defer",
                (TI_ID, DeferTask(next_method="execute_callback", classpath="my-classpath")),
                {},
                "",
                id="patch_task_instance_to_deferred",
            ),
            pytest.param(
                RescheduleTask(
                    reschedule_date=timezone.parse("2024-10-31T12:00:00Z"),
                    end_date=timezone.parse("2024-10-31T12:00:00Z"),
                ),
                b"",
                "task_instances.reschedule",
                (
                    TI_ID,
                    RescheduleTask(
                        reschedule_date=timezone.parse("2024-10-31T12:00:00Z"),
                        end_date=timezone.parse("2024-10-31T12:00:00Z"),
                    ),
                ),
                {},
                "",
                id="patch_task_instance_to_up_for_reschedule",
            ),
            pytest.param(
                GetXCom(dag_id="test_dag", run_id="test_run", task_id="test_task", key="test_key"),
                b'{"key":"test_key","value":"test_value","type":"XComResult"}\n',
                "xcoms.get",
                ("test_dag", "test_run", "test_task", "test_key", None, False),
                {},
                XComResult(key="test_key", value="test_value"),
                id="get_xcom",
            ),
            pytest.param(
                GetXCom(
                    dag_id="test_dag", run_id="test_run", task_id="test_task", key="test_key", map_index=2
                ),
                b'{"key":"test_key","value":"test_value","type":"XComResult"}\n',
                "xcoms.get",
                ("test_dag", "test_run", "test_task", "test_key", 2, False),
                {},
                XComResult(key="test_key", value="test_value"),
                id="get_xcom_map_index",
            ),
            pytest.param(
                GetXCom(dag_id="test_dag", run_id="test_run", task_id="test_task", key="test_key"),
                b'{"key":"test_key","value":null,"type":"XComResult"}\n',
                "xcoms.get",
                ("test_dag", "test_run", "test_task", "test_key", None, False),
                {},
                XComResult(key="test_key", value=None, type="XComResult"),
                id="get_xcom_not_found",
            ),
            pytest.param(
                GetXCom(
                    dag_id="test_dag",
                    run_id="test_run",
                    task_id="test_task",
                    key="test_key",
                    include_prior_dates=True,
                ),
                b'{"key":"test_key","value":null,"type":"XComResult"}\n',
                "xcoms.get",
                ("test_dag", "test_run", "test_task", "test_key", None, True),
                {},
                XComResult(key="test_key", value=None, type="XComResult"),
                id="get_xcom_include_prior_dates",
            ),
            pytest.param(
                SetXCom(
                    dag_id="test_dag",
                    run_id="test_run",
                    task_id="test_task",
                    key="test_key",
                    value='{"key": "test_key", "value": {"key2": "value2"}}',
                ),
                b"",
                "xcoms.set",
                (
                    "test_dag",
                    "test_run",
                    "test_task",
                    "test_key",
                    '{"key": "test_key", "value": {"key2": "value2"}}',
                    None,
                    None,
                ),
                {},
                OKResponse(ok=True),
                id="set_xcom",
            ),
            pytest.param(
                SetXCom(
                    dag_id="test_dag",
                    run_id="test_run",
                    task_id="test_task",
                    key="test_key",
                    value='{"key": "test_key", "value": {"key2": "value2"}}',
                    map_index=2,
                ),
                b"",
                "xcoms.set",
                (
                    "test_dag",
                    "test_run",
                    "test_task",
                    "test_key",
                    '{"key": "test_key", "value": {"key2": "value2"}}',
                    2,
                    None,
                ),
                {},
                OKResponse(ok=True),
                id="set_xcom_with_map_index",
            ),
            pytest.param(
                SetXCom(
                    dag_id="test_dag",
                    run_id="test_run",
                    task_id="test_task",
                    key="test_key",
                    value='{"key": "test_key", "value": {"key2": "value2"}}',
                    map_index=2,
                    mapped_length=3,
                ),
                b"",
                "xcoms.set",
                (
                    "test_dag",
                    "test_run",
                    "test_task",
                    "test_key",
                    '{"key": "test_key", "value": {"key2": "value2"}}',
                    2,
                    3,
                ),
                {},
                OKResponse(ok=True),
                id="set_xcom_with_map_index_and_mapped_length",
            ),
            pytest.param(
                DeleteXCom(
                    dag_id="test_dag",
                    run_id="test_run",
                    task_id="test_task",
                    key="test_key",
                    map_index=2,
                ),
                b"",
                "xcoms.delete",
                (
                    "test_dag",
                    "test_run",
                    "test_task",
                    "test_key",
                    2,
                ),
                {},
                OKResponse(ok=True),
                id="delete_xcom",
            ),
            # we aren't adding all states under TerminalTIState here, because this test's scope is only to check
            # if it can handle TaskState message
            pytest.param(
                TaskState(state=TerminalTIState.SKIPPED, end_date=timezone.parse("2024-10-31T12:00:00Z")),
                b"",
                "",
                (),
                {},
                "",
                id="patch_task_instance_to_skipped",
            ),
            pytest.param(
                RetryTask(end_date=timezone.parse("2024-10-31T12:00:00Z")),
                b"",
                "task_instances.retry",
                (),
                {"id": TI_ID, "end_date": timezone.parse("2024-10-31T12:00:00Z")},
                "",
                id="up_for_retry",
            ),
            pytest.param(
                SetRenderedFields(rendered_fields={"field1": "rendered_value1", "field2": "rendered_value2"}),
                b"",
                "task_instances.set_rtif",
                (TI_ID, {"field1": "rendered_value1", "field2": "rendered_value2"}),
                {},
                OKResponse(ok=True),
                id="set_rtif",
            ),
            pytest.param(
                GetAssetByName(name="asset"),
                b'{"name":"asset","uri":"s3://bucket/obj","group":"asset","type":"AssetResult"}\n',
                "assets.get",
                [],
                {"name": "asset"},
                AssetResult(name="asset", uri="s3://bucket/obj", group="asset"),
                id="get_asset_by_name",
            ),
            pytest.param(
                GetAssetByUri(uri="s3://bucket/obj"),
                b'{"name":"asset","uri":"s3://bucket/obj","group":"asset","type":"AssetResult"}\n',
                "assets.get",
                [],
                {"uri": "s3://bucket/obj"},
                AssetResult(name="asset", uri="s3://bucket/obj", group="asset"),
                id="get_asset_by_uri",
            ),
            pytest.param(
                GetAssetEventByAsset(uri="s3://bucket/obj", name="test"),
                (
                    b'{"asset_events":'
                    b'[{"id":1,"timestamp":"2024-10-31T12:00:00Z","asset":{"name":"asset","uri":"s3://bucket/obj","group":"asset"},'
                    b'"created_dagruns":[]}],"type":"AssetEventsResult"}\n'
                ),
                "asset_events.get",
                [],
                {"uri": "s3://bucket/obj", "name": "test"},
                AssetEventsResult(
                    asset_events=[
                        AssetEventResponse(
                            id=1,
                            asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                            created_dagruns=[],
                            timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                        )
                    ]
                ),
                id="get_asset_events_by_uri_and_name",
            ),
            pytest.param(
                GetAssetEventByAsset(uri="s3://bucket/obj", name=None),
                (
                    b'{"asset_events":'
                    b'[{"id":1,"timestamp":"2024-10-31T12:00:00Z","asset":{"name":"asset","uri":"s3://bucket/obj","group":"asset"},'
                    b'"created_dagruns":[]}],"type":"AssetEventsResult"}\n'
                ),
                "asset_events.get",
                [],
                {"uri": "s3://bucket/obj", "name": None},
                AssetEventsResult(
                    asset_events=[
                        AssetEventResponse(
                            id=1,
                            asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                            created_dagruns=[],
                            timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                        )
                    ]
                ),
                id="get_asset_events_by_uri",
            ),
            pytest.param(
                GetAssetEventByAsset(uri=None, name="test"),
                (
                    b'{"asset_events":'
                    b'[{"id":1,"timestamp":"2024-10-31T12:00:00Z","asset":{"name":"asset","uri":"s3://bucket/obj","group":"asset"},'
                    b'"created_dagruns":[]}],"type":"AssetEventsResult"}\n'
                ),
                "asset_events.get",
                [],
                {"uri": None, "name": "test"},
                AssetEventsResult(
                    asset_events=[
                        AssetEventResponse(
                            id=1,
                            asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                            created_dagruns=[],
                            timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                        )
                    ]
                ),
                id="get_asset_events_by_name",
            ),
            pytest.param(
                GetAssetEventByAssetAlias(alias_name="test_alias"),
                (
                    b'{"asset_events":'
                    b'[{"id":1,"timestamp":"2024-10-31T12:00:00Z","asset":{"name":"asset","uri":"s3://bucket/obj","group":"asset"},'
                    b'"created_dagruns":[]}],"type":"AssetEventsResult"}\n'
                ),
                "asset_events.get",
                [],
                {"alias_name": "test_alias"},
                AssetEventsResult(
                    asset_events=[
                        AssetEventResponse(
                            id=1,
                            asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                            created_dagruns=[],
                            timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                        )
                    ]
                ),
                id="get_asset_events_by_asset_alias",
            ),
            pytest.param(
                SucceedTask(end_date=timezone.parse("2024-10-31T12:00:00Z")),
                b"",
                "task_instances.succeed",
                (),
                {
                    "id": TI_ID,
                    "outlet_events": None,
                    "task_outlets": None,
                    "when": timezone.parse("2024-10-31T12:00:00Z"),
                },
                "",
                id="succeed_task",
            ),
            pytest.param(
                GetPrevSuccessfulDagRun(ti_id=TI_ID),
                (
                    b'{"data_interval_start":"2025-01-10T12:00:00Z","data_interval_end":"2025-01-10T14:00:00Z",'
                    b'"start_date":"2025-01-10T12:00:00Z","end_date":"2025-01-10T14:00:00Z",'
                    b'"type":"PrevSuccessfulDagRunResult"}\n'
                ),
                "task_instances.get_previous_successful_dagrun",
                (TI_ID,),
                {},
                PrevSuccessfulDagRunResult(
                    start_date=timezone.parse("2025-01-10T12:00:00Z"),
                    end_date=timezone.parse("2025-01-10T14:00:00Z"),
                    data_interval_start=timezone.parse("2025-01-10T12:00:00Z"),
                    data_interval_end=timezone.parse("2025-01-10T14:00:00Z"),
                ),
                id="get_prev_successful_dagrun",
            ),
            pytest.param(
                TriggerDagRun(
                    dag_id="test_dag",
                    run_id="test_run",
                    conf={"key": "value"},
                    logical_date=timezone.datetime(2025, 1, 1),
                    reset_dag_run=True,
                ),
                b'{"ok":true,"type":"OKResponse"}\n',
                "dag_runs.trigger",
                ("test_dag", "test_run", {"key": "value"}, timezone.datetime(2025, 1, 1), True),
                {},
                OKResponse(ok=True),
                id="dag_run_trigger",
            ),
            pytest.param(
                TriggerDagRun(dag_id="test_dag", run_id="test_run"),
                b'{"error":"DAGRUN_ALREADY_EXISTS","detail":null,"type":"ErrorResponse"}\n',
                "dag_runs.trigger",
                ("test_dag", "test_run", None, None, False),
                {},
                ErrorResponse(error=ErrorType.DAGRUN_ALREADY_EXISTS),
                id="dag_run_trigger_already_exists",
            ),
            pytest.param(
                GetDagRunState(dag_id="test_dag", run_id="test_run"),
                b'{"state":"running","type":"DagRunStateResult"}\n',
                "dag_runs.get_state",
                ("test_dag", "test_run"),
                {},
                DagRunStateResult(state=DagRunState.RUNNING),
                id="get_dag_run_state",
            ),
            pytest.param(
                GetTaskRescheduleStartDate(ti_id=TI_ID),
                b'{"start_date":"2024-10-31T12:00:00Z","type":"TaskRescheduleStartDate"}\n',
                "task_instances.get_reschedule_start_date",
                (TI_ID, 1),
                {},
                TaskRescheduleStartDate(start_date=timezone.parse("2024-10-31T12:00:00Z")),
                id="get_task_reschedule_start_date",
            ),
            pytest.param(
                GetTICount(dag_id="test_dag", task_ids=["task1", "task2"]),
                b'{"count":2,"type":"TICount"}\n',
                "task_instances.get_count",
                (),
                {
                    "dag_id": "test_dag",
                    "logical_dates": None,
                    "run_ids": None,
                    "states": None,
                    "task_group_id": None,
                    "task_ids": ["task1", "task2"],
                },
                TICount(count=2),
                id="get_ti_count",
            ),
            pytest.param(
                GetDRCount(dag_id="test_dag", states=["success", "failed"]),
                b'{"count":2,"type":"DRCount"}\n',
                "dag_runs.get_count",
                (),
                {
                    "dag_id": "test_dag",
                    "logical_dates": None,
                    "run_ids": None,
                    "states": ["success", "failed"],
                },
                DRCount(count=2),
                id="get_dr_count",
            ),
            pytest.param(
                GetTaskStates(dag_id="test_dag", task_group_id="test_group"),
                b'{"task_states":{"run_id":{"task1":"success","task2":"failed"}},"type":"TaskStatesResult"}\n',
                "task_instances.get_task_states",
                (),
                {
                    "dag_id": "test_dag",
                    "task_ids": None,
                    "logical_dates": None,
                    "run_ids": None,
                    "task_group_id": "test_group",
                },
                TaskStatesResult(task_states={"run_id": {"task1": "success", "task2": "failed"}}),
                id="get_task_states",
            ),
        ],
    )
    def test_handle_requests(
        self,
        mock_secrets_masker,
        watched_subprocess,
        mocker,
        time_machine,
        message,
        expected_buffer,
        client_attr_path,
        method_arg,
        method_kwarg,
        mock_response,
    ):
        """
        Test handling of different messages to the subprocess. For any new message type, add a
        new parameter set to the `@pytest.mark.parametrize` decorator.

        For each message type, this test:

            1. Sends the message to the subprocess.
            2. Verifies that the correct client method is called with the expected argument.
            3. Checks that the buffer is updated with the expected response.
            4. Verifies that the response is correctly decoded.
        """
        mock_secrets_masker.return_value = SecretsMasker()
        watched_subprocess, read_socket = watched_subprocess

        # Mock the client method. E.g. `client.variables.get` or `client.connections.get`
        mock_client_method = attrgetter(client_attr_path)(watched_subprocess.client)
        mock_client_method.return_value = mock_response

        # Simulate the generator
        generator = watched_subprocess.handle_requests(log=mocker.Mock())
        # Initialize the generator
        next(generator)
        msg = message.model_dump_json().encode() + b"\n"
        generator.send(msg)
        time_machine.move_to(timezone.datetime(2024, 10, 31), tick=False)

        # Verify the correct client method was called
        if client_attr_path:
            mock_client_method.assert_called_once_with(*method_arg, **method_kwarg)

        # Read response from the read end of the socket
        read_socket.settimeout(0.1)
        val = b""
        try:
            while not val.endswith(b"\n"):
                chunk = read_socket.recv(BUFFER_SIZE)
                if not chunk:
                    break
                val += chunk
        except (BlockingIOError, TimeoutError, socket.timeout):
            # no response written, valid for some message types like setters and TI operations.
            pass

        # Verify the response was added to the buffer
        assert val == expected_buffer

        # Verify the response is correctly decoded
        # This is important because the subprocess/task runner will read the response
        # and deserialize it to the correct message type

        # Only decode the buffer if it contains data. An empty buffer implies no response was written.
        if not val and (mock_response and not isinstance(mock_response, OKResponse)):
            pytest.fail("Expected a response, but got an empty buffer.")

        if val:
            # Using BytesIO to simulate a readable stream for CommsDecoder.
            input_stream = BytesIO(val)
            decoder = CommsDecoder(input=input_stream)
            assert decoder.get_message() == mock_response

    def test_handle_requests_api_server_error(self, watched_subprocess, mocker):
        """Test that API server errors are properly handled and sent back to the task."""

        # Unpack subprocess and the reader socket
        watched_subprocess, read_socket = watched_subprocess

        error = ServerResponseError(
            message="API Server Error",
            request=httpx.Request("GET", "http://test"),
            response=httpx.Response(500, json={"detail": "Internal Server Error"}),
        )

        mock_client_method = mocker.Mock(side_effect=error)
        watched_subprocess.client.task_instances.succeed = mock_client_method

        # Initialize and send message
        generator = watched_subprocess.handle_requests(log=mocker.Mock())

        next(generator)

        msg = SucceedTask(end_date=timezone.parse("2024-10-31T12:00:00Z")).model_dump_json().encode() + b"\n"
        generator.send(msg)

        # Read response directly from the reader socket
        read_socket.settimeout(0.1)
        val = b""
        try:
            while not val.endswith(b"\n"):
                val += read_socket.recv(4096)
        except (BlockingIOError, TimeoutError):
            pass

        assert val == (
            b'{"error":"API_SERVER_ERROR","detail":{"status_code":500,"message":"API Server Error",'
            b'"detail":{"detail":"Internal Server Error"}},"type":"ErrorResponse"}\n'
        )

        # Verify the error can be decoded correctly
        input_stream = BytesIO(val)
        decoder = CommsDecoder(input=input_stream)
        with pytest.raises(AirflowRuntimeError) as exc_info:
            decoder.get_message()

        assert exc_info.value.error.error == ErrorType.API_SERVER_ERROR
        assert exc_info.value.error.detail == {
            "status_code": error.response.status_code,
            "message": str(error),
            "detail": error.response.json(),
        }
