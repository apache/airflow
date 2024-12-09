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
import logging
import os
import selectors
import signal
import sys
from io import BytesIO
from operator import attrgetter
from time import sleep
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import httpx
import psutil
import pytest
from pytest_unordered import unordered
from uuid6 import uuid7

from airflow.sdk.api import client as sdk_client
from airflow.sdk.api.client import ServerResponseError
from airflow.sdk.api.datamodels._generated import TaskInstance, TerminalTIState
from airflow.sdk.execution_time.comms import (
    DeferTask,
    TaskState,
)
from airflow.sdk.execution_time.supervisor import WatchedSubprocess, supervise
from airflow.utils import timezone, timezone as tz

from task_sdk.tests.api.test_client import make_client

if TYPE_CHECKING:
    import kgb

TI_ID = uuid7()


def lineno():
    """Returns the current line number in our program."""
    return inspect.currentframe().f_back.f_lineno


@pytest.mark.usefixtures("disable_capturing")
class TestWatchedSubprocess:
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

        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti=TaskInstance(
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

        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti=TaskInstance(
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

        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti=TaskInstance(
                id=uuid7(),
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
            ),
            client=MagicMock(spec=sdk_client.Client),
            target=subprocess_main,
        )

        rc = proc.wait()

        assert rc == 126

        captured = capfd.readouterr()
        assert "Last chance exception handler" in captured.err
        assert "RuntimeError: Fake syntax error" in captured.err

    def test_regular_heartbeat(self, spy_agency: kgb.SpyAgency, monkeypatch):
        """Test that the WatchedSubprocess class regularly sends heartbeat requests, up to a certain frequency"""
        import airflow.sdk.execution_time.supervisor

        monkeypatch.setattr(airflow.sdk.execution_time.supervisor, "MIN_HEARTBEAT_INTERVAL", 0.1)

        def subprocess_main():
            sys.stdin.readline()

            for _ in range(5):
                print("output", flush=True)
                sleep(0.05)

        ti_id = uuid7()
        spy = spy_agency.spy_on(sdk_client.TaskInstanceOperations.heartbeat)
        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti=TaskInstance(
                id=ti_id,
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
            ),
            client=sdk_client.Client(base_url="", dry_run=True, token=""),
            target=subprocess_main,
        )
        assert proc.wait() == 0
        assert spy.called_with(ti_id, pid=proc.pid)  # noqa: PGH005
        # The exact number we get will depend on timing behaviour, so be a little lenient
        assert 1 <= len(spy.calls) <= 4

    def test_run_simple_dag(self, test_dags_dir, captured_logs, time_machine):
        """Test running a simple DAG in a subprocess and capturing the output."""

        instant = tz.datetime(2024, 11, 7, 12, 34, 56, 78901)
        time_machine.move_to(instant, tick=False)

        dagfile_path = test_dags_dir / "super_basic_run.py"
        ti = TaskInstance(
            id=uuid7(),
            task_id="hello",
            dag_id="super_basic_run",
            run_id="c",
            try_number=1,
        )
        # Assert Exit Code is 0
        assert supervise(ti=ti, dag_path=dagfile_path, token="", server="", dry_run=True) == 0

        # We should have a log from the task!
        assert {
            "chan": "stdout",
            "event": "Hello World hello!",
            "level": "info",
            "logger": "task",
            "timestamp": "2024-11-07T12:34:56.078901Z",
        } in captured_logs

    def test_supervise_handles_deferred_task(self, test_dags_dir, captured_logs, time_machine, mocker):
        """
        Test that the supervisor handles a deferred task correctly.

        This includes ensuring the task starts and executes successfully, and that the task is deferred (via
        the API client) with the expected parameters.
        """

        ti = TaskInstance(
            id=uuid7(), task_id="async", dag_id="super_basic_deferred_run", run_id="d", try_number=1
        )
        dagfile_path = test_dags_dir / "super_basic_deferred_run.py"

        # Create a mock client to assert calls to the client
        # We assume the implementation of the client is correct and only need to check the calls
        mock_client = mocker.Mock(spec=sdk_client.Client)

        instant = tz.datetime(2024, 11, 7, 12, 34, 56, 0)
        time_machine.move_to(instant, tick=False)

        # Assert supervisor runs the task successfully
        assert supervise(ti=ti, dag_path=dagfile_path, token="", client=mock_client) == 0

        # Validate calls to the client
        mock_client.task_instances.start.assert_called_once_with(ti.id, mocker.ANY, mocker.ANY)
        mock_client.task_instances.heartbeat.assert_called_once_with(ti.id, pid=mocker.ANY)
        mock_client.task_instances.defer.assert_called_once_with(
            ti.id,
            DeferTask(
                classpath="airflow.providers.standard.triggers.temporal.DateTimeTrigger",
                trigger_kwargs={"moment": "2024-11-07T12:34:59Z", "end_from_trigger": False},
                next_method="execute_complete",
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
            if request.url.path == f"/task-instances/{ti.id}/state":
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
            WatchedSubprocess.start(path=os.devnull, ti=ti, client=client)

        assert err.value.response.status_code == 409
        assert err.value.detail == {
            "reason": "invalid_state",
            "message": "TI was not in a state where it could be marked as running",
            "previous_state": "running",
        }

    @pytest.mark.parametrize("captured_logs", [logging.ERROR], indirect=True, ids=["log_level=error"])
    def test_state_conflict_on_heartbeat(self, captured_logs, monkeypatch, mocker):
        """
        Test that ensures that the Supervisor does not cause the task to fail if the Task Instance is no longer
        in the running state. Instead, it logs the error and terminates the task process if it
        might be running in a different state or has already completed -- or running on a different worker.
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
                else:
                    # Second request returns a conflict status code
                    return httpx.Response(
                        409,
                        json={
                            "reason": "not_running",
                            "message": "TI is no longer in the running state and task should terminate",
                            "current_state": "success",
                        },
                    )
            # Return a 204 for all other requests like the initial call to mark the task as running
            return httpx.Response(status_code=204)

        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti=TaskInstance(id=ti_id, task_id="b", dag_id="c", run_id="d", try_number=1),
            client=make_client(transport=httpx.MockTransport(handle_request)),
            target=subprocess_main,
        )

        # Wait for the subprocess to finish -- it should have been terminated
        assert proc.wait() == -signal.SIGTERM

        assert request_count["count"] == 2
        # Verify the number of requests made
        assert captured_logs == [
            {
                "detail": {
                    "reason": "not_running",
                    "message": "TI is no longer in the running state and task should terminate",
                    "current_state": "success",
                },
                "event": "Server indicated the task shouldn't be running anymore",
                "level": "error",
                "status_code": 409,
                "logger": "supervisor",
                "timestamp": mocker.ANY,
            }
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

        proc = WatchedSubprocess(
            ti_id=TI_ID,
            pid=mock_process.pid,
            stdin=mocker.MagicMock(),
            client=client,
            process=mock_process,
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
        monkeypatch.setattr(WatchedSubprocess, "TASK_OVERTIME_THRESHOLD", overtime_threshold)

        mock_watched_subprocess = WatchedSubprocess(
            ti_id=TI_ID,
            pid=12345,
            stdin=mocker.Mock(),
            process=mocker.Mock(),
            client=mocker.Mock(),
        )

        # Set the terminal state and task end datetime
        mock_watched_subprocess._terminal_state = terminal_state
        mock_watched_subprocess._task_end_time_monotonic = task_end_time_monotonic

        # Call `wait` to trigger the overtime handling
        # This will call the `kill` method if the task has been running for too long
        mock_watched_subprocess._handle_task_overtime_if_needed()

        # Validate process kill behavior and log messages
        if expected_kill:
            mock_kill.assert_called_once_with(signal.SIGTERM, force=True)
            mock_logger.warning.assert_called_once_with(
                "Task success overtime reached; terminating process",
                ti_id=TI_ID,
            )
        else:
            mock_kill.assert_not_called()
            mock_logger.warning.assert_not_called()


class TestWatchedSubprocessKill:
    @pytest.fixture
    def mock_process(self, mocker):
        process = mocker.Mock(spec=psutil.Process)
        process.pid = 12345
        return process

    @pytest.fixture
    def watched_subprocess(self, mocker, mock_process):
        proc = WatchedSubprocess(
            ti_id=TI_ID,
            pid=12345,
            stdin=mocker.Mock(),
            client=mocker.Mock(),
            process=mock_process,
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
                    exit(sig)
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

        proc = WatchedSubprocess.start(
            path=os.devnull,
            ti=TaskInstance(id=ti_id, task_id="b", dag_id="c", run_id="d", try_number=1),
            client=MagicMock(spec=sdk_client.Client),
            target=subprocess_main,
        )
        # Ensure we get one normal run, to give the proc time to register it's custom sighandler
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
            # expected_logs.push({"chan": "stderr", "event": "Signal 9 received", "logger": "task"})
            ...

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
        """Fixture to provide a WatchedSubprocess instance."""
        return WatchedSubprocess(
            ti_id=TI_ID,
            pid=12345,
            stdin=BytesIO(),
            client=mocker.Mock(),
            process=mocker.Mock(),
        )

    @pytest.mark.parametrize(
        ["message", "expected_buffer", "client_attr_path", "method_arg", "mock_response"],
        [
            # pytest.param(
            #     GetConnection(conn_id="test_conn"),
            #     b'{"conn_id":"test_conn","conn_type":"mysql"}\n',
            #     "connections.get",
            #     ("test_conn",),
            #     ConnectionResult(conn_id="test_conn", conn_type="mysql"),
            #     id="get_connection",
            # ),
            # pytest.param(
            #     GetVariable(key="test_key"),
            #     b'{"key":"test_key","value":"test_value"}\n',
            #     "variables.get",
            #     ("test_key",),
            #     VariableResult(key="test_key", value="test_value"),
            #     id="get_variable",
            # ),
            # pytest.param(
            #     PutVariable(key="test_key", value="test_value", description="test_description"),
            #     b"",
            #     "variables.set",
            #     ("test_key", "test_value", "test_description"),
            #     {"ok": True},
            #     id="set_variable",
            # ),
            # pytest.param(
            #     DeferTask(next_method="execute_callback", classpath="my-classpath"),
            #     b"",
            #     "task_instances.defer",
            #     (TI_ID, DeferTask(next_method="execute_callback", classpath="my-classpath")),
            #     "",
            #     id="patch_task_instance_to_deferred",
            # ),
            # pytest.param(
            #     GetXCom(dag_id="test_dag", run_id="test_run", task_id="test_task", key="test_key"),
            #     b'{"key":"test_key","value":"test_value"}\n',
            #     "xcoms.get",
            #     ("test_dag", "test_run", "test_task", "test_key", -1),
            #     XComResult(key="test_key", value="test_value"),
            #     id="get_xcom",
            # ),
            # pytest.param(
            #     GetXCom(
            #         dag_id="test_dag", run_id="test_run", task_id="test_task", key="test_key", map_index=2
            #     ),
            #     b'{"key":"test_key","value":"test_value"}\n',
            #     "xcoms.get",
            #     ("test_dag", "test_run", "test_task", "test_key", 2),
            #     XComResult(key="test_key", value="test_value"),
            #     id="get_xcom_map_index",
            # ),
            # pytest.param(
            #     SetXCom(
            #         dag_id="test_dag",
            #         run_id="test_run",
            #         task_id="test_task",
            #         key="test_key",
            #         value='{"key": "test_key", "value": {"key2": "value2"}}',
            #     ),
            #     b"",
            #     "xcoms.set",
            #     (
            #         "test_dag",
            #         "test_run",
            #         "test_task",
            #         "test_key",
            #         '{"key": "test_key", "value": {"key2": "value2"}}',
            #         None,
            #     ),
            #     {"ok": True},
            #     id="set_xcom",
            # ),
            # pytest.param(
            #     SetXCom(
            #         dag_id="test_dag",
            #         run_id="test_run",
            #         task_id="test_task",
            #         key="test_key",
            #         value='{"key": "test_key", "value": {"key2": "value2"}}',
            #         map_index=2,
            #     ),
            #     b"",
            #     "xcoms.set",
            #     (
            #         "test_dag",
            #         "test_run",
            #         "test_task",
            #         "test_key",
            #         '{"key": "test_key", "value": {"key2": "value2"}}',
            #         2,
            #     ),
            #     {"ok": True},
            #     id="set_xcom_with_map_index",
            # ),
            pytest.param(
                TaskState(state="skipped", end_date=timezone.parse("2024-10-31T12:00:00Z")),
                b"",
                "",
                (),
                "",
                id="patch_task_instance_to_skipped",
            ),
        ],
    )
    def test_handle_requests(
        self,
        watched_subprocess,
        mocker,
        message,
        expected_buffer,
        client_attr_path,
        method_arg,
        mock_response,
    ):
        """
        Test handling of different messages to the subprocess. For any new message type, add a
        new parameter set to the `@pytest.mark.parametrize` decorator.

        For each message type, this test:

            1. Sends the message to the subprocess.
            2. Verifies that the correct client method is called with the expected argument.
            3. Checks that the buffer is updated with the expected response.
        """

        # Mock the client method. E.g. `client.variables.get` or `client.connections.get`
        mock_client_method = attrgetter(client_attr_path)(watched_subprocess.client)
        mock_client_method.return_value = mock_response

        # Simulate the generator
        generator = watched_subprocess.handle_requests(log=mocker.Mock())
        # Initialize the generator
        next(generator)
        msg = message.model_dump_json().encode() + b"\n"
        generator.send(msg)

        # Verify the correct client method was called
        if client_attr_path:
            mock_client_method.assert_called_once_with(*method_arg)

        # Verify the response was added to the buffer
        assert watched_subprocess.stdin.getvalue() == expected_buffer
