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
import signal
import sys
from io import BytesIO
from operator import attrgetter
from time import sleep
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

import httpx
import pytest
from uuid6 import uuid7

from airflow.sdk.api import client as sdk_client
from airflow.sdk.api.client import ServerResponseError
from airflow.sdk.api.datamodels._generated import TaskInstance
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    DeferTask,
    GetConnection,
    GetVariable,
    GetXCom,
    VariableResult,
    XComResult,
)
from airflow.sdk.execution_time.supervisor import WatchedSubprocess, supervise
from airflow.utils import timezone as tz

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
        assert captured_logs == [
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

        monkeypatch.setattr(airflow.sdk.execution_time.supervisor, "MIN_HEARTBEAT_INTERVAL", 0.1)

        def subprocess_main():
            sys.stdin.readline()
            sleep(5)

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

        # Verify the number of requests made
        assert request_count["count"] == 2
        assert captured_logs == [
            {
                "detail": {
                    "current_state": "success",
                    "message": "TI is no longer in the running state and task should terminate",
                    "reason": "not_running",
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
        mock_kill.assert_called_once_with(signal.SIGTERM)
        mock_client_heartbeat.assert_called_with(TI_ID, pid=mock_process.pid)
        assert {
            "event": "Too many failed heartbeats; terminating process",
            "level": "error",
            "failed_heartbeats": max_failed_heartbeats,
            "logger": "supervisor",
            "timestamp": mocker.ANY,
        } in captured_logs


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
            pytest.param(
                GetConnection(conn_id="test_conn"),
                b'{"conn_id":"test_conn","conn_type":"mysql"}\n',
                "connections.get",
                ("test_conn",),
                ConnectionResult(conn_id="test_conn", conn_type="mysql"),
                id="get_connection",
            ),
            pytest.param(
                GetVariable(key="test_key"),
                b'{"key":"test_key","value":"test_value"}\n',
                "variables.get",
                ("test_key",),
                VariableResult(key="test_key", value="test_value"),
                id="get_variable",
            ),
            pytest.param(
                GetXCom(dag_id="test_dag", run_id="test_run", task_id="test_task", key="test_key"),
                b'{"key":"test_key","value":"test_value"}\n',
                "xcoms.get",
                ("test_dag", "test_run", "test_task", "test_key", -1),
                XComResult(key="test_key", value="test_value"),
                id="get_xcom",
            ),
            pytest.param(
                DeferTask(next_method="execute_callback", classpath="my-classpath"),
                b"",
                "task_instances.defer",
                (TI_ID, DeferTask(next_method="execute_callback", classpath="my-classpath")),
                "",
                id="patch_task_instance_to_deferred",
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
        mock_client_method.assert_called_once_with(*method_arg)

        # Verify the response was added to the buffer
        assert watched_subprocess.stdin.getvalue() == expected_buffer
