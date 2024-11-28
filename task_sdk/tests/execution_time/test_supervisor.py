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
import structlog
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
        # Ignore anything lower than INFO for this test. Captured_logs resets things for us afterwards
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))

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
        # Ignore anything lower than INFO for this test. Captured_logs resets things for us afterwards
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))

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

        monkeypatch.setattr(airflow.sdk.execution_time.supervisor, "FASTEST_HEARTBEAT_INTERVAL", 0.1)

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

        # Ignore anything lower than INFO for this test.
        structlog.configure(wrapper_class=structlog.make_filtering_bound_logger(logging.INFO))

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
