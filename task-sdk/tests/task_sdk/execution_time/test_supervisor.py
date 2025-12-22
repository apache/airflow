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
import subprocess
import sys
import time
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import datetime
from operator import attrgetter
from random import randint
from textwrap import dedent
from time import sleep
from typing import TYPE_CHECKING, Any
from unittest import mock
from unittest.mock import MagicMock, patch

import httpx
import msgspec
import psutil
import pytest
import structlog
from pytest_unordered import unordered
from task_sdk import FAKE_BUNDLE, make_client
from uuid6 import uuid7

from airflow.executors.workloads import BundleInfo
from airflow.sdk import BaseOperator, timezone
from airflow.sdk.api import client as sdk_client
from airflow.sdk.api.client import ServerResponseError
from airflow.sdk.api.datamodels._generated import (
    AssetEventResponse,
    AssetProfile,
    AssetResponse,
    DagRun,
    DagRunState,
    DagRunType,
    PreviousTIResponse,
    TaskInstance,
    TaskInstanceState,
)
from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
from airflow.sdk.execution_time import task_runner
from airflow.sdk.execution_time.comms import (
    AssetEventsResult,
    AssetResult,
    CommsDecoder,
    ConnectionResult,
    CreateHITLDetailPayload,
    DagRunResult,
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
    GetDagRun,
    GetDagRunState,
    GetDRCount,
    GetHITLDetailResponse,
    GetPreviousDagRun,
    GetPreviousTI,
    GetPrevSuccessfulDagRun,
    GetTaskBreadcrumbs,
    GetTaskRescheduleStartDate,
    GetTaskStates,
    GetTICount,
    GetVariable,
    GetXCom,
    GetXComCount,
    GetXComSequenceItem,
    GetXComSequenceSlice,
    HITLDetailRequestResult,
    InactiveAssetsResult,
    MaskSecret,
    OKResponse,
    PreviousDagRunResult,
    PreviousTIResult,
    PrevSuccessfulDagRunResult,
    PutVariable,
    RescheduleTask,
    ResendLoggingFD,
    RetryTask,
    SentFDs,
    SetRenderedFields,
    SetRenderedMapIndex,
    SetXCom,
    SkipDownstreamTasks,
    SucceedTask,
    TaskBreadcrumbsResult,
    TaskRescheduleStartDate,
    TaskState,
    TaskStatesResult,
    TICount,
    ToSupervisor,
    TriggerDagRun,
    UpdateHITLDetail,
    ValidateInletsAndOutlets,
    VariableResult,
    XComCountResponse,
    XComResult,
    XComSequenceIndexResult,
    XComSequenceSliceResult,
    _RequestFrame,
    _ResponseFrame,
)
from airflow.sdk.execution_time.supervisor import (
    ActivitySubprocess,
    InProcessSupervisorComms,
    InProcessTestSupervisor,
    _remote_logging_conn,
    process_log_messages_from_subprocess,
    set_supervisor_comms,
    supervise,
)
from airflow.sdk.execution_time.task_runner import run

from tests_common.test_utils.config import conf_vars

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


@pytest.fixture
def client_with_ti_start(make_ti_context):
    client = MagicMock(spec=sdk_client.Client)
    client.task_instances.start.return_value = make_ti_context()
    return client


@pytest.mark.usefixtures("disable_capturing")
class TestSupervisor:
    @pytest.mark.parametrize(
        ("server", "dry_run", "expectation"),
        [
            ("/execution/", False, pytest.raises(ValueError, match="Invalid execution API server URL")),
            ("", False, pytest.raises(ValueError, match="Invalid execution API server URL")),
            ("http://localhost:8080", True, pytest.raises(ValueError, match="Can only specify one of")),
            (None, True, nullcontext()),
            ("http://localhost:8080/execution/", False, nullcontext()),
            ("https://localhost:8080/execution/", False, nullcontext()),
        ],
    )
    def test_supervise(
        self,
        server,
        dry_run,
        expectation,
        test_dags_dir,
        client_with_ti_start,
    ):
        """
        Test that the supervisor validates server URL and dry_run parameter combinations correctly.
        """
        ti = TaskInstance(
            id=uuid7(),
            task_id="async",
            dag_id="super_basic_deferred_run",
            run_id="d",
            try_number=1,
            dag_version_id=uuid7(),
        )

        bundle_info = BundleInfo(name="my-bundle", version=None)

        kw = {
            "ti": ti,
            "dag_rel_path": "super_basic_deferred_run.py",
            "token": "",
            "bundle_info": bundle_info,
            "dry_run": dry_run,
            "server": server,
        }
        if isinstance(expectation, nullcontext):
            kw["client"] = client_with_ti_start

        with patch.dict(os.environ, local_dag_bundle_cfg(test_dags_dir, bundle_info.name)):
            with expectation:
                supervise(**kw)


@pytest.mark.usefixtures("disable_capturing")
class TestWatchedSubprocess:
    @pytest.fixture(autouse=True)
    def disable_log_upload(self, spy_agency):
        spy_agency.spy_on(ActivitySubprocess._upload_logs, call_original=False)

    @pytest.fixture(autouse=True)
    def use_real_secrets_backends(self, monkeypatch):
        """
        Ensure that real secrets backend instances are used instead of mocks.

        This prevents Python 3.13 RuntimeWarning when hasattr checks async methods
        on mocked backends. The warning occurs because hasattr on AsyncMock creates
        unawaited coroutines.

        This fixture ensures test isolation when running in parallel with pytest-xdist,
        regardless of what other tests patch.
        """
        from airflow.sdk.execution_time.secrets import ExecutionAPISecretsBackend
        from airflow.secrets.environment_variables import EnvironmentVariablesBackend

        monkeypatch.setattr(
            "airflow.sdk.execution_time.supervisor.ensure_secrets_backend_loaded",
            lambda: [EnvironmentVariablesBackend(), ExecutionAPISecretsBackend()],
        )

    def test_reading_from_pipes(self, captured_logs, time_machine, client_with_ti_start):
        def subprocess_main():
            # This is run in the subprocess!

            # Ensure we follow the "protocol" and get the startup message before we do anything else
            CommsDecoder()._get_response()

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

            warnings.warn("Warning should be appear from the correct callsite", stacklevel=1)

        line = lineno() - 2  # Line the error should be on

        instant = timezone.datetime(2024, 11, 7, 12, 34, 56, 78901)
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
                dag_version_id=uuid7(),
            ),
            client=client_with_ti_start,
            target=subprocess_main,
        )

        rc = proc.wait()

        assert rc == 0
        assert captured_logs == unordered(
            [
                {
                    "logger": "task.stdout",
                    "event": "I'm a short message",
                    "level": "info",
                    "timestamp": "2024-11-07T12:34:56.078901Z",
                },
                {
                    "logger": "task.stderr",
                    "event": "stderr message",
                    "level": "error",
                    "timestamp": "2024-11-07T12:34:56.078901Z",
                },
                {
                    "logger": "task.stdout",
                    "event": "Message split across two writes",
                    "level": "info",
                    "timestamp": "2024-11-07T12:34:56.078901Z",
                },
                {
                    "event": "An error message",
                    "level": "error",
                    "logger": "airflow.foobar",
                    "timestamp": instant,
                    "loc": mock.ANY,
                },
                {
                    "category": "UserWarning",
                    "event": "Warning should be appear from the correct callsite",
                    "filename": __file__,
                    "level": "warning",
                    "lineno": line,
                    "logger": "py.warnings",
                    "timestamp": instant,
                },
            ]
        )

    def test_reopen_log_fd(self, captured_logs, client_with_ti_start):
        def subprocess_main():
            # This is run in the subprocess!

            # Ensure we follow the "protocol" and get the startup message before we do anything else
            comms = CommsDecoder()
            comms._get_response()

            logs = comms.send(ResendLoggingFD())
            assert isinstance(logs, SentFDs)
            fd = os.fdopen(logs.fds[0], "w")
            logging.root.info("Log on old socket")
            json.dump({"level": "info", "event": "Log on new socket"}, fp=fd)

        line = lineno() - 3  # Line the error should be on

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id="4d828a62-a417-4936-a7a6-2b3fabacecab",
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
                dag_version_id=uuid7(),
            ),
            client=client_with_ti_start,
            target=subprocess_main,
        )

        rc = proc.wait()

        assert rc == 0
        assert captured_logs == unordered(
            [
                {
                    "event": "Log on new socket",
                    "level": "info",
                    "logger": "task",
                    "timestamp": mock.ANY,
                    # Since this is set as json, without filename or linno, we _should_ not add any.
                },
                {
                    "event": "Log on old socket",
                    "level": "info",
                    "logger": "root",
                    "timestamp": mock.ANY,
                    "loc": f"{os.path.basename(__file__)}:{line}",
                },
            ]
        )

    def test_on_kill_hook_called_when_sigkilled(
        self,
        client_with_ti_start,
        mocked_parse,
        make_ti_context,
        mock_supervisor_comms,
        create_runtime_ti,
        make_ti_context_dict,
        capfd,
    ):
        main_pid = os.getpid()
        ti_id = "4d828a62-a417-4936-a7a6-2b3fabacecab"

        def handle_request(request: httpx.Request) -> httpx.Response:
            if request.url.path == f"/task-instances/{ti_id}/heartbeat":
                return httpx.Response(
                    status_code=409,
                    json={
                        "detail": {
                            "reason": "not_running",
                            "message": "TI is no longer in the 'running' state. Task state might be externally set and task should terminate",
                            "current_state": "failed",
                        }
                    },
                )
            if request.url.path == f"/task-instances/{ti_id}/run":
                return httpx.Response(200, json=make_ti_context_dict())
            return httpx.Response(status_code=204)

        def subprocess_main():
            # Ensure we follow the "protocol" and get the startup message before we do anything
            CommsDecoder()._get_response()

            class CustomOperator(BaseOperator):
                def execute(self, context):
                    for i in range(1000):
                        print(f"Iteration {i}")
                        sleep(1)

                def on_kill(self) -> None:
                    print("On kill hook called!")

            task = CustomOperator(task_id="print-params")
            runtime_ti = create_runtime_ti(
                dag_id="c",
                task=task,
                conf={
                    "x": 3,
                    "text": "Hello World!",
                    "flag": False,
                    "a_simple_list": ["one", "two", "three", "actually one value is made per line"],
                },
            )
            run(runtime_ti, context=runtime_ti.get_template_context(), log=mock.MagicMock())

            assert os.getpid() != main_pid
            os.kill(os.getpid(), signal.SIGTERM)
            # Ensure that the signal is serviced before we finish and exit the subprocess.
            sleep(0.5)

        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id=ti_id,
                task_id="b",
                dag_id="c",
                run_id="d",
                try_number=1,
                dag_version_id=uuid7(),
            ),
            client=make_client(transport=httpx.MockTransport(handle_request)),
            target=subprocess_main,
        )

        proc.wait()
        captured = capfd.readouterr()
        assert "On kill hook called!" in captured.out

    def test_subprocess_sigkilled(self, client_with_ti_start):
        main_pid = os.getpid()

        def subprocess_main():
            # Ensure we follow the "protocol" and get the startup message before we do anything
            CommsDecoder()._get_response()

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
                dag_version_id=uuid7(),
            ),
            client=client_with_ti_start,
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
            what=TaskInstance(
                id=uuid7(), task_id="b", dag_id="c", run_id="d", try_number=1, dag_version_id=uuid7()
            ),
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
            CommsDecoder()._get_response()

            for _ in range(5):
                print("output", flush=True)
                sleep(0.05)

        ti_id = uuid7()
        _ = mocker.patch.object(sdk_client.TaskInstanceOperations, "start", return_value=make_ti_context())

        spy = spy_agency.spy_on(sdk_client.TaskInstanceOperations.heartbeat)
        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id=ti_id, task_id="b", dag_id="c", run_id="d", try_number=1, dag_version_id=uuid7()
            ),
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
            CommsDecoder()._get_response()

            for _ in range(5):
                print("output", flush=True)
                sleep(0.05)

        ti_id = uuid7()
        _ = mocker.patch.object(sdk_client.TaskInstanceOperations, "start", return_value=make_ti_context())

        @spy_agency.spy_for(ActivitySubprocess._on_child_started)
        def _on_child_started(self, *args, **kwargs):
            # Set it up so we are in overtime straight away
            self._terminal_state = TaskInstanceState.SUCCESS
            ActivitySubprocess._on_child_started.call_original(self, *args, **kwargs)

        heartbeat_spy = spy_agency.spy_on(sdk_client.TaskInstanceOperations.heartbeat)
        proc = ActivitySubprocess.start(
            dag_rel_path=os.devnull,
            bundle_info=FAKE_BUNDLE,
            what=TaskInstance(
                id=ti_id, task_id="b", dag_id="c", run_id="d", try_number=1, dag_version_id=uuid7()
            ),
            client=sdk_client.Client(base_url="", dry_run=True, token=""),
            target=subprocess_main,
        )
        assert proc.wait() == 0
        spy_agency.assert_spy_not_called(heartbeat_spy)

    def test_run_simple_dag(self, test_dags_dir, captured_logs, time_machine, mocker, client_with_ti_start):
        """Test running a simple DAG in a subprocess and capturing the output."""

        instant = timezone.datetime(2024, 11, 7, 12, 34, 56, 78901)
        time_machine.move_to(instant, tick=False)

        dagfile_path = test_dags_dir
        ti = TaskInstance(
            id=uuid7(),
            task_id="hello",
            dag_id="super_basic_run",
            run_id="c",
            try_number=1,
            dag_version_id=uuid7(),
        )

        bundle_info = BundleInfo(name="my-bundle", version=None)
        with patch.dict(os.environ, local_dag_bundle_cfg(test_dags_dir, bundle_info.name)):
            exit_code = supervise(
                ti=ti,
                dag_rel_path=dagfile_path,
                token="",
                server="",
                dry_run=True,
                client=client_with_ti_start,
                bundle_info=bundle_info,
            )
            assert exit_code == 0, captured_logs

        # We should have a log from the task!
        assert {
            "logger": "task.stdout",
            "event": "Hello World hello!",
            "level": "info",
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
        instant = timezone.datetime(2024, 11, 7, 12, 34, 56, 0)

        ti = TaskInstance(
            id=uuid7(),
            task_id="async",
            dag_id="super_basic_deferred_run",
            run_id="d",
            try_number=1,
            dag_version_id=uuid7(),
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
            "loc": mocker.ANY,
            "task_instance_id": str(ti.id),
        } in captured_logs

    def test_supervisor_handles_already_running_task(self):
        """Test that Supervisor prevents starting a Task Instance that is already running."""
        ti = TaskInstance(
            id=uuid7(), task_id="b", dag_id="c", run_id="d", try_number=1, dag_version_id=uuid7()
        )

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
            CommsDecoder()._get_response()
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
            what=TaskInstance(
                id=ti_id, task_id="b", dag_id="c", run_id="d", try_number=1, dag_version_id=uuid7()
            ),
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
                "loc": mocker.ANY,
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
                "loc": mocker.ANY,
            },
            {
                "event": "Task killed!",
                "level": "error",
                "logger": "task",
                "timestamp": mocker.ANY,
                "loc": mocker.ANY,
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
        )

        time_now = timezone.datetime(2024, 11, 28, 12, 0, 0)
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
                "exc_info": mocker.ANY,
                "loc": mocker.ANY,
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
            "loc": mocker.ANY,
        } in captured_logs

    @pytest.mark.parametrize(
        ("terminal_state", "task_end_time_monotonic", "overtime_threshold", "expected_kill"),
        [
            pytest.param(
                None,
                15.0,
                10,
                False,
                id="no_terminal_state",
            ),
            pytest.param(TaskInstanceState.SUCCESS, 15.0, 10, False, id="below_threshold"),
            pytest.param(TaskInstanceState.SUCCESS, 9.0, 10, True, id="above_threshold"),
            pytest.param(TaskInstanceState.FAILED, 9.0, 10, True, id="above_threshold_failed_state"),
            pytest.param(TaskInstanceState.SKIPPED, 9.0, 10, True, id="above_threshold_skipped_state"),
            pytest.param(TaskInstanceState.SUCCESS, None, 20, False, id="task_end_datetime_none"),
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
        monkeypatch.setattr(
            "airflow.sdk.execution_time.supervisor.TASK_OVERTIME_THRESHOLD", overtime_threshold
        )

        mock_watched_subprocess = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
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
        mock_watched_subprocess._handle_process_overtime_if_needed()

        # Validate process kill behavior and log messages
        if expected_kill:
            mock_kill.assert_called_once_with(signal.SIGTERM, force=True)
            mock_logger.warning.assert_called_once_with(
                "Task success overtime reached; terminating process. "
                "Modify `task_success_overtime` setting in [core] section of "
                "Airflow configuration to change this limit.",
                ti_id=TI_ID,
            )
        else:
            mock_kill.assert_not_called()
            mock_logger.warning.assert_not_called()

    @pytest.mark.parametrize(
        ("signal_to_raise", "log_pattern", "level"),
        (
            pytest.param(
                signal.SIGKILL,
                re.compile(r"Process terminated by signal. Likely out of memory error"),
                "critical",
                id="kill",
            ),
            pytest.param(
                signal.SIGTERM,
                re.compile(r"Process terminated by signal. For more information"),
                "error",
                id="term",
            ),
            pytest.param(
                signal.SIGSEGV,
                re.compile(r".*SIGSEGV \(Segmentation Violation\) signal indicates", re.DOTALL),
                "critical",
                id="segv",
            ),
        ),
    )
    def test_exit_by_signal(self, signal_to_raise, log_pattern, level, cap_structlog, client_with_ti_start):
        def subprocess_main():
            import faulthandler
            import os

            # Disable pytest fault handler
            if faulthandler.is_enabled():
                faulthandler.disable()

            # Ensure we follow the "protocol" and get the startup message before we do anything
            CommsDecoder()._get_response()

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
                dag_version_id=uuid7(),
            ),
            client=client_with_ti_start,
            target=subprocess_main,
        )

        rc = proc.wait()

        assert {
            "log_level": level,
            "event": log_pattern,
        } in cap_structlog
        assert rc == -signal_to_raise

    @pytest.mark.execution_timeout(3)
    def test_cleanup_sockets_after_delay(self, monkeypatch, mocker, time_machine):
        """Supervisor should close sockets if EOF events are missed."""

        monkeypatch.setattr("airflow.sdk.execution_time.supervisor.SOCKET_CLEANUP_TIMEOUT", 1.0)

        mock_process = mocker.Mock(pid=12345)

        time_machine.move_to(time.monotonic(), tick=False)

        proc = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=mock_process.pid,
            stdin=mocker.MagicMock(),
            client=mocker.MagicMock(),
            process=mock_process,
        )

        proc.selector = mocker.MagicMock()
        proc.selector.select.return_value = []

        proc._exit_code = 0
        # Create a fake placeholder in the open socket weakref
        proc._open_sockets[mocker.MagicMock()] = "test placeholder"
        proc._process_exit_monotonic = time.monotonic()

        mocker.patch.object(
            ActivitySubprocess,
            "_cleanup_open_sockets",
            side_effect=lambda: setattr(proc, "_open_sockets", {}),
        )

        time_machine.shift(2)

        proc._monitor_subprocess()
        assert len(proc._open_sockets) == 0


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
        ("signal_to_send", "exit_after"),
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
    def test_kill_escalation_path(self, signal_to_send, exit_after, captured_logs, client_with_ti_start):
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
                CommsDecoder()._get_response()
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
            what=TaskInstance(
                id=ti_id, task_id="b", dag_id="c", run_id="d", try_number=1, dag_version_id=uuid7()
            ),
            client=client_with_ti_start,
            target=subprocess_main,
        )

        # Ensure we get one normal run, to give the proc time to register it's custom sighandler
        time.sleep(0.1)
        proc._service_subprocess(max_wait_time=1)
        proc.kill(signal_to_send=signal_to_send, escalation_delay=0.5, force=True)

        # Wait for the subprocess to finish
        assert proc.wait() == exit_after or -signal.SIGKILL
        exit_after = exit_after or signal.SIGKILL

        logs = [{"event": m["event"], "logger": m["logger"]} for m in captured_logs]
        expected_logs = [
            {"logger": "task.stdout", "event": "Ready"},
        ]
        # Work out what logs we expect to see
        if signal_to_send == signal.SIGINT:
            expected_logs.append({"logger": "task.stderr", "event": "Signal 2 received"})
        if signal_to_send == signal.SIGTERM or (
            signal_to_send == signal.SIGINT and exit_after != signal.SIGINT
        ):
            if signal_to_send == signal.SIGINT:
                expected_logs.append(
                    {
                        "event": "Process did not terminate in time; escalating",
                        "logger": "supervisor",
                    }
                )
            expected_logs.append({"logger": "task.stderr", "event": "Signal 15 received"})
        if exit_after == signal.SIGKILL:
            if signal_to_send in {signal.SIGINT, signal.SIGTERM}:
                expected_logs.append(
                    {
                        "event": "Process did not terminate in time; escalating",
                        "logger": "supervisor",
                    }
                )

        expected_logs.extend(({"event": "Process exited", "logger": "supervisor"},))
        assert logs == expected_logs

    def test_service_subprocess(self, watched_subprocess, mock_process, mocker):
        """Test `_service_subprocess` processes selector events and handles subprocess exit."""
        # Given

        # Mock file objects and handlers
        mock_stdout = mocker.Mock()
        mock_stderr = mocker.Mock()

        # Handlers for stdout and stderr
        mock_stdout_handler = mocker.Mock(return_value=False)  # Simulate EOF for stdout
        mock_stderr_handler = mocker.Mock(return_value=True)  # Continue processing for stderr

        mock_on_close = mocker.Mock()

        # Mock selector to return events
        mock_key_stdout = mocker.Mock(fileobj=mock_stdout, data=(mock_stdout_handler, mock_on_close))
        mock_key_stderr = mocker.Mock(fileobj=mock_stderr, data=(mock_stderr_handler, mock_on_close))
        watched_subprocess.selector.select.return_value = [(mock_key_stdout, None), (mock_key_stderr, None)]

        # Mock to simulate process exited successfully
        mock_process.wait.return_value = 0

        # Our actual test
        watched_subprocess._service_subprocess(max_wait_time=1.0)

        # Validations!
        # Validate selector interactions
        watched_subprocess.selector.select.assert_called_once_with(timeout=1.0)

        # Validate handler calls
        mock_stdout_handler.assert_called_once_with(mock_stdout)
        mock_stderr_handler.assert_called_once_with(mock_stderr)

        # Validate unregistering and closing of EOF file object
        mock_on_close.assert_called_once_with(mock_stdout)

        # Validate that `_check_subprocess_exit` is called
        mock_process.wait.assert_called_once_with(timeout=0)

    def test_max_wait_time_prevents_cpu_spike(self, watched_subprocess, mock_process, monkeypatch):
        """Test that max_wait_time calculation prevents CPU spike when heartbeat timeout is reached."""
        # Mock the configuration to reproduce the CPU spike scenario
        # Set heartbeat timeout to be very small relative to MIN_HEARTBEAT_INTERVAL
        monkeypatch.setattr("airflow.sdk.execution_time.supervisor.HEARTBEAT_TIMEOUT", 1)
        monkeypatch.setattr("airflow.sdk.execution_time.supervisor.MIN_HEARTBEAT_INTERVAL", 10)

        # Set up a scenario where the last successful heartbeat was a long time ago
        # This will cause the heartbeat calculation to result in a negative value
        mock_process._last_successful_heartbeat = time.monotonic() - 100  # 100 seconds ago

        # Mock process to still be alive (not exited)
        mock_process.wait.side_effect = psutil.TimeoutExpired(pid=12345, seconds=0)

        # Call _service_subprocess which is used in _monitor_subprocess
        # This tests the max_wait_time calculation directly
        watched_subprocess._service_subprocess(max_wait_time=0.005)  # Very small timeout to verify our fix

        # Verify that selector.select was called with a minimum timeout of 0.01
        # This proves our fix prevents the timeout=0 scenario that causes CPU spike
        watched_subprocess.selector.select.assert_called_once()
        call_args = watched_subprocess.selector.select.call_args
        timeout_arg = call_args[1]["timeout"] if "timeout" in call_args[1] else call_args[0][0]

        # The timeout should be at least 0.01 (our minimum), never 0
        assert timeout_arg >= 0.01, f"Expected timeout >= 0.01, got {timeout_arg}"

    @pytest.mark.parametrize(
        ("heartbeat_timeout", "min_interval", "heartbeat_ago", "expected_min_timeout"),
        [
            # Normal case: heartbeat is recent, should use calculated value
            pytest.param(30, 5, 5, 0.01, id="normal_heartbeat"),
            # Edge case: heartbeat timeout exceeded, should use minimum
            pytest.param(10, 20, 50, 0.01, id="heartbeat_timeout_exceeded"),
            # Bug reproduction case: timeout < interval, heartbeat very old
            pytest.param(5, 10, 100, 0.01, id="cpu_spike_scenario"),
        ],
    )
    def test_max_wait_time_calculation_edge_cases(
        self,
        watched_subprocess,
        mock_process,
        monkeypatch,
        heartbeat_timeout,
        min_interval,
        heartbeat_ago,
        expected_min_timeout,
    ):
        """Test max_wait_time calculation in various edge case scenarios."""
        monkeypatch.setattr("airflow.sdk.execution_time.supervisor.HEARTBEAT_TIMEOUT", heartbeat_timeout)
        monkeypatch.setattr("airflow.sdk.execution_time.supervisor.MIN_HEARTBEAT_INTERVAL", min_interval)

        watched_subprocess._last_successful_heartbeat = time.monotonic() - heartbeat_ago
        mock_process.wait.side_effect = psutil.TimeoutExpired(pid=12345, seconds=0)

        # Call the method and verify timeout is never less than our minimum
        watched_subprocess._service_subprocess(
            max_wait_time=999
        )  # Large value, should be overridden by calculation

        # Extract the timeout that was actually used
        watched_subprocess.selector.select.assert_called_once()
        call_args = watched_subprocess.selector.select.call_args
        actual_timeout = call_args[1]["timeout"] if "timeout" in call_args[1] else call_args[0][0]

        assert actual_timeout >= expected_min_timeout


@dataclass
class ClientMock:
    """Configuration for mocking client method calls."""

    method_path: str
    """Path to the client method to mock (e.g., 'connections.get', 'variables.set')."""

    args: tuple = field(default_factory=tuple)
    """Positional arguments the client method should be called with."""

    kwargs: dict = field(default_factory=dict)
    """Keyword arguments the client method should be called with."""

    response: Any = None
    """What the mocked client method should return when called."""


@dataclass
class RequestTestCase:
    """Test case data for request handling tests in `TestHandleRequest` class."""

    message: Any
    """The request message to send to the supervisor (e.g., GetConnection, SetXCom)."""

    test_id: str
    """Unique identifier for this test case, used in pytest parameterization."""

    client_mock: ClientMock | None = None
    """Client method mocking configuration. None for messages that don't require client calls."""

    expected_body: dict | None = None
    """Expected response body from supervisor. None if no response body expected."""

    mask_secret_args: tuple | None = None
    """Arguments that should be passed to the secret masker for redaction."""


# Test cases for request handling
REQUEST_TEST_CASES = [
    RequestTestCase(
        message=GetConnection(conn_id="test_conn"),
        test_id="get_connection",
        client_mock=ClientMock(
            method_path="connections.get",
            args=("test_conn",),
            response=ConnectionResult(conn_id="test_conn", conn_type="mysql"),
        ),
        expected_body={"conn_id": "test_conn", "conn_type": "mysql", "type": "ConnectionResult"},
    ),
    RequestTestCase(
        message=GetConnection(conn_id="test_conn"),
        test_id="get_connection_with_password",
        client_mock=ClientMock(
            method_path="connections.get",
            args=("test_conn",),
            response=ConnectionResult(conn_id="test_conn", conn_type="mysql", password="password"),
        ),
        expected_body={
            "conn_id": "test_conn",
            "conn_type": "mysql",
            "password": "password",
            "type": "ConnectionResult",
        },
        mask_secret_args=("password",),
    ),
    RequestTestCase(
        message=GetConnection(conn_id="test_conn"),
        test_id="get_connection_with_alias",
        client_mock=ClientMock(
            method_path="connections.get",
            args=("test_conn",),
            response=ConnectionResult(conn_id="test_conn", conn_type="mysql", schema="mysql"),  # type: ignore[call-arg]
        ),
        expected_body={
            "conn_id": "test_conn",
            "conn_type": "mysql",
            "schema": "mysql",
            "type": "ConnectionResult",
        },
    ),
    RequestTestCase(
        message=GetVariable(key="test_key"),
        test_id="get_variable",
        client_mock=ClientMock(
            method_path="variables.get",
            args=("test_key",),
            response=VariableResult(key="test_key", value="test_value"),
        ),
        expected_body={"key": "test_key", "value": "test_value", "type": "VariableResult"},
        mask_secret_args=("test_value", "test_key"),
    ),
    RequestTestCase(
        message=PutVariable(key="test_key", value="test_value", description="test_description"),
        test_id="set_variable",
        client_mock=ClientMock(
            method_path="variables.set",
            args=("test_key", "test_value", "test_description"),
            response=OKResponse(ok=True),
        ),
    ),
    RequestTestCase(
        message=DeleteVariable(key="test_key"),
        test_id="delete_variable",
        client_mock=ClientMock(
            method_path="variables.delete",
            args=("test_key",),
            response=OKResponse(ok=True),
        ),
        expected_body={"ok": True, "type": "OKResponse"},
    ),
    RequestTestCase(
        message=DeferTask(next_method="execute_callback", classpath="my-classpath"),
        test_id="patch_task_instance_to_deferred",
        client_mock=ClientMock(
            method_path="task_instances.defer",
            args=(TI_ID, DeferTask(next_method="execute_callback", classpath="my-classpath")),
        ),
    ),
    RequestTestCase(
        message=RescheduleTask(
            reschedule_date=timezone.parse("2024-10-31T12:00:00Z"),
            end_date=timezone.parse("2024-10-31T12:00:00Z"),
        ),
        test_id="patch_task_instance_to_up_for_reschedule",
        client_mock=ClientMock(
            method_path="task_instances.reschedule",
            args=(
                TI_ID,
                RescheduleTask(
                    reschedule_date=timezone.parse("2024-10-31T12:00:00Z"),
                    end_date=timezone.parse("2024-10-31T12:00:00Z"),
                ),
            ),
        ),
    ),
    RequestTestCase(
        message=GetXCom(dag_id="test_dag", run_id="test_run", task_id="test_task", key="test_key"),
        test_id="get_xcom",
        client_mock=ClientMock(
            method_path="xcoms.get",
            args=("test_dag", "test_run", "test_task", "test_key", None, False),
            response=XComResult(key="test_key", value="test_value"),
        ),
        expected_body={"key": "test_key", "value": "test_value", "type": "XComResult"},
    ),
    RequestTestCase(
        message=GetXCom(
            dag_id="test_dag", run_id="test_run", task_id="test_task", key="test_key", map_index=2
        ),
        test_id="get_xcom_map_index",
        client_mock=ClientMock(
            method_path="xcoms.get",
            args=("test_dag", "test_run", "test_task", "test_key", 2, False),
            response=XComResult(key="test_key", value="test_value"),
        ),
        expected_body={"key": "test_key", "value": "test_value", "type": "XComResult"},
    ),
    RequestTestCase(
        message=GetXCom(dag_id="test_dag", run_id="test_run", task_id="test_task", key="test_key"),
        test_id="get_xcom_not_found",
        client_mock=ClientMock(
            method_path="xcoms.get",
            args=("test_dag", "test_run", "test_task", "test_key", None, False),
            response=XComResult(key="test_key", value=None, type="XComResult"),
        ),
        expected_body={"key": "test_key", "value": None, "type": "XComResult"},
    ),
    RequestTestCase(
        message=GetXCom(
            dag_id="test_dag",
            run_id="test_run",
            task_id="test_task",
            key="test_key",
            include_prior_dates=True,
        ),
        test_id="get_xcom_include_prior_dates",
        client_mock=ClientMock(
            method_path="xcoms.get",
            args=("test_dag", "test_run", "test_task", "test_key", None, True),
            response=XComResult(key="test_key", value=None, type="XComResult"),
        ),
        expected_body={"key": "test_key", "value": None, "type": "XComResult"},
    ),
    RequestTestCase(
        message=SetXCom(
            dag_id="test_dag",
            run_id="test_run",
            task_id="test_task",
            key="test_key",
            value='{"key": "test_key", "value": {"key2": "value2"}}',
        ),
        client_mock=ClientMock(
            method_path="xcoms.set",
            args=(
                "test_dag",
                "test_run",
                "test_task",
                "test_key",
                '{"key": "test_key", "value": {"key2": "value2"}}',
                None,
                None,
            ),
            response=OKResponse(ok=True),
        ),
        test_id="set_xcom",
    ),
    RequestTestCase(
        message=SetXCom(
            dag_id="test_dag",
            run_id="test_run",
            task_id="test_task",
            key="test_key",
            value='{"key": "test_key", "value": {"key2": "value2"}}',
            map_index=2,
        ),
        client_mock=ClientMock(
            method_path="xcoms.set",
            args=(
                "test_dag",
                "test_run",
                "test_task",
                "test_key",
                '{"key": "test_key", "value": {"key2": "value2"}}',
                2,
                None,
            ),
            response=OKResponse(ok=True),
        ),
        test_id="set_xcom_with_map_index",
    ),
    RequestTestCase(
        message=SetXCom(
            dag_id="test_dag",
            run_id="test_run",
            task_id="test_task",
            key="test_key",
            value='{"key": "test_key", "value": {"key2": "value2"}}',
            map_index=2,
            mapped_length=3,
        ),
        client_mock=ClientMock(
            method_path="xcoms.set",
            args=(
                "test_dag",
                "test_run",
                "test_task",
                "test_key",
                '{"key": "test_key", "value": {"key2": "value2"}}',
                2,
                3,
            ),
            response=OKResponse(ok=True),
        ),
        test_id="set_xcom_with_map_index_and_mapped_length",
    ),
    RequestTestCase(
        message=DeleteXCom(
            dag_id="test_dag",
            run_id="test_run",
            task_id="test_task",
            key="test_key",
            map_index=2,
        ),
        client_mock=ClientMock(
            method_path="xcoms.delete",
            args=("test_dag", "test_run", "test_task", "test_key", 2),
            response=OKResponse(ok=True),
        ),
        test_id="delete_xcom",
    ),
    RequestTestCase(
        message=RetryTask(
            end_date=timezone.parse("2024-10-31T12:00:00Z"), rendered_map_index="test retry task"
        ),
        client_mock=ClientMock(
            method_path="task_instances.retry",
            kwargs={
                "id": TI_ID,
                "end_date": timezone.parse("2024-10-31T12:00:00Z"),
                "rendered_map_index": "test retry task",
            },
            response=OKResponse(ok=True),
        ),
        test_id="up_for_retry",
    ),
    RequestTestCase(
        message=SetRenderedFields(rendered_fields={"field1": "rendered_value1", "field2": "rendered_value2"}),
        client_mock=ClientMock(
            method_path="task_instances.set_rtif",
            args=(TI_ID, {"field1": "rendered_value1", "field2": "rendered_value2"}),
            response=OKResponse(ok=True),
        ),
        test_id="set_rtif",
    ),
    RequestTestCase(
        message=SetRenderedMapIndex(rendered_map_index="Label: task_1"),
        client_mock=ClientMock(
            method_path="task_instances.set_rendered_map_index",
            args=(TI_ID, "Label: task_1"),
            response=OKResponse(ok=True),
        ),
        test_id="set_rendered_map_index",
    ),
    RequestTestCase(
        message=SucceedTask(
            end_date=timezone.parse("2024-10-31T12:00:00Z"), rendered_map_index="test success task"
        ),
        client_mock=ClientMock(
            method_path="task_instances.succeed",
            kwargs={
                "id": TI_ID,
                "outlet_events": None,
                "task_outlets": None,
                "when": timezone.parse("2024-10-31T12:00:00Z"),
                "rendered_map_index": "test success task",
            },
        ),
        test_id="succeed_task",
    ),
    RequestTestCase(
        message=GetAssetByName(name="asset"),
        expected_body={"name": "asset", "uri": "s3://bucket/obj", "group": "asset", "type": "AssetResult"},
        client_mock=ClientMock(
            method_path="assets.get",
            kwargs={"name": "asset"},
            response=AssetResult(name="asset", uri="s3://bucket/obj", group="asset"),
        ),
        test_id="get_asset_by_name",
    ),
    RequestTestCase(
        message=GetAssetByUri(uri="s3://bucket/obj"),
        expected_body={"name": "asset", "uri": "s3://bucket/obj", "group": "asset", "type": "AssetResult"},
        client_mock=ClientMock(
            method_path="assets.get",
            kwargs={"uri": "s3://bucket/obj"},
            response=AssetResult(name="asset", uri="s3://bucket/obj", group="asset"),
        ),
        test_id="get_asset_by_uri",
    ),
    RequestTestCase(
        message=GetAssetEventByAsset(uri="s3://bucket/obj", name="test"),
        expected_body={
            "asset_events": [
                {
                    "id": 1,
                    "timestamp": timezone.parse("2024-10-31T12:00:00Z"),
                    "asset": {"name": "asset", "uri": "s3://bucket/obj", "group": "asset"},
                    "created_dagruns": [],
                }
            ],
            "type": "AssetEventsResult",
        },
        client_mock=ClientMock(
            method_path="asset_events.get",
            kwargs={
                "uri": "s3://bucket/obj",
                "name": "test",
                "after": None,
                "before": None,
                "limit": None,
                "ascending": True,
            },
            response=AssetEventsResult(
                asset_events=[
                    AssetEventResponse(
                        id=1,
                        asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                        created_dagruns=[],
                        timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                    ),
                ],
            ),
        ),
        test_id="get_asset_events_by_uri_and_name",
    ),
    RequestTestCase(
        message=GetAssetEventByAsset(
            uri="s3://bucket/obj",
            name="test",
            after=datetime(2024, 10, 1, 12, 0, 0, tzinfo=timezone.utc),
            before=datetime(2024, 10, 15, 12, 0, 0, tzinfo=timezone.utc),
            limit=5,
            ascending=False,
        ),
        expected_body={
            "asset_events": [
                {
                    "id": 1,
                    "timestamp": timezone.parse("2024-10-31T12:00:00Z"),
                    "asset": {"name": "asset", "uri": "s3://bucket/obj", "group": "asset"},
                    "created_dagruns": [],
                }
            ],
            "type": "AssetEventsResult",
        },
        client_mock=ClientMock(
            method_path="asset_events.get",
            kwargs={
                "uri": "s3://bucket/obj",
                "name": "test",
                "after": timezone.parse("2024-10-01T12:00:00Z"),
                "before": timezone.parse("2024-10-15T12:00:00Z"),
                "limit": 5,
                "ascending": False,
            },
            response=AssetEventsResult(
                asset_events=[
                    AssetEventResponse(
                        id=1,
                        asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                        created_dagruns=[],
                        timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                    ),
                ],
            ),
        ),
        test_id="get_asset_events_by_uri_and_name_with_filters",
    ),
    RequestTestCase(
        message=GetAssetEventByAsset(uri="s3://bucket/obj", name=None),
        expected_body={
            "asset_events": [
                {
                    "id": 1,
                    "timestamp": timezone.parse("2024-10-31T12:00:00Z"),
                    "asset": {"name": "asset", "uri": "s3://bucket/obj", "group": "asset"},
                    "created_dagruns": [],
                }
            ],
            "type": "AssetEventsResult",
        },
        client_mock=ClientMock(
            method_path="asset_events.get",
            kwargs={
                "uri": "s3://bucket/obj",
                "name": None,
                "after": None,
                "before": None,
                "limit": None,
                "ascending": True,
            },
            response=AssetEventsResult(
                asset_events=[
                    AssetEventResponse(
                        id=1,
                        asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                        created_dagruns=[],
                        timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                    )
                ],
            ),
        ),
        test_id="get_asset_events_by_uri",
    ),
    RequestTestCase(
        message=GetAssetEventByAsset(
            uri="s3://bucket/obj",
            name=None,
            after=datetime(2024, 10, 1, 12, 0, 0, tzinfo=timezone.utc),
            before=datetime(2024, 10, 15, 12, 0, 0, tzinfo=timezone.utc),
            limit=5,
            ascending=False,
        ),
        expected_body={
            "asset_events": [
                {
                    "id": 1,
                    "timestamp": timezone.parse("2024-10-31T12:00:00Z"),
                    "asset": {"name": "asset", "uri": "s3://bucket/obj", "group": "asset"},
                    "created_dagruns": [],
                }
            ],
            "type": "AssetEventsResult",
        },
        client_mock=ClientMock(
            method_path="asset_events.get",
            kwargs={
                "uri": "s3://bucket/obj",
                "name": None,
                "after": timezone.parse("2024-10-01T12:00:00Z"),
                "before": timezone.parse("2024-10-15T12:00:00Z"),
                "limit": 5,
                "ascending": False,
            },
            response=AssetEventsResult(
                asset_events=[
                    AssetEventResponse(
                        id=1,
                        asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                        created_dagruns=[],
                        timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                    )
                ],
            ),
        ),
        test_id="get_asset_events_by_uri_with_filters",
    ),
    RequestTestCase(
        message=GetAssetEventByAsset(uri=None, name="test"),
        expected_body={
            "asset_events": [
                {
                    "id": 1,
                    "timestamp": timezone.parse("2024-10-31T12:00:00Z"),
                    "asset": {"name": "asset", "uri": "s3://bucket/obj", "group": "asset"},
                    "created_dagruns": [],
                }
            ],
            "type": "AssetEventsResult",
        },
        client_mock=ClientMock(
            method_path="asset_events.get",
            kwargs={
                "uri": None,
                "name": "test",
                "after": None,
                "before": None,
                "limit": None,
                "ascending": True,
            },
            response=AssetEventsResult(
                asset_events=[
                    AssetEventResponse(
                        id=1,
                        asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                        created_dagruns=[],
                        timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                    )
                ]
            ),
        ),
        test_id="get_asset_events_by_name",
    ),
    RequestTestCase(
        message=GetAssetEventByAsset(
            uri=None,
            name="test",
            after=datetime(2024, 10, 1, 12, 0, 0, tzinfo=timezone.utc),
            before=datetime(2024, 10, 15, 12, 0, 0, tzinfo=timezone.utc),
            limit=5,
            ascending=False,
        ),
        expected_body={
            "asset_events": [
                {
                    "id": 1,
                    "timestamp": timezone.parse("2024-10-31T12:00:00Z"),
                    "asset": {"name": "asset", "uri": "s3://bucket/obj", "group": "asset"},
                    "created_dagruns": [],
                }
            ],
            "type": "AssetEventsResult",
        },
        client_mock=ClientMock(
            method_path="asset_events.get",
            kwargs={
                "uri": None,
                "name": "test",
                "after": timezone.parse("2024-10-01T12:00:00Z"),
                "before": timezone.parse("2024-10-15T12:00:00Z"),
                "limit": 5,
                "ascending": False,
            },
            response=AssetEventsResult(
                asset_events=[
                    AssetEventResponse(
                        id=1,
                        asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                        created_dagruns=[],
                        timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                    )
                ]
            ),
        ),
        test_id="get_asset_events_by_name_with_filters",
    ),
    RequestTestCase(
        message=GetAssetEventByAssetAlias(alias_name="test_alias"),
        expected_body={
            "asset_events": [
                {
                    "id": 1,
                    "timestamp": timezone.parse("2024-10-31T12:00:00Z"),
                    "asset": {"name": "asset", "uri": "s3://bucket/obj", "group": "asset"},
                    "created_dagruns": [],
                }
            ],
            "type": "AssetEventsResult",
        },
        client_mock=ClientMock(
            method_path="asset_events.get",
            kwargs={
                "alias_name": "test_alias",
                "after": None,
                "before": None,
                "limit": None,
                "ascending": True,
            },
            response=AssetEventsResult(
                asset_events=[
                    AssetEventResponse(
                        id=1,
                        asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                        created_dagruns=[],
                        timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                    )
                ]
            ),
        ),
        test_id="get_asset_events_by_asset_alias",
    ),
    RequestTestCase(
        message=GetAssetEventByAssetAlias(
            alias_name="test_alias",
            after=datetime(2024, 10, 1, 12, 0, 0, tzinfo=timezone.utc),
            before=datetime(2024, 10, 15, 12, 0, 0, tzinfo=timezone.utc),
            limit=5,
            ascending=False,
        ),
        expected_body={
            "asset_events": [
                {
                    "id": 1,
                    "timestamp": timezone.parse("2024-10-31T12:00:00Z"),
                    "asset": {"name": "asset", "uri": "s3://bucket/obj", "group": "asset"},
                    "created_dagruns": [],
                }
            ],
            "type": "AssetEventsResult",
        },
        client_mock=ClientMock(
            method_path="asset_events.get",
            kwargs={
                "alias_name": "test_alias",
                "after": timezone.parse("2024-10-01T12:00:00Z"),
                "before": timezone.parse("2024-10-15T12:00:00Z"),
                "limit": 5,
                "ascending": False,
            },
            response=AssetEventsResult(
                asset_events=[
                    AssetEventResponse(
                        id=1,
                        asset=AssetResponse(name="asset", uri="s3://bucket/obj", group="asset"),
                        created_dagruns=[],
                        timestamp=timezone.parse("2024-10-31T12:00:00Z"),
                    )
                ]
            ),
        ),
        test_id="get_asset_events_by_asset_alias_with_filters",
    ),
    RequestTestCase(
        message=ValidateInletsAndOutlets(ti_id=TI_ID),
        expected_body={
            "inactive_assets": [{"name": "asset_name", "uri": "asset_uri", "type": "asset"}],
            "type": "InactiveAssetsResult",
        },
        client_mock=ClientMock(
            method_path="task_instances.validate_inlets_and_outlets",
            args=(TI_ID,),
            response=InactiveAssetsResult(
                inactive_assets=[AssetProfile(name="asset_name", uri="asset_uri", type="asset")]
            ),
        ),
        test_id="validate_inlets_and_outlets",
    ),
    RequestTestCase(
        message=GetPrevSuccessfulDagRun(ti_id=TI_ID),
        expected_body={
            "data_interval_start": timezone.parse("2025-01-10T12:00:00Z"),
            "data_interval_end": timezone.parse("2025-01-10T14:00:00Z"),
            "start_date": timezone.parse("2025-01-10T12:00:00Z"),
            "end_date": timezone.parse("2025-01-10T14:00:00Z"),
            "type": "PrevSuccessfulDagRunResult",
        },
        client_mock=ClientMock(
            method_path="task_instances.get_previous_successful_dagrun",
            args=(TI_ID,),
            response=PrevSuccessfulDagRunResult(
                start_date=timezone.parse("2025-01-10T12:00:00Z"),
                end_date=timezone.parse("2025-01-10T14:00:00Z"),
                data_interval_start=timezone.parse("2025-01-10T12:00:00Z"),
                data_interval_end=timezone.parse("2025-01-10T14:00:00Z"),
            ),
        ),
        test_id="get_prev_successful_dagrun",
    ),
    RequestTestCase(
        message=TriggerDagRun(
            dag_id="test_dag",
            run_id="test_run",
            conf={"key": "value"},
            logical_date=timezone.datetime(2025, 1, 1),
            reset_dag_run=True,
        ),
        expected_body={"ok": True, "type": "OKResponse"},
        client_mock=ClientMock(
            method_path="dag_runs.trigger",
            args=("test_dag", "test_run", {"key": "value"}, timezone.datetime(2025, 1, 1), True),
            response=OKResponse(ok=True),
        ),
        test_id="dag_run_trigger",
    ),
    RequestTestCase(
        message=TriggerDagRun(dag_id="test_dag", run_id="test_run"),
        expected_body={"error": "DAGRUN_ALREADY_EXISTS", "detail": None, "type": "ErrorResponse"},
        client_mock=ClientMock(
            method_path="dag_runs.trigger",
            args=("test_dag", "test_run", None, None, False),
            response=ErrorResponse(error=ErrorType.DAGRUN_ALREADY_EXISTS),
        ),
        test_id="dag_run_trigger_already_exists",
    ),
    RequestTestCase(
        message=GetDagRun(dag_id="test_dag", run_id="test_run"),
        expected_body={
            "dag_id": "test_dag",
            "run_id": "prev_run",
            "logical_date": timezone.parse("2024-01-14T12:00:00Z"),
            "partition_key": None,
            "run_type": "scheduled",
            "start_date": timezone.parse("2024-01-15T12:00:00Z"),
            "run_after": timezone.parse("2024-01-15T12:00:00Z"),
            "consumed_asset_events": [],
            "state": "success",
            "data_interval_start": None,
            "data_interval_end": None,
            "end_date": None,
            "clear_number": 0,
            "conf": None,
            "triggering_user_name": None,
            "type": "DagRunResult",
        },
        client_mock=ClientMock(
            method_path="dag_runs.get_detail",
            args=("test_dag", "test_run"),
            response=DagRunResult(
                dag_id="test_dag",
                run_id="prev_run",
                logical_date=timezone.parse("2024-01-14T12:00:00Z"),
                run_type=DagRunType.SCHEDULED,
                start_date=timezone.parse("2024-01-15T12:00:00Z"),
                run_after=timezone.parse("2024-01-15T12:00:00Z"),
                consumed_asset_events=[],
                state=DagRunState.SUCCESS,
                triggering_user_name=None,
            ),
        ),
        test_id="get_dag_run",
    ),
    RequestTestCase(
        message=GetDagRunState(dag_id="test_dag", run_id="test_run"),
        expected_body={"state": "running", "type": "DagRunStateResult"},
        client_mock=ClientMock(
            method_path="dag_runs.get_state",
            args=("test_dag", "test_run"),
            response=DagRunStateResult(state=DagRunState.RUNNING),
        ),
        test_id="get_dag_run_state",
    ),
    RequestTestCase(
        message=GetPreviousDagRun(
            dag_id="test_dag",
            logical_date=timezone.parse("2024-01-15T12:00:00Z"),
        ),
        expected_body={
            "dag_run": {
                "dag_id": "test_dag",
                "run_id": "prev_run",
                "logical_date": timezone.parse("2024-01-14T12:00:00Z"),
                "partition_key": None,
                "run_type": "scheduled",
                "start_date": timezone.parse("2024-01-15T12:00:00Z"),
                "run_after": timezone.parse("2024-01-15T12:00:00Z"),
                "consumed_asset_events": [],
                "state": "success",
                "data_interval_start": None,
                "data_interval_end": None,
                "end_date": None,
                "clear_number": 0,
                "conf": None,
                "triggering_user_name": None,
            },
            "type": "PreviousDagRunResult",
        },
        client_mock=ClientMock(
            method_path="dag_runs.get_previous",
            kwargs={
                "dag_id": "test_dag",
                "logical_date": timezone.parse("2024-01-15T12:00:00Z"),
                "state": None,
            },
            response=PreviousDagRunResult(
                dag_run=DagRun(
                    dag_id="test_dag",
                    run_id="prev_run",
                    logical_date=timezone.parse("2024-01-14T12:00:00Z"),
                    run_type=DagRunType.SCHEDULED,
                    start_date=timezone.parse("2024-01-15T12:00:00Z"),
                    run_after=timezone.parse("2024-01-15T12:00:00Z"),
                    consumed_asset_events=[],
                    state=DagRunState.SUCCESS,
                    triggering_user_name=None,
                )
            ),
        ),
        test_id="get_previous_dagrun",
    ),
    RequestTestCase(
        message=GetPreviousDagRun(
            dag_id="test_dag",
            logical_date=timezone.parse("2024-01-15T12:00:00Z"),
            state="success",
        ),
        expected_body={
            "dag_run": None,
            "type": "PreviousDagRunResult",
        },
        client_mock=ClientMock(
            method_path="dag_runs.get_previous",
            kwargs={
                "dag_id": "test_dag",
                "logical_date": timezone.parse("2024-01-15T12:00:00Z"),
                "state": "success",
            },
            response=PreviousDagRunResult(dag_run=None),
        ),
        test_id="get_previous_dagrun_with_state",
    ),
    RequestTestCase(
        message=GetPreviousTI(
            dag_id="test_dag",
            task_id="test_task",
            logical_date=timezone.parse("2024-01-15T12:00:00Z"),
            state=TaskInstanceState.SUCCESS,
        ),
        expected_body={
            "task_instance": {
                "task_id": "test_task",
                "dag_id": "test_dag",
                "run_id": "prev_run",
                "logical_date": timezone.parse("2024-01-14T12:00:00Z"),
                "start_date": timezone.parse("2024-01-14T12:05:00Z"),
                "end_date": timezone.parse("2024-01-14T12:10:00Z"),
                "state": "success",
                "try_number": 1,
                "map_index": -1,
                "duration": 300.0,
            },
            "type": "PreviousTIResult",
        },
        client_mock=ClientMock(
            method_path="task_instances.get_previous",
            kwargs={
                "dag_id": "test_dag",
                "task_id": "test_task",
                "logical_date": timezone.parse("2024-01-15T12:00:00Z"),
                "state": TaskInstanceState.SUCCESS,
                "run_id": None,
            },
            response=PreviousTIResult(
                task_instance=PreviousTIResponse(
                    task_id="test_task",
                    dag_id="test_dag",
                    run_id="prev_run",
                    logical_date=timezone.parse("2024-01-14T12:00:00Z"),
                    start_date=timezone.parse("2024-01-14T12:05:00Z"),
                    end_date=timezone.parse("2024-01-14T12:10:00Z"),
                    state="success",
                    try_number=1,
                    map_index=-1,
                    duration=300.0,
                )
            ),
        ),
        test_id="get_previous_ti",
    ),
    RequestTestCase(
        message=GetTaskRescheduleStartDate(ti_id=TI_ID),
        expected_body={
            "start_date": timezone.parse("2024-10-31T12:00:00Z"),
            "type": "TaskRescheduleStartDate",
        },
        client_mock=ClientMock(
            method_path="task_instances.get_reschedule_start_date",
            args=(TI_ID, 1),
            response=TaskRescheduleStartDate(start_date=timezone.parse("2024-10-31T12:00:00Z")),
        ),
        test_id="get_task_reschedule_start_date",
    ),
    RequestTestCase(
        message=GetTICount(dag_id="test_dag", task_ids=["task1", "task2"]),
        expected_body={"count": 2, "type": "TICount"},
        client_mock=ClientMock(
            method_path="task_instances.get_count",
            kwargs={
                "dag_id": "test_dag",
                "map_index": None,
                "logical_dates": None,
                "run_ids": None,
                "states": None,
                "task_group_id": None,
                "task_ids": ["task1", "task2"],
            },
            response=TICount(count=2),
        ),
        test_id="get_ti_count",
    ),
    RequestTestCase(
        message=GetDRCount(dag_id="test_dag", states=["success", "failed"]),
        expected_body={"count": 2, "type": "DRCount"},
        client_mock=ClientMock(
            method_path="dag_runs.get_count",
            kwargs={
                "dag_id": "test_dag",
                "logical_dates": None,
                "run_ids": None,
                "states": ["success", "failed"],
            },
            response=DRCount(count=2),
        ),
        test_id="get_dr_count",
    ),
    RequestTestCase(
        message=GetTaskStates(dag_id="test_dag", task_group_id="test_group"),
        expected_body={
            "task_states": {"run_id": {"task1": "success", "task2": "failed"}},
            "type": "TaskStatesResult",
        },
        client_mock=ClientMock(
            method_path="task_instances.get_task_states",
            kwargs={
                "dag_id": "test_dag",
                "map_index": None,
                "task_ids": None,
                "logical_dates": None,
                "run_ids": None,
                "task_group_id": "test_group",
            },
            response=TaskStatesResult(task_states={"run_id": {"task1": "success", "task2": "failed"}}),
        ),
        test_id="get_task_states",
    ),
    RequestTestCase(
        message=GetXComSequenceItem(
            key="test_key",
            dag_id="test_dag",
            run_id="test_run",
            task_id="test_task",
            offset=0,
        ),
        expected_body={"root": "test_value", "type": "XComSequenceIndexResult"},
        client_mock=ClientMock(
            method_path="xcoms.get_sequence_item",
            args=("test_dag", "test_run", "test_task", "test_key", 0),
            response=XComSequenceIndexResult(root="test_value"),
        ),
        test_id="get_xcom_seq_item",
    ),
    RequestTestCase(
        message=GetXComSequenceItem(
            key="test_key",
            dag_id="test_dag",
            run_id="test_run",
            task_id="test_task",
            offset=2,
        ),
        expected_body={"error": "XCOM_NOT_FOUND", "detail": None, "type": "ErrorResponse"},
        client_mock=ClientMock(
            method_path="xcoms.get_sequence_item",
            args=("test_dag", "test_run", "test_task", "test_key", 2),
            response=ErrorResponse(error=ErrorType.XCOM_NOT_FOUND),
        ),
        test_id="get_xcom_seq_item_not_found",
    ),
    RequestTestCase(
        message=GetXComSequenceSlice(
            key="test_key",
            dag_id="test_dag",
            run_id="test_run",
            task_id="test_task",
            start=None,
            stop=None,
            step=None,
            include_prior_dates=False,
        ),
        expected_body={"root": ["foo", "bar"], "type": "XComSequenceSliceResult"},
        client_mock=ClientMock(
            method_path="xcoms.get_sequence_slice",
            args=("test_dag", "test_run", "test_task", "test_key", None, None, None, False),
            response=XComSequenceSliceResult(root=["foo", "bar"]),
        ),
        test_id="get_xcom_seq_slice",
    ),
    RequestTestCase(
        message=TaskState(state=TaskInstanceState.SKIPPED, end_date=timezone.parse("2024-10-31T12:00:00Z")),
        test_id="patch_task_instance_to_skipped",
    ),
    RequestTestCase(
        message=CreateHITLDetailPayload(
            ti_id=TI_ID,
            options=["Approve", "Reject"],
            subject="This is subject",
            body="This is body",
            defaults=["Approve"],
            multiple=False,
            params={},
        ),
        expected_body={
            "ti_id": str(TI_ID),
            "options": ["Approve", "Reject"],
            "subject": "This is subject",
            "body": "This is body",
            "defaults": ["Approve"],
            "params": {},
            "type": "HITLDetailRequestResult",
        },
        client_mock=ClientMock(
            method_path="hitl.add_response",
            kwargs={
                "body": "This is body",
                "defaults": ["Approve"],
                "multiple": False,
                "options": ["Approve", "Reject"],
                "params": {},
                "assigned_users": None,
                "subject": "This is subject",
                "ti_id": TI_ID,
            },
            response=HITLDetailRequestResult(
                ti_id=TI_ID,
                options=["Approve", "Reject"],
                subject="This is subject",
                body="This is body",
                defaults=["Approve"],
                multiple=False,
                params={},
            ),
        ),
        test_id="create_hitl_detail_payload",
    ),
    RequestTestCase(
        message=MaskSecret(value=["iter1", "iter2", {"key": "value"}], name="test_secret"),
        mask_secret_args=(["iter1", "iter2", {"key": "value"}], "test_secret"),
        test_id="mask_secret_list",
    ),
    RequestTestCase(
        message=GetXComCount(key="test_key", dag_id="test_dag", run_id="test_run", task_id="test_task"),
        expected_body={"len": 5, "type": "XComLengthResponse"},
        client_mock=ClientMock(
            method_path="xcoms.head",
            args=("test_dag", "test_run", "test_task", "test_key"),
            response=XComCountResponse(len=5),
        ),
        test_id="get_xcom_count",
    ),
    RequestTestCase(
        message=ResendLoggingFD(),
        expected_body={"fds": mock.ANY, "type": "SentFDs"},
        test_id="resend_logging_fd",
    ),
    RequestTestCase(
        message=SkipDownstreamTasks(tasks=["task1", "task2"]),
        client_mock=ClientMock(
            method_path="task_instances.skip_downstream_tasks",
            args=(TI_ID, SkipDownstreamTasks(tasks=["task1", "task2"])),
            response=OKResponse(ok=True),
        ),
        test_id="skip_downstream_tasks",
    ),
    RequestTestCase(
        message=GetTaskBreadcrumbs(dag_id="test_dag", run_id="test_run"),
        client_mock=ClientMock(
            method_path="task_instances.get_task_breakcrumbs",
            kwargs={"dag_id": "test_dag", "run_id": "test_run"},
            response=TaskBreadcrumbsResult(
                breadcrumbs=[
                    {
                        "task_id": "test_task",
                        "map_index": 2,
                        "state": "success",
                        "operator": "PythonOperator",
                        "duration": 432.0,
                    },
                ],
            ),
        ),
        expected_body={
            "breadcrumbs": [
                {
                    "task_id": "test_task",
                    "map_index": 2,
                    "state": "success",
                    "operator": "PythonOperator",
                    "duration": 432.0,
                },
            ],
            "type": "TaskBreadcrumbsResult",
        },
        test_id="get_task_breadcrumbs",
    ),
]


class TestHandleRequest:
    @pytest.fixture
    def watched_subprocess(self, mocker):
        read_end, write_end = socket.socketpair()

        subprocess = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=12345,
            stdin=write_end,
            client=mocker.Mock(),
            process=mocker.Mock(),
        )

        return subprocess, read_end

    @patch("airflow.sdk.execution_time.supervisor.mask_secret")
    @pytest.mark.parametrize("test_case", REQUEST_TEST_CASES, ids=lambda tc: tc.test_id)
    def test_handle_requests(
        self,
        mock_mask_secret,
        watched_subprocess,
        mocker,
        time_machine,
        test_case: RequestTestCase,
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
        # Extract values from test_case
        message = test_case.message
        expected_body = test_case.expected_body
        client_mock = test_case.client_mock
        mask_secret_args = test_case.mask_secret_args

        # Rest of test implementation (copied from original)
        watched_subprocess, read_socket = watched_subprocess

        # Mock the client method. E.g. `client.variables.get` or `client.connections.get`
        if client_mock:
            mock_client_method = attrgetter(client_mock.method_path)(watched_subprocess.client)
            mock_client_method.return_value = client_mock.response

        # Simulate the generator
        generator = watched_subprocess.handle_requests(log=mocker.Mock())
        # Initialize the generator
        next(generator)

        req_frame = _RequestFrame(id=randint(1, 2**32 - 1), body=message.model_dump())
        generator.send(req_frame)

        if mask_secret_args is not None:
            mock_mask_secret.assert_called_with(*mask_secret_args)

        time_machine.move_to(timezone.datetime(2024, 10, 31), tick=False)

        # Verify the correct client method was called
        if client_mock:
            mock_client_method.assert_called_once_with(*client_mock.args, **client_mock.kwargs)

        # Read response from the read end of the socket
        read_socket.settimeout(0.1)
        frame_len = int.from_bytes(read_socket.recv(4), "big")
        bytes = read_socket.recv(frame_len)
        frame = msgspec.msgpack.Decoder(_ResponseFrame).decode(bytes)

        assert frame.id == req_frame.id

        # Verify the response was added to the buffer
        assert frame.body == expected_body

        # Verify the response is correctly decoded
        # This is important because the subprocess/task runner will read the response
        # and deserialize it to the correct message type

        if frame.body is not None and client_mock:
            decoder = CommsDecoder(socket=None).body_decoder  # type: ignore[var-annotated, arg-type]
            assert decoder.validate_python(frame.body) == client_mock.response

    def test_all_to_supervisor_messages_are_covered(self):
        """Ensure all ToSupervisor message types have test coverage."""

        # Extract the individual message types from the Union
        union_type = ToSupervisor.__args__[0]
        supervisor_message_types = set(union_type.__args__)

        # Get all message types covered in our test cases
        tested_message_types = {type(test_case.message) for test_case in REQUEST_TEST_CASES}

        # Message types which are excluded for a good reason
        excluded_message_types = {
            GetHITLDetailResponse,  # Only used in Triggerer, not needed in worker
            UpdateHITLDetail,  # Only used in Triggerer, not needed in worker
        }

        untested_types = supervisor_message_types - tested_message_types - excluded_message_types

        # Assert all types are covered
        assert not untested_types, (
            f"Missing test coverage for {len(untested_types)}/{len(supervisor_message_types)} "
            f"ToSupervisor message types:\n"
            + "\n".join(f"  - {t.__name__}" for t in sorted(untested_types, key=lambda x: x.__name__))
            + "\n\nPlease add test cases to REQUEST_TEST_CASES."
        )

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

        msg = SucceedTask(end_date=timezone.parse("2024-10-31T12:00:00Z"))
        req_frame = _RequestFrame(id=randint(1, 2**32 - 1), body=msg.model_dump())
        generator.send(req_frame)

        # Read response from the read end of the socket
        read_socket.settimeout(0.1)
        frame_len = int.from_bytes(read_socket.recv(4), "big")
        bytes = read_socket.recv(frame_len)
        frame = msgspec.msgpack.Decoder(_ResponseFrame).decode(bytes)

        assert frame.id == req_frame.id

        assert frame.error == {
            "error": "API_SERVER_ERROR",
            "detail": {
                "status_code": 500,
                "message": "API Server Error",
                "detail": {"detail": "Internal Server Error"},
            },
            "type": "ErrorResponse",
        }

        # Verify the error can be decoded correctly
        comms = CommsDecoder(socket=None)
        with pytest.raises(AirflowRuntimeError) as exc_info:
            comms._from_frame(frame)

        assert exc_info.value.error.error == ErrorType.API_SERVER_ERROR
        assert exc_info.value.error.detail == {
            "status_code": error.response.status_code,
            "message": str(error),
            "detail": error.response.json(),
        }


class TestSetSupervisorComms:
    class DummyComms:
        pass

    @pytest.fixture(autouse=True)
    def cleanup_supervisor_comms(self):
        # Ensure clean state before/after test
        if hasattr(task_runner, "SUPERVISOR_COMMS"):
            delattr(task_runner, "SUPERVISOR_COMMS")
        yield
        if hasattr(task_runner, "SUPERVISOR_COMMS"):
            delattr(task_runner, "SUPERVISOR_COMMS")

    def test_set_supervisor_comms_overrides_and_restores(self):
        task_runner.SUPERVISOR_COMMS = self.DummyComms()
        original = task_runner.SUPERVISOR_COMMS
        replacement = self.DummyComms()

        with set_supervisor_comms(replacement):
            assert task_runner.SUPERVISOR_COMMS is replacement
        assert task_runner.SUPERVISOR_COMMS is original

    def test_set_supervisor_comms_sets_temporarily_when_not_set(self):
        assert not hasattr(task_runner, "SUPERVISOR_COMMS")
        replacement = self.DummyComms()

        with set_supervisor_comms(replacement):
            assert task_runner.SUPERVISOR_COMMS is replacement
        assert not hasattr(task_runner, "SUPERVISOR_COMMS")

    def test_set_supervisor_comms_unsets_temporarily_when_not_set(self):
        assert not hasattr(task_runner, "SUPERVISOR_COMMS")

        # This will delete an attribute that isn't set, and restore it likewise
        with set_supervisor_comms(None):
            assert not hasattr(task_runner, "SUPERVISOR_COMMS")

        assert not hasattr(task_runner, "SUPERVISOR_COMMS")


class TestInProcessTestSupervisor:
    def test_inprocess_supervisor_comms_roundtrip(self):
        """
        Test that InProcessSupervisorComms correctly sends a message to the supervisor,
        and that the supervisor's response is received via the message queue.

        This verifies the end-to-end communication flow:
        - send_request() dispatches a message to the supervisor
        - the supervisor handles the request and appends a response via send_msg()
        - get_message() returns the enqueued response

        This test mocks the supervisor's `_handle_request()` method to simulate
        a simple echo-style response, avoiding full task execution.
        """

        class MinimalSupervisor(InProcessTestSupervisor):
            def _handle_request(self, msg, log, req_id):
                resp = VariableResult(key=msg.key, value="value")
                self.send_msg(resp, req_id)

        supervisor = MinimalSupervisor(
            id="test",
            pid=123,
            process=MagicMock(),
            process_log=MagicMock(),
            client=MagicMock(),
        )
        comms = InProcessSupervisorComms(supervisor=supervisor)
        supervisor.comms = comms

        test_msg = GetVariable(key="test_key")

        response = comms.send(test_msg)

        # Ensure we got back what we expect
        assert isinstance(response, VariableResult)
        assert response.value == "value"


class TestInProcessClient:
    def test_no_retries(self):
        called = 0

        def noop_handler(request: httpx.Request) -> httpx.Response:
            nonlocal called
            called += 1
            return httpx.Response(500)

        transport = httpx.MockTransport(noop_handler)
        client = InProcessTestSupervisor._Client(
            base_url="http://local.invalid", token="", transport=transport
        )

        with pytest.raises(httpx.HTTPStatusError):
            client.get("/goo")

        assert called == 1


@pytest.mark.parametrize(
    ("remote_logging", "remote_conn", "expected_env"),
    (
        pytest.param(True, "", "AIRFLOW_CONN_AWS_DEFAULT", id="no-conn-id"),
        pytest.param(True, "aws_default", "AIRFLOW_CONN_AWS_DEFAULT", id="explicit-default"),
        pytest.param(True, "my_aws", "AIRFLOW_CONN_MY_AWS", id="other"),
        pytest.param(False, "", "", id="no-remote-logging"),
    ),
)
def test_remote_logging_conn(remote_logging, remote_conn, expected_env, monkeypatch, mocker):
    # This doesn't strictly need the AWS provider, but it does need something that
    # airflow.config_templates.airflow_local_settings.DEFAULT_LOGGING_CONFIG knows about
    pytest.importorskip("airflow.providers.amazon", reason="'amazon' provider not installed")

    # This test is a little bit overly specific to how the logging is currently configured :/
    monkeypatch.delitem(sys.modules, "airflow.logging_config")
    monkeypatch.delitem(sys.modules, "airflow.config_templates.airflow_local_settings", raising=False)

    def handle_request(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            json={
                # Minimal enough to pass validation, we don't care what fields are in here for the tests
                "conn_id": remote_conn,
                "conn_type": "aws",
            },
        )

    # Patch configurations in both airflow-core and task-sdk due to shared library refactoring.
    #
    # conf_vars() patches airflow.configuration.conf (airflow-core):
    #   - remote_logging: needed by airflow_local_settings.py to decide whether to set up REMOTE_TASK_LOG
    #   - remote_base_log_folder: needed by airflow_local_settings.py to create the CloudWatch handler
    #
    # task_sdk_conf_vars() patches airflow.sdk.configuration.conf (task-sdk):
    #   - remote_log_conn_id: needed by load_remote_conn_id() to return the correct connection id
    with conf_vars(
        {
            ("logging", "remote_logging"): str(remote_logging),
            ("logging", "remote_base_log_folder"): "cloudwatch://arn:aws:logs:::log-group:test",
            ("logging", "remote_log_conn_id"): remote_conn,
        }
    ):
        with conf_vars(
            {
                ("logging", "remote_log_conn_id"): remote_conn,
            }
        ):
            env = os.environ.copy()
            client = make_client(transport=httpx.MockTransport(handle_request))

            with _remote_logging_conn(client):
                new_keys = os.environ.keys() - env.keys()
                if remote_logging:
                    # _remote_logging_conn sets both the connection env var and _AIRFLOW_PROCESS_CONTEXT
                    assert new_keys == {expected_env, "_AIRFLOW_PROCESS_CONTEXT"}
                else:
                    assert not new_keys

            if remote_logging and expected_env:
                connection_available = {"available": False, "conn_uri": None}

                def mock_upload_to_remote(process_log, ti):
                    connection_available["available"] = expected_env in os.environ
                    connection_available["conn_uri"] = os.environ.get(expected_env)

                mocker.patch("airflow.sdk.log.upload_to_remote", side_effect=mock_upload_to_remote)

                activity_subprocess = ActivitySubprocess(
                    process_log=mocker.MagicMock(),
                    id=TI_ID,
                    pid=12345,
                    stdin=mocker.MagicMock(),
                    client=client,
                    process=mocker.MagicMock(),
                )
                activity_subprocess.ti = mocker.MagicMock()

                activity_subprocess._upload_logs()

                assert connection_available["available"], (
                    f"Connection {expected_env} was not available during upload_to_remote call"
                )
                assert connection_available["conn_uri"] is not None, "Connection URI was None during upload"


def test_remote_logging_conn_sets_process_context(monkeypatch, mocker):
    """
    Test that _remote_logging_conn sets _AIRFLOW_PROCESS_CONTEXT=client.
    """
    pytest.importorskip("airflow.providers.amazon", reason="'amazon' provider not installed")
    from airflow.models.connection import Connection as CoreConnection
    from airflow.sdk.definitions.connection import Connection as SDKConnection

    monkeypatch.delitem(sys.modules, "airflow.logging_config")
    monkeypatch.delitem(sys.modules, "airflow.config_templates.airflow_local_settings", raising=False)

    conn_id = "s3_conn_logs"
    conn_uri = "aws:///?region_name=us-east-1"

    def handle_request(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            status_code=200,
            json={
                "conn_id": conn_id,
                "conn_type": "aws",
                "host": None,
                "login": None,
                "password": None,
                "port": None,
                "schema": None,
                "extra": '{"region_name": "us-east-1"}',
            },
        )

    with conf_vars(
        {
            ("logging", "remote_logging"): "True",
            ("logging", "remote_base_log_folder"): "s3://bucket/logs",
            ("logging", "remote_log_conn_id"): conn_id,
        }
    ):
        with conf_vars(
            {
                ("logging", "remote_log_conn_id"): conn_id,
            }
        ):
            client = make_client(transport=httpx.MockTransport(handle_request))

            assert os.getenv("_AIRFLOW_PROCESS_CONTEXT") is None

            conn_env_key = f"AIRFLOW_CONN_{conn_id.upper()}"

            with _remote_logging_conn(client):
                assert os.getenv("_AIRFLOW_PROCESS_CONTEXT") == "client"

                assert conn_env_key in os.environ
                stored_uri = os.environ[conn_env_key]
                assert stored_uri == conn_uri

                # Verify that Connection.get() uses SDK Connection class when _AIRFLOW_PROCESS_CONTEXT=client
                # Without _AIRFLOW_PROCESS_CONTEXT=client, _get_connection_class() would return core
                # Connection. While core Connection can handle URI deserialization via its __init__,
                # using SDK Connection ensures consistency and proper behavior in supervisor context.
                from airflow.sdk.execution_time.context import _get_connection

                retrieved_conn = _get_connection(conn_id)

                assert isinstance(retrieved_conn, SDKConnection)
                assert not isinstance(retrieved_conn, CoreConnection)
                assert retrieved_conn.conn_id == conn_id
                assert retrieved_conn.conn_type == "aws"

            # Verify _AIRFLOW_PROCESS_CONTEXT and env var is cleaned up
            assert os.getenv("_AIRFLOW_PROCESS_CONTEXT") is None
            assert conn_env_key not in os.environ


class TestSignalRetryLogic:
    """Test retry logic for exit codes (signals and non-signal failures) in ActivitySubprocess."""

    @pytest.mark.parametrize(
        "signal",
        [
            signal.SIGTERM,
            signal.SIGKILL,
            signal.SIGABRT,
            signal.SIGSEGV,
        ],
    )
    def test_signals_with_retry(self, mocker, signal):
        """Test that signals with task retries."""
        mock_watched_subprocess = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=12345,
            stdin=mocker.Mock(),
            process=mocker.Mock(),
            client=mocker.Mock(),
        )

        mock_watched_subprocess._exit_code = -signal
        mock_watched_subprocess._should_retry = True

        result = mock_watched_subprocess.final_state
        assert result == TaskInstanceState.UP_FOR_RETRY

    @pytest.mark.parametrize(
        "signal",
        [
            signal.SIGKILL,
            signal.SIGTERM,
            signal.SIGABRT,
            signal.SIGSEGV,
        ],
    )
    def test_signals_without_retry_always_fail(self, mocker, signal):
        """Test that signals without task retries enabled always fail."""
        mock_watched_subprocess = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=12345,
            stdin=mocker.Mock(),
            process=mocker.Mock(),
            client=mocker.Mock(),
        )
        mock_watched_subprocess._should_retry = False
        mock_watched_subprocess._exit_code = -signal

        result = mock_watched_subprocess.final_state
        assert result == TaskInstanceState.FAILED

    def test_non_signal_exit_code_with_retry_goes_to_up_for_retry(self, mocker):
        """Test that non-signal exit codes with retries enabled go to UP_FOR_RETRY."""
        mock_watched_subprocess = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=12345,
            stdin=mocker.Mock(),
            process=mocker.Mock(),
            client=mocker.Mock(),
        )
        mock_watched_subprocess._exit_code = 1
        mock_watched_subprocess._should_retry = True

        assert mock_watched_subprocess.final_state == TaskInstanceState.UP_FOR_RETRY

    def test_non_signal_exit_code_without_retry_goes_to_failed(self, mocker):
        """Test that non-signal exit codes without retries enabled go to FAILED."""
        mock_watched_subprocess = ActivitySubprocess(
            process_log=mocker.MagicMock(),
            id=TI_ID,
            pid=12345,
            stdin=mocker.Mock(),
            process=mocker.Mock(),
            client=mocker.Mock(),
        )
        mock_watched_subprocess._exit_code = 1
        mock_watched_subprocess._should_retry = False

        assert mock_watched_subprocess.final_state == TaskInstanceState.FAILED


def test_remote_logging_conn_caches_connection_not_client(monkeypatch):
    """Test that connection caching doesn't retain API client references."""
    import gc
    import weakref

    from airflow.sdk import log as sdk_log
    from airflow.sdk.execution_time import supervisor

    class ExampleBackend:
        def __init__(self):
            self.calls = 0

        def get_connection(self, conn_id: str):
            self.calls += 1
            from airflow.sdk.definitions.connection import Connection

            return Connection(conn_id=conn_id, conn_type="example")

    backend = ExampleBackend()
    monkeypatch.setattr(supervisor, "ensure_secrets_backend_loaded", lambda: [backend])
    monkeypatch.setattr(sdk_log, "load_remote_log_handler", lambda: object())
    monkeypatch.setattr(sdk_log, "load_remote_conn_id", lambda: "test_conn")
    monkeypatch.delenv("AIRFLOW_CONN_TEST_CONN", raising=False)

    def noop_request(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200)

    clients = []
    for _ in range(3):
        client = make_client(transport=httpx.MockTransport(noop_request))
        clients.append(weakref.ref(client))
        with _remote_logging_conn(client):
            pass
        client.close()
        del client

    gc.collect()
    assert backend.calls == 1, "Connection should be cached, not fetched multiple times"
    assert all(ref() is None for ref in clients), "Client instances should be garbage collected"


def test_process_log_messages_from_subprocess(monkeypatch, caplog):
    from airflow.sdk._shared.logging.structlog import PER_LOGGER_LEVELS

    read_end, write_end = socket.socketpair()

    # Set global level at warning
    monkeypatch.setitem(PER_LOGGER_LEVELS, "", logging.WARNING)
    output_log = structlog.get_logger()

    gen = process_log_messages_from_subprocess(loggers=(output_log,))

    # We need to start up the generator to get it to the point it's at waiting on the yield
    next(gen)

    # Now we can send in messages to it.
    gen.send(b'{"level": "debug", "event": "A debug"}\n')
    gen.send(b'{"level": "error", "event": "An error"}\n')

    assert caplog.record_tuples == [
        (None, logging.DEBUG, "A debug"),
        (None, logging.ERROR, "An error"),
    ]


def test_reinit_supervisor_comms(monkeypatch, client_with_ti_start, caplog):
    def subprocess_main():
        # This is run in the subprocess!

        # Ensure we follow the "protocol" and get the startup message before we do anything else
        c = CommsDecoder()
        c._get_response()

        # This mirrors what the VirtualEnvProvider puts in it's script
        script = """
            import os
            import sys
            import structlog

            from airflow.sdk import Connection
            from airflow.sdk.execution_time.task_runner import reinit_supervisor_comms

            reinit_supervisor_comms()

            Connection.get("a")
            print("ok")
            sys.stdout.flush()

            structlog.get_logger().info("is connected")
        """
        # Now we launch a new process, as VirtualEnvOperator will do
        subprocess.check_call([sys.executable, "-c", dedent(script)])

    client_with_ti_start.connections.get.return_value = ConnectionResult(
        conn_id="test_conn", conn_type="mysql", login="a", password="password1"
    )
    proc = ActivitySubprocess.start(
        dag_rel_path=os.devnull,
        bundle_info=FAKE_BUNDLE,
        what=TaskInstance(
            id="4d828a62-a417-4936-a7a6-2b3fabacecab",
            task_id="b",
            dag_id="c",
            run_id="d",
            try_number=1,
            dag_version_id=uuid7(),
        ),
        client=client_with_ti_start,
        target=subprocess_main,
    )

    rc = proc.wait()

    assert rc == 0, caplog.text
    # Check that the log messages are write. We should expect stdout to apper right, and crucially, we should
    # expect logs from the venv process to appear without extra "wrapping"
    assert {
        "logger": "task.stdout",
        "event": "ok",
        "log_level": "info",
        "timestamp": mock.ANY,
    } in caplog, caplog.text
    assert {
        "logger_name": "task",
        "log_level": "info",
        "event": "is connected",
        "timestamp": mock.ANY,
    } in caplog, caplog.text
