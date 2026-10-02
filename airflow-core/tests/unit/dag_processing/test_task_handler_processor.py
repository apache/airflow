#
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

import io
import os
import selectors
import signal
import socket
import sys
import threading
import time
import uuid
from pathlib import Path
from unittest.mock import ANY, MagicMock, patch

import psutil
import pytest
import structlog
from pydantic import TypeAdapter
from structlog.typing import FilteringBoundLogger

from airflow.configuration import conf
from airflow.dag_processing.processor import (
    BaseDagFileProcessorProcess,
    DagFileParseRequest,
    DagFileParsingResult,
    DagFileProcessorProcess,
    TaskHandlerDeclaration,
    TaskHandlerParam,
    TaskHandlerParseRequest,
    TaskHandlerParsingResult,
    ToDagProcessor,
    ToManager,
)
from airflow.dag_processing.task_handler_processor import (
    LangSDKRuntimeSchemaVersion,
    LangSDKTaskHandlerProcessorProcess,
    _get_import_timeout,
)
from airflow.sdk.api.client import Client, VariableOperations
from airflow.sdk.api.datamodels._generated import VariableResponse
from airflow.sdk.coordinators._subprocess import SubprocessCoordinator
from airflow.sdk.exceptions import AirflowRuntimeError, ErrorType
from airflow.sdk.execution_time import supervisor, task_runner
from airflow.sdk.execution_time.comms import (
    CommsDecoder,
    ErrorResponse,
    GetVariable,
    MaskSecret,
    VariableResult,
    _RequestFrame,
)

from tests_common.test_utils.config import conf_vars
from unit.dag_processing.fake_task_handler_runtime import (
    FakeCoordinator,
    fake_coordinator,
    play_runtime,
    write_artifact,
)

# The oldest supervisor schema version, so the parse request is downgraded.
OLDEST_SCHEMA_VERSION = "2026-06-16"


def _reply_with(*task_ids: str, **result):
    """Reply with a handler for each of *task_ids* under the Dag ``etl``."""

    def reply(request: TaskHandlerParseRequest, comms) -> TaskHandlerParsingResult:
        declarations = [
            TaskHandlerDeclaration(task_id=task_id, binding="positional", params=[]) for task_id in task_ids
        ]
        return TaskHandlerParsingResult(fileloc=request.file, task_handlers={"etl": declarations}, **result)

    return reply


def _get_task_ids(result: TaskHandlerParsingResult) -> list[str]:
    return [declaration.task_id for declaration in result.task_handlers["etl"]]


def _get_open_fds() -> set[int]:
    # Without /proc, as on macOS, this is empty, so the fd leak checks pass trivially.
    return {int(fd) for fd in os.listdir("/proc/self/fd")} if os.path.isdir("/proc/self/fd") else set()


@pytest.fixture(autouse=True)
def _coordinator():
    with fake_coordinator():
        yield


@pytest.fixture
def supervisor_comms(monkeypatch):
    """Give this process a supervisor channel, as a Dag-parsing child has."""
    comms = MagicMock(spec=CommsDecoder)
    monkeypatch.setattr(task_runner, "SUPERVISOR_COMMS", comms, raising=False)
    return comms


def _start(tmp_path, selector, *, client: Client | None = None, **spec) -> LangSDKTaskHandlerProcessorProcess:
    return LangSDKTaskHandlerProcessorProcess.start(
        id=uuid.uuid4(),
        coordinator="fake",
        path=write_artifact(tmp_path / "etl.artifact", **spec),
        bundle_path=tmp_path,
        bundle_name="task-handlers",
        artifact_rel_path="etl.artifact",
        selector=selector,
        logger=structlog.get_logger(),
        client=client,
    )


@pytest.fixture
def parse(tmp_path):
    """Probe ``etl.artifact`` under a caller's selector loop, and check that nothing is left open."""

    def _parse(**kwargs) -> LangSDKTaskHandlerProcessorProcess:
        fds_before = _get_open_fds()
        with selectors.DefaultSelector() as selector:
            proc = _start(tmp_path, selector, **kwargs)
            deadline = time.monotonic() + 30
            while not proc.is_ready:
                assert time.monotonic() < deadline, "the Lang-SDK parse did not finish"
                proc._service_subprocess(max_wait_time=0.1)
            assert selector.get_map() == {}
            proc.close()
        assert _get_open_fds() <= fds_before
        return proc

    return _parse


def _block_until_killed(comms) -> None:
    """Hang as a stuck runtime does, until it is killed or the parent closes the comm socket."""
    comms.socket.recv(1)


def _send_an_invalid_frame(request, comms) -> None:
    comms.socket.sendall(bytes.fromhex("00000003c1c1c1"))
    _block_until_killed(comms)


def _send_a_frame(comms, body: dict) -> None:
    comms.socket.sendall(_RequestFrame(id=1, body=body).as_bytes())
    _block_until_killed(comms)


def _send_an_invalid_result(request, comms) -> None:
    _send_a_frame(
        comms, {"type": "TaskHandlerParsingResult", "fileloc": request.file, "task_handlers": "none"}
    )


def _send_a_start_message(request, comms) -> None:
    _send_a_frame(comms, LangSDKRuntimeSchemaVersion(schema_version=None).model_dump())


def _run(tmp_path, *, coordinator: str = "fake", **spec) -> TaskHandlerParsingResult:
    return LangSDKTaskHandlerProcessorProcess.run(
        coordinator=coordinator,
        path=write_artifact(tmp_path / "etl.artifact", **spec),
        bundle_path=tmp_path,
        bundle_name="task-handlers",
        artifact_rel_path="etl.artifact",
        logger=structlog.get_logger(),
    )


class TestLangSDKTaskHandlerProcessorProcess:
    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_collects_the_result_the_runtime_returns(
        self, mock_parse_task_handler, parse, tmp_path, cap_structlog
    ):
        mock_parse_task_handler.side_effect = play_runtime(
            _reply_with("extract"),
            schema_version=OLDEST_SCHEMA_VERSION,
            log_lines=[{"event": "Registering handlers", "level": "info"}],
        )

        proc = parse()

        assert proc.parsing_result.fileloc == os.fspath(tmp_path / "etl.artifact")
        assert proc.parsing_result.import_errors is None
        assert _get_task_ids(proc.parsing_result) == ["extract"]
        assert proc._subprocess_schema_version == OLDEST_SCHEMA_VERSION
        assert "Registering handlers" in cap_structlog

    @patch("airflow.dag_processing.task_handler_processor._is_connection_from_pid", autospec=True)
    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_connection_is_used_once_it_is_verified(self, mock_parse_task_handler, mock_owned, tmp_path):
        mock_parse_task_handler.side_effect = play_runtime(_reply_with("extract"))
        mock_owned.return_value = False
        with selectors.DefaultSelector() as selector:
            proc = _start(tmp_path, selector)
            child_stdin = proc.stdin
            deadline = time.monotonic() + 30
            while len(proc._unverified_connections) < 2:
                assert proc.stdin is child_stdin, "an unverified connection was used"
                assert time.monotonic() < deadline, "the runtime did not connect"
                proc._service_subprocess(max_wait_time=0.1)
            assert proc.stdin is child_stdin

            mock_owned.return_value = True
            while not proc.is_ready:
                assert time.monotonic() < deadline, "the Lang-SDK parse did not finish"
                proc._service_subprocess(max_wait_time=0.1)
            proc.close()

        assert _get_task_ids(proc.parsing_result) == ["extract"]

    @patch.object(BaseDagFileProcessorProcess, "start", autospec=True, side_effect=OSError("fork failed"))
    def test_a_start_that_fails_before_the_fork_closes_its_listeners(self, mock_start, tmp_path):
        fds_before = _get_open_fds()

        with selectors.DefaultSelector() as selector:
            with pytest.raises(OSError, match="fork failed"):
                _start(tmp_path, selector)

        listeners = mock_start.call_args.kwargs["listeners"]
        assert [listener.fileno() for listener in listeners.values()] == [-1, -1]
        assert _get_open_fds() <= fds_before

    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_requests_are_answered_by_the_client(self, mock_parse_task_handler, parse):
        def reply(request, comms):
            variable = comms.send(GetVariable(key="probe_var"))
            return _reply_with(variable.value)(request, comms)

        mock_parse_task_handler.side_effect = play_runtime(reply)
        client = MagicMock(spec=Client)
        client.variables = MagicMock(spec=VariableOperations)
        client.variables.get.return_value = VariableResponse(key="probe_var", value="from-the-client")

        proc = parse(client=client)

        assert _get_task_ids(proc.parsing_result) == ["from-the-client"]

    @pytest.mark.parametrize(
        ("spec", "reply", "error"),
        [
            pytest.param(
                {"command_error": "no runtime"},
                None,
                "Cannot start the Lang-SDK runtime: FileNotFoundError: no runtime",
                id="command-not-resolved",
            ),
            pytest.param(
                {"argv": ["/no/such/runtime"]},
                None,
                "Cannot start the Lang-SDK runtime: FileNotFoundError: "
                "[Errno 2] No such file or directory: '/no/such/runtime'",
                id="exec-failed",
            ),
            pytest.param(
                {"argv": ["/bin/sh", "-c", "exit 3"]},
                None,
                "The Lang-SDK runtime exited with code 3 without a parse result",
                id="exits-before-connecting",
            ),
            pytest.param(
                {},
                lambda request, comms: None,
                "The Lang-SDK runtime exited with code 0 without a parse result",
                id="exits-without-a-result",
            ),
            pytest.param(
                {},
                _send_an_invalid_frame,
                "The Lang-SDK runtime sent an invalid frame: MessagePack data is malformed: "
                "invalid opcode '\\xc1' (byte 0)",
                id="invalid-frame",
            ),
        ],
    )
    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_failed_parse_is_an_import_error(
        self, mock_parse_task_handler, parse, tmp_path, spec, reply, error
    ):
        mock_parse_task_handler.side_effect = (
            play_runtime(reply) if reply else SubprocessCoordinator.parse_task_handler
        )

        proc = parse(**spec)

        assert proc.parsing_result == TaskHandlerParsingResult(
            fileloc=os.fspath(tmp_path / "etl.artifact"),
            task_handlers={},
            import_errors={"etl.artifact": error},
        )

    def test_a_coordinator_that_is_not_configured_is_an_import_error(self, tmp_path):
        result = _run(tmp_path, coordinator="missing")

        assert result == TaskHandlerParsingResult(
            fileloc=os.fspath(tmp_path / "etl.artifact"),
            task_handlers={},
            import_errors={
                "etl.artifact": "Cannot start the Lang-SDK runtime: "
                "InvalidCoordinatorError: No coordinator 'missing' in [sdk] coordinators"
            },
        )

    @patch.object(
        FakeCoordinator,
        "_build_parse_task_handler_command",
        SubprocessCoordinator._build_parse_task_handler_command,
    )
    def test_a_coordinator_that_does_not_parse_task_handlers_is_an_import_error(self, parse):
        proc = parse()

        assert proc.parsing_result.import_errors == {
            "etl.artifact": "Cannot start the Lang-SDK runtime: "
            "NotImplementedError: FakeCoordinator does not parse task handlers"
        }

    @pytest.mark.parametrize(
        ("reply", "error"),
        [
            pytest.param(
                _send_an_invalid_result,
                "TaskHandlerParsingResult.task_handlers\n  Input should be a valid dictionary",
                id="result",
            ),
            pytest.param(
                _send_a_start_message, "does not match any of the expected tags", id="start-message"
            ),
        ],
    )
    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_message_that_does_not_validate_is_an_import_error(
        self, mock_parse_task_handler, parse, reply, error
    ):
        mock_parse_task_handler.side_effect = play_runtime(reply)

        proc = parse()

        [message] = proc.parsing_result.import_errors.values()
        assert message.startswith("The Lang-SDK runtime sent a message that does not validate: ")
        assert error in message
        assert proc._exit_code == -signal.SIGKILL

    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_dag_file_parsing_result_is_not_a_task_handler_result(self, mock_parse_task_handler, tmp_path):
        def reply(request, comms):
            with pytest.raises(AirflowRuntimeError, match="Unhandled request"):
                comms.send(DagFileParsingResult(fileloc=request.file, serialized_dags=[]))

        mock_parse_task_handler.side_effect = play_runtime(reply)

        result = _run(tmp_path)

        assert result.import_errors == {
            "etl.artifact": "The Lang-SDK runtime exited with code 0 without a parse result"
        }

    @patch.object(
        FakeCoordinator, "parse_task_handler", autospec=True, side_effect=play_runtime(_send_an_invalid_frame)
    )
    def test_killing_the_runtime_is_not_reported_as_out_of_memory(
        self, mock_parse_task_handler, parse, cap_structlog
    ):
        proc = parse()

        assert proc._exit_code == -signal.SIGKILL
        assert not any("Likely out of memory" in str(entry.get("event")) for entry in cap_structlog.entries)

    @patch("airflow.dag_processing.task_handler_processor._EXIT_GRACE_PERIOD", 0.5)
    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_runtime_that_runs_on_after_its_result_is_killed(
        self, mock_parse_task_handler, parse, cap_structlog
    ):
        def reply(request, comms):
            comms.send(_reply_with("extract")(request, comms))
            _block_until_killed(comms)

        mock_parse_task_handler.side_effect = play_runtime(reply)

        proc = parse()

        assert _get_task_ids(proc.parsing_result) == ["extract"]
        assert proc._exit_code == -signal.SIGKILL
        assert "The Lang-SDK runtime did not exit after its parse result; killing it" in cap_structlog

    @pytest.mark.parametrize(
        ("policy", "error"),
        [
            pytest.param(RuntimeError("policy bug"), "RuntimeError: policy bug", id="raises"),
            pytest.param(
                lambda path: "30",
                "TypeError: Value (30) from get_dagbag_import_timeout must be int or float",
                id="not-a-number",
            ),
        ],
    )
    @patch("airflow.settings.get_dagbag_import_timeout", autospec=True)
    def test_a_failing_import_timeout_policy_is_an_import_error(self, mock_timeout, parse, policy, error):
        mock_timeout.side_effect = policy

        proc = parse()

        assert proc.parsing_result.import_errors == {
            "etl.artifact": f"Cannot start the Lang-SDK runtime: {error}"
        }

    @pytest.mark.skipif(not Path("/proc/self/fd").is_dir(), reason="reads /proc")
    @pytest.mark.parametrize("use_exec", [False, True], ids=["fork", "spawn"])
    def test_the_runtime_inherits_only_its_standard_streams(self, monkeypatch, tmp_path, use_exec):
        if use_exec:
            # The spawned interpreter finds the coordinator again from its environment.
            monkeypatch.setattr(supervisor, "_should_use_exec", lambda: True)
            monkeypatch.setenv("PYTHONPATH", os.pathsep.join(sys.path))
            monkeypatch.setenv("AIRFLOW__SDK__COORDINATORS", conf.get("sdk", "coordinators"))
        with selectors.DefaultSelector() as selector:
            proc = _start(tmp_path, selector, argv=["/bin/sh", "-c", "exec sleep 30"])
            deadline = time.monotonic() + 30
            while psutil.Process(proc.pid).name() != "sleep":
                assert time.monotonic() < deadline, "the runtime did not start"
                proc._service_subprocess(max_wait_time=0.1)
            fd_dir = Path(f"/proc/{proc.pid}/fd")
            fds = {fd.name: os.readlink(fd) for fd in fd_dir.iterdir()}
            proc.kill(signal.SIGKILL)
            proc.close()

        assert sorted(fds) == ["0", "1", "2"]
        assert fds["0"] == "/dev/null"


def _probe_from_a_dag_parsing_child() -> None:
    """Stand in for ``_parse_file_entrypoint``: probe the file it is asked to parse, and return the result."""
    comms_decoder = CommsDecoder[ToDagProcessor, ToManager](body_decoder=TypeAdapter(ToDagProcessor))
    request = comms_decoder._get_response()
    assert isinstance(request, DagFileParseRequest)
    task_runner.SUPERVISOR_COMMS = comms_decoder  # type: ignore[assignment]

    result = LangSDKTaskHandlerProcessorProcess.run(
        coordinator="fake",
        path=request.file,
        bundle_path=request.bundle_path,
        bundle_name=request.bundle_name,
        artifact_rel_path="etl.artifact",
        logger=structlog.get_logger(logger_name="task"),
    )
    comms_decoder.send(
        DagFileParsingResult(
            fileloc=request.file, serialized_dags=[], warnings=[result.model_dump(mode="json")]
        )
    )


class TestRequestsWithoutAClient:
    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_request_is_relayed_to_the_parent(self, mock_parse_task_handler, parse, supervisor_comms):
        def reply(request, comms):
            variable = comms.send(GetVariable(key="probe_var"))
            return _reply_with(variable.value)(request, comms)

        mock_parse_task_handler.side_effect = play_runtime(reply, schema_version=OLDEST_SCHEMA_VERSION)
        # As decoded from the parent's frame, which always carries its type.
        supervisor_comms.send.return_value = VariableResult.model_validate(
            {"key": "probe_var", "value": "from-the-parent", "type": "VariableResult"}
        )

        proc = parse()

        assert _get_task_ids(proc.parsing_result) == ["from-the-parent"]
        supervisor_comms.send.assert_called_once_with(GetVariable(key="probe_var"))

    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_the_parent_s_error_reaches_the_runtime(self, mock_parse_task_handler, parse, supervisor_comms):
        def reply(request, comms):
            with pytest.raises(AirflowRuntimeError) as ctx:
                comms.send(GetVariable(key="probe_var"))
            error = ctx.value.error
            return _reply_with(f"{error.error.value}:{error.detail['key']}")(request, comms)

        mock_parse_task_handler.side_effect = play_runtime(reply)
        supervisor_comms.send.side_effect = AirflowRuntimeError(
            ErrorResponse(error=ErrorType.VARIABLE_NOT_FOUND, detail={"key": "probe_var"})
        )

        proc = parse()

        assert _get_task_ids(proc.parsing_result) == [f"{ErrorType.VARIABLE_NOT_FOUND.value}:probe_var"]

    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_request_gets_an_error_without_a_parent(self, mock_parse_task_handler, monkeypatch, tmp_path):
        monkeypatch.delattr(task_runner, "SUPERVISOR_COMMS", raising=False)

        def reply(request, comms):
            with pytest.raises(AirflowRuntimeError) as ctx:
                comms.send(GetVariable(key="probe_var"))
            return _reply_with(ctx.value.error.detail["message"])(request, comms)

        mock_parse_task_handler.side_effect = play_runtime(reply)

        result = _run(tmp_path)

        assert _get_task_ids(result) == ["GetVariable is answered only in the Dag processor"]

    @patch("airflow.sdk.log._secrets_masker", autospec=True)
    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_secret_is_masked_here_and_by_the_parent(
        self, mock_parse_task_handler, mock_secrets_masker, parse, supervisor_comms
    ):
        def reply(request, comms):
            comms.send(MaskSecret(value="probe-secret", name="probe_conn"))
            return _reply_with("extract")(request, comms)

        mock_parse_task_handler.side_effect = play_runtime(reply)

        proc = parse()

        assert _get_task_ids(proc.parsing_result) == ["extract"]
        mock_secrets_masker.return_value.add_mask.assert_called_once_with("probe-secret", "probe_conn")
        supervisor_comms.send.assert_called_once_with(MaskSecret(value="probe-secret", name="probe_conn"))

    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_runtime_request_is_answered_by_the_dag_processor(self, mock_parse_task_handler, tmp_path):
        def reply(request, comms):
            variable = comms.send(GetVariable(key="probe_var"))
            return _reply_with(variable.value)(request, comms)

        mock_parse_task_handler.side_effect = play_runtime(reply)
        client = MagicMock(spec=Client)
        client.variables = MagicMock(spec=VariableOperations)
        client.variables.get.return_value = VariableResponse(key="probe_var", value="from-the-dag-processor")
        artifact = write_artifact(tmp_path / "etl.artifact")

        proc = DagFileProcessorProcess.start(
            id=1,
            path=artifact,
            bundle_path=tmp_path,
            bundle_name="task-handlers",
            dag_file_rel_path="etl.artifact",
            callbacks=[],
            target=_probe_from_a_dag_parsing_child,
            logger=structlog.get_logger(),
            logger_filehandle=io.BytesIO(),
            client=client,
        )
        deadline = time.monotonic() + 30
        while not proc.is_ready:
            assert time.monotonic() < deadline, "the Dag-parsing child did not finish"
            proc._service_subprocess(max_wait_time=0.1)
        proc.close()

        client.variables.get.assert_called_once_with("probe_var")
        [probe_result] = proc.parsing_result.warnings
        assert TaskHandlerParsingResult.model_validate(probe_result) == TaskHandlerParsingResult(
            fileloc=os.fspath(artifact),
            task_handlers={
                "etl": [
                    TaskHandlerDeclaration(task_id="from-the-dag-processor", binding="positional", params=[])
                ]
            },
        )


class TestRun:
    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_returns_every_handler_the_runtime_registers(self, mock_parse_task_handler, tmp_path):
        def reply(request, comms):
            # Echo what the request carries, so the test can check it.
            param = TaskHandlerParam(name=request.bundle_name, value_schema={"type": "string"})
            return TaskHandlerParsingResult(
                fileloc=request.file,
                task_handlers={
                    dag_id: [
                        TaskHandlerDeclaration(
                            task_id=os.fspath(request.bundle_path), binding="positional", params=[param]
                        )
                    ]
                    for dag_id in ("etl", "report")
                },
            )

        mock_parse_task_handler.side_effect = play_runtime(reply)

        result = _run(tmp_path)

        param = TaskHandlerParam(name="task-handlers", value_schema={"type": "string"})
        declaration = TaskHandlerDeclaration(
            task_id=os.fspath(tmp_path), binding="positional", params=[param]
        )
        assert result == TaskHandlerParsingResult(
            fileloc=os.fspath(tmp_path / "etl.artifact"),
            task_handlers={"etl": [declaration], "report": [declaration]},
        )

    @pytest.mark.execution_timeout(30)
    @pytest.mark.parametrize("connected", [False, True], ids=["before-connecting", "after-connecting"])
    @patch("airflow.settings.get_dagbag_import_timeout", autospec=True, return_value=1)
    @patch.object(
        LangSDKTaskHandlerProcessorProcess,
        "close",
        autospec=True,
        side_effect=LangSDKTaskHandlerProcessorProcess.close,
    )
    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_parse_past_the_import_timeout_is_killed(
        self, mock_parse_task_handler, mock_close, mock_timeout, tmp_path, connected
    ):
        mock_parse_task_handler.side_effect = (
            play_runtime(lambda request, comms: _block_until_killed(comms))
            if connected
            else SubprocessCoordinator.parse_task_handler
        )
        # A runtime that never connects leaves both listeners open when it is killed.
        fds_before = _get_open_fds()

        result = _run(tmp_path, argv=["/bin/sh", "-c", "exec sleep 60"])

        assert result.import_errors == {
            "etl.artifact": f"The Lang-SDK runtime did not parse {tmp_path / 'etl.artifact'} within 1.0s"
        }
        [proc] = [c.args[0] for c in mock_close.call_args_list]
        assert proc._exit_code == -9
        assert not proc._open_sockets
        assert _get_open_fds() <= fds_before

    @pytest.mark.execution_timeout(30)
    @patch("airflow.settings.get_dagbag_import_timeout", autospec=True, return_value=1)
    @patch.object(FakeCoordinator, "parse_task_handler", autospec=True)
    def test_a_runtime_that_stops_mid_frame_is_timed_out(
        self, mock_parse_task_handler, mock_timeout, tmp_path
    ):
        def reply(request, comms):
            comms.socket.sendall((100).to_bytes(4, byteorder="big"))
            _block_until_killed(comms)

        mock_parse_task_handler.side_effect = play_runtime(reply)

        result = _run(tmp_path)

        assert result.import_errors == {
            "etl.artifact": f"The Lang-SDK runtime did not parse {tmp_path / 'etl.artifact'} within 1.0s"
        }

    @patch("airflow.settings.get_dagbag_import_timeout", autospec=True, return_value=1)
    @patch.object(
        LangSDKTaskHandlerProcessorProcess,
        "close",
        autospec=True,
        side_effect=LangSDKTaskHandlerProcessorProcess.close,
    )
    def test_the_import_timeout_holds_after_the_runtime_exits(self, mock_close, mock_timeout, tmp_path):
        # The runtime exits, and the process it leaves behind keeps its output open.
        result = _run(tmp_path, argv=["/bin/sh", "-c", "sleep 30 & exit 0"])
        [proc] = [c.args[0] for c in mock_close.call_args_list]
        os.killpg(proc.pid, signal.SIGKILL)

        assert result.import_errors == {
            "etl.artifact": f"The Lang-SDK runtime did not parse {tmp_path / 'etl.artifact'} within 1.0s"
        }
        assert proc._exit_code == 0
        assert not proc._open_sockets

    @pytest.mark.execution_timeout(30)
    @conf_vars({("dag_processor", "dag_file_processor_timeout"): "1"})
    @patch.object(
        FakeCoordinator,
        "_build_parse_task_handler_command",
        autospec=True,
        side_effect=lambda self, *, path: threading.Event().wait(),
    )
    def test_the_dag_file_processor_timeout_applies_until_the_import_timeout_is_reported(
        self, mock_build_command, tmp_path
    ):
        result = _run(tmp_path)

        assert result.import_errors == {
            "etl.artifact": f"The Lang-SDK runtime did not parse {tmp_path / 'etl.artifact'} within 1.0s"
        }


@pytest.mark.parametrize(("configured", "expected"), [(30, 30), (0.5, 0.5), (0, None), (-1, None)])
@patch("airflow.settings.get_dagbag_import_timeout", autospec=True)
def test_only_a_positive_import_timeout_applies(mock_timeout, configured, expected):
    mock_timeout.return_value = configured

    assert _get_import_timeout("/b/etl.artifact") == expected
    mock_timeout.assert_called_once_with("/b/etl.artifact")


def _make_process(**kwargs) -> LangSDKTaskHandlerProcessorProcess:
    return LangSDKTaskHandlerProcessorProcess(
        id=uuid.uuid4(),
        pid=1,
        stdin=MagicMock(spec=socket.socket),
        process=MagicMock(spec=supervisor.ProcessTracker),
        process_log=MagicMock(spec=FilteringBoundLogger),
        selector=MagicMock(spec=selectors.BaseSelector),
        bundle_name="task-handlers",
        dag_file_rel_path="etl.artifact",
        coordinator="fake",
        listeners={},
        parse_request=TaskHandlerParseRequest(
            file="/b/etl.artifact", bundle_path=Path("/b"), bundle_name="task-handlers"
        ),
        **kwargs,
    )


def _build_result(*task_ids: str) -> TaskHandlerParsingResult:
    return TaskHandlerParsingResult(
        fileloc="/b/etl.artifact",
        task_handlers={
            "etl": [
                TaskHandlerDeclaration(task_id=task_id, binding="positional", params=[])
                for task_id in task_ids
            ]
        },
    )


@patch.object(LangSDKTaskHandlerProcessorProcess, "send_msg", autospec=True)
def test_the_first_parse_result_wins(mock_send_msg):
    proc = _make_process()

    proc._handle_request(_build_result("first"), structlog.get_logger(), 1)
    proc._handle_request(_build_result("second"), structlog.get_logger(), 2)

    assert _get_task_ids(proc.parsing_result) == ["first"]
    assert mock_send_msg.call_args.kwargs["error"].detail == {
        "message": "A parse result was already received"
    }


@pytest.mark.parametrize(
    "invalid_frame",
    [
        pytest.param(bytes.fromhex("00000003c1c1c1"), id="does-not-decode"),
        pytest.param(
            _RequestFrame(
                id=2,
                body={
                    "type": "TaskHandlerParsingResult",
                    "fileloc": "/b/etl.artifact",
                    "task_handlers": "none",
                },
            ).as_bytes(),
            id="does-not-validate",
        ),
    ],
)
@patch.object(LangSDKTaskHandlerProcessorProcess, "_kill_runtime", autospec=True)
@patch.object(LangSDKTaskHandlerProcessorProcess, "send_msg", autospec=True)
def test_an_invalid_message_after_the_parse_result_keeps_it(mock_send_msg, mock_kill_runtime, invalid_frame):
    proc = _make_process()
    runtime, conn = socket.socketpair()
    with runtime, conn:
        proc._register_comm(conn)
        read_frame, _ = proc.selector.register.call_args.args[2]
        runtime.sendall(_RequestFrame(id=1, body=_build_result("extract").model_dump(mode="json")).as_bytes())
        assert read_frame(conn)
        runtime.sendall(invalid_frame)
        assert not read_frame(conn)

    assert _get_task_ids(proc.parsing_result) == ["extract"]
    assert proc.parsing_result.import_errors is None
    proc.process_log.warning.assert_called_once_with(
        "Ignoring an invalid message from the Lang-SDK runtime after its parse result", error=ANY
    )
    mock_kill_runtime.assert_called_once_with(proc)


@patch.object(LangSDKTaskHandlerProcessorProcess, "send_msg", autospec=True)
def test_the_schema_version_is_reported_once(mock_send_msg):
    proc = _make_process()

    proc._handle_request(
        LangSDKRuntimeSchemaVersion(schema_version=OLDEST_SCHEMA_VERSION), structlog.get_logger(), 1
    )
    proc._handle_request(LangSDKRuntimeSchemaVersion(schema_version=None), structlog.get_logger(), 2)

    assert proc._runtime_schema_version == OLDEST_SCHEMA_VERSION
    assert mock_send_msg.call_args.kwargs["error"].detail["message"] == "Unhandled request"
