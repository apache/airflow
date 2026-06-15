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
"""Tests for the callback supervisor module."""

from __future__ import annotations

import signal
import socket
import uuid
from dataclasses import dataclass
from operator import attrgetter
from types import SimpleNamespace
from typing import Any
from unittest.mock import ANY, Mock, patch

import pytest
import structlog

from airflow.sdk._shared.timezones import timezone
from airflow.sdk.api.datamodels._generated import DagRun, DagRunState, DagRunType
from airflow.sdk.execution_time.callback_supervisor import (
    CallbackSubprocess,
    Path,
    _render_callback_kwargs,
    execute_callback,
)
from airflow.sdk.execution_time.comms import (
    BundleInfo,
    ConnectionResult,
    ErrorResponse,
    GetConnection,
    GetDagRun,
    GetVariable,
    GetVariableKeys,
    GetXCom,
    MaskSecret,
    VariableKeysResult,
    VariableResult,
    _RequestFrame,
)

# A minimal DagRun instance used in the GetDagRun test case.
_MOCK_DAG_RUN = DagRun(
    dag_id="test_dag",
    run_id="test_run",
    run_after=timezone.parse("2024-01-01T00:00:00+00:00"),
    run_type=DagRunType.MANUAL,
    state=DagRunState.RUNNING,
    consumed_asset_events=[],
)


def callback_no_args():
    """A simple callback that takes no arguments."""
    return "ok"


def callback_with_kwargs(arg1, arg2):
    """A callback that accepts keyword arguments."""
    return f"{arg1}-{arg2}"


def callback_that_raises():
    """A callback that always raises."""
    raise ValueError("something went wrong")


def callback_that_system_exits_zero():
    """A callback that raises SystemExit(0).

    A library called inside a user callback may call ``sys.exit()`` on a soft condition.
    SystemExit is a BaseException, not an Exception, so an ``except Exception`` handler would
    let it propagate out of the forked callback target — exiting the child with code 0, which
    the supervisor reads as SUCCESS, silently recording an aborted callback as completed.
    """
    raise SystemExit(0)


def callback_that_system_exits_nonzero():
    """A callback that raises SystemExit(2)."""
    raise SystemExit(2)


def callback_that_keyboard_interrupts():
    """A callback that raises KeyboardInterrupt (also a BaseException, not Exception)."""
    raise KeyboardInterrupt()


_ASYNC_CALLBACK_BODY_RAN = {"ran": False}


async def async_callback_used_as_sync():
    """An async function — if wrongly used as a SyncCallback body, execute_callback would create
    its coroutine but never await it, so this flag would stay False (silent no-op)."""
    _ASYNC_CALLBACK_BODY_RAN["ran"] = True
    return "async-ran"


class CallableClass:
    """A class that returns a callable instance (like BaseNotifier)."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, context):
        return "notified"


class TestExecuteCallback:
    @pytest.mark.parametrize(
        ("path", "kwargs", "dag_rel_path", "bundle_path", "expect_success", "error_contains"),
        [
            pytest.param(
                f"{__name__}.callback_no_args",
                {},
                Path("test.py"),
                Path("bundle/path"),
                True,
                None,
                id="successful_no_args",
            ),
            pytest.param(
                f"{__name__}.callback_with_kwargs",
                {"arg1": "hello", "arg2": "world"},
                Path("test.py"),
                Path("bundle/path"),
                True,
                None,
                id="successful_with_kwargs",
            ),
            pytest.param(
                f"{__name__}.CallableClass",
                {"msg": "alert"},
                Path("test.py"),
                Path("bundle/path"),
                True,
                None,
                id="callable_class_pattern",
            ),
            pytest.param(
                "",
                {},
                Path("test.py"),
                Path("bundle/path"),
                False,
                "Callback path not found",
                id="empty_path",
            ),
            pytest.param(
                "nonexistent.module.function",
                {},
                Path("test.py"),
                Path("bundle/path"),
                False,
                "ModuleNotFoundError",
                id="import_error",
            ),
            pytest.param(
                f"{__name__}.callback_that_raises",
                {},
                Path("test.py"),
                Path("bundle/path"),
                False,
                "ValueError",
                id="execution_error",
            ),
            pytest.param(
                f"{__name__}.nonexistent_function_xyz",
                {},
                Path("test.py"),
                Path("bundle/path"),
                False,
                "AttributeError",
                id="attribute_error",
            ),
            # BaseException variants must be caught and reported as a failure, not allowed to
            # propagate — otherwise SystemExit(0) makes the forked child exit 0 (false success).
            pytest.param(
                f"{__name__}.callback_that_system_exits_zero",
                {},
                Path("test.py"),
                Path("bundle/path"),
                False,
                "SystemExit",
                id="system_exit_zero_is_failure",
            ),
            pytest.param(
                f"{__name__}.callback_that_system_exits_nonzero",
                {},
                Path("test.py"),
                Path("bundle/path"),
                False,
                "SystemExit",
                id="system_exit_nonzero_is_failure",
            ),
            pytest.param(
                f"{__name__}.callback_that_keyboard_interrupts",
                {},
                Path("test.py"),
                Path("bundle/path"),
                False,
                "KeyboardInterrupt",
                id="keyboard_interrupt_is_failure",
            ),
        ],
    )
    def test_execute_callback(self, path, kwargs, dag_rel_path, bundle_path, expect_success, error_contains):
        log = structlog.get_logger()
        success, error = execute_callback(
            callback_path=path,
            callback_kwargs=kwargs,
            dag_rel_path=dag_rel_path,
            bundle_path=bundle_path,
            log=log,
        )

        assert success is expect_success
        if error_contains:
            assert error_contains in error
        else:
            assert error is None

    def test_async_function_as_sync_callback_silently_noops(self):
        """CHARACTERIZATION (documents CURRENT behavior — see the MEDIUM finding in
        OVERNIGHT-QA-SUMMARY.md): SyncCallback accepts an async function at authoring (tested as
        intended), but ``execute_callback`` invokes it synchronously → it returns an un-awaited
        coroutine, the body NEVER runs, yet the call is reported as SUCCESS. If the design is later
        changed to reject-at-authoring or await-at-runtime, this test should be updated.
        """
        import warnings

        _ASYNC_CALLBACK_BODY_RAN["ran"] = False
        log = structlog.get_logger()

        with warnings.catch_warnings():
            warnings.simplefilter("ignore")  # suppress "coroutine was never awaited"
            success, error = execute_callback(
                callback_path=f"{__name__}.async_callback_used_as_sync",
                callback_kwargs={},
                dag_rel_path=Path("test.py"),
                bundle_path=Path("bundle/path"),
                log=log,
            )

        # CURRENT behavior: reported success, but the async body never executed (coroutine discarded).
        assert success is True
        assert error is None
        assert _ASYNC_CALLBACK_BODY_RAN["ran"] is False, (
            "CHARACTERIZATION: the async body silently did not run; if it now runs, the "
            "SyncCallback/async handling was fixed — update this test"
        )


class TestCallbackHandleRequest:
    """Verify that CallbackSubprocess._handle_request dispatches each message type to the correct handler."""

    @dataclass
    class ClientMock:
        method_path: str
        args: tuple = ()
        kwargs: dict | None = None
        response: Any = None

        def __post_init__(self):
            if self.kwargs is None:
                self.kwargs = {}

    @dataclass
    class RequestCase:
        message: Any
        test_id: str
        client_mock: Any = None  # Should be ClientMock but Python can't forward-ref sibling nested classes
        mask_secret_args: tuple | None = None

    REQUEST_CASES = [
        RequestCase(
            message=GetConnection(conn_id="test_conn"),
            test_id="get_connection",
            client_mock=ClientMock(
                method_path="connections.get",
                args=("test_conn",),
                response=ConnectionResult(conn_id="test_conn", conn_type="mysql"),
            ),
        ),
        RequestCase(
            message=GetConnection(conn_id="test_conn"),
            test_id="get_connection_with_password",
            client_mock=ClientMock(
                method_path="connections.get",
                args=("test_conn",),
                response=ConnectionResult(conn_id="test_conn", conn_type="mysql", password="secret"),
            ),
            mask_secret_args=("secret",),
        ),
        RequestCase(
            message=GetDagRun(dag_id="test_dag", run_id="test_run"),
            test_id="get_dag_run",
            client_mock=ClientMock(
                method_path="dag_runs.get_detail",
                args=("test_dag", "test_run"),
                response=_MOCK_DAG_RUN,
            ),
        ),
        RequestCase(
            message=GetVariable(key="test_key"),
            test_id="get_variable",
            client_mock=ClientMock(
                method_path="variables.get",
                args=("test_key",),
                response=VariableResult(key="test_key", value="test_value"),
            ),
        ),
        RequestCase(
            message=GetVariableKeys(prefix="test_"),
            test_id="get_variable_keys",
            client_mock=ClientMock(
                method_path="variables.keys",
                kwargs={"prefix": "test_", "limit": 1000, "offset": 0},
                response=VariableKeysResult(keys=["test_key"], total_entries=1),
            ),
        ),
        RequestCase(
            message=MaskSecret(value="super_secret", name="api_key"),
            test_id="mask_secret",
            mask_secret_args=("super_secret", "api_key"),
        ),
    ]

    @pytest.fixture
    def callback_subprocess(self, mocker):
        read_end, write_end = socket.socketpair()
        proc = CallbackSubprocess(
            process_log=mocker.MagicMock(),
            id="12345678-1234-5678-1234-567812345678",
            pid=12345,
            stdin=write_end,
            client=mocker.Mock(),
            process=mocker.Mock(),
        )
        return proc, read_end

    @patch("airflow.sdk.execution_time.request_handlers.mask_secret")
    @pytest.mark.parametrize("test_case", REQUEST_CASES, ids=lambda tc: tc.test_id)
    def test_handle_requests(
        self,
        mock_mask_secret,
        callback_subprocess,
        mocker,
        test_case,
    ):
        client_mock = test_case.client_mock

        proc, _read_end = callback_subprocess

        if client_mock:
            mock_client_method = attrgetter(client_mock.method_path)(proc.client)
            mock_client_method.return_value = client_mock.response

        generator = proc.handle_requests(log=mocker.Mock())
        next(generator)

        req_frame = _RequestFrame(id=42, body=test_case.message.model_dump())
        generator.send(req_frame)

        if test_case.mask_secret_args is not None:
            mock_mask_secret.assert_called_with(*test_case.mask_secret_args)

        if client_mock:
            mock_client_method.assert_called_once_with(*client_mock.args, **client_mock.kwargs)

    def test_unhandled_request_replies_with_400_and_does_not_crash(self, callback_subprocess, mocker):
        """Graceful degradation: a request type NOT in CallbackToSupervisor (e.g. an XCom read,
        which is GetXCom — present in ToSupervisor but absent from the callback union on this
        branch) must hit the ``else`` branch of ``_handle_request`` and get a clean
        ``ErrorResponse(status_code=400, "Unhandled request")``.

        The contract being pinned (the adversarial concern): the supervisor REPLIES — so the
        callback's blocking ``send()`` gets an answer back over the socket and does not hang
        forever — and RETURNS without raising, so one bad request does not crash the supervisor's
        request loop. We read the actual response frame off the real socketpair (rather than
        mocking send_msg, which is read-only on the slotted attrs class) to prove a reply is
        physically written to the wire.
        """
        import msgspec

        from airflow.sdk.execution_time.comms import _ResponseFrame

        proc, read_end = callback_subprocess

        # An XCom read is exactly the unhandled case on #66608: GetXCom is a real ToSupervisor
        # message but is NOT a member of CallbackToSupervisor, so it falls through the if/elif chain.
        unhandled_msg = GetXCom(key="my_key", dag_id="test_dag", run_id="test_run", task_id="test_task")

        # Call _handle_request directly: the decoder's discriminated union would reject GetXCom
        # before dispatch, so directly invoking the handler is what exercises the else branch.
        # It must not raise — a raise here would crash the supervisor's request loop.
        proc._handle_request(unhandled_msg, log=mocker.Mock(), req_id=99)

        # A reply was physically written to the socket (no hang): read the length-prefixed frame.
        read_end.settimeout(5)
        prefix = read_end.recv(4)
        assert len(prefix) == 4, "no response frame was written — the client would hang"
        n = int.from_bytes(prefix, byteorder="big")
        payload = b""
        while len(payload) < n:
            payload += read_end.recv(n - len(payload))

        frame = msgspec.msgpack.Decoder(_ResponseFrame).decode(payload)

        assert frame.id == 99
        assert frame.body is None
        # The error frame carries a 400 "Unhandled request" — graceful rejection.
        error = ErrorResponse(**frame.error)
        assert error.detail["status_code"] == 400
        assert error.detail["message"] == "Unhandled request"


class TestConfigureLogging:
    """Tests for _configure_logging remote logging connection setup."""

    def test_configure_logging_uses_remote_logging_conn(self, tmp_path, mocker):
        """Verify that _remote_logging_conn is invoked with the client during logging setup."""
        from airflow.sdk.execution_time.callback_supervisor import _configure_logging

        mock_client = mocker.Mock()
        log_path = str(tmp_path / "callback.log")

        mock_remote_conn = mocker.patch(
            "airflow.sdk.execution_time.supervisor._remote_logging_conn",
        )

        logger, fd = _configure_logging(log_path, mock_client)
        fd.close()

        mock_remote_conn.assert_called_once_with(mock_client)


class TestUploadLogs:
    """Tests for CallbackSubprocess._upload_logs."""

    @pytest.fixture
    def callback_subprocess(self, mocker):
        read_end, write_end = socket.socketpair()
        proc = CallbackSubprocess(
            process_log=mocker.MagicMock(),
            id="12345678-1234-5678-1234-567812345678",
            pid=12345,
            stdin=write_end,
            client=mocker.Mock(),
            process=mocker.Mock(),
        )
        yield proc
        read_end.close()
        write_end.close()

    def test_wait_calls_upload_logs_after_subprocess_completes(self, callback_subprocess, mocker):
        """wait() should call _upload_logs() after the subprocess finishes."""
        mock_upload = mocker.patch(
            "airflow.sdk.execution_time.callback_supervisor.CallbackSubprocess._upload_logs"
        )
        mocker.patch("airflow.sdk.execution_time.callback_supervisor.CallbackSubprocess._monitor_subprocess")
        mocker.patch.object(callback_subprocess, "selector")

        callback_subprocess.wait()

        mock_upload.assert_called_once()

    def test_upload_logs_delegates_to_upload_to_remote(self, callback_subprocess, mocker):
        """_upload_logs calls upload_to_remote with the process logger and no ti."""
        mock_upload = mocker.patch("airflow.sdk.log.upload_to_remote")
        mocker.patch("airflow.sdk.execution_time.supervisor._remote_logging_conn")

        callback_subprocess._upload_logs()

        mock_upload.assert_called_once_with(callback_subprocess.process_log)

    def test_upload_logs_failure_is_swallowed(self, callback_subprocess, mocker):
        """Upload failures must not propagate — callback exit code should still be returned."""
        mocker.patch(
            "airflow.sdk.log.upload_to_remote",
            side_effect=RuntimeError("S3 unreachable"),
        )
        mocker.patch("airflow.sdk.execution_time.supervisor._remote_logging_conn")

        callback_subprocess._upload_logs()

    def test_upload_logs_no_remote_logging_configured(self, callback_subprocess, mocker):
        """When remote logging is not configured, _upload_logs completes without error."""
        mock_load_handler = mocker.patch("airflow.sdk.log.load_remote_log_handler", return_value=None)
        mocker.patch("airflow.sdk.execution_time.supervisor._remote_logging_conn")

        callback_subprocess._upload_logs()

        mock_load_handler.assert_called_once()


class TestCallbackExecutionTimeout:
    """Tests for the callback_execution_timeout config enforcement."""

    @pytest.fixture
    def callback_subprocess(self, mocker):
        read_end, write_end = socket.socketpair()
        proc = CallbackSubprocess(
            process_log=mocker.MagicMock(),
            id="12345678-1234-5678-1234-567812345678",
            pid=12345,
            stdin=write_end,
            client=mocker.Mock(),
            process=mocker.Mock(),
        )
        yield proc
        read_end.close()
        write_end.close()

    def test_timeout_zero_does_not_kill(self, callback_subprocess, mocker):
        """When timeout=0, no kill is issued regardless of how long the subprocess runs."""
        proc = callback_subprocess

        call_count = 0

        def fake_service_subprocess(self_arg, **kwargs):
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                proc._exit_code = 0
            return proc._exit_code

        mocker.patch.object(
            CallbackSubprocess, "_service_subprocess", autospec=True, side_effect=fake_service_subprocess
        )
        mocker.patch.object(
            CallbackSubprocess, "_get_callback_execution_timeout", autospec=True, return_value=0
        )
        mock_kill = mocker.patch.object(CallbackSubprocess, "kill", autospec=True)

        proc._monitor_subprocess()

        mock_kill.assert_not_called()
        assert proc._exit_code == 0

    def test_timeout_kills_long_running_subprocess(self, callback_subprocess, mocker):
        """When timeout>0 and the subprocess exceeds the timeout, it is killed."""
        proc = callback_subprocess

        time_values = iter([100.0, 100.0, 106.0])
        mocker.patch("airflow.sdk.execution_time.callback_supervisor.time.monotonic", side_effect=time_values)

        mocker.patch.object(CallbackSubprocess, "_service_subprocess", autospec=True, return_value=None)
        mocker.patch.object(
            CallbackSubprocess, "_get_callback_execution_timeout", autospec=True, return_value=5
        )
        mock_kill = mocker.patch.object(CallbackSubprocess, "kill", autospec=True)

        proc._monitor_subprocess()

        mock_kill.assert_called_once_with(proc, signal.SIGTERM, escalation_delay=5.0, force=True)

    def test_timeout_kill_uses_sigkill_escalation(self, callback_subprocess, mocker):
        """force=True with SIGTERM must build an escalation path that ends in SIGKILL.

        This guards the adversarial case where a callback ignores SIGTERM: the kill()
        contract is that with force=True the signal escalates SIGTERM -> SIGKILL, so a
        wedged subprocess cannot leak past the timeout.
        """
        proc = callback_subprocess

        time_values = iter([100.0, 100.0, 106.0])
        mocker.patch("airflow.sdk.execution_time.callback_supervisor.time.monotonic", side_effect=time_values)
        mocker.patch.object(CallbackSubprocess, "_service_subprocess", autospec=True, return_value=None)
        mocker.patch.object(
            CallbackSubprocess, "_get_callback_execution_timeout", autospec=True, return_value=5
        )

        captured = {}

        def fake_kill(self_arg, signal_to_send, escalation_delay=5.0, force=False):
            # Re-derive the escalation path exactly as WatchedSubprocess.kill does so we
            # assert the real escalation contract rather than the (mocked) signal delivery.
            escalation_path = [signal.SIGINT, signal.SIGTERM, signal.SIGKILL]
            if force and signal_to_send in escalation_path:
                escalation_path = escalation_path[escalation_path.index(signal_to_send) :]
            else:
                escalation_path = [signal_to_send]
            captured["path"] = escalation_path

        mocker.patch.object(CallbackSubprocess, "kill", autospec=True, side_effect=fake_kill)

        proc._monitor_subprocess()

        assert captured["path"] == [signal.SIGTERM, signal.SIGKILL]

    def test_wait_reports_failure_when_timeout_kill_leaves_exit_code_none(self, callback_subprocess, mocker):
        """End-to-end: a timed-out callback must surface as a non-zero exit code.

        When the timeout branch fires and kill() returns without _exit_code being set
        (e.g. the kill is still escalating, or the process record is gone), wait() must
        default _exit_code to 1 so the callback is recorded as FAILURE, never SUCCESS.
        """
        proc = callback_subprocess

        time_values = iter([100.0, 100.0, 106.0])
        mocker.patch("airflow.sdk.execution_time.callback_supervisor.time.monotonic", side_effect=time_values)
        mocker.patch.object(CallbackSubprocess, "_service_subprocess", autospec=True, return_value=None)
        mocker.patch.object(
            CallbackSubprocess, "_get_callback_execution_timeout", autospec=True, return_value=5
        )
        # kill() that does NOT set _exit_code, mimicking a SIGTERM still in flight.
        mock_kill = mocker.patch.object(CallbackSubprocess, "kill", autospec=True)
        mock_upload = mocker.patch.object(CallbackSubprocess, "_upload_logs", autospec=True)
        mocker.patch.object(proc, "selector")

        exit_code = proc.wait()

        mock_kill.assert_called_once_with(proc, signal.SIGTERM, escalation_delay=5.0, force=True)
        assert exit_code == 1
        assert proc._exit_code == 1
        # Logs are still uploaded even on the timeout/failure path.
        mock_upload.assert_called_once()

    def test_wait_preserves_nonzero_exit_code_from_killed_process(self, callback_subprocess, mocker):
        """If kill() does set a (negative) signal exit code, wait() must not overwrite it with 1."""
        proc = callback_subprocess

        time_values = iter([100.0, 100.0, 106.0])
        mocker.patch("airflow.sdk.execution_time.callback_supervisor.time.monotonic", side_effect=time_values)
        mocker.patch.object(CallbackSubprocess, "_service_subprocess", autospec=True, return_value=None)
        mocker.patch.object(
            CallbackSubprocess, "_get_callback_execution_timeout", autospec=True, return_value=5
        )

        def fake_kill(self_arg, *args, **kwargs):
            # Simulate the process being reaped as killed by SIGTERM.
            self_arg._exit_code = -int(signal.SIGTERM)

        mocker.patch.object(CallbackSubprocess, "kill", autospec=True, side_effect=fake_kill)
        mocker.patch.object(CallbackSubprocess, "_upload_logs", autospec=True)
        mocker.patch.object(proc, "selector")

        exit_code = proc.wait()

        # Non-zero (failure), and the specific signal code is preserved, not clobbered to 1.
        assert exit_code == -int(signal.SIGTERM)
        assert exit_code != 0


class TestCallbackSubprocessStart:
    """Verify that CallbackSubprocess.start() properly initializes and executes the callback target."""

    @pytest.fixture
    def mock_client(self):
        """Mock HTTP client."""
        return Mock()

    @pytest.fixture
    def base_start_kwargs(self, mock_client):
        """Base kwargs."""
        return {
            "id": str(uuid.uuid4()),
            "callback_path": "my_module.my_callback_function",
            "callback_kwargs": {"param1": "value1", "param2": 1},
            "dag_rel_path": Path("dags/my_test_dag.py"),
            "bundle_info": None,
            "client": mock_client,
        }

    @pytest.fixture(autouse=True)
    def base_mocks_setup(self, mock_supervisor_comms):
        """Base mocks for all tests in this class."""
        with (
            patch("airflow.sdk.execution_time.comms.CommsDecoder") as mock_comms,
            patch("airflow.sdk.execution_time.callback_supervisor.WatchedSubprocess.start") as mock_super,
            patch("airflow.sdk.execution_time.callback_supervisor.execute_callback") as mock_execute,
        ):
            mock_execute.return_value = (True, None)

            self.mock_comms_decoder = mock_comms
            self.mock_super_start = mock_super
            self.mock_execute_callback = mock_execute
            yield

    @pytest.fixture
    def mock_bundle_setup(self):
        """Setup bundle-related mocks."""
        with patch("airflow.dag_processing.bundles.manager.DagBundlesManager") as mock_manager_class:
            mock_bundle = Mock()
            bundle_path = Path("/path/to/bundle")
            mock_bundle.path = bundle_path
            mock_bundle.name = "test-bundle"

            mock_bundle_manager = Mock()
            mock_manager_class.return_value = mock_bundle_manager
            mock_bundle_manager.get_bundle.return_value = mock_bundle

            yield {
                "manager_class": mock_manager_class,
                "manager": mock_bundle_manager,
                "bundle": mock_bundle,
                "bundle_path": bundle_path,
            }

    def test_execute_callback_receives_correct_parameters(self, base_start_kwargs):
        """Test that execute_callback receives the correct parameters."""
        CallbackSubprocess.start(**base_start_kwargs)
        self.mock_super_start.call_args.kwargs["target"]()

        self.mock_super_start.assert_called_with(
            id=uuid.UUID(base_start_kwargs["id"]), client=base_start_kwargs["client"], target=ANY, logger=None
        )

        self.mock_execute_callback.assert_called_with(
            bundle_path=None,
            callback_kwargs=base_start_kwargs["callback_kwargs"],
            callback_path=base_start_kwargs["callback_path"],
            dag_rel_path=base_start_kwargs["dag_rel_path"],
            log=ANY,
        )

    def test_execute_callback_with_bundle_info_should_pass_correct_parameters(
        self, base_start_kwargs, mock_bundle_setup
    ):
        """Test that execute_callback receives the correct parameters when bundle_info is provided."""
        bundle_info = BundleInfo(name="test-bundle", version="1.0")
        adjusted_kwargs = {**base_start_kwargs, "bundle_info": bundle_info}

        CallbackSubprocess.start(**adjusted_kwargs)
        self.mock_super_start.call_args.kwargs["target"]()

        self.mock_super_start.assert_called_with(
            id=uuid.UUID(adjusted_kwargs["id"]), client=adjusted_kwargs["client"], target=ANY, logger=None
        )

        mock_bundle_setup["manager"].get_bundle.assert_called_once_with(
            name=bundle_info.name,
            version=bundle_info.version,
        )
        mock_bundle_setup["bundle"].initialize.assert_called_once()

        self.mock_execute_callback.assert_called_with(
            bundle_path=str(mock_bundle_setup["bundle_path"]),
            callback_kwargs=adjusted_kwargs["callback_kwargs"],
            callback_path=adjusted_kwargs["callback_path"],
            dag_rel_path=adjusted_kwargs["dag_rel_path"],
            log=ANY,
        )

    def test_context_includes_deadline_metadata(self, base_start_kwargs):
        """When dag_id/run_id are provided, the fetched context is passed to the callback, and
        deadline_id/deadline_time are injected as context['deadline'] — the core deliverable of
        the context-fetch PR (giving a deadline callback access to its own deadline metadata)."""
        deadline_id = str(uuid.uuid4())
        deadline_time = "2026-06-13T00:00:00+00:00"
        adjusted = {
            **base_start_kwargs,
            "dag_id": "my_dag",
            "run_id": "my_run",
            "deadline_id": deadline_id,
            "deadline_time": deadline_time,
        }

        with patch(
            "airflow.sdk.execution_time.callback_supervisor._fetch_and_build_context",
            return_value={"dag_run": Mock(), "run_id": "my_run"},
        ) as mock_fetch:
            CallbackSubprocess.start(**adjusted)
            self.mock_super_start.call_args.kwargs["target"]()

        mock_fetch.assert_called_once()
        passed_kwargs = self.mock_execute_callback.call_args.kwargs["callback_kwargs"]
        assert "context" in passed_kwargs
        assert passed_kwargs["context"]["deadline"] == {
            "id": deadline_id,
            "deadline_time": deadline_time,
        }

    def test_context_has_no_deadline_key_when_metadata_absent(self, base_start_kwargs):
        """With dag_id/run_id but no deadline_id/deadline_time, the context is still passed but
        carries no 'deadline' key (a non-deadline callback that just needs DagRun context)."""
        adjusted = {**base_start_kwargs, "dag_id": "my_dag", "run_id": "my_run"}

        with patch(
            "airflow.sdk.execution_time.callback_supervisor._fetch_and_build_context",
            return_value={"dag_run": Mock(), "run_id": "my_run"},
        ):
            CallbackSubprocess.start(**adjusted)
            self.mock_super_start.call_args.kwargs["target"]()

        passed_kwargs = self.mock_execute_callback.call_args.kwargs["callback_kwargs"]
        assert "context" in passed_kwargs
        assert "deadline" not in passed_kwargs["context"]

    def test_callback_supervisor_with_bundle_info_should_adjust_sys_path(
        self, base_start_kwargs, mock_bundle_setup
    ):
        """Test that bundle_path is added to sys.path when bundle path is provided."""
        with patch("sys.path", new_callable=list) as mock_sys_path:
            bundle_info = BundleInfo(name="test-bundle", version="1.0")
            adjusted_kwargs = {**base_start_kwargs, "bundle_info": bundle_info}

            CallbackSubprocess.start(**adjusted_kwargs)
            self.mock_super_start.call_args.kwargs["target"]()

            assert str(mock_bundle_setup["bundle_path"]) in mock_sys_path

    def test_callback_supervisor_should_exit_on_error(self, base_start_kwargs):
        """Test that callback supervisor exits if execute_callback returns an error."""
        self.mock_execute_callback.return_value = (False, "Some error occurred")

        CallbackSubprocess.start(**base_start_kwargs)

        with pytest.raises(SystemExit) as exc_info:
            self.mock_super_start.call_args.kwargs["target"]()

        assert exc_info.value.code == 1


class TestFetchAndBuildContext:
    """Tests for _fetch_and_build_context."""

    def test_returns_none_on_unexpected_response(self, mocker):
        """When the comms channel returns a non-DagRunResult, context should be None."""
        from airflow.sdk.execution_time.callback_supervisor import _fetch_and_build_context
        from airflow.sdk.execution_time.comms import ErrorResponse

        mock_comms = mocker.Mock()
        mock_comms.send.return_value = ErrorResponse(
            error="API_SERVER_ERROR",
            detail={"status_code": 404, "message": "DagRun not found"},
        )
        log = structlog.get_logger()

        result = _fetch_and_build_context(mock_comms, "missing_dag", "missing_run", log)

        assert result is None

    def test_returns_none_on_exception(self, mocker):
        """When the comms channel raises an exception, context should be None."""
        from airflow.sdk.execution_time.callback_supervisor import _fetch_and_build_context

        mock_comms = mocker.Mock()
        mock_comms.send.side_effect = RuntimeError("connection lost")
        log = structlog.get_logger()

        result = _fetch_and_build_context(mock_comms, "dag_1", "run_1", log)

        assert result is None

    def test_returns_context_dict_on_success(self, mocker):
        """When DagRunResult is returned, a context dict is built."""
        from airflow.sdk.execution_time.callback_supervisor import _fetch_and_build_context
        from airflow.sdk.execution_time.comms import DagRunResult

        mock_comms = mocker.Mock()
        mock_comms.send.return_value = DagRunResult(
            dag_id="test_dag",
            run_id="test_run",
            run_after=timezone.parse("2024-01-01T00:00:00+00:00"),
            run_type="manual",
            state="running",
            consumed_asset_events=[],
        )
        log = structlog.get_logger()

        result = _fetch_and_build_context(mock_comms, "test_dag", "test_run", log)

        assert result is not None
        assert result["dag_run"].dag_id == "test_dag"
        assert result["dag_run"].run_id == "test_run"

    def test_subprocess_exits_when_context_is_none(self, mocker):
        """When _fetch_and_build_context returns None, the subprocess target calls sys.exit(1)."""
        from airflow.sdk.execution_time.callback_supervisor import _fetch_and_build_context

        # Verify that the function returns None in error cases, which triggers sys.exit(1)
        # in the _target closure. Testing the actual fork/exit is beyond unit test scope,
        # but we can verify the contract: None return → caller exits.
        mock_comms = mocker.Mock()
        mock_comms.send.side_effect = RuntimeError("boom")
        log = structlog.get_logger()

        result = _fetch_and_build_context(mock_comms, "dag", "run", log)
        assert result is None


class TestLoadMangledModule:
    """Tests for _load_mangled_module."""

    def test_loads_real_file_under_mangled_name(self, tmp_path):
        import sys

        from airflow.sdk._shared.module_loading import load_mangled_dag_module

        stem = "my_dag"
        mod_name = f"unusual_prefix_{'a' * 40}_{stem}"
        (tmp_path / f"{stem}.py").write_text("def my_callback(): return 42\n")

        result = load_mangled_dag_module(mod_name, str(tmp_path / f"{stem}.py"))

        assert result is True
        assert mod_name in sys.modules
        assert sys.modules[mod_name].my_callback() == 42
        sys.modules.pop(mod_name)

    def test_returns_false_when_file_missing(self, tmp_path):
        from airflow.sdk._shared.module_loading import load_mangled_dag_module

        result = load_mangled_dag_module(
            "unusual_prefix_" + "b" * 40 + "_absent",
            str(tmp_path / "absent.py"),
        )

        assert result is False

    def test_returns_false_on_syntax_error(self, tmp_path):
        import sys

        from airflow.sdk._shared.module_loading import load_mangled_dag_module

        stem = "bad_dag"
        mod_name = f"unusual_prefix_{'c' * 40}_{stem}"
        (tmp_path / f"{stem}.py").write_text("def broken(: pass\n")  # syntax error

        result = load_mangled_dag_module(mod_name, str(tmp_path / f"{stem}.py"))

        assert result is False
        assert mod_name not in sys.modules

    def test_skips_registration_when_already_in_sys_modules(self, tmp_path, mocker):
        import sys

        from airflow.sdk.execution_time.callback_supervisor import _register_unusual_prefix_module
        from airflow.utils.file import get_unique_dag_module_name

        stem = "cached_dag"
        (tmp_path / f"{stem}.py").write_text("def fn(): return 'cached'\n")
        mod_name = get_unique_dag_module_name(str(tmp_path / f"{stem}.py"))

        log = mocker.Mock()
        _register_unusual_prefix_module(f"{mod_name}.fn", tmp_path, log)
        assert mod_name in sys.modules

        # Second call should be a no-op; module should not be re-loaded
        (tmp_path / f"{stem}.py").write_text("def fn(): return 'new'\n")
        _register_unusual_prefix_module(f"{mod_name}.fn", tmp_path, log)

        assert sys.modules[mod_name].fn() == "cached"
        sys.modules.pop(mod_name)

    def test_finds_file_with_dashes_and_dots_in_name(self, tmp_path, mocker):
        """File with dashes/dots (my-dag.file.py) is found despite sanitized stem."""
        import sys

        from airflow.sdk.execution_time.callback_supervisor import _register_unusual_prefix_module
        from airflow.utils.file import get_unique_dag_module_name

        # File with dashes and dots: stem "my-dag.file" sanitizes to "my_dag_file"
        dag_file = tmp_path / "my-dag.file.py"
        dag_file.write_text("def callback(): return 'dashed'\n")
        mod_name = get_unique_dag_module_name(str(dag_file))

        # Verify the stem was sanitized
        assert "my_dag_file" in mod_name

        log = mocker.Mock()
        _register_unusual_prefix_module(f"{mod_name}.callback", tmp_path, log)

        assert mod_name in sys.modules
        assert sys.modules[mod_name].callback() == "dashed"
        sys.modules.pop(mod_name)


class TestRenderCallbackKwargs:
    """Tests for _render_callback_kwargs Jinja template rendering."""

    def test_renders_template_in_string_value(self):
        from airflow.sdk.execution_time.callback_supervisor import _render_callback_kwargs

        kwargs = {"text": "DAG {{ dag_run.dag_id }} missed deadline", "context": {"dag_run": "x"}}
        context = {"dag_run": {"dag_id": "my_dag", "run_id": "run_1"}}
        log = structlog.get_logger()

        result = _render_callback_kwargs(kwargs, context, log)

        assert result["text"] == "DAG my_dag missed deadline"
        assert result["context"] == {"dag_run": "x"}

    def test_skips_non_string_values(self):
        from airflow.sdk.execution_time.callback_supervisor import _render_callback_kwargs

        kwargs = {"count": 42, "items": ["a", "b"], "flag": True}
        context = {"dag_run": {"dag_id": "test"}}
        log = structlog.get_logger()

        result = _render_callback_kwargs(kwargs, context, log)

        assert result == kwargs

    def test_skips_strings_without_template_markers(self):
        from airflow.sdk.execution_time.callback_supervisor import _render_callback_kwargs

        kwargs = {"channel": "#alerts", "text": "plain message"}
        context = {"dag_run": {"dag_id": "test"}}
        log = structlog.get_logger()

        result = _render_callback_kwargs(kwargs, context, log)

        assert result == kwargs

    def test_renders_deadline_context(self):
        from airflow.sdk.execution_time.callback_supervisor import _render_callback_kwargs

        kwargs = {"text": "Deadline {{ deadline.deadline_time }} missed for {{ dag_run.dag_id }}"}
        context = {
            "deadline": {"id": "dl-123", "deadline_time": "2026-06-01T07:00:00Z"},
            "dag_run": {"dag_id": "production_etl"},
        }
        log = structlog.get_logger()

        result = _render_callback_kwargs(kwargs, context, log)

        assert result["text"] == "Deadline 2026-06-01T07:00:00Z missed for production_etl"

    def test_graceful_on_invalid_template(self):
        from airflow.sdk.execution_time.callback_supervisor import _render_callback_kwargs

        kwargs = {"text": "{{ invalid. }}"}
        context = {"dag_run": {"dag_id": "test"}}
        log = structlog.get_logger()

        result = _render_callback_kwargs(kwargs, context, log)

        assert result["text"] == "{{ invalid. }}"

    def test_renders_multiple_kwargs(self):
        from airflow.sdk.execution_time.callback_supervisor import _render_callback_kwargs

        kwargs = {
            "subject": "Alert: {{ dag_run.dag_id }}",
            "body": "Deadline {{ deadline.id }} fired",
            "channel": "#ops",
        }
        context = {
            "dag_run": {"dag_id": "payments"},
            "deadline": {"id": "dl-456"},
        }
        log = structlog.get_logger()

        result = _render_callback_kwargs(kwargs, context, log)

        assert result["subject"] == "Alert: payments"
        assert result["body"] == "Deadline dl-456 fired"
        assert result["channel"] == "#ops"


class TestRenderCallbackKwargsAdversarial:
    """Adversarial QA-loop tests for _render_callback_kwargs (mutation/SSTI/missing-key/guard edges)."""

    @pytest.fixture
    def context(self):
        return {
            "dag_run": SimpleNamespace(run_id="manual__2026-06-10"),
            "ds": "2026-06-10",
        }

    def test_renders_string_kwarg_using_context(self, context):
        """A string kwarg containing {{ }} is rendered against the context."""
        log = Mock()
        result = _render_callback_kwargs({"msg": "Deadline {{ dag_run.run_id }} missed"}, context, log)
        assert result["msg"] == "Deadline manual__2026-06-10 missed"
        log.warning.assert_not_called()

    @pytest.mark.parametrize(
        "value",
        [42, 3.14, True, None, {"nested": "{{ dag_run.run_id }}"}, ["{{ ds }}"], ("{{ ds }}",)],
    )
    def test_non_string_values_pass_through_untouched(self, context, value):
        """Non-string kwargs (int/float/bool/None/dict/list/tuple) are returned unchanged."""
        log = Mock()
        result = _render_callback_kwargs({"arg": value}, context, log)
        assert result["arg"] == value
        log.warning.assert_not_called()

    def test_context_key_is_never_rendered(self, context):
        """The reserved 'context' key is passed through even if it looks templatey."""
        log = Mock()
        sentinel = "{{ dag_run.run_id }}"
        result = _render_callback_kwargs({"context": sentinel}, context, log)
        assert result["context"] == sentinel
        log.warning.assert_not_called()

    def test_string_without_markers_passes_through(self, context):
        """A plain string with no {{ markers is returned verbatim, never rendered."""
        log = Mock()
        result = _render_callback_kwargs({"msg": "no templates here"}, context, log)
        assert result["msg"] == "no templates here"
        log.warning.assert_not_called()

    @pytest.mark.parametrize(
        "broken",
        ["{{ undefined.attr.fails() }}", "{{ 1/0 }}", "{{ dag_run.does_not_exist.boom }}"],
    )
    def test_broken_template_does_not_raise_and_returns_raw(self, context, broken):
        """A template that errors at render time returns the raw value and logs a warning."""
        log = Mock()
        result = _render_callback_kwargs({"msg": broken}, context, log)
        assert result["msg"] == broken
        log.warning.assert_called_once()

    def test_does_not_mutate_input_dict(self, context):
        """Rendering builds a new dict and leaves the caller's kwargs untouched."""
        log = Mock()
        original = {"msg": "Run {{ dag_run.run_id }}"}
        result = _render_callback_kwargs(original, context, log)
        assert original == {"msg": "Run {{ dag_run.run_id }}"}
        assert result is not original
        assert result["msg"] == "Run manual__2026-06-10"

    def test_missing_context_key_renders_empty_not_raises(self, context):
        """A reference to an absent top-level context key renders to empty string."""
        log = Mock()
        result = _render_callback_kwargs({"msg": "value=[{{ missing_key }}]"}, context, log)
        assert result["msg"] == "value=[]"
        log.warning.assert_not_called()

    def test_jinja_statement_block_is_skipped(self, context):
        """A string with only {% %} (no {{ }}) is skipped by the marker guard, left raw."""
        log = Mock()
        raw = "{% if dag_run %}hit{% endif %}"
        result = _render_callback_kwargs({"msg": raw}, context, log)
        assert result["msg"] == raw
        log.warning.assert_not_called()

    def test_ssti_payload_is_rendered(self, context):
        """SSTI-style payload IS evaluated by the Template engine (documents the behavior)."""
        log = Mock()
        result = _render_callback_kwargs({"msg": "{{ ''.__class__.__mro__ }}"}, context, log)
        # Sandbox is NOT used -> attribute access succeeds and the tuple is rendered.
        assert "object" in result["msg"]


class TestJinjaContextIntegration:
    """
    Integration tests coupling the two newly-added features:

    1. ``build_context_from_dag_run`` (context.py) which constructs the real context dict.
    2. ``_render_callback_kwargs`` (callback_supervisor.py) which Jinja-renders kwargs.

    These verify the ACTUAL fields produced by ``build_context_from_dag_run`` render
    correctly when a deadline-callback kwarg references them, rather than rendering
    against a hand-rolled stub dict (which is what the unit tests above do).
    """

    @pytest.fixture
    def logical_date(self):
        # Aware datetime so coerce_datetime keeps the tz; a non-midnight time so we can
        # distinguish ds (date only) from ts (full isoformat) and the _nodash variants.
        return timezone.parse("2026-06-01T07:30:45+00:00")

    @pytest.fixture
    def real_context(self, logical_date):
        """Build a real context via build_context_from_dag_run, then inject deadline."""
        from airflow.sdk.execution_time.context import build_context_from_dag_run

        dag_run = SimpleNamespace(
            run_id="manual__2026-06-01T07:30:45+00:00",
            logical_date=logical_date,
            data_interval_start=logical_date,
            data_interval_end=timezone.parse("2026-06-01T08:30:45+00:00"),
            dag_id="deadline_dag",
        )
        context = build_context_from_dag_run(dag_run)
        context["deadline"] = {"id": "dl-1", "deadline_time": "2026-06-01T07:00:00Z"}
        return context

    def test_real_context_fields_render(self, real_context):
        """run_id, ds, and the injected deadline_time all render from a real context."""
        log = Mock()
        kwargs = {
            "msg": "run={{ dag_run.run_id }} ds={{ ds }} dl={{ deadline.deadline_time }}",
            "context": real_context,
        }
        result = _render_callback_kwargs(kwargs, real_context, log)

        assert result["msg"] == (
            "run=manual__2026-06-01T07:30:45+00:00 ds=2026-06-01 dl=2026-06-01T07:00:00Z"
        )
        # The reserved context key is passed through untouched.
        assert result["context"] is real_context
        log.warning.assert_not_called()

    def test_top_level_run_id_and_ts_fields_render(self, real_context):
        """The top-level run_id/ts/ts_nodash fields produced by the builder render."""
        log = Mock()
        kwargs = {
            "msg": "run_id={{ run_id }} ts={{ ts }} tsnd={{ ts_nodash }} dsnd={{ ds_nodash }}",
        }
        result = _render_callback_kwargs(kwargs, real_context, log)

        assert result["msg"] == (
            "run_id=manual__2026-06-01T07:30:45+00:00 "
            "ts=2026-06-01T07:30:45+00:00 "
            "tsnd=20260601T073045 "
            "dsnd=20260601"
        )
        log.warning.assert_not_called()

    def test_logical_date_none_branch_renders_empty_for_missing_keys(self):
        """
        When logical_date is None, build_context_from_dag_run only emits dag_run + run_id.

        A template referencing {{ ds }} must render to empty string (Jinja Undefined),
        NOT raise -- mirroring the behavior callers depend on.
        """
        from airflow.sdk.execution_time.context import build_context_from_dag_run

        dag_run = SimpleNamespace(
            run_id="asset_triggered_run",
            logical_date=None,
            data_interval_start=None,
            data_interval_end=None,
            dag_id="asset_dag",
        )
        context = build_context_from_dag_run(dag_run)
        # Confirm the builder really omitted the date-derived keys.
        assert "ds" not in context
        assert "ts" not in context
        assert set(context) == {"dag_run", "run_id"}

        log = Mock()
        kwargs = {"msg": "run={{ dag_run.run_id }} ds=[{{ ds }}] ts=[{{ ts }}]"}
        result = _render_callback_kwargs(kwargs, context, log)

        assert result["msg"] == "run=asset_triggered_run ds=[] ts=[]"
        log.warning.assert_not_called()

    def test_datetime_logical_date_renders_sanely(self, real_context, logical_date):
        """
        Adversarial: {{ dag_run.logical_date }} and {{ data_interval_start }} are pendulum
        DateTimes. They must render as a clean ISO-ish string, not a Python repr or a crash.
        """
        log = Mock()
        kwargs = {
            "ld": "{{ dag_run.logical_date }}",
            "dis": "{{ data_interval_start }}",
            "die": "{{ data_interval_end }}",
        }
        result = _render_callback_kwargs(kwargs, real_context, log)

        # Jinja renders via str(pendulum.DateTime), which uses a SPACE separator
        # ("2026-06-01 07:30:45+00:00"), NOT the ISO "T" separator. This is sane --
        # human-readable, tz-aware, no Python repr or memory-address leakage. A callback
        # author who needs strict ISO-8601 must call {{ ... .isoformat() }} explicitly.
        assert result["ld"] == str(real_context["logical_date"])
        assert result["ld"] == "2026-06-01 07:30:45+00:00"
        assert "DateTime(" not in result["ld"]
        assert "object at 0x" not in result["ld"]

        assert result["dis"] == str(real_context["data_interval_start"])
        assert result["dis"] == "2026-06-01 07:30:45+00:00"
        assert result["die"] == "2026-06-01 08:30:45+00:00"
        log.warning.assert_not_called()

    def test_present_but_none_key_renders_literal_none_vs_missing_renders_empty(self):
        """
        Adversarial / sharp edge: a key that is PRESENT-but-None renders the literal
        string "None", whereas a MISSING key renders empty "". This is standard Jinja2
        behavior (default Undefined prints "", but None prints "None") and does not raise.

        It matters because the two states are NOT interchangeable in rendered output.
        build_context_from_dag_run avoids the trap for date fields: in the logical_date
        is None branch it OMITS ds/ts entirely (so they render empty) rather than setting
        them to None (which would render "None"). This test pins that distinction so a
        future change that starts inserting None-valued context keys is caught.
        """
        log = Mock()
        context = {"dag_run": SimpleNamespace(run_id="r1"), "ds": None}
        result = _render_callback_kwargs(
            {"msg": "present_none=[{{ ds }}] missing=[{{ ts }}] run={{ dag_run.run_id }}"},
            context,
            log,
        )

        # ds is present-but-None -> literal "None"; ts is absent -> empty string.
        assert result["msg"] == "present_none=[None] missing=[] run=r1"
        log.warning.assert_not_called()

    def test_missing_attribute_on_real_dag_run_does_not_raise(self, real_context):
        """
        Adversarial: referencing an attribute the dag_run stub does not have
        (e.g. {{ dag_run.conf }}) -- SimpleNamespace raises AttributeError, which
        Jinja surfaces; _render_callback_kwargs must catch it and return the raw value.
        """
        log = Mock()
        kwargs = {"msg": "conf={{ dag_run.conf['x'] }}"}
        result = _render_callback_kwargs(kwargs, real_context, log)

        # dag_run is a SimpleNamespace without 'conf' -> render fails -> raw value kept.
        assert result["msg"] == "conf={{ dag_run.conf['x'] }}"
        log.warning.assert_called_once()
