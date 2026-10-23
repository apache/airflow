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

import json
import signal
import socket
import textwrap
import uuid
from dataclasses import dataclass
from datetime import timedelta
from operator import attrgetter
from typing import Any, get_args
from unittest.mock import ANY, Mock, create_autospec, patch

import msgspec
import pytest
import structlog
from pydantic import TypeAdapter

from airflow.sdk.api.client import CallbackOperations, Client, VariableOperations
from airflow.sdk.api.datamodels._generated import (
    CallbackState,
    DagCallbackContext,
    PrevSuccessfulDagRunResponse,
    VariableResponse,
    XComResponse,
)
from airflow.sdk.execution_time.callback_supervisor import (
    _CALLBACK_REQUEST_TYPES,
    CallbackSubprocess,
    CallbackToSupervisor,
    DagCallbackStartupDetails,
    Path,
    _run_dag_callback_entrypoint,
    execute_callback,
    supervise_callback,
)
from airflow.sdk.execution_time.comms import (
    BundleInfo,
    ConnectionResult,
    GetConnection,
    GetPrevSuccessfulDagRun,
    GetVariable,
    GetVariableKeys,
    GetXCom,
    MaskSecret,
    PutVariable,
    VariableKeysResult,
    VariableResult,
    XComResult,
    _RequestFrame,
    _ResponseFrame,
)

from tests_common.test_utils.config import conf_vars


def callback_no_args():
    """A simple callback that takes no arguments."""
    return "ok"


def callback_with_kwargs(arg1, arg2):
    """A callback that accepts keyword arguments."""
    return f"{arg1}-{arg2}"


def callback_that_raises():
    """A callback that always raises."""
    raise ValueError("something went wrong")


received_kwargs: dict[str, Any] = {}


def callback_recording_kwargs(**kwargs):
    """A callback that records the kwargs it was called with."""
    received_kwargs.clear()
    received_kwargs.update(kwargs)


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

    @pytest.mark.parametrize(
        "encoded_kwargs",
        [
            pytest.param(
                {
                    "interval": {"__classname__": "datetime.timedelta", "__version__": 2, "__data__": 3600.0},
                    "pair": {"__classname__": "builtins.tuple", "__version__": 1, "__data__": [1, 2]},
                    "plain": {"nested": [1, "two"]},
                },
                id="current",
            ),
            pytest.param(
                {
                    "interval": {"__type": "timedelta", "__var": 3600.0},
                    "pair": {"__type": "tuple", "__var": [1, 2]},
                    "plain": {"nested": [1, "two"]},
                },
                id="legacy",
            ),
        ],
    )
    def test_execute_callback_decodes_serde_encoded_kwargs(self, encoded_kwargs):
        context = {"dag_run": {"dag_id": "test_dag"}, "deadline": {"id": "1"}}

        success, error = execute_callback(
            callback_path=f"{__name__}.callback_recording_kwargs",
            callback_kwargs={**encoded_kwargs, "context": context},
            dag_rel_path=Path("test.py"),
            bundle_path=Path("bundle/path"),
            log=structlog.get_logger(),
            callback_kwargs_encoding="serde",
        )

        assert (success, error) == (True, None)
        assert received_kwargs == {
            "interval": timedelta(hours=1),
            "pair": (1, 2),
            "plain": {"nested": [1, "two"]},
            "context": context,
        }

    def test_execute_callback_passes_kwargs_as_is_without_encoding(self):
        kwargs = {
            "interval": {"__classname__": "datetime.timedelta", "__version__": 2, "__data__": 3600.0},
            "plain": {"nested": [1, "two"]},
        }

        success, error = execute_callback(
            callback_path=f"{__name__}.callback_recording_kwargs",
            callback_kwargs=kwargs,
            dag_rel_path=Path("test.py"),
            bundle_path=Path("bundle/path"),
            log=structlog.get_logger(),
        )

        assert (success, error) == (True, None)
        assert received_kwargs == kwargs

    def test_execute_callback_fails_on_unsupported_kwargs_encoding(self):
        success, error = execute_callback(
            callback_path=f"{__name__}.callback_recording_kwargs",
            callback_kwargs={"arg1": "value1"},
            dag_rel_path=Path("test.py"),
            bundle_path=Path("bundle/path"),
            log=structlog.get_logger(),
            callback_kwargs_encoding="unknown",
        )

        assert success is False
        assert "Unsupported callback kwargs encoding" in error


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
                response=ConnectionResult(
                    conn_id="test_conn",
                    conn_type="mysql",
                    host=None,
                    schema=None,
                    login=None,
                    password=None,
                    port=None,
                    extra=None,
                ),  # type: ignore[call-arg]
            ),
        ),
        RequestCase(
            message=GetConnection(conn_id="test_conn"),
            test_id="get_connection_with_password",
            client_mock=ClientMock(
                method_path="connections.get",
                args=("test_conn",),
                response=ConnectionResult(
                    conn_id="test_conn",
                    conn_type="mysql",
                    password="secret",
                    host=None,
                    schema=None,
                    login=None,
                    port=None,
                    extra=None,
                ),  # type: ignore[call-arg]
            ),
            mask_secret_args=("secret",),
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
            message=GetXCom(
                key="return_value",
                dag_id="test_dag",
                run_id="test_run_1",
                task_id="upstream_task",
            ),
            test_id="get_xcom",
            client_mock=ClientMock(
                method_path="xcoms.get",
                args=("test_dag", "test_run_1", "upstream_task", "return_value", None, False),
                kwargs={"previous_iteration": False},
                response=XComResponse(key="return_value", value="xcom_payload"),
            ),
        ),
        RequestCase(
            message=GetXCom(
                key="custom_key",
                dag_id="dag_a",
                run_id="run_42",
                task_id="task_b",
                map_index=3,
                include_prior_dates=True,
            ),
            test_id="get_xcom_with_map_index",
            client_mock=ClientMock(
                method_path="xcoms.get",
                args=("dag_a", "run_42", "task_b", "custom_key", 3, True),
                kwargs={"previous_iteration": False},
                response=XComResponse(key="custom_key", value={"nested": "data"}),
            ),
        ),
        RequestCase(
            message=MaskSecret(value="super_secret", name="api_key"),
            test_id="mask_secret",
            mask_secret_args=("super_secret", "api_key"),
        ),
        RequestCase(
            message=GetXCom(key="return_value", dag_id="d", run_id="r", task_id="t"),
            test_id="get_xcom",
            client_mock=ClientMock(
                method_path="xcoms.get",
                args=("d", "r", "t", "return_value", None, False),
                kwargs={"previous_iteration": False},
                response=XComResponse(key="return_value", value="v"),
            ),
        ),
        RequestCase(
            message=PutVariable(key="k", value="v", description=None),
            test_id="put_variable",
            client_mock=ClientMock(method_path="variables.set", args=("k", "v", None), response=None),
        ),
        RequestCase(
            message=GetPrevSuccessfulDagRun(ti_id=uuid.UUID("12345678-1234-5678-1234-567812345678")),
            test_id="get_prev_successful_dag_run",
            client_mock=ClientMock(
                # The callback's own endpoint: the task-instance one knows no callback id.
                method_path="callbacks.get_previous_successful_dagrun",
                args=("12345678-1234-5678-1234-567812345678",),
                response=PrevSuccessfulDagRunResponse(),
            ),
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

    def test_a_request_a_callback_cannot_make_gets_an_error(self, callback_subprocess, mocker):
        """The child waits for every response, so a dropped request would block it until its timeout."""
        proc, _read_end = callback_subprocess
        send_msg = mocker.patch.object(CallbackSubprocess, "send_msg", autospec=True)

        generator = proc.handle_requests(log=mocker.Mock())
        next(generator)
        generator.send(_RequestFrame(id=7, body={"type": "GetAssetsByAlias", "alias_name": "alias"}))

        send_msg.assert_called_once_with(proc, None, request_id=7, error=ANY)
        assert send_msg.call_args.kwargs["error"].detail["message"] == "Unhandled request"


def test_callbacks_can_make_the_requests_a_dag_processor_answers():
    """A routed callback reaches the same context as one a Dag processor runs."""
    from airflow.dag_processing.processor import BaseDagFileProcessorProcess

    expected = {*BaseDagFileProcessorProcess._client_request_types, MaskSecret}
    assert set(_CALLBACK_REQUEST_TYPES) == expected
    assert set(get_args(get_args(CallbackToSupervisor)[0])) == expected
    assert set(CallbackSubprocess._request_handlers) == expected


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

    @pytest.mark.parametrize(
        ("execution_timeout", "expected"),
        [
            pytest.param(7.0, 7.0, id="stamped"),
            pytest.param(0, 0, id="stamped-no-timeout"),
            pytest.param(None, 3, id="config"),
        ],
    )
    def test_timeout_stamped_by_the_scheduler_overrides_the_config(
        self, callback_subprocess, execution_timeout, expected
    ):
        callback_subprocess.execution_timeout = execution_timeout

        with conf_vars({("callbacks", "callback_execution_timeout"): "3"}):
            assert callback_subprocess._get_callback_execution_timeout() == expected

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
        assert proc.timed_out is True


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
    def base_mocks_setup(self, mock_supervisor_comms, monkeypatch):
        """Base mocks for all tests in this class."""
        monkeypatch.setattr("airflow.sdk.execution_time.callback_supervisor._should_use_exec", lambda: False)
        with (
            patch("airflow.sdk.execution_time.comms.CommsDecoder") as mock_comms,
            patch("airflow.sdk.execution_time.callback_supervisor.WatchedSubprocess.start") as mock_super,
            patch("airflow.sdk.execution_time.callback_supervisor.execute_callback") as mock_execute,
        ):
            mock_execute.return_value = (True, None)

            def send_startup(data):
                frame = msgspec.msgpack.decode(data[4:], type=_ResponseFrame)
                msg = TypeAdapter(DagCallbackStartupDetails).validate_python(frame.body)
                mock_comms.__getitem__.return_value.return_value._get_response.return_value = msg

            mock_super.return_value.stdin.sendall.side_effect = send_startup

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

    @pytest.mark.parametrize("callback_kwargs_encoding", [None, "serde"])
    def test_execute_callback_receives_correct_parameters(self, base_start_kwargs, callback_kwargs_encoding):
        """Test that execute_callback receives the correct parameters."""
        CallbackSubprocess.start(**base_start_kwargs, callback_kwargs_encoding=callback_kwargs_encoding)
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
            callback_kwargs_encoding=callback_kwargs_encoding,
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
            version_data=bundle_info.version_data,
        )
        mock_bundle_setup["bundle"].initialize.assert_called_once()

        self.mock_execute_callback.assert_called_with(
            bundle_path=str(mock_bundle_setup["bundle_path"]),
            callback_kwargs=adjusted_kwargs["callback_kwargs"],
            callback_path=adjusted_kwargs["callback_path"],
            dag_rel_path=adjusted_kwargs["dag_rel_path"],
            log=ANY,
            callback_kwargs_encoding=None,
        )

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

    @pytest.mark.parametrize(
        ("errors", "expected_exit"),
        [pytest.param([], None, id="succeeded"), pytest.param(["boom"], 1, id="failed")],
    )
    @pytest.mark.parametrize("platform_uses_exec", [True, False])
    @patch("airflow.sdk.execution_time.callback_supervisor._should_use_exec", autospec=True)
    def test_dag_attribute_callback_runs_from_the_parsed_dag(
        self, should_use_exec, base_start_kwargs, errors, expected_exit, platform_uses_exec
    ):
        should_use_exec.return_value = platform_uses_exec
        context = DagCallbackContext(type="DagCallback", dag_id="test_dag", run_id="test_run")
        bundle_info = BundleInfo(name="test-bundle", version="1.0", version_data={"manifest": ["dag.py"]})
        CallbackSubprocess.start(**{**base_start_kwargs, "bundle_info": bundle_info, "context": context})

        with patch(
            "airflow.sdk.execution_time.dag_attribute_callbacks.run_dag_attribute_callbacks",
            autospec=True,
            return_value=errors,
        ) as mock_run:
            if expected_exit is None:
                self.mock_super_start.call_args.kwargs["target"]()
            else:
                with pytest.raises(SystemExit) as exc_info:
                    self.mock_super_start.call_args.kwargs["target"]()
                assert exc_info.value.code == expected_exit

        mock_run.assert_called_once_with(
            context, dag_rel_path=base_start_kwargs["dag_rel_path"], bundle_info=bundle_info, log=ANY
        )
        self.mock_execute_callback.assert_not_called()

    @pytest.mark.parametrize("platform_uses_exec", [True, False])
    @patch("airflow.sdk.execution_time.callback_supervisor._should_use_exec", autospec=True)
    def test_dag_attribute_callback_without_a_bundle_fails(
        self, should_use_exec, base_start_kwargs, platform_uses_exec
    ):
        should_use_exec.return_value = platform_uses_exec
        context = DagCallbackContext(type="DagCallback", dag_id="test_dag", run_id="test_run")
        CallbackSubprocess.start(**{**base_start_kwargs, "context": context})

        with pytest.raises(SystemExit) as exc_info:
            self.mock_super_start.call_args.kwargs["target"]()

        assert exc_info.value.code == 1
        self.mock_execute_callback.assert_not_called()

    def test_callback_supervisor_should_exit_on_error(self, base_start_kwargs):
        """Test that callback supervisor exits if execute_callback returns an error."""
        self.mock_execute_callback.return_value = (False, "Some error occurred")

        CallbackSubprocess.start(**base_start_kwargs)

        with pytest.raises(SystemExit) as exc_info:
            self.mock_super_start.call_args.kwargs["target"]()

        assert exc_info.value.code == 1

    def _run_target_with_context(self, base_start_kwargs, context):
        CallbackSubprocess.start(**{**base_start_kwargs, "callback_kwargs": {"context": context}})
        self.mock_super_start.call_args.kwargs["target"]()
        return self.mock_execute_callback.call_args.kwargs["callback_kwargs"]["context"]

    @pytest.mark.parametrize(
        ("xcom_value", "expected"),
        [
            pytest.param("payload", "payload", id="value"),
            pytest.param(None, "fallback", id="missing"),
        ],
    )
    def test_context_xcom_pull_uses_dag_run_ids(self, base_start_kwargs, xcom_value, expected):
        comms = self.mock_comms_decoder.__getitem__.return_value.return_value
        comms.send.return_value = XComResult(key="out", value=xcom_value)

        context = self._run_target_with_context(
            base_start_kwargs, {"dag_run": {"dag_id": "dag_a", "dag_run_id": "run_1"}}
        )

        assert context["xcom_pull"]("upstream", key="out", map_index=2, default="fallback") == expected
        comms.send.assert_called_once_with(
            GetXCom(key="out", dag_id="dag_a", run_id="run_1", task_id="upstream", map_index=2)
        )

    def test_context_without_dag_run_has_no_xcom_pull(self, base_start_kwargs, caplog):
        context = self._run_target_with_context(base_start_kwargs, {"deadline": {"id": "x"}})

        assert "xcom_pull" not in context
        assert "Callback context has no Dag run, so xcom_pull is not available" in caplog

    @pytest.mark.parametrize("option_value", ["True", "False"])
    @patch(
        "airflow.sdk.execution_time.callback_supervisor._should_use_exec", autospec=True, return_value=True
    )
    def test_start_keeps_bare_fork_regardless_of_exec_option(
        self, _should_use_exec, base_start_kwargs, option_value
    ):
        """
        ``[core] execute_tasks_new_python_interpreter`` is a task-process opt-in and must not reach the
        callback child: its target is a closure, which only a bare fork can run.
        """
        with conf_vars({("core", "execute_tasks_new_python_interpreter"): option_value}):
            CallbackSubprocess.start(**base_start_kwargs)

        assert self.mock_super_start.call_args.kwargs.get("use_exec", False) is False
        self.mock_super_start.return_value.stdin.sendall.assert_not_called()

    def test_start_imports_task_runner_before_forking(self, base_start_kwargs):
        """The callback child is forked from this process, so it inherits a task runner imported here."""
        calls = []
        self.mock_super_start.side_effect = lambda **kwargs: calls.append("fork")

        with patch(
            "airflow.sdk.execution_time.callback_supervisor._import_task_runner_before_fork",
            autospec=True,
            side_effect=lambda: calls.append("import task runner"),
        ):
            CallbackSubprocess.start(**base_start_kwargs)

        assert calls == ["import task runner", "fork"]

    @pytest.mark.parametrize("platform_uses_exec", [True, False])
    @pytest.mark.parametrize("option_value", ["True", "False"])
    @patch("airflow.sdk.execution_time.callback_supervisor._should_use_exec", autospec=True)
    @patch("airflow.sdk.execution_time.callback_supervisor._import_task_runner_before_fork", autospec=True)
    def test_routed_start_uses_the_platform_exec_guard(
        self, import_task_runner, should_use_exec, base_start_kwargs, option_value, platform_uses_exec
    ):
        should_use_exec.return_value = platform_uses_exec
        context = DagCallbackContext(type="DagCallback", dag_id="test_dag", run_id="test_run")
        with conf_vars({("core", "execute_tasks_new_python_interpreter"): option_value}):
            CallbackSubprocess.start(**base_start_kwargs, context=context)

        assert self.mock_super_start.call_args.kwargs.get("use_exec", False) is platform_uses_exec
        assert import_task_runner.call_count == (0 if platform_uses_exec else 1)
        if platform_uses_exec:
            assert self.mock_super_start.call_args.kwargs["target"] is _run_dag_callback_entrypoint
            comms = self.mock_comms_decoder.__getitem__.return_value.return_value
            assert comms._get_response.return_value == DagCallbackStartupDetails(
                dag_rel_path=base_start_kwargs["dag_rel_path"], context=context
            )
        else:
            self.mock_super_start.return_value.stdin.sendall.assert_not_called()

    @patch(
        "airflow.sdk.execution_time.callback_supervisor._should_use_exec", autospec=True, return_value=True
    )
    def test_invalid_startup_payload_does_not_spawn_a_child(self, _should_use_exec, base_start_kwargs):
        context = DagCallbackContext(type="DagCallback", dag_id="test_dag", run_id="test_run")
        bundle_info = BundleInfo(name="test-bundle", version_data={"unsupported": object()})

        with pytest.raises(NotImplementedError, match="are not supported"):
            CallbackSubprocess.start(**{**base_start_kwargs, "bundle_info": bundle_info}, context=context)

        self.mock_super_start.assert_not_called()

    @patch(
        "airflow.sdk.execution_time.callback_supervisor._should_use_exec", autospec=True, return_value=True
    )
    def test_failed_startup_send_terminates_the_child(self, _should_use_exec, base_start_kwargs):
        context = DagCallbackContext(type="DagCallback", dag_id="test_dag", run_id="test_run")
        process = self.mock_super_start.return_value
        process.stdin.sendall.side_effect = BrokenPipeError("child exited")

        with pytest.raises(BrokenPipeError, match="child exited"):
            CallbackSubprocess.start(**base_start_kwargs, context=context)

        process.kill.assert_called_once_with(signal.SIGKILL)
        process._cleanup_open_sockets.assert_called_once_with()
        process.selector.close.assert_called_once_with()

    def test_routed_entrypoint_requires_the_startup_message(self):
        comms = self.mock_comms_decoder.__getitem__.return_value.return_value
        comms._get_response.return_value = None

        with pytest.raises(RuntimeError, match="Required first message to be DagCallbackStartupDetails"):
            _run_dag_callback_entrypoint()

        self.mock_execute_callback.assert_not_called()


@pytest.mark.usefixtures("disable_capturing")
@patch("airflow.sdk.execution_time.callback_supervisor._should_use_exec", autospec=True, return_value=True)
@patch.object(CallbackSubprocess, "_upload_logs", autospec=True)
def test_routed_callback_runs_in_a_fresh_interpreter(_upload_logs, _should_use_exec, tmp_path, monkeypatch):
    output_path = tmp_path / "callback-ran"
    (tmp_path / "dag.py").write_text(
        textwrap.dedent(
            f"""
            from pathlib import Path
            from airflow.sdk import DAG, BaseOperator, Variable

            def callback(context):
                value = Variable.get("callback_exec_test_value")
                Path({str(output_path)!r}).write_text(context["run_id"] + ":" + value)

            with DAG(dag_id="test_dag", on_success_callback=callback):
                BaseOperator(task_id="task")
            """
        )
    )
    monkeypatch.setenv(
        "AIRFLOW__DAG_PROCESSOR__DAG_BUNDLE_CONFIG_LIST",
        json.dumps(
            [
                {
                    "name": "test-bundle",
                    "classpath": "airflow.dag_processing.bundles.local.LocalDagBundle",
                    "kwargs": {"path": str(tmp_path)},
                }
            ]
        ),
    )
    client = Mock(spec=Client)
    client.variables = create_autospec(VariableOperations, instance=True)
    client.variables.get.return_value = VariableResponse(
        key="callback_exec_test_value", value="from-supervisor"
    )
    process = CallbackSubprocess.start(
        id=str(uuid.uuid4()),
        callback_path="",
        callback_kwargs={},
        dag_rel_path=Path("dag.py"),
        bundle_info=BundleInfo(name="test-bundle"),
        context=DagCallbackContext(
            type="DagCallback", dag_id="test_dag", run_id="test_run", is_failure_callback=False
        ),
        client=client,
        execution_timeout=15,
    )

    assert process.wait() == 0
    assert output_path.read_text() == "test_run:from-supervisor"
    client.variables.get.assert_called_once_with("callback_exec_test_value")


class TestSuperviseCallbackExchangesTokenFirst:
    """No callback code may run until the single-use token has been exchanged."""

    CALLBACK_ID = "01890a5d-ac70-7a5b-b7d5-0dd5b1c7be47"

    def _supervise(self, client):
        return supervise_callback(
            id=self.CALLBACK_ID,
            callback_path="does.not.matter",
            callback_kwargs={},
            dag_rel_path=Path("dag.py"),
            client=client,
        )

    @patch("airflow.sdk.execution_time.callback_supervisor._make_process_nondumpable")
    @patch.object(CallbackSubprocess, "start")
    def test_exchange_happens_before_the_subprocess_starts(self, mock_start, _nondumpable):
        calls: list[str] = []
        client = Mock()
        client.callbacks.run.side_effect = lambda callback_id: calls.append("exchange")

        def record_start(**kwargs):
            calls.append("start")
            return Mock(wait=Mock(return_value=0))

        mock_start.side_effect = record_start

        assert self._supervise(client) == 0
        client.callbacks.run.assert_called_once_with(uuid.UUID(self.CALLBACK_ID))
        assert calls == ["exchange", "start"]

    @patch("airflow.sdk.execution_time.callback_supervisor._make_process_nondumpable")
    @patch.object(CallbackSubprocess, "start")
    def test_rejected_exchange_prevents_the_subprocess_from_starting(self, mock_start, _nondumpable):
        client = Mock()
        client.callbacks.run.side_effect = RuntimeError("409 Conflict: token already exchanged")

        with pytest.raises(RuntimeError, match="already exchanged"):
            self._supervise(client)

        mock_start.assert_not_called()

    @pytest.mark.parametrize("callback_kwargs_encoding", [None, "serde"])
    @patch("airflow.sdk.execution_time.callback_supervisor._make_process_nondumpable")
    @patch.object(CallbackSubprocess, "start")
    def test_kwargs_encoding_is_passed_to_the_subprocess(
        self, mock_start, _nondumpable, callback_kwargs_encoding
    ):
        mock_start.return_value = Mock(wait=Mock(return_value=0))

        supervise_callback(
            id=self.CALLBACK_ID,
            callback_path="does.not.matter",
            callback_kwargs={},
            dag_rel_path=Path("dag.py"),
            callback_kwargs_encoding=callback_kwargs_encoding,
            client=Mock(),
        )

        assert mock_start.call_args.kwargs["callback_kwargs_encoding"] == callback_kwargs_encoding


def _make_client() -> Mock:
    client = Mock(spec=Client)
    client.callbacks = create_autospec(CallbackOperations, instance=True)
    return client


class TestSuperviseRoutedCallback:
    """A routed callback reads its context after the exchange and reports its outcome."""

    CALLBACK_ID = "01890a5d-ac70-7a5b-b7d5-0dd5b1c7be47"

    def _supervise(self, client, **kwargs):
        return supervise_callback(
            id=self.CALLBACK_ID,
            callback_path="",
            callback_kwargs={},
            dag_rel_path=Path("dag.py"),
            client=client,
            **kwargs,
        )

    @pytest.mark.parametrize(
        ("fetch_method", "reads_context"),
        [
            pytest.param("dag_attribute", True, id="dag-attribute"),
            pytest.param(None, False, id="import-path"),
        ],
    )
    @patch("airflow.sdk.execution_time.callback_supervisor._make_process_nondumpable")
    @patch.object(CallbackSubprocess, "start")
    def test_context_is_read_after_the_exchange(self, mock_start, _nondumpable, fetch_method, reads_context):
        calls: list[str] = []
        client = _make_client()
        client.callbacks.run.side_effect = lambda callback_id: calls.append("exchange")
        context = DagCallbackContext(type="DagCallback", dag_id="test_dag", run_id="test_run")
        client.callbacks.get_context.side_effect = lambda callback_id: (
            calls.append("context") or Mock(context=context)
        )
        mock_start.return_value = Mock(wait=Mock(return_value=0))

        self._supervise(client, fetch_method=fetch_method, execution_timeout=30)

        assert calls == (["exchange", "context"] if reads_context else ["exchange"])
        assert mock_start.call_args.kwargs["context"] == (context if reads_context else None)
        assert mock_start.call_args.kwargs["execution_timeout"] == 30

    @pytest.mark.parametrize(
        ("exit_code", "timed_out", "expected_state", "expected_output"),
        [
            pytest.param(0, False, CallbackState.SUCCESS, None, id="success"),
            pytest.param(
                1, False, CallbackState.FAILED, "Callback subprocess exited with code 1", id="failed"
            ),
            pytest.param(
                -15, True, CallbackState.FAILED, "Callback timed out after 30 seconds", id="timed-out"
            ),
        ],
    )
    @patch("airflow.sdk.execution_time.callback_supervisor.stats", autospec=True)
    @patch("airflow.sdk.execution_time.callback_supervisor._make_process_nondumpable")
    @patch.object(CallbackSubprocess, "start")
    def test_outcome_is_reported(
        self, mock_start, _nondumpable, mock_stats, exit_code, timed_out, expected_state, expected_output
    ):
        client = _make_client()
        process = Mock(wait=Mock(return_value=exit_code), timed_out=timed_out)
        process._get_callback_execution_timeout.return_value = 30
        mock_start.return_value = process

        if exit_code:
            with pytest.raises(RuntimeError, match=expected_output):
                self._supervise(client)
        else:
            assert self._supervise(client) == 0

        client.callbacks.update_state.assert_called_once_with(
            uuid.UUID(self.CALLBACK_ID), expected_state, expected_output
        )
        if timed_out:
            mock_stats.incr.assert_called_once_with("callback.timeouts")
        else:
            mock_stats.incr.assert_not_called()

    @pytest.mark.parametrize("exit_code", [0, 1])
    @patch("airflow.sdk.execution_time.callback_supervisor._make_process_nondumpable")
    @patch.object(CallbackSubprocess, "start")
    def test_outcome_is_reported_before_the_logs_are_uploaded(self, mock_start, _nondumpable, exit_code):
        calls: list[str] = []
        client = _make_client()
        client.callbacks.update_state.side_effect = lambda *args: calls.append("report")
        process = Mock(wait=Mock(return_value=exit_code), timed_out=False)
        process._upload_logs.side_effect = lambda: calls.append("upload")
        mock_start.return_value = process

        if exit_code:
            with pytest.raises(RuntimeError):
                self._supervise(client)
        else:
            self._supervise(client)

        assert calls == ["report", "upload"]

    @patch("airflow.sdk.execution_time.callback_supervisor._make_process_nondumpable")
    @patch.object(CallbackSubprocess, "start")
    def test_failing_to_report_leaves_the_outcome_to_the_executor(self, mock_start, _nondumpable):
        client = _make_client()
        client.callbacks.update_state.side_effect = RuntimeError("404 Not Found")
        mock_start.return_value = Mock(wait=Mock(return_value=0))

        assert self._supervise(client) == 0
