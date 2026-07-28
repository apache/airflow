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
from typing import Any
from unittest.mock import ANY, Mock, patch

import pytest
import structlog

from airflow.sdk.execution_time.callback_supervisor import CallbackSubprocess, Path, execute_callback
from airflow.sdk.execution_time.comms import (
    BundleInfo,
    ConnectionResult,
    GetConnection,
    GetVariable,
    GetVariableKeys,
    MaskSecret,
    VariableKeysResult,
    VariableResult,
    _RequestFrame,
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
            version_data=bundle_info.version_data,
        )
        mock_bundle_setup["bundle"].initialize.assert_called_once()

        self.mock_execute_callback.assert_called_with(
            bundle_path=str(mock_bundle_setup["bundle_path"]),
            callback_kwargs=adjusted_kwargs["callback_kwargs"],
            callback_path=adjusted_kwargs["callback_path"],
            dag_rel_path=adjusted_kwargs["dag_rel_path"],
            log=ANY,
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

    def test_callback_supervisor_should_exit_on_error(self, base_start_kwargs):
        """Test that callback supervisor exits if execute_callback returns an error."""
        self.mock_execute_callback.return_value = (False, "Some error occurred")

        CallbackSubprocess.start(**base_start_kwargs)

        with pytest.raises(SystemExit) as exc_info:
            self.mock_super_start.call_args.kwargs["target"]()

        assert exc_info.value.code == 1
