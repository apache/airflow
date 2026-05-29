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

import socket
from dataclasses import dataclass
from operator import attrgetter
from typing import Any
from unittest.mock import patch

import pytest
import structlog

from airflow.sdk._shared.timezones import timezone
from airflow.sdk.api.datamodels._generated import DagRun, DagRunState, DagRunType
from airflow.sdk.execution_time.callback_supervisor import CallbackSubprocess, execute_callback
from airflow.sdk.execution_time.comms import (
    ConnectionResult,
    GetConnection,
    GetDagRun,
    GetVariable,
    GetVariableKeys,
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


class CallableClass:
    """A class that returns a callable instance (like BaseNotifier)."""

    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def __call__(self, context):
        return "notified"


class TestExecuteCallback:
    @pytest.mark.parametrize(
        ("path", "kwargs", "expect_success", "error_contains"),
        [
            pytest.param(
                f"{__name__}.callback_no_args",
                {},
                True,
                None,
                id="successful_no_args",
            ),
            pytest.param(
                f"{__name__}.callback_with_kwargs",
                {"arg1": "hello", "arg2": "world"},
                True,
                None,
                id="successful_with_kwargs",
            ),
            pytest.param(
                f"{__name__}.CallableClass",
                {"msg": "alert"},
                True,
                None,
                id="callable_class_pattern",
            ),
            pytest.param(
                "",
                {},
                False,
                "Callback path not found",
                id="empty_path",
            ),
            pytest.param(
                "nonexistent.module.function",
                {},
                False,
                "ModuleNotFoundError",
                id="import_error",
            ),
            pytest.param(
                f"{__name__}.callback_that_raises",
                {},
                False,
                "ValueError",
                id="execution_error",
            ),
            pytest.param(
                f"{__name__}.nonexistent_function_xyz",
                {},
                False,
                "AttributeError",
                id="attribute_error",
            ),
        ],
    )
    def test_execute_callback(self, path, kwargs, expect_success, error_contains):
        log = structlog.get_logger()
        success, error = execute_callback(path, kwargs, log)

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


class TestLoadMangledModule:
    """Tests for _load_mangled_module."""

    def test_loads_real_file_under_mangled_name(self, tmp_path):
        import sys

        from airflow._shared.module_loading import load_mangled_dag_module

        stem = "my_dag"
        mod_name = f"unusual_prefix_{'a' * 40}_{stem}"
        (tmp_path / f"{stem}.py").write_text("def my_callback(): return 42\n")

        result = load_mangled_dag_module(mod_name, str(tmp_path / f"{stem}.py"))

        assert result is True
        assert mod_name in sys.modules
        assert sys.modules[mod_name].my_callback() == 42
        sys.modules.pop(mod_name)

    def test_returns_false_when_file_missing(self, tmp_path):
        from airflow._shared.module_loading import load_mangled_dag_module

        result = load_mangled_dag_module(
            "unusual_prefix_" + "b" * 40 + "_absent",
            str(tmp_path / "absent.py"),
        )

        assert result is False

    def test_returns_false_on_syntax_error(self, tmp_path):
        import sys

        from airflow._shared.module_loading import load_mangled_dag_module

        stem = "bad_dag"
        mod_name = f"unusual_prefix_{'c' * 40}_{stem}"
        (tmp_path / f"{stem}.py").write_text("def broken(: pass\n")  # syntax error

        result = load_mangled_dag_module(mod_name, str(tmp_path / f"{stem}.py"))

        assert result is False
        assert mod_name not in sys.modules

    def test_skips_registration_when_already_in_sys_modules(self, tmp_path, mocker):
        import sys

        from airflow.sdk.execution_time.callback_supervisor import _register_unusual_prefix_module

        stem = "cached_dag"
        mod_name = f"unusual_prefix_{'d' * 40}_{stem}"
        (tmp_path / f"{stem}.py").write_text("def fn(): return 'cached'\n")

        log = mocker.Mock()
        _register_unusual_prefix_module(f"{mod_name}.fn", tmp_path, log)
        assert mod_name in sys.modules

        # Second call should be a no-op; module should not be re-loaded
        (tmp_path / f"{stem}.py").write_text("def fn(): return 'new'\n")
        _register_unusual_prefix_module(f"{mod_name}.fn", tmp_path, log)

        assert sys.modules[mod_name].fn() == "cached"
        sys.modules.pop(mod_name)
