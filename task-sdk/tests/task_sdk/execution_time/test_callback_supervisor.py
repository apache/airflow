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

from airflow.sdk._shared.template_rendering import render_callback_kwargs
from airflow.sdk._shared.timezones import timezone
from airflow.sdk.api.datamodels._generated import DagRun, DagRunState, DagRunType, XComResponse
from airflow.sdk.execution_time.callback_supervisor import (
    CALLBACK_CONTEXT_FETCH_EXIT_CODE,
    CallbackContextFetchError,
    CallbackSubprocess,
    Path,
    execute_callback,
    supervise_callback,
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
                "",
                {},
                Path("test.py"),
                Path("bundle/path"),
                False,
                "Callback path not found",
                id="empty_path",
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
            message=GetDagRun(dag_id="test_dag", run_id="test_run"),
            test_id="get_dag_run",
            client_mock=ClientMock(
                method_path="dag_runs.get_detail",
                args=("test_dag", "test_run"),
                response=_MOCK_DAG_RUN,
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
            message=GetXCom(
                key="return_value",
                dag_id="test_dag",
                run_id="test_run_1",
                task_id="upstream_task",
                map_index=None,
            ),
            test_id="get_xcom",
            client_mock=ClientMock(
                method_path="xcoms.get",
                args=("test_dag", "test_run_1", "upstream_task", "return_value", None, False),
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
                response=XComResponse(key="custom_key", value={"nested": "data"}),
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

        # The deadline dict is assembled by the caller and threaded into
        # _fetch_and_build_context (which forwards it to the shared build_context_from_dag_run),
        # so both the executor and triggerer paths produce context["deadline"] identically.
        mock_fetch.assert_called_once()
        assert mock_fetch.call_args.kwargs["deadline"] == {
            "id": deadline_id,
            "deadline_time": deadline_time,
        }
        passed_kwargs = self.mock_execute_callback.call_args.kwargs["callback_kwargs"]
        assert "context" in passed_kwargs

    def test_context_has_no_deadline_key_when_metadata_absent(self, base_start_kwargs):
        """With dag_id/run_id but no deadline_id/deadline_time, the context is still passed but
        carries no 'deadline' key (a non-deadline callback that just needs DagRun context)."""
        adjusted = {**base_start_kwargs, "dag_id": "my_dag", "run_id": "my_run"}

        with patch(
            "airflow.sdk.execution_time.callback_supervisor._fetch_and_build_context",
            return_value={"dag_run": Mock(), "run_id": "my_run"},
        ) as mock_fetch:
            CallbackSubprocess.start(**adjusted)
            self.mock_super_start.call_args.kwargs["target"]()

        # No deadline metadata -> the caller passes deadline=None, so context carries no deadline key.
        assert mock_fetch.call_args.kwargs["deadline"] is None
        passed_kwargs = self.mock_execute_callback.call_args.kwargs["callback_kwargs"]
        assert "context" in passed_kwargs
        assert "deadline" not in passed_kwargs["context"]


class TestFetchAndBuildContext:
    """Tests for _fetch_and_build_context."""

    def test_returns_none_on_unexpected_response(self, mocker):
        """When the comms channel returns a non-DagRunResult, context should be None."""
        from airflow.sdk.execution_time.callback_supervisor import _fetch_and_build_context

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


class TestLoadMangledModule:
    """Tests for _load_mangled_module."""

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


class TestRenderCallbackKwargsAdversarial:
    """Adversarial QA-loop tests for render_callback_kwargs (mutation/SSTI/missing-key/guard edges)."""

    @pytest.fixture
    def context(self):
        return {
            "dag_run": SimpleNamespace(run_id="manual__2026-06-10"),
            "ds": "2026-06-10",
        }

    def test_renders_string_kwarg_using_context(self, context):
        """A string kwarg containing {{ }} is rendered against the context."""
        result = render_callback_kwargs({"msg": "Deadline {{ dag_run.run_id }} missed"}, context)
        assert result["msg"] == "Deadline manual__2026-06-10 missed"

    @pytest.mark.parametrize(
        "value",
        [42, 3.14, True, None, {"nested": "{{ dag_run.run_id }}"}, ["{{ ds }}"], ("{{ ds }}",)],
    )
    def test_non_string_values_pass_through_untouched(self, context, value):
        """Non-string kwargs (int/float/bool/None/dict/list/tuple) are returned unchanged."""
        result = render_callback_kwargs({"arg": value}, context)
        assert result["arg"] == value

    def test_string_without_markers_passes_through(self, context):
        """A plain string with no {{ markers is returned verbatim, never rendered."""
        result = render_callback_kwargs({"msg": "no templates here"}, context)
        assert result["msg"] == "no templates here"

    @pytest.mark.parametrize(
        "broken",
        ["{{ undefined.attr.fails() }}", "{{ 1/0 }}", "{{ dag_run.does_not_exist.boom }}"],
    )
    def test_broken_template_does_not_raise_and_returns_raw(self, context, broken):
        """A template that errors at render time returns the raw value and logs a warning."""
        with patch("airflow.sdk._shared.template_rendering.log") as mock_log:
            result = render_callback_kwargs({"msg": broken}, context)
        assert result["msg"] == broken
        mock_log.warning.assert_called_once()

    def test_does_not_mutate_input_dict(self, context):
        """Rendering builds a new dict and leaves the caller's kwargs untouched."""
        original = {"msg": "Run {{ dag_run.run_id }}"}
        result = render_callback_kwargs(original, context)
        assert original == {"msg": "Run {{ dag_run.run_id }}"}
        assert result is not original
        assert result["msg"] == "Run manual__2026-06-10"


class TestJinjaContextIntegration:
    """
    Integration tests coupling the two newly-added features:

    1. ``build_context_from_dag_run`` (context.py) which constructs the real context dict.
    2. ``render_callback_kwargs`` (callback_supervisor.py) which Jinja-renders kwargs.

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
        kwargs = {
            "msg": "run={{ dag_run.run_id }} ds={{ ds }} dl={{ deadline.deadline_time }}",
            "context": real_context,
        }
        result = render_callback_kwargs(kwargs, real_context)

        assert result["msg"] == (
            "run=manual__2026-06-01T07:30:45+00:00 ds=2026-06-01 dl=2026-06-01T07:00:00Z"
        )
        # The reserved context key is passed through untouched.
        assert result["context"] is real_context

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

        kwargs = {"msg": "run={{ dag_run.run_id }} ds=[{{ ds }}] ts=[{{ ts }}]"}
        result = render_callback_kwargs(kwargs, context)

        assert result["msg"] == "run=asset_triggered_run ds=[] ts=[]"


class TestSuperviseCallbackExitMapping:
    """supervise_callback maps the subprocess exit code to the right exception so the executor can
    distinguish a transient context-fetch failure (retryable) from a real callback failure."""

    def _run(self, mocker, exit_code):
        proc = mocker.Mock()
        proc.wait.return_value = exit_code
        mocker.patch.object(CallbackSubprocess, "start", return_value=proc)
        return supervise_callback(
            id=str(uuid.uuid4()),
            callback_path="some.module.cb",
            callback_kwargs={},
            dag_id="d",
            run_id="r",
            client=mocker.Mock(),
        )

    def test_context_fetch_exit_code_raises_transient_error(self, mocker):
        """The sentinel context-fetch exit code -> CallbackContextFetchError (retryable)."""
        with pytest.raises(CallbackContextFetchError):
            self._run(mocker, CALLBACK_CONTEXT_FETCH_EXIT_CODE)

    def test_other_nonzero_exit_raises_generic_runtime_error(self, mocker):
        """A real callback failure (any other non-zero exit) -> generic RuntimeError, NOT the
        transient subclass, so the executor still marks it terminally FAILED."""
        with pytest.raises(RuntimeError) as exc_info:
            self._run(mocker, 1)
        assert not isinstance(exc_info.value, CallbackContextFetchError)

    def test_zero_exit_returns_without_raising(self, mocker):
        """A clean exit returns the code and raises nothing."""
        assert self._run(mocker, 0) == 0
