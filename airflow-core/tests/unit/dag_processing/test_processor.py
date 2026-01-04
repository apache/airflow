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

import inspect
import pathlib
import sys
import textwrap
import typing
import uuid
from collections.abc import Callable
from socket import socketpair
from typing import TYPE_CHECKING, BinaryIO
from unittest.mock import MagicMock, patch

import pytest
import structlog
from pydantic import TypeAdapter
from sqlalchemy import select
from structlog.typing import FilteringBoundLogger

from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    DagRun as DRDataModel,
    TaskInstance as TIDataModel,
    TIRunContext,
)
from airflow.callbacks.callback_requests import (
    CallbackRequest,
    DagCallbackRequest,
    DagRunContext,
    EmailRequest,
    TaskCallbackRequest,
)
from airflow.dag_processing.dagbag import DagBag
from airflow.dag_processing.manager import process_parse_results
from airflow.dag_processing.processor import (
    DagFileParseRequest,
    DagFileParsingResult,
    DagFileProcessorProcess,
    ToDagProcessor,
    ToManager,
    _execute_dag_callbacks,
    _execute_email_callbacks,
    _execute_task_callbacks,
    _parse_file,
    _pre_import_airflow_modules,
)
from airflow.models import DagRun
from airflow.sdk import DAG, BaseOperator
from airflow.sdk.api.client import Client
from airflow.sdk.api.datamodels._generated import DagRunState
from airflow.sdk.execution_time import comms
from airflow.sdk.execution_time.comms import (
    GetTaskStates,
    GetTICount,
    GetXCom,
    GetXComSequenceSlice,
    TaskStatesResult,
    TICount,
    ToSupervisor,
    ToTask,
    XComResult,
    XComSequenceSliceResult,
)
from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
from airflow.utils.session import create_session
from airflow.utils.state import TaskInstanceState

from tests_common.test_utils.config import conf_vars, env_vars

if TYPE_CHECKING:
    from kgb import SpyAgency

pytestmark = pytest.mark.db_test

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

# Filename to be used for dags that are created in an ad-hoc manner and can be removed/
# created at runtime
TEST_DAG_FOLDER = pathlib.Path(__file__).parents[1].resolve() / "dags"


@pytest.fixture(scope="class")
def disable_load_example():
    with conf_vars({("core", "load_examples"): "false"}):
        with env_vars({"AIRFLOW__CORE__LOAD_EXAMPLES": "false"}):
            yield


@pytest.fixture
def inprocess_client():
    """Provides an in-process Client backed by a single API server."""
    api = InProcessExecutionAPI()
    client = Client(base_url=None, token="", dry_run=True, transport=api.transport)
    client.base_url = "http://in-process.invalid/"
    return client


@pytest.mark.usefixtures("disable_load_example")
class TestDagFileProcessor:
    def _process_file(
        self, file_path, callback_requests: list[CallbackRequest] | None = None
    ) -> DagFileParsingResult | None:
        return _parse_file(
            DagFileParseRequest(
                file=file_path,
                bundle_path=TEST_DAG_FOLDER,
                bundle_name="testing",
                callback_requests=callback_requests or [],
            ),
            log=structlog.get_logger(),
        )

    def test_dagbag_import_errors_captured(self, spy_agency: SpyAgency):
        @spy_agency.spy_for(DagBag.collect_dags, owner=DagBag)
        def fake_collect_dags(dagbag: DagBag, *args, **kwargs):
            dagbag.import_errors["a.py"] = "Import error"

        resp = self._process_file("a.py")
        assert resp is not None
        assert not resp.serialized_dags
        assert resp.import_errors is not None
        assert "a.py" in resp.import_errors

    def test_serialization_errors_use_relative_paths(self, tmp_path: pathlib.Path):
        """
        Test that serialization errors use relative file paths.
        
        This ensures that errors during DAG serialization (e.g., in _serialize_dags)
        are stored with relative paths, matching the format of parse-time import errors.
        This is critical for bundle-backed DAGs (Git, S3, etc.) where import errors
        need to be properly persisted to the database.
        """
        # Create a DAG file that will fail during serialization
        dag_file = tmp_path / "test_serialization_error.py"
        dag_file.write_text(textwrap.dedent("""
            from airflow.sdk import DAG
            from airflow.providers.standard.operators.empty import EmptyOperator
            from datetime import datetime
            
            # Create a DAG that will fail during serialization
            # by having a non-serializable custom attribute
            dag = DAG("test_dag", start_date=datetime(2023, 1, 1))
            
            # Add a non-serializable object that will cause serialization to fail
            class NonSerializable:
                def __getstate__(self):
                    raise TypeError("Cannot serialize this object")
            
            dag._non_serializable = NonSerializable()
            
            task = EmptyOperator(task_id="test_task", dag=dag)
        """))

        # Process the file with bundle_path set
        resp = _parse_file(
            DagFileParseRequest(
                file=str(dag_file),
                bundle_path=tmp_path,
                bundle_name="testing",
                callback_requests=[],
            ),
            log=structlog.get_logger(),
        )

        assert resp is not None
        # The DAG should have been parsed successfully
        assert len(resp.serialized_dags) >= 0
        
        # Check that any serialization errors use relative paths, not absolute paths
        if resp.import_errors:
            for error_path in resp.import_errors.keys():
                # The error path should be relative (just the filename)
                # not an absolute path
                assert not pathlib.Path(error_path).is_absolute(), (
                    f"Serialization error path '{error_path}' should be relative, not absolute. "
                    f"This ensures consistency across bundle types (Git, Local, etc.)"
                )
                # For this test, it should be the filename relative to bundle_path
                assert error_path == "test_serialization_error.py", (
                    f"Expected relative path 'test_serialization_error.py', got '{error_path}'"
                )

    def test_top_level_variable_access(
        self,
        spy_agency: SpyAgency,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        inprocess_client,
    ):
        logger = MagicMock(spec=FilteringBoundLogger)
        logger_filehandle = MagicMock(spec=BinaryIO)

        def dag_in_a_fn():
            from airflow.sdk import DAG, Variable

            with DAG(f"test_{Variable.get('myvar')}"):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)

        monkeypatch.setenv("AIRFLOW_VAR_MYVAR", "abc")
        proc = DagFileProcessorProcess.start(
            id=1,
            path=path,
            bundle_path=tmp_path,
            bundle_name="testing",
            callbacks=[],
            logger=logger,
            logger_filehandle=logger_filehandle,
            client=inprocess_client,
        )

        while not proc.is_ready:
            proc._service_subprocess(0.1)

        result = proc.parsing_result
        assert result is not None
        assert result.import_errors == {}
        assert result.serialized_dags[0].dag_id == "test_abc"

    def test_top_level_variable_access_not_found(
        self,
        spy_agency: SpyAgency,
        tmp_path: pathlib.Path,
        monkeypatch: pytest.MonkeyPatch,
        inprocess_client,
    ):
        logger = MagicMock(spec=FilteringBoundLogger)
        logger_filehandle = MagicMock(spec=BinaryIO)

        def dag_in_a_fn():
            from airflow.sdk import DAG, Variable

            with DAG(f"test_{Variable.get('myvar')}"):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)
        proc = DagFileProcessorProcess.start(
            id=1,
            path=path,
            bundle_path=tmp_path,
            bundle_name="testing",
            callbacks=[],
            logger=logger,
            logger_filehandle=logger_filehandle,
            client=inprocess_client,
        )

        while not proc.is_ready:
            proc._service_subprocess(0.1)

        result = proc.parsing_result
        assert result is not None
        assert result.import_errors != {}
        if result.import_errors:
            assert "VARIABLE_NOT_FOUND" in next(iter(result.import_errors.values()))

    def test_top_level_variable_set(self, tmp_path: pathlib.Path, inprocess_client):
        from airflow.models.variable import Variable as VariableORM

        logger = MagicMock(spec=FilteringBoundLogger)
        logger_filehandle = MagicMock(spec=BinaryIO)

        def dag_in_a_fn():
            from airflow.sdk import DAG, Variable

            Variable.set(key="mykey", value="myvalue")
            with DAG(f"test_{Variable.get('mykey')}"):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)
        proc = DagFileProcessorProcess.start(
            id=1,
            path=path,
            bundle_path=tmp_path,
            bundle_name="testing",
            callbacks=[],
            logger=logger,
            logger_filehandle=logger_filehandle,
            client=inprocess_client,
        )

        while not proc.is_ready:
            proc._service_subprocess(0.1)

        with create_session() as session:
            result = proc.parsing_result
            assert result is not None
            assert result.import_errors == {}
            assert result.serialized_dags[0].dag_id == "test_myvalue"

            all_vars = session.scalars(select(VariableORM)).all()
            assert len(all_vars) == 1
            assert all_vars[0].key == "mykey"

    def test_top_level_variable_delete(self, tmp_path: pathlib.Path, inprocess_client):
        from airflow.models.variable import Variable as VariableORM

        logger = MagicMock(spec=FilteringBoundLogger)
        logger_filehandle = MagicMock(spec=BinaryIO)

        def dag_in_a_fn():
            from airflow.sdk import DAG, Variable

            Variable.set(key="mykey", value="myvalue")
            Variable.delete(key="mykey")
            try:
                v = Variable.get(key="mykey")
            except Exception:
                v = "not-found"

            with DAG(v):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)
        proc = DagFileProcessorProcess.start(
            id=1,
            path=path,
            bundle_path=tmp_path,
            bundle_name="testing",
            callbacks=[],
            logger=logger,
            logger_filehandle=logger_filehandle,
            client=inprocess_client,
        )

        while not proc.is_ready:
            proc._service_subprocess(0.1)

        with create_session() as session:
            result = proc.parsing_result
            assert result is not None
            assert result.import_errors == {}
            assert result.serialized_dags[0].dag_id == "not-found"

            all_vars = session.scalars(select(VariableORM)).all()
            assert len(all_vars) == 0

    def test_top_level_connection_access(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch, inprocess_client
    ):
        logger = MagicMock(spec=FilteringBoundLogger)
        logger_filehandle = MagicMock(spec=BinaryIO)

        def dag_in_a_fn():
            from airflow.sdk import DAG, BaseHook

            with DAG(f"test_{BaseHook.get_connection(conn_id='my_conn').conn_id}"):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)

        monkeypatch.setenv("AIRFLOW_CONN_MY_CONN", '{"conn_type": "aws"}')
        proc = DagFileProcessorProcess.start(
            id=1,
            path=path,
            bundle_path=tmp_path,
            bundle_name="testing",
            callbacks=[],
            logger=logger,
            logger_filehandle=logger_filehandle,
            client=inprocess_client,
        )

        while not proc.is_ready:
            proc._service_subprocess(0.1)

        result = proc.parsing_result
        assert result is not None
        assert result.import_errors == {}
        assert result.serialized_dags[0].dag_id == "test_my_conn"

    def test_top_level_connection_access_not_found(self, tmp_path: pathlib.Path, inprocess_client):
        logger = MagicMock(spec=FilteringBoundLogger)
        logger_filehandle = MagicMock(spec=BinaryIO)

        def dag_in_a_fn():
            from airflow.sdk import DAG, BaseHook

            with DAG(f"test_{BaseHook.get_connection(conn_id='my_conn').conn_id}"):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)
        proc = DagFileProcessorProcess.start(
            id=1,
            path=path,
            bundle_path=tmp_path,
            bundle_name="testing",
            callbacks=[],
            logger=logger,
            logger_filehandle=logger_filehandle,
            client=inprocess_client,
        )

        while not proc.is_ready:
            proc._service_subprocess(0.1)

        result = proc.parsing_result
        assert result is not None
        assert result.import_errors != {}
        if result.import_errors:
            assert "The conn_id `my_conn` isn't defined" in next(iter(result.import_errors.values()))

    def test_import_module_in_bundle_root(self, tmp_path: pathlib.Path, inprocess_client):
        tmp_path.joinpath("util.py").write_text("NAME = 'dag_name'")

        dag1_path = tmp_path.joinpath("dag1.py")
        dag1_code = """
        from util import NAME

        from airflow.sdk import DAG

        with DAG(NAME):
            pass
        """
        dag1_path.write_text(textwrap.dedent(dag1_code))

        proc = DagFileProcessorProcess.start(
            id=1,
            path=dag1_path,
            bundle_path=tmp_path,
            bundle_name="testing",
            callbacks=[],
            logger=MagicMock(spec=FilteringBoundLogger),
            logger_filehandle=MagicMock(spec=BinaryIO),
            client=inprocess_client,
        )
        while not proc.is_ready:
            proc._service_subprocess(0.1)

        result = proc.parsing_result
        assert result is not None
        assert result.import_errors == {}
        assert result.serialized_dags[0].dag_id == "dag_name"

    def test__pre_import_airflow_modules_when_disabled(self):
        logger = MagicMock(spec=FilteringBoundLogger)
        with (
            env_vars({"AIRFLOW__DAG_PROCESSOR__PARSING_PRE_IMPORT_MODULES": "false"}),
            patch("airflow.dag_processing.processor.iter_airflow_imports") as mock_iter,
        ):
            _pre_import_airflow_modules("test.py", logger)

        mock_iter.assert_not_called()
        logger.warning.assert_not_called()

    def test__pre_import_airflow_modules_when_enabled(self):
        logger = MagicMock(spec=FilteringBoundLogger)
        with (
            env_vars({"AIRFLOW__DAG_PROCESSOR__PARSING_PRE_IMPORT_MODULES": "true"}),
            patch("airflow.dag_processing.processor.iter_airflow_imports", return_value=["airflow.models"]),
            patch("airflow.dag_processing.processor.importlib.import_module") as mock_import,
        ):
            _pre_import_airflow_modules("test.py", logger)

        mock_import.assert_called_once_with("airflow.models")
        logger.warning.assert_not_called()

    @pytest.mark.parametrize(
        "exception",
        [
            ModuleNotFoundError("module not found"),
            RuntimeError("import failed"),
            ImportError("import error"),
        ],
    )
    def test__pre_import_airflow_modules_warns_on_import_errors(self, exception):
        """Test that pre-import logs warnings for any import exception type."""
        logger = MagicMock(spec=FilteringBoundLogger)
        with (
            env_vars({"AIRFLOW__DAG_PROCESSOR__PARSING_PRE_IMPORT_MODULES": "true"}),
            patch("airflow.dag_processing.processor.iter_airflow_imports", return_value=["some_module"]),
            patch("airflow.dag_processing.processor.importlib.import_module", side_effect=exception),
        ):
            _pre_import_airflow_modules("test.py", logger)

        logger.warning.assert_called_once()
        warning_args = logger.warning.call_args[0]
        assert "Error when trying to pre-import module" in warning_args[0]
        assert "some_module" in warning_args[1]
        assert "test.py" in warning_args[2]

    def test__pre_import_airflow_modules_partial_success_and_warning(self):
        logger = MagicMock(spec=FilteringBoundLogger)
        with (
            env_vars({"AIRFLOW__DAG_PROCESSOR__PARSING_PRE_IMPORT_MODULES": "true"}),
            patch(
                "airflow.dag_processing.processor.iter_airflow_imports",
                return_value=["airflow.models", "non_existent_module"],
            ),
            patch(
                "airflow.dag_processing.processor.importlib.import_module",
                side_effect=[None, ModuleNotFoundError()],
            ),
        ):
            _pre_import_airflow_modules("test.py", logger)

        assert logger.warning.call_count == 1


def write_dag_in_a_fn_to_file(fn: Callable[[], None], folder: pathlib.Path) -> pathlib.Path:
    # Create the dag in a fn, and use inspect.getsource to write it to a file so that
    # a) the test dag is directly viewable here in the tests
    # b) that it shows to IDEs/mypy etc.
    assert folder.is_dir()
    name = fn.__name__
    path = folder.joinpath(name + ".py")
    path.write_text(textwrap.dedent(inspect.getsource(fn)) + f"\n\n{name}()")

    return path


@pytest.fixture
def disable_capturing():
    old_in, old_out, old_err = sys.stdin, sys.stdout, sys.stderr

    sys.stdin = sys.__stdin__
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    yield
    sys.stdin, sys.stdout, sys.stderr = old_in, old_out, old_err


@pytest.mark.usefixtures("testing_dag_bundle")
@pytest.mark.usefixtures("disable_capturing")
def test_parse_file_entrypoint_parses_dag_callbacks(mocker):
    r, w = socketpair()

    frame = comms._ResponseFrame(
        id=1,
        body={
            "file": "/files/dags/wait.py",
            "bundle_path": "/files/dags",
            "bundle_name": "testing",
            "callback_requests": [
                {
                    "filepath": "wait.py",
                    "bundle_name": "testing",
                    "bundle_version": None,
                    "msg": "task_failure",
                    "dag_id": "wait_to_fail",
                    "run_id": "manual__2024-12-30T21:02:55.203691+00:00",
                    "is_failure_callback": True,
                    "type": "DagCallbackRequest",
                }
            ],
            "type": "DagFileParseRequest",
        },
    )
    bytes = frame.as_bytes()
    w.sendall(bytes)

    decoder = comms.CommsDecoder[DagFileParseRequest, DagFileParsingResult](
        socket=r,
        body_decoder=TypeAdapter[DagFileParseRequest](DagFileParseRequest),
    )

    msg = decoder._get_response()
    assert isinstance(msg, DagFileParseRequest)
    assert msg.file == "/files/dags/wait.py"
    assert msg.callback_requests == [
        DagCallbackRequest(
            filepath="wait.py",
            msg="task_failure",
            dag_id="wait_to_fail",
            run_id="manual__2024-12-30T21:02:55.203691+00:00",
            is_failure_callback=True,
            bundle_name="testing",
            bundle_version=None,
        )
    ]


def test_parse_file_with_dag_callbacks(spy_agency):
    from airflow import DAG

    called = False

    def on_failure(context):
        nonlocal called
        called = True

    dag = DAG(dag_id="a", on_failure_callback=on_failure)

    def fake_collect_dags(self, *args, **kwargs):
        self.dags[dag.dag_id] = dag

    spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

    requests = [
        DagCallbackRequest(
            filepath="A",
            msg="Message",
            dag_id="a",
            run_id="b",
            bundle_name="testing",
            bundle_version=None,
        )
    ]
    _parse_file(
        DagFileParseRequest(
            file="A", bundle_path="no matter", bundle_name="testing", callback_requests=requests
        ),
        log=structlog.get_logger(),
    )

    assert called is True


def test_parse_file_with_task_callbacks(spy_agency):
    called = False

    def on_failure(context):
        nonlocal called
        called = True

    with DAG(dag_id="a", on_failure_callback=on_failure) as dag:
        BaseOperator(task_id="b", on_failure_callback=on_failure)

    def fake_collect_dags(self, *args, **kwargs):
        self.dags[dag.dag_id] = dag

    spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

    # Create a minimal TaskInstance for the request
    ti_data = TIDataModel(
        id=uuid.uuid4(),
        dag_id="a",
        task_id="b",
        run_id="test_run",
        map_index=-1,
        try_number=1,
        dag_version_id=uuid.uuid4(),
    )

    requests = [
        TaskCallbackRequest(
            filepath="A",
            msg="Message",
            ti=ti_data,
            bundle_name="testing",
            bundle_version=None,
        )
    ]
    _parse_file(
        DagFileParseRequest(file="A", bundle_path="test", bundle_name="testing", callback_requests=requests),
        log=structlog.get_logger(),
    )

    assert called is True


def test_callback_processing_does_not_update_timestamps(session):
    """Callback processing should not update last_finish_time to prevent stale DAG detection."""
    stat = process_parse_results(
        run_duration=1.0,
        finish_time=timezone.utcnow(),
        run_count=5,
        bundle_name="test",
        bundle_version=None,
        parsing_result=None,
        session=session,
        is_callback_only=True,
    )

    assert stat.last_finish_time is None
    assert stat.run_count == 5


def test_normal_parsing_updates_timestamps(session):
    """last_finish_time should be updated when parsing a dag file."""
    finish_time = timezone.utcnow()

    stat = process_parse_results(
        run_duration=2.0,
        finish_time=finish_time,
        run_count=3,
        bundle_name="test-bundle",
        bundle_version="v1",
        parsing_result=DagFileParsingResult(fileloc="test.py", serialized_dags=[]),
        session=session,
        is_callback_only=False,
    )

    assert stat.last_finish_time == finish_time
    assert stat.run_count == 4
    assert stat.import_errors == 0


def test_import_error_updates_timestamps(session):
    """last_finish_time should be updated when parsing a dag file results in import errors."""
    finish_time = timezone.utcnow()

    stat = process_parse_results(
        run_duration=1.5,
        finish_time=finish_time,
        run_count=2,
        bundle_name="test-bundle",
        bundle_version="v1",
        parsing_result=None,
        session=session,
        is_callback_only=False,
    )

    assert stat.last_finish_time == finish_time
    assert stat.run_count == 3
    assert stat.import_errors == 1


class TestExecuteDagCallbacks:
    """Test the _execute_dag_callbacks function with context_from_server"""

    def test_execute_dag_callbacks_with_context_from_server(self, spy_agency):
        """Test _execute_dag_callbacks uses RuntimeTaskInstance context when context_from_server is provided"""
        called = False
        context_received = None

        def on_failure(context):
            nonlocal called, context_received
            called = True
            context_received = context

        with DAG(dag_id="test_dag", on_failure_callback=on_failure) as dag:
            BaseOperator(task_id="test_task")

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        current_time = timezone.utcnow()
        dag_run_data = DRDataModel(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=current_time,
            data_interval_start=current_time,
            data_interval_end=current_time,
            run_after=current_time,
            start_date=current_time,
            end_date=None,
            run_type="manual",
            state="running",
            consumed_asset_events=[],
            partition_key=None,
        )

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            dag_version_id=uuid.uuid4(),
        )

        context_from_server = DagRunContext(dag_run=dag_run_data, last_ti=ti_data)

        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="test_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            context_from_server=context_from_server,
            is_failure_callback=True,
            msg="Test failure message",
        )

        log = structlog.get_logger()
        _execute_dag_callbacks(dagbag, request, log)

        assert called is True
        assert context_received is not None
        # When context_from_server is provided, we get a full RuntimeTaskInstance context
        assert "dag_run" in context_received
        assert "logical_date" in context_received
        assert "reason" in context_received
        assert context_received["reason"] == "Test failure message"
        # Check that we have template context variables from RuntimeTaskInstance
        assert "ts" in context_received
        assert "params" in context_received

    def test_execute_dag_callbacks_without_context_from_server(self, spy_agency):
        """Test _execute_dag_callbacks falls back to simple context when context_from_server is None"""
        called = False
        context_received = None

        def on_failure(context):
            nonlocal called, context_received
            called = True
            context_received = context

        with DAG(dag_id="test_dag", on_failure_callback=on_failure) as dag:
            BaseOperator(task_id="test_task")

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="test_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            context_from_server=None,  # No context from server
            is_failure_callback=True,
            msg="Test failure message",
        )

        log = structlog.get_logger()
        _execute_dag_callbacks(dagbag, request, log)

        assert called is True
        assert context_received is not None
        # When context_from_server is None, we get simple context
        assert context_received["dag"] == dag
        assert context_received["run_id"] == "test_run"
        assert context_received["reason"] == "Test failure message"
        # Should not have template context variables
        assert "ts" not in context_received
        assert "params" not in context_received

    def test_execute_dag_callbacks_success_callback(self, spy_agency):
        """Test _execute_dag_callbacks executes success callback with context_from_server"""
        called = False
        context_received = None

        def on_success(context):
            nonlocal called, context_received
            called = True
            context_received = context

        with DAG(dag_id="test_dag", on_success_callback=on_success) as dag:
            BaseOperator(task_id="test_task")

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        # Create test data
        current_time = timezone.utcnow()
        dag_run_data = DRDataModel(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=current_time,
            data_interval_start=current_time,
            data_interval_end=current_time,
            run_after=current_time,
            start_date=current_time,
            end_date=None,
            run_type="manual",
            state="success",
            consumed_asset_events=[],
            partition_key=None,
        )

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            dag_version_id=uuid.uuid4(),
        )

        context_from_server = DagRunContext(dag_run=dag_run_data, last_ti=ti_data)

        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="test_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            context_from_server=context_from_server,
            is_failure_callback=False,  # Success callback
            msg="Test success message",
        )

        log = structlog.get_logger()
        _execute_dag_callbacks(dagbag, request, log)

        assert called is True
        assert context_received is not None
        assert "dag_run" in context_received
        assert context_received["reason"] == "Test success message"

    def test_execute_dag_callbacks_multiple_callbacks(self, spy_agency):
        """Test _execute_dag_callbacks executes multiple callbacks"""
        call_count = 0

        def on_failure_1(context):
            nonlocal call_count
            call_count += 1

        def on_failure_2(context):
            nonlocal call_count
            call_count += 1

        with DAG(dag_id="test_dag", on_failure_callback=[on_failure_1, on_failure_2]) as dag:
            BaseOperator(task_id="test_task")

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="test_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            is_failure_callback=True,
            msg="Test failure message",
        )

        log = structlog.get_logger()
        _execute_dag_callbacks(dagbag, request, log)

        assert call_count == 2

    def test_execute_dag_callbacks_no_callback_defined(self, spy_agency):
        """Test _execute_dag_callbacks when no callback is defined"""
        with DAG(dag_id="test_dag") as dag:  # No callbacks defined
            BaseOperator(task_id="test_task")

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="test_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            is_failure_callback=True,
            msg="Test failure message",
        )

        log = MagicMock(spec=FilteringBoundLogger)
        _execute_dag_callbacks(dagbag, request, log)

        # Should log warning about no callback found
        log.warning.assert_called_once_with("Callback requested, but dag didn't have any", dag_id="test_dag")

    def test_execute_dag_callbacks_missing_dag(self):
        """Test _execute_dag_callbacks raises ValueError for missing DAG"""
        dagbag = DagBag()

        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="missing_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            is_failure_callback=True,
            msg="Test failure message",
        )

        log = structlog.get_logger()

        with pytest.raises(ValueError, match="DAG 'missing_dag' not found in DagBag"):
            _execute_dag_callbacks(dagbag, request, log)

    @pytest.mark.parametrize(
        ("xcom_operation", "expected_message_type", "expected_message", "mock_response"),
        [
            (
                lambda ti, task_ids: ti.xcom_pull(key="report_df", task_ids=task_ids),
                "GetXComSequenceSlice",
                GetXComSequenceSlice(
                    key="report_df",
                    dag_id="test_dag",
                    run_id="test_run",
                    task_id="test_task",
                    start=None,
                    stop=None,
                    step=None,
                    include_prior_dates=False,
                ),
                XComSequenceSliceResult(root=["test data"]),
            ),
            (
                lambda ti, task_ids: ti.xcom_pull(key="single_value", task_ids=["test_task"]),
                "GetXComSequenceSlice",
                GetXComSequenceSlice(
                    key="single_value",
                    dag_id="test_dag",
                    run_id="test_run",
                    task_id="test_task",
                    start=None,
                    stop=None,
                    step=None,
                    include_prior_dates=False,
                ),
                XComSequenceSliceResult(root=["test data"]),
            ),
            (
                lambda ti, task_ids: ti.xcom_pull(key="direct_value", task_ids="test_task", map_indexes=None),
                "GetXCom",
                GetXCom(
                    key="direct_value",
                    dag_id="test_dag",
                    run_id="test_run",
                    task_id="test_task",
                    map_index=None,
                    include_prior_dates=False,
                ),
                XComResult(
                    key="direct_value",
                    value="test",
                ),
            ),
        ],
    )
    def test_notifier_xcom_operations_send_correct_messages(
        self,
        spy_agency,
        mock_supervisor_comms,
        xcom_operation,
        expected_message_type,
        expected_message,
        mock_response,
    ):
        """Test that different XCom operations send correct message types"""

        mock_supervisor_comms.send.return_value = mock_response

        class TestNotifier:
            def __call__(self, context):
                ti = context["ti"]
                dag = context["dag"]
                task_ids = list(dag.task_dict)
                xcom_operation(ti, task_ids)

        with DAG(dag_id="test_dag", on_success_callback=TestNotifier()) as dag:
            BaseOperator(task_id="test_task")

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        current_time = timezone.utcnow()
        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="test_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            context_from_server=DagRunContext(
                dag_run=DRDataModel(
                    dag_id="test_dag",
                    run_id="test_run",
                    logical_date=current_time,
                    data_interval_start=current_time,
                    data_interval_end=current_time,
                    run_after=current_time,
                    start_date=current_time,
                    end_date=None,
                    run_type="manual",
                    state="success",
                    consumed_asset_events=[],
                    partition_key=None,
                ),
                last_ti=TIDataModel(
                    id=uuid.uuid4(),
                    dag_id="test_dag",
                    task_id="test_task",
                    run_id="test_run",
                    map_index=-1,
                    try_number=1,
                    dag_version_id=uuid.uuid4(),
                ),
            ),
            is_failure_callback=False,
            msg="Test success message",
        )

        _execute_dag_callbacks(dagbag, request, structlog.get_logger())

        mock_supervisor_comms.send.assert_called_once_with(msg=expected_message)

    @pytest.mark.parametrize(
        ("request_operation", "operation_type", "mock_response", "operation_response"),
        [
            (
                lambda context: context["task_instance"].get_ti_count(dag_id="test_dag"),
                GetTICount(dag_id="test_dag"),
                TICount(count=2),
                "Got response 2",
            ),
            (
                lambda context: context["task_instance"].get_task_states(
                    dag_id="test_dag", task_ids=["test_task"]
                ),
                GetTaskStates(
                    dag_id="test_dag",
                    task_ids=["test_task"],
                ),
                TaskStatesResult(task_states={"test_run": {"task1": "running"}}),
                "Got response {'test_run': {'task1': 'running'}}",
            ),
        ],
    )
    def test_dagfileprocessorprocess_request_handler_operations(
        self,
        spy_agency,
        mock_supervisor_comms,
        request_operation,
        operation_type,
        mock_response,
        operation_response,
        caplog,
    ):
        """Test that DagFileProcessorProcess Request Handler Operations"""

        mock_supervisor_comms.send.return_value = mock_response

        def callback_fn(context):
            log = structlog.get_logger()
            log.info("Callback started..")
            log.info("Got response %s", request_operation(context))

        with DAG(dag_id="test_dag", on_success_callback=callback_fn) as dag:
            BaseOperator(task_id="test_task")

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        current_time = timezone.utcnow()
        request = DagCallbackRequest(
            filepath="test.py",
            dag_id="test_dag",
            run_id="test_run",
            bundle_name="testing",
            bundle_version=None,
            context_from_server=DagRunContext(
                dag_run=DRDataModel(
                    dag_id="test_dag",
                    run_id="test_run",
                    logical_date=current_time,
                    data_interval_start=current_time,
                    data_interval_end=current_time,
                    run_after=current_time,
                    start_date=current_time,
                    end_date=None,
                    run_type="manual",
                    state="success",
                    consumed_asset_events=[],
                    partition_key=None,
                ),
                last_ti=TIDataModel(
                    id=uuid.uuid4(),
                    dag_id="test_dag",
                    task_id="test_task",
                    run_id="test_run",
                    map_index=-1,
                    try_number=1,
                    dag_version_id=uuid.uuid4(),
                ),
            ),
            is_failure_callback=False,
            msg="Test success message",
        )

        _execute_dag_callbacks(dagbag, request, structlog.get_logger())

        mock_supervisor_comms.send.assert_called_once_with(msg=operation_type)
        assert operation_response in caplog.text


class TestExecuteTaskCallbacks:
    """Test the _execute_task_callbacks function"""

    def test_execute_task_callbacks_failure_callback(self, spy_agency):
        """Test _execute_task_callbacks executes failure callbacks"""
        called = False
        context_received = None

        def on_failure(context):
            nonlocal called, context_received
            called = True
            context_received = context

        with DAG(dag_id="test_dag") as dag:
            BaseOperator(task_id="test_task", on_failure_callback=on_failure)

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=1,
            dag_version_id=uuid.uuid4(),
        )

        request = TaskCallbackRequest(
            filepath="test.py",
            msg="Task failed",
            ti=ti_data,
            bundle_name="testing",
            bundle_version=None,
            task_callback_type=TaskInstanceState.FAILED,
        )

        log = structlog.get_logger()
        _execute_task_callbacks(dagbag, request, log)

        assert called is True
        assert context_received is not None
        assert context_received["dag"] == dag
        assert "ti" in context_received

    def test_execute_task_callbacks_retry_callback(self, spy_agency):
        """Test _execute_task_callbacks executes retry callbacks"""
        called = False
        context_received = None

        def on_retry(context):
            nonlocal called, context_received
            called = True
            context_received = context

        with DAG(dag_id="test_dag") as dag:
            BaseOperator(task_id="test_task", on_retry_callback=on_retry)

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            map_index=-1,
            try_number=1,
            dag_version_id=uuid.uuid4(),
            state=TaskInstanceState.UP_FOR_RETRY,
        )

        request = TaskCallbackRequest(
            filepath="test.py",
            msg="Task retrying",
            ti=ti_data,
            bundle_name="testing",
            bundle_version=None,
            task_callback_type=TaskInstanceState.UP_FOR_RETRY,
        )

        log = structlog.get_logger()
        _execute_task_callbacks(dagbag, request, log)

        assert called is True
        assert context_received is not None
        assert context_received["dag"] == dag
        assert "ti" in context_received

    def test_execute_task_callbacks_with_context_from_server(self, spy_agency):
        """Test _execute_task_callbacks with context_from_server creates full context"""
        called = False
        context_received = None

        def on_failure(context):
            nonlocal called, context_received
            called = True
            context_received = context

        with DAG(dag_id="test_dag") as dag:
            BaseOperator(task_id="test_task", on_failure_callback=on_failure)

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        dag_run = DagRun(
            dag_id="test_dag",
            run_id="test_run",
            logical_date=timezone.utcnow(),
            start_date=timezone.utcnow(),
            run_type="manual",
            state=DagRunState.RUNNING,
        )
        dag_run.run_after = timezone.utcnow()

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=1,
            dag_version_id=uuid.uuid4(),
        )

        context_from_server = TIRunContext(
            dag_run=dag_run,
            max_tries=3,
        )

        request = TaskCallbackRequest(
            filepath="test.py",
            msg="Task failed",
            ti=ti_data,
            bundle_name="testing",
            bundle_version=None,
            task_callback_type=TaskInstanceState.FAILED,
            context_from_server=context_from_server,
        )

        log = structlog.get_logger()
        _execute_task_callbacks(dagbag, request, log)

        assert called is True
        assert context_received is not None
        # When context_from_server is provided, we get a full RuntimeTaskInstance context
        assert "dag_run" in context_received
        assert "logical_date" in context_received

    def test_execute_task_callbacks_not_failure_callback(self, spy_agency):
        """Test _execute_task_callbacks when request is not a failure callback"""
        called = False

        def on_failure(context):
            nonlocal called
            called = True

        with DAG(dag_id="test_dag") as dag:
            BaseOperator(task_id="test_task", on_failure_callback=on_failure)

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=1,
            dag_version_id=uuid.uuid4(),
            state=TaskInstanceState.SUCCESS,
        )

        request = TaskCallbackRequest(
            filepath="test.py",
            msg="Task succeeded",
            ti=ti_data,
            bundle_name="testing",
            bundle_version=None,
            task_callback_type=TaskInstanceState.SUCCESS,
        )

        log = structlog.get_logger()
        _execute_task_callbacks(dagbag, request, log)

        # Should not call the callback since it's not a failure callback
        assert called is False

    def test_execute_task_callbacks_multiple_callbacks(self, spy_agency):
        """Test _execute_task_callbacks with multiple callbacks"""
        call_count = 0

        def on_failure_1(context):
            nonlocal call_count
            call_count += 1

        def on_failure_2(context):
            nonlocal call_count
            call_count += 1

        with DAG(dag_id="test_dag") as dag:
            BaseOperator(task_id="test_task", on_failure_callback=[on_failure_1, on_failure_2])

        def fake_collect_dags(self, *args, **kwargs):
            self.dags[dag.dag_id] = dag

        spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

        dagbag = DagBag()
        dagbag.collect_dags()

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id="test_dag",
            task_id="test_task",
            run_id="test_run",
            try_number=1,
            dag_version_id=uuid.uuid4(),
            state=TaskInstanceState.FAILED,
        )

        request = TaskCallbackRequest(
            filepath="test.py",
            msg="Task failed",
            ti=ti_data,
            bundle_name="testing",
            bundle_version=None,
            task_callback_type=TaskInstanceState.FAILED,
        )

        log = structlog.get_logger()
        _execute_task_callbacks(dagbag, request, log)

        assert call_count == 2

    @pytest.mark.parametrize(
        ("dag_exists", "task_exists", "expected_error"),
        [
            (False, False, "DAG 'missing_dag' not found in DagBag"),
            (True, False, "Task 'missing_task' not found in DAG 'test_dag'"),
        ],
    )
    def test_execute_task_callbacks_missing_dag_or_task(
        self, spy_agency, dag_exists, task_exists, expected_error
    ):
        """Test _execute_task_callbacks raises ValueError for missing DAG or task"""
        if dag_exists:
            with DAG(dag_id="test_dag") as dag:
                BaseOperator(task_id="existing_task")

            def fake_collect_dags(self, *args, **kwargs):
                self.dags[dag.dag_id] = dag

            spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

            dagbag = DagBag()
            dagbag.collect_dags()
            dag_id = "test_dag"
            task_id = "missing_task"
        else:
            dagbag = DagBag()
            dag_id = "missing_dag"
            task_id = "test_task"

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id=dag_id,
            task_id=task_id,
            run_id="test_run",
            try_number=1,
            dag_version_id=uuid.uuid4(),
        )

        request = TaskCallbackRequest(
            filepath="test.py",
            msg="Task failed",
            ti=ti_data,
            bundle_name="testing",
            bundle_version=None,
            task_callback_type=TaskInstanceState.FAILED,
        )

        log = structlog.get_logger()

        with pytest.raises(ValueError, match=expected_error):
            _execute_task_callbacks(dagbag, request, log)


class TestExecuteEmailCallbacks:
    """Test the email callback execution functionality."""

    @patch("airflow.dag_processing.processor._send_error_email_notification")
    def test_execute_email_callbacks_failure(self, mock_send_email):
        """Test email callback execution for task failure."""
        dagbag = MagicMock(spec=DagBag)
        with DAG(dag_id="test_dag") as dag:
            task = BaseOperator(task_id="test_task", email="test@example.com")
        dagbag.dags = {"test_dag": dag}

        # Create TI data
        ti_data = TIDataModel(
            id=str(uuid.uuid4()),
            task_id="test_task",
            dag_id="test_dag",
            run_id="test_run",
            logical_date="2023-01-01T00:00:00Z",
            try_number=1,
            attempt_number=1,
            state="failed",
            dag_version_id=str(uuid.uuid4()),
        )

        current_time = timezone.utcnow()
        request = EmailRequest(
            filepath="/path/to/dag.py",
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            ti=ti_data,
            context_from_server=TIRunContext(
                dag_run=DRDataModel(
                    dag_id="test_dag",
                    run_id="test_run",
                    logical_date="2023-01-01T00:00:00Z",
                    data_interval_start=current_time,
                    data_interval_end=current_time,
                    run_after=current_time,
                    start_date=current_time,
                    end_date=None,
                    run_type="manual",
                    state="running",
                    consumed_asset_events=[],
                    partition_key=None,
                ),
                max_tries=2,
            ),
            email_type="failure",
            msg="Task failed",
        )

        log = MagicMock(spec=FilteringBoundLogger)
        runtime_ti = RuntimeTaskInstance.model_construct(
            **request.ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=request.context_from_server,
            max_tries=request.context_from_server.max_tries,
        )

        # Execute email callbacks
        _execute_email_callbacks(dagbag, request, log)

        # Verify email was sent
        mock_send_email.assert_called_once()
        call_args = mock_send_email.call_args[0]

        assert call_args[0] == task
        assert call_args[1].task_id == runtime_ti.task_id
        assert call_args[1].dag_id == runtime_ti.dag_id
        assert call_args[2] is not None  # context
        assert isinstance(call_args[3], Exception)
        assert call_args[3].args[0] == request.msg
        assert call_args[4] == log

    @patch("airflow.dag_processing.processor._send_error_email_notification")
    def test_execute_email_callbacks_retry(self, mock_send_email):
        """Test email callback execution for task retry."""
        dagbag = MagicMock(spec=DagBag)
        with DAG(dag_id="test_dag") as dag:
            task = BaseOperator(task_id="test_task", email=["test@example.com"])
        dagbag.dags = {"test_dag": dag}

        ti_data = TIDataModel(
            id=str(uuid.uuid4()),
            task_id="test_task",
            dag_id="test_dag",
            run_id="test_run",
            logical_date="2023-01-01T00:00:00Z",
            try_number=2,
            attempt_number=2,
            state="up_for_retry",
            dag_version_id=str(uuid.uuid4()),
        )

        current_time = timezone.utcnow()

        request = EmailRequest(
            filepath="/path/to/dag.py",
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            ti=ti_data,
            email_type="retry",
            context_from_server=TIRunContext(
                dag_run=DRDataModel(
                    dag_id="test_dag",
                    run_id="test_run",
                    logical_date="2023-01-01T00:00:00Z",
                    data_interval_start=current_time,
                    data_interval_end=current_time,
                    run_after=current_time,
                    start_date=current_time,
                    end_date=None,
                    run_type="manual",
                    state="running",
                    consumed_asset_events=[],
                    partition_key=None,
                ),
                max_tries=2,
            ),
            msg="Task retry",
        )

        log = MagicMock(spec=FilteringBoundLogger)
        runtime_ti = RuntimeTaskInstance.model_construct(
            **request.ti.model_dump(exclude_unset=True),
            task=task,
            _ti_context_from_server=request.context_from_server,
            max_tries=request.context_from_server.max_tries,
        )

        # Execute email callbacks
        _execute_email_callbacks(dagbag, request, log)

        mock_send_email.assert_called_once()
        call_args = mock_send_email.call_args[0]

        assert call_args[0] == task
        assert call_args[1].task_id == runtime_ti.task_id
        assert call_args[1].dag_id == runtime_ti.dag_id
        assert call_args[2] is not None  # context
        assert isinstance(call_args[3], Exception)
        assert call_args[3].args[0] == request.msg
        assert call_args[4] == log

    @patch("airflow.dag_processing.processor._send_error_email_notification")
    def test_execute_email_callbacks_no_email_configured(self, mock_send_email):
        """Test email callback when no email is configured."""
        dagbag = MagicMock(spec=DagBag)
        with DAG(dag_id="test_dag") as dag:
            BaseOperator(task_id="test_task", email=None)
        dagbag.dags = {"test_dag": dag}

        ti_data = TIDataModel(
            id=str(uuid.uuid4()),
            task_id="test_task",
            dag_id="test_dag",
            run_id="test_run",
            logical_date="2023-01-01T00:00:00Z",
            try_number=1,
            attempt_number=1,
            state="failed",
            dag_version_id=str(uuid.uuid4()),
        )

        current_time = timezone.utcnow()
        request = EmailRequest(
            filepath="/path/to/dag.py",
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            ti=ti_data,
            context_from_server=TIRunContext(
                dag_run=DRDataModel(
                    dag_id="test_dag",
                    run_id="test_run",
                    logical_date="2023-01-01T00:00:00Z",
                    data_interval_start=current_time,
                    data_interval_end=current_time,
                    run_after=current_time,
                    start_date=current_time,
                    end_date=None,
                    run_type="manual",
                    state="running",
                    consumed_asset_events=[],
                    partition_key=None,
                ),
                max_tries=2,
            ),
            email_type="failure",
        )

        log = MagicMock(spec=FilteringBoundLogger)

        # Execute email callbacks - should not raise exception
        _execute_email_callbacks(dagbag, request, log)

        # Verify warning was logged
        log.warning.assert_called_once()
        warning_call = log.warning.call_args[0][0]
        assert "Email callback requested but no email configured" in warning_call
        mock_send_email.assert_not_called()

    def test_execute_email_callbacks_email_disabled_for_type(self):
        """Test email callback when email is disabled for the specific type."""
        dagbag = MagicMock(spec=DagBag)
        with DAG(dag_id="test_dag") as dag:
            BaseOperator(task_id="test_task", email=["test@example.com"], email_on_failure=False)
        dagbag.dags = {"test_dag": dag}

        ti_data = TIDataModel(
            id=str(uuid.uuid4()),
            task_id="test_task",
            dag_id="test_dag",
            run_id="test_run",
            logical_date="2023-01-01T00:00:00Z",
            try_number=1,
            attempt_number=1,
            state="failed",
            dag_version_id=str(uuid.uuid4()),
        )

        current_time = timezone.utcnow()

        # Create request for failure (but email_on_failure is False)
        request = EmailRequest(
            filepath="/path/to/dag.py",
            bundle_name="test_bundle",
            bundle_version="1.0.0",
            ti=ti_data,
            context_from_server=TIRunContext(
                dag_run=DRDataModel(
                    dag_id="test_dag",
                    run_id="test_run",
                    logical_date="2023-01-01T00:00:00Z",
                    data_interval_start=current_time,
                    data_interval_end=current_time,
                    run_after=current_time,
                    start_date=current_time,
                    end_date=None,
                    run_type="manual",
                    state="running",
                    consumed_asset_events=[],
                    partition_key=None,
                ),
                max_tries=2,
            ),
            email_type="failure",
        )

        log = MagicMock(spec=FilteringBoundLogger)

        # Execute email callbacks
        _execute_email_callbacks(dagbag, request, log)

        # Verify info log about email being disabled
        log.info.assert_called_once()
        info_call = log.info.call_args[0][0]
        assert "Email not sent - task configured with email_on_" in info_call

    @pytest.mark.parametrize(
        ("dag_exists", "task_exists", "expected_error"),
        [
            (False, False, "DAG 'missing_dag' not found in DagBag"),
            (True, False, "Task 'missing_task' not found in DAG 'test_dag'"),
        ],
    )
    def test_execute_email_callbacks_missing_dag_or_task(
        self, spy_agency, dag_exists, task_exists, expected_error
    ):
        """Test _execute_email_callbacks raises ValueError for missing DAG or task"""
        if dag_exists:
            with DAG(dag_id="test_dag") as dag:
                BaseOperator(task_id="existing_task", email="test@example.com")

            def fake_collect_dags(self, *args, **kwargs):
                self.dags[dag.dag_id] = dag

            spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

            dagbag = DagBag()
            dagbag.collect_dags()
            dag_id = "test_dag"
            task_id = "missing_task"
        else:
            dagbag = DagBag()
            dag_id = "missing_dag"
            task_id = "test_task"

        ti_data = TIDataModel(
            id=uuid.uuid4(),
            dag_id=dag_id,
            task_id=task_id,
            run_id="test_run",
            try_number=1,
            dag_version_id=uuid.uuid4(),
        )

        current_time = timezone.utcnow()
        request = EmailRequest(
            filepath="test.py",
            bundle_name="testing",
            bundle_version=None,
            ti=ti_data,
            context_from_server=TIRunContext(
                dag_run=DRDataModel(
                    dag_id=dag_id,
                    run_id="test_run",
                    logical_date=current_time,
                    data_interval_start=current_time,
                    data_interval_end=current_time,
                    run_after=current_time,
                    start_date=current_time,
                    end_date=None,
                    run_type="manual",
                    state="running",
                    consumed_asset_events=[],
                    partition_key=None,
                ),
                max_tries=2,
            ),
            email_type="failure",
            msg="Task failed",
        )

        log = structlog.get_logger()

        with pytest.raises(ValueError, match=expected_error):
            _execute_email_callbacks(dagbag, request, log)

    def test_parse_file_passes_bundle_name_to_dagbag(self):
        """Test that _parse_file() creates DagBag with correct bundle_name parameter"""
        # Mock the DagBag constructor to capture its arguments
        with patch("airflow.dag_processing.processor.DagBag") as mock_dagbag_class:
            # Create a mock instance with proper attributes for Pydantic validation
            mock_dagbag_instance = MagicMock()
            mock_dagbag_instance.dags = {}
            mock_dagbag_instance.import_errors = {}  # Must be a dict, not MagicMock for Pydantic validation
            mock_dagbag_class.return_value = mock_dagbag_instance

            request = DagFileParseRequest(
                file="/test/dag.py",
                bundle_path=pathlib.Path("/test"),
                bundle_name="test_bundle",
                callback_requests=[],
            )

            _parse_file(request, log=structlog.get_logger())

            # Verify DagBag was called with correct bundle_name
            mock_dagbag_class.assert_called_once()
            call_kwargs = mock_dagbag_class.call_args.kwargs
            assert call_kwargs["bundle_name"] == "test_bundle"


class TestDagProcessingMessageTypes:
    def test_message_types_in_dag_processor(self):
        """
        Test that ToSupervisor is a superset of ToManager and ToTask is a superset of ToDagProcessor.

        This test ensures that when new message types are added to ToSupervisor or ToTask,
        they are also properly handled in ToManager and ToDagProcessor.
        """

        def get_type_names(union_type):
            union_args = typing.get_args(union_type.__args__[0])
            return {arg.__name__ for arg in union_args}

        supervisor_types = get_type_names(ToSupervisor)
        task_types = get_type_names(ToTask)

        manager_types = get_type_names(ToManager)
        dag_processor_types = get_type_names(ToDagProcessor)

        in_supervisor_but_not_in_manager = {
            "DeferTask",
            "DeleteXCom",
            "GetAssetByName",
            "GetAssetByUri",
            "GetAssetEventByAsset",
            "GetAssetEventByAssetAlias",
            "GetDagRun",
            "GetDagRunState",
            "GetDRCount",
            "GetTaskBreadcrumbs",
            "GetTaskRescheduleStartDate",
            "GetTICount",
            "GetTaskStates",
            "RescheduleTask",
            "RetryTask",
            "SetRenderedFields",
            "SetXCom",
            "SkipDownstreamTasks",
            "SucceedTask",
            "ValidateInletsAndOutlets",
            "TaskState",
            "TriggerDagRun",
            "ResendLoggingFD",
            "CreateHITLDetailPayload",
            "UpdateHITLDetail",
            "GetHITLDetailResponse",
            "SetRenderedMapIndex",
        }

        in_task_runner_but_not_in_dag_processing_process = {
            "AssetResult",
            "AssetEventsResult",
            "DagRunResult",
            "DagRunStateResult",
            "DRCount",
            "SentFDs",
            "StartupDetails",
            "TaskBreadcrumbsResult",
            "TaskRescheduleStartDate",
            "TICount",
            "TaskStatesResult",
            "InactiveAssetsResult",
            "CreateHITLDetailPayload",
            "HITLDetailRequestResult",
        }

        supervisor_diff = supervisor_types - manager_types - in_supervisor_but_not_in_manager
        task_diff = task_types - dag_processor_types - in_task_runner_but_not_in_dag_processing_process

        assert not supervisor_diff, (
            f"New message types in ToSupervisor not handled in ToManager: "
            f"{len(supervisor_diff)} types found:\n"
            + "\n".join(f"  - {t}" for t in sorted(supervisor_diff))
            + "\n\nEither handle these types in ToManager or update in_supervisor_but_not_in_manager list."
        )

        assert not task_diff, (
            f"New message types in ToTask not handled in ToDagProcessor: "
            f"{len(task_diff)} types found:\n"
            + "\n".join(f"  - {t}" for t in sorted(task_diff))
            + "\n\nEither handle these types in ToDagProcessor or update in_task_runner_but_not_in_dag_processing_process list."
        )
