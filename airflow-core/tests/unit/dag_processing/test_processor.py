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
import uuid
from collections.abc import Callable
from socket import socketpair
from typing import TYPE_CHECKING, BinaryIO
from unittest.mock import MagicMock, patch

import pytest
import structlog
from pydantic import TypeAdapter
from structlog.typing import FilteringBoundLogger

from airflow.api_fastapi.execution_api.app import InProcessExecutionAPI
from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
    TaskInstance as TIDataModel,
    TIRunContext,
)
from airflow.callbacks.callback_requests import CallbackRequest, DagCallbackRequest, TaskCallbackRequest
from airflow.dag_processing.processor import (
    DagFileParseRequest,
    DagFileParsingResult,
    DagFileProcessorProcess,
    _execute_task_callbacks,
    _parse_file,
    _pre_import_airflow_modules,
)
from airflow.models import DagBag, DagRun
from airflow.models.baseoperator import BaseOperator
from airflow.sdk import DAG
from airflow.sdk.api.client import Client
from airflow.sdk.execution_time import comms
from airflow.utils import timezone
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

            all_vars = session.query(VariableORM).all()
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

            all_vars = session.query(VariableORM).all()
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

    def test__pre_import_airflow_modules_warns_on_missing_module(self):
        logger = MagicMock(spec=FilteringBoundLogger)
        with (
            env_vars({"AIRFLOW__DAG_PROCESSOR__PARSING_PRE_IMPORT_MODULES": "true"}),
            patch(
                "airflow.dag_processing.processor.iter_airflow_imports", return_value=["non_existent_module"]
            ),
            patch(
                "airflow.dag_processing.processor.importlib.import_module", side_effect=ModuleNotFoundError()
            ),
        ):
            _pre_import_airflow_modules("test.py", logger)

        logger.warning.assert_called_once()
        warning_args = logger.warning.call_args[0]
        assert "Error when trying to pre-import module" in warning_args[0]
        assert "non_existent_module" in warning_args[1]
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
        DagFileParseRequest(file="A", bundle_path="no matter", callback_requests=requests),
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
        DagFileParseRequest(file="A", bundle_path="test", callback_requests=requests),
        log=structlog.get_logger(),
    )

    assert called is True


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
