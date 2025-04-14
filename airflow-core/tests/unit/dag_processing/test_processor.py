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
from socket import socketpair
from typing import TYPE_CHECKING, Callable
from unittest.mock import MagicMock, patch

import pytest
import structlog
from pydantic import TypeAdapter

from airflow.callbacks.callback_requests import CallbackRequest, DagCallbackRequest, TaskCallbackRequest
from airflow.configuration import conf
from airflow.dag_processing.processor import (
    DagFileParseRequest,
    DagFileParsingResult,
    DagFileProcessorProcess,
    _parse_file,
)
from airflow.models import DagBag, TaskInstance
from airflow.models.baseoperator import BaseOperator
from airflow.models.serialized_dag import SerializedDagModel
from airflow.sdk.execution_time.task_runner import CommsDecoder
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

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


@pytest.mark.usefixtures("disable_load_example")
class TestDagFileProcessor:
    def _process_file(
        self, file_path, callback_requests: list[CallbackRequest] | None = None
    ) -> DagFileParsingResult | None:
        return _parse_file(
            DagFileParseRequest(
                file=file_path,
                bundle_path=TEST_DAG_FOLDER,
                requests_fd=1,
                callback_requests=callback_requests or [],
            ),
            log=structlog.get_logger(),
        )

    @pytest.mark.xfail(reason="TODO: AIP-72")
    @pytest.mark.parametrize(
        ["has_serialized_dag"],
        [pytest.param(True, id="dag_in_db"), pytest.param(False, id="no_dag_found")],
    )
    @patch.object(TaskInstance, "handle_failure")
    def test_execute_on_failure_callbacks_without_dag(self, mock_ti_handle_failure, has_serialized_dag):
        dagbag = DagBag(dag_folder="/dev/null", include_examples=True, read_dags_from_db=False)
        with create_session() as session:
            session.query(TaskInstance).delete()
            dag = dagbag.get_dag("example_branch_operator")
            assert dag is not None
            dag.sync_to_db()
            dagrun = dag.create_dagrun(
                state=DagRunState.RUNNING,
                logical_date=DEFAULT_DATE,
                run_type=DagRunType.SCHEDULED,
                data_interval=dag.infer_automated_data_interval(DEFAULT_DATE),
                run_after=DEFAULT_DATE,
                triggered_by=DagRunTriggeredByType.TEST,
                session=session,
            )
            task = dag.get_task(task_id="run_this_first")
            ti = TaskInstance(task, run_id=dagrun.run_id, state=TaskInstanceState.QUEUED)
            session.add(ti)

            if has_serialized_dag:
                assert SerializedDagModel.write_dag(dag, bundle_name="testing", session=session) is True
                session.flush()

        requests = [TaskCallbackRequest(full_filepath="A", ti=ti, msg="Message")]
        self._process_file(dag.fileloc, requests)
        mock_ti_handle_failure.assert_called_once_with(
            error="Message", test_mode=conf.getboolean("core", "unit_test_mode"), session=session
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

    # @pytest.mark.execution_timeout(10)
    def test_top_level_variable_access(
        self, spy_agency: SpyAgency, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ):
        logger_filehandle = MagicMock()

        def dag_in_a_fn():
            from airflow.sdk import DAG, Variable

            with DAG(f"test_{Variable.get('myvar')}"):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)

        monkeypatch.setenv("AIRFLOW_VAR_MYVAR", "abc")
        proc = DagFileProcessorProcess.start(
            id=1, path=path, bundle_path=tmp_path, callbacks=[], logger_filehandle=logger_filehandle
        )

        while not proc.is_ready:
            proc._service_subprocess(0.1)

        result = proc.parsing_result
        assert result is not None
        assert result.import_errors == {}
        assert result.serialized_dags[0].dag_id == "test_abc"

    def test_top_level_variable_access_not_found(
        self, spy_agency: SpyAgency, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ):
        logger_filehandle = MagicMock()

        def dag_in_a_fn():
            from airflow.sdk import DAG, Variable

            with DAG(f"test_{Variable.get('myvar')}"):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)
        proc = DagFileProcessorProcess.start(
            id=1, path=path, bundle_path=tmp_path, callbacks=[], logger_filehandle=logger_filehandle
        )

        while not proc.is_ready:
            proc._service_subprocess(0.1)

        result = proc.parsing_result
        assert result is not None
        assert result.import_errors != {}
        if result.import_errors:
            assert "VARIABLE_NOT_FOUND" in next(iter(result.import_errors.values()))

    def test_top_level_variable_set(self, tmp_path: pathlib.Path):
        from airflow.models.variable import Variable as VariableORM

        logger_filehandle = MagicMock()

        def dag_in_a_fn():
            from airflow.sdk import DAG, Variable

            Variable.set(key="mykey", value="myvalue")
            with DAG(f"test_{Variable.get('mykey')}"):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)
        proc = DagFileProcessorProcess.start(
            id=1, path=path, bundle_path=tmp_path, callbacks=[], logger_filehandle=logger_filehandle
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

    def test_top_level_variable_delete(self, tmp_path: pathlib.Path):
        from airflow.models.variable import Variable as VariableORM

        logger_filehandle = MagicMock()

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
            id=1, path=path, bundle_path=tmp_path, callbacks=[], logger_filehandle=logger_filehandle
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

    def test_top_level_connection_access(self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch):
        logger_filehandle = MagicMock()

        def dag_in_a_fn():
            from airflow.hooks.base import BaseHook
            from airflow.sdk import DAG

            with DAG(f"test_{BaseHook.get_connection(conn_id='my_conn').conn_id}"):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)

        monkeypatch.setenv("AIRFLOW_CONN_MY_CONN", '{"conn_type": "aws"}')
        proc = DagFileProcessorProcess.start(
            id=1, path=path, bundle_path=tmp_path, callbacks=[], logger_filehandle=logger_filehandle
        )

        while not proc.is_ready:
            proc._service_subprocess(0.1)

        result = proc.parsing_result
        assert result is not None
        assert result.import_errors == {}
        assert result.serialized_dags[0].dag_id == "test_my_conn"

    def test_top_level_connection_access_not_found(
        self, tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
    ):
        logger_filehandle = MagicMock()

        def dag_in_a_fn():
            from airflow.hooks.base import BaseHook
            from airflow.sdk import DAG

            with DAG(f"test_{BaseHook.get_connection(conn_id='my_conn').conn_id}"):
                ...

        path = write_dag_in_a_fn_to_file(dag_in_a_fn, tmp_path)
        proc = DagFileProcessorProcess.start(
            id=1, path=path, bundle_path=tmp_path, callbacks=[], logger_filehandle=logger_filehandle
        )

        while not proc.is_ready:
            proc._service_subprocess(0.1)

        result = proc.parsing_result
        assert result is not None
        assert result.import_errors != {}
        if result.import_errors:
            assert "CONNECTION_NOT_FOUND" in next(iter(result.import_errors.values()))


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
def test_parse_file_entrypoint_parses_dag_callbacks(spy_agency):
    r, w = socketpair()
    # Create a valid FD for the decoder to open
    _, w2 = socketpair()

    w.makefile("wb").write(
        b'{"file":"/files/dags/wait.py","bundle_path":"/files/dags","requests_fd":'
        + str(w2.fileno()).encode("ascii")
        + b',"callback_requests": [{"filepath": "wait.py", "bundle_name": "testing", "bundle_version": null, '
        b'"msg": "task_failure", "dag_id": "wait_to_fail", "run_id": '
        b'"manual__2024-12-30T21:02:55.203691+00:00", '
        b'"is_failure_callback": true, "type": "DagCallbackRequest"}], "type": "DagFileParseRequest"}\n'
    )

    decoder = CommsDecoder[DagFileParseRequest, DagFileParsingResult](
        input=r.makefile("r"),
        decoder=TypeAdapter[DagFileParseRequest](DagFileParseRequest),
    )

    msg = decoder.get_message()
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
        DagFileParseRequest(file="A", bundle_path="no matter", requests_fd=1, callback_requests=requests),
        log=structlog.get_logger(),
    )

    assert called is True


@pytest.mark.xfail(reason="TODO: AIP-72: Task level callbacks not yet supported")
def test_parse_file_with_task_callbacks(spy_agency):
    from airflow import DAG

    called = False

    def on_failure(context):
        nonlocal called
        called = True

    with DAG(dag_id="a", on_failure_callback=on_failure) as dag:
        BaseOperator(task_id="b", on_failure_callback=on_failure)

    def fake_collect_dags(self, *args, **kwargs):
        self.dags[dag.dag_id] = dag

    spy_agency.spy_on(DagBag.collect_dags, call_fake=fake_collect_dags, owner=DagBag)

    requests = [
        TaskCallbackRequest(
            filepath="A",
            msg="Message",
            ti=None,
            bundle_name="testing",
            bundle_version=None,
        )
    ]
    _parse_file(
        DagFileParseRequest(file="A", requests_fd=1, callback_requests=requests), log=structlog.get_logger()
    )

    assert called is True
