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

import pathlib
import sys
from socket import socketpair
from typing import TYPE_CHECKING
from unittest.mock import patch

import pytest
import structlog
from pydantic import TypeAdapter

from airflow.callbacks.callback_requests import CallbackRequest, DagCallbackRequest, TaskCallbackRequest
from airflow.configuration import conf
from airflow.dag_processing.processor import (
    DagFileParseRequest,
    DagFileParsingResult,
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
PY311 = sys.version_info >= (3, 11)

# Include the words "airflow" and "dag" in the file contents,
# tricking airflow into thinking these
# files contain a DAG (otherwise Airflow will skip them)
PARSEABLE_DAG_FILE_CONTENTS = '"airflow DAG"'

# Filename to be used for dags that are created in an ad-hoc manner and can be removed/
# created at runtime
TEMP_DAG_FILENAME = "temp_dag.py"
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
    ) -> DagFileParsingResult:
        return _parse_file(
            DagFileParseRequest(file=file_path, requests_fd=1, callback_requests=callback_requests or []),
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
                triggered_by=DagRunTriggeredByType.TEST,
                session=session,
            )
            task = dag.get_task(task_id="run_this_first")
            ti = TaskInstance(task, run_id=dagrun.run_id, state=TaskInstanceState.QUEUED)
            session.add(ti)

            if has_serialized_dag:
                assert SerializedDagModel.write_dag(dag, session=session) is True
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

        assert not resp.serialized_dags
        assert resp.import_errors is not None
        assert "a.py" in resp.import_errors


#     @conf_vars({("logging", "dag_processor_log_target"): "stdout"})
#     @mock.patch("airflow.dag_processing.processor.settings.dispose_orm", MagicMock)
#     @mock.patch("airflow.dag_processing.processor.redirect_stdout")
#     def test_dag_parser_output_when_logging_to_stdout(self, mock_redirect_stdout_for_file):
#         processor = DagFileProcessorProcess(
#             file_path="abc.txt",
#             dag_directory=[],
#             callback_requests=[],
#         )
#         processor._run_file_processor(
#             result_channel=MagicMock(),
#             parent_channel=MagicMock(),
#             file_path="fake_file_path",
#             thread_name="fake_thread_name",
#             callback_requests=[],
#             dag_directory=[],
#         )
#         mock_redirect_stdout_for_file.assert_not_called()
#
#     @conf_vars({("logging", "dag_processor_log_target"): "file"})
#     @mock.patch("airflow.dag_processing.processor.settings.dispose_orm", MagicMock)
#     @mock.patch("airflow.dag_processing.processor.redirect_stdout")
#     def test_dag_parser_output_when_logging_to_file(self, mock_redirect_stdout_for_file):
#         processor = DagFileProcessorProcess(
#             file_path="abc.txt",
#             dag_directory=[],
#             callback_requests=[],
#         )
#         processor._run_file_processor(
#             result_channel=MagicMock(),
#             parent_channel=MagicMock(),
#             file_path="fake_file_path",
#             thread_name="fake_thread_name",
#             callback_requests=[],
#             dag_directory=[],
#         )
#         mock_redirect_stdout_for_file.assert_called_once()


@pytest.fixture
def disable_capturing():
    old_in, old_out, old_err = sys.stdin, sys.stdout, sys.stderr

    sys.stdin = sys.__stdin__
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    yield
    sys.stdin, sys.stdout, sys.stderr = old_in, old_out, old_err


@pytest.mark.usefixtures("disable_capturing")
def test_parse_file_entrypoint_parses_dag_callbacks(spy_agency):
    r, w = socketpair()
    # Create a valid FD for the decoder to open
    _, w2 = socketpair()

    w.makefile("wb").write(
        b'{"file":"/files/dags/wait.py","requests_fd":'
        + str(w2.fileno()).encode("ascii")
        + b',"callback_requests": [{"full_filepath": "/files/dags/wait.py", '
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
            full_filepath="/files/dags/wait.py",
            msg="task_failure",
            dag_id="wait_to_fail",
            run_id="manual__2024-12-30T21:02:55.203691+00:00",
            is_failure_callback=True,
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
            full_filepath="A",
            msg="Message",
            dag_id="a",
            run_id="b",
        )
    ]
    _parse_file(
        DagFileParseRequest(file="A", requests_fd=1, callback_requests=requests), log=structlog.get_logger()
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
            full_filepath="A",
            msg="Message",
            ti=None,
        )
    ]
    _parse_file(
        DagFileParseRequest(file="A", requests_fd=1, callback_requests=requests), log=structlog.get_logger()
    )

    assert called is True
