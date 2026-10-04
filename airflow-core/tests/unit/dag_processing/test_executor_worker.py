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

from unittest import mock
from uuid import uuid4

import httpx
import pytest

from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.dag_processing.executor_worker import supervise_dag_parse
from airflow.dag_processing.processor import DagFileParsingResult, DagFileProcessorProcess
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.workloads import BundleInfo, ParseDagFile


@pytest.fixture
def workload(tmp_path):
    return ParseDagFile(
        workload_id=uuid4(),
        bundle_info=BundleInfo(name="local"),
        bundle_path=tmp_path,
        relative_path="dag.py",
        log_path=str(tmp_path / "parser.log"),
        control_dir=tmp_path,
        timeout=10,
    )


@pytest.fixture
def parser():
    with (
        mock.patch("airflow.dag_processing.executor_worker._get_execution_api", autospec=True) as api,
        mock.patch.object(DagFileProcessorProcess, "start", autospec=True) as start,
    ):
        api.return_value.transport = httpx.MockTransport(lambda request: httpx.Response(500))
        proc = mock.create_autospec(DagFileProcessorProcess, instance=True)
        proc.is_ready = True
        proc._check_subprocess_exit.return_value = 0
        proc.parsing_result = DagFileParsingResult(fileloc="dag.py", serialized_dags=[], import_errors={})
        start.return_value = proc
        yield start, proc


@pytest.mark.parametrize("callbacks", [False, True])
def test_dispatch_reuses_file_processor_and_returns_serialized_data(workload, parser, callbacks):
    start, proc = parser
    if callbacks:
        request = DagCallbackRequest(
            filepath="dag.py", bundle_name="local", bundle_version=None, dag_id="dag", run_id="run"
        )
        workload.callbacks = [request.to_json()]
        proc.parsing_result = None
    assert BaseExecutor.run_workload(workload) == 0
    assert workload.result_path.read_text() == (
        "null" if callbacks else proc.parsing_result.model_dump_json()
    )
    assert start.call_args.kwargs["path"] == workload.bundle_path / workload.relative_path
    assert start.call_args.kwargs["callbacks"] == ([request] if callbacks else [])
    assert start.call_args.kwargs["new_process_group"] is True
    proc.close.assert_called_once()


def test_cancelled_delivery_never_starts_parser(workload, parser):
    workload.cancel_path.touch()
    assert supervise_dag_parse(workload) == 0
    parser[0].assert_not_called()
    assert not workload.result_path.exists()


@mock.patch("airflow.dag_processing.executor_worker.time.monotonic", autospec=True, side_effect=[0, 11])
@pytest.mark.parametrize("reason", ["timeout", "cancel", "unconfirmed_exit"])
def test_interrupt_stops_importer_without_publishing(monotonic, workload, parser, reason):
    start, proc = parser
    proc.is_ready = False
    proc._check_subprocess_exit.side_effect = [None, None if reason == "unconfirmed_exit" else 0]
    if reason == "cancel":

        def cancel(**kwargs):
            workload.cancel_path.touch()
            return proc

        start.side_effect = cancel
        assert supervise_dag_parse(workload) == 0
    else:
        error = RuntimeError if reason == "unconfirmed_exit" else TimeoutError
        with pytest.raises(error):
            supervise_dag_parse(workload)
    proc.kill.assert_called_once()
    proc.close.assert_called_once()
    assert not workload.result_path.exists()


def test_missing_result_is_an_executor_failure(workload, parser):
    parser[1].parsing_result = None
    with pytest.raises(RuntimeError, match="without a serialized result"):
        supervise_dag_parse(workload)
    parser[1].close.assert_called_once()
    assert not workload.result_path.exists()
