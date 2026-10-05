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

from contextlib import ExitStack
from pathlib import Path
from unittest import mock

import pytest

from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.dag_processing.executor_manager import ExecutorDagFileProcessorManager
from airflow.dag_processing.manager import DagFileInfo, DagFileProcessorManager
from airflow.dag_processing.processor import DagFileParsingResult
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads import WorkloadType
from airflow.executors.workloads.parsing import ParseDagFileState

pytestmark = pytest.mark.db_test


@pytest.fixture
def manager(tmp_path):
    manager = ExecutorDagFileProcessorManager(max_runs=1)
    executor = mock.create_autospec(LocalExecutor, instance=True)
    executor.executor_queues = {WorkloadType.PARSE_DAG_FILE: {}}
    executor.running = set()
    executor.get_event_buffer.return_value = {}
    manager._executor = executor
    manager._control_dir = tmp_path
    manager._resources = ExitStack()
    manager.base_log_dir = str(tmp_path / "logs")
    yield manager
    manager.after_run()


@pytest.fixture
def file(tmp_path):
    return DagFileInfo(Path("dag.py"), "local", tmp_path, "v1")


def test_submission_keeps_manager_callback_and_bundle_context(manager, file):
    callback = DagCallbackRequest(
        filepath="dag.py", bundle_name="local", bundle_version="v1", dag_id="dag", run_id="run"
    )
    manager._callback_to_execute[file] = [callback]
    proc = manager._create_process(file)
    queued = manager._executor.queue_workload.call_args
    assert queued.args[0] == proc.workload
    assert queued.kwargs["session"].bind is not None
    assert proc.workload.callbacks == [callback.to_json()]
    assert proc.workload.bundle_info.version == "v1"
    assert proc.workload.bundle_path == file.bundle_path
    assert proc.had_callbacks
    assert file not in manager._callback_to_execute


@mock.patch.object(ExecutorDagFileProcessorManager, "persist_parsing_result", autospec=True)
def test_executor_result_uses_existing_manager_persistence(persist, manager, file):
    proc = manager._create_process(file)
    manager._processors[file] = proc
    manager._bundle_versions["local"] = "v1"
    parsed = DagFileParsingResult(fileloc=str(file.absolute_path), serialized_dags=[], import_errors={})
    proc.workload.result_path.write_text(parsed.model_dump_json())
    manager._executor.get_event_buffer.return_value = {proc.workload.key: (ParseDagFileState.SUCCESS, None)}
    manager._service_processor_sockets(timeout=0)
    manager._collect_results()
    persist.assert_called_once()
    assert persist.call_args.kwargs["parsing_result"] == parsed
    assert persist.call_args.kwargs["bundle_version"] == "v1"
    assert manager._file_stats[file].run_count == 1
    assert not manager._processors
    assert not proc.workload.result_path.exists()


@mock.patch.object(ExecutorDagFileProcessorManager, "persist_parsing_result", autospec=True)
def test_removed_file_cancels_delivery_and_ignores_late_event(persist, manager, file):
    proc = manager._create_process(file)
    manager._processors[file] = proc
    manager._executor.running.add(proc.workload.key)
    manager.terminate_orphan_processes(set())
    assert proc.workload.cancel_path.exists()
    assert manager._executor.running == {proc.workload.key}
    proc.workload.result_path.write_text("stale result")
    manager._executor.get_event_buffer.return_value = {proc.workload.key: (ParseDagFileState.SUCCESS, None)}
    manager._service_processor_sockets(timeout=0)
    manager._collect_results()
    persist.assert_not_called()
    assert not manager._processors
    assert not proc.workload.result_path.exists()
    assert not proc.workload.cancel_path.exists()


@mock.patch("airflow.dag_processing.executor_manager.time.sleep", autospec=True)
def test_polling_returns_on_first_completion_before_timeout(sleep, manager, file):
    proc = manager._create_process(file)
    manager._processors[file] = proc
    manager._executor.get_event_buffer.side_effect = [
        {},
        {},
        {proc.workload.key: (ParseDagFileState.FAILED, None)},
    ]
    manager._service_processor_sockets(timeout=60)
    assert proc.is_ready
    assert sleep.call_args_list == [mock.call(0.01), mock.call(0.01)]
    assert manager._executor.sync.call_count == 2


@pytest.mark.parametrize("state", [ParseDagFileState.SUCCESS, ParseDagFileState.FAILED])
def test_missing_result_does_not_block_other_files(manager, file, state):
    proc = manager._create_process(file)
    manager._processors[file] = proc
    manager._executor.get_event_buffer.return_value = {proc.workload.key: (state, None)}
    manager._service_processor_sockets(timeout=0)
    manager._collect_results()
    assert not manager._processors
    assert manager._file_stats[file].run_count == 1


@mock.patch.object(DagFileProcessorManager, "before_run", autospec=True)
@mock.patch("airflow.dag_processing.executor_manager.LocalExecutor", autospec=True)
def test_pool_capacity_and_cleanup_follow_manager_lifetime(executor_class, before_run):
    manager = ExecutorDagFileProcessorManager(max_runs=1, parallelism=3)
    manager.before_run()
    directory = manager._control_dir
    executor_class.assert_called_once_with(parallelism=3)
    assert executor_class.return_value.supported_workload_types == {WorkloadType.PARSE_DAG_FILE}
    manager.after_run()
    manager.after_run()
    executor_class.return_value.end.assert_called_once()
    assert not directory.exists()
