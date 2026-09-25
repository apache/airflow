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

import json
import multiprocessing
import os
import pickle
import signal
import time
from contextlib import suppress
from datetime import datetime, timedelta
from multiprocessing import Value
from pathlib import Path
from queue import SimpleQueue
from unittest import mock
from uuid import uuid4

import pytest
from pydantic import TypeAdapter, ValidationError
from sqlalchemy.orm import Session

from airflow.executors import workloads
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads import WorkloadType
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.parsing import (
    DagDefinitionAttempt,
    DagDefinitionResult,
    ParseDagDefinitions,
    ParseDagDefinitionsState,
)
from airflow.executors.workloads.types import state_class_for_key


@pytest.fixture
def parsing_workload():
    return ParseDagDefinitions(
        workload_id=uuid4(),
        bundle_info=BundleInfo(name="parsing-fixture", version="v1"),
        definitions=(
            DagDefinitionAttempt(
                attempt_id=uuid4(),
                relative_path="nested/example.py",
                source_revision="sha256:fixture",
                timeout_seconds=10,
            ),
        ),
        start_deadline=datetime.fromisoformat("2026-09-25T10:00:00+00:00"),
        stop_deadline=datetime.fromisoformat("2026-09-25T10:01:00+00:00"),
        token="private-parsing-token",
        queue="parsing",
    )


@pytest.mark.parametrize("alias", [workloads.ExecutorWorkload, workloads.All])
def test_parsing_transport_round_trip(alias, parsing_workload):
    received = TypeAdapter(alias).validate_json(parsing_workload.model_dump_json())

    assert received == parsing_workload
    assert received.key == parsing_workload.key
    assert state_class_for_key(received.key) is ParseDagDefinitionsState
    assert received.token not in repr(received)
    assert received.token_scope == "dag-parsing-poc"


@pytest.mark.parametrize(
    "path", ["/tmp/dag.py", "../dag.py", "a/../../dag.py", "C:/dag.py", "a\\b.py", ".", "\x00.py"]
)
def test_definition_rejects_paths_outside_bundle(parsing_workload, path):
    values = parsing_workload.definitions[0].model_dump() | {"relative_path": path}
    with pytest.raises(ValidationError, match="relative POSIX paths"):
        DagDefinitionAttempt.model_validate(values)


@pytest.mark.parametrize("count", [0, 101])
def test_parsing_batch_is_bounded(parsing_workload, count):
    values = parsing_workload.model_dump()
    values["definitions"] = [
        parsing_workload.definitions[0].model_dump() | {"attempt_id": uuid4()} for _ in range(count)
    ]
    with pytest.raises(ValidationError, match="definitions"):
        ParseDagDefinitions.model_validate(values)


def test_parsing_batch_rejects_repeated_attempt_ids(parsing_workload):
    values = parsing_workload.model_dump()
    values["definitions"] *= 2
    with pytest.raises(ValidationError, match="attempt IDs must be unique"):
        ParseDagDefinitions.model_validate(values)


@pytest.mark.parametrize("stop_delta", [timedelta(), timedelta(seconds=-1)])
def test_parsing_batch_rejects_invalid_deadline_order(parsing_workload, stop_delta):
    values = parsing_workload.model_dump() | {
        "stop_deadline": parsing_workload.start_deadline + stop_delta,
    }
    with pytest.raises(ValidationError, match="stop_deadline must follow start_deadline"):
        ParseDagDefinitions.model_validate(values)


@pytest.mark.parametrize("field", ["start_deadline", "stop_deadline"])
def test_parsing_deadlines_require_timezone(parsing_workload, field):
    values = parsing_workload.model_dump()
    values[field] = values[field].replace(tzinfo=None)
    with pytest.raises(ValidationError, match="timezone"):
        ParseDagDefinitions.model_validate(values)


def test_results_reject_live_objects(parsing_workload):
    attempt = parsing_workload.definitions[0]
    with pytest.raises(ValidationError, match="serialized_dags"):
        DagDefinitionResult(
            attempt_id=attempt.attempt_id,
            relative_path=attempt.relative_path,
            source_revision=attempt.source_revision,
            outcome="success",
            serialized_dags=[{"dag": object()}],
            duration_seconds=1,
        )


@pytest.mark.parametrize("value", [0, -1, float("inf"), float("nan")])
def test_definition_timeout_is_positive_and_finite(parsing_workload, value):
    values = parsing_workload.definitions[0].model_dump() | {"timeout_seconds": value}
    with pytest.raises(ValidationError, match="timeout_seconds"):
        DagDefinitionAttempt.model_validate(values)


def test_definition_rejects_arbitrary_adapter_import(parsing_workload):
    values = parsing_workload.definitions[0].model_dump() | {"adapter": "user.module.load"}
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        DagDefinitionAttempt.model_validate(values)


def test_dedicated_parsing_capacity_does_not_change_task_executor(parsing_workload):
    task_executor = LocalExecutor(parallelism=3)
    parsing_executor = LocalExecutor(parallelism=1)
    original_types = task_executor.supported_workload_types
    parsing_executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})

    with pytest.raises(NotImplementedError, match="does not support ParseDagDefinitions"):
        task_executor.queue_workload(parsing_workload, session=mock.Mock(spec=Session))
    parsing_executor.queue_workload(parsing_workload, session=mock.Mock(spec=Session))

    assert parsing_executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS] == {
        parsing_workload.key: parsing_workload
    }
    assert not task_executor.executor_queues
    assert task_executor.parallelism == 3
    assert parsing_executor.parallelism == 1
    assert task_executor.supported_workload_types == original_types == LocalExecutor.supported_workload_types


@mock.patch("airflow.dag_processing.executor_worker.supervise_dag_parse", autospec=True)
def test_run_workload_dispatches_parsing(mock_supervise, parsing_workload):
    mock_supervise.return_value = 0
    server = "http://api.example/execution/"

    assert BaseExecutor.run_workload(parsing_workload, server=server) == 0

    mock_supervise.assert_called_once_with(parsing_workload, server=server)


@mock.patch("airflow.dag_processing.executor_worker.supervise_dag_parse", autospec=True)
def test_run_workload_rejects_failed_supervisor_exit(mock_supervise, parsing_workload):
    mock_supervise.return_value = 17
    with pytest.raises(RuntimeError, match="exited with 17"):
        BaseExecutor.run_workload(parsing_workload, server="http://api.example/execution/")


@mock.patch.object(LocalExecutor, "_check_workers", autospec=True)
@pytest.mark.parametrize(
    "terminal_state", [ParseDagDefinitionsState.SUCCESS, ParseDagDefinitionsState.FAILED]
)
def test_local_parsing_reservation_lasts_until_terminal_event(
    mock_check_workers, parsing_workload, terminal_state
):
    executor = LocalExecutor(parallelism=1)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    executor.activity_queue = SimpleQueue()
    executor.result_queue = SimpleQueue()
    executor._unread_messages = Value("i", 0)
    executor.queue_workload(parsing_workload, session=mock.Mock(spec=Session))

    executor.trigger_workloads(1)

    assert executor.activity_queue.get() == parsing_workload
    assert executor.running == {parsing_workload.key}
    assert not executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS]
    executor.result_queue.put((parsing_workload.key, ParseDagDefinitionsState.RUNNING, None))
    executor._read_results()
    assert executor.running == {parsing_workload.key}
    executor.result_queue.put((parsing_workload.key, terminal_state, None))
    executor._read_results()
    assert not executor.running
    assert executor.get_event_buffer() == {parsing_workload.key: (terminal_state, None)}


@mock.patch.object(LocalExecutor, "_check_workers", autospec=True)
@pytest.mark.parametrize("put_fails", [False, True])
def test_local_dispatch_counts_before_delivery_and_rolls_back_failed_put(
    mock_check_workers, parsing_workload, put_fails
):
    executor = LocalExecutor(parallelism=1)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    executor._unread_messages = Value("i", 0)
    executor.activity_queue = mock.Mock(spec=SimpleQueue)
    executor.queue_workload(parsing_workload, session=mock.Mock(spec=Session))

    def put(workload):
        assert executor._unread_messages.value == 1
        mock_check_workers.assert_called_once_with(executor, pending_workloads=1)
        if put_fails:
            raise OSError("Queue write failed")
        with executor._unread_messages:
            executor._unread_messages.value -= 1

    executor.activity_queue.put.side_effect = put
    if put_fails:
        with pytest.raises(OSError, match="Queue write failed"):
            executor.trigger_workloads(1)
        assert executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS] == {
            parsing_workload.key: parsing_workload
        }
        assert not executor.running
    else:
        executor.trigger_workloads(1)
        assert not executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS]
        assert executor.running == {parsing_workload.key}
    assert executor._unread_messages.value == 0


def _dispatch_large_parsing_batch(start_method, workload_json, status_path):
    os.setsid()
    status = Path(status_path)
    status.write_text("started")
    multiprocessing.set_start_method(start_method, force=True)
    os.environ["AIRFLOW__CORE__EXECUTION_API_SERVER_URL"] = "invalid://parsing-test"
    workload = ParseDagDefinitions.model_validate_json(workload_json)
    executor = LocalExecutor(parallelism=1)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    executor.start()
    completed = False
    try:
        assert not executor.workers
        executor.queue_workload(workload, session=None)
        executor.heartbeat()
        deadline = time.monotonic() + 15
        events = executor.get_event_buffer()
        while events.get(workload.key, (None,))[0] != ParseDagDefinitionsState.FAILED:
            if time.monotonic() >= deadline:
                raise TimeoutError("The parsing worker did not return its terminal event")
            executor.sync()
            events.update(executor.get_event_buffer())
            time.sleep(0.01)
        assert "explicit HTTP Execution API URL" in str(events[workload.key][1])
        assert not executor.running
        assert executor._unread_messages.value == 0
        completed = True
    finally:
        if not completed:
            executor.terminate()
        executor.end()
    status.write_text(json.dumps({"phase": "closed", "workers": len(executor.workers)}))


@pytest.mark.parametrize("start_method", ["spawn", "forkserver"])
def test_large_parsing_batch_dispatches_and_closes_with_non_fork_workers(
    start_method, parsing_workload, tmp_path
):
    if start_method not in multiprocessing.get_all_start_methods():
        pytest.skip(f"{start_method} is unavailable on this platform")
    values = parsing_workload.model_dump()
    values["definitions"] = [
        parsing_workload.definitions[0].model_dump()
        | {
            "attempt_id": uuid4(),
            "relative_path": ("segment" * 14 + "/") * 8 + f"definition_{index}.py",
        }
        for index in range(100)
    ]
    workload = ParseDagDefinitions.model_validate(values)
    assert len(pickle.dumps(workload)) > 64 * 1024
    status = tmp_path / "dispatch-status.json"
    process = multiprocessing.get_context("spawn").Process(
        target=_dispatch_large_parsing_batch,
        args=(start_method, workload.model_dump_json(), str(status)),
    )
    process.start()
    try:
        process.join(timeout=40)
        assert process.exitcode == 0, "Dispatch or shutdown did not finish successfully"
        assert json.loads(status.read_text()) == {"phase": "closed", "workers": 1}
    finally:
        # The child owns a process group so a failed regression also cleans up any spawned workers.
        if status.exists():
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        if process.is_alive():
            process.kill()
        process.join(timeout=5)
        process.close()
