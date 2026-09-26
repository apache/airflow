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

import subprocess
import sys
from datetime import datetime, timedelta, timezone
from unittest import mock
from uuid import uuid4

import pytest
from celery import Celery, Task, states
from celery.result import AsyncResult
from sqlalchemy.orm import Session

from airflow.providers.celery.executors import celery_executor_utils
from airflow.providers.celery.executors.celery_executor import CeleryExecutor
from airflow.providers.common.compat.sdk import AirflowTaskTimeout

try:
    from airflow.executors.workloads import BundleInfo, WorkloadType
    from airflow.executors.workloads.parsing import (
        DagDefinitionAttempt,
        ParseDagDefinitions,
        ParseDagDefinitionsState,
    )
except ImportError:
    pytest.skip("Requires the experimental parsing workload schema", allow_module_level=True)


@pytest.fixture
def workload():
    start = datetime(2026, 9, 25, tzinfo=timezone.utc)
    return ParseDagDefinitions(
        workload_id=uuid4(),
        bundle_info=BundleInfo(name="parsing-fixture", version="v1"),
        definitions=[
            DagDefinitionAttempt(
                attempt_id=uuid4(),
                relative_path="example.py",
                source_revision="source-v1",
                timeout_seconds=10,
            )
        ],
        start_deadline=start + timedelta(minutes=1),
        stop_deadline=start + timedelta(minutes=2),
        token="parsing-token",
        queue="parsing",
    )


@pytest.fixture
def celery_app():
    app = Celery("parsing-tests", broker="memory://", backend="cache+memory://", set_as_current=False)
    app.task(name="execute_workload")(celery_executor_utils.execute_workload.__wrapped__)
    celery_executor_utils._get_celery_app_for_workload.cache_clear()
    try:
        with mock.patch.object(celery_executor_utils, "create_celery_app", autospec=True, return_value=app):
            yield app
    finally:
        celery_executor_utils._get_celery_app_for_workload.cache_clear()
        app.close()


@pytest.fixture
def executor(celery_app):
    executor = CeleryExecutor(parallelism=1)
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    executor._sync_parallelism = 1
    return executor


def test_parsing_is_an_instance_opt_in(celery_app, workload):
    tasks = CeleryExecutor(parallelism=2)
    parsing = CeleryExecutor(parallelism=1)
    defaults = CeleryExecutor.supported_workload_types
    parsing.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})

    with pytest.raises(NotImplementedError, match="does not support ParseDagDefinitions"):
        tasks.queue_workload(workload, session=mock.Mock(spec=Session))
    with pytest.raises(ValueError, match="dedicated prototype executor"):
        tasks._process_workloads([workload])
    parsing.queue_workload(workload, session=mock.Mock(spec=Session))

    assert tasks.supported_workload_types == CeleryExecutor.supported_workload_types == defaults
    assert WorkloadType.PARSE_DAG_DEFINITIONS not in defaults
    assert tasks.slots_available == 2
    assert parsing.slots_available == 0


@mock.patch.object(CeleryExecutor, "_send_workloads", autospec=True)
@pytest.mark.parametrize("team_name", [None, "analytics"])
def test_parsing_uses_explicit_queue_and_preserves_team(send, executor, workload, team_name):
    executor.team_name = team_name
    executor._process_workloads([workload])
    send.assert_called_once_with(executor, [(workload.key, workload, "parsing", team_name)])


@mock.patch.object(CeleryExecutor, "_send_workloads", autospec=True)
@pytest.mark.parametrize("queue", [None, "", " "])
def test_parsing_never_falls_back_to_task_queue(send, executor, workload, queue):
    workload.queue = queue
    with pytest.raises(ValueError, match="explicit Celery queue"):
        executor._process_workloads([workload])
    send.assert_not_called()


@mock.patch.object(CeleryExecutor, "_send_workloads_to_celery", autospec=True)
@pytest.mark.parametrize(
    ("state", "terminal"),
    [
        (states.SUCCESS, ParseDagDefinitionsState.SUCCESS),
        (states.FAILURE, ParseDagDefinitionsState.FAILED),
        (states.REVOKED, ParseDagDefinitionsState.FAILED),
        (states.STARTED, None),
        (states.PENDING, None),
        (states.RETRY, None),
    ],
)
def test_parsing_publication_and_terminal_events_account_for_capacity(
    send, executor, celery_app, workload, state, terminal
):
    result = celery_app.AsyncResult(str(workload.workload_id))
    send.return_value = [(workload.key, (workload.model_dump_json(),), result)]
    executor.queue_workload(workload, session=mock.Mock(spec=Session))
    executor.trigger_workloads(1)

    assert not executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS]
    assert executor.running == {workload.key}
    assert executor.workloads == {workload.key: result}
    assert executor.slots_available == 0
    queued, identity = executor.get_event_buffer()[workload.key]
    assert queued is ParseDagDefinitionsState.QUEUED
    assert identity == str(workload.workload_id)

    executor.update_task_state(workload.key, state, "provider detail")

    if terminal is None:
        assert executor.running == {workload.key}
        assert executor.workloads == {workload.key: result}
        assert not executor.get_event_buffer()
        assert executor.slots_available == 0
    else:
        assert not executor.running
        assert not executor.workloads
        actual, info = executor.get_event_buffer()[workload.key]
        assert actual is terminal
        assert info == "provider detail"
        assert executor.slots_available == 1


@mock.patch.object(CeleryExecutor, "_send_workloads_to_celery", autospec=True)
def test_parsing_publish_timeout_retries_without_losing_queued_work(send, executor, celery_app, workload):
    failure = celery_executor_utils.ExceptionWithTraceback(AirflowTaskTimeout(), "publish timeout")
    result = celery_app.AsyncResult(str(workload.workload_id))
    send.side_effect = [[(workload.key, None, failure)], [(workload.key, None, result)]]
    executor.queue_workload(workload, session=mock.Mock(spec=Session))
    executor.trigger_workloads(1)

    assert executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS] == {workload.key: workload}
    assert executor.workload_publish_retries[workload.key] == 1
    assert not executor.running
    assert not executor.get_event_buffer()

    executor.trigger_workloads(1)

    assert not executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS]
    assert not executor.workload_publish_retries
    assert executor.running == {workload.key}
    assert send.call_args_list[0] == send.call_args_list[1]


@mock.patch.object(CeleryExecutor, "_send_workloads_to_celery", autospec=True)
@pytest.mark.parametrize("error", [ValueError("publish failed"), AirflowTaskTimeout()])
def test_parsing_publish_failure_is_terminal_after_retry_budget(send, executor, workload, error):
    executor.workload_publish_max_retries = 0
    failure = celery_executor_utils.ExceptionWithTraceback(error, "publish failed")
    send.return_value = [(workload.key, None, failure)]
    executor.queue_workload(workload, session=mock.Mock(spec=Session))
    executor.trigger_workloads(1)

    assert not executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS]
    assert not executor.workloads
    assert not executor.running
    assert not executor.workload_publish_retries
    assert executor.get_event_buffer()[workload.key][0] is ParseDagDefinitionsState.FAILED
    assert executor.slots_available == 1


@mock.patch.object(celery_executor_utils, "_get_celery_app_for_workload", autospec=True)
def test_parsing_publish_reuses_workload_id_without_changing_execution_identity(get_app, workload):
    task = mock.Mock(spec=Task)
    result = mock.Mock(spec=AsyncResult, task_id=str(workload.workload_id))
    task.apply_async.side_effect = [AirflowTaskTimeout(), result]
    app = mock.Mock(spec=Celery)
    app.tasks = {"execute_workload": task}
    get_app.return_value = app
    item = (workload.key, workload, "parsing", "analytics")

    first = celery_executor_utils.send_workload_to_executor(item)
    second = celery_executor_utils.send_workload_to_executor(item)

    assert isinstance(first[2], celery_executor_utils.ExceptionWithTraceback)
    assert second[2] is result
    expected = mock.call(
        args=(workload.model_dump_json(),),
        queue="parsing",
        task_id=str(workload.workload_id),
        argsrepr="(<redacted parsing workload>,)",
    )
    assert task.apply_async.call_args_list == [expected, expected]
    assert get_app.call_args_list == [mock.call("analytics"), mock.call("analytics")]
    assert "execution_id" not in workload.model_dump()


@mock.patch("airflow.executors.base_executor.BaseExecutor.run_workload", autospec=True)
@pytest.mark.parametrize(
    ("error_name", "expected_state"),
    [
        ("ParsingAttemptAlreadyClaimedError", states.IGNORED),
        ("ParsingClaimDispositionUnknownError", states.IGNORED),
        ("ParsingWorkerError", states.FAILURE),
        ("ParsingRequestTooLargeError", states.FAILURE),
    ],
)
def test_only_claim_conflicts_and_uncertainty_preserve_shared_backend_state(
    run_workload, celery_app, workload, error_name, expected_state
):
    from airflow.dag_processing import executor_worker

    run_workload.side_effect = getattr(executor_worker, error_name)("test error")
    task = celery_app.tasks["execute_workload"]
    task.store_eager_result = True
    celery_id = str(workload.workload_id)
    celery_app.backend.store_result(celery_id, {"worker": "original"}, states.STARTED)

    delivery = task.apply(args=(workload.model_dump_json(),), task_id=celery_id, throw=False)

    assert delivery.state == expected_state
    assert celery_app.backend.get_task_meta(celery_id, cache=False)["status"] == (
        states.STARTED if expected_state == states.IGNORED else states.FAILURE
    )


@pytest.mark.parametrize("modern_core", [False, True])
def test_provider_imports_without_experimental_core_module(modern_core):
    script = """
import builtins
import importlib
import sys
from unittest.mock import patch

from airflow.executors.base_executor import BaseExecutor
from airflow.providers.celery import version_compat

modern = sys.argv[1] == "True"
version_compat.AIRFLOW_V_3_4_PLUS = modern
real_import = builtins.__import__
attempts = []

def optional_schema_missing(name, *args, **kwargs):
    if name == "airflow.executors.workloads.parsing":
        attempts.append(name)
        if not modern:
            raise AssertionError("Older core must not import the experimental module")
        raise ModuleNotFoundError(name, name=name)
    return real_import(name, *args, **kwargs)

utils_name = "airflow.providers.celery.executors.celery_executor_utils"
executor_name = "airflow.providers.celery.executors.celery_executor"
sys.modules.pop(utils_name, None)
sys.modules.pop(executor_name, None)
with patch("builtins.__import__", side_effect=optional_schema_missing):
    utils = importlib.import_module(utils_name)
    importlib.import_module(executor_name)
assert len(attempts) == int(modern)
assert not utils._is_parsing_workload(object())
assert not utils._is_parsing_workload_key(object())
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(modern_core)],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
