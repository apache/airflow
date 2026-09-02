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

import json
from collections.abc import Iterator
from datetime import datetime
from typing import Any
from unittest import mock
from uuid import UUID

import pytest
from google.api_core.exceptions import AlreadyExists, NotFound
from google.cloud import batch_v1

from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.google.cloud.operators.cloud_batch import (
    CloudBatchDeleteJobOperator,
    CloudBatchListJobsOperator,
    CloudBatchListTasksOperator,
    CloudBatchSubmitJobOperator,
)

from tests_common.test_utils.compat import DagSerialization
from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

if AIRFLOW_V_3_3_PLUS:
    from airflow.sdk.execution_time.comms import (
        ErrorResponse,
        ErrorType,
        GetTaskStateStore,
        OKResponse,
        SetTaskStateStore,
        TaskStateStoreResult,
    )
    from airflow.sdk.execution_time.context import TaskStateStoreAccessor
    from airflow.sdk.state import TaskScope

CLOUD_BATCH_HOOK_PATH = "airflow.providers.google.cloud.operators.cloud_batch.CloudBatchHook"
TASK_ID = "test"
PROJECT_ID = "testproject"
REGION = "us-central1"
JOB_NAME = "test"
JOB = batch_v1.Job()
JOB.name = JOB_NAME
FULL_JOB_NAME = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/{JOB_NAME}"
OPERATOR_ARGS: dict[str, Any] = {
    "task_id": TASK_ID,
    "project_id": PROJECT_ID,
    "region": REGION,
    "job_name": JOB_NAME,
    "job": JOB,
}


def _job_with_state(state: batch_v1.JobStatus.State | int) -> batch_v1.Job:
    return batch_v1.Job(name=FULL_JOB_NAME, status=batch_v1.JobStatus(state=state))


@pytest.fixture
def task_state_store() -> Iterator[tuple[TaskStateStoreAccessor, dict[str, Any], mock.Mock]]:
    if not AIRFLOW_V_3_3_PLUS:
        pytest.skip("Task state store recovery requires Airflow 3.3+")

    stored: dict[str, Any] = {}
    supervisor_comms = mock.Mock(spec=["send"])

    def send(message: object) -> object:
        if isinstance(message, GetTaskStateStore):
            if message.key in stored:
                return TaskStateStoreResult(value=stored[message.key])
            return ErrorResponse(error=ErrorType.TASK_STORE_NOT_FOUND, detail={"key": message.key})
        if isinstance(message, SetTaskStateStore):
            stored[message.key] = message.value
            return OKResponse(ok=True)
        raise AssertionError(f"Unexpected task state store message: {message!r}")

    supervisor_comms.send.side_effect = send
    state_store = TaskStateStoreAccessor(
        ti_id=UUID("01900000-0000-0000-0000-000000000001"),
        scope=TaskScope(dag_id="dag", run_id="run", task_id=TASK_ID),
    )
    with mock.patch("airflow.sdk.execution_time.task_runner.SUPERVISOR_COMMS", supervisor_comms, create=True):
        yield state_store, stored, supervisor_comms


def _context(task_state_store: TaskStateStoreAccessor) -> dict[str, Any]:
    return {"task_state_store": task_state_store}


class TestCloudBatchSubmitJobOperator:
    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute(self, mock):
        mock.return_value.wait_for_job.return_value = JOB
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            job=JOB,
        )

        completed_job = operator.execute(context={})

        assert completed_job["name"] == JOB_NAME

        mock.return_value.submit_batch_job.assert_called_with(
            job_name=JOB_NAME, job=JOB, region=REGION, project_id=PROJECT_ID
        )
        mock.return_value.wait_for_job.assert_called()

    @mock.patch(CLOUD_BATCH_HOOK_PATH, autospec=True)
    def test_fresh_run_persists_exact_job_name_before_waiting(self, mock_hook, task_state_store):
        store, stored, _ = task_state_store
        submitted_job = _job_with_state(batch_v1.JobStatus.State.RUNNING)
        completed_job = _job_with_state(batch_v1.JobStatus.State.SUCCEEDED)
        mock_hook.return_value.submit_batch_job.return_value = submitted_job

        def wait_for_job(**_: Any) -> batch_v1.Job:
            assert stored["cloud_batch_job_name"] == FULL_JOB_NAME
            return completed_job

        mock_hook.return_value.wait_for_job.side_effect = wait_for_job
        operator = CloudBatchSubmitJobOperator(**OPERATOR_ARGS)

        result = operator.execute(context=_context(store))

        assert result["name"] == FULL_JOB_NAME
        mock_hook.return_value.submit_batch_job.assert_called_once_with(
            job_name=JOB_NAME, job=JOB, region=REGION, project_id=PROJECT_ID
        )
        mock_hook.return_value.wait_for_job.assert_called_once_with(
            job_name=FULL_JOB_NAME,
            polling_period_seconds=10,
            timeout=None,
        )

    @mock.patch(CLOUD_BATCH_HOOK_PATH, autospec=True)
    def test_retry_reconnects_to_running_job(self, mock_hook, task_state_store):
        store, _, supervisor_comms = task_state_store
        store.set(key="cloud_batch_job_name", value=FULL_JOB_NAME)
        supervisor_comms.reset_mock()
        mock_hook.return_value.submit_batch_job.side_effect = AlreadyExists(
            f"Job {FULL_JOB_NAME} already exists"
        )
        mock_hook.return_value.get_job.return_value = _job_with_state(batch_v1.JobStatus.State.RUNNING)
        mock_hook.return_value.wait_for_job.return_value = _job_with_state(batch_v1.JobStatus.State.SUCCEEDED)
        operator = CloudBatchSubmitJobOperator(**OPERATOR_ARGS)

        result = operator.execute(context=_context(store))

        assert result["name"] == FULL_JOB_NAME
        mock_hook.return_value.submit_batch_job.assert_not_called()
        mock_hook.return_value.get_job.assert_called_once_with(job_name=FULL_JOB_NAME)
        mock_hook.return_value.wait_for_job.assert_called_once_with(
            job_name=FULL_JOB_NAME,
            polling_period_seconds=10,
            timeout=None,
        )

    @mock.patch(CLOUD_BATCH_HOOK_PATH, autospec=True)
    def test_retry_restores_succeeded_job(self, mock_hook, task_state_store):
        store, _, _ = task_state_store
        store.set(key="cloud_batch_job_name", value=FULL_JOB_NAME)
        mock_hook.return_value.get_job.return_value = _job_with_state(batch_v1.JobStatus.State.SUCCEEDED)
        operator = CloudBatchSubmitJobOperator(**OPERATOR_ARGS)

        result = operator.execute(context=_context(store))

        assert result["name"] == FULL_JOB_NAME
        mock_hook.return_value.submit_batch_job.assert_not_called()
        mock_hook.return_value.wait_for_job.assert_not_called()

    @pytest.mark.parametrize(
        ("state", "message"),
        [
            (batch_v1.JobStatus.State.FAILED, "has failed its execution"),
            (batch_v1.JobStatus.State.DELETION_IN_PROGRESS, "is being deleted"),
            (7, "is being cancelled"),
            (8, "was cancelled"),
        ],
    )
    @mock.patch(CLOUD_BATCH_HOOK_PATH, autospec=True)
    def test_retry_does_not_replace_terminal_job(self, mock_hook, state, message, task_state_store):
        store, _, _ = task_state_store
        store.set(key="cloud_batch_job_name", value=FULL_JOB_NAME)
        mock_hook.return_value.get_job.return_value = _job_with_state(state)
        operator = CloudBatchSubmitJobOperator(**OPERATOR_ARGS)

        with pytest.raises(RuntimeError, match=message):
            operator.execute(context=_context(store))

        mock_hook.return_value.submit_batch_job.assert_not_called()
        mock_hook.return_value.wait_for_job.assert_not_called()

    @mock.patch(CLOUD_BATCH_HOOK_PATH, autospec=True)
    def test_retry_replaces_missing_job(self, mock_hook, task_state_store):
        store, stored, _ = task_state_store
        store.set(key="cloud_batch_job_name", value=FULL_JOB_NAME)
        mock_hook.return_value.get_job.side_effect = NotFound("job was deleted")
        mock_hook.return_value.submit_batch_job.return_value = _job_with_state(
            batch_v1.JobStatus.State.RUNNING
        )
        mock_hook.return_value.wait_for_job.return_value = _job_with_state(batch_v1.JobStatus.State.SUCCEEDED)
        operator = CloudBatchSubmitJobOperator(**OPERATOR_ARGS)

        result = operator.execute(context=_context(store))

        assert result["name"] == FULL_JOB_NAME
        assert stored["cloud_batch_job_name"] == FULL_JOB_NAME
        mock_hook.return_value.get_job.assert_called_once_with(job_name=FULL_JOB_NAME)
        mock_hook.return_value.submit_batch_job.assert_called_once_with(
            job_name=JOB_NAME, job=JOB, region=REGION, project_id=PROJECT_ID
        )

    @mock.patch(CLOUD_BATCH_HOOK_PATH, autospec=True)
    def test_retry_rejects_non_string_job_name(self, mock_hook, task_state_store):
        store, _, _ = task_state_store
        store.set(key="cloud_batch_job_name", value=42)
        operator = CloudBatchSubmitJobOperator(**OPERATOR_ARGS)

        with pytest.raises(ValueError, match="Stored Cloud Batch job name is not a string: 42"):
            operator.execute(context=_context(store))

        mock_hook.return_value.submit_batch_job.assert_not_called()

    @mock.patch(CLOUD_BATCH_HOOK_PATH, autospec=True)
    def test_durable_false_submits_fresh(self, mock_hook, task_state_store):
        store, _, supervisor_comms = task_state_store
        store.set(key="cloud_batch_job_name", value=FULL_JOB_NAME)
        supervisor_comms.reset_mock()
        mock_hook.return_value.submit_batch_job.return_value = _job_with_state(
            batch_v1.JobStatus.State.RUNNING
        )
        mock_hook.return_value.wait_for_job.return_value = _job_with_state(batch_v1.JobStatus.State.SUCCEEDED)
        operator = CloudBatchSubmitJobOperator(**OPERATOR_ARGS, durable=False)

        result = operator.execute(context=_context(store))

        assert result["name"] == FULL_JOB_NAME
        mock_hook.return_value.submit_batch_job.assert_called_once()
        mock_hook.return_value.get_job.assert_not_called()
        assert not any(
            isinstance(call.args[0], GetTaskStateStore) for call in supervisor_comms.send.call_args_list
        )

    @mock.patch(CLOUD_BATCH_HOOK_PATH, autospec=True)
    def test_missing_task_state_store_submits_fresh(self, mock_hook):
        mock_hook.return_value.submit_batch_job.return_value = _job_with_state(
            batch_v1.JobStatus.State.RUNNING
        )
        mock_hook.return_value.wait_for_job.return_value = _job_with_state(batch_v1.JobStatus.State.SUCCEEDED)
        operator = CloudBatchSubmitJobOperator(**OPERATOR_ARGS)

        result = operator.execute(context={})

        assert result["name"] == FULL_JOB_NAME
        mock_hook.return_value.submit_batch_job.assert_called_once()

    @mock.patch(CLOUD_BATCH_HOOK_PATH, autospec=True)
    def test_execute_deferrable(self, mock_hook, task_state_store):
        store, _, supervisor_comms = task_state_store
        store.set(key="cloud_batch_job_name", value=FULL_JOB_NAME)
        supervisor_comms.reset_mock()
        mock_hook.return_value.submit_batch_job.return_value = JOB
        operator = CloudBatchSubmitJobOperator(**OPERATOR_ARGS, deferrable=True)

        with pytest.raises(expected_exception=TaskDeferred):
            operator.execute(context=_context(store))

        mock_hook.return_value.submit_batch_job.assert_called_once()
        mock_hook.return_value.get_job.assert_not_called()
        mock_hook.return_value.wait_for_job.assert_not_called()
        assert not supervisor_comms.send.called

    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute_complete(self, mock):
        mock.return_value.get_job.return_value = JOB
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB, deferrable=True
        )

        event = {"status": "success", "job_name": JOB_NAME, "message": "test error"}
        completed_job = operator.execute_complete(context=mock.MagicMock(), event=event)

        assert completed_job["name"] == JOB_NAME

        mock.return_value.get_job.assert_called_once_with(job_name=JOB_NAME)

    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute_complete_exception(self, mock):
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, job_name=JOB_NAME, job=JOB, deferrable=True
        )

        event = {"status": "error", "job_name": JOB_NAME, "message": "test error"}
        with pytest.raises(
            expected_exception=AirflowException, match="Unexpected error in the operation: test error"
        ):
            operator.execute_complete(context=mock.MagicMock(), event=event)


def _job_dict_with_template() -> dict:
    return {
        "task_groups": [
            {
                "task_spec": {
                    "runnables": [
                        {
                            "container": {
                                "image_uri": "gcr.io/google-containers/busybox",
                                "entrypoint": "/bin/sh",
                                "commands": ["-c", "echo {{ ds }}"],
                            }
                        }
                    ]
                }
            }
        ],
        "labels": {"run_id": "{{ run_id }}"},
    }


class TestCloudBatchSubmitJobOperatorTemplating:
    def test_template_fields_includes_job(self):
        assert "job" in CloudBatchSubmitJobOperator.template_fields

    @pytest.mark.parametrize(
        ("job_input_factory", "job_input_type"),
        [
            pytest.param(lambda d: d, dict, id="dict"),
            pytest.param(lambda d: batch_v1.Job.from_json(json.dumps(d)), batch_v1.Job, id="protobuf-Job"),
        ],
    )
    def test_job_is_unchanged_until_template_preparation(self, job_input_factory, job_input_type):
        job = job_input_factory(_job_dict_with_template())
        operator = CloudBatchSubmitJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            job=job,
        )

        assert operator.job is job
        assert isinstance(operator.job, job_input_type)

        operator.resolve_template_files()
        assert isinstance(operator.job, dict)
        prepared_job = operator.job
        operator.resolve_template_files()
        assert operator.job is prepared_job

    @pytest.mark.db_test
    @pytest.mark.parametrize(
        "job_input_factory",
        [
            pytest.param(lambda d: d, id="dict"),
            pytest.param(lambda d: batch_v1.Job.from_json(json.dumps(d)), id="protobuf-Job"),
        ],
    )
    def test_jinja_in_job_commands_is_rendered(
        self, dag_maker, create_task_instance_of_operator, job_input_factory
    ):
        job = job_input_factory(_job_dict_with_template())
        ti = create_task_instance_of_operator(
            CloudBatchSubmitJobOperator,
            dag_id="test_cloud_batch_render",
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
            job=job,
            logical_date=datetime(2026, 1, 15),
        )
        operator = dag_maker.dag.get_task(TASK_ID)
        assert isinstance(operator.job, dict)

        task = ti.render_templates()

        assert isinstance(task.job, dict)
        rendered_cmd = task.job["task_groups"][0]["task_spec"]["runnables"][0]["container"]["commands"][1]
        assert rendered_cmd == "echo 2026-01-15"
        # dag_maker's default run_id is "test"; the point is {{ run_id }} got substituted at all.
        assert task.job["labels"]["run_id"] == "test"

    @pytest.mark.db_test
    @pytest.mark.need_serialized_dag
    def test_protobuf_job_is_prepared_before_serialization(self, dag_maker):
        job = batch_v1.Job.from_json(json.dumps(_job_dict_with_template()))

        with dag_maker(dag_id="test_cloud_batch_serialization"):
            operator = CloudBatchSubmitJobOperator(
                task_id=TASK_ID,
                project_id=PROJECT_ID,
                region=REGION,
                job_name=JOB_NAME,
                job=job,
            )
            assert operator.job is job
            dag_maker.dag.resolve_template_files()

        serialized_dag = DagSerialization.deserialize_dag(dag_maker.get_serialized_data()["dag"])
        serialized_job = serialized_dag.get_task(TASK_ID).job

        assert isinstance(serialized_job, dict)
        command = serialized_job["task_groups"][0]["task_spec"]["runnables"][0]["container"]["commands"][1]
        assert command == "echo {{ ds }}"


class TestCloudBatchDeleteJobOperator:
    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute(self, hook_mock):
        delete_operation_mock = self._delete_operation_mock()
        hook_mock.return_value.delete_job.return_value = delete_operation_mock

        operator = CloudBatchDeleteJobOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=JOB_NAME,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.delete_job.assert_called_once_with(
            job_name=JOB_NAME, region=REGION, project_id=PROJECT_ID
        )
        delete_operation_mock.result.assert_called_once()

    def _delete_operation_mock(self):
        operation = mock.MagicMock()
        operation.result.return_value = mock.MagicMock()
        return operation


class TestCloudBatchListJobsOperator:
    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute(self, hook_mock):
        filter = "filter_description"
        limit = 2
        operator = CloudBatchListJobsOperator(
            task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, filter=filter, limit=limit
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.list_jobs.assert_called_once_with(
            region=REGION, project_id=PROJECT_ID, filter=filter, limit=limit
        )

    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute_with_invalid_limit(self, hook_mock):
        filter = "filter_description"
        limit = -1

        with pytest.raises(expected_exception=AirflowException):
            CloudBatchListJobsOperator(
                task_id=TASK_ID, project_id=PROJECT_ID, region=REGION, filter=filter, limit=limit
            )


class TestCloudBatchListTasksOperator:
    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute(self, hook_mock):
        filter = "filter_description"
        limit = 2
        job_name = "test_job"

        operator = CloudBatchListTasksOperator(
            task_id=TASK_ID,
            project_id=PROJECT_ID,
            region=REGION,
            job_name=job_name,
            filter=filter,
            limit=limit,
        )

        operator.execute(context=mock.MagicMock())

        hook_mock.return_value.list_tasks.assert_called_once_with(
            region=REGION,
            project_id=PROJECT_ID,
            filter=filter,
            job_name=job_name,
            limit=limit,
            group_name="group0",
        )

    @mock.patch(CLOUD_BATCH_HOOK_PATH)
    def test_execute_with_invalid_limit(self, hook_mock):
        filter = "filter_description"
        limit = -1
        job_name = "test_job"

        with pytest.raises(expected_exception=AirflowException):
            CloudBatchListTasksOperator(
                task_id=TASK_ID,
                project_id=PROJECT_ID,
                region=REGION,
                job_name=job_name,
                filter=filter,
                limit=limit,
            )
