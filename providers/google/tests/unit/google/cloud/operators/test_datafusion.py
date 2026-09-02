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

import warnings
from typing import Any
from unittest import mock

import pytest

from airflow import DAG
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred
from airflow.providers.google.cloud.hooks.datafusion import SUCCESS_STATES, PipelineStates
from airflow.providers.google.cloud.openlineage.facets import DataFusionRunFacet
from airflow.providers.google.cloud.operators.datafusion import (
    _DURABLE_UNSET,
    CloudDataFusionCreateInstanceOperator,
    CloudDataFusionCreatePipelineOperator,
    CloudDataFusionDeleteInstanceOperator,
    CloudDataFusionDeletePipelineOperator,
    CloudDataFusionGetInstanceOperator,
    CloudDataFusionListPipelinesOperator,
    CloudDataFusionRestartInstanceOperator,
    CloudDataFusionStartPipelineOperator,
    CloudDataFusionStopPipelineOperator,
    CloudDataFusionUpdateInstanceOperator,
    _warn_and_disable_durable_pre_3_3,
)
from airflow.providers.google.cloud.triggers.datafusion import DataFusionStartPipelineTrigger
from airflow.providers.google.cloud.utils.datafusion import DataFusionPipelineType

from tests_common.test_utils.version_compat import AIRFLOW_V_3_3_PLUS

HOOK_STR = "airflow.providers.google.cloud.operators.datafusion.DataFusionHook"
RESOURCE_PATH_TO_DICT_STR = "airflow.providers.google.cloud.operators.datafusion.resource_path_to_dict"
REQUIRES_TASK_STATE_STORE = pytest.mark.skipif(
    not AIRFLOW_V_3_3_PLUS, reason="task_state_store (durable execution) requires Airflow 3.3+"
)

TASK_ID = "test_task"
LOCATION = "test-location"
INSTANCE_NAME = "airflow-test-instance"
INSTANCE = {"type": "BASIC", "displayName": INSTANCE_NAME}
PROJECT_ID = "test_project_id"
PIPELINE_NAME = "shrubberyPipeline"
PIPELINE = {"test": "pipeline"}
PIPELINE_ID = "test_pipeline_id"
INSTANCE_URL = "http://datafusion.instance.com"
SERVICE_URL = "http://datafusion.service.com"
NAMESPACE = "TEST_NAMESPACE"
RUNTIME_ARGS = {"arg1": "a", "arg2": "b"}


class FakeTaskStateStore:
    def __init__(self, stored: dict[str, str] | None = None) -> None:
        self.values = stored or {}

    def get(self, key: str) -> str | None:
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.values[key] = value


class TestCloudDataFusionUpdateInstanceOperator:
    @mock.patch(RESOURCE_PATH_TO_DICT_STR)
    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully(self, mock_hook, mock_resource_to_dict):
        update_maks = "instance.name"
        mock_resource_to_dict.return_value = {"projects": PROJECT_ID}
        op = CloudDataFusionUpdateInstanceOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            instance=INSTANCE,
            update_mask=update_maks,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute(context=mock.MagicMock())

        mock_hook.return_value.patch_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME,
            instance=INSTANCE,
            update_mask=update_maks,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        assert mock_hook.return_value.wait_for_operation.call_count == 1


class TestCloudDataFusionRestartInstanceOperator:
    @mock.patch(RESOURCE_PATH_TO_DICT_STR)
    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully(self, mock_hook, mock_resource_path_to_dict):
        mock_resource_path_to_dict.return_value = {"projects": PROJECT_ID}
        op = CloudDataFusionRestartInstanceOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute(context=mock.MagicMock())

        mock_hook.return_value.restart_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )
        assert mock_hook.return_value.wait_for_operation.call_count == 1


class TestCloudDataFusionCreateInstanceOperator:
    @mock.patch(RESOURCE_PATH_TO_DICT_STR)
    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully(self, mock_hook, mock_resource_path_to_dict):
        mock_resource_path_to_dict.return_value = {"projects": PROJECT_ID}
        op = CloudDataFusionCreateInstanceOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            instance=INSTANCE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute(context=mock.MagicMock())

        mock_hook.return_value.create_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME,
            instance=INSTANCE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        assert mock_hook.return_value.wait_for_operation.call_count == 1


class TestCloudDataFusionDeleteInstanceOperator:
    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully(self, mock_hook):
        op = CloudDataFusionDeleteInstanceOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute(context=mock.MagicMock())

        mock_hook.return_value.delete_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )
        assert mock_hook.return_value.wait_for_operation.call_count == 1


class TestCloudDataFusionGetInstanceOperator:
    @mock.patch(RESOURCE_PATH_TO_DICT_STR)
    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully(self, mock_hook, mock_resource_path_to_dict):
        mock_resource_path_to_dict.return_value = {"projects": PROJECT_ID}
        op = CloudDataFusionGetInstanceOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute(context=mock.MagicMock())

        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )


class TestCloudDataFusionCreatePipelineOperator:
    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {
            "apiEndpoint": INSTANCE_URL,
            "serviceEndpoint": INSTANCE_URL,
        }
        op = CloudDataFusionCreatePipelineOperator(
            task_id="test_tasks",
            pipeline_name=PIPELINE_NAME,
            pipeline=PIPELINE,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.create_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            pipeline=PIPELINE,
            namespace=NAMESPACE,
        )


class TestCloudDataFusionDeletePipelineOperator:
    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {
            "apiEndpoint": INSTANCE_URL,
            "serviceEndpoint": INSTANCE_URL,
        }
        op = CloudDataFusionDeletePipelineOperator(
            task_id="test_tasks",
            pipeline_name=PIPELINE_NAME,
            version_id="1.12",
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.delete_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            namespace=NAMESPACE,
            version_id="1.12",
        )


class TestCloudDataFusionStartPipelineOperator:
    @staticmethod
    def make_operator(**kwargs: Any) -> CloudDataFusionStartPipelineOperator:
        op = CloudDataFusionStartPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            runtime_args=RUNTIME_ARGS,
            **kwargs,
        )
        op.dag = mock.MagicMock(spec=DAG, task_dict={}, dag_id="test")
        return op

    @staticmethod
    def make_context(task_state_store: FakeTaskStateStore) -> dict[str, Any]:
        task_instance = mock.MagicMock()
        task_instance.stats_tags = {}
        return {"task_state_store": task_state_store, "ti": task_instance}

    @staticmethod
    def configure_hook(mock_hook: mock.MagicMock, pipeline_id: str = PIPELINE_ID) -> mock.MagicMock:
        hook = mock_hook.return_value
        hook.get_instance.return_value = {
            "apiEndpoint": INSTANCE_URL,
            "serviceEndpoint": SERVICE_URL,
        }
        hook.start_pipeline.return_value = pipeline_id
        return hook

    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {
            "apiEndpoint": INSTANCE_URL,
            "serviceEndpoint": INSTANCE_URL,
        }
        mock_hook.return_value.start_pipeline.return_value = PIPELINE_ID

        op = CloudDataFusionStartPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            runtime_args=RUNTIME_ARGS,
        )
        op.dag = mock.MagicMock(spec=DAG, task_dict={}, dag_id="test")

        op.execute(context=self.make_context(FakeTaskStateStore()))
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.start_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            namespace=NAMESPACE,
            runtime_args=RUNTIME_ARGS,
            pipeline_type=DataFusionPipelineType.BATCH,
        )

        mock_hook.return_value.wait_for_pipeline_state.assert_called_once_with(
            success_states=[*SUCCESS_STATES, PipelineStates.RUNNING],
            pipeline_id=PIPELINE_ID,
            pipeline_name=PIPELINE_NAME,
            pipeline_type=DataFusionPipelineType.BATCH,
            namespace=NAMESPACE,
            instance_url=INSTANCE_URL,
            timeout=300,
        )

    @REQUIRES_TASK_STATE_STORE
    @mock.patch(HOOK_STR)
    def test_first_run_persists_pipeline_id_before_polling(self, mock_hook):
        hook = self.configure_hook(mock_hook)
        task_state_store = FakeTaskStateStore()
        context = self.make_context(task_state_store)
        persisted_at_poll: list[str | None] = []

        def record_persisted_id(**kwargs: Any) -> None:
            persisted_at_poll.append(task_state_store.get("datafusion_pipeline_run_id"))

        hook.wait_for_pipeline_state.side_effect = record_persisted_id

        result = self.make_operator().execute(context=context)

        assert result == PIPELINE_ID
        assert persisted_at_poll == [PIPELINE_ID]
        assert task_state_store.values == {"datafusion_pipeline_run_id": PIPELINE_ID}

    @pytest.mark.parametrize(
        "status",
        [
            PipelineStates.PENDING,
            PipelineStates.STARTING,
            PipelineStates.RUNNING,
            PipelineStates.SUSPENDED,
            PipelineStates.RESUMING,
            "UNKNOWN",
        ],
    )
    @REQUIRES_TASK_STATE_STORE
    @mock.patch(HOOK_STR)
    def test_retry_reconnects_active_pipeline(self, mock_hook, status):
        hook = self.configure_hook(mock_hook)
        hook.get_pipeline_workflow.return_value = {"status": status}
        context = self.make_context(
            FakeTaskStateStore({"datafusion_pipeline_run_id": "existing_pipeline_id"})
        )
        op = self.make_operator()

        result = op.execute(context=context)

        assert result == "existing_pipeline_id"
        assert op.pipeline_id == "existing_pipeline_id"
        hook.start_pipeline.assert_not_called()
        hook.get_pipeline_workflow.assert_called_once_with(
            pipeline_name=PIPELINE_NAME,
            pipeline_type=DataFusionPipelineType.BATCH,
            namespace=NAMESPACE,
            instance_url=INSTANCE_URL,
            pipeline_id="existing_pipeline_id",
        )
        hook.wait_for_pipeline_state.assert_called_once_with(
            success_states=[*SUCCESS_STATES, PipelineStates.RUNNING],
            pipeline_id="existing_pipeline_id",
            pipeline_name=PIPELINE_NAME,
            pipeline_type=DataFusionPipelineType.BATCH,
            namespace=NAMESPACE,
            instance_url=INSTANCE_URL,
            timeout=300,
        )

    @REQUIRES_TASK_STATE_STORE
    @mock.patch("airflow.providers.google.cloud.operators.datafusion.DataFusionPipelineLink.persist")
    @mock.patch(HOOK_STR)
    def test_retry_recovers_completed_pipeline(self, mock_hook, mock_persist):
        hook = self.configure_hook(mock_hook)
        hook.get_pipeline_workflow.return_value = {"status": PipelineStates.COMPLETED}
        context = self.make_context(
            FakeTaskStateStore({"datafusion_pipeline_run_id": "completed_pipeline_id"})
        )
        op = self.make_operator()

        result = op.execute(context=context)

        assert result == "completed_pipeline_id"
        assert op.pipeline_id == "completed_pipeline_id"
        hook.start_pipeline.assert_not_called()
        hook.wait_for_pipeline_state.assert_not_called()
        hook.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        hook.get_pipeline_workflow.assert_called_once_with(
            pipeline_name=PIPELINE_NAME,
            pipeline_type=DataFusionPipelineType.BATCH,
            namespace=NAMESPACE,
            instance_url=INSTANCE_URL,
            pipeline_id="completed_pipeline_id",
        )
        mock_persist.assert_called_once_with(
            context=context,
            uri=SERVICE_URL,
            namespace=NAMESPACE,
            pipeline_name=PIPELINE_NAME,
        )

    @pytest.mark.parametrize(
        "status",
        [PipelineStates.FAILED, PipelineStates.KILLED, PipelineStates.REJECTED],
    )
    @REQUIRES_TASK_STATE_STORE
    @mock.patch(HOOK_STR)
    def test_retry_resubmits_after_terminal_pipeline(self, mock_hook, status):
        hook = self.configure_hook(mock_hook, pipeline_id="new_pipeline_id")
        hook.get_pipeline_workflow.return_value = {"status": status}
        task_state_store = FakeTaskStateStore({"datafusion_pipeline_run_id": "failed_pipeline_id"})
        context = self.make_context(task_state_store)
        op = self.make_operator()

        result = op.execute(context=context)

        assert result == "new_pipeline_id"
        assert op.pipeline_id == "new_pipeline_id"
        assert task_state_store.values == {"datafusion_pipeline_run_id": "new_pipeline_id"}
        hook.start_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            namespace=NAMESPACE,
            runtime_args=RUNTIME_ARGS,
            pipeline_type=DataFusionPipelineType.BATCH,
        )
        hook.wait_for_pipeline_state.assert_called_once_with(
            success_states=[*SUCCESS_STATES, PipelineStates.RUNNING],
            pipeline_id="new_pipeline_id",
            pipeline_name=PIPELINE_NAME,
            pipeline_type=DataFusionPipelineType.BATCH,
            namespace=NAMESPACE,
            instance_url=INSTANCE_URL,
            timeout=300,
        )

    @REQUIRES_TASK_STATE_STORE
    @mock.patch(HOOK_STR)
    def test_retry_lookup_error_does_not_submit_another_pipeline(self, mock_hook):
        hook = self.configure_hook(mock_hook)
        hook.get_pipeline_workflow.side_effect = RuntimeError("stored run is not visible")
        context = self.make_context(
            FakeTaskStateStore({"datafusion_pipeline_run_id": "existing_pipeline_id"})
        )

        with pytest.raises(RuntimeError, match="stored run is not visible"):
            self.make_operator().execute(context=context)

        hook.start_pipeline.assert_not_called()

    @mock.patch(HOOK_STR)
    def test_durable_false_submits_without_task_state_store(self, mock_hook):
        hook = self.configure_hook(mock_hook, pipeline_id="new_pipeline_id")
        task_state_store = FakeTaskStateStore({"datafusion_pipeline_run_id": "existing_pipeline_id"})
        context = self.make_context(task_state_store)

        result = self.make_operator(durable=False).execute(context=context)

        assert result == "new_pipeline_id"
        assert task_state_store.values == {"datafusion_pipeline_run_id": "existing_pipeline_id"}
        hook.get_pipeline_workflow.assert_not_called()
        hook.start_pipeline.assert_called_once()

    @mock.patch(HOOK_STR)
    def test_asynchronous_execution_does_not_use_recovery(self, mock_hook):
        hook = self.configure_hook(mock_hook, pipeline_id="new_pipeline_id")
        task_state_store = FakeTaskStateStore({"datafusion_pipeline_run_id": "existing_pipeline_id"})
        context = self.make_context(task_state_store)

        result = self.make_operator(asynchronous=True).execute(context=context)

        assert result == "new_pipeline_id"
        assert task_state_store.values == {"datafusion_pipeline_run_id": "existing_pipeline_id"}
        hook.get_pipeline_workflow.assert_not_called()
        hook.wait_for_pipeline_state.assert_not_called()

    @mock.patch(HOOK_STR)
    def test_deferrable_execution_does_not_use_recovery(self, mock_hook):
        hook = self.configure_hook(mock_hook, pipeline_id="new_pipeline_id")
        task_state_store = FakeTaskStateStore({"datafusion_pipeline_run_id": "existing_pipeline_id"})
        context = self.make_context(task_state_store)

        with pytest.raises(TaskDeferred) as exc:
            self.make_operator(deferrable=True).execute(context=context)

        assert exc.value.trigger.pipeline_id == "new_pipeline_id"
        assert task_state_store.values == {"datafusion_pipeline_run_id": "existing_pipeline_id"}
        hook.get_pipeline_workflow.assert_not_called()
        hook.wait_for_pipeline_state.assert_not_called()

    @REQUIRES_TASK_STATE_STORE
    @mock.patch(HOOK_STR)
    def test_streaming_retry_reconnects_running_pipeline(self, mock_hook):
        hook = self.configure_hook(mock_hook)
        hook.get_pipeline_workflow.return_value = {"status": PipelineStates.RUNNING}
        context = self.make_context(
            FakeTaskStateStore({"datafusion_pipeline_run_id": "existing_pipeline_id"})
        )

        result = self.make_operator(pipeline_type=DataFusionPipelineType.STREAM).execute(context=context)

        assert result == "existing_pipeline_id"
        hook.start_pipeline.assert_not_called()
        hook.get_pipeline_workflow.assert_called_once_with(
            pipeline_name=PIPELINE_NAME,
            pipeline_type=DataFusionPipelineType.STREAM,
            namespace=NAMESPACE,
            instance_url=INSTANCE_URL,
            pipeline_id="existing_pipeline_id",
        )
        hook.wait_for_pipeline_state.assert_called_once_with(
            success_states=[*SUCCESS_STATES, PipelineStates.RUNNING],
            pipeline_id="existing_pipeline_id",
            pipeline_name=PIPELINE_NAME,
            pipeline_type=DataFusionPipelineType.STREAM,
            namespace=NAMESPACE,
            instance_url=INSTANCE_URL,
            timeout=300,
        )

    @REQUIRES_TASK_STATE_STORE
    @mock.patch(HOOK_STR)
    def test_on_kill_stops_only_recovered_pipeline_run(self, mock_hook):
        hook = self.configure_hook(mock_hook)
        hook.get_pipeline_workflow.return_value = {"status": PipelineStates.RUNNING}
        context = self.make_context(
            FakeTaskStateStore({"datafusion_pipeline_run_id": "existing_pipeline_id"})
        )
        op = self.make_operator()
        hook.wait_for_pipeline_state.side_effect = lambda **kwargs: op.on_kill()

        op.execute(context=context)

        hook.stop_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            namespace=NAMESPACE,
            pipeline_type=DataFusionPipelineType.BATCH,
            run_id="existing_pipeline_id",
        )

    def test_default_args_durable_reaches_operator(self):
        op = self.make_operator(default_args={"durable": False})

        assert op.durable is False

    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_asynch_param_should_execute_successfully(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {
            "apiEndpoint": INSTANCE_URL,
            "serviceEndpoint": INSTANCE_URL,
        }
        mock_hook.return_value.start_pipeline.return_value = PIPELINE_ID

        op = CloudDataFusionStartPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            runtime_args=RUNTIME_ARGS,
            asynchronous=True,
        )
        op.dag = mock.MagicMock(spec=DAG, task_dict={}, dag_id="test")
        op.execute(context=mock.MagicMock())

        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )
        mock_hook.return_value.start_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            namespace=NAMESPACE,
            runtime_args=RUNTIME_ARGS,
            pipeline_type=DataFusionPipelineType.BATCH,
        )
        mock_hook.return_value.wait_for_pipeline_state.assert_not_called()


class TestCloudDataFusionStartPipelineOperatorAsync:
    @mock.patch(HOOK_STR)
    def test_asynch_execute_should_execute_successfully(self, mock_hook):
        """
        Asserts that a task is deferred and a DataFusionStartPipelineTrigger will be fired
        when the CloudDataFusionStartPipelineOperator is executed in deferrable mode when deferrable=True.
        """

        op = CloudDataFusionStartPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            runtime_args=RUNTIME_ARGS,
            deferrable=True,
        )
        op.dag = mock.MagicMock(spec=DAG, task_dict={}, dag_id="test")
        with pytest.raises(TaskDeferred) as exc:
            op.execute(context=mock.MagicMock())

        assert isinstance(exc.value.trigger, DataFusionStartPipelineTrigger), (
            "Trigger is not a DataFusionStartPipelineTrigger"
        )

    def test_asynch_execute_should_should_throw_exception(self):
        """Tests that an AirflowException is raised in case of error event"""

        op = CloudDataFusionStartPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            runtime_args=RUNTIME_ARGS,
            deferrable=True,
        )
        with pytest.raises(AirflowException):
            op.execute_complete(
                context=mock.MagicMock(), event={"status": "error", "message": "test failure message"}
            )

    def test_asynch_execute_logging_should_execute_successfully(self):
        """Asserts that logging occurs as expected"""

        op = CloudDataFusionStartPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            runtime_args=RUNTIME_ARGS,
            deferrable=True,
        )
        with mock.patch.object(op.log, "info") as mock_log_info:
            op.execute_complete(
                context=mock.MagicMock(),
                event={"status": "success", "message": "Pipeline completed", "pipeline_id": PIPELINE_ID},
            )
        mock_log_info.assert_called_with("%s completed with response %s ", TASK_ID, "Pipeline completed")

    @mock.patch(HOOK_STR)
    def test_asynch_execute_check_hook_call_should_execute_successfully(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {
            "apiEndpoint": INSTANCE_URL,
            "serviceEndpoint": INSTANCE_URL,
        }
        mock_hook.return_value.start_pipeline.return_value = PIPELINE_ID
        op = CloudDataFusionStartPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            runtime_args=RUNTIME_ARGS,
            deferrable=True,
        )
        with pytest.raises(TaskDeferred):
            op.execute(context=mock.MagicMock())

        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )
        mock_hook.return_value.start_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            namespace=NAMESPACE,
            runtime_args=RUNTIME_ARGS,
            pipeline_type=DataFusionPipelineType.BATCH,
        )

    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_asynch_param_should_execute_successfully(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {
            "apiEndpoint": INSTANCE_URL,
            "serviceEndpoint": INSTANCE_URL,
        }
        mock_hook.return_value.start_pipeline.return_value = PIPELINE_ID
        op = CloudDataFusionStartPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            runtime_args=RUNTIME_ARGS,
            asynchronous=True,
            deferrable=True,
        )
        op.dag = mock.MagicMock(spec=DAG, task_dict={}, dag_id="test")
        with pytest.raises(
            AirflowException,
            match=r"Both asynchronous and deferrable parameters were passed. Please, provide only one.",
        ):
            op.execute(context=mock.MagicMock())

    @pytest.mark.parametrize(
        ("pipeline_id", "runtime_args", "expected_run_id", "expected_runtime_args", "expected_output_suffix"),
        [
            ("abc123", {"arg1": "val1"}, "abc123", {"arg1": "val1"}, "abc123"),
            (None, None, None, None, "unknown"),
        ],
    )
    @mock.patch("airflow.providers.google.cloud.operators.datafusion.DataFusionPipelineLink.persist")
    @mock.patch(HOOK_STR)
    def test_openlineage_facets_with_mock(
        self,
        mock_hook,
        mock_persist,
        pipeline_id,
        runtime_args,
        expected_run_id,
        expected_runtime_args,
        expected_output_suffix,
    ):
        mock_persist.return_value = None

        mock_instance = {"apiEndpoint": "https://mock-endpoint", "serviceEndpoint": "https://mock-service"}
        mock_hook.return_value.get_instance.return_value = mock_instance
        mock_hook.return_value.start_pipeline.return_value = pipeline_id

        op = CloudDataFusionStartPipelineOperator(
            task_id=TASK_ID,
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            runtime_args=runtime_args,
        )

        result_pipeline_id = op.execute(context={})
        results = op.get_openlineage_facets_on_complete(task_instance=None)

        assert result_pipeline_id == pipeline_id
        assert op.pipeline_id == pipeline_id

        expected_input_name = f"{PROJECT_ID}:{LOCATION}:{INSTANCE_NAME}:{PIPELINE_NAME}"

        assert results is not None
        assert len(results.inputs) == 1
        assert results.inputs[0].namespace == "datafusion"
        assert results.inputs[0].name == expected_input_name

        assert len(results.outputs) == 1
        assert results.outputs[0].namespace == "datafusion"
        assert results.outputs[0].name == f"{expected_input_name}:{expected_output_suffix}"

        facet = results.run_facets["dataFusionRun"]
        assert isinstance(facet, DataFusionRunFacet)
        assert facet.runId == expected_run_id
        assert facet.runtimeArgs == expected_runtime_args

        assert results.job_facets == {}


class TestWarnAndDisableDurableAirflowPre3_3:
    def test_no_warning_when_unset(self):
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = _warn_and_disable_durable_pre_3_3(_DURABLE_UNSET)

        assert result is False
        assert caught == []

    @pytest.mark.parametrize("value", [True, False])
    def test_warns_and_disables_when_explicitly_set(self, value):
        with pytest.warns(UserWarning, match="durable.*no effect"):
            result = _warn_and_disable_durable_pre_3_3(value)

        assert result is False


class TestCloudDataFusionStopPipelineOperator:
    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {
            "apiEndpoint": INSTANCE_URL,
            "serviceEndpoint": INSTANCE_URL,
        }
        op = CloudDataFusionStopPipelineOperator(
            task_id="test_tasks",
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.stop_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            namespace=NAMESPACE,
            pipeline_type=DataFusionPipelineType.BATCH,
            run_id=None,
        )

    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully_with_runId(self, mock_hook):
        mock_hook.return_value.get_instance.return_value = {
            "apiEndpoint": INSTANCE_URL,
            "serviceEndpoint": INSTANCE_URL,
        }
        op = CloudDataFusionStopPipelineOperator(
            task_id="test_tasks",
            pipeline_name=PIPELINE_NAME,
            instance_name=INSTANCE_NAME,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
            run_id="sample-run-id",
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.stop_pipeline.assert_called_once_with(
            instance_url=INSTANCE_URL,
            pipeline_name=PIPELINE_NAME,
            pipeline_type=DataFusionPipelineType.BATCH,
            namespace=NAMESPACE,
            run_id="sample-run-id",
        )


class TestCloudDataFusionListPipelinesOperator:
    @mock.patch(HOOK_STR)
    def test_execute_check_hook_call_should_execute_successfully(self, mock_hook):
        artifact_version = "artifact_version"
        artifact_name = "artifact_name"
        mock_hook.return_value.get_instance.return_value = {
            "apiEndpoint": INSTANCE_URL,
            "serviceEndpoint": INSTANCE_URL,
        }
        op = CloudDataFusionListPipelinesOperator(
            task_id="test_tasks",
            instance_name=INSTANCE_NAME,
            artifact_version=artifact_version,
            artifact_name=artifact_name,
            namespace=NAMESPACE,
            location=LOCATION,
            project_id=PROJECT_ID,
        )
        op.execute(context=mock.MagicMock())
        mock_hook.return_value.get_instance.assert_called_once_with(
            instance_name=INSTANCE_NAME, location=LOCATION, project_id=PROJECT_ID
        )

        mock_hook.return_value.list_pipelines.assert_called_once_with(
            instance_url=INSTANCE_URL,
            namespace=NAMESPACE,
            artifact_version=artifact_version,
            artifact_name=artifact_name,
        )
