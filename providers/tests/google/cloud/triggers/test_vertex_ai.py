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

import asyncio
from unittest import mock

import pytest

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.cloud.aiplatform_v1 import (
    BatchPredictionJob,
    HyperparameterTuningJob,
    JobState,
    PipelineServiceAsyncClient,
    PipelineState,
    types,
)

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.custom_job import CustomJobAsyncHook
from airflow.providers.google.cloud.hooks.vertex_ai.pipeline_job import (
    PipelineJobAsyncHook,
)
from airflow.providers.google.cloud.triggers.vertex_ai import (
    BaseVertexAIJobTrigger,
    CreateBatchPredictionJobTrigger,
    CreateHyperparameterTuningJobTrigger,
    CustomContainerTrainingJobTrigger,
    CustomPythonPackageTrainingJobTrigger,
    CustomTrainingJobTrigger,
    RunPipelineJobTrigger,
)
from airflow.triggers.base import TriggerEvent

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
)

TEST_CONN_ID = "test_connection"
TEST_PROJECT_ID = "test_propject_id"
TEST_LOCATION = "us-central-1"
TEST_HPT_JOB_ID = "test_job_id"
TEST_POLL_INTERVAL = 20
TEST_IMPERSONATION_CHAIN = "test_chain"
TEST_HPT_JOB_NAME = f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/hyperparameterTuningJobs/{TEST_HPT_JOB_ID}"
VERTEX_AI_TRIGGER_PATH = "airflow.providers.google.cloud.triggers.vertex_ai.{}"
TEST_ERROR_MESSAGE = "Error"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"


@pytest.fixture
def run_pipeline_job_trigger():
    return RunPipelineJobTrigger(
        conn_id=TEST_CONN_ID,
        project_id=TEST_PROJECT_ID,
        location=TEST_LOCATION,
        job_id=TEST_HPT_JOB_ID,
        poll_interval=TEST_POLL_INTERVAL,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def custom_container_training_job_trigger():
    return CustomContainerTrainingJobTrigger(
        conn_id=TEST_CONN_ID,
        project_id=TEST_PROJECT_ID,
        location=TEST_LOCATION,
        job_id=TEST_HPT_JOB_ID,
        poll_interval=TEST_POLL_INTERVAL,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def custom_python_package_training_job_trigger():
    return CustomPythonPackageTrainingJobTrigger(
        conn_id=TEST_CONN_ID,
        project_id=TEST_PROJECT_ID,
        location=TEST_LOCATION,
        job_id=TEST_HPT_JOB_ID,
        poll_interval=TEST_POLL_INTERVAL,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def custom_training_job_trigger():
    return CustomTrainingJobTrigger(
        conn_id=TEST_CONN_ID,
        project_id=TEST_PROJECT_ID,
        location=TEST_LOCATION,
        job_id=TEST_HPT_JOB_ID,
        poll_interval=TEST_POLL_INTERVAL,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def custom_job_async_hook():
    return CustomJobAsyncHook(
        gcp_conn_id=TEST_CONN_ID,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def pipeline_job_async_hook():
    return PipelineJobAsyncHook(
        gcp_conn_id=TEST_CONN_ID,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


@pytest.fixture
def pipeline_service_async_client():
    return PipelineServiceAsyncClient(
        credentials=mock.MagicMock(),
    )


@pytest.fixture
def test_pipeline_job_name(pipeline_service_async_client):
    return pipeline_service_async_client.pipeline_job_path(
        project=TEST_PROJECT_ID,
        location=TEST_LOCATION,
        pipeline_job=TEST_HPT_JOB_ID,
    )


@pytest.fixture
def test_training_pipeline_name(pipeline_service_async_client):
    return pipeline_service_async_client.training_pipeline_path(
        project=TEST_PROJECT_ID,
        location=TEST_LOCATION,
        training_pipeline=TEST_HPT_JOB_ID,
    )


class TestBaseVertexAIJobTrigger:
    def setup_method(self, method):
        self.trigger = BaseVertexAIJobTrigger(
            conn_id=TEST_CONN_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            job_id=TEST_HPT_JOB_ID,
            poll_interval=TEST_POLL_INTERVAL,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_serialize(self):
        classpath, kwargs = self.trigger.serialize()
        assert classpath == VERTEX_AI_TRIGGER_PATH.format("BaseVertexAIJobTrigger")
        assert kwargs == dict(
            conn_id=TEST_CONN_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            job_id=TEST_HPT_JOB_ID,
            poll_interval=TEST_POLL_INTERVAL,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "job_state, job_name, status, message",
        [
            (
                JobState.JOB_STATE_CANCELLED,
                "test_job_name_0",
                "error",
                "Vertex AI Job test_job_name_0 completed with status JOB_STATE_CANCELLED",
            ),
            (
                JobState.JOB_STATE_FAILED,
                "test_job_name_1",
                "error",
                "Vertex AI Job test_job_name_1 completed with status JOB_STATE_FAILED",
            ),
            (
                JobState.JOB_STATE_PAUSED,
                "test_job_name_2",
                "success",
                "Vertex AI Job test_job_name_2 completed with status JOB_STATE_PAUSED",
            ),
            (
                JobState.JOB_STATE_SUCCEEDED,
                "test_job_name_3",
                "success",
                "Vertex AI Job test_job_name_3 completed with status JOB_STATE_SUCCEEDED",
            ),
        ],
    )
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("BaseVertexAIJobTrigger._serialize_job"))
    async def test_run(self, mock_serialize_job, job_state, job_name, status, message):
        mock_job = mock.MagicMock()
        mock_job.state = job_state
        mock_job.name = job_name

        mock_serialized_job = mock.MagicMock()
        mock_serialize_job.return_value = mock_serialized_job

        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            with mock.patch.object(self.trigger, "_wait_job") as mock_wait_job:
                mock_wait_job.side_effect = mock.AsyncMock(return_value=mock_job)

                generator = self.trigger.run()
                event_actual = await generator.asend(None)  # type:ignore[attr-defined]

        mock_wait_job.assert_awaited_once()
        mock_serialize_job.assert_called_once_with(mock_job)
        assert event_actual == TriggerEvent(
            {
                "status": status,
                "message": message,
                "job": mock_serialized_job,
            }
        )

    @pytest.mark.asyncio
    async def test_run_exception(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            with mock.patch.object(self.trigger, "_wait_job") as mock_wait_job:
                mock_wait_job.side_effect = AirflowException(TEST_ERROR_MESSAGE)

                generator = self.trigger.run()
                event_actual = await generator.asend(None)  # type:ignore[attr-defined]

        assert event_actual == TriggerEvent(
            {
                "status": "error",
                "message": TEST_ERROR_MESSAGE,
            }
        )

    @pytest.mark.asyncio
    async def test_wait_job(self):
        with pytest.raises(NotImplementedError):
            await self.trigger._wait_job()

    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("BaseVertexAIJobTrigger.job_serializer_class")
    )
    def test_serialize_job(self, mock_job_serializer_class):
        mock_job = mock.MagicMock()
        mock_job_serialized = mock.MagicMock()
        mock_to_dict = mock.MagicMock(return_value=mock_job_serialized)
        mock_job_serializer_class.to_dict = mock_to_dict

        result = self.trigger._serialize_job(mock_job)

        mock_to_dict.assert_called_once_with(mock_job)
        assert result == mock_job_serialized


class TestCreateHyperparameterTuningJobTrigger:
    def setup_method(self):
        self.trigger = CreateHyperparameterTuningJobTrigger(
            conn_id=TEST_CONN_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            job_id=TEST_HPT_JOB_ID,
            poll_interval=TEST_POLL_INTERVAL,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_class_attributes(self):
        assert self.trigger.trigger_class_path == (
            "airflow.providers.google.cloud.triggers.vertex_ai.CreateHyperparameterTuningJobTrigger"
        )
        assert self.trigger.job_type_verbose_name == "Hyperparameter Tuning Job"
        assert self.trigger.job_serializer_class == HyperparameterTuningJob

    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("HyperparameterTuningJobAsyncHook"))
    def test_async_hook(self, mock_async_hook):
        async_hook_actual = self.trigger.async_hook

        mock_async_hook.assert_called_once_with(
            gcp_conn_id=self.trigger.conn_id,
            impersonation_chain=self.trigger.impersonation_chain,
        )
        assert async_hook_actual == mock_async_hook.return_value

    @pytest.mark.asyncio
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format(
            "HyperparameterTuningJobAsyncHook.wait_hyperparameter_tuning_job"
        )
    )
    async def test_wait_job(self, mock_wait_hyperparameter_tuning_job):
        job_expected = mock.MagicMock()
        async_mock = mock.AsyncMock(return_value=job_expected)
        mock_wait_hyperparameter_tuning_job.side_effect = async_mock

        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            job_actual = await self.trigger._wait_job()

        mock_wait_hyperparameter_tuning_job.assert_awaited_once_with(
            project_id=self.trigger.project_id,
            location=self.trigger.location,
            job_id=self.trigger.job_id,
            poll_interval=self.trigger.poll_interval,
        )
        assert job_actual == job_expected

    def test_serialize(self):
        classpath, kwargs = self.trigger.serialize()
        assert (
            classpath
            == "airflow.providers.google.cloud.triggers.vertex_ai.CreateHyperparameterTuningJobTrigger"
        )
        assert kwargs == dict(
            conn_id=TEST_CONN_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            job_id=TEST_HPT_JOB_ID,
            poll_interval=TEST_POLL_INTERVAL,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )


class TestCreateBatchPredictionJobTrigger:
    def setup_method(self):
        self.trigger = CreateBatchPredictionJobTrigger(
            conn_id=TEST_CONN_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            job_id=TEST_HPT_JOB_ID,
            poll_interval=TEST_POLL_INTERVAL,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )

    def test_class_attributes(self):
        assert self.trigger.trigger_class_path == (
            "airflow.providers.google.cloud.triggers.vertex_ai.CreateBatchPredictionJobTrigger"
        )
        assert self.trigger.job_type_verbose_name == "Batch Prediction Job"
        assert self.trigger.job_serializer_class == BatchPredictionJob

    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("BatchPredictionJobAsyncHook"))
    def test_async_hook(self, mock_async_hook):
        async_hook_actual = self.trigger.async_hook

        mock_async_hook.assert_called_once_with(
            gcp_conn_id=self.trigger.conn_id,
            impersonation_chain=self.trigger.impersonation_chain,
        )
        assert async_hook_actual == mock_async_hook.return_value

    @pytest.mark.asyncio
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format(
            "BatchPredictionJobAsyncHook.wait_batch_prediction_job"
        )
    )
    async def test_wait_job(self, mock_wait_batch_prediction_job):
        job_expected = mock.MagicMock()
        async_mock = mock.AsyncMock(return_value=job_expected)
        mock_wait_batch_prediction_job.side_effect = async_mock

        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            job_actual = await self.trigger._wait_job()

        mock_wait_batch_prediction_job.assert_awaited_once_with(
            project_id=self.trigger.project_id,
            location=self.trigger.location,
            job_id=self.trigger.job_id,
            poll_interval=self.trigger.poll_interval,
        )
        assert job_actual == job_expected

    def test_serialize(self):
        classpath, kwargs = self.trigger.serialize()
        assert (
            classpath
            == "airflow.providers.google.cloud.triggers.vertex_ai.CreateBatchPredictionJobTrigger"
        )
        assert kwargs == dict(
            conn_id=TEST_CONN_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            job_id=TEST_HPT_JOB_ID,
            poll_interval=TEST_POLL_INTERVAL,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )


class TestRunPipelineJobTrigger:
    def test_serialize(self, run_pipeline_job_trigger):
        actual_data = run_pipeline_job_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.vertex_ai.RunPipelineJobTrigger",
            {
                "conn_id": TEST_CONN_ID,
                "project_id": TEST_PROJECT_ID,
                "location": TEST_LOCATION,
                "job_id": TEST_HPT_JOB_ID,
                "poll_interval": TEST_POLL_INTERVAL,
                "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            },
        )
        actual_data == expected_data

    @pytest.mark.asyncio
    async def test_async_hook(self, run_pipeline_job_trigger):
        hook = run_pipeline_job_trigger.async_hook
        actual_conn_id = hook._hook_kwargs.get("gcp_conn_id")
        actual_imp_chain = hook._hook_kwargs.get("impersonation_chain")
        assert (actual_conn_id, actual_imp_chain) == (
            TEST_CONN_ID,
            TEST_IMPERSONATION_CHAIN,
        )

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_SUCCEEDED,
            PipelineState.PIPELINE_STATE_PAUSED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("google.cloud.aiplatform_v1.types.PipelineJob.to_dict")
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("PipelineJobAsyncHook.get_pipeline_job"))
    async def test_run_yields_success_event_on_successful_pipeline_state(
        self,
        mock_get_pipeline_job,
        mock_pipeline_job_dict,
        run_pipeline_job_trigger,
        pipeline_state_value,
        test_pipeline_job_name,
    ):
        mock_get_pipeline_job.return_value = types.PipelineJob(
            state=pipeline_state_value, name=test_pipeline_job_name
        )
        mock_pipeline_job_dict.return_value = {}
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": (
                    f"{run_pipeline_job_trigger.job_type_verbose_name} {test_pipeline_job_name} "
                    f"completed with status {pipeline_state_value.name}"
                ),
                "job": {},
            }
        )
        actual_event = await run_pipeline_job_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_FAILED,
            PipelineState.PIPELINE_STATE_CANCELLED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("google.cloud.aiplatform_v1.types.PipelineJob.to_dict")
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("PipelineJobAsyncHook.get_pipeline_job"))
    async def test_run_yields_error_event_on_failed_pipeline_state(
        self,
        mock_get_pipeline_job,
        mock_pipeline_job_dict,
        pipeline_state_value,
        run_pipeline_job_trigger,
        test_pipeline_job_name,
    ):
        mock_get_pipeline_job.return_value = types.PipelineJob(
            state=pipeline_state_value,
            name=test_pipeline_job_name,
        )
        mock_pipeline_job_dict.return_value = {}
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": (
                    f"{run_pipeline_job_trigger.job_type_verbose_name} {test_pipeline_job_name} "
                    f"completed with status {pipeline_state_value.name}"
                ),
                "job": {},
            }
        )
        actual_event = await run_pipeline_job_trigger.run().asend(None)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_CANCELLING,
            PipelineState.PIPELINE_STATE_PENDING,
            PipelineState.PIPELINE_STATE_QUEUED,
            PipelineState.PIPELINE_STATE_RUNNING,
            PipelineState.PIPELINE_STATE_UNSPECIFIED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("PipelineJobAsyncHook.get_pipeline_job"))
    async def test_run_test_run_loop_is_still_running_if_pipeline_is_running(
        self, mock_get_pipeline_job, pipeline_state_value, run_pipeline_job_trigger
    ):
        mock_get_pipeline_job.return_value = types.PipelineJob(state=pipeline_state_value)
        task = asyncio.create_task(run_pipeline_job_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("PipelineJobAsyncHook.get_pipeline_job"))
    async def test_run_raises_exception(
        self, mock_get_pipeline_job, run_pipeline_job_trigger
    ):
        mock_get_pipeline_job.side_effect = mock.AsyncMock(
            side_effect=Exception("Test exception")
        )
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
            }
        )
        actual_event = await run_pipeline_job_trigger.run().asend(None)
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("PipelineJobAsyncHook.wait_for_pipeline_job")
    )
    async def test_wait_job(self, mock_wait_for_pipeline_job, run_pipeline_job_trigger):
        await run_pipeline_job_trigger._wait_job()
        mock_wait_for_pipeline_job.assert_awaited_once_with(
            project_id=run_pipeline_job_trigger.project_id,
            location=run_pipeline_job_trigger.location,
            job_id=run_pipeline_job_trigger.job_id,
            poll_interval=run_pipeline_job_trigger.poll_interval,
        )


class TestCustomTrainingJobTrigger:
    def test_serialize(self, custom_training_job_trigger):
        actual_data = custom_training_job_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.vertex_ai.CustomTrainingJobTrigger",
            {
                "conn_id": TEST_CONN_ID,
                "project_id": TEST_PROJECT_ID,
                "location": TEST_LOCATION,
                "job_id": TEST_HPT_JOB_ID,
                "poll_interval": TEST_POLL_INTERVAL,
                "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            },
        )
        actual_data == expected_data

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_SUCCEEDED,
            PipelineState.PIPELINE_STATE_PAUSED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("google.cloud.aiplatform_v1.types.TrainingPipeline.to_dict")
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_yields_success_event_on_successful_pipeline_state(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        mock_pipeline_job_dict,
        custom_training_job_trigger,
        pipeline_state_value,
        test_training_pipeline_name,
    ):
        mock_get_training_pipeline.return_value = types.TrainingPipeline(
            state=pipeline_state_value,
            name=test_training_pipeline_name,
        )
        mock_pipeline_job_dict.return_value = {}
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": (
                    f"{custom_training_job_trigger.job_type_verbose_name} {test_training_pipeline_name} "
                    f"completed with status {pipeline_state_value.name}"
                ),
                "job": {},
            }
        )
        actual_event = await custom_training_job_trigger.run().asend(None)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_FAILED,
            PipelineState.PIPELINE_STATE_CANCELLED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("google.cloud.aiplatform_v1.types.TrainingPipeline.to_dict")
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_yields_error_event_on_failed_pipeline_state(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        mock_pipeline_job_dict,
        pipeline_state_value,
        custom_training_job_trigger,
        test_training_pipeline_name,
    ):
        mock_get_training_pipeline.return_value = types.TrainingPipeline(
            state=pipeline_state_value,
            name=test_training_pipeline_name,
        )
        mock_pipeline_job_dict.return_value = {}
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": (
                    f"{custom_training_job_trigger.job_type_verbose_name} {test_training_pipeline_name} "
                    f"completed with status {pipeline_state_value.name}"
                ),
                "job": {},
            }
        )
        actual_event = await custom_training_job_trigger.run().asend(None)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_CANCELLING,
            PipelineState.PIPELINE_STATE_PENDING,
            PipelineState.PIPELINE_STATE_QUEUED,
            PipelineState.PIPELINE_STATE_RUNNING,
            PipelineState.PIPELINE_STATE_UNSPECIFIED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_test_run_loop_is_still_running_if_pipeline_is_running(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        pipeline_state_value,
        custom_training_job_trigger,
    ):
        mock_get_training_pipeline.return_value = types.TrainingPipeline(
            state=pipeline_state_value
        )
        task = asyncio.create_task(custom_training_job_trigger.run().__anext__())
        await asyncio.sleep(0.5)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_raises_exception(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        custom_training_job_trigger,
    ):
        mock_get_training_pipeline.side_effect = mock.AsyncMock(
            side_effect=Exception("Test exception")
        )
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
            }
        )
        actual_event = await custom_training_job_trigger.run().asend(None)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.wait_for_training_pipeline")
    )
    async def test_wait_training_pipeline(
        self, mock_wait_for_training_pipeline, custom_training_job_trigger
    ):
        await custom_training_job_trigger._wait_job()
        mock_wait_for_training_pipeline.assert_awaited_once_with(
            project_id=custom_training_job_trigger.project_id,
            location=custom_training_job_trigger.location,
            pipeline_id=custom_training_job_trigger.job_id,
            poll_interval=custom_training_job_trigger.poll_interval,
        )


class TestCustomContainerTrainingJobTrigger:
    def test_serialize(self, custom_container_training_job_trigger):
        actual_data = custom_container_training_job_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.vertex_ai.CustomContainerTrainingJobTrigger",
            {
                "conn_id": TEST_CONN_ID,
                "project_id": TEST_PROJECT_ID,
                "location": TEST_LOCATION,
                "job_id": TEST_HPT_JOB_ID,
                "poll_interval": TEST_POLL_INTERVAL,
                "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            },
        )
        actual_data == expected_data

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_SUCCEEDED,
            PipelineState.PIPELINE_STATE_PAUSED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("google.cloud.aiplatform_v1.types.TrainingPipeline.to_dict")
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_yields_success_event_on_successful_pipeline_state(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        mock_pipeline_job_dict,
        custom_container_training_job_trigger,
        pipeline_state_value,
        test_training_pipeline_name,
    ):
        mock_get_training_pipeline.return_value = types.TrainingPipeline(
            state=pipeline_state_value,
            name=test_training_pipeline_name,
        )
        mock_pipeline_job_dict.return_value = {}
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": (
                    f"{custom_container_training_job_trigger.job_type_verbose_name} {test_training_pipeline_name} "
                    f"completed with status {pipeline_state_value.name}"
                ),
                "job": {},
            }
        )
        actual_event = await custom_container_training_job_trigger.run().asend(None)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_FAILED,
            PipelineState.PIPELINE_STATE_CANCELLED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("google.cloud.aiplatform_v1.types.TrainingPipeline.to_dict")
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_yields_error_event_on_failed_pipeline_state(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        mock_pipeline_job_dict,
        pipeline_state_value,
        custom_container_training_job_trigger,
        test_training_pipeline_name,
    ):
        mock_get_training_pipeline.return_value = types.TrainingPipeline(
            state=pipeline_state_value,
            name=test_training_pipeline_name,
        )
        mock_pipeline_job_dict.return_value = {}
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": (
                    f"{custom_container_training_job_trigger.job_type_verbose_name} {test_training_pipeline_name} "
                    f"completed with status {pipeline_state_value.name}"
                ),
                "job": {},
            }
        )
        actual_event = await custom_container_training_job_trigger.run().asend(None)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_CANCELLING,
            PipelineState.PIPELINE_STATE_PENDING,
            PipelineState.PIPELINE_STATE_QUEUED,
            PipelineState.PIPELINE_STATE_RUNNING,
            PipelineState.PIPELINE_STATE_UNSPECIFIED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_test_run_loop_is_still_running_if_pipeline_is_running(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        pipeline_state_value,
        custom_container_training_job_trigger,
    ):
        mock_get_training_pipeline.return_value = types.TrainingPipeline(
            state=pipeline_state_value
        )
        task = asyncio.create_task(
            custom_container_training_job_trigger.run().__anext__()
        )
        await asyncio.sleep(0.5)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_raises_exception(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        custom_container_training_job_trigger,
    ):
        mock_get_training_pipeline.side_effect = mock.AsyncMock(
            side_effect=Exception("Test exception")
        )
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
            }
        )
        actual_event = await custom_container_training_job_trigger.run().asend(None)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.wait_for_training_pipeline")
    )
    async def test_wait_training_pipeline(
        self, mock_wait_for_training_pipeline, custom_container_training_job_trigger
    ):
        await custom_container_training_job_trigger._wait_job()
        mock_wait_for_training_pipeline.assert_awaited_once_with(
            project_id=custom_container_training_job_trigger.project_id,
            location=custom_container_training_job_trigger.location,
            pipeline_id=custom_container_training_job_trigger.job_id,
            poll_interval=custom_container_training_job_trigger.poll_interval,
        )


class TestCustomPythonPackageTrainingJobTrigger:
    def test_serialize(self, custom_python_package_training_job_trigger):
        actual_data = custom_python_package_training_job_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.vertex_ai.CustomPythonPackageTrainingJobTrigger",
            {
                "conn_id": TEST_CONN_ID,
                "project_id": TEST_PROJECT_ID,
                "location": TEST_LOCATION,
                "job_id": TEST_HPT_JOB_ID,
                "poll_interval": TEST_POLL_INTERVAL,
                "impersonation_chain": TEST_IMPERSONATION_CHAIN,
            },
        )
        actual_data == expected_data

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_SUCCEEDED,
            PipelineState.PIPELINE_STATE_PAUSED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("google.cloud.aiplatform_v1.types.TrainingPipeline.to_dict")
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_yields_success_event_on_successful_pipeline_state(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        mock_pipeline_job_dict,
        custom_python_package_training_job_trigger,
        pipeline_state_value,
        test_training_pipeline_name,
    ):
        mock_get_training_pipeline.return_value = types.TrainingPipeline(
            state=pipeline_state_value,
            name=test_training_pipeline_name,
        )
        mock_pipeline_job_dict.return_value = {}
        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": (
                    f"{custom_python_package_training_job_trigger.job_type_verbose_name} {test_training_pipeline_name} "
                    f"completed with status {pipeline_state_value.name}"
                ),
                "job": {},
            }
        )
        actual_event = await custom_python_package_training_job_trigger.run().asend(None)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_FAILED,
            PipelineState.PIPELINE_STATE_CANCELLED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch("google.cloud.aiplatform_v1.types.TrainingPipeline.to_dict")
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_yields_error_event_on_failed_pipeline_state(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        mock_pipeline_job_dict,
        pipeline_state_value,
        custom_python_package_training_job_trigger,
        test_training_pipeline_name,
    ):
        mock_get_training_pipeline.return_value = types.TrainingPipeline(
            state=pipeline_state_value,
            name=test_training_pipeline_name,
        )
        mock_pipeline_job_dict.return_value = {}
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": (
                    f"{custom_python_package_training_job_trigger.job_type_verbose_name} {test_training_pipeline_name} "
                    f"completed with status {pipeline_state_value.name}"
                ),
                "job": {},
            }
        )
        actual_event = await custom_python_package_training_job_trigger.run().asend(None)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert actual_event == expected_event

    @pytest.mark.parametrize(
        "pipeline_state_value",
        [
            PipelineState.PIPELINE_STATE_CANCELLING,
            PipelineState.PIPELINE_STATE_PENDING,
            PipelineState.PIPELINE_STATE_QUEUED,
            PipelineState.PIPELINE_STATE_RUNNING,
            PipelineState.PIPELINE_STATE_UNSPECIFIED,
        ],
    )
    @pytest.mark.asyncio
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_test_run_loop_is_still_running_if_pipeline_is_running(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        pipeline_state_value,
        custom_python_package_training_job_trigger,
    ):
        mock_get_training_pipeline.return_value = types.TrainingPipeline(
            state=pipeline_state_value
        )
        task = asyncio.create_task(
            custom_python_package_training_job_trigger.run().__anext__()
        )
        await asyncio.sleep(0.5)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert task.done() is False
        task.cancel()

    @pytest.mark.asyncio
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_training_pipeline"))
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.get_pipeline_service_client")
    )
    async def test_run_raises_exception(
        self,
        mock_get_pipeline_service_client,
        mock_get_training_pipeline,
        custom_python_package_training_job_trigger,
    ):
        mock_get_training_pipeline.side_effect = mock.AsyncMock(
            side_effect=Exception("Test exception")
        )
        expected_event = TriggerEvent(
            {
                "status": "error",
                "message": "Test exception",
            }
        )
        actual_event = await custom_python_package_training_job_trigger.run().asend(None)
        mock_get_pipeline_service_client.assert_awaited_once_with(region=TEST_LOCATION)
        assert expected_event == actual_event

    @pytest.mark.asyncio
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("CustomJobAsyncHook.wait_for_training_pipeline")
    )
    async def test_wait_training_pipeline(
        self, mock_wait_for_training_pipeline, custom_python_package_training_job_trigger
    ):
        await custom_python_package_training_job_trigger._wait_job()
        mock_wait_for_training_pipeline.assert_awaited_once_with(
            project_id=custom_python_package_training_job_trigger.project_id,
            location=custom_python_package_training_job_trigger.location,
            pipeline_id=custom_python_package_training_job_trigger.job_id,
            poll_interval=custom_python_package_training_job_trigger.poll_interval,
        )
