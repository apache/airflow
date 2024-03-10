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
from unittest.mock import patch

import pytest
from google.cloud.aiplatform_v1 import BatchPredictionJob, HyperparameterTuningJob, JobState

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.triggers.vertex_ai import (
    BaseVertexAIJobTrigger,
    CreateBatchPredictionJobTrigger,
    CreateHyperparameterTuningJobTrigger,
)
from airflow.triggers.base import TriggerEvent
from tests.providers.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

TEST_CONN_ID = "test_connection"
TEST_PROJECT_ID = "test_propject_id"
TEST_LOCATION = "us-central-1"
TEST_HPT_JOB_ID = "test_job_id"
TEST_POLL_INTERVAL = 20
TEST_IMPERSONATION_CHAIN = "test_chain"
TEST_HPT_JOB_NAME = (
    f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/hyperparameterTuningJobs/{TEST_HPT_JOB_ID}"
)
VERTEX_AI_TRIGGER_PATH = "airflow.providers.google.cloud.triggers.vertex_ai.{}"
TEST_ERROR_MESSAGE = "Error"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"


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
    @patch(VERTEX_AI_TRIGGER_PATH.format("BaseVertexAIJobTrigger._serialize_job"))
    async def test_run(self, mock_serialize_job, job_state, job_name, status, message):
        mock_job = mock.MagicMock()
        mock_job.state = job_state
        mock_job.name = job_name

        mock_serialized_job = mock.MagicMock()
        mock_serialize_job.return_value = mock_serialized_job

        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
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
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
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

    @patch(VERTEX_AI_TRIGGER_PATH.format("BaseVertexAIJobTrigger.job_serializer_class"))
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

    @patch(VERTEX_AI_TRIGGER_PATH.format("HyperparameterTuningJobAsyncHook"))
    def test_async_hook(self, mock_async_hook):
        async_hook_actual = self.trigger.async_hook

        mock_async_hook.assert_called_once_with(
            gcp_conn_id=self.trigger.conn_id,
            impersonation_chain=self.trigger.impersonation_chain,
        )
        assert async_hook_actual == mock_async_hook.return_value

    @pytest.mark.asyncio
    @mock.patch(
        VERTEX_AI_TRIGGER_PATH.format("HyperparameterTuningJobAsyncHook.wait_hyperparameter_tuning_job")
    )
    async def test_wait_job(self, mock_wait_hyperparameter_tuning_job):
        job_expected = mock.MagicMock()
        async_mock = mock.AsyncMock(return_value=job_expected)
        mock_wait_hyperparameter_tuning_job.side_effect = async_mock

        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
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

    @patch(VERTEX_AI_TRIGGER_PATH.format("BatchPredictionJobAsyncHook"))
    def test_async_hook(self, mock_async_hook):
        async_hook_actual = self.trigger.async_hook

        mock_async_hook.assert_called_once_with(
            gcp_conn_id=self.trigger.conn_id,
            impersonation_chain=self.trigger.impersonation_chain,
        )
        assert async_hook_actual == mock_async_hook.return_value

    @pytest.mark.asyncio
    @mock.patch(VERTEX_AI_TRIGGER_PATH.format("BatchPredictionJobAsyncHook.wait_batch_prediction_job"))
    async def test_wait_job(self, mock_wait_batch_prediction_job):
        job_expected = mock.MagicMock()
        async_mock = mock.AsyncMock(return_value=job_expected)
        mock_wait_batch_prediction_job.side_effect = async_mock

        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
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
            classpath == "airflow.providers.google.cloud.triggers.vertex_ai.CreateBatchPredictionJobTrigger"
        )
        assert kwargs == dict(
            conn_id=TEST_CONN_ID,
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            job_id=TEST_HPT_JOB_ID,
            poll_interval=TEST_POLL_INTERVAL,
            impersonation_chain=TEST_IMPERSONATION_CHAIN,
        )
