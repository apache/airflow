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
from google.cloud.aiplatform_v1 import JobState

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.triggers.vertex_ai import CreateHyperparameterTuningJobTrigger
from airflow.triggers.base import TriggerEvent

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


@pytest.fixture
def create_hyperparameter_tuning_job_trigger():
    return CreateHyperparameterTuningJobTrigger(
        conn_id=TEST_CONN_ID,
        project_id=TEST_PROJECT_ID,
        location=TEST_LOCATION,
        job_id=TEST_HPT_JOB_ID,
        poll_interval=TEST_POLL_INTERVAL,
        impersonation_chain=TEST_IMPERSONATION_CHAIN,
    )


class TestCreateHyperparameterTuningJobTrigger:
    def test_serialize(self, create_hyperparameter_tuning_job_trigger):
        classpath, kwargs = create_hyperparameter_tuning_job_trigger.serialize()
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

    @patch(VERTEX_AI_TRIGGER_PATH.format("HyperparameterTuningJobAsyncHook"))
    def test_get_async_hook(self, mock_async_hook, create_hyperparameter_tuning_job_trigger):
        hook_expected = mock_async_hook.return_value

        hook_created = create_hyperparameter_tuning_job_trigger._get_async_hook()

        mock_async_hook.assert_called_once_with(
            gcp_conn_id=TEST_CONN_ID, impersonation_chain=TEST_IMPERSONATION_CHAIN
        )
        assert hook_created == hook_expected

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "state, status",
        [
            (JobState.JOB_STATE_CANCELLED, "error"),
            (JobState.JOB_STATE_FAILED, "error"),
            (JobState.JOB_STATE_PAUSED, "success"),
            (JobState.JOB_STATE_SUCCEEDED, "success"),
        ],
    )
    @patch(VERTEX_AI_TRIGGER_PATH.format("HyperparameterTuningJobAsyncHook"))
    @patch(VERTEX_AI_TRIGGER_PATH.format("HyperparameterTuningJob"))
    async def test_run(
        self, mock_hpt_job, mock_async_hook, state, status, create_hyperparameter_tuning_job_trigger
    ):
        mock_job = mock.MagicMock(
            status="success",
            state=state,
        )
        mock_job.name = TEST_HPT_JOB_NAME
        mock_async_wait_hyperparameter_tuning_job = mock.AsyncMock(return_value=mock_job)
        mock_async_hook.return_value.wait_hyperparameter_tuning_job.side_effect = mock.MagicMock(
            side_effect=mock_async_wait_hyperparameter_tuning_job
        )
        mock_dict_job = mock.MagicMock()
        mock_hpt_job.to_dict.return_value = mock_dict_job

        generator = create_hyperparameter_tuning_job_trigger.run()
        event_actual = await generator.asend(None)  # type:ignore[attr-defined]

        mock_async_wait_hyperparameter_tuning_job.assert_awaited_once_with(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            job_id=TEST_HPT_JOB_ID,
            poll_interval=TEST_POLL_INTERVAL,
        )
        assert event_actual == TriggerEvent({
            "status": status,
            "message": f"Hyperparameter tuning job {TEST_HPT_JOB_NAME} completed with status {state.name}",
            "job": mock_dict_job,
        })

    @pytest.mark.asyncio
    @patch(VERTEX_AI_TRIGGER_PATH.format("HyperparameterTuningJobAsyncHook"))
    async def test_run_exception(self, mock_async_hook, create_hyperparameter_tuning_job_trigger):
        mock_async_hook.return_value.wait_hyperparameter_tuning_job.side_effect = AirflowException(
            "test error"
        )

        generator = create_hyperparameter_tuning_job_trigger.run()
        event_actual = await generator.asend(None)  # type:ignore[attr-defined]

        assert event_actual == TriggerEvent({
            "status": "error",
            "message": "test error",
        })
