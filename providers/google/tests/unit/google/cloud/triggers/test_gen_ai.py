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
import logging
from unittest import mock

import pytest

from airflow.models import Connection
from airflow.providers.google.cloud.hooks.gen_ai import BatchJobStatus
from airflow.providers.google.cloud.triggers.gen_ai import (
    GenAIGeminiCreateBatchJobTrigger,
    GenAIGeminiCreateEmbeddingsBatchJobTrigger,
)
from airflow.triggers.base import TriggerEvent

BATCH_JOB_INLINED_REQUESTS = [
    {"contents": [{"parts": [{"text": "Tell me a one-sentence joke."}], "role": "user"}]},
    {"contents": [{"parts": [{"text": "Why is the sky blue?"}], "role": "user"}]},
]

EMBEDDINGS_JOB_INLINED_REQUESTS = {
    "contents": [{"parts": [{"text": "Why is the sky blue?"}], "role": "user"}]
}
GEMINI_API_KEY = "test-key"
GEMINI_MODEL = "test-gemini-model"
BATCH_JOB_NAME = "test-name"
FILE_NAME = "test-file"
FILE_PATH = "test/path/to/file"
TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
BATCH_JOB_CONFIG = {"display_name": "test-batch-job"}
RESULTS_FOLDER = "test/results/folder"


@pytest.fixture
@mock.patch(
    "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
    return_value=Connection(conn_id=GCP_CONN_ID),
)
def create_batch_job_trigger(mock_conn):
    return GenAIGeminiCreateBatchJobTrigger(
        project_id=GCP_PROJECT,
        location=GCP_LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
        model=GEMINI_MODEL,
        input_source=BATCH_JOB_INLINED_REQUESTS,
        create_batch_job_config=BATCH_JOB_CONFIG,
        gemini_api_key=GEMINI_API_KEY,
        retrieve_result=True,
        polling_interval=30,
        results_folder=RESULTS_FOLDER,
    )


@pytest.fixture
@mock.patch(
    "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_connection",
    return_value=Connection(conn_id=GCP_CONN_ID),
)
def create_embeddings_batch_job_trigger(mock_conn):
    return GenAIGeminiCreateEmbeddingsBatchJobTrigger(
        project_id=GCP_PROJECT,
        location=GCP_LOCATION,
        gcp_conn_id=GCP_CONN_ID,
        impersonation_chain=IMPERSONATION_CHAIN,
        model=GEMINI_MODEL,
        input_source=BATCH_JOB_INLINED_REQUESTS,
        create_embeddings_config=BATCH_JOB_CONFIG,
        gemini_api_key=GEMINI_API_KEY,
        retrieve_result=True,
        polling_interval=30,
        results_folder=RESULTS_FOLDER,
    )


class TestGenAIGeminiCreateBatchJobTrigger:
    def test_serialize(self, create_batch_job_trigger):
        actual_data = create_batch_job_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.gen_ai.GenAIGeminiCreateBatchJobTrigger",
            {
                "project_id": GCP_PROJECT,
                "location": GCP_LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "model": GEMINI_MODEL,
                "input_source": BATCH_JOB_INLINED_REQUESTS,
                "create_batch_job_config": BATCH_JOB_CONFIG,
                "gemini_api_key": GEMINI_API_KEY,
                "retrieve_result": True,
                "polling_interval": 30,
                "results_folder": RESULTS_FOLDER,
            },
        )
        assert actual_data == expected_data

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.create_batch_job")
    @mock.patch("airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.get_batch_job")
    async def test_run_loop_return_success_event(
        self, mock_job_status, mock_create_batch_job, create_batch_job_trigger
    ):
        mock_job_status.return_value.state.name = BatchJobStatus.SUCCEEDED.value
        mock_job_status.return_value.name = BATCH_JOB_CONFIG["display_name"]

        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": "Job completed",
                "job_name": BATCH_JOB_CONFIG["display_name"],
            }
        )
        actual_event = await create_batch_job_trigger.run().asend(None)

        mock_create_batch_job.assert_called_once()
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.create_batch_job")
    @mock.patch("airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.get_batch_job")
    async def test_run_loop_return_failed_event(
        self, mock_job_status, mock_create_batch_job, create_batch_job_trigger
    ):
        mock_job_status.return_value.state.name = BatchJobStatus.FAILED.value
        mock_job_status.return_value.name = BATCH_JOB_NAME

        expected_event = TriggerEvent(
            {"status": "error", "message": f"Job {BATCH_JOB_NAME} execution was not completed!"}
        )
        actual_event = await create_batch_job_trigger.run().asend(None)

        mock_create_batch_job.assert_called_once()
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch("airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.create_batch_job")
    @mock.patch("airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.get_batch_job")
    async def test_run_loop_is_still_running(
        self, mock_job_status, mock_create_batch_job, create_batch_job_trigger, caplog
    ):
        mock_job_status.return_value.state.name = BatchJobStatus.RUNNING.value
        caplog.set_level(logging.INFO)

        task = asyncio.create_task(create_batch_job_trigger.run().__anext__())
        await asyncio.sleep(5)

        assert not task.done()
        assert (
            f"Waiting for job execution, polling interval: 30 seconds, current state: {BatchJobStatus.RUNNING.value}"
            in caplog.text
        )
        # cancel the task to suppress test warnings
        task.cancel()


class TestGenAIGeminiCreateEmbeddingsBatchJobTrigger:
    def test_serialize(self, create_embeddings_batch_job_trigger):
        actual_data = create_embeddings_batch_job_trigger.serialize()
        expected_data = (
            "airflow.providers.google.cloud.triggers.gen_ai.GenAIGeminiCreateEmbeddingsBatchJobTrigger",
            {
                "project_id": GCP_PROJECT,
                "location": GCP_LOCATION,
                "gcp_conn_id": GCP_CONN_ID,
                "impersonation_chain": IMPERSONATION_CHAIN,
                "model": GEMINI_MODEL,
                "input_source": BATCH_JOB_INLINED_REQUESTS,
                "create_embeddings_config": BATCH_JOB_CONFIG,
                "gemini_api_key": GEMINI_API_KEY,
                "retrieve_result": True,
                "polling_interval": 30,
                "results_folder": RESULTS_FOLDER,
            },
        )
        assert actual_data == expected_data

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.create_embeddings_batch_job"
    )
    @mock.patch("airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.get_batch_job")
    async def test_run_loop_return_success_event(
        self, mock_job_status, mock_create_embeddings_batch_job, create_embeddings_batch_job_trigger
    ):
        test_job_model_dump = {"id": "test_job_id", "status": "succeeded"}
        mock_job_status.return_value.state.name = BatchJobStatus.SUCCEEDED.value
        mock_job_status.return_value.model_dump = mock.Mock(return_value=test_job_model_dump)

        expected_event = TriggerEvent(
            {
                "status": "success",
                "message": "Job completed",
                "job": test_job_model_dump,
            }
        )
        actual_event = await create_embeddings_batch_job_trigger.run().asend(None)
        mock_create_embeddings_batch_job.assert_called_once()
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.create_embeddings_batch_job"
    )
    @mock.patch("airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.get_batch_job")
    async def test_run_loop_return_failed_event(
        self, mock_job_status, mock_create_embeddings_batch_job, create_embeddings_batch_job_trigger
    ):
        mock_job_status.return_value.state.name = BatchJobStatus.FAILED.value
        mock_job_status.return_value.name = BATCH_JOB_NAME

        expected_event = TriggerEvent(
            {"status": "error", "message": f"Job {BATCH_JOB_NAME} execution was not completed!"}
        )
        actual_event = await create_embeddings_batch_job_trigger.run().asend(None)

        mock_create_embeddings_batch_job.assert_called_once()
        assert actual_event == expected_event

    @pytest.mark.asyncio
    @mock.patch(
        "airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.create_embeddings_batch_job"
    )
    @mock.patch("airflow.providers.google.cloud.hooks.gen_ai.GenAIGeminiAPIAsyncHook.get_batch_job")
    async def test_run_loop_is_still_running(
        self, mock_job_status, mock_create_embeddings_batch_job, create_embeddings_batch_job_trigger, caplog
    ):
        mock_job_status.return_value.state.name = BatchJobStatus.RUNNING.value
        caplog.set_level(logging.INFO)

        task = asyncio.create_task(create_embeddings_batch_job_trigger.run().__anext__())
        await asyncio.sleep(5)

        assert not task.done()
        assert (
            f"Waiting for job execution, polling interval: 30 seconds, current state: {BatchJobStatus.RUNNING.value}"
            in caplog.text
        )
        # cancel the task to suppress test warnings
        task.cancel()
