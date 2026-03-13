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
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.google.cloud.hooks.gen_ai import BatchJobStatus, GenAIGeminiAPIAsyncHook
from airflow.triggers.base import BaseTrigger, TriggerEvent

if TYPE_CHECKING:
    from google.genai.types import CreateBatchJobConfig


class GenAIGeminiCreateBatchJobTrigger(BaseTrigger):
    """
    Trigger that creates Gemini Batch Job and waiting for execution.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param model: Required. The name of the publisher model to use for Batch job.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
    :param input_source: Required. Source of requests, could be inline requests or file name.
    :param results_folder: Optional. Path to a folder on local machine where file with results will be saved.
    :param create_batch_job_config: Optional. Config for batch job creation.
    :param retrieve_result: Optional. Push the result to XCom. If the input_source is inline, this pushes
        the execution result. If a file name is specified, this pushes the output file path.
    :param polling_interval: Optional. The interval, in seconds, to poll the job status.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        project_id: str,
        location: str,
        model: str,
        input_source: list | str,
        gemini_api_key: str,
        create_batch_job_config: CreateBatchJobConfig | dict | None = None,
        results_folder: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        retrieve_result: bool = False,
        polling_interval: int = 30,
    ):
        super().__init__()

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.model = model
        self.input_source = input_source
        self.create_batch_job_config = create_batch_job_config
        self.gemini_api_key = gemini_api_key
        self.retrieve_result = retrieve_result
        self.polling_interval = polling_interval
        self.results_folder = results_folder

    def _get_async_hook(self) -> GenAIGeminiAPIAsyncHook:
        return GenAIGeminiAPIAsyncHook(gemini_api_key=self.gemini_api_key)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.gen_ai.GenAIGeminiCreateBatchJobTrigger",
            {
                "project_id": self.project_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "model": self.model,
                "input_source": self.input_source,
                "create_batch_job_config": self.create_batch_job_config,
                "gemini_api_key": self.gemini_api_key,
                "retrieve_result": self.retrieve_result,
                "polling_interval": self.polling_interval,
                "results_folder": self.results_folder,
            },
        )

    async def run(self):
        """
        Loop until the job reaches  successful final or error state.

        Yields a TriggerEvent with success status, if the job reaches successful state.

        Yields a TriggerEvent with error status, if the client returns an unexpected terminal
        job status or any exception is raised while looping.

        In any other case the Trigger will wait for a specified amount of time
        stored in self.polling_interval variable.
        """
        try:
            hook = self._get_async_hook()
            job = await hook.create_batch_job(
                model=self.model,
                source=self.input_source,
                create_batch_job_config=self.create_batch_job_config,
            )
            while True:
                job = await hook.get_batch_job(job_name=job.name)
                if job.state.name == BatchJobStatus.SUCCEEDED.value:
                    self.log.info("Job execution completed")
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Job completed",
                            "job_name": job.name,
                        }
                    )
                    return
                if job.state.name in [
                    BatchJobStatus.FAILED.value,
                    BatchJobStatus.EXPIRED.value,
                    BatchJobStatus.CANCELLED.value,
                ]:
                    self.log.error("Job execution was not completed!")
                    yield TriggerEvent(
                        {"status": "error", "message": f"Job {job.name} execution was not completed!"}
                    )
                    return
                self.log.info(
                    "Waiting for job execution, polling interval: %s seconds, current state: %s",
                    self.polling_interval,
                    job.state.name,
                )
                await asyncio.sleep(self.polling_interval)

        except Exception as e:
            self.log.exception("Exception occurred while checking for job completion.")
            yield TriggerEvent({"status": "error", "message": str(e)})


class GenAIGeminiCreateEmbeddingsBatchJobTrigger(BaseTrigger):
    """
    Trigger that creates Gemini Embeddings Batch Job and waiting for execution.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param model: Required. The name of the publisher model to use for Batch job.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
    :param input_source: Required. Source of requests, could be inline requests or file name.
    :param results_folder: Optional. Path to a folder on local machine where file with results will be saved.
    :param create_embeddings_config: Optional. Config for batch job creation.
    :param retrieve_result: Optional. Push the result to XCom. If the input_source is inline, this pushes
        the execution result. If a file name is specified, this pushes the output file path.
    :param polling_interval: Optional. The interval, in seconds, to poll the job status.
    :param gcp_conn_id: The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    def __init__(
        self,
        project_id: str,
        location: str,
        model: str,
        input_source: dict | str,
        gemini_api_key: str,
        create_embeddings_config: CreateBatchJobConfig | dict | None = None,
        results_folder: str | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        retrieve_result: bool = False,
        polling_interval: int = 30,
    ):
        super().__init__()

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.model = model
        self.input_source = input_source
        self.create_embeddings_config = create_embeddings_config
        self.gemini_api_key = gemini_api_key
        self.retrieve_result = retrieve_result
        self.polling_interval = polling_interval
        self.results_folder = results_folder

    def _get_async_hook(self) -> GenAIGeminiAPIAsyncHook:
        return GenAIGeminiAPIAsyncHook(gemini_api_key=self.gemini_api_key)

    def serialize(self) -> tuple[str, dict[str, Any]]:
        """Serialize class arguments and classpath."""
        return (
            "airflow.providers.google.cloud.triggers.gen_ai.GenAIGeminiCreateEmbeddingsBatchJobTrigger",
            {
                "project_id": self.project_id,
                "location": self.location,
                "gcp_conn_id": self.gcp_conn_id,
                "impersonation_chain": self.impersonation_chain,
                "model": self.model,
                "input_source": self.input_source,
                "create_embeddings_config": self.create_embeddings_config,
                "gemini_api_key": self.gemini_api_key,
                "retrieve_result": self.retrieve_result,
                "polling_interval": self.polling_interval,
                "results_folder": self.results_folder,
            },
        )

    async def run(self):
        """
        Loop until the job reaches  successful final or error state.

        Yields a TriggerEvent with success status, if the job reaches successful state.

        Yields a TriggerEvent with error status, if the client returns an unexpected terminal
        job status or any exception is raised while looping.

        In any other case the Trigger will wait for a specified amount of time
        stored in self.polling_interval variable.
        """
        try:
            hook = self._get_async_hook()
            job = await hook.create_embeddings_batch_job(
                model=self.model,
                source=self.input_source,
                create_embeddings_config=self.create_embeddings_config,
            )
            while True:
                job = await hook.get_batch_job(job_name=job.name)
                if job.state.name == BatchJobStatus.SUCCEEDED.value:
                    self.log.info("Job execution completed")
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": "Job completed",
                            "job": job.model_dump(),
                        }
                    )
                    return
                if job.state.name in [
                    BatchJobStatus.FAILED.value,
                    BatchJobStatus.EXPIRED.value,
                    BatchJobStatus.CANCELLED.value,
                ]:
                    self.log.error("Job execution was not completed!")
                    yield TriggerEvent(
                        {"status": "error", "message": f"Job {job.name} execution was not completed!"}
                    )
                    return
                self.log.info(
                    "Waiting for job execution, polling interval: %s seconds, current state: %s",
                    self.polling_interval,
                    job.state.name,
                )
                await asyncio.sleep(self.polling_interval)

        except Exception as e:
            self.log.exception("Exception occurred while checking for job completion.")
            yield TriggerEvent({"status": "error", "message": str(e)})
