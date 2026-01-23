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
"""This module contains Google Gen AI operators."""

from __future__ import annotations

import enum
import os.path
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from google.genai.errors import ClientError

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.gen_ai import (
    GenAIGeminiAPIHook,
    GenAIGenerativeModelHook,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from google.genai.types import (
        ContentListUnion,
        ContentListUnionDict,
        CountTokensConfigOrDict,
        CreateBatchJobConfig,
        CreateCachedContentConfigOrDict,
        CreateTuningJobConfigOrDict,
        EmbedContentConfigOrDict,
        GenerateContentConfig,
        ListBatchJobsConfig,
        TuningDatasetOrDict,
    )

    from airflow.providers.common.compat.sdk import Context


class GenAIGenerateEmbeddingsOperator(GoogleCloudBaseOperator):
    """
    Uses the Gemini AI Embeddings API to generate embeddings for words, phrases, sentences, and code.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to (templated).
    :param location: Required. The ID of the Google Cloud location that the
        service belongs to (templated).
    :param model: Required. The name of the model to use for content generation,
        which can be a text-only or multimodal model. For example, `gemini-pro` or
        `gemini-pro-vision`.
    :param contents: Optional. The contents to use for embedding.
    :param config: Optional. Configuration for embeddings.
    :param gcp_conn_id: Optional. The connection ID to use connecting to Google Cloud.
    :param impersonation_chain: Optional. Service account to impersonate using short-term
        credentials, or chained list of accounts required to get the access_token
        of the last account in the list, which will be impersonated in the request.
        If set as a string, the account must grant the originating account
        the Service Account Token Creator IAM role.
        If set as a sequence, the identities from the list must grant
        Service Account Token Creator IAM role to the directly preceding identity, with first
        account from the list granting this role to the originating account (templated).
    """

    template_fields = ("location", "project_id", "impersonation_chain", "contents", "model", "config")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        model: str,
        contents: ContentListUnion | ContentListUnionDict | list[str],
        config: EmbedContentConfigOrDict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.contents = contents
        self.config = config
        self.model = model
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenAIGenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Generating text embeddings...")
        response = self.hook.embed_content(
            project_id=self.project_id,
            location=self.location,
            contents=self.contents,
            model=self.model,
            config=self.config,
        )

        self.log.info("Model response: %s", response)
        context["ti"].xcom_push(key="model_response", value=response)

        return response


class GenAIGenerateContentOperator(GoogleCloudBaseOperator):
    """
    Generate a model response based on given configuration. Input capabilities differ between models, including tuned models.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to (templated).
    :param location: Required. The ID of the Google Cloud location that the
        service belongs to (templated).
    :param model: Required. The name of the model to use for content generation,
        which can be a text-only or multimodal model. For example, `gemini-pro` or
        `gemini-pro-vision`.
    :param contents: Required. The multi-part content of a message that a user or a program
        gives to the generative model, in order to elicit a specific response.
    :param generation_config: Optional. Generation configuration settings.
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

    template_fields = (
        "generation_config",
        "location",
        "project_id",
        "impersonation_chain",
        "contents",
        "model",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        contents: ContentListUnionDict,
        model: str,
        generation_config: GenerateContentConfig | dict[str, Any] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.contents = contents
        self.generation_config = generation_config
        self.model = model
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenAIGenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = self.hook.generate_content(
            project_id=self.project_id,
            location=self.location,
            model=self.model,
            contents=self.contents,
            generation_config=self.generation_config,
        )

        self.log.info("Created Content: %s", response)
        context["ti"].xcom_push(key="model_response", value=response)

        return response


class GenAISupervisedFineTuningTrainOperator(GoogleCloudBaseOperator):
    """
    Create a tuning job to adapt model behavior with a labeled dataset.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param source_model: Required. A pre-trained model optimized for performing natural
        language tasks such as classification, summarization, extraction, content
        creation, and ideation.
    :param training_dataset: Required. Cloud Storage URI of your training dataset. The dataset
        must be formatted as a JSONL file. For best results, provide at least 100 to 500 examples.
    :param tuning_job_config: Optional. Configuration of the Tuning job to be created.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "training_dataset",
        "tuning_job_config",
        "source_model",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        source_model: str,
        training_dataset: TuningDatasetOrDict,
        tuning_job_config: CreateTuningJobConfigOrDict | dict[str, Any] | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.source_model = source_model
        self.training_dataset = training_dataset
        self.tuning_job_config = tuning_job_config
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenAIGenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = self.hook.supervised_fine_tuning_train(
            project_id=self.project_id,
            location=self.location,
            source_model=self.source_model,
            training_dataset=self.training_dataset,
            tuning_job_config=self.tuning_job_config,
        )

        self.log.info("Tuned Model Name: %s", response.tuned_model.model)  # type: ignore[union-attr,arg-type]
        self.log.info("Tuned Model EndpointName: %s", response.tuned_model.endpoint)  # type: ignore[union-attr,arg-type]

        context["ti"].xcom_push(key="tuned_model_name", value=response.tuned_model.model)  # type: ignore[union-attr,arg-type]
        context["ti"].xcom_push(key="tuned_model_endpoint_name", value=response.tuned_model.endpoint)  # type: ignore[union-attr,arg-type]

        result = {
            "tuned_model_name": response.tuned_model.model,  # type: ignore[union-attr,arg-type]
            "tuned_model_endpoint_name": response.tuned_model.endpoint,  # type: ignore[union-attr,arg-type]
        }

        return result


class GenAICountTokensOperator(GoogleCloudBaseOperator):
    """
    Use Count Tokens API to calculate the number of input tokens before sending a request to Gemini API.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to (templated).
    :param location: Required. The ID of the Google Cloud location that the
        service belongs to (templated).
    :param contents: Required. The multi-part content of a message that a user or a program
        gives to the generative model, in order to elicit a specific response.
    :param model: Required. Model, supporting prompts with text-only input,
        including natural language tasks, multi-turn text and code chat,
        and code generation. It can output text and code.
    :param config: Optional. Configuration for Count Tokens.
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

    template_fields = ("location", "project_id", "impersonation_chain", "contents", "model", "config")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        contents: ContentListUnion | ContentListUnionDict,
        model: str,
        config: CountTokensConfigOrDict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.contents = contents
        self.model = model
        self.config = config
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenAIGenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = self.hook.count_tokens(
            project_id=self.project_id,
            location=self.location,
            contents=self.contents,
            model=self.model,
            config=self.config,
        )

        self.log.info("Total tokens: %s", response.total_tokens)
        context["ti"].xcom_push(key="total_tokens", value=response.total_tokens)


class GenAICreateCachedContentOperator(GoogleCloudBaseOperator):
    """
    Create CachedContent resource to reduce the cost of requests that contain repeat content with high input token counts.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param model: Required. The name of the publisher model to use for cached content.
    :param cached_content_config: Optional. Configuration of the Cached Content.
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

    template_fields = ("location", "project_id", "impersonation_chain", "model", "cached_content_config")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        model: str,
        cached_content_config: CreateCachedContentConfigOrDict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.model = model
        self.cached_content_config = cached_content_config
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenAIGenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        cached_content_name = self.hook.create_cached_content(
            project_id=self.project_id,
            location=self.location,
            model=self.model,
            cached_content_config=self.cached_content_config,
        )

        self.log.info("Cached Content Name: %s", cached_content_name)
        context["ti"].xcom_push(key="cached_content", value=cached_content_name)

        return cached_content_name


class BatchJobStatus(enum.Enum):
    """Possible states of batch job in Gemini Batch API."""

    SUCCEEDED = "JOB_STATE_SUCCEEDED"
    PENDING = "JOB_STATE_PENDING"
    FAILED = "JOB_STATE_FAILED"
    RUNNING = "JOB_STATE_RUNNING"
    CANCELLED = "JOB_STATE_CANCELLED"
    EXPIRED = "JOB_STATE_EXPIRED"


class GenAIGeminiCreateBatchJobOperator(GoogleCloudBaseOperator):
    """
    Create Batch job using Gemini Batch API. Use to generate model response for several requests.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param model: Required. The name of the publisher model to use for Batch job.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
    :param input_source: Required. Source of requests, could be inline requests or file name.
    :param results_folder: Optional. Path to a folder on local machine where file with results will be saved.
    :param create_batch_job_config: Optional. Config for batch job creation.
    :param wait_until_complete: Optional. Await job completion.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "model",
        "create_batch_job_config",
        "gemini_api_key",
        "input_source",
    )

    def __init__(
        self,
        *,
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
        wait_until_complete: bool = False,
        polling_interval: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.model = model
        self.input_source = input_source
        self.create_batch_job_config = create_batch_job_config
        self.gemini_api_key = gemini_api_key
        self.retrieve_result = retrieve_result
        self.wait_until_complete = wait_until_complete
        self.polling_interval = polling_interval
        self.results_folder = results_folder

        if self.retrieve_result and not self.wait_until_complete:
            raise AirflowException("Retrieving results is possible only if wait_until_complete set to True")
        if self.results_folder and not isinstance(self.input_source, str):
            raise AirflowException("results_folder works only when input_source is file name")
        if self.results_folder and not os.path.exists(os.path.abspath(self.results_folder)):
            raise AirflowException("path to results_folder does not exist, please provide correct path")

    def _wait_until_complete(self, job, polling_interval: int = 30):
        try:
            while True:
                job = self.hook.get_batch_job(job_name=job.name)
                if job.state.name == BatchJobStatus.SUCCEEDED.value:
                    self.log.info("Job execution completed")
                    break
                if job.state.name in [
                    BatchJobStatus.FAILED.value,
                    BatchJobStatus.EXPIRED.value,
                    BatchJobStatus.CANCELLED.value,
                ]:
                    self.log.error("Job execution was not completed!")
                    break
                self.log.info(
                    "Waiting for job execution, polling interval: %s seconds, current state: %s",
                    self.polling_interval,
                    job.state.name,
                )
                time.sleep(polling_interval)
        except Exception:
            raise AirflowException("Something went wrong during waiting of the batch job.")
        return job

    def _prepare_results_for_xcom(self, job):
        results = []
        if job.dest and job.dest.inlined_responses:
            self.log.info("Results are inline")
            for inline_response in job.dest.inlined_responses:
                if inline_response.response:
                    # Accessing response, structure may vary.
                    try:
                        results.append(inline_response.response.text)
                    except AttributeError:
                        results.append(inline_response.response)
                elif inline_response.error:
                    self.log.warning("Error found in the inline result")
                    results.append(inline_response.error)
        elif job.dest and job.dest.file_name:
            file_content_bytes = self.hook.download_file(file_name=job.dest.file_name)
            file_content = file_content_bytes.decode("utf-8")
            file_name = job.display_name or job.name.replace("/", "-")
            path_to_file = os.path.abspath(f"{self.results_folder}/{file_name}.jsonl")
            with open(path_to_file, "w") as file_with_results:
                file_with_results.writelines(file_content.splitlines(True))
            results = path_to_file

        return results

    def execute(self, context: Context):
        self.hook = GenAIGeminiAPIHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            gemini_api_key=self.gemini_api_key,
        )

        try:
            job = self.hook.create_batch_job(
                model=self.model,
                source=self.input_source,
                create_batch_job_config=self.create_batch_job_config,
            )
        except Exception as e:
            raise AirflowException("Something went wrong during creation of the batch job: %s", e)

        self.log.info("Job with name %s was successfully created!", job.name)
        context["ti"].xcom_push(key="job_name", value=job.name)

        if self.wait_until_complete:
            job = self._wait_until_complete(job, self.polling_interval)
            if self.retrieve_result and job.error is None:
                job_results = self._prepare_results_for_xcom(job)
                context["ti"].xcom_push(key="job_results", value=job_results)

        return dict(job)


class GenAIGeminiGetBatchJobOperator(GoogleCloudBaseOperator):
    """
    Get Batch job using Gemini API.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param model: Required. The name of the publisher model to use for Batch job.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
    :param job_name: Required. Name of the batch job.
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

    template_fields = ("location", "project_id", "impersonation_chain", "job_name", "gemini_api_key")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        job_name: str,
        gemini_api_key: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.job_name = job_name
        self.gemini_api_key = gemini_api_key

    def execute(self, context: Context):
        self.hook = GenAIGeminiAPIHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            gemini_api_key=self.gemini_api_key,
        )

        try:
            job = self.hook.get_batch_job(job_name=self.job_name)
        except ValueError:
            raise AirflowException("Job with name %s not found", self.job_name)

        context["ti"].xcom_push(key="job_status", value=job.state)
        return dict(job)


class GenAIGeminiListBatchJobsOperator(GoogleCloudBaseOperator):
    """
    Get list of Batch jobs metadata using Gemini API.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param model: Required. The name of the publisher model to use for Batch job.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "list_batch_jobs_config",
        "gemini_api_key",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        gemini_api_key: str,
        list_batch_jobs_config: ListBatchJobsConfig | dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.list_batch_jobs_config = list_batch_jobs_config
        self.gemini_api_key = gemini_api_key

    def execute(self, context: Context):
        self.hook = GenAIGeminiAPIHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            gemini_api_key=self.gemini_api_key,
        )

        jobs_list = self.hook.list_batch_jobs(list_batch_jobs_config=self.list_batch_jobs_config)

        job_names = []
        job_objs = []

        try:
            for job in jobs_list:
                job_names.append(job.name)
                job_objs.append(job.model_dump(exclude={"dest"}))
        except RuntimeError:
            self.log.info("%s jobs found", len(job_names))

        context["ti"].xcom_push(key="job_names", value=job_names)

        return job_objs


class GenAIGeminiDeleteBatchJobOperator(GoogleCloudBaseOperator):
    """
    Queue a batch job for deletion using the Gemini API.

    The job will not be deleted immediately. After submitting it for deletion, it will still be available
    through GenAIGeminiListBatchJobsOperator or GenAIGeminiGetBatchJobOperator for some time.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param model: Required. The name of the publisher model to use for Batch job.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
    :param job_name: Required. Name of the batch job.
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

    template_fields = ("location", "project_id", "impersonation_chain", "job_name", "gemini_api_key")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        job_name: str,
        gemini_api_key: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.job_name = job_name
        self.gemini_api_key = gemini_api_key

    def execute(self, context: Context):
        self.hook = GenAIGeminiAPIHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            gemini_api_key=self.gemini_api_key,
        )

        try:
            delete_response = self.hook.delete_batch_job(job_name=self.job_name)
        except ValueError:
            raise AirflowException("Job with name %s was not found", self.job_name)

        self.log.info("Job with name %s was submitted for deletion.", self.job_name)

        if delete_response.error:
            raise AirflowException(
                "Job with name %s was not deleted due to error: %s", self.job_name, delete_response.error
            )

        return delete_response.model_dump()


class GenAIGeminiCancelBatchJobOperator(GoogleCloudBaseOperator):
    """
    Cancel Batch job using Gemini API.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param model: Required. The name of the publisher model to use for Batch job.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
    :param job_name: Required. Name of the batch job.
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

    template_fields = ("location", "project_id", "impersonation_chain", "job_name", "gemini_api_key")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        job_name: str,
        gemini_api_key: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.job_name = job_name
        self.gemini_api_key = gemini_api_key

    def execute(self, context: Context):
        self.hook = GenAIGeminiAPIHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            gemini_api_key=self.gemini_api_key,
        )
        self.log.info("Cancelling job with name %s ...", self.job_name)

        try:
            self.hook.cancel_batch_job(job_name=self.job_name)
        except ValueError:
            raise AirflowException("Job with name %s was not found", self.job_name)

        self.log.info("Job with name %s was successfully cancelled", self.job_name)


class GenAIGeminiCreateEmbeddingsBatchJobOperator(GoogleCloudBaseOperator):
    """
    Create embeddings Batch job using Gemini Batch API.

    Use to generate embeddings for words, phrases, sentences, and code for several requests.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param model: Required. The name of the publisher model to use for Batch job.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
    :param input_source: Required. Source of requests, could be inline requests or file name.
    :param results_folder: Optional. Path to a folder on local machine where file with results will be saved.
    :param create_embeddings_config: Optional. Config for batch job creation.
    :param wait_until_complete: Optional. Await job completion.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "model",
        "create_embeddings_config",
        "gemini_api_key",
        "input_source",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        model: str,
        gemini_api_key: str,
        input_source: dict | str,
        results_folder: str | None = None,
        create_embeddings_config: CreateBatchJobConfig | dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        wait_until_complete: bool = False,
        retrieve_result: bool = False,
        polling_interval: int = 30,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.model = model
        self.input_source = input_source
        self.create_embeddings_config = create_embeddings_config
        self.gemini_api_key = gemini_api_key
        self.wait_until_complete = wait_until_complete
        self.retrieve_result = retrieve_result
        self.polling_interval = polling_interval
        self.results_folder = results_folder

        if self.retrieve_result and not self.wait_until_complete:
            raise AirflowException("Retrieving results is possible only if wait_until_complete set to True")
        if self.results_folder and not isinstance(self.input_source, str):
            raise AirflowException("results_folder works only when input_source is file name")
        if self.results_folder and not os.path.exists(os.path.abspath(self.results_folder)):
            raise AirflowException("path to results_folder does not exist, please provide correct path")

    def _wait_until_complete(self, job, polling_interval: int = 30):
        try:
            while True:
                job = self.hook.get_batch_job(job_name=job.name)
                if job.state.name == BatchJobStatus.SUCCEEDED.value:
                    self.log.info("Job execution completed")
                    break
                if job.state.name in [
                    BatchJobStatus.FAILED.value,
                    BatchJobStatus.EXPIRED.value,
                    BatchJobStatus.CANCELLED.value,
                ]:
                    self.log.error("Job execution was not completed!")
                    break
                self.log.info(
                    "Waiting for job execution, polling interval: %s seconds, current state: %s",
                    self.polling_interval,
                    job.state.name,
                )
                time.sleep(polling_interval)
        except Exception as e:
            raise AirflowException("Something went wrong during waiting of the batch job: %s", e)
        return job

    def _prepare_results_for_xcom(self, job):
        results = []
        if job.dest and job.dest.inlined_embed_content_responses:
            self.log.info("Results are inline")
            for inline_embed_response in job.dest.inlined_embed_content_responses:
                if inline_embed_response.response:
                    # Accessing response, structure may vary.
                    try:
                        results.append(dict(inline_embed_response.response.embedding))
                    except AttributeError:
                        results.append(inline_embed_response.response)
                elif inline_embed_response.error:
                    self.log.warning("Error found in the inline result")
                    results.append(inline_embed_response.error)
        elif job.dest and job.dest.file_name:
            file_content_bytes = self.hook.download_file(file_name=job.dest.file_name)
            file_content = file_content_bytes.decode("utf-8")
            file_name = job.display_name or job.name.replace("/", "-")
            path_to_file = os.path.abspath(f"{self.results_folder}/{file_name}.jsonl")
            with open(path_to_file, "w") as file_with_results:
                file_with_results.writelines(file_content.splitlines(True))
            results = path_to_file

        return results

    def execute(self, context: Context):
        self.hook = GenAIGeminiAPIHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            gemini_api_key=self.gemini_api_key,
        )

        try:
            embeddings_job = self.hook.create_embeddings(
                model=self.model,
                source=self.input_source,
                create_embeddings_config=self.create_embeddings_config,
            )
        except Exception:
            raise AirflowException("Something went wrong during creation of the embeddings job.")

        self.log.info("Embeddings Job with name %s was successfully created!", embeddings_job.name)
        context["ti"].xcom_push(key="job_name", value=embeddings_job.name)

        if self.wait_until_complete:
            embeddings_job = self._wait_until_complete(embeddings_job, self.polling_interval)
            if self.retrieve_result and embeddings_job.error is None:
                job_results = self._prepare_results_for_xcom(embeddings_job)
                context["ti"].xcom_push(key="job_results", value=job_results)

        return embeddings_job.model_dump()


class GenAIGeminiUploadFileOperator(GoogleCloudBaseOperator):
    """
    Get file uploaded to Gemini Files API.

    The Files API lets you store up to 20GB of files per project, with each file not exceeding 2GB in size.
    Supported types are audio files, images, videos, documents, and others. Files are stored for 48 hours.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
    :param file_path: Required. Path to file on your local machine.
    :param upload_file_config: Optional. Metadata configuration for file upload.
        Defaults to display name and mime type parsed from file_path.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "file_path",
        "gemini_api_key",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        file_path: str,
        gemini_api_key: str,
        upload_file_config: dict | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.gemini_api_key = gemini_api_key
        self.file_path = file_path
        self.upload_file_config = upload_file_config

    def execute(self, context: Context):
        self.hook = GenAIGeminiAPIHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            gemini_api_key=self.gemini_api_key,
        )

        try:
            file = self.hook.upload_file(
                path_to_file=self.file_path, upload_file_config=self.upload_file_config
            )
        except RuntimeError as exc:
            raise exc
        except ValueError:
            raise AirflowException("Error during file upload! Check file name or mime type!")
        except FileNotFoundError:
            raise AirflowException("Provided file was not found!")

        self.log.info("File with name %s successfully uploaded!", file.name)
        context["ti"].xcom_push(key="file_name", value=file.name)

        return file.model_dump()


class GenAIGeminiGetFileOperator(GoogleCloudBaseOperator):
    """
    Get file's metadata uploaded to Gemini Files API by using GenAIGeminiUploadFileOperator.

    The Files API lets you store up to 20GB of files per project, with each file not exceeding 2GB in size.
    Files are stored for 48 hours.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
    :param file_name: Required. File name in Gemini Files API to get
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "file_name",
        "gemini_api_key",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        file_name: str,
        gemini_api_key: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.gemini_api_key = gemini_api_key
        self.file_name = file_name

    def execute(self, context: Context):
        self.hook = GenAIGeminiAPIHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            gemini_api_key=self.gemini_api_key,
        )
        self.log.info("Looking for file with name: %s", self.file_name)

        try:
            file = self.hook.get_file(file_name=self.file_name)
        except ClientError:
            raise AirflowException("File with name %s not found", self.file_name)

        self.log.info("Find file with name: %s", file.name)
        context["ti"].xcom_push(key="file_uri", value=file.uri)

        return file.model_dump()


class GenAIGeminiListFilesOperator(GoogleCloudBaseOperator):
    """
    List files uploaded to Gemini Files API.

    The Files API lets you store up to 20GB of files per project, with each file not exceeding 2GB in size.
    Files are stored for 48 hours.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "gemini_api_key",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        gemini_api_key: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.gemini_api_key = gemini_api_key

    def execute(self, context: Context):
        self.hook = GenAIGeminiAPIHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            gemini_api_key=self.gemini_api_key,
        )

        files = self.hook.list_files()

        if files:
            xcom_file_names = []
            xcom_files = []
            try:
                for file in files:
                    xcom_file_names.append(file.name)
                    xcom_files.append(file.model_dump())
            except RuntimeError:
                self.log.info("%s files found", len(xcom_files))

            context["ti"].xcom_push(key="file_names", value=xcom_file_names)
            return xcom_files

        self.log.info("No files found")


class GenAIGeminiDeleteFileOperator(GoogleCloudBaseOperator):
    """
    Delete file uploaded to Gemini Files API.

    The Files API lets you store up to 20GB of files per project, with each file not exceeding 2GB in size.
    Files are stored for 48 hours.

    :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
    :param location: Required. The ID of the Google Cloud location that the service belongs to.
    :param gemini_api_key: Required. Key to interact with Gemini Batch API.
    :param file_name: Required. File name in Gemini Files API to delete.
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

    template_fields = (
        "location",
        "project_id",
        "impersonation_chain",
        "file_name",
        "gemini_api_key",
    )

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        file_name: str,
        gemini_api_key: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        self.project_id = project_id
        self.location = location
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain
        self.file_name = file_name
        self.gemini_api_key = gemini_api_key

    def execute(self, context: Context):
        self.hook = GenAIGeminiAPIHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
            gemini_api_key=self.gemini_api_key,
        )

        try:
            delete_response = self.hook.delete_file(file_name=self.file_name)
        except ClientError:
            raise AirflowException("File %s not found!", self.file_name)

        self.log.info("File %s was successfully deleted!", self.file_name)

        return delete_response.model_dump()
