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
"""This module contains a Google Cloud GenAI Generative Model hook."""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any

from google import genai

from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID, GoogleBaseHook

if TYPE_CHECKING:
    from google.genai.pagers import Pager
    from google.genai.types import (
        BatchJob,
        ContentListUnion,
        ContentListUnionDict,
        CountTokensConfigOrDict,
        CountTokensResponse,
        CreateBatchJobConfig,
        CreateCachedContentConfigOrDict,
        CreateTuningJobConfigOrDict,
        DeleteFileResponse,
        DeleteResourceJob,
        EmbedContentConfigOrDict,
        EmbedContentResponse,
        File,
        GenerateContentConfig,
        ListBatchJobsConfig,
        TuningDatasetOrDict,
        TuningJob,
    )


class GenAIGenerativeModelHook(GoogleBaseHook):
    """Class for Google Cloud Generative AI Vertex AI hook."""

    def get_genai_client(self, project_id: str, location: str):
        return genai.Client(
            vertexai=True,
            project=project_id,
            location=location,
        )

    @GoogleBaseHook.fallback_to_default_project_id
    def embed_content(
        self,
        model: str,
        location: str,
        contents: ContentListUnion | ContentListUnionDict | list[str],
        config: EmbedContentConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> EmbedContentResponse:
        """
        Generate embeddings for words, phrases, sentences, and code.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param model: Required. The model to use.
        :param contents: Optional. The contents to use for embedding.
        :param config: Optional. Configuration for embeddings.
        """
        client = self.get_genai_client(project_id=project_id, location=location)

        resp = client.models.embed_content(model=model, contents=contents, config=config)
        return resp

    @GoogleBaseHook.fallback_to_default_project_id
    def generate_content(
        self,
        location: str,
        model: str,
        contents: ContentListUnionDict,
        generation_config: GenerateContentConfig | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Make an API request to generate content using a model.

        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param model: Required. The model to use.
        :param contents: Required. The multi-part content of a message that a user or a program
            gives to the generative model, in order to elicit a specific response.
        :param generation_config: Optional. Generation configuration settings.
        """
        client = self.get_genai_client(project_id=project_id, location=location)
        response = client.models.generate_content(
            model=model,
            contents=contents,
            config=generation_config,
        )

        return response.text

    @GoogleBaseHook.fallback_to_default_project_id
    def supervised_fine_tuning_train(
        self,
        source_model: str,
        location: str,
        training_dataset: TuningDatasetOrDict,
        tuning_job_config: CreateTuningJobConfigOrDict | dict[str, Any] | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> TuningJob:
        """
        Create a tuning job to adapt model behavior with a labeled dataset.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param source_model: Required. A pre-trained model optimized for performing natural
            language tasks such as classification, summarization, extraction, content
            creation, and ideation.
        :param train_dataset: Required. Cloud Storage URI of your training dataset. The dataset
            must be formatted as a JSONL file. For best results, provide at least 100 to 500 examples.
        :param tuning_job_config: Optional. Configuration of the Tuning job to be created.
        """
        client = self.get_genai_client(project_id=project_id, location=location)

        tuning_job = client.tunings.tune(
            base_model=source_model,
            training_dataset=training_dataset,
            config=tuning_job_config,
        )

        # Poll until completion
        running = {"JOB_STATE_PENDING", "JOB_STATE_RUNNING"}
        while tuning_job.state in running:
            time.sleep(60)
            tuning_job = client.tunings.get(name=tuning_job.name)

        return tuning_job

    @GoogleBaseHook.fallback_to_default_project_id
    def count_tokens(
        self,
        location: str,
        model: str,
        contents: ContentListUnion | ContentListUnionDict,
        config: CountTokensConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> CountTokensResponse:
        """
        Use Count Tokens API to calculate the number of input tokens before sending a request to Gemini API.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param contents: Required. The multi-part content of a message that a user or a program
            gives to the generative model, in order to elicit a specific response.
        :param model: Required. Model,
            supporting prompts with text-only input, including natural language
            tasks, multi-turn text and code chat, and code generation. It can
            output text and code.
        :param config: Optional. Configuration for Count Tokens.
        """
        client = self.get_genai_client(project_id=project_id, location=location)
        response = client.models.count_tokens(
            model=model,
            contents=contents,
            config=config,
        )

        return response

    @GoogleBaseHook.fallback_to_default_project_id
    def create_cached_content(
        self,
        model: str,
        location: str,
        cached_content_config: CreateCachedContentConfigOrDict | None = None,
        project_id: str = PROVIDE_PROJECT_ID,
    ) -> str:
        """
        Create CachedContent to reduce the cost of requests containing repeat content.

        :param project_id: Required. The ID of the Google Cloud project that the service belongs to.
        :param location: Required. The ID of the Google Cloud location that the service belongs to.
        :param model: Required. The name of the publisher model to use for cached content.
        :param cached_content_config: Optional. Configuration of the Cached Content.
        """
        client = self.get_genai_client(project_id=project_id, location=location)
        resp = client.caches.create(
            model=model,
            config=cached_content_config,
        )

        return resp.name


class GenAIGeminiAPIHook(GoogleBaseHook):
    """Class for Google Cloud Generative AI Gemini Developer API hook."""

    def __init__(self, gemini_api_key: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.gemini_api_key = gemini_api_key

    def get_genai_client(self):
        return genai.Client(
            api_key=self.gemini_api_key,
            vertexai=False,
        )

    def get_batch_job(
        self,
        job_name: str,
    ) -> BatchJob:
        """
        Get batch job using Gemini Batch API.

        :param job_name: Required. Batch job name.
        """
        client = self.get_genai_client()
        resp = client.batches.get(name=job_name)
        return resp

    def list_batch_jobs(
        self,
        list_batch_jobs_config: ListBatchJobsConfig | dict | None = None,
    ) -> Pager[BatchJob]:
        """
        Get list of batch jobs using Gemini Batch API.

        :param list_batch_jobs_config: Optional. Configuration of returned iterator.
        """
        client = self.get_genai_client()
        resp = client.batches.list(
            config=list_batch_jobs_config,
        )
        return resp

    def create_batch_job(
        self,
        model: str,
        source: list | str,
        create_batch_job_config: CreateBatchJobConfig | dict | None = None,
    ) -> BatchJob:
        """
        Create batch job using Gemini Batch API to process large-scale, non-urgent tasks.

        :param model: Required. Gemini model name to process requests.
        :param source: Required. Requests that will be sent to chosen model.
            Can be in format of Inline requests or file name.
        :param create_batch_job_config: Optional. Configuration parameters for batch job.
        """
        client = self.get_genai_client()
        resp = client.batches.create(
            model=model,
            src=source,
            config=create_batch_job_config,
        )
        return resp

    def delete_batch_job(
        self,
        job_name: str,
    ) -> DeleteResourceJob:
        """
        Delete batch job using Gemini Batch API.

        :param job_name: Required. Batch job name.
        """
        client = self.get_genai_client()
        resp = client.batches.delete(name=job_name)
        return resp

    def cancel_batch_job(
        self,
        job_name: str,
    ) -> None:
        """
        Cancel batch job using Gemini Batch API.

        :param job_name: Required. Batch job name.
        """
        client = self.get_genai_client()
        client.batches.cancel(
            name=job_name,
        )

    def create_embeddings(
        self,
        model: str,
        source: dict | str,
        create_embeddings_config: CreateBatchJobConfig | dict | None = None,
    ) -> BatchJob:
        """
        Create batch job for embeddings using Gemini Batch API to process large-scale, non-urgent tasks.

        :param model: Required. Gemini model name to process requests.
        :param source: Required. Requests that will be sent to chosen model.
            Can be in format of Inline requests or file name.
        :param create_embeddings_config: Optional. Configuration parameters for embeddings batch job.
        """
        client = self.get_genai_client()
        input_type = "inlined_requests"

        if isinstance(source, str):
            input_type = "file_name"

        self.log.info("Using %s to create embeddings", input_type)

        resp = client.batches.create_embeddings(
            model=model,
            src={input_type: source},
            config=create_embeddings_config,
        )
        return resp

    def upload_file(self, path_to_file: str, upload_file_config: dict | None = None) -> File:
        """
        Upload file for batch job or embeddings batch job using Gemini Files API.

        :param path_to_file: Required. Path to file on local filesystem.
        :param upload_file_config: Optional. Configuration for file upload.
        """
        client = self.get_genai_client()

        if upload_file_config is None:
            self.log.info("Default configuration will be used to upload file")
            try:
                file_name, file_type = path_to_file.split("/")[-1].split(".")
                upload_file_config = {"display_name": file_name, "mime_type": file_type}
            except ValueError as exc:
                raise ValueError(
                    "Error during unpacking file name or mime type. Please check file path"
                ) from exc

        resp = client.files.upload(
            file=path_to_file,
            config=upload_file_config,
        )
        return resp

    def get_file(self, file_name: str) -> File:
        """
        Get file's metadata for batch job or embeddings batch job using Gemini Files API.

        :param file_name: Required. Name of the file in Gemini Files API.
        """
        client = self.get_genai_client()
        resp = client.files.get(name=file_name)
        return resp

    def download_file(self, file_name: str) -> bytes:
        """
        Download file for batch job or embeddings batch job using Gemini Files API.

        :param file_name: Required. Name of the file in Gemini Files API.
        """
        client = self.get_genai_client()
        resp = client.files.download(file=file_name)
        return resp

    def list_files(self) -> Pager[File]:
        """List files for stored in Gemini Files API."""
        client = self.get_genai_client()
        resp = client.files.list()
        return resp

    def delete_file(self, file_name: str) -> DeleteFileResponse:
        """
        Delete file from Gemini Files API storage.

        :param file_name: Required. Name of the file in Gemini Files API.
        """
        client = self.get_genai_client()
        resp = client.files.delete(name=file_name)
        return resp
