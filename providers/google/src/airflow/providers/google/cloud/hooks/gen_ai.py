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
    from google.genai.types import (
        ContentListUnion,
        ContentListUnionDict,
        CountTokensConfigOrDict,
        CountTokensResponse,
        CreateCachedContentConfigOrDict,
        CreateTuningJobConfigOrDict,
        EmbedContentConfigOrDict,
        EmbedContentResponse,
        GenerateContentConfig,
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
