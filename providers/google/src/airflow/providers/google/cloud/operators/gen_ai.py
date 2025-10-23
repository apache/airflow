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

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.google.cloud.hooks.gen_ai import (
    GenAIGenerativeModelHook,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from google.genai.types import (
        ContentListUnion,
        ContentListUnionDict,
        CountTokensConfigOrDict,
        CreateCachedContentConfigOrDict,
        CreateTuningJobConfigOrDict,
        EmbedContentConfigOrDict,
        GenerateContentConfig,
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
