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
"""This module contains Google Vertex AI Generative AI operators."""

from __future__ import annotations

from typing import TYPE_CHECKING, Sequence

from airflow.providers.google.cloud.hooks.vertex_ai.generative_model import GenerativeModelHook
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class PromptLanguageModelOperator(GoogleCloudBaseOperator):
    """
    Uses the Vertex AI PaLM API to generate natural language text.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to (templated).
    :param location: Required. The ID of the Google Cloud location that the
        service belongs to (templated).
    :param prompt: Required. Inputs or queries that a user or a program gives
        to the Vertex AI PaLM API, in order to elicit a specific response (templated).
    :param pretrained_model: By default uses the pre-trained model `text-bison`,
        optimized for performing natural language tasks such as classification,
        summarization, extraction, content creation, and ideation.
    :param temperature: Temperature controls the degree of randomness in token
        selection. Defaults to 0.0.
    :param max_output_tokens: Token limit determines the maximum amount of text
        output. Defaults to 256.
    :param top_p: Tokens are selected from most probable to least until the sum
        of their probabilities equals the top_p value. Defaults to 0.8.
    :param top_k: A top_k of 1 means the selected token is the most probable
        among all tokens. Defaults to 0.4.
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

    template_fields = ("location", "project_id", "impersonation_chain", "prompt")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        prompt: str,
        pretrained_model: str = "text-bison",
        temperature: float = 0.0,
        max_output_tokens: int = 256,
        top_p: float = 0.8,
        top_k: int = 40,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.prompt = prompt
        self.pretrained_model = pretrained_model
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens
        self.top_p = top_p
        self.top_k = top_k
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Submitting prompt")
        response = self.hook.prompt_language_model(
            project_id=self.project_id,
            location=self.location,
            prompt=self.prompt,
            pretrained_model=self.pretrained_model,
            temperature=self.temperature,
            max_output_tokens=self.max_output_tokens,
            top_p=self.top_p,
            top_k=self.top_k,
        )

        self.log.info("Model response: %s", response)
        self.xcom_push(context, key="prompt_response", value=response)

        return response


class GenerateTextEmbeddingsOperator(GoogleCloudBaseOperator):
    """
    Uses the Vertex AI PaLM API to generate natural language text.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to (templated).
    :param location: Required. The ID of the Google Cloud location that the
        service belongs to (templated).
    :param prompt: Required. Inputs or queries that a user or a program gives
        to the Vertex AI PaLM API, in order to elicit a specific response (templated).
    :param pretrained_model: By default uses the pre-trained model `textembedding-gecko`,
        optimized for performing text embeddings.
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

    template_fields = ("location", "project_id", "impersonation_chain", "prompt")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        prompt: str,
        pretrained_model: str = "textembedding-gecko",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.prompt = prompt
        self.pretrained_model = pretrained_model
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )

        self.log.info("Generating text embeddings")
        response = self.hook.generate_text_embeddings(
            project_id=self.project_id,
            location=self.location,
            prompt=self.prompt,
            pretrained_model=self.pretrained_model,
        )

        self.log.info("Model response: %s", response)
        self.xcom_push(context, key="prompt_response", value=response)

        return response


class PromptMultimodalModelOperator(GoogleCloudBaseOperator):
    """
    Use the Vertex AI Gemini Pro foundation model to generate natural language text.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to (templated).
    :param location: Required. The ID of the Google Cloud location that the
        service belongs to (templated).
    :param prompt: Required. Inputs or queries that a user or a program gives
        to the Multi-modal model, in order to elicit a specific response (templated).
    :param pretrained_model: By default uses the pre-trained model `gemini-pro`,
        supporting prompts with text-only input, including natural language
        tasks, multi-turn text and code chat, and code generation. It can
        output text and code.
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

    template_fields = ("location", "project_id", "impersonation_chain", "prompt")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        prompt: str,
        pretrained_model: str = "gemini-pro",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.prompt = prompt
        self.pretrained_model = pretrained_model
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = self.hook.prompt_multimodal_model(
            project_id=self.project_id,
            location=self.location,
            prompt=self.prompt,
            pretrained_model=self.pretrained_model,
        )

        self.log.info("Model response: %s", response)
        self.xcom_push(context, key="prompt_response", value=response)

        return response


class PromptMultimodalModelWithMediaOperator(GoogleCloudBaseOperator):
    """
    Use the Vertex AI Gemini Pro foundation model to generate natural language text.

    :param project_id: Required. The ID of the Google Cloud project that the
        service belongs to (templated).
    :param location: Required. The ID of the Google Cloud location that the
        service belongs to (templated).
    :param prompt: Required. Inputs or queries that a user or a program gives
        to the Multi-modal model, in order to elicit a specific response (templated).
    :param pretrained_model: By default uses the pre-trained model `gemini-pro-vision`,
        supporting prompts with text-only input, including natural language
        tasks, multi-turn text and code chat, and code generation. It can
        output text and code.
    :param media_gcs_path: A GCS path to a media file such as an image or a video.
        Can be passed to the multi-modal model as part of the prompt. Used with vision models.
    :param mime_type: Validates the media type presented by the file in the media_gcs_path.
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

    template_fields = ("location", "project_id", "impersonation_chain", "prompt")

    def __init__(
        self,
        *,
        project_id: str,
        location: str,
        prompt: str,
        media_gcs_path: str,
        mime_type: str,
        pretrained_model: str = "gemini-pro-vision",
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.project_id = project_id
        self.location = location
        self.prompt = prompt
        self.pretrained_model = pretrained_model
        self.media_gcs_path = media_gcs_path
        self.mime_type = mime_type
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = GenerativeModelHook(
            gcp_conn_id=self.gcp_conn_id,
            impersonation_chain=self.impersonation_chain,
        )
        response = self.hook.prompt_multimodal_model_with_media(
            project_id=self.project_id,
            location=self.location,
            prompt=self.prompt,
            pretrained_model=self.pretrained_model,
            media_gcs_path=self.media_gcs_path,
            mime_type=self.mime_type,
        )

        self.log.info("Model response: %s", response)
        self.xcom_push(context, key="prompt_response", value=response)

        return response
