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

from airflow.providers.google.cloud.hooks.vertex_ai.generative_model import (
    LanguageModelHook,
    MultimodalModelHook,
)
from airflow.providers.google.cloud.operators.cloud_base import GoogleCloudBaseOperator

if TYPE_CHECKING:
    from vertexai.preview.generative_models import ChatSession
    from airflow.utils.context import Context


class LanguageModelGenerateTextOperator(GoogleCloudBaseOperator):
    """
    Uses the Vertex AI PaLM API to generate natural language text.

    :param prompt: Required. Inputs or queries that a user or a program gives
        to the Vertex AI PaLM API, in order to elicit a specific response.
    :param pretrained_model: By default uses the pretrained_model text-bison,
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

    def __init__(
        self,
        *,
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
        self.prompt = prompt
        self.pretrained_model = pretrained_model
        self.temperature = temperature
        self.max_output_tokens = max_output_tokens
        self.top_p = top_p
        self.top_k = top_k
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = LanguageModelHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )

        self.log.info("Submitting prompt")
        response = self.hook.generate_text(
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


class MultimodalModelChatOperator(GoogleCloudBaseOperator):
    """
    Use a Vertex AI Multimodal model to generate natural language text.

    :param prompt: Required. Inputs or queries that a user or a program gives
        to the Multimodal model, in order to elicit a specific response.
    :param pretrained_model: By default uses the pretrained_model gemini-pro,
        supporting prompts with text-only input, including natural language
        tasks, multi-turn text and code chat, and code generation. It can
        output text and code.
    :param chat: ChatSession object that holds history of prompts and outputs.
        Used to interact with responses. Defaults to None, which indicates a
        one-off prompt and response.
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
        *,
        prompt: str,
        pretrained_model: str = "gemini-pro",
        chat: ChatSession | None = None,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.prompt = prompt
        self.chat = chat
        self.pretrained_model = pretrained_model
        self.gcp_conn_id = gcp_conn_id
        self.impersonation_chain = impersonation_chain

    def execute(self, context: Context):
        self.hook = MultimodalModelHook(
            gcp_conn_id=self.gcp_conn_id, impersonation_chain=self.impersonation_chain
        )
        response = self.hook.chat_prompt_request(
            prompt=self.prompt, chat=self.chat, pretrained_model=self.pretrained_model
        )

        self.log.info("Model response: %s", response)
        self.xcom_push(context, key="prompt_response", value=response)

        return response
