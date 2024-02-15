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
"""This module contains a Google Cloud Vertex AI Generative Model hook."""

from __future__ import annotations

from typing import Sequence

import vertexai
from vertexai.language_models import TextGenerationModel
from vertexai.preview.generative_models import ChatSession, GenerativeModel

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook


class GenerativeModelHook(GoogleBaseHook):
    """Hook for Google Cloud Vertex AI Generative Model APIs."""

    def __init__(
        self,
        project_id: str,
        location: str,
        gcp_conn_id: str = "google_cloud_default",
        impersonation_chain: str | Sequence[str] | None = None,
        **kwargs,
    ):
        if kwargs.get("delegate_to") is not None:
            raise RuntimeError(
                "The `delegate_to` parameter has been deprecated before and finally removed in this version"
                " of Google Provider. You MUST convert it to `impersonate_chain`"
            )
        super().__init__(gcp_conn_id=gcp_conn_id, impersonation_chain=impersonation_chain, **kwargs)
        vertexai.init(project=project_id, location=location, credentials=self.get_credentials())

    def get_text_generation_model(self, pretrained_model: str):
        """
        Return a Model Garden Model object based on Text Generation.
        """
        model = TextGenerationModel.from_pretrained(pretrained_model)
        return model

    def get_generative_model(self, pretrained_model: str) -> GenerativeModel:
        """
        Return a Generative Model object.
        """
        model = GenerativeModel(pretrained_model)
        return model

    def prompt_language_model(
        self,
        prompt: str,
        pretrained_model: str,
        temperature: float,
        max_output_tokens: int,
        top_p: float,
        top_k: int,
    ) -> str:
        """
        Use the Vertex AI PaLM API to generate natural language text.

        :param prompt: Required. Inputs or queries that a user or a program gives
            to the Vertex AI PaLM API, in order to elicit a specific response.
        :param pretrained_model: A pretrained_model optimized for performing natural
            language tasks such as classification, summarization, extraction, content
            creation, and ideation.
        :param temperature: Temperature controls the degree of randomness in token
            selection.
        :param max_output_tokens: Token limit determines the maximum amount of text
            output.
        :param top_p: Tokens are selected from most probable to least until the sum
            of their probabilities equals the top_p value. Defaults to 0.8.
        :param top_k: A top_k of 1 means the selected token is the most probable
            among all tokens.
        """
        parameters = {
            "temperature": temperature,
            "max_output_tokens": max_output_tokens,
            "top_p": top_p,
            "top_k": top_k,
        }

        model = self.get_text_generation_model(pretrained_model)

        response = model.predict(
            prompt=prompt,
            **parameters,
        )
        return response.text

    def prompt_multimodal_model(
        self, prompt: str, chat: ChatSession | None = None, pretrained_model: str = "gemini-pro"
    ) -> str:
        """
        Use the Vertex AI Gemini Pro foundation model to generate natural language text.

        :param prompt: Required. Inputs or queries that a user or a program gives
            to the Vertex AI PaLM API, in order to elicit a specific response.
        :param pretrained_model: By default uses the pretrained_model gemini-pro,
            supporting prompts with text-only input, including natural language
            tasks, multi-turn text and code chat, and code generation. It can
            output text and code.
        :param chat: ChatSession object that holds history of prompts and outputs.
            Used to interact with responses. Defaults to None, which indicates a
            one-off prompt and response.
        """
        model = self.get_generative_model(pretrained_model)

        if chat is None:  # signals one-off chat request
            chat = model.start_chat()

        response = chat.send_message(prompt)
        return response.text
