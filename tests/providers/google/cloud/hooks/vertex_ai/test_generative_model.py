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
from __future__ import annotations

from unittest import mock

from airflow.providers.google.cloud.hooks.vertex_ai.generative_model import (
    GenerativeModelHook,
)
from tests.providers.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"

TEST_PROMPT = "In 10 words or less, what is apache airflow?"
TEST_LANGUAGE_PRETRAINED_MODEL = "text-bison"
TEST_TEMPERATURE = 0.0
TEST_MAX_OUTPUT_TOKENS = 256
TEST_TOP_P = 0.8
TEST_TOP_K = 40

TEST_MULTIMODAL_PRETRAINED_MODEL = "gemini-pro"
TEST_CHAT = None

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
GENERATIVE_MODEL_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.generative_model.{}"


class TestGenerativeModelWithDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = GenerativeModelHook(gcp_conn_id=TEST_GCP_CONN_ID)

    def test_prompt_language_model(self) -> None:
        self.hook.prompt_language_model(
            prompt=TEST_PROMPT,
            pretrained_model=TEST_LANGUAGE_PRETRAINED_MODEL,
            temperature=TEST_TEMPERATURE,
            max_output_tokens=TEST_MAX_OUTPUT_TOKENS,
            top_p=TEST_TOP_P,
            top_k=TEST_TOP_K,
        )
        self.assert_called_once_with(TEST_GCP_CONN_ID)
        self.return_value.prompt_language_model.assert_called_once_with(
            prompt=TEST_PROMPT,
            pretrained_model=TEST_LANGUAGE_PRETRAINED_MODEL,
            temperature=TEST_TEMPERATURE,
            max_output_tokens=TEST_MAX_OUTPUT_TOKENS,
            top_p=TEST_TOP_P,
            top_k=TEST_TOP_K,
        )

    def test_prompt_multimodal_model(self) -> None:
        self.hook.prompt_multimodal_model(
            prompt=TEST_PROMPT, pretrained_model=TEST_MULTIMODAL_PRETRAINED_MODEL, chat=TEST_CHAT
        )
        self.assert_called_once_with(TEST_GCP_CONN_ID)
        self.return_value.prompt_language_model.assert_called_once_with(
            prompt=TEST_PROMPT, pretrained_model=TEST_MULTIMODAL_PRETRAINED_MODEL, chat=TEST_CHAT
        )
