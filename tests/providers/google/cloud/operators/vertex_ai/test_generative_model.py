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

import pytest

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")
vertexai = pytest.importorskip("vertexai.generative_models")
from vertexai.generative_models import HarmBlockThreshold, HarmCategory

from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    GenerateTextEmbeddingsOperator,
    PromptLanguageModelOperator,
    PromptMultimodalModelOperator,
    PromptMultimodalModelWithMediaOperator,
)

VERTEX_AI_PATH = "airflow.providers.google.cloud.operators.vertex_ai.{}"

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestVertexAIPromptLanguageModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        prompt = "In 10 words or less, what is Apache Airflow?"
        pretrained_model = "text-bison"
        temperature = 0.0
        max_output_tokens = 256
        top_p = 0.8
        top_k = 40

        op = PromptLanguageModelOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
            top_p=top_p,
            top_k=top_k,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.prompt_language_model.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
            top_p=top_p,
            top_k=top_k,
        )


class TestVertexAIGenerateTextEmbeddingsOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        prompt = "In 10 words or less, what is Apache Airflow?"
        pretrained_model = "textembedding-gecko"

        op = GenerateTextEmbeddingsOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.generate_text_embeddings.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
        )


class TestVertexAIPromptMultimodalModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        prompt = "In 10 words or less, what is Apache Airflow?"
        pretrained_model = "gemini-pro"
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        }
        generation_config = {"max_output_tokens": 256, "top_p": 0.8, "temperature": 0.0}

        op = PromptMultimodalModelOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            generation_config=generation_config,
            safety_settings=safety_settings,
            pretrained_model=pretrained_model,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.prompt_multimodal_model.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            generation_config=generation_config,
            safety_settings=safety_settings,
            pretrained_model=pretrained_model,
        )


class TestVertexAIPromptMultimodalModelWithMediaOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        pretrained_model = "gemini-pro-vision"
        vision_prompt = "In 10 words or less, describe this content."
        media_gcs_path = "gs://download.tensorflow.org/example_images/320px-Felis_catus-cat_on_snow.jpg"
        mime_type = "image/jpeg"
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        }
        generation_config = {"max_output_tokens": 256, "top_p": 0.8, "temperature": 0.0}

        op = PromptMultimodalModelWithMediaOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=vision_prompt,
            generation_config=generation_config,
            safety_settings=safety_settings,
            pretrained_model=pretrained_model,
            media_gcs_path=media_gcs_path,
            mime_type=mime_type,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.prompt_multimodal_model_with_media.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=vision_prompt,
            generation_config=generation_config,
            safety_settings=safety_settings,
            pretrained_model=pretrained_model,
            media_gcs_path=media_gcs_path,
            mime_type=mime_type,
        )
