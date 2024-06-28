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
from vertexai.generative_models import HarmBlockThreshold, HarmCategory, Tool, grounding

from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    GenerativeModelGenerateContentOperator,
    TextEmbeddingModelGetEmbeddingsOperator,
    TextGenerationModelPredictOperator,
)

VERTEX_AI_PATH = "airflow.providers.google.cloud.operators.vertex_ai.{}"

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]


class TestVertexAITextGenerationModelPredictOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        prompt = "In 10 words or less, what is Apache Airflow?"
        pretrained_model = "text-bison"
        temperature = 0.0
        max_output_tokens = 256
        top_p = 0.8
        top_k = 40

        op = TextGenerationModelPredictOperator(
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
        mock_hook.return_value.text_generation_model_predict.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
            temperature=temperature,
            max_output_tokens=max_output_tokens,
            top_p=top_p,
            top_k=top_k,
        )


class TestVertexAITextEmbeddingModelGetEmbeddingsOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        prompt = "In 10 words or less, what is Apache Airflow?"
        pretrained_model = "textembedding-gecko"

        op = TextEmbeddingModelGetEmbeddingsOperator(
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
        mock_hook.return_value.text_embedding_model_get_embeddings.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
        )


class TestVertexAIGenerativeModelGenerateContentOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        contents = ["In 10 words or less, what is Apache Airflow?"]
        tools = [Tool.from_google_search_retrieval(grounding.GoogleSearchRetrieval())]
        pretrained_model = "gemini-pro"
        safety_settings = {
            HarmCategory.HARM_CATEGORY_HATE_SPEECH: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
            HarmCategory.HARM_CATEGORY_HARASSMENT: HarmBlockThreshold.BLOCK_ONLY_HIGH,
        }
        generation_config = {"max_output_tokens": 256, "top_p": 0.8, "temperature": 0.0}

        op = GenerativeModelGenerateContentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=contents,
            tools=tools,
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
        mock_hook.return_value.generative_model_generate_content.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=contents,
            tools=tools,
            generation_config=generation_config,
            safety_settings=safety_settings,
            pretrained_model=pretrained_model,
        )
