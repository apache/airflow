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
from google.api_core.retry import Retry


from airflow.providers.google.cloud.operators.vertex_ai.generative_model import (
    PromptLanguageModelOperator,
    PromptMultimodalModelOperator,
)

VERTEX_AI_PATH = "airflow.providers.google.cloud.operators.vertex_ai.{}"
TIMEOUT = 120
RETRY = mock.MagicMock(Retry)
METADATA = [("key", "value")]

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


class TestVertexAIPromptMultimodalModelOperator:
    @mock.patch(VERTEX_AI_PATH.format("generative_model.GenerativeModelHook"))
    def test_execute(self, mock_hook):
        prompt = "In 10 words or less, what is Apache Airflow?"
        pretrained_model = "gemini-pro"
        chat = None

        op = PromptMultimodalModelOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            prompt=prompt,
            pretrained_model=pretrained_model,
            chat=chat,
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
            pretrained_model=pretrained_model,
            chat=chat,
        )
