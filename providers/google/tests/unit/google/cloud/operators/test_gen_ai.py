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

from google.genai.types import (
    Content,
    CreateCachedContentConfig,
    GenerateContentConfig,
    GoogleSearch,
    Part,
    Tool,
    TuningDataset,
)

from airflow.providers.google.cloud.operators.gen_ai import (
    GenAICountTokensOperator,
    GenAICreateCachedContentOperator,
    GenAIGenerateContentOperator,
    GenAIGenerateEmbeddingsOperator,
    GenAISupervisedFineTuningTrainOperator,
)

GEN_AI_PATH = "airflow.providers.google.cloud.operators.gen_ai.{}"

TASK_ID = "test_task_id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "test-location"
GCP_CONN_ID = "test-conn"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]
CACHED_SYSTEM_INSTRUCTION = """
You are an expert researcher. You always stick to the facts in the sources provided, and never make up new facts.
Now look at these research papers, and answer the following questions.
"""
CACHED_CONTENT_CONFIG = CreateCachedContentConfig(
    contents=[
        Content(
            role="user",
            parts=[
                Part.from_uri(
                    file_uri="gs://cloud-samples-data/generative-ai/pdf/2312.11805v3.pdf",
                    mime_type="application/pdf",
                ),
                Part.from_uri(
                    file_uri="gs://cloud-samples-data/generative-ai/pdf/2403.05530.pdf",
                    mime_type="application/pdf",
                ),
            ],
        )
    ],
    system_instruction=CACHED_SYSTEM_INSTRUCTION,
    display_name="test-cache",
    ttl="3600s",
)
EMBEDDING_MODEL = "textembedding-gecko"
GEMINI_MODEL = "gemini-pro"
CONTENTS = ["In 10 words or less, what is Apache Airflow?"]
CONTENT_GENERATION_CONFIG = GenerateContentConfig(
    max_output_tokens=256,
    top_p=0.95,
    temperature=0.0,
    tools=[Tool(google_search=GoogleSearch())],
)
TUNING_JOB_CONFIG = TuningDataset(
    gcs_uri="gs://cloud-samples-data/ai-platform/generative_ai/gemini-1_5/text/sft_train_data.jsonl",
)
TUNING_TRAINING_DATASET = "gs://cloud-samples-data/ai-platform/generative_ai/sft_train_data.jsonl"
GENERATE_FROM_CACHED_MODEL_CONFIG = {
    "cached_content": "cached_name",
}


def assert_warning(msg: str, warnings):
    assert any(msg in str(w) for w in warnings)


class TestGenAIGenerateEmbeddingsOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGenerativeModelHook"))
    def test_execute(self, mock_hook):
        op = GenAIGenerateEmbeddingsOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=CONTENTS,
            model=EMBEDDING_MODEL,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.embed_content.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=CONTENTS,
            model=EMBEDDING_MODEL,
            config=None,
        )


class TestGenAIGenerateContentOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGenerativeModelHook"))
    def test_execute(self, mock_hook):
        op = GenAIGenerateContentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=CONTENTS,
            generation_config=CONTENT_GENERATION_CONFIG,
            model=GEMINI_MODEL,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.generate_content.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=CONTENTS,
            generation_config=CONTENT_GENERATION_CONFIG,
            model=GEMINI_MODEL,
        )


class TestGenAISupervisedFineTuningTrainOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGenerativeModelHook"))
    def test_execute(
        self,
        mock_hook,
    ):
        op = GenAISupervisedFineTuningTrainOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            source_model=GEMINI_MODEL,
            training_dataset=TUNING_TRAINING_DATASET,
            tuning_job_config=TUNING_JOB_CONFIG,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.supervised_fine_tuning_train.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            source_model=GEMINI_MODEL,
            training_dataset=TUNING_TRAINING_DATASET,
            tuning_job_config=TUNING_JOB_CONFIG,
        )


class TestGenAICountTokensOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGenerativeModelHook"))
    def test_execute(self, mock_hook):
        op = GenAICountTokensOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=CONTENTS,
            model=GEMINI_MODEL,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.count_tokens.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=CONTENTS,
            model=GEMINI_MODEL,
            config=None,
        )


class TestGenAICreateCachedContentOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGenerativeModelHook"))
    def test_execute(self, mock_hook):
        op = GenAICreateCachedContentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            model=GEMINI_MODEL,
            cached_content_config=CACHED_CONTENT_CONFIG,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.create_cached_content.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            model=GEMINI_MODEL,
            cached_content_config=CACHED_CONTENT_CONFIG,
        )


class TestGenAIGenerateFromCachedContentOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGenerativeModelHook"))
    def test_execute(self, mock_hook):
        op = GenAIGenerateContentOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            model=GEMINI_MODEL,
            contents=CONTENTS,
            generation_config=GENERATE_FROM_CACHED_MODEL_CONFIG,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.generate_content.assert_called_once_with(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            model=GEMINI_MODEL,
            contents=CONTENTS,
            generation_config=GENERATE_FROM_CACHED_MODEL_CONFIG,
        )
