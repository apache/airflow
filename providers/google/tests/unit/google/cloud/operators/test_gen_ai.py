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
    GenAIGeminiCancelBatchJobOperator,
    GenAIGeminiCreateBatchJobOperator,
    GenAIGeminiCreateEmbeddingsBatchJobOperator,
    GenAIGeminiDeleteBatchJobOperator,
    GenAIGeminiDeleteFileOperator,
    GenAIGeminiGetBatchJobOperator,
    GenAIGeminiGetFileOperator,
    GenAIGeminiListBatchJobsOperator,
    GenAIGeminiListFilesOperator,
    GenAIGeminiUploadFileOperator,
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

TEST_BATCH_JOB_INLINED_REQUESTS = [
    {"contents": [{"parts": [{"text": "Tell me a one-sentence joke."}], "role": "user"}]},
    {"contents": [{"parts": [{"text": "Why is the sky blue?"}], "role": "user"}]},
]

TEST_EMBEDDINGS_JOB_INLINED_REQUESTS = {
    "contents": [{"parts": [{"text": "Why is the sky blue?"}], "role": "user"}]
}
TEST_GEMINI_API_KEY = "test-key"
TEST_GEMINI_MODEL = "test-gemini-model"
TEST_BATCH_JOB_NAME = "test-name"
TEST_FILE_NAME = "test-file"
TEST_FILE_PATH = "test/path/to/file"


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


class TestGenAIGeminiCreateBatchJobOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGeminiAPIHook"))
    def test_execute(self, mock_hook):
        op = GenAIGeminiCreateBatchJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            model=TEST_GEMINI_MODEL,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            input_source=TEST_BATCH_JOB_INLINED_REQUESTS,
            gemini_api_key=TEST_GEMINI_API_KEY,
            wait_until_complete=False,
            retrieve_result=False,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        mock_hook.return_value.create_batch_job.assert_called_once_with(
            source=TEST_BATCH_JOB_INLINED_REQUESTS,
            model=TEST_GEMINI_MODEL,
            create_batch_job_config=None,
        )


class TestGenAIGeminiGetBatchJobOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGeminiAPIHook"))
    def test_execute(self, mock_hook):
        op = GenAIGeminiGetBatchJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            job_name=TEST_BATCH_JOB_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        mock_hook.return_value.get_batch_job.assert_called_once_with(
            job_name=TEST_BATCH_JOB_NAME,
        )


class TestGenAIGeminiListBatchJobsOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGeminiAPIHook"))
    def test_execute(self, mock_hook):
        op = GenAIGeminiListBatchJobsOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            gemini_api_key=TEST_GEMINI_API_KEY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        mock_hook.return_value.list_batch_jobs.assert_called_once_with(list_batch_jobs_config=None)


class TestGenAIGeminiDeleteBatchJobOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGeminiAPIHook"))
    def test_execute(self, mock_hook):
        mock_hook.return_value.delete_batch_job.return_value = mock.MagicMock(error=False)
        op = GenAIGeminiDeleteBatchJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            job_name=TEST_BATCH_JOB_NAME,
            gemini_api_key=TEST_GEMINI_API_KEY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        mock_hook.return_value.delete_batch_job.assert_called_once_with(
            job_name=TEST_BATCH_JOB_NAME,
        )


class TestGenAIGeminiCancelBatchJobOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGeminiAPIHook"))
    def test_execute(self, mock_hook):
        op = GenAIGeminiCancelBatchJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            job_name=TEST_BATCH_JOB_NAME,
            gemini_api_key=TEST_GEMINI_API_KEY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        mock_hook.return_value.cancel_batch_job.assert_called_once_with(
            job_name=TEST_BATCH_JOB_NAME,
        )


class TestGenAIGeminiCreateEmbeddingsBatchJobOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGeminiAPIHook"))
    def test_execute(self, mock_hook):
        op = GenAIGeminiCreateEmbeddingsBatchJobOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            input_source=TEST_EMBEDDINGS_JOB_INLINED_REQUESTS,
            model=EMBEDDING_MODEL,
            gemini_api_key=TEST_GEMINI_API_KEY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            wait_until_complete=False,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        mock_hook.return_value.create_embeddings.assert_called_once_with(
            source=TEST_EMBEDDINGS_JOB_INLINED_REQUESTS,
            model=EMBEDDING_MODEL,
            create_embeddings_config=None,
        )


class TestGenAIGeminiGetFileOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGeminiAPIHook"))
    def test_execute(self, mock_hook):
        op = GenAIGeminiGetFileOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            gemini_api_key=TEST_GEMINI_API_KEY,
            file_name=TEST_FILE_NAME,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        mock_hook.return_value.get_file.assert_called_once_with(
            file_name=TEST_FILE_NAME,
        )


class TestGenAIGeminiUploadFileOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGeminiAPIHook"))
    def test_execute(self, mock_hook):
        op = GenAIGeminiUploadFileOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            file_path=TEST_FILE_PATH,
            gemini_api_key=TEST_GEMINI_API_KEY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        mock_hook.return_value.upload_file.assert_called_once_with(
            path_to_file=TEST_FILE_PATH,
            upload_file_config=None,
        )


class TestGenAIGeminiListFilesOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGeminiAPIHook"))
    def test_execute(self, mock_hook):
        op = GenAIGeminiListFilesOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            gemini_api_key=TEST_GEMINI_API_KEY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        mock_hook.return_value.list_files.assert_called_once_with()


class TestGenAIGeminiDeleteFileOperator:
    @mock.patch(GEN_AI_PATH.format("GenAIGeminiAPIHook"))
    def test_execute(self, mock_hook):
        op = GenAIGeminiDeleteFileOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            file_name=TEST_FILE_NAME,
            gemini_api_key=TEST_GEMINI_API_KEY,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        op.execute(context={"ti": mock.MagicMock()})
        mock_hook.assert_called_once_with(
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
            gemini_api_key=TEST_GEMINI_API_KEY,
        )
        mock_hook.return_value.delete_file.assert_called_once_with(
            file_name=TEST_FILE_NAME,
        )
