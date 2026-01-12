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

from google.genai.types import (
    Content,
    CreateCachedContentConfig,
    EmbedContentConfig,
    GoogleSearch,
    Part,
    Tool,
)

from airflow.providers.google.cloud.hooks.gen_ai import (
    GenAIGeminiAPIHook,
    GenAIGenerativeModelHook,
)

from unit.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
GCP_PROJECT = "test-project"
GCP_LOCATION = "us-central1"

TEST_PROMPT = "In 10 words or less, what is apache airflow?"
TEST_CONTENTS = [TEST_PROMPT]
TEST_LANGUAGE_PRETRAINED_MODEL = "textembedding-gecko"
TEST_TEMPERATURE = 0.0
TEST_MAX_OUTPUT_TOKENS = 256
TEST_TOP_P = 0.8
TEST_TOP_K = 40

TEST_TEXT_EMBEDDING_MODEL = ""
TEST_TEXT_EMBEDDING_CONFIG = EmbedContentConfig(output_dimensionality=10)

TEST_MULTIMODAL_PRETRAINED_MODEL = "gemini-pro"

TEST_GENERATION_CONFIG = {
    "max_output_tokens": TEST_MAX_OUTPUT_TOKENS,
    "top_p": TEST_TOP_P,
    "temperature": TEST_TEMPERATURE,
}
TEST_TOOLS = [Tool(google_search=GoogleSearch())]

TEST_MULTIMODAL_VISION_MODEL = "gemini-pro-vision"

SOURCE_MODEL = "gemini-1.0-pro-002"
TRAIN_DATASET = "gs://cloud-samples-data/ai-platform/generative_ai/sft_train_data.jsonl"

TEST_CACHED_MODEL = "gemini-1.5-pro-002"
TEST_CACHED_SYSTEM_INSTRUCTION = """
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
    system_instruction=TEST_CACHED_SYSTEM_INSTRUCTION,
    display_name="test-cache",
    ttl="3600s",
)

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
GENERATIVE_MODEL_STRING = "airflow.providers.google.cloud.hooks.gen_ai.{}"

TEST_API_KEY = "test-api-key"
TEST_JOB_NAME = "batches/test-job-id"
TEST_MODEL = "models/gemini-2.5-flash"
TEST_BATCH_JOB_SOURCE_INLINE = [
    {"contents": [{"parts": [{"text": "Tell me a one-sentence joke."}], "role": "user"}]},
    {"contents": [{"parts": [{"text": "Why is the sky blue?"}], "role": "user"}]},
]
TEST_EMBEDDINGS_JOB_SOURCE_INLINE = {
    "contents": [{"parts": [{"text": "Why is the sky blue?"}], "role": "user"}]
}
TEST_SOURCE_FILE = "test-bucket/source.jsonl"
TEST_LOCAL_FILE_PATH = "/tmp/data/test_file.json"
TEST_FILE_NAME = "files/test-file-id"

# Mock constants for configuration objects
TEST_LIST_BATCH_JOBS_CONFIG = {"page_size": 10}
TEST_CREATE_BATCH_JOB_CONFIG = {"display_name": "test-job"}
TEST_UPLOAD_FILE_CONFIG = {"display_name": "custom_name", "mime_type": "text/plain"}


def assert_warning(msg: str, warnings):
    assert any(msg in str(w) for w in warnings)


class TestGenAIGenerativeModelHookWithDefaultProjectId:
    def dummy_get_credentials(self):
        pass

    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = GenAIGenerativeModelHook(gcp_conn_id=TEST_GCP_CONN_ID)
            self.hook.get_credentials = self.dummy_get_credentials

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGenerativeModelHook.get_genai_client"))
    def test_text_embedding_model_get_embeddings(self, mock_get_client) -> None:
        client_mock = mock_get_client.return_value
        client_mock.models = mock.Mock()
        self.hook.embed_content(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=TEST_CONTENTS,
            model=TEST_TEXT_EMBEDDING_MODEL,
            config=TEST_TEXT_EMBEDDING_CONFIG,
        )
        client_mock.models.embed_content.assert_called_once_with(
            model=TEST_TEXT_EMBEDDING_MODEL,
            contents=TEST_CONTENTS,
            config=TEST_TEXT_EMBEDDING_CONFIG,
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGenerativeModelHook.get_genai_client"))
    def test_generative_model_generate_content(self, mock_get_client) -> None:
        client_mock = mock_get_client.return_value
        client_mock.models = mock.Mock()
        self.hook.generate_content(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            contents=TEST_CONTENTS,
            generation_config=TEST_GENERATION_CONFIG,
            model=TEST_MULTIMODAL_PRETRAINED_MODEL,
        )
        client_mock.models.generate_content.assert_called_once_with(
            model=TEST_MULTIMODAL_PRETRAINED_MODEL,
            contents=TEST_CONTENTS,
            config=TEST_GENERATION_CONFIG,
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGenerativeModelHook.get_genai_client"))
    def test_supervised_fine_tuning_train(self, mock_get_client) -> None:
        client_mock = mock_get_client.return_value
        client_mock.models = mock.Mock()
        self.hook.supervised_fine_tuning_train(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            source_model=SOURCE_MODEL,
            training_dataset=TRAIN_DATASET,
        )
        client_mock.tunings.tune.assert_called_once_with(
            base_model=SOURCE_MODEL,
            training_dataset=TRAIN_DATASET,
            config=None,
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGenerativeModelHook.get_genai_client"))
    def test_count_tokens(self, mock_get_client) -> None:
        client_mock = mock_get_client.return_value
        client_mock.models = mock.Mock()
        self.hook.count_tokens(
            project_id=GCP_PROJECT,
            contents=TEST_CONTENTS,
            location=GCP_LOCATION,
            model=TEST_MULTIMODAL_PRETRAINED_MODEL,
        )
        client_mock.models.count_tokens.assert_called_once_with(
            model=TEST_MULTIMODAL_PRETRAINED_MODEL,
            contents=TEST_CONTENTS,
            config=None,
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGenerativeModelHook.get_genai_client"))
    def test_create_cached_content(self, mock_get_client) -> None:
        client_mock = mock_get_client.return_value
        client_mock.models = mock.Mock()
        self.hook.create_cached_content(
            project_id=GCP_PROJECT,
            location=GCP_LOCATION,
            model=TEST_CACHED_MODEL,
            cached_content_config=CACHED_CONTENT_CONFIG,
        )
        client_mock.caches.create.assert_called_once_with(
            model=TEST_CACHED_MODEL,
            config=CACHED_CONTENT_CONFIG,
        )


class TestGenAIGeminiAPIHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = GenAIGeminiAPIHook(gemini_api_key=TEST_API_KEY)

    @mock.patch("google.genai.Client")
    def test_get_genai_client(self, mock_client):
        """Test client initialization with correct parameters."""
        self.hook.get_genai_client()

        mock_client.assert_called_once_with(
            api_key=TEST_API_KEY,
            vertexai=False,
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_get_batch_job(self, mock_get_client):
        client_mock = mock_get_client.return_value
        client_mock.batches = mock.Mock()

        self.hook.get_batch_job(job_name=TEST_JOB_NAME)

        client_mock.batches.get.assert_called_once_with(name=TEST_JOB_NAME)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_list_batch_jobs(self, mock_get_client):
        client_mock = mock_get_client.return_value
        client_mock.batches = mock.Mock()

        self.hook.list_batch_jobs(list_batch_jobs_config=TEST_LIST_BATCH_JOBS_CONFIG)

        client_mock.batches.list.assert_called_once_with(config=TEST_LIST_BATCH_JOBS_CONFIG)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_create_batch_job(self, mock_get_client):
        client_mock = mock_get_client.return_value
        client_mock.batches = mock.Mock()

        self.hook.create_batch_job(
            model=TEST_MODEL,
            source=TEST_BATCH_JOB_SOURCE_INLINE,
            create_batch_job_config=TEST_CREATE_BATCH_JOB_CONFIG,
        )

        client_mock.batches.create.assert_called_once_with(
            model=TEST_MODEL, src=TEST_BATCH_JOB_SOURCE_INLINE, config=TEST_CREATE_BATCH_JOB_CONFIG
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_delete_batch_job(self, mock_get_client):
        client_mock = mock_get_client.return_value
        client_mock.batches = mock.Mock()

        self.hook.delete_batch_job(job_name=TEST_JOB_NAME)

        client_mock.batches.delete.assert_called_once_with(name=TEST_JOB_NAME)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_cancel_batch_job(self, mock_get_client):
        client_mock = mock_get_client.return_value
        client_mock.batches = mock.Mock()

        self.hook.cancel_batch_job(job_name=TEST_JOB_NAME)

        client_mock.batches.cancel.assert_called_once_with(name=TEST_JOB_NAME)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_create_embeddings_with_inline_source(self, mock_get_client):
        """Test create_embeddings when source is a dict (inline)."""
        client_mock = mock_get_client.return_value
        client_mock.batches = mock.Mock()

        self.hook.create_embeddings(
            model=TEST_MODEL,
            source=TEST_EMBEDDINGS_JOB_SOURCE_INLINE,
            create_embeddings_config=TEST_CREATE_BATCH_JOB_CONFIG,
        )

        client_mock.batches.create_embeddings.assert_called_once_with(
            model=TEST_MODEL,
            src={"inlined_requests": TEST_EMBEDDINGS_JOB_SOURCE_INLINE},
            config=TEST_CREATE_BATCH_JOB_CONFIG,
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_create_embeddings_with_file_source(self, mock_get_client):
        """Test create_embeddings when source is a string (file name)."""
        client_mock = mock_get_client.return_value
        client_mock.batches = mock.Mock()

        # Test with str (File name)
        source_file = "/bucket/file.jsonl"
        self.hook.create_embeddings(
            model=TEST_MODEL, source=source_file, create_embeddings_config=TEST_CREATE_BATCH_JOB_CONFIG
        )

        client_mock.batches.create_embeddings.assert_called_once_with(
            model=TEST_MODEL, src={"file_name": source_file}, config=TEST_CREATE_BATCH_JOB_CONFIG
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_upload_file_with_provided_config(self, mock_get_client):
        """Test upload_file when explicit config is provided."""
        client_mock = mock_get_client.return_value
        client_mock.files = mock.Mock()

        self.hook.upload_file(path_to_file=TEST_LOCAL_FILE_PATH, upload_file_config=TEST_UPLOAD_FILE_CONFIG)

        client_mock.files.upload.assert_called_once_with(
            file=TEST_LOCAL_FILE_PATH, config=TEST_UPLOAD_FILE_CONFIG
        )

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_upload_file_default_config_generation(self, mock_get_client):
        """Test that upload_file generates correct config from filename if config is None."""
        client_mock = mock_get_client.return_value
        client_mock.files = mock.Mock()

        # Path: /tmp/data/test_file.json -> name: test_file, type: json
        self.hook.upload_file(path_to_file=TEST_LOCAL_FILE_PATH, upload_file_config=None)

        expected_config = {"display_name": "test_file", "mime_type": "json"}

        client_mock.files.upload.assert_called_once_with(file=TEST_LOCAL_FILE_PATH, config=expected_config)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_get_file(self, mock_get_client):
        client_mock = mock_get_client.return_value
        client_mock.files = mock.Mock()

        self.hook.get_file(file_name=TEST_FILE_NAME)

        client_mock.files.get.assert_called_once_with(name=TEST_FILE_NAME)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_download_file(self, mock_get_client):
        client_mock = mock_get_client.return_value
        client_mock.files = mock.Mock()

        self.hook.download_file(file_name=TEST_FILE_NAME)

        client_mock.files.download.assert_called_once_with(file=TEST_FILE_NAME)

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_list_files(self, mock_get_client):
        client_mock = mock_get_client.return_value
        client_mock.files = mock.Mock()

        self.hook.list_files()

        client_mock.files.list.assert_called_once()

    @mock.patch(GENERATIVE_MODEL_STRING.format("GenAIGeminiAPIHook.get_genai_client"))
    def test_delete_file(self, mock_get_client):
        client_mock = mock_get_client.return_value
        client_mock.files = mock.Mock()

        self.hook.delete_file(file_name=TEST_FILE_NAME)

        client_mock.files.delete.assert_called_once_with(name=TEST_FILE_NAME)
