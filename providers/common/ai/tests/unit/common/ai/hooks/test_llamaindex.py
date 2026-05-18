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

from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.ai.hooks.llamaindex import LlamaIndexHook


class TestLlamaIndexHookInit:
    def test_default_params(self):
        hook = LlamaIndexHook()
        assert hook.llm_conn_id == "pydanticai_default"
        assert hook.embed_conn_id == "pydanticai_default"
        assert hook.embed_model == "text-embedding-3-small"
        assert hook.llm_model is None

    def test_separate_embed_conn_id(self):
        hook = LlamaIndexHook(llm_conn_id="llm_conn", embed_conn_id="embed_conn")
        assert hook.llm_conn_id == "llm_conn"
        assert hook.embed_conn_id == "embed_conn"

    def test_embed_conn_defaults_to_llm_conn(self):
        hook = LlamaIndexHook(llm_conn_id="my_conn")
        assert hook.embed_conn_id == "my_conn"


class TestResolveConnectionKwargs:
    @patch.object(LlamaIndexHook, "get_connection")
    def test_extracts_password_as_api_key(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test-key"
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        hook = LlamaIndexHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {"api_key": "sk-test-key"}

    @patch.object(LlamaIndexHook, "get_connection")
    def test_extracts_host_as_api_base(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = ""
        mock_conn.host = "https://custom.api.com"
        mock_get_conn.return_value = mock_conn

        hook = LlamaIndexHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {"api_base": "https://custom.api.com"}

    @patch.object(LlamaIndexHook, "get_connection")
    def test_both_password_and_host(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-key"
        mock_conn.host = "https://api.example.com"
        mock_get_conn.return_value = mock_conn

        hook = LlamaIndexHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {"api_key": "sk-key", "api_base": "https://api.example.com"}

    @patch.object(LlamaIndexHook, "get_connection")
    def test_empty_fields_return_empty_dict(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = ""
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        hook = LlamaIndexHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {}


def _make_mock_openai_embedding_module():
    mock_module = MagicMock()
    mock_cls = MagicMock()
    mock_module.OpenAIEmbedding = mock_cls
    return mock_module, mock_cls


def _make_mock_openai_llm_module():
    mock_module = MagicMock()
    mock_cls = MagicMock()
    mock_module.OpenAI = mock_cls
    return mock_module, mock_cls


class TestGetEmbeddingModel:
    @patch.object(LlamaIndexHook, "get_connection")
    def test_returns_openai_embedding(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test"
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        mock_embed_module, mock_embed_cls = _make_mock_openai_embedding_module()

        hook = LlamaIndexHook(embed_model="text-embedding-3-large")
        with patch.dict("sys.modules", {"llama_index.embeddings.openai": mock_embed_module}):
            result = hook.get_embedding_model()

        mock_embed_cls.assert_called_once_with(model="text-embedding-3-large", api_key="sk-test")
        assert result == mock_embed_cls.return_value


class TestGetLLM:
    def test_raises_without_llm_model(self):
        hook = LlamaIndexHook()
        with pytest.raises(ValueError, match="llm_model must be set"):
            hook.get_llm()

    @patch.object(LlamaIndexHook, "get_connection")
    def test_returns_openai_llm(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test"
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        mock_llm_module, mock_llm_cls = _make_mock_openai_llm_module()

        hook = LlamaIndexHook(llm_model="gpt-4o")
        with patch.dict("sys.modules", {"llama_index.llms.openai": mock_llm_module}):
            result = hook.get_llm()

        mock_llm_cls.assert_called_once_with(model="gpt-4o", api_key="sk-test")
        assert result == mock_llm_cls.return_value


class TestConfigureSettings:
    @patch.object(LlamaIndexHook, "get_connection")
    def test_sets_embed_model(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test"
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        mock_embed_module, mock_embed_cls = _make_mock_openai_embedding_module()
        mock_settings_module = MagicMock()

        hook = LlamaIndexHook()
        with patch.dict(
            "sys.modules",
            {
                "llama_index.embeddings.openai": mock_embed_module,
                "llama_index": MagicMock(),
                "llama_index.core": mock_settings_module,
            },
        ):
            hook.configure_settings()

        assert mock_settings_module.Settings.embed_model == mock_embed_cls.return_value

    @patch.object(LlamaIndexHook, "get_connection")
    def test_sets_llm_when_model_provided(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test"
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        mock_embed_module, _ = _make_mock_openai_embedding_module()
        mock_llm_module, mock_llm_cls = _make_mock_openai_llm_module()
        mock_settings_module = MagicMock()

        hook = LlamaIndexHook(llm_model="gpt-4o")
        with patch.dict(
            "sys.modules",
            {
                "llama_index.embeddings.openai": mock_embed_module,
                "llama_index.llms.openai": mock_llm_module,
                "llama_index": MagicMock(),
                "llama_index.core": mock_settings_module,
            },
        ):
            hook.configure_settings()

        assert mock_settings_module.Settings.llm == mock_llm_cls.return_value
