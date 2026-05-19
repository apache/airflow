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

from airflow.providers.common.ai.hooks.langchain import LangChainHook


class TestLangChainHookInit:
    def test_default_params(self):
        hook = LangChainHook()
        assert hook.llm_conn_id == "pydanticai_default"
        assert hook.embed_conn_id == "pydanticai_default"
        assert hook.embed_model == "text-embedding-3-small"
        assert hook.llm_model is None

    def test_separate_embed_conn_id(self):
        hook = LangChainHook(llm_conn_id="llm_conn", embed_conn_id="embed_conn")
        assert hook.llm_conn_id == "llm_conn"
        assert hook.embed_conn_id == "embed_conn"

    def test_embed_conn_defaults_to_llm_conn(self):
        hook = LangChainHook(llm_conn_id="my_conn")
        assert hook.embed_conn_id == "my_conn"

    def test_conn_type_is_pydanticai(self):
        assert LangChainHook.conn_type == "pydanticai"
        assert LangChainHook.default_conn_name == "pydanticai_default"


class TestResolveConnectionKwargs:
    @patch.object(LangChainHook, "get_connection")
    def test_extracts_password_as_api_key(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test-key"
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        hook = LangChainHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {"api_key": "sk-test-key"}

    @patch.object(LangChainHook, "get_connection")
    def test_extracts_host_as_base_url(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = ""
        mock_conn.host = "https://custom.api.com"
        mock_get_conn.return_value = mock_conn

        hook = LangChainHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {"base_url": "https://custom.api.com"}

    @patch.object(LangChainHook, "get_connection")
    def test_both_password_and_host(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-key"
        mock_conn.host = "https://api.example.com"
        mock_get_conn.return_value = mock_conn

        hook = LangChainHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {"api_key": "sk-key", "base_url": "https://api.example.com"}

    @patch.object(LangChainHook, "get_connection")
    def test_empty_fields_return_empty_dict(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = ""
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        hook = LangChainHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {}


def _make_mock_chat_openai_module():
    mock_module = MagicMock()
    mock_cls = MagicMock()
    mock_module.ChatOpenAI = mock_cls
    return mock_module, mock_cls


def _make_mock_openai_embeddings_module():
    mock_module = MagicMock()
    mock_cls = MagicMock()
    mock_module.OpenAIEmbeddings = mock_cls
    return mock_module, mock_cls


class TestGetChatModel:
    def test_raises_without_llm_model(self):
        hook = LangChainHook()
        with pytest.raises(ValueError, match="llm_model must be set"):
            hook.get_chat_model()

    @patch.object(LangChainHook, "get_connection")
    def test_returns_chat_openai(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test"
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        mock_module, mock_cls = _make_mock_chat_openai_module()

        hook = LangChainHook(llm_model="gpt-4o")
        with patch.dict("sys.modules", {"langchain_openai": mock_module}):
            result = hook.get_chat_model()

        mock_cls.assert_called_once_with(model="gpt-4o", api_key="sk-test")
        assert result == mock_cls.return_value

    @patch.object(LangChainHook, "get_connection")
    def test_passes_base_url(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test"
        mock_conn.host = "https://custom.api.com"
        mock_get_conn.return_value = mock_conn

        mock_module, mock_cls = _make_mock_chat_openai_module()

        hook = LangChainHook(llm_model="gpt-4o")
        with patch.dict("sys.modules", {"langchain_openai": mock_module}):
            hook.get_chat_model()

        mock_cls.assert_called_once_with(
            model="gpt-4o", api_key="sk-test", base_url="https://custom.api.com"
        )


class TestGetEmbeddingModel:
    @patch.object(LangChainHook, "get_connection")
    def test_returns_openai_embeddings(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test"
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        mock_module, mock_cls = _make_mock_openai_embeddings_module()

        hook = LangChainHook(embed_model="text-embedding-3-large")
        with patch.dict("sys.modules", {"langchain_openai": mock_module}):
            result = hook.get_embedding_model()

        mock_cls.assert_called_once_with(model="text-embedding-3-large", api_key="sk-test")
        assert result == mock_cls.return_value

    @patch.object(LangChainHook, "get_connection")
    def test_uses_embed_conn_id(self, mock_get_conn):
        mock_conn_llm = MagicMock()
        mock_conn_llm.password = "sk-llm"
        mock_conn_llm.host = ""

        mock_conn_embed = MagicMock()
        mock_conn_embed.password = "sk-embed"
        mock_conn_embed.host = ""

        mock_get_conn.side_effect = lambda conn_id: (
            mock_conn_embed if conn_id == "embed_conn" else mock_conn_llm
        )

        mock_module, mock_cls = _make_mock_openai_embeddings_module()

        hook = LangChainHook(llm_conn_id="llm_conn", embed_conn_id="embed_conn")
        with patch.dict("sys.modules", {"langchain_openai": mock_module}):
            hook.get_embedding_model()

        mock_cls.assert_called_once_with(model="text-embedding-3-small", api_key="sk-embed")
