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


def _conn(password: str = "", host: str = "", extra: dict | None = None) -> MagicMock:
    mock_conn = MagicMock()
    mock_conn.password = password
    mock_conn.host = host
    mock_conn.extra_dejson = extra or {}
    return mock_conn


class TestLlamaIndexHookInit:
    def test_default_params(self):
        hook = LlamaIndexHook()
        assert hook.llm_conn_id == "llamaindex_default"
        assert hook.embed_conn_id == "llamaindex_default"
        assert hook.embed_model is None
        assert hook.llm_model is None

    def test_embed_conn_falls_back_to_llm_conn(self):
        hook = LlamaIndexHook(llm_conn_id="my_conn")
        assert hook.embed_conn_id == "my_conn"

    def test_explicit_separate_conns_and_models(self):
        hook = LlamaIndexHook(
            llm_conn_id="chat_conn",
            embed_conn_id="embed_conn",
            embed_model="text-embedding-3-large",
            llm_model="gpt-4o",
        )
        assert hook.llm_conn_id == "chat_conn"
        assert hook.embed_conn_id == "embed_conn"
        assert hook.embed_model == "text-embedding-3-large"
        assert hook.llm_model == "gpt-4o"

    def test_conn_type_is_llamaindex(self):
        assert LlamaIndexHook.conn_type == "llamaindex"
        assert LlamaIndexHook.default_conn_name == "llamaindex_default"
        assert LlamaIndexHook.conn_name_attr == "llm_conn_id"
        assert LlamaIndexHook.hook_name == "LlamaIndex"


class TestGetUiFieldBehaviour:
    def test_shape(self):
        behaviour = LlamaIndexHook.get_ui_field_behaviour()
        assert behaviour["hidden_fields"] == ["schema", "port", "login"]
        assert behaviour["relabeling"] == {"password": "API Key"}
        assert "host" in behaviour["placeholders"]
        assert "embed_model" in behaviour["placeholders"]["extra"]
        assert "llm_model" in behaviour["placeholders"]["extra"]


class TestResolveModel:
    def test_constructor_wins_over_extra(self):
        result = LlamaIndexHook._resolve_model(
            {"embed_model": "old"},
            constructor_value="new",
            extra_key="embed_model",
            kind="embedding",
        )
        assert result == "new"

    def test_falls_back_to_extra(self):
        result = LlamaIndexHook._resolve_model(
            {"embed_model": "from-extra"},
            constructor_value=None,
            extra_key="embed_model",
            kind="embedding",
        )
        assert result == "from-extra"

    def test_raises_when_neither_set(self):
        with pytest.raises(ValueError, match="No embedding model identifier set"):
            LlamaIndexHook._resolve_model(
                {},
                constructor_value=None,
                extra_key="embed_model",
                kind="embedding",
            )


class TestGetEmbeddingModel:
    @patch("llama_index.embeddings.openai.OpenAIEmbedding")
    @patch.object(LlamaIndexHook, "get_connection")
    def test_dispatches_with_api_key(self, mock_get_conn, mock_cls):
        mock_get_conn.return_value = _conn(password="sk-test")
        hook = LlamaIndexHook(embed_model="text-embedding-3-small")

        result = hook.get_embedding_model()

        mock_get_conn.assert_called_once_with("llamaindex_default")
        mock_cls.assert_called_once_with(model="text-embedding-3-small", api_key="sk-test")
        assert result is mock_cls.return_value

    @patch("llama_index.embeddings.openai.OpenAIEmbedding")
    @patch.object(LlamaIndexHook, "get_connection")
    def test_dispatches_with_api_base(self, mock_get_conn, mock_cls):
        mock_get_conn.return_value = _conn(password="sk-test", host="http://localhost:11434/v1")
        hook = LlamaIndexHook(embed_model="text-embedding-3-small")

        hook.get_embedding_model()

        mock_cls.assert_called_once_with(
            model="text-embedding-3-small",
            api_key="sk-test",
            api_base="http://localhost:11434/v1",
        )

    @patch("llama_index.embeddings.openai.OpenAIEmbedding")
    @patch.object(LlamaIndexHook, "get_connection")
    def test_resolves_model_from_extra(self, mock_get_conn, mock_cls):
        mock_get_conn.return_value = _conn(
            password="sk-test", extra={"embed_model": "text-embedding-3-large"}
        )
        hook = LlamaIndexHook()

        hook.get_embedding_model()

        mock_cls.assert_called_once_with(model="text-embedding-3-large", api_key="sk-test")

    @patch.object(LlamaIndexHook, "get_connection")
    def test_raises_when_no_model_anywhere(self, mock_get_conn):
        mock_get_conn.return_value = _conn(password="sk-test")
        hook = LlamaIndexHook()

        with pytest.raises(ValueError, match="No embedding model identifier set"):
            hook.get_embedding_model()


class TestGetLlm:
    @patch("llama_index.llms.openai.OpenAI")
    @patch.object(LlamaIndexHook, "get_connection")
    def test_dispatches_with_api_key(self, mock_get_conn, mock_cls):
        mock_get_conn.return_value = _conn(password="sk-test")
        hook = LlamaIndexHook(llm_model="gpt-4o")

        result = hook.get_llm()

        mock_cls.assert_called_once_with(model="gpt-4o", api_key="sk-test")
        assert result is mock_cls.return_value

    @patch.object(LlamaIndexHook, "get_connection")
    def test_raises_when_no_llm_model(self, mock_get_conn):
        mock_get_conn.return_value = _conn(password="sk-test")
        hook = LlamaIndexHook()

        with pytest.raises(ValueError, match="No llm model identifier set"):
            hook.get_llm()
