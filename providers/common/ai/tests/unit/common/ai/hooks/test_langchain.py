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

import sys
from unittest.mock import MagicMock, patch

import pytest

from airflow.providers.common.ai.hooks.langchain import LangChainHook


@pytest.fixture(autouse=True)
def _stub_langchain_modules():
    # langchain is an optional dep; stub sys.modules so @patch can resolve
    # langchain.* targets without it being installed.
    # Submodule entries are derived from parent mock attributes so @patch
    # (which resolves via getattr) and the hook's lazy imports (which read
    # sys.modules["langchain.chat_models"]) see the same object.
    lc = MagicMock()
    lc_core = MagicMock()
    mocks = {
        "langchain": lc,
        "langchain.chat_models": lc.chat_models,
        "langchain.embeddings": lc.embeddings,
        "langchain_core": lc_core,
        "langchain_core.embeddings": lc_core.embeddings,
        "langchain_core.language_models": lc_core.language_models,
        "langchain_core.language_models.chat_models": lc_core.language_models.chat_models,
    }
    with patch.dict(sys.modules, mocks):
        yield


def _conn(password: str = "", host: str = "", extra: dict | None = None) -> MagicMock:
    mock_conn = MagicMock()
    mock_conn.password = password
    mock_conn.host = host
    mock_conn.extra_dejson = extra or {}
    return mock_conn


class TestLangChainHookInit:
    def test_default_params(self):
        hook = LangChainHook()
        assert hook.llm_conn_id == "langchain_default"
        assert hook.embed_conn_id == "langchain_default"
        assert hook.llm_model is None
        assert hook.embed_model is None

    def test_embed_conn_falls_back_to_llm_conn(self):
        hook = LangChainHook(llm_conn_id="my_conn")
        assert hook.llm_conn_id == "my_conn"
        assert hook.embed_conn_id == "my_conn"

    def test_explicit_separate_conns_and_models(self):
        hook = LangChainHook(
            llm_conn_id="chat_conn",
            embed_conn_id="embed_conn",
            llm_model="openai:gpt-4o",
            embed_model="openai:text-embedding-3-small",
        )
        assert hook.llm_conn_id == "chat_conn"
        assert hook.embed_conn_id == "embed_conn"
        assert hook.llm_model == "openai:gpt-4o"
        assert hook.embed_model == "openai:text-embedding-3-small"

    def test_conn_type_is_langchain(self):
        assert LangChainHook.conn_type == "langchain"
        assert LangChainHook.default_conn_name == "langchain_default"
        assert LangChainHook.conn_name_attr == "llm_conn_id"
        assert LangChainHook.hook_name == "LangChain"


class TestGetUiFieldBehaviour:
    def test_shape(self):
        behaviour = LangChainHook.get_ui_field_behaviour()
        assert behaviour["hidden_fields"] == ["schema", "port", "login"]
        assert behaviour["relabeling"] == {"password": "API Key"}
        assert "host" in behaviour["placeholders"]
        assert "extra" in behaviour["placeholders"]
        # extra placeholder shows both chat and embedding model keys
        assert "model" in behaviour["placeholders"]["extra"]
        assert "embed_model" in behaviour["placeholders"]["extra"]


class TestResolveModelId:
    def test_constructor_wins_over_extra(self):
        result = LangChainHook._resolve_model_id(
            {"model": "anthropic:claude-3-7-sonnet"},
            constructor_value="openai:gpt-4o",
            extra_key="model",
            kind="chat",
        )
        assert result == "openai:gpt-4o"

    def test_falls_back_to_extra(self):
        result = LangChainHook._resolve_model_id(
            {"model": "anthropic:claude-3-7-sonnet"},
            constructor_value=None,
            extra_key="model",
            kind="chat",
        )
        assert result == "anthropic:claude-3-7-sonnet"

    def test_raises_when_neither_set_for_chat(self):
        with pytest.raises(ValueError, match="No chat model identifier set"):
            LangChainHook._resolve_model_id(
                {},
                constructor_value=None,
                extra_key="model",
                kind="chat",
            )

    def test_raises_when_neither_set_for_embedding(self):
        with pytest.raises(ValueError, match="No embedding model identifier set"):
            LangChainHook._resolve_model_id(
                {},
                constructor_value=None,
                extra_key="embed_model",
                kind="embedding",
            )


class TestGetChatModel:
    @patch("langchain.chat_models.init_chat_model")
    @patch.object(LangChainHook, "get_connection")
    def test_dispatches_via_init_chat_model_with_api_key(self, mock_get_conn, mock_init_chat_model):
        mock_get_conn.return_value = _conn(password="sk-test")

        hook = LangChainHook(llm_model="openai:gpt-4o")
        result = hook.get_chat_model()

        mock_get_conn.assert_called_once_with("langchain_default")
        mock_init_chat_model.assert_called_once_with("openai:gpt-4o", api_key="sk-test")
        assert result is mock_init_chat_model.return_value

    @patch("langchain.chat_models.init_chat_model")
    @patch.object(LangChainHook, "get_connection")
    def test_dispatches_with_base_url(self, mock_get_conn, mock_init_chat_model):
        mock_get_conn.return_value = _conn(password="sk-test", host="http://localhost:11434/v1")

        hook = LangChainHook(llm_model="openai:llama3")
        hook.get_chat_model()

        mock_init_chat_model.assert_called_once_with(
            "openai:llama3", api_key="sk-test", base_url="http://localhost:11434/v1"
        )

    @patch("langchain.chat_models.init_chat_model")
    @patch.object(LangChainHook, "get_connection")
    def test_resolves_model_from_extra(self, mock_get_conn, mock_init_chat_model):
        mock_get_conn.return_value = _conn(password="sk-test", extra={"model": "anthropic:claude-3-7-sonnet"})

        hook = LangChainHook()
        hook.get_chat_model()

        mock_init_chat_model.assert_called_once_with("anthropic:claude-3-7-sonnet", api_key="sk-test")

    @patch("langchain.chat_models.init_chat_model")
    @patch.object(LangChainHook, "get_connection")
    def test_raises_when_no_model(self, mock_get_conn, mock_init_chat_model):
        mock_get_conn.return_value = _conn(password="sk-test")

        hook = LangChainHook()
        with pytest.raises(ValueError, match="No chat model identifier set"):
            hook.get_chat_model()
        mock_init_chat_model.assert_not_called()

    @patch("langchain.chat_models.init_chat_model")
    @patch.object(LangChainHook, "get_connection")
    def test_no_credentials_passes_empty_kwargs(self, mock_get_conn, mock_init_chat_model):
        mock_get_conn.return_value = _conn()

        hook = LangChainHook(llm_model="openai:gpt-4o")
        hook.get_chat_model()

        mock_init_chat_model.assert_called_once_with("openai:gpt-4o")


class TestGetEmbeddingModel:
    @patch("langchain.embeddings.init_embeddings")
    @patch.object(LangChainHook, "get_connection")
    def test_dispatches_via_init_embeddings_with_api_key(self, mock_get_conn, mock_init_embeddings):
        mock_get_conn.return_value = _conn(password="sk-test")

        hook = LangChainHook(embed_model="openai:text-embedding-3-small")
        result = hook.get_embedding_model()

        mock_get_conn.assert_called_once_with("langchain_default")
        mock_init_embeddings.assert_called_once_with("openai:text-embedding-3-small", api_key="sk-test")
        assert result is mock_init_embeddings.return_value

    @patch("langchain.embeddings.init_embeddings")
    @patch.object(LangChainHook, "get_connection")
    def test_dispatches_with_base_url(self, mock_get_conn, mock_init_embeddings):
        mock_get_conn.return_value = _conn(password="sk-test", host="http://localhost:11434/v1")

        hook = LangChainHook(embed_model="openai:nomic-embed-text")
        hook.get_embedding_model()

        mock_init_embeddings.assert_called_once_with(
            "openai:nomic-embed-text",
            api_key="sk-test",
            base_url="http://localhost:11434/v1",
        )

    @patch("langchain.embeddings.init_embeddings")
    @patch.object(LangChainHook, "get_connection")
    def test_resolves_embed_model_from_extra(self, mock_get_conn, mock_init_embeddings):
        mock_get_conn.return_value = _conn(
            password="sk-test", extra={"embed_model": "openai:text-embedding-3-large"}
        )

        hook = LangChainHook()
        hook.get_embedding_model()

        mock_init_embeddings.assert_called_once_with("openai:text-embedding-3-large", api_key="sk-test")

    @patch("langchain.embeddings.init_embeddings")
    @patch.object(LangChainHook, "get_connection")
    def test_uses_embed_conn_id_when_set(self, mock_get_conn, mock_init_embeddings):
        # llm_conn_id and embed_conn_id resolve to different connections.
        llm_conn = _conn(password="sk-chat", extra={"model": "openai:gpt-4o"})
        embed_conn = _conn(
            password="sk-embed",
            extra={"embed_model": "openai:text-embedding-3-small"},
        )
        mock_get_conn.side_effect = lambda conn_id: embed_conn if conn_id == "embed_conn" else llm_conn

        hook = LangChainHook(llm_conn_id="chat_conn", embed_conn_id="embed_conn")
        hook.get_embedding_model()

        # embed conn picked, with its api key and model
        mock_get_conn.assert_called_with("embed_conn")
        mock_init_embeddings.assert_called_once_with("openai:text-embedding-3-small", api_key="sk-embed")

    @patch("langchain.embeddings.init_embeddings")
    @patch.object(LangChainHook, "get_connection")
    def test_raises_when_no_embed_model(self, mock_get_conn, mock_init_embeddings):
        # extra has chat model but no embed_model -- should still raise
        mock_get_conn.return_value = _conn(password="sk-test", extra={"model": "openai:gpt-4o"})

        hook = LangChainHook()
        with pytest.raises(ValueError, match="No embedding model identifier set"):
            hook.get_embedding_model()
        mock_init_embeddings.assert_not_called()

    @patch("langchain.embeddings.init_embeddings")
    @patch.object(LangChainHook, "get_connection")
    def test_no_credentials_passes_empty_kwargs(self, mock_get_conn, mock_init_embeddings):
        mock_get_conn.return_value = _conn()

        hook = LangChainHook(embed_model="openai:text-embedding-3-small")
        hook.get_embedding_model()

        mock_init_embeddings.assert_called_once_with("openai:text-embedding-3-small")


class TestSameHookForBoth:
    """A single hook instance must serve both chat and embedding calls."""

    @patch("langchain.embeddings.init_embeddings")
    @patch("langchain.chat_models.init_chat_model")
    @patch.object(LangChainHook, "get_connection")
    def test_chat_and_embed_from_one_hook(self, mock_get_conn, mock_init_chat_model, mock_init_embeddings):
        mock_get_conn.return_value = _conn(password="sk-test")

        hook = LangChainHook(
            llm_model="openai:gpt-4o",
            embed_model="openai:text-embedding-3-small",
        )
        chat = hook.get_chat_model()
        embed = hook.get_embedding_model()

        mock_init_chat_model.assert_called_once_with("openai:gpt-4o", api_key="sk-test")
        mock_init_embeddings.assert_called_once_with("openai:text-embedding-3-small", api_key="sk-test")
        assert chat is mock_init_chat_model.return_value
        assert embed is mock_init_embeddings.return_value
