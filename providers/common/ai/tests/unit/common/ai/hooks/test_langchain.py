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
import warnings
from unittest.mock import MagicMock, patch

import pytest

from airflow.exceptions import AirflowProviderDeprecationWarning
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


def _init_embeddings(model: str, *, provider: str | None = None, **kwargs):
    return {"model": model, "provider": provider, "kwargs": kwargs}


class TestLangChainHookInit:
    def test_default_params(self):
        hook = LangChainHook()
        assert hook.llm_conn_id == "langchain_default"
        assert hook.embed_conn_id == "langchain_default"
        assert hook.llm_model is None
        assert hook.embed_model is None
        assert hook.embedding_kwargs == {}

    def test_embed_conn_falls_back_to_llm_conn(self):
        hook = LangChainHook(llm_conn_id="my_conn")
        assert hook.llm_conn_id == "my_conn"
        assert hook.embed_conn_id == "my_conn"

    @pytest.mark.parametrize(
        ("positional", "expected"),
        [
            pytest.param(("llm-pos",), {"llm_model": "llm-pos", "embed_model": None}, id="one"),
            pytest.param(
                ("llm-pos", "embed-pos"),
                {"llm_model": "llm-pos", "embed_model": "embed-pos"},
                id="two",
            ),
        ],
    )
    def test_positional_model_emits_deprecation_warning(self, positional, expected):
        with pytest.warns(AirflowProviderDeprecationWarning) as record:
            hook = LangChainHook("c1", "c2", *positional)
        assert len(record) == len(positional)
        assert "Parameter `llm_model` passed positionally is deprecated" in str(record[0].message)
        assert hook.llm_conn_id == "c1"
        assert hook.embed_conn_id == "c2"
        assert hook.llm_model == expected["llm_model"]
        assert hook.embed_model == expected["embed_model"]

    def test_positional_warning_stacklevel_points_to_caller(self):
        with pytest.warns(AirflowProviderDeprecationWarning) as record:
            LangChainHook("c1", "c2", "llm-pos")
        assert record[0].filename == __file__

    def test_one_warning_per_positional_argument(self):
        with pytest.warns(AirflowProviderDeprecationWarning) as record:
            LangChainHook("c1", "c2", "llm-pos", "embed-pos")
        messages = [str(w.message) for w in record]
        assert len(messages) == 2
        assert "`llm_model`" in messages[0]
        assert "`embed_model`" in messages[1]

    def test_keyword_model_arguments_do_not_warn(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error", AirflowProviderDeprecationWarning)
            hook = LangChainHook("c1", "c2", llm_model="llm-pos", embed_model="embed-pos")
        assert hook.llm_model == "llm-pos"
        assert hook.embed_model == "embed-pos"

    def test_connection_positional_arguments_do_not_warn(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error", AirflowProviderDeprecationWarning)
            hook = LangChainHook("c1", "c2")
        assert hook.embed_model is None
        assert hook.llm_model is None

    def test_too_many_positional_arguments_raise_type_error(self):
        with warnings.catch_warnings():
            warnings.simplefilter("error", AirflowProviderDeprecationWarning)
            with pytest.raises(TypeError, match="positional argument"):
                LangChainHook("c1", "c2", "llm-pos", "embed-pos", "extra")

    def test_positional_and_keyword_same_parameter_raise_type_error(self):
        with pytest.raises(TypeError, match="multiple values for argument 'llm_model'"):
            LangChainHook("c1", "c2", "llm-pos", llm_model="other")

    def test_second_positional_and_keyword_same_parameter_raise_type_error(self):
        with (
            pytest.warns(AirflowProviderDeprecationWarning),
            pytest.raises(TypeError, match="multiple values for argument 'embed_model'"),
        ):
            LangChainHook("c1", "c2", "L", "E", embed_model="X")

    def test_positional_and_keyword_different_parameters_combine(self):
        with pytest.warns(AirflowProviderDeprecationWarning) as record:
            hook = LangChainHook("c1", "c2", "llm-pos", embed_model="embed-pos")
        assert len(record) == 1
        assert hook.llm_model == "llm-pos"
        assert hook.embed_model == "embed-pos"

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
    def test_dispatches_with_embedding_kwargs(self, mock_get_conn, mock_init_embeddings, caplog):
        mock_get_conn.return_value = _conn(password="sk-test")

        hook = LangChainHook(
            embed_model="openai:Qwen/Qwen3-Embedding-0.6B",
            embedding_kwargs={"api_key": "from-kwargs", "dimensions": 128, "timeout": 30},
        )
        hook.get_embedding_model()

        mock_init_embeddings.assert_called_once_with(
            "openai:Qwen/Qwen3-Embedding-0.6B",
            api_key="sk-test",
            dimensions=128,
            timeout=30,
        )
        assert "Connection parameters override embedding_kwargs values: ['api_key']" in caplog.messages

    @pytest.mark.parametrize(
        ("embed_model", "expect_warning"),
        [
            ("openai:text-embedding-3-small", True),
            ("text-embedding-3-small", False),
        ],
    )
    @patch("langchain.embeddings.init_embeddings")
    @patch.object(LangChainHook, "get_connection")
    def test_embedding_kwargs_provider_warning(
        self, mock_get_conn, mock_init_embeddings, caplog, embed_model, expect_warning
    ):
        mock_get_conn.return_value = _conn()
        mock_init_embeddings.side_effect = _init_embeddings
        hook = LangChainHook(
            embed_model=embed_model,
            embedding_kwargs={"provider": "custom-provider"},
        )

        result = hook.get_embedding_model()

        assert result["model"] == embed_model
        assert result["provider"] == "custom-provider"
        warning = (
            "embedding_kwargs['provider'] takes precedence over the provider prefix in embed_model; "
            "pass an unprefixed model name"
        )
        assert (warning in caplog.messages) is expect_warning

    @patch("langchain.embeddings.init_embeddings")
    @patch.object(LangChainHook, "get_connection")
    def test_model_in_embedding_kwargs_raises(self, mock_get_conn, mock_init_embeddings):
        mock_get_conn.return_value = _conn()
        mock_init_embeddings.side_effect = _init_embeddings
        hook = LangChainHook(
            embed_model="openai:text-embedding-3-small",
            embedding_kwargs={"model": "other-model"},
        )

        with pytest.raises(TypeError, match="multiple values.*model"):
            hook.get_embedding_model()

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


class TestConnectionTest:
    @patch("langchain.chat_models.init_chat_model")
    @patch.object(LangChainHook, "get_connection")
    def test_successful_connection(self, mock_get_conn, mock_init_chat_model):
        mock_get_conn.return_value = _conn(password="sk-test", extra={"model": "openai:gpt-4o"})

        hook = LangChainHook()
        success, message = hook.test_connection()

        assert success is True
        assert message == "Model resolved successfully."

    @patch("langchain.chat_models.init_chat_model")
    @patch.object(LangChainHook, "get_connection")
    def test_failed_connection(self, mock_get_conn, mock_init_chat_model):
        mock_get_conn.return_value = _conn(password="sk-test", extra={"model": "openai:gpt-4o"})
        mock_init_chat_model.side_effect = ValueError("Unknown provider 'badprovider'")

        hook = LangChainHook()
        success, message = hook.test_connection()

        assert success is False
        assert "Unknown provider" in message

    @patch.object(LangChainHook, "get_connection")
    def test_failed_connection_no_model(self, mock_get_conn):
        mock_get_conn.return_value = _conn()

        hook = LangChainHook()
        success, message = hook.test_connection()

        assert success is False
        assert "No chat model identifier set" in message


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
