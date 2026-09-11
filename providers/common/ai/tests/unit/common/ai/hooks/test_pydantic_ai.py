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

import contextlib
import json
import re
import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch, sentinel

import pytest
from pydantic_ai import Embedder
from pydantic_ai.embeddings import EmbeddingModel, EmbeddingSettings, infer_embedding_model
from pydantic_ai.exceptions import UserError
from pydantic_ai.models import Model
from pydantic_ai.models.test import TestModel
from pydantic_ai.providers import infer_provider_class

from airflow.models.connection import Connection
from airflow.providers.common.ai.get_provider_info import get_provider_info
from airflow.providers.common.ai.hooks.pydantic_ai import (
    PydanticAIAzureHook,
    PydanticAIBedrockHook,
    PydanticAIHook,
    PydanticAIVertexHook,
)
from airflow.providers.common.compat.sdk import AirflowNotFoundException

# Matches the `google...` provider key pydantic-ai expects before the `:model-name`
# separator, e.g. "google-cloud" out of "google-cloud:gemini-2.0-flash".
_GOOGLE_MODEL_PREFIX_RE = re.compile(r"google[\w-]*(?=:)")


def _assert_prefix_is_known_provider(prefix: str) -> None:
    """
    Assert pydantic-ai's provider registry recognizes ``prefix``.

    ``infer_provider_class`` raises ``ValueError: Unknown provider: ...`` for a
    name it doesn't recognize, but ``ImportError`` for a recognized name whose
    optional dependency (``google-genai``) isn't installed in this test env.
    Only the former indicates the advertised prefix has drifted out of sync
    with what's actually installed.
    """
    try:
        with contextlib.suppress(ImportError):
            infer_provider_class(prefix)
    except ValueError as exc:
        pytest.fail(f"{prefix!r} is not a recognized pydantic-ai provider: {exc}")


def _assert_embedding_model_is_supported(model_name: str) -> None:
    def create_provider(_: str) -> Any:
        return MagicMock()

    try:
        with contextlib.suppress(ImportError):
            infer_embedding_model(model_name, provider_factory=create_provider)
    except (UserError, ValueError) as exc:
        pytest.fail(f"{model_name!r} is not a recognized pydantic-ai embedding model: {exc}")


def _extract_google_cloud_prefix(text: str) -> str:
    """
    Extract the ``google...`` model prefix out of ``text`` and assert it is exactly
    ``"google-cloud"``.

    ``_GOOGLE_MODEL_PREFIX_RE`` alone would also match the bare ``"google"``
    provider (the Generative Language API, which pydantic-ai also recognizes),
    so a documented prefix that silently regressed from ``google-cloud:`` to
    ``google:`` would still pass a plain "is it a known provider" check. The
    exact-match assertion here is what actually catches that drift.
    """
    match = _GOOGLE_MODEL_PREFIX_RE.search(text)
    assert match, f"no google model prefix found in: {text!r}"
    assert match.group() == "google-cloud", f"expected 'google-cloud' prefix, got {match.group()!r}"
    return match.group()


class TestPydanticAIHookInit:
    def test_default_conn_id(self):
        hook = PydanticAIHook()
        assert hook.llm_conn_id == "pydanticai_default"
        assert hook.embed_conn_id == "pydanticai_default"
        assert hook.model_id is None
        assert hook.embed_model_id is None

    def test_custom_conn_id(self):
        hook = PydanticAIHook(
            llm_conn_id="my_llm",
            model_id="openai:gpt-5.6-sol",
            embed_model_id="openai:text-embedding-3-small",
            embed_conn_id="my_embeddings",
        )
        assert hook.llm_conn_id == "my_llm"
        assert hook.embed_conn_id == "my_embeddings"
        assert hook.model_id == "openai:gpt-5.6-sol"
        assert hook.embed_model_id == "openai:text-embedding-3-small"

    def test_azure_hook_uses_own_default_conn_name(self):
        """Subclass default_conn_name is used, not the base class value."""
        hook = PydanticAIAzureHook()
        assert hook.llm_conn_id == "pydanticai_azure_default"

    def test_bedrock_hook_uses_own_default_conn_name(self):
        hook = PydanticAIBedrockHook()
        assert hook.llm_conn_id == "pydanticai_bedrock_default"

    def test_vertex_hook_uses_own_default_conn_name(self):
        hook = PydanticAIVertexHook()
        assert hook.llm_conn_id == "pydanticai_vertex_default"


class TestPydanticAIHookGetConn:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_with_api_key_and_base_url(self, mock_infer_provider_class, mock_infer_model):
        """Credentials are injected via provider_factory, not as direct kwargs."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model
        mock_provider = MagicMock()
        mock_infer_provider_class.return_value = MagicMock(return_value=mock_provider)

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
            host="https://api.openai.com/v1",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        assert result is mock_model
        mock_infer_model.assert_called_once()
        call_args = mock_infer_model.call_args
        assert call_args[0][0] == "openai:gpt-5.6-sol"
        # provider_factory should be passed as keyword arg
        assert "provider_factory" in call_args[1]

        # Call the factory to verify it creates the provider with credentials
        factory = call_args[1]["provider_factory"]
        factory("openai")
        mock_infer_provider_class.assert_called_with("openai")
        mock_infer_provider_class.return_value.assert_called_with(
            api_key="sk-test-key", base_url="https://api.openai.com/v1"
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_with_model_from_extra(self, mock_infer_provider_class, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model
        mock_infer_provider_class.return_value = MagicMock(return_value=MagicMock())

        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
            extra='{"model": "anthropic:claude-opus-4-6"}',
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        assert result is mock_model
        assert mock_infer_model.call_args[0][0] == "anthropic:claude-opus-4-6"

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_model_id_param_overrides_extra(self, mock_infer_provider_class, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_provider_class.return_value = MagicMock(return_value=MagicMock())

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
            extra='{"model": "anthropic:claude-opus-4-6"}',
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        # model_id param takes priority over extra
        assert mock_infer_model.call_args[0][0] == "openai:gpt-5.6-sol"

    def test_get_conn_raises_when_no_model(self):
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="No model specified"):
                hook.get_conn()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_conn_without_credentials_uses_default_provider(self, mock_infer_model):
        """No api_key or base_url means env-based auth (Bedrock, Vertex, etc.)."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="bedrock:us.anthropic.claude-v2")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        # No provider_factory — uses default infer_provider which reads env vars
        mock_infer_model.assert_called_once_with("bedrock:us.anthropic.claude-v2")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_with_base_url_only(self, mock_infer_provider_class, mock_infer_model):
        """Ollama / vLLM: base_url but no API key."""
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_provider_class.return_value = MagicMock(return_value=MagicMock())

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:llama3")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            host="http://localhost:11434/v1",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        # provider_factory should be used since base_url is set
        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("openai")
        mock_infer_provider_class.return_value.assert_called_with(base_url="http://localhost:11434/v1")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_conn_caches_model(self, mock_infer_model):
        """get_conn() should resolve the model once and cache it."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            first = hook.get_conn()
            second = hook.get_conn()

        assert first is second
        mock_infer_model.assert_called_once()

    @pytest.mark.parametrize(
        ("model_name", "provider_name", "replacement_fields"),
        [
            (
                "bedrock:us.anthropic.claude-opus-4-6-v1:0",
                "bedrock",
                ["api_key", "base_url", "region_name"],
            ),
            ("google:gemini-2.0-flash", "google", ["api_key", "base_url"]),
            (
                "google-cloud:gemini-2.0-flash",
                "google-cloud",
                ["api_key", "base_url", "project", "location"],
            ),
        ],
    )
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_generic_connection_warns_when_password_and_host_are_ignored(
        self, mock_infer_model, model_name, provider_name, replacement_fields
    ):
        mock_infer_model.return_value = MagicMock(spec=Model)
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="provider-key",
            host="https://provider.example.com",
            extra=json.dumps({"model": model_name}),
        )

        with (
            patch.object(hook, "get_connection", return_value=conn),
            patch.object(hook.log, "warning", autospec=True) as mock_warning,
        ):
            hook.get_conn()

        mock_warning.assert_called_once_with(
            "Connection fields are ignored for provider; configure provider-specific values in extra",
            conn_id="test_conn",
            provider=provider_name,
            ignored_fields=["password", "host"],
            replacement_fields=replacement_fields,
        )


class TestPydanticAIHookGetEmbedder:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_embedding_uses_model_provider_mapping_instead_of_hook_type(
        self, mock_infer_provider_class, mock_infer_embedding_model
    ):
        mock_embedding_model = MagicMock(spec=EmbeddingModel)
        mock_infer_embedding_model.return_value = mock_embedding_model
        mock_provider = mock_infer_provider_class.return_value.return_value
        hook = PydanticAIAzureHook(embed_conn_id="test_conn", embed_model_id="openai:text-embedding-3-small")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            password="sk-test-key",
            host="https://api.openai.com/v1",
        )

        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_embedder()

        assert isinstance(result, Embedder)
        assert result.model is mock_embedding_model
        call_args = mock_infer_embedding_model.call_args
        assert call_args.args == ("openai:text-embedding-3-small",)
        provider_factory = call_args.kwargs["provider_factory"]
        assert provider_factory("openai") is mock_provider
        mock_infer_provider_class.return_value.assert_called_once_with(
            api_key="sk-test-key", base_url="https://api.openai.com/v1"
        )

    @pytest.mark.parametrize(
        ("provider_name", "extra", "expected_provider_kwargs"),
        [
            ("openai", {}, {"api_key": "connection-key", "base_url": "https://example.com"}),
            (
                "azure",
                {"api_version": "2024-07-01-preview"},
                {
                    "api_key": "connection-key",
                    "azure_endpoint": "https://example.com",
                    "api_version": "2024-07-01-preview",
                },
            ),
            (
                "azure-responses",
                {"api_version": "2024-07-01-preview"},
                {
                    "api_key": "connection-key",
                    "azure_endpoint": "https://example.com",
                    "api_version": "2024-07-01-preview",
                },
            ),
            (
                "bedrock",
                {"region_name": "us-east-1"},
                {"region_name": "us-east-1"},
            ),
            (
                "google",
                {
                    "api_key": "extra-key",
                    "base_url": "https://extra.example.com",
                    "project": "project",
                },
                {
                    "api_key": "extra-key",
                    "base_url": "https://extra.example.com",
                },
            ),
            (
                "google-cloud",
                {
                    "api_key": "extra-key",
                    "base_url": "https://extra.example.com",
                    "project": "project",
                },
                {
                    "api_key": "extra-key",
                    "base_url": "https://extra.example.com",
                    "project": "project",
                },
            ),
        ],
    )
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_embedding_credentials_are_mapped_by_model_provider(
        self,
        mock_infer_embedding_model,
        mock_infer_provider_class,
        provider_name,
        extra,
        expected_provider_kwargs,
    ):
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(embed_model_id=f"{provider_name}:embedding-model")
        conn = Connection(
            conn_id="pydanticai_default",
            conn_type="pydanticai",
            password="connection-key",
            host="https://example.com",
            extra=json.dumps(extra),
        )

        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_embedder()

        provider_factory = mock_infer_embedding_model.call_args.kwargs["provider_factory"]
        provider_factory(provider_name)
        mock_infer_provider_class.return_value.assert_called_once_with(**expected_provider_kwargs)

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_embedder_reports_connection_fields_rejected_by_provider(
        self, mock_infer_provider_class, mock_infer_embedding_model
    ):
        mock_infer_provider_class.return_value.side_effect = TypeError("unexpected credential")
        mock_infer_embedding_model.side_effect = lambda model_name, *, provider_factory: provider_factory(
            "openai"
        )
        hook = PydanticAIHook(embed_conn_id="embedding_conn", embed_model_id="openai:text-embedding-3-small")
        conn = Connection(
            conn_id="embedding_conn",
            conn_type="pydanticai",
            password="connection-key",
        )

        with (
            patch.object(hook, "get_connection", return_value=conn),
            pytest.raises(TypeError, match="embedding_conn.*api_key"),
        ):
            hook.get_embedder()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_mixed_providers_require_separate_embedding_connection(self, mock_infer_embedding_model):
        hook = PydanticAIHook(embed_model_id="cohere:embed-english-v3.0")
        conn = Connection(
            conn_id="pydanticai_default",
            conn_type="pydanticai",
            password="openai-key",
            extra='{"model": "openai:gpt-5.6-sol"}',
        )

        with (
            patch.object(hook, "get_connection", return_value=conn),
            pytest.raises(ValueError, match="Set embed_conn_id"),
        ):
            hook.get_embedder()

        mock_infer_embedding_model.assert_not_called()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_local_embedding_does_not_receive_connection_credentials(self, mock_infer_embedding_model):
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(embed_model_id="sentence-transformers:all-MiniLM-L6-v2")
        conn = Connection(
            conn_id="pydanticai_default",
            conn_type="pydanticai",
            password="chat-key",
            host="https://chat.example.com",
            extra='{"model": "openai:gpt-5.6-sol"}',
        )

        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_embedder()

        mock_infer_embedding_model.assert_called_once_with("sentence-transformers:all-MiniLM-L6-v2")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.genai_instrumentation_settings")
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_get_embedder_uses_instrumentation(self, mock_infer_embedding_model, mock_settings):
        mock_settings.return_value = sentinel.instrumentation_settings
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(embed_model_id="openai:text-embedding-3-small")
        conn = Connection(conn_id="pydanticai_default", conn_type="pydanticai")

        with patch.object(hook, "get_connection", return_value=conn):
            embedder = hook.get_embedder()

        mock_settings.assert_called_once_with()
        assert embedder.instrument is sentinel.instrumentation_settings

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.genai_instrumentation_settings")
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Embedder", autospec=True)
    def test_get_embedder_forwards_settings_and_preserves_caller_instrumentation(
        self, mock_embedder, mock_infer_embedding_model, mock_settings
    ):
        embedding_model = MagicMock(spec=EmbeddingModel)
        mock_infer_embedding_model.return_value = embedding_model
        settings = EmbeddingSettings(dimensions=512)
        hook = PydanticAIHook(embed_model_id="openai:text-embedding-3-small")
        conn = Connection(conn_id="pydanticai_default", conn_type="pydanticai")

        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_embedder(settings=settings, defer_model_check=False, instrument=False)

        mock_embedder.assert_called_once_with(
            embedding_model,
            settings=settings,
            defer_model_check=False,
            instrument=False,
        )
        mock_settings.assert_not_called()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.genai_instrumentation_settings")
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Embedder", autospec=True)
    def test_get_embedder_omits_instrument_when_automatic_instrumentation_is_disabled(
        self, mock_embedder, mock_infer_embedding_model, mock_settings
    ):
        embedding_model = MagicMock(spec=EmbeddingModel)
        mock_infer_embedding_model.return_value = embedding_model
        mock_settings.return_value = None
        hook = PydanticAIHook(embed_model_id="openai:text-embedding-3-small")
        conn = Connection(conn_id="pydanticai_default", conn_type="pydanticai")

        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_embedder()

        mock_embedder.assert_called_once_with(embedding_model)

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_get_embedder_uses_embed_conn_id(self, mock_infer_embedding_model):
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(llm_conn_id="chat_conn", embed_conn_id="embedding_conn")
        embedding_conn = Connection(
            conn_id="embedding_conn",
            conn_type="pydanticai",
            extra='{"embed_model": "openai:text-embedding-3-small"}',
        )

        with patch.object(hook, "get_connection", return_value=embedding_conn) as mock_get_connection:
            hook.get_embedder()

        mock_get_connection.assert_called_once_with("embedding_conn")
        mock_infer_embedding_model.assert_called_once_with("openai:text-embedding-3-small")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_get_embedder_without_credentials_uses_default_provider(self, mock_infer_embedding_model):
        mock_embedding_model = MagicMock(spec=EmbeddingModel)
        mock_infer_embedding_model.return_value = mock_embedding_model
        hook = PydanticAIHook(llm_conn_id="test_conn", embed_model_id="openai:text-embedding-3-small")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")

        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_embedder()

        assert isinstance(result, Embedder)
        assert result.model is mock_embedding_model
        mock_infer_embedding_model.assert_called_once_with("openai:text-embedding-3-small")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_embedder_with_base_url_only(
        self,
        mock_infer_provider_class,
        mock_infer_embedding_model,
    ):
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(llm_conn_id="test_conn", embed_model_id="openai:text-embedding-3-small")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            host="http://localhost:8000/v1",
        )

        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_embedder()

        provider_factory = mock_infer_embedding_model.call_args.kwargs["provider_factory"]
        provider_factory("openai")
        mock_infer_provider_class.return_value.assert_called_once_with(base_url="http://localhost:8000/v1")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_get_embedder_with_model_from_extra(self, mock_infer_embedding_model):
        mock_embedding_model = MagicMock(spec=EmbeddingModel)
        mock_infer_embedding_model.return_value = mock_embedding_model
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            extra='{"embed_model": "openai:text-embedding-3-large"}',
        )

        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_embedder()

        assert isinstance(result, Embedder)
        assert result.model is mock_embedding_model
        mock_infer_embedding_model.assert_called_once_with("openai:text-embedding-3-large")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_embed_model_id_param_overrides_extra(self, mock_infer_embedding_model):
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(llm_conn_id="test_conn", embed_model_id="openai:text-embedding-3-small")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            extra='{"embed_model": "openai:text-embedding-3-large"}',
        )

        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_embedder()

        assert mock_infer_embedding_model.call_args.args == ("openai:text-embedding-3-small",)

    def test_get_embedder_raises_when_no_model(self):
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")

        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="No embedding model specified"):
                hook.get_embedder()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_get_embedder_caches_embedder(self, mock_infer_embedding_model):
        mock_embedding_model = MagicMock(spec=EmbeddingModel)
        mock_infer_embedding_model.return_value = mock_embedding_model
        hook = PydanticAIHook(llm_conn_id="test_conn", embed_model_id="openai:text-embedding-3-small")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")

        with patch.object(hook, "get_connection", return_value=conn):
            first = hook.get_embedder()
            second = hook.get_embedder()

        assert first is second
        assert first.model is mock_embedding_model
        mock_infer_embedding_model.assert_called_once()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_get_embedder_caches_embedder_for_same_kwargs(self, mock_infer_embedding_model):
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(embed_model_id="openai:text-embedding-3-small")
        conn = Connection(conn_id="pydanticai_default", conn_type="pydanticai")

        with patch.object(hook, "get_connection", return_value=conn):
            first = hook.get_embedder(settings=EmbeddingSettings(dimensions=512))
            second = hook.get_embedder(settings=EmbeddingSettings(dimensions=512))

        assert first is second
        mock_infer_embedding_model.assert_called_once()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_get_embedder_rebuilds_cached_embedder_for_different_kwargs(self, mock_infer_embedding_model):
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(embed_model_id="openai:text-embedding-3-small")
        conn = Connection(conn_id="pydanticai_default", conn_type="pydanticai")

        with patch.object(hook, "get_connection", return_value=conn):
            first = hook.get_embedder(settings=EmbeddingSettings(dimensions=512))
            second = hook.get_embedder(settings=EmbeddingSettings(dimensions=256))

        assert first is not second
        assert mock_infer_embedding_model.call_count == 2

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Embedder", autospec=True)
    def test_failed_embedder_rebuild_does_not_update_cache_kwargs(
        self, mock_embedder, mock_infer_embedding_model
    ):
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        mock_embedder.side_effect = [
            sentinel.first_embedder,
            TypeError("invalid settings"),
            sentinel.second_embedder,
        ]
        hook = PydanticAIHook(embed_model_id="openai:text-embedding-3-small")
        conn = Connection(conn_id="pydanticai_default", conn_type="pydanticai")
        first_settings = EmbeddingSettings(dimensions=512)
        second_settings = EmbeddingSettings(dimensions=256)

        with patch.object(hook, "get_connection", return_value=conn):
            first = hook.get_embedder(settings=first_settings)
            with pytest.raises(TypeError, match="invalid settings"):
                hook.get_embedder(settings=second_settings)
            second = hook.get_embedder(settings=second_settings)

        assert first is sentinel.first_embedder
        assert second is sentinel.second_embedder
        assert mock_embedder.call_count == 3


class TestPydanticAIHookCreateAgent:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_defaults(self, mock_agent_cls, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(instructions="You are a helpful assistant.")

        mock_agent_cls.assert_called_once_with(
            mock_model,
            output_type=str,
            instructions="You are a helpful assistant.",
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_with_params(self, mock_agent_cls, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                output_type=dict,
                instructions="Be helpful.",
                retries=3,
            )

        mock_agent_cls.assert_called_once_with(
            mock_model,
            output_type=dict,
            instructions="Be helpful.",
            retries=3,
        )

    def test_create_agent_without_instructions_or_spec_file_raises(self):
        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        with pytest.raises(ValueError, match="instructions is required"):
            hook.create_agent()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_calls_from_file(self, mock_agent_cls, mock_infer_model):
        """spec_file routes to Agent.from_file with the hook model when configured."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(spec_file="/path/to/agent.yaml")

        mock_agent_cls.from_file.assert_called_once_with(
            "/path/to/agent.yaml",
            model=mock_model,
            output_type=str,
        )
        mock_agent_cls.assert_not_called()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_uses_file_model_when_hook_model_not_configured(
        self, mock_agent_cls, mock_infer_model
    ):
        """spec_file model is used when neither model_id nor connection model is configured."""
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(spec_file="/path/to/agent.yaml")

        mock_infer_model.assert_not_called()
        mock_agent_cls.from_file.assert_called_once_with(
            "/path/to/agent.yaml",
            output_type=str,
        )
        mock_agent_cls.assert_not_called()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_path_object(self, mock_agent_cls, mock_infer_model):
        """spec_file accepts a pathlib.Path object."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        spec_path = Path("/path/to/agent.yaml")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(spec_file=spec_path)

        mock_agent_cls.from_file.assert_called_once_with(
            spec_path,
            model=mock_model,
            output_type=str,
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_merges_additional_instructions(
        self, mock_agent_cls, mock_infer_model
    ):
        """Explicit instructions are forwarded so pydantic-ai merges them with the spec."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                spec_file="/path/to/agent.yaml",
                instructions="Override instructions.",
            )

        mock_agent_cls.from_file.assert_called_once_with(
            "/path/to/agent.yaml",
            model=mock_model,
            output_type=str,
            instructions="Override instructions.",
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_custom_output_type(self, mock_agent_cls, mock_infer_model):
        """output_type is forwarded to Agent.from_file."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(output_type=dict, spec_file="/path/to/agent.yaml")

        mock_agent_cls.from_file.assert_called_once_with(
            "/path/to/agent.yaml",
            model=mock_model,
            output_type=dict,
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent")
    def test_create_agent_with_spec_file_forwards_agent_kwargs(self, mock_agent_cls, mock_infer_model):
        """Extra agent_kwargs are forwarded to Agent.from_file."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                spec_file="/path/to/agent.yaml",
                retries=5,
                end_strategy="early",
            )

        mock_agent_cls.from_file.assert_called_once_with(
            "/path/to/agent.yaml",
            model=mock_model,
            output_type=str,
            retries=5,
            end_strategy="early",
        )


class TestPydanticAIHookCreateAgentInstrumentation:
    """create_agent() wires OpenTelemetry instrumentation from observability."""

    @staticmethod
    def _hook() -> PydanticAIHook:
        return PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.genai_instrumentation_settings")
    def test_instrument_set_when_settings_returned(self, mock_settings):
        sentinel = MagicMock(name="InstrumentationSettings")
        mock_settings.return_value = sentinel
        hook = self._hook()
        with patch.object(hook, "get_conn", return_value=TestModel()):
            agent = hook.create_agent(instructions="hi")

        assert agent.instrument is sentinel

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.genai_instrumentation_settings")
    def test_no_instrument_when_settings_none(self, mock_settings):
        mock_settings.return_value = None
        hook = self._hook()
        with patch.object(hook, "get_conn", return_value=TestModel()):
            agent = hook.create_agent(instructions="hi")

        mock_settings.assert_called_once()
        assert agent.instrument is None

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.genai_instrumentation_settings")
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_caller_instrument_short_circuits(self, mock_infer_model, mock_settings, mock_agent_cls):
        """A caller that passes its own ``instrument`` wins; we don't override it."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model
        hook = self._hook()
        conn = Connection(conn_id="test_conn", conn_type="pydanticai")
        with patch.object(hook, "get_connection", return_value=conn):
            agent = hook.create_agent(instructions="hi", instrument=False)

        # ``instrument`` is not an Agent() constructor kwarg in pydantic-ai 2.x:
        # it must be stripped from the constructor call and applied through the
        # ``agent.instrument`` property instead, and the provider's own
        # auto-instrumentation must not override the caller's value.
        mock_agent_cls.assert_called_once_with(
            mock_model,
            output_type=str,
            instructions="hi",
        )
        assert agent.instrument is False
        mock_settings.assert_not_called()


class TestPydanticAIHookTestConnection:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_successful_connection(self, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.6-sol")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is True
        assert message == "Model resolved successfully."

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_successful_embedding_only_connection(self, mock_infer_embedding_model):
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            extra='{"embed_model": "openai:text-embedding-3-small"}',
        )

        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is True
        assert message == "Embedding model resolved successfully."

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    def test_successful_embedding_only_connection_without_default_llm_connection(
        self, mock_infer_embedding_model
    ):
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(embed_conn_id="embedding_conn", embed_model_id="openai:text-embedding-3-small")
        embedding_conn = Connection(conn_id="embedding_conn", conn_type="pydanticai")

        def get_connection(conn_id):
            if conn_id == "pydanticai_default":
                raise AirflowNotFoundException("missing default LLM connection")
            return embedding_conn

        with patch.object(hook, "get_connection", side_effect=get_connection) as mock_get_connection:
            success, message = hook.test_connection()

        assert success is True
        assert message == "Embedding model resolved successfully."
        assert [call.args for call in mock_get_connection.call_args_list] == [
            ("pydanticai_default",),
            ("embedding_conn",),
        ]

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_successful_connection_with_both_models(self, mock_infer_model, mock_infer_embedding_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
            extra=('{"model": "openai:gpt-5.6-sol", "embed_model": "openai:text-embedding-3-small"}'),
        )

        with patch.object(hook, "get_connection", return_value=conn) as mock_get_connection:
            success, message = hook.test_connection()

        assert success is True
        assert message == "Model and embedding model resolved successfully."
        mock_get_connection.assert_called_once_with("test_conn")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_connection_caches_distinct_model_connections(self, mock_infer_model, mock_infer_embedding_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_embedding_model.return_value = MagicMock(spec=EmbeddingModel)
        hook = PydanticAIHook(llm_conn_id="chat_conn", embed_conn_id="embedding_conn")
        connections = {
            "chat_conn": Connection(
                conn_id="chat_conn",
                conn_type="pydanticai",
                extra=json.dumps({"model": "openai:gpt-5.6-sol"}),
            ),
            "embedding_conn": Connection(
                conn_id="embedding_conn",
                conn_type="pydanticai",
                extra=json.dumps({"embed_model": "openai:text-embedding-3-small"}),
            ),
        }

        with patch.object(hook, "get_connection", side_effect=connections.__getitem__) as mock_get_connection:
            success, message = hook.test_connection()

        assert success is True
        assert message == "Model and embedding model resolved successfully."
        assert [call.args for call in mock_get_connection.call_args_list] == [
            ("chat_conn",),
            ("embedding_conn",),
        ]

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_embedding_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_failed_connection_with_valid_model_and_invalid_embedding_model(
        self, mock_infer_model, mock_infer_embedding_model
    ):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_embedding_model.side_effect = ValueError("Unknown provider 'badprovider'")
        hook = PydanticAIHook(llm_conn_id="test_conn", embed_conn_id="embedding_conn")
        connections = {
            "test_conn": Connection(
                conn_id="test_conn",
                conn_type="pydanticai",
                extra='{"model": "openai:gpt-5.6-sol"}',
            ),
            "embedding_conn": Connection(
                conn_id="embedding_conn",
                conn_type="pydanticai",
                extra='{"embed_model": "badprovider:model"}',
            ),
        }

        with patch.object(hook, "get_connection", side_effect=connections.__getitem__):
            success, message = hook.test_connection()

        assert success is False
        assert "Unknown provider" in message

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_failed_connection(self, mock_infer_model):
        mock_infer_model.side_effect = ValueError("Unknown provider 'badprovider'")

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="badprovider:model")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is False
        assert "Unknown provider" in message

    def test_failed_connection_no_model(self):
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydanticai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is False
        assert message == (
            "No model or embedding model specified. Set model_id or embed_model_id on the hook, "
            "or the model or embed_model field on the connection."
        )


# ---------------------------------------------------------------------------
# Subclass hook tests
# ---------------------------------------------------------------------------


class TestPydanticAIAzureHook:
    """Tests for PydanticAIAzureHook."""

    def test_conn_type(self):
        assert PydanticAIAzureHook.conn_type == "pydanticai_azure"

    def test_hook_name(self):
        assert "Azure" in PydanticAIAzureHook.hook_name

    def test_ui_field_behaviour_relabels_host(self):
        behaviour = PydanticAIAzureHook.get_ui_field_behaviour()
        assert behaviour["relabeling"].get("host") == "Azure Endpoint"

    def test_get_provider_kwargs_maps_azure_endpoint(self):
        hook = PydanticAIAzureHook.__new__(PydanticAIAzureHook)
        result = hook._get_provider_kwargs(
            "my-key",
            "https://myresource.openai.azure.com",
            {"model": "azure:gpt-4o", "api_version": "2024-07-01-preview"},
        )
        assert result["azure_endpoint"] == "https://myresource.openai.azure.com"
        assert result["api_key"] == "my-key"
        assert result["api_version"] == "2024-07-01-preview"
        assert "base_url" not in result

    def test_get_provider_kwargs_omits_none_api_key(self):
        hook = PydanticAIAzureHook.__new__(PydanticAIAzureHook)
        result = hook._get_provider_kwargs(
            None,
            "https://myresource.openai.azure.com",
            {"model": "azure:gpt-4o", "api_version": "2024-07-01-preview"},
        )
        assert "api_key" not in result
        assert result["azure_endpoint"] == "https://myresource.openai.azure.com"

    def test_get_provider_kwargs_omits_azure_endpoint_when_no_host(self):
        hook = PydanticAIAzureHook.__new__(PydanticAIAzureHook)
        result = hook._get_provider_kwargs(
            "my-key",
            None,
            {"model": "azure:gpt-4o", "api_version": "2024-07-01-preview"},
        )
        assert "azure_endpoint" not in result
        assert result["api_key"] == "my-key"

    def test_get_provider_kwargs_empty_without_api_version(self):
        hook = PydanticAIAzureHook.__new__(PydanticAIAzureHook)
        result = hook._get_provider_kwargs(
            "my-key",
            "https://myresource.openai.azure.com",
            {"model": "azure:gpt-4o"},
        )
        # api_version should not appear if not in extra
        assert "api_version" not in result

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_uses_azure_endpoint(self, mock_infer_provider_class, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_provider_cls = MagicMock(return_value=MagicMock())
        mock_infer_provider_class.return_value = mock_provider_cls

        hook = PydanticAIAzureHook(llm_conn_id="azure_test")
        conn = Connection(
            conn_id="azure_test",
            conn_type="pydanticai_azure",
            password="azure-key",
            host="https://myresource.openai.azure.com",
            extra=json.dumps({"model": "azure:gpt-4o", "api_version": "2024-07-01-preview"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("azure")
        mock_provider_cls.assert_called_with(
            api_key="azure-key",
            azure_endpoint="https://myresource.openai.azure.com",
            api_version="2024-07-01-preview",
        )

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_conn_falls_back_to_env_auth_when_no_kwargs(self, mock_infer_model):
        """No host + no password → env-var auth path (empty _get_provider_kwargs)."""
        mock_infer_model.return_value = MagicMock(spec=Model)
        hook = PydanticAIAzureHook(llm_conn_id="azure_test")
        conn = Connection(
            conn_id="azure_test",
            conn_type="pydanticai_azure",
            extra=json.dumps({"model": "azure:gpt-4o"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_infer_model.assert_called_once_with("azure:gpt-4o")


class TestPydanticAIBedrockHook:
    """Tests for PydanticAIBedrockHook."""

    def test_conn_type(self):
        assert PydanticAIBedrockHook.conn_type == "pydanticai_bedrock"

    def test_hook_name(self):
        assert "Bedrock" in PydanticAIBedrockHook.hook_name

    def test_ui_hides_host_and_password(self):
        behaviour = PydanticAIBedrockHook.get_ui_field_behaviour()
        assert "host" in behaviour["hidden_fields"]
        assert "password" in behaviour["hidden_fields"]

    def test_get_provider_kwargs_maps_bedrock_fields(self):
        hook = PydanticAIBedrockHook.__new__(PydanticAIBedrockHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {
                "model": "bedrock:us.anthropic.claude-opus-4-5",
                "region_name": "us-east-1",
                "aws_access_key_id": "AKIA123",
                "aws_secret_access_key": "secret",
            },
        )
        assert result["region_name"] == "us-east-1"
        assert result["aws_access_key_id"] == "AKIA123"
        assert result["aws_secret_access_key"] == "secret"
        assert "model" not in result
        assert "api_key" not in result

    def test_get_provider_kwargs_returns_empty_for_env_auth(self):
        """When no keys are in extra, return {} so env-auth path is taken."""
        hook = PydanticAIBedrockHook.__new__(PydanticAIBedrockHook)
        result = hook._get_provider_kwargs(None, None, {"model": "bedrock:us.anthropic.claude-opus-4-5"})
        assert result == {}

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_conn_falls_back_to_env_auth(self, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        hook = PydanticAIBedrockHook(llm_conn_id="bedrock_test")
        conn = Connection(
            conn_id="bedrock_test",
            conn_type="pydanticai_bedrock",
            extra=json.dumps({"model": "bedrock:us.anthropic.claude-opus-4-5"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_infer_model.assert_called_once_with("bedrock:us.anthropic.claude-opus-4-5")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_uses_explicit_keys(self, mock_infer_provider_class, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_provider_cls = MagicMock(return_value=MagicMock())
        mock_infer_provider_class.return_value = mock_provider_cls

        hook = PydanticAIBedrockHook(llm_conn_id="bedrock_test")
        conn = Connection(
            conn_id="bedrock_test",
            conn_type="pydanticai_bedrock",
            extra=json.dumps(
                {
                    "model": "bedrock:us.anthropic.claude-opus-4-5",
                    "region_name": "eu-west-1",
                    "aws_access_key_id": "AKIA123",
                    "aws_secret_access_key": "secret",
                }
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("bedrock")
        mock_provider_cls.assert_called_with(
            region_name="eu-west-1",
            aws_access_key_id="AKIA123",
            aws_secret_access_key="secret",
        )

    def test_get_provider_kwargs_bearer_token(self):
        """api_key in extra maps to BedrockProvider's bearer-token param."""
        hook = PydanticAIBedrockHook.__new__(PydanticAIBedrockHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {
                "model": "bedrock:us.anthropic.claude-opus-4-5",
                "api_key": "bearer-token-value",
                "region_name": "us-east-1",
            },
        )
        assert result["api_key"] == "bearer-token-value"
        assert result["region_name"] == "us-east-1"
        assert "aws_access_key_id" not in result

    def test_get_provider_kwargs_base_url(self):
        """base_url in extra is forwarded to BedrockProvider."""
        hook = PydanticAIBedrockHook.__new__(PydanticAIBedrockHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {
                "model": "bedrock:us.anthropic.claude-opus-4-5",
                "base_url": "https://custom-bedrock.example.com",
            },
        )
        assert result["base_url"] == "https://custom-bedrock.example.com"

    def test_get_provider_kwargs_float_timeouts(self):
        """Timeout values are coerced to float (JSON delivers them as int)."""
        hook = PydanticAIBedrockHook.__new__(PydanticAIBedrockHook)
        result = hook._get_provider_kwargs(
            None,
            None,
            {
                "model": "bedrock:us.anthropic.claude-opus-4-5",
                "aws_read_timeout": 60,  # int from JSON
                "aws_connect_timeout": 10.5,  # float already
            },
        )
        assert result["aws_read_timeout"] == 60.0
        assert isinstance(result["aws_read_timeout"], float)
        assert result["aws_connect_timeout"] == 10.5
        assert isinstance(result["aws_connect_timeout"], float)


class TestPydanticAIVertexHook:
    """Tests for PydanticAIVertexHook."""

    def test_conn_type(self):
        assert PydanticAIVertexHook.conn_type == "pydanticai_vertex"

    def test_hook_name(self):
        assert "Vertex" in PydanticAIVertexHook.hook_name

    def test_ui_hides_host_and_password(self):
        behaviour = PydanticAIVertexHook.get_ui_field_behaviour()
        assert "host" in behaviour["hidden_fields"]
        assert "password" in behaviour["hidden_fields"]

    def test_get_google_cloud_provider_kwargs_maps_vertex_fields(self):
        """project and location are passed directly; api_key absent when not in extra."""
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        result = hook._get_google_cloud_provider_kwargs(
            None,
            None,
            {
                "model": "google-cloud:gemini-2.0-flash",
                "project": "my-project",
                "location": "us-central1",
            },
        )
        assert result["project"] == "my-project"
        assert result["location"] == "us-central1"
        assert "model" not in result
        assert "api_key" not in result
        assert "project_id" not in result

    def test_get_google_provider_kwargs_api_key_gla_mode(self):
        """api_key in extra is forwarded for Generative Language API mode."""
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        result = hook._get_google_provider_kwargs(
            None,
            None,
            {"model": "google:gemini-2.0-flash", "api_key": "gla-key"},
        )
        assert result["api_key"] == "gla-key"

    def test_get_google_provider_kwargs_excludes_vertex_fields(self):
        result = PydanticAIVertexHook._get_google_provider_kwargs(
            None,
            None,
            {
                "api_key": "gla-key",
                "base_url": "https://google.example.com",
                "project": "my-project",
                "location": "us-central1",
            },
        )

        assert result == {"api_key": "gla-key", "base_url": "https://google.example.com"}

    @pytest.mark.parametrize("vertexai_value", [True, False])
    def test_get_google_cloud_provider_kwargs_vertexai_flag_is_not_forwarded(self, vertexai_value):
        """The ``vertexai`` extra field must never reach the provider constructor.

        Neither ``GoogleProvider`` nor ``GoogleCloudProvider`` in current pydantic-ai
        accept a ``vertexai`` kwarg (pydantic/pydantic-ai#5336 hardcoded it inside
        ``GoogleCloudProvider`` instead). Forwarding it raises ``TypeError``, which the
        base hook's fallback then swallows by dropping every other kwarg -- silently
        re-resolving credentials from the environment. Regression test for that bug.
        """
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        result = hook._get_google_cloud_provider_kwargs(
            None,
            None,
            {
                "model": "google-cloud:gemini-2.0-flash",
                "project": "my-project",
                "location": "us-central1",
                "vertexai": vertexai_value,
            },
        )
        assert "vertexai" not in result
        # The other credential kwargs must still go through untouched.
        assert result["project"] == "my-project"
        assert result["location"] == "us-central1"

    def test_get_google_cloud_provider_kwargs_service_account_info_loads_credentials(self):
        """service_account_info dict is loaded into a Credentials object."""
        mock_sa = MagicMock()
        mock_creds = MagicMock()
        mock_sa.Credentials.from_service_account_info.return_value = mock_creds

        mock_google_oauth2 = MagicMock()
        mock_google_oauth2.service_account = mock_sa

        sa_info_dict = {"type": "service_account", "project_id": "my-project", "private_key": "..."}
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        with patch.dict(
            sys.modules,
            {
                "google": MagicMock(),
                "google.oauth2": mock_google_oauth2,
                "google.oauth2.service_account": mock_sa,
            },
        ):
            result = hook._get_google_cloud_provider_kwargs(
                None,
                None,
                {
                    "model": "google-cloud:gemini-2.0-flash",
                    "service_account_info": sa_info_dict,
                },
            )

        mock_sa.Credentials.from_service_account_info.assert_called_once_with(
            sa_info_dict,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        assert result["credentials"] is mock_creds
        assert "service_account_info" not in result

    def test_get_google_cloud_provider_kwargs_returns_empty_for_adc(self):
        """When no keys are in extra, return {} so ADC path is taken."""
        hook = PydanticAIVertexHook.__new__(PydanticAIVertexHook)
        result = hook._get_google_cloud_provider_kwargs(
            None, None, {"model": "google-cloud:gemini-2.0-flash"}
        )
        assert result == {}

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_get_conn_falls_back_to_adc(self, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        hook = PydanticAIVertexHook(llm_conn_id="vertex_test")
        conn = Connection(
            conn_id="vertex_test",
            conn_type="pydanticai_vertex",
            extra=json.dumps({"model": "google-cloud:gemini-2.0-flash"}),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        mock_infer_model.assert_called_once_with("google-cloud:gemini-2.0-flash")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_uses_explicit_project(self, mock_infer_provider_class, mock_infer_model):
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_provider_cls = MagicMock(return_value=MagicMock())
        mock_infer_provider_class.return_value = mock_provider_cls

        hook = PydanticAIVertexHook(llm_conn_id="vertex_test")
        conn = Connection(
            conn_id="vertex_test",
            conn_type="pydanticai_vertex",
            extra=json.dumps(
                {
                    "model": "google-cloud:gemini-2.0-flash",
                    "project": "my-project",
                    "location": "europe-west4",
                }
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        factory("google-cloud")
        mock_provider_cls.assert_called_with(project="my-project", location="europe-west4")

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_google_uses_google_provider_signature(
        self, mock_infer_provider_class, mock_infer_model
    ):
        class FakeGoogleProvider:
            def __init__(
                self,
                *,
                api_key=None,
                base_url=None,
                client=None,
                http_client=None,
                retry_options=None,
            ):
                self.api_key = api_key
                self.base_url = base_url

        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_provider_class.return_value = FakeGoogleProvider
        hook = PydanticAIVertexHook(model_id="google:gemini-2.0-flash")
        conn = Connection(
            conn_id="pydanticai_vertex_default",
            conn_type="pydanticai-vertex",
            extra=json.dumps(
                {
                    "api_key": "gla-key",
                    "base_url": "https://google.example.com",
                    "project": "my-project",
                    "location": "us-central1",
                }
            ),
        )

        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        factory = mock_infer_model.call_args.kwargs["provider_factory"]
        provider = factory("google")

        assert isinstance(provider, FakeGoogleProvider)
        assert provider.api_key == "gla-key"
        assert provider.base_url == "https://google.example.com"

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_vertexai_flag_is_not_forwarded(self, mock_infer_provider_class, mock_infer_model):

        class FakeGoogleCloudProvider:
            def __init__(
                self,
                *,
                api_key=None,
                credentials=None,
                project=None,
                location=None,
                client=None,
                http_client=None,
                base_url=None,
                retry_options=None,
            ):
                self.kwargs = {
                    "api_key": api_key,
                    "credentials": credentials,
                    "project": project,
                    "location": location,
                }

        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_infer_provider_class.return_value = FakeGoogleCloudProvider

        hook = PydanticAIVertexHook(llm_conn_id="vertex_test")
        conn = Connection(
            conn_id="vertex_test",
            conn_type="pydanticai_vertex",
            extra=json.dumps(
                {
                    "model": "google-cloud:gemini-2.0-flash",
                    "project": "my-project",
                    "location": "us-central1",
                    "vertexai": True,
                }
            ),
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        provider = factory("google-cloud")

        assert isinstance(provider, FakeGoogleCloudProvider)
        assert provider.kwargs["project"] == "my-project"
        assert provider.kwargs["location"] == "us-central1"

    def test_documented_model_prefix_is_a_valid_pydantic_ai_provider(self):
        """Regression test: the model-prefix documented in the connection form and
        docstrings must be a provider id pydantic-ai actually recognizes (see
        pydantic/pydantic-ai#5336, which renamed the old Vertex provider id shortly
        before Airflow's docstrings/placeholders were written).
        """
        _assert_prefix_is_known_provider("google-cloud")

    def test_conn_fields_model_description_prefix_is_valid_provider(self):
        """
        Drift tripwire for the ``provider.yaml`` conn-field, the actual UI source.

        Once a hook's ``provider.yaml`` declares ``conn-fields``, the connection
        form renders those and ``get_ui_field_behaviour`` placeholders are never
        shown (``providers_manager.py``'s ``ui_metadata_loaded``, deprecated
        since 3.2.0) — so this description, not the placeholder below, is what
        a user actually copies the model prefix from.
        """
        connection_types = get_provider_info()["connection-types"]
        vertex_conn_fields = next(
            c["conn-fields"] for c in connection_types if c["connection-type"] == "pydanticai_vertex"
        )
        description = vertex_conn_fields["model"]["description"]
        prefix = _extract_google_cloud_prefix(description)
        _assert_prefix_is_known_provider(prefix)

    @pytest.mark.parametrize(
        "connection_type",
        ["pydanticai", "pydanticai_azure", "pydanticai_bedrock", "pydanticai_vertex"],
    )
    def test_conn_fields_embed_model_example_is_supported(self, connection_type):
        connection_types = get_provider_info()["connection-types"]
        conn_fields = next(
            item["conn-fields"] for item in connection_types if item["connection-type"] == connection_type
        )
        description = conn_fields["embed_model"]["description"]
        match = re.search(r"\(e\.g\. ([^)]+)\)", description)
        assert match, f"no example embedding model found in: {description!r}"

        _assert_embedding_model_is_supported(match.group(1))

    def test_conn_types_ui_field_behaviour_placeholder_prefix_is_valid_provider(self):
        """
        Drift tripwire for the ``provider.yaml`` ``ui-field-behaviour.placeholders.extra``,
        a sibling of ``conn-fields`` under the same connection-type block.

        This placeholder is superseded at runtime by the ``conn-fields`` description
        above (once ``conn-fields`` is declared, the connection form no longer shows
        ``ui-field-behaviour`` placeholders), but ``provider.yaml`` still carries its
        own independent copy of the model prefix here, and nothing was covering it.
        """
        connection_types = get_provider_info()["connection-types"]
        vertex_connection_type = next(
            c for c in connection_types if c["connection-type"] == "pydanticai_vertex"
        )
        placeholder = vertex_connection_type["ui-field-behaviour"]["placeholders"]["extra"]
        prefix = _extract_google_cloud_prefix(placeholder)
        _assert_prefix_is_known_provider(prefix)

    def test_ui_field_behaviour_placeholder_prefix_is_valid_provider(self):
        """
        Drift tripwire for the ``get_ui_field_behaviour`` placeholder.

        Superseded at runtime by the ``provider.yaml`` conn-field above, but
        still source code a developer can read and copy from directly, so it
        needs to stay accurate too.
        """
        placeholder = PydanticAIVertexHook.get_ui_field_behaviour()["placeholders"]["extra"]
        prefix = _extract_google_cloud_prefix(placeholder)
        _assert_prefix_is_known_provider(prefix)


ALL_HOOKS = [PydanticAIHook, PydanticAIAzureHook, PydanticAIBedrockHook, PydanticAIVertexHook]
DECLARED_CONNECTION_TYPES = [c["connection-type"] for c in get_provider_info()["connection-types"]]


class TestConnTypeResolution:
    """
    Every hook is registered under the literal ``connection-type`` string from
    ``provider.yaml``, but ``Connection.from_uri`` and ``Connection.from_json`` rewrite
    ``-`` to ``_`` before the lookup happens. A hyphen in ``conn_type`` therefore makes
    the hook unreachable from every secrets backend, which is what happened to the three
    vendor types (apache/airflow#72316): only a connection read straight out of the
    metadata DB kept its hyphen and resolved.
    """

    def test_provider_declares_connection_types(self):
        """Keeps the round-trip guard below from passing vacuously."""
        assert DECLARED_CONNECTION_TYPES

    @pytest.mark.parametrize("conn_type", DECLARED_CONNECTION_TYPES)
    def test_declared_connection_type_survives_a_uri_round_trip(self, conn_type):
        """Guards every connection-type this provider declares, current and future."""
        source = Connection(conn_id="c", conn_type=conn_type)

        parsed = Connection(conn_id="c", uri=source.get_uri())

        assert parsed.conn_type == conn_type, (
            f"connection-type {conn_type!r} does not survive URI serialization, so its hook "
            "cannot be looked up from any secrets backend; declare it with underscores"
        )

    @pytest.mark.parametrize("hook_class", ALL_HOOKS, ids=lambda c: c.__name__)
    def test_hook_resolves_from_uri(self, hook_class):
        conn = Connection(conn_id="c", conn_type=hook_class.conn_type, host="example.com")

        round_tripped = Connection(conn_id="c", uri=conn.get_uri())

        assert round_tripped.conn_type == hook_class.conn_type
        assert type(round_tripped.get_hook()) is hook_class

    @pytest.mark.parametrize("hook_class", ALL_HOOKS, ids=lambda c: c.__name__)
    def test_hook_resolves_from_json(self, hook_class):
        conn = Connection(conn_id="c", conn_type=hook_class.conn_type, host="example.com")

        round_tripped = Connection.from_json(conn.as_json(), conn_id="c")

        assert round_tripped.conn_type == hook_class.conn_type
        assert type(round_tripped.get_hook()) is hook_class

    @pytest.mark.parametrize("hook_class", ALL_HOOKS, ids=lambda c: c.__name__)
    @pytest.mark.parametrize("serializer", ["uri", "json"])
    def test_hook_resolves_from_environment_variable(self, hook_class, serializer, monkeypatch):
        """
        ``AIRFLOW_CONN_*`` is enabled by default and is how most deployments define
        connections, so it is the widest blast radius for a conn_type that does not
        round-trip.
        """
        conn_id = f"vendor_{hook_class.conn_type}_{serializer}"
        conn = Connection(conn_id=conn_id, conn_type=hook_class.conn_type, host="example.com")
        serialized = conn.get_uri() if serializer == "uri" else conn.as_json()
        monkeypatch.setenv(f"AIRFLOW_CONN_{conn_id.upper()}", serialized)

        resolved = Connection.get_connection_from_secrets(conn_id)

        assert resolved.conn_type == hook_class.conn_type
        assert type(resolved.get_hook()) is hook_class
