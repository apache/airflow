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
from pydantic_ai.models import Model

from airflow.models.connection import Connection
from airflow.providers.common.ai.hooks.pydantic_ai import PydanticAIHook


class TestPydanticAIHookInit:
    def test_default_conn_id(self):
        hook = PydanticAIHook()
        assert hook.llm_conn_id == "pydantic_ai_default"
        assert hook.model_id is None

    def test_custom_conn_id(self):
        hook = PydanticAIHook(llm_conn_id="my_llm", model_id="openai:gpt-5.3")
        assert hook.llm_conn_id == "my_llm"
        assert hook.model_id == "openai:gpt-5.3"


class TestPydanticAIHookGetConn:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    def test_get_conn_with_api_key_and_base_url(self, mock_infer_provider_class, mock_infer_model):
        """Credentials are injected via provider_factory, not as direct kwargs."""
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model
        mock_provider = MagicMock()
        mock_infer_provider_class.return_value = MagicMock(return_value=mock_provider)

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydantic_ai",
            password="sk-test-key",
            host="https://api.openai.com/v1",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        assert result is mock_model
        mock_infer_model.assert_called_once()
        call_args = mock_infer_model.call_args
        assert call_args[0][0] == "openai:gpt-5.3"
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
            conn_type="pydantic_ai",
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

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydantic_ai",
            password="sk-test-key",
            extra='{"model": "anthropic:claude-opus-4-6"}',
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        # model_id param takes priority over extra
        assert mock_infer_model.call_args[0][0] == "openai:gpt-5.3"

    def test_get_conn_raises_when_no_model(self):
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydantic_ai",
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
            conn_type="pydantic_ai",
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
            conn_type="pydantic_ai",
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

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(conn_id="test_conn", conn_type="pydantic_ai")
        with patch.object(hook, "get_connection", return_value=conn):
            first = hook.get_conn()
            second = hook.get_conn()

        assert first is second
        mock_infer_model.assert_called_once()

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_provider_class", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_provider_factory_falls_back_on_unsupported_kwargs(
        self, mock_infer_model, mock_infer_provider_class, mock_infer_provider
    ):
        """If a provider rejects api_key/base_url, fall back to default resolution."""
        mock_infer_model.return_value = MagicMock(spec=Model)
        mock_fallback_provider = MagicMock()
        mock_infer_provider.return_value = mock_fallback_provider
        # Simulate a provider that doesn't accept api_key/base_url
        mock_infer_provider_class.return_value = MagicMock(side_effect=TypeError("unexpected keyword"))

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="google:gemini-2.0-flash")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydantic_ai",
            password="some-key",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            hook.get_conn()

        factory = mock_infer_model.call_args[1]["provider_factory"]
        result = factory("google-gla")

        # Should have tried provider_cls first, then fallen back to infer_provider
        mock_infer_provider_class.return_value.assert_called_once_with(api_key="some-key")
        mock_infer_provider.assert_called_with("google-gla")
        assert result is mock_fallback_provider


class TestPydanticAIHookCreateAgent:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.Agent", autospec=True)
    def test_create_agent_defaults(self, mock_agent_cls, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydantic_ai",
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

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydantic_ai",
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


class TestPydanticAIHookTestConnection:
    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_successful_connection(self, mock_infer_model):
        mock_model = MagicMock(spec=Model)
        mock_infer_model.return_value = mock_model

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="openai:gpt-5.3")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydantic_ai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is True
        assert message == "Model resolved successfully."

    @patch("airflow.providers.common.ai.hooks.pydantic_ai.infer_model", autospec=True)
    def test_failed_connection(self, mock_infer_model):
        mock_infer_model.side_effect = ValueError("Unknown provider 'badprovider'")

        hook = PydanticAIHook(llm_conn_id="test_conn", model_id="badprovider:model")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydantic_ai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is False
        assert "Unknown provider" in message

    def test_failed_connection_no_model(self):
        hook = PydanticAIHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="pydantic_ai",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is False
        assert "No model specified" in message
