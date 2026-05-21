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

from airflow.providers.common.ai.hooks.crewai import CrewAIHook


@pytest.fixture(autouse=True)
def _stub_crewai_module():
    """Stub ``crewai`` in sys.modules so @patch can resolve targets without
    the real package installed.

    crewai is intentionally NOT in the provider's dev group (see
    ``providers/common/ai/pyproject.toml`` for the rationale -- crewai's
    pins conflict with Airflow's resolved click/tomli/rich and break the
    CI image build's ``pip check``). Tests have to stub it explicitly.
    """
    crewai_mod = MagicMock()
    with patch.dict(sys.modules, {"crewai": crewai_mod}):
        yield


def _conn(password: str = "", host: str = "", extra: dict | None = None) -> MagicMock:
    mock_conn = MagicMock()
    mock_conn.password = password
    mock_conn.host = host
    mock_conn.extra_dejson = extra or {}
    return mock_conn


class TestCrewAIHookInit:
    def test_default_params(self):
        hook = CrewAIHook()
        assert hook.llm_conn_id == "crewai_default"
        assert hook.llm_model is None

    def test_explicit_conn_and_model(self):
        hook = CrewAIHook(llm_conn_id="my_conn", llm_model="openai/gpt-4o")
        assert hook.llm_conn_id == "my_conn"
        assert hook.llm_model == "openai/gpt-4o"

    def test_conn_type_is_crewai(self):
        assert CrewAIHook.conn_type == "crewai"
        assert CrewAIHook.default_conn_name == "crewai_default"
        assert CrewAIHook.conn_name_attr == "llm_conn_id"
        assert CrewAIHook.hook_name == "CrewAI"


class TestGetUiFieldBehaviour:
    def test_shape(self):
        behaviour = CrewAIHook.get_ui_field_behaviour()
        assert behaviour["hidden_fields"] == ["schema", "port", "login"]
        assert behaviour["relabeling"] == {"password": "API Key"}
        assert "host" in behaviour["placeholders"]
        # Extras placeholder shows the LiteLLM slash format, not the pydantic-ai colon.
        assert "openai/gpt-4o" in behaviour["placeholders"]["extra"]


class TestResolveModelId:
    def test_constructor_wins_over_extra(self):
        result = CrewAIHook._resolve_model_id(
            {"model": "anthropic/claude-3-5-sonnet"},
            "openai/gpt-4o",
        )
        assert result == "openai/gpt-4o"

    def test_falls_back_to_extra(self):
        result = CrewAIHook._resolve_model_id(
            {"model": "anthropic/claude-3-5-sonnet"},
            None,
        )
        assert result == "anthropic/claude-3-5-sonnet"

    def test_raises_when_neither_set(self):
        with pytest.raises(ValueError, match="No model identifier set"):
            CrewAIHook._resolve_model_id({}, None)


class TestCheckProviderSupported:
    @pytest.mark.parametrize(
        "model_id",
        [
            "bedrock/anthropic.claude-3-5-sonnet-20241022-v2:0",
            "vertex_ai/gemini-2.0-flash",
            "azure/gpt-4o",
        ],
    )
    def test_raises_for_non_openai_compatible_prefixes(self, model_id):
        with pytest.raises(NotImplementedError, match="does not yet support"):
            CrewAIHook._check_provider_supported(model_id)

    @pytest.mark.parametrize(
        "model_id",
        [
            "openai/gpt-4o",
            "anthropic/claude-3-5-sonnet",
            "groq/llama-3.3-70b-versatile",
            "ollama/llama3",
            "gemini/gemini-2.0-flash",
        ],
    )
    def test_passes_for_openai_compatible_providers(self, model_id):
        # Should not raise.
        CrewAIHook._check_provider_supported(model_id)


class TestGetLlm:
    @patch("crewai.LLM")
    @patch.object(CrewAIHook, "get_connection")
    def test_dispatches_with_api_key(self, mock_get_conn, mock_llm_cls):
        mock_get_conn.return_value = _conn(password="sk-test")

        hook = CrewAIHook(llm_model="openai/gpt-4o")
        result = hook.get_llm()

        mock_get_conn.assert_called_once_with("crewai_default")
        mock_llm_cls.assert_called_once_with(model="openai/gpt-4o", api_key="sk-test")
        assert result is mock_llm_cls.return_value

    @patch("crewai.LLM")
    @patch.object(CrewAIHook, "get_connection")
    def test_dispatches_with_base_url(self, mock_get_conn, mock_llm_cls):
        mock_get_conn.return_value = _conn(password="sk-test", host="http://localhost:11434")

        hook = CrewAIHook(llm_model="ollama/llama3")
        hook.get_llm()

        mock_llm_cls.assert_called_once_with(
            model="ollama/llama3",
            api_key="sk-test",
            base_url="http://localhost:11434",
        )

    @patch("crewai.LLM")
    @patch.object(CrewAIHook, "get_connection")
    def test_resolves_model_from_extra(self, mock_get_conn, mock_llm_cls):
        mock_get_conn.return_value = _conn(
            password="sk-test",
            extra={"model": "anthropic/claude-3-5-sonnet"},
        )

        hook = CrewAIHook()
        hook.get_llm()

        mock_llm_cls.assert_called_once_with(
            model="anthropic/claude-3-5-sonnet",
            api_key="sk-test",
        )

    @patch("crewai.LLM")
    @patch.object(CrewAIHook, "get_connection")
    def test_raises_when_no_model(self, mock_get_conn, mock_llm_cls):
        mock_get_conn.return_value = _conn(password="sk-test")

        hook = CrewAIHook()
        with pytest.raises(ValueError, match="No model identifier set"):
            hook.get_llm()
        mock_llm_cls.assert_not_called()

    @patch("crewai.LLM")
    @patch.object(CrewAIHook, "get_connection")
    def test_raises_for_bedrock_model(self, mock_get_conn, mock_llm_cls):
        mock_get_conn.return_value = _conn(password="sk-test")

        hook = CrewAIHook(llm_model="bedrock/anthropic.claude-3-5-sonnet-20241022-v2:0")
        with pytest.raises(NotImplementedError, match="does not yet support"):
            hook.get_llm()
        mock_llm_cls.assert_not_called()

    @patch("crewai.LLM")
    @patch.object(CrewAIHook, "get_connection")
    def test_no_credentials_passes_empty_kwargs(self, mock_get_conn, mock_llm_cls):
        mock_get_conn.return_value = _conn()

        hook = CrewAIHook(llm_model="openai/gpt-4o")
        hook.get_llm()

        mock_llm_cls.assert_called_once_with(model="openai/gpt-4o")
