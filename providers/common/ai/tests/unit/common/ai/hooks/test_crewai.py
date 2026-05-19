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

from airflow.providers.common.ai.hooks.crewai import CrewAIHook


class TestCrewAIHookInit:
    def test_default_params(self):
        hook = CrewAIHook()
        assert hook.llm_conn_id == "pydanticai_default"
        assert hook.llm_model is None

    def test_custom_params(self):
        hook = CrewAIHook(llm_conn_id="my_conn", llm_model="openai/gpt-4o")
        assert hook.llm_conn_id == "my_conn"
        assert hook.llm_model == "openai/gpt-4o"

    def test_conn_type_is_pydanticai(self):
        assert CrewAIHook.conn_type == "pydanticai"
        assert CrewAIHook.default_conn_name == "pydanticai_default"


class TestResolveConnectionKwargs:
    @patch.object(CrewAIHook, "get_connection")
    def test_extracts_password_as_api_key(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test-key"
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        hook = CrewAIHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {"api_key": "sk-test-key"}

    @patch.object(CrewAIHook, "get_connection")
    def test_extracts_host_as_base_url(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = ""
        mock_conn.host = "https://custom.api.com"
        mock_get_conn.return_value = mock_conn

        hook = CrewAIHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {"base_url": "https://custom.api.com"}

    @patch.object(CrewAIHook, "get_connection")
    def test_both_password_and_host(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-key"
        mock_conn.host = "https://api.example.com"
        mock_get_conn.return_value = mock_conn

        hook = CrewAIHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {"api_key": "sk-key", "base_url": "https://api.example.com"}

    @patch.object(CrewAIHook, "get_connection")
    def test_empty_fields_return_empty_dict(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = ""
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        hook = CrewAIHook()
        result = hook._resolve_connection_kwargs("test_conn")

        assert result == {}


def _make_mock_crewai_module():
    mock_module = MagicMock()
    mock_cls = MagicMock()
    mock_module.LLM = mock_cls
    return mock_module, mock_cls


class TestGetLlm:
    def test_raises_without_llm_model(self):
        hook = CrewAIHook()
        with pytest.raises(ValueError, match="llm_model must be set"):
            hook.get_llm()

    @patch.object(CrewAIHook, "get_connection")
    def test_returns_crewai_llm(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test"
        mock_conn.host = ""
        mock_get_conn.return_value = mock_conn

        mock_module, mock_cls = _make_mock_crewai_module()

        hook = CrewAIHook(llm_model="openai/gpt-4o")
        with patch.dict("sys.modules", {"crewai": mock_module}):
            result = hook.get_llm()

        mock_cls.assert_called_once_with(model="openai/gpt-4o", api_key="sk-test")
        assert result == mock_cls.return_value

    @patch.object(CrewAIHook, "get_connection")
    def test_passes_base_url(self, mock_get_conn):
        mock_conn = MagicMock()
        mock_conn.password = "sk-test"
        mock_conn.host = "https://custom.api.com"
        mock_get_conn.return_value = mock_conn

        mock_module, mock_cls = _make_mock_crewai_module()

        hook = CrewAIHook(llm_model="openai/gpt-4o")
        with patch.dict("sys.modules", {"crewai": mock_module}):
            hook.get_llm()

        mock_cls.assert_called_once_with(
            model="openai/gpt-4o", api_key="sk-test", base_url="https://custom.api.com"
        )
