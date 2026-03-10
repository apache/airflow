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

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from airflow.models.connection import Connection
from airflow.providers.common.ai.hooks.adk import AdkHook


class TestAdkHookInit:
    def test_default_conn_id(self):
        hook = AdkHook()
        assert hook.llm_conn_id == "adk_default"
        assert hook.model_id is None

    def test_custom_conn_id(self):
        hook = AdkHook(llm_conn_id="my_adk", model_id="gemini-2.5-flash")
        assert hook.llm_conn_id == "my_adk"
        assert hook.model_id == "gemini-2.5-flash"


class TestAdkHookGetConn:
    def test_get_conn_with_api_key(self):
        """API key from connection is set as GOOGLE_API_KEY env var."""
        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        conn = Connection(
            conn_id="test_conn",
            conn_type="adk",
            password="test-google-api-key",
        )
        with (
            patch.object(hook, "get_connection", return_value=conn),
            patch.dict("os.environ", {}, clear=False),
        ):
            result = hook.get_conn()
            assert result == "gemini-2.5-flash"
            assert os.environ.get("GOOGLE_API_KEY") == "test-google-api-key"

    def test_get_conn_with_model_from_extra(self):
        """Model is read from connection extra when model_id is not set."""
        hook = AdkHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="adk",
            password="test-key",
            extra='{"model": "gemini-2.5-pro"}',
        )
        with (
            patch.object(hook, "get_connection", return_value=conn),
            patch.dict("os.environ", {}, clear=False),
        ):
            result = hook.get_conn()

        assert result == "gemini-2.5-pro"
        assert hook.model_id == "gemini-2.5-pro"

    def test_model_id_param_overrides_extra(self):
        """model_id parameter takes priority over connection extra."""
        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        conn = Connection(
            conn_id="test_conn",
            conn_type="adk",
            password="test-key",
            extra='{"model": "gemini-2.5-pro"}',
        )
        with (
            patch.object(hook, "get_connection", return_value=conn),
            patch.dict("os.environ", {}, clear=False),
        ):
            result = hook.get_conn()

        assert result == "gemini-2.5-flash"

    def test_get_conn_raises_when_no_model(self):
        hook = AdkHook(llm_conn_id="test_conn")
        conn = Connection(
            conn_id="test_conn",
            conn_type="adk",
            password="test-key",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            with pytest.raises(ValueError, match="No model specified"):
                hook.get_conn()

    def test_get_conn_without_credentials_uses_env(self):
        """No API key in connection means env-based auth (ADC, etc.)."""
        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        conn = Connection(
            conn_id="test_conn",
            conn_type="adk",
        )
        with patch.object(hook, "get_connection", return_value=conn):
            result = hook.get_conn()

        assert result == "gemini-2.5-flash"

    def test_get_conn_caches_result(self):
        """get_conn() should resolve the model once and cache it."""
        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        conn = Connection(conn_id="test_conn", conn_type="adk")
        with patch.object(hook, "get_connection", return_value=conn) as mock_get_conn:
            first = hook.get_conn()
            second = hook.get_conn()

        assert first == second
        mock_get_conn.assert_called_once()

    def test_get_conn_falls_back_when_connection_not_found(self):
        """Falls back to model_id when connection cannot be found."""
        hook = AdkHook(llm_conn_id="nonexistent", model_id="gemini-2.5-flash")
        with patch.object(hook, "get_connection", side_effect=Exception("Connection not found")):
            result = hook.get_conn()

        assert result == "gemini-2.5-flash"


class TestAdkHookCreateAgent:
    @patch("airflow.providers.common.ai.hooks.adk.ADKAgent", autospec=True)
    def test_create_agent_defaults(self, mock_agent_cls):
        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        conn = Connection(conn_id="test_conn", conn_type="adk")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(instruction="You are a helpful assistant.")

        mock_agent_cls.assert_called_once_with(
            name="airflow_agent",
            model="gemini-2.5-flash",
            instruction="You are a helpful assistant.",
            tools=[],
        )

    @patch("airflow.providers.common.ai.hooks.adk.ADKAgent", autospec=True)
    def test_create_agent_with_tools_and_params(self, mock_agent_cls):
        def my_tool():
            """A test tool."""
            return "ok"

        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        conn = Connection(conn_id="test_conn", conn_type="adk")
        with patch.object(hook, "get_connection", return_value=conn):
            hook.create_agent(
                name="custom_agent",
                instruction="Be helpful.",
                tools=[my_tool],
                description="A custom agent",
            )

        mock_agent_cls.assert_called_once_with(
            name="custom_agent",
            model="gemini-2.5-flash",
            instruction="Be helpful.",
            tools=[my_tool],
            description="A custom agent",
        )


class TestAdkHookRunAgentSync:
    @patch("airflow.providers.common.ai.hooks.adk.ADKRunner", autospec=True)
    @patch("airflow.providers.common.ai.hooks.adk.InMemorySessionService", autospec=True)
    @patch("airflow.providers.common.ai.hooks.adk.genai_types")
    def test_run_agent_sync_returns_final_text(
        self, mock_genai_types, mock_session_service_cls, mock_runner_cls
    ):
        """run_agent_sync collects final response text from the event stream."""
        # Set up mock session
        mock_session = MagicMock()
        mock_session.id = "session-123"
        mock_session_service = mock_session_service_cls.return_value
        mock_session_service.create_session = AsyncMock(return_value=mock_session)

        # Set up mock event with final response
        mock_part = MagicMock()
        mock_part.text = "The answer is 42."
        mock_event = MagicMock()
        mock_event.is_final_response.return_value = True
        mock_event.content.parts = [mock_part]

        # Make runner.run_async return an async iterator with the event
        async def mock_run_async(**kwargs):
            yield mock_event

        mock_runner = mock_runner_cls.return_value
        mock_runner.run_async = mock_run_async

        mock_agent = MagicMock()

        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        result = hook.run_agent_sync(agent=mock_agent, prompt="What is the answer?")

        assert result == "The answer is 42."
        mock_runner_cls.assert_called_once_with(
            agent=mock_agent,
            app_name="airflow",
            session_service=mock_session_service,
        )

    @patch("airflow.providers.common.ai.hooks.adk.ADKRunner", autospec=True)
    @patch("airflow.providers.common.ai.hooks.adk.InMemorySessionService", autospec=True)
    @patch("airflow.providers.common.ai.hooks.adk.genai_types")
    def test_run_agent_sync_skips_non_final_events(
        self, mock_genai_types, mock_session_service_cls, mock_runner_cls
    ):
        """Non-final events are ignored; only final response text is collected."""
        mock_session = MagicMock()
        mock_session.id = "session-456"
        mock_session_service = mock_session_service_cls.return_value
        mock_session_service.create_session = AsyncMock(return_value=mock_session)

        # Non-final event (e.g. tool call)
        non_final_event = MagicMock()
        non_final_event.is_final_response.return_value = False

        # Final event
        mock_part = MagicMock()
        mock_part.text = "Done."
        final_event = MagicMock()
        final_event.is_final_response.return_value = True
        final_event.content.parts = [mock_part]

        async def mock_run_async(**kwargs):
            yield non_final_event
            yield final_event

        mock_runner = mock_runner_cls.return_value
        mock_runner.run_async = mock_run_async

        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        result = hook.run_agent_sync(agent=MagicMock(), prompt="Do something")

        assert result == "Done."


class TestAdkHookTestConnection:
    def test_successful_connection(self):
        hook = AdkHook(llm_conn_id="test_conn", model_id="gemini-2.5-flash")
        conn = Connection(conn_id="test_conn", conn_type="adk")
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is True
        assert "gemini-2.5-flash" in message

    def test_failed_connection_no_model(self):
        hook = AdkHook(llm_conn_id="test_conn")
        conn = Connection(conn_id="test_conn", conn_type="adk")
        with patch.object(hook, "get_connection", return_value=conn):
            success, message = hook.test_connection()

        assert success is False
        assert "No model specified" in message


class TestAdkHookUIFieldBehaviour:
    def test_ui_field_behaviour(self):
        result = AdkHook.get_ui_field_behaviour()
        assert "schema" in result["hidden_fields"]
        assert "port" in result["hidden_fields"]
        assert "login" in result["hidden_fields"]
        assert result["relabeling"]["password"] == "API Key"
