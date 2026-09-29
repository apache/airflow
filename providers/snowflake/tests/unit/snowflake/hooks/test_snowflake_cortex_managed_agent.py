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

import json
from unittest import mock

import pytest

pytest.importorskip("airflow.providers.common.ai.managed_agents.contract")

import requests

from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
from airflow.providers.common.ai.managed_agents.contract import ManagedAgentCapabilities, ManagedAgentRequest
from airflow.providers.snowflake.hooks.snowflake_cortex_managed_agent import SnowflakeCortexManagedAgentHook

AGENT_MODULE_PATH = "airflow.providers.snowflake.hooks.snowflake_cortex_agent"
HOOK_PATH = "airflow.providers.snowflake.hooks.snowflake_cortex_managed_agent.SnowflakeCortexManagedAgentHook"

ACCOUNT = "test-account"
ACCESS_TOKEN = "test-token"
CONN_PARAMS = {"account": ACCOUNT, "token": ACCESS_TOKEN, "authenticator": "oauth"}
STATIC_CONN_PARAMS = {"account": ACCOUNT}


def create_response(status_code: int = 200, *, json_body=None):
    response = mock.MagicMock()
    response.status_code = status_code
    response.json.return_value = {} if json_body is None else json_body

    if status_code >= 400:
        response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=response)
    else:
        response.raise_for_status.return_value = None

    return response


@pytest.fixture
def mocked_hook():
    with (
        mock.patch(f"{HOOK_PATH}._get_conn_params", autospec=True) as mock_conn_params,
        mock.patch(f"{HOOK_PATH}._get_static_conn_params", new_callable=mock.PropertyMock) as mock_static,
        mock.patch(f"{AGENT_MODULE_PATH}.requests.request", autospec=True) as mock_request,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static.return_value = STATIC_CONN_PARAMS
        hook = SnowflakeCortexManagedAgentHook(snowflake_conn_id="mock_conn_id")
        yield hook, mock_request


class TestResolveAgent:
    def test_valid_agent(self, mocked_hook):
        hook, _ = mocked_hook
        ref = hook.resolve_agent("DB.SCHEMA.AGENT")
        assert ref.platform == "snowflake.cortex_agent"
        assert ref.name == "DB.SCHEMA.AGENT"

    @pytest.mark.parametrize(
        "agent",
        [
            pytest.param("DB.SCHEMA", id="two_segments"),
            pytest.param("DB.SCHEMA.AGENT.EXTRA", id="four_segments"),
            pytest.param("DB..AGENT", id="empty_segment"),
        ],
    )
    def test_invalid_agent_shapes_raise(self, mocked_hook, agent):
        hook, _ = mocked_hook
        with pytest.raises(ValueError, match="DATABASE.SCHEMA.NAME"):
            hook.resolve_agent(agent)


class TestAgentCapabilities:
    def test_all_capabilities_false(self, mocked_hook):
        hook, _ = mocked_hook
        assert hook.agent_capabilities("DB.SCHEMA.AGENT") == ManagedAgentCapabilities()


class TestInvokeAgent:
    def test_prompt_becomes_messages_payload(self, mocked_hook):
        hook, mock_request = mocked_hook
        mock_request.return_value = create_response(json_body={"content": [{"type": "text", "text": "hi"}]})

        response = hook.invoke_agent("DB.SCHEMA.AGENT", ManagedAgentRequest(prompt="hello"))

        assert response.text == "hi"
        payload = mock_request.call_args.kwargs["json"]
        assert payload["messages"] == [{"role": "user", "content": [{"type": "text", "text": "hello"}]}]

    def test_messages_forwarded_as_is(self, mocked_hook):
        hook, mock_request = mocked_hook
        mock_request.return_value = create_response(json_body={"content": []})
        messages = [{"role": "user", "content": [{"type": "text", "text": "hi"}]}]

        hook.invoke_agent("DB.SCHEMA.AGENT", ManagedAgentRequest(messages=messages))

        payload = mock_request.call_args.kwargs["json"]
        assert payload["messages"] == messages

    def test_allowed_vendor_option_passes_through(self, mocked_hook):
        hook, mock_request = mocked_hook
        mock_request.return_value = create_response(json_body={"content": []})

        hook.invoke_agent(
            "DB.SCHEMA.AGENT",
            ManagedAgentRequest(prompt="hi", vendor_options={"tool_choice": {"type": "auto"}}),
        )

        payload = mock_request.call_args.kwargs["json"]
        assert payload["tool_choice"] == {"type": "auto"}

    def test_unknown_vendor_option_raises_without_request(self, mocked_hook):
        hook, mock_request = mocked_hook

        with pytest.raises(ValueError, match="vendor_options"):
            hook.invoke_agent(
                "DB.SCHEMA.AGENT",
                ManagedAgentRequest(prompt="hi", vendor_options={"thread_id": 1}),
            )

        mock_request.assert_not_called()

    def test_session_id_raises_without_request(self, mocked_hook):
        hook, mock_request = mocked_hook

        with pytest.raises(ValueError, match="session_id"):
            hook.invoke_agent("DB.SCHEMA.AGENT", ManagedAgentRequest(prompt="hi", session_id="abc"))

        mock_request.assert_not_called()

    def test_timeout_forwarded_when_set(self, mocked_hook):
        hook, mock_request = mocked_hook
        mock_request.return_value = create_response(json_body={"content": []})

        hook.invoke_agent("DB.SCHEMA.AGENT", ManagedAgentRequest(prompt="hi", timeout=30))

        assert mock_request.call_args.kwargs["timeout"] == 30

    def test_timeout_defaults_to_run_agent_default(self, mocked_hook):
        hook, mock_request = mocked_hook
        mock_request.return_value = create_response(json_body={"content": []})

        hook.invoke_agent("DB.SCHEMA.AGENT", ManagedAgentRequest(prompt="hi"))

        assert mock_request.call_args.kwargs["timeout"] == 600

    @pytest.mark.parametrize("status_code", [401, 404, 400])
    def test_terminal_4xx_raises_managed_agent_invocation_error(self, mocked_hook, status_code):
        hook, mock_request = mocked_hook
        mock_request.return_value = create_response(status_code=status_code, json_body={"error": "boom"})

        with pytest.raises(ManagedAgentInvocationError):
            hook.invoke_agent("DB.SCHEMA.AGENT", ManagedAgentRequest(prompt="hi"))

    @pytest.mark.parametrize("status_code", [429, 503])
    def test_retryable_status_propagates_unchanged(self, mocked_hook, status_code):
        hook, mock_request = mocked_hook
        mock_request.return_value = create_response(status_code=status_code, json_body={"error": "boom"})

        with pytest.raises(requests.exceptions.HTTPError):
            hook.invoke_agent("DB.SCHEMA.AGENT", ManagedAgentRequest(prompt="hi"))

    def test_no_text_block_falls_back_to_json_content(self, mocked_hook):
        hook, mock_request = mocked_hook
        content = [{"type": "tool_use", "tool": "search_tool"}]
        mock_request.return_value = create_response(json_body={"content": content})

        response = hook.invoke_agent("DB.SCHEMA.AGENT", ManagedAgentRequest(prompt="hi"))

        assert response.text == json.dumps(content, default=str)
        assert response.raw == {"content": content}


class TestBoundManagedAgent:
    def test_agent_bind_and_invoke_full_path(self, mocked_hook):
        hook, mock_request = mocked_hook
        mock_request.return_value = create_response(json_body={"content": [{"type": "text", "text": "hi"}]})

        bound = hook.agent("DB.SCHEMA.AGENT")
        response = bound.invoke(ManagedAgentRequest(prompt="hello"))

        assert response.text == "hi"
        assert bound.ref.name == "DB.SCHEMA.AGENT"
        assert bound.ref.platform == "snowflake.cortex_agent"
