#
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

import asyncio
from unittest import mock

import pytest
from requests import Response, exceptions as requests_exceptions

from airflow.providers.common.ai.exceptions import (
    ManagedAgentInvocationError,
    ManagedAgentRejected,
)
from airflow.providers.common.ai.managed_agents.base import (
    ManagedAgentCapabilities,
    ManagedAgentRef,
    ManagedAgentRequest,
    ManagedAgentResponse,
)
from airflow.providers.common.compat.sdk import AirflowException
from airflow.providers.databricks.exceptions import DatabricksApiError
from airflow.providers.databricks.hooks.genie import DatabricksGenieHook

SPACE_ID = "01ef8392-4f3b-1234-9abc-1234567890ab"
CONVERSATION_ID = "01ef8395-9999-1234-9abc-1234567890cd"
MESSAGE_ID = "01ef8396-8888-1234-9abc-1234567890ef"


@pytest.fixture
def hook():
    return DatabricksGenieHook(databricks_conn_id="databricks_default")


class TestDatabricksGenieHookInit:
    def test_init_defaults(self, hook):
        assert hook.databricks_conn_id == "databricks_default"
        assert hook.timeout_seconds == 180
        assert hook.retry_limit == 3
        assert hook.retry_delay == 1.0

    def test_init_custom_params(self):
        custom_hook = DatabricksGenieHook(
            databricks_conn_id="custom_conn",
            timeout_seconds=300,
            retry_limit=5,
            retry_delay=2.5,
        )
        assert custom_hook.databricks_conn_id == "custom_conn"
        assert custom_hook.timeout_seconds == 300
        assert custom_hook.retry_limit == 5
        assert custom_hook.retry_delay == 2.5


class TestDatabricksGenieHookPublicApi:
    @mock.patch.object(DatabricksGenieHook, "_do_api_call")
    def test_start_conversation(self, mock_do_api_call, hook):
        mock_do_api_call.return_value = {
            "conversation_id": CONVERSATION_ID,
            "id": MESSAGE_ID,
            "status": "SUBMITTED",
        }
        res = hook.start_conversation(SPACE_ID, "What is our quarterly ARR?")

        mock_do_api_call.assert_called_once_with(
            ("POST", f"2.0/genie/spaces/{SPACE_ID}/start-conversation"),
            json={"content": "What is our quarterly ARR?"},
        )
        assert res["conversation_id"] == CONVERSATION_ID
        assert res["id"] == MESSAGE_ID

    @mock.patch.object(DatabricksGenieHook, "_do_api_call")
    def test_create_message(self, mock_do_api_call, hook):
        mock_do_api_call.return_value = {
            "id": MESSAGE_ID,
            "status": "SUBMITTED",
        }
        res = hook.create_message(SPACE_ID, CONVERSATION_ID, "Break it down by region")

        mock_do_api_call.assert_called_once_with(
            ("POST", f"2.0/genie/spaces/{SPACE_ID}/conversations/{CONVERSATION_ID}/messages"),
            json={"content": "Break it down by region"},
        )
        assert res["id"] == MESSAGE_ID

    @mock.patch.object(DatabricksGenieHook, "_do_api_call")
    def test_get_message(self, mock_do_api_call, hook):
        mock_do_api_call.return_value = {
            "id": MESSAGE_ID,
            "conversation_id": CONVERSATION_ID,
            "status": "COMPLETED",
        }
        res = hook.get_message(SPACE_ID, CONVERSATION_ID, MESSAGE_ID)

        mock_do_api_call.assert_called_once_with(
            ("GET", f"2.0/genie/spaces/{SPACE_ID}/conversations/{CONVERSATION_ID}/messages/{MESSAGE_ID}")
        )
        assert res["status"] == "COMPLETED"

    @mock.patch.object(DatabricksGenieHook, "_do_api_call")
    def test_get_query_result(self, mock_do_api_call, hook):
        mock_do_api_call.return_value = {
            "statement_id": "stmt_1",
            "status": {"state": "SUCCEEDED"},
            "result": {"data_array": [["EMEA", 1200000]]},
        }
        res = hook.get_query_result(SPACE_ID, CONVERSATION_ID, MESSAGE_ID)

        mock_do_api_call.assert_called_once_with(
            (
                "GET",
                f"2.0/genie/spaces/{SPACE_ID}/conversations/{CONVERSATION_ID}/messages/{MESSAGE_ID}/query-result",
            )
        )
        assert res["statement_id"] == "stmt_1"

    @mock.patch.object(DatabricksGenieHook, "get_message")
    def test_wait_for_message_completed(self, mock_get_message, hook):
        mock_get_message.side_effect = [
            {"id": MESSAGE_ID, "status": "EXECUTING_QUERY"},
            {"id": MESSAGE_ID, "status": "COMPLETED", "content": "Done"},
        ]
        result = hook.wait_for_message(SPACE_ID, CONVERSATION_ID, MESSAGE_ID, poll_interval=0.01)
        assert result["status"] == "COMPLETED"
        assert mock_get_message.call_count == 2

    @mock.patch.object(DatabricksGenieHook, "get_message")
    def test_wait_for_message_timeout(self, mock_get_message, hook):
        mock_get_message.return_value = {"id": MESSAGE_ID, "status": "EXECUTING_QUERY"}
        with pytest.raises(AirflowException, match="Timed out waiting"):
            hook.wait_for_message(SPACE_ID, CONVERSATION_ID, MESSAGE_ID, poll_interval=0.01, timeout=0.02)

    @pytest.mark.asyncio
    @mock.patch.object(DatabricksGenieHook, "_a_do_api_call")
    async def test_async_operations(self, mock_a_do_api_call, hook):
        mock_a_do_api_call.return_value = {
            "conversation_id": CONVERSATION_ID,
            "id": MESSAGE_ID,
            "status": "COMPLETED",
        }

        start_res = await hook.a_start_conversation(SPACE_ID, "Async start")
        assert start_res["conversation_id"] == CONVERSATION_ID

        msg_res = await hook.a_create_message(SPACE_ID, CONVERSATION_ID, "Async msg")
        assert msg_res["id"] == MESSAGE_ID

        get_res = await hook.a_get_message(SPACE_ID, CONVERSATION_ID, MESSAGE_ID)
        assert get_res["status"] == "COMPLETED"

        query_res = await hook.a_get_query_result(SPACE_ID, CONVERSATION_ID, MESSAGE_ID)
        assert query_res["status"] == "COMPLETED"


class TestDatabricksGenieHookManagedAgentContract:
    def test_resolve_agent_valid(self, hook):
        ref = hook.resolve_agent(SPACE_ID)
        assert ref == ManagedAgentRef(platform="databricks.genie", name=SPACE_ID)

    @pytest.mark.parametrize("invalid_id", ["", "   ", None])
    def test_resolve_agent_invalid(self, hook, invalid_id):
        with pytest.raises(ValueError, match="must be a non-empty space ID"):
            hook.resolve_agent(invalid_id)

    def test_get_agent_capabilities(self, hook):
        caps = hook.get_agent_capabilities(SPACE_ID)
        assert caps == ManagedAgentCapabilities(
            sessions=True,
            structured_output=True,
            usage=False,
            trace=True,
        )

    @mock.patch.object(DatabricksGenieHook, "wait_for_message")
    @mock.patch.object(DatabricksGenieHook, "start_conversation")
    def test_invoke_agent_new_conversation(self, mock_start, mock_wait, hook):
        mock_start.return_value = {
            "conversation_id": CONVERSATION_ID,
            "id": MESSAGE_ID,
            "status": "SUBMITTED",
        }
        mock_wait.return_value = {
            "id": MESSAGE_ID,
            "conversation_id": CONVERSATION_ID,
            "status": "COMPLETED",
            "attachments": [
                {"text": {"content": "Total revenue is $5M."}},
                {"query": {"query": "SELECT SUM(amount) FROM revenue"}},
            ],
        }

        request = ManagedAgentRequest(prompt="What is total revenue?")
        response = hook.invoke_agent(SPACE_ID, request)

        mock_start.assert_called_once_with(space_id=SPACE_ID, content="What is total revenue?")
        mock_wait.assert_called_once_with(
            space_id=SPACE_ID,
            conversation_id=CONVERSATION_ID,
            message_id=MESSAGE_ID,
            poll_interval=2.0,
            timeout=None,
        )

        assert "Total revenue is $5M." in response.text
        assert "```sql\nSELECT SUM(amount) FROM revenue\n```" in response.text
        assert response.session_id == CONVERSATION_ID
        assert response.trace_ref == MESSAGE_ID
        assert response.structured is not None
        assert len(response.structured["attachments"]) == 2

    @mock.patch.object(DatabricksGenieHook, "wait_for_message")
    @mock.patch.object(DatabricksGenieHook, "create_message")
    def test_invoke_agent_with_session(self, mock_create, mock_wait, hook):
        mock_create.return_value = {
            "id": MESSAGE_ID,
            "status": "SUBMITTED",
        }
        mock_wait.return_value = {
            "id": MESSAGE_ID,
            "conversation_id": CONVERSATION_ID,
            "status": "COMPLETED",
            "content": "Follow-up answered.",
        }

        request = ManagedAgentRequest(
            prompt="Follow-up question",
            session_id=CONVERSATION_ID,
        )
        response = hook.invoke_agent(SPACE_ID, request)

        mock_create.assert_called_once_with(
            space_id=SPACE_ID,
            conversation_id=CONVERSATION_ID,
            content="Follow-up question",
        )
        assert response.text == "Follow-up answered."
        assert response.session_id == CONVERSATION_ID

    @mock.patch.object(DatabricksGenieHook, "wait_for_message")
    @mock.patch.object(DatabricksGenieHook, "start_conversation")
    def test_invoke_agent_with_messages(self, mock_start, mock_wait, hook):
        mock_start.return_value = {"conversation_id": CONVERSATION_ID, "id": MESSAGE_ID}
        mock_wait.return_value = {"id": MESSAGE_ID, "status": "COMPLETED", "content": "Answer"}

        request = ManagedAgentRequest(
            messages=[
                {"role": "user", "content": [{"type": "text", "text": "Extracted prompt text"}]}
            ]
        )
        hook.invoke_agent(SPACE_ID, request)
        mock_start.assert_called_once_with(space_id=SPACE_ID, content="Extracted prompt text")

    def test_invoke_agent_reserved_vendor_options(self, hook):
        request = ManagedAgentRequest(
            prompt="Test",
            vendor_options={"session_id": "hack"},
        )
        with pytest.raises(ValueError, match="vendor_options cannot override"):
            hook.invoke_agent(SPACE_ID, request)

    @mock.patch.object(DatabricksGenieHook, "wait_for_message")
    @mock.patch.object(DatabricksGenieHook, "start_conversation")
    def test_invoke_agent_failed_rejection(self, mock_start, mock_wait, hook):
        mock_start.return_value = {"conversation_id": CONVERSATION_ID, "id": MESSAGE_ID}
        mock_wait.return_value = {
            "id": MESSAGE_ID,
            "status": "FAILED",
            "error": {"error_code": "BAD_REQUEST", "message": "Unknown table requested."},
        }

        request = ManagedAgentRequest(prompt="Query bad table")
        with pytest.raises(ManagedAgentRejected, match="Unknown table requested"):
            hook.invoke_agent(SPACE_ID, request)

    @mock.patch.object(DatabricksGenieHook, "wait_for_message")
    @mock.patch.object(DatabricksGenieHook, "start_conversation")
    def test_invoke_agent_failed_invocation_error(self, mock_start, mock_wait, hook):
        mock_start.return_value = {"conversation_id": CONVERSATION_ID, "id": MESSAGE_ID}
        mock_wait.return_value = {
            "id": MESSAGE_ID,
            "status": "FAILED",
            "error": {"error_code": "INTERNAL_ERROR", "message": "Internal warehouse fault."},
        }

        request = ManagedAgentRequest(prompt="Test")
        with pytest.raises(ManagedAgentInvocationError, match="Internal warehouse fault"):
            hook.invoke_agent(SPACE_ID, request)

    @mock.patch.object(DatabricksGenieHook, "wait_for_message")
    @mock.patch.object(DatabricksGenieHook, "start_conversation")
    def test_invoke_agent_cancelled(self, mock_start, mock_wait, hook):
        mock_start.return_value = {"conversation_id": CONVERSATION_ID, "id": MESSAGE_ID}
        mock_wait.return_value = {
            "id": MESSAGE_ID,
            "status": "CANCELLED",
        }

        request = ManagedAgentRequest(prompt="Test")
        with pytest.raises(ManagedAgentInvocationError, match="Message .* was cancelled"):
            hook.invoke_agent(SPACE_ID, request)

    @mock.patch.object(DatabricksGenieHook, "start_conversation")
    def test_invoke_agent_http_auth_error(self, mock_start, hook):
        mock_start.side_effect = DatabricksApiError("Unauthorized", http_status_code=401)
        request = ManagedAgentRequest(prompt="Test")
        with pytest.raises(ManagedAgentInvocationError, match="Authentication or permission denied"):
            hook.invoke_agent(SPACE_ID, request)

    @mock.patch.object(DatabricksGenieHook, "start_conversation")
    def test_invoke_agent_http_not_found(self, mock_start, hook):
        mock_start.side_effect = DatabricksApiError("Space not found", http_status_code=404)
        request = ManagedAgentRequest(prompt="Test")
        with pytest.raises(ManagedAgentInvocationError, match="not found"):
            hook.invoke_agent(SPACE_ID, request)

    @mock.patch.object(DatabricksGenieHook, "start_conversation")
    def test_invoke_agent_http_bad_request(self, mock_start, hook):
        mock_start.side_effect = DatabricksApiError("Malformed parameter", http_status_code=400)
        request = ManagedAgentRequest(prompt="Test")
        with pytest.raises(ManagedAgentRejected, match="rejected request"):
            hook.invoke_agent(SPACE_ID, request)


class TestDatabricksGenieHookWithoutCommonAi:
    def test_missing_common_ai_raises_optional_feature_exception(self):
        from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException
        from airflow.providers.databricks.hooks.genie import _needs_common_ai

        with pytest.raises(
            AirflowOptionalProviderFeatureException,
            match="Consulting a Databricks Genie space as a managed agent needs the 'common.ai' extra",
        ):
            _needs_common_ai()

