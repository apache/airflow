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

import builtins
import importlib.util
import json
from unittest import mock

import pytest
import requests

from airflow.providers.common.ai.toolsets import ManagedAgentToolset
from airflow.providers.common.compat.sdk import AirflowException, AirflowOptionalProviderFeatureException
from airflow.providers.databricks.hooks import genie as genie_module
from airflow.providers.databricks.hooks.databricks_base import _TokenAuth
from airflow.providers.databricks.hooks.genie import (
    DatabricksGenieError,
    DatabricksGenieHook,
    _summarize_query_result,
    _truncate_result,
)

REQUEST = "airflow.providers.databricks.hooks.genie.requests.request"
GENIE_URL = "https://workspace/api/2.0/genie/message"


def build_response(
    status_code: int = 200, content: bytes = b'{"status":"COMPLETED"}', url: str = GENIE_URL
) -> requests.Response:
    response = requests.Response()
    response.status_code = status_code
    response._content = content
    response._content_consumed = True
    response.url = url
    return response


@pytest.fixture
def stub_hook():
    """A hook whose connection lookups are spec'd mocks, so ``_request`` runs without a real connection."""
    hook = DatabricksGenieHook(retry_limit=2, retry_delay=0.001)
    hook.user_agent_header = {}
    with (
        mock.patch.object(hook, "_endpoint_url", autospec=True, return_value=GENIE_URL),
        mock.patch.object(hook, "_get_aad_headers", autospec=True, return_value={}),
        mock.patch.object(hook, "_get_token", autospec=True, return_value="secret-token"),
        mock.patch.object(hook, "_get_requests_kwargs", autospec=True, return_value={}),
    ):
        yield hook


class TestDatabricksGenieHook:
    def test_query_result_marks_omitted_chunks_and_columns_as_truncated(self):
        partial_rows = _summarize_query_result(
            {
                "statement_response": {
                    "manifest": {
                        "schema": {"columns": [{"name": "count"}]},
                        "total_row_count": 10,
                        "total_chunk_count": 2,
                        "truncated": False,
                    },
                    "result": {"data_array": [[str(index)] for index in range(5)]},
                }
            }
        )
        assert len(partial_rows["rows"]) == 5
        assert partial_rows["total_row_count"] == 10
        assert partial_rows["truncated"] is True

        capped_columns = _summarize_query_result(
            {
                "manifest": {
                    "schema": {"columns": [{"name": f"column_{index}"} for index in range(101)]},
                    "total_row_count": 1,
                    "truncated": False,
                },
                "result": {"data_array": [["value"]]},
            }
        )
        assert len(capped_columns["columns"]) == 100
        assert capped_columns["truncated"] is True

    def test_direct_hook_works_without_common_ai(self):
        """Reload from the installed module path so wheel/compat layouts work."""
        original_import = builtins.__import__

        def block_common_ai(name, *args, **kwargs):
            if name.startswith("airflow.providers.common.ai"):
                raise ImportError("common.ai is unavailable")
            return original_import(name, *args, **kwargs)

        # Prefer module.__file__ over a hardcoded src/ tree path: Compat matrix
        # installs the provider from a wheel, so the editable src tree is absent.
        assert genie_module.__file__ is not None
        spec = importlib.util.spec_from_file_location("_genie_without_common_ai", genie_module.__file__)
        assert spec is not None
        assert spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        with mock.patch("builtins.__import__", side_effect=block_common_ai):
            spec.loader.exec_module(module)

        hook = module.DatabricksGenieHook()
        hook._request = mock.create_autospec(
            hook._request,
            side_effect=[
                {"conversation_id": "conversation-1", "message_id": "message-1"},
                {"status": "COMPLETED", "attachments": [{"text": {"content": "Answer."}}]},
            ],
        )
        result = hook.consult("space-1", "Question?")
        assert result["answer"] == "Answer."
        assert result["conversation_id"] == "conversation-1"
        assert hook._request.call_count == 2
        with pytest.raises(AirflowOptionalProviderFeatureException, match="common.ai"):
            hook.agent("space-1")

    def test_standard_managed_agent_toolset_returns_bounded_structured_ids(self):
        hook = DatabricksGenieHook()
        hook._request = mock.create_autospec(
            hook._request,
            side_effect=[
                {"conversation_id": "conversation-1", "message_id": "message-1"},
                {"status": "COMPLETED", "attachments": [{"text": {"content": "Answer."}}]},
            ],
        )
        toolset = ManagedAgentToolset(hook.agent("space-1"), tool_name="ask_genie", max_retries=0)

        result = json.loads(toolset.invoke_sync("Question?"))

        assert result["space_id"] == "space-1"
        assert result["conversation_id"] == "conversation-1"
        assert result["message_id"] == "message-1"
        assert result["answer"] == "Answer."
        assert hook._request.call_count == 2

    def test_consult_starts_once_polls_reads_and_returns_ids_and_query_result(self):
        hook = DatabricksGenieHook(poll_interval=0.001)
        hook._request = mock.create_autospec(
            hook._request,
            side_effect=[
                {"conversation_id": "conversation-1", "message_id": "message-1"},
                {"status": "ASKING_AI"},
                {
                    "status": "COMPLETED",
                    "attachments": [
                        {
                            "attachment_id": "attachment-1",
                            "text": {"content": "There are 12 open orders."},
                            "query": {"title": "Open orders", "query": "select count(*)"},
                        }
                    ],
                },
                {
                    "statement_response": {
                        "manifest": {
                            "schema": {"columns": [{"name": "count"}]},
                            "total_row_count": 1,
                        },
                        "result": {"data_array": [["12"]]},
                    }
                },
            ],
        )

        result = hook.consult("space-1", "How many open orders?", timeout=1)

        assert result == {
            "space_id": "space-1",
            "conversation_id": "conversation-1",
            "message_id": "message-1",
            "status": "COMPLETED",
            "answer": "There are 12 open orders.",
            "queries": [{"title": "Open orders", "query": "select count(*)"}],
            "query_results": [
                {"columns": ["count"], "rows": [["12"]], "total_row_count": 1, "truncated": False}
            ],
        }
        assert hook._request.call_count == 4
        assert hook._request.call_args_list[0].args[:2] == (
            "POST",
            "spaces/space-1/start-conversation",
        )
        assert hook._request.call_args_list[0].kwargs["retry_read"] is False
        assert all(call.kwargs.get("retry_read") is True for call in hook._request.call_args_list[1:])

    def test_resumed_consult_posts_into_existing_conversation_once(self):
        hook = DatabricksGenieHook()
        hook._request = mock.create_autospec(
            hook._request,
            side_effect=[
                {"conversation_id": "conversation-1", "message_id": "message-2"},
                {"status": "COMPLETED", "attachments": [{"text": {"content": "Answer."}}]},
            ],
        )

        hook.consult("space-1", "Another question", conversation_id="conversation-1")

        assert hook._request.call_args_list[0].args[:2] == (
            "POST",
            "spaces/space-1/conversations/conversation-1/messages",
        )
        assert hook._request.call_args_list[0].kwargs["retry_read"] is False
        assert hook._request.call_count == 2

    def test_non_idempotent_post_is_never_retried_after_timeout(self, stub_hook):
        with (
            mock.patch(REQUEST, autospec=True, side_effect=requests.Timeout("timeout")) as request,
            pytest.raises(DatabricksGenieError, match="did not retry") as error,
        ):
            stub_hook._request(
                "POST",
                "spaces/id/start-conversation",
                body={"content": "question"},
                retry_read=False,
            )

        assert request.call_count == 1
        assert "secret-token" not in str(error.value)

    def test_token_request_uses_token_auth_so_netrc_cannot_override_bearer(self, stub_hook):
        """_TokenAuth keeps requests from applying ~/.netrc over the bearer token."""
        with mock.patch(REQUEST, autospec=True, return_value=build_response()) as request:
            assert stub_hook._request("GET", "message", retry_read=True) == {"status": "COMPLETED"}

        auth = request.call_args.kwargs["auth"]
        assert isinstance(auth, _TokenAuth)
        assert auth.token == "secret-token"
        assert "Authorization" not in request.call_args.kwargs["headers"]

    def test_get_read_is_retried_after_a_server_error(self, stub_hook):
        responses = [build_response(503, b'{"error":"temporarily unavailable"}'), build_response()]
        with mock.patch(REQUEST, autospec=True, side_effect=responses) as request:
            result = stub_hook._request("GET", "message", retry_read=True)

        assert result == {"status": "COMPLETED"}
        assert request.call_count == 2

    @pytest.mark.parametrize(
        "response",
        [
            pytest.param(build_response(503, b'{"error":"temporarily unavailable"}'), id="server-error"),
            pytest.param(build_response(200, b"not json"), id="invalid-json"),
        ],
    )
    def test_ambiguous_post_is_never_retried(self, stub_hook, response):
        with (
            mock.patch(REQUEST, autospec=True, return_value=response) as request,
            pytest.raises(DatabricksGenieError, match="outcome is unknown") as error,
        ):
            stub_hook._request("POST", "message", body={"content": "question"}, retry_read=False)

        assert request.call_count == 1
        assert "secret-token" not in str(error.value)

    def test_remote_http_endpoint_is_rejected_before_credentials_are_sent(self, stub_hook):
        stub_hook._endpoint_url.return_value = "http://workspace.example/api/2.0/genie/message"
        with (
            mock.patch(REQUEST, autospec=True) as request,
            pytest.raises(DatabricksGenieError, match="requires HTTPS"),
        ):
            stub_hook._request("GET", "message", retry_read=True)
        request.assert_not_called()
        stub_hook._get_token.assert_not_called()

    def test_credential_failure_is_raised_before_anything_is_sent(self, stub_hook):
        stub_hook._get_token.side_effect = AirflowException("token endpoint unavailable")
        with (
            mock.patch(REQUEST, autospec=True) as request,
            pytest.raises(AirflowException, match="token endpoint unavailable"),
        ):
            stub_hook._prepare_request("spaces/id/start-conversation")
        request.assert_not_called()

    def test_prepared_request_is_sent_without_resolving_credentials_again(self, stub_hook):
        prepared = stub_hook._prepare_request("spaces/id/start-conversation")
        stub_hook._get_token.reset_mock()
        stub_hook._endpoint_url.reset_mock()

        with mock.patch(REQUEST, autospec=True, return_value=build_response()) as request:
            stub_hook._request(
                "POST",
                "spaces/id/start-conversation",
                body={"content": "question"},
                retry_read=False,
                prepared=prepared,
            )

        stub_hook._get_token.assert_not_called()
        stub_hook._endpoint_url.assert_not_called()
        assert request.call_args.args == ("POST", GENIE_URL)
        assert request.call_args.kwargs["auth"] is prepared.auth

    @pytest.mark.parametrize(
        ("status", "message"),
        [
            (401, "authentication failed"),
            (403, "may also return 403"),
            (404, "not found"),
            (429, "rate-limited"),
        ],
    )
    def test_clear_errors_for_common_status_codes(self, status, message):
        assert message in str(DatabricksGenieHook._http_error(status)).lower()

    def test_large_result_keeps_ids_and_valid_bounded_answer(self):
        result = _truncate_result(
            {
                "space_id": "space",
                "conversation_id": "conversation",
                "message_id": "message",
                "status": "COMPLETED",
                "answer": "x" * 100_000,
                "query_results": [{"rows": [["x"] * 20]}],
            }
        )
        encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode()
        assert len(encoded) <= 48 * 1024
        assert result["space_id"] == "space"
        assert result["conversation_id"] == "conversation"
        assert result["message_id"] == "message"
        assert result["truncated"] is True
