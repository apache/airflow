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
import requests

from airflow.providers.databricks.hooks.genie import (
    DatabricksGenieError,
    DatabricksGenieHook,
    _truncate_result,
)


class TestDatabricksGenieHook:
    def test_consult_starts_once_polls_reads_and_returns_ids_and_query_result(self):
        hook = DatabricksGenieHook(poll_interval=0.001)
        hook._request = mock.Mock(
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
            ]
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
        hook._request = mock.Mock(
            side_effect=[
                {"conversation_id": "conversation-1", "message_id": "message-2"},
                {"status": "COMPLETED", "attachments": [{"text": {"content": "Answer."}}]},
            ]
        )

        hook.consult("space-1", "Another question", conversation_id="conversation-1")

        assert hook._request.call_args_list[0].args[:2] == (
            "POST",
            "spaces/space-1/conversations/conversation-1/messages",
        )
        assert hook._request.call_args_list[0].kwargs["retry_read"] is False
        assert hook._request.call_count == 2

    def test_non_idempotent_post_is_never_retried_after_timeout(self):
        hook = DatabricksGenieHook()
        hook._endpoint_url = mock.Mock(
            return_value="https://workspace/api/2.0/genie/spaces/id/start-conversation"
        )
        hook.user_agent_header = {}
        hook._get_aad_headers = mock.Mock(return_value={})
        hook._get_token = mock.Mock(return_value="secret-token")
        hook._get_requests_kwargs = mock.Mock(return_value={})

        with (
            mock.patch(
                "airflow.providers.databricks.hooks.genie.requests.request",
                side_effect=requests.Timeout("timeout"),
            ) as request,
            pytest.raises(DatabricksGenieError, match="did not retry") as error,
        ):
            hook._request(
                "POST",
                "spaces/id/start-conversation",
                body={"content": "question"},
                retry_read=False,
            )

        assert request.call_count == 1
        assert "secret-token" not in str(error.value)

    def test_get_read_is_retried_after_a_server_error(self):
        hook = DatabricksGenieHook(retry_limit=2, retry_delay=0.001)
        hook._endpoint_url = mock.Mock(return_value="https://workspace/api/2.0/genie/message")
        hook.user_agent_header = {}
        hook._get_aad_headers = mock.Mock(return_value={})
        hook._get_token = mock.Mock(return_value="secret-token")
        hook._get_requests_kwargs = mock.Mock(return_value={})
        failed_response = requests.Response()
        failed_response.status_code = 503
        failed_response._content = b'{"error":"temporarily unavailable"}'
        failed_response._content_consumed = True
        failed_response.url = "https://workspace/api/2.0/genie/message"
        success_response = requests.Response()
        success_response.status_code = 200
        success_response._content = b'{"status":"COMPLETED"}'
        success_response._content_consumed = True
        success_response.url = "https://workspace/api/2.0/genie/message"

        with mock.patch(
            "airflow.providers.databricks.hooks.genie.requests.request",
            side_effect=[failed_response, success_response],
        ) as request:
            result = hook._request("GET", "message", retry_read=True)

        assert result == {"status": "COMPLETED"}
        assert request.call_count == 2

    def test_server_error_on_post_is_ambiguous_and_never_retried(self):
        hook = DatabricksGenieHook()
        hook._endpoint_url = mock.Mock(return_value="https://workspace/api/2.0/genie/message")
        hook.user_agent_header = {}
        hook._get_aad_headers = mock.Mock(return_value={})
        hook._get_token = mock.Mock(return_value="secret-token")
        hook._get_requests_kwargs = mock.Mock(return_value={})
        response = requests.Response()
        response.status_code = 503
        response._content = b'{"error":"temporarily unavailable"}'
        response._content_consumed = True
        response.url = "https://workspace/api/2.0/genie/message"

        with (
            mock.patch(
                "airflow.providers.databricks.hooks.genie.requests.request", return_value=response
            ) as request,
            pytest.raises(DatabricksGenieError, match="outcome is unknown") as error,
        ):
            hook._request("POST", "message", body={"content": "question"}, retry_read=False)

        assert request.call_count == 1
        assert "secret-token" not in str(error.value)

    def test_invalid_json_on_post_is_ambiguous_and_never_retried(self):
        hook = DatabricksGenieHook()
        hook._endpoint_url = mock.Mock(return_value="https://workspace/api/2.0/genie/message")
        hook.user_agent_header = {}
        hook._get_aad_headers = mock.Mock(return_value={})
        hook._get_token = mock.Mock(return_value="secret-token")
        hook._get_requests_kwargs = mock.Mock(return_value={})
        response = requests.Response()
        response.status_code = 200
        response._content = b"not json"
        response._content_consumed = True
        response.url = "https://workspace/api/2.0/genie/message"

        with (
            mock.patch(
                "airflow.providers.databricks.hooks.genie.requests.request", return_value=response
            ) as request,
            pytest.raises(DatabricksGenieError, match="outcome is unknown") as error,
        ):
            hook._request("POST", "message", body={"content": "question"}, retry_read=False)

        assert request.call_count == 1
        assert "secret-token" not in str(error.value)

    def test_remote_http_endpoint_is_rejected_before_credentials_are_sent(self):
        hook = DatabricksGenieHook()
        hook._endpoint_url = mock.Mock(return_value="http://workspace.example/api/2.0/genie/message")
        with (
            mock.patch("airflow.providers.databricks.hooks.genie.requests.request") as request,
            pytest.raises(DatabricksGenieError, match="requires HTTPS"),
        ):
            hook._request("GET", "message", retry_read=True)
        request.assert_not_called()

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
