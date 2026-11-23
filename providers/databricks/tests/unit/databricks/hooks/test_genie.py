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
import io
import json
from types import SimpleNamespace
from unittest import mock

import pytest
import requests

from airflow.providers.common.ai.managed_agents.base import ManagedAgentRequest
from airflow.providers.common.ai.toolsets import ManagedAgentToolset
from airflow.providers.common.compat.sdk import AirflowException, AirflowOptionalProviderFeatureException
from airflow.providers.databricks.exceptions import DatabricksApiError, DatabricksGenieError
from airflow.providers.databricks.hooks import genie as genie_module
from airflow.providers.databricks.hooks.databricks_base import _TokenAuth
from airflow.providers.databricks.hooks.genie import (
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
    response.raw = io.BytesIO(content)
    response.url = url
    return response


MESSAGE_PATH = "spaces/space-1/conversations/conversation-1/messages/message-1"
CREATED = {"conversation_id": "conversation-1", "message_id": "message-1"}
ANSWERED = {"status": "COMPLETED", "attachments": [{"text": {"content": "Answer."}}]}


@pytest.fixture
def hook():
    with mock.patch.object(
        DatabricksGenieHook, "user_agent_header", new_callable=mock.PropertyMock, return_value={}
    ):
        yield DatabricksGenieHook(retry_limit=2, retry_delay=0.001, poll_interval=0.001)


@pytest.fixture
def genie_request():
    """Replace the hook's HTTP call, so consult tests only script Genie's answers."""
    with mock.patch.object(DatabricksGenieHook, "_request", autospec=True) as request:
        yield request


@pytest.fixture
def connection():
    """Spec'd stand-ins for the connection lookups, so ``_request`` runs without a real connection."""
    with (
        mock.patch.object(DatabricksGenieHook, "_endpoint_url", autospec=True, return_value=GENIE_URL) as url,
        mock.patch.object(DatabricksGenieHook, "_get_aad_headers", autospec=True, return_value={}),
        mock.patch.object(
            DatabricksGenieHook, "_get_token", autospec=True, return_value="secret-token"
        ) as token,
        mock.patch.object(DatabricksGenieHook, "_get_requests_kwargs", autospec=True, return_value={}),
    ):
        yield SimpleNamespace(endpoint_url=url, get_token=token)


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
        with mock.patch.object(
            module.DatabricksGenieHook, "_request", autospec=True, side_effect=[CREATED, ANSWERED]
        ) as request:
            result = hook.consult("space-1", "Question?")
        assert result["answer"] == "Answer."
        assert result["conversation_id"] == "conversation-1"
        assert request.call_count == 2
        with pytest.raises(AirflowOptionalProviderFeatureException, match="common.ai"):
            hook.agent("space-1")

    def test_standard_managed_agent_toolset_returns_bounded_structured_ids(self, hook, genie_request):
        genie_request.side_effect = [CREATED, ANSWERED]
        toolset = ManagedAgentToolset(hook.agent("space-1"), tool_name="ask_genie", max_retries=0)

        result = json.loads(toolset.invoke_sync("Question?"))

        assert result["space_id"] == "space-1"
        assert result["conversation_id"] == "conversation-1"
        assert result["message_id"] == "message-1"
        assert result["answer"] == "Answer."
        assert genie_request.call_count == 2

    def test_invoke_agent_refuses_vendor_options(self, hook, genie_request):
        request = ManagedAgentRequest(prompt="Question?", vendor_options={"space_id": "other-space"})

        with pytest.raises(ValueError, match=r"does not support vendor_options: \['space_id'\]"):
            hook.invoke_agent("space-1", request)
        genie_request.assert_not_called()

    def test_consult_starts_once_polls_reads_and_returns_ids_and_query_result(self, hook, genie_request):
        genie_request.side_effect = [
            CREATED,
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
                    "manifest": {"schema": {"columns": [{"name": "count"}]}, "total_row_count": 1},
                    "result": {"data_array": [["12"]]},
                }
            },
        ]

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
        calls = genie_request.call_args_list
        assert len(calls) == 4
        assert calls[0].args[1:] == ("POST", "spaces/space-1/start-conversation")
        assert calls[0].kwargs["retry_read"] is False
        assert all(call.kwargs.get("retry_read") is True for call in calls[1:])

    def test_resumed_consult_posts_into_existing_conversation_once(self, hook, genie_request):
        genie_request.side_effect = [{**CREATED, "message_id": "message-2"}, ANSWERED]

        hook.consult("space-1", "Another question", conversation_id="conversation-1")

        calls = genie_request.call_args_list
        assert calls[0].args[1:] == ("POST", "spaces/space-1/conversations/conversation-1/messages")
        assert calls[0].kwargs["retry_read"] is False
        assert len(calls) == 2

    @pytest.mark.parametrize(
        ("message", "match"),
        [
            pytest.param(
                {"status": "FAILED", "error": {"error": "bad SQL"}},
                "could not answer the question: bad SQL",
                id="failed",
            ),
            pytest.param({"status": "CANCELLED"}, "ended with status CANCELLED", id="cancelled"),
            pytest.param(
                {"status": "QUERY_RESULT_EXPIRED"}, "ended with status QUERY_RESULT_EXPIRED", id="expired"
            ),
            pytest.param(
                {"status": "SOMETHING_NEW"}, "unknown message status: 'SOMETHING_NEW'", id="unknown"
            ),
        ],
    )
    def test_consult_raises_for_terminal_status(self, hook, genie_request, message, match):
        genie_request.side_effect = [CREATED, message]

        with pytest.raises(DatabricksGenieError, match=match) as error:
            hook.consult("space-1", "Question?")

        assert isinstance(error.value, DatabricksApiError)
        assert error.value.http_status_code is None
        assert genie_request.call_args_list[1].args[1:] == ("GET", MESSAGE_PATH)

    def test_consult_times_out(self, hook, genie_request):
        genie_request.side_effect = [CREATED, {"status": "ASKING_AI"}]

        with (
            mock.patch.object(genie_module.time, "monotonic", autospec=True, side_effect=[0, 11]),
            mock.patch.object(genie_module.time, "sleep", autospec=True) as sleep,
            pytest.raises(DatabricksGenieError, match="Timed out waiting for Genie message message-1"),
        ):
            hook.consult("space-1", "Question?", timeout=10)

        sleep.assert_not_called()
        assert [call.args[1] for call in genie_request.call_args_list] == ["POST", "GET"]

    def test_non_idempotent_post_is_never_retried_after_timeout(self, hook, connection):
        with (
            mock.patch(REQUEST, autospec=True, side_effect=requests.Timeout("timeout")) as request,
            pytest.raises(DatabricksGenieError, match="did not retry") as error,
        ):
            hook._request(
                "POST", "spaces/id/start-conversation", body={"content": "question"}, retry_read=False
            )

        assert request.call_count == 1
        assert "secret-token" not in str(error.value)

    def test_token_request_uses_token_auth_so_netrc_cannot_override_bearer(self, hook, connection):
        """_TokenAuth keeps requests from applying ~/.netrc over the bearer token."""
        with mock.patch(REQUEST, autospec=True, return_value=build_response()) as request:
            assert hook._request("GET", "message", retry_read=True) == {"status": "COMPLETED"}

        auth = request.call_args.kwargs["auth"]
        assert isinstance(auth, _TokenAuth)
        assert auth.token == "secret-token"
        assert "Authorization" not in request.call_args.kwargs["headers"]

    def test_get_read_is_retried_after_a_server_error(self, hook, connection):
        responses = [build_response(503, b'{"error":"temporarily unavailable"}'), build_response()]
        with mock.patch(REQUEST, autospec=True, side_effect=responses) as request:
            result = hook._request("GET", "message", retry_read=True)

        assert result == {"status": "COMPLETED"}
        assert request.call_count == 2

    @pytest.mark.parametrize(
        "response",
        [
            pytest.param(build_response(503, b'{"error":"temporarily unavailable"}'), id="server-error"),
            pytest.param(build_response(200, b"not json"), id="invalid-json"),
        ],
    )
    def test_ambiguous_post_is_never_retried(self, hook, connection, response):
        with (
            mock.patch(REQUEST, autospec=True, return_value=response) as request,
            pytest.raises(DatabricksGenieError, match="outcome is unknown") as error,
        ):
            hook._request("POST", "message", body={"content": "question"}, retry_read=False)

        assert request.call_count == 1
        assert "secret-token" not in str(error.value)

    def test_rejected_request_carries_http_status_code(self, hook, connection):
        with (
            mock.patch(REQUEST, autospec=True, return_value=build_response(404, b"{}")),
            pytest.raises(DatabricksApiError, match="not found") as error,
        ):
            hook._request("GET", "message", retry_read=True)

        assert isinstance(error.value, DatabricksGenieError)
        assert error.value.http_status_code == 404

    def test_remote_http_endpoint_is_rejected_before_credentials_are_sent(self, hook, connection):
        connection.endpoint_url.return_value = "http://workspace.example/api/2.0/genie/message"
        with (
            mock.patch(REQUEST, autospec=True) as request,
            pytest.raises(DatabricksGenieError, match="requires HTTPS"),
        ):
            hook._request("GET", "message", retry_read=True)
        request.assert_not_called()
        connection.get_token.assert_not_called()

    def test_credential_failure_is_raised_before_anything_is_sent(self, hook, connection):
        connection.get_token.side_effect = AirflowException("token endpoint unavailable")
        with (
            mock.patch(REQUEST, autospec=True) as request,
            pytest.raises(AirflowException, match="token endpoint unavailable"),
        ):
            hook._prepare_request("spaces/id/start-conversation")
        request.assert_not_called()

    def test_prepared_request_is_sent_without_resolving_credentials_again(self, hook, connection):
        prepared = hook._prepare_request("spaces/id/start-conversation")
        connection.get_token.reset_mock()
        connection.endpoint_url.reset_mock()

        with mock.patch(REQUEST, autospec=True, return_value=build_response()) as request:
            hook._request(
                "POST",
                "spaces/id/start-conversation",
                body={"content": "question"},
                retry_read=False,
                prepared=prepared,
            )

        connection.get_token.assert_not_called()
        connection.endpoint_url.assert_not_called()
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
