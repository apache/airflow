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

from unittest import mock

import pytest
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa

from airflow.providers.snowflake.hooks.snowflake_cortex_agent import (
    JsonResponse,
    SnowflakeCortexAgentHook,
)

MODULE_PATH = "airflow.providers.snowflake.hooks.snowflake_cortex_agent"
HOOK_PATH = f"{MODULE_PATH}.SnowflakeCortexAgentHook"

ACCOUNT = "test-account"
ACCESS_TOKEN = "test-token"
DATABASE = "TEST/DATABASE"
SCHEMA = "TEST?SCHEMA"
AGENT_NAME = "TEST#AGENT"

ENCODED_DATABASE = "TEST%2FDATABASE"
ENCODED_SCHEMA = "TEST%3FSCHEMA"
ENCODED_AGENT_NAME = "TEST%23AGENT"

CONN_PARAMS = {
    "account": ACCOUNT,
    "token": ACCESS_TOKEN,
    "authenticator": "oauth",
}

STATIC_CONN_PARAMS = {
    "account": ACCOUNT,
}

REQUEST_TIMEOUT = 600


def create_response(
    status_code: int = 200,
    *,
    json_body: JsonResponse | None = None,
):
    response = mock.MagicMock()
    response.status_code = status_code
    response.json.return_value = {} if json_body is None else json_body

    if status_code >= 400:
        response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=response)
    else:
        response.raise_for_status.return_value = None

    return response


class TestSnowflakeCortexAgentHook:
    @pytest.mark.parametrize(
        ("method_name", "method_kwargs", "json_body", "expected_error"),
        [
            pytest.param(
                "describe_agent",
                {
                    "database": DATABASE,
                    "schema": SCHEMA,
                    "agent_name": AGENT_NAME,
                },
                [{"name": AGENT_NAME}],
                "Expected dict response, got list",
                id="describe_agent_expected_dict_got_list",
            ),
            pytest.param(
                "list_agents",
                {
                    "database": DATABASE,
                    "schema": SCHEMA,
                },
                {"name": AGENT_NAME},
                r"Expected list\[dict\] response, got dict",
                id="list_agents_expected_list_got_dict",
            ),
            pytest.param(
                "list_agents",
                {
                    "database": DATABASE,
                    "schema": SCHEMA,
                },
                [{"name": AGENT_NAME}, 1],
                r"Expected list\[dict\] response, got list containing non-dict elements",
                id="list_agents_contains_non_dict_element",
            ),
        ],
    )
    @mock.patch(f"{MODULE_PATH}.requests.request")
    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    @mock.patch(f"{HOOK_PATH}._get_static_conn_params", new_callable=mock.PropertyMock)
    def test_agent_methods_raise_for_unexpected_response_shape(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
        method_name,
        method_kwargs,
        json_body,
        expected_error,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_request.return_value = create_response(json_body=json_body)

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        with pytest.raises(TypeError, match=expected_error):
            getattr(hook, method_name)(**method_kwargs)

    @mock.patch(f"{MODULE_PATH}.requests.request")
    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_run_agent(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_request.return_value = create_response(json_body={"status": "completed"})

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        result = hook.run_agent(
            database=DATABASE,
            schema=SCHEMA,
            agent_name=AGENT_NAME,
            messages=[
                {
                    "role": "user",
                    "content": [
                        {
                            "type": "text",
                            "text": "Hello",
                        }
                    ],
                }
            ],
        )

        assert result == {"status": "completed"}

        mock_request.assert_called_once_with(
            method="POST",
            url=(
                f"https://{ACCOUNT}.snowflakecomputing.com"
                f"/api/v2/databases/{ENCODED_DATABASE}"
                f"/schemas/{ENCODED_SCHEMA}"
                f"/agents/{ENCODED_AGENT_NAME}:run"
            ),
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
                "Content-Type": "application/json",
            },
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "text",
                                "text": "Hello",
                            }
                        ],
                    }
                ],
                "stream": False,
            },
            params=None,
            timeout=REQUEST_TIMEOUT,
        )

    def test_run_agent_requires_parent_message_id_when_thread_id_provided(self):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        with pytest.raises(
            ValueError,
            match="parent_message_id must be provided",
        ):
            hook.run_agent(
                database=DATABASE,
                schema=SCHEMA,
                agent_name=AGENT_NAME,
                messages=[],
                thread_id=123,
            )

    @mock.patch(f"{MODULE_PATH}.requests.request")
    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_run_agent_includes_thread_fields(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_request.return_value = create_response()

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        hook.run_agent(
            database=DATABASE,
            schema=SCHEMA,
            agent_name=AGENT_NAME,
            messages=[],
            thread_id=123,
            parent_message_id=456,
        )

        payload = mock_request.call_args.kwargs["json"]

        assert payload["thread_id"] == 123
        assert payload["parent_message_id"] == 456
        assert payload["stream"] is False

    @mock.patch(f"{MODULE_PATH}.requests.request")
    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_run_agent_includes_optional_fields(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_request.return_value = create_response()

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        hook.run_agent(
            database=DATABASE,
            schema=SCHEMA,
            agent_name=AGENT_NAME,
            messages=[],
            tool_choice={"type": "auto"},
            models={"orchestration": "claude-4-sonnet"},
            instructions={"response": "be concise"},
            orchestration={"max_tokens": 1000},
            tools=[{"name": "search_tool"}],
            tool_resources={"search_tool": {"config": "value"}},
        )

        payload = mock_request.call_args.kwargs["json"]

        assert payload["tool_choice"] == {"type": "auto"}
        assert payload["models"] == {"orchestration": "claude-4-sonnet"}
        assert payload["instructions"] == {"response": "be concise"}
        assert payload["orchestration"] == {"max_tokens": 1000}
        assert payload["tools"] == [{"name": "search_tool"}]
        assert payload["tool_resources"] == {"search_tool": {"config": "value"}}
        assert payload["stream"] is False

    @mock.patch(f"{MODULE_PATH}.requests.request")
    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_run_agent_http_error(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_request.return_value = create_response(
            status_code=400,
            json_body={"error": "boom"},
        )

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        with pytest.raises(requests.exceptions.HTTPError):
            hook.run_agent(
                database=DATABASE,
                schema=SCHEMA,
                agent_name=AGENT_NAME,
                messages=[],
            )

    @mock.patch(f"{HOOK_PATH}.get_private_key", autospec=True, return_value=None)
    @mock.patch(f"{HOOK_PATH}._get_conn_params", autospec=True)
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_request_raises_when_no_rest_credentials_available(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_private_key,
    ):
        """Neither OAuth, PAT, nor a private key is configured: this must raise ValueError."""
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_conn_params.return_value = {"account": ACCOUNT}

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        with pytest.raises(
            ValueError,
            match="key-pair JWT",
        ):
            hook.describe_agent(database=DATABASE, schema=SCHEMA, agent_name=AGENT_NAME)

    @mock.patch(f"{MODULE_PATH}.requests.request", autospec=True)
    @mock.patch(f"{HOOK_PATH}._get_conn_params", autospec=True)
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_request_sends_pat_headers(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
    ):
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_conn_params.return_value = {
            "account": ACCOUNT,
            "authenticator": "programmatic_access_token",
            "password": "my-pat-value",
        }
        mock_request.return_value = create_response(json_body={"name": AGENT_NAME})

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")
        hook.describe_agent(database=DATABASE, schema=SCHEMA, agent_name=AGENT_NAME)

        headers = mock_request.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer my-pat-value"
        assert headers["X-Snowflake-Authorization-Token-Type"] == "PROGRAMMATIC_ACCESS_TOKEN"

    @mock.patch(f"{MODULE_PATH}.requests.request", autospec=True)
    @mock.patch(f"{HOOK_PATH}.get_private_key", autospec=True, return_value=None)
    @mock.patch(f"{HOOK_PATH}._get_conn_params", autospec=True)
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_request_raises_for_workload_identity_connections(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_get_private_key,
        mock_request,
    ):
        """WORKLOAD_IDENTITY connections without a private key are not supported for REST APIs --
        SnowflakeHook sets a ``token`` in extras for this authenticator, but it is not one of the
        three REST-accepted credential types, so this must be an explicit, actionable error rather
        than silently falling into (and failing) the key-pair branch."""
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_conn_params.return_value = {
            "account": ACCOUNT,
            "workload_identity_provider": "AWS",
            "token": "wif-token",
        }

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        with pytest.raises(ValueError, match="Workload identity federation"):
            hook.describe_agent(database=DATABASE, schema=SCHEMA, agent_name=AGENT_NAME)

        mock_request.assert_not_called()

    @mock.patch(f"{MODULE_PATH}.requests.request", autospec=True)
    @mock.patch(f"{HOOK_PATH}.get_private_key", autospec=True)
    @mock.patch(f"{HOOK_PATH}._get_conn_params", autospec=True)
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_request_sends_key_pair_headers(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_private_key,
        mock_request,
    ):
        key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
        mock_private_key.return_value = key
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_conn_params.return_value = {"account": ACCOUNT, "user": "user"}
        mock_request.return_value = create_response(json_body={"name": AGENT_NAME})

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")
        hook.describe_agent(database=DATABASE, schema=SCHEMA, agent_name=AGENT_NAME)

        headers = mock_request.call_args.kwargs["headers"]
        assert headers["Authorization"].startswith("Bearer ")
        assert headers["X-Snowflake-Authorization-Token-Type"] == "KEYPAIR_JWT"

    @mock.patch(f"{MODULE_PATH}.requests.request", autospec=True)
    @mock.patch(f"{HOOK_PATH}.get_private_key", autospec=True)
    @mock.patch(f"{HOOK_PATH}._get_conn_params", autospec=True)
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_request_reuses_jwt_within_renewal_window(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_private_key,
        mock_request,
        time_machine,
    ):
        """A single hook instance must reuse its JWT within the renewal window and mint a new
        one once it has passed -- pinned with real elapsed time, not call count, since two
        back-to-back calls produce byte-identical JWTs on the old mint-every-call code too
        (RS256 signing is deterministic and `iat` only has one-second resolution)."""
        key = rsa.generate_private_key(backend=default_backend(), public_exponent=65537, key_size=2048)
        mock_private_key.return_value = key
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_conn_params.return_value = {"account": ACCOUNT, "user": "user"}
        mock_request.return_value = create_response(json_body={"name": AGENT_NAME})

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        time_machine.move_to("2024-01-01T00:00:00+00:00", tick=False)
        hook.describe_agent(database=DATABASE, schema=SCHEMA, agent_name=AGENT_NAME)

        time_machine.move_to("2024-01-01T00:10:00+00:00", tick=False)
        hook.describe_agent(database=DATABASE, schema=SCHEMA, agent_name=AGENT_NAME)

        first_auth = mock_request.call_args_list[0].kwargs["headers"]["Authorization"]
        second_auth = mock_request.call_args_list[1].kwargs["headers"]["Authorization"]
        assert first_auth == second_auth
        assert mock_private_key.call_count == 1

        time_machine.move_to("2024-01-01T01:00:00+00:00", tick=False)
        hook.describe_agent(database=DATABASE, schema=SCHEMA, agent_name=AGENT_NAME)

        third_auth = mock_request.call_args_list[2].kwargs["headers"]["Authorization"]
        assert third_auth != first_auth

    @pytest.mark.parametrize(
        ("response", "expected"),
        [
            (
                {
                    "content": [
                        {
                            "type": "text",
                            "text": "Hello ",
                        },
                        {
                            "type": "thinking",
                            "thinking": {
                                "text": "internal reasoning",
                            },
                        },
                        {
                            "type": "text",
                            "text": "world",
                        },
                    ]
                },
                "Hello world",
            ),
            (
                {},
                "",
            ),
            (
                {
                    "content": [
                        {
                            "type": "thinking",
                            "thinking": {
                                "text": "internal reasoning",
                            },
                        },
                        {
                            "type": "tool_use",
                            "tool": "search_tool",
                        },
                    ]
                },
                "",
            ),
        ],
    )
    def test_get_text_response(
        self,
        response,
        expected,
    ):
        assert SnowflakeCortexAgentHook.get_text_response(response) == expected

    @mock.patch(f"{MODULE_PATH}.requests.request")
    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_describe_agent(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_request.return_value = create_response(
            json_body={"name": AGENT_NAME},
        )

        hook = SnowflakeCortexAgentHook(
            snowflake_conn_id="mock_conn_id",
        )

        result = hook.describe_agent(
            database=DATABASE,
            schema=SCHEMA,
            agent_name=AGENT_NAME,
        )

        assert result == {"name": AGENT_NAME}

        mock_request.assert_called_once_with(
            method="GET",
            url=(
                f"https://{ACCOUNT}.snowflakecomputing.com"
                f"/api/v2/databases/{ENCODED_DATABASE}"
                f"/schemas/{ENCODED_SCHEMA}"
                f"/agents/{ENCODED_AGENT_NAME}"
            ),
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
                "Content-Type": "application/json",
            },
            json=None,
            params=None,
            timeout=REQUEST_TIMEOUT,
        )

    @mock.patch(f"{MODULE_PATH}.requests.request")
    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_list_agents(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_request.return_value = create_response(
            json_body=[{"name": AGENT_NAME}],
        )

        hook = SnowflakeCortexAgentHook(
            snowflake_conn_id="mock_conn_id",
        )

        result = hook.list_agents(
            database=DATABASE,
            schema=SCHEMA,
            like="AIRFLOW%",
            from_name="AIRFLOW_TEST",
            show_limit=10,
        )

        assert result == [{"name": AGENT_NAME}]

        mock_request.assert_called_once_with(
            method="GET",
            url=(
                f"https://{ACCOUNT}.snowflakecomputing.com"
                f"/api/v2/databases/{ENCODED_DATABASE}"
                f"/schemas/{ENCODED_SCHEMA}"
                f"/agents"
            ),
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
                "Content-Type": "application/json",
            },
            json=None,
            params={
                "like": "AIRFLOW%",
                "fromName": "AIRFLOW_TEST",
                "showLimit": 10,
            },
            timeout=REQUEST_TIMEOUT,
        )

    @pytest.mark.parametrize(
        ("if_exists", "expected"),
        [
            pytest.param(True, "true", id="if_exists"),
            pytest.param(False, "false", id="error_if_missing"),
        ],
    )
    @mock.patch(f"{MODULE_PATH}.requests.request")
    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_delete_agent(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
        if_exists,
        expected,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_request.return_value = create_response(
            json_body={"status": "deleted"},
        )

        hook = SnowflakeCortexAgentHook(
            snowflake_conn_id="mock_conn_id",
        )

        result = hook.delete_agent(
            database=DATABASE,
            schema=SCHEMA,
            agent_name=AGENT_NAME,
            if_exists=if_exists,
        )

        assert result == {"status": "deleted"}

        mock_request.assert_called_once_with(
            method="DELETE",
            url=(
                f"https://{ACCOUNT}.snowflakecomputing.com"
                f"/api/v2/databases/{ENCODED_DATABASE}"
                f"/schemas/{ENCODED_SCHEMA}"
                f"/agents/{ENCODED_AGENT_NAME}"
            ),
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
                "Content-Type": "application/json",
            },
            json=None,
            params={"ifExists": expected},
            timeout=REQUEST_TIMEOUT,
        )

    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_base_url_appends_region(self, mock_static_conn_params):
        mock_static_conn_params.return_value = {"account": ACCOUNT, "region": "us-east-2.aws"}

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        assert hook._get_base_url() == f"https://{ACCOUNT}.us-east-2.aws.snowflakecomputing.com"

    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_base_url_rejects_account_outside_charset(self, mock_static_conn_params):
        """``account`` is interpolated into the base URL, so it may not carry URL punctuation."""
        mock_static_conn_params.return_value = {"account": "acct.example.com/x"}

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        with pytest.raises(ValueError, match="Invalid Snowflake account"):
            hook._get_base_url()
