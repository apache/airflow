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

import subprocess
import sys
from importlib import import_module
from textwrap import dedent
from unittest import mock

import pytest
import requests
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa

import airflow.utils.deprecation_tools as deprecation_tools
from airflow.providers.snowflake.hooks.cortex_agent import (
    CreateMode,
    JsonResponse,
    SnowflakeCortexAgentHook,
)

deprecated_import_warning: type[Warning] = getattr(
    deprecation_tools,
    "DeprecatedImportWarning",
    DeprecationWarning,
)


try:
    from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
    from airflow.providers.common.ai.managed_agents import (
        ManagedAgentCapabilities,
        ManagedAgentRef,
        ManagedAgentRequest,
    )
except ImportError:
    HAS_COMMON_AI = False
else:
    HAS_COMMON_AI = True

needs_common_ai = pytest.mark.skipif(
    not HAS_COMMON_AI,
    reason="requires apache-airflow-providers-common-ai",
)

MODULE_PATH = "airflow.providers.snowflake.hooks.cortex_agent"
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
    content: bytes = b"{}",
):
    response = mock.MagicMock()
    response.status_code = status_code
    response.content = content
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

    @pytest.mark.parametrize(
        (
            "create_mode",
            "comment",
            "profile",
            "models",
            "instructions",
            "orchestration",
            "tools",
            "tool_resources",
            "expected_params",
            "expected_payload",
        ),
        [
            pytest.param(
                CreateMode.ERROR_IF_EXISTS,
                "Created by Airflow",
                {"display_name": "Airflow Agent"},
                {"orchestration": "claude-4-sonnet"},
                {"response": "Be concise"},
                {"max_tokens": 1000},
                [{"name": "search_tool"}],
                {"search_tool": {"config": "value"}},
                {"createMode": "errorIfExists"},
                {
                    "name": AGENT_NAME,
                    "comment": "Created by Airflow",
                    "profile": {"display_name": "Airflow Agent"},
                    "models": {"orchestration": "claude-4-sonnet"},
                    "instructions": {"response": "Be concise"},
                    "orchestration": {"max_tokens": 1000},
                    "tools": [{"name": "search_tool"}],
                    "tool_resources": {"search_tool": {"config": "value"}},
                },
                id="default",
            ),
            pytest.param(
                CreateMode.OR_REPLACE,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                {"createMode": "orReplace"},
                {"name": AGENT_NAME},
                id="or_replace",
            ),
            pytest.param(
                CreateMode.IF_NOT_EXISTS,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                {"createMode": "ifNotExists"},
                {"name": AGENT_NAME},
                id="if_not_exists",
            ),
            pytest.param(
                "orReplace",
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                {"createMode": "orReplace"},
                {"name": AGENT_NAME},
                id="raw_string_create_mode",
            ),
        ],
    )
    @mock.patch(f"{MODULE_PATH}.requests.request")
    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_create_agent(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
        create_mode,
        comment,
        profile,
        models,
        instructions,
        orchestration,
        tools,
        tool_resources,
        expected_params,
        expected_payload,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_request.return_value = create_response(
            json_body={"status": "created"},
        )

        hook = SnowflakeCortexAgentHook(
            snowflake_conn_id="mock_conn_id",
        )

        result = hook.create_agent(
            database=DATABASE,
            schema=SCHEMA,
            agent_name=AGENT_NAME,
            create_mode=create_mode,
            comment=comment,
            profile=profile,
            models=models,
            instructions=instructions,
            orchestration=orchestration,
            tools=tools,
            tool_resources=tool_resources,
        )

        assert result == {"status": "created"}

        mock_request.assert_called_once_with(
            method="POST",
            url=(
                f"https://{ACCOUNT}.snowflakecomputing.com"
                f"/api/v2/databases/{ENCODED_DATABASE}"
                f"/schemas/{ENCODED_SCHEMA}"
                f"/agents"
            ),
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "Content-Type": "application/json",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
            },
            json=expected_payload,
            params=expected_params,
            timeout=REQUEST_TIMEOUT,
        )

    def test_create_agent_rejects_invalid_create_mode(self):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        with pytest.raises(ValueError, match="invalid"):
            hook.create_agent(
                database=DATABASE,
                schema=SCHEMA,
                agent_name=AGENT_NAME,
                create_mode="invalid",
            )

    @mock.patch(f"{MODULE_PATH}.requests.request")
    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    @mock.patch(
        f"{HOOK_PATH}._get_static_conn_params",
        new_callable=mock.PropertyMock,
    )
    def test_update_agent(
        self,
        mock_static_conn_params,
        mock_conn_params,
        mock_request,
    ):
        mock_conn_params.return_value = CONN_PARAMS
        mock_static_conn_params.return_value = STATIC_CONN_PARAMS
        mock_request.return_value = create_response(content=b"")

        hook = SnowflakeCortexAgentHook(
            snowflake_conn_id="mock_conn_id",
        )

        result = hook.update_agent(
            database=DATABASE,
            schema=SCHEMA,
            agent_name=AGENT_NAME,
            comment="Updated by Airflow",
            profile={"display_name": "Updated Agent"},
            instructions={"response": "Always answer in one sentence."},
        )

        assert result == {}
        mock_request.return_value.json.assert_not_called()

        mock_request.assert_called_once_with(
            method="PUT",
            url=(
                f"https://{ACCOUNT}.snowflakecomputing.com"
                f"/api/v2/databases/{ENCODED_DATABASE}"
                f"/schemas/{ENCODED_SCHEMA}"
                f"/agents/{ENCODED_AGENT_NAME}"
            ),
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "Content-Type": "application/json",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
            },
            json={
                "comment": "Updated by Airflow",
                "profile": {"display_name": "Updated Agent"},
                "instructions": {"response": "Always answer in one sentence."},
            },
            params=None,
            timeout=REQUEST_TIMEOUT,
        )

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


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("SnowflakeCortexAgentHook", SnowflakeCortexAgentHook),
        ("CreateMode", CreateMode),
        ("JsonResponse", JsonResponse),
    ],
)
def test_deprecated_import(name, expected):
    old_module = import_module("airflow.providers.snowflake.hooks.snowflake_cortex_agent")
    with pytest.warns(deprecated_import_warning, match=name):
        assert getattr(old_module, name) is expected


@pytest.fixture
def managed_agent_request():
    with (
        mock.patch(
            f"{HOOK_PATH}._get_static_conn_params",
            new_callable=mock.PropertyMock,
            return_value=STATIC_CONN_PARAMS,
        ),
        mock.patch(
            f"{HOOK_PATH}._get_conn_params",
            return_value=CONN_PARAMS,
        ),
        mock.patch(f"{MODULE_PATH}.requests.request") as mock_request,
    ):
        mock_request.return_value = create_response(
            json_body={"content": [{"type": "text", "text": "Answer"}]},
        )
        yield mock_request


@needs_common_ai
class TestSnowflakeCortexAgentManagedAgent:
    @pytest.mark.parametrize(
        ("agent", "expected_path"),
        [
            pytest.param(
                "db.schema.agent",
                "/api/v2/databases/DB/schemas/SCHEMA/agents/AGENT:run",
                id="unquoted",
            ),
            pytest.param(
                'db."schema.part"."agent""name"',
                "/api/v2/databases/DB/schemas/schema.part/agents/agent%22name:run",
                id="quoted-dot-and-escaped-quote",
            ),
            pytest.param(
                '"My/Db"."My Schema"."MyAgent"',
                "/api/v2/databases/My%2FDb/schemas/My%20Schema/agents/MyAgent:run",
                id="quoted-case-space-and-slash",
            ),
        ],
    )
    def test_prompt_and_options_reach_the_api(self, managed_agent_request, agent, expected_path):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        response = hook.agent(agent).invoke(
            ManagedAgentRequest(
                prompt="Hello",
                timeout=1.5,
                vendor_options={"tool_choice": {"type": "auto"}},
            )
        )

        managed_agent_request.assert_called_once_with(
            method="POST",
            url=f"https://{ACCOUNT}.snowflakecomputing.com{expected_path}",
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "X-Snowflake-Authorization-Token-Type": "OAUTH",
                "Content-Type": "application/json",
            },
            json={
                "messages": [
                    {
                        "role": "user",
                        "content": [{"type": "text", "text": "Hello"}],
                    }
                ],
                "stream": False,
                "tool_choice": {"type": "auto"},
            },
            params=None,
            timeout=1.5,
        )
        assert response.text == "Answer"
        assert response.raw == {
            "content": [{"type": "text", "text": "Answer"}],
        }

    @pytest.mark.parametrize(
        ("agent", "expected_name"),
        [
            pytest.param(
                "db.schema.agent",
                "DB.SCHEMA.AGENT",
                id="unquoted",
            ),
            pytest.param(
                '"MyDb"."MySchema"."MyAgent"',
                '"MyDb"."MySchema"."MyAgent"',
                id="quoted-case",
            ),
            pytest.param(
                'db."schema.part"."agent""name"',
                'DB."schema.part"."agent""name"',
                id="quoted-dot-and-escaped-quote",
            ),
            pytest.param(
                'DB."A.B".C',
                'DB."A.B".C',
                id="dot-in-schema",
            ),
            pytest.param(
                'DB.A."B.C"',
                'DB.A."B.C"',
                id="dot-in-agent",
            ),
            pytest.param(
                'DB.SCHEMA."agent"',
                'DB.SCHEMA."agent"',
                id="quoted-lowercase-agent",
            ),
        ],
    )
    def test_resolve_agent(self, managed_agent_request, agent, expected_name):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        result = hook.resolve_agent(agent)

        assert result == ManagedAgentRef(
            platform="snowflake.cortex",
            name=expected_name,
        )
        assert hook.resolve_agent(result.name) == result
        managed_agent_request.assert_not_called()

    @pytest.mark.parametrize(
        "agent",
        [
            pytest.param("", id="empty"),
            pytest.param("AGENT", id="missing-database-and-schema"),
            pytest.param("DB.AGENT", id="missing-schema"),
            pytest.param("DB..AGENT", id="empty-schema"),
            pytest.param("DB.SCHEMA.AGENT.EXTRA", id="too-many-parts"),
            pytest.param('DB.SCHEMA."agent', id="unclosed-quote"),
            pytest.param('DB.SCHEMA.""', id="empty-quoted-name"),
        ],
    )
    def test_resolve_agent_rejects_invalid_names(self, managed_agent_request, agent):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        with pytest.raises(ValueError, match="fully qualified"):
            hook.resolve_agent(agent)

        managed_agent_request.assert_not_called()

    def test_get_agent_capabilities(self, managed_agent_request):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        capabilities = hook.get_agent_capabilities("DB.SCHEMA.AGENT")

        assert capabilities == ManagedAgentCapabilities(
            sessions=False,
            structured_output=False,
            usage=False,
            trace=False,
        )
        managed_agent_request.assert_not_called()

    @pytest.mark.parametrize(
        "string_content",
        [
            pytest.param(False, id="text-blocks"),
            pytest.param(True, id="string-content"),
        ],
    )
    def test_message_history_reaches_the_api(self, managed_agent_request, string_content):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")
        history = [
            ("user", "Hello"),
            ("assistant", "Hi"),
            ("user", "How are you?"),
        ]
        messages = [
            {
                "role": role,
                "content": text if string_content else [{"type": "text", "text": text}],
            }
            for role, text in history
        ]
        expected_messages = [
            {
                "role": role,
                "content": [{"type": "text", "text": text}],
            }
            for role, text in history
        ]

        response = hook.agent("DB.SCHEMA.AGENT").invoke(ManagedAgentRequest(messages=messages))

        assert managed_agent_request.call_args.kwargs["json"] == {
            "messages": expected_messages,
            "stream": False,
        }
        assert response.text == "Answer"

        # Converting string content must not mutate the caller's messages.
        assert messages == [
            {
                "role": role,
                "content": text if string_content else [{"type": "text", "text": text}],
            }
            for role, text in history
        ]

    @pytest.mark.parametrize(
        "bound",
        [
            pytest.param(False, id="direct"),
            pytest.param(True, id="bound"),
        ],
    )
    def test_session_id_is_rejected_before_the_call(self, managed_agent_request, bound):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")
        request = ManagedAgentRequest(prompt="Hello", session_id="123")

        invoke = (
            hook.agent("DB.SCHEMA.AGENT").invoke
            if bound
            else lambda request: hook.invoke_agent("DB.SCHEMA.AGENT", request)
        )

        with pytest.raises(ValueError, match="session_id"):
            invoke(request)

        managed_agent_request.assert_not_called()

    @pytest.mark.parametrize(
        ("options", "expected_error"),
        [
            pytest.param(
                {"database": "OTHER_DB"},
                "cannot override contract fields",
                id="database",
            ),
            pytest.param(
                {"schema": "OTHER_SCHEMA"},
                "cannot override contract fields",
                id="schema",
            ),
            pytest.param(
                {"agent_name": "OTHER_AGENT"},
                "cannot override contract fields",
                id="agent-name",
            ),
            pytest.param(
                {"snowflake_conn_id": "other_conn"},
                "cannot override contract fields",
                id="connection",
            ),
            pytest.param(
                {"messages": []},
                "cannot override contract fields",
                id="messages",
            ),
            pytest.param(
                {"timeout": 30},
                "cannot override contract fields",
                id="timeout",
            ),
            pytest.param(
                {"thread_id": 123},
                "cannot override contract fields",
                id="thread-id",
            ),
            pytest.param(
                {"parent_message_id": 456},
                "cannot override contract fields",
                id="parent-message-id",
            ),
            pytest.param(
                {"stream": True},
                "cannot override contract fields",
                id="stream",
            ),
            pytest.param(
                {"unknown_option": True},
                "Unsupported Cortex vendor_options",
                id="unknown-option",
            ),
        ],
    )
    def test_vendor_options_are_rejected_before_the_call(
        self, managed_agent_request, options, expected_error
    ):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        with pytest.raises(ValueError, match=expected_error):
            hook.agent("DB.SCHEMA.AGENT").invoke(ManagedAgentRequest(prompt="Hello", vendor_options=options))

        managed_agent_request.assert_not_called()

    @pytest.mark.parametrize(
        ("status_code", "terminal"),
        [
            pytest.param(400, True, id="bad-request"),
            pytest.param(401, True, id="unauthorized"),
            pytest.param(403, True, id="forbidden"),
            pytest.param(404, True, id="not-found"),
            pytest.param(405, True, id="method-not-allowed"),
            pytest.param(422, True, id="unprocessable"),
            pytest.param(408, False, id="request-timeout"),
            pytest.param(409, False, id="conflict"),
            pytest.param(429, False, id="throttled"),
            pytest.param(500, False, id="server-error"),
            pytest.param(503, False, id="unavailable"),
        ],
    )
    def test_http_errors_are_terminal_or_propagate(self, managed_agent_request, status_code, terminal):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")
        http_response = create_response(status_code=status_code)
        managed_agent_request.return_value = http_response
        original_error = http_response.raise_for_status.side_effect

        # Resolve the optional exception inside the test, not during collection.
        expected_exception = ManagedAgentInvocationError if terminal else requests.exceptions.HTTPError

        with pytest.raises(expected_exception) as raised:
            hook.agent("DB.SCHEMA.AGENT").invoke(ManagedAgentRequest(prompt="Hello"))

        if terminal:
            assert raised.value.__cause__ is original_error
        else:
            assert raised.value is original_error

        managed_agent_request.assert_called_once()

    @pytest.mark.parametrize(
        "error_type",
        [
            requests.exceptions.ConnectionError,
            requests.exceptions.ReadTimeout,
        ],
    )
    def test_transport_errors_propagate(self, managed_agent_request, error_type):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")
        error = error_type("Request failed")
        managed_agent_request.side_effect = error

        with pytest.raises(error_type) as raised:
            hook.agent("DB.SCHEMA.AGENT").invoke(ManagedAgentRequest(prompt="Hello"))

        assert raised.value is error
        managed_agent_request.assert_called_once()

    def test_invalid_json_is_terminal(self, managed_agent_request):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")
        error = requests.exceptions.JSONDecodeError("Invalid JSON", "{", 1)
        managed_agent_request.return_value.json.side_effect = error

        with pytest.raises(
            ManagedAgentInvocationError,
            match="could not be decoded as JSON",
        ) as raised:
            hook.agent("DB.SCHEMA.AGENT").invoke(ManagedAgentRequest(prompt="Hello"))

        assert raised.value.__cause__ is error

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param([], id="list"),
            pytest.param("unexpected", id="string"),
        ],
    )
    def test_invalid_response_shape_is_terminal(self, managed_agent_request, body):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")
        managed_agent_request.return_value = create_response(json_body=body)

        with pytest.raises(
            ManagedAgentInvocationError,
            match="unexpected response shape",
        ) as raised:
            hook.agent("DB.SCHEMA.AGENT").invoke(ManagedAgentRequest(prompt="Hello"))

        assert isinstance(raised.value.__cause__, TypeError)

    @pytest.mark.parametrize(
        "body",
        [
            pytest.param({"content": None}, id="null-content"),
            pytest.param({"content": [1]}, id="invalid-block"),
            pytest.param(
                {"content": [{"type": "text", "text": None}]},
                id="invalid-text",
            ),
        ],
    )
    def test_invalid_response_content_is_terminal(self, managed_agent_request, body):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")
        managed_agent_request.return_value = create_response(json_body=body)

        with pytest.raises(
            ManagedAgentInvocationError,
            match="invalid response content",
        ):
            hook.agent("DB.SCHEMA.AGENT").invoke(ManagedAgentRequest(prompt="Hello"))

    def test_unrelated_type_error_propagates(self):
        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")
        request = ManagedAgentRequest(prompt="Hello")
        error = TypeError("Unrelated implementation error")

        with mock.patch.object(hook, "run_agent", autospec=True, side_effect=error):
            with pytest.raises(TypeError) as raised:
                hook.invoke_agent("DB.SCHEMA.AGENT", request)

        assert raised.value is error


def test_hook_imports_without_common_ai():
    script = dedent(
        """
        import sys
        from unittest import mock

        for name in (
            "airflow.providers.common.ai.exceptions",
            "airflow.providers.common.ai.managed_agents.base",
        ):
            sys.modules[name] = None

        from airflow.providers.snowflake.hooks.cortex_agent import SnowflakeCortexAgentHook
        from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        for method, args in (
            ("agent", ("DB.SCHEMA.AGENT",)),
            ("resolve_agent", ("DB.SCHEMA.AGENT",)),
            ("get_agent_capabilities", ("DB.SCHEMA.AGENT",)),
            ("invoke_agent", ("DB.SCHEMA.AGENT", None)),
        ):
            try:
                getattr(hook, method)(*args)
            except AirflowOptionalProviderFeatureException as exc:
                assert "snowflake[common.ai]" in str(exc), exc
            else:
                raise AssertionError(f"{method} did not raise")

        with mock.patch.object(hook, "_request", return_value={}) as request:
            assert hook.run_agent(
                database="DB",
                schema="SCHEMA",
                agent_name="AGENT",
                messages=[],
            ) == {}
            request.assert_called_once()
        """
    )
    subprocess.run([sys.executable, "-c", script], check=True)
