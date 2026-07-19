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

from airflow.providers.snowflake.hooks.snowflake_cortex_agent import (
    SnowflakeCortexAgentHook,
)

MODULE_PATH = "airflow.providers.snowflake.hooks.snowflake_cortex_agent"
HOOK_PATH = f"{MODULE_PATH}.SnowflakeCortexAgentHook"

ACCOUNT = "test-account"
ACCESS_TOKEN = "test-token"
DATABASE = "TEST_DATABASE"
SCHEMA = "TEST_SCHEMA"
AGENT_NAME = "TEST_AGENT"

CONN_PARAMS = {
    "account": ACCOUNT,
    "token": ACCESS_TOKEN,
}

STATIC_CONN_PARAMS = {
    "account": ACCOUNT,
}

REQUEST_TIMEOUT = 600


def create_response(
    status_code: int = 200,
    *,
    json_body: dict | None = None,
):
    response = mock.MagicMock()
    response.status_code = status_code
    response.json.return_value = json_body or {}

    if status_code >= 400:
        response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=response)
    else:
        response.raise_for_status.return_value = None

    return response


class TestSnowflakeCortexAgentHook:
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
                f"/api/v2/databases/{DATABASE}"
                f"/schemas/{SCHEMA}"
                f"/agents/{AGENT_NAME}:run"
            ),
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
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

    @mock.patch(f"{HOOK_PATH}._get_conn_params")
    def test_get_access_token_raises_when_token_missing(
        self,
        mock_conn_params,
    ):
        mock_conn_params.return_value = {}

        hook = SnowflakeCortexAgentHook(snowflake_conn_id="mock_conn_id")

        with pytest.raises(
            ValueError,
            match="access token",
        ):
            hook._get_access_token()

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
                f"/api/v2/databases/{DATABASE}"
                f"/schemas/{SCHEMA}"
                f"/agents/{AGENT_NAME}"
            ),
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            json=None,
            params=None,
            timeout=None,
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
                f"/api/v2/databases/{DATABASE}"
                f"/schemas/{SCHEMA}"
                f"/agents"
            ),
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            json=None,
            params={
                "like": "AIRFLOW%",
                "fromName": "AIRFLOW_TEST",
                "showLimit": 10,
            },
            timeout=None,
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
                f"/api/v2/databases/{DATABASE}"
                f"/schemas/{SCHEMA}"
                f"/agents/{AGENT_NAME}"
            ),
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "Content-Type": "application/json",
            },
            json=None,
            params={"ifExists": expected},
            timeout=None,
        )
