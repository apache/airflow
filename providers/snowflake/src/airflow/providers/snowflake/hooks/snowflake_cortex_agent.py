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

from typing import Any

import requests

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowflakeCortexAgentHook(SnowflakeHook):
    """Hook for interacting with Snowflake Cortex Agents."""

    def _get_base_url(self) -> str:
        conn_config = self._get_static_conn_params

        host = conn_config.get("host")
        if host:
            return f"https://{host}"

        return f"https://{conn_config['account']}.snowflakecomputing.com"

    def _get_access_token(self) -> str:
        conn_config = self._get_conn_params()

        token = conn_config.get("token")
        if not token:
            raise ValueError(
                "Snowflake connection does not provide an OAuth access token. "
                "This hook currently requires an OAuth access token."
            )

        return token

    def _request(
        self,
        *,
        method: str,
        endpoint: str,
        payload: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any]:

        response = requests.request(
            method=method,
            url=f"{self._get_base_url()}{endpoint}",
            headers={
                "Authorization": f"Bearer {self._get_access_token()}",
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=timeout,
        )

        if response.status_code >= 400:
            self.log.error(
                "Snowflake Cortex Agent request failed with status %s: %s",
                response.status_code,
                response.text,
            )

        response.raise_for_status()

        return response.json()

    def run_agent(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        messages: list[dict[str, Any]],
        thread_id: int | None = None,
        parent_message_id: int | None = None,
        tool_choice: dict[str, Any] | None = None,
        models: dict[str, Any] | None = None,
        instructions: dict[str, Any] | None = None,
        orchestration: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_resources: dict[str, Any] | None = None,
        timeout: int | None = 600,
    ) -> dict[str, Any]:
        """
        Execute a Snowflake Cortex Agent and return the response payload.

        :param database: Database containing the Cortex Agent.
        :param schema: Schema containing the Cortex Agent.
        :param agent_name: Name of the Cortex Agent to execute.
        :param messages: Conversation messages to send to the agent. For a new
            conversation, this should contain the conversation history and the
            current user message. When ``thread_id`` and ``parent_message_id``
            are provided, this should contain only the current user message.
        :param thread_id: Existing conversation thread identifier. Optional.
            When provided, ``parent_message_id`` must also be supplied.
            Defaults to ``None``.
        :param parent_message_id: Parent message identifier within the specified
            thread. Required when ``thread_id`` is provided. Defaults to ``None``.
        :param tool_choice: Tool selection configuration for the agent. Optional.
            Defaults to ``None``.
        :param models: Model configuration for the agent. Optional. Defaults to
            ``None``.
        :param instructions: Agent instruction overrides. Optional. Defaults to
            ``None``.
        :param orchestration: Orchestration configuration for the agent.
            Optional. Defaults to ``None``.
        :param tools: Additional tools available to the agent. Optional.
            Defaults to ``None``.
        :param tool_resources: Configuration for tools specified in ``tools``.
            Optional. Defaults to ``None``.
        :param timeout: Maximum time in seconds to wait for the Cortex Agent request
            to complete. Defaults to ``600``.
        :return: JSON response returned by the Cortex Agent.
        """
        if thread_id is not None and parent_message_id is None:
            raise ValueError("parent_message_id must be provided when thread_id is specified.")

        payload: dict[str, Any] = {
            "messages": messages,
            "stream": False,
        }

        if thread_id is not None:
            payload["thread_id"] = thread_id
            payload["parent_message_id"] = parent_message_id

        if tool_choice is not None:
            payload["tool_choice"] = tool_choice

        if models is not None:
            payload["models"] = models

        if instructions is not None:
            payload["instructions"] = instructions

        if orchestration is not None:
            payload["orchestration"] = orchestration

        if tools is not None:
            payload["tools"] = tools

        if tool_resources is not None:
            payload["tool_resources"] = tool_resources

        endpoint = f"/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}:run"

        return self._request(
            method="POST",
            endpoint=endpoint,
            payload=payload,
            timeout=timeout,
        )

    @staticmethod
    def get_text_response(response: dict[str, Any]) -> str:
        """Extract text blocks from a Cortex Agent response."""
        return "".join(
            block.get("text", "") for block in response.get("content", []) if block.get("type") == "text"
        )
