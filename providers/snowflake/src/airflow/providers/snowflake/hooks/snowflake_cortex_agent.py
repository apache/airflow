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

from enum import Enum
from json.decoder import JSONDecodeError
from typing import Any, cast

import requests

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class CreateMode(str, Enum):
    """Resource creation modes for Cortex Agents."""

    ERROR_IF_EXISTS = "errorIfExists"
    OR_REPLACE = "orReplace"
    IF_NOT_EXISTS = "ifNotExists"


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
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
    ) -> dict[str, Any] | list[dict[str, Any]]:

        response = requests.request(
            method=method,
            url=f"{self._get_base_url()}{endpoint}",
            headers={
                "Authorization": f"Bearer {self._get_access_token()}",
                "Content-Type": "application/json",
            },
            json=payload,
            params=params,
            timeout=timeout,
        )

        if not response.ok:
            self.log.error(
                "Snowflake Cortex Agent request failed with status %s: %s",
                response.status_code,
                response.text,
            )

            try:
                error = response.json()
                raise RuntimeError(
                    f"Snowflake Cortex Agent API request failed: {error.get('message', response.text)}"
                )
            except JSONDecodeError:
                response.raise_for_status()

        if not response.content:
            return {}

        response.raise_for_status()

        return response.json()

    def _build_agent_payload(
        self,
        *,
        comment: str | None = None,
        profile: dict[str, Any] | None = None,
        models: dict[str, Any] | None = None,
        instructions: dict[str, Any] | None = None,
        orchestration: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_resources: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Build a Cortex Agent request payload."""
        payload: dict[str, Any] = {}

        if comment is not None:
            payload["comment"] = comment

        if profile is not None:
            payload["profile"] = profile

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

        return payload

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

        return cast(
            "dict[str, Any]",
            self._request(
                method="POST",
                endpoint=endpoint,
                payload=payload,
                timeout=timeout,
            ),
        )

    def create_agent(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        comment: str | None = None,
        profile: dict[str, Any] | None = None,
        models: dict[str, Any] | None = None,
        instructions: dict[str, Any] | None = None,
        orchestration: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_resources: dict[str, Any] | None = None,
        create_mode: CreateMode = CreateMode.ERROR_IF_EXISTS,
        timeout: int | None = 600,
    ) -> dict[str, Any]:
        """
        Create a Snowflake Cortex Agent.

        :param database: Database in which to create the agent.
        :param schema: Schema in which to create the agent.
        :param agent_name: Name of the Cortex Agent.
        :param comment: Optional comment. Optional. Defaults to ``None``.
        :param profile: Agent profile configuration. Optional. Defaults to ``None``.
        :param models: Model configuration. Optional. Defaults to ``None``.
        :param instructions: Agent instructions. Optional. Defaults to ``None``.
        :param orchestration: Orchestration configuration. Optional. Defaults to ``None``.
        :param tools: Agent tools. Optional. Defaults to ``None``.
        :param tool_resources: Tool resource configuration. Optional. Defaults to ``None``.
        :param create_mode: Resource creation mode. One of ``errorIfExists``, ``orReplace``
            or ``ifNotExists``. Optional. Defaults to ``errorIfExists``.
        :param timeout: Maximum time in seconds to wait for the Cortex Agent request
            to complete. Defaults to ``600``.
        """
        payload = {
            "name": agent_name,
            **self._build_agent_payload(
                comment=comment,
                profile=profile,
                models=models,
                instructions=instructions,
                orchestration=orchestration,
                tools=tools,
                tool_resources=tool_resources,
            ),
        }

        endpoint = f"/api/v2/databases/{database}/schemas/{schema}/agents"

        return cast(
            "dict[str, Any]",
            self._request(
                method="POST",
                endpoint=endpoint,
                payload=payload,
                params={"createMode": create_mode.value},
                timeout=timeout,
            ),
        )

    def update_agent(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        comment: str | None = None,
        profile: dict[str, Any] | None = None,
        models: dict[str, Any] | None = None,
        instructions: dict[str, Any] | None = None,
        orchestration: dict[str, Any] | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_resources: dict[str, Any] | None = None,
        timeout: int | None = 600,
    ) -> dict[str, Any]:
        """
        Update a Snowflake Cortex Agent.

        :param database: Database containing the agent.
        :param schema: Schema containing the agent.
        :param agent_name: Name of the Cortex Agent.
        :param timeout: Maximum time in seconds to wait for the Cortex Agent request
            to complete. Defaults to ``600``.
        """
        endpoint = f"/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}"

        return cast(
            "dict[str, Any]",
            self._request(
                method="PUT",
                endpoint=endpoint,
                payload=self._build_agent_payload(
                    comment=comment,
                    profile=profile,
                    models=models,
                    instructions=instructions,
                    orchestration=orchestration,
                    tools=tools,
                    tool_resources=tool_resources,
                ),
                timeout=timeout,
            ),
        )

    def describe_agent(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        timeout: int | None = 600,
    ) -> dict[str, Any]:
        """
        Describe a Snowflake Cortex Agent.

        :param database: Database containing the Cortex Agent.
        :param schema: Schema containing the Cortex Agent.
        :param agent_name: Name of the Cortex Agent.
        :param timeout: Maximum time in seconds to wait for the Cortex Agent #
            request to complete. Defaults to ``600``.
        :return: JSON description of the Cortex Agent.
        """
        endpoint = f"/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}"

        return cast(
            "dict[str, Any]",
            self._request(
                method="GET",
                endpoint=endpoint,
                timeout=timeout,
            ),
        )

    def list_agents(
        self,
        *,
        database: str,
        schema: str,
        like: str | None = None,
        from_name: str | None = None,
        show_limit: int | None = None,
        timeout: int | None = 600,
    ) -> list[dict[str, Any]]:
        """
        List Snowflake Cortex Agents.

        :param database: Database containing the Cortex Agents.
        :param schema: Schema containing the Cortex Agents.
        :param like: Optional case-insensitive name filter. Optional.
            Defaults to ``None``.
        :param from_name: Optional pagination starting point. Optional.
            Defaults to ``None``.
        :param show_limit: Maximum number of agents to return. Optional.
            Defaults to ``None``.
        :param timeout: Maximum time in seconds to wait for the Cortex Agent
            request to complete. Defaults to ``600``.
        :return: List of Cortex Agents.
        """
        endpoint = f"/api/v2/databases/{database}/schemas/{schema}/agents"

        params: dict[str, Any] = {}

        if like is not None:
            params["like"] = like

        if from_name is not None:
            params["fromName"] = from_name

        if show_limit is not None:
            params["showLimit"] = show_limit

        return cast(
            "list[dict[str, Any]]",
            self._request(
                method="GET",
                endpoint=endpoint,
                params=params or None,
                timeout=timeout,
            ),
        )

    def delete_agent(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        if_exists: bool = False,
        timeout: int | None = 600,
    ) -> dict[str, Any]:
        """
        Delete a Snowflake Cortex Agent.

        :param database: Database containing the Cortex Agent.
        :param schema: Schema containing the Cortex Agent.
        :param agent_name: Name of the Cortex Agent.
        :param if_exists: If ``True``, do not fail when the agent does not exist.
            Defaults to ``False``.
        :param timeout: Maximum time in seconds to wait for the Cortex Agent request
            to complete. Defaults to ``600``.
        :return: JSON response confirming deletion.
        """
        endpoint = f"/api/v2/databases/{database}/schemas/{schema}/agents/{agent_name}"

        return cast(
            "dict[str, Any]",
            self._request(
                method="DELETE",
                endpoint=endpoint,
                params={"ifExists": str(if_exists).lower()},
                timeout=timeout,
            ),
        )

    @staticmethod
    def get_text_response(response: dict[str, Any]) -> str:
        """Extract text blocks from a Cortex Agent response."""
        return "".join(
            block.get("text", "") for block in response.get("content", []) if block.get("type") == "text"
        )
