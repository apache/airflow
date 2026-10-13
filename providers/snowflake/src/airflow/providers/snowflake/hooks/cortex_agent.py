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

import re
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar, Literal, overload
from urllib.parse import quote

import requests

from airflow.exceptions import AirflowOptionalProviderFeatureException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.utils._rest_auth import SnowflakeRestTokenProvider, get_cortex_base_url

if TYPE_CHECKING:
    from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
    from airflow.providers.common.ai.managed_agents.base import (
        BaseManagedAgentHook,
        ManagedAgentCapabilities,
        ManagedAgentRef,
        ManagedAgentRequest,
        ManagedAgentResponse,
    )
else:
    try:
        from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
        from airflow.providers.common.ai.managed_agents.base import (
            BaseManagedAgentHook,
            ManagedAgentCapabilities,
            ManagedAgentRef,
            ManagedAgentResponse,
        )
    except ImportError:

        def _needs_common_ai(*args: Any, **kwargs: Any) -> Any:
            raise AirflowOptionalProviderFeatureException(
                "Using a Cortex Agent through the managed-agent contract needs the 'common.ai' "
                "extra of the snowflake provider: "
                "pip install 'apache-airflow-providers-snowflake[common.ai]'."
            )

        class BaseManagedAgentHook:
            """Stand-in for the optional Common AI contract."""

            agent = _needs_common_ai

        ManagedAgentCapabilities = ManagedAgentRef = ManagedAgentResponse = _needs_common_ai
        ManagedAgentInvocationError = _needs_common_ai


# Quoted identifiers can contain dots and escaped double quotes. Unquoted identifiers
# follow Snowflake's identifier rules and are normalized to uppercase.
_IDENTIFIER = r'(?:"(?:[^"\x00]|"")+"|[A-Za-z_][A-Za-z0-9_$]*)'
_AGENT_NAME = re.compile(rf"({_IDENTIFIER})\.({_IDENTIFIER})\.({_IDENTIFIER})")

_RESERVED_OPTIONS = frozenset(
    {
        "database",
        "schema",
        "agent_name",
        "agent",
        "snowflake_conn_id",
        "connection",
        "messages",
        "prompt",
        "session_id",
        "timeout",
        "thread_id",
        "parent_message_id",
        "stream",
    }
)

# The named-agent run endpoint supports tool selection; configuration changes belong
# to update_agent(). Keep the adapter's option surface explicit.
_SUPPORTED_OPTIONS = frozenset({"tool_choice"})
_TERMINAL_HTTP_STATUSES = frozenset({400, 401, 403, 404, 405, 422})

JsonDict = dict[str, Any]
JsonList = list[JsonDict]
JsonResponse = JsonDict | JsonList


class _UnexpectedResponseShape(TypeError):
    """The decoded response does not have the expected JSON structure."""


class CreateMode(str, Enum):
    """Resource creation modes for Cortex Agents."""

    ERROR_IF_EXISTS = "errorIfExists"
    OR_REPLACE = "orReplace"
    IF_NOT_EXISTS = "ifNotExists"


class SnowflakeCortexAgentHook(SnowflakeHook, BaseManagedAgentHook):
    """
    Hook for interacting with Snowflake Cortex Agents.

    Authenticates the same three ways as ``SnowflakeSqlApiHook``, chosen by the connection's
    ``authenticator`` extra:

    1. OAuth: set ``authenticator`` to ``oauth`` and configure a refresh token, client
       credentials grant, or ``azure_conn_id``, as on ``SnowflakeHook``.
    2. PAT (Programmatic Access Token): set ``authenticator`` to ``programmatic_access_token``
       and put the PAT value in the connection ``password`` field.
    3. Key-pair JWT: the default when neither of the above is set. Configure
       ``private_key_file`` or ``private_key_content`` (optionally with a passphrase in
       ``password``), as on ``SnowflakeHook``.

    The resolved token is cached and renewed the same way as ``SnowflakeSqlApiHook`` -- see
    ``SnowflakeRestTokenProvider``, one instance per hook, so a long-running agent reuses the
    same JWT within its renewal window.

    With the ``common.ai`` extra installed, ``hook.agent("DATABASE.SCHEMA.NAME")``
    supports the Common AI managed-agent contract, accepting prompts or message
    history and returning answer text with the raw response. The only supported
    vendor option is ``tool_choice``. Agent configuration changes remain available
    through ``update_agent``.
    """

    agent_platform: ClassVar[str] = "snowflake.cortex"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._rest_token_provider: SnowflakeRestTokenProvider | None = None

    @staticmethod
    def _parse_agent_name(agent: str) -> tuple[str, str, str]:
        """Validate and normalize name components, preserving quoted identifier syntax."""
        match = _AGENT_NAME.fullmatch(agent)
        if match is None:
            raise ValueError(f"A Cortex agent must be a fully qualified DATABASE.SCHEMA.NAME, got {agent!r}.")

        database, schema, agent_name = (
            part if part.startswith('"') else part.upper() for part in match.groups()
        )
        return database, schema, agent_name

    def resolve_agent(self, agent: str) -> ManagedAgentRef:
        """Return the normalized agent reference without making a network call."""
        # Construct first so the optional-dependency error precedes validation.
        ref = ManagedAgentRef(platform=self.agent_platform, name=agent)
        parts = self._parse_agent_name(agent)
        return ManagedAgentRef(platform=ref.platform, name=".".join(parts))

    def get_agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
        """Report the stateless adapter's capabilities without making a network call."""
        self.resolve_agent(agent)
        return ManagedAgentCapabilities()

    def invoke_agent(
        self,
        agent: str,
        request: ManagedAgentRequest,
    ) -> ManagedAgentResponse:
        """Invoke a named Cortex Agent through the optional Common AI contract."""
        # Check the optional dependency before accessing the request or validating the name.
        ManagedAgentRef(platform=self.agent_platform, name=agent)

        database, schema, agent_name = (
            part[1:-1].replace('""', '"') if part.startswith('"') else part
            for part in self._parse_agent_name(agent)
        )

        if request.session_id is not None:
            raise ValueError("The Cortex managed-agent adapter does not support session_id.")

        if reserved := _RESERVED_OPTIONS.intersection(request.vendor_options):
            raise ValueError(f"vendor_options cannot override contract fields: {sorted(reserved)}")

        if unsupported := request.vendor_options.keys() - _SUPPORTED_OPTIONS:
            raise ValueError(f"Unsupported Cortex vendor_options: {sorted(unsupported)}")

        messages = []
        for message in request.as_messages():
            normalized_message = dict(message)
            if isinstance(normalized_message.get("content"), str):
                normalized_message["content"] = [{"type": "text", "text": normalized_message["content"]}]
            messages.append(normalized_message)

        try:
            response = self.run_agent(
                database=database,
                schema=schema,
                agent_name=agent_name,
                messages=messages,
                timeout=request.timeout,
                **request.vendor_options,
            )
        except requests.HTTPError as exc:
            if exc.response is not None and exc.response.status_code in _TERMINAL_HTTP_STATUSES:
                raise ManagedAgentInvocationError(
                    f"Cortex agent {agent!r} invocation failed with HTTP {exc.response.status_code}."
                ) from exc
            raise
        except requests.exceptions.JSONDecodeError as exc:
            raise ManagedAgentInvocationError(
                f"Cortex agent {agent!r} returned a response that could not be decoded as JSON."
            ) from exc
        except _UnexpectedResponseShape as exc:
            raise ManagedAgentInvocationError(
                f"Cortex agent {agent!r} returned an unexpected response shape: {exc}"
            ) from exc

        try:
            text = self.get_text_response(response)
        except (AttributeError, TypeError) as exc:
            raise ManagedAgentInvocationError(
                f"Cortex agent {agent!r} returned invalid response content."
            ) from exc

        return ManagedAgentResponse(text=text, raw=response)

    def _get_base_url(self) -> str:
        return get_cortex_base_url(self._get_static_conn_params)

    @overload
    def _request(
        self,
        *,
        method: str,
        endpoint: str,
        payload: JsonDict | None = None,
        params: JsonDict | None = None,
        timeout: float | None = None,
        response_type: Literal["dict"],
        allow_empty: bool = False,
    ) -> JsonDict: ...

    @overload
    def _request(
        self,
        *,
        method: str,
        endpoint: str,
        payload: JsonDict | None = None,
        params: JsonDict | None = None,
        timeout: float | None = None,
        response_type: Literal["list"],
        allow_empty: Literal[False] = False,
    ) -> JsonList: ...

    def _request(
        self,
        *,
        method: str,
        endpoint: str,
        payload: JsonDict | None = None,
        params: JsonDict | None = None,
        timeout: float | None = None,
        response_type: Literal["dict", "list"],
        allow_empty: bool = False,
    ) -> JsonResponse:

        if self._rest_token_provider is None:
            self._rest_token_provider = SnowflakeRestTokenProvider(self)

        response = requests.request(
            method=method,
            url=f"{self._get_base_url()}{endpoint}",
            headers={
                **self._rest_token_provider.build_auth_headers(),
                "Content-Type": "application/json",
            },
            json=payload,
            params=params,
            timeout=timeout,
        )

        if response.status_code >= 400:
            self.log.error(
                "Snowflake Cortex Agent request failed with status %s: %s",
                response.status_code,
                response.text,
            )

        response.raise_for_status()

        if not response.content and allow_empty:
            return {}

        data = response.json()

        if response_type == "dict":
            if not isinstance(data, dict):
                raise _UnexpectedResponseShape(f"Expected dict response, got {type(data).__name__}")
            return data

        if not isinstance(data, list):
            raise _UnexpectedResponseShape(f"Expected list[dict] response, got {type(data).__name__}")

        if not all(isinstance(item, dict) for item in data):
            raise _UnexpectedResponseShape(
                "Expected list[dict] response, got list containing non-dict elements"
            )

        return data

    @staticmethod
    def _build_agent_payload(
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
        timeout: float | None = 600,
    ) -> JsonDict:
        """
        Execute a Snowflake Cortex Agent and return the response payload.

        :param database: Database containing the Cortex Agent.
        :param schema: Schema containing the Cortex Agent.
        :param agent_name: Name of the Cortex Agent to execute.
        :param messages: Conversation messages to send to the agent. For a new
            conversation, this should contain the conversation history and the
            current user message. When ``thread_id`` and ``parent_message_id``
            are provided, this should contain only the current user message.
        :param thread_id: Existing conversation thread identifier. When provided,
            ``parent_message_id`` must also be supplied. Optional. Defaults to ``None``.
        :param parent_message_id: Parent message identifier within the specified
            thread. Required when ``thread_id`` is provided. Optional. Defaults to ``None``.
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
            to complete. Optional. Defaults to ``600``.
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

        endpoint = (
            f"/api/v2/databases/{quote(database, safe='')}"
            f"/schemas/{quote(schema, safe='')}"
            f"/agents/{quote(agent_name, safe='')}:run"
        )

        return self._request(
            method="POST",
            endpoint=endpoint,
            payload=payload,
            timeout=timeout,
            response_type="dict",
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
        create_mode: CreateMode | str = CreateMode.ERROR_IF_EXISTS,
        timeout: int | None = 600,
    ) -> JsonDict:
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
        :return: JSON response confirming creation.
        """
        payload: dict[str, Any] = {
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

        endpoint = f"/api/v2/databases/{quote(database, safe='')}/schemas/{quote(schema, safe='')}/agents"

        return self._request(
            method="POST",
            endpoint=endpoint,
            payload=payload,
            params={"createMode": CreateMode(create_mode).value},
            timeout=timeout,
            response_type="dict",
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
    ) -> JsonDict:
        """
        Update a Snowflake Cortex Agent.

        Only provided fields are updated; omitted fields retain their existing values.

        :param database: Database containing the agent.
        :param schema: Schema containing the agent.
        :param agent_name: Name of the Cortex Agent.
        :param comment: Comment associated with the agent. Optional.
            Defaults to ``None``.
        :param profile: Agent profile configuration. Optional.
            Defaults to ``None``.
        :param models: Model configuration. Optional.
            Defaults to ``None``.
        :param instructions: Agent instructions. Optional.
            Defaults to ``None``.
        :param orchestration: Agent orchestration configuration. Optional.
            Defaults to ``None``.
        :param tools: Tools available to the agent. Optional.
            Defaults to ``None``.
        :param tool_resources: Resources used by the agent's tools. Optional.
            Defaults to ``None``.
        :param timeout: Maximum time in seconds to wait for the Cortex Agent
            request to complete. Optional. Defaults to ``600``.
        :return: JSON response confirming the update, or an empty dictionary when
            Snowflake returns a successful response without a body.
        """
        endpoint = (
            f"/api/v2/databases/{quote(database, safe='')}"
            f"/schemas/{quote(schema, safe='')}"
            f"/agents/{quote(agent_name, safe='')}"
        )

        return self._request(
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
            response_type="dict",
            allow_empty=True,
        )

    def describe_agent(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        timeout: int | None = 600,
    ) -> JsonDict:
        """
        Describe a Snowflake Cortex Agent.

        :param database: Database containing the Cortex Agent.
        :param schema: Schema containing the Cortex Agent.
        :param agent_name: Name of the Cortex Agent.
        :param timeout: Maximum time in seconds to wait for the Cortex Agent
            request to complete. Optional. Defaults to ``600``.
        :return: JSON description of the Cortex Agent.
        """
        endpoint = (
            f"/api/v2/databases/{quote(database, safe='')}"
            f"/schemas/{quote(schema, safe='')}"
            f"/agents/{quote(agent_name, safe='')}"
        )

        return self._request(
            method="GET",
            endpoint=endpoint,
            timeout=timeout,
            response_type="dict",
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
    ) -> JsonList:
        """
        List one page of Snowflake Cortex Agents.

        :param database: Database containing the Cortex Agents.
        :param schema: Schema containing the Cortex Agents.
        :param like: Case-insensitive name filter. Optional.
            Defaults to ``None``.
        :param from_name: Agent name from which to continue listing results. Pass the
            pagination value from the preceding page to retrieve the next page. Optional.
            Defaults to ``None``.
        :param show_limit: Maximum number of agents to include in this page. Optional.
            Defaults to ``None``.
        :param timeout: Maximum time in seconds to wait for the Cortex Agent
            request to complete. Optional. Defaults to ``600``.
        :return: One page of Cortex Agents.
        """
        endpoint = f"/api/v2/databases/{quote(database, safe='')}/schemas/{quote(schema, safe='')}/agents"

        params: dict[str, Any] = {}

        if like is not None:
            params["like"] = like

        if from_name is not None:
            params["fromName"] = from_name

        if show_limit is not None:
            params["showLimit"] = show_limit

        return self._request(
            method="GET",
            endpoint=endpoint,
            params=params or None,
            timeout=timeout,
            response_type="list",
        )

    def delete_agent(
        self,
        *,
        database: str,
        schema: str,
        agent_name: str,
        if_exists: bool = False,
        timeout: int | None = 600,
    ) -> JsonDict:
        """
        Delete a Snowflake Cortex Agent.

        :param database: Database containing the Cortex Agent.
        :param schema: Schema containing the Cortex Agent.
        :param agent_name: Name of the Cortex Agent.
        :param if_exists: If ``True``, do not fail when the agent does not exist.
            Optional. Defaults to ``False``.
        :param timeout: Maximum time in seconds to wait for the Cortex Agent request
            to complete. Optional. Defaults to ``600``.
        :return: JSON response confirming deletion.
        """
        endpoint = (
            f"/api/v2/databases/{quote(database, safe='')}"
            f"/schemas/{quote(schema, safe='')}"
            f"/agents/{quote(agent_name, safe='')}"
        )

        return self._request(
            method="DELETE",
            endpoint=endpoint,
            params={"ifExists": str(if_exists).lower()},
            timeout=timeout,
            response_type="dict",
        )

    @staticmethod
    def get_text_response(response: dict[str, Any]) -> str:
        """Extract text blocks from a Cortex Agent response."""
        return "".join(
            block.get("text", "") for block in response.get("content", []) if block.get("type") == "text"
        )
