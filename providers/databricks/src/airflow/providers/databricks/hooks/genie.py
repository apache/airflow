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
"""Databricks Genie hook."""

from __future__ import annotations

import asyncio
import time
from typing import TYPE_CHECKING, Any, ClassVar

from requests import exceptions as requests_exceptions

from airflow.providers.common.compat.sdk import AirflowException, AirflowOptionalProviderFeatureException
from airflow.providers.databricks.exceptions import DatabricksApiError
from airflow.providers.databricks.hooks.databricks_base import BaseDatabricksHook

if TYPE_CHECKING:
    from collections.abc import Sequence

    from airflow.providers.common.ai.exceptions import (
        ManagedAgentInvocationError,
        ManagedAgentRejected,
    )
    from airflow.providers.common.ai.managed_agents.base import (
        BaseManagedAgentHook,
        ManagedAgentCapabilities,
        ManagedAgentRef,
        ManagedAgentRequest,
        ManagedAgentResponse,
    )
else:
    try:
        from airflow.providers.common.ai.exceptions import (
            ManagedAgentInvocationError,
            ManagedAgentRejected,
        )
        from airflow.providers.common.ai.managed_agents.base import (
            BaseManagedAgentHook,
            ManagedAgentCapabilities,
            ManagedAgentRef,
            ManagedAgentResponse,
        )
    except ImportError:
        # The common.ai provider is optional. This module imports without it,
        # and managed-agent entry points on DatabricksGenieHook report what is missing.
        def _needs_common_ai(*args: Any, **kwargs: Any) -> Any:
            raise AirflowOptionalProviderFeatureException(
                "Consulting a Databricks Genie space as a managed agent needs the 'common.ai' extra of the "
                "databricks provider: pip install 'apache-airflow-providers-databricks[common.ai]'."
            )

        class BaseManagedAgentHook:
            """Stand-in for the Common AI contract base; ``agent()`` names the missing extra."""

            agent = _needs_common_ai

        ManagedAgentCapabilities = ManagedAgentRef = ManagedAgentResponse = _needs_common_ai
        ManagedAgentInvocationError = ManagedAgentRejected = _needs_common_ai

_RESERVED_OPTIONS = frozenset({"prompt", "messages", "session_id", "timeout"})
TERMINAL_STATUSES = frozenset({"COMPLETED", "FAILED", "CANCELLED"})


class DatabricksGenieHook(BaseDatabricksHook, BaseManagedAgentHook):
    """
    Interact with the Databricks Genie API.

    Provides methods to start conversations, send follow-up messages, poll message status,
    and retrieve query results from Databricks Genie spaces.

    With the ``common.ai`` extra installed, this hook adopts the Common AI managed-agent
    contract, allowing ``hook.agent(space_id)`` to be handed to a ``ManagedAgentToolset``:

    .. code-block:: python

        from airflow.providers.common.ai.toolsets import ManagedAgentToolset
        from airflow.providers.databricks.hooks.genie import DatabricksGenieHook

        hook = DatabricksGenieHook(databricks_conn_id="databricks_default")
        sales_analyst = hook.agent("01ef8392-4f3b-1234-9abc-1234567890ab")
        toolset = ManagedAgentToolset(
            sales_analyst,
            tool_name="ask_sales_analyst",
            description="Consults Databricks Genie for sales, revenue, and pipeline data.",
        )

    :param databricks_conn_id: Reference to the Databricks connection.
    :param timeout_seconds: Timeout in seconds for HTTP requests.
    :param retry_limit: Number of times to retry failed requests.
    :param retry_delay: Wait in seconds between retries.
    :param retry_args: Optional dictionary with arguments passed to tenacity Retrying.
    :param caller: Name of the caller for user agent logging.
    """

    agent_platform: ClassVar[str] = "databricks.genie"

    def __init__(
        self,
        databricks_conn_id: str = "databricks_default",
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
        retry_args: dict[Any, Any] | None = None,
        caller: str = "DatabricksGenieHook",
        **kwargs: Any,
    ) -> None:
        super().__init__(
            databricks_conn_id=databricks_conn_id,
            timeout_seconds=timeout_seconds,
            retry_limit=retry_limit,
            retry_delay=retry_delay,
            retry_args=retry_args,
            caller=caller,
            **kwargs,
        )

    # -------------------------------------------------------------------------
    # Public Databricks Genie API methods (Synchronous)
    # -------------------------------------------------------------------------

    def start_conversation(self, space_id: str, content: str) -> dict[str, Any]:
        """
        Start a new conversation thread in a Databricks Genie Space.

        :param space_id: The unique identifier of the Genie space.
        :param content: The natural-language prompt or question.
        :return: Initial message object containing conversation_id, id, and status.
        """
        endpoint = ("POST", f"2.0/genie/spaces/{space_id}/start-conversation")
        return self._do_api_call(endpoint, json={"content": content})

    def create_message(self, space_id: str, conversation_id: str, content: str) -> dict[str, Any]:
        """
        Send a follow-up message in an existing conversation thread.

        :param space_id: The unique identifier of the Genie space.
        :param conversation_id: The conversation ID to continue.
        :param content: The follow-up question or instruction.
        :return: Message object containing id and status.
        """
        endpoint = ("POST", f"2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages")
        return self._do_api_call(endpoint, json={"content": content})

    def get_message(self, space_id: str, conversation_id: str, message_id: str) -> dict[str, Any]:
        """
        Retrieve a message and its execution status within a conversation.

        :param space_id: The unique identifier of the Genie space.
        :param conversation_id: The conversation ID.
        :param message_id: The message ID to fetch.
        :return: Message object including status, content, attachments, and errors if any.
        """
        endpoint = (
            "GET",
            f"2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
        )
        return self._do_api_call(endpoint)

    def get_query_result(
        self, space_id: str, conversation_id: str, message_id: str
    ) -> dict[str, Any]:
        """
        Retrieve query result data associated with a completed message.

        :param space_id: The unique identifier of the Genie space.
        :param conversation_id: The conversation ID.
        :param message_id: The message ID.
        :return: Query result object including schema and data rows.
        """
        endpoint = (
            "GET",
            f"2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result",
        )
        return self._do_api_call(endpoint)

    def wait_for_message(
        self,
        space_id: str,
        conversation_id: str,
        message_id: str,
        poll_interval: float = 2.0,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """
        Poll a message until it reaches a terminal status (COMPLETED, FAILED, CANCELLED).

        :param space_id: The unique identifier of the Genie space.
        :param conversation_id: The conversation ID.
        :param message_id: The message ID.
        :param poll_interval: Polling frequency in seconds.
        :param timeout: Maximum seconds to wait before raising TimeoutError.
        :return: Terminal message object.
        """
        start_time = time.time()
        while True:
            msg = self.get_message(space_id, conversation_id, message_id)
            status = msg.get("status")
            if status in TERMINAL_STATUSES:
                return msg
            if timeout is not None and time.time() - start_time > timeout:
                raise AirflowException(
                    f"Timed out waiting for Genie message {message_id} in conversation {conversation_id} "
                    f"after {timeout} seconds."
                )
            time.sleep(poll_interval)

    # -------------------------------------------------------------------------
    # Public Databricks Genie API methods (Asynchronous)
    # -------------------------------------------------------------------------

    async def a_start_conversation(self, space_id: str, content: str) -> dict[str, Any]:
        """Asynchronously start a new conversation thread in a Databricks Genie Space."""
        endpoint = ("POST", f"2.0/genie/spaces/{space_id}/start-conversation")
        return await self._a_do_api_call(endpoint, json={"content": content})

    async def a_create_message(
        self, space_id: str, conversation_id: str, content: str
    ) -> dict[str, Any]:
        """Asynchronously send a follow-up message in an existing conversation thread."""
        endpoint = ("POST", f"2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages")
        return await self._a_do_api_call(endpoint, json={"content": content})

    async def a_get_message(
        self, space_id: str, conversation_id: str, message_id: str
    ) -> dict[str, Any]:
        """Asynchronously retrieve a message and its execution status."""
        endpoint = (
            "GET",
            f"2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
        )
        return await self._a_do_api_call(endpoint)

    async def a_get_query_result(
        self, space_id: str, conversation_id: str, message_id: str
    ) -> dict[str, Any]:
        """Asynchronously retrieve query result data associated with a message."""
        endpoint = (
            "GET",
            f"2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result",
        )
        return await self._a_do_api_call(endpoint)

    async def a_wait_for_message(
        self,
        space_id: str,
        conversation_id: str,
        message_id: str,
        poll_interval: float = 2.0,
        timeout: float | None = None,
    ) -> dict[str, Any]:
        """Asynchronously poll a message until it reaches a terminal status."""
        start_time = time.time()
        while True:
            msg = await self.a_get_message(space_id, conversation_id, message_id)
            status = msg.get("status")
            if status in TERMINAL_STATUSES:
                return msg
            if timeout is not None and time.time() - start_time > timeout:
                raise AirflowException(
                    f"Timed out waiting for Genie message {message_id} in conversation {conversation_id} "
                    f"after {timeout} seconds."
                )
            await asyncio.sleep(poll_interval)

    # -------------------------------------------------------------------------
    # BaseManagedAgentHook Contract Implementation
    # -------------------------------------------------------------------------

    def resolve_agent(self, agent: str) -> ManagedAgentRef:
        """
        Normalize ``agent`` (Genie Space ID) into a platform-qualified reference.

        :param agent: The Genie Space ID.
        """
        if not agent or not isinstance(agent, str) or not agent.strip():
            raise ValueError(f"A Databricks Genie agent identifier must be a non-empty space ID, got {agent!r}.")
        return ManagedAgentRef(platform=self.agent_platform, name=agent.strip())

    def get_agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
        """Report capabilities for a Genie Space (multi-turn sessions, structured responses, and tracing)."""
        self.resolve_agent(agent)
        return ManagedAgentCapabilities(
            sessions=True,
            structured_output=True,
            usage=False,
            trace=True,
        )

    def invoke_agent(self, agent: str, request: ManagedAgentRequest) -> ManagedAgentResponse:
        """
        Send a request to a Databricks Genie Space and return its answer.

        :param agent: The Genie Space ID.
        :param request: The managed agent request.
        """
        ref = self.resolve_agent(agent)
        space_id = ref.name

        if reserved := _RESERVED_OPTIONS.intersection(request.vendor_options):
            raise ValueError(f"vendor_options cannot override contract fields: {sorted(reserved)}")

        prompt = self._extract_prompt(request)
        poll_interval = float(request.vendor_options.get("poll_interval", 2.0))
        include_query_result = bool(request.vendor_options.get("include_query_result", False))

        conversation_id: str | None = request.session_id
        message_id: str | None = None

        try:
            if conversation_id:
                initial_msg = self.create_message(
                    space_id=space_id,
                    conversation_id=conversation_id,
                    content=prompt,
                )
            else:
                initial_msg = self.start_conversation(space_id=space_id, content=prompt)
                conversation_id = initial_msg.get("conversation_id", "")

            message_id = initial_msg.get("id") or initial_msg.get("message_id")
            if not message_id:
                raise ManagedAgentInvocationError(
                    f"{self._describe_call(space_id)} returned an initial response without a message id: {initial_msg}"
                )

            final_msg = self.wait_for_message(
                space_id=space_id,
                conversation_id=conversation_id,
                message_id=message_id,
                poll_interval=poll_interval,
                timeout=request.timeout,
            )
        except DatabricksApiError as exc:
            self._handle_api_error(space_id, conversation_id, exc)
            raise
        except requests_exceptions.HTTPError as exc:
            self._handle_http_error(space_id, conversation_id, exc)
            raise

        status = final_msg.get("status")
        if status == "FAILED":
            err_info = final_msg.get("error") or {}
            err_msg = err_info.get("message") or f"Databricks Genie message {message_id} failed."
            err_code = err_info.get("error_code") or ""
            if err_code in ("INVALID_PARAMETER_VALUE", "BAD_REQUEST", "QUERY_COMPILATION_ERROR"):
                raise ManagedAgentRejected(f"{self._describe_call(space_id)}: {err_msg}")
            raise ManagedAgentInvocationError(f"{self._describe_call(space_id)}: {err_msg}")

        if status == "CANCELLED":
            raise ManagedAgentInvocationError(
                f"{self._describe_call(space_id)}: Message {message_id} was cancelled."
            )

        attachments = final_msg.get("attachments") or []
        query_result = None
        if include_query_result:
            try:
                query_result = self.get_query_result(space_id, conversation_id, message_id)
            except Exception as exc:
                self.log.warning("Could not fetch Genie query result for message %s: %s", message_id, exc)

        response_text = self._extract_response_text(final_msg)

        structured_output = {
            "attachments": attachments,
            **({"query_result": query_result} if query_result is not None else {}),
        }

        return ManagedAgentResponse(
            text=response_text,
            raw=final_msg,
            structured=structured_output if structured_output else None,
            session_id=conversation_id,
            trace_ref=message_id,
        )

    # -------------------------------------------------------------------------
    # Internal Helpers
    # -------------------------------------------------------------------------

    def _describe_call(self, space_id: str) -> str:
        return f"Genie space {space_id} via connection {self.databricks_conn_id!r}"

    @staticmethod
    def _extract_prompt(request: ManagedAgentRequest) -> str:
        if request.prompt is not None:
            return request.prompt
        if request.messages:
            last_msg = request.messages[-1]
            content = last_msg.get("content", "")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                parts = [
                    item.get("text", "")
                    for item in content
                    if isinstance(item, dict) and item.get("type") == "text"
                ]
                return " ".join(parts) if parts else str(content)
            return str(content)
        raise ValueError("ManagedAgentRequest requires either prompt or messages.")

    @staticmethod
    def _extract_response_text(message: dict[str, Any]) -> str:
        text_lines: list[str] = []
        attachments = message.get("attachments") or []
        for att in attachments:
            if not isinstance(att, dict):
                continue
            if "text" in att and isinstance(att["text"], dict):
                if content := att["text"].get("content"):
                    text_lines.append(content)
            elif "query" in att and isinstance(att["query"], dict):
                if query_sql := att["query"].get("query"):
                    text_lines.append(f"```sql\n{query_sql}\n```")

        if text_lines:
            return "\n\n".join(text_lines)
        return message.get("content") or ""

    def _handle_api_error(
        self, space_id: str, conversation_id: str | None, exc: DatabricksApiError
    ) -> None:
        status_code = exc.http_status_code
        if status_code in (401, 403):
            raise ManagedAgentInvocationError(
                f"Authentication or permission denied for {self._describe_call(space_id)}: {exc}"
            ) from exc
        if status_code == 404:
            raise ManagedAgentInvocationError(
                f"Genie space {space_id} or conversation {conversation_id!r} not found: {exc}"
            ) from exc
        if status_code == 400:
            raise ManagedAgentRejected(
                f"Databricks Genie rejected request for space {space_id}: {exc}"
            ) from exc

    def _handle_http_error(
        self, space_id: str, conversation_id: str | None, exc: requests_exceptions.HTTPError
    ) -> None:
        status_code = getattr(exc.response, "status_code", None)
        if status_code in (401, 403):
            raise ManagedAgentInvocationError(
                f"Authentication or permission denied for {self._describe_call(space_id)}: {exc}"
            ) from exc
        if status_code == 404:
            raise ManagedAgentInvocationError(
                f"Genie space {space_id} or conversation {conversation_id!r} not found: {exc}"
            ) from exc
        if status_code == 400:
            raise ManagedAgentRejected(
                f"Databricks Genie rejected request for space {space_id}: {exc}"
            ) from exc
