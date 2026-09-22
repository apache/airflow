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
"""Expose Amazon Bedrock AgentCore runtimes through the Common AI managed-agent contract."""

from __future__ import annotations

import json
import threading
from contextlib import closing
from typing import Any

from botocore.config import Config
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.bedrock import BedrockAgentCoreHook
from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

try:
    from airflow.providers.common.ai.exceptions import ManagedAgentInvocationError
    from airflow.providers.common.ai.managed_agents.contract import (
        BaseManagedAgentHook,
        ManagedAgentCapabilities,
        ManagedAgentRef,
        ManagedAgentRequest,
        ManagedAgentResponse,
    )
except ImportError:
    raise AirflowOptionalProviderFeatureException(
        "This feature requires the 'common.ai' provider, in a version that ships "
        "airflow.providers.common.ai.managed_agents."
    )

# Fields the contract already covers. Letting ``vendor_options`` carry them would let a
# caller re-target the call, which is a change of authority, not an option.
_RESERVED_OPTIONS = frozenset(
    {"agentRuntimeArn", "payload", "contentType", "accept", "runtimeSessionId", "accountId", "mcpSessionId"}
)
# Error codes InvokeAgentRuntime can return that no retry or rephrase will fix. The container's own
# errors come back inside a 200 body, so AgentCore has no error that means "rephrase the prompt".
_TERMINAL_ERROR_CODES = frozenset(
    {
        "ValidationException",
        "ResourceNotFoundException",
        "AccessDeniedException",
        "ServiceQuotaExceededException",
    }
)
# Do not retry an invocation whose effects are unknown; Airflow's task-level retry is the right layer.
_NO_RETRIES = Config(retries={"total_max_attempts": 1})
# The service model's bounds for runtimeSessionId. botocore rejects a shorter value client-side with a
# ParamValidationError, which is not a ClientError and would otherwise escape the error classes.
_SESSION_ID_LENGTH = range(33, 257)
_MAX_RESPONSE_BYTES = 1024 * 1024
_TEXT_KEYS = ("output", "result", "text", "response")


class BedrockAgentCoreManagedAgentHook(BedrockAgentCoreHook, BaseManagedAgentHook):
    """
    Invoke an AgentCore Runtime as a Common AI managed agent.

    The agent is the runtime ARN; the session is AgentCore's ``runtimeSessionId``, which the
    service requires to be 33 to 256 characters long. A request carrying a ``prompt`` is sent as
    ``{"prompt": ...}`` and a request carrying ``messages`` as ``{"messages": [...]}``, both as
    ``application/json``. The container behind the runtime defines its own response shape, so the
    answer text is taken from the first of ``output``, ``result``, ``text`` or ``response`` that
    holds a string, or from ``text_key`` when the container's contract is known; otherwise the
    whole JSON body is returned as text. The decoded body is always available on
    ``ManagedAgentResponse.raw``.

    A remote invocation may have unknown effects, so this hook disables botocore's retries unless
    the connection or the caller configured them, and lets failures propagate to Airflow's
    task-level retry instead. ``ManagedAgentRequest.timeout`` is honored as the botocore connect
    and read timeout of the call; a client is built per distinct timeout and reused. AgentCore has
    no error that means "rephrase the prompt" (a container's own errors arrive inside a successful
    body), so this hook raises terminal errors or lets transient ones propagate, never
    :class:`~airflow.providers.common.ai.exceptions.ManagedAgentRejected`.

    .. code-block:: python

        from airflow.providers.amazon.aws.hooks.bedrock_managed_agent import BedrockAgentCoreManagedAgentHook
        from airflow.providers.common.ai.toolsets import ManagedAgentToolset

        claims = BedrockAgentCoreManagedAgentHook(aws_conn_id="aws_prod", region_name="us-east-1").agent(
            "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/claims"
        )
        toolset = ManagedAgentToolset(claims, tool_name="ask_claims_agent", description="...")

    :param text_key: Key of the response body that holds the answer text, when the container's
        contract is known. Overrides the default lookup; a body without a string there is an error.
    :param max_response_bytes: Upper bound on the response body read into worker memory.

    Additional arguments (such as ``aws_conn_id`` and ``config``) are passed down to
    :class:`~airflow.providers.amazon.aws.hooks.bedrock.BedrockAgentCoreHook`; the connection's
    ``config_kwargs`` apply as they do for every other AWS hook.
    """

    agent_platform = "aws.bedrock_agentcore"

    def __init__(
        self,
        *args: Any,
        text_key: str | None = None,
        max_response_bytes: int = _MAX_RESPONSE_BYTES,
        **kwargs: Any,
    ) -> None:
        if max_response_bytes <= 0:
            raise ValueError("max_response_bytes must be positive.")
        super().__init__(*args, **kwargs)
        self.text_key = text_key
        self.max_response_bytes = max_response_bytes
        self._clients: dict[float | None, Any] = {}
        self._clients_lock = threading.Lock()

    def resolve_agent(self, agent: str) -> ManagedAgentRef:
        if not agent.startswith("arn:") or ":runtime/" not in agent:
            raise ValueError(f"An AgentCore agent is a runtime ARN, got {agent!r}.")
        return ManagedAgentRef(platform=self.agent_platform, name=agent)

    def agent_capabilities(self, agent: str) -> ManagedAgentCapabilities:
        return ManagedAgentCapabilities(sessions=True, structured_output=True, trace=True)

    def invoke_agent(self, agent: str, request: ManagedAgentRequest) -> ManagedAgentResponse:
        self.resolve_agent(agent)
        reserved = _RESERVED_OPTIONS.intersection(request.vendor_options)
        if reserved:
            raise ValueError(f"vendor_options cannot override contract fields: {sorted(reserved)}")
        if request.session_id is not None and len(request.session_id) not in _SESSION_ID_LENGTH:
            raise ValueError(
                f"AgentCore requires a session id of {_SESSION_ID_LENGTH.start} to {_SESSION_ID_LENGTH[-1]} "
                f"characters; got {len(request.session_id)}."
            )
        payload = (
            {"prompt": request.prompt} if request.prompt is not None else {"messages": request.as_messages()}
        )
        kwargs: dict[str, Any] = dict(request.vendor_options)
        if request.session_id is not None:
            kwargs["runtimeSessionId"] = request.session_id
        try:
            response = self._client_for(request.timeout).invoke_agent_runtime(
                agentRuntimeArn=agent,
                payload=json.dumps(payload).encode(),
                contentType="application/json",
                accept="application/json",
                **kwargs,
            )
            body = self._read_json_body(agent, response)
        except ClientError as exc:
            if exc.response.get("Error", {}).get("Code") in _TERMINAL_ERROR_CODES:
                raise ManagedAgentInvocationError(f"{self._where(agent)}: {exc}") from exc
            raise  # throttling, conflicts, server errors: Airflow's task retry is the right layer
        raw = {key: value for key, value in response.items() if key != "response"} | {"response": body}
        return ManagedAgentResponse(
            text=self._text(agent, body),
            raw=raw,
            structured=None if isinstance(body, str) else body,
            session_id=response.get("runtimeSessionId"),
            trace_ref=response.get("ResponseMetadata", {}).get("RequestId"),
        )

    def _client_for(self, timeout: float | None) -> Any:
        """
        One boto3 client per distinct request timeout, built on first use and reused.

        The timeout is a client setting, so it cannot ride on the hook's shared client, and a
        toolset uses one timeout, so this is one client per hook in practice. Clients are
        thread-safe, which the toolset relies on when a model issues two calls in one turn.
        """
        with self._clients_lock:
            client = self._clients.get(timeout)
            if client is None:
                client = self._clients[timeout] = self.get_client_type(config=self._call_config(timeout))
            return client

    def _call_config(self, timeout: float | None) -> Config:
        """Return the connection's or caller's botocore config, with retries off unless they set them."""
        base = self.config or Config()
        if base.retries is None:
            base = base.merge(_NO_RETRIES)
        if timeout is None:
            return base
        return base.merge(Config(connect_timeout=timeout, read_timeout=timeout))

    def _where(self, agent: str) -> str:
        return f"AgentCore agent {agent} via connection {self.aws_conn_id!r}"

    def _read_json_body(self, agent: str, response: dict[str, Any]) -> Any:
        with closing(response["response"]) as stream:
            content_type = response.get("contentType", "").split(";", 1)[0].strip().lower()
            if content_type != "application/json":
                raise ManagedAgentInvocationError(
                    f"{self._where(agent)} returned {content_type or 'no Content-Type'}; "
                    "this hook handles application/json only."
                )
            data = stream.read(self.max_response_bytes + 1)
        if len(data) > self.max_response_bytes:
            raise ManagedAgentInvocationError(
                f"{self._where(agent)} returned more than max_response_bytes={self.max_response_bytes}."
            )
        try:
            return json.loads(data)
        except ValueError as exc:
            raise ManagedAgentInvocationError(
                f"{self._where(agent)} returned a body that is not JSON: {exc}"
            ) from exc

    def _text(self, agent: str, body: Any) -> str:
        if self.text_key is not None:
            value = body.get(self.text_key) if isinstance(body, dict) else None
            if not isinstance(value, str):
                raise ManagedAgentInvocationError(
                    f"{self._where(agent)} returned no string at text_key={self.text_key!r}."
                )
            return value
        if isinstance(body, str):
            return body
        if isinstance(body, dict):
            for key in _TEXT_KEYS:
                if isinstance(body.get(key), str):
                    return body[key]
        return json.dumps(body, default=str)
