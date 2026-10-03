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
"""
Run Claude Code's own agent loop in-process through the Claude Agent SDK.

See `the SDK's docs <https://platform.claude.com/docs/en/api/agent-sdk/overview>`__.

.. note:: Experimental; see :mod:`airflow.providers.common.ai.harness`.
"""

from __future__ import annotations

import copy
import json
import logging
from typing import TYPE_CHECKING, Any, ClassVar

try:
    from claude_agent_sdk import ClaudeAgentOptions, ResultMessage, create_sdk_mcp_server, query, tool
except ImportError as e:
    from airflow.providers.common.compat.sdk import AirflowOptionalProviderFeatureException

    raise AirflowOptionalProviderFeatureException(e)

from airflow.providers.common.ai.harness.base import (
    HarnessBackend,
    HarnessRequest,
    HarnessResult,
    HarnessRunError,
)
from airflow.providers.common.ai.tools import AirflowTool, ToolCallError
from airflow.providers.common.ai.tools._from_toolset import tool_call_scope
from airflow.providers.common.ai.utils.coroutines import run_coroutine_sync
from airflow.providers.common.ai.utils.tool_metrics import calling_framework
from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Sequence

    from claude_agent_sdk import McpSdkServerConfig


log = logging.getLogger(__name__)

__all__ = ["ClaudeAgentSDKBackend"]

# The MCP server key the tool bridge is registered under, and the allowed-tools wildcard
# derived from it -- the two have to agree (facts: ``mcp__<server-key>__<tool-name>``), so
# both are written from this one constant rather than as two literal strings.
_MCP_SERVER_KEY = "airflow"
_ALLOWED_TOOLS = [f"mcp__{_MCP_SERVER_KEY}__*"]

# Connection types this harness refuses: it only talks to Anthropic directly with an API key.
_UNSUPPORTED_CONN_TYPES = frozenset({"pydanticai_bedrock", "pydanticai_vertex", "pydanticai_azure"})


class ClaudeAgentSDKBackend(HarnessBackend):
    """
    Run the agent loop bundled with the Claude Agent SDK's Claude Code CLI.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Only a direct Anthropic connection is supported: :meth:`run` refuses a Bedrock,
    Vertex or Azure connection, or one with a custom ``host``, rather than silently
    sending the request somewhere other than what the connection says. Built-in
    tools, including the shell, are off: the agent can call only the
    :class:`~airflow.providers.common.ai.tools.AirflowTool` objects in
    :attr:`HarnessRequest.tools`, through an in-process MCP server, and nothing in
    ``~/.claude`` or a project's settings files is read.
    """

    name: ClassVar[str] = "claude_agent_sdk"

    def run(self, request: HarnessRequest) -> HarnessResult:
        return run_coroutine_sync(self._run_async(request))

    async def _run_async(self, request: HarnessRequest) -> HarnessResult:
        api_key, model = _get_api_key_and_model(request)
        # One token per run: every tool call this run makes shares it, so a toolset's
        # per-run retry budget (tool_call_scope) resets only between runs, never within one.
        failures: list[ToolCallError] = []
        mcp_server = _build_mcp_server(request.tools, failures, run=object())
        options = _build_options(request, api_key=api_key, model=model, mcp_server=mcp_server)

        result_message: ResultMessage | None = None
        async for message in query(prompt=request.prompt, options=options):
            if isinstance(message, ResultMessage):
                result_message = message
            if failures:
                # The CLI already reported this call's failure to the model as an error
                # result (see _build_tool_handler); this is Airflow's own copy of that
                # failure, which a tool call the model cannot fix must end the run with.
                raise failures[0]
        if result_message is None:
            raise HarnessRunError(
                f"The Claude Agent SDK query for connection {request.llm_conn_id!r} ended without a result."
            )
        return _convert_result(result_message)


def _resolve_model(model_name: str) -> str:
    """Strip a ``anthropic:`` prefix, or reject any other provider prefix."""
    prefix, sep, rest = model_name.partition(":")
    if not sep:
        return model_name
    if prefix == "anthropic":
        return rest
    raise ValueError(
        f"Model {model_name!r} has provider prefix {prefix + ':'!r}, but the Claude Agent SDK "
        "harness only talks to Anthropic directly. Use a bare model id, or the 'anthropic:' prefix."
    )


def _get_api_key_and_model(request: HarnessRequest) -> tuple[str, str]:
    """Resolve the Anthropic API key and model id for ``request``, refusing unsupported connections."""
    conn = BaseHook.get_connection(request.llm_conn_id)
    if conn.conn_type in _UNSUPPORTED_CONN_TYPES:
        raise ValueError(
            f"Connection {request.llm_conn_id!r} is a {conn.conn_type!r} connection. The Claude Agent "
            "SDK harness only supports talking to Anthropic directly with an API key; Bedrock, Vertex "
            "and Azure are not supported."
        )
    api_key = conn.password
    if not api_key:
        raise ValueError(
            f"Connection {request.llm_conn_id!r} has no password set. The Claude Agent SDK harness "
            "only supports talking to Anthropic directly with an API key; Bedrock and Vertex are not "
            "supported."
        )
    if conn.host:
        raise ValueError(
            f"Connection {request.llm_conn_id!r} sets a custom host ({conn.host!r}). The Claude Agent "
            "SDK harness has no verified way to point the bundled CLI at a custom endpoint, so the "
            "connection is refused rather than silently sent to the default one."
        )
    model_name = request.model_id or conn.extra_dejson.get("model")
    if not model_name:
        raise ValueError(
            f"No model specified for connection {request.llm_conn_id!r}. Set model_id on the request "
            "or the Model field on the connection."
        )
    return api_key, _resolve_model(model_name)


def _build_options(
    request: HarnessRequest, *, api_key: str, model: str, mcp_server: McpSdkServerConfig | None
) -> ClaudeAgentOptions:
    """Build safe-by-default options: no built-in tools, no settings files, only the Airflow bridge."""
    return ClaudeAgentOptions(
        model=model,
        system_prompt=request.system_prompt or None,
        max_turns=request.max_turns,
        tools=[],
        allowed_tools=_ALLOWED_TOOLS,
        setting_sources=[],
        mcp_servers={_MCP_SERVER_KEY: mcp_server} if mcp_server is not None else {},
        env={"ANTHROPIC_API_KEY": api_key},
    )


def _build_mcp_server(
    tools: Sequence[AirflowTool], failures: list[ToolCallError], *, run: object
) -> McpSdkServerConfig | None:
    """Wrap ``tools`` into an in-process MCP server, or ``None`` when there are none to serve."""
    if not tools:
        return None
    sdk_tools = [
        tool(airflow_tool.name, airflow_tool.description, copy.deepcopy(airflow_tool.parameters))(
            _build_tool_handler(airflow_tool, failures, run=run)
        )
        for airflow_tool in tools
    ]
    return create_sdk_mcp_server(name=_MCP_SERVER_KEY, tools=sdk_tools)


def _build_tool_handler(
    airflow_tool: AirflowTool, failures: list[ToolCallError], *, run: object
) -> Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]:
    """
    Build the in-process MCP handler for ``airflow_tool``.

    A :class:`~airflow.providers.common.ai.tools.ToolCallError` is caught here rather than
    let propagate: the SDK's own tool dispatch catches any exception a handler raises and
    turns it into an ``is_error`` result the model sees, the same as a correctable failure,
    so raising here would never reach :meth:`ClaudeAgentSDKBackend._run_async`. It is
    recorded into ``failures`` instead, which the run loop checks after every message so the
    run ends with the error rather than letting the model try to carry on past it.
    """

    async def handler(args: dict[str, Any]) -> dict[str, Any]:
        with calling_framework("claude_agent_sdk"), tool_call_scope(run=run):
            try:
                result = await airflow_tool.call(args)
            except ToolCallError as e:
                failures.append(e)
                return {"content": [{"type": "text", "text": str(e)}], "is_error": True}
        text = result.content if isinstance(result.content, str) else json.dumps(result.content)
        payload: dict[str, Any] = {"content": [{"type": "text", "text": text}]}
        if result.is_error:
            payload["is_error"] = True
        return payload

    return handler


def _convert_result(result: ResultMessage) -> HarnessResult:
    return HarnessResult(
        output=result.result,
        is_error=result.is_error,
        subtype=result.subtype,
        session_id=result.session_id,
        num_turns=result.num_turns,
        cost_usd=result.total_cost_usd,
        usage=result.usage,
    )
