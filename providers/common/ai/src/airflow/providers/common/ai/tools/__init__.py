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
Framework-neutral tools backed by Airflow connections.

An :class:`AirflowTool` is one operation an agent can call: a name, a
description, a JSON Schema for its arguments, and an async function. Nothing in
it belongs to a particular agent framework, so the same tool can be handed to
Strands Agents, Google ADK or LangChain through a small adapter, while the agent
itself stays native to that framework.

The toolsets this provider ships, such as
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` and
:class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`, implement
:class:`ToolProvider` and expose their tools through
:meth:`ToolProvider.airflow_tools`.

.. note::

    Experimental: this interface can change or be removed in a minor release of this
    provider.
    See :ref:`howto/stability`.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol

from airflow.providers.common.ai.utils.masking import mask_secrets

if TYPE_CHECKING:
    from pydantic import JsonValue

log = logging.getLogger(__name__)

__all__ = ["AirflowTool", "ToolCallError", "ToolProvider", "ToolResult", "collect_tools"]


class ToolCallError(Exception):
    """
    A tool call failed in a way the model cannot fix by changing its arguments.

    The message has already been through Airflow's secret masker. Adapters let it
    propagate so the agent run, and with it the task, fails and Airflow's retry
    handles it, the same way an unhandled tool error fails an ``AgentOperator`` run.
    """


@dataclass(frozen=True)
class ToolResult:
    """
    The outcome of one tool call, as the agent framework receives it.

    :param content: What the model sees. A string or any JSON-compatible value.
    :param is_error: Whether the call failed in a way the model can correct, such as
        a query naming a column that does not exist. Adapters map this to the
        framework's own error status, so a failure is never inferred from the text of
        ``content``.
    """

    content: JsonValue
    is_error: bool = False


@dataclass(frozen=True)
class AirflowTool:
    """
    One operation an agent can call, independent of any agent framework.

    Call it through :meth:`call`, never ``function`` directly: :meth:`call` is the
    one path every adapter shares, and it is where Airflow's secret masker runs on
    everything returned to the model.

    :param name: Tool name shown to the model.
    :param description: What the tool does, shown to the model.
    :param parameters: JSON Schema (``"type": "object"``) for the tool's arguments.
    :param function: Async function that performs the operation. It receives the
        arguments the model supplied and returns a :class:`ToolResult`. It returns an
        error result for a failure the model can correct, and raises for anything else.
    """

    name: str
    description: str
    parameters: dict[str, Any]
    function: Callable[[dict[str, Any]], Awaitable[ToolResult]]

    async def call(self, arguments: dict[str, Any]) -> ToolResult:
        """
        Run the tool and return a result that is safe to hand back to the model.

        The content of the result, including error text, passes through Airflow's
        secret masker, so a connection password that ends up in a result is replaced
        with ``***`` before it reaches the model, the model provider, or anything the
        framework records. The masker only knows secrets Airflow has registered, such
        as connection passwords and sensitive connection extras.

        :raises ToolCallError: when ``function`` raises. The original exception is
            logged to the task log, which masks it; only the masked message travels
            on, because frameworks record a failed call's exception, cause included,
            in their traces.
        """
        log.info("::group::Tool call: %s", self.name)
        start = time.monotonic()
        failure: str | None = None
        try:
            result = await self.function(arguments)
        except Exception as e:
            log.warning("Tool %s failed after %.2fs", self.name, time.monotonic() - start, exc_info=True)
            failure = (
                str(e) if isinstance(e, ToolCallError) else f"{self.name} failed: {type(e).__name__}: {e}"
            )
        else:
            outcome = "returned an error" if result.is_error else "returned"
            log.info("Tool %s %s in %.2fs", self.name, outcome, time.monotonic() - start)
        finally:
            log.info("::endgroup::")
        if failure is not None:
            # Raised outside the except block, so the original is not attached as its context.
            raise ToolCallError(mask_secrets(failure))
        return ToolResult(content=mask_secrets(result.content), is_error=result.is_error)


class ToolProvider(Protocol):
    """A source of :class:`AirflowTool` objects, such as a toolset bound to one connection."""

    def airflow_tools(self) -> Sequence[AirflowTool]:
        """Return the tools this provider exposes."""
        ...


def collect_tools(sources: Iterable[ToolProvider | AirflowTool]) -> list[AirflowTool]:
    """
    Flatten toolsets and individual tools, as a framework adapter receives them, into tools.

    :raises ValueError: when two tools share a name. Frameworks route a call by name, so one
        of them would silently never run; give one toolset a tool name prefix.
    """
    tools: list[AirflowTool] = []
    for source in sources:
        if isinstance(source, AirflowTool):
            tools.append(source)
        else:
            tools.extend(source.airflow_tools())
    names = [tool.name for tool in tools]
    if duplicates := sorted({name for name in names if names.count(name) > 1}):
        raise ValueError(
            f"More than one tool is named {', '.join(duplicates)}. Give the toolsets different tool "
            "name prefixes where they take one (HookToolset's tool_name_prefix, the tool_prefix of "
            "SandboxToolset), so the model can call each of them."
        )
    return tools
