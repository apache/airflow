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
Strands Agents or any other framework through a small adapter such as
:func:`~airflow.providers.common.ai.tools.strands.as_strands_tools`, while the
agent itself stays native to that framework.

Toolsets that implement :class:`ToolProvider`, such as
:class:`~airflow.providers.common.ai.toolsets.sql.SQLToolset` and
:class:`~airflow.providers.common.ai.toolsets.hook.HookToolset`, expose their
tools through :meth:`ToolProvider.airflow_tools`.

.. warning::

    This interface is experimental. It may change in a minor release of this
    provider until it has been proven against more than one agent framework.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Protocol, cast

from airflow.providers.common.compat.sdk import redact

if TYPE_CHECKING:
    from pydantic import JsonValue

log = logging.getLogger(__name__)

__all__ = ["AirflowTool", "ToolProvider", "ToolResult"]

# The masker stops matching secret values below five levels of nesting by default. A tool can
# return an ordinary API payload nested deeper than that, so look further; the extra depth only
# costs time on results that are actually that deep.
_REDACT_MAX_DEPTH = 32


@dataclass(frozen=True)
class ToolResult:
    """
    The outcome of one tool call, as the agent framework receives it.

    :param content: What the model sees. A string or any JSON-compatible value.
    :param is_error: Whether the call failed. Adapters map this to the
        framework's own error status, so a failure is never inferred from the
        text of ``content``.
    """

    content: JsonValue
    is_error: bool = False


@dataclass(frozen=True)
class AirflowTool:
    """
    One operation an agent can call, independent of any agent framework.

    Call it through :meth:`call`, never ``function`` directly: :meth:`call` is
    the one path every adapter shares, and it is where exceptions become error
    results and where Airflow's secret masker runs on everything returned to the
    model.

    :param name: Tool name shown to the model.
    :param description: What the tool does, shown to the model.
    :param parameters: JSON Schema (``"type": "object"``) for the tool's arguments.
    :param function: Async function that performs the operation. It receives the
        arguments the model supplied and returns a :class:`ToolResult`.
    """

    name: str
    description: str
    parameters: dict[str, Any]
    function: Callable[[dict[str, Any]], Awaitable[ToolResult]]

    async def call(self, arguments: dict[str, Any]) -> ToolResult:
        """
        Run the tool and return a result that is safe to hand back to the model.

        An exception raised by ``function`` becomes a :class:`ToolResult` with
        ``is_error=True`` rather than propagating, so every framework feeds the
        failure back to the model the same way. The content of every result,
        including error text, passes through Airflow's secret masker, so a
        connection password that ends up in a result or an exception message is
        replaced with ``***`` before it reaches the model, the model provider,
        or anything the framework records.

        The masker only knows secrets Airflow has registered, such as
        connection passwords and sensitive connection extras. It does not
        recognize credentials that exist only in the data a tool returns.
        """
        log.info("::group::Tool call: %s", self.name)
        start = time.monotonic()
        try:
            result = await self.function(arguments)
        except Exception as e:
            log.warning("Tool %s failed after %.2fs", self.name, time.monotonic() - start, exc_info=True)
            result = ToolResult(content=f"{type(e).__name__}: {e}", is_error=True)
        else:
            outcome = "returned an error" if result.is_error else "returned"
            log.info("Tool %s %s in %.2fs", self.name, outcome, time.monotonic() - start)
        log.info("::endgroup::")
        # redact() is typed for arbitrary containers; it returns the same shape it is given.
        content = cast("JsonValue", redact(result.content, max_depth=_REDACT_MAX_DEPTH))
        return ToolResult(content=content, is_error=result.is_error)


class ToolProvider(Protocol):
    """A source of :class:`AirflowTool` objects, such as a toolset bound to one connection."""

    def airflow_tools(self) -> Sequence[AirflowTool]:
        """Return the tools this provider exposes."""
        ...
