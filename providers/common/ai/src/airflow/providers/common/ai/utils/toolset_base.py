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
"""Behaviour shared by the toolsets this provider ships."""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import threading
from abc import abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, TypeVar

from pydantic_ai.exceptions import ApprovalRequired, CallDeferred, ModelRetry, ToolFailed
from pydantic_ai.messages import ToolReturn
from pydantic_ai.toolsets import DynamicToolset
from pydantic_ai.toolsets.abstract import AbstractToolset
from pydantic_ai.toolsets.wrapper import WrapperToolset
from typing_extensions import ParamSpec

from airflow.providers.common.ai.utils.masking import mask_secrets

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pydantic_ai._run_context import RunContext
    from pydantic_ai.toolsets import ToolsetFunc
    from pydantic_ai.toolsets.abstract import ToolsetTool

log = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

# One blocking call through AirflowToolset.run_blocking at a time in the process, across every
# toolset that uses it. Hooks are not thread-safe in general, and before Airflow 3.2 the channel
# to the supervisor that resolves connections and variables has no lock of its own. Agent
# frameworks run tool calls concurrently, so one lock per instance would not be enough.
_blocking_call_lock = threading.Lock()

# Set on an exception _masked has already stripped, so a second masking layer around the same
# toolset does not log it again.
_STRIPPED = "_airflow_secrets_masked"

# How the model or the run acts on a call without a result, rather than failures: pydantic-ai
# asks the model to correct its call, or pauses the run for approval or deferred execution.
_CONTROL_FLOW = (ModelRetry, ToolFailed, ApprovalRequired, CallDeferred)


def _call_locked(fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    with _blocking_call_lock:
        return fn(*args, **kwargs)


async def _masked(name: str, call: Awaitable[Any]) -> Any:
    """
    Await a tool call and mask everything it hands on: its result, or the exception it raised.

    An exception usually keeps its type, so retry policies and pydantic-ai's own handling
    still recognize it, but its message is masked and the chain of exceptions that caused it
    is dropped: frameworks and tracing record a failed call's traceback, cause included. A
    retry rule can therefore match the exception's type but not its cause. A failure is
    logged first, with its cause, to the task log, which masks it on the way out.
    """
    error: Exception | None = None
    try:
        result = await call
    except _CONTROL_FLOW as e:
        log.debug("Tool %s returned no result", name, exc_info=e)
        error = _strip(e)
    except Exception as e:
        if not getattr(e, _STRIPPED, False):
            log.warning("Tool %s failed", name, exc_info=e)
        error = _strip(e)
    if error is not None:
        # Raised outside the except blocks, so Python does not chain the original back on.
        raise error
    if isinstance(result, ToolReturn):
        return dataclasses.replace(
            result, return_value=mask_secrets(result.return_value), content=mask_secrets(result.content)
        )
    return mask_secrets(result)


def _strip(error: Exception) -> Exception:
    """
    Mask what ``error`` would print and drop its cause chain.

    An exception's message need not come from its ``args``: ``OSError`` formats its
    ``strerror`` and ``filename``, and a custom ``__str__`` can read any attribute. Those
    are masked too, and so are the exceptions inside an exception group. If the message
    still holds a registered secret after that, or masking it fails, a ``RuntimeError``
    carrying only what could be masked is returned in its place.
    """
    error.__cause__ = None
    error.__context__ = None
    try:
        stripped = _stripped(error)
        message = str(stripped)
        if (masked := mask_secrets(message)) != message:
            stripped = RuntimeError(f"{type(error).__name__}: {masked}")
    except Exception:
        stripped = RuntimeError(f"{type(error).__name__}: details withheld, they could not be masked")
    setattr(stripped, _STRIPPED, True)
    return stripped


def _stripped(error: Exception) -> Exception:
    group = getattr(error, "exceptions", None)
    if isinstance(group, tuple) and hasattr(error, "derive"):
        # An exception group's own message and arguments are set when it is built.
        return type(error)(mask_secrets(getattr(error, "message", "")), [_strip(e) for e in group])
    error.args = mask_secrets(error.args)
    for attribute, value in vars(error).items():
        # Only text and containers: turning a model into a dict could break the __str__ that reads it.
        if isinstance(value, (str, bytes, dict, list, tuple, set, frozenset)):
            vars(error)[attribute] = mask_secrets(value)
    if isinstance(error, OSError):
        # Only those that are set: assigning None to an unset one changes how it prints.
        for attribute in ("strerror", "filename", "filename2"):
            if (value := getattr(error, attribute)) is not None:
                setattr(error, attribute, mask_secrets(value))
    return error


class AirflowToolset(AbstractToolset[Any]):
    """
    A toolset whose tool results are safe to hand to a model.

    Subclasses implement :meth:`_execute_tool`. :meth:`call_tool` runs it and passes what it
    returns, and any exception it raises, through Airflow's secret masker, so a connection
    password that ends up in a database error or a hook's return value is replaced with
    ``***`` before the model, the model provider or a trace sees it.
    """

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        return await _masked(name, self._execute_tool(name, tool_args, ctx, tool))

    @abstractmethod
    async def _execute_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        """Run tool ``name`` with validated ``tool_args``; :meth:`call_tool` masks what it returns."""

    @staticmethod
    async def run_blocking(fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
        """
        Run a blocking hook call in a worker thread, keeping the event loop free.

        Calls made through this method are serialized across the process.
        """
        return await asyncio.to_thread(_call_locked, fn, *args, **kwargs)


@dataclass
class MaskingToolset(WrapperToolset[Any]):
    """
    Apply the same masking as :class:`AirflowToolset` to any toolset.

    ``AgentOperator`` wraps every toolset it runs in one, so a toolset the Dag author wrote
    gets masked output too.
    """

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        return await _masked(name, self.wrapped.call_tool(name, tool_args, ctx, tool))


def with_masking(toolset: AbstractToolset[Any] | ToolsetFunc[Any]) -> AbstractToolset[Any]:
    """
    Return ``toolset`` wrapped in :class:`MaskingToolset`, unless it already masks its own output.

    A function that builds a toolset for each run, which pydantic-ai also accepts, is wrapped
    too. So is an :class:`AirflowToolset` whose ``call_tool`` is overridden, since the override
    can bypass the masking.
    """
    if not isinstance(toolset, AbstractToolset):
        toolset = DynamicToolset(toolset)
    elif isinstance(toolset, AirflowToolset) and type(toolset).call_tool is AirflowToolset.call_tool:
        return toolset
    return MaskingToolset(wrapped=toolset)
