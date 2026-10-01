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
from typing import TYPE_CHECKING, Any, Literal, TypeVar

from pydantic_ai.capabilities import AbstractCapability, CapabilityOrdering
from pydantic_ai.exceptions import (
    ApprovalRequired,
    CallDeferred,
    ModelRetry,
    ToolFailed,
    ToolFailedError,
    ToolRetryError,
)
from pydantic_ai.messages import ToolReturn
from pydantic_ai.toolsets import DynamicToolset
from pydantic_ai.toolsets.abstract import AbstractToolset
from pydantic_ai.toolsets.wrapper import WrapperToolset
from typing_extensions import ParamSpec

from airflow.providers.common.ai.tools._from_toolset import airflow_tools_from_toolset
from airflow.providers.common.ai.utils.masking import mask_secrets
from airflow.providers.common.ai.utils.tool_metrics import record_tool_call

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from pydantic_ai._run_context import RunContext
    from pydantic_ai.capabilities.abstract import ValidatedToolArgs, WrapToolExecuteHandler
    from pydantic_ai.messages import ToolCallPart
    from pydantic_ai.tools import ToolDefinition
    from pydantic_ai.toolsets import ToolsetFunc
    from pydantic_ai.toolsets.abstract import ToolsetTool

    from airflow.providers.common.ai.tools import AirflowTool

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

# How pydantic-ai pauses a run until a person approves a call or the call runs elsewhere.
_PAUSED = (ApprovalRequired, CallDeferred)


def _call_locked(fn: Callable[P, R], /, *args: P.args, **kwargs: P.kwargs) -> R:
    with _blocking_call_lock:
        return fn(*args, **kwargs)


async def _mask_call(name: str, call: Awaitable[Any], *, count_as: str | None = None) -> Any:
    """
    Await a tool call and mask everything it hands on: its result, or the exception it raised.

    An exception usually keeps its type, so retry policies and pydantic-ai's own handling
    still recognize it, but its message is masked and the chain of exceptions that caused it
    is dropped: frameworks and tracing record a failed call's traceback, cause included. A
    retry rule can therefore match the exception's type but not its cause. A failure is
    logged first, with its cause, to the task log, which masks it on the way out.
    """
    outcome: Literal["executed", "failed"] | None = "failed"
    error: Exception | None = None
    try:
        result = await call
        outcome = "executed"
    except _PAUSED as e:
        # The run pauses for approval or deferred execution; the call has not happened yet.
        log.debug("Tool %s is waiting to run", name, exc_info=e)
        outcome = None
        error = _strip(e)
    except (ModelRetry, ToolFailed) as e:
        log.debug("Tool %s returned an error for the model", name, exc_info=e)
        error = _strip(e)
    except (ToolRetryError, ToolFailedError) as e:
        # The form a ModelRetry or ToolFailed takes by the time a capability's
        # wrap_tool_execute sees it. The model is sent the message part it carries, not the
        # exception's text, so that part is what gets masked.
        log.debug("Tool %s returned an error for the model", name, exc_info=e)
        error = _mask_tool_error_part(e)
    except Exception as e:
        if not getattr(e, _STRIPPED, False):
            log.warning("Tool %s failed", name, exc_info=e)
        error = _strip(e)
    finally:
        if count_as and outcome:
            record_tool_call(count_as, outcome)
    if error is not None:
        # Raised outside the except blocks, so Python does not chain the original back on.
        raise error
    if isinstance(result, ToolReturn):
        return dataclasses.replace(
            result,
            return_value=mask_secrets(result.return_value),
            content=mask_secrets(result.content),
            # Not sent to the model, but kept in the message history and recorded in traces.
            metadata=mask_secrets(result.metadata),
        )
    return mask_secrets(result)


def _mask_tool_error_part(error: ToolRetryError | ToolFailedError) -> ToolRetryError | ToolFailedError:
    """Rebuild ``error`` around a copy of the message part it carries, with that part's content masked."""
    if isinstance(error, ToolRetryError):
        return ToolRetryError(
            dataclasses.replace(error.tool_retry, content=mask_secrets(error.tool_retry.content))
        )
    return ToolFailedError(
        dataclasses.replace(error.tool_failed, content=mask_secrets(error.tool_failed.content))
    )


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
        stripped = _mask_attributes(error)
        message = str(stripped)
        if (masked := mask_secrets(message)) != message:
            stripped = RuntimeError(f"{type(error).__name__}: {masked}")
    except Exception:
        stripped = RuntimeError(f"{type(error).__name__}: details withheld, they could not be masked")
    setattr(stripped, _STRIPPED, True)
    return stripped


def _mask_attributes(error: Exception) -> Exception:
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

    Two methods look alike and have different jobs. :meth:`execute_tool` is the one a subclass
    writes: it runs the tool and returns the result as the tool produced it. :meth:`call_tool`
    is pydantic-ai's entry point, implemented here once: it runs ``execute_tool`` and passes what
    it returns, and any exception it raises, through Airflow's secret masker, so a connection
    password that ends up in a database error or a hook's return value is replaced with ``***``
    before the model, the model provider or a trace sees it. A subclass that overrides
    ``call_tool`` instead skips that masking, which is why :func:`ensure_masked` wraps such a
    toolset again.

    The two signatures differ on purpose. ``call_tool`` keeps the positional shape pydantic-ai
    invokes it with. ``execute_tool`` takes ``ctx`` and ``tool`` keyword-only, so arguments can
    be added to it later without breaking subclasses.
    """

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        # pydantic-ai calls this positionally, so its signature has to stay as it defines it.
        return await _mask_call(
            name, self.execute_tool(name, tool_args, ctx=ctx, tool=tool), count_as=type(self).__name__
        )

    @abstractmethod
    async def execute_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        *,
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        """
        Run tool ``name`` with validated ``tool_args`` and return its result unmasked.

        This is the method a subclass implements; :meth:`call_tool` runs it and masks what it
        returns. ``ctx`` and ``tool`` are keyword-only so that arguments can be added here later
        without breaking subclasses.
        """

    def airflow_tools(self) -> list[AirflowTool]:
        """
        Return this toolset's tools for an agent framework other than Pydantic AI.

        See :mod:`airflow.providers.common.ai.tools` for the adapters that take them.
        """
        return airflow_tools_from_toolset(self)

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
        return await _mask_call(name, self.wrapped.call_tool(name, tool_args, ctx, tool))


@dataclass
class MaskingCapability(AbstractCapability[Any]):
    """
    Mask what every tool hands the model, however the tool reached the agent.

    :class:`MaskingToolset` only covers a toolset the operator can find and wrap. A
    capability can supply tools the operator never sees as a toolset: ``MCP``, a
    ``Toolset`` built per run, or a ``Toolset`` inside ``PrefixTools`` or a
    ``CombinedCapability``. This masks around every tool call instead.

    Other capabilities see a tool's result only after this one has masked it, provided it
    is the innermost capability: it declares the innermost position, and pydantic-ai breaks
    ties between capabilities that declare it by list order, the last one innermost. Put it
    last in the capability list, as ``AgentOperator`` does.
    """

    def get_ordering(self) -> CapabilityOrdering:
        """Join the innermost capabilities; list order decides which of them is closest to the tool."""
        return CapabilityOrdering(position="innermost")

    async def wrap_tool_execute(
        self,
        ctx: RunContext[Any],
        *,
        call: ToolCallPart,
        tool_def: ToolDefinition,
        args: ValidatedToolArgs,
        handler: WrapToolExecuteHandler,
    ) -> Any:
        """Run the tool and mask its result, or the error it raised, before anything else sees it."""
        return await _mask_call(call.tool_name, handler(args))


def ensure_masked(toolset: AbstractToolset[Any] | ToolsetFunc[Any]) -> AbstractToolset[Any]:
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
