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
"""Logging wrapper toolset for pydantic-ai tool calls."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from pydantic_ai.capabilities import AbstractCapability, CapabilityOrdering
from pydantic_ai.exceptions import (
    ApprovalRequired,
    CallDeferred,
    ModelRetry,
    SkipToolExecution,
    ToolFailed,
)
from pydantic_ai.toolsets.wrapper import WrapperToolset

if TYPE_CHECKING:
    from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool

    from airflow.sdk.types import Logger


@dataclass
class LoggingToolset(WrapperToolset[Any]):
    """
    Wrap a toolset to log each tool call with timing.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.
    """

    logger: Logger | logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: Any,
        tool: ToolsetTool[Any],
    ) -> Any:
        self.logger.info("::group::Tool call: %s", name)
        if tool_args and self.logger.isEnabledFor(logging.DEBUG):
            try:
                serialized_args = json.dumps(tool_args, default=str)
            except (TypeError, ValueError):
                serialized_args = repr(tool_args)
            self.logger.debug("Tool args: %s", serialized_args)
        start = time.monotonic()
        try:
            result = await self.wrapped.call_tool(name, tool_args, ctx, tool)
            elapsed = time.monotonic() - start
            self.logger.info("Tool %s returned in %.2fs", name, elapsed)
            self.logger.info("::endgroup::")
            return result
        except asyncio.CancelledError:
            self.logger.info("::endgroup::")
            raise
        except ApprovalRequired:
            # Not a failure: the run pauses here until a person approves or rejects the call.
            elapsed = time.monotonic() - start
            self.logger.info("Tool %s is waiting to be approved after %.2fs", name, elapsed)
            self.logger.info("::endgroup::")
            raise
        except (ModelRetry, ToolFailed, CallDeferred, SkipToolExecution) as e:
            elapsed = time.monotonic() - start
            self.logger.info("Tool %s requested %s after %.2fs: %s", name, type(e).__name__, elapsed, e)
            self.logger.info("::endgroup::")
            raise
        except Exception:
            elapsed = time.monotonic() - start
            self.logger.info("::endgroup::")
            self.logger.exception("Tool %s failed after %.2fs", name, elapsed)
            raise


@dataclass
class ToolLoggingCapability(AbstractCapability[Any]):
    """Apply tool-call logging to the assembled function toolset for an agent run."""

    logger: Logger | logging.Logger = field(default_factory=lambda: logging.getLogger(__name__))

    @classmethod
    def get_serialization_name(cls) -> str | None:
        return None

    def get_ordering(self) -> CapabilityOrdering:
        """Place logging innermost so outer capability wrappers delegate base-tool calls through it."""
        return CapabilityOrdering(position="innermost")

    def get_wrapper_toolset(self, toolset: AbstractToolset[Any]) -> AbstractToolset[Any]:
        return LoggingToolset(wrapped=toolset, logger=self.logger)
