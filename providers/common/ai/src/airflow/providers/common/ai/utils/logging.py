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
"""Logging utilities for pydantic-ai agent runs."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from pydantic_ai.messages import ToolCallPart

from airflow.providers.common.ai.toolsets.logging import LoggingToolset

if TYPE_CHECKING:
    from pydantic_ai.result import AgentRunResult
    from pydantic_ai.toolsets.abstract import AbstractToolset

    from airflow.sdk.types import Logger

_MAX_OUTPUT_LEN = 500


def log_run_summary(logger: Logger | logging.Logger, result: AgentRunResult[Any]) -> None:
    """Log model name, token usage, and tool call sequence from an agent run."""
    usage = result.usage()
    model_name = getattr(result.response, "model_name", "unknown")
    logger.info(
        "::group::LLM run complete: model=%s, requests=%s, tool_calls=%s, "
        "input_tokens=%s, output_tokens=%s, total_tokens=%s",
        model_name,
        usage.requests,
        usage.tool_calls,
        usage.input_tokens,
        usage.output_tokens,
        usage.total_tokens,
    )

    tool_names = _extract_tool_sequence(result)
    if tool_names:
        logger.info("Tool call sequence: %s", " -> ".join(tool_names))

    _log_output_debug(logger, result.output)
    logger.info("::endgroup::")


def _log_output_debug(logger: Logger | logging.Logger, output: Any) -> None:
    """Log a truncated representation of the agent output at DEBUG level."""
    if not logger.isEnabledFor(logging.DEBUG):
        return
    from pydantic import BaseModel

    if isinstance(output, BaseModel):
        text = repr(output.model_dump())
    else:
        text = repr(output)
    if len(text) > _MAX_OUTPUT_LEN:
        text = text[:_MAX_OUTPUT_LEN] + "..."
    logger.debug("Output: %s", text)


def _extract_tool_sequence(result: AgentRunResult[Any]) -> list[str]:
    """Extract ordered tool names from the message history."""
    tool_names: list[str] = []
    for message in result.all_messages():
        for part in getattr(message, "parts", []):
            if isinstance(part, ToolCallPart):
                tool_names.append(part.tool_name)
    return tool_names


def wrap_toolsets_for_logging(
    toolsets: list[AbstractToolset[Any]],
    logger: Logger | logging.Logger,
) -> list[AbstractToolset[Any]]:
    """Wrap each toolset in a LoggingToolset."""
    return [LoggingToolset(wrapped=ts, logger=logger) for ts in toolsets]
