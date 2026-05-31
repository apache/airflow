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
"""Logging utilities for agent runs."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from airflow.providers.common.ai.hooks.base import AgentRunResult
    from airflow.sdk.types import Logger

_MAX_OUTPUT_LEN = 500


def log_run_summary(logger: Logger | logging.Logger, result: AgentRunResult) -> None:
    """Log model name, token usage, and tool call sequence from an agent run."""
    model_name = result.model_name or "unknown"
    usage = result.usage
    if usage is not None:
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
    else:
        logger.info("::group::LLM run complete: model=%s", model_name)

    if result.tool_names:
        logger.info("Tool call sequence: %s", " -> ".join(result.tool_names))

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
