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
"""Count calls to the toolsets this provider ships, by toolset, agent framework and outcome."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from typing import TYPE_CHECKING, Literal

from airflow.providers.common.compat.sdk import Stats

if TYPE_CHECKING:
    from collections.abc import Iterator

#: The agent framework a tool call came through, as the ``framework`` tag reports it.
Framework = Literal["pydantic_ai", "strands", "adk", "langchain", "none"]

# Set by each framework adapter around the calls it makes.
_framework: ContextVar[Framework | None] = ContextVar("common_ai_tool_framework", default=None)


@contextmanager
def calling_framework(name: Framework) -> Iterator[None]:
    """Attribute the tool calls made inside the block to agent framework ``name``."""
    token = _framework.set(name)
    try:
        yield
    finally:
        _framework.reset(token)


def current_framework() -> Framework | None:
    """Return the framework the current tool call is attributed to, if an adapter set one."""
    return _framework.get()


def record_tool_call(toolset: str, outcome: Literal["executed", "failed", "replayed"]) -> None:
    """
    Count one call to a toolset.

    Tags stay low-cardinality: the toolset class, the framework and the outcome, never
    arguments, connection IDs, table names or paths. A call made outside any adapter is a
    Pydantic AI agent's, such as ``AgentOperator``'s.
    """
    Stats.incr(
        "common_ai.tool_calls",
        tags={"toolset": toolset, "framework": _framework.get() or "pydantic_ai", "outcome": outcome},
    )
