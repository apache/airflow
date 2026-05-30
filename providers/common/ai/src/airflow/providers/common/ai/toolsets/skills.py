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
A pydantic-ai toolset that loads `Agent Skills <https://agentskills.io>`__.

``AgentSkillsToolset`` is a normal pydantic-ai ``AbstractToolset``: it can be
passed to :class:`~airflow.providers.common.ai.operators.agent.AgentOperator`
via ``toolsets=`` or used directly with a ``pydantic_ai.Agent`` anywhere the
Airflow connection backend is reachable (i.e. inside a worker/task runtime).

Skill sources are resolved lazily when the agent enters the toolset (run time,
on the worker), never at DAG-parse time, so a Git token resolved from an Airflow
connection is never baked into the serialized DAG. Cloned repositories are
removed when the toolset context exits.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.skills import SkillSource, _materialize_skills

try:
    from pydantic_ai.toolsets.abstract import AbstractToolset
except ImportError:  # pragma: no cover - pydantic-ai is a provider dependency
    AbstractToolset = object  # type: ignore[assignment,misc]

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from pydantic_ai._run_context import RunContext
    from pydantic_ai.messages import InstructionPart
    from pydantic_ai.toolsets.abstract import ToolsetTool


class AgentSkillsToolset(AbstractToolset):
    """
    A pydantic-ai toolset that loads Agent Skills, with Git credentials from Airflow connections.

    Sources are local directory paths and/or
    :class:`~airflow.providers.common.ai.skills.GitSkills`.

    :param sources: Skill sources -- local directory paths and/or ``GitSkills``.
    :param exclude_tools: Optional set of skill tool names to hide from the agent
        (e.g. ``{"run_skill_script"}`` to disable on-worker script execution).

    Requires the ``skills`` extra: ``pip install "apache-airflow-providers-common-ai[skills]"``.
    """

    def __init__(
        self,
        sources: list[SkillSource],
        *,
        exclude_tools: set[str] | None = None,
    ) -> None:
        self._sources = list(sources)
        self._exclude_tools = exclude_tools
        self._inner: Any = None
        self._cleanup: Callable[[], None] | None = None

    @property
    def id(self) -> str | None:
        return None

    async def for_run(self, ctx: RunContext) -> AbstractToolset:
        # Per-run isolation: pydantic-ai shares one toolset instance across runs,
        # but we hold per-run clone/cleanup state on __aenter__/__aexit__. Hand
        # each run its own instance so concurrent runs never clobber each other.
        return AgentSkillsToolset(self._sources, exclude_tools=self._exclude_tools)

    async def __aenter__(self) -> AgentSkillsToolset:
        # Resolve + clone at run time, on the worker -- not at DAG-parse time.
        try:
            from pydantic_ai_skills import SkillsToolset
        except ImportError as e:
            raise ValueError(
                "AgentSkillsToolset requires the optional 'skills' extra: "
                "pip install 'apache-airflow-providers-common-ai[skills]'."
            ) from e

        directories, cleanup = _materialize_skills(self._sources)
        self._cleanup = cleanup
        try:
            kwargs: dict[str, Any] = {"directories": directories}
            if self._exclude_tools:
                kwargs["exclude_tools"] = self._exclude_tools
            self._inner = SkillsToolset(**kwargs)
            await self._inner.__aenter__()
        except BaseException:
            cleanup()
            self._inner = None
            self._cleanup = None
            raise
        return self

    async def __aexit__(self, *args: Any) -> bool | None:
        try:
            if self._inner is not None:
                return await self._inner.__aexit__(*args)
            return None
        finally:
            if self._cleanup is not None:
                self._cleanup()
            self._inner = None
            self._cleanup = None

    def _require_inner(self) -> Any:
        if self._inner is None:
            raise RuntimeError(
                "AgentSkillsToolset must be entered via 'async with' (the agent does this "
                "during a run) before its tools are used."
            )
        return self._inner

    async def get_tools(self, ctx: RunContext) -> dict[str, ToolsetTool]:
        return await self._require_inner().get_tools(ctx)

    async def call_tool(
        self, name: str, tool_args: dict[str, Any], ctx: RunContext, tool: ToolsetTool
    ) -> Any:
        return await self._require_inner().call_tool(name, tool_args, ctx, tool)

    async def get_instructions(
        self, ctx: RunContext
    ) -> str | InstructionPart | Sequence[str | InstructionPart] | None:
        return await self._require_inner().get_instructions(ctx)
