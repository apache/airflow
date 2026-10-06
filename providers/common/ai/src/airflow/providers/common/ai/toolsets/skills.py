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

import dataclasses
from typing import TYPE_CHECKING, Any

from airflow.providers.common.ai.skills import SkillSource, _materialize_skills
from airflow.providers.common.ai.utils.toolset_base import validate_max_retries

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

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Sources are local directory paths and/or
    :class:`~airflow.providers.common.ai.skills.GitSkills`.

    :param sources: Skill sources -- local directory paths and/or ``GitSkills``.
    :param exclude_tools: Optional set of skill tool names to hide from the agent
        (e.g. ``{"run_skill_script"}`` to disable on-worker script execution).
    :param exclude_resources: Optional glob patterns to exclude from resource
        discovery, added on top of the built-in defaults (``__pycache__``,
        ``*.pyc``, ``*.pyo``, ``.DS_Store``, ``.git``). A skill exposes every
        readable text file it contains as a resource; these patterns keep matched
        files out of the resource list and the ``read_skill_resource`` tool
        (e.g. ``["*.env", "secrets/*"]``). Patterns match the full skill-relative
        path or any single path component. This hides files from resource
        discovery only -- it does not stop a skill's ``run_skill_script`` from
        reading them off disk, so pair it with ``exclude_tools={"run_skill_script"}``
        when the files are genuinely sensitive. Requires ``pydantic-ai-skills>=1.2.0``.
    :param max_retries: How many times the model may correct failed calls to one skills tool,
        such as a resource name that does not exist, before the run fails; a successful call
        to that tool resets the count. ``None`` (the default) uses the agent's tool retry
        budget, its ``retries``, as the provider's other toolsets do.

    Requires the ``skills`` extra: ``pip install "apache-airflow-providers-common-ai[skills]"``.
    """

    def __init__(
        self,
        sources: list[SkillSource],
        *,
        exclude_tools: set[str] | None = None,
        exclude_resources: list[str] | None = None,
        max_retries: int | None = None,
    ) -> None:
        self._sources = list(sources)
        self._exclude_tools = exclude_tools
        self._exclude_resources = exclude_resources
        self._max_retries = validate_max_retries(max_retries)
        self._inner: Any = None
        self._cleanup: Callable[[], None] | None = None

    @property
    def id(self) -> str | None:
        return None

    async def for_run(self, ctx: RunContext) -> AbstractToolset:
        # Per-run isolation: pydantic-ai shares one toolset instance across runs,
        # but we hold per-run clone/cleanup state on __aenter__/__aexit__. Hand
        # each run its own instance so concurrent runs never clobber each other.
        return AgentSkillsToolset(
            self._sources,
            exclude_tools=self._exclude_tools,
            exclude_resources=self._exclude_resources,
            max_retries=self._max_retries,
        )

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
            if self._exclude_resources:
                kwargs["exclude_resources"] = self._exclude_resources
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
        # pydantic-ai-skills fixes its tools' budget at one correction, whatever the agent's
        # retries say; resolve it the way the provider's other toolsets do instead.
        max_retries = ctx.max_retries if self._max_retries is None else self._max_retries
        tools = await self._require_inner().get_tools(ctx)
        return {name: dataclasses.replace(tool, max_retries=max_retries) for name, tool in tools.items()}

    async def call_tool(
        self, name: str, tool_args: dict[str, Any], ctx: RunContext, tool: ToolsetTool
    ) -> Any:
        return await self._require_inner().call_tool(name, tool_args, ctx, tool)

    async def get_instructions(
        self, ctx: RunContext
    ) -> str | InstructionPart | Sequence[str | InstructionPart] | None:
        return await self._require_inner().get_instructions(ctx)
