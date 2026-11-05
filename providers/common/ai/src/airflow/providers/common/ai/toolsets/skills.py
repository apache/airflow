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
import logging
import os
from collections import Counter, defaultdict
from importlib import metadata
from pathlib import Path, PurePosixPath
from typing import TYPE_CHECKING, Any

from pydantic_ai.messages import ToolCallPart, ToolReturnPart

from airflow.providers.common.ai.skills import SkillSource, _materialize_skills, _SkillsLocation
from airflow.providers.common.ai.utils.logging import SKILLS_XCOM_KEY
from airflow.providers.common.ai.utils.toolset_base import validate_max_retries
from airflow.providers.common.compat.sdk import get_current_context

# pydantic-ai-skills 2.0 removed SkillsToolset, leaving only SkillsCapability, so the
# ``skills`` extra caps it below 2.
_SUPPORTED_PYDANTIC_AI_SKILLS = ">=1.2.0,<2"

try:
    from pydantic_ai.toolsets.abstract import AbstractToolset
except ImportError:  # pragma: no cover - pydantic-ai is a provider dependency
    AbstractToolset = object  # type: ignore[assignment,misc]

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence

    from pydantic_ai._run_context import RunContext
    from pydantic_ai.messages import InstructionPart, ModelMessage
    from pydantic_ai.toolsets.abstract import ToolsetTool

log = logging.getLogger(__name__)

# The pydantic-ai-skills tools that act on one skill, named by their ``skill_name`` argument.
_PER_SKILL_TOOLS = frozenset({"load_skill", "read_skill_resource", "run_skill_script"})


def _describe_skill_source(skill_uri: str, locations: list[_SkillsLocation]) -> dict[str, Any]:
    """Name where a discovered skill came from: its local path, or its repository, path and commit."""
    skill_dir = os.path.realpath(skill_uri)
    # Nested sources can both contain the skill; the deepest one is where it came from.
    owner, owner_root = None, ""
    for location in locations:
        root = os.path.realpath(location.directory)
        if os.path.commonpath([skill_dir, root]) == root and len(root) > len(owner_root):
            owner, owner_root = location, root
    if owner is None or owner.git is None:
        return {"path": skill_dir}
    return {
        # GitSkills rejects a repo_url carrying credentials, so it is safe to publish.
        "repo_url": owner.git.repo_url,
        "path": PurePosixPath(owner.git.path, Path(skill_dir).relative_to(owner_root).as_posix()).as_posix(),
        "branch": owner.git.branch,
        "commit": owner.commit,
    }


def _count_skill_calls(messages: Sequence[ModelMessage]) -> defaultdict[str, Counter[str]]:
    """Count, per skill, the calls to each per-skill tool that returned a result."""
    calls: dict[str, ToolCallPart] = {}
    counts: defaultdict[str, Counter[str]] = defaultdict(Counter)
    for message in messages:
        for part in message.parts:
            if isinstance(part, ToolCallPart):
                calls[part.tool_call_id] = part
            elif isinstance(part, ToolReturnPart) and (call := calls.get(part.tool_call_id)):
                # Two skills toolsets on one agent need ``prefixed()`` to keep their tool names apart.
                tool = next(
                    (t for t in _PER_SKILL_TOOLS if call.tool_name == t or call.tool_name.endswith(f"_{t}")),
                    None,
                )
                if tool is not None and isinstance(skill_name := call.args_as_dict().get("skill_name"), str):
                    counts[skill_name][tool] += 1
    return counts


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
        self._locations: list[_SkillsLocation] = []
        # The run's messages, prior history included. Calls made before an approval pause or
        # replayed on a durable retry never reach call_tool, so the skills XCom counts these.
        self._messages: Sequence[ModelMessage] = ()

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
        # pydantic-ai-skills is an optional extra, so it is imported here rather than at module level.
        try:
            import pydantic_ai_skills
        except ImportError as e:
            raise ValueError(
                "AgentSkillsToolset requires the optional 'skills' extra: "
                "pip install 'apache-airflow-providers-common-ai[skills]'."
            ) from e
        if not hasattr(pydantic_ai_skills, "SkillsToolset"):
            raise ValueError(
                f"AgentSkillsToolset needs pydantic-ai-skills{_SUPPORTED_PYDANTIC_AI_SKILLS}, but "
                f"pydantic-ai-skills {metadata.version('pydantic-ai-skills')} is installed and has no "
                "SkillsToolset. Install a supported version: "
                f"pip install 'pydantic-ai-skills{_SUPPORTED_PYDANTIC_AI_SKILLS}'."
            )
        SkillsToolset = pydantic_ai_skills.SkillsToolset

        locations, cleanup = _materialize_skills(self._sources)
        self._cleanup = cleanup
        self._locations = locations
        try:
            kwargs: dict[str, Any] = {"directories": [location.directory for location in locations]}
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
                self._push_skills_xcom()
                return await self._inner.__aexit__(*args)
            return None
        finally:
            if self._cleanup is not None:
                self._cleanup()
            self._inner = None
            self._cleanup = None

    def _push_skills_xcom(self) -> None:
        """Publish the skills offered to the agent and the skill tools it called, for the AI Model tab."""
        try:
            context = get_current_context()
        except RuntimeError:
            # Used outside an Airflow task: there is no task instance to publish on.
            return
        if not context["ti"].task.do_xcom_push:
            return
        # Telemetry only: a failure here must not fail the run or skip the clean-up after it.
        try:
            counts = _count_skill_calls(self._messages)
            skills = {
                name: {
                    "name": name,
                    "description": skill.description,
                    "source": _describe_skill_source(skill.uri, self._locations),
                    "calls": dict(counts.get(name, {})),
                }
                for name, skill in self._inner.skills.items()
            }
            # Another skills toolset on the agent, or this run before it paused for approval, may
            # have published already. XComs from an earlier try are cleared before this one starts.
            ti = context["ti"]
            stored = ti.xcom_pull(key=SKILLS_XCOM_KEY, task_ids=ti.task_id, map_indexes=ti.map_index) or []
            merged = {entry["name"]: entry for entry in stored} | skills
            ti.xcom_push(key=SKILLS_XCOM_KEY, value=[merged[name] for name in sorted(merged)])
        except Exception:
            log.warning("Failed to push the skills XCom", exc_info=True)

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
        self._messages = ctx.messages
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
