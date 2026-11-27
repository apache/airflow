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
`Agent Skills <https://agentskills.io>`__ sources for Airflow, and the pydantic-ai capability that offers them.

``resolve_skills`` and ``GitSkills`` do not depend on an agent framework. They resolve skill
*sources* -- a local directory or a ``GitSkills`` descriptor -- into local ``SKILL.md``
directories, cloning Git repositories with a token taken from an Airflow connection. The
output is a list of directory paths, the interchange format every Agent Skills implementation
consumes (pydantic-ai-skills, LangChain DeepAgents, Strands), so the same Airflow credential
handling works across frameworks.

For pydantic-ai use :class:`AirflowSkills`, or the deprecated
:class:`~airflow.providers.common.ai.toolsets.skills.AgentSkillsToolset` binding. For other frameworks,
resolve the directories yourself::

    from airflow.providers.common.ai.skills import GitSkills, resolve_skills

    with resolve_skills(["./skills", GitSkills(repo_url="https://...", conn_id="github")]) as dirs:
        # LangChain DeepAgents
        agent = create_deep_agent(model=..., skills=dirs)
        # ...or Strands
        agent = Agent(plugins=[AgentSkills(skills=dirs)])

Resolution (connection lookup, clone) happens when ``resolve_skills`` is entered,
so run it inside the task, not at module import / DAG-parse time. The context
manager removes any cloned directories on exit.
"""

from __future__ import annotations

import copy
import os
import shutil
import tempfile
import weakref
from collections.abc import Callable, Collection, Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Union
from urllib.parse import urlsplit

from pydantic_ai.capabilities import AbstractCapability, CombinedCapability
from pydantic_ai.tools import AgentDepsT

from airflow.providers.common.ai.exceptions import SkillsConfigurationError
from airflow.providers.common.ai.utils.skill_files import SkillRunState, build_skill_files_toolset

if TYPE_CHECKING:
    from pydantic_ai import RunContext
    from pydantic_ai.capabilities import WrapRunHandler
    from pydantic_ai.run import AgentRunResult
    from pydantic_ai.toolsets import AbstractToolset
    from pydantic_ai.workspaces import LocalWorkspaceBackend
    from pydantic_ai_harness.skills import Skills

__all__ = ["AirflowSkills", "GitSkills", "SkillSource", "resolve_skills"]


@dataclass
class GitSkills:
    """
    Agent Skills cloned from a Git repository when resolved.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    :param repo_url: HTTPS or SSH URL of the repository to clone.
    :param conn_id: Airflow ``git`` connection used for credentials, resolved
        through the Git provider's ``GitHook`` (HTTPS token in the connection
        password, or an SSH key in the connection's extra). **Set this for private
        repositories.** Plain ``http://`` is rejected when ``conn_id`` is set so a
        credential is never sent in cleartext, and a ``repo_url`` with embedded
        credentials is rejected (use ``conn_id`` instead). When ``conn_id`` is
        ``None`` the clone is unauthenticated; as with any ``git clone``, the
        worker's own git configuration (credential helpers, SSH agent) may still
        apply, so run workers without ambient git credentials if you need strict
        isolation.
    :param path: Sub-path inside the repository that holds the skill directories
        (e.g. ``"skills"``). Defaults to the repository root.
    :param branch: Branch, tag, or ref to check out. Defaults to the
        repository's default branch.

    .. warning::

        Skill bundles can contain scripts an agent may run on the worker. Because
        the repository is fetched at run time, anyone who can modify it can
        introduce code that runs in your environment, outside DAG review. Point
        ``repo_url`` at a trusted repository and set ``branch`` to a tag, because a branch name moves.
    """

    repo_url: str
    conn_id: str | None = None
    path: str = ""
    branch: str | None = None


SkillSource = Union[str, "os.PathLike[str]", GitSkills]


@dataclass(frozen=True)
class _SkillsLocation:
    """A resolved skills directory, with the Git source and commit it was cloned from."""

    directory: str
    git: GitSkills | None = None
    commit: str | None = None


def _clone_git(source: GitSkills) -> tuple[str, str, str | None]:
    """
    Clone *source* into a fresh temp dir; return (skills_dir, temp_dir_to_remove, commit).

    When ``conn_id`` is set, credentials come from the Airflow ``git`` connection
    via ``GitHook`` (HTTPS token or SSH key). The token is stripped from the
    clone's ``.git/config`` afterwards so a skill script in the checkout cannot
    read it back, interactive credential prompts are disabled, and the temp dir is
    removed if the clone fails. As with any ``git clone``, the worker's own git
    configuration (credential helpers, SSH agent) may still apply.
    """
    try:
        from git import Repo
    except ImportError as e:
        raise ValueError(
            "GitSkills requires GitPython. Install the 'skills' extra: "
            "pip install 'apache-airflow-providers-common-ai[skills]'."
        ) from e

    # Reject credentials embedded directly in the URL: they would be stored in
    # the serialized DAG, written back into .git/config by the scrub below, and
    # leak into error messages. Credentials must come from ``conn_id`` instead.
    split = urlsplit(source.repo_url)
    if split.username or split.password:
        raise ValueError(
            "GitSkills repo_url must not embed a username or password; pass credentials "
            "through a git connection via conn_id instead."
        )
    if source.conn_id and source.repo_url.startswith("http://"):
        raise ValueError(
            f"GitSkills refuses to send credentials from conn_id {source.conn_id!r} over plain "
            f"http://; use an https:// URL (token) or an ssh URL with a key on the connection."
        )

    # ``path`` is joined onto the clone dir; an absolute or upward-traversing
    # value would point outside the checkout. Require a relative sub-path so a
    # misconfigured value fails fast instead of silently reading elsewhere.
    if source.path:
        normalized = os.path.normpath(source.path)
        if os.path.isabs(source.path) or normalized == ".." or normalized.startswith(".." + os.sep):
            raise ValueError(
                f"GitSkills path must be a relative sub-path inside the repository; got {source.path!r}."
            )

    clone_kwargs: dict[str, Any] = {"depth": 1}
    if source.branch:
        clone_kwargs["branch"] = source.branch
    # Never drop into an interactive credential prompt on the worker.
    base_env = {"GIT_TERMINAL_PROMPT": "0"}

    temp_dir = tempfile.mkdtemp(prefix="airflow_skills_")
    hook = None
    try:
        if source.conn_id:
            from airflow.providers.git.hooks.git import GitHook

            hook = GitHook(git_conn_id=source.conn_id, repo_url=source.repo_url)
            with hook.configure_hook_env():
                clone_url = hook.repo_url or source.repo_url
                repo = Repo.clone_from(clone_url, temp_dir, env={**base_env, **hook.env}, **clone_kwargs)
        else:
            repo = Repo.clone_from(source.repo_url, temp_dir, env=base_env, **clone_kwargs)
        # Strip any embedded credential from .git/config so a skill script in the
        # checkout cannot read it back out of the temporary clone.
        repo.remote("origin").set_url(source.repo_url)
        # An empty repository has no commit to record, and its clone still worked.
        commit = repo.head.commit.hexsha if repo.head.is_valid() else None
    except Exception as exc:
        shutil.rmtree(temp_dir, ignore_errors=True)
        # GitPython errors embed the failing command, which may contain the
        # token-bearing URL GitHook built -- scrub it before surfacing to logs.
        message = str(exc)
        if hook is not None and hook.repo_url and hook.repo_url != source.repo_url:
            message = message.replace(hook.repo_url, source.repo_url)
        raise RuntimeError(f"Failed to clone {source.repo_url}: {message}") from None
    except BaseException:
        shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    skills_dir = os.path.join(temp_dir, source.path) if source.path else temp_dir
    return skills_dir, temp_dir, commit


def _materialize_skills(sources: list[SkillSource]) -> tuple[list[_SkillsLocation], Callable[[], None]]:
    """
    Resolve *sources* to local directories; return (locations, cleanup).

    ``cleanup`` removes any directories cloned for ``GitSkills`` sources; local
    directory sources are returned untouched and are not removed.
    """
    locations: list[_SkillsLocation] = []
    temp_dirs: list[str] = []
    try:
        for source in sources:
            if isinstance(source, GitSkills):
                skills_dir, temp_dir, commit = _clone_git(source)
                locations.append(_SkillsLocation(skills_dir, git=source, commit=commit))
                temp_dirs.append(temp_dir)
            elif isinstance(source, (str, os.PathLike)):
                locations.append(_SkillsLocation(os.fspath(source)))
            else:
                raise TypeError(
                    f"Unsupported skill source {type(source).__name__!r}; expected a path or GitSkills."
                )
    except BaseException:
        # Don't leak partially cloned directories if a later source fails.
        for temp_dir in temp_dirs:
            shutil.rmtree(temp_dir, ignore_errors=True)
        raise

    def cleanup() -> None:
        for temp_dir in temp_dirs:
            shutil.rmtree(temp_dir, ignore_errors=True)

    return locations, cleanup


@contextmanager
def resolve_skills(sources: list[SkillSource]) -> Iterator[list[str]]:
    """
    Resolve skill *sources* to local ``SKILL.md`` directories.

    Yields a list of directory paths suitable for any Agent Skills loader
    (pydantic-ai-skills, LangChain DeepAgents ``skills=``, Strands
    ``AgentSkills(skills=...)``). Cloned repositories are removed on exit, so use
    the returned directories inside the ``with`` block.
    """
    locations, cleanup = _materialize_skills(sources)
    try:
        yield [location.directory for location in locations]
    finally:
        cleanup()


def _import_harness_skills() -> tuple[type[Skills[Any]], type[LocalWorkspaceBackend]]:
    try:
        from pydantic_ai.workspaces import LocalWorkspaceBackend
        from pydantic_ai_harness.skills import Skills
    except ImportError as e:
        raise SkillsConfigurationError(
            "AirflowSkills requires pydantic-ai-harness. Install the 'skills' extra: "
            "pip install 'apache-airflow-providers-common-ai[skills]'."
        ) from e
    return Skills, LocalWorkspaceBackend


def _list_absolute_directories(locations: list[_SkillsLocation]) -> list[str]:
    # Last first: the harness keeps the first of two same-named skills and pydantic-ai-skills 1.x kept the last.
    return [os.path.abspath(location.directory) for location in reversed(locations)]


def _discard_run(
    runs: dict[str, SkillRunState], run_id: str, state: SkillRunState, cleanup: Callable[[], None]
) -> None:
    if runs.get(run_id) is state:
        del runs[run_id]
    cleanup()


class AirflowSkills(AbstractCapability[AgentDepsT]):
    """
    Offer `Agent Skills <https://agentskills.io>`__ from local directories and Git repositories to an agent.

    .. note::

        Experimental: this can change or be removed in a minor release of this provider.
        See :ref:`howto/stability`.

    Builds on pydantic-ai-harness's ``Skills`` capability. The model sees each skill's name and
    description and loads its instructions with ``load_capability``. Airflow adds what the harness
    does not: sourcing from :class:`GitSkills` with credentials from an Airflow connection, and two
    tools, ``read_skill_resource`` and ``run_skill_script``. Both refuse a skill until it is loaded.

    Sources are resolved when a run starts, not when the Dag is parsed, and cloned repositories
    are removed when the run ends. ``${SKILL_DIR}/`` and ``${CLAUDE_SKILL_DIR}/`` at the start of a
    path given to either tool name the skill's own directory.

    :param sources: Skill library directories and :class:`GitSkills` descriptors. Each directory
        holds one sub-directory per skill. When two sources hold a skill of the same name, the one
        from the later source is used and a warning is emitted, unless their ``SKILL.md`` files are
        byte-identical: the harness then treats them as one skill and says nothing.
    :param include: Names of the skills to offer; all others are left out. Cannot be combined with
        ``exclude``.
    :param exclude: Names of the skills to leave out. Cannot be combined with ``include``.
    :param exclude_resources: Glob patterns for files the two tools refuse, matched against a
        path relative to the skill or any one component of it, ignoring case. ``__pycache__``, ``*.pyc``, ``*.pyo``,
        ``.DS_Store`` and ``.git`` are always excluded.
    :param resources: Register ``read_skill_resource``. Defaults to ``True``.
    :param scripts: ``"worker"`` registers ``run_skill_script``, which runs a script as a subprocess
        of the worker, with the worker's permissions and a 30 second timeout. A script that prints more
        than 50,000 bytes is killed and its output cut off there. ``None`` registers no script tool. Defaults to ``"worker"``.
    :param max_retries: Correction attempts the model gets per tool call. Defaults to the agent's.
    """

    id = "airflow_skills"

    def __init__(
        self,
        sources: list[SkillSource],
        *,
        include: Collection[str] | None = None,
        exclude: Collection[str] | None = None,
        exclude_resources: list[str] | None = None,
        resources: bool = True,
        scripts: Literal["worker"] | None = "worker",
        max_retries: int | None = None,
    ) -> None:
        if not sources:
            raise ValueError("AirflowSkills needs at least one source.")
        if include is not None and exclude is not None:
            raise ValueError("include and exclude cannot be used together.")
        if scripts is not None and scripts != "worker":
            raise ValueError(
                f"AirflowSkills scripts must be 'worker' or None, got {scripts!r}. Running scripts in a "
                "sandbox needs a way to copy skills into it, which is not available yet."
            )
        self._sources = list(sources)
        self._include = include
        self._exclude = exclude
        self._runs: dict[str, SkillRunState] = {}
        self._toolset = build_skill_files_toolset(
            runs=self._runs,
            resources=resources,
            scripts=scripts is not None,
            exclude_resources=exclude_resources or [],
            max_retries=max_retries,
        )
        self._finalizer: weakref.finalize | None = None

    @classmethod
    def get_serialization_name(cls) -> str | None:
        return None

    @classmethod
    def combine(
        cls, capabilities: Sequence[AbstractCapability[AgentDepsT]]
    ) -> AbstractCapability[AgentDepsT]:
        raise ValueError(
            "An agent can have only one AirflowSkills, because its file tools share one toolset id. "
            "Put every source in a single AirflowSkills."
        )

    def get_toolset(self) -> AbstractToolset[Any] | None:
        return self._toolset

    async def for_run(self, ctx: RunContext[AgentDepsT]) -> AbstractCapability[AgentDepsT]:
        if self._finalizer is not None:
            return self
        skills_class, workspace_class = _import_harness_skills()
        # pydantic-ai advises against acquiring temporary files in for_run, because nothing releases them if the
        # run fails before it starts. Here the harness reads the directories as soon as the run starts, so the clone
        # has to exist by now. The copy returned below removes it when the run ends; for a run that never starts,
        # the finalizer (garbage collection, or at interpreter exit) is the backstop. The run's state is kept in
        # ``self._runs`` in process memory, which is why only Airflow's durable execution is supported.
        try:
            locations, cleanup = _materialize_skills(self._sources)
        except (ValueError, TypeError) as error:
            raise SkillsConfigurationError(f"AirflowSkills could not load its skills: {error}") from error
        run_id = ctx.run_id or ""
        state = SkillRunState(skill_dirs={})
        try:
            directories = _list_absolute_directories(locations)
            workspace = workspace_class(Path.cwd())
            if self._include is not None:
                skills = skills_class(
                    directories, include=self._include, duplicate_names="keep_first", workspace=workspace
                )
            else:
                skills = skills_class(
                    directories, exclude=self._exclude, duplicate_names="keep_first", workspace=workspace
                )
            catalog = await skills.load()
            state.skill_dirs = {skill.name: Path(skill.directory) for skill in catalog.skills}
            offered = await skills.for_run(ctx)
        except (ValueError, TypeError) as error:
            cleanup()
            raise SkillsConfigurationError(f"AirflowSkills could not load its skills: {error}") from error
        except BaseException:
            cleanup()
            raise
        run = copy.copy(self)
        self._runs[run_id] = state
        run._finalizer = weakref.finalize(run, _discard_run, self._runs, run_id, state, cleanup)
        return CombinedCapability([run, offered])

    async def wrap_run(self, ctx: RunContext[AgentDepsT], *, handler: WrapRunHandler) -> AgentRunResult[Any]:
        if self._finalizer is None:
            return await handler()
        try:
            return await handler()
        finally:
            self._finalizer()
