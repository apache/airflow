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
Framework-agnostic `Agent Skills <https://agentskills.io>`__ sources for Airflow.

This module resolves skill *sources* -- a local directory or a ``GitSkills``
descriptor -- into local ``SKILL.md`` directories, cloning Git repositories with
a token taken from an Airflow connection. The output is a list of directory
paths, the interchange format every Agent Skills implementation consumes
(pydantic-ai-skills, LangChain DeepAgents, Strands), so the same Airflow
credential handling works across frameworks.

For pydantic-ai use the :class:`~airflow.providers.common.ai.toolsets.skills.AgentSkillsToolset`
binding. For other frameworks, resolve the directories yourself::

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

import os
import shutil
import tempfile
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Union
from urllib.parse import urlsplit

__all__ = ["GitSkills", "SkillSource", "resolve_skills"]


@dataclass
class GitSkills:
    """
    Agent Skills cloned from a Git repository when resolved.

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
        ``repo_url`` at a trusted repository and pin ``branch`` to a trusted ref.
    """

    repo_url: str
    conn_id: str | None = None
    path: str = ""
    branch: str | None = None


SkillSource = Union[str, "os.PathLike[str]", GitSkills]


def _clone_git(source: GitSkills) -> tuple[str, str]:
    """
    Clone *source* into a fresh temp dir; return (skills_dir, temp_dir_to_remove).

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
    return skills_dir, temp_dir


def _materialize_skills(sources: list[SkillSource]) -> tuple[list[str], Callable[[], None]]:
    """
    Resolve *sources* to local directories; return (directories, cleanup).

    ``cleanup`` removes any directories cloned for ``GitSkills`` sources; local
    directory sources are returned untouched and are not removed.
    """
    directories: list[str] = []
    temp_dirs: list[str] = []
    try:
        for source in sources:
            if isinstance(source, GitSkills):
                skills_dir, temp_dir = _clone_git(source)
                directories.append(skills_dir)
                temp_dirs.append(temp_dir)
            elif isinstance(source, (str, os.PathLike)):
                directories.append(os.fspath(source))
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

    return directories, cleanup


@contextmanager
def resolve_skills(sources: list[SkillSource]) -> Iterator[list[str]]:
    """
    Resolve skill *sources* to local ``SKILL.md`` directories.

    Yields a list of directory paths suitable for any Agent Skills loader
    (pydantic-ai-skills, LangChain DeepAgents ``skills=``, Strands
    ``AgentSkills(skills=...)``). Cloned repositories are removed on exit, so use
    the returned directories inside the ``with`` block.
    """
    directories, cleanup = _materialize_skills(sources)
    try:
        yield directories
    finally:
        cleanup()
