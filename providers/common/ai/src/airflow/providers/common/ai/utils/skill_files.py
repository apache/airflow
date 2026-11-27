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
File tools for :class:`~airflow.providers.common.ai.skills.AirflowSkills`.

``read_skill_resource`` reads a file bundled with a skill and ``run_skill_script`` runs one of its
scripts on the worker. Both resolve the skill through the run's own table of skill directories,
so one toolset instance, built once with a stable id, serves every run.
"""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
import fnmatch
import json
import os
import re
import shlex
import shutil
import signal
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path, PurePosixPath
from typing import Any

import yaml
from pydantic_ai import ModelRetry, RunContext
from pydantic_ai.toolsets import FunctionToolset

from airflow.providers.common.ai.utils.toolset_base import validate_max_retries

TOOLSET_ID = "airflow_skills"

DEFAULT_RESOURCE_EXCLUDES = ("__pycache__", "*.pyc", "*.pyo", ".DS_Store", ".git")

# Spellings of the skill's own directory that skill authors write in SKILL.md; the harness leaves them as
# literal text, and the tools take them to mean the skill's root.
_SKILL_DIR_PREFIXES = ("${SKILL_DIR}/", "${CLAUDE_SKILL_DIR}/")

# The variables a script inherits from the worker, the same as pydantic-ai's ``LocalWorkspaceBackend``.
_INHERITED_ENV_NAMES = ("PATH", "HOME", "LANG", "LC_ALL", "LC_CTYPE")

_SCRIPT_TIMEOUT_SECONDS = 30.0
# How long to wait for the pipe to close after the script was killed; a process that left its group can hold it open.
_REAP_TIMEOUT_SECONDS = 2.0
# Output read after the script itself has exited, in case a background child it started is still printing.
_OUTPUT_DRAIN_GRACE_SECONDS = 2.0
# asyncio's ``Process.wait`` returns only once the output pipe is closed, which a background child can prevent,
# so the exit is found by polling ``returncode`` (pydantic-ai's ``LocalWorkspaceBackend`` does the same).
_EXIT_POLL_INTERVAL_SECONDS = 0.005
_MAX_SCRIPT_OUTPUT_BYTES = 50_000
_MAX_RESOURCE_BYTES = 1_000_000
_ARGUMENT_NAME = re.compile(r"[A-Za-z][A-Za-z0-9_-]*")
_SCRIPT_PARENTS = frozenset({".", "scripts"})
_SCRIPT_SUFFIXES = frozenset({".py", ".sh", ".bash", ".zsh"})
_IGNORED_SCRIPT_NAMES = frozenset({"__init__.py", "SKILL.md"})
_SHELL_INTERPRETERS: dict[str, list[str]] = {
    ".sh": ["sh"],
    ".bash": ["bash"],
    ".zsh": ["zsh"],
}


@dataclasses.dataclass
class SkillRunState:
    """The skill directories one run can reach, by skill name."""

    skill_dirs: dict[str, Path]


def _build_script_env(inherit_env: Sequence[str]) -> dict[str, str]:
    names = (*_INHERITED_ENV_NAMES, *inherit_env)
    return {name: os.environ[name] for name in names if name in os.environ}


def _strip_skill_dir_prefix(relative: str) -> str:
    for prefix in _SKILL_DIR_PREFIXES:
        if relative.startswith(prefix):
            return relative[len(prefix) :]
    return relative


def _is_excluded(relative: PurePosixPath, patterns: Sequence[str]) -> bool:
    # Case-folded because macOS and Windows filesystems serve ``.GIT/config`` as ``.git/config``.
    text = relative.as_posix().casefold()
    parts = [part.casefold() for part in relative.parts]
    return any(
        fnmatch.fnmatchcase(text, folded) or any(fnmatch.fnmatchcase(part, folded) for part in parts)
        for folded in (pattern.casefold() for pattern in patterns)
    )


def _resolve_skill_file(skill_dir: Path, relative: str, patterns: Sequence[str]) -> Path:
    """Return the file *relative* names inside *skill_dir*, or raise ``ModelRetry`` if it is not one."""
    if "\0" in relative:
        raise ModelRetry("The path contains a NUL character.")
    pure = PurePosixPath(_strip_skill_dir_prefix(relative))
    if not pure.parts or pure.is_absolute() or ".." in pure.parts:
        raise ModelRetry(
            f"{relative!r} is not a path inside the skill. Use a path relative to the skill directory."
        )
    if _is_excluded(pure, patterns):
        raise ModelRetry(f"{relative!r} is excluded from this skill's files.")
    root = skill_dir.resolve()
    resolved = (root / pure).resolve()
    if not resolved.is_relative_to(root):
        raise ModelRetry(f"{relative!r} resolves outside the skill directory.")
    if _is_excluded(PurePosixPath(resolved.relative_to(root).as_posix()), patterns):
        raise ModelRetry(f"{relative!r} is excluded from this skill's files.")
    if not resolved.is_file():
        raise ModelRetry(f"{relative!r} is not a file in the skill.")
    return resolved


def _read_resource_text(path: Path, resource: str) -> str:
    with path.open("rb") as handle:
        data = handle.read(_MAX_RESOURCE_BYTES + 1)
    if len(data) > _MAX_RESOURCE_BYTES:
        raise ModelRetry(f"{resource!r} is larger than {_MAX_RESOURCE_BYTES} bytes and cannot be read.")
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        raise ModelRetry(f"{resource!r} is not UTF-8 text.") from None


def _parse_resource(path: Path, text: str) -> Any:
    suffix = path.suffix.lower()
    if suffix == ".json":
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return text
    if suffix in (".yaml", ".yml"):
        try:
            return yaml.safe_load(text)
        except yaml.YAMLError:
            return text
    return text


def _resolve_shebang_command(script: Path) -> list[str] | None:
    try:
        with script.open("rb") as handle:
            first_line = handle.readline()
    except OSError:
        return None
    if not first_line.startswith(b"#!"):
        return None
    try:
        parts = shlex.split(first_line[2:].decode("utf-8", errors="ignore").strip())
    except ValueError:
        return None
    if parts and Path(parts[0]).name == "env":
        parts = [part for part in parts[1:] if not part.startswith("-")]
    if not parts:
        return None
    interpreter = parts[0]
    resolved = interpreter if Path(interpreter).is_absolute() else shutil.which(interpreter)
    if not resolved or not Path(resolved).exists():
        return None
    return [resolved, *parts[1:]]


def _build_script_command(script: Path) -> list[str]:
    shebang = _resolve_shebang_command(script)
    if shebang:
        return [*shebang, str(script)]
    suffix = script.suffix.lower()
    if suffix == ".py":
        return [sys.executable, str(script)]
    if suffix in _SHELL_INTERPRETERS:
        return [*_SHELL_INTERPRETERS[suffix], str(script)]
    return [str(script)]


def _build_script_args(args: Mapping[str, Any] | None) -> list[str]:
    argv: list[str] = []
    for key, value in (args or {}).items():
        if not _ARGUMENT_NAME.fullmatch(key):
            raise ModelRetry(
                "Argument names must start with a letter and contain only letters, digits, '-' and '_'."
            )
        if isinstance(value, bool):
            if value:
                argv.append(f"--{key}")
        elif isinstance(value, list):
            for item in value:
                argv.extend([f"--{key}", str(item)])
        elif value is not None:
            argv.extend([f"--{key}", str(value)])
    if any("\0" in arg for arg in argv):
        raise ModelRetry("Script arguments cannot contain a NUL character.")
    return argv


def _kill_process(process: asyncio.subprocess.Process) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (OSError, ValueError):
        with contextlib.suppress(OSError):
            process.kill()


class _SkillFilesToolset(FunctionToolset[Any]):
    def __init__(
        self,
        runs: Mapping[str, SkillRunState],
        *,
        resources: bool,
        scripts: bool,
        exclude_resources: Sequence[str],
        inherit_env: Sequence[str],
        max_retries: int | None,
    ) -> None:
        super().__init__(id=TOOLSET_ID, max_retries=validate_max_retries(max_retries))
        self._runs = runs
        self._inherit_env = tuple(inherit_env)
        self._exclude_patterns = (*DEFAULT_RESOURCE_EXCLUDES, *exclude_resources)
        if resources:
            self.add_function(self.read_skill_resource, takes_ctx=True)
        if scripts:
            self.add_function(self.run_skill_script, takes_ctx=True)

    def _get_skill_dir(self, ctx: RunContext[Any], skill_name: str) -> Path:
        state = self._runs.get(ctx.run_id or "")
        if state is None or skill_name not in state.skill_dirs:
            available = ", ".join(sorted(state.skill_dirs)) if state else "none"
            raise ModelRetry(f"Unknown skill {skill_name!r}. Available skills: {available}.")
        if skill_name not in ctx.loaded_capability_ids:
            raise ModelRetry(f"Load the skill first with load_capability(id={skill_name!r}).")
        return state.skill_dirs[skill_name]

    async def read_skill_resource(self, ctx: RunContext[Any], skill_name: str, resource: str) -> Any:
        """
        Read a file bundled with a loaded skill, such as a reference document or a template.

        Args:
            skill_name: The name of a skill already loaded with load_capability.
            resource: Path of the file relative to the skill directory. A leading ${SKILL_DIR}/
                names the skill directory itself.
        """
        path = _resolve_skill_file(self._get_skill_dir(ctx, skill_name), resource, self._exclude_patterns)
        text = await asyncio.to_thread(_read_resource_text, path, resource)
        return _parse_resource(path, text)

    async def run_skill_script(
        self,
        ctx: RunContext[Any],
        skill_name: str,
        script: str,
        args: dict[str, Any] | None = None,
    ) -> str:
        """
        Run a script from a loaded skill on the worker and return its combined output.

        Args:
            skill_name: The name of a skill already loaded with load_capability.
            script: Path of the script relative to the skill directory, for example scripts/run.py.
                A leading ${SKILL_DIR}/ names the skill directory itself.
            args: Named arguments passed as command-line flags: {"week": 42} becomes --week 42, true
                becomes a bare flag, and a list repeats the flag for each item.
                A name starts with a letter and contains only letters, digits, - and _.
        """
        skill_dir = self._get_skill_dir(ctx, skill_name)
        path = _resolve_skill_file(skill_dir, script, self._exclude_patterns)
        skill_root = skill_dir.resolve()
        if path.parent.relative_to(skill_root).as_posix() not in _SCRIPT_PARENTS:
            raise ModelRetry(f"{script!r} is not a script: scripts live in the skill root or in scripts/.")
        if path.name in _IGNORED_SCRIPT_NAMES or not (
            path.suffix.lower() in _SCRIPT_SUFFIXES or os.access(path, os.X_OK)
        ):
            raise ModelRetry(f"{script!r} is not a runnable script.")
        return await _run_script(
            path, skill_root, _build_script_args(args), _build_script_env(self._inherit_env)
        )


async def _run_script(script: Path, cwd: Path, argv: list[str], env: Mapping[str, str]) -> str:
    try:
        process = await asyncio.create_subprocess_exec(
            *_build_script_command(script),
            *argv,
            cwd=cwd,
            env=env,
            stdin=asyncio.subprocess.DEVNULL,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as exc:
        raise ModelRetry(f"Failed to start {script.name!r}: {exc}") from None
    output = bytearray()
    try:
        truncated = await asyncio.wait_for(_collect_output(process, output), timeout=_SCRIPT_TIMEOUT_SECONDS)
    except TimeoutError:
        await _reap_process(process)
        note = f"Script {script.name!r} timed out after {_SCRIPT_TIMEOUT_SECONDS:g} seconds and was killed."
        partial = _decode_output(output)
        return f"{partial}\n{note}" if partial else note
    except BaseException:
        _kill_process(process)
        raise
    text = _decode_output(output)
    if truncated:
        text = f"{text}\n[output truncated at {_MAX_SCRIPT_OUTPUT_BYTES} bytes and the script was killed]"
    elif process.returncode:
        text = f"{text}\n\nScript exited with code {process.returncode}".strip()
    return text or "(no output)"


def _decode_output(data: bytearray) -> str:
    return data[:_MAX_SCRIPT_OUTPUT_BYTES].decode("utf-8", errors="replace").strip()


async def _reap_process(process: asyncio.subprocess.Process) -> None:
    """
    Kill the script's process group and wait until its pipe is closed.

    When the script has already exited, this kills the background processes it left behind.
    Unread output keeps the pipe open, and ``Process.wait`` does not return, nor is the transport
    closed, until it is. So the output is read and dropped to EOF.
    """
    _kill_process(process)

    async def finish() -> None:
        stream = process.stdout
        while stream is not None and await stream.read(65536):
            pass
        await process.wait()

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(finish(), timeout=_REAP_TIMEOUT_SECONDS)


async def _wait_for_exit(process: asyncio.subprocess.Process) -> int:
    while (returncode := process.returncode) is None:  # noqa: ASYNC110
        await asyncio.sleep(_EXIT_POLL_INTERVAL_SECONDS)
    return returncode


async def _read_output(stream: asyncio.StreamReader | None, data: bytearray) -> None:
    while len(data) <= _MAX_SCRIPT_OUTPUT_BYTES and stream is not None:
        chunk = await stream.read(_MAX_SCRIPT_OUTPUT_BYTES + 1 - len(data))
        if not chunk:
            break
        data.extend(chunk)


async def _collect_output(process: asyncio.subprocess.Process, data: bytearray) -> bool:
    """
    Read the script's output into *data* up to the cap, and return whether it was cut off there.

    Returns once the script has exited and its output has been read for a short grace period, even if a
    background child still holds the pipe; then kills the process group, including such a child. A script
    that prints more than the cap is killed too. *data* is filled as the output arrives, so the caller
    still has what was read when the timeout cancels this.
    """
    reader = asyncio.create_task(_read_output(process.stdout, data))
    exit_waiter = asyncio.create_task(_wait_for_exit(process))
    try:
        done, _ = await asyncio.wait({reader, exit_waiter}, return_when=asyncio.FIRST_COMPLETED)
        if reader in done:
            if len(data) <= _MAX_SCRIPT_OUTPUT_BYTES:
                await exit_waiter
        else:
            await asyncio.wait({reader}, timeout=_OUTPUT_DRAIN_GRACE_SECONDS)
        if reader.done():
            reader.result()
    finally:
        # ``_reap_process`` reads the same stream, so the reader must be gone before it starts.
        for task in (reader, exit_waiter):
            task.cancel()
        await asyncio.gather(reader, exit_waiter, return_exceptions=True)
    await _reap_process(process)
    return len(data) > _MAX_SCRIPT_OUTPUT_BYTES


def build_skill_files_toolset(
    *,
    runs: Mapping[str, SkillRunState],
    resources: bool,
    scripts: bool,
    exclude_resources: Sequence[str],
    inherit_env: Sequence[str],
    max_retries: int | None,
) -> FunctionToolset[Any] | None:
    """
    Build the toolset serving *runs*, or ``None`` when both tools are switched off.

    Build it once and keep it: durable execution identifies a toolset by its id, and a run finds its
    skill directories in *runs* by ``ctx.run_id``.
    """
    if not resources and not scripts:
        return None
    return _SkillFilesToolset(
        runs,
        resources=resources,
        scripts=scripts,
        exclude_resources=exclude_resources,
        inherit_env=inherit_env,
        max_retries=max_retries,
    )
