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
"""Toolset giving an agent shell and file access inside an isolated sandbox, off the worker."""

from __future__ import annotations

import asyncio
import logging
import math
from typing import TYPE_CHECKING, Any

from pydantic_ai.exceptions import ModelRetry
from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from typing_extensions import Self

from airflow.providers.common.ai.sandbox.base import (
    SandboxError,
    SandboxFileTooLargeError,
    SandboxSpec,
    SandboxTerminalError,
    _validate_positive_finite,
)
from airflow.providers.common.ai.sandbox.output import (
    format_size,
    render_file_window,
    truncate_output,
)
from airflow.providers.common.ai.utils.tool_definition import (
    build_args_validator,
    code_arg_kwargs,
    return_schema_kwargs,
)

if TYPE_CHECKING:
    from pydantic_ai._run_context import RunContext

    from airflow.providers.common.ai.sandbox.base import SandboxBackend

log = logging.getLogger(__name__)

RUN_COMMAND = "run_command"
READ_FILE = "read_file"
WRITE_FILE = "write_file"
LIST_DIRECTORY = "list_directory"

_SCHEMAS: dict[str, dict[str, Any]] = {
    RUN_COMMAND: {
        "type": "object",
        "properties": {
            "command": {"type": "string", "description": "Shell command to run."},
            "timeout_seconds": {
                "type": ["number", "null"],
                "description": "Maximum seconds to wait. Defaults to the configured timeout.",
            },
        },
        "required": ["command"],
    },
    READ_FILE: {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Path to the file inside the sandbox."},
            "offset": {
                "type": ["integer", "null"],
                "description": "Line number to start reading from (1-indexed).",
            },
            "limit": {"type": ["integer", "null"], "description": "Maximum number of lines to read."},
        },
        "required": ["path"],
    },
    WRITE_FILE: {
        "type": "object",
        "properties": {
            "path": {"type": "string", "description": "Path to the file inside the sandbox."},
            "content": {"type": "string", "description": "Text to write."},
        },
        "required": ["path", "content"],
    },
    LIST_DIRECTORY: {
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "description": "Directory to list. Defaults to the working directory.",
            },
        },
        "required": [],
    },
}

_DESCRIPTIONS = {
    RUN_COMMAND: (
        "Run a shell command inside an isolated sandbox and return its output. Pipes, "
        "redirection, && and globs work. A non-zero exit is reported, not raised, so read "
        "stderr and fix your command. The sandbox is separate from the Airflow worker and "
        "is not given Airflow's connections, variables, or environment."
    ),
    READ_FILE: (
        "Read a text file from the sandbox. Long files are truncated and the result tells "
        "you the next offset to continue from."
    ),
    WRITE_FILE: "Write text to a file in the sandbox, creating parent directories as needed.",
    LIST_DIRECTORY: "List the entries in a sandbox directory. Directories are shown with a trailing slash.",
}


class SandboxToolset(AbstractToolset[Any]):
    """
    Give an agent shell and file access inside a disposable sandbox, off the Airflow worker.

    Exposes four tools -- ``run_command``, ``read_file``, ``write_file`` and
    ``list_directory`` -- against a sandbox provisioned by the given
    :class:`~airflow.providers.common.ai.sandbox.SandboxBackend`. The same four
    names and shapes are what pydantic-ai's own sandbox capabilities use, so a
    model that has seen one already knows this one.

    **What the boundary covers.** Only what these tools do runs in the sandbox.
    The agent loop, the LLM calls, and every other toolset on the same agent
    still run in the Airflow worker process with its credentials. This contains
    model-written code; it does not contain the agent. See the toolsets
    documentation for the full picture of which boundary protects what.

    The sandbox is created lazily on the first tool call, shared by every call
    within one agent run, and destroyed when that run ends. A run that never
    calls a tool never provisions one. Files persist between calls in a run;
    each ``run_command`` is a fresh shell, so shell variables do not.

    A non-zero exit or a timeout is normal tool output -- the model reads it and
    corrects itself. A recoverable sandbox failure becomes a bounded retry. Only
    a terminal failure (credentials rejected, daemon unreachable) fails the task,
    so Airflow's own retry handles it.

    :param backend: Backend that provisions and drives the sandbox.
    :param spec: What to provision the sandbox with -- environment variables and
        network policy. Defaults to no environment and no egress.
    :param default_command_timeout: Seconds allowed for a ``run_command`` call
        when the model does not ask for one. Default ``60``.
    :param max_command_timeout: Hard ceiling in seconds for any single command,
        including a model-supplied ``timeout_seconds``. Default ``300``.
    :param max_output_lines: Maximum lines retained per output stream or file
        read. Default ``2000``.
    :param max_output_bytes: Maximum bytes retained per output stream or file
        read. Default 50 KiB. Whichever cap is reached first wins.
    :param max_read_bytes: Largest file ``read_file`` will transfer. Default
        5 MiB; larger files are refused with a hint to slice them in the shell.
    :param tool_prefix: Prefix for the four tool names, e.g. ``"local"`` gives
        ``local_run_command``. Set this when one agent has more than one
        ``SandboxToolset``, since duplicate tool names are rejected.
    """

    def __init__(
        self,
        backend: SandboxBackend,
        *,
        spec: SandboxSpec | None = None,
        default_command_timeout: float = 60.0,
        max_command_timeout: float = 300.0,
        max_output_lines: int = 2000,
        max_output_bytes: int = 50 * 1024,
        max_read_bytes: int = 5 * 1024 * 1024,
        tool_prefix: str = "",
    ) -> None:
        _validate_positive_finite(default_command_timeout, "default_command_timeout")
        _validate_positive_finite(max_command_timeout, "max_command_timeout")
        _validate_positive_finite(max_output_lines, "max_output_lines")
        _validate_positive_finite(max_output_bytes, "max_output_bytes")
        _validate_positive_finite(max_read_bytes, "max_read_bytes")
        if default_command_timeout > max_command_timeout:
            raise ValueError(
                f"default_command_timeout ({default_command_timeout}) must not exceed "
                f"max_command_timeout ({max_command_timeout})."
            )
        if tool_prefix and not tool_prefix.isidentifier():
            # The prefixed names are rendered as Python function signatures under
            # code mode, so a name that is not an identifier breaks there.
            raise ValueError(f"tool_prefix must be a valid Python identifier, got {tool_prefix!r}.")
        self._backend = backend
        # Never None: the documented default is "no environment, no egress", and a
        # backend reads None as "no requirements stated". Passing None through would
        # silently skip the contract check and hand back an unrestricted sandbox,
        # which is the opposite of what the default promises.
        self._spec = spec if spec is not None else SandboxSpec()
        self._default_command_timeout = default_command_timeout
        self._max_command_timeout = max_command_timeout
        self._max_output_lines = int(max_output_lines)
        self._max_output_bytes = int(max_output_bytes)
        self._max_read_bytes = int(max_read_bytes)
        self._tool_prefix = tool_prefix
        self._sandbox: str | None = None
        self._create_task: asyncio.Task[str] | None = None

    @property
    def id(self) -> str:
        suffix = f"-{self._tool_prefix}" if self._tool_prefix else ""
        return f"sandbox-{self._backend.name}{suffix}"

    def _tool_name(self, base: str) -> str:
        return f"{self._tool_prefix}_{base}" if self._tool_prefix else base

    def _base_name(self, tool_name: str) -> str | None:
        if not self._tool_prefix:
            return tool_name if tool_name in _SCHEMAS else None
        prefix = f"{self._tool_prefix}_"
        if not tool_name.startswith(prefix):
            return None
        base = tool_name[len(prefix) :]
        return base if base in _SCHEMAS else None

    async def for_run(self, ctx: RunContext[Any]) -> AbstractToolset[Any]:
        # pydantic-ai shares one toolset instance across runs, but each run holds
        # its own sandbox in ``_sandbox``/``_create_task``. Hand every run a fresh
        # instance so concurrent runs never share a sandbox or destroy each
        # other's. The backend keys all state by unique sandbox handle, so
        # sharing the backend itself is safe. ``type(self)`` so a subclass does
        # not silently degrade to this class on every run.
        return type(self)(
            self._backend,
            spec=self._spec,
            default_command_timeout=self._default_command_timeout,
            max_command_timeout=self._max_command_timeout,
            max_output_lines=self._max_output_lines,
            max_output_bytes=self._max_output_bytes,
            max_read_bytes=self._max_read_bytes,
            tool_prefix=self._tool_prefix,
        )

    async def __aenter__(self) -> Self:
        # The sandbox is provisioned lazily on first use, not here: a durable
        # replay that only serves cached tool results must not provision one, and
        # nothing leaks if the run fails before any tool executes.
        return self

    async def __aexit__(self, *args: Any) -> bool | None:
        sandbox = self._sandbox
        # Clear first: re-entering the same instance (HITL regenerate_with_feedback
        # does) must never reuse a sandbox whose cleanup was attempted.
        self._sandbox = None
        if sandbox is None:
            return None
        try:
            await asyncio.to_thread(self._backend.destroy, sandbox)
        except Exception:
            # The model work is finished and paid for by this point, so a teardown
            # blip must not turn a successful run into a task failure. Log loudly
            # and let the backend's own backstop (a server-side TTL, or an
            # operator sweep) reclaim it.
            log.warning(
                "Failed to destroy sandbox %s on backend %s; it may need manual cleanup",
                sandbox,
                self._backend.name,
                exc_info=True,
            )
        return None

    async def _ensure_sandbox(self) -> str:
        if self._sandbox is not None:
            return self._sandbox
        if self._create_task is None:
            self._create_task = asyncio.create_task(asyncio.to_thread(self._backend.create, spec=self._spec))
        create_task = self._create_task
        try:
            sandbox = await asyncio.shield(create_task)
        except asyncio.CancelledError:
            # A thread cannot be cancelled. Wait until it publishes the handle so
            # __aexit__ can destroy a sandbox created during cancellation.
            self._sandbox = await create_task
            raise
        else:
            self._sandbox = sandbox
            return sandbox
        finally:
            if self._create_task is create_task:
                self._create_task = None

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        tools: dict[str, ToolsetTool[Any]] = {}
        for base, schema in _SCHEMAS.items():
            # sequential=True: every tool shares one sandbox and later calls
            # depend on files earlier ones wrote, so they must not interleave.
            # return_schema "string": each returns text, so code mode renders
            # `-> str` rather than `-> Any`.
            # run_command additionally carries code_arg metadata so code mode
            # leaves it native instead of folding a shell surface into run_code.
            extra = code_arg_kwargs("command", "shell") if base == RUN_COMMAND else {}
            name = self._tool_name(base)
            tool_def = ToolDefinition(
                name=name,
                description=_DESCRIPTIONS[base],
                parameters_json_schema=schema,
                sequential=True,
                **return_schema_kwargs({"type": "string"}),
                **extra,
            )
            tools[name] = ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=2,
                args_validator=build_args_validator(schema),
            )
        return tools

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        base = self._base_name(name)
        if base is None:
            raise ValueError(f"Unknown tool: {name!r}")
        # Backend calls are synchronous and a command can take minutes, so offload
        # them to a thread instead of blocking the event loop.
        sandbox = await self._ensure_sandbox()
        try:
            if base == RUN_COMMAND:
                return await self._run_command(sandbox, tool_args)
            if base == READ_FILE:
                return await self._read_file(sandbox, tool_args)
            if base == WRITE_FILE:
                return await self._write_file(sandbox, tool_args)
            return await self._list_directory(sandbox, tool_args)
        except SandboxTerminalError:
            # Retrying cannot help: the sandbox is gone or the credentials are
            # bad. Fail the task so Airflow retries the whole thing.
            raise
        except SandboxFileTooLargeError as e:
            raise ModelRetry(
                f"{e.path!r} is {format_size(e.size_bytes)}, over the "
                f"{format_size(e.max_bytes)} read limit. Read just the part you need with a "
                "shell command instead (e.g. head, tail, sed -n, or grep)."
            ) from e
        except SandboxError as e:
            raise ModelRetry(f"The {name} tool failed: {e}") from e

    def _command_timeout(self, requested: float | None) -> float:
        if requested is None:
            return self._default_command_timeout
        if not math.isfinite(requested) or requested <= 0:
            # Reject rather than silently clamping: a surprise "[timed out after
            # 1s]" would hide the model's own mistake from it.
            raise ModelRetry(f"timeout_seconds must be greater than 0, got {requested}.")
        return min(requested, self._max_command_timeout)

    async def _run_command(self, sandbox: str, tool_args: dict[str, Any]) -> str:
        timeout = self._command_timeout(tool_args.get("timeout_seconds"))
        result = await asyncio.to_thread(
            self._backend.run_command,
            sandbox,
            tool_args["command"],
            timeout=timeout,
            max_output_bytes=self._max_output_bytes,
        )
        if result.sandbox_terminated:
            self._sandbox = None
        # Truncate each stream separately and attach its label afterwards, so the
        # markers always survive and a large stderr cannot crowd out stdout.
        parts: list[str] = []
        if result.stdout:
            parts.append(f"[stdout]\n{self._truncate(result.stdout, result.stdout_truncated)}")
        if result.stderr:
            parts.append(f"[stderr]\n{self._truncate(result.stderr, result.stderr_truncated)}")
        output = "\n".join(parts) if parts else "(no output)"
        if result.timed_out:
            note = f"[timed out after {timeout:g}s]"
            if result.sandbox_terminated:
                note += " [sandbox was replaced; files from earlier calls are gone]"
            return f"{output}\n{note}"
        if result.exit_code:
            return f"{output}\n[exit code: {result.exit_code}]"
        return output

    def _truncate(self, text: str, already_truncated: bool) -> str:
        return truncate_output(
            text,
            max_lines=self._max_output_lines,
            max_bytes=self._max_output_bytes,
            already_truncated=already_truncated,
        )

    async def _read_file(self, sandbox: str, tool_args: dict[str, Any]) -> str:
        data = await asyncio.to_thread(
            self._backend.read_file,
            sandbox,
            tool_args["path"],
            max_bytes=self._max_read_bytes,
        )
        return render_file_window(
            data,
            offset=tool_args.get("offset"),
            limit=tool_args.get("limit"),
            max_lines=self._max_output_lines,
            max_bytes=self._max_output_bytes,
        )

    async def _write_file(self, sandbox: str, tool_args: dict[str, Any]) -> str:
        path = tool_args["path"]
        try:
            data = tool_args["content"].encode("utf-8")
        except UnicodeEncodeError as e:
            # Reachable when a provider's pre-parsed tool arguments carry an
            # unpaired surrogate. A model mistake, so retry rather than fail.
            raise ModelRetry(
                "content contains characters that cannot be encoded as UTF-8 (unpaired surrogates)."
            ) from e
        await asyncio.to_thread(self._backend.write_file, sandbox, path, data)
        return f"Wrote {len(data)} bytes to {path!r}."

    async def _list_directory(self, sandbox: str, tool_args: dict[str, Any]) -> str:
        path = tool_args.get("path") or "."
        entries = await asyncio.to_thread(self._backend.list_directory, sandbox, path)
        if not entries:
            return "(empty)"
        # Sort by name before adding the "/" suffix so directories keep plain name
        # order ("/" sorts after "-" and ".", which would misplace suffixed names).
        listing = "\n".join(f"{name}/" if is_dir else name for name, is_dir in sorted(entries))
        # Bounded like every other tool result: an unpacked dataset or a
        # node_modules is tens of thousands of names, which would blow the
        # model's context mid-run after everything before it has been paid for.
        return truncate_output(
            listing,
            max_lines=self._max_output_lines,
            max_bytes=self._max_output_bytes,
        )
