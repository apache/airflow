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
"""Toolset that executes agent-written Python in an isolated sandbox, off the worker."""

from __future__ import annotations

import asyncio
import json
from typing import TYPE_CHECKING, Any

from pydantic_ai.tools import ToolDefinition
from pydantic_ai.toolsets.abstract import AbstractToolset, ToolsetTool
from pydantic_core import SchemaValidator, core_schema
from typing_extensions import Self

from airflow.providers.common.ai.sandbox.base import _validate_positive_finite
from airflow.providers.common.ai.utils.tool_definition import return_schema_kwargs

if TYPE_CHECKING:
    from pydantic_ai._run_context import RunContext

    from airflow.providers.common.ai.sandbox.base import SandboxBackend

# Deliberately not "run_code": that name is reserved by the Monty code_mode
# meta-tool, and the pinned pydantic-ai-harness rejects a second tool claiming
# it. A distinct name lets this toolset and code_mode coexist.
_TOOL_NAME = "run_python_in_sandbox"

_RUN_CODE_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "code": {"type": "string", "description": "Python code to execute."},
    },
    "required": ["code"],
}

# Must stay in sync with _RUN_CODE_SCHEMA. A ValidationError raised here becomes
# a retry prompt (honoring max_retries) instead of a KeyError in call_tool that
# would fail the whole task.
_RUN_CODE_VALIDATOR = SchemaValidator(
    core_schema.typed_dict_schema(
        {"code": core_schema.typed_dict_field(core_schema.str_schema())},
        extra_behavior="ignore",
    )
)

_RUN_CODE_DESCRIPTION = (
    "Execute Python code in an isolated sandbox and return a JSON object with "
    "exit_code, stdout, stderr, timed_out, and optionally truncated or "
    "sandbox_terminated. Airflow does not inject its context, connections, or "
    "worker environment. Each call runs a fresh interpreter, but files written "
    "by earlier calls persist unless sandbox_terminated is true. Print anything "
    "you need to see."
)


class SandboxToolset(AbstractToolset[Any]):
    """
    Toolset that executes agent-written Python in an isolated sandbox.

    Provides one tool, ``run_python_in_sandbox``. Unlike ``code_mode`` — whose
    ``run_code`` executes in-process on the worker via the Monty interpreter —
    this ships the code to a disposable sandbox provisioned by the given
    :class:`~airflow.providers.common.ai.sandbox.SandboxBackend`. Airflow does
    not inject its context, connections, credentials, or worker environment;
    custom images and hosted backends can still provide their own credentials.

    The sandbox is created lazily on the first ``run_python_in_sandbox`` call, shared by
    every call within the agent run, and destroyed when the run ends. A run
    that never calls the tool never provisions one.

    A nonzero exit code or a timeout is normal tool output — the model reads
    ``stderr`` and fixes its own code. A backend can terminate and transparently
    replace the sandbox when that is required to stop timed-out code. Only
    infrastructure exceptions from the backend (daemon unreachable, API
    failure) propagate and fail the task.

    :param backend: Sandbox backend that provisions and runs the sandbox, e.g.
        :class:`~airflow.providers.common.ai.sandbox.SbxSandboxBackend` or
        :class:`~airflow.providers.common.ai.sandbox.IsloSandboxBackend`.
    :param timeout: Timeout in seconds for a single ``run_python_in_sandbox`` call.
        Default ``300``.
    :param python_command: Python executable inside the sandbox.
        Default ``"python3"``.
    """

    def __init__(
        self,
        backend: SandboxBackend,
        *,
        timeout: float = 300.0,
        python_command: str = "python3",
    ) -> None:
        _validate_positive_finite(timeout, "timeout")
        if not python_command:
            raise ValueError("python_command must not be empty.")
        self._backend = backend
        self._timeout = timeout
        self._python_command = python_command
        self._sandbox: str | None = None
        self._create_task: asyncio.Task[str] | None = None

    @property
    def id(self) -> str:
        return f"sandbox-{self._backend.name}"

    async def for_run(self, ctx: RunContext[Any]) -> AbstractToolset[Any]:
        # Per-run isolation: pydantic-ai shares one toolset instance across runs,
        # but each run holds its own sandbox in ``_sandbox``/``_create_task``.
        # Hand every run a fresh instance so concurrent runs never share a
        # sandbox or destroy each other's. The backend keys all state by unique
        # sandbox name, so sharing it across runs is safe.
        return SandboxToolset(self._backend, timeout=self._timeout, python_command=self._python_command)

    async def __aenter__(self) -> Self:
        # The sandbox is provisioned lazily in call_tool, not here: a durable
        # replay that only serves cached tool results must not provision one,
        # and nothing leaks if the run fails before any tool executes.
        return self

    async def __aexit__(self, *args: Any) -> bool | None:
        if self._sandbox is not None:
            try:
                await asyncio.to_thread(self._backend.destroy, self._sandbox)
            finally:
                # Re-entering the same instance (HITL regenerate_with_feedback
                # does) must never reuse a sandbox whose cleanup was attempted.
                self._sandbox = None
        return None

    async def _ensure_sandbox(self) -> str:
        if self._sandbox is not None:
            return self._sandbox
        if self._create_task is None:
            self._create_task = asyncio.create_task(asyncio.to_thread(self._backend.create))
        create_task = self._create_task
        try:
            sandbox = await asyncio.shield(create_task)
        except asyncio.CancelledError:
            # A thread cannot be cancelled. Wait until it publishes the handle
            # so __aexit__ can destroy a sandbox created during cancellation.
            self._sandbox = await create_task
            raise
        else:
            self._sandbox = sandbox
            return sandbox
        finally:
            if self._create_task is create_task:
                self._create_task = None

    async def get_tools(self, ctx: RunContext[Any]) -> dict[str, ToolsetTool[Any]]:
        # sequential=True: every call shares one sandbox and may depend on
        # state left by earlier calls — they must not run concurrently.
        # return_schema is "string": the tool returns a JSON-encoded string
        # (json.dumps), so code mode renders `-> str` instead of `-> Any`.
        tool_def = ToolDefinition(
            name=_TOOL_NAME,
            description=_RUN_CODE_DESCRIPTION,
            parameters_json_schema=_RUN_CODE_SCHEMA,
            sequential=True,
            **return_schema_kwargs({"type": "string"}),
        )
        return {
            _TOOL_NAME: ToolsetTool(
                toolset=self,
                tool_def=tool_def,
                max_retries=2,
                args_validator=_RUN_CODE_VALIDATOR,
            )
        }

    async def call_tool(
        self,
        name: str,
        tool_args: dict[str, Any],
        ctx: RunContext[Any],
        tool: ToolsetTool[Any],
    ) -> Any:
        if name != _TOOL_NAME:
            raise ValueError(f"Unknown tool: {name!r}")
        # Backend calls are synchronous and a sandbox run can take minutes, so
        # offload them to a thread instead of blocking the event loop (unlike
        # SQLToolset, whose queries are expected to be short).
        sandbox = await self._ensure_sandbox()
        result = await asyncio.to_thread(
            self._backend.run,
            sandbox,
            [self._python_command, "-c", tool_args["code"]],
            timeout=self._timeout,
        )
        if result.sandbox_terminated:
            self._sandbox = None
        payload: dict[str, Any] = {
            "exit_code": result.exit_code,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "timed_out": result.timed_out,
        }
        if result.truncated:
            payload["truncated"] = True
        if result.sandbox_terminated:
            payload["sandbox_terminated"] = True
        # A nonzero exit or a timeout is still a normal return: a failed *user
        # code* run is valid tool output, not a tool failure — the model reads
        # stderr and fixes its own code.
        return json.dumps(payload)
