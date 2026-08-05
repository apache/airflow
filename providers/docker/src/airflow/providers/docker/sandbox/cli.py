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
"""Small, bounded asynchronous adapter for the standalone ``sbx`` CLI."""

from __future__ import annotations

import asyncio
import json
import os
import re
import signal
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, ClassVar

from airflow.providers.docker.sandbox.exceptions import (
    DockerSandboxCommandError,
    DockerSandboxProtocolError,
)

_MAX_COMMAND_OUTPUT_BYTES = 1024 * 1024
_MAX_ERROR_DETAIL_BYTES = 4096
_VERSION_PATTERN = re.compile(r"(?<![0-9])v?([0-9]+)\.([0-9]+)\.([0-9]+)(?![0-9])")


class _OutputLimitExceeded(Exception):
    """Internal signal used to stop a command with unbounded output."""


@dataclass(frozen=True)
class SbxSandbox:
    """Stable identity and lifecycle state returned by ``sbx ls --json``."""

    sandbox_id: str
    name: str
    status: str


def _get_compatible_field(value: dict[str, Any], *names: str) -> Any:
    present = [name for name in names if name in value]
    if len(present) != 1:
        raise DockerSandboxProtocolError(
            f"Docker Sandbox list item must contain exactly one of {', '.join(names)}"
        )
    return value[present[0]]


def parse_sandbox_list(raw: str) -> tuple[SbxSandbox, ...]:
    """Parse documented and legacy-wrapper JSON list shapes without guessing field values."""
    try:
        payload = json.loads(raw)
    except (TypeError, json.JSONDecodeError) as error:
        raise DockerSandboxProtocolError("sbx ls returned malformed JSON") from error
    if isinstance(payload, dict):
        if set(payload) != {"sandboxes"}:
            raise DockerSandboxProtocolError("sbx ls returned an unsupported JSON object")
        payload = payload["sandboxes"]
    if not isinstance(payload, list):
        raise DockerSandboxProtocolError("sbx ls JSON must be a list of sandboxes")

    sandboxes = []
    seen_ids: set[str] = set()
    seen_names: set[str] = set()
    for item in payload:
        if not isinstance(item, dict):
            raise DockerSandboxProtocolError("sbx ls list entries must be objects")
        sandbox_id = _get_compatible_field(item, "id", "ID")
        name = _get_compatible_field(item, "name", "Name")
        status = _get_compatible_field(item, "status", "Status")
        if not isinstance(sandbox_id, str) or not sandbox_id or "\0" in sandbox_id:
            raise DockerSandboxProtocolError("sbx ls returned an invalid stable sandbox ID")
        if not isinstance(name, str) or not name or "\0" in name:
            raise DockerSandboxProtocolError("sbx ls returned an invalid sandbox name")
        if not isinstance(status, str) or not status.strip() or "\0" in status:
            raise DockerSandboxProtocolError("sbx ls returned an invalid sandbox status")
        if sandbox_id in seen_ids or name in seen_names:
            raise DockerSandboxProtocolError("sbx ls returned duplicate sandbox identity")
        seen_ids.add(sandbox_id)
        seen_names.add(name)
        sandboxes.append(SbxSandbox(sandbox_id=sandbox_id, name=name, status=status.strip().lower()))
    return tuple(sandboxes)


async def _read_limited(stream: asyncio.StreamReader, *, limit: int) -> bytes:
    chunks = []
    size = 0
    while chunk := await stream.read(min(65536, limit - size + 1)):
        size += len(chunk)
        if size > limit:
            raise _OutputLimitExceeded
        chunks.append(chunk)
    return b"".join(chunks)


class AsyncSbxCli:
    """Invoke ``sbx`` without a shell, unbounded buffers, or secret-bearing arguments."""

    minimum_version: ClassVar[tuple[int, int, int]] = (0, 35, 0)

    def __init__(self, *, binary: str, timeout_seconds: float) -> None:
        self._binary = binary
        self._timeout_seconds = timeout_seconds

    async def health_check(self) -> None:
        """Require a compatible CLI and a reachable sandbox daemon."""
        version_output = await self._run("version")
        matches = _VERSION_PATTERN.findall(version_output)
        if not matches:
            raise DockerSandboxProtocolError("sbx version returned no semantic version")
        versions = [tuple(int(part) for part in match) for match in matches]
        if incompatible := [version for version in versions if version < self.minimum_version]:
            minimum = ".".join(str(part) for part in self.minimum_version)
            actual = ", ".join(".".join(str(part) for part in version) for version in incompatible)
            raise DockerSandboxCommandError(f"all sbx components must be {minimum} or newer; found {actual}")
        await self.list_sandboxes()

    async def list_sandboxes(self) -> tuple[SbxSandbox, ...]:
        """Return validated sandbox identities from the control plane."""
        return parse_sandbox_list(await self._run("ls", "--json"))

    async def find_sandbox(self, name: str) -> SbxSandbox | None:
        """Find a sandbox by its deterministic exact name."""
        return next((sandbox for sandbox in await self.list_sandboxes() if sandbox.name == name), None)

    async def create(self, args: tuple[str, ...]) -> None:
        """Create one sandbox using already validated non-sensitive arguments."""
        await self._run("create", *args)

    async def execute_detached(self, args: tuple[str, ...]) -> None:
        """Start the scratch supervisor in a sandbox."""
        await self._run("exec", *args)

    async def remove(self, name: str) -> None:
        """Forcibly remove one sandbox without an interactive prompt."""
        await self._run("rm", "--force", name)

    async def _run(self, *args: str) -> str:
        subcommand = args[0] if args else "command"
        try:
            process = await asyncio.create_subprocess_exec(
                self._binary,
                *args,
                stdin=asyncio.subprocess.DEVNULL,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                start_new_session=os.name == "posix",
            )
        except OSError as error:
            raise DockerSandboxCommandError(f"could not start sbx {subcommand}: {error}") from error

        if process.stdout is None or process.stderr is None:
            await self._stop_process(process, ())
            raise DockerSandboxCommandError(f"sbx {subcommand} did not expose captured output")

        wait_task = asyncio.create_task(process.wait())
        stdout_task = asyncio.create_task(_read_limited(process.stdout, limit=_MAX_COMMAND_OUTPUT_BYTES))
        stderr_task = asyncio.create_task(_read_limited(process.stderr, limit=_MAX_COMMAND_OUTPUT_BYTES))
        tasks = (wait_task, stdout_task, stderr_task)
        try:
            return_code, stdout, stderr = await asyncio.wait_for(
                asyncio.gather(*tasks), timeout=self._timeout_seconds
            )
        except TimeoutError as error:
            await self._stop_process(process, tasks)
            raise DockerSandboxCommandError(
                f"sbx {subcommand} exceeded the {self._timeout_seconds:g} second command timeout"
            ) from error
        except _OutputLimitExceeded as error:
            await self._stop_process(process, tasks)
            raise DockerSandboxCommandError(
                f"sbx {subcommand} exceeded the {_MAX_COMMAND_OUTPUT_BYTES} byte output limit"
            ) from error
        except asyncio.CancelledError:
            await asyncio.shield(self._stop_process(process, tasks))
            raise

        if return_code != 0:
            detail = stderr[:_MAX_ERROR_DETAIL_BYTES].decode(errors="replace").strip()
            suffix = f": {detail}" if detail else ""
            raise DockerSandboxCommandError(f"sbx {subcommand} failed with exit code {return_code}{suffix}")
        try:
            return stdout.decode()
        except UnicodeDecodeError as error:
            raise DockerSandboxProtocolError(f"sbx {subcommand} returned non-UTF-8 output") from error

    @staticmethod
    async def _stop_process(
        process: asyncio.subprocess.Process,
        tasks: tuple[asyncio.Task[Any], ...],
    ) -> None:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        if process.returncode is not None:
            await process.wait()
            return
        if os.name == "posix":
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
        else:
            process.terminate()
        try:
            await asyncio.wait_for(process.wait(), timeout=1.0)
            return
        except TimeoutError:
            pass
        if os.name == "posix":
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        else:
            process.kill()
        await process.wait()
