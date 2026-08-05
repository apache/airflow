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
"""Development/e2e driver for one Airflow task attempt per Docker Sandbox."""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import stat
import tempfile
import time
from pathlib import Path
from typing import Any

from airflow.providers.common.sandbox.driver import SandboxDriver
from airflow.providers.common.sandbox.exceptions import SandboxInvalidHandleError
from airflow.providers.common.sandbox.models import (
    RecoveredSandbox,
    SandboxHandle,
    SandboxLaunchRequest,
    SandboxResult,
    SandboxState,
)
from airflow.providers.docker.sandbox.cli import AsyncSbxCli, SbxSandbox
from airflow.providers.docker.sandbox.exceptions import (
    DockerSandboxConfigurationError,
    DockerSandboxProtocolError,
)
from airflow.providers.docker.sandbox.models import (
    METADATA_FILENAME,
    SCRATCH_SCHEMA_VERSION,
    SPEC_FILENAME,
    STATUS_FILENAME,
    SUPERVISOR_MODULE,
    DockerSandboxDriverConfig,
    DockerSandboxHandle,
    DockerSandboxLaunchConfig,
    DockerSandboxMetadata,
    DockerSandboxStatus,
    DockerSandboxStatusState,
    sandbox_name_from_request_id,
)

_MAX_SCRATCH_DOCUMENT_BYTES = 1024 * 1024
_ACTIVE_SANDBOX_STATES = {"creating", "running", "starting"}


class DockerSandboxDriver(SandboxDriver):
    """
    Run Airflow workloads through the standalone Docker Sandboxes CLI.

    Docker Sandboxes does not currently provide the hard provider-enforced TTL required for a
    production sandbox driver. This implementation exists only for local development and e2e
    validation of the common executor contract. The task sandbox can also access its mounted
    scratch directory, so the status file is not a security boundary against malicious task code.
    """

    driver_id = "docker-sandbox"

    def __init__(self, config: DockerSandboxDriverConfig) -> None:
        self._config = config
        self._root = config.root_path
        self._cli = AsyncSbxCli(
            binary=config.sbx_binary,
            timeout_seconds=config.command_timeout_seconds,
        )

    async def health_check(self) -> None:
        self._ensure_scratch_root()
        await self._cli.health_check()

    async def launch(self, request: SandboxLaunchRequest) -> SandboxHandle:
        launch_config = DockerSandboxLaunchConfig.from_json(request.provider_config)
        self._validate_launch_request(request)
        request_dir = self._create_request_dir(request.request_id)
        spec_path = request_dir / SPEC_FILENAME
        status_path = request_dir / STATUS_FILENAME
        metadata_path = request_dir / METADATA_FILENAME
        self._write_new_json(
            spec_path,
            {
                "command": list(request.command),
                "env": request.env,
                "request_id": request.request_id,
                "schema_version": SCRATCH_SCHEMA_VERSION,
                "timeout_seconds": request.timeout_seconds,
                "workdir": request.workdir,
            },
        )

        sandbox_name = sandbox_name_from_request_id(request.request_id)
        create_args = ["--quiet", "--name", sandbox_name]
        if launch_config.cpus is not None:
            create_args.extend(("--cpus", str(launch_config.cpus)))
        if launch_config.memory is not None:
            create_args.extend(("--memory", launch_config.memory))
        create_args.extend(("--template", launch_config.template, "shell", str(request_dir)))
        await self._cli.create(tuple(create_args))

        deadline = time.monotonic() + self._config.acceptance_timeout_seconds
        sandbox = await self._wait_for_sandbox(sandbox_name, deadline=deadline)
        ref = DockerSandboxHandle(
            request_id=request.request_id,
            sandbox_name=sandbox.name,
            sandbox_id=sandbox.sandbox_id,
        )
        metadata = DockerSandboxMetadata(
            request_id=request.request_id,
            sandbox_name=sandbox.name,
            sandbox_id=sandbox.sandbox_id,
        )
        self._write_atomic_json(metadata_path, metadata.to_json())

        await self._cli.execute_detached(
            (
                "-d",
                sandbox_name,
                "python",
                "-m",
                SUPERVISOR_MODULE,
                "--spec",
                str(spec_path),
                "--status",
                str(status_path),
            )
        )
        await self._wait_for_acceptance(ref, deadline=deadline)
        return ref.to_common()

    def validate_handle(self, handle: SandboxHandle, *, request_id: str) -> None:
        ref = DockerSandboxHandle.from_common(handle)
        if ref.request_id != request_id:
            raise SandboxInvalidHandleError(
                "Docker Sandbox handle request ID does not match its execution reference"
            )

    async def get_status(self, handle: SandboxHandle) -> SandboxResult:
        ref = DockerSandboxHandle.from_common(handle)
        try:
            status = self._read_status(ref.request_id)
        except FileNotFoundError as error:
            raise DockerSandboxProtocolError(
                "Docker Sandbox status disappeared after task acceptance"
            ) from error
        sandbox = await self._cli.find_sandbox(ref.sandbox_name)
        if sandbox is not None and sandbox.sandbox_id != ref.sandbox_id:
            raise DockerSandboxProtocolError(
                "Docker Sandbox stable ID changed for the persisted sandbox name"
            )

        if status.state is DockerSandboxStatusState.SUCCEEDED:
            return SandboxResult(SandboxState.SUCCEEDED, exit_code=0)
        if status.state is DockerSandboxStatusState.FAILED:
            return SandboxResult(
                SandboxState.FAILED,
                exit_code=status.exit_code,
                message=status.message,
            )
        if sandbox is None:
            return SandboxResult(SandboxState.GONE, message="Docker Sandbox no longer exists")
        if sandbox.status not in _ACTIVE_SANDBOX_STATES:
            return SandboxResult(
                SandboxState.GONE,
                message=f"Docker Sandbox is {sandbox.status} while its supervisor reports running",
            )
        return SandboxResult(SandboxState.RUNNING)

    async def terminate(self, handle: SandboxHandle) -> None:
        ref = DockerSandboxHandle.from_common(handle)
        sandbox = await self._cli.find_sandbox(ref.sandbox_name)
        if sandbox is None:
            self._remove_request_dir(ref.request_id)
            return
        if sandbox.sandbox_id != ref.sandbox_id:
            raise DockerSandboxProtocolError(
                "refusing to remove a Docker Sandbox whose stable ID does not match the handle"
            )
        await self._remove_and_confirm_absent(ref.sandbox_name)
        self._remove_request_dir(ref.request_id)

    async def fence(self, request_id: str) -> None:
        sandbox_name = sandbox_name_from_request_id(request_id)
        if await self._cli.find_sandbox(sandbox_name) is not None:
            await self._remove_and_confirm_absent(sandbox_name)
        self._remove_request_dir(request_id)

    async def recover(self, request_id: str) -> RecoveredSandbox | None:
        try:
            metadata = DockerSandboxMetadata.from_json(
                self._read_json(self._request_dir(request_id) / METADATA_FILENAME)
            )
            if metadata.request_id != request_id:
                raise DockerSandboxProtocolError("Docker Sandbox metadata request ID does not match")
            status = self._read_status(request_id)
            sandbox = await self._cli.find_sandbox(metadata.sandbox_name)
            if sandbox is None or sandbox.sandbox_id != metadata.sandbox_id:
                raise DockerSandboxProtocolError(
                    "Docker Sandbox durable identity cannot be matched during recovery"
                )
            if (
                status.state is DockerSandboxStatusState.RUNNING
                and sandbox.status not in _ACTIVE_SANDBOX_STATES
            ):
                raise DockerSandboxProtocolError(
                    "Docker Sandbox is inactive while durable status reports running"
                )
        except (OSError, DockerSandboxConfigurationError, DockerSandboxProtocolError):
            await self.fence(request_id)
            return None

        handle = DockerSandboxHandle(
            request_id=metadata.request_id,
            sandbox_name=metadata.sandbox_name,
            sandbox_id=metadata.sandbox_id,
        )
        return RecoveredSandbox(handle=handle.to_common())

    async def close(self) -> None:
        """Release driver resources; the CLI adapter owns no persistent transport."""

    async def _wait_for_sandbox(self, sandbox_name: str, *, deadline: float) -> SbxSandbox:
        while True:
            if sandbox := await self._cli.find_sandbox(sandbox_name):
                return sandbox
            await self._sleep_until_next_poll(
                deadline,
                error_message="Docker Sandbox did not publish a stable ID before launch timeout",
            )

    async def _wait_for_acceptance(self, ref: DockerSandboxHandle, *, deadline: float) -> None:
        while True:
            try:
                status = self._read_status(ref.request_id)
            except FileNotFoundError:
                status = None
            sandbox = await self._cli.find_sandbox(ref.sandbox_name)
            if sandbox is None:
                raise DockerSandboxProtocolError("Docker Sandbox disappeared before task acceptance")
            if sandbox.sandbox_id != ref.sandbox_id:
                raise DockerSandboxProtocolError("Docker Sandbox stable ID changed before task acceptance")
            if status is not None:
                if (
                    status.state is DockerSandboxStatusState.RUNNING
                    and sandbox.status not in _ACTIVE_SANDBOX_STATES
                ):
                    raise DockerSandboxProtocolError("Docker Sandbox became inactive before task acceptance")
                return
            await self._sleep_until_next_poll(
                deadline,
                error_message="Docker Sandbox supervisor did not accept the task before launch timeout",
            )

    async def _sleep_until_next_poll(self, deadline: float, *, error_message: str) -> None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise DockerSandboxProtocolError(error_message)
        await asyncio.sleep(min(self._config.acceptance_poll_interval, remaining))

    async def _remove_and_confirm_absent(self, sandbox_name: str) -> None:
        await self._cli.remove(sandbox_name)
        if await self._cli.find_sandbox(sandbox_name) is not None:
            raise DockerSandboxProtocolError(
                f"Docker Sandbox {sandbox_name!r} still exists after forced removal"
            )

    def _ensure_scratch_root(self) -> None:
        try:
            self._root.mkdir(mode=0o700, parents=True, exist_ok=True)
            root_stat = self._root.lstat()
        except OSError as error:
            raise DockerSandboxConfigurationError(
                f"cannot prepare Docker Sandbox scratch root {self._root}: {error}"
            ) from error
        if not stat.S_ISDIR(root_stat.st_mode) or stat.S_ISLNK(root_stat.st_mode):
            raise DockerSandboxConfigurationError(
                "Docker Sandbox scratch root must be a directory, not a symbolic link"
            )
        if stat.S_IMODE(root_stat.st_mode) != 0o700:
            raise DockerSandboxConfigurationError("Docker Sandbox scratch root permissions must be 0700")
        if hasattr(os, "getuid") and root_stat.st_uid != os.getuid():
            raise DockerSandboxConfigurationError(
                "Docker Sandbox scratch root must be owned by the scheduler user"
            )

    def _create_request_dir(self, request_id: str) -> Path:
        self._ensure_scratch_root()
        request_dir = self._request_dir(request_id)
        try:
            request_dir.mkdir(mode=0o700)
            request_dir.chmod(0o700)
        except OSError as error:
            raise DockerSandboxConfigurationError(
                f"cannot create Docker Sandbox request directory for {request_id}: {error}"
            ) from error
        return request_dir

    def _request_dir(self, request_id: str) -> Path:
        sandbox_name_from_request_id(request_id)
        return self._root / request_id

    def _read_status(self, request_id: str) -> DockerSandboxStatus:
        value = self._read_json(self._request_dir(request_id) / STATUS_FILENAME)
        return DockerSandboxStatus.from_json(value, expected_request_id=request_id)

    @staticmethod
    def _read_json(path: Path) -> Any:
        file_stat = path.lstat()
        if not stat.S_ISREG(file_stat.st_mode) or stat.S_ISLNK(file_stat.st_mode):
            raise DockerSandboxProtocolError(f"scratch document {path.name} must be a regular file")
        if stat.S_IMODE(file_stat.st_mode) != 0o600:
            raise DockerSandboxProtocolError(f"scratch document {path.name} permissions must be 0600")
        if file_stat.st_size > _MAX_SCRATCH_DOCUMENT_BYTES:
            raise DockerSandboxProtocolError(f"scratch document {path.name} exceeds the size limit")
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags)
        try:
            with os.fdopen(descriptor, "rb", closefd=False) as stream:
                raw = stream.read(_MAX_SCRATCH_DOCUMENT_BYTES + 1)
        finally:
            os.close(descriptor)
        if len(raw) > _MAX_SCRATCH_DOCUMENT_BYTES:
            raise DockerSandboxProtocolError(f"scratch document {path.name} exceeds the size limit")
        try:
            return json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise DockerSandboxProtocolError(f"scratch document {path.name} is malformed") from error

    @staticmethod
    def _write_new_json(path: Path, value: Any) -> None:
        payload = DockerSandboxDriver._encode_json(value)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(path, flags, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
        finally:
            os.close(descriptor)

    @staticmethod
    def _write_atomic_json(path: Path, value: Any) -> None:
        payload = DockerSandboxDriver._encode_json(value)
        descriptor, temporary_name = tempfile.mkstemp(
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
        )
        temporary_path = Path(temporary_name)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb", closefd=False) as stream:
                stream.write(payload)
                stream.flush()
                os.fsync(stream.fileno())
            os.close(descriptor)
            descriptor = -1
            os.replace(temporary_path, path)
        finally:
            if descriptor >= 0:
                os.close(descriptor)
            temporary_path.unlink(missing_ok=True)

    @staticmethod
    def _encode_json(value: Any) -> bytes:
        try:
            payload = json.dumps(
                value,
                allow_nan=False,
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        except (TypeError, ValueError) as error:
            raise DockerSandboxConfigurationError(
                "Docker Sandbox scratch data must contain only JSON values"
            ) from error
        if len(payload) > _MAX_SCRATCH_DOCUMENT_BYTES:
            raise DockerSandboxConfigurationError("Docker Sandbox scratch document is too large")
        return payload

    def _remove_request_dir(self, request_id: str) -> None:
        request_dir = self._request_dir(request_id)
        try:
            path_stat = request_dir.lstat()
        except FileNotFoundError:
            return
        if stat.S_ISLNK(path_stat.st_mode) or not stat.S_ISDIR(path_stat.st_mode):
            request_dir.unlink()
            return
        shutil.rmtree(request_dir)

    @staticmethod
    def _validate_launch_request(request: SandboxLaunchRequest) -> None:
        if request.keep:
            raise DockerSandboxConfigurationError(
                "Docker Sandbox launches cannot be retained because the provider has no hard TTL"
            )
        if any("\0" in part for part in request.command):
            raise DockerSandboxConfigurationError("Docker Sandbox command arguments cannot contain NUL")
        if any(not key or "=" in key or "\0" in key or "\0" in value for key, value in request.env.items()):
            raise DockerSandboxConfigurationError("Docker Sandbox environment contains an invalid entry")
        if request.workdir is not None and (
            "\0" in request.workdir or not Path(request.workdir).is_absolute()
        ):
            raise DockerSandboxConfigurationError("Docker Sandbox workdir must be an absolute path or null")
