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
"""Run one argv workload and publish its state through an atomic scratch protocol."""

from __future__ import annotations

import argparse
import json
import os
import signal
import stat
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import UUID

_SCHEMA_VERSION = 1
_SPEC_FILENAME = "launch.json"
_STATUS_FILENAME = "status.json"
_MAX_SPEC_BYTES = 1024 * 1024
_MAX_MESSAGE_LENGTH = 4096


class SupervisorProtocolError(ValueError):
    """Raised when the host scratch protocol is invalid or unsafe."""


@dataclass(frozen=True)
class LaunchSpec:
    """Validated command inputs removed from scratch before the workload starts."""

    request_id: str
    command: tuple[str, ...]
    env: dict[str, str]
    workdir: str | None
    timeout_seconds: int


def _is_canonical_uuid(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    try:
        return str(UUID(value)) == value
    except ValueError:
        return False


def _validate_protocol_paths(spec_path: Path, status_path: Path) -> str:
    if not spec_path.is_absolute() or not status_path.is_absolute():
        raise SupervisorProtocolError("spec and status paths must be absolute")
    if spec_path.name != _SPEC_FILENAME or status_path.name != _STATUS_FILENAME:
        raise SupervisorProtocolError("spec and status filenames are invalid")
    if spec_path.parent != status_path.parent:
        raise SupervisorProtocolError("spec and status must use the same request directory")
    request_id = spec_path.parent.name
    if not _is_canonical_uuid(request_id):
        raise SupervisorProtocolError("request directory must be named with a canonical UUID")
    directory_stat = spec_path.parent.lstat()
    if not stat.S_ISDIR(directory_stat.st_mode) or stat.S_ISLNK(directory_stat.st_mode):
        raise SupervisorProtocolError("request path must be a directory, not a symbolic link")
    if stat.S_IMODE(directory_stat.st_mode) != 0o700:
        raise SupervisorProtocolError("request directory permissions must be 0700")
    if status_path.exists() or status_path.is_symlink():
        raise SupervisorProtocolError("status file must not exist before supervisor acceptance")
    return request_id


def _read_and_remove_spec(path: Path) -> Any:
    file_stat = path.lstat()
    if not stat.S_ISREG(file_stat.st_mode) or stat.S_ISLNK(file_stat.st_mode):
        raise SupervisorProtocolError("launch spec must be a regular file, not a symbolic link")
    if stat.S_IMODE(file_stat.st_mode) != 0o600:
        raise SupervisorProtocolError("launch spec permissions must be 0600")
    if file_stat.st_size > _MAX_SPEC_BYTES:
        raise SupervisorProtocolError("launch spec exceeds the size limit")

    flags = os.O_RDONLY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags)
    try:
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            raw = stream.read(_MAX_SPEC_BYTES + 1)
    finally:
        os.close(descriptor)
        path.unlink()
    if len(raw) > _MAX_SPEC_BYTES:
        raise SupervisorProtocolError("launch spec exceeds the size limit")
    try:
        return json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise SupervisorProtocolError("launch spec is not valid UTF-8 JSON") from error


def _parse_launch_spec(value: Any, *, expected_request_id: str) -> LaunchSpec:
    expected_fields = {
        "command",
        "env",
        "request_id",
        "schema_version",
        "timeout_seconds",
        "workdir",
    }
    if not isinstance(value, dict) or set(value) != expected_fields:
        raise SupervisorProtocolError("launch spec fields are invalid")
    if isinstance(value["schema_version"], bool) or value["schema_version"] != _SCHEMA_VERSION:
        raise SupervisorProtocolError("launch spec schema version is unsupported")
    if value["request_id"] != expected_request_id:
        raise SupervisorProtocolError("launch spec request ID does not match its directory")

    command = value["command"]
    if (
        not isinstance(command, list)
        or not command
        or not all(isinstance(part, str) and part and "\0" not in part for part in command)
    ):
        raise SupervisorProtocolError("launch command must contain non-empty argv strings")
    env = value["env"]
    if not isinstance(env, dict) or not all(
        isinstance(key, str)
        and key
        and "=" not in key
        and "\0" not in key
        and isinstance(item, str)
        and "\0" not in item
        for key, item in env.items()
    ):
        raise SupervisorProtocolError("launch env must be a valid string mapping")
    workdir = value["workdir"]
    if workdir is not None and (
        not isinstance(workdir, str) or not workdir or "\0" in workdir or not Path(workdir).is_absolute()
    ):
        raise SupervisorProtocolError("launch workdir must be an absolute path or null")
    timeout_seconds = value["timeout_seconds"]
    if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int) or timeout_seconds <= 0:
        raise SupervisorProtocolError("launch timeout_seconds must be a positive integer")
    return LaunchSpec(
        request_id=value["request_id"],
        command=tuple(command),
        env=dict(env),
        workdir=workdir,
        timeout_seconds=timeout_seconds,
    )


def _write_status(
    path: Path,
    *,
    request_id: str,
    state: str,
    exit_code: int | None,
    message: str | None,
) -> None:
    payload = json.dumps(
        {
            "exit_code": exit_code,
            "message": message,
            "request_id": request_id,
            "schema_version": _SCHEMA_VERSION,
            "state": state,
        },
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
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
        directory_descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        temporary_path.unlink(missing_ok=True)


def _kill_process_group(process: subprocess.Popen[Any]) -> None:
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except (OSError, ProcessLookupError):
        process.kill()
    process.wait()


def run_supervisor(spec_path: Path, status_path: Path) -> None:
    """Run the validated workload and publish RUNNING followed by one terminal state."""
    request_id = _validate_protocol_paths(spec_path, status_path)
    spec = _parse_launch_spec(
        _read_and_remove_spec(spec_path),
        expected_request_id=request_id,
    )
    _write_status(
        status_path,
        request_id=request_id,
        state="running",
        exit_code=None,
        message=None,
    )

    environment = os.environ.copy()
    environment.update(spec.env)
    try:
        process = subprocess.Popen(
            spec.command,
            cwd=spec.workdir,
            env=environment,
            shell=False,
            start_new_session=True,
        )
    except OSError as error:
        message = f"could not start task command: {error}"
        _write_status(
            status_path,
            request_id=request_id,
            state="failed",
            exit_code=127,
            message=message[:_MAX_MESSAGE_LENGTH],
        )
        return

    try:
        exit_code = process.wait(timeout=spec.timeout_seconds)
    except subprocess.TimeoutExpired:
        _kill_process_group(process)
        _write_status(
            status_path,
            request_id=request_id,
            state="failed",
            exit_code=124,
            message=f"task command timed out after {spec.timeout_seconds} seconds",
        )
        return

    succeeded = exit_code == 0
    _write_status(
        status_path,
        request_id=request_id,
        state="succeeded" if succeeded else "failed",
        exit_code=exit_code,
        message=None if succeeded else f"task command exited with code {exit_code}",
    )


def _create_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--status", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the scratch supervisor command-line entry point."""
    arguments = _create_argument_parser().parse_args(argv)
    try:
        run_supervisor(arguments.spec, arguments.status)
    except (OSError, SupervisorProtocolError) as error:
        print(f"Docker Sandbox supervisor refused launch: {error}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
