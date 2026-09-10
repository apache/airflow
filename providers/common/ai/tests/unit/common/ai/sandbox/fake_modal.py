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
A stand-in for the ``modal`` SDK, faithful in the ways the backend depends on.

The backend's error handling turns on ``isinstance`` checks whose ordering only matters
because of how Modal's own classes are related, so the hierarchy here mirrors
``modal==1.5.5`` exactly: ``ConflictError`` derives from ``InvalidError``,
``SandboxTimeoutError`` and ``ExecTimeoutError`` share a ``TimeoutError`` parent while
meaning opposite things, and every ``SandboxFilesystem*Error`` derives from
``SandboxFilesystemError`` and from nothing else, so a missing file is never mistaken for
a missing sandbox. A fake that flattened any of that would let a real bug through.

What it cannot prove, and what the system test therefore has to: that a deadline arrives
as returncode -1, that the filesystem API rejects relative paths, that ``write_bytes``
replaces a symlink, and that ``terminate`` is not synchronous.
"""

from __future__ import annotations

import types
from typing import Any


class Error(Exception):
    """Root of Modal's error hierarchy."""


class TimeoutError(Error):
    pass


class ExecTimeoutError(TimeoutError):
    pass


class SandboxTimeoutError(TimeoutError):
    pass


class SandboxTerminatedError(Error):
    pass


class ConnectionError(Error):
    pass


class InvalidError(Error):
    pass


class ConflictError(InvalidError):
    pass


class NotFoundError(Error):
    pass


class AuthError(Error):
    pass


class PermissionDeniedError(Error):
    """A sibling of AuthError in the real SDK, not a subclass, which is the point."""


class SandboxFilesystemError(Error):
    pass


class SandboxFilesystemNotFoundError(SandboxFilesystemError):
    pass


class SandboxFilesystemIsADirectoryError(SandboxFilesystemError):
    pass


class SandboxFilesystemPermissionError(SandboxFilesystemError):
    pass


class SandboxFilesystemFileTooLargeError(SandboxFilesystemError):
    pass


class FileInfo:
    """
    Mirrors ``modal.types.FileInfo`` in the parts the backend reads.

    ``kind`` is Modal's three-valued ``FileType``, not a boolean, which matters: a symlink
    is its own type, so ``is_dir()`` is false even for a link that points at a directory.
    """

    def __init__(self, name: str, *, kind: str = "file", size: int = 0) -> None:
        if kind not in ("file", "directory", "symlink"):
            raise ValueError(f"kind must be file, directory or symlink, got {kind!r}")
        self.name = name
        self.size = size
        self.kind = kind

    def is_dir(self) -> bool:
        return self.kind == "directory"

    def is_symlink(self) -> bool:
        return self.kind == "symlink"


class FakeProcess:
    """A container process whose streams and exit status are scripted by a test."""

    def __init__(
        self,
        *,
        returncode: int = 0,
        stdout: list[bytes] | None = None,
        stderr: list[bytes] | None = None,
        wait_error: Exception | None = None,
        stream_error: Exception | None = None,
    ) -> None:
        self._returncode = returncode
        self._wait_error = wait_error
        self.stdout = _FakeStream(stdout or [], stream_error)
        self.stderr = _FakeStream(stderr or [], None)
        self.waited = False

    def wait(self) -> int:
        self.waited = True
        if self._wait_error is not None:
            raise self._wait_error
        return self._returncode


class _FakeStream:
    """Yields byte chunks, optionally dying part-way through like a severed connection."""

    def __init__(self, chunks: list[bytes], error: Exception | None) -> None:
        self._chunks = chunks
        self._error = error

    def __iter__(self):
        yield from self._chunks
        if self._error is not None:
            raise self._error


class FakeFilesystem:
    def __init__(self, sandbox: FakeSandbox) -> None:
        self._sandbox = sandbox

    def write_bytes(self, data: bytes, remote_path: str) -> None:
        self._sandbox.calls.append(("write_bytes", remote_path, data))
        if self._sandbox.filesystem_error is not None:
            raise self._sandbox.filesystem_error
        self._sandbox.files[remote_path] = data

    def read_bytes(self, remote_path: str) -> bytes:
        self._sandbox.calls.append(("read_bytes", remote_path))
        if self._sandbox.filesystem_error is not None:
            raise self._sandbox.filesystem_error
        try:
            return self._sandbox.files[remote_path]
        except KeyError:
            raise SandboxFilesystemNotFoundError(f"path does not exist: {remote_path}") from None

    def list_files(self, remote_path: str) -> list[FileInfo]:
        self._sandbox.calls.append(("list_files", remote_path))
        if self._sandbox.filesystem_error is not None:
            raise self._sandbox.filesystem_error
        return self._sandbox.listing

    def stat(self, remote_path: str) -> FileInfo:
        self._sandbox.calls.append(("stat", remote_path))
        if self._sandbox.filesystem_error is not None:
            raise self._sandbox.filesystem_error
        return FileInfo(remote_path.rsplit("/", 1)[-1])


class FakeSandbox:
    """One sandbox. Tests drive its behaviour by setting the attributes below."""

    def __init__(self, object_id: str, **create_kwargs: Any) -> None:
        self.object_id = object_id
        self.create_kwargs = create_kwargs
        self.calls: list[tuple] = []
        self.files: dict[str, bytes] = {}
        self.listing: list[FileInfo] = []
        self.terminated = False
        self.terminate_error: Exception | None = None
        self.exec_error: Exception | None = None
        # Consumed one per exec when set, so a test can let the command through and
        # fail only the liveness probe that follows it.
        self.exec_errors: list[Exception | None] = []
        self.filesystem_error: Exception | None = None
        self.poll_result: int | None = None
        # Either one process for every exec, or a queue consumed in order.
        self.process = FakeProcess()
        self.processes: list[FakeProcess] = []

    @property
    def filesystem(self) -> FakeFilesystem:
        return FakeFilesystem(self)

    def exec(self, *args: str, **kwargs: Any) -> FakeProcess:
        self.calls.append(("exec", args, kwargs))
        error = self.exec_errors.pop(0) if self.exec_errors else self.exec_error
        if error is not None:
            raise error
        if self.processes:
            return self.processes.pop(0)
        return self.process

    def terminate(self, *, wait: bool = False) -> None:
        self.calls.append(("terminate", wait))
        if self.terminate_error is not None:
            raise self.terminate_error
        self.terminated = True

    def poll(self) -> int | None:
        return self.poll_result


class FakeSandboxFactory:
    """Stands in for ``modal.Sandbox``: records creations and hands out fakes."""

    def __init__(self) -> None:
        self.created: list[FakeSandbox] = []
        self.create_error: Exception | None = None
        self.from_id_error: Exception | None = None
        self.by_id: dict[str, FakeSandbox] = {}
        self.next_process: FakeProcess | None = None

    def create(self, **kwargs: Any) -> FakeSandbox:
        if self.create_error is not None:
            raise self.create_error
        sandbox = FakeSandbox(f"sb-{len(self.created)}", **kwargs)
        if self.next_process is not None:
            sandbox.process = self.next_process
        self.created.append(sandbox)
        self.by_id[sandbox.object_id] = sandbox
        return sandbox

    def from_id(self, object_id: str) -> FakeSandbox:
        if self.from_id_error is not None:
            raise self.from_id_error
        try:
            return self.by_id[object_id]
        except KeyError:
            raise NotFoundError(f"no such sandbox: {object_id}") from None


class FakeApp:
    def __init__(self, name: str) -> None:
        self.name = name


class FakeAppFactory:
    def __init__(self) -> None:
        self.lookups: list[tuple[str, bool]] = []

    def lookup(self, name: str, *, create_if_missing: bool = False) -> FakeApp:
        self.lookups.append((name, create_if_missing))
        return FakeApp(name)


class FakeImageFactory:
    def __init__(self) -> None:
        self.registries: list[str] = []

    def from_registry(self, tag: str) -> str:
        self.registries.append(tag)
        return f"image:{tag}"


def build_fake_modal() -> types.ModuleType:
    """Assemble a module object shaped like ``modal``, with its exception hierarchy."""
    exception = types.ModuleType("modal.exception")
    for name, cls in (
        ("Error", Error),
        ("TimeoutError", TimeoutError),
        ("ExecTimeoutError", ExecTimeoutError),
        ("SandboxTimeoutError", SandboxTimeoutError),
        ("SandboxTerminatedError", SandboxTerminatedError),
        ("ConnectionError", ConnectionError),
        ("InvalidError", InvalidError),
        ("ConflictError", ConflictError),
        ("NotFoundError", NotFoundError),
        ("AuthError", AuthError),
        ("PermissionDeniedError", PermissionDeniedError),
        ("SandboxFilesystemError", SandboxFilesystemError),
        ("SandboxFilesystemNotFoundError", SandboxFilesystemNotFoundError),
        ("SandboxFilesystemIsADirectoryError", SandboxFilesystemIsADirectoryError),
        ("SandboxFilesystemPermissionError", SandboxFilesystemPermissionError),
        ("SandboxFilesystemFileTooLargeError", SandboxFilesystemFileTooLargeError),
    ):
        setattr(exception, name, cls)

    module = types.ModuleType("modal")
    module.exception = exception
    module.Sandbox = FakeSandboxFactory()
    module.App = FakeAppFactory()
    module.Image = FakeImageFactory()
    return module
