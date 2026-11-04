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
"""A stand-in for ``modal.Sandbox`` that runs the sandbox command in a local subprocess."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

import modal

# Modal 1.6.3's stdin buffer for a sandbox entrypoint (``modal.io_streams.MAX_BUFFER_SIZE``).
STDIN_BUFFER_BYTES = 2 * 1024 * 1024

# Exit code Modal reports for a sandbox that reached its timeout.
TIMEOUT_RETURNCODE = 124


class _Stdin:
    def __init__(self, sandbox: LocalSandbox) -> None:
        self._sandbox = sandbox
        self._pending = b""
        self._received = b""

    def write(self, data: str | bytes) -> None:
        if isinstance(data, str):
            data = data.encode()
        if len(self._pending) + len(data) > STDIN_BUFFER_BYTES:
            raise BufferError("Buffer size exceed limit. Call drain to flush the buffer.")
        self._pending += data

    def write_eof(self) -> None:
        self._sandbox._stdin_closed = True

    def drain(self) -> None:
        self._received += self._pending
        self._pending = b""
        if self._sandbox._stdin_closed:
            self._sandbox._run(self._received)


class LocalSandbox:
    """
    Run a sandbox's command locally, the way Modal runs it in a container.

    Behaves like the parts of ``modal.Sandbox`` that ``@task.modal`` uses: stdin holds at most
    2 MiB between drains and reaches the command once it is closed and drained, ``stdout``
    yields lines with their newline as Modal's line reader does, ``returncode`` is set once the
    command has exited, and a command that outlives ``timeout`` gets exit code 124 and makes
    ``wait`` raise ``SandboxTimeoutError`` whatever ``raise_on_termination`` says. ``python`` in
    the command is replaced by the interpreter running the tests, and ``TMPDIR`` points into the
    test's temporary directory.
    """

    object_id = "sb-local"

    def __init__(self, *entrypoint: str, tmp_path: Path, timeout: int | None = None) -> None:
        self.entrypoint = entrypoint
        self.tmp_path = tmp_path
        self.timeout = timeout
        self.stdin = _Stdin(self)
        self.returncode: int | None = None
        self.terminated = False
        self._stdin_closed = False
        self._lines: list[str] = []

    def _run(self, stdin: bytes) -> None:
        program, *args = self.entrypoint
        command = [sys.executable if program == "python" else program, *args]
        try:
            completed = subprocess.run(
                command,
                input=stdin,
                capture_output=True,
                env={**os.environ, "TMPDIR": str(self.tmp_path)},
                timeout=self.timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as e:
            self._lines = (e.stdout or b"").decode().splitlines(keepends=True)
            self.returncode = TIMEOUT_RETURNCODE
            return
        self._lines = (completed.stdout + completed.stderr).decode().splitlines(keepends=True)
        self.returncode = completed.returncode

    @property
    def stdout(self):
        return iter(self._lines)

    def wait(self, raise_on_termination: bool = True) -> None:
        if self.returncode == TIMEOUT_RETURNCODE:
            raise modal.exception.SandboxTimeoutError()

    def terminate(self) -> None:
        self.terminated = True
