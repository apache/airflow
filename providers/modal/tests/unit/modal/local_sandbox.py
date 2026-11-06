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

from airflow.providers.modal.utils.python_sandbox import TIMEOUT_RETURNCODE

# Modal's stdin buffer for a sandbox entrypoint, ``MAX_BUFFER_SIZE`` in ``modal/io_streams.py``
# (not in the package's type stubs, so not imported).
STDIN_BUFFER_BYTES = 2 * 1024 * 1024


class _Stdin:
    def __init__(self, sandbox: LocalSandbox) -> None:
        self._sandbox = sandbox
        self._pending = b""
        self._received = b""

    def write(self, data: str | bytes | memoryview) -> None:
        data = data.encode() if isinstance(data, str) else bytes(data)
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


class _Stream:
    """An output stream as Modal's reader serves it: line by line with newlines, or whole."""

    def __init__(self, text: str) -> None:
        self._text = text

    def __iter__(self):
        return iter(self._text.splitlines(keepends=True))

    def read(self) -> str:
        return self._text


class LocalSandbox:
    """
    Run a sandbox's command locally, the way Modal runs it in a container.

    Behaves like the parts of ``modal.Sandbox`` that ``@task.modal`` uses: stdin holds at most
    2 MiB between drains and reaches the command once it is closed and drained, ``stdout`` and
    ``stderr`` are separate streams read line by line or whole, ``returncode`` stays ``None``
    until ``wait`` has seen the command exit, and a command that outlives ``timeout`` gets exit
    code 124 and makes ``wait`` raise ``SandboxTimeoutError`` whatever ``raise_on_termination``
    says. ``python`` in
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
        self._stdout = self._stderr = ""
        self._exit_code: int | None = None

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
            self._stdout = (e.stdout or b"").decode()
            self._stderr = (e.stderr or b"").decode()
            self._exit_code = TIMEOUT_RETURNCODE
            return
        self._stdout = completed.stdout.decode()
        self._stderr = completed.stderr.decode()
        self._exit_code = completed.returncode

    @property
    def stdout(self) -> _Stream:
        return _Stream(self._stdout)

    @property
    def stderr(self) -> _Stream:
        return _Stream(self._stderr)

    def wait(self, raise_on_termination: bool = True) -> None:
        self.returncode = self._exit_code
        if self.returncode == TIMEOUT_RETURNCODE:
            raise modal.exception.SandboxTimeoutError()

    def terminate(self) -> None:
        self.terminated = True
