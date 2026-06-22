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
"""``local`` provider — runs the workload in a local subprocess.

This is the open-source reference backend. It needs no SaaS account, powers the
demo and the test suite, and anchors the vendor-neutrality argument when the
executor abstraction is proposed upstream (there is at least one fully open
backend, not only commercial SaaS).

Each "sandbox" is a temp working directory; each "exec" is a background
``subprocess.Popen`` whose stdout/stderr are captured to files in that dir.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile
import uuid

from airflow.providers.sandbox.backends.base import (
    ExecResult,
    SandboxCapabilities,
    SandboxProvider,
    SandboxSpec,
    SandboxState,
)


class _Proc:
    __slots__ = ("popen", "out_path", "err_path")

    def __init__(self, popen, out_path, err_path):
        self.popen = popen
        self.out_path = out_path
        self.err_path = err_path


class LocalProvider(SandboxProvider):
    """Subprocess-backed reference implementation of the provider contract."""

    capabilities = SandboxCapabilities(
        kind="delegated-run",
        supports_file_upload=True,
        supports_async_exec=True,
        supports_kill=True,
        supports_reattach=False,  # PIDs/temp dirs don't survive a scheduler crash
        supports_pause_resume=False,
    )

    def __init__(self) -> None:
        self._dirs: dict[str, str] = {}      # handle -> working dir
        self._procs: dict[str, _Proc] = {}   # exec_ref -> running process

    def authenticate(self) -> None:
        return None  # nothing to authenticate locally

    def create_sandbox(self, spec: SandboxSpec) -> str:
        handle = spec.name or f"local-{uuid.uuid4().hex[:12]}"
        workdir = tempfile.mkdtemp(prefix=f"sandbox-{handle}-")
        self._dirs[handle] = workdir
        return handle

    def upload_workload(self, handle: str, payload: bytes, dest: str) -> None:
        workdir = self._dirs[handle]
        target = os.path.join(workdir, os.path.basename(dest) or "workload")
        with open(target, "wb") as fh:
            fh.write(payload)

    def run(
        self,
        handle: str,
        command: list[str],
        env: dict[str, str],
        cwd: str | None = None,
        timeout: int | None = None,
    ) -> str:
        workdir = self._dirs[handle]
        exec_ref = f"{handle}:{uuid.uuid4().hex[:8]}"
        out_path = os.path.join(workdir, "stdout.log")
        err_path = os.path.join(workdir, "stderr.log")
        out_fh = open(out_path, "wb")
        err_fh = open(err_path, "wb")
        popen = subprocess.Popen(
            command,
            cwd=cwd or workdir,
            env={**os.environ, **env},
            stdout=out_fh,
            stderr=err_fh,
        )
        self._procs[exec_ref] = _Proc(popen, out_path, err_path)
        return exec_ref

    def poll_status(self, handle: str, exec_ref: str) -> ExecResult:
        proc = self._procs.get(exec_ref)
        if proc is None:
            return ExecResult(state=SandboxState.GONE)
        rc = proc.popen.poll()
        if rc is None:
            return ExecResult(state=SandboxState.RUNNING)
        state = SandboxState.SUCCEEDED if rc == 0 else SandboxState.FAILED
        return ExecResult(state=state, exit_code=rc)

    def fetch_logs(self, handle: str, exec_ref: str) -> tuple[list[str], list[str]]:
        proc = self._procs.get(exec_ref)
        if proc is None:
            return [], []
        lines: list[str] = []
        for path in (proc.out_path, proc.err_path):
            try:
                with open(path, encoding="utf-8", errors="replace") as fh:
                    lines.extend(fh.read().splitlines())
            except FileNotFoundError:
                pass
        return [f"local sandbox {handle}"], lines

    def destroy(self, handle: str) -> None:
        for ref, proc in list(self._procs.items()):
            if ref.startswith(f"{handle}:"):
                if proc.popen.poll() is None:
                    proc.popen.kill()
                self._procs.pop(ref, None)
        workdir = self._dirs.pop(handle, None)
        if workdir:
            shutil.rmtree(workdir, ignore_errors=True)
