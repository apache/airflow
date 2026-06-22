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
"""``e2b`` provider — E2B sandboxes via the ``e2b`` SDK.

Opaque-handle backend: ``supports_reattach=False`` (adoption only if the handle
was persisted before a crash). Requires ``E2B_API_KEY``.
"""

from __future__ import annotations

from airflow.providers.sandbox.backends.base import (
    ExecResult,
    SandboxCapabilities,
    SandboxProvider,
    SandboxSpec,
    SandboxState,
)


class E2BProvider(SandboxProvider):
    capabilities = SandboxCapabilities(
        kind="delegated-run",
        supports_file_upload=True,
        supports_async_exec=True,
        supports_kill=True,
        supports_reattach=False,
    )

    def __init__(self) -> None:
        self._sandboxes: dict[str, object] = {}      # handle -> Sandbox
        self._handles: dict[str, object] = {}        # exec_ref -> CommandHandle

    def authenticate(self) -> None:
        try:
            import e2b  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "E2BProvider needs the e2b SDK: pip install 'airflow-provider-sandbox[e2b]'"
            ) from exc

    def create_sandbox(self, spec: SandboxSpec) -> str:
        from e2b import Sandbox

        sandbox = Sandbox(template=spec.image or "base", timeout=spec.timeout, metadata=spec.labels)
        handle = sandbox.sandbox_id
        self._sandboxes[handle] = sandbox
        return handle

    def upload_workload(self, handle: str, payload: bytes, dest: str) -> None:
        self._sandboxes[handle].files.write(dest, payload)

    def run(self, handle, command, env, cwd=None, timeout=None) -> str:
        sandbox = self._sandboxes[handle]
        cmd_handle = sandbox.commands.run(
            " ".join(command), background=True, envs=env, cwd=cwd
        )
        exec_ref = f"{handle}:{getattr(cmd_handle, 'pid', id(cmd_handle))}"
        self._handles[exec_ref] = cmd_handle
        return exec_ref

    def poll_status(self, handle: str, exec_ref: str) -> ExecResult:
        cmd = self._handles.get(exec_ref)
        if cmd is None:
            return ExecResult(state=SandboxState.GONE)
        exit_code = getattr(cmd, "exit_code", None)
        if exit_code is None:
            return ExecResult(state=SandboxState.RUNNING)
        state = SandboxState.SUCCEEDED if exit_code == 0 else SandboxState.FAILED
        return ExecResult(state=state, exit_code=exit_code)

    def fetch_logs(self, handle: str, exec_ref: str) -> tuple[list[str], list[str]]:
        cmd = self._handles.get(exec_ref)
        if cmd is None:
            return [], []
        out = getattr(cmd, "stdout", "") or ""
        err = getattr(cmd, "stderr", "") or ""
        return [f"e2b sandbox {handle}"], (out + err).splitlines()

    def destroy(self, handle: str) -> None:
        sandbox = self._sandboxes.pop(handle, None)
        if sandbox is not None:
            try:
                sandbox.kill()
            except Exception:
                pass
