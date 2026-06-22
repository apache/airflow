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
"""``daytona`` provider — ephemeral Daytona sandboxes via the ``daytona`` SDK.

Cleanest SaaS backend: named/labelled sandboxes make adoption work. Requires
``pip install 'airflow-provider-sandbox[daytona]'`` and ``DAYTONA_API_KEY``.
"""

from __future__ import annotations

import os

from airflow.providers.sandbox.backends.base import (
    ExecResult,
    SandboxCapabilities,
    SandboxProvider,
    SandboxSpec,
    SandboxState,
)


class DaytonaProvider(SandboxProvider):
    capabilities = SandboxCapabilities(
        kind="delegated-run",
        supports_file_upload=True,
        supports_async_exec=True,
        supports_kill=True,
        supports_reattach=True,  # labelled sandboxes adopt via daytona.list(labels=...)
    )

    def __init__(self) -> None:
        self._client = None
        self._sessions: dict[str, tuple[object, str]] = {}  # exec_ref -> (sandbox, cmd_id)

    def authenticate(self) -> None:
        try:
            from daytona import Daytona, DaytonaConfig
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "DaytonaProvider needs the daytona SDK: "
                "pip install 'airflow-provider-sandbox[daytona]'"
            ) from exc
        self._client = Daytona(
            DaytonaConfig(
                api_key=os.environ["DAYTONA_API_KEY"],
                api_url=os.environ.get("DAYTONA_API_URL"),
                target=os.environ.get("DAYTONA_TARGET"),
            )
        )

    def create_sandbox(self, spec: SandboxSpec) -> str:
        from daytona import CreateSandboxFromSnapshotParams

        sandbox = self._client.create(
            CreateSandboxFromSnapshotParams(
                snapshot=spec.image,
                ephemeral=True,
                auto_stop_interval=max(1, spec.timeout // 60),
                labels=spec.labels,
                env_vars=spec.env or None,
            )
        )
        return sandbox.id

    def _sandbox(self, handle: str):
        return self._client.get(handle)

    def upload_workload(self, handle: str, payload: bytes, dest: str) -> None:
        self._sandbox(handle).fs.upload_file(payload, dest)

    def run(self, handle, command, env, cwd=None, timeout=None) -> str:
        sandbox = self._sandbox(handle)
        session_id = f"af-{handle}"
        sandbox.process.create_session(session_id)
        from daytona import SessionExecuteRequest

        resp = sandbox.process.execute_session_command(
            session_id,
            SessionExecuteRequest(command=" ".join(command), run_async=True),
        )
        cmd_id = resp.cmd_id
        exec_ref = f"{session_id}:{cmd_id}"
        self._sessions[exec_ref] = (sandbox, session_id)
        return exec_ref

    def poll_status(self, handle: str, exec_ref: str) -> ExecResult:
        try:
            sandbox, session_id = self._sessions.get(exec_ref, (self._sandbox(handle), exec_ref))
            cmd_id = exec_ref.split(":", 1)[-1]
            cmd = sandbox.process.get_session_command(session_id, cmd_id)
        except Exception:
            # Could not reach the sandbox — transient, not confirmed gone.
            return ExecResult(state=SandboxState.UNKNOWN)
        if cmd.exit_code is None:
            return ExecResult(state=SandboxState.RUNNING)
        state = SandboxState.SUCCEEDED if cmd.exit_code == 0 else SandboxState.FAILED
        return ExecResult(state=state, exit_code=cmd.exit_code)

    def fetch_logs(self, handle: str, exec_ref: str) -> tuple[list[str], list[str]]:
        sandbox, session_id = self._sessions.get(exec_ref, (self._sandbox(handle), exec_ref))
        cmd_id = exec_ref.split(":", 1)[-1]
        logs = sandbox.process.get_session_command_logs(session_id, cmd_id)
        return [f"daytona sandbox {handle}"], str(logs).splitlines()

    def destroy(self, handle: str) -> None:
        try:
            self._sandbox(handle).delete()
        except Exception:
            pass
