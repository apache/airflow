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
``modal`` provider — Modal sandboxes via the ``modal`` SDK.

Requires an App + Image and account creds (MODAL_TOKEN_ID / MODAL_TOKEN_SECRET).
Opaque-handle backend, so ``supports_reattach=False``.
"""

from __future__ import annotations

import contextlib
import os

from airflow.providers.sandbox.backends.base import (
    ExecResult,
    SandboxCapabilities,
    SandboxProvider,
    SandboxSpec,
    SandboxState,
)


class ModalProvider(SandboxProvider):
    """Modal sandbox backend (image-baked workload)."""

    capabilities = SandboxCapabilities(
        kind="delegated-run",
        supports_file_upload=False,  # workload baked into the Image
        supports_async_exec=True,
        supports_kill=True,
        supports_reattach=False,
    )

    def __init__(self) -> None:
        self._app = None
        self._sandboxes: dict[str, object] = {}
        self._procs: dict[str, object] = {}

    def authenticate(self) -> None:
        try:
            import modal  # noqa: F401
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "ModalProvider needs the modal SDK: pip install 'airflow-provider-sandbox[modal]'"
            ) from exc
        os.environ.setdefault("MODAL_TOKEN_ID", os.environ.get("MODAL_TOKEN_ID", ""))

    def create_sandbox(self, spec: SandboxSpec) -> str:
        import modal

        app_name = os.environ.get("MODAL_APP", "airflow-sandbox")
        self._app = modal.App.lookup(app_name, create_if_missing=True)
        image = modal.Image.from_registry(spec.image) if spec.image else modal.Image.debian_slim()
        sandbox = modal.Sandbox.create(
            app=self._app, image=image, timeout=spec.timeout, cpu=spec.cpu,
            memory=spec.memory_mb,
        )
        handle = sandbox.object_id
        self._sandboxes[handle] = sandbox
        return handle

    def upload_workload(self, handle: str, payload: bytes, dest: str) -> None:
        # Modal workloads are image-baked; transfer via mounted volume if needed.
        return None

    def run(self, handle, command, env, cwd=None, timeout=None) -> str:
        sandbox = self._sandboxes[handle]
        proc = sandbox.exec(*command)
        exec_ref = f"{handle}:{id(proc)}"
        self._procs[exec_ref] = proc
        return exec_ref

    def poll_status(self, handle: str, exec_ref: str) -> ExecResult:
        proc = self._procs.get(exec_ref)
        if proc is None:
            return ExecResult(state=SandboxState.GONE)
        rc = proc.poll()
        if rc is None:
            return ExecResult(state=SandboxState.RUNNING)
        state = SandboxState.SUCCEEDED if rc == 0 else SandboxState.FAILED
        return ExecResult(state=state, exit_code=rc)

    def fetch_logs(self, handle: str, exec_ref: str) -> tuple[list[str], list[str]]:
        proc = self._procs.get(exec_ref)
        if proc is None:
            return [], []
        try:
            out = proc.stdout.read()
        except Exception:
            out = ""
        return [f"modal sandbox {handle}"], str(out).splitlines()

    def destroy(self, handle: str) -> None:
        sandbox = self._sandboxes.pop(handle, None)
        if sandbox is not None:
            with contextlib.suppress(Exception):
                sandbox.terminate()
