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
``islo`` provider — islo.dev sandboxes via the ``islo`` SDK.

islo's native exec model is asynchronous (submit ``exec_in_sandbox`` -> poll
``get_exec_result`` by ``exec_id``), which maps directly onto the watcher's
polling contract. Sandboxes are name-keyed, so adoption works. islo also
supports pause/resume. Requires ``pip install 'airflow-provider-sandbox[islo]'``
and ``ISLO_API_KEY`` (optionally ``ISLO_BASE_URL`` / ``ISLO_COMPUTE_URL``).
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


class IsloProvider(SandboxProvider):
    """islo.dev sandbox backend (named sandboxes; submit/poll exec)."""

    capabilities = SandboxCapabilities(
        kind="delegated-run",
        # islo's upload_file is path-based (no in-band bytes), so the executor
        # transfers workloads via image/snapshot/git sources rather than upload.
        supports_file_upload=False,
        supports_async_exec=True,    # native submit→poll
        supports_kill=False,         # exec is not individually killable; sandbox stop/pause is
        supports_reattach=True,      # name-keyed sandboxes
        supports_pause_resume=True,  # pause_sandbox / resume_sandbox
    )

    def __init__(self) -> None:
        self._client = None

    def _sandboxes(self):
        return self._client.sandboxes

    def authenticate(self) -> None:
        # A factory hook lets tests inject a fake client.
        client_factory = getattr(self, "_client_factory", None)
        if client_factory is not None:
            self._client = client_factory()
            return
        try:
            from islo import Islo
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "IsloProvider needs the islo SDK: pip install 'airflow-provider-sandbox[islo]'"
            ) from exc
        kwargs: dict = {"api_key": os.environ["ISLO_API_KEY"]}
        if os.environ.get("ISLO_BASE_URL"):
            kwargs["base_url"] = os.environ["ISLO_BASE_URL"]
        if os.environ.get("ISLO_COMPUTE_URL"):
            kwargs["compute_url"] = os.environ["ISLO_COMPUTE_URL"]
        self._client = Islo(**kwargs)

    def create_sandbox(self, spec: SandboxSpec) -> str:
        kwargs: dict = {}
        if spec.name:
            kwargs["name"] = spec.name
        if spec.image:
            kwargs["image"] = spec.image
        if spec.env:
            kwargs["env"] = spec.env
        if spec.cpu:
            kwargs["vcpus"] = spec.cpu
        if spec.memory_mb:
            kwargs["memory_mb"] = spec.memory_mb
        if spec.disk_gb:
            kwargs["disk_gb"] = spec.disk_gb
        resp = self._sandboxes().create_sandbox(**kwargs)
        # name is the stable handle (adoptable); fall back to id.
        return getattr(resp, "name", None) or resp.id

    def upload_workload(self, handle: str, payload: bytes, dest: str) -> None:
        # Not used: capabilities.supports_file_upload is False (path-based API).
        raise NotImplementedError("islo workloads are provisioned via image/snapshot/git sources")

    def run(self, handle, command, env, cwd=None, timeout=None) -> str:
        resp = self._sandboxes().exec_in_sandbox(
            handle,
            command=list(command),
            env=env or None,
            timeout_secs=timeout,
            workdir=cwd,
        )
        return resp.exec_id

    def poll_status(self, handle: str, exec_ref: str) -> ExecResult:
        try:
            res = self._sandboxes().get_exec_result(handle, exec_ref)
        except Exception:
            return ExecResult(state=SandboxState.UNKNOWN)
        exit_code = getattr(res, "exit_code", None)
        if exit_code is None:
            return ExecResult(state=SandboxState.RUNNING)
        state = SandboxState.SUCCEEDED if exit_code == 0 else SandboxState.FAILED
        return ExecResult(
            state=state,
            exit_code=exit_code,
            stdout=getattr(res, "stdout", "") or "",
            stderr=getattr(res, "stderr", "") or "",
        )

    def fetch_logs(self, handle: str, exec_ref: str) -> tuple[list[str], list[str]]:
        try:
            res = self._sandboxes().get_exec_result(handle, exec_ref)
        except Exception:
            return [], []
        out = (getattr(res, "stdout", "") or "") + (getattr(res, "stderr", "") or "")
        return [f"islo sandbox {handle}"], out.splitlines()

    def destroy(self, handle: str) -> None:
        with contextlib.suppress(Exception):
            self._sandboxes().delete_sandbox(handle)
