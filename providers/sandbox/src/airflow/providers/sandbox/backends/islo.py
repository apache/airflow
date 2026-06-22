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
"""``islo`` provider — islo.dev sandboxes (submit→poll exec model).

islo's native shape is asynchronous: submit an exec and poll for the result by
``exec_id`` — which is exactly the watcher's polling contract. Named sandboxes
make adoption work. Requires ``ISLO_API_KEY``.

The islo public SDK surface is still thin; this maps the documented
create / exec / get-result / destroy verbs and degrades gracefully.
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


class IsloProvider(SandboxProvider):
    capabilities = SandboxCapabilities(
        kind="delegated-run",
        supports_file_upload=True,
        supports_async_exec=True,   # native submit→poll
        supports_kill=False,
        supports_reattach=True,     # named sandboxes
    )

    def __init__(self) -> None:
        self._client = None

    def authenticate(self) -> None:
        try:
            from islo import Islo  # type: ignore
        except ImportError as exc:  # pragma: no cover
            raise RuntimeError(
                "IsloProvider needs the islo SDK: pip install 'airflow-provider-sandbox[islo]'"
            ) from exc
        self._client = Islo(api_key=os.environ["ISLO_API_KEY"])

    def create_sandbox(self, spec: SandboxSpec) -> str:
        sandbox = self._client.create_sandbox(name=spec.name, image=spec.image, env=spec.env)
        return getattr(sandbox, "name", None) or getattr(sandbox, "id")

    def upload_workload(self, handle: str, payload: bytes, dest: str) -> None:
        self._client.upload_file(handle, dest, payload)

    def run(self, handle, command, env, cwd=None, timeout=None) -> str:
        resp = self._client.exec_in_sandbox(handle, command=command, env=env, cwd=cwd)
        return getattr(resp, "exec_id", None) or getattr(resp, "id")

    def poll_status(self, handle: str, exec_ref: str) -> ExecResult:
        try:
            res = self._client.get_exec_result(handle, exec_ref)
        except Exception:
            return ExecResult(state=SandboxState.UNKNOWN)
        if res is None or getattr(res, "finished", False) is False:
            return ExecResult(state=SandboxState.RUNNING)
        rc = getattr(res, "exit_code", 0)
        state = SandboxState.SUCCEEDED if rc == 0 else SandboxState.FAILED
        return ExecResult(state=state, exit_code=rc, stdout=getattr(res, "stdout", ""))

    def fetch_logs(self, handle: str, exec_ref: str) -> tuple[list[str], list[str]]:
        try:
            res = self._client.get_exec_result(handle, exec_ref)
        except Exception:
            return [], []
        return [f"islo sandbox {handle}"], str(getattr(res, "stdout", "")).splitlines()

    def destroy(self, handle: str) -> None:
        try:
            self._client.destroy_sandbox(handle)
        except Exception:
            pass
