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
"""islo.dev microVM backend for :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`."""

from __future__ import annotations

import math
import time
from contextlib import suppress
from typing import Any

# The missing-``islo`` case is turned into AirflowOptionalProviderFeatureException
# by the package __init__'s lazy import, so import the SDK plainly here.
from islo import Islo
from islo.errors import NotFoundError
from islo.types import LifecyclePolicy

from airflow.providers.common.ai.sandbox.base import (
    SandboxBackend,
    SandboxResult,
    _cap_output,
    _new_sandbox_name,
    _validate_positive_finite,
)
from airflow.providers.common.compat.sdk import BaseHook

_TERMINAL_EXEC_STATUSES = frozenset({"completed", "failed", "timeout"})
_EXEC_POLL_INTERVAL = 0.2
_EXEC_POLL_GRACE = 5.0


class IsloSandboxBackend(SandboxBackend):
    """
    Sandbox backend that runs agent code in an `islo.dev <https://islo.dev>`__ microVM.

    Credentials come from an Airflow connection: ``password`` is the islo API
    key (required), ``host`` the compute URL (optional), and the extra field
    may set ``base_url`` and ``timeout`` (request timeout in seconds).

    :param islo_conn_id: Airflow connection ID for islo. ``None`` lets the SDK
        resolve credentials from its environment variables (``ISLO_API_KEY`` etc.).
    :param image: Sandbox image. ``None`` (default) uses the server default.
    :param vcpus: Number of virtual CPUs. ``None`` uses the server default.
    :param memory_mb: Memory in MB. ``None`` uses the server default.
    :param internet_enabled: Allow outbound internet access from the sandbox.
        Default ``False`` (no egress) — a safe default matching an isolated
        sandbox; pass ``True`` to allow it, or ``None`` to defer to the server.
    :param delete_after: Server-side TTL in seconds after which the sandbox is
        deleted even if the worker never got to destroy it (killed mid-run).
        Default ``3600``.
    """

    name = "islo"

    def __init__(
        self,
        islo_conn_id: str | None = "islo_default",
        *,
        image: str | None = None,
        vcpus: int | None = None,
        memory_mb: int | None = None,
        internet_enabled: bool | None = False,
        delete_after: int = 3600,
    ) -> None:
        _validate_positive_finite(delete_after, "delete_after")
        if vcpus is not None:
            _validate_positive_finite(vcpus, "vcpus")
        if memory_mb is not None:
            _validate_positive_finite(memory_mb, "memory_mb")
        if image == "":
            raise ValueError("image must not be empty.")
        self._islo_conn_id = islo_conn_id
        self._image = image
        self._vcpus = vcpus
        self._memory_mb = memory_mb
        self._internet_enabled = internet_enabled
        self._delete_after = delete_after
        self._client: Islo | None = None

    def _get_client(self) -> Islo:
        if self._client is not None:
            return self._client
        if self._islo_conn_id is None:
            self._client = Islo()
            return self._client
        conn = BaseHook.get_connection(self._islo_conn_id)
        api_key = (conn.password or "").strip()
        if not api_key:
            raise ValueError(
                f"Connection {self._islo_conn_id!r} has no password; set it to the islo API key."
            )
        kwargs: dict[str, Any] = {"api_key": api_key}
        if conn.host:
            kwargs["compute_url"] = conn.host
        extra = conn.extra_dejson
        if extra.get("base_url"):
            kwargs["base_url"] = extra["base_url"]
        if extra.get("timeout") is not None:
            request_timeout = float(extra["timeout"])
            _validate_positive_finite(request_timeout, "connection extra timeout")
            kwargs["timeout"] = request_timeout
        self._client = Islo(**kwargs)
        return self._client

    def create(self) -> str:
        # Pass only the kwargs that were set; the SDK treats absent kwargs as
        # "omit" and the server fills in its defaults.
        kwargs: dict[str, Any] = {}
        if self._image is not None:
            kwargs["image"] = self._image
        if self._vcpus is not None:
            kwargs["vcpus"] = self._vcpus
        if self._memory_mb is not None:
            kwargs["memory_mb"] = self._memory_mb
        if self._internet_enabled is not None:
            kwargs["internet_enabled"] = self._internet_enabled
        sandbox = self._get_client().sandboxes.create_sandbox(
            name=_new_sandbox_name(),
            lifecycle=LifecyclePolicy(delete_after=self._delete_after),
            **kwargs,
        )
        # The server may normalize the name; the response is authoritative.
        return sandbox.name

    def run(self, sandbox: str, command: list[str], *, timeout: float) -> SandboxResult:
        _validate_positive_finite(timeout, "timeout")
        client = self._get_client()
        # ``timeout_secs`` is only a client-side hint to the islo API (not
        # server-enforced), so the real bound is our poll deadline below: if no
        # terminal result arrives in time we delete the microVM ourselves.
        response = client.sandboxes.exec_in_sandbox(
            sandbox,
            command=list(command),
            timeout_secs=max(1, math.ceil(timeout)),
        )
        deadline = time.monotonic() + timeout + _EXEC_POLL_GRACE
        while time.monotonic() < deadline:
            result = client.sandboxes.get_exec_result(sandbox, response.exec_id)
            if result.status in _TERMINAL_EXEC_STATUSES:
                stdout, stdout_truncated = _cap_output(result.stdout or "")
                stderr, stderr_truncated = _cap_output(result.stderr or "")
                return SandboxResult(
                    exit_code=result.exit_code if result.exit_code is not None else -1,
                    stdout=stdout,
                    stderr=stderr,
                    timed_out=result.status == "timeout",
                    truncated=result.truncated or stdout_truncated or stderr_truncated,
                )
            time.sleep(_EXEC_POLL_INTERVAL)

        # No terminal result within our poll deadline. Delete the microVM so the
        # command cannot continue in the shared sandbox, then tell the toolset
        # to provision a fresh one. A
        # transient delete failure must not fail the task — the server-side TTL
        # (delete_after) is the backstop — so this teardown is best-effort.
        with suppress(Exception):
            self.destroy(sandbox)
        return SandboxResult(
            exit_code=-1,
            stdout="",
            stderr="",
            timed_out=True,
            sandbox_terminated=True,
        )

    def destroy(self, sandbox: str) -> None:
        # Already-gone is fine — destroy is idempotent.
        with suppress(NotFoundError):
            self._get_client().sandboxes.delete_sandbox(sandbox_name=sandbox)
