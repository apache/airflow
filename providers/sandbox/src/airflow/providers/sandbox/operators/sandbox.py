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
"""``SandboxOperator`` — run a command in an ephemeral cloud sandbox.

Unlike :class:`~airflow.providers.sandbox.executors.sandbox_executor.SandboxExecutor`
(which routes *every* task through a sandbox and needs the api-server/Task-SDK
plumbing), this operator runs *one* command in a sandbox from inside a normal
task. It needs no special executor, so it is the simplest way to adopt sandbox
execution incrementally — the KubernetesPodOperator pattern, for sandboxes.
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Sequence

try:  # Airflow 3 Task SDK
    from airflow.sdk import BaseOperator
except ImportError:  # Airflow 2.x
    from airflow.models import BaseOperator

from airflow.exceptions import AirflowException

from airflow.providers.sandbox.provider_loader import load_provider
from airflow.providers.sandbox.backends.base import (
    SandboxSpec,
    SandboxState,
)

if TYPE_CHECKING:
    from airflow.utils.context import Context


class SandboxOperator(BaseOperator):
    """Execute ``command`` inside an ephemeral sandbox and return its stdout.

    :param command: Shell command (str) or argv (list[str]) to run.
    :param provider: Backend alias (local|daytona|e2b|modal|islo) or ``module:Class``.
        Defaults to the ``[sandbox] provider`` config (or ``local``).
    :param image: Provider image/template/snapshot reference.
    :param env: Extra environment variables for the command.
    :param cpu/memory_mb/disk_gb: Optional resource sizing.
    :param sandbox_timeout: Sandbox lifetime ceiling (seconds).
    :param poll_interval: Seconds between status polls.
    :param keep: If True, do not destroy the sandbox after completion.
    """

    template_fields: Sequence[str] = ("command", "env", "image")
    ui_color = "#c5f0d4"

    def __init__(
        self,
        *,
        command: str | list[str],
        provider: str | None = None,
        image: str | None = None,
        env: dict[str, str] | None = None,
        cpu: int | None = None,
        memory_mb: int | None = None,
        disk_gb: int | None = None,
        sandbox_timeout: int = 600,
        poll_interval: int = 2,
        keep: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.command = command
        self.provider = provider
        self.image = image
        self.env = env or {}
        self.cpu = cpu
        self.memory_mb = memory_mb
        self.disk_gb = disk_gb
        self.sandbox_timeout = sandbox_timeout
        self.poll_interval = poll_interval
        self.keep = keep

    def _resolve_provider_name(self) -> str:
        if self.provider:
            return self.provider
        try:
            from airflow.configuration import conf

            return conf.get("sandbox", "provider", fallback="local")
        except Exception:
            return "local"

    def _argv(self) -> list[str]:
        if isinstance(self.command, str):
            return ["sh", "-c", self.command]
        return list(self.command)

    def execute(self, context: "Context") -> str:
        provider = load_provider(self._resolve_provider_name())
        provider.authenticate()

        ti = context.get("ti") or context.get("task_instance") if context else None
        name = None
        if ti is not None:
            name = f"af-{ti.dag_id}-{ti.task_id}-{getattr(ti, 'try_number', 1)}"
            name = "".join(c if c.isalnum() or c == "-" else "-" for c in name)[:63]

        spec = SandboxSpec(
            name=name,
            image=self.image,
            cpu=self.cpu,
            memory_mb=self.memory_mb,
            disk_gb=self.disk_gb,
            timeout=self.sandbox_timeout,
            env=self.env,
            keep=self.keep,
        )

        handle = provider.create_sandbox(spec)
        self.log.info("Created sandbox %s via %s", handle, provider.__class__.__name__)
        try:
            exec_ref = provider.run(handle, self._argv(), env=self.env, timeout=self.sandbox_timeout)
            deadline = time.monotonic() + self.sandbox_timeout
            res = provider.poll_status(handle, exec_ref)
            while res.state in (SandboxState.PENDING, SandboxState.RUNNING, SandboxState.UNKNOWN):
                if time.monotonic() > deadline:
                    raise AirflowException(f"Sandbox command timed out after {self.sandbox_timeout}s")
                time.sleep(self.poll_interval)
                res = provider.poll_status(handle, exec_ref)

            _, log_lines = provider.fetch_logs(handle, exec_ref)
            for line in log_lines:
                self.log.info("[sandbox] %s", line)

            if res.state is not SandboxState.SUCCEEDED:
                raise AirflowException(
                    f"Sandbox command failed (state={res.state.value}, exit_code={res.exit_code})"
                )
            return res.stdout or "\n".join(log_lines)
        finally:
            if not self.keep:
                try:
                    provider.destroy(handle)
                    self.log.info("Destroyed sandbox %s", handle)
                except Exception:
                    self.log.exception("Failed to destroy sandbox %s", handle)
