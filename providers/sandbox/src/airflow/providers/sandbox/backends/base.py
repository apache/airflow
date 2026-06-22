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
"""Pluggable sandbox-provider abstraction for the Airflow ``SandboxExecutor``.

The design mirrors `crabbox <https://github.com/openclaw/crabbox>`_'s provider
contract — *provision → sync → run(stream) → cleanup* — reduced to the minimal
verb set every backend (Daytona, E2B, Modal, islo, …) can support. The executor
talks only to :class:`SandboxProvider`; concrete backends are swapped via the
``[sandbox] provider`` config key, exactly like ``crabbox --provider``.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum


class SandboxState(str, Enum):
    """Normalized provider state, mapped onto Airflow TI states in ``sync()``.

    ``UNKNOWN`` is deliberately distinct from ``GONE``: a transient provider API
    error (rate-limit, timeout) must surface as ``UNKNOWN`` so a healthy running
    task is never killed by a single failed poll. Only a *confirmed* missing
    sandbox is ``GONE``.
    """

    PENDING = "pending"      # creating / queued
    RUNNING = "running"      # sandbox up, command in flight
    SUCCEEDED = "succeeded"  # exit_code == 0
    FAILED = "failed"        # exit_code != 0 (the task itself failed)
    GONE = "gone"            # sandbox confirmed absent (adoption: unadoptable)
    UNKNOWN = "unknown"      # transient poll failure; do NOT act on this


TERMINAL_STATES = frozenset({SandboxState.SUCCEEDED, SandboxState.FAILED})


@dataclass
class SandboxSpec:
    """Provider-agnostic creation request (crabbox ``run`` config block)."""

    image: str | None = None            # docker ref / E2B template / Daytona snapshot
    cpu: int | None = None
    memory_mb: int | None = None
    disk_gb: int | None = None
    timeout: int = 600                  # sandbox lifetime ceiling (sec)
    env: dict[str, str] = field(default_factory=dict)
    labels: dict[str, str] = field(default_factory=dict)  # dag_id/task_id/run_id/try_number
    name: str | None = None             # deterministic name -> adoptable across all providers
    keep: bool = False                  # crabbox --keep: don't destroy on completion


@dataclass
class ExecResult:
    """Outcome of a single ``poll_status`` call."""

    state: SandboxState
    exit_code: int | None = None        # None until terminal
    stdout: str = ""
    stderr: str = ""


@dataclass(frozen=True)
class SandboxCapabilities:
    """What a backend can actually do — gates executor features per provider.

    ``kind`` follows crabbox's taxonomy: ``delegated-run`` (e2b/modal/islo —
    managed remote exec, no SSH lease), ``ssh-lease`` (brokered VM), or
    ``service-control`` (start/stop a persistent service).
    """

    kind: str = "delegated-run"
    supports_file_upload: bool = True   # vs image-baked workload only
    supports_async_exec: bool = True    # submit→poll (islo) vs blocking (E2B)
    supports_kill: bool = False         # E2B/Modal yes; islo/Azure no
    supports_reattach: bool = False     # named/labelled handles → adoptable
    supports_pause_resume: bool = False


class SandboxProvider(ABC):
    """One concrete subclass per backend. Credentials are resolved internally
    (env var / Airflow Connection) — never passed as method arguments.
    """

    capabilities: SandboxCapabilities = SandboxCapabilities()

    @abstractmethod
    def authenticate(self) -> None:
        """Resolve creds and build the backend client. Called once at ``start()``."""

    @abstractmethod
    def create_sandbox(self, spec: SandboxSpec) -> str:
        """Provision an ephemeral sandbox; return an opaque handle (id or name).

        Implementations SHOULD honour ``spec.name`` when the backend supports
        named/labelled sandboxes, so the handle is deterministic and adoptable.
        """

    @abstractmethod
    def upload_workload(self, handle: str, payload: bytes, dest: str) -> None:
        """Transfer the task workload into the sandbox (no-op if image-baked)."""

    @abstractmethod
    def run(
        self,
        handle: str,
        command: list[str],
        env: dict[str, str],
        cwd: str | None = None,
        timeout: int | None = None,
    ) -> str:
        """Start ``command`` (argv form). Return an ``exec_ref`` for polling.

        For blocking backends, kick the command off in the background so the
        watcher's ``poll_status`` never blocks the scheduler heartbeat.
        """

    @abstractmethod
    def poll_status(self, handle: str, exec_ref: str) -> ExecResult:
        """Non-blocking status. Return ``UNKNOWN`` on transient error, ``GONE``
        only when the sandbox is *confirmed* absent."""

    @abstractmethod
    def fetch_logs(self, handle: str, exec_ref: str) -> tuple[list[str], list[str]]:
        """Best-effort log pull from a *live* sandbox -> (messages, log_lines).

        NOTE: this is a fallback only. The canonical log path is the in-sandbox
        Task SDK supervisor pushing to remote storage; see ``executors`` docs.
        """

    @abstractmethod
    def destroy(self, handle: str) -> None:
        """Best-effort teardown. No-op where the backend self-expires."""
