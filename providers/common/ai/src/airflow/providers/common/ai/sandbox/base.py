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
"""Vendor-neutral contract for running agent-written code in an isolated sandbox."""

from __future__ import annotations

import math
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import ClassVar

# Cap on each of stdout/stderr a backend returns from a command, protecting the
# LLM context window from unbounded output. Shared by every backend.
_MAX_OUTPUT_CHARS = 64 * 1024


def _validate_positive_finite(value: float, name: str) -> None:
    if not math.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be a positive finite number, got {value!r}.")


def _new_sandbox_name() -> str:
    """Generate a unique sandbox name, ``airflow-sandbox-`` prefixed for correlation and cleanup."""
    return f"airflow-sandbox-{uuid.uuid4().hex[:12]}"


def _cap_output(text: str) -> tuple[str, bool]:
    """Return ``text`` capped at ``_MAX_OUTPUT_CHARS`` and whether it was truncated."""
    if len(text) <= _MAX_OUTPUT_CHARS:
        return text, False
    return text[:_MAX_OUTPUT_CHARS], True


@dataclass(frozen=True)
class SandboxResult:
    """
    Outcome of one command execution inside a sandbox.

    ``timed_out`` means the command hit the timeout (``exit_code`` is then
    meaningless). ``truncated`` means the backend capped ``stdout``/``stderr``.
    ``sandbox_terminated`` means the backend destroyed the sandbox while
    stopping the command, so the toolset must provision a fresh one next time.
    """

    exit_code: int
    stdout: str
    stderr: str
    timed_out: bool = False
    truncated: bool = False
    sandbox_terminated: bool = False


class SandboxBackend(ABC):
    """
    Contract for running commands in an isolated sandbox.

    The lifecycle is create → run (any number of times) → destroy, driven by
    :class:`~airflow.providers.common.ai.toolsets.sandbox.SandboxToolset`.
    Implementations must be cheap to construct — constructors run at Dag-parse
    time — so credentials and connections are resolved lazily, on first use.
    ``destroy`` must be idempotent: destroying an already-gone sandbox is not
    an error. All methods are synchronous; the toolset offloads them to a
    thread.
    """

    name: ClassVar[str]
    """Short backend identifier (e.g. ``"docker"``), used in the toolset id."""

    @abstractmethod
    def create(self) -> str:
        """Provision one sandbox and return its handle (name or id)."""

    @abstractmethod
    def run(self, sandbox: str, command: list[str], *, timeout: float) -> SandboxResult:
        """Run ``command`` in the sandbox, bounded by ``timeout`` seconds."""

    @abstractmethod
    def destroy(self, sandbox: str) -> None:
        """Tear down the sandbox. Must be idempotent."""
