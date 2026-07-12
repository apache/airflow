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
"""Driver contract for vendor-owned sandbox integrations."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Callable
from typing import TYPE_CHECKING, ClassVar, TypeAlias

if TYPE_CHECKING:
    from airflow.providers.common.sandbox.models import (
        RecoveredSandbox,
        SandboxHandle,
        SandboxLaunchRequest,
        SandboxOutput,
        SandboxResult,
    )


class SandboxDriver(ABC):
    """
    Translate portable executor lifecycle operations to one sandbox service.

    Drivers must use ``request.request_id`` as a deterministic correlation key. ``fence`` and
    ``terminate`` must be idempotent and return only after the driver can confirm that no workload
    represented by the request or handle is still running.
    """

    driver_id: ClassVar[str]

    @abstractmethod
    async def health_check(self) -> None:
        """Validate credentials and control-plane reachability."""

    @abstractmethod
    async def launch(self, request: SandboxLaunchRequest) -> SandboxHandle:
        """Create an isolated environment and start the provided argv workload."""

    @abstractmethod
    def validate_handle(self, handle: SandboxHandle, *, request_id: str) -> None:
        """Validate a handle locally, raising ``SandboxInvalidHandleError`` when invalid."""

    @abstractmethod
    async def get_status(self, handle: SandboxHandle) -> SandboxResult:
        """Return a normalized observation for a launched workload."""

    @abstractmethod
    async def terminate(self, handle: SandboxHandle) -> None:
        """Idempotently stop and remove the resource represented by ``handle``."""

    @abstractmethod
    async def fence(self, request_id: str) -> None:
        """Idempotently stop every possible workload correlated with ``request_id``."""

    async def recover(self, request_id: str) -> RecoveredSandbox | None:
        """
        Recover a handle after a scheduler crash before handle persistence.

        The safe default fences the deterministic request and reports that it cannot be adopted.
        A driver may return ``RecoveredSandbox`` only when it can identify the exact launched
        workload and reconstruct its retention policy.
        """
        await self.fence(request_id)
        return None

    async def get_output(self, handle: SandboxHandle) -> SandboxOutput | None:
        """Return optional provider-captured diagnostics; Airflow remote logs remain canonical."""
        return None

    @abstractmethod
    async def close(self) -> None:
        """Close driver-owned clients and transports."""


SandboxDriverFactory: TypeAlias = Callable[[], SandboxDriver]
