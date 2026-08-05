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
"""Islo implementation of the common sandbox driver contract."""

from __future__ import annotations

from airflow.providers.common.sandbox.driver import SandboxDriver
from airflow.providers.common.sandbox.exceptions import (
    SandboxInvalidHandleError,
    SandboxLaunchUnfencedError,
)
from airflow.providers.common.sandbox.models import (
    SandboxHandle,
    SandboxLaunchRequest,
    SandboxOutput,
    SandboxResult,
    SandboxState,
)
from airflow.providers.islo.exceptions import IsloProtocolError
from airflow.providers.islo.hooks.islo import AsyncIsloClient, IsloClientConfig
from airflow.providers.islo.models import (
    IsloExecutionState,
    IsloSandboxConfig,
    IsloSandboxHandle,
    IsloSandboxSpec,
    sandbox_name_from_request_id,
)


class IsloSandboxDriver(SandboxDriver):
    """Run one Airflow workload in one deterministically named Islo sandbox."""

    driver_id = "islo"

    def __init__(self, client_config: IsloClientConfig) -> None:
        self._client = AsyncIsloClient(client_config)
        self._fence_targets: dict[str, set[str]] = {}

    async def health_check(self) -> None:
        await self._client.health_check()

    async def launch(self, request: SandboxLaunchRequest) -> SandboxHandle:
        config = IsloSandboxConfig.from_json(request.provider_config)
        expected_name = sandbox_name_from_request_id(request.request_id)
        spec = IsloSandboxSpec(
            name=expected_name,
            request_id=request.request_id,
            config=config,
            ttl_seconds=request.ttl_seconds,
        )
        sandbox_name, sandbox_id = await self._client.create_sandbox(spec)
        self._fence_targets.setdefault(request.request_id, set()).add(sandbox_name)
        if sandbox_name != expected_name:
            error = IsloProtocolError(
                f"Islo created sandbox {sandbox_name!r} instead of requested name {expected_name!r}"
            )
            try:
                await self._client.delete_sandbox(sandbox_name)
            except BaseException as fence_error:
                raise SandboxLaunchUnfencedError(
                    request.request_id,
                    error,
                    fence_error,
                ) from error
            raise error
        started = await self._client.execute(
            sandbox_name,
            list(request.command),
            request.env,
            workdir=request.workdir,
            timeout_seconds=request.timeout_seconds,
        )
        if started.sandbox_id != sandbox_id:
            raise IsloProtocolError(
                f"Islo started execution in sandbox {started.sandbox_id!r}, expected {sandbox_id!r}"
            )
        self._fence_targets.pop(request.request_id, None)
        return IsloSandboxHandle(
            request_id=request.request_id,
            sandbox_name=sandbox_name,
            sandbox_id=sandbox_id,
            execution_id=started.execution_id,
        ).to_common()

    def validate_handle(self, handle: SandboxHandle, *, request_id: str) -> None:
        try:
            ref = IsloSandboxHandle.from_common(handle)
        except Exception as error:
            raise SandboxInvalidHandleError(f"invalid Islo sandbox handle: {error}") from error
        if ref.request_id != request_id:
            raise SandboxInvalidHandleError("Islo handle request ID does not match its execution reference")

    async def get_status(self, handle: SandboxHandle) -> SandboxResult:
        result = await self._client.execution_result(IsloSandboxHandle.from_common(handle))
        state = SandboxState(result.state.value)
        exit_code = (
            result.exit_code
            if result.state in {IsloExecutionState.SUCCEEDED, IsloExecutionState.FAILED}
            else None
        )
        return SandboxResult(state, exit_code)

    async def terminate(self, handle: SandboxHandle) -> None:
        ref = IsloSandboxHandle.from_common(handle)
        sandbox_id = await self._client.get_sandbox_id(ref.sandbox_name)
        if sandbox_id is None:
            return
        if sandbox_id != ref.sandbox_id:
            raise IsloProtocolError(
                "refusing to delete an Islo sandbox whose stable ID does not match the handle"
            )
        await self._client.delete_sandbox(ref.sandbox_name)

    async def fence(self, request_id: str) -> None:
        targets = set(getattr(self, "_fence_targets", {}).get(request_id, set()))
        targets.add(sandbox_name_from_request_id(request_id))
        for sandbox_name in sorted(targets):
            await self._client.delete_sandbox(sandbox_name)
        getattr(self, "_fence_targets", {}).pop(request_id, None)

    async def get_output(self, handle: SandboxHandle) -> SandboxOutput:
        result = await self._client.execution_result(IsloSandboxHandle.from_common(handle))
        return SandboxOutput(
            stdout=result.stdout,
            stderr=result.stderr,
            truncated=result.truncated,
        )

    async def close(self) -> None:
        await self._client.close()
