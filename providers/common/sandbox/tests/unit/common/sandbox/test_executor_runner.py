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
from __future__ import annotations

import asyncio
import queue
import threading
from unittest import mock
from uuid import uuid4

import pytest

from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.providers.common.sandbox.driver import SandboxDriver
from airflow.providers.common.sandbox.exceptions import (
    SandboxConfigurationError,
    SandboxInvalidHandleError,
    SandboxLaunchUnfencedError,
    SandboxProtocolError,
)
from airflow.providers.common.sandbox.executor_runner import (
    SandboxRunnerOperation,
    SandboxRunnerResult,
    _SandboxExecutorRunner,
)
from airflow.providers.common.sandbox.models import (
    SandboxExecutionRef,
    SandboxHandle,
    SandboxLaunchRequest,
    SandboxResult,
    SandboxState,
)


def make_request() -> SandboxLaunchRequest:
    return SandboxLaunchRequest(
        request_id=str(uuid4()),
        command=("true",),
        env={},
        provider_config={},
        workdir=None,
        timeout_seconds=10,
        ttl_seconds=20,
    )


def make_ref(request_id: str, *, driver: str = "fake") -> SandboxExecutionRef:
    return SandboxExecutionRef(
        driver=driver,
        request_id=request_id,
        handle=SandboxHandle({"job_id": request_id}),
    )


def make_runner(driver, *, cleanup_concurrency: int = 1):
    results: queue.SimpleQueue[SandboxRunnerResult] = queue.SimpleQueue()
    runner = _SandboxExecutorRunner(
        lambda: driver,
        results,
        expected_driver_id="fake",
        launch_concurrency=1,
        status_concurrency=1,
        cleanup_concurrency=cleanup_concurrency,
    )
    runner._driver = driver
    runner._launch_semaphore = asyncio.Semaphore(1)
    runner._status_semaphore = asyncio.Semaphore(1)
    runner._cleanup_semaphore = asyncio.Semaphore(cleanup_concurrency)
    return runner, results


@pytest.mark.asyncio
@pytest.mark.parametrize("fence_fails", [False, True])
async def test_failed_launch_is_fenced_before_the_error_is_returned(fence_fails: bool) -> None:
    driver = mock.AsyncMock(spec=SandboxDriver)
    driver.driver_id = "fake"
    driver.launch.side_effect = TimeoutError("launch response lost")
    if fence_fails:
        driver.fence.side_effect = OSError("fence failed")
    runner, _ = make_runner(driver)
    request = make_request()

    expected_error = SandboxLaunchUnfencedError if fence_fails else TimeoutError
    with pytest.raises(expected_error):
        await runner._launch(request)

    driver.fence.assert_awaited_once_with(request.request_id)


@pytest.mark.asyncio
async def test_invalid_launch_handle_is_fenced() -> None:
    driver = mock.AsyncMock(spec=SandboxDriver)
    driver.driver_id = "fake"
    driver.launch.return_value = SandboxHandle({"payload": "x" * 70_000})
    runner, _ = make_runner(driver)
    request = make_request()

    with pytest.raises(SandboxConfigurationError, match="too large"):
        await runner._launch(request)

    driver.fence.assert_awaited_once_with(request.request_id)


@pytest.mark.asyncio
async def test_status_rejects_a_non_result_response() -> None:
    driver = mock.AsyncMock(spec=SandboxDriver)
    driver.driver_id = "fake"
    driver.get_status.return_value = object()
    runner, _ = make_runner(driver)
    ref = make_ref(str(uuid4()))

    with pytest.raises(SandboxProtocolError, match="non-SandboxResult"):
        await runner._get_status(ref)


@pytest.mark.asyncio
async def test_ref_from_another_driver_is_rejected_before_provider_access() -> None:
    driver = mock.AsyncMock(spec=SandboxDriver)
    driver.driver_id = "fake"
    runner, _ = make_runner(driver)
    ref = make_ref(str(uuid4()), driver="other")

    with pytest.raises(SandboxInvalidHandleError, match="belongs to driver"):
        await runner._get_status(ref)

    driver.get_status.assert_not_awaited()


@pytest.mark.asyncio
async def test_invalid_recovery_result_is_fenced() -> None:
    driver = mock.AsyncMock(spec=SandboxDriver)
    driver.driver_id = "fake"
    driver.recover.return_value = object()
    runner, _ = make_runner(driver)
    request_id = str(uuid4())

    assert await runner._recover(request_id) is None
    driver.fence.assert_awaited_once_with(request_id)


@pytest.mark.parametrize("cleanup_concurrency", [1, 2])
@mock.patch.object(_SandboxExecutorRunner, "_enqueue", autospec=True)
def test_best_effort_cleanup_is_bounded(mock_enqueue, cleanup_concurrency: int) -> None:
    driver = mock.AsyncMock(spec=SandboxDriver)
    driver.driver_id = "fake"
    runner, _ = make_runner(driver, cleanup_concurrency=cleanup_concurrency)
    ref = make_ref(str(uuid4()))

    assert runner.submit_terminate(ref, required=False) is True
    assert runner.submit_terminate(ref, required=False) is False
    assert mock_enqueue.call_count == 1


class BlockingDriver(SandboxDriver):
    driver_id = "fake"

    def __init__(self, blocked_operation: str) -> None:
        self.blocked_operation = blocked_operation
        self.started = threading.Event()
        self.release = threading.Event()
        self.closed = threading.Event()

    async def _block(self, operation: str) -> None:
        if self.blocked_operation != operation:
            return
        self.started.set()
        await asyncio.to_thread(self.release.wait)

    async def health_check(self) -> None:
        return None

    async def launch(self, request: SandboxLaunchRequest) -> SandboxHandle:
        await self._block("launch")
        return SandboxHandle({"job_id": request.request_id})

    def validate_handle(self, handle: SandboxHandle, *, request_id: str) -> None:
        if handle.data.get("job_id") != request_id:
            raise SandboxInvalidHandleError("request ID mismatch")

    async def get_status(self, handle: SandboxHandle) -> SandboxResult:
        return SandboxResult(SandboxState.RUNNING)

    async def terminate(self, handle: SandboxHandle) -> None:
        return None

    async def fence(self, request_id: str) -> None:
        await self._block("fence")

    async def close(self) -> None:
        self.closed.set()


class BackgroundTaskDriver(BlockingDriver):
    def __init__(self) -> None:
        super().__init__(blocked_operation="")
        self.background_task: asyncio.Task[bool] | None = None

    async def health_check(self) -> None:
        self.background_task = asyncio.create_task(asyncio.Event().wait())

    async def close(self) -> None:
        if self.background_task is not None:
            self.background_task.cancel()
            await asyncio.gather(self.background_task, return_exceptions=True)
        await super().close()


def test_close_leaves_driver_background_tasks_to_driver_close() -> None:
    driver = BackgroundTaskDriver()
    runner = _SandboxExecutorRunner(
        lambda: driver,
        queue.SimpleQueue(),
        expected_driver_id="fake",
        launch_concurrency=1,
        status_concurrency=1,
        cleanup_concurrency=1,
    )
    runner.start()

    runner.submit_health_check().result(timeout=2)

    assert runner.close(timeout=2) is True
    assert driver.closed.is_set()
    assert driver.background_task is not None
    assert driver.background_task.cancelled()


@pytest.mark.parametrize(
    ("operation", "expected_operation"),
    [
        pytest.param("launch", SandboxRunnerOperation.LAUNCH, id="launch"),
        pytest.param("fence", SandboxRunnerOperation.FENCE, id="required-fence"),
    ],
)
def test_close_does_not_cancel_required_lifecycle_work(
    operation: str,
    expected_operation: SandboxRunnerOperation,
) -> None:
    driver = BlockingDriver(operation)
    results: queue.SimpleQueue[SandboxRunnerResult] = queue.SimpleQueue()
    runner = _SandboxExecutorRunner(
        lambda: driver,
        results,
        expected_driver_id="fake",
        launch_concurrency=1,
        status_concurrency=1,
        cleanup_concurrency=1,
    )
    request = make_request()
    key = TaskInstanceKey("dag", "task", "run", 1, -1)
    runner.start()
    try:
        if operation == "launch":
            runner.submit_launch(key, request)
        else:
            runner.submit_fence(key, request.request_id)
        assert driver.started.wait(timeout=2)

        assert runner.close(timeout=0) is False
        assert not driver.closed.is_set()

        driver.release.set()
        assert driver.closed.wait(timeout=2)
        result = results.get(timeout=2)
        assert result.operation is expected_operation
        assert result.error is None
    finally:
        driver.release.set()
        if runner._thread is not None:
            runner._thread.join(timeout=2)
