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
"""Asynchronous provider runner for the common sandbox executor."""

from __future__ import annotations

import asyncio
import contextlib
import queue
import threading
import time
from collections.abc import Coroutine
from concurrent.futures import Future
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, TypeAlias, TypeVar, cast

from airflow.providers.common.sandbox.exceptions import (
    SandboxConfigurationError,
    SandboxInvalidHandleError,
    SandboxLaunchUnfencedError,
    SandboxProtocolError,
)
from airflow.providers.common.sandbox.models import (
    RecoveredSandbox,
    SandboxExecutionRef,
    SandboxLaunchOutcome,
    SandboxLaunchRequest,
    SandboxResult,
)

if TYPE_CHECKING:
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.providers.common.sandbox.driver import SandboxDriver, SandboxDriverFactory

T = TypeVar("T")


class SandboxRunnerOperation(str, Enum):
    """Operation completed by ``_SandboxExecutorRunner``."""

    LAUNCH = "launch"
    STATUS = "status"
    TERMINATE = "terminate"
    FENCE = "fence"


@dataclass(frozen=True)
class SandboxRunnerResult:
    """One provider result delivered back to the scheduler thread."""

    operation: SandboxRunnerOperation
    key: TaskInstanceKey | None
    request_id: str
    value: SandboxLaunchOutcome | SandboxResult | None = None
    error: BaseException | None = None


@dataclass(frozen=True)
class _LaunchCommand:
    key: TaskInstanceKey
    request: SandboxLaunchRequest


@dataclass(frozen=True)
class _StatusCommand:
    key: TaskInstanceKey
    ref: SandboxExecutionRef


@dataclass(frozen=True)
class _TerminateCommand:
    key: TaskInstanceKey | None
    ref: SandboxExecutionRef
    best_effort: bool


@dataclass(frozen=True)
class _FenceCommand:
    key: TaskInstanceKey
    request_id: str


_RunnerCommand: TypeAlias = _LaunchCommand | _StatusCommand | _TerminateCommand | _FenceCommand


class _SandboxExecutorRunner:
    """Own one driver and execute provider operations away from the scheduler thread."""

    def __init__(
        self,
        driver_factory: SandboxDriverFactory,
        result_queue: queue.SimpleQueue[SandboxRunnerResult],
        *,
        expected_driver_id: str,
        launch_concurrency: int,
        status_concurrency: int,
        cleanup_concurrency: int,
    ) -> None:
        if min(launch_concurrency, status_concurrency, cleanup_concurrency) <= 0:
            raise ValueError("sandbox runner concurrency limits must be greater than zero")
        self._driver_factory = driver_factory
        self._result_queue = result_queue
        self._expected_driver_id = expected_driver_id
        self._launch_concurrency = launch_concurrency
        self._status_concurrency = status_concurrency
        self._cleanup_concurrency = cleanup_concurrency
        self._command_queue: queue.SimpleQueue[_RunnerCommand] = queue.SimpleQueue()
        self._best_effort_cleanup_slots = threading.BoundedSemaphore(max(1, cleanup_concurrency - 1))
        self._lifecycle_lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._driver: SandboxDriver | None = None
        self._launch_semaphore: asyncio.Semaphore | None = None
        self._status_semaphore: asyncio.Semaphore | None = None
        self._cleanup_semaphore: asyncio.Semaphore | None = None
        self._tasks: dict[asyncio.Task[None], _RunnerCommand] = {}
        self._direct_tasks: set[asyncio.Task[Any]] = set()
        self._ready = threading.Event()
        self._start_error: BaseException | None = None
        self._accepting = True
        self._closed = False

    def start(self) -> None:
        if self._closed:
            raise RuntimeError("sandbox executor runner is closed")
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="sandbox-executor-runner", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=15):
            raise RuntimeError("timed out starting sandbox executor runner")
        if self._start_error is not None:
            raise RuntimeError("failed to start sandbox executor runner") from self._start_error

    def _run(self) -> None:
        loop: asyncio.AbstractEventLoop | None = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            self._driver = self._driver_factory()
            if self._driver.driver_id != self._expected_driver_id:
                raise SandboxConfigurationError(
                    f"sandbox driver id {self._driver.driver_id!r} does not match "
                    f"executor driver id {self._expected_driver_id!r}"
                )
            self._launch_semaphore = asyncio.Semaphore(self._launch_concurrency)
            self._status_semaphore = asyncio.Semaphore(self._status_concurrency)
            self._cleanup_semaphore = asyncio.Semaphore(self._cleanup_concurrency)
        except BaseException as error:
            self._start_error = error
            if loop is not None:
                if self._driver is not None:
                    with contextlib.suppress(Exception):
                        loop.run_until_complete(self._driver.close())
                loop.close()
            self._driver = None
            self._loop = None
            self._ready.set()
            return
        self._ready.set()
        if self._closed:
            loop.run_until_complete(self._driver.close())
            loop.close()
            return
        loop.run_forever()
        loop.close()

    def _enqueue(self, command: _RunnerCommand) -> None:
        with self._lifecycle_lock:
            if not self._accepting or self._closed or self._loop is None:
                raise RuntimeError("sandbox executor runner is not accepting work")
            self._command_queue.put(command)
            self._loop.call_soon_threadsafe(self._drain_commands)

    def _drain_commands(self) -> None:
        while True:
            try:
                command = self._command_queue.get_nowait()
            except queue.Empty:
                return
            task = asyncio.create_task(self._execute(command))
            self._tasks[task] = command
            task.add_done_callback(self._discard_task)

    def _discard_task(self, future: asyncio.Future[None]) -> None:
        self._tasks.pop(cast("asyncio.Task[None]", future), None)

    async def _execute(self, command: _RunnerCommand) -> None:
        operation: SandboxRunnerOperation
        key: TaskInstanceKey | None
        request_id: str
        value: SandboxLaunchOutcome | SandboxResult | None = None
        error: BaseException | None = None
        try:
            if isinstance(command, _LaunchCommand):
                operation = SandboxRunnerOperation.LAUNCH
                key = command.key
                request_id = command.request.request_id
                outcome = await self._launch(command.request)
                value = outcome
            elif isinstance(command, _StatusCommand):
                operation = SandboxRunnerOperation.STATUS
                key = command.key
                request_id = command.ref.request_id
                value = await self._get_status(command.ref)
            elif isinstance(command, _TerminateCommand):
                operation = SandboxRunnerOperation.TERMINATE
                key = command.key
                request_id = command.ref.request_id
                await self._terminate(command.ref)
            else:
                operation = SandboxRunnerOperation.FENCE
                key = command.key
                request_id = command.request_id
                await self._fence(command.request_id)
        except asyncio.CancelledError:
            raise
        except BaseException as caught_error:
            error = caught_error
        finally:
            if isinstance(command, _TerminateCommand) and command.best_effort:
                self._best_effort_cleanup_slots.release()
        self._result_queue.put(
            SandboxRunnerResult(
                operation=operation,
                key=key,
                request_id=request_id,
                value=value,
                error=error,
            )
        )

    async def _launch(self, request: SandboxLaunchRequest) -> SandboxLaunchOutcome:
        driver = cast("SandboxDriver", self._driver)
        semaphore = cast("asyncio.Semaphore", self._launch_semaphore)
        async with semaphore:
            try:
                handle = await driver.launch(request)
                driver.validate_handle(handle, request_id=request.request_id)
                return SandboxLaunchOutcome(
                    SandboxExecutionRef(
                        driver=driver.driver_id,
                        request_id=request.request_id,
                        handle=handle,
                        keep=request.keep,
                    )
                )
            except SandboxLaunchUnfencedError:
                raise
            except asyncio.CancelledError as launch_error:
                await self._fence_failed_launch(request.request_id, launch_error)
                raise
            except Exception as launch_error:
                await self._fence_failed_launch(request.request_id, launch_error)
                raise

    async def _fence_failed_launch(
        self,
        request_id: str,
        launch_error: BaseException,
    ) -> None:
        try:
            await self._fence(request_id)
        except BaseException as fence_error:
            raise SandboxLaunchUnfencedError(request_id, launch_error, fence_error) from launch_error

    async def _get_status(self, ref: SandboxExecutionRef) -> SandboxResult:
        driver = cast("SandboxDriver", self._driver)
        semaphore = cast("asyncio.Semaphore", self._status_semaphore)
        async with semaphore:
            self._validate_ref(ref)
            try:
                result = await driver.get_status(ref.handle)
            except SandboxConfigurationError as error:
                raise SandboxProtocolError(f"sandbox driver returned an invalid status: {error}") from error
            if not isinstance(result, SandboxResult):
                raise SandboxProtocolError("sandbox driver returned a non-SandboxResult status")
            return result

    async def _terminate(self, ref: SandboxExecutionRef) -> None:
        semaphore = cast("asyncio.Semaphore", self._cleanup_semaphore)
        async with semaphore:
            self._validate_ref(ref)
            await cast("SandboxDriver", self._driver).terminate(ref.handle)

    async def _fence(self, request_id: str) -> None:
        driver = cast("SandboxDriver", self._driver)
        semaphore = cast("asyncio.Semaphore", self._cleanup_semaphore)
        async with semaphore:
            await driver.fence(request_id)

    def submit_launch(self, key: TaskInstanceKey, request: SandboxLaunchRequest) -> None:
        self._enqueue(_LaunchCommand(key=key, request=request))

    def submit_status(self, key: TaskInstanceKey, ref: SandboxExecutionRef) -> None:
        self._enqueue(_StatusCommand(key=key, ref=ref))

    def submit_terminate(
        self,
        ref: SandboxExecutionRef,
        *,
        key: TaskInstanceKey | None = None,
        required: bool,
    ) -> bool:
        best_effort = not required
        if best_effort and not self._best_effort_cleanup_slots.acquire(blocking=False):
            return False
        try:
            self._enqueue(_TerminateCommand(key=key, ref=ref, best_effort=best_effort))
        except Exception:
            if best_effort:
                self._best_effort_cleanup_slots.release()
            raise
        return True

    def submit_fence(self, key: TaskInstanceKey, request_id: str) -> None:
        self._enqueue(_FenceCommand(key=key, request_id=request_id))

    async def _track_direct_coroutine(self, coroutine: Coroutine[Any, Any, T]) -> T:
        task = asyncio.current_task()
        if task is None:
            raise RuntimeError("sandbox executor runner coroutine has no task")
        self._direct_tasks.add(task)
        try:
            return await coroutine
        finally:
            self._direct_tasks.discard(task)

    def _submit_coroutine(self, coroutine: Coroutine[Any, Any, T]) -> Future[T]:
        with self._lifecycle_lock:
            if self._closed or not self._accepting or self._loop is None:
                coroutine.close()
                raise RuntimeError("sandbox executor runner is not running")
            tracked_coroutine = self._track_direct_coroutine(coroutine)
            try:
                return asyncio.run_coroutine_threadsafe(tracked_coroutine, self._loop)
            except Exception:
                tracked_coroutine.close()
                coroutine.close()
                raise

    async def _health_check(self) -> None:
        await cast("SandboxDriver", self._driver).health_check()

    def submit_health_check(self) -> Future[None]:
        return self._submit_coroutine(self._health_check())

    async def _validate(self, ref: SandboxExecutionRef) -> None:
        self._validate_ref(ref)

    def _validate_ref(self, ref: SandboxExecutionRef) -> None:
        if ref.driver != self._expected_driver_id:
            raise SandboxInvalidHandleError(
                f"sandbox execution reference belongs to driver {ref.driver!r}, "
                f"expected {self._expected_driver_id!r}"
            )
        cast("SandboxDriver", self._driver).validate_handle(ref.handle, request_id=ref.request_id)

    def submit_validate(self, ref: SandboxExecutionRef) -> Future[None]:
        return self._submit_coroutine(self._validate(ref))

    async def _recover(self, request_id: str) -> SandboxLaunchOutcome | None:
        driver = cast("SandboxDriver", self._driver)
        semaphore = cast("asyncio.Semaphore", self._cleanup_semaphore)
        async with semaphore:
            try:
                recovered = await driver.recover(request_id)
                if recovered is None:
                    return None
                if not isinstance(recovered, RecoveredSandbox):
                    raise SandboxInvalidHandleError("sandbox driver returned an invalid recovery result")
                driver.validate_handle(recovered.handle, request_id=request_id)
                return SandboxLaunchOutcome(
                    SandboxExecutionRef(
                        driver=driver.driver_id,
                        request_id=request_id,
                        handle=recovered.handle,
                        keep=recovered.keep,
                    )
                )
            except asyncio.CancelledError:
                raise
            except Exception as recovery_error:
                try:
                    await driver.fence(request_id)
                except BaseException as fence_error:
                    raise SandboxLaunchUnfencedError(
                        request_id,
                        recovery_error,
                        fence_error,
                    ) from recovery_error
                return None

    def submit_recover(self, request_id: str) -> Future[SandboxLaunchOutcome | None]:
        return self._submit_coroutine(self._recover(request_id))

    def close(self, timeout: float = 10.0) -> bool:
        with self._lifecycle_lock:
            if self._closed:
                return self._thread is None or not self._thread.is_alive()
            self._accepting = False
            self._closed = True
        loop = self._loop
        driver = self._driver
        completed = True
        deadline = time.monotonic() + timeout
        if loop is not None and not loop.is_closed() and driver is not None:

            async def shutdown() -> None:
                try:
                    self._drain_commands()
                    command_tasks = list(self._tasks)
                    for task in command_tasks:
                        command = self._tasks.get(task)
                        if isinstance(command, _StatusCommand) or (
                            isinstance(command, _TerminateCommand) and command.best_effort
                        ):
                            task.cancel()
                    owned_tasks = [*command_tasks, *self._direct_tasks]
                    if owned_tasks:
                        await asyncio.gather(*owned_tasks, return_exceptions=True)
                    await driver.close()
                finally:
                    loop.call_soon(loop.stop)

            shutdown_coroutine = shutdown()
            shutdown_future: Future[None] | None = None
            try:
                shutdown_future = asyncio.run_coroutine_threadsafe(shutdown_coroutine, loop)
                shutdown_future.result(timeout=max(0.0, deadline - time.monotonic()))
            except Exception:
                completed = False
                if shutdown_future is None:
                    shutdown_coroutine.close()
                elif shutdown_future.done():
                    with contextlib.suppress(Exception):
                        shutdown_future.result()
        if self._thread is not None:
            self._thread.join(timeout=max(0.0, deadline - time.monotonic()))
            completed = completed and not self._thread.is_alive()
        return completed
