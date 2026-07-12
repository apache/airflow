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
"""Provider-neutral executor engine for one-sandbox-per-task execution."""

from __future__ import annotations

import asyncio
import contextlib
import random
import threading
import time
from abc import ABC, abstractmethod
from collections.abc import Coroutine, Sequence
from concurrent.futures import CancelledError, Future, wait
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast

from airflow.executors.base_executor import BaseExecutor, get_execution_api_server_url
from airflow.executors.workloads import ExecuteTask, ExecutorWorkload
from airflow.providers.common.sandbox.exceptions import (
    SandboxConfigurationError,
    SandboxInvalidHandleError,
    SandboxLaunchUnfencedError,
)
from airflow.providers.common.sandbox.models import (
    RecoveredSandbox,
    RunningSandbox,
    SandboxExecutionRef,
    SandboxLaunchConfig,
    SandboxLaunchRequest,
    SandboxOutput,
    SandboxResult,
    SandboxState,
    is_preassigned_executor_id,
)
from airflow.utils.log.logging_mixin import remove_escape_codes

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.providers.common.sandbox.driver import SandboxDriver, SandboxDriverFactory

T = TypeVar("T")

__all__ = ["BaseSandboxExecutor"]


@dataclass(frozen=True)
class _PendingLaunch:
    key: TaskInstanceKey
    request_id: str


@dataclass(frozen=True)
class _Cleanup:
    request_id: str
    ref: SandboxExecutionRef | None = None
    fail_key: TaskInstanceKey | None = None
    failure_info: str | None = None


@dataclass
class _Fence:
    request_id: str
    failure_info: str
    attempts: int = 0
    next_attempt: float = 0.0


@dataclass
class _CleanupFence:
    request_id: str
    attempts: int = 0
    next_attempt: float = 0.0


class _SandboxExecutorManager:
    """Own an asyncio loop, one driver, and bounded provider API concurrency."""

    def __init__(
        self,
        driver_factory: SandboxDriverFactory,
        *,
        expected_driver_id: str,
        launch_concurrency: int,
        status_concurrency: int,
    ) -> None:
        if launch_concurrency <= 0 or status_concurrency <= 0:
            raise ValueError("sandbox executor manager concurrency limits must be greater than zero")
        self._driver_factory = driver_factory
        self._expected_driver_id = expected_driver_id
        self._launch_concurrency = launch_concurrency
        self._status_concurrency = status_concurrency
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._driver: SandboxDriver | None = None
        self._launch_semaphore: asyncio.Semaphore | None = None
        self._status_semaphore: asyncio.Semaphore | None = None
        self._ready = threading.Event()
        self._start_error: BaseException | None = None
        self._closed = False

    def start(self) -> None:
        if self._closed:
            raise RuntimeError("sandbox executor manager is closed")
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="sandbox-executor-manager", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=15):
            raise RuntimeError("timed out starting sandbox executor manager")
        if self._start_error is not None:
            raise RuntimeError("failed to start sandbox executor manager") from self._start_error

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

    def _submit(self, coroutine: Coroutine[Any, Any, T]) -> Future[T]:
        if self._closed or self._loop is None:
            coroutine.close()
            raise RuntimeError("sandbox executor manager is not running")
        try:
            return asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        except Exception:
            coroutine.close()
            raise

    async def _health_check(self) -> None:
        await cast("SandboxDriver", self._driver).health_check()

    def submit_health_check(self) -> Future[None]:
        return self._submit(self._health_check())

    async def _launch(self, request: SandboxLaunchRequest) -> RunningSandbox:
        driver = cast("SandboxDriver", self._driver)
        semaphore = cast("asyncio.Semaphore", self._launch_semaphore)
        async with semaphore:
            try:
                handle = await driver.launch(request)
                driver.validate_handle(handle, request_id=request.request_id)
                running = RunningSandbox(
                    ref=SandboxExecutionRef(
                        driver=driver.driver_id,
                        request_id=request.request_id,
                        handle=handle,
                        keep=request.keep,
                    )
                )
            except SandboxLaunchUnfencedError:
                raise
            except BaseException as launch_error:
                try:
                    await driver.fence(request.request_id)
                except BaseException as fence_error:
                    raise SandboxLaunchUnfencedError(
                        request.request_id, launch_error, fence_error
                    ) from launch_error
                raise
            return running

    def submit_launch(self, request: SandboxLaunchRequest) -> Future[RunningSandbox]:
        return self._submit(self._launch(request))

    async def _status(self, ref: SandboxExecutionRef) -> SandboxResult:
        driver = cast("SandboxDriver", self._driver)
        semaphore = cast("asyncio.Semaphore", self._status_semaphore)
        async with semaphore:
            driver.validate_handle(ref.handle, request_id=ref.request_id)
            return await driver.get_status(ref.handle)

    def submit_status(self, ref: SandboxExecutionRef) -> Future[SandboxResult]:
        return self._submit(self._status(ref))

    async def _terminate(self, ref: SandboxExecutionRef) -> None:
        driver = cast("SandboxDriver", self._driver)
        semaphore = cast("asyncio.Semaphore", self._launch_semaphore)
        async with semaphore:
            await driver.terminate(ref.handle)

    def submit_terminate(self, ref: SandboxExecutionRef) -> Future[None]:
        return self._submit(self._terminate(ref))

    async def _fence(self, request_id: str) -> None:
        driver = cast("SandboxDriver", self._driver)
        semaphore = cast("asyncio.Semaphore", self._launch_semaphore)
        async with semaphore:
            await driver.fence(request_id)

    def submit_fence(self, request_id: str) -> Future[None]:
        return self._submit(self._fence(request_id))

    async def _validate(self, ref: SandboxExecutionRef) -> None:
        driver = cast("SandboxDriver", self._driver)
        driver.validate_handle(ref.handle, request_id=ref.request_id)

    def submit_validate(self, ref: SandboxExecutionRef) -> Future[None]:
        return self._submit(self._validate(ref))

    async def _recover(self, request_id: str) -> RunningSandbox | None:
        driver = cast("SandboxDriver", self._driver)
        semaphore = cast("asyncio.Semaphore", self._launch_semaphore)
        async with semaphore:
            recovered = await driver.recover(request_id)
            if recovered is None:
                return None
            if not isinstance(recovered, RecoveredSandbox):
                raise SandboxInvalidHandleError("sandbox driver returned an invalid recovery result")
            try:
                driver.validate_handle(recovered.handle, request_id=request_id)
                return RunningSandbox(
                    SandboxExecutionRef(
                        driver=driver.driver_id,
                        request_id=request_id,
                        handle=recovered.handle,
                        keep=recovered.keep,
                    )
                )
            except BaseException as recovery_error:
                try:
                    await driver.fence(request_id)
                except BaseException as fence_error:
                    raise SandboxLaunchUnfencedError(
                        request_id,
                        recovery_error,
                        fence_error,
                    ) from recovery_error
                return None

    def submit_recover(self, request_id: str) -> Future[RunningSandbox | None]:
        return self._submit(self._recover(request_id))

    def close(self, timeout: float = 10.0) -> None:
        if self._closed:
            return
        self._closed = True
        loop = self._loop
        driver = self._driver
        if loop is not None and not loop.is_closed() and driver is not None:

            async def shutdown() -> None:
                current = asyncio.current_task()
                tasks = [task for task in asyncio.all_tasks() if task is not current]
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                await driver.close()

            shutdown_coroutine = shutdown()
            shutdown_future: Future[None] | None = None
            try:
                shutdown_future = asyncio.run_coroutine_threadsafe(shutdown_coroutine, loop)
                shutdown_future.result(timeout=timeout)
            except Exception:
                if shutdown_future is None:
                    shutdown_coroutine.close()
                else:
                    shutdown_future.cancel()
            with contextlib.suppress(RuntimeError):
                loop.call_soon_threadsafe(loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=timeout)


class BaseSandboxExecutor(BaseExecutor, ABC):
    """Reusable executor state machine for a concrete provider-owned sandbox executor."""

    is_local = False
    is_production = True
    serve_logs = False
    supports_ad_hoc_ti_run = False
    supports_multi_team = True
    pre_assigns_external_executor_id: ClassVar[bool] = True

    driver_id: ClassVar[str]
    config_section: ClassVar[str]

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if not getattr(type(self), "driver_id", "") or not getattr(type(self), "config_section", ""):
            raise TypeError("concrete sandbox executors must define driver_id and config_section")
        self._manager: _SandboxExecutorManager | None = None
        self._launch_futures: dict[Future, _PendingLaunch] = {}
        self._status_futures: dict[Future, tuple[TaskInstanceKey, SandboxExecutionRef]] = {}
        self._cleanup_futures: dict[Future, _Cleanup] = {}
        self._cleanup_fences: dict[str, _CleanupFence] = {}
        self._active: dict[TaskInstanceKey, RunningSandbox] = {}
        self._fences: dict[TaskInstanceKey, _Fence] = {}
        self._revoked: set[TaskInstanceKey] = set()
        self._next_poll: dict[TaskInstanceKey, float] = {}
        self._poll_failures: dict[TaskInstanceKey, int] = {}
        self._closing = False
        section = self.config_section
        self._poll_interval = self.conf.getint(section, "poll_interval", fallback=2)
        self._max_poll_interval = self.conf.getint(section, "max_poll_interval", fallback=30)
        self._creation_batch_size = self.conf.getint(section, "creation_batch_size", fallback=128)
        self._status_batch_size = self.conf.getint(section, "status_batch_size", fallback=1000)
        self._shutdown_timeout = self.conf.getint(section, "shutdown_timeout", fallback=60)
        self._adoption_timeout = self.conf.getint(section, "adoption_timeout", fallback=30)
        self._health_check_timeout = self.conf.getint(section, "health_check_timeout", fallback=30)
        self._launch_concurrency = self.conf.getint(section, "launch_concurrency", fallback=32)
        self._status_concurrency = self.conf.getint(section, "status_concurrency", fallback=128)
        self._validate_configuration()

    def _validate_configuration(self) -> None:
        positive_options = {
            "adoption_timeout": self._adoption_timeout,
            "creation_batch_size": self._creation_batch_size,
            "health_check_timeout": self._health_check_timeout,
            "launch_concurrency": self._launch_concurrency,
            "max_poll_interval": self._max_poll_interval,
            "poll_interval": self._poll_interval,
            "shutdown_timeout": self._shutdown_timeout,
            "status_batch_size": self._status_batch_size,
            "status_concurrency": self._status_concurrency,
        }
        if invalid := sorted(name for name, value in positive_options.items() if value <= 0):
            raise SandboxConfigurationError(
                f"sandbox executor configuration must be greater than zero: {', '.join(invalid)}"
            )
        if self._max_poll_interval < self._poll_interval:
            raise SandboxConfigurationError(
                "max_poll_interval must be greater than or equal to poll_interval"
            )

    @abstractmethod
    def get_driver_factory(self) -> SandboxDriverFactory:
        """Resolve credentials synchronously and return a thread-safe driver factory."""

    @abstractmethod
    def build_launch_config(self, workload: ExecuteTask, request_id: str) -> SandboxLaunchConfig:
        """Validate provider task configuration and prepare one launch."""

    def start(self) -> None:
        if not self.conf.getboolean("logging", "remote_logging", fallback=False):
            raise SandboxConfigurationError(
                f"{type(self).__name__} requires [logging] remote_logging=True because completed "
                "sandboxes are ephemeral"
            )
        if self._manager is None:
            self._manager = _SandboxExecutorManager(
                self.get_driver_factory(),
                expected_driver_id=self.driver_id,
                launch_concurrency=self._launch_concurrency,
                status_concurrency=self._status_concurrency,
            )
        try:
            self._manager.start()
            if self.conf.getboolean(self.config_section, "check_health_on_startup", fallback=True):
                self._manager.submit_health_check().result(timeout=self._health_check_timeout)
        except Exception:
            self._manager.close()
            raise

    def _process_workloads(self, workload_items: Sequence[ExecutorWorkload]) -> None:
        if self._closing:
            return
        if self._manager is None:
            raise RuntimeError(f"{type(self).__name__}.start() must be called before processing workloads")
        capacity = max(0, self._creation_batch_size - len(self._launch_futures))
        for workload in workload_items[:capacity]:
            if not isinstance(workload, ExecuteTask):
                raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(workload)}")
            key = workload.ti.key
            del self.queued_tasks[key]
            self.running.add(key)
            try:
                request_id = workload.ti.external_executor_id
                if not is_preassigned_executor_id(request_id):
                    raise SandboxConfigurationError(
                        "task has no pre-assigned external executor ID; sandbox executors require Airflow 3.3+"
                    )
                launch_config = self.build_launch_config(workload, request_id)
                request = SandboxLaunchRequest(
                    request_id=request_id,
                    command=tuple(self._workload_command(workload)),
                    env=self._runtime_env(launch_config.env),
                    provider_config=launch_config.provider_config,
                    workdir=launch_config.workdir,
                    timeout_seconds=launch_config.timeout_seconds,
                    ttl_seconds=launch_config.ttl_seconds,
                    keep=launch_config.keep,
                )
                future = self._manager.submit_launch(request)
                self._launch_futures[future] = _PendingLaunch(key=key, request_id=request_id)
            except Exception as error:
                self.log.exception("Unable to configure sandbox for %s", key)
                self.fail(key, info=str(error))

    @staticmethod
    def _workload_command(workload: ExecuteTask) -> list[str]:
        return [
            "python",
            "-m",
            "airflow.sdk.execution_time.execute_workload",
            "--json-string",
            workload.model_dump_json(),
        ]

    def _runtime_env(self, task_env: dict[str, str]) -> dict[str, str]:
        env = dict(task_env)
        env.update(
            {
                "AIRFLOW_IS_EXECUTOR_CONTAINER": "true",
                "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": get_execution_api_server_url(self.conf),
                "AIRFLOW__LOGGING__REMOTE_LOGGING": "True",
            }
        )
        for option in ("remote_base_log_folder", "remote_log_conn_id", "remote_task_handler_kwargs"):
            if value := self.conf.get("logging", option, fallback=None):
                env[f"AIRFLOW__LOGGING__{option.upper()}"] = str(value)
        return env

    def sync(self) -> None:
        self._drain_launches()
        self._drain_statuses()
        self._drain_cleanups()
        self._schedule_fences()
        self._schedule_statuses()

    def _drain_launches(self) -> None:
        for future, pending in list(self._launch_futures.items()):
            if not future.done():
                continue
            del self._launch_futures[future]
            try:
                running = future.result()
            except CancelledError:
                if pending.key not in self._revoked:
                    self.fail(pending.key, info="sandbox launch was cancelled")
                else:
                    self._maybe_finish_revocation(pending.key)
            except SandboxLaunchUnfencedError as error:
                self.log.error(
                    "Launch for %s became ambiguous; fencing request %s before reporting failure",
                    pending.key,
                    error.request_id,
                )
                self._fences[pending.key] = _Fence(
                    request_id=error.request_id,
                    failure_info=str(error.launch_error),
                )
            except Exception as error:
                self.log.exception("Sandbox launch failed for %s", pending.key)
                self.fail(pending.key, info=str(error))
            else:
                if pending.key in self._revoked:
                    self._submit_cleanup(
                        running.ref,
                        fail_key=pending.key,
                        failure_info="sandbox task was revoked during launch",
                    )
                    self.running.discard(pending.key)
                    continue
                self._active[pending.key] = running
                self._next_poll[pending.key] = 0.0
                self.running_state(pending.key, info=running.external_executor_id)

    def _drain_statuses(self) -> None:
        for future, (key, ref) in list(self._status_futures.items()):
            if not future.done():
                continue
            del self._status_futures[future]
            current = self._active.get(key)
            if current is None or current.ref != ref:
                continue
            try:
                result = future.result()
            except SandboxInvalidHandleError as error:
                del self._active[key]
                self._next_poll.pop(key, None)
                self._poll_failures.pop(key, None)
                self._fence_request(key, ref.request_id, str(error))
                continue
            except Exception:
                self.log.warning("Transient sandbox status error for %s", key, exc_info=True)
                self._defer_poll(key, failed=True)
                continue
            if result.state in {SandboxState.PENDING, SandboxState.RUNNING}:
                self._defer_poll(key, failed=False, retry_after=result.retry_after)
                continue
            del self._active[key]
            self._next_poll.pop(key, None)
            self._poll_failures.pop(key, None)
            if result.state is SandboxState.SUCCEEDED:
                self.success(key)
            elif result.state is SandboxState.GONE:
                self.fail(key, info=result.message or f"sandbox {self._display_name(ref)} no longer exists")
            else:
                self.fail(
                    key,
                    info=result.message or f"sandbox workload exited with code {result.exit_code}",
                )
            if not current.keep:
                self._submit_cleanup(ref)

    def _defer_poll(
        self,
        key: TaskInstanceKey,
        *,
        failed: bool,
        retry_after: float | None = None,
    ) -> None:
        failures = self._poll_failures.get(key, 0) + 1 if failed else 0
        self._poll_failures[key] = failures
        interval = retry_after or self._poll_interval * (2 ** min(failures, 8))
        interval = min(interval, self._max_poll_interval)
        self._next_poll[key] = time.monotonic() + interval + random.uniform(0, min(interval / 5, 1.0))

    def _schedule_statuses(self) -> None:
        if self._manager is None or self._closing:
            return
        capacity = max(0, self._status_batch_size - len(self._status_futures))
        now = time.monotonic()
        in_progress = {key for key, _ in self._status_futures.values()}
        due = [
            (key, running)
            for key, running in self._active.items()
            if key not in in_progress and self._next_poll.get(key, 0.0) <= now
        ]
        due.sort(key=lambda item: self._next_poll.get(item[0], 0.0))
        for key, running in due[:capacity]:
            try:
                future = self._manager.submit_status(running.ref)
            except Exception:
                self.log.warning("Unable to submit sandbox status request for %s", key, exc_info=True)
                self._defer_poll(key, failed=True)
            else:
                self._status_futures[future] = (key, running.ref)

    def _submit_cleanup(
        self,
        ref: SandboxExecutionRef | None = None,
        *,
        request_id: str | None = None,
        fail_key: TaskInstanceKey | None = None,
        failure_info: str | None = None,
    ) -> Future | None:
        if self._manager is None:
            return None
        if ref is None and request_id is None:
            raise ValueError("cleanup requires a sandbox reference or request ID")
        cleanup_request_id = ref.request_id if ref is not None else cast("str", request_id)
        try:
            future = (
                self._manager.submit_terminate(ref)
                if ref is not None
                else self._manager.submit_fence(cleanup_request_id)
            )
        except Exception:
            self.log.warning("Unable to submit sandbox cleanup for %s", cleanup_request_id, exc_info=True)
            if fail_key is not None:
                fence = self._fences.setdefault(
                    fail_key,
                    _Fence(cleanup_request_id, failure_info or "ambiguous sandbox launch"),
                )
                fence.attempts += 1
                fence.next_attempt = time.monotonic() + min(2**fence.attempts, 30)
            else:
                self._defer_cleanup_fence(cleanup_request_id)
            return None
        self._cleanup_futures[future] = _Cleanup(
            request_id=cleanup_request_id,
            ref=ref,
            fail_key=fail_key,
            failure_info=failure_info,
        )
        return future

    def _drain_cleanups(self) -> None:
        for future, cleanup in list(self._cleanup_futures.items()):
            if not future.done():
                continue
            del self._cleanup_futures[future]
            try:
                future.result()
            except Exception:
                self.log.warning("Unable to clean up sandbox request %s", cleanup.request_id, exc_info=True)
                if cleanup.fail_key is not None:
                    fence = self._fences.setdefault(
                        cleanup.fail_key,
                        _Fence(
                            cleanup.request_id,
                            cleanup.failure_info or "ambiguous sandbox launch",
                        ),
                    )
                    fence.attempts += 1
                    fence.next_attempt = time.monotonic() + min(2**fence.attempts, 30)
                else:
                    self._defer_cleanup_fence(cleanup.request_id)
            else:
                if cleanup.fail_key is not None:
                    self._fences.pop(cleanup.fail_key, None)
                    if cleanup.fail_key in self._revoked:
                        self.running.discard(cleanup.fail_key)
                        self._maybe_finish_revocation(cleanup.fail_key)
                    else:
                        self.fail(cleanup.fail_key, info=cleanup.failure_info)
                else:
                    self._cleanup_fences.pop(cleanup.request_id, None)

    def _defer_cleanup_fence(self, request_id: str) -> None:
        fence = self._cleanup_fences.setdefault(request_id, _CleanupFence(request_id))
        fence.attempts += 1
        fence.next_attempt = time.monotonic() + min(2**fence.attempts, 30)

    def _schedule_fences(self) -> None:
        if self._manager is None:
            return
        pending_keys = {
            cleanup.fail_key for cleanup in self._cleanup_futures.values() if cleanup.fail_key is not None
        }
        pending_request_ids = {
            cleanup.request_id for cleanup in self._cleanup_futures.values() if cleanup.fail_key is None
        }
        now = time.monotonic()
        for key, fence in list(self._fences.items()):
            if key in pending_keys or fence.next_attempt > now:
                continue
            self._submit_cleanup(
                request_id=fence.request_id,
                fail_key=key,
                failure_info=fence.failure_info,
            )
        for request_id, cleanup_fence in list(self._cleanup_fences.items()):
            if request_id in pending_request_ids or cleanup_fence.next_attempt > now:
                continue
            self._submit_cleanup(request_id=request_id)

    def _fence_request(self, key: TaskInstanceKey, request_id: str, failure_info: str) -> None:
        """Retry fencing until a sandbox that must not keep running is confirmed gone."""
        self._fences[key] = _Fence(request_id=request_id, failure_info=failure_info)
        if not any(cleanup.fail_key == key for cleanup in self._cleanup_futures.values()):
            self._submit_cleanup(
                request_id=request_id,
                fail_key=key,
                failure_info=failure_info,
            )

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        if self._manager is None:
            raise RuntimeError(f"{type(self).__name__}.start() must be called before adoption")
        not_adopted: list[TaskInstance] = []
        validation_phase: dict[Future, tuple[TaskInstance, SandboxExecutionRef]] = {}
        recovery_phase: dict[Future, tuple[TaskInstance, str]] = {}
        for ti in tis:
            if ref := SandboxExecutionRef.decode(
                ti.external_executor_id,
                expected_driver=self.driver_id,
            ):
                validation_phase[self._manager.submit_validate(ref)] = (ti, ref)
            elif is_preassigned_executor_id(ti.external_executor_id):
                request_id = str(ti.external_executor_id)
                recovery_phase[self._manager.submit_recover(request_id)] = (ti, request_id)
            else:
                not_adopted.append(ti)
        all_futures = [*validation_phase, *recovery_phase]
        done, _ = wait(all_futures, timeout=self._adoption_timeout) if all_futures else (set(), set())
        for future, (ti, ref) in validation_phase.items():
            if future in done:
                try:
                    future.result()
                    running = RunningSandbox(ref)
                except Exception as error:
                    failure_info = f"persisted sandbox handle is invalid: {error}"
                else:
                    self._active[ti.key] = running
                    self.running.add(ti.key)
                    self._next_poll[ti.key] = 0.0
                    continue
            else:
                future.cancel()
                failure_info = "sandbox handle validation timed out during scheduler adoption"
            self.running.add(ti.key)
            self._fence_request(ti.key, ref.request_id, failure_info)
        for future, (ti, request_id) in recovery_phase.items():
            if future in done:
                try:
                    running = future.result()
                except Exception:
                    pass
                else:
                    if running is None:
                        not_adopted.append(ti)
                    else:
                        self._active[ti.key] = running
                        self.running.add(ti.key)
                        self._next_poll[ti.key] = 0.0
                        self.running_state(ti.key, info=running.external_executor_id)
                    continue
            future.cancel()
            self.running.add(ti.key)
            self._fence_request(
                ti.key,
                request_id,
                "fenced an ambiguous sandbox launch after scheduler restart",
            )
        return not_adopted

    def revoke_task(self, *, ti: TaskInstance) -> None:
        key = ti.key
        self._revoked.add(key)
        self.queued_tasks.pop(key, None)
        self.running.discard(key)
        self._next_poll.pop(key, None)
        self._poll_failures.pop(key, None)
        for future, (status_key, _) in list(self._status_futures.items()):
            if status_key == key:
                future.cancel()
                del self._status_futures[future]
        refs: list[SandboxExecutionRef] = []
        request_ids: set[str] = set()
        if running := self._active.pop(key, None):
            refs.append(running.ref)
        for future, pending in list(self._launch_futures.items()):
            if pending.key == key:
                future.cancel()
                request_ids.add(pending.request_id)
        if ref := SandboxExecutionRef.decode(
            ti.external_executor_id,
            expected_driver=self.driver_id,
        ):
            if all(existing != ref for existing in refs):
                refs.append(ref)
        elif is_preassigned_executor_id(ti.external_executor_id):
            request_ids.add(str(ti.external_executor_id))
        for ref in refs:
            self._submit_cleanup(
                ref,
                fail_key=key,
                failure_info="sandbox task was revoked",
            )
        for request_id in request_ids:
            self._fence_request(key, request_id, "sandbox task was revoked")
        self._maybe_finish_revocation(key)

    def _maybe_finish_revocation(self, key: TaskInstanceKey) -> None:
        if key not in self._revoked or key in self._active or key in self._fences:
            return
        if any(pending.key == key for pending in self._launch_futures.values()):
            return
        if any(cleanup.fail_key == key for cleanup in self._cleanup_futures.values()):
            return
        self._revoked.discard(key)

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        del try_number
        ref = SandboxExecutionRef.decode(
            ti.external_executor_id,
            expected_driver=self.driver_id,
        )
        if ref is None:
            return ["No persisted sandbox execution reference is available."], []

        async def fetch_output() -> SandboxOutput | None:
            driver = self.get_driver_factory()()
            try:
                driver.validate_handle(ref.handle, request_id=ref.request_id)
                return await driver.get_output(ref.handle)
            finally:
                await driver.close()

        try:
            result = asyncio.run(fetch_output())
        except Exception:
            self.log.warning("Unable to fetch diagnostics for sandbox %s", ref.request_id, exc_info=True)
            return ["Unable to fetch live sandbox output; use the configured remote task log."], []
        if result is None:
            return ["This sandbox driver does not expose live output; use the remote task log."], []
        if not isinstance(result, SandboxOutput):
            self.log.warning("Sandbox driver returned invalid diagnostics for %s", ref.request_id)
            return ["Unable to fetch live sandbox output; use the configured remote task log."], []
        messages = [f"Live output from sandbox {self._display_name(ref)}."]
        if result.truncated:
            messages.append("The sandbox provider truncated this output; the remote task log is canonical.")
        output = [remove_escape_codes(line) for line in result.stdout.splitlines()]
        output.extend(remove_escape_codes(line) for line in result.stderr.splitlines())
        return messages, output

    @staticmethod
    def _display_name(ref: SandboxExecutionRef) -> str:
        return ref.handle.display_name or ref.request_id

    def end(self) -> None:
        self._closing = True
        deadline = time.monotonic() + self._shutdown_timeout
        while self._launch_futures and time.monotonic() < deadline:
            self._drain_launches()
            self._drain_cleanups()
            self._schedule_fences()
            time.sleep(0.1)
        for future, pending in list(self._launch_futures.items()):
            self._revoked.add(pending.key)
            future.cancel()
            self._fence_request(
                pending.key,
                pending.request_id,
                "sandbox launch was interrupted by executor shutdown",
            )
        self._drain_launches()
        self._wait_for_cleanups(time.monotonic() + self._shutdown_timeout)
        self._close_manager()

    def terminate(self) -> None:
        self._closing = True
        for key in cast("set[TaskInstanceKey]", self.running):
            self._revoked.add(key)
        for future, pending in list(self._launch_futures.items()):
            future.cancel()
            self._fence_request(pending.key, pending.request_id, "sandbox executor was terminated")
        for key, running in list(self._active.items()):
            self._fence_request(key, running.ref.request_id, "sandbox executor was terminated")
        for key, fence in list(self._fences.items()):
            self._revoked.add(key)
            self._fence_request(key, fence.request_id, fence.failure_info)
        self.running.clear()
        self._wait_for_cleanups(time.monotonic() + self._shutdown_timeout)
        self._close_manager()

    def _wait_for_cleanups(self, deadline: float) -> None:
        while (self._cleanup_futures or self._cleanup_fences or self._fences) and time.monotonic() < deadline:
            self._drain_cleanups()
            self._schedule_fences()
            time.sleep(0.1)
        if self._cleanup_futures or self._cleanup_fences or self._fences:
            self.log.warning(
                "Timed out shutting down sandboxes; provider lifecycle TTLs and scheduler adoption remain active"
            )

    def _close_manager(self) -> None:
        for future in self._status_futures:
            future.cancel()
        if self._manager is not None:
            self._manager.close()

    @staticmethod
    def get_cli_commands() -> list:
        return []
