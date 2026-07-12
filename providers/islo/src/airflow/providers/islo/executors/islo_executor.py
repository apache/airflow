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
"""Run each Airflow task in a fresh Islo sandbox."""

from __future__ import annotations

import asyncio
import contextlib
import random
import threading
import time
from collections.abc import Coroutine, Sequence
from concurrent.futures import CancelledError, Future, wait
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, TypeVar, cast

from airflow.executors.base_executor import BaseExecutor, get_execution_api_server_url
from airflow.executors.workloads import ExecuteTask, ExecutorWorkload
from airflow.providers.islo.exceptions import (
    IsloConfigurationError,
    IsloUnfencedLaunchError,
)
from airflow.providers.islo.hooks.islo import AsyncIsloClient, IsloClientConfig, IsloHook
from airflow.providers.islo.models import (
    IsloExecutionRef,
    IsloExecutionResult,
    IsloExecutionState,
    IsloSandboxSpec,
    RunningIsloSandbox,
    coerce_islo_executor_config,
    is_preassigned_executor_id,
    sandbox_name_from_request_id,
)
from airflow.utils.log.logging_mixin import remove_escape_codes

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey

T = TypeVar("T")


@dataclass(frozen=True)
class _PendingLaunch:
    key: TaskInstanceKey
    sandbox_name: str


@dataclass(frozen=True)
class _Cleanup:
    sandbox_name: str
    fail_key: TaskInstanceKey | None = None
    failure_info: str | None = None


@dataclass
class _Fence:
    sandbox_name: str
    failure_info: str
    attempts: int = 0
    next_attempt: float = 0.0


class IsloExecutorManager:
    """Own one asyncio loop and bounded Islo API concurrency outside the scheduler heartbeat."""

    def __init__(
        self,
        client_config: IsloClientConfig,
        *,
        launch_concurrency: int,
        status_concurrency: int,
    ) -> None:
        if launch_concurrency <= 0 or status_concurrency <= 0:
            raise ValueError("Islo executor manager concurrency limits must be greater than zero")
        self._client_config = client_config
        self._launch_concurrency = launch_concurrency
        self._status_concurrency = status_concurrency
        self._thread: threading.Thread | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._client: AsyncIsloClient | None = None
        self._launch_semaphore: asyncio.Semaphore | None = None
        self._status_semaphore: asyncio.Semaphore | None = None
        self._ready = threading.Event()
        self._start_error: BaseException | None = None
        self._closed = False

    def start(self) -> None:
        if self._closed:
            raise RuntimeError("Islo executor manager is closed")
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="islo-executor-manager", daemon=True)
        self._thread.start()
        if not self._ready.wait(timeout=15):
            raise RuntimeError("timed out starting Islo executor manager")
        if self._start_error is not None:
            raise RuntimeError("failed to start Islo executor manager") from self._start_error

    def _run(self) -> None:
        loop: asyncio.AbstractEventLoop | None = None
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            self._loop = loop
            self._client = AsyncIsloClient(self._client_config)
            self._launch_semaphore = asyncio.Semaphore(self._launch_concurrency)
            self._status_semaphore = asyncio.Semaphore(self._status_concurrency)
        except BaseException as error:
            self._start_error = error
            self._ready.set()
            if loop is not None:
                loop.close()
            return
        self._ready.set()
        if self._closed:
            loop.run_until_complete(self._client.close())
            loop.close()
            return
        loop.run_forever()
        loop.close()

    def _submit(self, coroutine: Coroutine[Any, Any, T]) -> Future[T]:
        if self._closed or self._loop is None:
            coroutine.close()
            raise RuntimeError("Islo executor manager is not running")
        try:
            return asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        except Exception:
            coroutine.close()
            raise

    async def _health_check(self) -> None:
        await cast("AsyncIsloClient", self._client).health_check()

    def submit_health_check(self) -> Future[None]:
        return self._submit(self._health_check())

    async def _launch(
        self,
        spec: IsloSandboxSpec,
        command: list[str],
        env: dict[str, str],
    ) -> RunningIsloSandbox:
        client = cast("AsyncIsloClient", self._client)
        launch_semaphore = cast("asyncio.Semaphore", self._launch_semaphore)
        async with launch_semaphore:
            sandbox_name = spec.name
            try:
                sandbox_name, sandbox_id = await client.create_sandbox(spec)
                execution_id = await client.execute(
                    sandbox_name,
                    command,
                    env,
                    workdir=spec.workdir,
                    timeout_seconds=spec.timeout_seconds,
                )
            except BaseException as launch_error:
                try:
                    await client.delete_sandbox(sandbox_name)
                except BaseException as delete_error:
                    raise IsloUnfencedLaunchError(sandbox_name, launch_error, delete_error) from launch_error
                raise
            return RunningIsloSandbox(
                ref=IsloExecutionRef(
                    request_id=spec.request_id,
                    sandbox_name=sandbox_name,
                    sandbox_id=sandbox_id,
                    execution_id=execution_id,
                    keep=spec.keep,
                ),
            )

    def submit_launch(
        self,
        spec: IsloSandboxSpec,
        command: list[str],
        env: dict[str, str],
    ) -> Future[RunningIsloSandbox]:
        return self._submit(self._launch(spec, command, env))

    async def _status(self, ref: IsloExecutionRef) -> IsloExecutionResult:
        client = cast("AsyncIsloClient", self._client)
        status_semaphore = cast("asyncio.Semaphore", self._status_semaphore)
        async with status_semaphore:
            return await client.execution_result(ref)

    def submit_status(self, ref: IsloExecutionRef) -> Future[IsloExecutionResult]:
        return self._submit(self._status(ref))

    async def _delete(self, sandbox_name: str) -> None:
        client = cast("AsyncIsloClient", self._client)
        launch_semaphore = cast("asyncio.Semaphore", self._launch_semaphore)
        async with launch_semaphore:
            await client.delete_sandbox(sandbox_name)

    def submit_delete(self, sandbox_name: str) -> Future[None]:
        return self._submit(self._delete(sandbox_name))

    def close(self, timeout: float = 10.0) -> None:
        if self._closed:
            return
        self._closed = True
        if self._loop is not None and self._client is not None:
            client = self._client

            async def shutdown() -> None:
                current = asyncio.current_task()
                tasks = [task for task in asyncio.all_tasks() if task is not current]
                for task in tasks:
                    task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                await client.close()

            with contextlib.suppress(Exception):
                asyncio.run_coroutine_threadsafe(shutdown(), self._loop).result(timeout=timeout)
            self._loop.call_soon_threadsafe(self._loop.stop)
        if self._thread is not None:
            self._thread.join(timeout=timeout)


class IsloExecutor(BaseExecutor):
    """Airflow executor that assigns every task try to an isolated Islo sandbox."""

    is_local = False
    is_production = True
    serve_logs = False
    supports_ad_hoc_ti_run = False
    supports_multi_team = True
    pre_assigns_external_executor_id: ClassVar[bool] = True

    def __init__(self, *args, manager: IsloExecutorManager | None = None, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._manager = manager
        self._launch_futures: dict[Future, _PendingLaunch] = {}
        self._status_futures: dict[Future, tuple[TaskInstanceKey, IsloExecutionRef]] = {}
        self._cleanup_futures: dict[Future, _Cleanup] = {}
        self._active: dict[TaskInstanceKey, RunningIsloSandbox] = {}
        self._fences: dict[TaskInstanceKey, _Fence] = {}
        self._revoked: set[TaskInstanceKey] = set()
        self._next_poll: dict[TaskInstanceKey, float] = {}
        self._poll_failures: dict[TaskInstanceKey, int] = {}
        self._closing = False
        self._poll_interval = self.conf.getint("islo", "poll_interval", fallback=2)
        self._max_poll_interval = self.conf.getint("islo", "max_poll_interval", fallback=30)
        self._creation_batch_size = self.conf.getint("islo", "creation_batch_size", fallback=128)
        self._status_batch_size = self.conf.getint("islo", "status_batch_size", fallback=1000)
        self._shutdown_timeout = self.conf.getint("islo", "shutdown_timeout", fallback=60)
        self._adoption_timeout = self.conf.getint("islo", "adoption_timeout", fallback=30)
        self._health_check_timeout = self.conf.getint("islo", "health_check_timeout", fallback=30)
        self._launch_concurrency = self.conf.getint("islo", "launch_concurrency", fallback=32)
        self._status_concurrency = self.conf.getint("islo", "status_concurrency", fallback=128)
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
            raise IsloConfigurationError(
                f"IsloExecutor configuration must be greater than zero: {', '.join(invalid)}"
            )
        if self._max_poll_interval < self._poll_interval:
            raise IsloConfigurationError("max_poll_interval must be greater than or equal to poll_interval")

    def start(self) -> None:
        if not self.conf.getboolean("logging", "remote_logging", fallback=False):
            raise IsloConfigurationError(
                "IsloExecutor requires [logging] remote_logging=True because completed sandboxes are deleted"
            )
        if self._manager is None:
            conn_id = self.conf.get("islo", "conn_id", fallback=IsloHook.default_conn_name)
            client_config = IsloHook(str(conn_id)).get_client_config()
            self._manager = IsloExecutorManager(
                client_config,
                launch_concurrency=self._launch_concurrency,
                status_concurrency=self._status_concurrency,
            )
        try:
            self._manager.start()
            if self.conf.getboolean("islo", "check_health_on_startup", fallback=True):
                self._manager.submit_health_check().result(timeout=self._health_check_timeout)
        except Exception:
            self._manager.close()
            raise

    def _process_workloads(self, workload_items: Sequence[ExecutorWorkload]) -> None:
        if self._closing:
            return
        if self._manager is None:
            raise RuntimeError("IsloExecutor.start() must be called before processing workloads")
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
                    raise IsloConfigurationError(
                        "task has no pre-assigned external executor ID; IsloExecutor requires Airflow 3.3+"
                    )
                spec = self._build_spec(workload, request_id)
                future = self._manager.submit_launch(
                    spec,
                    self._workload_command(workload),
                    self._runtime_env(workload, spec.env),
                )
                self._launch_futures[future] = _PendingLaunch(key=key, sandbox_name=spec.name)
            except Exception as error:
                self.log.exception("Unable to configure Islo sandbox for %s", key)
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

    def _runtime_env(self, workload: ExecuteTask, task_env: dict[str, str]) -> dict[str, str]:
        del workload
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

    def _optional_int(self, option: str) -> int | None:
        value = self.conf.get("islo", option, fallback=None)
        if value is None or value == "":
            return None
        return int(value)

    def _optional_str(self, option: str) -> str | None:
        value = self.conf.get("islo", option, fallback=None)
        return str(value) if value not in (None, "") else None

    def _build_spec(self, workload: ExecuteTask, request_id: str) -> IsloSandboxSpec:
        override = coerce_islo_executor_config(workload.ti.executor_config)
        defaults = {
            "image": self._optional_str("default_image"),
            "snapshot_name": self._optional_str("default_snapshot_name"),
            "snapshot_url": self._optional_str("default_snapshot_url"),
        }
        if any(key in override for key in defaults):
            sources = {key: override.get(key) for key in defaults}
        else:
            sources = defaults
        return IsloSandboxSpec(
            name=sandbox_name_from_request_id(request_id),
            request_id=request_id,
            **sources,
            vcpus=override.get("vcpus", self._optional_int("default_vcpus")),
            memory_mb=override.get("memory_mb", self._optional_int("default_memory_mb")),
            disk_gb=override.get("disk_gb", self._optional_int("default_disk_gb")),
            timeout_seconds=override.get(
                "timeout_seconds", self.conf.getint("islo", "default_timeout_seconds", fallback=3600)
            ),
            ttl_seconds=override.get(
                "ttl_seconds", self.conf.getint("islo", "default_ttl_seconds", fallback=86400)
            ),
            env=override.get("env", {}),
            workdir=override.get("workdir", self._optional_str("default_workdir")),
            gateway_profile=override.get("gateway_profile", self._optional_str("default_gateway_profile")),
            internet_enabled=override.get(
                "internet_enabled", self.conf.getboolean("islo", "internet_enabled", fallback=True)
            ),
            keep=bool(override.get("keep", False)),
        )

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
                    self.fail(pending.key, info="Islo sandbox launch was cancelled")
            except IsloUnfencedLaunchError as error:
                self.log.error(
                    "Launch for %s became ambiguous; fencing sandbox %s before reporting failure",
                    pending.key,
                    error.sandbox_name,
                )
                self._fences[pending.key] = _Fence(
                    sandbox_name=error.sandbox_name,
                    failure_info=str(error.launch_error),
                )
            except Exception as error:
                self.log.exception("Islo sandbox launch failed for %s", pending.key)
                self.fail(pending.key, info=str(error))
            else:
                if pending.key in self._revoked:
                    self._submit_cleanup(running.ref.sandbox_name)
                    self.running.discard(pending.key)
                    continue
                self._active[pending.key] = running
                self._next_poll[pending.key] = 0.0
                self.running_state(pending.key, info=running.ref.encode())

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
            except Exception:
                self.log.warning("Transient Islo status error for %s", key, exc_info=True)
                self._defer_poll(key, failed=True)
                continue
            if result.state is IsloExecutionState.UNKNOWN:
                self._defer_poll(key, failed=True)
                continue
            if result.state in {IsloExecutionState.PENDING, IsloExecutionState.RUNNING}:
                self._defer_poll(key, failed=False)
                continue
            del self._active[key]
            self._next_poll.pop(key, None)
            self._poll_failures.pop(key, None)
            if result.state is IsloExecutionState.SUCCEEDED:
                self.success(key)
            elif result.state is IsloExecutionState.GONE:
                self.fail(key, info=f"Islo sandbox {ref.sandbox_name} no longer exists")
            else:
                self.fail(key, info=f"Islo execution exited with code {result.exit_code}")
            if not current.keep:
                self._submit_cleanup(ref.sandbox_name)

    def _defer_poll(self, key: TaskInstanceKey, *, failed: bool) -> None:
        failures = self._poll_failures.get(key, 0) + 1 if failed else 0
        self._poll_failures[key] = failures
        interval = min(self._poll_interval * (2 ** min(failures, 8)), self._max_poll_interval)
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
                self.log.warning("Unable to submit Islo status request for %s", key, exc_info=True)
                self._defer_poll(key, failed=True)
            else:
                self._status_futures[future] = (key, running.ref)

    def _submit_cleanup(
        self,
        sandbox_name: str,
        *,
        fail_key: TaskInstanceKey | None = None,
        failure_info: str | None = None,
    ) -> Future | None:
        if self._manager is None:
            return None
        try:
            future = self._manager.submit_delete(sandbox_name)
        except Exception:
            self.log.warning("Unable to submit deletion for Islo sandbox %s", sandbox_name, exc_info=True)
            if fail_key is not None:
                fence = self._fences.setdefault(
                    fail_key,
                    _Fence(sandbox_name, failure_info or "ambiguous Islo launch"),
                )
                fence.attempts += 1
                fence.next_attempt = time.monotonic() + min(2**fence.attempts, 30)
            return None
        self._cleanup_futures[future] = _Cleanup(sandbox_name, fail_key, failure_info)
        return future

    def _drain_cleanups(self) -> None:
        for future, cleanup in list(self._cleanup_futures.items()):
            if not future.done():
                continue
            del self._cleanup_futures[future]
            try:
                future.result()
            except Exception:
                self.log.warning("Unable to delete Islo sandbox %s", cleanup.sandbox_name, exc_info=True)
                if cleanup.fail_key is not None:
                    fence = self._fences.setdefault(
                        cleanup.fail_key,
                        _Fence(cleanup.sandbox_name, cleanup.failure_info or "ambiguous Islo launch"),
                    )
                    fence.attempts += 1
                    fence.next_attempt = time.monotonic() + min(2**fence.attempts, 30)
            else:
                if cleanup.fail_key is not None:
                    self._fences.pop(cleanup.fail_key, None)
                    if cleanup.fail_key in self._revoked:
                        self.running.discard(cleanup.fail_key)
                    else:
                        self.fail(cleanup.fail_key, info=cleanup.failure_info)

    def _schedule_fences(self) -> None:
        if self._manager is None:
            return
        pending_keys = {cleanup.fail_key for cleanup in self._cleanup_futures.values()}
        now = time.monotonic()
        for key, fence in list(self._fences.items()):
            if key in pending_keys or fence.next_attempt > now:
                continue
            self._submit_cleanup(
                fence.sandbox_name,
                fail_key=key,
                failure_info=fence.failure_info,
            )

    def _fence_sandbox(self, key: TaskInstanceKey, sandbox_name: str, failure_info: str) -> None:
        """Retry deletion until a sandbox that must not keep running is confirmed gone."""
        self._fences[key] = _Fence(sandbox_name=sandbox_name, failure_info=failure_info)
        if not any(cleanup.fail_key == key for cleanup in self._cleanup_futures.values()):
            self._submit_cleanup(sandbox_name, fail_key=key, failure_info=failure_info)

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        if self._manager is None:
            raise RuntimeError("IsloExecutor.start() must be called before adoption")
        not_adopted: list[TaskInstance] = []
        launch_phase: dict[Future, TaskInstance] = {}
        for ti in tis:
            if ref := IsloExecutionRef.decode(ti.external_executor_id):
                self._active[ti.key] = RunningIsloSandbox(ref=ref)
                self.running.add(ti.key)
                self._next_poll[ti.key] = 0.0
            elif is_preassigned_executor_id(ti.external_executor_id):
                sandbox_name = sandbox_name_from_request_id(str(ti.external_executor_id))
                launch_phase[self._manager.submit_delete(sandbox_name)] = ti
            else:
                not_adopted.append(ti)
        if launch_phase:
            done, _ = wait(launch_phase, timeout=self._adoption_timeout)
            for future, ti in launch_phase.items():
                if future in done:
                    try:
                        future.result()
                    except Exception:
                        pass
                    else:
                        not_adopted.append(ti)
                        continue
                self.running.add(ti.key)
                self._fences[ti.key] = _Fence(
                    sandbox_name=sandbox_name_from_request_id(str(ti.external_executor_id)),
                    failure_info="fenced an ambiguous Islo launch after scheduler restart",
                )
                self._cleanup_futures[future] = _Cleanup(
                    sandbox_name=self._fences[ti.key].sandbox_name,
                    fail_key=ti.key,
                    failure_info=self._fences[ti.key].failure_info,
                )
        return not_adopted

    def revoke_task(self, *, ti: TaskInstance) -> None:
        key = ti.key
        self._revoked.add(key)
        self.queued_tasks.pop(key, None)
        self.running.discard(key)
        sandbox_names: set[str] = set()
        if running := self._active.pop(key, None):
            sandbox_names.add(running.ref.sandbox_name)
        for future, pending in list(self._launch_futures.items()):
            if pending.key == key:
                future.cancel()
                sandbox_names.add(pending.sandbox_name)
        if ref := IsloExecutionRef.decode(ti.external_executor_id):
            sandbox_names.add(ref.sandbox_name)
        for sandbox_name in sandbox_names:
            self._fence_sandbox(key, sandbox_name, "Islo task was revoked")

    def get_task_log(self, ti: TaskInstance, try_number: int) -> tuple[list[str], list[str]]:
        del try_number
        ref = IsloExecutionRef.decode(ti.external_executor_id)
        if ref is None:
            return ["No persisted Islo execution reference is available."], []

        async def fetch_output() -> tuple[str, str, bool]:
            client = IsloHook(
                str(self.conf.get("islo", "conn_id", fallback=IsloHook.default_conn_name))
            ).get_async_client()
            try:
                return await client.execution_output(ref)
            finally:
                await client.close()

        try:
            stdout, stderr, truncated = asyncio.run(fetch_output())
        except Exception:
            self.log.warning("Unable to fetch logs for Islo execution %s", ref.execution_id, exc_info=True)
            return ["Unable to fetch live Islo output; use the configured remote task log."], []
        messages = [f"Live output from Islo sandbox {ref.sandbox_name}."]
        if truncated:
            messages.append("Islo truncated the captured output; the remote Airflow task log is canonical.")
        output = [remove_escape_codes(line) for line in stdout.splitlines()]
        output.extend(remove_escape_codes(line) for line in stderr.splitlines())
        return messages, output

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
            self._fence_sandbox(
                pending.key,
                pending.sandbox_name,
                "Islo sandbox launch was interrupted by executor shutdown",
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
            self._fence_sandbox(pending.key, pending.sandbox_name, "Islo executor was terminated")
        for key, running in list(self._active.items()):
            self._fence_sandbox(key, running.ref.sandbox_name, "Islo executor was terminated")
        for key, fence in list(self._fences.items()):
            self._revoked.add(key)
            self._fence_sandbox(key, fence.sandbox_name, fence.failure_info)
        self.running.clear()
        self._wait_for_cleanups(time.monotonic() + self._shutdown_timeout)
        self._close_manager()

    def _wait_for_cleanups(self, deadline: float) -> None:
        while (self._cleanup_futures or self._fences) and time.monotonic() < deadline:
            self._drain_cleanups()
            self._schedule_fences()
            time.sleep(0.1)
        if self._cleanup_futures or self._fences:
            self.log.warning(
                "Timed out shutting down Islo sandboxes; lifecycle TTLs and scheduler adoption remain active"
            )

    def _close_manager(self) -> None:
        for future in self._status_futures:
            future.cancel()
        if self._manager is not None:
            self._manager.close()

    @staticmethod
    def get_cli_commands() -> list:
        return []
