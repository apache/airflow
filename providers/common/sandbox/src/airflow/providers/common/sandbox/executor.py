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
import queue
import random
import time
from abc import ABC, abstractmethod
from collections.abc import Sequence
from concurrent.futures import Future, wait
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, ClassVar

from airflow.executors.base_executor import BaseExecutor, get_execution_api_server_url
from airflow.executors.workloads import ExecuteTask, ExecutorWorkload
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
    SandboxLaunchConfig,
    SandboxLaunchOutcome,
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
    from airflow.providers.common.sandbox.driver import SandboxDriverFactory

__all__ = ["BaseSandboxExecutor"]


class _GenerationPhase(str, Enum):
    LAUNCHING = "launching"
    RUNNING = "running"
    TERMINATING = "terminating"
    FENCING = "fencing"
    QUARANTINED = "quarantined"
    REVOKED = "revoked"


@dataclass
class _TaskGeneration:
    request_id: str | None
    phase: _GenerationPhase
    ref: SandboxExecutionRef | None = None
    operation_inflight: bool = False
    poll_failures: int = 0
    fence_attempts: int = 0
    next_action: float = 0.0
    failure_info: str | None = None
    revoked: bool = False
    terminal_succeeded: bool = False


class BaseSandboxExecutor(BaseExecutor, ABC):
    """Run task attempts through a concrete provider's sandbox driver."""

    is_local = False
    is_production = True
    serve_logs = False
    supports_ad_hoc_ti_run = False
    supports_multi_team = True
    pre_assigns_external_executor_id: ClassVar[bool] = True
    requires_terminal_cleanup: ClassVar[bool] = False

    driver_id: ClassVar[str]
    _CONFIG_SECTION = "common.sandbox"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        if not getattr(type(self), "driver_id", ""):
            raise TypeError("concrete sandbox executors must define driver_id")
        self._result_queue: queue.SimpleQueue[SandboxRunnerResult] = queue.SimpleQueue()
        self._runner: _SandboxExecutorRunner | None = None
        self._generations: dict[TaskInstanceKey, _TaskGeneration] = {}
        self._closing = False
        section = self._CONFIG_SECTION
        self._poll_interval = self.conf.getint(section, "poll_interval", fallback=2)
        self._max_poll_interval = self.conf.getint(section, "max_poll_interval", fallback=30)
        self._creation_batch_size = self.conf.getint(section, "creation_batch_size", fallback=128)
        self._status_batch_size = self.conf.getint(section, "status_batch_size", fallback=1000)
        self._max_status_errors = self.conf.getint(section, "max_status_errors", fallback=10)
        self._max_ttl_seconds = self.conf.getint(section, "max_ttl_seconds", fallback=86400)
        self._allow_keep = self.conf.getboolean(section, "allow_keep", fallback=False)
        self._shutdown_timeout = self.conf.getint(section, "shutdown_timeout", fallback=60)
        self._adoption_timeout = self.conf.getint(section, "adoption_timeout", fallback=30)
        self._health_check_timeout = self.conf.getint(section, "health_check_timeout", fallback=30)
        self._launch_concurrency = self.conf.getint(section, "launch_concurrency", fallback=32)
        self._status_concurrency = self.conf.getint(section, "status_concurrency", fallback=128)
        self._cleanup_concurrency = self.conf.getint(section, "cleanup_concurrency", fallback=32)
        self._validate_configuration()

    def _validate_configuration(self) -> None:
        options = {
            "adoption_timeout": self._adoption_timeout,
            "cleanup_concurrency": self._cleanup_concurrency,
            "creation_batch_size": self._creation_batch_size,
            "health_check_timeout": self._health_check_timeout,
            "launch_concurrency": self._launch_concurrency,
            "max_poll_interval": self._max_poll_interval,
            "max_status_errors": self._max_status_errors,
            "max_ttl_seconds": self._max_ttl_seconds,
            "poll_interval": self._poll_interval,
            "shutdown_timeout": self._shutdown_timeout,
            "status_batch_size": self._status_batch_size,
            "status_concurrency": self._status_concurrency,
        }
        if invalid := sorted(name for name, value in options.items() if value <= 0):
            raise SandboxConfigurationError(
                f"sandbox executor configuration must be greater than zero: {', '.join(invalid)}"
            )
        if self._max_poll_interval < self._poll_interval:
            raise SandboxConfigurationError(
                "max_poll_interval must be greater than or equal to poll_interval"
            )

    @abstractmethod
    def get_driver_factory(self) -> SandboxDriverFactory:
        """Resolve credentials and return a driver factory."""

    @abstractmethod
    def build_launch_config(self, workload: ExecuteTask) -> SandboxLaunchConfig:
        """Validate provider task configuration and prepare one launch."""

    def start(self) -> None:
        if self._closing:
            raise RuntimeError("a closed sandbox executor cannot be restarted")
        if not self.conf.getboolean("logging", "remote_logging", fallback=False):
            raise SandboxConfigurationError(
                f"{type(self).__name__} requires [logging] remote_logging=True because completed "
                "sandboxes are ephemeral"
            )
        if self._runner is not None:
            return
        runner = _SandboxExecutorRunner(
            self.get_driver_factory(),
            self._result_queue,
            expected_driver_id=self.driver_id,
            launch_concurrency=self._launch_concurrency,
            status_concurrency=self._status_concurrency,
            cleanup_concurrency=self._cleanup_concurrency,
        )
        self._runner = runner
        try:
            runner.start()
            if self.conf.getboolean(self._CONFIG_SECTION, "check_health_on_startup", fallback=True):
                runner.submit_health_check().result(timeout=self._health_check_timeout)
        except Exception:
            runner.close(timeout=self._shutdown_timeout)
            self._runner = None
            raise

    def _process_workloads(self, workload_items: Sequence[ExecutorWorkload]) -> None:
        if self._closing:
            return
        runner = self._require_runner()
        pending_launches = sum(
            generation.phase is _GenerationPhase.LAUNCHING for generation in self._generations.values()
        )
        launch_capacity = min(
            self._creation_batch_size,
            max(0, self._launch_concurrency - pending_launches),
        )
        submitted = 0
        for workload in workload_items:
            if submitted >= launch_capacity:
                break
            if not isinstance(workload, ExecuteTask):
                raise RuntimeError(f"{type(self)} cannot handle workloads of type {type(workload)}")
            key = workload.ti.key
            if key in self._generations:
                continue
            self.queued_tasks.pop(key, None)
            request_id = workload.ti.external_executor_id
            try:
                if not is_preassigned_executor_id(request_id):
                    raise SandboxConfigurationError(
                        "task has no pre-assigned external executor ID; sandbox executors require Airflow 3.3+"
                    )
                launch_config = self.build_launch_config(workload)
                self._validate_launch_policy(launch_config)
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
                generation = _TaskGeneration(
                    request_id=request_id,
                    phase=_GenerationPhase.LAUNCHING,
                    operation_inflight=True,
                )
                self._generations[key] = generation
                self.running.add(key)
                runner.submit_launch(key, request)
            except Exception as error:
                self._generations.pop(key, None)
                self.log.exception("Unable to submit sandbox launch for %s", key)
                self.fail(key, info=str(error))
            else:
                submitted += 1

    def _validate_launch_policy(self, launch_config: SandboxLaunchConfig) -> None:
        if launch_config.ttl_seconds > self._max_ttl_seconds:
            raise SandboxConfigurationError(
                f"sandbox ttl_seconds cannot exceed the deployment maximum of {self._max_ttl_seconds}"
            )
        if launch_config.keep and not self._allow_keep:
            raise SandboxConfigurationError("sandbox keep=True requires [common.sandbox] allow_keep=True")

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
                "AIRFLOW_SANDBOX_DRIVER": self.driver_id,
                "AIRFLOW__CORE__EXECUTION_API_SERVER_URL": get_execution_api_server_url(self.conf),
                "AIRFLOW__LOGGING__REMOTE_LOGGING": "True",
            }
        )
        for option in ("remote_base_log_folder", "remote_log_conn_id"):
            if value := self.conf.get("logging", option, fallback=None):
                env[f"AIRFLOW__LOGGING__{option.upper()}"] = str(value)
        return env

    def sync(self) -> None:
        if self._runner is None:
            return
        self._drain_runner_results()
        self._schedule_fences()
        self._schedule_terminations()
        self._schedule_statuses()

    def _drain_runner_results(self) -> None:
        while True:
            try:
                result = self._result_queue.get_nowait()
            except queue.Empty:
                return
            self._handle_runner_result(result)

    def _handle_runner_result(self, result: SandboxRunnerResult) -> None:
        if result.key is None:
            if result.error is not None:
                self.log.warning(
                    "Best-effort sandbox cleanup failed for request %s: %s",
                    result.request_id,
                    result.error,
                )
            return
        key = result.key
        generation = self._generations.get(key)
        if generation is None or generation.request_id != result.request_id:
            self._cleanup_stale_launch(result)
            return
        generation.operation_inflight = False
        if result.operation is SandboxRunnerOperation.LAUNCH:
            self._handle_launch_result(key, generation, result)
        elif result.operation is SandboxRunnerOperation.STATUS:
            self._handle_status_result(key, generation, result)
        elif result.operation is SandboxRunnerOperation.TERMINATE:
            self._handle_terminate_result(key, generation, result)
        elif result.operation is SandboxRunnerOperation.FENCE:
            self._handle_fence_result(key, generation, result)

    def _cleanup_stale_launch(self, result: SandboxRunnerResult) -> None:
        if result.operation is not SandboxRunnerOperation.LAUNCH:
            return
        if not isinstance(result.value, SandboxLaunchOutcome) or self._runner is None:
            return
        try:
            accepted = self._runner.submit_terminate(result.value.ref, required=False)
        except Exception:
            accepted = False
        if not accepted:
            self.log.warning(
                "Could not schedule cleanup for stale sandbox launch %s; provider TTL remains active",
                result.request_id,
            )

    def _handle_launch_result(
        self,
        key: TaskInstanceKey,
        generation: _TaskGeneration,
        result: SandboxRunnerResult,
    ) -> None:
        if generation.phase is not _GenerationPhase.LAUNCHING:
            self._cleanup_stale_launch(result)
            return
        if result.error is not None:
            if isinstance(result.error, SandboxLaunchUnfencedError):
                self.log.error(
                    "Sandbox launch for %s is ambiguous; fencing request %s",
                    key,
                    result.request_id,
                )
                self._begin_fence(generation, str(result.error.launch_error))
            else:
                self._finish_generation(key, generation, failure_info=str(result.error))
            return
        if not isinstance(result.value, SandboxLaunchOutcome):
            self._begin_fence(generation, "sandbox runner returned an invalid launch result")
            return
        generation.ref = result.value.ref
        if generation.revoked:
            generation.phase = _GenerationPhase.TERMINATING
            generation.next_action = 0.0
            return
        generation.phase = _GenerationPhase.RUNNING
        generation.next_action = 0.0
        self.running_state(key, info=result.value.external_executor_id)

    def _handle_status_result(
        self,
        key: TaskInstanceKey,
        generation: _TaskGeneration,
        result: SandboxRunnerResult,
    ) -> None:
        if generation.phase is not _GenerationPhase.RUNNING:
            return
        if result.error is not None:
            self._handle_status_error(generation, result.error)
            return
        if not isinstance(result.value, SandboxResult):
            self._begin_fence(generation, "sandbox runner returned an invalid status result")
            return
        status = result.value
        if status.state in {SandboxState.PENDING, SandboxState.RUNNING}:
            generation.poll_failures = 0
            self._defer_poll(generation, retry_after=status.retry_after)
            return
        ref = generation.ref
        if status.state is SandboxState.SUCCEEDED:
            succeeded = True
            failure_info = None
        elif status.state is SandboxState.GONE:
            display_name = self._display_name(ref) if ref is not None else result.request_id
            succeeded = False
            failure_info = status.message or f"sandbox {display_name} no longer exists"
        else:
            default_message = (
                f"sandbox workload exited with code {status.exit_code}"
                if status.exit_code is not None
                else "sandbox workload failed"
            )
            succeeded = False
            failure_info = status.message or default_message
        if ref is None or not ref.keep:
            if self.requires_terminal_cleanup:
                generation.phase = _GenerationPhase.TERMINATING
                generation.terminal_succeeded = succeeded
                generation.failure_info = failure_info
                generation.next_action = 0.0
                return
            if ref is not None:
                self._submit_best_effort_cleanup(ref)
        self._finish_generation(key, generation, succeeded=succeeded, failure_info=failure_info)

    def _handle_status_error(self, generation: _TaskGeneration, error: BaseException) -> None:
        if isinstance(error, SandboxProtocolError):
            self._begin_fence(generation, f"sandbox status protocol error: {error}")
            return
        generation.poll_failures += 1
        if generation.poll_failures >= self._max_status_errors:
            self._begin_fence(
                generation,
                f"sandbox status failed {generation.poll_failures} consecutive times: {error}",
            )
            return
        self.log.warning(
            "Transient sandbox status error for request %s (%s/%s): %s",
            generation.request_id,
            generation.poll_failures,
            self._max_status_errors,
            error,
        )
        self._defer_poll(generation, failed=True)

    def _handle_terminate_result(
        self,
        key: TaskInstanceKey,
        generation: _TaskGeneration,
        result: SandboxRunnerResult,
    ) -> None:
        if generation.phase is not _GenerationPhase.TERMINATING:
            return
        if result.error is not None:
            self._begin_fence(
                generation, generation.failure_info or f"sandbox termination failed: {result.error}"
            )
            return
        self._finish_generation(
            key,
            generation,
            succeeded=generation.terminal_succeeded,
            failure_info=generation.failure_info,
        )

    def _handle_fence_result(
        self,
        key: TaskInstanceKey,
        generation: _TaskGeneration,
        result: SandboxRunnerResult,
    ) -> None:
        if generation.phase is not _GenerationPhase.FENCING:
            return
        if result.error is not None:
            generation.fence_attempts += 1
            generation.next_action = time.monotonic() + self._retry_delay(generation.fence_attempts)
            self.log.warning(
                "Unable to fence sandbox request %s; retrying: %s",
                generation.request_id,
                result.error,
            )
            return
        self._finish_generation(
            key,
            generation,
            succeeded=generation.terminal_succeeded,
            failure_info=generation.failure_info,
        )

    def _finish_generation(
        self,
        key: TaskInstanceKey,
        generation: _TaskGeneration,
        *,
        succeeded: bool = False,
        failure_info: str | None = None,
    ) -> None:
        if self._generations.get(key) is not generation:
            return
        if generation.revoked:
            generation.phase = _GenerationPhase.REVOKED
            generation.operation_inflight = False
            self.running.discard(key)
            return
        del self._generations[key]
        if succeeded:
            self.success(key)
        else:
            self.fail(key, info=failure_info)

    def _begin_fence(self, generation: _TaskGeneration, failure_info: str) -> None:
        if generation.request_id is None:
            generation.phase = _GenerationPhase.QUARANTINED
            generation.failure_info = failure_info
            return
        generation.phase = _GenerationPhase.FENCING
        generation.failure_info = failure_info
        generation.fence_attempts = 0
        generation.next_action = 0.0

    def _defer_poll(
        self,
        generation: _TaskGeneration,
        *,
        failed: bool = False,
        retry_after: float | None = None,
    ) -> None:
        if retry_after is not None:
            interval = min(retry_after, self._max_poll_interval)
        elif failed:
            interval = min(
                self._poll_interval * (2 ** min(generation.poll_failures, 8)),
                self._max_poll_interval,
            )
        else:
            interval = self._poll_interval
        generation.next_action = (
            time.monotonic()
            + interval
            + random.uniform(
                0,
                min(interval / 5, 1.0),
            )
        )

    def _retry_delay(self, attempts: int) -> float:
        interval = min(2 ** min(attempts, 8), self._max_poll_interval)
        return interval + random.uniform(0, min(interval / 5, 1.0))

    def _schedule_fences(self) -> None:
        if self._runner is None:
            return
        now = time.monotonic()
        for key, generation in self._generations.items():
            if (
                generation.phase is not _GenerationPhase.FENCING
                or generation.operation_inflight
                or generation.next_action > now
                or generation.request_id is None
            ):
                continue
            try:
                self._runner.submit_fence(key, generation.request_id)
            except Exception as error:
                generation.fence_attempts += 1
                generation.next_action = time.monotonic() + self._retry_delay(generation.fence_attempts)
                self.log.warning(
                    "Unable to submit fence for sandbox request %s: %s",
                    generation.request_id,
                    error,
                )
            else:
                generation.operation_inflight = True

    def _schedule_terminations(self) -> None:
        if self._runner is None:
            return
        now = time.monotonic()
        for key, generation in self._generations.items():
            if (
                generation.phase is not _GenerationPhase.TERMINATING
                or generation.operation_inflight
                or generation.next_action > now
            ):
                continue
            if generation.ref is None:
                self._begin_fence(
                    generation, generation.failure_info or "sandbox termination lacked a handle"
                )
                continue
            try:
                self._runner.submit_terminate(generation.ref, key=key, required=True)
            except Exception as error:
                self._begin_fence(
                    generation,
                    generation.failure_info or f"unable to submit sandbox termination: {error}",
                )
            else:
                generation.operation_inflight = True

    def _schedule_statuses(self) -> None:
        if self._runner is None or self._closing:
            return
        inflight = sum(
            generation.operation_inflight
            for generation in self._generations.values()
            if generation.phase is _GenerationPhase.RUNNING
        )
        capacity = max(0, self._status_batch_size - inflight)
        now = time.monotonic()
        due = sorted(
            (
                (key, generation)
                for key, generation in self._generations.items()
                if generation.phase is _GenerationPhase.RUNNING
                and not generation.operation_inflight
                and generation.next_action <= now
            ),
            key=lambda item: item[1].next_action,
        )
        for key, generation in due[:capacity]:
            ref = generation.ref
            if ref is None:
                self._begin_fence(generation, "running sandbox generation has no handle")
                continue
            try:
                self._runner.submit_status(key, ref)
            except Exception as error:
                self._handle_status_error(generation, error)
            else:
                generation.operation_inflight = True

    def _submit_best_effort_cleanup(self, ref: SandboxExecutionRef) -> None:
        if self._runner is None:
            return
        try:
            accepted = self._runner.submit_terminate(ref, required=False)
        except Exception:
            accepted = False
        if not accepted:
            self.log.warning(
                "Cleanup capacity is full for sandbox request %s; provider TTL remains active",
                ref.request_id,
            )

    def try_adopt_task_instances(self, tis: Sequence[TaskInstance]) -> Sequence[TaskInstance]:
        runner = self._require_runner()
        not_adopted: set[TaskInstanceKey] = set()
        validations: dict[Future[None], tuple[TaskInstance, SandboxExecutionRef]] = {}
        recoveries: dict[Future[SandboxLaunchOutcome | None], tuple[TaskInstance, str]] = {}
        for ti in tis:
            if ti.key in self._generations:
                self.running.add(ti.key)
                continue
            external_id = ti.external_executor_id
            if SandboxExecutionRef.has_envelope(external_id):
                try:
                    ref = SandboxExecutionRef.decode(external_id, strict=True)
                except SandboxInvalidHandleError as error:
                    self._quarantine(ti, f"persisted sandbox reference is malformed: {error}")
                    continue
                if ref is None or ref.driver != self.driver_id:
                    owner = ref.driver if ref is not None else "unknown"
                    self._quarantine(ti, f"persisted sandbox reference belongs to driver {owner!r}")
                    continue
                try:
                    validations[runner.submit_validate(ref)] = (ti, ref)
                except Exception as error:
                    self._adopt_fence(ti, ref.request_id, f"sandbox handle validation failed: {error}")
            elif is_preassigned_executor_id(external_id):
                request_id = str(external_id)
                try:
                    recoveries[runner.submit_recover(request_id)] = (ti, request_id)
                except Exception as error:
                    self._adopt_fence(ti, request_id, f"sandbox recovery could not start: {error}")
            elif external_id:
                self._quarantine(ti, "persisted sandbox reference has an unknown format")
            else:
                not_adopted.add(ti.key)
        pending_futures: list[Future[Any]] = [*validations, *recoveries]
        done, _ = wait(pending_futures, timeout=self._adoption_timeout) if pending_futures else (set(), set())
        for validation_future, (ti, ref) in validations.items():
            if validation_future not in done:
                validation_future.cancel()
                self._adopt_fence(
                    ti,
                    ref.request_id,
                    "sandbox handle validation timed out during scheduler adoption",
                )
                continue
            try:
                validation_future.result()
            except Exception as error:
                self._adopt_fence(ti, ref.request_id, f"persisted sandbox handle is invalid: {error}")
            else:
                self._generations[ti.key] = _TaskGeneration(
                    request_id=ref.request_id,
                    phase=_GenerationPhase.RUNNING,
                    ref=ref,
                )
                self.running.add(ti.key)
        for recovery_future, (ti, request_id) in recoveries.items():
            if recovery_future not in done:
                recovery_future.cancel()
                self._adopt_fence(
                    ti,
                    request_id,
                    "sandbox recovery timed out during scheduler adoption",
                )
                continue
            try:
                outcome = recovery_future.result()
            except Exception as error:
                self._adopt_fence(ti, request_id, f"sandbox recovery failed: {error}")
            else:
                if outcome is None:
                    not_adopted.add(ti.key)
                    continue
                if not isinstance(outcome, SandboxLaunchOutcome):
                    self._adopt_fence(ti, request_id, "sandbox driver returned an invalid recovery result")
                    continue
                self._generations[ti.key] = _TaskGeneration(
                    request_id=request_id,
                    phase=_GenerationPhase.RUNNING,
                    ref=outcome.ref,
                )
                self.running.add(ti.key)
                self.running_state(ti.key, info=outcome.external_executor_id)
        self._schedule_fences()
        return [ti for ti in tis if ti.key in not_adopted]

    def _quarantine(self, ti: TaskInstance, reason: str) -> None:
        self._generations[ti.key] = _TaskGeneration(
            request_id=None,
            phase=_GenerationPhase.QUARANTINED,
            failure_info=reason,
        )
        self.running.add(ti.key)
        self.log.error("Quarantining %s instead of resetting it: %s", ti.key, reason)

    def _adopt_fence(self, ti: TaskInstance, request_id: str, failure_info: str) -> None:
        self._generations[ti.key] = _TaskGeneration(
            request_id=request_id,
            phase=_GenerationPhase.FENCING,
            failure_info=failure_info,
        )
        self.running.add(ti.key)

    def revoke_task(self, *, ti: TaskInstance) -> bool:
        key = ti.key
        self.queued_tasks.pop(key, None)
        self._drain_runner_results()
        generation = self._generations.get(key)
        if generation is None:
            generation = self._generation_from_external_id(ti.external_executor_id)
            if generation is None:
                self.running.discard(key)
                return True
            self._generations[key] = generation
            self.running.add(key)
        if generation.phase is _GenerationPhase.REVOKED:
            del self._generations[key]
            return True
        if generation.phase is _GenerationPhase.QUARANTINED:
            self.log.warning(
                "Cannot clean up quarantined sandbox task %s without an owned reference: %s",
                key,
                generation.failure_info,
            )
            return False
        generation.revoked = True
        generation.failure_info = "sandbox task was revoked"
        if generation.phase is _GenerationPhase.RUNNING:
            generation.phase = _GenerationPhase.TERMINATING
            generation.next_action = 0.0
        self._schedule_terminations()
        self._schedule_fences()
        return False

    def _generation_from_external_id(self, external_id: str | None) -> _TaskGeneration | None:
        if SandboxExecutionRef.has_envelope(external_id):
            try:
                ref = SandboxExecutionRef.decode(external_id, strict=True)
            except SandboxInvalidHandleError as error:
                return _TaskGeneration(
                    request_id=None,
                    phase=_GenerationPhase.QUARANTINED,
                    failure_info=f"persisted sandbox reference is malformed: {error}",
                )
            if ref is None or ref.driver != self.driver_id:
                owner = ref.driver if ref is not None else "unknown"
                return _TaskGeneration(
                    request_id=None,
                    phase=_GenerationPhase.QUARANTINED,
                    failure_info=f"persisted sandbox reference belongs to driver {owner!r}",
                )
            return _TaskGeneration(
                request_id=ref.request_id,
                phase=_GenerationPhase.RUNNING,
                ref=ref,
            )
        if is_preassigned_executor_id(external_id):
            return _TaskGeneration(
                request_id=str(external_id),
                phase=_GenerationPhase.FENCING,
            )
        if external_id:
            return _TaskGeneration(
                request_id=None,
                phase=_GenerationPhase.QUARANTINED,
                failure_info="persisted sandbox reference has an unknown format",
            )
        return None

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
        if self._runner is None:
            return
        self._closing = True
        deadline = time.monotonic() + self._shutdown_timeout
        self._wait_for_safety(deadline)
        self._close_runner(deadline)

    def terminate(self) -> None:
        if self._runner is None:
            return
        self._closing = True
        self.queued_tasks.clear()
        for key, generation in list(self._generations.items()):
            generation.revoked = True
            generation.failure_info = "sandbox executor was terminated"
            if generation.phase is _GenerationPhase.RUNNING:
                generation.phase = _GenerationPhase.FENCING
                generation.next_action = 0.0
            elif generation.phase is _GenerationPhase.QUARANTINED:
                self.log.warning(
                    "Cannot clean up quarantined sandbox task %s without an owned reference", key
                )
        deadline = time.monotonic() + self._shutdown_timeout
        self._wait_for_safety(deadline)
        self._close_runner(deadline)

    def _wait_for_safety(self, deadline: float) -> None:
        while self._has_unsettled_safety_work() and time.monotonic() < deadline:
            self.sync()
            time.sleep(0.05)
        if self._has_unsettled_safety_work():
            self.log.warning(
                "Timed out settling sandbox lifecycle operations; the runner will continue and provider TTLs remain active"
            )

    def _has_unsettled_safety_work(self) -> bool:
        return any(
            generation.phase
            in {_GenerationPhase.LAUNCHING, _GenerationPhase.TERMINATING, _GenerationPhase.FENCING}
            for generation in self._generations.values()
        )

    def _close_runner(self, deadline: float) -> None:
        runner = self._runner
        if runner is None:
            return
        completed = runner.close(timeout=max(0.0, deadline - time.monotonic()))
        self._drain_runner_results()
        if not completed:
            self.log.warning("Sandbox runner is still completing required lifecycle operations")
        self._runner = None

    def _require_runner(self) -> _SandboxExecutorRunner:
        if self._runner is None:
            raise RuntimeError(f"{type(self).__name__}.start() must be called first")
        return self._runner

    @staticmethod
    def get_cli_commands() -> list:
        return []
