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
"""
Utilities and classes used by the Celery Executor.

Much of this code is expensive to import/load, be careful where this module is imported.
"""

from __future__ import annotations

import contextlib
import gc
import logging
import math
import multiprocessing
import os
import subprocess
import sys
import traceback
from collections.abc import Collection, Mapping, MutableMapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from functools import cache
from importlib import import_module
from typing import TYPE_CHECKING, Any

from celery import Celery, states as celery_states
from celery.backends.base import BaseKeyValueStoreBackend
from celery.backends.database import DatabaseBackend, Task as TaskDb, retry, session_cleanup
from celery.signals import import_modules as celery_import_modules, worker_ready
from sqlalchemy import select

from airflow.executors.base_executor import BaseExecutor
from airflow.providers.celery.version_compat import (
    AIRFLOW_V_3_0_PLUS,
    AIRFLOW_V_3_1_9_PLUS,
    AIRFLOW_V_3_2_PLUS,
    AIRFLOW_V_3_3_PLUS,
)
from airflow.providers.common.compat.sdk import AirflowException, AirflowTaskTimeout, Stats, conf, timeout
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.providers_configuration_loader import providers_configuration_loaded

try:
    from airflow.sdk.definitions._internal.dag_parsing_context import _airflow_parsing_context_manager
except ImportError:
    from airflow.utils.dag_parsing_context import _airflow_parsing_context_manager  # type:ignore[no-redef]


log = logging.getLogger(__name__)

if sys.platform == "darwin":
    setproctitle = lambda title: log.debug("Mac OS detected, skipping setproctitle")
else:
    from setproctitle import setproctitle

if TYPE_CHECKING:
    from typing import TypeAlias

    from celery.result import AsyncResult

    from airflow.configuration import AirflowConfigParser
    from airflow.executors import workloads
    from airflow.executors.base_executor import EventBufferValueType, ExecutorConf
    from airflow.executors.workloads.types import WorkloadKey
    from airflow.models.taskinstance import TaskInstanceKey

    # We can't use `if AIRFLOW_V_3_0_PLUS` conditions in type checks, so unfortunately we just have to define
    # the type as the union of both kinds.
    CommandType = Sequence[str]

    WorkloadInCelery: TypeAlias = tuple[WorkloadKey, workloads.All | CommandType, str | None, str | None]
    WorkloadInCeleryResult: TypeAlias = tuple[
        WorkloadKey, CommandType, AsyncResult | "ExceptionWithTraceback"
    ]

    # Deprecated alias for backward compatibility.
    TaskInstanceInCelery: TypeAlias = WorkloadInCelery

    TaskTuple = tuple[TaskInstanceKey, CommandType, str | None, Any | None]

OPERATION_TIMEOUT = conf.getfloat("celery", "operation_timeout")

# Make it constant for unit test.
CELERY_FETCH_ERR_MSG_HEADER = "Error fetching Celery task state"


@cache
def get_celery_configuration() -> dict[str, Any]:
    """Get the Celery configuration dictionary."""
    if conf.has_option("celery", "celery_config_options"):
        return conf.getimport("celery", "celery_config_options")

    from airflow.providers.celery.executors.default_celery import DEFAULT_CELERY_CONFIG

    return DEFAULT_CELERY_CONFIG


@providers_configuration_loaded
def _get_celery_app() -> Celery:
    """Init providers before importing the configuration, so the _SECRET and _CMD options work."""
    celery_app_name = conf.get("celery", "CELERY_APP_NAME")

    return Celery(celery_app_name, config_source=get_celery_configuration())


def create_celery_app(team_conf: ExecutorConf | AirflowConfigParser) -> Celery:
    """
    Create a Celery app, supporting team-specific configuration.

    :param team_conf: ExecutorConf instance with team-specific configuration, or global conf
    :return: Celery app instance
    """
    from airflow.providers.celery.executors.default_celery import (
        DEFAULT_CELERY_CONFIG,
        get_default_celery_config,
    )

    celery_app_name = team_conf.get("celery", "CELERY_APP_NAME")

    # Make app name unique per team to ensure proper broker isolation.
    # Each team's executor needs a distinct Celery app name to prevent
    # tasks from being routed to the wrong broker.
    # Only do this if team_conf is an ExecutorConf with team_name (not global conf).
    team_name = getattr(team_conf, "team_name", None)
    if team_name:
        celery_app_name = f"{celery_app_name}_{team_name}"

    config = get_default_celery_config(team_conf)

    # Apply user-provided celery_config_options on top of team config.
    # Skip if it resolves to DEFAULT_CELERY_CONFIG (built from global conf, not team-aware).
    configured_path = team_conf.get("celery", "celery_config_options", fallback=None)
    if configured_path:
        module_path, _, attr_name = configured_path.rpartition(".")
        user_config = getattr(import_module(module_path), attr_name)
        if user_config is not DEFAULT_CELERY_CONFIG and isinstance(user_config, dict):
            config.update(user_config)

    celery_app = Celery(celery_app_name, config_source=config)

    # Register tasks with this app.
    celery_app.task(name="execute_workload")(execute_workload)
    if not AIRFLOW_V_3_0_PLUS:
        celery_app.task(name="execute_command")(execute_command)

    return celery_app


@cache
def _get_celery_app_for_workload(team_name: str | None) -> Celery:
    """
    Return a Celery app cached by team name for task publishing.

    Publishing runs in the caller process (the scheduler). Caching the app there amortizes
    result backend resolution across publish cycles while retaining per-team broker isolation.
    """
    if AIRFLOW_V_3_2_PLUS:
        from airflow.executors.base_executor import ExecutorConf

        _conf = ExecutorConf(team_name)
    else:
        # Airflow <3.2 ExecutorConf doesn't exist (at least not with the required attributes), fall back to global conf.
        _conf = conf
    return create_celery_app(_conf)


# Keep module-level app for backward compatibility.
app = _get_celery_app()


@celery_import_modules.connect
def on_celery_import_modules(*args, **kwargs):
    """
    Preload some "expensive" airflow modules once, so other task processes won't have to import it again.

    Loading these for each task adds 0.3-0.5s *per task* before the task can run. For long-running tasks this
    doesn't matter, but for short tasks this starts to be a noticeable impact.
    """
    import jinja2.ext  # noqa: F401

    if not AIRFLOW_V_3_0_PLUS:
        import airflow.jobs.local_task_job_runner
        import airflow.macros

    try:
        import airflow.providers.standard.operators.bash
        import airflow.providers.standard.operators.python
    except ImportError:
        import airflow.operators.bash
        import airflow.operators.python  # noqa: F401

    with contextlib.suppress(ImportError):
        import numpy  # noqa: F401

    with contextlib.suppress(ImportError):
        import kubernetes.client  # noqa: F401

    # To prevent memory increase by COW in celery's ForkPoolWorker.
    gc.freeze()


@worker_ready.connect
def on_celery_worker_ready(*args, **kwargs):
    # Unfreeze the objects from gc freeze when the ForkPoolWorker is all loaded.
    gc.unfreeze()


# Once Celery 5.5 is out of beta, we can pass `pydantic=True` to the decorator and it will handle the validation
# and deserialization for us.
@app.task(name="execute_workload")
def execute_workload(input: str) -> None:
    if not AIRFLOW_V_3_3_PLUS:
        return _execute_workload_pre_3_3(input)

    from celery.exceptions import Ignore
    from pydantic import TypeAdapter

    from airflow.executors.workloads import ExecutorWorkload

    decoder = TypeAdapter[ExecutorWorkload](ExecutorWorkload)
    workload = decoder.validate_json(input)

    celery_task_id = app.current_task.request.id

    log.info("[%s] Executing workload in Celery: %s", celery_task_id, workload)

    try:
        BaseExecutor.run_workload(workload)
    except Exception as e:
        from airflow.sdk.exceptions import TaskAlreadyRunningError

        if isinstance(e, TaskAlreadyRunningError):
            log.info("[%s] Task already running elsewhere, ignoring redelivered message", celery_task_id)
            # Raise Ignore() so Celery does not record a FAILURE result for this duplicate
            # delivery. Without this, the broker redelivering the message (e.g. after a
            # visibility timeout) would cause Celery to mark the task as failed, even though
            # the original worker is still executing it successfully.
            raise Ignore()
        raise


def _execute_workload_pre_3_3(input: str) -> None:
    """Fallback for Airflow < 3.3 which lacks BaseExecutor.run_workload() and ExecutorWorkload."""
    from celery.exceptions import Ignore
    from pydantic import TypeAdapter

    from airflow.executors import workloads
    from airflow.sdk.execution_time.supervisor import supervise

    decoder = TypeAdapter[workloads.All](workloads.All)
    workload = decoder.validate_json(input)

    celery_task_id = app.current_task.request.id

    log.info("[%s] Executing workload in Celery: %s", celery_task_id, workload)

    base_url = conf.get("api", "base_url", fallback="/")
    # If it's a relative URL, use localhost:8080 as the default.
    if base_url.startswith("/"):
        base_url = f"http://localhost:8080{base_url}"
    default_execution_api_server = f"{base_url.rstrip('/')}/execution/"

    try:
        if isinstance(workload, workloads.ExecuteTask):
            supervise(
                # This is the "wrong" ti type, but it duck types the same. TODO: Create a protocol for this.
                ti=workload.ti,  # type: ignore[arg-type]
                dag_rel_path=workload.dag_rel_path,
                bundle_info=workload.bundle_info,
                token=workload.token,
                server=conf.get("core", "execution_api_server_url", fallback=default_execution_api_server),
                log_path=workload.log_path,
            )
        else:
            raise ValueError(f"CeleryExecutor does not know how to handle {type(workload)}")
    except Exception as e:
        if AIRFLOW_V_3_1_9_PLUS:
            from airflow.sdk.exceptions import TaskAlreadyRunningError

            if isinstance(e, TaskAlreadyRunningError):
                log.info("[%s] Task already running elsewhere, ignoring redelivered message", celery_task_id)
                # Raise Ignore() so Celery does not record a FAILURE result for this duplicate
                # delivery. Without this, the broker redelivering the message (e.g. after a
                # visibility timeout) would cause Celery to mark the task as failed, even though
                # the original worker is still executing it successfully.
                raise Ignore()
        raise


if not AIRFLOW_V_3_0_PLUS:

    @app.task(name="execute_command")
    def execute_command(command_to_exec: CommandType) -> None:
        """Execute command."""
        EXECUTE_TASKS_NEW_PYTHON_INTERPRETER = not hasattr(os, "fork") or conf.getboolean(
            "core",
            "execute_tasks_new_python_interpreter",
            fallback=False,
        )

        dag_id, task_id = BaseExecutor.validate_airflow_tasks_run_command(command_to_exec)  # type: ignore[attr-defined]
        celery_task_id = app.current_task.request.id
        log.info("[%s] Executing command in Celery: %s", celery_task_id, command_to_exec)
        with _airflow_parsing_context_manager(dag_id=dag_id, task_id=task_id):
            try:
                if EXECUTE_TASKS_NEW_PYTHON_INTERPRETER:
                    _execute_in_subprocess(command_to_exec, celery_task_id)
                else:
                    _execute_in_fork(command_to_exec, celery_task_id)
            except Exception:
                Stats.incr("celery.execute_command.failure")
                raise


def _execute_in_fork(command_to_exec: CommandType, celery_task_id: str | None = None) -> None:
    pid = os.fork()
    if pid:
        # In parent, wait for the child.
        pid, ret = os.waitpid(pid, 0)
        if ret == 0:
            return

        msg = f"Celery command failed on host: {get_hostname()} with celery_task_id {celery_task_id} (PID: {pid}, Return Code: {ret})"
        raise AirflowException(msg)

    from airflow.sentry import Sentry

    ret = 1
    try:
        from airflow.cli.cli_parser import get_parser

        parser = get_parser()
        # [1:] - remove "airflow" from the start of the command.
        args = parser.parse_args(command_to_exec[1:])
        args.shut_down_logging = False
        if celery_task_id:
            args.external_executor_id = celery_task_id

        setproctitle(f"airflow task supervisor: {command_to_exec}")
        log.debug("calling func '%s' with args %s", args.func.__name__, args)
        args.func(args)
        ret = 0
    except Exception:
        log.exception("[%s] Failed to execute task.", celery_task_id)
        ret = 1
    finally:
        try:
            Sentry.flush()
            logging.shutdown()
        except Exception:
            log.exception("[%s] Failed to clean up.", celery_task_id)
            ret = 1
        os._exit(ret)


def _execute_in_subprocess(command_to_exec: CommandType, celery_task_id: str | None = None) -> None:
    env = os.environ.copy()
    if celery_task_id:
        env["external_executor_id"] = celery_task_id
    try:
        subprocess.run(
            command_to_exec,
            check=False,
            stderr=sys.__stderr__,
            stdout=sys.__stdout__,
            close_fds=True,
            env=env,
        )
    except subprocess.CalledProcessError as e:
        log.exception("[%s] execute_command encountered a CalledProcessError", celery_task_id)
        log.error(e.output)
        msg = f"Celery command failed on host: {get_hostname()} with celery_task_id {celery_task_id}"
        raise AirflowException(msg)


class ExceptionWithTraceback:
    """
    Wrapper class used to propagate exceptions to parent processes from subprocesses.

    :param exception: The exception to wrap
    :param exception_traceback: The stacktrace to wrap
    """

    def __init__(self, exception: BaseException, exception_traceback: str):
        self.exception = exception
        self.traceback = exception_traceback


def send_workload_to_executor(
    workload_tuple: WorkloadInCelery,
) -> WorkloadInCeleryResult:
    """
    Send workload to executor (serialized and executed as a Celery task).

    This runs inline in the long-lived caller process (the scheduler, when publishing). The
    team-specific Celery app is built from ``team_name`` at call time and cached for the life of
    that process, so result backend resolution is amortized across publish cycles.
    """
    key, args, queue, team_name = workload_tuple

    celery_app = _get_celery_app_for_workload(team_name)

    celery_task_id = None
    if AIRFLOW_V_3_0_PLUS:
        # Get the task from the app.
        celery_task = celery_app.tasks["execute_workload"]
        if TYPE_CHECKING:
            assert isinstance(args, workloads.BaseWorkload)
        # Extract the pre-assigned Celery task ID before serializing the workload.
        # This ID was committed to the DB at queuing time (as external_executor_id) and is
        # excluded from model_dump_json(), so workers never see it. Passing it to apply_async()
        # makes the Celery task ID deterministic from DB state, closing the race window where a
        # scheduler crash between apply_async() and event processing left external_executor_id
        # unset and the task unadoptable.
        if executor_id := getattr(getattr(args, "ti", None), "external_executor_id", None):
            celery_task_id = executor_id
        args = (args.model_dump_json(),)
    else:
        # Get the task from the app.
        celery_task = celery_app.tasks["execute_command"]
        args = [args]  # type: ignore[list-item]

    # Pre-import redis.client to avoid SIGALRM interrupting module initialization.
    # If timeout fires during import, redis module gets partially cached in sys.modules
    # without the 'client' submodule bound, causing AttributeError on subsequent access.
    # See: https://github.com/apache/airflow/issues/41359
    # Redis not installed or not using Redis backend.
    with contextlib.suppress(ImportError):
        import redis.client  # noqa: F401

    try:
        with timeout(seconds=OPERATION_TIMEOUT):
            result = celery_task.apply_async(args=args, queue=queue, task_id=celery_task_id)
    except (Exception, AirflowTaskTimeout) as e:
        exception_traceback = f"Celery Task ID: {key}\n{traceback.format_exc()}"
        result = ExceptionWithTraceback(e, exception_traceback)

    # The type is right for the version, but the type cannot be defined correctly for Airflow 2 and 3
    # concurrently.
    return key, args, result


def fetch_celery_task_state(async_result: AsyncResult) -> tuple[str, str | ExceptionWithTraceback, Any]:
    """
    Fetch and return the state of the given celery task (workload execution).

    The scope of this function is global so that it can be called by subprocesses in the pool.

    :param async_result: a tuple of the Celery task key and the async Celery object used
        to fetch the task's state
    :return: a tuple of the Celery task key and the Celery state and the celery info
        of the task
    """
    # Pre-import redis.client to avoid SIGALRM interrupting module initialization.
    # See: https://github.com/apache/airflow/issues/41359
    # Redis not installed or not using Redis backend.
    with contextlib.suppress(ImportError):
        import redis.client  # noqa: F401

    try:
        with timeout(seconds=OPERATION_TIMEOUT):
            # Accessing state property of celery task (workload execution) triggers a network request
            # to get the current state of the task.
            info = async_result.info if hasattr(async_result, "info") else None
            return async_result.task_id, async_result.state, info
    except Exception as e:
        exception_traceback = f"Celery Task ID: {async_result}\n{traceback.format_exc()}"
        return async_result.task_id, ExceptionWithTraceback(e, exception_traceback), None


def _get_state_fetch_mp_context() -> multiprocessing.context.BaseContext:
    """
    Return the ``multiprocessing`` context for the bulk state-fetch pool.

    ``fork`` is unsafe here because the pool is created from the multi-threaded scheduler
    process: a worker can inherit a mutex held by a thread that ``fork()`` did not copy, then
    block forever acquiring it, which stalls the scheduling loop until the scheduler is
    restarted. ``forkserver``/``spawn`` workers start without the parent's locks.

    Honours ``[celery] mp_start_method`` (then ``[core] mp_start_method``) so an operator who
    has deliberately pinned a method keeps control, and falls back to ``forkserver`` then
    ``spawn``. The lookup is inlined rather than delegated to
    ``airflow.utils.process_utils.resolve_mp_start_method`` because that helper only exists on
    Airflow 3.3+, and this provider supports 2.11 onwards.
    """
    available = multiprocessing.get_all_start_methods()
    configured = conf.get("celery", "mp_start_method", fallback=None) or conf.get(
        "core", "mp_start_method", fallback=None
    )
    configured = configured.strip() if configured else None

    if configured:
        if configured not in available:
            log.warning(
                "Configured mp_start_method=%r is not available on this platform (available: %s); "
                "falling back to a non-fork start method for the Celery state-fetch pool.",
                configured,
                available,
            )
        else:
            if configured == "fork":
                log.warning(
                    "mp_start_method is set to 'fork' for the Celery state-fetch pool. Forking the "
                    "multi-threaded scheduler can deadlock a worker on an inherited lock and stall "
                    "scheduling; prefer 'forkserver' or 'spawn'."
                )
            return multiprocessing.get_context(configured)

    return multiprocessing.get_context("forkserver" if "forkserver" in available else "spawn")


class BulkStateFetcher(LoggingMixin):
    """
    Gets status for many Celery tasks using the best method available.

    If BaseKeyValueStoreBackend is used as result backend, the mget method is used.
    If DatabaseBackend is used as result backend, the SELECT ...WHERE task_id IN (...) query is used
    Otherwise, multiprocessing.Pool will be used. Each task status will be downloaded individually.
    """

    def __init__(self, sync_parallelism: int, celery_app: Celery | None = None):
        super().__init__()
        self._sync_parallelism = sync_parallelism
        self.celery_app = celery_app or app  # Use provided app or fall back to module-level app.
        self._sync_pool: ProcessPoolExecutor | None = None

    def _get_or_create_sync_pool(self) -> ProcessPoolExecutor:
        """
        Return the state-fetch pool, creating it on first use.

        The pool is reused across syncs. ``forkserver``/``spawn`` workers re-import Airflow
        instead of inheriting it, so rebuilding the pool on every sync would pay that import
        cost on every scheduler heartbeat.
        """
        if self._sync_pool is None:
            self._sync_pool = ProcessPoolExecutor(
                max_workers=max(1, self._sync_parallelism), mp_context=_get_state_fetch_mp_context()
            )
        return self._sync_pool

    def shutdown(self, wait: bool = True) -> None:
        """Shut the state-fetch pool down, if one was ever created."""
        if self._sync_pool is None:
            return
        pool, self._sync_pool = self._sync_pool, None
        pool.shutdown(wait=wait, cancel_futures=not wait)

    def _tasks_list_to_task_ids(self, async_tasks: Collection[AsyncResult]) -> set[str]:
        return {a.task_id for a in async_tasks}

    def get_many(self, async_results: Collection[AsyncResult]) -> Mapping[str, EventBufferValueType]:
        """Get status for many Celery tasks using the best method available."""
        if isinstance(self.celery_app.backend, BaseKeyValueStoreBackend):
            result = self._get_many_from_kv_backend(async_results)
        elif isinstance(self.celery_app.backend, DatabaseBackend):
            result = self._get_many_from_db_backend(async_results)
        else:
            result = self._get_many_using_multiprocessing(async_results)
        self.log.debug("Fetched %d state(s) for %d task(s)", len(result), len(async_results))
        return result

    def _get_many_from_kv_backend(
        self, async_tasks: Collection[AsyncResult]
    ) -> Mapping[str, EventBufferValueType]:
        task_ids = self._tasks_list_to_task_ids(async_tasks)
        keys = [self.celery_app.backend.get_key_for_task(k) for k in task_ids]
        values = self.celery_app.backend.mget(keys)
        task_results = [self.celery_app.backend.decode_result(v) for v in values if v]
        task_results_by_task_id = {task_result["task_id"]: task_result for task_result in task_results}

        return self._prepare_state_and_info_by_task_dict(task_ids, task_results_by_task_id)

    @retry
    def _query_task_cls_from_db_backend(self, task_ids: set[str], **kwargs):
        session = self.celery_app.backend.ResultSession()
        task_cls = getattr(self.celery_app.backend, "task_cls", TaskDb)
        with session_cleanup(session):
            return session.scalars(select(task_cls).where(task_cls.task_id.in_(task_ids))).all()

    def _get_many_from_db_backend(
        self, async_tasks: Collection[AsyncResult]
    ) -> Mapping[str, EventBufferValueType]:
        task_ids = self._tasks_list_to_task_ids(async_tasks)
        tasks = self._query_task_cls_from_db_backend(task_ids)
        task_results = [self.celery_app.backend.meta_from_decoded(task.to_dict()) for task in tasks]
        task_results_by_task_id = {task_result["task_id"]: task_result for task_result in task_results}

        return self._prepare_state_and_info_by_task_dict(task_ids, task_results_by_task_id)

    @staticmethod
    def _prepare_state_and_info_by_task_dict(
        task_ids: set[str], task_results_by_task_id: dict[str, dict[str, Any]]
    ) -> Mapping[str, EventBufferValueType]:
        state_info: MutableMapping[str, EventBufferValueType] = {}
        for task_id in task_ids:
            task_result = task_results_by_task_id.get(task_id)
            if task_result:
                state = task_result["status"]
                info = task_result.get("info")
            else:
                state = celery_states.PENDING
                info = None
            state_info[task_id] = state, info
        return state_info

    def _get_many_using_multiprocessing(
        self, async_results: Collection[AsyncResult]
    ) -> Mapping[str, EventBufferValueType]:
        chunksize = max(1, math.ceil(len(async_results) / self._sync_parallelism))

        try:
            task_id_to_states_and_info = self._map_fetch_over_pool(async_results, chunksize)
        except BrokenProcessPool:
            # A worker died, which breaks the pool permanently. Discard it and retry once on a
            # fresh pool so one dead worker does not wedge state fetching until a restart.
            self.log.warning("Celery state-fetch pool broke; recreating it and retrying.")
            self.shutdown(wait=False)
            task_id_to_states_and_info = self._map_fetch_over_pool(async_results, chunksize)

        states_and_info_by_task_id: MutableMapping[str, EventBufferValueType] = {}
        for task_id, state_or_exception, info in task_id_to_states_and_info:
            if isinstance(state_or_exception, ExceptionWithTraceback):
                self.log.error(
                    "%s:%s\n%s\n",
                    CELERY_FETCH_ERR_MSG_HEADER,
                    state_or_exception.exception,
                    state_or_exception.traceback,
                )
            else:
                states_and_info_by_task_id[task_id] = state_or_exception, info
        return states_and_info_by_task_id

    def _map_fetch_over_pool(self, async_results: Collection[AsyncResult], chunksize: int) -> list:
        pool = self._get_or_create_sync_pool()
        return list(pool.map(fetch_celery_task_state, async_results, chunksize=chunksize))
