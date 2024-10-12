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
import logging
import math
import os
import subprocess
import traceback
import warnings
from concurrent.futures import ProcessPoolExecutor
from typing import TYPE_CHECKING, Any, Mapping, MutableMapping, Optional, Tuple

from celery import Celery, Task, states as celery_states
from celery.backends.base import BaseKeyValueStoreBackend
from celery.backends.database import DatabaseBackend, Task as TaskDb, retry, session_cleanup
from celery.signals import import_modules as celery_import_modules
from setproctitle import setproctitle
from sqlalchemy import select

import airflow.settings as settings
from airflow.api_internal.internal_api_call import InternalApiConfig
from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning, AirflowTaskTimeout
from airflow.executors.base_executor import BaseExecutor
from airflow.stats import Stats
from airflow.utils.dag_parsing_context import _airflow_parsing_context_manager
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.timeout import timeout

log = logging.getLogger(__name__)

if TYPE_CHECKING:
    from celery.result import AsyncResult

    from airflow.executors.base_executor import CommandType, EventBufferValueType
    from airflow.models.taskinstance import TaskInstanceKey

    TaskInstanceInCelery = Tuple[TaskInstanceKey, CommandType, Optional[str], Task]

OPERATION_TIMEOUT = conf.getfloat("celery", "operation_timeout")

# Make it constant for unit test.
CELERY_FETCH_ERR_MSG_HEADER = "Error fetching Celery task state"

celery_configuration = None


@providers_configuration_loaded
def _get_celery_app() -> Celery:
    """Init providers before importing the configuration, so the _SECRET and _CMD options work."""
    global celery_configuration

    if conf.has_option("celery", "celery_config_options"):
        celery_configuration = conf.getimport("celery", "celery_config_options")
    else:
        from airflow.providers.celery.executors.default_celery import DEFAULT_CELERY_CONFIG

        celery_configuration = DEFAULT_CELERY_CONFIG

    celery_app_name = conf.get("celery", "CELERY_APP_NAME")
    if celery_app_name == "airflow.executors.celery_executor":
        warnings.warn(
            "The celery.CELERY_APP_NAME configuration uses deprecated package name: "
            "'airflow.executors.celery_executor'. "
            "Change it to `airflow.providers.celery.executors.celery_executor`, and "
            "update the `-app` flag in your Celery Health Checks "
            "to use `airflow.providers.celery.executors.celery_executor.app`.",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )

    return Celery(celery_app_name, config_source=celery_configuration)


app = _get_celery_app()


@celery_import_modules.connect
def on_celery_import_modules(*args, **kwargs):
    """
    Preload some "expensive" airflow modules once, so other task processes won't have to import it again.

    Loading these for each task adds 0.3-0.5s *per task* before the task can run. For long running tasks this
    doesn't matter, but for short tasks this starts to be a noticeable impact.
    """
    import jinja2.ext  # noqa: F401

    import airflow.jobs.local_task_job_runner
    import airflow.macros

    try:
        import airflow.providers.standard.operators.python
        import airflow.providers.standard.operators.bash
    except ImportError:
        import airflow.operators.bash  # noqa: F401
        import airflow.operators.python  # noqa: F401

    with contextlib.suppress(ImportError):
        import numpy  # noqa: F401

    with contextlib.suppress(ImportError):
        import kubernetes.client  # noqa: F401


@app.task
def execute_command(command_to_exec: CommandType) -> None:
    """Execute command."""
    dag_id, task_id = BaseExecutor.validate_airflow_tasks_run_command(command_to_exec)
    celery_task_id = app.current_task.request.id
    log.info("[%s] Executing command in Celery: %s", celery_task_id, command_to_exec)
    with _airflow_parsing_context_manager(dag_id=dag_id, task_id=task_id):
        try:
            if settings.EXECUTE_TASKS_NEW_PYTHON_INTERPRETER:
                _execute_in_subprocess(command_to_exec, celery_task_id)
            else:
                _execute_in_fork(command_to_exec, celery_task_id)
        except Exception:
            Stats.incr("celery.execute_command.failure")
            raise


def _execute_in_fork(command_to_exec: CommandType, celery_task_id: str | None = None) -> None:
    pid = os.fork()
    if pid:
        # In parent, wait for the child
        pid, ret = os.waitpid(pid, 0)
        if ret == 0:
            return

        msg = f"Celery command failed on host: {get_hostname()} with celery_task_id {celery_task_id} (PID: {pid}, Return Code: {ret})"
        raise AirflowException(msg)

    from airflow.sentry import Sentry

    ret = 1
    try:
        from airflow.cli.cli_parser import get_parser

        if not InternalApiConfig.get_use_internal_api():
            settings.engine.pool.dispose()
            settings.engine.dispose()

        parser = get_parser()
        # [1:] - remove "airflow" from the start of the command
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
        subprocess.check_output(command_to_exec, stderr=subprocess.STDOUT, close_fds=True, env=env)
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


def send_task_to_executor(
    task_tuple: TaskInstanceInCelery,
) -> tuple[TaskInstanceKey, CommandType, AsyncResult | ExceptionWithTraceback]:
    """Send task to executor."""
    key, command, queue, task_to_run = task_tuple
    try:
        with timeout(seconds=OPERATION_TIMEOUT):
            result = task_to_run.apply_async(args=[command], queue=queue)
    except (Exception, AirflowTaskTimeout) as e:
        exception_traceback = f"Celery Task ID: {key}\n{traceback.format_exc()}"
        result = ExceptionWithTraceback(e, exception_traceback)

    return key, command, result


def fetch_celery_task_state(async_result: AsyncResult) -> tuple[str, str | ExceptionWithTraceback, Any]:
    """
    Fetch and return the state of the given celery task.

    The scope of this function is global so that it can be called by subprocesses in the pool.

    :param async_result: a tuple of the Celery task key and the async Celery object used
        to fetch the task's state
    :return: a tuple of the Celery task key and the Celery state and the celery info
        of the task
    """
    try:
        with timeout(seconds=OPERATION_TIMEOUT):
            # Accessing state property of celery task will make actual network request
            # to get the current state of the task
            info = async_result.info if hasattr(async_result, "info") else None
            return async_result.task_id, async_result.state, info
    except Exception as e:
        exception_traceback = f"Celery Task ID: {async_result}\n{traceback.format_exc()}"
        return async_result.task_id, ExceptionWithTraceback(e, exception_traceback), None


class BulkStateFetcher(LoggingMixin):
    """
    Gets status for many Celery tasks using the best method available.

    If BaseKeyValueStoreBackend is used as result backend, the mget method is used.
    If DatabaseBackend is used as result backend, the SELECT ...WHERE task_id IN (...) query is used
    Otherwise, multiprocessing.Pool will be used. Each task status will be downloaded individually.
    """

    def __init__(self, sync_parallelism=None):
        super().__init__()
        self._sync_parallelism = sync_parallelism

    def _tasks_list_to_task_ids(self, async_tasks) -> set[str]:
        return {a.task_id for a in async_tasks}

    def get_many(self, async_results) -> Mapping[str, EventBufferValueType]:
        """Get status for many Celery tasks using the best method available."""
        if isinstance(app.backend, BaseKeyValueStoreBackend):
            result = self._get_many_from_kv_backend(async_results)
        elif isinstance(app.backend, DatabaseBackend):
            result = self._get_many_from_db_backend(async_results)
        else:
            result = self._get_many_using_multiprocessing(async_results)
        self.log.debug("Fetched %d state(s) for %d task(s)", len(result), len(async_results))
        return result

    def _get_many_from_kv_backend(self, async_tasks) -> Mapping[str, EventBufferValueType]:
        task_ids = self._tasks_list_to_task_ids(async_tasks)
        keys = [app.backend.get_key_for_task(k) for k in task_ids]
        values = app.backend.mget(keys)
        task_results = [app.backend.decode_result(v) for v in values if v]
        task_results_by_task_id = {task_result["task_id"]: task_result for task_result in task_results}

        return self._prepare_state_and_info_by_task_dict(task_ids, task_results_by_task_id)

    @retry
    def _query_task_cls_from_db_backend(self, task_ids, **kwargs):
        session = app.backend.ResultSession()
        task_cls = getattr(app.backend, "task_cls", TaskDb)
        with session_cleanup(session):
            return session.scalars(select(task_cls).where(task_cls.task_id.in_(task_ids))).all()

    def _get_many_from_db_backend(self, async_tasks) -> Mapping[str, EventBufferValueType]:
        task_ids = self._tasks_list_to_task_ids(async_tasks)
        tasks = self._query_task_cls_from_db_backend(task_ids)
        task_results = [app.backend.meta_from_decoded(task.to_dict()) for task in tasks]
        task_results_by_task_id = {task_result["task_id"]: task_result for task_result in task_results}

        return self._prepare_state_and_info_by_task_dict(task_ids, task_results_by_task_id)

    @staticmethod
    def _prepare_state_and_info_by_task_dict(
        task_ids, task_results_by_task_id
    ) -> Mapping[str, EventBufferValueType]:
        state_info: MutableMapping[str, EventBufferValueType] = {}
        for task_id in task_ids:
            task_result = task_results_by_task_id.get(task_id)
            if task_result:
                state = task_result["status"]
                info = None if not hasattr(task_result, "info") else task_result["info"]
            else:
                state = celery_states.PENDING
                info = None
            state_info[task_id] = state, info
        return state_info

    def _get_many_using_multiprocessing(self, async_results) -> Mapping[str, EventBufferValueType]:
        num_process = min(len(async_results), self._sync_parallelism)

        with ProcessPoolExecutor(max_workers=num_process) as sync_pool:
            chunksize = max(1, math.ceil(len(async_results) / self._sync_parallelism))

            task_id_to_states_and_info = list(
                sync_pool.map(fetch_celery_task_state, async_results, chunksize=chunksize)
            )

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
