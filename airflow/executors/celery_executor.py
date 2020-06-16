#
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
"""CeleryExecutor

.. seealso::
    For more information on how the CeleryExecutor works, take a look at the guide:
    :ref:`executor:CeleryExecutor`
"""
import logging
import math
import os
import subprocess
import time
import traceback
from multiprocessing import Pool, cpu_count
from typing import Any, List, Mapping, MutableMapping, Optional, Set, Tuple, Union

from celery import Celery, Task, states as celery_states
from celery.backends.base import BaseKeyValueStoreBackend
from celery.backends.database import DatabaseBackend, Task as TaskDb, session_cleanup
from celery.result import AsyncResult

from airflow.config_templates.default_celery import DEFAULT_CELERY_CONFIG
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor, CommandType, EventBufferValueType
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstanceKeyType
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.timeout import timeout

log = logging.getLogger(__name__)

# Make it constant for unit test.
CELERY_FETCH_ERR_MSG_HEADER = 'Error fetching Celery task state'

CELERY_SEND_ERR_MSG_HEADER = 'Error sending Celery task'

OPERATION_TIMEOUT = conf.getint('celery', 'operation_timeout', fallback=2)

'''
To start the celery worker, run the command:
airflow celery worker
'''

if conf.has_option('celery', 'celery_config_options'):
    celery_configuration = conf.getimport('celery', 'celery_config_options')
else:
    celery_configuration = DEFAULT_CELERY_CONFIG

app = Celery(
    conf.get('celery', 'CELERY_APP_NAME'),
    config_source=celery_configuration)


@app.task
def execute_command(command_to_exec: CommandType) -> None:
    """Executes command."""
    if command_to_exec[0:3] != ["airflow", "tasks", "run"]:
        raise ValueError('The command must start with ["airflow", "tasks", "run"].')

    log.info("Executing command in Celery: %s", command_to_exec)
    env = os.environ.copy()
    try:
        subprocess.check_call(command_to_exec, stderr=subprocess.STDOUT,
                              close_fds=True, env=env)
    except subprocess.CalledProcessError as e:
        log.exception('execute_command encountered a CalledProcessError')
        log.error(e.output)
        msg = 'Celery command failed on host: ' + get_hostname()
        raise AirflowException(msg)


class ExceptionWithTraceback:
    """
    Wrapper class used to propagate exceptions to parent processes from subprocesses.

    :param exception: The exception to wrap
    :type exception: Exception
    :param exception_traceback: The stacktrace to wrap
    :type exception_traceback: str
    """

    def __init__(self, exception: Exception, exception_traceback: str):
        self.exception = exception
        self.traceback = exception_traceback


# Task instance that is sent over Celery queues
# TaskInstanceKeyType, SimpleTaskInstance, Command, queue_name, CallableTask
TaskInstanceInCelery = Tuple[TaskInstanceKeyType, SimpleTaskInstance, CommandType, Optional[str], Task]


def send_task_to_executor(task_tuple: TaskInstanceInCelery) \
        -> Tuple[TaskInstanceKeyType, CommandType, Union[AsyncResult, ExceptionWithTraceback]]:
    """Sends task to executor."""
    key, _, command, queue, task_to_run = task_tuple
    try:
        with timeout(seconds=OPERATION_TIMEOUT):
            result = task_to_run.apply_async(args=[command], queue=queue)
    except Exception as e:  # pylint: disable=broad-except
        exception_traceback = "Celery Task ID: {}\n{}".format(key, traceback.format_exc())
        result = ExceptionWithTraceback(e, exception_traceback)

    return key, command, result


class CeleryExecutor(BaseExecutor):
    """
    CeleryExecutor is recommended for production use of Airflow. It allows
    distributing the execution of task instances to multiple worker nodes.

    Celery is a simple, flexible and reliable distributed system to process
    vast amounts of messages, while providing operations with the tools
    required to maintain such a system.
    """

    def __init__(self):
        super().__init__()

        # Celery doesn't support bulk sending the tasks (which can become a bottleneck on bigger clusters)
        # so we use a multiprocessing pool to speed this up.
        # How many worker processes are created for checking celery task state.
        self._sync_parallelism = conf.getint('celery', 'SYNC_PARALLELISM')
        if self._sync_parallelism == 0:
            self._sync_parallelism = max(1, cpu_count() - 1)
        self.bulk_state_fetcher = BulkStateFetcher(self._sync_parallelism)
        self.tasks = {}
        self.last_state = {}

    def start(self) -> None:
        self.log.debug(
            'Starting Celery Executor using %s processes for syncing',
            self._sync_parallelism
        )

    def _num_tasks_per_send_process(self, to_send_count: int) -> int:
        """
        How many Celery tasks should each worker process send.

        :return: Number of tasks that should be sent per process
        :rtype: int
        """
        return max(1,
                   int(math.ceil(1.0 * to_send_count / self._sync_parallelism)))

    def trigger_tasks(self, open_slots: int) -> None:
        """
        Overwrite trigger_tasks function from BaseExecutor

        :param open_slots: Number of open slots
        :return:
        """
        sorted_queue = self.order_queued_tasks_by_priority()

        task_tuples_to_send: List[TaskInstanceInCelery] = []

        for _ in range(min((open_slots, len(self.queued_tasks)))):
            key, (command, _, queue, simple_ti) = sorted_queue.pop(0)
            task_tuples_to_send.append((key, simple_ti, command, queue, execute_command))

        if task_tuples_to_send:
            first_task = next(t[4] for t in task_tuples_to_send)

            # Celery state queries will stuck if we do not use one same backend
            # for all tasks.
            cached_celery_backend = first_task.backend

            key_and_async_results = self._send_tasks_to_celery(task_tuples_to_send)
            self.log.debug('Sent all tasks.')

            for key, command, result in key_and_async_results:
                if isinstance(result, ExceptionWithTraceback):
                    self.log.error(  # pylint: disable=logging-not-lazy
                        CELERY_SEND_ERR_MSG_HEADER + ":%s\n%s\n", result.exception, result.traceback
                    )
                elif result is not None:
                    # Only pops when enqueued successfully, otherwise keep it
                    # and expect scheduler loop to deal with it.
                    self.queued_tasks.pop(key)
                    result.backend = cached_celery_backend
                    self.running.add(key)
                    self.tasks[key] = result
                    self.last_state[key] = celery_states.PENDING

    def _send_tasks_to_celery(self, task_tuples_to_send):
        if len(task_tuples_to_send) == 1 or self._sync_parallelism == 1:
            # One tuple, or max one process -> send it in the main thread.
            return list(map(send_task_to_executor, task_tuples_to_send))

        # Use chunks instead of a work queue to reduce context switching
        # since tasks are roughly uniform in size
        chunksize = self._num_tasks_per_send_process(len(task_tuples_to_send))
        num_processes = min(len(task_tuples_to_send), self._sync_parallelism)
        with Pool(processes=num_processes) as send_pool:
            key_and_async_results = send_pool.map(
                send_task_to_executor,
                task_tuples_to_send,
                chunksize=chunksize)
        return key_and_async_results

    def sync(self) -> None:
        if not self.tasks:
            self.log.debug("No task to query celery, skipping sync")
            return
        self.update_all_task_states()

    def update_all_task_states(self) -> None:
        """Updates states of the tasks."""

        self.log.debug("Inquiring about %s celery task(s)", len(self.tasks))
        state_and_info_by_celery_task_id = self.bulk_state_fetcher.get_many(self.tasks.values())

        self.log.debug("Inquiries completed.")
        for key, async_result in list(self.tasks.items()):
            state, info = state_and_info_by_celery_task_id.get(async_result.task_id)
            if state:
                self.update_task_state(key, state, info)

    def update_task_state(self, key: TaskInstanceKeyType, state: str, info: Any) -> None:
        """Updates state of a single task."""
        # noinspection PyBroadException
        try:
            if self.last_state[key] != state:
                if state == celery_states.SUCCESS:
                    self.success(key, info)
                    del self.tasks[key]
                    del self.last_state[key]
                elif state == celery_states.FAILURE:
                    self.fail(key, info)
                    del self.tasks[key]
                    del self.last_state[key]
                elif state == celery_states.REVOKED:
                    self.fail(key, info)
                    del self.tasks[key]
                    del self.last_state[key]
                else:
                    self.log.info("Unexpected state: %s", state)
                    self.last_state[key] = state
        except Exception:  # pylint: disable=broad-except
            self.log.exception("Error syncing the Celery executor, ignoring it.")

    def end(self, synchronous: bool = False) -> None:
        if synchronous:
            while any([task.state not in celery_states.READY_STATES for task in self.tasks.values()]):
                time.sleep(5)
        self.sync()

    def execute_async(self,
                      key: TaskInstanceKeyType,
                      command: CommandType,
                      queue: Optional[str] = None,
                      executor_config: Optional[Any] = None):
        """Do not allow async execution for Celery executor."""
        raise AirflowException("No Async execution for Celery executor.")

    def terminate(self):
        pass


def fetch_celery_task_state(async_result: AsyncResult) -> \
        Tuple[str, Union[str, ExceptionWithTraceback], Any]:
    """
    Fetch and return the state of the given celery task. The scope of this function is
    global so that it can be called by subprocesses in the pool.

    :param async_result: a tuple of the Celery task key and the async Celery object used
        to fetch the task's state
    :type async_result: tuple(str, celery.result.AsyncResult)
    :return: a tuple of the Celery task key and the Celery state and the celery info
        of the task
    :rtype: tuple[str, str, str]
    """

    try:
        with timeout(seconds=OPERATION_TIMEOUT):
            # Accessing state property of celery task will make actual network request
            # to get the current state of the task
            info = async_result.info if hasattr(async_result, 'info') else None
            return async_result.task_id, async_result.state, info
    except Exception as e:  # pylint: disable=broad-except
        exception_traceback = f"Celery Task ID: {async_result}\n{traceback.format_exc()}"
        return async_result.task_id, ExceptionWithTraceback(e, exception_traceback), None


def _tasks_list_to_task_ids(async_tasks) -> Set[str]:
    return {a.task_id for a in async_tasks}


class BulkStateFetcher(LoggingMixin):
    """
    Gets status for many Celery tasks using the best method available

    If BaseKeyValueStoreBackend is used as result backend, the mget method is used.
    If DatabaseBackend is used as result backend, the SELECT ...WHER task_id IN (...) query is used
    Otherwise, multiprocessing.Pool will be used. Each task status will be downloaded individually.
    """
    def __init__(self, sync_parralelism=None):
        super().__init__()
        self._sync_parallelism = sync_parralelism

    def get_many(self, async_results) -> Mapping[str, EventBufferValueType]:
        """
        Gets status for many Celery tasks using the best method available.
        """
        if isinstance(app.backend, BaseKeyValueStoreBackend):
            result = self._get_many_from_kv_backend(async_results)
            return result
        if isinstance(app.backend, DatabaseBackend):
            result = self._get_many_from_db_backend(async_results)
            return result
        result = self._get_many_using_multiprocessing(async_results)
        self.log.debug("Fetched %d states for %d task", len(result), len(async_results))
        return result

    def _get_many_from_kv_backend(self, async_tasks) -> Mapping[str, EventBufferValueType]:
        task_ids = _tasks_list_to_task_ids(async_tasks)
        keys = [app.backend.get_key_for_task(k) for k in task_ids]
        values = app.backend.mget(keys)
        task_results = [app.backend.decode_result(v) for v in values if v]
        task_results_by_task_id = {task_result["task_id"]: task_result for task_result in task_results}

        return self._prepare_state_and_info_by_task_dict(task_ids, task_results_by_task_id)

    def _get_many_from_db_backend(self, async_tasks) -> Mapping[str, EventBufferValueType]:
        task_ids = _tasks_list_to_task_ids(async_tasks)
        session = app.backend.ResultSession()
        with session_cleanup(session):
            tasks = session.query(TaskDb).filter(TaskDb.task_id.in_(task_ids)).all()

        task_results = [app.backend.meta_from_decoded(task.to_dict()) for task in tasks]
        task_results_by_task_id = {task_result["task_id"]: task_result for task_result in task_results}
        return self._prepare_state_and_info_by_task_dict(task_ids, task_results_by_task_id)

    @staticmethod
    def _prepare_state_and_info_by_task_dict(task_ids,
                                             task_results_by_task_id) -> Mapping[str, EventBufferValueType]:
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

        with Pool(processes=num_process) as sync_pool:
            chunksize = max(1, math.floor(math.ceil(1.0 * len(async_results) / self._sync_parallelism)))

            task_id_to_states_and_info = sync_pool.map(
                fetch_celery_task_state,
                async_results,
                chunksize=chunksize)

            states_and_info_by_task_id: MutableMapping[str, EventBufferValueType] = {}
            for task_id, state_or_exception, info in task_id_to_states_and_info:
                if isinstance(state_or_exception, ExceptionWithTraceback):
                    self.log.error(  # pylint: disable=logging-not-lazy
                        CELERY_FETCH_ERR_MSG_HEADER + ":%s\n%s\n",
                        state_or_exception.exception, state_or_exception.traceback
                    )
                else:
                    states_and_info_by_task_id[task_id] = state_or_exception, info
        return states_and_info_by_task_id
