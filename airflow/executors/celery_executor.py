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
"""Celery executor."""
import math
import os
import subprocess
import time
import traceback
from multiprocessing import Pool, cpu_count
from typing import Any, List, Optional, Tuple, Union

from celery import Celery, Task, states as celery_states
from celery.result import AsyncResult

from airflow.config_templates.default_celery import DEFAULT_CELERY_CONFIG
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor, CommandType, QueuedTaskInstanceType
from airflow.models.taskinstance import SimpleTaskInstance, TaskInstanceKeyType, TaskInstanceStateType
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string
from airflow.utils.timeout import timeout

# Make it constant for unit test.
CELERY_FETCH_ERR_MSG_HEADER = 'Error fetching Celery task state'

CELERY_SEND_ERR_MSG_HEADER = 'Error sending Celery task'

'''
To start the celery worker, run the command:
airflow worker
'''

if conf.has_option('celery', 'celery_config_options'):
    celery_configuration = import_string(
        conf.get('celery', 'celery_config_options')
    )
else:
    celery_configuration = DEFAULT_CELERY_CONFIG

app = Celery(
    conf.get('celery', 'CELERY_APP_NAME'),
    config_source=celery_configuration)


@app.task
def execute_command(command_to_exec: str) -> None:
    """Executes command."""
    log = LoggingMixin().log
    log.info("Executing command in Celery: %s", command_to_exec)
    env = os.environ.copy()
    try:
        subprocess.check_call(command_to_exec, stderr=subprocess.STDOUT,
                              close_fds=True, env=env)
    except subprocess.CalledProcessError as e:
        log.exception('execute_command encountered a CalledProcessError')
        log.error(e.output)
        raise AirflowException('Celery command failed')


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


def fetch_celery_task_state(celery_task: Tuple[TaskInstanceKeyType, AsyncResult]) \
        -> Union[TaskInstanceStateType, ExceptionWithTraceback]:
    """
    Fetch and return the state of the given celery task. The scope of this function is
    global so that it can be called by subprocesses in the pool.

    :param celery_task: a tuple of the Celery task key and the async Celery object used
        to fetch the task's state
    :type celery_task: tuple(str, celery.result.AsyncResult)
    :return: a tuple of the Celery task key and the Celery state of the task
    :rtype: tuple[str, str]
    """

    try:
        with timeout(seconds=2):
            # Accessing state property of celery task will make actual network request
            # to get the current state of the task.
            return celery_task[0], celery_task[1].state
    except Exception as e:  # pylint: disable=broad-except
        exception_traceback = "Celery Task ID: {}\n{}".format(celery_task[0],
                                                              traceback.format_exc())
        return ExceptionWithTraceback(e, exception_traceback)


# Task instance that is sent over Celery queues
# TaskInstanceKeyType, SimpleTaskInstance, Command, queue_name, CallableTask
TaskInstanceInCelery = Tuple[TaskInstanceKeyType, SimpleTaskInstance, CommandType, Optional[str], Task]


def send_task_to_executor(task_tuple: TaskInstanceInCelery) \
        -> Tuple[TaskInstanceKeyType, CommandType, Union[AsyncResult, ExceptionWithTraceback]]:
    """Sends task to executor."""
    key, _, command, queue, task_to_run = task_tuple
    try:
        with timeout(seconds=2):
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

        # Celery doesn't support querying the state of multiple tasks in parallel
        # (which can become a bottleneck on bigger clusters) so we use
        # a multiprocessing pool to speed this up.
        # How many worker processes are created for checking celery task state.
        self._sync_parallelism = conf.getint('celery', 'SYNC_PARALLELISM')
        if self._sync_parallelism == 0:
            self._sync_parallelism = max(1, cpu_count() - 1)

        self._sync_pool = None
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

    def _num_tasks_per_fetch_process(self) -> int:
        """
        How many Celery tasks should be sent to each worker process.

        :return: Number of tasks that should be used per process
        :rtype: int
        """
        return max(1, int(math.ceil(1.0 * len(self.tasks) / self._sync_parallelism)))

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

        cached_celery_backend = None
        if task_tuples_to_send:
            tasks = [t[4] for t in task_tuples_to_send]

            # Celery state queries will stuck if we do not use one same backend
            # for all tasks.
            cached_celery_backend = tasks[0].backend

        if task_tuples_to_send:
            # Use chunks instead of a work queue to reduce context switching
            # since tasks are roughly uniform in size
            chunksize = self._num_tasks_per_send_process(len(task_tuples_to_send))
            num_processes = min(len(task_tuples_to_send), self._sync_parallelism)

            send_pool = Pool(processes=num_processes)
            key_and_async_results = send_pool.map(
                send_task_to_executor,
                task_tuples_to_send,
                chunksize=chunksize)

            send_pool.close()
            send_pool.join()
            self.log.debug('Sent all tasks.')

            for key, command, result in key_and_async_results:
                if isinstance(result, ExceptionWithTraceback):
                    self.log.error(
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

    def order_queued_tasks_by_priority(self) -> List[Tuple[TaskInstanceKeyType, QueuedTaskInstanceType]]:
        """
        Orders the queued tasks by priority.

        :return: List of tuples from the queued_tasks according to the priority.
        """
        return sorted(
            [(k, v) for k, v in self.queued_tasks.items()],  # pylint: disable=unnecessary-comprehension
            key=lambda x: x[1][1],
            reverse=True)

    def sync(self) -> None:
        num_processes = min(len(self.tasks), self._sync_parallelism)
        if num_processes == 0:
            self.log.debug("No task to query celery, skipping sync")
            return

        self.log.debug("Inquiring about %s celery task(s) using %s processes",
                       len(self.tasks), num_processes)

        # Recreate the process pool each sync in case processes in the pool die
        self._sync_pool = Pool(processes=num_processes)

        # Use chunks instead of a work queue to reduce context switching since tasks are
        # roughly uniform in size
        chunksize = self._num_tasks_per_fetch_process()

        self.log.debug("Waiting for inquiries to complete...")
        task_keys_to_states = self._sync_pool.map(
            fetch_celery_task_state,
            self.tasks.items(),
            chunksize=chunksize)
        self._sync_pool.close()
        self._sync_pool.join()
        self.log.debug("Inquiries completed.")

        self.update_task_states(task_keys_to_states)

    def update_task_states(self,
                           task_keys_to_states: List[Union[TaskInstanceStateType,
                                                           ExceptionWithTraceback]]) -> None:
        """Updates states of the tasks."""
        for key_and_state in task_keys_to_states:
            if isinstance(key_and_state, ExceptionWithTraceback):
                self.log.error(
                    CELERY_FETCH_ERR_MSG_HEADER + ", ignoring it:%s\n%s\n",
                    repr(key_and_state.exception), key_and_state.traceback
                )
                continue
            key, state = key_and_state
            self.update_task_state(key, state)

    def update_task_state(self, key: TaskInstanceKeyType, state: str) -> None:
        """Updates state of a single task."""
        # noinspection PyBroadException
        try:
            if self.last_state[key] != state:
                if state == celery_states.SUCCESS:
                    self.success(key)
                    del self.tasks[key]
                    del self.last_state[key]
                elif state == celery_states.FAILURE:
                    self.fail(key)
                    del self.tasks[key]
                    del self.last_state[key]
                elif state == celery_states.REVOKED:
                    self.fail(key)
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
