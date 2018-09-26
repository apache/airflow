# -*- coding: utf-8 -*-
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

import math
import os
import subprocess
import time
import traceback
from multiprocessing import Pool, cpu_count

from celery import Celery
from celery import states as celery_states

from airflow import configuration
from airflow.config_templates.default_celery import DEFAULT_CELERY_CONFIG
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string

# Make it constant for unit test.
CELERY_FETCH_ERR_MSG_HEADER = 'Error fetching Celery task state'

'''
To start the celery worker, run the command:
airflow worker
'''

if configuration.conf.has_option('celery', 'celery_config_options'):
    celery_configuration = import_string(
        configuration.conf.get('celery', 'celery_config_options')
    )
else:
    celery_configuration = DEFAULT_CELERY_CONFIG

app = Celery(
    configuration.conf.get('celery', 'CELERY_APP_NAME'),
    config_source=celery_configuration)


@app.task
def execute_command(command):
    log = LoggingMixin().log
    log.info("Executing command in Celery: %s", command)
    env = os.environ.copy()
    try:
        subprocess.check_call(command, stderr=subprocess.STDOUT,
                              close_fds=True, env=env)
    except subprocess.CalledProcessError as e:
        log.exception('execute_command encountered a CalledProcessError')
        log.error(e.output)

        raise AirflowException('Celery command failed')


class ExceptionWithTraceback(object):
    """
    Wrapper class used to propogate exceptions to parent processes from subprocesses.
    :param exception: The exception to wrap
    :type exception: Exception
    :param traceback: The stacktrace to wrap
    :type traceback: str
    """

    def __init__(self, exception, exception_traceback):
        self.exception = exception
        self.traceback = exception_traceback


def fetch_celery_task_state(celery_task):
    """
    Fetch and return the state of the given celery task. The scope of this function is
    global so that it can be called by subprocesses in the pool.
    :param celery_task: a tuple of the Celery task key and the async Celery object used
                        to fetch the task's state
    :type celery_task: (str, celery.result.AsyncResult)
    :return: a tuple of the Celery task key and the Celery state of the task
    :rtype: (str, str)
    """

    try:
        # Accessing state property of celery task will make actual network request
        # to get the current state of the task.
        res = (celery_task[0], celery_task[1].state)
    except Exception as e:
        exception_traceback = "Celery Task ID: {}\n{}".format(celery_task[0],
                                                              traceback.format_exc())
        res = ExceptionWithTraceback(e, exception_traceback)
    return res


class CeleryExecutor(BaseExecutor):
    """
    CeleryExecutor is recommended for production use of Airflow. It allows
    distributing the execution of task instances to multiple worker nodes.

    Celery is a simple, flexible and reliable distributed system to process
    vast amounts of messages, while providing operations with the tools
    required to maintain such a system.
    """

    def __init__(self):
        super(CeleryExecutor, self).__init__()

        # Celery doesn't support querying the state of multiple tasks in parallel
        # (which can become a bottleneck on bigger clusters) so we use
        # a multiprocessing pool to speed this up.
        # How many worker processes are created for checking celery task state.
        self._sync_parallelism = configuration.getint('celery', 'SYNC_PARALLELISM')
        if self._sync_parallelism == 0:
            self._sync_parallelism = max(1, cpu_count() - 1)

        self._sync_pool = None
        self.tasks = {}
        self.last_state = {}

    def start(self):
        self.log.debug(
            'Starting Celery Executor using {} processes for syncing'.format(
                self._sync_parallelism))

    def execute_async(self, key, command,
                      queue=DEFAULT_CELERY_CONFIG['task_default_queue'],
                      executor_config=None):
        self.log.info("[celery] queuing {key} through celery, "
                      "queue={queue}".format(**locals()))
        self.tasks[key] = execute_command.apply_async(
            args=[command], queue=queue)
        self.last_state[key] = celery_states.PENDING

    def _num_tasks_per_process(self):
        """
        How many Celery tasks should be sent to each worker process.
        :return: Number of tasks that should be used per process
        :rtype: int
        """
        return max(1,
                   int(math.ceil(1.0 * len(self.tasks) / self._sync_parallelism)))

    def sync(self):
        num_processes = min(len(self.tasks), self._sync_parallelism)
        if num_processes == 0:
            self.log.debug("No task to query celery, skipping sync")
            return

        self.log.debug("Inquiring about %s celery task(s) using %s processes",
                       len(self.tasks), num_processes)

        # Recreate the process pool each sync in case processes in the pool die
        self._sync_pool = Pool(processes=num_processes)

        # Use chunking instead of a work queue to reduce context switching since tasks are
        # roughly uniform in size
        chunksize = self._num_tasks_per_process()

        self.log.debug("Waiting for inquiries to complete...")
        task_keys_to_states = self._sync_pool.map(
            fetch_celery_task_state,
            self.tasks.items(),
            chunksize=chunksize)
        self._sync_pool.close()
        self._sync_pool.join()
        self.log.debug("Inquiries completed.")

        for key_and_state in task_keys_to_states:
            if isinstance(key_and_state, ExceptionWithTraceback):
                self.log.error(
                    CELERY_FETCH_ERR_MSG_HEADER + ", ignoring it:{}\n{}\n".format(
                        key_and_state.exception, key_and_state.traceback))
                continue
            key, state = key_and_state
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
                        self.log.info("Unexpected state: " + state)
                        self.last_state[key] = state
            except Exception:
                self.log.exception("Error syncing the Celery executor, ignoring it.")

    def end(self, synchronous=False):
        if synchronous:
            while any([
                    task.state not in celery_states.READY_STATES
                    for task in self.tasks.values()]):
                time.sleep(5)
        self.sync()
