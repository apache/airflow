# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from builtins import object
import logging
import subprocess
import time

from celery import Celery
from celery import states as celery_states
from collections import defaultdict

from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow import configuration

PARALLELISM = configuration.get('core', 'PARALLELISM')

'''
To start the celery worker, run the command:
airflow worker
'''

DEFAULT_QUEUE = configuration.get('celery', 'DEFAULT_QUEUE')


class CeleryConfig(object):
    CELERY_ACCEPT_CONTENT = ['json', 'pickle']
    CELERYD_PREFETCH_MULTIPLIER = 1
    CELERY_ACKS_LATE = True
    BROKER_URL = configuration.get('celery', 'BROKER_URL')
    CELERY_RESULT_BACKEND = configuration.get('celery', 'CELERY_RESULT_BACKEND')
    CELERYD_CONCURRENCY = configuration.getint('celery', 'CELERYD_CONCURRENCY')
    CELERY_DEFAULT_QUEUE = DEFAULT_QUEUE
    CELERY_DEFAULT_EXCHANGE = DEFAULT_QUEUE

app = Celery(
    configuration.get('celery', 'CELERY_APP_NAME'),
    config_source=CeleryConfig)


@app.task
def execute_command(command):
    logging.info("Executing command in Celery " + command)
    try:
        subprocess.check_call(command, shell=True)
    except subprocess.CalledProcessError as e:
        logging.error(e)
        raise AirflowException('Celery command failed')


LAST_STATE = 'last_state'
QUEUE_NAME = 'queue_name'


class CeleryExecutor(BaseExecutor):
    """
    CeleryExecutor is recommended for production use of Airflow. It allows
    distributing the execution of task instances to multiple worker nodes.

    Celery is a simple, flexible and reliable distributed system to process
    vast amounts of messages, while providing operations with the tools
    required to maintain such a system.
    """

    def start(self):
        self.tasks = {}
        self.task_info = defaultdict(dict)

    def execute_async(self, key, command, queue=DEFAULT_QUEUE):
        self.logger.info( "[celery] queuing {key} through celery, "
                       "queue={queue}".format(**locals()))
        self.tasks[key] = execute_command.apply_async(
            args=[command], queue=queue)
        self.task_info[key][LAST_STATE] = celery_states.PENDING
        self.task_info[key][QUEUE_NAME] = queue

    def sync(self):

        self.logger.debug(
            "Inquiring about {} celery task(s)".format(len(self.tasks)))
        for key, async in list(self.tasks.items()):
            state = async.state
            if self.task_info[key][LAST_STATE] != state:
                if state == celery_states.SUCCESS:
                    self.success(key)
                    del self.tasks[key]
                    del self.task_info[key]
                elif state == celery_states.FAILURE:
                    self.fail(key)
                    del self.tasks[key]
                    del self.task_info[key]
                elif state == celery_states.REVOKED:
                    self.fail(key)
                    del self.tasks[key]
                    del self.task_info[key]
                else:
                    self.logger.info("Unexpected state: " + async.state)
                self.task_info[key][LAST_STATE] = async.state

    def end(self, synchronous=False):
        if synchronous:
            while any([
                    async.state not in celery_states.READY_STATES
                    for async in self.tasks.values()]):
                time.sleep(5)
        self.sync()

    def report_metrics(self):
        # count elements in each queue
        super(CeleryExecutor, self).report_metrics()
        counts = defaultdict(lambda: 0)
        for d in self.task_info.values():
            if d[LAST_STATE] == celery_states.PENDING: # only count enqueued tasks
                counts[d[QUEUE_NAME]] += 1
        for queue_name, queue_length in counts.items():
            self.stats.gauge("queue_length_" + queue_name, queue_length)
