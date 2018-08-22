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

import subprocess
import time
import os

from celery import Celery
from celery import states as celery_states

from airflow.config_templates.default_celery import DEFAULT_CELERY_CONFIG
from airflow.exceptions import AirflowException
from airflow.executors.base_executor import BaseExecutor
from airflow import configuration
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.module_loading import import_string

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
        self.last_state = {}

    def execute_async(self, key, command,
                      queue=DEFAULT_CELERY_CONFIG['task_default_queue'],
                      executor_config=None):
        self.log.info("[celery] queuing {key} through celery, "
                      "queue={queue}".format(**locals()))
        self.tasks[key] = execute_command.apply_async(
            args=command, queue=queue)
        self.last_state[key] = celery_states.PENDING

    def sync(self):
        self.log.debug("Inquiring about %s celery task(s)", len(self.tasks))
        for key, task in list(self.tasks.items()):
            try:
                state = task.state
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
            except Exception as e:
                self.log.error("Error syncing the celery executor, ignoring it:")
                self.log.exception(e)

    def end(self, synchronous=False):
        if synchronous:
            while any([
                    task.state not in celery_states.READY_STATES
                    for task in self.tasks.values()]):
                time.sleep(5)
        self.sync()
