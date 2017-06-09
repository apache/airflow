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
import ssl
import time
import traceback

from celery import Celery
from celery import states as celery_states

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
    CELERY_EVENT_SERIALIZER = 'json'
    CELERY_RESULT_SERIALIZER = 'pickle'
    CELERY_TASK_SERIALIZER = 'pickle'
    CELERYD_PREFETCH_MULTIPLIER = 1
    CELERY_ACKS_LATE = True
    BROKER_URL = configuration.get('celery', 'BROKER_URL')
    CELERY_RESULT_BACKEND = configuration.get('celery', 'CELERY_RESULT_BACKEND')
    CELERYD_CONCURRENCY = configuration.getint('celery', 'CELERYD_CONCURRENCY')
    CELERY_DEFAULT_QUEUE = DEFAULT_QUEUE
    CELERY_DEFAULT_EXCHANGE = DEFAULT_QUEUE
    if configuration.getboolean('celery', 'CELERY_SSL_ACTIVE'):
        try:
            BROKER_USE_SSL = {'keyfile': configuration.get('celery', 'CELERY_SSL_KEY'),
                              'certfile': configuration.get('celery', 'CELERY_SSL_CERT'),
                              'ca_certs': configuration.get('celery', 'CELERY_SSL_CACERT'),
                              'cert_reqs': ssl.CERT_REQUIRED}
        except ValueError:
            raise AirflowException('ValueError: CELERY_SSL_ACTIVE is True, please ensure CELERY_SSL_KEY, '
                                   'CELERY_SSL_CERT and CELERY_SSL_CACERT are set')
        except Exception as e:
            raise AirflowException('Exception: There was an unknown Celery SSL Error.  Please ensure you want to use '
                                   'SSL and/or have all necessary certs and key.')

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

    def execute_async(self, key, command, queue=DEFAULT_QUEUE):
        self.logger.info( "[celery] queuing {key} through celery, "
                       "queue={queue}".format(**locals()))
        self.tasks[key] = execute_command.apply_async(
            args=[command], queue=queue)
        self.last_state[key] = celery_states.PENDING

    def sync(self):

        self.logger.debug(
            "Inquiring about {} celery task(s)".format(len(self.tasks)))
        for key, async in list(self.tasks.items()):
            try:
                state = async.state
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
                        self.logger.info("Unexpected state: " + async.state)
                    self.last_state[key] = async.state
            except Exception as e:
                logging.error("Error syncing the celery executor, ignoring "
                              "it:\n{}\n".format(e, traceback.format_exc()))

    def end(self, synchronous=False):
        if synchronous:
            while any([
                    async.state not in celery_states.READY_STATES
                    for async in self.tasks.values()]):
                time.sleep(5)
        self.sync()
