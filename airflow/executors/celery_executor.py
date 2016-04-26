from builtins import object
import logging
import subprocess
import time

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

    def end(self, synchronous=False):
        if synchronous:
            while any([
                    async.state not in celery_states.READY_STATES
                    for async in self.tasks.values()]):
                time.sleep(5)
