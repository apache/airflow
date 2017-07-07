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

import calendar
import logging
import time

from airflow.contrib.kubernetes.kubernetes_job_builder import KubernetesJobBuilder
from airflow.contrib.kubernetes.kubernetes_helper import KubernetesHelper
from queue import Queue

from airflow import settings
from airflow.contrib.kubernetes.kubernetes_request_factory import SimpleJobRequestFactory
from airflow.executors.base_executor import BaseExecutor
from airflow.models import TaskInstance
from airflow.utils.state import State


# TODO this is just for proof of concept. remove before merging.


def _prep_command_for_container(command):
    """  
    When creating a kubernetes pod, the yaml expects the command
    in the form of ["cmd","arg","arg","arg"...]
    This function splits the command string into tokens 
    and then matches it to the convention.

    :param command:
    
    :return:

    """
    return '"' + '","'.join(command.split(' ')[1:]) + '"'


class AirflowKubernetesScheduler(object):
    def __init__(self,
                 task_queue,
                 result_queue):
        logging.info("creating kubernetes executor")
        self.task_queue = task_queue
        self.result_queue = result_queue
        self.current_jobs = {}
        self._task_counter = 0
        self.helper = KubernetesHelper()

    def run_next(self, next_job):
        """

        The run_next command will check the task_queue for any un-run jobs.
        It will then create a unique job-id, launch that job in the cluster,
        and store relevent info in the current_jobs map so we can track the job's
        status

        :return: 

        """
        logging.info('job is {}'.format(str(next_job)))
        (key, command) = next_job
        logging.info("running for command {}".format(command))
        epoch_time = calendar.timegm(time.gmtime())
        command_list = ["/usr/local/airflow/entrypoint.sh"] + command.split()[1:] + \
                       ['-km']
        self._set_host_id(key)
        pod_id = self._create_job_id_from_key(key=key, epoch_time=epoch_time)
        self.current_jobs[pod_id] = key
        pod = KubernetesJobBuilder(
            image='airflow-slave:latest',
            cmds=command_list,
            kub_req_factory=SimpleJobRequestFactory)
        pod.add_name(pod_id)
        pod.launch()
        self._task_counter += 1
        logging.info("Job created!")

    def sync(self):
        """

        The sync function checks the status of all currently running kubernetes jobs.
        If a job is completed, it's status is placed in the result queue to 
        be sent back to the scheduler.

        :return:

        """
        current_jobs = iter(self.current_jobs.copy())
        for job_id in current_jobs:
            key = self.current_jobs[job_id]
            namespace = 'default'
            status = self.helper.get_status(job_id, namespace)
            self.process_status(job_id, key, status)

    def process_status(self, job_id, key, status):
        if status.failed:
            logging.info("{} Failed".format(key))
            self.result_queue.put((key, State.FAILED))
            self.helper.delete_job(job_id, namespace='default')
            self.current_jobs.pop(job_id)
        elif status.succeeded:
            logging.info("{} Succeeded".format(key))
            # self.result_queue.put((key, State.SUCCESS))
            self.current_jobs.pop(job_id)
        elif status.active:
            logging.info("{} is Running".format(job_id))

    def _create_job_id_from_key(self, key, epoch_time):
        """

        Kubernetes pod names must unique and match specific conventions 
        (i.e. no spaces, period, etc.)
        This function creates a unique name using the epoch time and internal counter

        :param key: 

        :param epoch_time: 

        :return:

        """

        keystr = '-'.join([str(x).replace(' ', '-') for x in key[:2]])
        job_fields = [keystr, str(self._task_counter), str(epoch_time)]
        unformatted_job_id = '-'.join(job_fields)
        job_id = unformatted_job_id.replace('_', '-')
        return job_id

    def _set_host_id(self, key):
        (dag_id, task_id, ex_time) = key
        session = settings.Session()
        item = session.query(TaskInstance) \
            .filter_by(dag_id=dag_id, task_id=task_id, execution_date=ex_time).one()

        host_id = item.hostname
        print("host is {}".format(host_id))


class KubernetesExecutor(BaseExecutor):
    def start(self):
        logging.info('starting kubernetes executor')
        self.task_queue = Queue()
        self._session = settings.Session()
        self.result_queue = Queue()
        self.kub_client = AirflowKubernetesScheduler(self.task_queue,
                                                     self.result_queue)

    def sync(self):
        self.kub_client.sync()
        while not self.result_queue.empty():
            results = self.result_queue.get()
            logging.info("reporting {}".format(results))
            self.change_state(*results)

        # TODO this could be a job_counter based on max jobs a user wants
        if len(self.kub_client.current_jobs) > 3:
            logging.info("currently a job is running")
        else:
            logging.info("queue empty, running next")
            if not self.task_queue.empty():
                (key, command) = self.task_queue.get()
                self.kub_client.run_next((key, command))

    def terminate(self):
        pass

    def change_state(self, key, state):
        if state != State.RUNNING:
            self.running.pop(key)
        self.event_buffer[key] = state
        (dag_id, task_id, ex_time) = key
        item = self._session.query(TaskInstance).filter_by(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=ex_time).one()

        item.state = state
        self._session.add(item)
        self._session.commit()

    def end(self):
        logging.info('ending kube executor')
        self.task_queue.join()

    def execute_async(self, key, command, queue=None):
        logging.info("adding task {} with command {}".format(key, command))
        self.task_queue.put((key, command))
