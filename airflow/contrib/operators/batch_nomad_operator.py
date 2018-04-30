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

"""
This module contains a Nomad Operator
which allows you to register and dispatch your nomad job,
"""

import time

from airflow.contrib.hooks.nomad_hook import NomadHook
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils import apply_defaults


class NomadOperator(BaseOperator):
    completed_states = ['complete']
    still_running_states = ['running', 'starting', 'pending', 'queued']

    @apply_defaults
    def __init__(self,
                 job,
                 nomad_conn_id='nomad_default',
                 sleep_amount=1,
                 meta=None,
                 payload=None,
                 *args,
                 **kwargs):
        """
        Create a new connection to Nomad
        :param nomad_conn_id: the connection to nomad server by default 'nomad_default'
        :type nomad_conn_id: string
        :param job: the json representation of nomad job
        :type job: string
        """
        super(NomadOperator, self).__init__(*args, **kwargs)

        self.nomad_client = NomadHook(nomad_conn_id).get_nomad_client()
        self.sleep_amount = sleep_amount
        self.meta = meta
        self.payload = payload
        self.job = job
        self.job_name = job.get("Job").get("Name")

    def _get_allocation_status(self, job_id):
        allocation = self.nomad_client.job.get_allocations(job_id)
        if not allocation:
            return 'pending'

        return allocation[0]['ClientStatus']

    def _dispatch_parameterized_job(self):
        self.log.info("dispatch job {}".format(self.job_name))
        res = self.nomad_client.job.dispatch_job(self.job_name,
                                                 meta=self.meta,
                                                 payload=self.payload)
        self.log.info("dispatch job {}".format(res["DispatchedJobID"]))
        return res["DispatchedJobID"]

    def _register_job(self):
        self.log.info("fetching registered jobs")
        registered_jobs = [job__["Name"]
                           for job__ in self.nomad_client.jobs.get_jobs()
                           if job__['Status'] == 'running']

        if self.job_name not in registered_jobs:
            self.log.info("register job name {}".format(self.job_name))
            self.nomad_client.job.register_job(self.job_name,
                                               self.job)
        else:
            self.log.info("job name {} already registered".format(self.job_name))

    def execute(self, context):
        self._register_job()
        job_id = self._dispatch_parameterized_job()
        while True:
            nomad_current_state = self._get_allocation_status(job_id)
            self.log.info("Current job {} state {}".format(self.job_name,
                                                           nomad_current_state))
            if nomad_current_state in NomadOperator.still_running_states:
                time.sleep(self.sleep_amount)
            elif nomad_current_state in NomadOperator.completed_states:
                self.log.info("Finished running job ".format(self.job_name,
                                                             nomad_current_state))
                break
            else:
                raise AirflowException("Task Failed To complete")
