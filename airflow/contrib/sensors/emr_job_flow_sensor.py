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
from airflow.contrib.hooks.emr_hook import EmrHook
from airflow.contrib.sensors.emr_base_sensor import EmrBaseSensor
from airflow.utils import apply_defaults


class EmrJobFlowSensor(EmrBaseSensor):
    """
    Asks for the state of the JobFlow until it reaches a terminal state.
    If it fails the sensor errors, failing the task.

    :param job_flow_id: job_flow_id to check the state of
    :type job_flow_id: string
    """

    NON_TERMINAL_STATES = ['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING', 'TERMINATING']
    FAILED_STATE = 'TERMINATED_WITH_ERRORS'
    template_fields = ['job_flow_id']
    template_ext = ()

    @apply_defaults
    def __init__(
            self,
            job_flow_id,
            *args, **kwargs):
        super(EmrJobFlowSensor, self).__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id

    def get_emr_response(self):
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        self.log.info('Poking cluster %s', self.job_flow_id)
        return emr.describe_cluster(ClusterId=self.job_flow_id)

    def state_from_response(self, response):
        return response['Cluster']['Status']['State']
