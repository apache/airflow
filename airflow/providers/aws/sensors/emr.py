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

from airflow.exceptions import AirflowException
from airflow.providers.aws.hooks.emr import EmrHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class EmrBaseSensor(BaseSensorOperator):
    """
    Contains general sensor behavior for EMR.
    Subclasses should implement get_emr_response() and state_from_response() methods.
    Subclasses should also implement NON_TERMINAL_STATES and FAILED_STATE constants.
    """
    ui_color = '#66c3ff'

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='aws_default',
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id

    def poke(self, context):
        response = self.get_emr_response()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        state = self.state_from_response(response)
        self.log.info('Job flow currently %s', state)

        if state in self.NON_TERMINAL_STATES:
            return False

        if state in self.FAILED_STATE:
            final_message = 'EMR job failed'
            failure_message = self.failure_message_from_response(response)
            if failure_message:
                final_message += ' ' + failure_message
            raise AirflowException(final_message)

        return True


class EmrJobFlowSensor(EmrBaseSensor):
    """
    Asks for the state of the JobFlow until it reaches a terminal state.
    If it fails the sensor errors, failing the task.

    :param job_flow_id: job_flow_id to check the state of
    :type job_flow_id: str
    """

    NON_TERMINAL_STATES = ['STARTING', 'BOOTSTRAPPING', 'RUNNING',
                           'WAITING', 'TERMINATING']
    FAILED_STATE = ['TERMINATED_WITH_ERRORS']
    template_fields = ['job_flow_id']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 job_flow_id,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id

    def get_emr_response(self):
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        self.log.info('Poking cluster %s', self.job_flow_id)
        return emr.describe_cluster(ClusterId=self.job_flow_id)

    @staticmethod
    def state_from_response(response):
        return response['Cluster']['Status']['State']

    @staticmethod
    def failure_message_from_response(response):
        state_change_reason = response['Cluster']['Status'].get('StateChangeReason')
        if state_change_reason:
            return 'for code: {} with message {}'.format(state_change_reason.get('Code', 'No code'),
                                                         state_change_reason.get('Message', 'Unknown'))
        return None


class EmrStepSensor(EmrBaseSensor):
    """
    Asks for the state of the step until it reaches a terminal state.
    If it fails the sensor errors, failing the task.

    :param job_flow_id: job_flow_id which contains the step check the state of
    :type job_flow_id: str
    :param step_id: step to check the state of
    :type step_id: str
    """

    NON_TERMINAL_STATES = ['PENDING', 'RUNNING', 'CONTINUE', 'CANCEL_PENDING']
    FAILED_STATE = ['CANCELLED', 'FAILED', 'INTERRUPTED']
    template_fields = ['job_flow_id', 'step_id']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 job_flow_id,
                 step_id,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id
        self.step_id = step_id

    def get_emr_response(self):
        emr = EmrHook(aws_conn_id=self.aws_conn_id).get_conn()

        self.log.info('Poking step %s on cluster %s', self.step_id, self.job_flow_id)
        return emr.describe_step(ClusterId=self.job_flow_id, StepId=self.step_id)

    @staticmethod
    def state_from_response(response):
        return response['Step']['Status']['State']

    @staticmethod
    def failure_message_from_response(response):
        fail_details = response['Step']['Status'].get('FailureDetails')
        if fail_details:
            return 'for reason {} with message {} and log file {}'.format(fail_details.get('Reason'),
                                                                          fail_details.get('Message'),
                                                                          fail_details.get('LogFile'))
        return None
