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

import time

from airflow.exceptions import AirflowException
from airflow.providers.aws.hooks.sagemaker import LogState, SageMakerHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class SageMakerBaseSensor(BaseSensorOperator):
    """
    Contains general sensor behavior for SageMaker.
    Subclasses should implement get_sagemaker_response()
    and state_from_response() methods.
    Subclasses should also implement NON_TERMINAL_STATES and FAILED_STATE methods.
    """
    ui_color = '#ededed'

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='aws_default',
            *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id

    def poke(self, context):
        response = self.get_sagemaker_response()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        state = self.state_from_response(response)

        self.log.info('Job currently %s', state)

        if state in self.non_terminal_states():
            return False

        if state in self.failed_states():
            failed_reason = self.get_failed_reason_from_response(response)
            raise AirflowException('Sagemaker job failed for the following reason: %s'
                                   % failed_reason)
        return True

    def non_terminal_states(self):
        raise NotImplementedError('Please implement non_terminal_states() in subclass')

    def failed_states(self):
        raise NotImplementedError('Please implement failed_states() in subclass')

    def get_sagemaker_response(self):
        raise NotImplementedError('Please implement get_sagemaker_response() in subclass')

    def get_failed_reason_from_response(self, response):
        return 'Unknown'

    def state_from_response(self, response):
        raise NotImplementedError('Please implement state_from_response() in subclass')


class SageMakerEndpointSensor(SageMakerBaseSensor):
    """
    Asks for the state of the endpoint state until it reaches a terminal state.
    If it fails the sensor errors, the task fails.

    :param job_name: job_name of the endpoint instance to check the state of
    :type job_name: str
    """

    template_fields = ['endpoint_name']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 endpoint_name,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint_name = endpoint_name

    def non_terminal_states(self):
        return SageMakerHook.endpoint_non_terminal_states

    def failed_states(self):
        return SageMakerHook.failed_states

    def get_sagemaker_response(self):
        sagemaker = SageMakerHook(aws_conn_id=self.aws_conn_id)

        self.log.info('Poking Sagemaker Endpoint %s', self.endpoint_name)
        return sagemaker.describe_endpoint(self.endpoint_name)

    def get_failed_reason_from_response(self, response):
        return response['FailureReason']

    def state_from_response(self, response):
        return response['EndpointStatus']


class SageMakerTrainingSensor(SageMakerBaseSensor):
    """
    Asks for the state of the training state until it reaches a terminal state.
    If it fails the sensor errors, failing the task.

    :param job_name: name of the SageMaker training job to check the state of
    :type job_name: str
    :param print_log: if the operator should print the cloudwatch log
    :type print_log: bool
    """

    template_fields = ['job_name']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 job_name,
                 print_log=True,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.job_name = job_name
        self.print_log = print_log
        self.positions = {}
        self.stream_names = []
        self.instance_count = None
        self.state = None
        self.last_description = None
        self.last_describe_job_call = None
        self.log_resource_inited = False

    def init_log_resource(self, hook):
        description = hook.describe_training_job(self.job_name)
        self.instance_count = description['ResourceConfig']['InstanceCount']

        status = description['TrainingJobStatus']
        job_already_completed = status not in self.non_terminal_states()
        self.state = LogState.TAILING if not job_already_completed else LogState.COMPLETE
        self.last_description = description
        self.last_describe_job_call = time.time()
        self.log_resource_inited = True

    def non_terminal_states(self):
        return SageMakerHook.non_terminal_states

    def failed_states(self):
        return SageMakerHook.failed_states

    def get_sagemaker_response(self):
        sagemaker_hook = SageMakerHook(aws_conn_id=self.aws_conn_id)
        if self.print_log:
            if not self.log_resource_inited:
                self.init_log_resource(sagemaker_hook)
            self.state, self.last_description, self.last_describe_job_call = \
                sagemaker_hook.describe_training_job_with_log(self.job_name,
                                                              self.positions, self.stream_names,
                                                              self.instance_count, self.state,
                                                              self.last_description,
                                                              self.last_describe_job_call)
        else:
            self.last_description = sagemaker_hook.describe_training_job(self.job_name)

        status = self.state_from_response(self.last_description)
        if status not in self.non_terminal_states() and status not in self.failed_states():
            billable_time = \
                (self.last_description['TrainingEndTime'] - self.last_description['TrainingStartTime']) * \
                self.last_description['ResourceConfig']['InstanceCount']
            self.log.info('Billable seconds: %s', int(billable_time.total_seconds()) + 1)

        return self.last_description

    def get_failed_reason_from_response(self, response):
        return response['FailureReason']

    def state_from_response(self, response):
        return response['TrainingJobStatus']


class SageMakerTransformSensor(SageMakerBaseSensor):
    """
    Asks for the state of the transform state until it reaches a terminal state.
    The sensor will error if the job errors, throwing a AirflowException
    containing the failure reason.

    :param job_name: job_name of the transform job instance to check the state of
    :type job_name: str
    """

    template_fields = ['job_name']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 job_name,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.job_name = job_name

    def non_terminal_states(self):
        return SageMakerHook.non_terminal_states

    def failed_states(self):
        return SageMakerHook.failed_states

    def get_sagemaker_response(self):
        sagemaker = SageMakerHook(aws_conn_id=self.aws_conn_id)

        self.log.info('Poking Sagemaker Transform Job %s', self.job_name)
        return sagemaker.describe_transform_job(self.job_name)

    def get_failed_reason_from_response(self, response):
        return response['FailureReason']

    def state_from_response(self, response):
        return response['TransformJobStatus']


class SageMakerTuningSensor(SageMakerBaseSensor):
    """
    Asks for the state of the tuning state until it reaches a terminal state.
    The sensor will error if the job errors, throwing a AirflowException
    containing the failure reason.

    :param job_name: job_name of the tuning instance to check the state of
    :type job_name: str
    """

    template_fields = ['job_name']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 job_name,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.job_name = job_name

    def non_terminal_states(self):
        return SageMakerHook.non_terminal_states

    def failed_states(self):
        return SageMakerHook.failed_states

    def get_sagemaker_response(self):
        sagemaker = SageMakerHook(aws_conn_id=self.aws_conn_id)

        self.log.info('Poking Sagemaker Tuning Job %s', self.job_name)
        return sagemaker.describe_tuning_job(self.job_name)

    def get_failed_reason_from_response(self, response):
        return response['FailureReason']

    def state_from_response(self, response):
        return response['HyperParameterTuningJobStatus']
