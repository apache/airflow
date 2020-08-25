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

from airflow.providers.amazon.aws.hooks.sagemaker import SageMakerHook
from airflow.providers.amazon.aws.sensors.sagemaker_base import SageMakerBaseSensor
from airflow.utils.decorators import apply_defaults


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
    def __init__(self, *, endpoint_name, **kwargs):
        super().__init__(**kwargs)
        self.endpoint_name = endpoint_name

    def non_terminal_states(self):
        return SageMakerHook.endpoint_non_terminal_states

    def failed_states(self):
        return SageMakerHook.failed_states

    def get_sagemaker_response(self):
        self.log.info('Poking Sagemaker Endpoint %s', self.endpoint_name)
        return self.get_hook().describe_endpoint(self.endpoint_name)

    def get_failed_reason_from_response(self, response):
        return response['FailureReason']

    def state_from_response(self, response):
        return response['EndpointStatus']
