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
from typing import Optional

import time

from airflow.providers.amazon.aws.hooks.sagemaker import LogState, SageMakerHook
from airflow.providers.amazon.aws.sensors.sagemaker_base import SageMakerBaseSensor
from airflow.utils.decorators import apply_defaults


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
    def __init__(self, *, job_name, print_log=True, **kwargs):
        super().__init__(**kwargs)
        self.job_name = job_name
        self.print_log = print_log
        self.positions = {}
        self.stream_names = []
        self.instance_count: Optional[int] = None
        self.state: Optional[int] = None
        self.last_description = None
        self.last_describe_job_call = None
        self.log_resource_inited = False

    def init_log_resource(self, hook: SageMakerHook) -> None:
        """Set tailing LogState for associated training job."""
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
        if self.print_log:
            if not self.log_resource_inited:
                self.init_log_resource(self.get_hook())
            (
                self.state,
                self.last_description,
                self.last_describe_job_call,
            ) = self.get_hook().describe_training_job_with_log(
                self.job_name,
                self.positions,
                self.stream_names,
                self.instance_count,
                self.state,
                self.last_description,
                self.last_describe_job_call,
            )
        else:
            self.last_description = self.get_hook().describe_training_job(self.job_name)

        status = self.state_from_response(self.last_description)
        if status not in self.non_terminal_states() and status not in self.failed_states():
            billable_time = (
                self.last_description['TrainingEndTime'] - self.last_description['TrainingStartTime']
            ) * self.last_description['ResourceConfig']['InstanceCount']
            self.log.info('Billable seconds: %s', int(billable_time.total_seconds()) + 1)

        return self.last_description

    def get_failed_reason_from_response(self, response):
        return response['FailureReason']

    def state_from_response(self, response):
        return response['TrainingJobStatus']
