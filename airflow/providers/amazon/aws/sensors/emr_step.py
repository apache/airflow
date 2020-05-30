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

from typing import Any, Dict, Iterable, Optional

from airflow.providers.amazon.aws.sensors.emr_base import EmrBaseSensor
from airflow.utils.decorators import apply_defaults


class EmrStepSensor(EmrBaseSensor):
    """
    Asks for the state of the step until it reaches any of the target states.
    If it fails the sensor errors, failing the task.

    With the default target states, sensor waits step to be completed.

    :param job_flow_id: job_flow_id which contains the step check the state of
    :type job_flow_id: str
    :param step_id: step to check the state of
    :type step_id: str
    :param target_states: the target states, sensor waits until
        step reaches any of these states
    :type target_states: list[str]
    :param failed_states: the failure states, sensor fails when
        step reaches any of these states
    :type failed_states: list[str]
    """

    template_fields = ['job_flow_id', 'step_id',
                       'target_states', 'failed_states']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 job_flow_id: str,
                 step_id: str,
                 target_states: Optional[Iterable[str]] = None,
                 failed_states: Optional[Iterable[str]] = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id
        self.step_id = step_id
        self.target_states = target_states or ['COMPLETED']
        self.failed_states = failed_states or ['CANCELLED', 'FAILED',
                                               'INTERRUPTED']

    def get_emr_response(self) -> Dict[str, Any]:
        """
        Make an API call with boto3 and get details about the cluster step.

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step

        :return: response
        :rtype: dict[str, Any]
        """
        emr_client = self.get_hook().get_conn()

        self.log.info('Poking step %s on cluster %s',
                      self.step_id,
                      self.job_flow_id)
        return emr_client.describe_step(
            ClusterId=self.job_flow_id,
            StepId=self.step_id)

    @staticmethod
    def state_from_response(response: Dict[str, Any]) -> str:
        """
        Get state from response dictionary.

        :param response: response from AWS API
        :type response: dict[str, Any]
        :return: execution state of the cluster step
        :rtype: str
        """
        return response['Step']['Status']['State']

    @staticmethod
    def failure_message_from_response(response: Dict[str, Any]) -> Optional[str]:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :type response: dict[str, Any]
        :return: failure message
        :rtype: Optional[str]
        """
        fail_details = response['Step']['Status'].get('FailureDetails')
        if fail_details:
            return 'for reason {} with message {} and log file {}'.format(
                fail_details.get('Reason'),
                fail_details.get('Message'),
                fail_details.get('LogFile'))
        return None
