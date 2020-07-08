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


class EmrJobFlowSensor(EmrBaseSensor):
    """
    Asks for the state of the EMR JobFlow (Cluster) until it reaches
    any of the target states.
    If it fails the sensor errors, failing the task.

    With the default target states, sensor waits cluster to be terminated.
    When target_states is set to ['RUNNING', 'WAITING'] sensor waits
    until job flow to be ready (after 'STARTING' and 'BOOTSTRAPPING' states)

    :param job_flow_id: job_flow_id to check the state of
    :type job_flow_id: str
    :param target_states: the target states, sensor waits until
        job flow reaches any of these states
    :type target_states: list[str]
    :param failed_states: the failure states, sensor fails when
        job flow reaches any of these states
    :type failed_states: list[str]
    """

    template_fields = ['job_flow_id', 'target_states', 'failed_states']
    template_ext = ()

    @apply_defaults
    def __init__(self,
                 job_flow_id: str,
                 target_states: Optional[Iterable[str]] = None,
                 failed_states: Optional[Iterable[str]] = None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)
        self.job_flow_id = job_flow_id
        self.target_states = target_states or ['TERMINATED']
        self.failed_states = failed_states or ['TERMINATED_WITH_ERRORS']

    def get_emr_response(self) -> Dict[str, Any]:
        """
        Make an API call with boto3 and get cluster-level details.

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_cluster

        :return: response
        :rtype: dict[str, Any]
        """
        emr_client = self.get_hook().get_conn()

        self.log.info('Poking cluster %s', self.job_flow_id)
        return emr_client.describe_cluster(ClusterId=self.job_flow_id)

    @staticmethod
    def state_from_response(response: Dict[str, Any]) -> str:
        """
        Get state from response dictionary.

        :param response: response from AWS API
        :type response: dict[str, Any]
        :return: current state of the cluster
        :rtype: str
        """
        return response['Cluster']['Status']['State']

    @staticmethod
    def failure_message_from_response(response: Dict[str, Any]) -> Optional[str]:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :type response: dict[str, Any]
        :return: failure message
        :rtype: Optional[str]
        """
        cluster_status = response['Cluster']['Status']
        state_change_reason = cluster_status.get('StateChangeReason')
        if state_change_reason:
            return 'for code: {} with message {}'.format(
                state_change_reason.get('Code', 'No code'),
                state_change_reason.get('Message', 'Unknown'))
        return None
