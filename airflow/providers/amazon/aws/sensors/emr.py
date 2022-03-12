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
import sys
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Sequence

if TYPE_CHECKING:
    from airflow.utils.context import Context


if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrContainerHook, EmrHook
from airflow.sensors.base import BaseSensorOperator


class EmrBaseSensor(BaseSensorOperator):
    """
    Contains general sensor behavior for EMR.

    Subclasses should implement following methods:
        - ``get_emr_response()``
        - ``state_from_response()``
        - ``failure_message_from_response()``

    Subclasses should set ``target_states`` and ``failed_states`` fields.

    :param aws_conn_id: aws connection to uses
    """

    ui_color = '#66c3ff'

    def __init__(self, *, aws_conn_id: str = 'aws_default', **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.target_states: Iterable[str] = []  # will be set in subclasses
        self.failed_states: Iterable[str] = []  # will be set in subclasses
        self.hook: Optional[EmrHook] = None

    def get_hook(self) -> EmrHook:
        """Get EmrHook"""
        if self.hook:
            return self.hook

        self.hook = EmrHook(aws_conn_id=self.aws_conn_id)
        return self.hook

    def poke(self, context: 'Context'):
        response = self.get_emr_response()

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False

        state = self.state_from_response(response)
        self.log.info('Job flow currently %s', state)

        if state in self.target_states:
            return True

        if state in self.failed_states:
            final_message = 'EMR job failed'
            failure_message = self.failure_message_from_response(response)
            if failure_message:
                final_message += ' ' + failure_message
            raise AirflowException(final_message)

        return False

    def get_emr_response(self) -> Dict[str, Any]:
        """
        Make an API call with boto3 and get response.

        :return: response
        :rtype: dict[str, Any]
        """
        raise NotImplementedError('Please implement get_emr_response() in subclass')

    @staticmethod
    def state_from_response(response: Dict[str, Any]) -> str:
        """
        Get state from response dictionary.

        :param response: response from AWS API
        :return: state
        :rtype: str
        """
        raise NotImplementedError('Please implement state_from_response() in subclass')

    @staticmethod
    def failure_message_from_response(response: Dict[str, Any]) -> Optional[str]:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        :rtype: Optional[str]
        """
        raise NotImplementedError('Please implement failure_message_from_response() in subclass')


class EmrContainerSensor(BaseSensorOperator):
    """
    Asks for the state of the job run until it reaches a failure state or success state.
    If the job run fails, the task will fail.

    :param job_id: job_id to check the state of
    :param max_retries: Number of times to poll for query state before
        returning the current state, defaults to None
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :param poll_interval: Time in seconds to wait between two consecutive call to
        check query status on athena, defaults to 10
    """

    INTERMEDIATE_STATES = (
        "PENDING",
        "SUBMITTED",
        "RUNNING",
    )
    FAILURE_STATES = (
        "FAILED",
        "CANCELLED",
        "CANCEL_PENDING",
    )
    SUCCESS_STATES = ("COMPLETED",)

    template_fields: Sequence[str] = ('virtual_cluster_id', 'job_id')
    template_ext: Sequence[str] = ()
    ui_color = '#66c3ff'

    def __init__(
        self,
        *,
        virtual_cluster_id: str,
        job_id: str,
        max_retries: Optional[int] = None,
        aws_conn_id: str = 'aws_default',
        poll_interval: int = 10,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.virtual_cluster_id = virtual_cluster_id
        self.job_id = job_id
        self.poll_interval = poll_interval
        self.max_retries = max_retries

    def poke(self, context: 'Context') -> bool:
        state = self.hook.poll_query_status(self.job_id, self.max_retries, self.poll_interval)

        if state in self.FAILURE_STATES:
            raise AirflowException('EMR Containers sensor failed')

        if state in self.INTERMEDIATE_STATES:
            return False
        return True

    @cached_property
    def hook(self) -> EmrContainerHook:
        """Create and return an EmrContainerHook"""
        return EmrContainerHook(self.aws_conn_id, virtual_cluster_id=self.virtual_cluster_id)


class EmrJobFlowSensor(EmrBaseSensor):
    """
    Asks for the state of the EMR JobFlow (Cluster) until it reaches
    any of the target states.
    If it fails the sensor errors, failing the task.

    With the default target states, sensor waits cluster to be terminated.
    When target_states is set to ['RUNNING', 'WAITING'] sensor waits
    until job flow to be ready (after 'STARTING' and 'BOOTSTRAPPING' states)

    :param job_flow_id: job_flow_id to check the state of
    :param target_states: the target states, sensor waits until
        job flow reaches any of these states
    :param failed_states: the failure states, sensor fails when
        job flow reaches any of these states
    """

    template_fields: Sequence[str] = ('job_flow_id', 'target_states', 'failed_states')
    template_ext: Sequence[str] = ()

    def __init__(
        self,
        *,
        job_flow_id: str,
        target_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
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
        :return: current state of the cluster
        :rtype: str
        """
        return response['Cluster']['Status']['State']

    @staticmethod
    def failure_message_from_response(response: Dict[str, Any]) -> Optional[str]:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        :rtype: Optional[str]
        """
        cluster_status = response['Cluster']['Status']
        state_change_reason = cluster_status.get('StateChangeReason')
        if state_change_reason:
            return 'for code: {} with message {}'.format(
                state_change_reason.get('Code', 'No code'), state_change_reason.get('Message', 'Unknown')
            )
        return None


class EmrStepSensor(EmrBaseSensor):
    """
    Asks for the state of the step until it reaches any of the target states.
    If it fails the sensor errors, failing the task.

    With the default target states, sensor waits step to be completed.

    :param job_flow_id: job_flow_id which contains the step check the state of
    :param step_id: step to check the state of
    :param target_states: the target states, sensor waits until
        step reaches any of these states
    :param failed_states: the failure states, sensor fails when
        step reaches any of these states
    """

    template_fields: Sequence[str] = ('job_flow_id', 'step_id', 'target_states', 'failed_states')
    template_ext: Sequence[str] = ()

    def __init__(
        self,
        *,
        job_flow_id: str,
        step_id: str,
        target_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_flow_id = job_flow_id
        self.step_id = step_id
        self.target_states = target_states or ['COMPLETED']
        self.failed_states = failed_states or ['CANCELLED', 'FAILED', 'INTERRUPTED']

    def get_emr_response(self) -> Dict[str, Any]:
        """
        Make an API call with boto3 and get details about the cluster step.

        .. seealso::
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.describe_step

        :return: response
        :rtype: dict[str, Any]
        """
        emr_client = self.get_hook().get_conn()

        self.log.info('Poking step %s on cluster %s', self.step_id, self.job_flow_id)
        return emr_client.describe_step(ClusterId=self.job_flow_id, StepId=self.step_id)

    @staticmethod
    def state_from_response(response: Dict[str, Any]) -> str:
        """
        Get state from response dictionary.

        :param response: response from AWS API
        :return: execution state of the cluster step
        :rtype: str
        """
        return response['Step']['Status']['State']

    @staticmethod
    def failure_message_from_response(response: Dict[str, Any]) -> Optional[str]:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :return: failure message
        :rtype: Optional[str]
        """
        fail_details = response['Step']['Status'].get('FailureDetails')
        if fail_details:
            return 'for reason {} with message {} and log file {}'.format(
                fail_details.get('Reason'), fail_details.get('Message'), fail_details.get('LogFile')
            )
        return None
