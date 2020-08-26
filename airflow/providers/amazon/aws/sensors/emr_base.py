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

from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr import EmrHook
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults


class EmrBaseSensor(BaseSensorOperator):
    """
    Contains general sensor behavior for EMR.

    Subclasses should implement following methods:
        - ``get_emr_response()``
        - ``state_from_response()``
        - ``failure_message_from_response()``

    Subclasses should set ``target_states`` and ``failed_states`` fields.

    :param aws_conn_id: aws connection to uses
    :type aws_conn_id: str
    """

    ui_color = '#66c3ff'

    @apply_defaults
    def __init__(self, *, aws_conn_id='aws_default', **kwargs):
        super().__init__(**kwargs)
        self.aws_conn_id = aws_conn_id
        self.target_states = None  # will be set in subclasses
        self.failed_states = None  # will be set in subclasses
        self.hook = None

    def get_hook(self):
        """Get EmrHook"""
        if not self.hook:
            self.hook = EmrHook(aws_conn_id=self.aws_conn_id)
        return self.hook

    def poke(self, context):
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
        :type response: dict[str, Any]
        :return: state
        :rtype: str
        """
        raise NotImplementedError('Please implement state_from_response() in subclass')

    @staticmethod
    def failure_message_from_response(response: Dict[str, Any]) -> Optional[str]:
        """
        Get failure message from response dictionary.

        :param response: response from AWS API
        :type response: dict[str, Any]
        :return: failure message
        :rtype: Optional[str]
        """
        raise NotImplementedError('Please implement failure_message_from_response() in subclass')
