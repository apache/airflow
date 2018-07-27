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
from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils import apply_defaults
from airflow.exceptions import AirflowException


class SageMakerBaseSensor(BaseSensorOperator):
    """
    Contains general sensor behavior for SageMaker.
    Subclasses should implement get_emr_response() and state_from_response() methods.
    Subclasses should also implement NON_TERMINAL_STATES and FAILED_STATE constants.
    """
    ui_color = '#66c3ff'

    @apply_defaults
    def __init__(
            self,
            aws_conn_id='aws_default',
            *args, **kwargs):
        super(SageMakerBaseSensor, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id

    def poke(self, context):
        try:
            response = self.get_sagemaker_response()
        except AttributeError:
            raise AirflowException(
                "Method get_sagemaker_response()not implemented.")

        if not response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.log.info('Bad HTTP response: %s', response)
            return False
        try:
            state = self.state_from_response(response)
        except ValueError:
            raise AirflowException(
                "Method state_from_response()not implemented.")

        self.log.info('Job currently %s', state)

        if state in self.NON_TERMINAL_STATES:
            return False

        if state in self.FAILED_STATE:
            raise AirflowException("Sagemaker job failed")
        return True
