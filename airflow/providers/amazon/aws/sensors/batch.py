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

from typing import Dict, Optional

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.batch_client import AwsBatchClientHook
from airflow.sensors.base import BaseSensorOperator


class BatchSensor(BaseSensorOperator):
    """
    Asks for the state of the Batch Job execution until it reaches a failure state or success state.
    If the job fails, the task will fail.

    :param job_id: Batch job_id to check the state for
    :type job_id: str
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :type aws_conn_id: str
    """

    INTERMEDIATE_STATES = (
        'SUBMITTED',
        'PENDING',
        'RUNNABLE',
        'STARTING',
        'RUNNING',
    )
    FAILURE_STATES = ('FAILED',)
    SUCCESS_STATES = ('SUCCEEDED',)

    template_fields = ['job_id']
    template_ext = ()
    ui_color = '#66c3ff'

    def __init__(
        self,
        *,
        job_id: str,
        aws_conn_id: str = 'aws_default',
        region_name: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.hook: Optional[AwsBatchClientHook] = None

    def poke(self, context: Dict) -> bool:
        job_description = self.get_hook().get_job_description(self.job_id)
        state = job_description['status']

        if state in self.FAILURE_STATES:
            raise AirflowException(f'Batch sensor failed. Batch Job Status: {state}')

        if state in self.INTERMEDIATE_STATES:
            return False

        return True

    def get_hook(self) -> AwsBatchClientHook:
        """Create and return a AwsBatchClientHook"""
        if self.hook:
            return self.hook

        self.hook = AwsBatchClientHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
        )
        return self.hook
