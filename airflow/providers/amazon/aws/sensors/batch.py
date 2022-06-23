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

from typing import TYPE_CHECKING, Optional, Sequence

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context


class BatchSensor(BaseSensorOperator):
    """
    Asks for the state of the Batch Job execution until it reaches a failure state or success state.
    If the job fails, the task will fail.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BatchSensor`

    :param job_id: Batch job_id to check the state for
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    """

    template_fields: Sequence[str] = ('job_id',)
    template_ext: Sequence[str] = ()
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
        self.hook: Optional[BatchClientHook] = None

    def poke(self, context: 'Context') -> bool:
        job_description = self.get_hook().get_job_description(self.job_id)
        state = job_description['status']

        if state == BatchClientHook.SUCCESS_STATE:
            return True

        if state in BatchClientHook.INTERMEDIATE_STATES:
            return False

        if state == BatchClientHook.FAILURE_STATE:
            raise AirflowException(f'Batch sensor failed. AWS Batch job status: {state}')

        raise AirflowException(f'Batch sensor failed. Unknown AWS Batch job status: {state}')

    def get_hook(self) -> BatchClientHook:
        """Create and return a BatchClientHook"""
        if self.hook:
            return self.hook

        self.hook = BatchClientHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
        )
        return self.hook
