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

from typing import Any, Optional

try:
    from functools import cached_property
except ImportError:
    from cached_property import cached_property

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.emr_containers import EMRContainerHook
from airflow.sensors.base import BaseSensorOperator


class EMRContainerSensor(BaseSensorOperator):
    """
    Asks for the state of the job run until it reaches a failure state or success state.
    If the job run fails, the task will fail.

    :param job_id: job_id to check the state of
    :type job_id: str
    :param max_retries: Number of times to poll for query state before
        returning the current state, defaults to None
    :type max_retries: int
    :param aws_conn_id: aws connection to use, defaults to 'aws_default'
    :type aws_conn_id: str
    :param poll_interval: Time in seconds to wait between two consecutive call to
        check query status on athena, defaults to 10
    :type poll_interval: int
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

    template_fields = ['virtual_cluster_id', 'job_id']
    template_ext = ()
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

    def poke(self, context: dict) -> bool:
        state = self.hook.poll_query_status(self.job_id, self.max_retries, self.poll_interval)

        if state in self.FAILURE_STATES:
            raise AirflowException('EMR Containers sensor failed')

        if state in self.INTERMEDIATE_STATES:
            return False
        return True

    @cached_property
    def hook(self) -> EMRContainerHook:
        """Create and return an EMRContainerHook"""
        return EMRContainerHook(self.aws_conn_id, virtual_cluster_id=self.virtual_cluster_id)
