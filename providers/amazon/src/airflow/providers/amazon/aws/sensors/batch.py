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
from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Sequence

if sys.version_info >= (3, 8):
    from functools import cached_property
else:
    from cached_property import cached_property

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
    :param region_name: aws region name associated with the client
    """

    template_fields: Sequence[str] = ("job_id",)
    template_ext: Sequence[str] = ()
    ui_color = "#66c3ff"

    def __init__(
        self,
        *,
        job_id: str,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.hook: BatchClientHook | None = None

    def poke(self, context: Context) -> bool:
        job_description = self.get_hook().get_job_description(self.job_id)
        state = job_description["status"]

        if state == BatchClientHook.SUCCESS_STATE:
            return True

        if state in BatchClientHook.INTERMEDIATE_STATES:
            return False

        if state == BatchClientHook.FAILURE_STATE:
            raise AirflowException(f"Batch sensor failed. AWS Batch job status: {state}")

        raise AirflowException(f"Batch sensor failed. Unknown AWS Batch job status: {state}")

    def get_hook(self) -> BatchClientHook:
        """Create and return a BatchClientHook"""
        if self.hook:
            return self.hook

        self.hook = BatchClientHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
        )
        return self.hook


class BatchComputeEnvironmentSensor(BaseSensorOperator):
    """
    Asks for the state of the Batch compute environment until it reaches a failure state or success state.
    If the environment fails, the task will fail.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BatchComputeEnvironmentSensor`

    :param compute_environment: Batch compute environment name

    :param aws_conn_id: aws connection to use, defaults to 'aws_default'

    :param region_name: aws region name associated with the client
    """

    template_fields: Sequence[str] = ("compute_environment",)
    template_ext: Sequence[str] = ()
    ui_color = "#66c3ff"

    def __init__(
        self,
        compute_environment: str,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.compute_environment = compute_environment
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    @cached_property
    def hook(self) -> BatchClientHook:
        """Create and return a BatchClientHook"""
        return BatchClientHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
        )

    def poke(self, context: Context) -> bool:
        response = self.hook.client.describe_compute_environments(
            computeEnvironments=[self.compute_environment]
        )

        if len(response["computeEnvironments"]) == 0:
            raise AirflowException(f"AWS Batch compute environment {self.compute_environment} not found")

        status = response["computeEnvironments"][0]["status"]

        if status in BatchClientHook.COMPUTE_ENVIRONMENT_TERMINAL_STATUS:
            return True

        if status in BatchClientHook.COMPUTE_ENVIRONMENT_INTERMEDIATE_STATUS:
            return False

        raise AirflowException(
            f"AWS Batch compute environment failed. AWS Batch compute environment status: {status}"
        )


class BatchJobQueueSensor(BaseSensorOperator):
    """
    Asks for the state of the Batch job queue until it reaches a failure state or success state.
    If the queue fails, the task will fail.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BatchJobQueueSensor`

    :param job_queue: Batch job queue name

    :param treat_non_existing_as_deleted: If True, a non-existing Batch job queue is considered as a deleted
        queue and as such a valid case.

    :param aws_conn_id: aws connection to use, defaults to 'aws_default'

    :param region_name: aws region name associated with the client
    """

    template_fields: Sequence[str] = ("job_queue",)
    template_ext: Sequence[str] = ()
    ui_color = "#66c3ff"

    def __init__(
        self,
        job_queue: str,
        treat_non_existing_as_deleted: bool = False,
        aws_conn_id: str = "aws_default",
        region_name: str | None = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_queue = job_queue
        self.treat_non_existing_as_deleted = treat_non_existing_as_deleted
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name

    @cached_property
    def hook(self) -> BatchClientHook:
        """Create and return a BatchClientHook"""
        return BatchClientHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
        )

    def poke(self, context: Context) -> bool:
        response = self.hook.client.describe_job_queues(jobQueues=[self.job_queue])

        if len(response["jobQueues"]) == 0:
            if self.treat_non_existing_as_deleted:
                return True
            else:
                raise AirflowException(f"AWS Batch job queue {self.job_queue} not found")

        status = response["jobQueues"][0]["status"]

        if status in BatchClientHook.JOB_QUEUE_TERMINAL_STATUS:
            return True

        if status in BatchClientHook.JOB_QUEUE_INTERMEDIATE_STATUS:
            return False

        raise AirflowException(f"AWS Batch job queue failed. AWS Batch job queue status: {status}")
