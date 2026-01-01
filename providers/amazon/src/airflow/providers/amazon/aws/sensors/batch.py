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

from collections.abc import Sequence
from datetime import timedelta
from typing import TYPE_CHECKING, Any

from airflow.configuration import conf
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.sensors.base_aws import AwsBaseSensor
from airflow.providers.amazon.aws.triggers.batch import BatchJobTrigger
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import AirflowException

if TYPE_CHECKING:
    from airflow.sdk import Context


class BatchSensor(AwsBaseSensor[BatchClientHook]):
    """
    Poll the state of the Batch Job until it reaches a terminal state; fails if the job fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BatchSensor`

    :param job_id: Batch job_id to check the state for
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param deferrable: Run sensor in the deferrable mode.
    :param poke_interval: polling period in seconds to check for the status of the job.
    :param max_retries: Number of times to poll for job state before
        returning the current state.
    """

    aws_hook_class = BatchClientHook
    template_fields: Sequence[str] = aws_template_fields(
        "job_id",
    )
    template_ext: Sequence[str] = ()
    ui_color = "#66c3ff"

    def __init__(
        self,
        *,
        job_id: str,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        poke_interval: float = 30,
        max_retries: int = 4200,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_id = job_id
        self.deferrable = deferrable
        self.poke_interval = poke_interval
        self.max_retries = max_retries

    def poke(self, context: Context) -> bool:
        job_description = self.hook.get_job_description(self.job_id)
        state = job_description["status"]

        if state == BatchClientHook.SUCCESS_STATE:
            return True

        if state in BatchClientHook.INTERMEDIATE_STATES:
            return False

        raise AirflowException(f"Batch sensor failed. AWS Batch job status: {state}")

    def execute(self, context: Context) -> None:
        if not self.deferrable:
            super().execute(context=context)
        else:
            timeout = (
                timedelta(seconds=self.max_retries * self.poke_interval + 60)
                if self.max_retries
                else self.execution_timeout
            )
            self.defer(
                timeout=timeout,
                trigger=BatchJobTrigger(
                    job_id=self.job_id,
                    aws_conn_id=self.aws_conn_id,
                    region_name=self.region_name,
                    waiter_delay=int(self.poke_interval),
                    waiter_max_attempts=self.max_retries,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context: Context, event: dict[str, Any]) -> None:
        """
        Execute when the trigger fires - returns immediately.

        Relies on trigger to throw an exception, otherwise it assumes execution was successful.
        """
        if event["status"] != "success":
            raise AirflowException(f"Error while running job: {event}")
        job_id = event["job_id"]
        self.log.info("Batch Job %s complete", job_id)


class BatchComputeEnvironmentSensor(AwsBaseSensor[BatchClientHook]):
    """
    Poll the state of the Batch environment until it reaches a terminal state; fails if the environment fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BatchComputeEnvironmentSensor`

    :param compute_environment: Batch compute environment name

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html

    """

    aws_hook_class = BatchClientHook
    template_fields: Sequence[str] = aws_template_fields(
        "compute_environment",
    )
    template_ext: Sequence[str] = ()
    ui_color = "#66c3ff"

    def __init__(
        self,
        compute_environment: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.compute_environment = compute_environment

    def poke(self, context: Context) -> bool:
        response = self.hook.client.describe_compute_environments(  # type: ignore[union-attr]
            computeEnvironments=[self.compute_environment]
        )

        if not response["computeEnvironments"]:
            raise AirflowException(f"AWS Batch compute environment {self.compute_environment} not found")

        status = response["computeEnvironments"][0]["status"]

        if status in BatchClientHook.COMPUTE_ENVIRONMENT_TERMINAL_STATUS:
            return True

        if status in BatchClientHook.COMPUTE_ENVIRONMENT_INTERMEDIATE_STATUS:
            return False

        raise AirflowException(
            f"AWS Batch compute environment failed. AWS Batch compute environment status: {status}"
        )


class BatchJobQueueSensor(AwsBaseSensor[BatchClientHook]):
    """
    Poll the state of the Batch job queue until it reaches a terminal state; fails if the queue fails.

    .. seealso::
        For more information on how to use this sensor, take a look at the guide:
        :ref:`howto/sensor:BatchJobQueueSensor`

    :param job_queue: Batch job queue name

    :param treat_non_existing_as_deleted: If True, a non-existing Batch job queue is considered as a deleted
        queue and as such a valid case.

    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    """

    aws_hook_class = BatchClientHook
    template_fields: Sequence[str] = aws_template_fields(
        "job_queue",
    )
    template_ext: Sequence[str] = ()
    ui_color = "#66c3ff"

    def __init__(
        self,
        job_queue: str,
        treat_non_existing_as_deleted: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.job_queue = job_queue
        self.treat_non_existing_as_deleted = treat_non_existing_as_deleted

    def poke(self, context: Context) -> bool:
        response = self.hook.client.describe_job_queues(  # type: ignore[union-attr]
            jobQueues=[self.job_queue]
        )

        if not response["jobQueues"]:
            if self.treat_non_existing_as_deleted:
                return True
            raise AirflowException(f"AWS Batch job queue {self.job_queue} not found")

        status = response["jobQueues"][0]["status"]

        if status in BatchClientHook.JOB_QUEUE_TERMINAL_STATUS:
            return True

        if status in BatchClientHook.JOB_QUEUE_INTERMEDIATE_STATUS:
            return False

        message = f"AWS Batch job queue failed. AWS Batch job queue status: {status}"
        raise AirflowException(message)
