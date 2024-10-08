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

from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class BatchJobTrigger(AwsBaseWaiterTrigger):
    """
    Checks for the status of a submitted job_id to AWS Batch until it reaches a failure or a success state.

    :param job_id: the job ID, to poll for job completion or not
    :param region_name: AWS region name to use
        Override the region_name in connection (if provided)
    :param aws_conn_id: connection id of AWS credentials / region name. If None,
        credential boto3 strategy will be used
    :param waiter_delay: polling period in seconds to check for the status of the job
    :param waiter_max_attempts: The maximum number of attempts to be made.
    """

    def __init__(
        self,
        job_id: str | None,
        region_name: str | None = None,
        aws_conn_id: str | None = "aws_default",
        waiter_delay: int = 5,
        waiter_max_attempts: int = 720,
    ):
        super().__init__(
            serialized_fields={"job_id": job_id},
            waiter_name="batch_job_complete",
            waiter_args={"jobs": [job_id]},
            failure_message=f"Failure while running batch job {job_id}",
            status_message=f"Batch job {job_id} not ready yet",
            status_queries=["jobs[].status", "computeEnvironments[].statusReason"],
            return_key="job_id",
            return_value=job_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return BatchClientHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)


class BatchCreateComputeEnvironmentTrigger(AwsBaseWaiterTrigger):
    """
    Asynchronously poll the boto3 API and wait for the compute environment to be ready.

    :param compute_env_arn: The ARN of the compute env.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: region name to use in AWS Hook
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    """

    def __init__(
        self,
        compute_env_arn: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 10,
        aws_conn_id: str | None = "aws_default",
        region_name: str | None = None,
    ):
        super().__init__(
            serialized_fields={"compute_env_arn": compute_env_arn},
            waiter_name="compute_env_ready",
            waiter_args={"computeEnvironments": [compute_env_arn]},
            failure_message="Failure while creating Compute Environment",
            status_message="Compute Environment not ready yet",
            status_queries=["computeEnvironments[].status", "computeEnvironments[].statusReason"],
            return_value=compute_env_arn,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return BatchClientHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
