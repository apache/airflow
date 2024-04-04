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

from airflow.providers.amazon.aws.hooks.bedrock import BedrockHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class BedrockCustomizeModelCompletedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a Bedrock model customization job is complete.

    :param job_name: The name of the Bedrock model customization job.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 120)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        *,
        job_name: str,
        waiter_delay: int = 120,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={"job_name": job_name},
            waiter_name="model_customization_job_complete",
            waiter_args={"jobIdentifier": job_name},
            failure_message="Bedrock model customization failed.",
            status_message="Status of Bedrock model customization job is",
            status_queries=["status"],
            return_key="job_name",
            return_value=job_name,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
        )

    def hook(self) -> AwsGenericHook:
        return BedrockHook(aws_conn_id=self.aws_conn_id)
