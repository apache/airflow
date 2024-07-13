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

from typing import TYPE_CHECKING, Any

from airflow.providers.amazon.aws.hooks.kinesis_analytics import KinesisAnalyticsV2Hook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class KinesisAnalyticsV2ApplicationOperationCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a Managed Service for Apache Flink application update is complete.

    :param application_name: Application name.
    :param operation_id: Identifier of the Operation.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 120)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    """

    def __init__(
        self,
        *,
        application_name: str,
        waiter_name: str,
        waiter_args: dict[str, Any],
        status_queries: list[str],
        operation_id: str,
        waiter_delay: int = 120,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = "aws_default",
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"application_name": application_name, "operation_id": operation_id},
            waiter_name=waiter_name,
            waiter_args=waiter_args,
            failure_message=f"AWS Managed Service for Apache Flink Application {application_name} failed.",
            status_message=f"Status of AWS Managed Service for Apache Flink Application {application_name} is",
            status_queries=status_queries,
            return_key="application_operation_details",
            return_value={"application_name": application_name, "operation_id": operation_id},
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return KinesisAnalyticsV2Hook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )
