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

from collections.abc import AsyncIterator
from typing import TYPE_CHECKING

from airflow.providers.amazon.aws.hooks.ssm import SsmHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.triggers.base import TriggerEvent

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class SsmRunCommandTrigger(AwsBaseWaiterTrigger):
    """
    Trigger when a SSM run command is complete.

    :param command_id: The ID of the AWS SSM Run Command.
    :param waiter_delay: The amount of time in seconds to wait between attempts. (default: 120)
    :param waiter_max_attempts: The maximum number of attempts to be made. (default: 75)
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.
    :param verify: Whether or not to verify SSL certificates. See:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html
    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    """

    def __init__(
        self,
        *,
        command_id: str,
        waiter_delay: int = 120,
        waiter_max_attempts: int = 75,
        aws_conn_id: str | None = None,
        region_name: str | None = None,
        verify: bool | str | None = None,
        botocore_config: dict | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={"command_id": command_id},
            waiter_name="command_executed",
            waiter_args={"CommandId": command_id},
            failure_message="SSM run command failed.",
            status_message="Status of SSM run command is",
            status_queries=["status"],
            return_key="command_id",
            return_value=command_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
            verify=verify,
            botocore_config=botocore_config,
        )
        self.command_id = command_id

    def hook(self) -> AwsGenericHook:
        return SsmHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )

    async def run(self) -> AsyncIterator[TriggerEvent]:
        hook = self.hook()
        async with await hook.get_async_conn() as client:
            response = await client.list_command_invocations(CommandId=self.command_id)
            instance_ids = [invocation["InstanceId"] for invocation in response.get("CommandInvocations", [])]
            waiter = hook.get_waiter(self.waiter_name, deferrable=True, client=client)

            for instance_id in instance_ids:
                self.waiter_args["InstanceId"] = instance_id
                await async_wait(
                    waiter,
                    self.waiter_delay,
                    self.attempts,
                    self.waiter_args,
                    self.failure_message,
                    self.status_message,
                    self.status_queries,
                )

        yield TriggerEvent({"status": "success", self.return_key: self.return_value})
