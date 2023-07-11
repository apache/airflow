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

from typing import Any

from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.triggers.base import BaseTrigger, TriggerEvent


class RdsDbInstanceTrigger(BaseTrigger):
    """
    Trigger for RdsCreateDbInstanceOperator and RdsDeleteDbInstanceOperator.

    The trigger will asynchronously poll the boto3 API and wait for the
    DB instance to be in the state specified by the waiter.

    :param waiter_name: Name of the waiter to use, for instance 'db_instance_available'
        or 'db_instance_deleted'.
    :param db_instance_identifier: The DB instance identifier for the DB instance to be polled.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param hook_params: The parameters to pass to the RdsHook.
    :param response: The response from the RdsHook, to be passed back to the operator.
    """

    def __init__(
        self,
        waiter_name: str,
        db_instance_identifier: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str,
        region_name: str | None,
        response: dict[str, Any],
    ):
        self.db_instance_identifier = db_instance_identifier
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.aws_conn_id = aws_conn_id
        self.region_name = region_name
        self.waiter_name = waiter_name
        self.response = response

    def serialize(self) -> tuple[str, dict[str, Any]]:
        return (
            # dynamically generate the fully qualified name of the class
            self.__class__.__module__ + "." + self.__class__.__qualname__,
            {
                "db_instance_identifier": self.db_instance_identifier,
                "waiter_delay": str(self.waiter_delay),
                "waiter_max_attempts": str(self.waiter_max_attempts),
                "aws_conn_id": self.aws_conn_id,
                "region_name": self.region_name,
                "waiter_name": self.waiter_name,
                "response": self.response,
            },
        )

    async def run(self):
        self.hook = RdsHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
        async with self.hook.async_conn as client:
            waiter = client.get_waiter(self.waiter_name)
            await async_wait(
                waiter=waiter,
                waiter_delay=int(self.waiter_delay),
                waiter_max_attempts=int(self.waiter_max_attempts),
                args={"DBInstanceIdentifier": self.db_instance_identifier},
                failure_message="Error checking DB Instance status",
                status_message="DB instance status is",
                status_args=["DBInstances[0].DBInstanceStatus"],
            )
        yield TriggerEvent({"status": "success", "response": self.response})
