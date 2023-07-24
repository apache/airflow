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

import warnings
from typing import Any

from airflow.exceptions import AirflowProviderDeprecationWarning
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.providers.amazon.aws.utils.rds import RdsDbType
from airflow.providers.amazon.aws.utils.waiter_with_logging import async_wait
from airflow.triggers.base import BaseTrigger, TriggerEvent


class RdsDbInstanceTrigger(BaseTrigger):
    """
    Deprecated Trigger for RDS operations. Do not use.

    :param waiter_name: Name of the waiter to use, for instance 'db_instance_available'
        or 'db_instance_deleted'.
    :param db_instance_identifier: The DB instance identifier for the DB instance to be polled.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region where the DB is located, if different from the default one.
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
        warnings.warn(
            "This trigger is deprecated, please use the other RDS triggers "
            "such as RdsDbDeletedTrigger, RdsDbStoppedTrigger or RdsDbAvailableTrigger",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
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


_waiter_arg = {
    RdsDbType.INSTANCE: "DBInstanceIdentifier",
    RdsDbType.CLUSTER: "DBClusterIdentifier",
}
_status_paths = {
    RdsDbType.INSTANCE: ["DBInstances[].DBInstanceStatus", "DBInstances[].StatusInfos"],
    RdsDbType.CLUSTER: ["DBClusters[].Status"],
}


class RdsDbAvailableTrigger(AwsBaseWaiterTrigger):
    """
    Trigger to wait asynchronously for a DB instance or cluster to be available.

    :param db_identifier: The DB identifier for the DB instance or cluster to be polled.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region where the DB is located, if different from the default one.
    :param response: The response from the RdsHook, to be passed back to the operator.
    :param db_type: The type of DB: instance or cluster.
    """

    def __init__(
        self,
        db_identifier: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str,
        response: dict[str, Any],
        db_type: RdsDbType,
        region_name: str | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={
                "db_identifier": db_identifier,
                "response": response,
                "db_type": db_type,
            },
            waiter_name=f"db_{db_type.value}_available",
            waiter_args={_waiter_arg[db_type]: db_identifier},
            failure_message="Error while waiting for DB to be available",
            status_message="DB initialization in progress",
            status_queries=_status_paths[db_type],
            return_key="response",
            return_value=response,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return RdsHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)


class RdsDbDeletedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger to wait asynchronously for a DB instance or cluster to be deleted.

    :param db_identifier: The DB identifier for the DB instance or cluster to be polled.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region where the DB is located, if different from the default one.
    :param response: The response from the RdsHook, to be passed back to the operator.
    :param db_type: The type of DB: instance or cluster.
    """

    def __init__(
        self,
        db_identifier: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str,
        response: dict[str, Any],
        db_type: RdsDbType,
        region_name: str | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={
                "db_identifier": db_identifier,
                "response": response,
                "db_type": db_type,
            },
            waiter_name=f"db_{db_type.value}_deleted",
            waiter_args={_waiter_arg[db_type]: db_identifier},
            failure_message="Error while deleting DB",
            status_message="DB deletion in progress",
            status_queries=_status_paths[db_type],
            return_key="response",
            return_value=response,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return RdsHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)


class RdsDbStoppedTrigger(AwsBaseWaiterTrigger):
    """
    Trigger to wait asynchronously for a DB instance or cluster to be stopped.

    :param db_identifier: The DB identifier for the DB instance or cluster to be polled.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region where the DB is located, if different from the default one.
    :param response: The response from the RdsHook, to be passed back to the operator.
    :param db_type: The type of DB: instance or cluster.
    """

    def __init__(
        self,
        db_identifier: str,
        waiter_delay: int,
        waiter_max_attempts: int,
        aws_conn_id: str,
        response: dict[str, Any],
        db_type: RdsDbType,
        region_name: str | None = None,
    ) -> None:
        super().__init__(
            serialized_fields={
                "db_identifier": db_identifier,
                "response": response,
                "db_type": db_type,
            },
            waiter_name=f"db_{db_type.value}_stopped",
            waiter_args={_waiter_arg[db_type]: db_identifier},
            failure_message="Error while stopping DB",
            status_message="DB is being stopped",
            status_queries=_status_paths[db_type],
            return_key="response",
            return_value=response,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return RdsHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
