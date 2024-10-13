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

from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger
from airflow.providers.amazon.aws.utils.rds import RdsDbType

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


_waiter_arg = {
    RdsDbType.INSTANCE.value: "DBInstanceIdentifier",
    RdsDbType.CLUSTER.value: "DBClusterIdentifier",
}
_status_paths = {
    RdsDbType.INSTANCE.value: ["DBInstances[].DBInstanceStatus", "DBInstances[].StatusInfos"],
    RdsDbType.CLUSTER.value: ["DBClusters[].Status"],
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
        aws_conn_id: str | None,
        response: dict[str, Any],
        db_type: RdsDbType | str,
        region_name: str | None = None,
    ) -> None:
        # allow passing enums for users,
        # but we can only rely on strings because (de-)serialization doesn't support enums
        if isinstance(db_type, RdsDbType):
            db_type_str = db_type.value
        else:
            db_type_str = db_type

        super().__init__(
            serialized_fields={
                "db_identifier": db_identifier,
                "response": response,
                "db_type": db_type_str,
            },
            waiter_name=f"db_{db_type_str}_available",
            waiter_args={_waiter_arg[db_type_str]: db_identifier},
            failure_message="Error while waiting for DB to be available",
            status_message="DB initialization in progress",
            status_queries=_status_paths[db_type_str],
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
        aws_conn_id: str | None,
        response: dict[str, Any],
        db_type: RdsDbType | str,
        region_name: str | None = None,
    ) -> None:
        # allow passing enums for users,
        # but we can only rely on strings because (de-)serialization doesn't support enums
        if isinstance(db_type, RdsDbType):
            db_type_str = db_type.value
        else:
            db_type_str = db_type

        super().__init__(
            serialized_fields={
                "db_identifier": db_identifier,
                "response": response,
                "db_type": db_type_str,
            },
            waiter_name=f"db_{db_type_str}_deleted",
            waiter_args={_waiter_arg[db_type_str]: db_identifier},
            failure_message="Error while deleting DB",
            status_message="DB deletion in progress",
            status_queries=_status_paths[db_type_str],
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
        aws_conn_id: str | None,
        response: dict[str, Any],
        db_type: RdsDbType | str,
        region_name: str | None = None,
    ) -> None:
        # allow passing enums for users,
        # but we can only rely on strings because (de-)serialization doesn't support enums
        if isinstance(db_type, RdsDbType):
            db_type_str = db_type.value
        else:
            db_type_str = db_type

        super().__init__(
            serialized_fields={
                "db_identifier": db_identifier,
                "response": response,
                "db_type": db_type_str,
            },
            waiter_name=f"db_{db_type_str}_stopped",
            waiter_args={_waiter_arg[db_type_str]: db_identifier},
            failure_message="Error while stopping DB",
            status_message="DB is being stopped",
            status_queries=_status_paths[db_type_str],
            return_key="response",
            return_value=response,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            region_name=region_name,
        )

    def hook(self) -> AwsGenericHook:
        return RdsHook(aws_conn_id=self.aws_conn_id, region_name=self.region_name)
