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

from airflow.providers.amazon.aws.hooks.neptune import NeptuneHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class NeptuneClusterAvailableTrigger(AwsBaseWaiterTrigger):
    """
    Triggers when a Neptune Cluster is available.

    :param db_cluster_id: Cluster ID to poll.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name (example: us-east-1)
    """

    def __init__(
        self,
        *,
        db_cluster_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = None,
        region_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"db_cluster_id": db_cluster_id},
            waiter_name="cluster_available",
            waiter_args={"DBClusterIdentifier": db_cluster_id},
            failure_message="Failed to start Neptune cluster",
            status_message="Status of Neptune cluster is",
            status_queries=["DBClusters[0].Status"],
            return_key="db_cluster_id",
            return_value=db_cluster_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return NeptuneHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )


class NeptuneClusterStoppedTrigger(AwsBaseWaiterTrigger):
    """
    Triggers when a Neptune Cluster is stopped.

    :param db_cluster_id: Cluster ID to poll.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name (example: us-east-1)
    """

    def __init__(
        self,
        *,
        db_cluster_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = None,
        region_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"db_cluster_id": db_cluster_id},
            waiter_name="cluster_stopped",
            waiter_args={"DBClusterIdentifier": db_cluster_id},
            failure_message="Failed to stop Neptune cluster",
            status_message="Status of Neptune cluster is",
            status_queries=["DBClusters[0].Status"],
            return_key="db_cluster_id",
            return_value=db_cluster_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return NeptuneHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )


class NeptuneClusterInstancesAvailableTrigger(AwsBaseWaiterTrigger):
    """
    Triggers when a Neptune Cluster Instance is available.

    :param db_cluster_id: Cluster ID to wait on instances from
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name (example: us-east-1)
    """

    def __init__(
        self,
        *,
        db_cluster_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        aws_conn_id: str | None = None,
        region_name: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"db_cluster_id": db_cluster_id},
            waiter_name="db_instance_available",
            waiter_args={
                "Filters": [{"Name": "db-cluster-id", "Values": [db_cluster_id]}]
            },
            failure_message="Failed to start Neptune instances",
            status_message="Status of Neptune instances are",
            status_queries=["DBInstances[].Status"],
            return_key="db_cluster_id",
            return_value=db_cluster_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            aws_conn_id=aws_conn_id,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return NeptuneHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )
