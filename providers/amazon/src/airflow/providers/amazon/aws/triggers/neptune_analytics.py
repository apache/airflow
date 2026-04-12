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

from airflow.providers.amazon.aws.hooks.neptune_analytics import NeptuneAnalyticsHook
from airflow.providers.amazon.aws.triggers.base import AwsBaseWaiterTrigger

if TYPE_CHECKING:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook


class NeptuneGraphAvailableTrigger(AwsBaseWaiterTrigger):
    """
    Triggers when a Neptune graph is available.

    :param graph_id: Graph ID to poll.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name (example: us-east-1)
    """

    def __init__(
        self,
        *,
        graph_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"graph_id": graph_id},
            waiter_name="graph_available",
            waiter_args={"graphIdentifier": graph_id},
            failure_message="Failed to create Neptune graph",
            status_message="Status of Neptune graph is",
            status_queries=["status"],
            return_key="graph_id",
            return_value=graph_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return NeptuneAnalyticsHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )


class NeptuneGraphPrivateEndpointAvailableTrigger(AwsBaseWaiterTrigger):
    """
    Triggers when a Neptune Graph private endpoint is available.

    :param graph_id: Graph Id waiting for the endpoint
    :param vpc_id: VPC id where endpoint is creating
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name (example: us-east-1)
    """

    def __init__(
        self,
        *,
        graph_id: str,
        vpc_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"graph_id": graph_id, "vpc_id": vpc_id},
            waiter_name="private_graph_endpoint_available",
            waiter_args={"graphIdentifier": graph_id, "vpcId": vpc_id},
            failure_message="Failed to create Neptune graph endpoint",
            status_message="Status of Neptune graph endpoint is",
            status_queries=["status"],
            return_key="graph_id",
            return_value=graph_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return NeptuneAnalyticsHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )


class NeptuneGraphPrivateEndpointDeletedTrigger(AwsBaseWaiterTrigger):
    """
    Triggers when a Neptune Graph private endpoint is deleted.

    :param graph_id: Graph Id of the endpoint
    :param vpc_id: VPC id where endpoint resides
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name (example: us-east-1)
    """

    def __init__(
        self,
        *,
        graph_id: str,
        vpc_id: str,
        endpoint_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"graph_id": graph_id, "vpc_id": vpc_id, "endpoint_id": endpoint_id},
            waiter_name="private_graph_endpoint_deleted",
            waiter_args={"graphIdentifier": graph_id, "vpcId": vpc_id},
            failure_message="Failed to delete Neptune graph endpoint",
            status_message="Status of Neptune graph endpoint is",
            status_queries=["status"],
            return_key="endpoint_id",
            return_value=endpoint_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return NeptuneAnalyticsHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )


class NeptuneGraphDeletedTrigger(AwsBaseWaiterTrigger):
    """
    Triggers when a Neptune Graph is deleted.

    :param graph_id: Graph Id to be deleted
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name (example: us-east-1)
    """

    def __init__(
        self,
        *,
        graph_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"graph_id": graph_id},
            waiter_name="graph_deleted",
            waiter_args={"graphIdentifier": graph_id},
            failure_message="Failed to delete Neptune graph",
            status_message="Status of Neptune graph is",
            status_queries=["status"],
            return_key="graph_id",
            return_value=graph_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return NeptuneAnalyticsHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )


class NeptuneImportTaskCompleteTrigger(AwsBaseWaiterTrigger):
    """
    Triggers when a Neptune import task successfully completes.

    :param task_id: Import task id to monitor
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name (example: us-east-1)
    """

    def __init__(
        self,
        *,
        import_task_id: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"import_task_id": import_task_id},
            waiter_name="import_task_successful",
            waiter_args={"taskIdentifier": import_task_id},
            failure_message="Import task failed",
            status_message="Status of import task is",
            status_queries=["status"],
            return_key="import_task_id",
            return_value=import_task_id,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return NeptuneAnalyticsHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )


class NeptuneImportTaskCancelledTrigger(AwsBaseWaiterTrigger):
    """
    Triggers when a Neptune import task is successfully cancelled.

    :param task_id: Import task id to monitor.
    :param waiter_delay: The amount of time in seconds to wait between attempts.
    :param waiter_max_attempts: The maximum number of attempts to be made.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
    :param region_name: AWS region name (example: us-east-1)
    """

    def __init__(
        self,
        *,
        task_identifier: str,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        **kwargs,
    ) -> None:
        super().__init__(
            serialized_fields={"task_identifier": task_identifier},
            waiter_name="import_task_cancelled",
            waiter_args={"taskIdentifier": task_identifier},
            failure_message="Import task cancellation failed",
            status_message="Status of import task is",
            status_queries=["status"],
            return_key="import_task_id",
            return_value=task_identifier,
            waiter_delay=waiter_delay,
            waiter_max_attempts=waiter_max_attempts,
            **kwargs,
        )

    def hook(self) -> AwsGenericHook:
        return NeptuneAnalyticsHook(
            aws_conn_id=self.aws_conn_id,
            region_name=self.region_name,
            verify=self.verify,
            config=self.botocore_config,
        )
