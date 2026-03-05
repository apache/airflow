#
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
from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.neptune_analytics import NeptuneAnalyticsHook
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.neptune_analytics import (
    NeptuneGraphAvailableTrigger,
    NeptuneGraphPrivateEndpointAvailableTrigger,
    NeptuneGraphPrivateEndpointDeletedTrigger,
)
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from airflow.sdk import Context


class NeptuneCreateGraphOperator(AwsBaseOperator[NeptuneAnalyticsHook]):
    """
    Creates an empty Amazon Neptune Graph database.

    Neptune Analytics is a memory-optimized graph database engine for analytics. With Neptune Analytics, you can get insights and find trends by processing large amounts of graph data in seconds.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneCreateGraphOperator`

    :param graph_name: Name of Neptune graph to create
    :param vector_search_config: Specifies the number of dimensions for vector embeddings that will be loaded into the graph.
    :param provisioned_memory: The provisioned memory-optimized Neptune Capacity Units (m-NCUs) to use for the graph.
    :param public_connectivity: Specifies whether or not the graph can be reachable over the internet.
    :param replica_count: The number of replicas in other AZs.
    :param deletion_protection:  Indicates whether or not to enable deletion protection on the graph.
        The graph can't be deleted when deletion protection is enabled.
    :param kms_key_id:  Specifies a KMS key to use to encrypt data in the new graph.
    :param tags Specifies metadata tags to add to the graph.
    :param wait_for_completion: Whether to wait for the graph to start. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the graph to start.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param waiter_delay: Time in seconds to wait between status checks.
    :param waiter_max_attempts: Maximum number of attempts to check for job completion.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.

    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    :return: dictionary with Neptune graph id
    """

    aws_hook_class = NeptuneAnalyticsHook
    template_fields: Sequence[str] = aws_template_fields()

    def __init__(
        self,
        graph_name: str,
        vector_search_config: dict,
        provisioned_memory: int,
        public_connectivity: bool | None = None,
        replica_count: int | None = None,
        deletion_protection: bool = False,
        kms_key_id: str | None = None,
        tags: dict | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.graph_name = graph_name
        self.vector_search_config = vector_search_config
        self.replica_count = replica_count
        self.provisioned_memory = provisioned_memory
        self.public_connectivity = public_connectivity
        self.deletion_protect = deletion_protection
        self.kms_key = kms_key_id
        self.tags = tags
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> dict:
        self.log.info("Creating graph %s", self.graph_name)

        # TODO perform check
        create_params = {
            "graphName": self.graph_name,
            "vectorSearchConfiguration": self.vector_search_config,
            "provisionedMemory": self.provisioned_memory,
            **{
                k: v
                for k, v in {
                    "replicaCount": self.replica_count,
                    "publicConnectivity": self.public_connectivity,
                    "deletionProtection": self.deletion_protect,
                    "kmsKeyIdentifier": self.kms_key,
                    "tags": self.tags,
                }.items()
                if v is not None
            },
        }

        response = self.hook.conn.create_graph(**create_params)

        self.log.info("Graph %s in status %s", self.graph_name, response.get("status", "Unknown"))
        self.graph_id = response.get("id", None)

        # TODO build extra link to console

        if self.deferrable:
            self.log.info("Deferring until graph %s is available", self.graph_id)
            self.defer(
                trigger=NeptuneGraphAvailableTrigger(
                    aws_conn_id=self.aws_conn_id,
                    graph_id=self.graph_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
            )

        if self.wait_for_completion:
            self.log.info("Waiting until graph %s is available", self.graph_id)
            self.hook.get_waiter("graph_available").wait(
                graphIdentifier=self.graph_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        return {"graph_id": self.graph_id}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, str]:
        graph_id = ""

        if event:
            graph_id = event.get("graph_id", "Unknown")

            self.log.info("Neptune graph % complete", graph_id)

        return {"graph_id": graph_id}


class NeptuneCreatePrivateGraphEndpointOperator(AwsBaseOperator[NeptuneAnalyticsHook]):
    """
    Creates a Neptune Graph private endpoint.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneCreateGraphOperator`

    :param graph_identifier: Neptune Graph id
    :param vpc_id: VPC to create endpoint in
    :param subnet_ids: Subnets in which private graph endpoint ENIs are created
    :param vpc_security_group_ids: Security groups to be attached to the private graph endpoint

    :param wait_for_completion: Whether to wait for the endpoint to be available. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the endpoint to become available.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param waiter_delay: Time in seconds to wait between status checks.
    :param waiter_max_attempts: Maximum number of attempts to check for job completion.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.

    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    :return: dictionary with Neptune graph id
    """

    aws_hook_class = NeptuneAnalyticsHook
    template_fields: Sequence[str] = aws_template_fields(
        "graph_identifier", "vpc_id", "subnet_ids", "vpc_security_group_ids"
    )

    def __init__(
        self,
        graph_identifier: str,
        vpc_id: str | None = None,
        subnet_ids: list[str] | None = None,
        vpc_security_group_ids: list[str] | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.graph_id = graph_identifier
        self.vpc_id = vpc_id
        self.subnet_ids = subnet_ids
        self.vpc_security_group_ids = vpc_security_group_ids
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> dict:
        self.log.info("Creating private endpoint for graph %s", self.graph_id)

        create_params = {
            "graphIdentifier": self.graph_id,
            **{
                k: v
                for k, v in {
                    "vpcId": self.vpc_id,
                    "subnetIds": self.subnet_ids,
                    "vpcSecurityGroupIds": self.vpc_security_group_ids,
                }.items()
                if v is not None
            },
        }

        # create the endpoint

        result = self.hook.conn.create_private_graph_endpoint(**create_params)
        status = result.get("status", "Unknown")
        endpoint_id = result.get("vpcEndpointId", "Unknown")

        self.log.info("Status of endpoint %s: %s", endpoint_id, status)

        if status in ["FAILED"]:
            raise AirflowException(f"Private endpoint failed to create for graph {self.graph_id}")

        # if VPC not provided, use the one that is returned. Required for the waiter
        self.vpc_id = result.get("vpcId", self.vpc_id)

        # TODO extra link to console

        if self.deferrable:
            self.log.info("Deferring until endpoint %s is available", endpoint_id)
            self.defer(
                trigger=NeptuneGraphPrivateEndpointAvailableTrigger(
                    aws_conn_id=self.aws_conn_id,
                    graph_id=self.graph_id,
                    vpc_id=self.vpc_id,
                    endpoint_id=endpoint_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
            )

        # TODO add test
        if self.wait_for_completion:
            self.log.info("Waiting until endpoint %s is available", endpoint_id)
            self.hook.get_waiter("private_graph_endpoint_available").wait(
                graphIdentifier=self.graph_id,
                vpcId=self.vpc_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        return {"vpc_endpoint_id": endpoint_id, "graph_id": self.graph_id, "vpc_id": self.vpc_id}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, str]:
        vpc_endpoint_id = ""

        if event and event.get("status") == "success":
            vpc_endpoint_id = event.get("endpoint_id", "")

        return {"vpc_endpoint_id": vpc_endpoint_id, "graph_id": self.graph_id, "vpc_id": self.vpc_id}


class NeptuneDeletePrivateGraphEndpointOperator(AwsBaseOperator[NeptuneAnalyticsHook]):
    """
    Deletes a Neptune Graph private endpoint.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneDeletePrivateGraphEndpointOperator`

    :param graph_identifier: Neptune Graph id
    :param vpc_id: VPC to create endpoint in
    :param subnet_ids: Subnets in which private graph endpoint ENIs are created
    :param vpc_security_group_ids: Security groups to be attached to the private graph endpoint

    :param wait_for_completion: Whether to wait for the endpoint to be available. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the endpoint to become available.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
    :param waiter_delay: Time in seconds to wait between status checks.
    :param waiter_max_attempts: Maximum number of attempts to check for job completion.
    :param aws_conn_id: The Airflow connection used for AWS credentials.
        If this is ``None`` or empty then the default boto3 behaviour is used. If
        running Airflow in a distributed manner and aws_conn_id is None or
        empty, then default boto3 configuration would be used (and must be
        maintained on each worker node).
    :param region_name: AWS region_name. If not specified then the default boto3 behaviour is used.

    :param botocore_config: Configuration dictionary (key-values) for botocore client. See:
        https://botocore.amazonaws.com/v1/documentation/api/latest/reference/config.html
    :return: dictionary with Neptune graph id
    """

    aws_hook_class = NeptuneAnalyticsHook
    template_fields: Sequence[str] = aws_template_fields("graph_identifier", "vpc_id")

    def __init__(
        self,
        graph_identifier: str,
        vpc_id: str,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.graph_id = graph_identifier
        self.vpc_id = vpc_id
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> None:
        self.log.info("Deleting private endpoint for graph %s", self.graph_id)

        result = self.hook.conn.delete_private_graph_endpoint(
            graphIdentifier=self.graph_id, vpcId=self.vpc_id
        )

        status = result.get("status")
        endpoint_id = result.get("vpcEndpointId")

        if status == "FAILED":
            raise AirflowException(f"Failed to delete private endpoint {endpoint_id}")

        if self.deferrable:
            self.log.info("Deferring until endpoint %s is deleted", endpoint_id)
            self.defer(
                trigger=NeptuneGraphPrivateEndpointDeletedTrigger(
                    aws_conn_id=self.aws_conn_id,
                    graph_id=self.graph_id,
                    vpc_id=self.vpc_id,
                    endpoint_id=endpoint_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
            )
        if self.wait_for_completion:
            self.log.info("Waiting until endpoint %s is deleted", endpoint_id)
            self.hook.get_waiter("private_graph_endpoint_deleted").wait(
                graphIdentifier=self.graph_id,
                vpcId=self.vpc_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        vpc_endpoint_id = ""

        if event and event.get("status") == "success":
            vpc_endpoint_id = event.get("endpoint_id")

            self.log.info("Endpoint id %s deleted", vpc_endpoint_id)
