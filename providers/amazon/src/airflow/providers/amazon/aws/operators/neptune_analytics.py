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

from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.hooks.neptune_analytics import NeptuneAnalyticsHook
from airflow.providers.amazon.aws.links.ec2 import VpcEndpointLink
from airflow.providers.amazon.aws.links.neptune_analytics import NeptuneGraphLink, NeptuneImportTaskLink
from airflow.providers.amazon.aws.operators.base_aws import AwsBaseOperator
from airflow.providers.amazon.aws.triggers.neptune_analytics import (
    NeptuneGraphAvailableTrigger,
    NeptuneGraphDeletedTrigger,
    NeptuneGraphPrivateEndpointAvailableTrigger,
    NeptuneGraphPrivateEndpointDeletedTrigger,
    NeptuneImportTaskCancelledTrigger,
    NeptuneImportTaskCompleteTrigger,
)
from airflow.providers.amazon.aws.utils import validate_execute_complete_event
from airflow.providers.amazon.aws.utils.mixins import aws_template_fields
from airflow.providers.common.compat.sdk import conf

if TYPE_CHECKING:
    from airflow.sdk import Context

from airflow.providers.amazon.aws.exceptions import (
    NeptuneGraphCreationFailedError,
    NeptuneGraphDeletionFailedError,
    NeptuneImportTaskCancellationFailedError,
    NeptuneImportTaskFailedError,
    NeptunePrivateEndpointCreationFailedError,
    NeptunePrivateEndpointDeletionFailedError,
)


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
    :param tags: Specifies metadata tags to add to the graph.
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
    template_fields: Sequence[str] = aws_template_fields(
        "graph_name", "vector_search_config", "provisioned_memory"
    )

    template_fields_renderers = {
        "vector_search_config": "json",
    }

    operator_extra_links = (NeptuneGraphLink(),)

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
        self.kms_key_id = kms_key_id
        self.tags = tags
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> dict:
        self.log.info("Creating graph %s", self.graph_name)

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
                    "kmsKeyIdentifier": self.kms_key_id,
                    "tags": self.tags,
                }.items()
                if v is not None
            },
        }

        response = self.hook.conn.create_graph(**create_params)

        self.log.info("Graph %s in status %s", self.graph_name, response.get("status", "Unknown"))
        self.graph_id = response.get("id", None)

        graph_url = NeptuneGraphLink.format_str.format(
            graph_id=self.graph_id,
            aws_domain=NeptuneGraphLink.get_aws_domain(self.hook.conn_partition),
            region_name=self.hook.conn_region_name,
        )

        NeptuneGraphLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            graph_id=self.graph_id,
        )
        self.log.info("You can view this Neptune Graph at : %s", graph_url)

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

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, Any]:

        validated_event = validate_execute_complete_event(event)

        if validated_event.get("status") != "success":
            raise NeptuneGraphCreationFailedError(
                validated_event.get(
                    "message",
                    f"Neptune graph {validated_event.get('graph_id')} creation did not complete successfully",
                )
            )

        self.log.info("Neptune graph %s complete", validated_event["graph_id"])

        return {"graph_id": validated_event["graph_id"]}


class NeptuneCreatePrivateGraphEndpointOperator(AwsBaseOperator[NeptuneAnalyticsHook]):
    """
    Creates a Neptune Graph private endpoint.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneCreatePrivateGraphEndpointOperator`

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
    operator_extra_links = (VpcEndpointLink(),)

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
        self.graph_identifier = graph_identifier
        self.vpc_id = vpc_id
        self.subnet_ids = subnet_ids
        self.vpc_security_group_ids = vpc_security_group_ids
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> dict:
        self.log.info("Creating private endpoint for graph %s", self.graph_identifier)

        create_params = {
            "graphIdentifier": self.graph_identifier,
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

        self.log.info("Status of endpoint: %s", status)

        if status in ["FAILED"]:
            raise NeptunePrivateEndpointCreationFailedError(
                f"Private endpoint failed to create for graph {self.graph_identifier}"
            )

        # if VPC not provided, use the one that is returned, which is the default VPC. Required for the waiter
        self.vpc_id = result.get("vpcId", self.vpc_id)

        # get the vpce id since it may not be returned immediately
        endpoint_id = self.hook._get_graph_endpoint_id(graph_id=self.graph_identifier, vpc_id=self.vpc_id)

        endpoint_url = VpcEndpointLink.format_str.format(
            endpoint_id=endpoint_id,
            aws_domain=VpcEndpointLink.get_aws_domain(self.hook.conn_partition),
            region_name=self.hook.conn_region_name,
        )

        VpcEndpointLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            endpoint_id=endpoint_id,
        )
        self.log.info("You can view this private endpoint at : %s", endpoint_url)

        if self.deferrable:
            self.log.info("Deferring until endpoint is available")
            self.defer(
                trigger=NeptuneGraphPrivateEndpointAvailableTrigger(
                    aws_conn_id=self.aws_conn_id,
                    graph_id=self.graph_identifier,
                    vpc_id=self.vpc_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
                kwargs={"vpc_id": self.vpc_id},
            )

        if self.wait_for_completion:
            self.log.info("Waiting until endpoint is available")
            self.hook.get_waiter("private_graph_endpoint_available").wait(
                graphIdentifier=self.graph_identifier,
                vpcId=self.vpc_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        return {"vpc_endpoint_id": endpoint_id, "graph_id": self.graph_identifier, "vpc_id": self.vpc_id}

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None, vpc_id: str = ""
    ) -> dict[str, Any]:
        validated_event = validate_execute_complete_event(event)

        if validated_event.get("status") != "success":
            raise NeptunePrivateEndpointCreationFailedError(
                validated_event.get("message", "Endpoint failed to create")
            )

        graph_id = validated_event["graph_id"]
        vpc_endpoint_id = self.hook._get_graph_endpoint_id(graph_id=graph_id, vpc_id=vpc_id)
        return {"vpc_endpoint_id": vpc_endpoint_id, "graph_id": graph_id, "vpc_id": vpc_id}


class NeptuneDeletePrivateGraphEndpointOperator(AwsBaseOperator[NeptuneAnalyticsHook]):
    """
    Deletes a Neptune Graph private endpoint.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneDeletePrivateGraphEndpointOperator`

    :param graph_identifier: Neptune Graph id
    :param vpc_id: VPC where endpoint resides
    :param wait_for_completion: Whether to wait for the endpoint to be deleted. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the endpoint to be deleted.
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
        self.graph_identifier = graph_identifier
        self.vpc_id = vpc_id
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> None:
        self.log.info("Deleting private endpoint for graph %s", self.graph_identifier)

        result = self.hook.conn.delete_private_graph_endpoint(
            graphIdentifier=self.graph_identifier, vpcId=self.vpc_id
        )

        status = result.get("status")
        endpoint_id = result.get("vpcEndpointId")

        if status == "FAILED":
            raise NeptunePrivateEndpointDeletionFailedError(
                f"Failed to delete private endpoint {endpoint_id}"
            )

        if self.deferrable:
            self.log.info("Deferring until endpoint %s is deleted", endpoint_id)
            self.defer(
                trigger=NeptuneGraphPrivateEndpointDeletedTrigger(
                    aws_conn_id=self.aws_conn_id,
                    graph_id=self.graph_identifier,
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
                graphIdentifier=self.graph_identifier,
                vpcId=self.vpc_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )
            self.log.info("Endpoint %s deleted", endpoint_id)

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> None:
        validated_event = validate_execute_complete_event(event)

        if validated_event.get("status") != "success":
            raise NeptunePrivateEndpointDeletionFailedError(
                validated_event.get("message", "Endpoint failed to delete.")
            )

        vpc_endpoint_id = validated_event.get("endpoint_id", "Unknown")
        self.log.info("Endpoint id %s deleted", vpc_endpoint_id)


class NeptuneDeleteGraphOperator(AwsBaseOperator[NeptuneAnalyticsHook]):
    """
    Deletes an Amazon Neptune Graph database.

    Neptune Analytics is a memory-optimized graph database engine for analytics. With Neptune Analytics, you can get insights and find trends by processing large amounts of graph data in seconds.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneDeleteGraphOperator`

    :param graph_id: Name of Neptune graph to delete
    :param skip_snapshot: Determines whether a final graph snapshot is created before the graph is deleted. If true is specified, no graph snapshot is created. If false is specified, a graph snapshot is created before the graph is deleted.
    :param wait_for_completion: Whether to wait for the graph to delete. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the graph to be deleted.
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
    template_fields: Sequence[str] = aws_template_fields("graph_id", "skip_snapshot")

    def __init__(
        self,
        graph_id: str,
        skip_snapshot: bool,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.graph_id = graph_id
        self.skip_snapshot = skip_snapshot
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context):
        self.log.info("Deleting graph %s", self.graph_id)

        try:
            self.hook.conn.delete_graph(graphIdentifier=self.graph_id, skipSnapshot=self.skip_snapshot)
        except ClientError as e:
            # if not found, just exit because there is nothing to delete
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                self.log.info("Graph %s not found. Nothing to delete", self.graph_id)
                return
            raise NeptuneGraphDeletionFailedError(e.response["Error"])

        if self.deferrable:
            self.log.info("Deferring until graph %s is deleted", self.graph_id)
            self.defer(
                trigger=NeptuneGraphDeletedTrigger(
                    aws_conn_id=self.aws_conn_id,
                    graph_id=self.graph_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="execute_complete",
            )
        if self.wait_for_completion:
            self.log.info("Waiting to delete %s", self.graph_id)

            self.hook.get_waiter("graph_deleted").wait(
                graphIdentifier=self.graph_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None):
        validated_event = validate_execute_complete_event(event)
        graph_id = validated_event.get("graph_id", "")
        if validated_event.get("status") != "success":
            raise NeptuneGraphDeletionFailedError(
                validated_event.get("message", f"Neptune graph {graph_id} deletion failed")
            )

        self.log.info("Neptune graph %s deleted", graph_id)


class NeptuneCreateGraphWithImportOperator(AwsBaseOperator[NeptuneAnalyticsHook]):
    """
    Creates a Neptune Graph and imports data into it.

    Neptune Analytics is a memory-optimized graph database engine for analytics. With Neptune Analytics,
    you can get insights and find trends by processing large amounts of graph data in seconds.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneCreateGraphWithImportOperator`

    :param graph_name: Name of Neptune graph to create
    :param vector_search_config: Specifies the number of dimensions for vector embeddings that will be loaded into the graph.
    :param source: The source from which to import data. Can be an S3 URI or Neptune database snapshot.
    :param role_arn: The ARN of the IAM role that Neptune Analytics can assume to access the data source.
    :param blank_node_handling: The method to handle blank nodes in the dataset. Options include 'convertToIri' or other handling strategies.
    :param parquet_type: The type of Parquet files in the data source (if applicable).
    :param format: The format of the data to be imported (e.g., 'csv', 'opencypher', 'ntriples', 'nquads', 'rdfxml', 'turtle').
    :param min_provisioned_memory: The minimum provisioned memory for the graph in GBs.
    :param max_provisioned_memory: The maximum provisioned memory for the graph in GBs.
    :param fail_on_error: If True, the import will fail if any errors are encountered. If False, the import will continue despite errors.
    :param public_connectivity: Specifies whether or not the graph can be reachable over the internet.
    :param replica_count: The number of replicas in other AZs.
    :param deletion_protection: Indicates whether or not to enable deletion protection on the graph.
        The graph can't be deleted when deletion protection is enabled. (default: False)
    :param kms_key_id: Specifies a KMS key to use to encrypt data in the new graph.
    :param tags: Specifies metadata tags to add to the graph.
    :param import_options: Contains options for controlling the import process.
    :param wait_for_completion: Whether to wait for the graph to be created and data imported. (default: True)
    :param waiter_delay: Time in seconds to wait between status checks. (default: 30)
    :param waiter_max_attempts: Maximum number of attempts to check for job completion. (default: 60)
    :param deferrable: If True, the operator will wait asynchronously for the graph to be created and data imported.
        This implies waiting for completion. This mode requires aiobotocore module to be installed.
        (default: False)
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
        "graph_name", "vector_search_config", "source", "role_arn"
    )

    template_fields_renderers = {
        "vector_search_config": "json",
    }

    operator_extra_links = (
        NeptuneImportTaskLink(),
        NeptuneGraphLink(),
    )

    def __init__(
        self,
        graph_name: str,
        vector_search_config: dict,
        source: str,
        role_arn: str,
        blank_node_handling: str | None = None,
        parquet_type: str | None = None,
        format: str | None = None,
        min_provisioned_memory: int | None = None,
        max_provisioned_memory: int | None = None,
        fail_on_error: bool | None = None,
        public_connectivity: bool | None = None,
        replica_count: int | None = None,
        deletion_protection: bool | None = None,
        kms_key_id: str | None = None,
        tags: dict | None = None,
        import_options: dict | None = None,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.graph_name = graph_name
        self.vector_search_config = vector_search_config
        self.source = source
        self.role_arn = role_arn
        self.blank_node_handling = blank_node_handling
        self.parquet_type = parquet_type
        self.format = format
        self.min_provisioned_memory = min_provisioned_memory
        self.max_provisioned_memory = max_provisioned_memory
        self.fail_on_error = fail_on_error
        self.public_connectivity = public_connectivity
        self.replica_count = replica_count
        self.deletion_protect = deletion_protection
        self.kms_key_id = kms_key_id
        self.tags = tags
        self.import_options = import_options
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts

    def execute(self, context: Context) -> dict:
        self.log.info("Creating graph %s with import", self.graph_name)

        # Build the import options
        import_options = {
            "neptune-analytics:blank-node-handling": self.blank_node_handling,
            "neptune-analytics:parquet-type": self.parquet_type,
        }

        # Remove None values from import_options
        import_options = {k: v for k, v in import_options.items() if v is not None}

        # Merge with user-provided import_options
        if self.import_options:
            import_options.update(self.import_options)

        create_params = {
            "graphName": self.graph_name,
            "vectorSearchConfiguration": self.vector_search_config,
            "source": self.source,
            "roleArn": self.role_arn,
            **{
                k: v
                for k, v in {
                    "format": self.format,
                    "minProvisionedMemory": self.min_provisioned_memory,
                    "maxProvisionedMemory": self.max_provisioned_memory,
                    "failOnError": self.fail_on_error,
                    "replicaCount": self.replica_count,
                    "publicConnectivity": self.public_connectivity,
                    "deletionProtection": self.deletion_protect,
                    "kmsKeyIdentifier": self.kms_key_id,
                    "tags": self.tags,
                    "importOptions": import_options if import_options else None,
                }.items()
                if v is not None
            },
        }

        response = self.hook.conn.create_graph_using_import_task(**create_params)

        self.log.info("Graph %s import task in status %s", self.graph_name, response.get("status", "Unknown"))
        self.graph_id = response.get("graphId", None)
        import_task_id = response.get("taskId")

        graph_url = NeptuneGraphLink.format_str.format(
            graph_id=self.graph_id,
            aws_domain=NeptuneGraphLink.get_aws_domain(self.hook.conn_partition),
            region_name=self.hook.conn_region_name,
        )

        NeptuneGraphLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            graph_id=self.graph_id,
        )

        import_task_url = NeptuneImportTaskLink.format_str.format(
            import_task_id=import_task_id,
            aws_domain=NeptuneImportTaskLink.get_aws_domain(self.hook.conn_partition),
            region_name=self.hook.conn_region_name,
        )

        NeptuneImportTaskLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            import_task_id=import_task_id,
        )

        self.log.info("You can view this import task at : %s", import_task_url)

        self.log.info("You can view this Neptune Graph at : %s", graph_url)

        if self.deferrable:
            self.log.info("Deferring until graph %s is available", self.graph_id)
            self.defer(
                trigger=NeptuneGraphAvailableTrigger(
                    aws_conn_id=self.aws_conn_id,
                    graph_id=self.graph_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                ),
                method_name="defer_wait_for_task",
                kwargs={"import_task_id": import_task_id},
            )

        if self.wait_for_completion:
            self.log.info("Waiting until graph %s is available", self.graph_id)
            self.hook.get_waiter("graph_available").wait(
                graphIdentifier=self.graph_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )
            # Once the graph is available, wait for the task to complete

            self.log.info("Waiting for import task %s", import_task_id)
            self.hook.get_waiter("import_task_successful").wait(
                taskIdentifier=import_task_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        return {"graph_id": self.graph_id}

    def defer_wait_for_task(
        self, context: Context, event: dict[str, Any] | None = None, import_task_id: str | None = None
    ) -> None:
        """Defers for import task completion."""
        validated_event = validate_execute_complete_event(event)
        graph_id = validated_event.get("graph_id")

        if validated_event.get("status") != "success":
            raise NeptuneGraphCreationFailedError(
                validated_event.get("message", f"Neptune graph {graph_id} did not become available")
            )

        if import_task_id:
            self.log.info("Deferring for import task %s completion", import_task_id)
            self.defer(
                trigger=NeptuneImportTaskCompleteTrigger(
                    import_task_id=import_task_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
                kwargs={"graph_id": graph_id},
            )

    def execute_complete(
        self, context: Context, event: dict[str, Any] | None = None, graph_id: str | None = None
    ) -> dict[str, Any]:
        validated_event = validate_execute_complete_event(event)

        if validated_event.get("status") != "success":
            raise NeptuneGraphCreationFailedError(
                validated_event.get(
                    "message", f"Neptune graph {graph_id} import did not complete successfully"
                )
            )

        self.log.info("Import complete for graph %s", graph_id)
        return {"graph_id": graph_id}


class NeptuneStartImportTaskOperator(AwsBaseOperator[NeptuneAnalyticsHook]):
    """
    Starts a bulk data import task to load data into an empty Neptune graph.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneStartImportTaskOperator`

    :param graph_identifier: Graph Id of target Neptune Graph
    :param role_arn: IAM role ARN granting access to source data
    :param source: URL identifying the source data location.
    :param blank_node_handling: Method to handle blank nodes in dataset.
    :param fail_on_error: If set to true, the task halts when an import error is encountered. If set to false, the task skips the data that caused the error and continues if possible.
    :param format: Specifies the format of the Amazon S3 data to be imported.
    :param import_options: Options on how to perform an import
    :param parquet_type: Parquet type of import task
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
        "graph_identifier", "role_arn", "source", "import_options"
    )
    template_fields_renderers = {
        "import_options": "json",
    }
    operator_extra_links = (NeptuneImportTaskLink(),)

    def __init__(
        self,
        graph_identifier: str,
        role_arn: str,
        source: str,
        blank_node_handling: str | None = None,
        fail_on_error: bool = True,
        format: str | None = None,
        import_options: dict | None = None,
        parquet_type: str | None = "COLUMNAR",
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.graph_identifier = graph_identifier
        self.role_arn = role_arn
        self.source = source
        self.blank_node_handling = blank_node_handling
        self.fail_on_error = fail_on_error
        self.format = format
        self.import_options = import_options
        self.parquet_type = parquet_type
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context) -> dict:
        self.log.info("Starting data import to graph %s", self.graph_identifier)

        create_params = {
            "graphIdentifier": self.graph_identifier,
            "roleArn": self.role_arn,
            "source": self.source,
            **{
                k: v
                for k, v in {
                    "blankNodeHandling": self.blank_node_handling,
                    "failOnError": self.fail_on_error,
                    "format": self.format,
                    "importOptions": self.import_options,
                    "parquetType": self.parquet_type,
                }.items()
                if v is not None
            },
        }

        response = self.hook.conn.start_import_task(**create_params)
        import_task_id = response.get("taskId")
        self.log.info("Import task %s started for graph %s", import_task_id, self.graph_identifier)

        # Create the console link
        import_task_url = NeptuneImportTaskLink.format_str.format(
            import_task_id=import_task_id,
            aws_domain=NeptuneImportTaskLink.get_aws_domain(self.hook.conn_partition),
            region_name=self.hook.conn_region_name,
        )

        NeptuneImportTaskLink.persist(
            context=context,
            operator=self,
            region_name=self.hook.conn_region_name,
            aws_partition=self.hook.conn_partition,
            import_task_id=import_task_id,
        )
        self.log.info("You can view this import task at : %s", import_task_url)

        if self.deferrable:
            self.log.info("Deferring until import task %s completes", import_task_id)
            self.defer(
                trigger=NeptuneImportTaskCompleteTrigger(
                    import_task_id=import_task_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )

        if self.wait_for_completion:
            self.log.info("Waiting for import task %s to complete", import_task_id)
            self.hook.get_waiter("import_task_successful").wait(
                taskIdentifier=import_task_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        return {"import_task_id": import_task_id, "graph_id": self.graph_identifier}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, Any]:
        validated_event = validate_execute_complete_event(event)

        if validated_event.get("status") != "success":
            raise NeptuneImportTaskFailedError(
                validated_event.get("message", "Import task did not complete successfully")
            )

        task_id = validated_event.get("import_task_id", "")
        self.log.info("Import task %s completed", task_id)
        return {"graph_id": self.graph_identifier, "import_task_id": task_id}


class NeptuneCancelImportTaskOperator(AwsBaseOperator[NeptuneAnalyticsHook]):
    """
    Cancels an active Neptune Graph import task.

    .. seealso::
        For more information on how to use this operator, take a look at the guide:
        :ref:`howto/operator:NeptuneCancelImportTaskOperator`

    :param import_task_id: Neptune Graph import task id to cancel.
    :param wait_for_completion: Whether to wait for the task to be cancelled. If the task is already
        in a completed state, the operator will end successfully. (default: True)
    :param deferrable: If True, the operator will wait asynchronously for the task to be cancelled.
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
    template_fields: Sequence[str] = aws_template_fields("import_task_id")

    def __init__(
        self,
        import_task_id: str,
        wait_for_completion: bool = True,
        waiter_delay: int = 30,
        waiter_max_attempts: int = 60,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.import_task_id = import_task_id
        self.wait_for_completion = wait_for_completion
        self.waiter_delay = waiter_delay
        self.waiter_max_attempts = waiter_max_attempts
        self.deferrable = deferrable

    def execute(self, context: Context) -> dict:
        self.log.info("Cancelling import task %s", self.import_task_id)

        response = self.hook.conn.cancel_import_task(taskIdentifier=self.import_task_id)

        self.log.info("Import task %s status is %s", self.import_task_id, response.get("status", "Unknown"))

        if self.deferrable:
            self.log.info("Deferring until import task %s is cancelled", self.import_task_id)
            self.defer(
                trigger=NeptuneImportTaskCancelledTrigger(
                    task_identifier=self.import_task_id,
                    waiter_delay=self.waiter_delay,
                    waiter_max_attempts=self.waiter_max_attempts,
                    aws_conn_id=self.aws_conn_id,
                ),
                method_name="execute_complete",
            )

        if self.wait_for_completion:
            self.log.info("Waiting for import task %s to be cancelled", self.import_task_id)
            self.hook.get_waiter("import_task_cancelled").wait(
                taskIdentifier=self.import_task_id,
                WaiterConfig={"Delay": self.waiter_delay, "MaxAttempts": self.waiter_max_attempts},
            )

        return {"import_task_id": self.import_task_id}

    def execute_complete(self, context: Context, event: dict[str, Any] | None = None) -> dict[str, Any]:
        validated_event = validate_execute_complete_event(event)

        if validated_event.get("status") != "success":
            raise NeptuneImportTaskCancellationFailedError(
                validated_event.get("message", "Error while waiting for Neptune import task cancellation")
            )

        task_id = validated_event.get("import_task_id", "")
        self.log.info("Import task %s cancelled", task_id)
        return {"import_task_id": task_id}
