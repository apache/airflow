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

from unittest import mock

import pytest
from botocore.exceptions import ClientError

from airflow.providers.amazon.aws.exceptions import (
    NeptuneGraphDeletionFailedError,
    NeptunePrivateEndpointCreationFailedError,
    NeptunePrivateEndpointDeletionFailedError,
)
from airflow.providers.amazon.aws.hooks.neptune_analytics import NeptuneAnalyticsHook
from airflow.providers.amazon.aws.links.neptune_analytics import NeptuneGraphLink, NeptuneImportTaskLink
from airflow.providers.amazon.aws.operators.neptune_analytics import (
    NeptuneCancelImportTaskOperator,
    NeptuneCreateGraphOperator,
    NeptuneCreateGraphWithImportOperator,
    NeptuneCreatePrivateGraphEndpointOperator,
    NeptuneDeleteGraphOperator,
    NeptuneDeletePrivateGraphEndpointOperator,
    NeptuneStartImportTaskOperator,
)
from airflow.providers.amazon.aws.triggers.neptune_analytics import (
    NeptuneGraphAvailableTrigger,
    NeptuneImportTaskCompleteTrigger,
)
from airflow.providers.common.compat.sdk import TaskDeferred

GRAPH_NAME = "test_graph"
GRAPH_ID = "test-graph-id"
VPC_ID = "vpc-12345"
SUBNET_IDS = ["subnet-1", "subnet-2"]
SECURITY_GROUP_IDS = ["sg-1", "sg-2"]
ENDPOINT_ID = "vpce-12345"
SOURCE_S3_URI = "s3://my-bucket/my-data/"
ROLE_ARN = "arn:aws:iam::123456789012:role/NeptuneImportRole"


class TestNeptuneCreateGraphOperator:
    def test_template_fields(self):
        # Verify template_fields includes the expected fields
        fields = NeptuneCreateGraphOperator.template_fields
        assert "graph_name" in fields
        assert "vector_search_config" in fields
        assert "provisioned_memory" in fields

    def test_template_fields_renderers(self):
        assert NeptuneCreateGraphOperator.template_fields_renderers == {"vector_search_config": "json"}

    def test_operator_extra_links(self):

        assert len(NeptuneCreateGraphOperator.operator_extra_links) == 1
        assert isinstance(NeptuneCreateGraphOperator.operator_extra_links[0], NeptuneGraphLink)

    @mock.patch("airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneGraphLink.persist")
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_defaults(self, mock_conn, mock_persist):
        mock_conn.create_graph.return_value = {"id": GRAPH_ID, "status": "CREATING"}

        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"test": 123},
            provisioned_memory=16,
        )

        assert operator.public_connectivity is None
        assert operator.replica_count is None
        assert operator.deletion_protect is False
        assert operator.kms_key_id is None
        assert operator.tags is None

        operator.execute(None)

        mock_conn.create_graph.assert_called_once_with(
            graphName=GRAPH_NAME,
            vectorSearchConfiguration={"test": 123},
            provisionedMemory=16,
            deletionProtection=False,
        )

    @mock.patch("airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneGraphLink.persist")
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_custom_args(self, mock_conn, mock_persist):
        mock_conn.create_graph.return_value = {"id": GRAPH_ID, "status": "CREATING"}

        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"test": 123},
            provisioned_memory=16,
            public_connectivity=True,
            replica_count=3,
            kms_key_id="test-key",
            tags={"key1": "test"},
            deletion_protection=True,
        )

        assert operator.public_connectivity is True
        assert operator.replica_count == 3
        assert operator.deletion_protect is True
        assert operator.kms_key_id == "test-key"
        assert operator.tags == {"key1": "test"}

        operator.execute(None)

        mock_conn.create_graph.assert_called_once_with(
            graphName=GRAPH_NAME,
            vectorSearchConfiguration={"test": 123},
            replicaCount=3,
            publicConnectivity=True,
            provisionedMemory=16,
            deletionProtection=True,
            kmsKeyIdentifier="test-key",
            tags={"key1": "test"},
        )

    @mock.patch("airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneGraphLink.persist")
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_create_graph(self, mock_hook_get_waiter, mock_conn, mock_persist):
        mock_conn.create_graph.return_value = {"id": GRAPH_ID, "status": "CREATING"}

        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            provisioned_memory=16,
            vector_search_config={"test": 123},
            wait_for_completion=False,
        )
        resp = operator.execute(None)

        mock_hook_get_waiter.assert_not_called()
        assert "graph_id" in resp
        assert resp["graph_id"] == GRAPH_ID

    @mock.patch("airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneGraphLink.persist")
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_create_graph_wait_for_completion(self, mock_hook_get_waiter, mock_conn, mock_persist):
        mock_conn.create_graph.return_value = {"id": GRAPH_ID, "status": "CREATING"}

        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            provisioned_memory=16,
            vector_search_config={"test": 123},
            wait_for_completion=True,
        )
        resp = operator.execute(None)

        mock_hook_get_waiter.assert_called_once_with("graph_available")
        assert "graph_id" in resp
        assert resp["graph_id"] == GRAPH_ID

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_persist_called_with_correct_args(self, mock_conn):
        """Test that NeptuneGraphLink.persist is called with the correct arguments."""
        mock_conn.create_graph.return_value = {"id": GRAPH_ID, "status": "CREATING"}

        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"test": 123},
            provisioned_memory=16,
            wait_for_completion=False,
        )

        mock_context = mock.MagicMock()
        with mock.patch(
            "airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneGraphLink.persist"
        ) as mock_persist:
            operator.execute(mock_context)

            mock_persist.assert_called_once_with(
                context=mock_context,
                operator=operator,
                region_name=mock.ANY,
                aws_partition=mock.ANY,
                graph_id=GRAPH_ID,
            )

    @mock.patch("airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneGraphLink.persist")
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_deferrable_defers_with_graph_available_trigger(self, mock_conn, mock_persist):
        """Test that deferrable mode defers with NeptuneGraphAvailableTrigger."""
        mock_conn.create_graph.return_value = {"id": GRAPH_ID, "status": "CREATING"}

        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"test": 123},
            provisioned_memory=16,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            operator.execute(None)

        trigger = exc_info.value.trigger
        assert isinstance(trigger, NeptuneGraphAvailableTrigger)
        assert exc_info.value.method_name == "execute_complete"


class TestNeptuneCreatePrivateGraphEndpointOperator:
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_defaults(self, mock_conn):
        mock_conn.create_private_graph_endpoint.return_value = {
            "status": "CREATING",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }
        mock_conn.get_private_graph_endpoint.return_value = {
            "vpcEndpointId": ENDPOINT_ID,
        }

        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
        )

        assert operator.graph_identifier == GRAPH_ID
        assert operator.vpc_id is None
        assert operator.subnet_ids is None
        assert operator.vpc_security_group_ids is None
        assert operator.wait_for_completion is True
        assert operator.waiter_delay == 30
        assert operator.waiter_max_attempts == 60

        result = operator.execute(None)

        mock_conn.create_private_graph_endpoint.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
        )
        mock_conn.get_private_graph_endpoint.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            vpcId=VPC_ID,
        )

        assert result is not None
        assert result["vpc_endpoint_id"] == ENDPOINT_ID
        assert result["graph_id"] == GRAPH_ID
        assert result["vpc_id"] == VPC_ID

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_custom_args(self, mock_conn):
        mock_conn.create_private_graph_endpoint.return_value = {
            "status": "CREATING",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }
        mock_conn.get_private_graph_endpoint.return_value = {
            "vpcEndpointId": ENDPOINT_ID,
        }

        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
            subnet_ids=SUBNET_IDS,
            vpc_security_group_ids=SECURITY_GROUP_IDS,
            waiter_delay=60,
            waiter_max_attempts=100,
        )

        assert operator.graph_identifier == GRAPH_ID
        assert operator.vpc_id == VPC_ID
        assert operator.subnet_ids == SUBNET_IDS
        assert operator.vpc_security_group_ids == SECURITY_GROUP_IDS
        assert operator.waiter_delay == 60
        assert operator.waiter_max_attempts == 100

        operator.execute(None)

        mock_conn.create_private_graph_endpoint.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            vpcId=VPC_ID,
            subnetIds=SUBNET_IDS,
            vpcSecurityGroupIds=SECURITY_GROUP_IDS,
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_create_endpoint_no_wait(self, mock_hook_get_waiter, mock_conn):
        mock_conn.create_private_graph_endpoint.return_value = {
            "status": "CREATING",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }
        mock_conn.get_private_graph_endpoint.return_value = {
            "vpcEndpointId": ENDPOINT_ID,
        }

        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
            wait_for_completion=False,
        )
        operator.execute(None)

        mock_hook_get_waiter.assert_not_called()

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_create_endpoint_wait_for_completion(self, mock_hook_get_waiter, mock_conn):
        mock_conn.create_private_graph_endpoint.return_value = {
            "status": "CREATING",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }
        mock_conn.get_private_graph_endpoint.return_value = {
            "vpcEndpointId": ENDPOINT_ID,
        }

        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
            wait_for_completion=True,
        )
        result = operator.execute(None)

        mock_hook_get_waiter.assert_called_once_with("private_graph_endpoint_available")
        mock_hook_get_waiter.return_value.wait.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            vpcId=VPC_ID,
            WaiterConfig={"Delay": 30, "MaxAttempts": 60},
        )
        assert result == {"vpc_endpoint_id": ENDPOINT_ID, "graph_id": GRAPH_ID, "vpc_id": VPC_ID}

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_create_endpoint_sets_vpc_id_from_response(self, mock_conn):
        """When vpc_id is not provided, the operator should use the vpc_id from the API response."""
        mock_conn.create_private_graph_endpoint.return_value = {
            "status": "CREATING",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }
        mock_conn.get_private_graph_endpoint.return_value = {
            "vpcEndpointId": ENDPOINT_ID,
        }

        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
        )

        assert operator.vpc_id is None
        result = operator.execute(None)

        # vpc_id should be set from the create response
        assert operator.vpc_id == VPC_ID
        assert result["vpc_id"] == VPC_ID

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_create_endpoint_failed_status(self, mock_conn):

        mock_conn.create_private_graph_endpoint.return_value = {
            "status": "FAILED",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }

        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
        )

        with pytest.raises(
            NeptunePrivateEndpointCreationFailedError,
            match=f"Private endpoint failed to create for graph {GRAPH_ID}",
        ):
            operator.execute(None)

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "_get_graph_endpoint_id")
    def test_execute_complete(self, mock_get_endpoint, mock_conn):
        mock_get_endpoint.return_value = ENDPOINT_ID
        mock_conn.get_private_graph_endpoint.return_value = {
            "vpcEndpointId": ENDPOINT_ID,
        }

        mock_conn.create_private_graph_endpoint.return_value = {"vpcId": VPC_ID}

        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task", graph_identifier=GRAPH_ID, vpc_id=VPC_ID
        )

        result = operator.execute_complete(
            context=None, event={"status": "success", "graph_id": GRAPH_ID}, vpc_id=VPC_ID
        )

        # mock_conn.get_private_graph_endpoint.assert_called_once_with(
        mock_get_endpoint.assert_called_once_with(
            graph_id=GRAPH_ID,
            vpc_id=VPC_ID,
        )
        assert result == {"vpc_endpoint_id": ENDPOINT_ID, "graph_id": GRAPH_ID, "vpc_id": VPC_ID}


class TestNeptuneDeletePrivateGraphEndpointOperator:
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_defaults(self, mock_conn):
        mock_conn.delete_private_graph_endpoint.return_value = {
            "status": "DELETING",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }

        operator = NeptuneDeletePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
        )

        assert operator.graph_identifier == GRAPH_ID
        assert operator.vpc_id == VPC_ID
        assert operator.wait_for_completion is True
        assert operator.waiter_delay == 30
        assert operator.waiter_max_attempts == 60

        operator.execute(None)

        mock_conn.delete_private_graph_endpoint.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            vpcId=VPC_ID,
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_custom_args(self, mock_conn):
        mock_conn.delete_private_graph_endpoint.return_value = {
            "status": "DELETING",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }

        operator = NeptuneDeletePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
            waiter_delay=60,
            waiter_max_attempts=100,
        )

        assert operator.graph_identifier == GRAPH_ID
        assert operator.vpc_id == VPC_ID
        assert operator.waiter_delay == 60
        assert operator.waiter_max_attempts == 100

        operator.execute(None)

        mock_conn.delete_private_graph_endpoint.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            vpcId=VPC_ID,
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_delete_endpoint_no_wait(self, mock_hook_get_waiter, mock_conn):
        mock_conn.delete_private_graph_endpoint.return_value = {
            "status": "DELETING",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }

        operator = NeptuneDeletePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
            wait_for_completion=False,
        )
        operator.execute(None)

        mock_hook_get_waiter.assert_not_called()

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_delete_endpoint_wait_for_completion(self, mock_hook_get_waiter, mock_conn):
        mock_conn.delete_private_graph_endpoint.return_value = {
            "status": "DELETING",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }

        operator = NeptuneDeletePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
            wait_for_completion=True,
        )
        operator.execute(None)

        mock_hook_get_waiter.assert_called_once_with("private_graph_endpoint_deleted")

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_delete_endpoint_failed_status(self, mock_conn):
        mock_conn.delete_private_graph_endpoint.return_value = {
            "status": "FAILED",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }

        operator = NeptuneDeletePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
        )

        with pytest.raises(
            NeptunePrivateEndpointDeletionFailedError,
            match=f"Failed to delete private endpoint {ENDPOINT_ID}",
        ):
            operator.execute(None)

    def test_execute_complete_success(self):
        operator = NeptuneDeletePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
        )

        event = {
            "status": "success",
            "endpoint_id": ENDPOINT_ID,
        }

        operator.execute_complete(None, event)

        # Verify the method completes without error and logs the endpoint_id


class TestNeptuneDeleteGraphOperator:
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_defaults(self, mock_conn):
        mock_conn.delete_graph.return_value = {
            "id": GRAPH_ID,
            "name": GRAPH_NAME,
            "status": "DELETING",
        }

        operator = NeptuneDeleteGraphOperator(
            task_id="test_task",
            graph_id=GRAPH_ID,
            skip_snapshot=True,
        )

        assert operator.graph_id == GRAPH_ID
        assert operator.skip_snapshot is True
        assert operator.wait_for_completion is True
        assert operator.waiter_delay == 30
        assert operator.waiter_max_attempts == 60

        operator.execute(None)

        mock_conn.delete_graph.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            skipSnapshot=True,
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_custom_args(self, mock_conn):
        mock_conn.delete_graph.return_value = {
            "id": GRAPH_ID,
            "name": GRAPH_NAME,
            "status": "DELETING",
        }

        operator = NeptuneDeleteGraphOperator(
            task_id="test_task",
            graph_id=GRAPH_ID,
            skip_snapshot=False,
            waiter_delay=60,
            waiter_max_attempts=100,
        )

        assert operator.graph_id == GRAPH_ID
        assert operator.skip_snapshot is False
        assert operator.waiter_delay == 60
        assert operator.waiter_max_attempts == 100

        operator.execute(None)

        mock_conn.delete_graph.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            skipSnapshot=False,
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_delete_graph_no_wait(self, mock_get_waiter, mock_conn):
        mock_conn.delete_graph.return_value = {
            "id": GRAPH_ID,
            "name": GRAPH_NAME,
            "status": "DELETING",
        }

        operator = NeptuneDeleteGraphOperator(
            task_id="test_task",
            graph_id=GRAPH_ID,
            skip_snapshot=True,
            wait_for_completion=False,
        )
        operator.execute(None)

        mock_conn.delete_graph.assert_called_once()
        mock_get_waiter.assert_not_called()

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_delete_graph_wait_for_completion(self, mock_get_waiter, mock_conn):
        mock_conn.delete_graph.return_value = {
            "id": GRAPH_ID,
            "name": GRAPH_NAME,
            "status": "DELETING",
        }
        mock_waiter = mock.MagicMock()
        mock_get_waiter.return_value = mock_waiter

        operator = NeptuneDeleteGraphOperator(
            task_id="test_task",
            graph_id=GRAPH_ID,
            skip_snapshot=True,
            wait_for_completion=True,
        )
        operator.execute(None)

        mock_get_waiter.assert_called_once_with("graph_deleted")
        mock_waiter.wait.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            WaiterConfig={"Delay": 30, "MaxAttempts": 60},
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_delete_graph_resource_not_found(self, mock_conn):

        # Simulate ResourceNotFoundException
        error_response = {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Graph not found",
            },
            "ResponseMetadata": {
                "HTTPStatusCode": 404,
            },
        }
        mock_conn.delete_graph.side_effect = ClientError(error_response, "delete_graph")

        operator = NeptuneDeleteGraphOperator(
            task_id="test_task",
            graph_id=GRAPH_ID,
            skip_snapshot=True,
        )

        # Should not raise an exception, just log that graph not found
        operator.execute(None)

        mock_conn.delete_graph.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            skipSnapshot=True,
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_delete_graph_other_client_error(self, mock_conn):

        # Simulate other ClientError
        error_response = {
            "Error": {
                "Code": "ValidationException",
                "Message": "Invalid parameter",
            },
            "ResponseMetadata": {
                "HTTPStatusCode": 400,
            },
        }
        mock_conn.delete_graph.side_effect = ClientError(error_response, "delete_graph")

        operator = NeptuneDeleteGraphOperator(
            task_id="test_task",
            graph_id=GRAPH_ID,
            skip_snapshot=True,
        )

        # Should raise NeptuneGraphDeletionFailedError for non-ResourceNotFoundException errors
        with pytest.raises(NeptuneGraphDeletionFailedError):
            operator.execute(None)


class TestNeptuneCreateGraphWithImportOperator:
    IMPORT_TASK_ID = "import-task-12345"

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_defaults(self, mock_conn):
        mock_conn.create_graph_using_import_task.return_value = {
            "graphId": GRAPH_ID,
            "taskId": self.IMPORT_TASK_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneCreateGraphWithImportOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"dimension": 128},
            source=SOURCE_S3_URI,
            role_arn=ROLE_ARN,
        )

        assert operator.graph_name == GRAPH_NAME
        assert operator.vector_search_config == {"dimension": 128}
        assert operator.source == SOURCE_S3_URI
        assert operator.role_arn == ROLE_ARN
        assert operator.blank_node_handling is None
        assert operator.parquet_type is None
        assert operator.format is None
        assert operator.min_provisioned_memory is None
        assert operator.max_provisioned_memory is None
        assert operator.fail_on_error is None
        assert operator.public_connectivity is None
        assert operator.replica_count is None
        assert operator.deletion_protect is None
        assert operator.kms_key_id is None
        assert operator.tags is None
        assert operator.import_options is None
        assert operator.wait_for_completion is True
        assert operator.waiter_delay == 30
        assert operator.waiter_max_attempts == 60

        operator.execute(None)

        mock_conn.create_graph_using_import_task.assert_called_once_with(
            graphName=GRAPH_NAME,
            vectorSearchConfiguration={"dimension": 128},
            source=SOURCE_S3_URI,
            roleArn=ROLE_ARN,
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_with_all_optional_params(self, mock_conn):
        mock_conn.create_graph_using_import_task.return_value = {
            "graphId": GRAPH_ID,
            "taskId": self.IMPORT_TASK_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneCreateGraphWithImportOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"dimension": 128},
            source=SOURCE_S3_URI,
            role_arn=ROLE_ARN,
            blank_node_handling="convertToIri",
            parquet_type="COLUMNAR",
            format="csv",
            min_provisioned_memory=16,
            max_provisioned_memory=32,
            fail_on_error=True,
            public_connectivity=True,
            replica_count=2,
            deletion_protection=True,
            kms_key_id="test-kms-key",
            tags={"env": "test"},
            import_options={"custom-option": "value"},
            waiter_delay=60,
            waiter_max_attempts=100,
        )

        assert operator.blank_node_handling == "convertToIri"
        assert operator.parquet_type == "COLUMNAR"
        assert operator.format == "csv"
        assert operator.min_provisioned_memory == 16
        assert operator.max_provisioned_memory == 32
        assert operator.fail_on_error is True
        assert operator.public_connectivity is True
        assert operator.replica_count == 2
        assert operator.deletion_protect is True
        assert operator.kms_key_id == "test-kms-key"
        assert operator.tags == {"env": "test"}
        assert operator.import_options == {"custom-option": "value"}
        assert operator.waiter_delay == 60
        assert operator.waiter_max_attempts == 100

        operator.execute(None)

        # Verify the call includes all parameters
        call_args = mock_conn.create_graph_using_import_task.call_args[1]
        assert call_args["graphName"] == GRAPH_NAME
        assert call_args["vectorSearchConfiguration"] == {"dimension": 128}
        assert call_args["source"] == SOURCE_S3_URI
        assert call_args["roleArn"] == ROLE_ARN
        assert call_args["format"] == "csv"
        assert call_args["minProvisionedMemory"] == 16
        assert call_args["maxProvisionedMemory"] == 32
        assert call_args["failOnError"] is True
        assert call_args["replicaCount"] == 2
        assert call_args["publicConnectivity"] is True
        assert call_args["deletionProtection"] is True
        assert call_args["kmsKeyIdentifier"] == "test-kms-key"
        assert call_args["tags"] == {"env": "test"}
        # Check import options were merged
        assert "neptune-analytics:blank-node-handling" in call_args["importOptions"]
        assert call_args["importOptions"]["neptune-analytics:blank-node-handling"] == "convertToIri"
        assert "neptune-analytics:parquet-type" in call_args["importOptions"]
        assert call_args["importOptions"]["neptune-analytics:parquet-type"] == "COLUMNAR"
        assert call_args["importOptions"]["custom-option"] == "value"

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_import_options_handling(self, mock_conn):
        mock_conn.create_graph_using_import_task.return_value = {
            "graphId": GRAPH_ID,
            "taskId": self.IMPORT_TASK_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneCreateGraphWithImportOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"dimension": 128},
            source=SOURCE_S3_URI,
            role_arn=ROLE_ARN,
            blank_node_handling="convertToIri",
            import_options={"another-option": "test"},
        )

        operator.execute(None)

        call_args = mock_conn.create_graph_using_import_task.call_args[1]
        # Verify import options were properly merged
        assert call_args["importOptions"]["neptune-analytics:blank-node-handling"] == "convertToIri"
        assert call_args["importOptions"]["another-option"] == "test"

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_create_graph_with_import_no_wait(self, mock_hook_get_waiter, mock_conn):
        mock_conn.create_graph_using_import_task.return_value = {
            "graphId": GRAPH_ID,
            "taskId": self.IMPORT_TASK_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneCreateGraphWithImportOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"dimension": 128},
            source=SOURCE_S3_URI,
            role_arn=ROLE_ARN,
            wait_for_completion=False,
        )
        result = operator.execute(None)

        mock_hook_get_waiter.assert_not_called()
        assert result == {"graph_id": GRAPH_ID}

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_create_graph_with_import_wait_for_completion(self, mock_hook_get_waiter, mock_conn):
        mock_conn.create_graph_using_import_task.return_value = {
            "graphId": GRAPH_ID,
            "taskId": self.IMPORT_TASK_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneCreateGraphWithImportOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"dimension": 128},
            source=SOURCE_S3_URI,
            role_arn=ROLE_ARN,
            wait_for_completion=True,
        )
        result = operator.execute(None)

        # Should wait for both graph_available and import_task_successful
        assert mock_hook_get_waiter.call_count == 2
        mock_hook_get_waiter.assert_any_call("graph_available")
        mock_hook_get_waiter.assert_any_call("import_task_successful")
        assert result == {"graph_id": GRAPH_ID}

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_import_options_none_values_filtered(self, mock_conn):
        mock_conn.create_graph_using_import_task.return_value = {
            "graphId": GRAPH_ID,
            "taskId": self.IMPORT_TASK_ID,
            "status": "IMPORTING",
        }

        # Test that None values in blank_node_handling and parquet_type are filtered out
        operator = NeptuneCreateGraphWithImportOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"dimension": 128},
            source=SOURCE_S3_URI,
            role_arn=ROLE_ARN,
            blank_node_handling=None,
            parquet_type=None,
        )

        operator.execute(None)

        call_args = mock_conn.create_graph_using_import_task.call_args[1]
        # importOptions should not be in call_args if all values are None
        assert "importOptions" not in call_args

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_defer_wait_for_task(self, mock_conn):
        """Test that defer_wait_for_task defers with the import task trigger."""

        operator = NeptuneCreateGraphWithImportOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"dimension": 128},
            source=SOURCE_S3_URI,
            role_arn=ROLE_ARN,
            waiter_delay=30,
            waiter_max_attempts=60,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            operator.defer_wait_for_task(
                import_task_id=self.IMPORT_TASK_ID,
                context=None,
                event={"status": "success"},
            )

        trigger = exc_info.value.trigger
        assert isinstance(trigger, NeptuneImportTaskCompleteTrigger)
        assert exc_info.value.method_name == "execute_complete"

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_deferrable_defers_with_graph_available_trigger(self, mock_conn):
        """Test that execute defers with graph_available trigger and passes import_task_id."""

        mock_conn.create_graph_using_import_task.return_value = {
            "graphId": GRAPH_ID,
            "taskId": self.IMPORT_TASK_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneCreateGraphWithImportOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"dimension": 128},
            source=SOURCE_S3_URI,
            role_arn=ROLE_ARN,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            operator.execute(None)

        trigger = exc_info.value.trigger
        assert isinstance(trigger, NeptuneGraphAvailableTrigger)
        assert exc_info.value.method_name == "defer_wait_for_task"
        assert exc_info.value.kwargs == {"import_task_id": self.IMPORT_TASK_ID}


TASK_ID = "import-task-id-12345"


class TestNeptuneStartImportTaskOperator:
    def test_template_fields(self):
        fields = NeptuneStartImportTaskOperator.template_fields
        assert "graph_identifier" in fields
        assert "role_arn" in fields
        assert "source" in fields
        assert "import_options" in fields

    def test_template_fields_renderers(self):
        assert NeptuneStartImportTaskOperator.template_fields_renderers == {"import_options": "json"}

    def test_operator_extra_links(self):
        assert len(NeptuneStartImportTaskOperator.operator_extra_links) == 1
        assert isinstance(NeptuneStartImportTaskOperator.operator_extra_links[0], NeptuneImportTaskLink)

    @mock.patch("airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneImportTaskLink.persist")
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_defaults(self, mock_conn, mock_persist):
        mock_conn.start_import_task.return_value = {
            "taskId": TASK_ID,
            "graphId": GRAPH_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneStartImportTaskOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            role_arn=ROLE_ARN,
            source=SOURCE_S3_URI,
        )

        assert operator.graph_identifier == GRAPH_ID
        assert operator.role_arn == ROLE_ARN
        assert operator.source == SOURCE_S3_URI
        assert operator.blank_node_handling is None
        assert operator.fail_on_error is True
        assert operator.format is None
        assert operator.import_options is None
        assert operator.parquet_type == "COLUMNAR"
        assert operator.wait_for_completion is True
        assert operator.waiter_delay == 30
        assert operator.waiter_max_attempts == 60

        operator.execute(None)

        mock_conn.start_import_task.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            roleArn=ROLE_ARN,
            source=SOURCE_S3_URI,
            failOnError=True,
            parquetType="COLUMNAR",
        )

    @mock.patch("airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneImportTaskLink.persist")
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_custom_args(self, mock_conn, mock_persist):
        mock_conn.start_import_task.return_value = {
            "taskId": TASK_ID,
            "graphId": GRAPH_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneStartImportTaskOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            role_arn=ROLE_ARN,
            source=SOURCE_S3_URI,
            blank_node_handling=None,
            fail_on_error=False,
            format="CSV",
            import_options={"neptune.csv.allowEmptyStrings": True},
            parquet_type=None,
            waiter_delay=60,
            waiter_max_attempts=100,
        )

        assert operator.blank_node_handling is None
        assert operator.fail_on_error is False
        assert operator.format == "CSV"
        assert operator.import_options == {"neptune.csv.allowEmptyStrings": True}
        assert operator.parquet_type is None
        assert operator.waiter_delay == 60
        assert operator.waiter_max_attempts == 100

        operator.execute(None)

        mock_conn.start_import_task.assert_called_once_with(
            graphIdentifier=GRAPH_ID,
            roleArn=ROLE_ARN,
            source=SOURCE_S3_URI,
            failOnError=False,
            format="CSV",
            importOptions={"neptune.csv.allowEmptyStrings": True},
        )

    @mock.patch("airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneImportTaskLink.persist")
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_start_import_no_wait(self, mock_get_waiter, mock_conn, mock_persist):
        mock_conn.start_import_task.return_value = {
            "taskId": TASK_ID,
            "graphId": GRAPH_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneStartImportTaskOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            role_arn=ROLE_ARN,
            source=SOURCE_S3_URI,
            wait_for_completion=False,
        )
        result = operator.execute(None)

        mock_get_waiter.assert_not_called()
        assert result == {"import_task_id": TASK_ID, "graph_id": GRAPH_ID}

    @mock.patch("airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneImportTaskLink.persist")
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_start_import_wait_for_completion(self, mock_get_waiter, mock_conn, mock_persist):
        mock_conn.start_import_task.return_value = {
            "taskId": TASK_ID,
            "graphId": GRAPH_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneStartImportTaskOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            role_arn=ROLE_ARN,
            source=SOURCE_S3_URI,
            wait_for_completion=True,
        )
        result = operator.execute(None)

        mock_get_waiter.assert_called_once_with("import_task_successful")
        assert result == {"import_task_id": TASK_ID, "graph_id": GRAPH_ID}

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_persist_called_with_correct_args(self, mock_conn):
        """Test that NeptuneImportTaskLink.persist is called with the correct arguments."""
        mock_conn.start_import_task.return_value = {
            "taskId": TASK_ID,
            "graphId": GRAPH_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneStartImportTaskOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            role_arn=ROLE_ARN,
            source=SOURCE_S3_URI,
            wait_for_completion=False,
        )

        mock_context = mock.MagicMock()
        with mock.patch(
            "airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneImportTaskLink.persist"
        ) as mock_persist:
            operator.execute(mock_context)

            mock_persist.assert_called_once_with(
                context=mock_context,
                operator=operator,
                region_name=mock.ANY,
                aws_partition=mock.ANY,
                import_task_id=TASK_ID,
            )

    @mock.patch("airflow.providers.amazon.aws.operators.neptune_analytics.NeptuneImportTaskLink.persist")
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_deferrable_defers_with_import_task_trigger(self, mock_conn, mock_persist):
        """Test that deferrable mode defers with NeptuneImportTaskCompleteTrigger."""

        mock_conn.start_import_task.return_value = {
            "taskId": TASK_ID,
            "graphId": GRAPH_ID,
            "status": "IMPORTING",
        }

        operator = NeptuneStartImportTaskOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            role_arn=ROLE_ARN,
            source=SOURCE_S3_URI,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc_info:
            operator.execute(None)

        trigger = exc_info.value.trigger
        assert isinstance(trigger, NeptuneImportTaskCompleteTrigger)
        assert exc_info.value.method_name == "execute_complete"

    def test_execute_complete_success(self):
        operator = NeptuneStartImportTaskOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            role_arn=ROLE_ARN,
            source=SOURCE_S3_URI,
        )

        event = {"status": "success", "import_task_id": TASK_ID}
        result = operator.execute_complete(None, event)

        assert result == {"graph_id": GRAPH_ID, "import_task_id": TASK_ID}


class TestNeptuneCancelImportTaskOperator:
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_defaults(self, mock_conn):
        mock_conn.cancel_import_task.return_value = {
            "taskId": TASK_ID,
            "graphId": GRAPH_ID,
            "status": "CANCELLING",
        }

        operator = NeptuneCancelImportTaskOperator(
            task_id="test_task",
            import_task_id=TASK_ID,
        )

        assert operator.import_task_id == TASK_ID
        assert operator.wait_for_completion is True
        assert operator.waiter_delay == 30
        assert operator.waiter_max_attempts == 60

        operator.execute(None)

        mock_conn.cancel_import_task.assert_called_once_with(taskIdentifier=TASK_ID)

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_custom_args(self, mock_conn):
        mock_conn.cancel_import_task.return_value = {
            "taskId": TASK_ID,
            "graphId": GRAPH_ID,
            "status": "CANCELLING",
        }

        operator = NeptuneCancelImportTaskOperator(
            task_id="test_task",
            import_task_id=TASK_ID,
            waiter_delay=60,
            waiter_max_attempts=100,
        )

        assert operator.import_task_id == TASK_ID
        assert operator.waiter_delay == 60
        assert operator.waiter_max_attempts == 100

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_cancel_no_wait(self, mock_get_waiter, mock_conn):
        mock_conn.cancel_import_task.return_value = {
            "taskId": TASK_ID,
            "graphId": GRAPH_ID,
            "status": "CANCELLING",
        }

        operator = NeptuneCancelImportTaskOperator(
            task_id="test_task",
            import_task_id=TASK_ID,
            wait_for_completion=False,
        )
        result = operator.execute(None)

        mock_get_waiter.assert_not_called()
        assert result == {"import_task_id": TASK_ID}

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_cancel_wait_for_completion(self, mock_get_waiter, mock_conn):
        mock_conn.cancel_import_task.return_value = {
            "taskId": TASK_ID,
            "graphId": GRAPH_ID,
            "status": "CANCELLING",
        }

        operator = NeptuneCancelImportTaskOperator(
            task_id="test_task",
            import_task_id=TASK_ID,
            wait_for_completion=True,
        )
        result = operator.execute(None)

        mock_get_waiter.assert_called_once_with("import_task_cancelled")
        assert result == {"import_task_id": TASK_ID}

    def test_execute_complete_success(self):
        operator = NeptuneCancelImportTaskOperator(
            task_id="test_task",
            import_task_id=TASK_ID,
        )

        event = {"status": "success", "import_task_id": TASK_ID}
        result = operator.execute_complete(None, event)

        assert result == {"import_task_id": TASK_ID}
