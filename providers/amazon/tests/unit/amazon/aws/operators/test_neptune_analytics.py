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

from collections.abc import Generator
from unittest import mock

import pytest
from moto import mock_aws

from airflow.providers.amazon.aws.hooks.neptune_analytics import NeptuneAnalyticsHook
from airflow.providers.amazon.aws.operators.neptune_analytics import (
    NeptuneCreateGraphOperator,
    NeptuneCreatePrivateGraphEndpointOperator,
    NeptuneDeletePrivateGraphEndpointOperator,
)

GRAPH_NAME = "test_graph"
GRAPH_ID = "test-graph-id"
VPC_ID = "vpc-12345"
SUBNET_IDS = ["subnet-1", "subnet-2"]
SECURITY_GROUP_IDS = ["sg-1", "sg-2"]
ENDPOINT_ID = "vpce-12345"


@pytest.fixture
def hook() -> Generator[NeptuneAnalyticsHook, None, None]:
    with mock_aws():
        yield NeptuneAnalyticsHook(aws_conn_id="aws_default")


class TestNeptuneCreateGraphOperator:
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_defaults(self, mock_conn):
        operator = NeptuneCreateGraphOperator(
            task_id="test_task",
            graph_name=GRAPH_NAME,
            vector_search_config={"test": 123},
            provisioned_memory=16,
        )

        assert operator.public_connectivity is None
        assert operator.replica_count is None
        assert operator.deletion_protect is False
        assert operator.kms_key is None
        assert operator.tags is None

        operator.execute(None)

        mock_conn.create_graph.assert_called_once_with(
            graphName=GRAPH_NAME,
            vectorSearchConfiguration={"test": 123},
            provisionedMemory=16,
            deletionProtection=False,
        )

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_custom_args(self, mock_conn):
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
        assert operator.kms_key == "test-key"
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

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_create_graph(self, mock_hook_get_waiter, mock_conn):
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
        assert resp["graph_id"] is not None

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    @mock.patch.object(NeptuneAnalyticsHook, "get_waiter")
    def test_create_graph_wait_for_completion(self, mock_hook_get_waiter, mock_conn):
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
        assert resp["graph_id"] is not None


class TestNeptuneCreatePrivateGraphEndpointOperator:
    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_init_defaults(self, mock_conn):
        mock_conn.create_private_graph_endpoint.return_value = {
            "status": "CREATING",
            "vpcEndpointId": ENDPOINT_ID,
            "vpcId": VPC_ID,
        }

        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
        )

        assert operator.graph_id == GRAPH_ID
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

        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
            subnet_ids=SUBNET_IDS,
            vpc_security_group_ids=SECURITY_GROUP_IDS,
            waiter_delay=60,
            waiter_max_attempts=100,
        )

        assert operator.graph_id == GRAPH_ID
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

        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task",
            graph_identifier=GRAPH_ID,
            vpc_id=VPC_ID,
            wait_for_completion=True,
        )
        operator.execute(None)

        # Note: The operator currently has 'pass' for wait_for_completion
        # This test documents the current behavior
        # When wait_for_completion is implemented, this test should verify the waiter is called

    @mock.patch.object(NeptuneAnalyticsHook, "conn")
    def test_create_endpoint_failed_status(self, mock_conn):
        from airflow.providers.common.compat.sdk import AirflowException

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

        with pytest.raises(AirflowException, match=f"Private endpoint failed to create for graph {GRAPH_ID}"):
            operator.execute(None)

    def test_execute_complete_success(self):
        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task", graph_identifier=GRAPH_ID, vpc_id=VPC_ID
        )

        event = {
            "status": "success",
            "endpoint_id": ENDPOINT_ID,
        }

        result = operator.execute_complete(None, event)

        assert result == {"vpc_endpoint_id": ENDPOINT_ID, "graph_id": GRAPH_ID, "vpc_id": VPC_ID}

    def test_execute_complete_failure_status(self):
        operator = NeptuneCreatePrivateGraphEndpointOperator(
            task_id="test_task", graph_identifier=GRAPH_ID, vpc_id=VPC_ID
        )

        event = {
            "status": "failure",
            "endpoint_id": ENDPOINT_ID,
        }

        result = operator.execute_complete(None, event)

        assert result == {"vpc_endpoint_id": "", "graph_id": GRAPH_ID, "vpc_id": VPC_ID}


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

        assert operator.graph_id == GRAPH_ID
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

        assert operator.graph_id == GRAPH_ID
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
        from airflow.providers.common.compat.sdk import AirflowException

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

        with pytest.raises(AirflowException, match=f"Failed to delete private endpoint {ENDPOINT_ID}"):
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
