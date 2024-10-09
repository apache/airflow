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

import boto3
import pytest
from botocore.exceptions import WaiterError

from airflow.providers.amazon.aws.hooks.ecs import EcsClusterStates, EcsHook, EcsTaskDefinitionStates


class TestCustomECSServiceWaiters:
    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.client = boto3.client("ecs", region_name="eu-west-3")
        monkeypatch.setattr(EcsHook, "conn", self.client)

    @pytest.fixture
    def mock_describe_clusters(self):
        """Mock ``ECS.Client.describe_clusters`` method."""
        with mock.patch.object(self.client, "describe_clusters") as m:
            yield m

    @pytest.fixture
    def mock_describe_task_definition(self):
        """Mock ``ECS.Client.describe_task_definition`` method."""
        with mock.patch.object(self.client, "describe_task_definition") as m:
            yield m

    def test_service_waiters(self):
        hook_waiters = EcsHook(aws_conn_id=None).list_waiters()
        assert "cluster_active" in hook_waiters
        assert "cluster_inactive" in hook_waiters

    @staticmethod
    def describe_clusters(
        status: str | EcsClusterStates, cluster_name: str = "spam-egg", failures: dict | list | None = None
    ):
        """
        Helper function for generate minimal DescribeClusters response for single job.
        https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_DescribeClusters.html
        """
        if isinstance(status, EcsClusterStates):
            status = status.value
        else:
            assert status in EcsClusterStates.__members__.values()

        failures = failures or []
        if isinstance(failures, dict):
            failures = [failures]

        return {"clusters": [{"clusterName": cluster_name, "status": status}], "failures": failures}

    def test_cluster_active(self, mock_describe_clusters):
        """Test cluster reach Active state during creation."""
        mock_describe_clusters.side_effect = [
            self.describe_clusters(EcsClusterStates.DEPROVISIONING),
            self.describe_clusters(EcsClusterStates.PROVISIONING),
            self.describe_clusters(EcsClusterStates.ACTIVE),
        ]
        waiter = EcsHook(aws_conn_id=None).get_waiter("cluster_active")
        waiter.wait(clusters=["spam-egg"], WaiterConfig={"Delay": 0.01, "MaxAttempts": 3})

    @pytest.mark.parametrize("state", ["FAILED", "INACTIVE"])
    def test_cluster_active_failure_states(self, mock_describe_clusters, state):
        """Test cluster reach inactive state during creation."""
        mock_describe_clusters.side_effect = [
            self.describe_clusters(EcsClusterStates.PROVISIONING),
            self.describe_clusters(state),
        ]
        waiter = EcsHook(aws_conn_id=None).get_waiter("cluster_active")
        with pytest.raises(WaiterError, match=f'matched expected path: "{state}"'):
            waiter.wait(clusters=["spam-egg"], WaiterConfig={"Delay": 0.01, "MaxAttempts": 3})

    def test_cluster_active_failure_reasons(self, mock_describe_clusters):
        """Test cluster reach failure state during creation."""
        mock_describe_clusters.side_effect = [
            self.describe_clusters(EcsClusterStates.PROVISIONING),
            self.describe_clusters(EcsClusterStates.PROVISIONING, failures={"reason": "MISSING"}),
        ]
        waiter = EcsHook(aws_conn_id=None).get_waiter("cluster_active")
        with pytest.raises(WaiterError, match='matched expected path: "MISSING"'):
            waiter.wait(clusters=["spam-egg"], WaiterConfig={"Delay": 0.01, "MaxAttempts": 3})

    def test_cluster_inactive(self, mock_describe_clusters):
        """Test cluster reach Inactive state during deletion."""
        mock_describe_clusters.side_effect = [
            self.describe_clusters(EcsClusterStates.ACTIVE),
            self.describe_clusters(EcsClusterStates.ACTIVE),
            self.describe_clusters(EcsClusterStates.INACTIVE),
        ]
        waiter = EcsHook(aws_conn_id=None).get_waiter("cluster_inactive")
        waiter.wait(clusters=["spam-egg"], WaiterConfig={"Delay": 0.01, "MaxAttempts": 3})

    def test_cluster_inactive_failure_reasons(self, mock_describe_clusters):
        """Test cluster reach failure state during deletion."""
        mock_describe_clusters.side_effect = [
            self.describe_clusters(EcsClusterStates.ACTIVE),
            self.describe_clusters(EcsClusterStates.DEPROVISIONING),
            self.describe_clusters(EcsClusterStates.DEPROVISIONING, failures={"reason": "MISSING"}),
        ]
        waiter = EcsHook(aws_conn_id=None).get_waiter("cluster_inactive")
        waiter.wait(clusters=["spam-egg"], WaiterConfig={"Delay": 0.01, "MaxAttempts": 3})

    @staticmethod
    def describe_task_definition(status: str | EcsTaskDefinitionStates, task_definition: str = "spam-egg"):
        """
        Helper function for generate minimal DescribeTaskDefinition response for single job.
        https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_DescribeTaskDefinition.html
        """
        if isinstance(status, EcsTaskDefinitionStates):
            status = status.value
        else:
            assert status in EcsTaskDefinitionStates.__members__.values()

        return {
            "taskDefinition": {
                "taskDefinitionArn": (
                    f"arn:aws:ecs:eu-west-3:123456789012:task-definition/{task_definition}:42"
                ),
                "status": status,
            }
        }
