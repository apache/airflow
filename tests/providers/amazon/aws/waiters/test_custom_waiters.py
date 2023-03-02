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

import json
from unittest import mock

import boto3
import pytest
from botocore.exceptions import WaiterError
from botocore.waiter import WaiterModel
from moto import mock_eks

from airflow.providers.amazon.aws.hooks.ecs import EcsClusterStates, EcsHook, EcsTaskDefinitionStates
from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.amazon.aws.waiters.base_waiter import BaseBotoWaiter


def assert_all_match(*args):
    assert len(set(args)) == 1


class TestBaseWaiter:
    def test_init(self):
        waiter_name = "test_waiter"
        client_name = "test_client"
        waiter_model_config = {
            "version": 2,
            "waiters": {
                waiter_name: {
                    "operation": "ListNodegroups",
                    "delay": 30,
                    "maxAttempts": 60,
                    "acceptors": [
                        {
                            "matcher": "path",
                            "argument": "length(nodegroups[]) == `0`",
                            "expected": True,
                            "state": "success",
                        },
                        {
                            "matcher": "path",
                            "expected": True,
                            "argument": "length(nodegroups[]) > `0`",
                            "state": "retry",
                        },
                    ],
                }
            },
        }
        expected_model = WaiterModel(waiter_model_config)

        waiter = BaseBotoWaiter(client_name, waiter_model_config)

        # WaiterModel objects don't implement an eq() so equivalence checking manually.
        for attr, _ in expected_model.__dict__.items():
            assert waiter.model.__getattribute__(attr) == expected_model.__getattribute__(attr)
        assert waiter.client == client_name


class TestCustomEKSServiceWaiters:
    def test_service_waiters(self):
        hook = EksHook()
        with open(hook.waiter_path) as config_file:
            expected_waiters = json.load(config_file)["waiters"]

        for waiter in list(expected_waiters.keys()):
            assert waiter in hook.list_waiters()
            assert waiter in hook._list_custom_waiters()

    @mock_eks
    def test_existing_waiter_inherited(self):
        """
        AwsBaseHook::get_waiter will first check if there is a custom waiter with the
        provided name and pass that through is it exists, otherwise it will check the
        custom waiters for the given service.  This test checks to make sure that the
        waiter is the same whichever way you get it and no modifications are made.
        """
        hook_waiter = EksHook().get_waiter("cluster_active")
        client_waiter = EksHook().conn.get_waiter("cluster_active")
        boto_waiter = boto3.client("eks").get_waiter("cluster_active")

        assert_all_match(hook_waiter.name, client_waiter.name, boto_waiter.name)
        assert_all_match(len(hook_waiter.__dict__), len(client_waiter.__dict__), len(boto_waiter.__dict__))
        for attr, _ in hook_waiter.__dict__.items():
            # Not all attributes in a Waiter are directly comparable
            # so the best we can do it make sure the same attrs exist.
            assert hasattr(boto_waiter, attr)
            assert hasattr(client_waiter, attr)


class TestCustomECSServiceWaiters:
    """Test waiters from ``amazon/aws/waiters/ecs.json``."""

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
        assert "task_definition_active" in hook_waiters
        assert "task_definition_inactive" in hook_waiters

    @staticmethod
    def describe_clusters(status: str, cluster_name: str = "spam-egg", failures: dict | list | None = None):
        """
        Helper function for generate minimal DescribeClusters response for single job.
        https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_DescribeClusters.html
        """
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

    @pytest.mark.parametrize("state", [EcsClusterStates.FAILED, EcsClusterStates.INACTIVE])
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
    def describe_task_definition(status: str, task_definition: str = "spam-egg"):
        """
        Helper function for generate minimal DescribeTaskDefinition response for single job.
        https://docs.aws.amazon.com/AmazonECS/latest/APIReference/API_DescribeTaskDefinition.html
        """
        return {
            "taskDefinition": {
                "taskDefinitionArn": (
                    f"arn:aws:ecs:eu-west-3:123456789012:task-definition/{task_definition}:42"
                ),
                "status": status,
            }
        }

    def test_task_definition_active(self, mock_describe_task_definition):
        """Test task definition reach active state during creation."""
        mock_describe_task_definition.side_effect = [
            self.describe_task_definition(EcsTaskDefinitionStates.INACTIVE),
            self.describe_task_definition(EcsTaskDefinitionStates.INACTIVE),
            self.describe_task_definition(EcsTaskDefinitionStates.ACTIVE),
        ]
        waiter = EcsHook(aws_conn_id=None).get_waiter("task_definition_active")
        waiter.wait(taskDefinition="spam-egg", WaiterConfig={"Delay": 0.01, "MaxAttempts": 3})

    def test_task_definition_failure(self, mock_describe_task_definition):
        """Test task definition reach delete in progress state during creation."""
        mock_describe_task_definition.side_effect = [
            self.describe_task_definition(EcsTaskDefinitionStates.DELETE_IN_PROGRESS),
        ]
        waiter = EcsHook(aws_conn_id=None).get_waiter("task_definition_active")
        with pytest.raises(WaiterError, match='matched expected path: "DELETE_IN_PROGRESS"'):
            waiter.wait(taskDefinition="spam-egg", WaiterConfig={"Delay": 0.01, "MaxAttempts": 1})

    def test_task_definition_inactive(self, mock_describe_task_definition):
        """Test task definition reach inactive state during deletion."""
        mock_describe_task_definition.side_effect = [
            self.describe_task_definition(EcsTaskDefinitionStates.ACTIVE),
            self.describe_task_definition(EcsTaskDefinitionStates.ACTIVE),
            self.describe_task_definition(EcsTaskDefinitionStates.INACTIVE),
        ]
        waiter = EcsHook(aws_conn_id=None).get_waiter("task_definition_inactive")
        waiter.wait(taskDefinition="spam-egg", WaiterConfig={"Delay": 0.01, "MaxAttempts": 3})
