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
from typing import Sequence
from unittest import mock

import boto3
import pytest
from botocore.exceptions import WaiterError
from botocore.waiter import WaiterModel
from moto import mock_eks

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.batch_client import BatchClientHook
from airflow.providers.amazon.aws.hooks.dynamodb import DynamoDBHook
from airflow.providers.amazon.aws.hooks.ecs import EcsClusterStates, EcsHook, EcsTaskDefinitionStates
from airflow.providers.amazon.aws.hooks.eks import EksHook
from airflow.providers.amazon.aws.hooks.emr import EmrHook
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
        for attr in expected_model.__dict__:
            assert waiter.model.__getattribute__(attr) == expected_model.__getattribute__(attr)
        assert waiter.client == client_name

    @pytest.mark.parametrize("boto_type", ["client", "resource"])
    def test_get_botocore_waiter(self, boto_type, monkeypatch):
        kw = {f"{boto_type}_type": "s3"}
        if boto_type == "client":
            fake_client = boto3.client("s3", region_name="eu-west-3")
        elif boto_type == "resource":
            fake_client = boto3.resource("s3", region_name="eu-west-3")
        else:
            raise ValueError(f"Unexpected value {boto_type!r} for `boto_type`.")
        monkeypatch.setattr(AwsBaseHook, "conn", fake_client)

        hook = AwsBaseHook(**kw)
        with mock.patch("botocore.client.BaseClient.get_waiter") as m:
            hook.get_waiter(waiter_name="FooBar")
            m.assert_called_once_with("FooBar")


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
        for attr in hook_waiter.__dict__:
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


class TestCustomDynamoDBServiceWaiters:
    """Test waiters from ``amazon/aws/waiters/dynamodb.json``."""

    STATUS_COMPLETED = "COMPLETED"
    STATUS_FAILED = "FAILED"
    STATUS_IN_PROGRESS = "IN_PROGRESS"

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.resource = boto3.resource("dynamodb", region_name="eu-west-3")
        monkeypatch.setattr(DynamoDBHook, "conn", self.resource)
        self.client = self.resource.meta.client

    @pytest.fixture
    def mock_describe_export(self):
        """Mock ``DynamoDBHook.Client.describe_export`` method."""
        with mock.patch.object(self.client, "describe_export") as m:
            yield m

    def test_service_waiters(self):
        hook_waiters = DynamoDBHook(aws_conn_id=None).list_waiters()
        assert "export_table" in hook_waiters

    @staticmethod
    def describe_export(status: str):
        """
        Helper function for generate minimal DescribeExport response for single job.
        https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_DescribeExport.html
        """
        return {"ExportDescription": {"ExportStatus": status}}

    def test_export_table_to_point_in_time_completed(self, mock_describe_export):
        """Test state transition from `in progress` to `completed` during init."""
        waiter = DynamoDBHook(aws_conn_id=None).get_waiter("export_table")
        mock_describe_export.side_effect = [
            self.describe_export(self.STATUS_IN_PROGRESS),
            self.describe_export(self.STATUS_COMPLETED),
        ]
        waiter.wait(
            ExportArn="LoremIpsumissimplydummytextoftheprintingandtypesettingindustry",
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )

    def test_export_table_to_point_in_time_failed(self, mock_describe_export):
        """Test state transition from `in progress` to `failed` during init."""
        with mock.patch("boto3.client") as client:
            client.return_value = self.client
            mock_describe_export.side_effect = [
                self.describe_export(self.STATUS_IN_PROGRESS),
                self.describe_export(self.STATUS_FAILED),
            ]
            waiter = DynamoDBHook(aws_conn_id=None).get_waiter("export_table", client=self.client)
            with pytest.raises(WaiterError, match='we matched expected path: "FAILED"'):
                waiter.wait(
                    ExportArn="LoremIpsumissimplydummytextoftheprintingandtypesettingindustry",
                    WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
                )


class TestCustomBatchServiceWaiters:
    """Test waiters from ``amazon/aws/waiters/batch.json``."""

    JOB_ID = "test_job_id"

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.client = boto3.client("batch", region_name="eu-west-3")
        monkeypatch.setattr(BatchClientHook, "conn", self.client)

    @pytest.fixture
    def mock_describe_jobs(self):
        """Mock ``BatchClientHook.Client.describe_jobs`` method."""
        with mock.patch.object(self.client, "describe_jobs") as m:
            yield m

    def test_service_waiters(self):
        hook_waiters = BatchClientHook(aws_conn_id=None).list_waiters()
        assert "batch_job_complete" in hook_waiters

    @staticmethod
    def describe_jobs(status: str):
        """
        Helper function for generate minimal DescribeJobs response for a single job.
        https://docs.aws.amazon.com/batch/latest/APIReference/API_DescribeJobs.html
        """
        return {
            "jobs": [
                {
                    "status": status,
                },
            ],
        }

    def test_job_succeeded(self, mock_describe_jobs):
        """Test job succeeded"""
        mock_describe_jobs.side_effect = [
            self.describe_jobs(BatchClientHook.RUNNING_STATE),
            self.describe_jobs(BatchClientHook.SUCCESS_STATE),
        ]
        waiter = BatchClientHook(aws_conn_id=None).get_waiter("batch_job_complete")
        waiter.wait(jobs=[self.JOB_ID], WaiterConfig={"Delay": 0.01, "MaxAttempts": 2})

    def test_job_failed(self, mock_describe_jobs):
        """Test job failed"""
        mock_describe_jobs.side_effect = [
            self.describe_jobs(BatchClientHook.RUNNING_STATE),
            self.describe_jobs(BatchClientHook.FAILURE_STATE),
        ]
        waiter = BatchClientHook(aws_conn_id=None).get_waiter("batch_job_complete")

        with pytest.raises(WaiterError, match="Waiter encountered a terminal failure state"):
            waiter.wait(jobs=[self.JOB_ID], WaiterConfig={"Delay": 0.01, "MaxAttempts": 2})


class TestCustomEmrServiceWaiters:
    """Test waiters from ``amazon/aws/waiters/emr.json``."""

    JOBFLOW_ID = "test_jobflow_id"
    STEP_ID1 = "test_step_id_1"
    STEP_ID2 = "test_step_id_2"

    @pytest.fixture(autouse=True)
    def setup_test_cases(self, monkeypatch):
        self.client = boto3.client("emr", region_name="eu-west-3")
        monkeypatch.setattr(EmrHook, "conn", self.client)

    @pytest.fixture
    def mock_list_steps(self):
        """Mock ``EmrHook.Client.list_steps`` method."""
        with mock.patch.object(self.client, "list_steps") as m:
            yield m

    def test_service_waiters(self):
        hook_waiters = EmrHook(aws_conn_id=None).list_waiters()
        assert "steps_wait_for_terminal" in hook_waiters

    @staticmethod
    def list_steps(step_records: Sequence[tuple[str, str]]):
        """
        Helper function to generate minimal ListSteps response.
        https://docs.aws.amazon.com/emr/latest/APIReference/API_ListSteps.html
        """
        return {
            "Steps": [
                {
                    "Id": step_record[0],
                    "Status": {
                        "State": step_record[1],
                    },
                }
                for step_record in step_records
            ],
        }

    def test_steps_succeeded(self, mock_list_steps):
        """Test steps succeeded"""
        mock_list_steps.side_effect = [
            self.list_steps([(self.STEP_ID1, "PENDING"), (self.STEP_ID2, "RUNNING")]),
            self.list_steps([(self.STEP_ID1, "RUNNING"), (self.STEP_ID2, "COMPLETED")]),
            self.list_steps([(self.STEP_ID1, "COMPLETED"), (self.STEP_ID2, "COMPLETED")]),
        ]
        waiter = EmrHook(aws_conn_id=None).get_waiter("steps_wait_for_terminal")
        waiter.wait(
            ClusterId=self.JOBFLOW_ID,
            StepIds=[self.STEP_ID1, self.STEP_ID2],
            WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
        )

    def test_steps_failed(self, mock_list_steps):
        """Test steps failed"""
        mock_list_steps.side_effect = [
            self.list_steps([(self.STEP_ID1, "PENDING"), (self.STEP_ID2, "RUNNING")]),
            self.list_steps([(self.STEP_ID1, "RUNNING"), (self.STEP_ID2, "COMPLETED")]),
            self.list_steps([(self.STEP_ID1, "FAILED"), (self.STEP_ID2, "COMPLETED")]),
        ]
        waiter = EmrHook(aws_conn_id=None).get_waiter("steps_wait_for_terminal")

        with pytest.raises(WaiterError, match="Waiter encountered a terminal failure state"):
            waiter.wait(
                ClusterId=self.JOBFLOW_ID,
                StepIds=[self.STEP_ID1, self.STEP_ID2],
                WaiterConfig={"Delay": 0.01, "MaxAttempts": 3},
            )
