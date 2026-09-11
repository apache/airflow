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
from unittest.mock import Mock

import boto3
import pytest
from botocore.exceptions import ClientError, WaiterError
from tenacity import wait_fixed

from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftCreateClusterOperator,
    RedshiftCreateClusterSnapshotOperator,
    RedshiftDeleteClusterOperator,
    RedshiftDeleteClusterSnapshotOperator,
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)
from airflow.providers.amazon.aws.triggers.redshift_cluster import (
    RedshiftClusterSettledTrigger,
    RedshiftCreateClusterSnapshotTrigger,
    RedshiftDeleteClusterTrigger,
    RedshiftPauseClusterTrigger,
    RedshiftResumeClusterTrigger,
)
from airflow.providers.common.compat.sdk import AirflowException, TaskDeferred

from unit.amazon.aws.utils.test_template_fields import validate_template_fields


class TestRedshiftCreateClusterOperator:
    def test_init(self):
        redshift_operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            node_type="ra3.large",
            master_username="adminuser",
            master_user_password="Test123$",
        )
        assert redshift_operator.task_id == "task_test"
        assert redshift_operator.cluster_identifier == "test_cluster"
        assert redshift_operator.node_type == "ra3.large"
        assert redshift_operator.master_username == "adminuser"
        assert redshift_operator.master_user_password == "Test123$"

    @mock.patch.object(RedshiftHook, "conn")
    def test_create_single_node_cluster(self, mock_conn):
        redshift_operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test-cluster",
            node_type="ra3.large",
            master_username="adminuser",
            master_user_password="Test123$",
            cluster_type="single-node",
            wait_for_completion=True,
        )
        redshift_operator.execute(None)
        params = {
            "DBName": "dev",
            "ClusterType": "single-node",
            "AutomatedSnapshotRetentionPeriod": 1,
            "ClusterVersion": "1.0",
            "AllowVersionUpgrade": True,
            "PubliclyAccessible": True,
            "Port": 5439,
        }
        mock_conn.create_cluster.assert_called_once_with(
            ClusterIdentifier="test-cluster",
            NodeType="ra3.large",
            MasterUsername="adminuser",
            MasterUserPassword="Test123$",
            **params,
        )

        # wait_for_completion is True so check waiter is called
        mock_conn.get_waiter.return_value.wait.assert_called_once_with(
            ClusterIdentifier="test-cluster", WaiterConfig={"Delay": 60, "MaxAttempts": 5}
        )

    @mock.patch.object(RedshiftHook, "conn")
    def test_create_multi_node_cluster(self, mock_conn):
        redshift_operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test-cluster",
            node_type="ra3.large",
            number_of_nodes=3,
            master_username="adminuser",
            master_user_password="Test123$",
            cluster_type="multi-node",
        )
        redshift_operator.execute(None)
        params = {
            "DBName": "dev",
            "ClusterType": "multi-node",
            "NumberOfNodes": 3,
            "AutomatedSnapshotRetentionPeriod": 1,
            "ClusterVersion": "1.0",
            "AllowVersionUpgrade": True,
            "PubliclyAccessible": True,
            "Port": 5439,
        }
        mock_conn.create_cluster.assert_called_once_with(
            ClusterIdentifier="test-cluster",
            NodeType="ra3.large",
            MasterUsername="adminuser",
            MasterUserPassword="Test123$",
            **params,
        )

        # wait_for_completion is False so check waiter is not called
        mock_conn.get_waiter.assert_not_called()

    @mock.patch.object(RedshiftHook, "conn")
    def test_create_cluster_deferrable(self, mock_conn):
        redshift_operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test-cluster",
            node_type="ra3.large",
            master_username="adminuser",
            master_user_password="Test123$",
            cluster_type="single-node",
            deferrable=True,
        )

        with pytest.raises(TaskDeferred):
            redshift_operator.execute(None)

    def test_template_fields(self):
        operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test-cluster",
            node_type="ra3.large",
            master_username="adminuser",
            master_user_password="Test123$",
            cluster_type="single-node",
            deferrable=True,
        )
        validate_template_fields(operator)

    @mock.patch.object(RedshiftHook, "delete_cluster")
    @mock.patch.object(RedshiftHook, "conn")
    def test_create_cluster_cleanup_on_waiter_auth_failure(
        self,
        mock_conn,
        mock_delete_cluster,
    ):
        # Simulate waiter failure (e.g. DescribeClusters denied).
        waiter_error = WaiterError(
            name="ClusterAvailable",
            reason="AccessDenied for DescribeClusters",
            last_response={},
        )
        mock_conn.get_waiter.return_value.wait.side_effect = waiter_error

        operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test-cluster",
            node_type="ra3.large",
            master_username="adminuser",
            master_user_password="Test123$",
            cluster_type="single-node",
            wait_for_completion=True,
            delete_cluster_on_failure=True,
            cleanup_timeout_seconds=300,
        )

        with pytest.raises(WaiterError):
            operator.execute({})

        # Cluster creation happened.
        mock_conn.create_cluster.assert_called_once()

        # Cleanup attempted at least once.
        mock_delete_cluster.assert_called_with(
            cluster_identifier="test-cluster",
        )
        assert mock_delete_cluster.call_count >= 1

    @mock.patch.object(RedshiftHook, "delete_cluster")
    @mock.patch.object(RedshiftHook, "conn")
    def test_create_cluster_cleanup_failure_does_not_mask_original_error(
        self,
        mock_conn,
        mock_delete_cluster,
    ):
        # Simulate waiter failure (e.g. DescribeClusters denied).
        waiter_error = WaiterError(
            name="ClusterAvailable",
            reason="AccessDenied for DescribeClusters",
            last_response={},
        )

        # Simulate deletion failure (e.g. DeleteCluster denied).
        cleanup_error = ClientError(
            error_response={
                "Error": {
                    "Code": "UnauthorizedOperation",
                    "Message": "You are not authorized to perform this operation",
                }
            },
            operation_name="DeleteCluster",
        )

        mock_conn.get_waiter.return_value.wait.side_effect = waiter_error
        mock_delete_cluster.side_effect = cleanup_error

        operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test-cluster",
            node_type="ra3.large",
            master_username="adminuser",
            master_user_password="Test123$",
            cluster_type="single-node",
            wait_for_completion=True,
            delete_cluster_on_failure=True,
            cleanup_timeout_seconds=300,
        )

        with pytest.raises(WaiterError) as exc:
            operator.execute({})

        # Original exception must be preserved.
        assert exc.value is waiter_error

        # Cluster creation happened.
        mock_conn.create_cluster.assert_called_once()

        # Cleanup attempted despite failure.
        mock_delete_cluster.assert_called_with(
            cluster_identifier="test-cluster",
        )

        # Cleanup attempted despite failure.
        mock_delete_cluster.assert_called_with(
            cluster_identifier="test-cluster",
        )
        assert mock_delete_cluster.call_count >= 1

    @mock.patch(
        "airflow.providers.amazon.aws.operators.redshift_cluster.wait_fixed",
    )
    @mock.patch.object(RedshiftHook, "delete_cluster")
    @mock.patch.object(RedshiftHook, "conn")
    def test_create_cluster_cleanup_retries_on_active_operation(
        self,
        mock_conn,
        mock_delete_cluster,
        mock_wait_fixed,
    ):

        mock_wait_fixed.return_value = wait_fixed(0)
        # Simulate waiter failure (e.g. DescribeClusters denied).
        waiter_error = WaiterError(
            name="ClusterAvailable",
            reason="AccessDenied for DescribeClusters",
            last_response={},
        )
        mock_conn.get_waiter.return_value.wait.side_effect = waiter_error

        # First deletion attempt fails due to cluster still modifying.
        active_operation_error = ClientError(
            error_response={
                "Error": {
                    "Code": "InvalidClusterStateFault",
                    "Message": "Cluster currently modifying",
                }
            },
            operation_name="DeleteCluster",
        )

        # Second attempt succeeds.
        mock_delete_cluster.side_effect = [
            active_operation_error,
            None,
        ]

        operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test-cluster",
            node_type="ra3.large",
            master_username="adminuser",
            master_user_password="Test123$",
            cluster_type="single-node",
            wait_for_completion=True,
            delete_cluster_on_failure=True,
            cleanup_timeout_seconds=300,
        )

        with pytest.raises(WaiterError):
            operator.execute({})

        # Retry should occur.
        assert mock_delete_cluster.call_count == 2


class TestRedshiftCreateClusterSnapshotOperator:
    @mock.patch.object(RedshiftHook, "cluster_status")
    @mock.patch.object(RedshiftHook, "conn")
    def test_create_cluster_snapshot_is_called_when_cluster_is_available(
        self, mock_conn, mock_cluster_status
    ):
        mock_cluster_status.return_value = "available"
        create_snapshot = RedshiftCreateClusterSnapshotOperator(
            task_id="test_snapshot",
            cluster_identifier="test_cluster",
            snapshot_identifier="test_snapshot",
            retention_period=1,
            tags=[
                {
                    "Key": "user",
                    "Value": "airflow",
                }
            ],
        )
        create_snapshot.execute(None)
        mock_conn.create_cluster_snapshot.assert_called_once_with(
            ClusterIdentifier="test_cluster",
            SnapshotIdentifier="test_snapshot",
            ManualSnapshotRetentionPeriod=1,
            Tags=[
                {
                    "Key": "user",
                    "Value": "airflow",
                }
            ],
        )

        mock_conn.get_waiter.assert_not_called()

    @mock.patch.object(RedshiftHook, "cluster_status")
    def test_raise_exception_when_cluster_is_not_available(self, mock_cluster_status):
        mock_cluster_status.return_value = "paused"
        create_snapshot = RedshiftCreateClusterSnapshotOperator(
            task_id="test_snapshot", cluster_identifier="test_cluster", snapshot_identifier="test_snapshot"
        )
        with pytest.raises(AirflowException):
            create_snapshot.execute(None)

    @mock.patch.object(RedshiftHook, "cluster_status")
    @mock.patch.object(RedshiftHook, "conn")
    def test_create_cluster_snapshot_with_wait(self, mock_conn, mock_cluster_status):
        mock_cluster_status.return_value = "available"
        create_snapshot = RedshiftCreateClusterSnapshotOperator(
            task_id="test_snapshot",
            cluster_identifier="test_cluster",
            snapshot_identifier="test_snapshot",
            wait_for_completion=True,
        )
        create_snapshot.execute(None)
        mock_conn.get_waiter.return_value.wait.assert_called_once_with(
            ClusterIdentifier="test_cluster",
            WaiterConfig={"Delay": 15, "MaxAttempts": 20},
        )

    @mock.patch.object(RedshiftHook, "cluster_status")
    @mock.patch.object(RedshiftHook, "create_cluster_snapshot")
    def test_create_cluster_snapshot_deferred(self, mock_create_cluster_snapshot, mock_cluster_status):
        mock_cluster_status.return_value = "available"
        mock_create_cluster_snapshot.return_value = True
        create_snapshot = RedshiftCreateClusterSnapshotOperator(
            task_id="test_snapshot",
            cluster_identifier="test_cluster",
            snapshot_identifier="test_snapshot",
            deferrable=True,
        )
        with pytest.raises(TaskDeferred) as exc:
            create_snapshot.execute(None)
        assert isinstance(exc.value.trigger, RedshiftCreateClusterSnapshotTrigger), (
            "Trigger is not a RedshiftCreateClusterSnapshotTrigger"
        )

    def test_template_fields(self):
        operator = RedshiftCreateClusterSnapshotOperator(
            task_id="test_snapshot",
            cluster_identifier="test_cluster",
            snapshot_identifier="test_snapshot",
            wait_for_completion=True,
        )
        validate_template_fields(operator)


class TestRedshiftDeleteClusterSnapshotOperator:
    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_cluster_snapshot_status"
    )
    @mock.patch.object(RedshiftHook, "conn")
    def test_delete_cluster_snapshot_wait(self, mock_conn, mock_get_cluster_snapshot_status):
        mock_get_cluster_snapshot_status.return_value = None
        delete_snapshot = RedshiftDeleteClusterSnapshotOperator(
            task_id="test_snapshot",
            cluster_identifier="test_cluster",
            snapshot_identifier="test_snapshot",
        )
        delete_snapshot.execute(None)
        mock_conn.delete_cluster_snapshot.assert_called_once_with(
            SnapshotClusterIdentifier="test_cluster",
            SnapshotIdentifier="test_snapshot",
        )

        mock_get_cluster_snapshot_status.assert_called_once_with(
            snapshot_identifier="test_snapshot",
        )

    @mock.patch(
        "airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_cluster_snapshot_status"
    )
    @mock.patch.object(RedshiftHook, "conn")
    def test_delete_cluster_snapshot(self, mock_conn, mock_get_cluster_snapshot_status):
        delete_snapshot = RedshiftDeleteClusterSnapshotOperator(
            task_id="test_snapshot",
            cluster_identifier="test_cluster",
            snapshot_identifier="test_snapshot",
            wait_for_completion=False,
        )
        delete_snapshot.execute(None)
        mock_conn.delete_cluster_snapshot.assert_called_once_with(
            SnapshotClusterIdentifier="test_cluster",
            SnapshotIdentifier="test_snapshot",
        )

        mock_get_cluster_snapshot_status.assert_not_called()

    def test_template_fields(self):
        operator = RedshiftDeleteClusterSnapshotOperator(
            task_id="test_snapshot",
            cluster_identifier="test_cluster",
            snapshot_identifier="test_snapshot",
            wait_for_completion=False,
        )
        validate_template_fields(operator)


class TestResumeClusterOperator:
    def test_init(self):
        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        assert redshift_operator.task_id == "task_test"
        assert redshift_operator.cluster_identifier == "test_cluster"
        assert redshift_operator.aws_conn_id == "aws_conn_test"

    @mock.patch.object(RedshiftHook, "conn")
    def test_resume_cluster_is_called_when_cluster_is_paused(self, mock_conn):
        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_conn.resume_cluster.assert_called_once_with(ClusterIdentifier="test_cluster")

    @mock.patch.object(RedshiftHook, "conn")
    @mock.patch("time.sleep", return_value=None)
    def test_resume_cluster_multiple_attempts(self, mock_sleep, mock_conn):
        exception = boto3.client("redshift").exceptions.InvalidClusterStateFault({}, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_conn.resume_cluster.side_effect = [exception, exception, True]
        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
        )
        redshift_operator.execute(None)
        assert mock_conn.resume_cluster.call_count == 3

    @mock.patch.object(RedshiftHook, "conn")
    @mock.patch("time.sleep", return_value=None)
    def test_resume_cluster_multiple_attempts_fail(self, mock_sleep, mock_conn):
        exception = boto3.client("redshift").exceptions.InvalidClusterStateFault({}, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_conn.resume_cluster.side_effect = exception

        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
        )
        with pytest.raises(returned_exception):
            redshift_operator.execute(None)
        assert mock_conn.resume_cluster.call_count == 10

    @mock.patch.object(RedshiftHook, "cluster_status")
    @mock.patch.object(RedshiftHook, "conn")
    def test_resume_cluster_deferrable(self, mock_conn, mock_cluster_status):
        """Test Resume cluster operator deferrable"""
        mock_conn.resume_cluster.return_value = True
        mock_cluster_status.return_value = "paused"

        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
            wait_for_completion=True,
            deferrable=True,
        )

        with pytest.raises(TaskDeferred) as exc:
            redshift_operator.execute({})

        assert isinstance(exc.value.trigger, RedshiftResumeClusterTrigger), (
            "Trigger is not a RedshiftResumeClusterTrigger"
        )

    @mock.patch("airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftResumeClusterOperator.defer")
    @mock.patch.object(RedshiftHook, "cluster_status")
    @mock.patch.object(RedshiftHook, "conn")
    def test_resume_cluster_deferrable_in_deleting_state(self, mock_conn, mock_cluster_status, mock_defer):
        """Test Resume cluster operator deferrable"""
        mock_conn.resume_cluster.return_value = True
        mock_cluster_status.return_value = "deleting"

        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
            wait_for_completion=True,
            deferrable=True,
        )

        with pytest.raises(AirflowException):
            redshift_operator.execute({})
        assert not mock_defer.called

    @mock.patch.object(RedshiftHook, "get_waiter")
    @mock.patch.object(RedshiftHook, "conn")
    def test_resume_cluster_wait_for_completion(self, mock_conn, mock_get_waiter):
        """Test Resume cluster operator wait for complettion"""
        mock_conn.resume_cluster.return_value = True
        mock_get_waiter().wait.return_value = None

        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
            wait_for_completion=True,
        )
        redshift_operator.execute(None)
        mock_conn.resume_cluster.assert_called_once_with(ClusterIdentifier="test_cluster")

        mock_get_waiter.assert_called_with("cluster_resumed")
        assert mock_get_waiter.call_count == 2
        mock_get_waiter().wait.assert_called_once_with(
            ClusterIdentifier="test_cluster", WaiterConfig={"Delay": 30, "MaxAttempts": 30}
        )

    def test_resume_cluster_failure(self):
        """Test Resume cluster operator Failure"""
        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
            deferrable=True,
        )

        with pytest.raises(AirflowException):
            redshift_operator.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    def test_template_fields(self):
        operator = RedshiftResumeClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
        )
        validate_template_fields(operator)


class TestPauseClusterOperator:
    def test_init(self):
        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        assert redshift_operator.task_id == "task_test"
        assert redshift_operator.cluster_identifier == "test_cluster"
        assert redshift_operator.aws_conn_id == "aws_conn_test"

    @mock.patch.object(RedshiftHook, "conn")
    def test_pause_cluster_is_called_when_cluster_is_available(self, mock_conn):
        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_conn.pause_cluster.assert_called_once_with(ClusterIdentifier="test_cluster")

    @mock.patch.object(RedshiftHook, "conn")
    @mock.patch("time.sleep", return_value=None)
    def test_pause_cluster_multiple_attempts(self, mock_sleep, mock_conn):
        exception = boto3.client("redshift").exceptions.InvalidClusterStateFault({}, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_conn.pause_cluster.side_effect = [exception, exception, True]

        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
        )

        redshift_operator.execute(None)
        assert mock_conn.pause_cluster.call_count == 3

    @mock.patch.object(RedshiftHook, "conn")
    @mock.patch("time.sleep", return_value=None)
    def test_pause_cluster_multiple_attempts_fail(self, mock_sleep, mock_conn):
        exception = boto3.client("redshift").exceptions.InvalidClusterStateFault({}, "test")
        returned_exception = type(exception)

        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_conn.pause_cluster.side_effect = exception

        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
        )
        with pytest.raises(returned_exception):
            redshift_operator.execute(None)
        assert mock_conn.pause_cluster.call_count == 10

    @mock.patch.object(RedshiftHook, "get_waiter")
    @mock.patch.object(RedshiftHook, "conn")
    def test_pause_cluster_wait_for_completion(self, mock_conn, mock_get_waiter):
        """Test Pause cluster operator with defer when deferrable param is true"""
        mock_conn.pause_cluster.return_value = True
        waiter = Mock()
        mock_get_waiter.return_value = waiter

        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", wait_for_completion=True
        )

        redshift_operator.execute(context=None)

        waiter.wait.assert_called_once()

    @mock.patch.object(RedshiftHook, "cluster_status")
    @mock.patch.object(RedshiftHook, "conn")
    def test_pause_cluster_deferrable_mode(self, mock_conn, mock_cluster_status):
        """Test Pause cluster operator with defer when deferrable param is true"""
        mock_conn.pause_cluster.return_value = True
        mock_cluster_status.return_value = "available"

        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", wait_for_completion=True, deferrable=True
        )

        with pytest.raises(TaskDeferred) as exc:
            redshift_operator.execute(context=None)

        assert isinstance(exc.value.trigger, RedshiftPauseClusterTrigger), (
            "Trigger is not a RedshiftPauseClusterTrigger"
        )

    @mock.patch("airflow.providers.amazon.aws.operators.redshift_cluster.RedshiftPauseClusterOperator.defer")
    @mock.patch.object(RedshiftHook, "cluster_status")
    @mock.patch.object(RedshiftHook, "conn")
    def test_pause_cluster_deferrable_mode_in_deleting_status(
        self, mock_conn, mock_cluster_status, mock_defer
    ):
        """Test Pause cluster operator with defer when deferrable param is true"""
        mock_conn.pause_cluster.return_value = True
        mock_cluster_status.return_value = "deleting"

        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", wait_for_completion=True, deferrable=True
        )

        with pytest.raises(AirflowException):
            redshift_operator.execute(context=None)
        assert not mock_defer.called

    def test_pause_cluster_execute_complete_success(self):
        """Asserts that logging occurs as expected"""
        task = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", deferrable=True
        )
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context=None, event={"status": "success"})
        mock_log_info.assert_called_with("Paused cluster successfully")

    def test_pause_cluster_execute_complete_fail(self):
        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", deferrable=True
        )

        with pytest.raises(AirflowException):
            redshift_operator.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    def test_template_fields(self):
        operator = RedshiftPauseClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
        )
        validate_template_fields(operator)


class TestDeleteClusterOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch.object(RedshiftHook, "conn")
    def test_delete_cluster_with_wait_for_completion(self, mock_conn, mock_cluster_status):
        mock_cluster_status.return_value = "cluster_not_found"
        redshift_operator = RedshiftDeleteClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_conn.delete_cluster.assert_called_once_with(
            ClusterIdentifier="test_cluster",
            SkipFinalClusterSnapshot=True,
            FinalClusterSnapshotIdentifier="",
        )

    @mock.patch.object(RedshiftHook, "conn")
    def test_delete_cluster_without_wait_for_completion(self, mock_conn):
        redshift_operator = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
            wait_for_completion=False,
        )
        redshift_operator.execute(None)
        mock_conn.delete_cluster.assert_called_once_with(
            ClusterIdentifier="test_cluster",
            SkipFinalClusterSnapshot=True,
            FinalClusterSnapshotIdentifier="",
        )

        mock_conn.cluster_status.assert_not_called()

    @mock.patch.object(RedshiftHook, "delete_cluster")
    @mock.patch.object(RedshiftHook, "conn")
    @mock.patch("time.sleep", return_value=None)
    def test_delete_cluster_multiple_attempts(self, _, mock_conn, mock_delete_cluster):
        exception = boto3.client("redshift").exceptions.InvalidClusterStateFault({}, "test")
        returned_exception = type(exception)
        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_delete_cluster.side_effect = [exception, exception, True]

        redshift_operator = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
            wait_for_completion=False,
        )
        redshift_operator.execute(None)

        assert mock_delete_cluster.call_count == 3

    @mock.patch.object(RedshiftHook, "delete_cluster")
    @mock.patch.object(RedshiftHook, "conn")
    @mock.patch("time.sleep", return_value=None)
    def test_delete_cluster_multiple_attempts_fail(self, _, mock_conn, mock_delete_cluster):
        exception = boto3.client("redshift").exceptions.InvalidClusterStateFault({}, "test")
        returned_exception = type(exception)
        mock_conn.exceptions.InvalidClusterStateFault = returned_exception
        mock_delete_cluster.side_effect = exception

        redshift_operator = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
            wait_for_completion=False,
            max_attempts=3,
        )
        with pytest.raises(returned_exception):
            redshift_operator.execute(None)

        assert mock_delete_cluster.call_count == 3

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.delete_cluster")
    def test_delete_cluster_deferrable_mode(self, mock_delete_cluster, mock_cluster_status):
        """When the delete is accepted, deferrable mode waits for deletion to complete."""
        mock_delete_cluster.return_value = True
        mock_cluster_status.return_value = "deleting"
        delete_cluster = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            deferrable=True,
            wait_for_completion=False,
        )

        with pytest.raises(TaskDeferred) as exc:
            delete_cluster.execute(context=None)

        assert isinstance(exc.value.trigger, RedshiftDeleteClusterTrigger), (
            "Trigger is not a RedshiftDeleteClusterTrigger"
        )
        # Delete is attempted exactly once (no synchronous busy-retry loop in deferrable mode).
        mock_delete_cluster.assert_called_once()

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.delete_cluster")
    def test_delete_cluster_deferrable_mode_already_gone(self, mock_delete_cluster, mock_cluster_status):
        """When the cluster is already gone after the delete, deferrable mode completes without deferring."""
        mock_delete_cluster.return_value = True
        mock_cluster_status.return_value = "cluster_not_found"
        delete_cluster = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            deferrable=True,
            wait_for_completion=False,
        )

        # No TaskDeferred is raised; the operator returns normally.
        delete_cluster.execute(context=None)
        mock_delete_cluster.assert_called_once()

    @mock.patch.object(RedshiftHook, "conn")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.delete_cluster")
    def test_delete_cluster_deferrable_mode_busy_defers_to_settle_trigger(
        self, mock_delete_cluster, mock_conn
    ):
        """A busy cluster (InvalidClusterStateFault) defers to the settle-wait trigger, not a sync loop."""
        exception = boto3.client("redshift").exceptions.InvalidClusterStateFault({}, "test")
        mock_conn.exceptions.InvalidClusterStateFault = type(exception)
        mock_delete_cluster.side_effect = exception

        delete_cluster = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            deferrable=True,
            wait_for_completion=False,
        )

        with pytest.raises(TaskDeferred) as exc:
            delete_cluster.execute(context=None)

        assert isinstance(exc.value.trigger, RedshiftClusterSettledTrigger), (
            "Trigger is not a RedshiftClusterSettledTrigger"
        )
        assert exc.value.method_name == "_retry_delete_when_settled"
        # Delete attempted once; the synchronous busy-retry loop never runs in deferrable mode.
        mock_delete_cluster.assert_called_once()

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.delete_cluster")
    def test_retry_delete_when_settled_reissues_delete(self, mock_delete_cluster, mock_cluster_status):
        """The settle-wait callback re-issues the delete and defers to the delete-complete trigger."""
        mock_delete_cluster.return_value = True
        mock_cluster_status.return_value = "deleting"
        delete_cluster = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            deferrable=True,
            wait_for_completion=False,
        )

        with pytest.raises(TaskDeferred) as exc:
            delete_cluster._retry_delete_when_settled(
                context=None, event={"status": "success", "cluster_identifier": "test_cluster"}
            )

        assert isinstance(exc.value.trigger, RedshiftDeleteClusterTrigger)
        mock_delete_cluster.assert_called_once()

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.delete_cluster")
    def test_retry_delete_when_settled_uses_cluster_id_from_event(
        self, mock_delete_cluster, mock_cluster_status
    ):
        """The re-issued delete targets the identifier from the trigger event, not the operator field."""
        mock_delete_cluster.return_value = True
        mock_cluster_status.return_value = "cluster_not_found"
        delete_cluster = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="rerendered_different_value",
            deferrable=True,
            wait_for_completion=False,
        )

        delete_cluster._retry_delete_when_settled(
            context=None, event={"status": "success", "cluster_identifier": "original_cluster"}
        )

        mock_delete_cluster.assert_called_once_with(
            cluster_identifier="original_cluster",
            skip_final_cluster_snapshot=True,
            final_cluster_snapshot_identifier=None,
        )

    @mock.patch.object(RedshiftHook, "conn")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.delete_cluster")
    def test_retry_delete_when_settled_redefers_on_race(self, mock_delete_cluster, mock_conn):
        """If the cluster re-enters a transitional state (race), the callback re-defers to settle-wait."""
        exception = boto3.client("redshift").exceptions.InvalidClusterStateFault({}, "test")
        mock_conn.exceptions.InvalidClusterStateFault = type(exception)
        mock_delete_cluster.side_effect = exception

        delete_cluster = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            deferrable=True,
            wait_for_completion=False,
        )

        with pytest.raises(TaskDeferred) as exc:
            delete_cluster._retry_delete_when_settled(
                context=None, event={"status": "success", "cluster_identifier": "test_cluster"}
            )

        assert isinstance(exc.value.trigger, RedshiftClusterSettledTrigger)
        assert exc.value.method_name == "_retry_delete_when_settled"

    def test_retry_delete_when_settled_error_event_raises(self):
        """A non-success event from the settle-wait trigger raises AirflowException."""
        delete_cluster = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            deferrable=True,
            wait_for_completion=False,
        )
        with pytest.raises(AirflowException):
            delete_cluster._retry_delete_when_settled(
                context=None, event={"status": "error", "message": "timed out"}
            )

    @mock.patch.object(RedshiftHook, "delete_cluster")
    @mock.patch.object(RedshiftHook, "conn")
    @mock.patch("time.sleep", return_value=None)
    def test_delete_cluster_sync_mode_still_uses_busy_retry_loop(
        self, mock_sleep, mock_conn, mock_delete_cluster
    ):
        """Sync mode (deferrable=False) must keep the synchronous busy-retry loop unchanged."""
        exception = boto3.client("redshift").exceptions.InvalidClusterStateFault({}, "test")
        mock_conn.exceptions.InvalidClusterStateFault = type(exception)
        mock_delete_cluster.side_effect = [exception, exception, True]

        redshift_operator = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
            deferrable=False,
            wait_for_completion=False,
        )
        redshift_operator.execute(None)

        # Three synchronous attempts, and time.sleep was used between the retries.
        assert mock_delete_cluster.call_count == 3
        assert mock_sleep.called

    def test_delete_cluster_execute_complete_success(self):
        """Asserts that logging occurs as expected"""
        task = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            deferrable=True,
            wait_for_completion=False,
        )
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(context=None, event={"status": "success", "message": "Cluster deleted"})
        mock_log_info.assert_called_with("Cluster deleted successfully")

    def test_delete_cluster_execute_complete_fail(self):
        redshift_operator = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            deferrable=True,
            wait_for_completion=False,
        )

        with pytest.raises(AirflowException):
            redshift_operator.execute_complete(
                context=None, event={"status": "error", "message": "test failure message"}
            )

    def test_template_fields(self):
        operator = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
        )
        validate_template_fields(operator)
