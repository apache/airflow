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

from unittest import mock

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftCreateClusterOperator,
    RedshiftCreateClusterSnapshotOperator,
    RedshiftDeleteClusterOperator,
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)


class TestRedshiftCreateClusterOperator:
    def test_init(self):
        redshift_operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            node_type="dc2.large",
            master_username="adminuser",
            master_user_password="Test123$",
        )
        assert redshift_operator.task_id == "task_test"
        assert redshift_operator.cluster_identifier == "test_cluster"
        assert redshift_operator.node_type == "dc2.large"
        assert redshift_operator.master_username == "adminuser"
        assert redshift_operator.master_user_password == "Test123$"

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_conn")
    def test_create_single_node_cluster(self, mock_get_conn):
        redshift_operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test-cluster",
            node_type="dc2.large",
            master_username="adminuser",
            master_user_password="Test123$",
            cluster_type="single-node",
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
        mock_get_conn.return_value.create_cluster.assert_called_once_with(
            ClusterIdentifier='test-cluster',
            NodeType="dc2.large",
            MasterUsername="adminuser",
            MasterUserPassword="Test123$",
            **params,
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_conn")
    def test_create_multi_node_cluster(self, mock_get_conn):
        redshift_operator = RedshiftCreateClusterOperator(
            task_id="task_test",
            cluster_identifier="test-cluster",
            node_type="dc2.large",
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
        mock_get_conn.return_value.create_cluster.assert_called_once_with(
            ClusterIdentifier='test-cluster',
            NodeType="dc2.large",
            MasterUsername="adminuser",
            MasterUserPassword="Test123$",
            **params,
        )


class TestRedshiftCreateClusterSnapshotOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_conn")
    def test_create_cluster_snapshot_is_called_when_cluster_is_available(
        self, mock_get_conn, mock_cluster_status
    ):
        mock_cluster_status.return_value = "available"
        create_snapshot = RedshiftCreateClusterSnapshotOperator(
            task_id="test_snapshot",
            cluster_identifier="test_cluster",
            snapshot_identifier="test_snapshot",
            retention_period=1,
        )
        create_snapshot.execute(None)
        mock_get_conn.return_value.create_cluster_snapshot.assert_called_once_with(
            ClusterIdentifier='test_cluster',
            SnapshotIdentifier="test_snapshot",
            ManualSnapshotRetentionPeriod=1,
        )

        mock_get_conn.return_value.get_waiter.assert_not_called()

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    def test_raise_exception_when_cluster_is_not_available(self, mock_cluster_status):
        mock_cluster_status.return_value = "paused"
        create_snapshot = RedshiftCreateClusterSnapshotOperator(
            task_id="test_snapshot", cluster_identifier="test_cluster", snapshot_identifier="test_snapshot"
        )
        with pytest.raises(AirflowException):
            create_snapshot.execute(None)

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_conn")
    def test_create_cluster_snapshot_with_wait(self, mock_get_conn, mock_cluster_status):
        mock_cluster_status.return_value = "available"
        create_snapshot = RedshiftCreateClusterSnapshotOperator(
            task_id="test_snapshot",
            cluster_identifier="test_cluster",
            snapshot_identifier="test_snapshot",
            wait_for_completion=True,
        )
        create_snapshot.execute(None)
        mock_get_conn.return_value.get_waiter.return_value.wait.assert_called_once_with(
            ClusterIdentifier="test_cluster",
            SnapshotIdentifier="test_snapshot",
            WaiterConfig={"Delay": 15, "MaxAttempts": 20},
        )


class TestResumeClusterOperator:
    def test_init(self):
        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        assert redshift_operator.task_id == "task_test"
        assert redshift_operator.cluster_identifier == "test_cluster"
        assert redshift_operator.aws_conn_id == "aws_conn_test"

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_conn")
    def test_resume_cluster_is_called_when_cluster_is_paused(self, mock_get_conn, mock_cluster_status):
        mock_cluster_status.return_value = 'paused'
        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_get_conn.return_value.resume_cluster.assert_called_once_with(ClusterIdentifier='test_cluster')

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_conn")
    def test_resume_cluster_not_called_when_cluster_is_not_paused(self, mock_get_conn, mock_cluster_status):
        mock_cluster_status.return_value = 'available'
        redshift_operator = RedshiftResumeClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_get_conn.return_value.resume_cluster.assert_not_called()


class TestPauseClusterOperator:
    def test_init(self):
        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        assert redshift_operator.task_id == "task_test"
        assert redshift_operator.cluster_identifier == "test_cluster"
        assert redshift_operator.aws_conn_id == "aws_conn_test"

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_conn")
    def test_pause_cluster_is_called_when_cluster_is_available(self, mock_get_conn, mock_cluster_status):
        mock_cluster_status.return_value = 'available'
        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_get_conn.return_value.pause_cluster.assert_called_once_with(ClusterIdentifier='test_cluster')

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_conn")
    def test_pause_cluster_not_called_when_cluster_is_not_available(self, mock_get_conn, mock_cluster_status):
        mock_cluster_status.return_value = 'paused'
        redshift_operator = RedshiftPauseClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_get_conn.return_value.pause_cluster.assert_not_called()


class TestDeleteClusterOperator:
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.cluster_status")
    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_conn")
    def test_delete_cluster_with_wait_for_completion(self, mock_get_conn, mock_cluster_status):
        mock_cluster_status.return_value = 'cluster_not_found'
        redshift_operator = RedshiftDeleteClusterOperator(
            task_id="task_test", cluster_identifier="test_cluster", aws_conn_id="aws_conn_test"
        )
        redshift_operator.execute(None)
        mock_get_conn.return_value.delete_cluster.assert_called_once_with(
            ClusterIdentifier='test_cluster',
            SkipFinalClusterSnapshot=True,
            FinalClusterSnapshotIdentifier='',
        )

    @mock.patch("airflow.providers.amazon.aws.hooks.redshift_cluster.RedshiftHook.get_conn")
    def test_delete_cluster_without_wait_for_completion(self, mock_get_conn):
        redshift_operator = RedshiftDeleteClusterOperator(
            task_id="task_test",
            cluster_identifier="test_cluster",
            aws_conn_id="aws_conn_test",
            wait_for_completion=False,
        )
        redshift_operator.execute(None)
        mock_get_conn.return_value.delete_cluster.assert_called_once_with(
            ClusterIdentifier='test_cluster',
            SkipFinalClusterSnapshot=True,
            FinalClusterSnapshotIdentifier='',
        )

        mock_get_conn.return_value.cluster_status.assert_not_called()
