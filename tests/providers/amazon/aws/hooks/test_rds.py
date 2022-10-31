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

from unittest.mock import patch

import boto3
import pytest
from moto import mock_rds, mock_s3, mock_sns

from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.hooks.sns import SnsHook


@pytest.fixture
def rds_hook() -> RdsHook:
    """Returns an RdsHook whose underlying connection is mocked with moto"""
    with mock_rds():
        yield RdsHook(aws_conn_id="aws_default", region_name="us-east-1")


@pytest.fixture
def db_instance_id(rds_hook: RdsHook) -> str:
    """Creates an RDS DB instance and returns its id"""
    response = rds_hook.conn.create_db_instance(
        DBInstanceIdentifier="testrdshook-db-instance",
        DBInstanceClass="db.t4g.micro",
        Engine="postgres",
        AllocatedStorage=20,
        MasterUsername="testrdshook",
        MasterUserPassword="testrdshook",
    )
    return response["DBInstance"]["DBInstanceIdentifier"]


@pytest.fixture
def db_cluster_id(rds_hook: RdsHook) -> str:
    """Creates an RDS DB cluster and returns its id"""
    response = rds_hook.conn.create_db_cluster(
        DBClusterIdentifier="testrdshook-db-cluster",
        Engine="postgres",
        MasterUsername="testrdshook",
        MasterUserPassword="testrdshook",
        DBClusterInstanceClass="db.t4g.micro",
        AllocatedStorage=20,
    )
    return response["DBCluster"]["DBClusterIdentifier"]


@pytest.fixture
def db_snapshot(rds_hook: RdsHook, db_instance_id: str):
    response = rds_hook.conn.create_db_snapshot(
        DBSnapshotIdentifier="testrdshook-db-instance-snapshot", DBInstanceIdentifier=db_instance_id
    )
    return response["DBSnapshot"]


@pytest.fixture
def db_snapshot_id(db_snapshot: dict) -> str:
    return db_snapshot["DBSnapshotIdentifier"]


@pytest.fixture
def db_snapshot_arn(db_snapshot: dict) -> str:
    return db_snapshot["DBSnapshotArn"]


@pytest.fixture
def db_cluster_snapshot(rds_hook: RdsHook, db_cluster_id: str):
    response = rds_hook.conn.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="testrdshook-db-cluster-snapshot", DBClusterIdentifier=db_cluster_id
    )
    return response["DBClusterSnapshot"]


@pytest.fixture
def db_cluster_snapshot_id(db_cluster_snapshot) -> str:
    return db_cluster_snapshot["DBClusterSnapshotIdentifier"]


@pytest.fixture
def s3_bucket_name():
    s3 = boto3.resource("s3")
    bucket = "testrdshook-s3-bucket"
    s3.create_bucket(Bucket=bucket)
    return bucket


@pytest.fixture
def export_task(rds_hook: RdsHook, db_snapshot_arn: str, s3_bucket_name: str) -> str:
    response = rds_hook.conn.start_export_task(
        ExportTaskIdentifier="testrdshook-export-task",
        SourceArn=db_snapshot_arn,
        S3BucketName=s3_bucket_name,
        # The ARN of the IAM role to use for writing to the Amazon S3 bucket when exporting a snapshot.
        IamRoleArn="string",
        # The ID of the Amazon Web Services KMS key to use to encrypt the snapshot exported to Amazon S3.
        # The Amazon Web Services KMS key identifier is the key ARN, key ID, alias ARN, or alias name for the KMS key.
        # The caller of this operation must be authorized to execute the following operations.
        # These can be set in the Amazon Web Services KMS key policy:
        KmsKeyId="string",
    )
    return response["ExportTaskIdentifier"]


@pytest.fixture
def sns_hook() -> SnsHook:
    with mock_sns():
        yield SnsHook(aws_conn_id="aws_default")


@pytest.fixture
def sns_topic_arn(sns_hook: SnsHook) -> str:
    response = sns_hook.conn.create_topic(Name="testrdshook-sns-topic")
    return response["TopicArn"]


@pytest.fixture
def event_subscription_name(rds_hook: RdsHook, sns_topic_arn: str, db_instance_id: str) -> str:
    response = rds_hook.conn.create_event_subscription(
        SubscriptionName="testrdshook-event-subscription",
        SnsTopicArn=sns_topic_arn,
        SourceType="db-instance",
        SourceIds=[db_instance_id],
        Enabled=True,
    )
    return response["EventSubscription"]["CustSubscriptionId"]


class TestRdsHook:
    def test_conn_attribute(self):
        hook = RdsHook(aws_conn_id="aws_default", region_name="us-east-1")
        assert hasattr(hook, "conn")
        assert hook.conn.__class__.__name__ == "RDS"
        conn = hook.conn
        assert conn is hook.conn  # Cached property
        assert conn is hook.get_conn()  # Same object as returned by `conn` property

    def test_get_db_instance_state(self, rds_hook: RdsHook, db_instance_id: str):
        response = rds_hook.conn.describe_db_instances(DBInstanceIdentifier=db_instance_id)
        state_expected = response["DBInstances"][0]["DBInstanceStatus"]
        state_actual = rds_hook.get_db_instance_state(db_instance_id)
        assert state_actual == state_expected

    def test_wait_for_db_instance_state(self, rds_hook: RdsHook, db_instance_id: str):
        """
        The db instance waiter uses AWS provided boto waiters where possible, and falls back to a
        custom waiter implementation.

        it should call wait_on_state otherwise, which should also call get_db_snapshot_state (from poke closure)
        """
        common_kwargs = {"check_interval": 0, "max_attempts": 1}

        with patch.object(rds_hook.conn, "get_waiter") as mock:
            rds_hook.wait_for_db_instance_state(db_instance_id, target_state="available", **common_kwargs)
            mock.assert_called_once_with("db_instance_available")

        with patch.object(rds_hook.conn, "get_waiter") as mock:
            rds_hook.wait_for_db_instance_state(db_instance_id, target_state="deleted", **common_kwargs)
            mock.assert_called_once_with("db_instance_deleted")

        with patch.object(rds_hook, "_wait_on_state") as mock:
            rds_hook.wait_for_db_instance_state(db_instance_id, target_state="stopped", **common_kwargs)
            mock.assert_called_once()

        with patch.object(rds_hook, "get_db_instance_state", return_value="stopped") as mock:
            rds_hook.wait_for_db_instance_state(db_instance_id, target_state="stopped", **common_kwargs)
            mock.assert_called_once_with(db_instance_id)

    def test_get_db_cluster_state(self, rds_hook: RdsHook, db_cluster_id: str):
        response = rds_hook.conn.describe_db_clusters(DBClusterIdentifier=db_cluster_id)
        state_expected = response["DBClusters"][0]["Status"]
        state_actual = rds_hook.get_db_cluster_state(db_cluster_id)
        assert state_actual == state_expected

    def test_wait_for_db_cluster_state(self, rds_hook: RdsHook, db_cluster_id: str):
        """
        xxx
        """
        common_kwargs = {"check_interval": 0, "max_attempts": 1}

        with patch.object(rds_hook.conn, "get_waiter") as mock:
            rds_hook.wait_for_db_cluster_state(db_cluster_id, target_state="available", **common_kwargs)
            mock.assert_called_once_with("db_cluster_available")

        with patch.object(rds_hook.conn, "get_waiter") as mock:
            rds_hook.wait_for_db_cluster_state(db_cluster_id, target_state="deleted", **common_kwargs)
            mock.assert_called_once_with("db_cluster_deleted")

        with patch.object(rds_hook, "_wait_on_state") as mock:
            rds_hook.wait_for_db_cluster_state(db_cluster_id, target_state="stopped", **common_kwargs)
            mock.assert_called_once()

        # set the return value to stopped (the same as target state) so that the waiter returns immediately
        with patch.object(rds_hook, "get_db_cluster_state", return_value="stopped") as mock:
            rds_hook.wait_for_db_cluster_state(db_cluster_id, target_state="stopped", **common_kwargs)
            mock.assert_called_once_with(db_cluster_id)

    def test_get_db_snapshot_state(self, rds_hook: RdsHook, db_snapshot_id: str):
        response = rds_hook.conn.describe_db_snapshots(DBSnapshotIdentifier=db_snapshot_id)
        state_expected = response["DBSnapshots"][0]["Status"]
        state_actual = rds_hook.get_db_snapshot_state(db_snapshot_id)
        assert state_actual == state_expected

    def test_wait_for_db_snapshot_state(self, rds_hook: RdsHook, db_snapshot_id: str):
        """
        setting check_interval=0, max_attempts=1 so that we dont trigger time.sleep during test
        """
        common_kwargs = {"check_interval": 0, "max_attempts": 1}

        with patch.object(rds_hook.conn, "get_waiter") as mock:
            rds_hook.wait_for_db_snapshot_state(db_snapshot_id, target_state="available", **common_kwargs)
            mock.assert_called_once_with("db_snapshot_available")

        with patch.object(rds_hook.conn, "get_waiter") as mock:
            rds_hook.wait_for_db_snapshot_state(db_snapshot_id, target_state="deleted", **common_kwargs)
            mock.assert_called_once_with("db_snapshot_deleted")

        with patch.object(rds_hook.conn, "get_waiter") as mock:
            rds_hook.wait_for_db_snapshot_state(db_snapshot_id, target_state="completed", **common_kwargs)
            mock.assert_called_once_with("db_snapshot_completed")

        with patch.object(rds_hook, "_wait_on_state") as mock:
            rds_hook.wait_for_db_snapshot_state(db_snapshot_id, target_state="canceled", **common_kwargs)
            mock.assert_called_once()

        with patch.object(rds_hook, "get_db_snapshot_state", return_value="canceled") as mock:
            rds_hook.wait_for_db_snapshot_state(db_snapshot_id, target_state="canceled", **common_kwargs)
            mock.assert_called_once_with(db_snapshot_id)

    def test_get_db_cluster_snapshot_state(self, rds_hook: RdsHook, db_cluster_snapshot_id: str):
        response = rds_hook.conn.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=db_cluster_snapshot_id
        )
        state_expected = response["DBClusterSnapshots"][0]["Status"]
        state_actual = rds_hook.get_db_cluster_snapshot_state(db_cluster_snapshot_id)
        assert state_actual == state_expected

    def test_wait_for_db_cluster_snapshot_state(self, rds_hook: RdsHook, db_cluster_snapshot_id: str):
        """
        setting check_interval=0, max_attempts=1 so that we dont trigger time.sleep during test
        """
        common_kwargs = {"check_interval": 0, "max_attempts": 1}

        with patch.object(rds_hook.conn, "get_waiter") as mock:
            rds_hook.wait_for_db_cluster_snapshot_state(
                db_cluster_snapshot_id, target_state="available", **common_kwargs
            )
            mock.assert_called_once_with("db_cluster_snapshot_available")

        with patch.object(rds_hook.conn, "get_waiter") as mock:
            rds_hook.wait_for_db_cluster_snapshot_state(
                db_cluster_snapshot_id, target_state="deleted", **common_kwargs
            )
            mock.assert_called_once_with("db_cluster_snapshot_deleted")

        with patch.object(rds_hook, "_wait_on_state") as mock:
            rds_hook.wait_for_db_cluster_snapshot_state(
                db_cluster_snapshot_id, target_state="canceled", **common_kwargs
            )
            mock.assert_called_once()

        with patch.object(rds_hook, "get_db_cluster_snapshot_state", return_value="canceled") as mock:
            rds_hook.wait_for_db_cluster_snapshot_state(
                db_cluster_snapshot_id, target_state="canceled", **common_kwargs
            )
            mock.assert_called_once_with(db_cluster_snapshot_id)

    # def test_get_export_task_state(self, rds_hook: RdsHook, export_task_id: str):
    #     response = rds_hook.conn.describe_export_tasks(ExportTaskIdentifier=export_task_id)
    #     state_expected = response["ExportTasks"][0]["Status"]
    #     state_actual = rds_hook.get_export_task_state(export_task_id)
    #     assert state_actual == state_expected

    def test_get_event_subscription_state(self, rds_hook: RdsHook, event_subscription_name: str):
        response = rds_hook.conn.describe_event_subscriptions(SubscriptionName=event_subscription_name)
        state_expected = response["EventSubscriptionsList"][0]["Status"]
        state_actual = rds_hook.get_event_subscription_state(event_subscription_name)
        assert state_actual == state_expected

    def test_wait_for_event_subscription_state(self, rds_hook: RdsHook, event_subscription_name: str):
        """
        xxx
        """
        common_kwargs = {"check_interval": 0, "max_attempts": 1}

        with patch.object(rds_hook, "_wait_on_state") as mock:
            rds_hook.wait_for_event_subscription_state(
                event_subscription_name, target_state="active", **common_kwargs
            )
            mock.assert_called_once()

        with patch.object(rds_hook, "get_event_subscription_state", return_value="active") as mock:
            rds_hook.wait_for_event_subscription_state(
                event_subscription_name, target_state="active", **common_kwargs
            )
            mock.assert_called_once_with(event_subscription_name)

    def test_wait_on_state(self, rds_hook: RdsHook):
        def poke():
            return "foo"

        # todo: target "Exceeded max attempts" message
        with pytest.raises(AirflowException, match="Max attempts exceeded"):
            with patch("airflow.providers.amazon.aws.hooks.rds.time.sleep") as mock:
                rds_hook._wait_on_state(poke, target_state="bar", check_interval=0, max_attempts=2)
        # must run this outside of the pytest.raises context otherwise exits before executed
        mock.assert_called_once_with(0)
