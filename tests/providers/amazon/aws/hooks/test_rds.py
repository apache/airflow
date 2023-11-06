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

from typing import TYPE_CHECKING, Generator
from unittest.mock import patch

import pytest
from moto import mock_rds

from airflow.exceptions import AirflowException, AirflowNotFoundException
from airflow.providers.amazon.aws.hooks.rds import RdsHook

if TYPE_CHECKING:
    from mypy_boto3_rds.type_defs import DBSnapshotTypeDef


@pytest.fixture
def rds_hook() -> Generator[RdsHook, None, None]:
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
def db_snapshot(rds_hook: RdsHook, db_instance_id: str) -> DBSnapshotTypeDef:
    """
    Creates a mock DB instance snapshot and returns the DBSnapshot dict from the boto response object.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.create_db_snapshot
    """
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
    """
    Creates a mock DB cluster snapshot and returns the DBClusterSnapshot dict from the boto response object.
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/rds.html#RDS.Client.create_db_cluster_snapshot
    """
    response = rds_hook.conn.create_db_cluster_snapshot(
        DBClusterSnapshotIdentifier="testrdshook-db-cluster-snapshot", DBClusterIdentifier=db_cluster_id
    )
    return response["DBClusterSnapshot"]


@pytest.fixture
def db_cluster_snapshot_id(db_cluster_snapshot) -> str:
    return db_cluster_snapshot["DBClusterSnapshotIdentifier"]


@pytest.fixture
def export_task_id(rds_hook: RdsHook, db_snapshot_arn: str) -> str:
    response = rds_hook.conn.start_export_task(
        ExportTaskIdentifier="testrdshook-export-task",
        SourceArn=db_snapshot_arn,
        S3BucketName="test",
        IamRoleArn="test",
        KmsKeyId="test",
    )
    return response["ExportTaskIdentifier"]


@pytest.fixture
def event_subscription_name(rds_hook: RdsHook, db_instance_id: str) -> str:
    """Creates an mock RDS event subscription and returns its name"""
    response = rds_hook.conn.create_event_subscription(
        SubscriptionName="testrdshook-event-subscription",
        SnsTopicArn="test",
        SourceType="db-instance",
        SourceIds=[db_instance_id],
        Enabled=True,
    )
    return response["EventSubscription"]["CustSubscriptionId"]


class TestRdsHook:
    # For testing, set the delay between status checks to 0 so that we aren't sleeping during tests,
    # and max_attempts to 1 so that we don't retry unless required.
    waiter_args = {"check_interval": 0, "max_attempts": 1}

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

    def test_wait_for_db_instance_state_boto_waiters(self, rds_hook: RdsHook, db_instance_id: str):
        """Checks that the DB instance waiter uses AWS boto waiters where possible"""
        for state in ("available", "deleted"):
            with patch.object(rds_hook.conn, "get_waiter") as mock:
                rds_hook.wait_for_db_instance_state(db_instance_id, target_state=state, **self.waiter_args)
                mock.assert_called_once_with(f"db_instance_{state}")
                mock.return_value.wait.assert_called_once_with(
                    DBInstanceIdentifier=db_instance_id,
                    WaiterConfig={
                        "MaxAttempts": self.waiter_args["max_attempts"],
                    },
                )

    def test_wait_for_db_instance_state_custom_waiter(self, rds_hook: RdsHook, db_instance_id: str):
        """Checks that the DB instance waiter uses custom wait logic when AWS boto waiters aren't available"""
        with patch.object(rds_hook, "_wait_for_state") as mock:
            rds_hook.wait_for_db_instance_state(db_instance_id, target_state="stopped", **self.waiter_args)
            mock.assert_called_once()

        with patch.object(rds_hook, "get_db_instance_state", return_value="stopped") as mock:
            rds_hook.wait_for_db_instance_state(db_instance_id, target_state="stopped", **self.waiter_args)
            mock.assert_called_once_with(db_instance_id)

    def test_get_db_cluster_state(self, rds_hook: RdsHook, db_cluster_id: str):
        response = rds_hook.conn.describe_db_clusters(DBClusterIdentifier=db_cluster_id)
        state_expected = response["DBClusters"][0]["Status"]
        state_actual = rds_hook.get_db_cluster_state(db_cluster_id)
        assert state_actual == state_expected

    def test_wait_for_db_cluster_state_boto_waiters(self, rds_hook: RdsHook, db_cluster_id: str):
        """Checks that the DB cluster waiter uses AWS boto waiters where possible"""
        for state in ("available", "deleted"):
            with patch.object(rds_hook.conn, "get_waiter") as mock:
                rds_hook.wait_for_db_cluster_state(db_cluster_id, target_state=state, **self.waiter_args)
                mock.assert_called_once_with(f"db_cluster_{state}")
                mock.return_value.wait.assert_called_once_with(
                    DBClusterIdentifier=db_cluster_id,
                    WaiterConfig={
                        "Delay": self.waiter_args["check_interval"],
                        "MaxAttempts": self.waiter_args["max_attempts"],
                    },
                )

    def test_wait_for_db_cluster_state_custom_waiter(self, rds_hook: RdsHook, db_cluster_id: str):
        """Checks that the DB cluster waiter uses custom wait logic when AWS boto waiters aren't available"""
        with patch.object(rds_hook, "_wait_for_state") as mock_wait_for_state:
            rds_hook.wait_for_db_cluster_state(db_cluster_id, target_state="stopped", **self.waiter_args)
            mock_wait_for_state.assert_called_once()

        with patch.object(rds_hook, "get_db_cluster_state", return_value="stopped") as mock:
            rds_hook.wait_for_db_cluster_state(db_cluster_id, target_state="stopped", **self.waiter_args)
            mock.assert_called_once_with(db_cluster_id)

    def test_get_db_snapshot_state(self, rds_hook: RdsHook, db_snapshot_id: str):
        response = rds_hook.conn.describe_db_snapshots(DBSnapshotIdentifier=db_snapshot_id)
        state_expected = response["DBSnapshots"][0]["Status"]
        state_actual = rds_hook.get_db_snapshot_state(db_snapshot_id)
        assert state_actual == state_expected

    def test_get_db_snapshot_state_not_found(self, rds_hook: RdsHook):
        with pytest.raises(AirflowNotFoundException):
            rds_hook.get_db_snapshot_state("does_not_exist")

    def test_wait_for_db_snapshot_state_boto_waiters(self, rds_hook: RdsHook, db_snapshot_id: str):
        """Checks that the DB snapshot waiter uses AWS boto waiters where possible"""
        for state in ("available", "deleted", "completed"):
            with patch.object(rds_hook.conn, "get_waiter") as mock:
                rds_hook.wait_for_db_snapshot_state(db_snapshot_id, target_state=state, **self.waiter_args)
                mock.assert_called_once_with(f"db_snapshot_{state}")
                mock.return_value.wait.assert_called_once_with(
                    DBSnapshotIdentifier=db_snapshot_id,
                    WaiterConfig={
                        "Delay": self.waiter_args["check_interval"],
                        "MaxAttempts": self.waiter_args["max_attempts"],
                    },
                )

    def test_wait_for_db_snapshot_state_custom_waiter(self, rds_hook: RdsHook, db_snapshot_id: str):
        """Checks that the DB snapshot waiter uses custom wait logic when AWS boto waiters aren't available"""
        with patch.object(rds_hook, "_wait_for_state") as mock:
            rds_hook.wait_for_db_snapshot_state(db_snapshot_id, target_state="canceled", **self.waiter_args)
            mock.assert_called_once()

        with patch.object(rds_hook, "get_db_snapshot_state", return_value="canceled") as mock:
            rds_hook.wait_for_db_snapshot_state(db_snapshot_id, target_state="canceled", **self.waiter_args)
            mock.assert_called_once_with(db_snapshot_id)

    def test_get_db_cluster_snapshot_state(self, rds_hook: RdsHook, db_cluster_snapshot_id: str):
        response = rds_hook.conn.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=db_cluster_snapshot_id
        )
        state_expected = response["DBClusterSnapshots"][0]["Status"]
        state_actual = rds_hook.get_db_cluster_snapshot_state(db_cluster_snapshot_id)
        assert state_actual == state_expected

    def test_get_db_cluster_snapshot_state_not_found(self, rds_hook: RdsHook):
        with pytest.raises(AirflowNotFoundException):
            rds_hook.get_db_cluster_snapshot_state("does_not_exist")

    def test_wait_for_db_cluster_snapshot_state_boto_waiters(
        self, rds_hook: RdsHook, db_cluster_snapshot_id: str
    ):
        """Checks that the DB cluster snapshot waiter uses AWS boto waiters where possible"""
        for state in ("available", "deleted"):
            with patch.object(rds_hook.conn, "get_waiter") as mock:
                rds_hook.wait_for_db_cluster_snapshot_state(
                    db_cluster_snapshot_id, target_state=state, **self.waiter_args
                )
                mock.assert_called_once_with(f"db_cluster_snapshot_{state}")
                mock.return_value.wait.assert_called_once_with(
                    DBClusterSnapshotIdentifier=db_cluster_snapshot_id,
                    WaiterConfig={
                        "Delay": self.waiter_args["check_interval"],
                        "MaxAttempts": self.waiter_args["max_attempts"],
                    },
                )

    def test_wait_for_db_cluster_snapshot_state_custom_waiter(
        self, rds_hook: RdsHook, db_cluster_snapshot_id: str
    ):
        """
        Checks that the DB cluster snapshot waiter uses custom wait logic when AWS boto waiters
        aren't available
        """
        with patch.object(rds_hook, "_wait_for_state") as mock:
            rds_hook.wait_for_db_cluster_snapshot_state(
                db_cluster_snapshot_id, target_state="canceled", **self.waiter_args
            )
            mock.assert_called_once()

        with patch.object(rds_hook, "get_db_cluster_snapshot_state", return_value="canceled") as mock:
            rds_hook.wait_for_db_cluster_snapshot_state(
                db_cluster_snapshot_id, target_state="canceled", **self.waiter_args
            )
            mock.assert_called_once_with(db_cluster_snapshot_id)

    def test_get_export_task_state(self, rds_hook: RdsHook, export_task_id: str):
        response = rds_hook.conn.describe_export_tasks(ExportTaskIdentifier=export_task_id)
        state_expected = response["ExportTasks"][0]["Status"]
        state_actual = rds_hook.get_export_task_state(export_task_id)
        assert state_actual == state_expected

    def test_get_export_task_state_not_found(self, rds_hook: RdsHook):
        with pytest.raises(AirflowNotFoundException):
            rds_hook.get_export_task_state("does_not_exist")

    def test_wait_for_export_task_state(self, rds_hook: RdsHook, export_task_id: str):
        """
        Checks that the export task waiter uses custom wait logic (no boto waiters exist for this resource)
        """
        with patch.object(rds_hook, "_wait_for_state") as mock:
            rds_hook.wait_for_export_task_state(export_task_id, target_state="complete", **self.waiter_args)
            mock.assert_called_once()

        with patch.object(rds_hook, "get_export_task_state", return_value="complete") as mock:
            rds_hook.wait_for_export_task_state(export_task_id, target_state="complete", **self.waiter_args)
            mock.assert_called_once_with(export_task_id)

    def test_get_event_subscription_state(self, rds_hook: RdsHook, event_subscription_name: str):
        response = rds_hook.conn.describe_event_subscriptions(SubscriptionName=event_subscription_name)
        state_expected = response["EventSubscriptionsList"][0]["Status"]
        state_actual = rds_hook.get_event_subscription_state(event_subscription_name)
        assert state_actual == state_expected

    def test_get_event_subscription_state_not_found(self, rds_hook: RdsHook):
        with pytest.raises(AirflowNotFoundException):
            rds_hook.get_event_subscription_state("does_not_exist")

    def test_wait_for_event_subscription_state(self, rds_hook: RdsHook, event_subscription_name: str):
        """
        Checks that the event subscription waiter uses custom wait logic (no boto waiters
        exist for this resource)
        """
        with patch.object(rds_hook, "_wait_for_state") as mock:
            rds_hook.wait_for_event_subscription_state(
                event_subscription_name, target_state="active", **self.waiter_args
            )
            mock.assert_called_once()

        with patch.object(rds_hook, "get_event_subscription_state", return_value="active") as mock:
            rds_hook.wait_for_event_subscription_state(
                event_subscription_name, target_state="active", **self.waiter_args
            )
            mock.assert_called_once_with(event_subscription_name)

    def test_wait_for_state(self, rds_hook: RdsHook):
        def poke():
            return "foo"

        with pytest.raises(AirflowException, match="Max attempts exceeded"):
            with patch("airflow.providers.amazon.aws.hooks.rds.time.sleep") as mock:
                rds_hook._wait_for_state(poke, target_state="bar", check_interval=0, max_attempts=2)
        # This next line should exist outside of the pytest.raises() context manager or else it won't
        # get executed
        mock.assert_called_once_with(0)
