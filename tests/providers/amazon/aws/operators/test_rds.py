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

import logging
from unittest.mock import patch

import pytest
from moto import mock_rds

from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.base_aws import AwsGenericHook
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.operators.rds import (
    RdsBaseOperator,
    RdsCancelExportTaskOperator,
    RdsCopyDbSnapshotOperator,
    RdsCreateDbInstanceOperator,
    RdsCreateDbSnapshotOperator,
    RdsCreateEventSubscriptionOperator,
    RdsDeleteDbInstanceOperator,
    RdsDeleteDbSnapshotOperator,
    RdsDeleteEventSubscriptionOperator,
    RdsStartDbOperator,
    RdsStartExportTaskOperator,
    RdsStopDbOperator,
)
from airflow.utils import timezone

DEFAULT_DATE = timezone.datetime(2019, 1, 1)

AWS_CONN = "amazon_default"

DB_INSTANCE_NAME = "my-db-instance"
DB_CLUSTER_NAME = "my-db-cluster"

DB_INSTANCE_SNAPSHOT = "my-db-instance-snap"
DB_CLUSTER_SNAPSHOT = "my-db-cluster-snap"

DB_INSTANCE_SNAPSHOT_COPY = "my-db-instance-snap-copy"
DB_CLUSTER_SNAPSHOT_COPY = "my-db-cluster-snap-copy"

EXPORT_TASK_NAME = "my-db-instance-snap-export"
EXPORT_TASK_SOURCE = "arn:aws:rds:es-east-1::snapshot:my-db-instance-snap"
EXPORT_TASK_ROLE_NAME = "MyRole"
EXPORT_TASK_ROLE_ARN = "arn:aws:iam:es-east-1::role/MyRole"
EXPORT_TASK_KMS = "arn:aws:kms:es-east-1::key/*****-****-****-****-********"
EXPORT_TASK_BUCKET = "my-exports-bucket"

SUBSCRIPTION_NAME = "my-db-instance-subscription"
SUBSCRIPTION_TOPIC = "arn:aws:sns:us-east-1::MyTopic"


def _create_db_instance(hook: RdsHook):
    hook.conn.create_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_NAME,
        DBInstanceClass="db.m4.large",
        Engine="postgres",
    )
    if not hook.conn.describe_db_instances()["DBInstances"]:
        raise ValueError("AWS not properly mocked")


def _create_db_cluster(hook: RdsHook):
    hook.conn.create_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_NAME,
        Engine="mysql",
        MasterUsername="admin",
        MasterUserPassword="admin-pass",
    )
    if not hook.conn.describe_db_clusters()["DBClusters"]:
        raise ValueError("AWS not properly mocked")


def _create_db_instance_snapshot(hook: RdsHook):
    hook.conn.create_db_snapshot(
        DBInstanceIdentifier=DB_INSTANCE_NAME,
        DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT,
    )
    if not hook.conn.describe_db_snapshots()["DBSnapshots"]:
        raise ValueError("AWS not properly mocked")


def _create_db_cluster_snapshot(hook: RdsHook):
    hook.conn.create_db_cluster_snapshot(
        DBClusterIdentifier=DB_CLUSTER_NAME,
        DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT,
    )
    if not hook.conn.describe_db_cluster_snapshots()["DBClusterSnapshots"]:
        raise ValueError("AWS not properly mocked")


def _start_export_task(hook: RdsHook):
    hook.conn.start_export_task(
        ExportTaskIdentifier=EXPORT_TASK_NAME,
        SourceArn=EXPORT_TASK_SOURCE,
        IamRoleArn=EXPORT_TASK_ROLE_ARN,
        KmsKeyId=EXPORT_TASK_KMS,
        S3BucketName=EXPORT_TASK_BUCKET,
    )
    if not hook.conn.describe_export_tasks()["ExportTasks"]:
        raise ValueError("AWS not properly mocked")


def _create_event_subscription(hook: RdsHook):
    hook.conn.create_event_subscription(
        SubscriptionName=SUBSCRIPTION_NAME,
        SnsTopicArn=SUBSCRIPTION_TOPIC,
        SourceType="db-instance",
        SourceIds=[DB_INSTANCE_NAME],
    )
    if not hook.conn.describe_event_subscriptions()["EventSubscriptionsList"]:
        raise ValueError("AWS not properly mocked")


def _patch_hook_get_connection(hook: AwsGenericHook) -> None:
    # We're mocking all actual AWS calls and don't need a connection. This
    # avoids an Airflow warning about connection cannot be found.
    hook.get_connection = lambda _: None


class TestBaseRdsOperator:
    dag = None
    op = None

    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.op = RdsBaseOperator(task_id="test_task", aws_conn_id="aws_default", dag=cls.dag)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.op

    def test_hook_attribute(self):
        assert hasattr(self.op, "hook")
        assert self.op.hook.__class__.__name__ == "RdsHook"


class TestRdsCreateDbSnapshotOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_create_db_instance_snapshot(self):
        _create_db_instance(self.hook)
        instance_snapshot_operator = RdsCreateDbSnapshotOperator(
            task_id="test_instance",
            db_type="instance",
            db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            db_identifier=DB_INSTANCE_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(instance_snapshot_operator.hook)
        instance_snapshot_operator.execute(None)

        result = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT)
        instance_snapshots = result.get("DBSnapshots")

        assert instance_snapshots
        assert len(instance_snapshots) == 1

    @mock_rds
    @patch.object(RdsHook, "wait_for_db_snapshot_state")
    def test_create_db_instance_snapshot_no_wait(self, mock_wait):
        _create_db_instance(self.hook)
        instance_snapshot_operator = RdsCreateDbSnapshotOperator(
            task_id="test_instance_no_wait",
            db_type="instance",
            db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            db_identifier=DB_INSTANCE_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        _patch_hook_get_connection(instance_snapshot_operator.hook)
        instance_snapshot_operator.execute(None)

        result = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT)
        instance_snapshots = result.get("DBSnapshots")

        assert instance_snapshots
        assert len(instance_snapshots) == 1
        mock_wait.assert_not_called()

    @mock_rds
    def test_create_db_cluster_snapshot(self):
        _create_db_cluster(self.hook)
        cluster_snapshot_operator = RdsCreateDbSnapshotOperator(
            task_id="test_cluster",
            db_type="cluster",
            db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            db_identifier=DB_CLUSTER_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(cluster_snapshot_operator.hook)
        cluster_snapshot_operator.execute(None)

        result = self.hook.conn.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT)
        cluster_snapshots = result.get("DBClusterSnapshots")

        assert cluster_snapshots
        assert len(cluster_snapshots) == 1

    @mock_rds
    @patch.object(RdsHook, "wait_for_db_cluster_snapshot_state")
    def test_create_db_cluster_snapshot_no_wait(self, mock_wait):
        _create_db_cluster(self.hook)
        cluster_snapshot_operator = RdsCreateDbSnapshotOperator(
            task_id="test_cluster_no_wait",
            db_type="cluster",
            db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            db_identifier=DB_CLUSTER_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        _patch_hook_get_connection(cluster_snapshot_operator.hook)
        cluster_snapshot_operator.execute(None)

        result = self.hook.conn.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT)
        cluster_snapshots = result.get("DBClusterSnapshots")

        assert cluster_snapshots
        assert len(cluster_snapshots) == 1
        mock_wait.assert_not_called()


class TestRdsCopyDbSnapshotOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_copy_db_instance_snapshot(self):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        instance_snapshot_operator = RdsCopyDbSnapshotOperator(
            task_id="test_instance",
            db_type="instance",
            source_db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            target_db_snapshot_identifier=DB_INSTANCE_SNAPSHOT_COPY,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(instance_snapshot_operator.hook)
        instance_snapshot_operator.execute(None)
        result = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT_COPY)
        instance_snapshots = result.get("DBSnapshots")

        assert instance_snapshots
        assert len(instance_snapshots) == 1

    @mock_rds
    @patch.object(RdsHook, "wait_for_db_snapshot_state")
    def test_copy_db_instance_snapshot_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        instance_snapshot_operator = RdsCopyDbSnapshotOperator(
            task_id="test_instance_no_wait",
            db_type="instance",
            source_db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            target_db_snapshot_identifier=DB_INSTANCE_SNAPSHOT_COPY,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        _patch_hook_get_connection(instance_snapshot_operator.hook)
        instance_snapshot_operator.execute(None)
        result = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT_COPY)
        instance_snapshots = result.get("DBSnapshots")

        assert instance_snapshots
        assert len(instance_snapshots) == 1
        mock_await_status.assert_not_called()

    @mock_rds
    def test_copy_db_cluster_snapshot(self):
        _create_db_cluster(self.hook)
        _create_db_cluster_snapshot(self.hook)

        cluster_snapshot_operator = RdsCopyDbSnapshotOperator(
            task_id="test_cluster",
            db_type="cluster",
            source_db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            target_db_snapshot_identifier=DB_CLUSTER_SNAPSHOT_COPY,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(cluster_snapshot_operator.hook)
        cluster_snapshot_operator.execute(None)
        result = self.hook.conn.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT_COPY
        )
        cluster_snapshots = result.get("DBClusterSnapshots")

        assert cluster_snapshots
        assert len(cluster_snapshots) == 1

    @mock_rds
    @patch.object(RdsHook, "wait_for_db_snapshot_state")
    def test_copy_db_cluster_snapshot_no_wait(self, mock_await_status):
        _create_db_cluster(self.hook)
        _create_db_cluster_snapshot(self.hook)

        cluster_snapshot_operator = RdsCopyDbSnapshotOperator(
            task_id="test_cluster_no_wait",
            db_type="cluster",
            source_db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            target_db_snapshot_identifier=DB_CLUSTER_SNAPSHOT_COPY,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(cluster_snapshot_operator.hook)
        cluster_snapshot_operator.execute(None)
        result = self.hook.conn.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT_COPY
        )
        cluster_snapshots = result.get("DBClusterSnapshots")

        assert cluster_snapshots
        assert len(cluster_snapshots) == 1
        mock_await_status.assert_not_called()


class TestRdsDeleteDbSnapshotOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_delete_db_instance_snapshot(self):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        instance_snapshot_operator = RdsDeleteDbSnapshotOperator(
            task_id="test_instance",
            db_type="instance",
            db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(instance_snapshot_operator.hook)
        with patch.object(instance_snapshot_operator.hook, "wait_for_db_snapshot_state") as mock_wait:
            instance_snapshot_operator.execute(None)
        mock_wait.assert_called_once_with(DB_INSTANCE_SNAPSHOT, target_state="deleted")

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT)

    @mock_rds
    def test_delete_db_instance_snapshot_no_wait(self):
        """
        Check that the operator does not wait for the DB instance snapshot delete operation to complete when
        wait_for_completion=False
        """
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        instance_snapshot_operator = RdsDeleteDbSnapshotOperator(
            task_id="test_delete_db_instance_snapshot_no_wait",
            db_type="instance",
            db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        _patch_hook_get_connection(instance_snapshot_operator.hook)
        with patch.object(instance_snapshot_operator.hook, "wait_for_db_snapshot_state") as mock_wait:
            instance_snapshot_operator.execute(None)
        mock_wait.assert_not_called()

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT)

    @mock_rds
    def test_delete_db_cluster_snapshot(self):
        _create_db_cluster(self.hook)
        _create_db_cluster_snapshot(self.hook)

        cluster_snapshot_operator = RdsDeleteDbSnapshotOperator(
            task_id="test_cluster",
            db_type="cluster",
            db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(cluster_snapshot_operator.hook)
        with patch.object(cluster_snapshot_operator.hook, "wait_for_db_cluster_snapshot_state") as mock_wait:
            cluster_snapshot_operator.execute(None)
        mock_wait.assert_called_once_with(DB_CLUSTER_SNAPSHOT, target_state="deleted")

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT)

    @mock_rds
    def test_delete_db_cluster_snapshot_no_wait(self):
        """
        Check that the operator does not wait for the DB cluster snapshot delete operation to complete when
        wait_for_completion=False
        """
        _create_db_cluster(self.hook)
        _create_db_cluster_snapshot(self.hook)

        cluster_snapshot_operator = RdsDeleteDbSnapshotOperator(
            task_id="test_delete_db_cluster_snapshot_no_wait",
            db_type="cluster",
            db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        _patch_hook_get_connection(cluster_snapshot_operator.hook)
        with patch.object(cluster_snapshot_operator.hook, "wait_for_db_cluster_snapshot_state") as mock_wait:
            cluster_snapshot_operator.execute(None)
        mock_wait.assert_not_called()

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT)


class TestRdsStartExportTaskOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_start_export_task(self):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        start_export_operator = RdsStartExportTaskOperator(
            task_id="test_start",
            export_task_identifier=EXPORT_TASK_NAME,
            source_arn=EXPORT_TASK_SOURCE,
            iam_role_arn=EXPORT_TASK_ROLE_ARN,
            kms_key_id=EXPORT_TASK_KMS,
            s3_bucket_name=EXPORT_TASK_BUCKET,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(start_export_operator.hook)
        start_export_operator.execute(None)

        result = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_NAME)
        export_tasks = result.get("ExportTasks")

        assert export_tasks
        assert len(export_tasks) == 1
        assert export_tasks[0]["Status"] == "complete"

    @mock_rds
    @patch.object(RdsHook, "wait_for_export_task_state")
    def test_start_export_task_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        start_export_operator = RdsStartExportTaskOperator(
            task_id="test_start_no_wait",
            export_task_identifier=EXPORT_TASK_NAME,
            source_arn=EXPORT_TASK_SOURCE,
            iam_role_arn=EXPORT_TASK_ROLE_ARN,
            kms_key_id=EXPORT_TASK_KMS,
            s3_bucket_name=EXPORT_TASK_BUCKET,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        _patch_hook_get_connection(start_export_operator.hook)
        start_export_operator.execute(None)

        result = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_NAME)
        export_tasks = result.get("ExportTasks")

        assert export_tasks
        assert len(export_tasks) == 1
        assert export_tasks[0]["Status"] == "complete"
        mock_await_status.assert_not_called()


class TestRdsCancelExportTaskOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_cancel_export_task(self):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)
        _start_export_task(self.hook)

        cancel_export_operator = RdsCancelExportTaskOperator(
            task_id="test_cancel",
            export_task_identifier=EXPORT_TASK_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(cancel_export_operator.hook)
        cancel_export_operator.execute(None)

        result = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_NAME)
        export_tasks = result.get("ExportTasks")

        assert export_tasks
        assert len(export_tasks) == 1
        assert export_tasks[0]["Status"] == "canceled"

    @mock_rds
    @patch.object(RdsHook, "wait_for_export_task_state")
    def test_cancel_export_task_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)
        _start_export_task(self.hook)

        cancel_export_operator = RdsCancelExportTaskOperator(
            task_id="test_cancel_no_wait",
            export_task_identifier=EXPORT_TASK_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        _patch_hook_get_connection(cancel_export_operator.hook)
        cancel_export_operator.execute(None)

        result = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_NAME)
        export_tasks = result.get("ExportTasks")

        assert export_tasks
        assert len(export_tasks) == 1
        assert export_tasks[0]["Status"] == "canceled"
        mock_await_status.assert_not_called()


class TestRdsCreateEventSubscriptionOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_create_event_subscription(self):
        _create_db_instance(self.hook)

        create_subscription_operator = RdsCreateEventSubscriptionOperator(
            task_id="test_create",
            subscription_name=SUBSCRIPTION_NAME,
            sns_topic_arn=SUBSCRIPTION_TOPIC,
            source_type="db-instance",
            source_ids=[DB_INSTANCE_NAME],
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(create_subscription_operator.hook)
        create_subscription_operator.execute(None)

        result = self.hook.conn.describe_event_subscriptions(SubscriptionName=SUBSCRIPTION_NAME)
        subscriptions = result.get("EventSubscriptionsList")

        assert subscriptions
        assert len(subscriptions) == 1
        assert subscriptions[0]["Status"] == "active"

    @mock_rds
    @patch.object(RdsHook, "wait_for_event_subscription_state")
    def test_create_event_subscription_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)

        create_subscription_operator = RdsCreateEventSubscriptionOperator(
            task_id="test_create_no_wait",
            subscription_name=SUBSCRIPTION_NAME,
            sns_topic_arn=SUBSCRIPTION_TOPIC,
            source_type="db-instance",
            source_ids=[DB_INSTANCE_NAME],
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        _patch_hook_get_connection(create_subscription_operator.hook)
        create_subscription_operator.execute(None)

        result = self.hook.conn.describe_event_subscriptions(SubscriptionName=SUBSCRIPTION_NAME)
        subscriptions = result.get("EventSubscriptionsList")

        assert subscriptions
        assert len(subscriptions) == 1
        assert subscriptions[0]["Status"] == "active"
        mock_await_status.assert_not_called()


class TestRdsDeleteEventSubscriptionOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_delete_event_subscription(self):
        _create_event_subscription(self.hook)

        delete_subscription_operator = RdsDeleteEventSubscriptionOperator(
            task_id="test_delete",
            subscription_name=SUBSCRIPTION_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(delete_subscription_operator.hook)
        delete_subscription_operator.execute(None)

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_event_subscriptions(SubscriptionName=EXPORT_TASK_NAME)


class TestRdsCreateDbInstanceOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_create_db_instance(self):
        create_db_instance_operator = RdsCreateDbInstanceOperator(
            task_id="test_create_db_instance",
            db_instance_identifier=DB_INSTANCE_NAME,
            db_instance_class="db.m5.large",
            engine="postgres",
            rds_kwargs={
                "DBName": DB_INSTANCE_NAME,
            },
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(create_db_instance_operator.hook)
        create_db_instance_operator.execute(None)

        result = self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        db_instances = result.get("DBInstances")

        assert db_instances
        assert len(db_instances) == 1
        assert db_instances[0]["DBInstanceStatus"] == "available"

    @mock_rds
    @patch.object(RdsHook, "wait_for_db_instance_state")
    def test_create_db_instance_no_wait(self, mock_await_status):
        create_db_instance_operator = RdsCreateDbInstanceOperator(
            task_id="test_create_db_instance_no_wait",
            db_instance_identifier=DB_INSTANCE_NAME,
            db_instance_class="db.m5.large",
            engine="postgres",
            rds_kwargs={
                "DBName": DB_INSTANCE_NAME,
            },
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        _patch_hook_get_connection(create_db_instance_operator.hook)
        create_db_instance_operator.execute(None)

        result = self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        db_instances = result.get("DBInstances")

        assert db_instances
        assert len(db_instances) == 1
        assert db_instances[0]["DBInstanceStatus"] == "available"
        mock_await_status.assert_not_called()


class TestRdsDeleteDbInstanceOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_delete_db_instance(self):
        _create_db_instance(self.hook)

        delete_db_instance_operator = RdsDeleteDbInstanceOperator(
            task_id="test_delete_db_instance",
            db_instance_identifier=DB_INSTANCE_NAME,
            rds_kwargs={
                "SkipFinalSnapshot": True,
            },
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        _patch_hook_get_connection(delete_db_instance_operator.hook)
        delete_db_instance_operator.execute(None)

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)

    @mock_rds
    @patch.object(RdsHook, "wait_for_db_instance_state")
    def test_delete_db_instance_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)

        delete_db_instance_operator = RdsDeleteDbInstanceOperator(
            task_id="test_delete_db_instance_no_wait",
            db_instance_identifier=DB_INSTANCE_NAME,
            rds_kwargs={
                "SkipFinalSnapshot": True,
            },
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        _patch_hook_get_connection(delete_db_instance_operator.hook)
        delete_db_instance_operator.execute(None)

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        mock_await_status.assert_not_called()


class TestRdsStopDbOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    @patch.object(RdsHook, "wait_for_db_instance_state")
    def test_stop_db_instance(self, mock_await_status):
        _create_db_instance(self.hook)
        stop_db_instance = RdsStopDbOperator(task_id="test_stop_db_instance", db_identifier=DB_INSTANCE_NAME)
        _patch_hook_get_connection(stop_db_instance.hook)
        stop_db_instance.execute(None)
        result = self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        status = result["DBInstances"][0]["DBInstanceStatus"]
        assert status == "stopped"
        mock_await_status.assert_called()

    @mock_rds
    @patch.object(RdsHook, "wait_for_db_instance_state")
    def test_stop_db_instance_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)
        stop_db_instance = RdsStopDbOperator(
            task_id="test_stop_db_instance_no_wait", db_identifier=DB_INSTANCE_NAME, wait_for_completion=False
        )
        _patch_hook_get_connection(stop_db_instance.hook)
        stop_db_instance.execute(None)
        result = self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        status = result["DBInstances"][0]["DBInstanceStatus"]
        assert status == "stopped"
        mock_await_status.assert_not_called()

    @mock_rds
    def test_stop_db_instance_create_snapshot(self):
        _create_db_instance(self.hook)
        stop_db_instance = RdsStopDbOperator(
            task_id="test_stop_db_instance_create_snapshot",
            db_identifier=DB_INSTANCE_NAME,
            db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
        )
        _patch_hook_get_connection(stop_db_instance.hook)
        stop_db_instance.execute(None)

        describe_result = self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        status = describe_result["DBInstances"][0]["DBInstanceStatus"]
        assert status == "stopped"

        snapshot_result = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT)
        instance_snapshots = snapshot_result.get("DBSnapshots")
        assert instance_snapshots
        assert len(instance_snapshots) == 1

    @mock_rds
    @patch.object(RdsHook, "wait_for_db_cluster_state")
    def test_stop_db_cluster(self, mock_await_status):
        _create_db_cluster(self.hook)
        stop_db_cluster = RdsStopDbOperator(
            task_id="test_stop_db_cluster", db_identifier=DB_CLUSTER_NAME, db_type="cluster"
        )
        _patch_hook_get_connection(stop_db_cluster.hook)
        stop_db_cluster.execute(None)

        describe_result = self.hook.conn.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_NAME)
        status = describe_result["DBClusters"][0]["Status"]
        assert status == "stopped"
        mock_await_status.assert_called()

    @mock_rds
    def test_stop_db_cluster_create_snapshot_logs_warning_message(self, caplog):
        _create_db_cluster(self.hook)
        stop_db_cluster = RdsStopDbOperator(
            task_id="test_stop_db_cluster",
            db_identifier=DB_CLUSTER_NAME,
            db_type="cluster",
            db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
        )
        _patch_hook_get_connection(stop_db_cluster.hook)
        with caplog.at_level(logging.WARNING, logger=stop_db_cluster.log.name):
            stop_db_cluster.execute(None)
        warning_message = (
            "'db_snapshot_identifier' does not apply to db clusters. Remove it to silence this warning."
        )
        assert warning_message in caplog.text


class TestRdsStartDbOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")
        _patch_hook_get_connection(cls.hook)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_start_db_instance(self):
        _create_db_instance(self.hook)
        self.hook.conn.stop_db_instance(DBInstanceIdentifier=DB_INSTANCE_NAME)
        result_before = self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        status_before = result_before["DBInstances"][0]["DBInstanceStatus"]
        assert status_before == "stopped"

        start_db_instance = RdsStartDbOperator(
            task_id="test_start_db_instance", db_identifier=DB_INSTANCE_NAME
        )
        _patch_hook_get_connection(start_db_instance.hook)
        start_db_instance.execute(None)

        result_after = self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        status_after = result_after["DBInstances"][0]["DBInstanceStatus"]
        assert status_after == "available"

    @mock_rds
    def test_start_db_cluster(self):
        _create_db_cluster(self.hook)
        self.hook.conn.stop_db_cluster(DBClusterIdentifier=DB_CLUSTER_NAME)
        result_before = self.hook.conn.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_NAME)
        status_before = result_before["DBClusters"][0]["Status"]
        assert status_before == "stopped"

        start_db_cluster = RdsStartDbOperator(
            task_id="test_start_db_cluster", db_identifier=DB_CLUSTER_NAME, db_type="cluster"
        )
        _patch_hook_get_connection(start_db_cluster.hook)
        start_db_cluster.execute(None)

        result_after = self.hook.conn.describe_db_clusters(DBClusterIdentifier=DB_CLUSTER_NAME)
        status_after = result_after["DBClusters"][0]["Status"]
        assert status_after == "available"
