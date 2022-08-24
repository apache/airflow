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
#
from unittest.mock import patch

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG
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
    RdsStartExportTaskOperator,
)
from airflow.utils import timezone

try:
    from moto import mock_rds
except ImportError:
    mock_rds = None


DEFAULT_DATE = timezone.datetime(2019, 1, 1)

AWS_CONN = 'amazon_default'

DB_INSTANCE_NAME = 'my-db-instance'
DB_CLUSTER_NAME = 'my-db-cluster'

DB_INSTANCE_SNAPSHOT = 'my-db-instance-snap'
DB_CLUSTER_SNAPSHOT = 'my-db-cluster-snap'

DB_INSTANCE_SNAPSHOT_COPY = 'my-db-instance-snap-copy'
DB_CLUSTER_SNAPSHOT_COPY = 'my-db-cluster-snap-copy'

EXPORT_TASK_NAME = 'my-db-instance-snap-export'
EXPORT_TASK_SOURCE = 'arn:aws:rds:es-east-1::snapshot:my-db-instance-snap'
EXPORT_TASK_ROLE_NAME = 'MyRole'
EXPORT_TASK_ROLE_ARN = 'arn:aws:iam:es-east-1::role/MyRole'
EXPORT_TASK_KMS = 'arn:aws:kms:es-east-1::key/*****-****-****-****-********'
EXPORT_TASK_BUCKET = 'my-exports-bucket'

SUBSCRIPTION_NAME = 'my-db-instance-subscription'
SUBSCRIPTION_TOPIC = 'arn:aws:sns:us-east-1::MyTopic'


def _create_db_instance(hook: RdsHook):
    hook.conn.create_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_NAME,
        DBInstanceClass='db.m4.large',
        Engine='postgres',
    )
    if not hook.conn.describe_db_instances()['DBInstances']:
        raise ValueError('AWS not properly mocked')


def _create_db_cluster(hook: RdsHook):
    hook.conn.create_db_cluster(
        DBClusterIdentifier=DB_CLUSTER_NAME,
        Engine='mysql',
        MasterUsername='admin',
        MasterUserPassword='admin-pass',
    )
    if not hook.conn.describe_db_clusters()['DBClusters']:
        raise ValueError('AWS not properly mocked')


def _create_db_instance_snapshot(hook: RdsHook):
    hook.conn.create_db_snapshot(
        DBInstanceIdentifier=DB_INSTANCE_NAME,
        DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT,
    )
    if not hook.conn.describe_db_snapshots()['DBSnapshots']:
        raise ValueError('AWS not properly mocked')


def _create_db_cluster_snapshot(hook: RdsHook):
    hook.conn.create_db_cluster_snapshot(
        DBClusterIdentifier=DB_CLUSTER_NAME,
        DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT,
    )
    if not hook.conn.describe_db_cluster_snapshots()['DBClusterSnapshots']:
        raise ValueError('AWS not properly mocked')


def _start_export_task(hook: RdsHook):
    hook.conn.start_export_task(
        ExportTaskIdentifier=EXPORT_TASK_NAME,
        SourceArn=EXPORT_TASK_SOURCE,
        IamRoleArn=EXPORT_TASK_ROLE_ARN,
        KmsKeyId=EXPORT_TASK_KMS,
        S3BucketName=EXPORT_TASK_BUCKET,
    )
    if not hook.conn.describe_export_tasks()['ExportTasks']:
        raise ValueError('AWS not properly mocked')


def _create_event_subscription(hook: RdsHook):
    hook.conn.create_event_subscription(
        SubscriptionName=SUBSCRIPTION_NAME,
        SnsTopicArn=SUBSCRIPTION_TOPIC,
        SourceType='db-instance',
        SourceIds=[DB_INSTANCE_NAME],
    )
    if not hook.conn.describe_event_subscriptions()['EventSubscriptionsList']:
        raise ValueError('AWS not properly mocked')


class TestBaseRdsOperator:
    dag = None
    op = None

    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.op = RdsBaseOperator(task_id='test_task', aws_conn_id='aws_default', dag=cls.dag)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.op

    def test_hook_attribute(self):
        assert hasattr(self.op, 'hook')
        assert self.op.hook.__class__.__name__ == 'RdsHook'

    def test_describe_item_wrong_type(self):
        with pytest.raises(AirflowException):
            self.op._describe_item('database', 'auth-db')

    def test_await_status_error(self):
        self.op._describe_item = lambda item_type, item_name: [{'Status': 'error'}]
        with pytest.raises(AirflowException):
            self.op._await_status(
                item_type='instance_snapshot',
                item_name='',
                wait_statuses=['wait'],
                error_statuses=['error'],
            )

    def test_await_status_ok(self):
        self.op._describe_item = lambda item_type, item_name: [{'Status': 'ok'}]
        self.op._await_status(
            item_type='instance_snapshot', item_name='', wait_statuses=['wait'], ok_statuses=['ok']
        )


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsCreateDbSnapshotOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_create_db_instance_snapshot(self):
        _create_db_instance(self.hook)
        instance_snapshot_operator = RdsCreateDbSnapshotOperator(
            task_id='test_instance',
            db_type='instance',
            db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            db_identifier=DB_INSTANCE_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        instance_snapshot_operator.execute(None)

        result = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT)
        instance_snapshots = result.get("DBSnapshots")

        assert instance_snapshots
        assert len(instance_snapshots) == 1

    @mock_rds
    @patch.object(RdsBaseOperator, '_await_status')
    def test_create_db_instance_snapshot_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)
        instance_snapshot_operator = RdsCreateDbSnapshotOperator(
            task_id='test_instance_no_wait',
            db_type='instance',
            db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            db_identifier=DB_INSTANCE_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        instance_snapshot_operator.execute(None)

        result = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT)
        instance_snapshots = result.get("DBSnapshots")

        assert instance_snapshots
        assert len(instance_snapshots) == 1
        assert mock_await_status.not_called()

    @mock_rds
    def test_create_db_cluster_snapshot(self):
        _create_db_cluster(self.hook)
        cluster_snapshot_operator = RdsCreateDbSnapshotOperator(
            task_id='test_cluster',
            db_type='cluster',
            db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            db_identifier=DB_CLUSTER_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        cluster_snapshot_operator.execute(None)

        result = self.hook.conn.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT)
        cluster_snapshots = result.get("DBClusterSnapshots")

        assert cluster_snapshots
        assert len(cluster_snapshots) == 1

    @mock_rds
    @patch.object(RdsBaseOperator, '_await_status')
    def test_create_db_cluster_snapshot_no_wait(self, mock_no_wait):
        _create_db_cluster(self.hook)
        cluster_snapshot_operator = RdsCreateDbSnapshotOperator(
            task_id='test_cluster_no_wait',
            db_type='cluster',
            db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            db_identifier=DB_CLUSTER_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        cluster_snapshot_operator.execute(None)

        result = self.hook.conn.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT)
        cluster_snapshots = result.get("DBClusterSnapshots")

        assert cluster_snapshots
        assert len(cluster_snapshots) == 1
        assert mock_no_wait.not_called()


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsCopyDbSnapshotOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_copy_db_instance_snapshot(self):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        instance_snapshot_operator = RdsCopyDbSnapshotOperator(
            task_id='test_instance',
            db_type='instance',
            source_db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            target_db_snapshot_identifier=DB_INSTANCE_SNAPSHOT_COPY,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        instance_snapshot_operator.execute(None)
        result = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT_COPY)
        instance_snapshots = result.get("DBSnapshots")

        assert instance_snapshots
        assert len(instance_snapshots) == 1

    @mock_rds
    @patch.object(RdsBaseOperator, '_await_status')
    def test_copy_db_instance_snapshot_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        instance_snapshot_operator = RdsCopyDbSnapshotOperator(
            task_id='test_instance_no_wait',
            db_type='instance',
            source_db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            target_db_snapshot_identifier=DB_INSTANCE_SNAPSHOT_COPY,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        instance_snapshot_operator.execute(None)
        result = self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT_COPY)
        instance_snapshots = result.get("DBSnapshots")

        assert instance_snapshots
        assert len(instance_snapshots) == 1
        assert mock_await_status.not_called()

    @mock_rds
    def test_copy_db_cluster_snapshot(self):
        _create_db_cluster(self.hook)
        _create_db_cluster_snapshot(self.hook)

        cluster_snapshot_operator = RdsCopyDbSnapshotOperator(
            task_id='test_cluster',
            db_type='cluster',
            source_db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            target_db_snapshot_identifier=DB_CLUSTER_SNAPSHOT_COPY,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        cluster_snapshot_operator.execute(None)
        result = self.hook.conn.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT_COPY
        )
        cluster_snapshots = result.get("DBClusterSnapshots")

        assert cluster_snapshots
        assert len(cluster_snapshots) == 1

    @mock_rds
    @patch.object(RdsBaseOperator, '_await_status')
    def test_copy_db_cluster_snapshot_no_wait(self, mock_await_status):
        _create_db_cluster(self.hook)
        _create_db_cluster_snapshot(self.hook)

        cluster_snapshot_operator = RdsCopyDbSnapshotOperator(
            task_id='test_cluster_no_wait',
            db_type='cluster',
            source_db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            target_db_snapshot_identifier=DB_CLUSTER_SNAPSHOT_COPY,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        cluster_snapshot_operator.execute(None)
        result = self.hook.conn.describe_db_cluster_snapshots(
            DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT_COPY
        )
        cluster_snapshots = result.get("DBClusterSnapshots")

        assert cluster_snapshots
        assert len(cluster_snapshots) == 1
        assert mock_await_status.not_called()


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsDeleteDbSnapshotOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_delete_db_instance_snapshot(self):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        instance_snapshot_operator = RdsDeleteDbSnapshotOperator(
            task_id='test_instance',
            db_type='instance',
            db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        instance_snapshot_operator.execute(None)

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_db_snapshots(DBSnapshotIdentifier=DB_CLUSTER_SNAPSHOT)

    @mock_rds
    def test_delete_db_cluster_snapshot(self):
        _create_db_cluster(self.hook)
        _create_db_cluster_snapshot(self.hook)

        cluster_snapshot_operator = RdsDeleteDbSnapshotOperator(
            task_id='test_cluster',
            db_type='cluster',
            db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        cluster_snapshot_operator.execute(None)

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_db_cluster_snapshots(DBClusterSnapshotIdentifier=DB_CLUSTER_SNAPSHOT)


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsStartExportTaskOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_start_export_task(self):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        start_export_operator = RdsStartExportTaskOperator(
            task_id='test_start',
            export_task_identifier=EXPORT_TASK_NAME,
            source_arn=EXPORT_TASK_SOURCE,
            iam_role_arn=EXPORT_TASK_ROLE_ARN,
            kms_key_id=EXPORT_TASK_KMS,
            s3_bucket_name=EXPORT_TASK_BUCKET,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        start_export_operator.execute(None)

        result = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_NAME)
        export_tasks = result.get("ExportTasks")

        assert export_tasks
        assert len(export_tasks) == 1
        assert export_tasks[0]['Status'] == 'complete'

    @mock_rds
    @patch.object(RdsBaseOperator, '_await_status')
    def test_start_export_task_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)

        start_export_operator = RdsStartExportTaskOperator(
            task_id='test_start_no_wait',
            export_task_identifier=EXPORT_TASK_NAME,
            source_arn=EXPORT_TASK_SOURCE,
            iam_role_arn=EXPORT_TASK_ROLE_ARN,
            kms_key_id=EXPORT_TASK_KMS,
            s3_bucket_name=EXPORT_TASK_BUCKET,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        start_export_operator.execute(None)

        result = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_NAME)
        export_tasks = result.get("ExportTasks")

        assert export_tasks
        assert len(export_tasks) == 1
        assert export_tasks[0]['Status'] == 'complete'
        assert mock_await_status.not_called()


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsCancelExportTaskOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

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
            task_id='test_cancel',
            export_task_identifier=EXPORT_TASK_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        cancel_export_operator.execute(None)

        result = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_NAME)
        export_tasks = result.get("ExportTasks")

        assert export_tasks
        assert len(export_tasks) == 1
        assert export_tasks[0]['Status'] == 'canceled'

    @mock_rds
    @patch.object(RdsBaseOperator, '_await_status')
    def test_cancel_export_task_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)
        _create_db_instance_snapshot(self.hook)
        _start_export_task(self.hook)

        cancel_export_operator = RdsCancelExportTaskOperator(
            task_id='test_cancel_no_wait',
            export_task_identifier=EXPORT_TASK_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        cancel_export_operator.execute(None)

        result = self.hook.conn.describe_export_tasks(ExportTaskIdentifier=EXPORT_TASK_NAME)
        export_tasks = result.get("ExportTasks")

        assert export_tasks
        assert len(export_tasks) == 1
        assert export_tasks[0]['Status'] == 'canceled'
        assert mock_await_status.not_called()


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsCreateEventSubscriptionOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_create_event_subscription(self):
        _create_db_instance(self.hook)

        create_subscription_operator = RdsCreateEventSubscriptionOperator(
            task_id='test_create',
            subscription_name=SUBSCRIPTION_NAME,
            sns_topic_arn=SUBSCRIPTION_TOPIC,
            source_type='db-instance',
            source_ids=[DB_INSTANCE_NAME],
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        create_subscription_operator.execute(None)

        result = self.hook.conn.describe_event_subscriptions(SubscriptionName=SUBSCRIPTION_NAME)
        subscriptions = result.get("EventSubscriptionsList")

        assert subscriptions
        assert len(subscriptions) == 1
        assert subscriptions[0]['Status'] == 'active'

    @mock_rds
    @patch.object(RdsBaseOperator, '_await_status')
    def test_create_event_subscription_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)

        create_subscription_operator = RdsCreateEventSubscriptionOperator(
            task_id='test_create_no_wait',
            subscription_name=SUBSCRIPTION_NAME,
            sns_topic_arn=SUBSCRIPTION_TOPIC,
            source_type='db-instance',
            source_ids=[DB_INSTANCE_NAME],
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        create_subscription_operator.execute(None)

        result = self.hook.conn.describe_event_subscriptions(SubscriptionName=SUBSCRIPTION_NAME)
        subscriptions = result.get("EventSubscriptionsList")

        assert subscriptions
        assert len(subscriptions) == 1
        assert subscriptions[0]['Status'] == 'active'
        assert mock_await_status.not_called()


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsDeleteEventSubscriptionOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_delete_event_subscription(self):
        _create_event_subscription(self.hook)

        delete_subscription_operator = RdsDeleteEventSubscriptionOperator(
            task_id='test_delete',
            subscription_name=SUBSCRIPTION_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        delete_subscription_operator.execute(None)

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_event_subscriptions(SubscriptionName=EXPORT_TASK_NAME)


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsCreateDbInstanceOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_create_db_instance(self):
        create_db_instance_operator = RdsCreateDbInstanceOperator(
            task_id='test_create_db_instance',
            db_instance_identifier=DB_INSTANCE_NAME,
            db_instance_class="db.m5.large",
            engine="postgres",
            rds_kwargs={
                "DBName": DB_INSTANCE_NAME,
            },
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        create_db_instance_operator.execute(None)

        result = self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        db_instances = result.get("DBInstances")

        assert db_instances
        assert len(db_instances) == 1
        assert db_instances[0]['DBInstanceStatus'] == 'available'

    @mock_rds
    @patch.object(RdsBaseOperator, '_await_status')
    def test_create_db_instance_no_wait(self, mock_await_status):
        create_db_instance_operator = RdsCreateDbInstanceOperator(
            task_id='test_create_db_instance_no_wait',
            db_instance_identifier=DB_INSTANCE_NAME,
            db_instance_class="db.m5.large",
            engine="postgres",
            rds_kwargs={
                "DBName": DB_INSTANCE_NAME,
            },
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        create_db_instance_operator.execute(None)

        result = self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        db_instances = result.get("DBInstances")

        assert db_instances
        assert len(db_instances) == 1
        assert db_instances[0]['DBInstanceStatus'] == 'available'
        assert mock_await_status.not_called()


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsDeleteDbInstanceOperator:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_delete_event_subscription(self):
        _create_db_instance(self.hook)

        delete_db_instance_operator = RdsDeleteDbInstanceOperator(
            task_id='test_delete_db_instance',
            db_instance_identifier=DB_INSTANCE_NAME,
            rds_kwargs={
                "SkipFinalSnapshot": True,
            },
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        delete_db_instance_operator.execute(None)

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)

    @mock_rds
    @patch.object(RdsBaseOperator, '_await_status')
    def test_delete_event_subscription_no_wait(self, mock_await_status):
        _create_db_instance(self.hook)

        delete_db_instance_operator = RdsDeleteDbInstanceOperator(
            task_id='test_delete_db_instance_no_wait',
            db_instance_identifier=DB_INSTANCE_NAME,
            rds_kwargs={
                "SkipFinalSnapshot": True,
            },
            aws_conn_id=AWS_CONN,
            dag=self.dag,
            wait_for_completion=False,
        )
        delete_db_instance_operator.execute(None)

        with pytest.raises(self.hook.conn.exceptions.ClientError):
            self.hook.conn.describe_db_instances(DBInstanceIdentifier=DB_INSTANCE_NAME)
        assert mock_await_status.not_called()
