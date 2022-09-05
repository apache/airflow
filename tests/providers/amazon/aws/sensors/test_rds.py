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

import pytest

from airflow.exceptions import AirflowException
from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.sensors.rds import (
    RdsBaseSensor,
    RdsDbSensor,
    RdsExportTaskExistenceSensor,
    RdsSnapshotExistenceSensor,
)
from airflow.providers.amazon.aws.utils.rds import RdsDbType
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


def _create_db_instance(hook: RdsHook):
    hook.conn.create_db_instance(
        DBInstanceIdentifier=DB_INSTANCE_NAME,
        DBInstanceClass="db.t4g.micro",
        Engine="postgres",
    )
    if not hook.conn.describe_db_instances()["DBInstances"]:
        raise ValueError("AWS not properly mocked")


def _create_db_instance_snapshot(hook: RdsHook):
    _create_db_instance(hook)
    hook.conn.create_db_snapshot(
        DBInstanceIdentifier=DB_INSTANCE_NAME,
        DBSnapshotIdentifier=DB_INSTANCE_SNAPSHOT,
    )
    if not hook.conn.describe_db_snapshots()['DBSnapshots']:
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


def _create_db_cluster_snapshot(hook: RdsHook):
    _create_db_cluster(hook)
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
        IamRoleArn='arn:aws:iam:es-east-1::role/MyRole',
        KmsKeyId='arn:aws:kms:es-east-1::key/*****-****-****-****-********',
        S3BucketName='my-exports-bucket',
    )
    if not hook.conn.describe_export_tasks()['ExportTasks']:
        raise ValueError('AWS not properly mocked')


class TestBaseRdsSensor:
    dag = None
    base_sensor = None

    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.base_sensor = RdsBaseSensor(task_id='test_task', aws_conn_id='aws_default', dag=cls.dag)

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.base_sensor

    def test_hook_attribute(self):
        assert hasattr(self.base_sensor, 'hook')
        assert self.base_sensor.hook.__class__.__name__ == 'RdsHook'

    def test_describe_item_wrong_type(self):
        with pytest.raises(AirflowException):
            self.base_sensor._describe_item('database', 'auth-db')

    def test_check_item_true(self):
        self.base_sensor._describe_item = lambda item_type, item_name: [{'Status': 'available'}]
        self.base_sensor.target_statuses = ['available', 'created']

        assert self.base_sensor._check_item(item_type='instance_snapshot', item_name='')

    def test_check_item_false(self):
        self.base_sensor._describe_item = lambda item_type, item_name: [{'Status': 'creating'}]
        self.base_sensor.target_statuses = ['available', 'created']

        assert not self.base_sensor._check_item(item_type='instance_snapshot', item_name='')


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsSnapshotExistenceSensor:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_db_instance_snapshot_poke_true(self):
        _create_db_instance_snapshot(self.hook)
        op = RdsSnapshotExistenceSensor(
            task_id='test_instance_snap_true',
            db_type='instance',
            db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        assert op.poke(None)

    @mock_rds
    def test_db_instance_snapshot_poke_false(self):
        op = RdsSnapshotExistenceSensor(
            task_id='test_instance_snap_false',
            db_type='instance',
            db_snapshot_identifier=DB_INSTANCE_SNAPSHOT,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        assert not op.poke(None)

    @mock_rds
    def test_db_instance_cluster_poke_true(self):
        _create_db_cluster_snapshot(self.hook)
        op = RdsSnapshotExistenceSensor(
            task_id='test_cluster_snap_true',
            db_type='cluster',
            db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        assert op.poke(None)

    @mock_rds
    def test_db_instance_cluster_poke_false(self):
        op = RdsSnapshotExistenceSensor(
            task_id='test_cluster_snap_false',
            db_type='cluster',
            db_snapshot_identifier=DB_CLUSTER_SNAPSHOT,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        assert not op.poke(None)


@pytest.mark.skipif(mock_rds is None, reason='mock_rds package not present')
class TestRdsExportTaskExistenceSensor:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name='us-east-1')

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_export_task_poke_true(self):
        _create_db_instance_snapshot(self.hook)
        _start_export_task(self.hook)
        op = RdsExportTaskExistenceSensor(
            task_id='export_task_true',
            export_task_identifier=EXPORT_TASK_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        assert op.poke(None)

    @mock_rds
    def test_export_task_poke_false(self):
        _create_db_instance_snapshot(self.hook)
        op = RdsExportTaskExistenceSensor(
            task_id='export_task_false',
            export_task_identifier=EXPORT_TASK_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        assert not op.poke(None)


@pytest.mark.skipif(mock_rds is None, reason="mock_rds package not present")
class TestRdsDbSensor:
    @classmethod
    def setup_class(cls):
        cls.dag = DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
        cls.hook = RdsHook(aws_conn_id=AWS_CONN, region_name="us-east-1")

    @classmethod
    def teardown_class(cls):
        del cls.dag
        del cls.hook

    @mock_rds
    def test_poke_true_instance(self):
        """
        By default RdsDbSensor should wait for an instance to enter the 'available' state
        """
        _create_db_instance(self.hook)
        op = RdsDbSensor(
            task_id="instance_poke_true",
            db_identifier=DB_INSTANCE_NAME,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        assert op.poke(None)

    @mock_rds
    def test_poke_false_instance(self):
        _create_db_instance(self.hook)
        op = RdsDbSensor(
            task_id="instance_poke_false",
            db_identifier=DB_INSTANCE_NAME,
            target_statuses=["stopped"],
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        assert not op.poke(None)

    @mock_rds
    def test_poke_true_cluster(self):
        _create_db_cluster(self.hook)
        op = RdsDbSensor(
            task_id="cluster_poke_true",
            db_identifier=DB_CLUSTER_NAME,
            db_type=RdsDbType.CLUSTER,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        assert op.poke(None)

    @mock_rds
    def test_poke_false_cluster(self):
        _create_db_cluster(self.hook)
        op = RdsDbSensor(
            task_id="cluster_poke_false",
            db_identifier=DB_CLUSTER_NAME,
            target_statuses=["stopped"],
            db_type=RdsDbType.CLUSTER,
            aws_conn_id=AWS_CONN,
            dag=self.dag,
        )
        assert not op.poke(None)
