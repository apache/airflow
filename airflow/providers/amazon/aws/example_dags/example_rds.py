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

from datetime import datetime

from airflow import DAG
from airflow.providers.amazon.aws.operators.rds import (
    RdsCancelExportTaskOperator,
    RdsCopyDbSnapshotOperator,
    RdsCreateDbSnapshotOperator,
    RdsCreateEventSubscriptionOperator,
    RdsDeleteDbSnapshotOperator,
    RdsDeleteEventSubscriptionOperator,
    RdsStartExportTaskOperator,
)
from airflow.providers.amazon.aws.sensors.rds import RdsExportTaskExistenceSensor, RdsSnapshotExistenceSensor

# [START rds_snapshots_howto_guide]
with DAG(
    dag_id='rds_snapshots', start_date=datetime(2021, 1, 1), schedule_interval=None, catchup=False
) as dag:
    # [START howto_guide_rds_create_snapshot]
    create_snapshot = RdsCreateDbSnapshotOperator(
        task_id='create_snapshot',
        db_type='instance',
        db_identifier='auth-db',
        db_snapshot_identifier='auth-db-snap',
        aws_conn_id='aws_default',
        hook_params={'region_name': 'us-east-1'},
    )
    # [END howto_guide_rds_create_snapshot]

    # [START howto_guide_rds_copy_snapshot]
    copy_snapshot = RdsCopyDbSnapshotOperator(
        task_id='copy_snapshot',
        db_type='instance',
        target_db_snapshot_identifier='auth-db-snap-backup',
        source_db_snapshot_identifier='auth-db-snap',
        aws_conn_id='aws_default',
        hook_params={'region_name': 'us-east-1'},
    )
    # [END howto_guide_rds_copy_snapshot]

    # [START howto_guide_rds_delete_snapshot]
    delete_snapshot = RdsDeleteDbSnapshotOperator(
        task_id='delete_snapshot',
        db_type='instance',
        db_snapshot_identifier='auth-db-snap-backup',
        aws_conn_id='aws_default',
        hook_params={'region_name': 'us-east-1'},
    )
    # [END howto_guide_rds_delete_snapshot]

    create_snapshot >> copy_snapshot >> delete_snapshot
# [END rds_snapshots_howto_guide]

# [START rds_exports_howto_guide]
with DAG(dag_id='rds_exports', start_date=datetime(2021, 1, 1), schedule_interval=None, catchup=False) as dag:
    # [START howto_guide_rds_start_export]
    start_export = RdsStartExportTaskOperator(
        task_id='start_export',
        export_task_identifier='export-auth-db-snap-{{ ds }}',
        source_arn='arn:aws:rds:<region>:<account number>:snapshot:auth-db-snap',
        s3_bucket_name='my_s3_bucket',
        s3_prefix='some/prefix',
        iam_role_arn='arn:aws:iam:<region>:<account number>:role/MyRole',
        kms_key_id='arn:aws:kms:<region>:<account number>:key/*****-****-****-****-********',
        aws_conn_id='aws_default',
        hook_params={'region_name': 'us-east-1'},
    )
    # [END howto_guide_rds_start_export]

    # [START howto_guide_rds_cancel_export]
    cancel_export = RdsCancelExportTaskOperator(
        task_id='cancel_export',
        export_task_identifier='export-auth-db-snap-{{ ds }}',
        aws_conn_id='aws_default',
        hook_params={'region_name': 'us-east-1'},
    )
    # [END howto_guide_rds_cancel_export]

    start_export >> cancel_export
# [END rds_exports_howto_guide]

# [START rds_events_howto_guide]
with DAG(dag_id='rds_events', start_date=datetime(2021, 1, 1), schedule_interval=None, catchup=False) as dag:
    # [START howto_guide_rds_create_subscription]
    create_subscription = RdsCreateEventSubscriptionOperator(
        task_id='create_subscription',
        subscription_name='my-topic-subscription',
        sns_topic_arn='arn:aws:sns:<region>:<account number>:MyTopic',
        source_type='db-instance',
        source_ids=['auth-db'],
        event_categories=['Availability', 'Backup'],
        aws_conn_id='aws_default',
        hook_params={'region_name': 'us-east-1'},
    )
    # [END howto_guide_rds_create_subscription]

    # [START howto_guide_rds_delete_subscription]
    delete_subscription = RdsDeleteEventSubscriptionOperator(
        task_id='delete_subscription',
        subscription_name='my-topic-subscription',
        aws_conn_id='aws_default',
        hook_params={'region_name': 'us-east-1'},
    )
    # [END howto_guide_rds_delete_subscription]

    create_subscription >> delete_subscription
# [END rds_events_howto_guide]

# [START rds_sensors_howto_guide]
with DAG(dag_id='rds_events', start_date=datetime(2021, 1, 1), schedule_interval=None, catchup=False) as dag:
    # [START howto_guide_rds_snapshot_sensor]
    snapshot_sensor = RdsSnapshotExistenceSensor(
        task_id='snapshot_sensor',
        db_type='instance',
        db_snapshot_identifier='auth-db-snap-{{ ds }}',
        target_statuses=['available'],
        aws_conn_id='aws_default',
        hook_params={'region_name': 'us-east-1'},
    )
    # [END howto_guide_rds_snapshot_sensor]

    # [START howto_guide_rds_export_sensor]
    export_sensor = RdsExportTaskExistenceSensor(
        task_id='export_sensor',
        export_task_identifier='export-auth-db-snap-{{ ds }}',
        target_statuses=['starting', 'in_progress', 'complete', 'canceling', 'canceled'],
        aws_conn_id='aws_default',
        hook_params={'region_name': 'us-east-1'},
    )
    # [END howto_guide_rds_export_sensor]
# [END rds_sensors_howto_guide]
