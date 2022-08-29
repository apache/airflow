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
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.rds import RdsHook
from airflow.providers.amazon.aws.operators.rds import (
    RdsCancelExportTaskOperator,
    RdsCreateDbSnapshotOperator,
    RdsDeleteDbSnapshotOperator,
    RdsStartExportTaskOperator,
)
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.sensors.rds import RdsExportTaskExistenceSensor, RdsSnapshotExistenceSensor
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = 'example_rds_export'

# Externally fetched variables:
KMS_KEY_ID_KEY = 'KMS_KEY_ID'
ROLE_ARN_KEY = 'ROLE_ARN'

sys_test_context_task = (
    SystemTestContextBuilder().add_variable(KMS_KEY_ID_KEY).add_variable(ROLE_ARN_KEY).build()
)


@task
def create_rds_instance(db_name: str, instance_name: str) -> None:
    rds_client = RdsHook().conn
    rds_client.create_db_instance(
        DBName=db_name,
        DBInstanceIdentifier=instance_name,
        AllocatedStorage=20,
        DBInstanceClass='db.t3.micro',
        Engine='postgres',
        MasterUsername='username',
        # NEVER store your production password in plaintext in a DAG like this.
        # Use Airflow Secrets or a secret manager for this in production.
        MasterUserPassword='rds_password',
    )

    rds_client.get_waiter('db_instance_available').wait(DBInstanceIdentifier=instance_name)


@task
def get_snapshot_arn(snapshot_name: str) -> str:
    result = RdsHook().conn.describe_db_snapshots(DBSnapshotIdentifier=snapshot_name)
    return result['DBSnapshots'][0]['DBSnapshotArn']


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_rds_instance(instance_name) -> None:
    rds_client = RdsHook().get_conn()
    rds_client.delete_db_instance(
        DBInstanceIdentifier=instance_name,
        SkipFinalSnapshot=True,
    )

    rds_client.get_waiter('db_instance_deleted').wait(DBInstanceIdentifier=instance_name)


with DAG(
    dag_id=DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    tags=['example'],
    catchup=False,
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]

    bucket_name: str = f'{env_id}-bucket'

    rds_db_name: str = f'{env_id}_db'
    rds_instance_name: str = f'{env_id}-instance'
    rds_snapshot_name: str = f'{env_id}-snapshot'
    rds_export_task_id: str = f'{env_id}-export-task'

    create_bucket = S3CreateBucketOperator(
        task_id='create_bucket',
        bucket_name=bucket_name,
    )

    create_snapshot = RdsCreateDbSnapshotOperator(
        task_id='create_snapshot',
        db_type='instance',
        db_identifier=rds_instance_name,
        db_snapshot_identifier=rds_snapshot_name,
    )

    await_snapshot = RdsSnapshotExistenceSensor(
        task_id='snapshot_sensor',
        db_type='instance',
        db_snapshot_identifier=rds_snapshot_name,
        target_statuses=['available'],
    )

    snapshot_arn = get_snapshot_arn(rds_snapshot_name)

    # [START howto_operator_rds_start_export_task]
    start_export = RdsStartExportTaskOperator(
        task_id='start_export',
        export_task_identifier=rds_export_task_id,
        source_arn=snapshot_arn,
        s3_bucket_name=bucket_name,
        s3_prefix='rds-test',
        iam_role_arn=test_context[ROLE_ARN_KEY],
        kms_key_id=test_context[KMS_KEY_ID_KEY],
        # Waits by default, set False to test the CancelExportTaskOperator below
        wait_for_completion=False,
    )
    # [END howto_operator_rds_start_export_task]

    # [START howto_operator_rds_cancel_export]
    cancel_export = RdsCancelExportTaskOperator(
        task_id='cancel_export',
        export_task_identifier=rds_export_task_id,
    )
    # [END howto_operator_rds_cancel_export]

    # [START howto_sensor_rds_export_task_existence]
    export_sensor = RdsExportTaskExistenceSensor(
        task_id='export_sensor',
        export_task_identifier=rds_export_task_id,
        target_statuses=['canceled'],
    )
    # [END howto_sensor_rds_export_task_existence]

    delete_snapshot = RdsDeleteDbSnapshotOperator(
        task_id='delete_snapshot',
        trigger_rule=TriggerRule.ALL_DONE,
        db_type='instance',
        db_snapshot_identifier=rds_snapshot_name,
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id='delete_bucket',
        trigger_rule=TriggerRule.ALL_DONE,
        bucket_name=bucket_name,
        force_delete=True,
    )

    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        create_rds_instance(rds_db_name, rds_instance_name),
        create_snapshot,
        await_snapshot,
        snapshot_arn,
        # TEST BODY
        start_export,
        cancel_export,
        export_sensor,
        # TEST TEARDOWN
        delete_snapshot,
        delete_bucket,
        delete_rds_instance(rds_instance_name),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
