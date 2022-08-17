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
    RdsCopyDbSnapshotOperator,
    RdsCreateDbSnapshotOperator,
    RdsDeleteDbSnapshotOperator,
)
from airflow.providers.amazon.aws.sensors.rds import RdsSnapshotExistenceSensor
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = 'example_rds_snapshot'

sys_test_context_task = SystemTestContextBuilder().build()


@task
def create_rds_instance(db_name, instance_name) -> None:
    rds_client = RdsHook().get_conn()
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

    rds_db_name = f'{test_context[ENV_ID_KEY]}_db'
    rds_instance_name = f'{test_context[ENV_ID_KEY]}-instance'
    rds_snapshot_name = f'{test_context[ENV_ID_KEY]}-snapshot'
    rds_snapshot_copy_name = f'{rds_snapshot_name}-copy'

    # [START howto_operator_rds_create_db_snapshot]
    create_snapshot = RdsCreateDbSnapshotOperator(
        task_id='create_snapshot',
        db_type='instance',
        db_identifier=rds_instance_name,
        db_snapshot_identifier=rds_snapshot_name,
    )
    # [END howto_operator_rds_create_db_snapshot]

    # [START howto_sensor_rds_snapshot_existence]
    snapshot_sensor = RdsSnapshotExistenceSensor(
        task_id='snapshot_sensor',
        db_type='instance',
        db_snapshot_identifier=rds_snapshot_name,
        target_statuses=['available'],
    )
    # [END howto_sensor_rds_snapshot_existence]

    # [START howto_operator_rds_copy_snapshot]
    copy_snapshot = RdsCopyDbSnapshotOperator(
        task_id='copy_snapshot',
        db_type='instance',
        source_db_snapshot_identifier=rds_snapshot_name,
        target_db_snapshot_identifier=rds_snapshot_copy_name,
    )
    # [END howto_operator_rds_copy_snapshot]

    # [START howto_operator_rds_delete_snapshot]
    delete_snapshot = RdsDeleteDbSnapshotOperator(
        task_id='delete_snapshot',
        db_type='instance',
        db_snapshot_identifier=rds_snapshot_name,
    )
    # [END howto_operator_rds_delete_snapshot]

    snapshot_copy_sensor = RdsSnapshotExistenceSensor(
        task_id='snapshot_copy_sensor',
        db_type='instance',
        db_snapshot_identifier=rds_snapshot_copy_name,
        target_statuses=['available'],
    )

    delete_snapshot_copy = RdsDeleteDbSnapshotOperator(
        task_id='delete_snapshot_copy',
        db_type='instance',
        db_snapshot_identifier=rds_snapshot_copy_name,
    )

    chain(
        # TEST SETUP
        test_context,
        create_rds_instance(rds_db_name, rds_instance_name),
        # TEST BODY
        create_snapshot,
        snapshot_sensor,
        copy_snapshot,
        delete_snapshot,
        # TEST TEARDOWN
        snapshot_copy_sensor,
        delete_snapshot_copy,
        delete_rds_instance(rds_instance_name),
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
