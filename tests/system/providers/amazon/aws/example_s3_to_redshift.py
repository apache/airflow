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

from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.decorators import task
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftCreateClusterOperator,
    RedshiftDeleteClusterOperator,
)
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests.system.providers.amazon.aws.utils.redshift import create_connection
from tests.system.providers.amazon.aws.utils.security_group import (
    create_security_group,
    delete_security_group,
)

DAG_ID = 'example_s3_to_redshift'

sys_test_context_task = SystemTestContextBuilder().build()
S3_BUCKET_NAME = getenv("S3_BUCKET_NAME", "s3_bucket_name")
S3_KEY = getenv("S3_KEY", "s3_filename")
REDSHIFT_TABLE = getenv("REDSHIFT_TABLE", "redshift_table")
REDSHIFT_DB_LOGIN = 'adminuser'
REDSHIFT_DB_PASS = 'MyAmazonPassword1'
REDSHIFT_DB_NAME = 'dev'
REDSHIFT_POLL_INTERVAL = 10

IP_PERMISSION = {
    'FromPort': -1,
    'IpProtocol': 'All',
    'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'Test description'}],
}


@task(task_id='setup__add_sample_data_to_s3')
def task_add_sample_data_to_s3():
    s3_hook = S3Hook()
    s3_hook.load_string("0,Airflow", f'{S3_KEY}/{REDSHIFT_TABLE}', S3_BUCKET_NAME, replace=True)


@task(task_id='teardown__remove_sample_data_from_s3')
def task_remove_sample_data_from_s3():
    s3_hook = S3Hook()
    if s3_hook.check_for_key(f'{S3_KEY}/{REDSHIFT_TABLE}', S3_BUCKET_NAME):
        s3_hook.delete_objects(S3_BUCKET_NAME, f'{S3_KEY}/{REDSHIFT_TABLE}')


@task
def setup_security_group(sec_group_name: str, ip_permissions: list[dict]):
    security_group = create_security_group(sec_group_name, ip_permissions)

    ti = get_current_context()['ti']
    ti.xcom_push(key='security_group_id', value=security_group['GroupId'])


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule='@once',
    catchup=False,
    tags=['example'],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    # S3
    bucket_name = f'{env_id}-s3-bucket'
    # Redshift
    redshift_cluster_identifier = f'{env_id}-redshift-cluster'
    redshift_cluster_snapshot_identifier = f'{env_id}-snapshot'
    conn_id_name = f'{env_id}-conn-id'
    sg_name = f'{env_id}-sg'

    # [START howto_operator_s3_create_bucket]
    create_bucket = S3CreateBucketOperator(
        task_id='create_bucket',
        bucket_name=bucket_name,
    )
    # [END howto_operator_s3_create_bucket]

    add_sample_data_to_s3 = task_add_sample_data_to_s3()

    # REDSHIFT
    set_up_sg = setup_security_group(sec_group_name=sg_name, ip_permissions=[IP_PERMISSION])

    # [START howto_operator_redshift_cluster]
    create_cluster = RedshiftCreateClusterOperator(
        task_id="create_cluster",
        cluster_identifier=redshift_cluster_identifier,
        vpc_security_group_ids=[set_up_sg['security_group_id']],
        publicly_accessible=True,
        cluster_type="single-node",
        node_type="dc2.large",
        master_username=REDSHIFT_DB_LOGIN,
        master_user_password=REDSHIFT_DB_PASS,
    )
    # [END howto_operator_redshift_cluster]

    # [START howto_sensor_redshift_cluster]
    wait_cluster_available = RedshiftClusterSensor(
        task_id='wait_cluster_available',
        cluster_identifier=redshift_cluster_identifier,
        target_status='available',
        poke_interval=15,
        timeout=60 * 30,
    )
    # [END howto_sensor_redshift_cluster]

    set_up_connection = create_connection(conn_id_name, cluster_id=redshift_cluster_identifier)
    # [START howto_operator_redshift_table]
    setup__task_create_table = RedshiftDataOperator(
        task_id='setup__create_table',
        cluster_identifier=redshift_cluster_identifier,
        database=REDSHIFT_DB_NAME,
        db_user=REDSHIFT_DB_LOGIN,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE} (
            Id int,
            Name varchar
            );
        """,
        poll_interval=REDSHIFT_POLL_INTERVAL,
        await_result=True,
        redshift_conn_id=conn_id_name,
    )
    # [END howto_operator_redshift_table]

    # [START howto_transfer_s3_to_redshift]
    task_transfer_s3_to_redshift = S3ToRedshiftOperator(
        s3_bucket=S3_BUCKET_NAME,
        s3_key=S3_KEY,
        schema='PUBLIC',
        table=REDSHIFT_TABLE,
        copy_options=['csv'],
        task_id='transfer_s3_to_redshift',
        redshift_conn_id=conn_id_name,
    )
    # [END howto_transfer_s3_to_redshift]

    teardown__task_drop_table = RedshiftSQLOperator(
        sql=f'DROP TABLE IF EXISTS {REDSHIFT_TABLE}',
        task_id='teardown__drop_table',
    )
    # [START howto_operator_redshift_delete_cluster]
    delete_cluster = RedshiftDeleteClusterOperator(
        task_id='delete_cluster',
        cluster_identifier=redshift_cluster_identifier,
    )
    # [END howto_operator_redshift_delete_cluster]
    delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    delete_sg = delete_security_group(
        sec_group_id=set_up_sg['security_group_id'],
        sec_group_name=sg_name,
    )

    # [START howto_operator_s3_delete_bucket]
    delete_bucket = S3DeleteBucketOperator(
        task_id='delete_bucket',
        bucket_name=bucket_name,
        force_delete=True,
    )
    # [END howto_operator_s3_delete_bucket]
    delete_bucket.trigger_rule = TriggerRule.ALL_DONE

    remove_sample_data_from_s3 = task_remove_sample_data_from_s3()

    chain(
        # TEST SETUP
        test_context,
        set_up_sg,
        # Setup Cluster
        create_cluster,
        wait_cluster_available,
        # Setup S3
        create_bucket,
        # Setup data in S3
        [add_sample_data_to_s3, setup__task_create_table],
        # Transfer
        task_transfer_s3_to_redshift,
        # TEST TEARDOWN
        [teardown__task_drop_table, remove_sample_data_from_s3],
        [delete_bucket, delete_cluster],
        delete_sg,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
