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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftCreateClusterOperator,
    RedshiftDeleteClusterOperator,
)
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_redshift_to_s3"

# Externally fetched variables:
SECURITY_GROUP_KEY = "SECURITY_GROUP"
CLUSTER_SUBNET_GROUP_KEY = "CLUSTER_SUBNET_GROUP"

sys_test_context_task = (
    SystemTestContextBuilder().add_variable(SECURITY_GROUP_KEY).add_variable(CLUSTER_SUBNET_GROUP_KEY).build()
)

DB_LOGIN = "adminuser"
DB_PASS = "MyAmazonPassword1"
DB_NAME = "dev"

S3_KEY = "s3_output_"
S3_KEY_2 = "s3_key_2"
S3_KEY_3 = "s3_output_tmp_table_"
S3_KEY_PREFIX = "s3_k"
REDSHIFT_TABLE = "test_table"
REDSHIFT_TMP_TABLE = "tmp_table"

DATA = "0, 'Airflow', 'testing'"


def _drop_table(table_name: str) -> str:
    return f"DROP TABLE IF EXISTS {table_name};"


def _create_table(table_name: str, is_temp: bool = False) -> str:
    temp_keyword = "TEMPORARY" if is_temp else ""
    return (
        _drop_table(table_name)
        + f"""
        CREATE {temp_keyword} TABLE {table_name} (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
        );
    """
    )


def _insert_data(table_name: str) -> str:
    return f"INSERT INTO {table_name} VALUES ( 1, 'Banana', 'Yellow');"


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    security_group_id = test_context[SECURITY_GROUP_KEY]
    cluster_subnet_group_name = test_context[CLUSTER_SUBNET_GROUP_KEY]
    redshift_cluster_identifier = f"{env_id}-redshift-cluster"
    sg_name = f"{env_id}-sg"
    bucket_name = f"{env_id}-bucket"

    create_bucket = S3CreateBucketOperator(
        task_id="s3_create_bucket",
        bucket_name=bucket_name,
    )

    create_cluster = RedshiftCreateClusterOperator(
        task_id="create_cluster",
        cluster_identifier=redshift_cluster_identifier,
        vpc_security_group_ids=[security_group_id],
        cluster_subnet_group_name=cluster_subnet_group_name,
        publicly_accessible=False,
        cluster_type="single-node",
        node_type="dc2.large",
        master_username=DB_LOGIN,
        master_user_password=DB_PASS,
    )

    wait_cluster_available = RedshiftClusterSensor(
        task_id="wait_cluster_available",
        cluster_identifier=redshift_cluster_identifier,
        target_status="available",
        poke_interval=5,
        timeout=60 * 30,
    )

    create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=S3_KEY_2,
        data=DATA,
        replace=True,
    )

    create_table_redshift_data = RedshiftDataOperator(
        task_id="create_table_redshift_data",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql=_create_table(REDSHIFT_TABLE),
        wait_for_completion=True,
    )

    insert_data = RedshiftDataOperator(
        task_id="insert_data",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql=_insert_data(REDSHIFT_TABLE),
        wait_for_completion=True,
    )

    # [START howto_transfer_redshift_to_s3]
    transfer_redshift_to_s3 = RedshiftToS3Operator(
        task_id="transfer_redshift_to_s3",
        redshift_data_api_kwargs={
            "database": DB_NAME,
            "cluster_identifier": redshift_cluster_identifier,
            "db_user": DB_LOGIN,
            "wait_for_completion": True,
        },
        s3_bucket=bucket_name,
        s3_key=S3_KEY,
        schema="PUBLIC",
        table=REDSHIFT_TABLE,
    )
    # [END howto_transfer_redshift_to_s3]

    check_if_key_exists = S3KeySensor(
        task_id="check_if_key_exists",
        bucket_name=bucket_name,
        bucket_key=f"{S3_KEY}/{REDSHIFT_TABLE}_0000_part_00",
    )

    create_tmp_table = RedshiftDataOperator(
        task_id="create_tmp_table",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql=_create_table(REDSHIFT_TMP_TABLE, is_temp=True) + _insert_data(REDSHIFT_TMP_TABLE),
        wait_for_completion=True,
        session_keep_alive_seconds=600,
    )

    transfer_redshift_to_s3_reuse_session = RedshiftToS3Operator(
        task_id="transfer_redshift_to_s3_reuse_session",
        redshift_data_api_kwargs={
            "wait_for_completion": True,
            "session_id": "{{ task_instance.xcom_pull(task_ids='create_tmp_table', key='session_id') }}",
        },
        s3_bucket=bucket_name,
        s3_key=S3_KEY_3,
        table=REDSHIFT_TMP_TABLE,
    )

    check_if_tmp_table_key_exists = S3KeySensor(
        task_id="check_if_tmp_table_key_exists",
        bucket_name=bucket_name,
        bucket_key=f"{S3_KEY_3}/{REDSHIFT_TMP_TABLE}_0000_part_00",
    )

    # [START howto_transfer_s3_to_redshift]
    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift",
        redshift_data_api_kwargs={
            "database": DB_NAME,
            "cluster_identifier": redshift_cluster_identifier,
            "db_user": DB_LOGIN,
            "wait_for_completion": True,
        },
        s3_bucket=bucket_name,
        s3_key=S3_KEY_2,
        schema="PUBLIC",
        table=REDSHIFT_TABLE,
        copy_options=["csv"],
    )
    # [END howto_transfer_s3_to_redshift]

    create_dest_tmp_table = RedshiftDataOperator(
        task_id="create_dest_tmp_table",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql=_create_table(REDSHIFT_TMP_TABLE, is_temp=True),
        wait_for_completion=True,
        session_keep_alive_seconds=600,
    )

    transfer_s3_to_redshift_tmp_table = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift_tmp_table",
        redshift_data_api_kwargs={
            "session_id": "{{ task_instance.xcom_pull(task_ids='create_dest_tmp_table', key='session_id') }}",
            "wait_for_completion": True,
        },
        s3_bucket=bucket_name,
        s3_key=S3_KEY_2,
        table=REDSHIFT_TMP_TABLE,
        copy_options=["csv"],
    )

    # [START howto_transfer_s3_to_redshift_multiple_keys]
    transfer_s3_to_redshift_multiple = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift_multiple",
        redshift_data_api_kwargs={
            "database": DB_NAME,
            "cluster_identifier": redshift_cluster_identifier,
            "db_user": DB_LOGIN,
            "wait_for_completion": True,
        },
        s3_bucket=bucket_name,
        s3_key=S3_KEY_PREFIX,
        schema="PUBLIC",
        table=REDSHIFT_TABLE,
        copy_options=["csv"],
    )
    # [END howto_transfer_s3_to_redshift_multiple_keys]

    drop_table = RedshiftDataOperator(
        task_id="drop_table",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql=_drop_table(REDSHIFT_TABLE),
        wait_for_completion=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_cluster = RedshiftDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_identifier=redshift_cluster_identifier,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        create_cluster,
        wait_cluster_available,
        create_object,
        create_table_redshift_data,
        insert_data,
        # TEST BODY
        transfer_redshift_to_s3,
        check_if_key_exists,
        transfer_s3_to_redshift,
        transfer_s3_to_redshift_multiple,
        # TEST TEARDOWN
        drop_table,
        delete_cluster,
        delete_bucket,
    )

    chain(
        # TEST SETUP
        wait_cluster_available,
        create_tmp_table,
        # TEST BODY
        transfer_redshift_to_s3_reuse_session,
        check_if_tmp_table_key_exists,
        # TEST TEARDOWN
        delete_cluster,
    )

    chain(
        # TEST SETUP
        wait_cluster_available,
        create_dest_tmp_table,
        # TEST BODY
        transfer_s3_to_redshift_tmp_table,
        # TEST TEARDOWN
        delete_cluster,
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
