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

import boto3

from airflow import DAG, settings
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.baseoperator import chain
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftCreateClusterOperator,
    RedshiftDeleteClusterOperator,
)
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.redshift_to_s3 import RedshiftToS3Operator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_redshift_to_s3"

DB_LOGIN = "adminuser"
DB_PASS = "MyAmazonPassword1"
DB_NAME = "dev"

IP_PERMISSION = {
    "FromPort": -1,
    "IpProtocol": "All",
    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Test description"}],
}

S3_KEY = "s3_output_"
S3_KEY_2 = "s3_key_2"
S3_KEY_PREFIX = "s3_k"
REDSHIFT_TABLE = "test_table"

SQL_CREATE_TABLE = f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE} (
    fruit_id INTEGER,
    name VARCHAR NOT NULL,
    color VARCHAR NOT NULL
    );
"""

SQL_INSERT_DATA = f"INSERT INTO {REDSHIFT_TABLE} VALUES ( 1, 'Banana', 'Yellow');"

SQL_DROP_TABLE = f"DROP TABLE IF EXISTS {REDSHIFT_TABLE};"

DATA = "0, 'Airflow', 'testing'"


sys_test_context_task = SystemTestContextBuilder().build()


@task
def create_connection(conn_id_name: str, cluster_id: str):
    redshift_hook = RedshiftHook()
    cluster_endpoint = redshift_hook.get_conn().describe_clusters(ClusterIdentifier=cluster_id)["Clusters"][0]
    conn = Connection(
        conn_id=conn_id_name,
        conn_type="redshift",
        host=cluster_endpoint["Endpoint"]["Address"],
        login=DB_LOGIN,
        password=DB_PASS,
        port=cluster_endpoint["Endpoint"]["Port"],
        schema=cluster_endpoint["DBName"],
    )
    session = settings.Session()
    session.add(conn)
    session.commit()


@task
def setup_security_group(sec_group_name: str, ip_permissions: list[dict]):
    client = boto3.client("ec2")
    vpc_id = client.describe_vpcs()["Vpcs"][0]["VpcId"]
    security_group = client.create_security_group(
        Description="Redshift-system-test", GroupName=sec_group_name, VpcId=vpc_id
    )
    client.get_waiter("security_group_exists").wait(
        GroupIds=[security_group["GroupId"]],
        GroupNames=[sec_group_name],
        WaiterConfig={"Delay": 15, "MaxAttempts": 4},
    )
    client.authorize_security_group_ingress(
        GroupId=security_group["GroupId"], GroupName=sec_group_name, IpPermissions=ip_permissions
    )
    return security_group["GroupId"]


@task(trigger_rule=TriggerRule.ALL_DONE)
def delete_security_group(sec_group_id: str, sec_group_name: str):
    boto3.client("ec2").delete_security_group(GroupId=sec_group_id, GroupName=sec_group_name)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    redshift_cluster_identifier = f"{env_id}-redshift-cluster"
    conn_id_name = f"{env_id}-conn-id"
    sg_name = f"{env_id}-sg"
    bucket_name = f"{env_id}-bucket"

    set_up_sg = setup_security_group(sec_group_name=sg_name, ip_permissions=[IP_PERMISSION])

    create_bucket = S3CreateBucketOperator(
        task_id="s3_create_bucket",
        bucket_name=bucket_name,
    )

    create_cluster = RedshiftCreateClusterOperator(
        task_id="create_cluster",
        cluster_identifier=redshift_cluster_identifier,
        vpc_security_group_ids=[set_up_sg],
        publicly_accessible=True,
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
        timeout=60 * 15,
    )

    set_up_connection = create_connection(conn_id_name, cluster_id=redshift_cluster_identifier)

    create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=bucket_name,
        s3_key=S3_KEY_2,
        data=DATA,
        replace=True,
    )

    create_table_redshift_data = SQLExecuteQueryOperator(
        task_id="create_table_redshift_data",
        conn_id=conn_id_name,
        sql=SQL_CREATE_TABLE,
    )
    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id=conn_id_name,
        sql=SQL_INSERT_DATA,
    )

    # [START howto_transfer_redshift_to_s3]
    transfer_redshift_to_s3 = RedshiftToS3Operator(
        task_id="transfer_redshift_to_s3",
        redshift_conn_id=conn_id_name,
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

    # [START howto_transfer_s3_to_redshift]
    transfer_s3_to_redshift = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift",
        redshift_conn_id=conn_id_name,
        s3_bucket=bucket_name,
        s3_key=S3_KEY_2,
        schema="PUBLIC",
        table=REDSHIFT_TABLE,
        copy_options=["csv"],
    )
    # [END howto_transfer_s3_to_redshift]

    # [START howto_transfer_s3_to_redshift_multiple_keys]
    transfer_s3_to_redshift_multiple = S3ToRedshiftOperator(
        task_id="transfer_s3_to_redshift_multiple",
        redshift_conn_id=conn_id_name,
        s3_bucket=bucket_name,
        s3_key=S3_KEY_PREFIX,
        schema="PUBLIC",
        table=REDSHIFT_TABLE,
        copy_options=["csv"],
    )
    # [END howto_transfer_s3_to_redshift_multiple_keys]

    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        conn_id=conn_id_name,
        sql=SQL_DROP_TABLE,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_cluster = RedshiftDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_identifier=redshift_cluster_identifier,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_sg = delete_security_group(
        sec_group_id=set_up_sg,
        sec_group_name=sg_name,
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
        set_up_sg,
        create_bucket,
        create_cluster,
        wait_cluster_available,
        set_up_connection,
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
        delete_sg,
        delete_bucket,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
