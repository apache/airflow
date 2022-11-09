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
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateBucketOperator, S3DeleteBucketOperator
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_sql_to_s3"
DB_LOGIN = "adminuser"
DB_PASS = "MyAmazonPassword1"
DB_NAME = "dev"

IP_PERMISSION = {
    "FromPort": -1,
    "IpProtocol": "All",
    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Test description"}],
}

REDSHIFT_TABLE = "test_table"
SQL_QUERY = f"SELECT * FROM {REDSHIFT_TABLE}"

SQL_CREATE_TABLE = f"""
    CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE} (
    fruit_id INTEGER,
    name VARCHAR NOT NULL,
    color VARCHAR NOT NULL
    );
"""

SQL_INSERT_DATA = f"INSERT INTO {REDSHIFT_TABLE} VALUES ( 1, 'Banana', 'Yellow');"


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
    bucket_name = f"{env_id}-s3-bucket"
    key = f"{env_id}-key"

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=bucket_name,
    )

    set_up_sg = setup_security_group(sec_group_name=sg_name, ip_permissions=[IP_PERMISSION])

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
        poke_interval=15,
        timeout=60 * 10,
    )

    set_up_connection = create_connection(conn_id_name, cluster_id=redshift_cluster_identifier)

    create_table_redshift_data = RedshiftSQLOperator(
        task_id="create_table_redshift_data",
        redshift_conn_id=conn_id_name,
        sql=SQL_CREATE_TABLE,
    )

    insert_data = RedshiftSQLOperator(
        task_id="insert_data",
        redshift_conn_id=conn_id_name,
        sql=SQL_INSERT_DATA,
    )

    # [START howto_transfer_sql_to_s3]
    sql_to_s3_task = SqlToS3Operator(
        task_id="sql_to_s3_task",
        sql_conn_id=conn_id_name,
        query=SQL_QUERY,
        s3_bucket=bucket_name,
        s3_key=key,
        replace=True,
    )
    # [END howto_transfer_sql_to_s3]

    delete_bucket = S3DeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=bucket_name,
        force_delete=True,
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
    chain(
        # TEST SETUP
        test_context,
        create_bucket,
        set_up_sg,
        create_cluster,
        wait_cluster_available,
        set_up_connection,
        create_table_redshift_data,
        insert_data,
        # TEST BODY
        sql_to_s3_task,
        # TEST TEARDOWN
        delete_bucket,
        delete_cluster,
        delete_sg,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
