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
    S3DeleteObjectsOperator,
)
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator, SQLTableCheckOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests.system.utils.watcher import watcher

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_s3_to_sql"

DB_LOGIN = "adminuser"
DB_PASS = "MyAmazonPassword1"
DB_NAME = "dev"

IP_PERMISSION = {
    "FromPort": -1,
    "IpProtocol": "All",
    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Test description"}],
}

SQL_TABLE_NAME = "cocktails"
SQL_COLUMN_LIST = ["cocktail_id", "cocktail_name", "base_spirit"]
SAMPLE_DATA = r"""1,Caipirinha,Cachaca
2,Bramble,Gin
3,Daiquiri,Rum
"""


@task
def create_connection(conn_id_name: str, cluster_id: str):
    cluster_endpoint = RedshiftHook().conn.describe_clusters(ClusterIdentifier=cluster_id)["Clusters"][0]
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
    default_vpc = client.describe_vpcs(Filters=[{"Name": "is-default", "Values": ["true"]}])["Vpcs"][0]

    security_group = client.create_security_group(
        Description="Redshift-system-test", GroupName=sec_group_name, VpcId=default_vpc["VpcId"]
    )
    client.get_waiter("security_group_exists").wait(
        GroupIds=[security_group["GroupId"]], GroupNames=[sec_group_name]
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
    start_date=datetime(2023, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:

    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    conn_id_name = f"{env_id}-conn-id"
    redshift_cluster_identifier = f"{env_id}-redshift-cluster"
    sg_name = f"{env_id}-sg"
    s3_bucket_name = f"{env_id}-bucket"
    s3_key = f"{env_id}/files/cocktail_list.csv"

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
        poke_interval=5,
    )

    set_up_connection = create_connection(conn_id_name, cluster_id=redshift_cluster_identifier)

    create_bucket = S3CreateBucketOperator(
        task_id="create_bucket",
        bucket_name=s3_bucket_name,
    )

    create_object = S3CreateObjectOperator(
        task_id="create_object",
        s3_bucket=s3_bucket_name,
        s3_key=s3_key,
        data=SAMPLE_DATA,
        replace=True,
    )

    create_table = SQLExecuteQueryOperator(
        task_id="create_sample_table",
        conn_id=conn_id_name,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {SQL_TABLE_NAME} (
            cocktail_id INT NOT NULL,
            cocktail_name VARCHAR NOT NULL,
            base_spirit VARCHAR NOT NULL);
          """,
    )

    # [START howto_transfer_s3_to_sql]
    #
    # This operator requires a parser method. The Parser should take a filename as input
    # and return an iterable of rows.
    # This example parser uses the builtin csv library and returns a list of rows
    #
    def parse_csv_to_list(filepath):
        import csv

        with open(filepath, newline="") as file:
            return [row for row in csv.reader(file)]

    transfer_s3_to_sql = S3ToSqlOperator(
        task_id="transfer_s3_to_sql",
        s3_bucket=s3_bucket_name,
        s3_key=s3_key,
        table=SQL_TABLE_NAME,
        column_list=SQL_COLUMN_LIST,
        parser=parse_csv_to_list,
        sql_conn_id=conn_id_name,
    )
    # [END howto_transfer_s3_to_sql]

    # [START howto_transfer_s3_to_sql_generator]
    #
    # As the parser can return any kind of iterator, a generator is also allowed.
    # This example parser returns a generator which prevents python from loading
    # the whole file into memory.
    #

    def parse_csv_to_generator(filepath):
        import csv

        with open(filepath, newline="") as file:
            yield from csv.reader(file)

    transfer_s3_to_sql_generator = S3ToSqlOperator(
        task_id="transfer_s3_to_sql_paser_to_generator",
        s3_bucket=s3_bucket_name,
        s3_key=s3_key,
        table=SQL_TABLE_NAME,
        column_list=SQL_COLUMN_LIST,
        parser=parse_csv_to_generator,
        sql_conn_id=conn_id_name,
    )
    # [END howto_transfer_s3_to_sql_generator]

    check_table = SQLTableCheckOperator(
        task_id="check_table",
        conn_id=conn_id_name,
        table=SQL_TABLE_NAME,
        checks={
            "row_count_check": {"check_statement": "COUNT(*) = 6"},
        },
    )

    drop_table = SQLExecuteQueryOperator(
        conn_id=conn_id_name,
        trigger_rule=TriggerRule.ALL_DONE,
        task_id="drop_table",
        sql=f"DROP TABLE {SQL_TABLE_NAME}",
    )

    delete_s3_objects = S3DeleteObjectsOperator(
        trigger_rule=TriggerRule.ALL_DONE,
        task_id="delete_objects",
        bucket=s3_bucket_name,
        keys=s3_key,
    )

    delete_s3_bucket = S3DeleteBucketOperator(
        trigger_rule=TriggerRule.ALL_DONE,
        task_id="delete_bucket",
        bucket_name=s3_bucket_name,
        force_delete=True,
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
        set_up_sg,
        create_cluster,
        wait_cluster_available,
        set_up_connection,
        create_bucket,
        create_object,
        create_table,
        # TEST BODY
        transfer_s3_to_sql,
        transfer_s3_to_sql_generator,
        check_table,
        # TEST TEARDOWN
        drop_table,
        delete_s3_objects,
        delete_s3_bucket,
        delete_cluster,
        delete_sg,
    )

    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
