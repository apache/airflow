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

from airflow import settings
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftCreateClusterOperator,
    RedshiftDeleteClusterOperator,
)
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3CreateObjectOperator,
    S3DeleteBucketOperator,
    S3DeleteObjectsOperator,
)
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder
from tests_common.test_utils.watcher import watcher

# Externally fetched variables:
SECURITY_GROUP_KEY = "SECURITY_GROUP"
CLUSTER_SUBNET_GROUP_KEY = "CLUSTER_SUBNET_GROUP"

sys_test_context_task = (
    SystemTestContextBuilder().add_variable(SECURITY_GROUP_KEY).add_variable(CLUSTER_SUBNET_GROUP_KEY).build()
)

DAG_ID = "example_s3_to_sql"

DB_LOGIN = "adminuser"
DB_PASS = "MyAmazonPassword1"
DB_NAME = "dev"

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


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2023, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    security_group_id = test_context[SECURITY_GROUP_KEY]
    cluster_subnet_group_name = test_context[CLUSTER_SUBNET_GROUP_KEY]
    conn_id_name = f"{env_id}-conn-id"
    redshift_cluster_identifier = f"{env_id}-redshift-cluster"
    sg_name = f"{env_id}-sg"
    s3_bucket_name = f"{env_id}-bucket"
    s3_key = f"{env_id}/files/cocktail_list.csv"

    create_cluster = RedshiftCreateClusterOperator(
        task_id="create_cluster",
        cluster_identifier=redshift_cluster_identifier,
        vpc_security_group_ids=[security_group_id],
        cluster_subnet_group_name=cluster_subnet_group_name,
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

    create_table = RedshiftDataOperator(
        task_id="create_sample_table",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql=f"""
            CREATE TABLE IF NOT EXISTS {SQL_TABLE_NAME} (
            cocktail_id INT NOT NULL,
            cocktail_name VARCHAR NOT NULL,
            base_spirit VARCHAR NOT NULL);
        """,
        wait_for_completion=True,
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
            return list(csv.reader(file))

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

    drop_table = RedshiftDataOperator(
        task_id="drop_table",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql=f"DROP TABLE {SQL_TABLE_NAME}",
        wait_for_completion=True,
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

    chain(
        # TEST SETUP
        test_context,
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
    )

    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
