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
from typing import List

import boto3

from airflow import DAG, settings
from airflow.decorators import task
from airflow.models import Connection
from airflow.models.baseoperator import chain
from airflow.operators.python import get_current_context
from airflow.providers.amazon.aws.hooks.redshift_cluster import RedshiftHook
from airflow.providers.amazon.aws.operators.redshift_cluster import (
    RedshiftCreateClusterOperator,
    RedshiftDeleteClusterOperator,
)
from airflow.providers.amazon.aws.operators.redshift_sql import RedshiftSQLOperator
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = 'example_redshift_sql'
REDSHIFT_CLUSTER_IDENTIFIER_KEY = 'REDSHIFT_CLUSTER_IDENTIFIER'
DB_LOGIN = 'adminuser'
DB_PASS = 'MyAmazonPassword1'

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(REDSHIFT_CLUSTER_IDENTIFIER_KEY, default_value='redshift-cluster-redesigned')
    .build()
)


@task
def set_up_connection(conn_id_name, cluster_id):
    redshift_hook = RedshiftHook(aws_conn_id='aws-default')
    cluster_endpoint = redshift_hook.get_conn().describe_clusters(ClusterIdentifier=cluster_id)
    conn = Connection(
        conn_id=conn_id_name,
        conn_type='redshift',
        host=cluster_endpoint['Clusters'][0]['Endpoint']['Address'],
        login=DB_LOGIN,
        password=DB_PASS,
        port=cluster_endpoint['Clusters'][0]['Endpoint']['Port'],
        schema=cluster_endpoint['Clusters'][0]['DBName'],
    )
    session = settings.Session()
    session.add(conn)
    session.commit()


@task
def setup_security_group(sc_group_name: str, ip_permissions: List[dict]):
    client = boto3.client('ec2')
    vpcs = client.describe_vpcs()
    vpc_id = vpcs['Vpcs'][0]['VpcId']
    sc_group = client.create_security_group(
        Description='Redshift-system-test', GroupName=sc_group_name, VpcId=vpc_id
    )
    waiter = client.get_waiter('security_group_exists')
    waiter.wait(
        GroupIds=[
            sc_group['GroupId'],
        ],
        GroupNames=[
            sc_group_name,
        ],
        WaiterConfig={'Delay': 15, 'MaxAttempts': 4},
    )
    client.authorize_security_group_ingress(
        GroupId=sc_group['GroupId'], GroupName=sc_group_name, IpPermissions=ip_permissions
    )
    ti = get_current_context()['ti']
    ti.xcom_push(key='sc_group_id', value=sc_group['GroupId'])


@task
def delete_security_group(sc_group_id, sc_group_name):
    client = boto3.client('ec2')
    client.delete_security_group(GroupId=sc_group_id, GroupName=sc_group_name)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule_interval='@once',
    catchup=False,
    tags=['example'],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context[ENV_ID_KEY]
    redshift_cluster_identifier = test_context[REDSHIFT_CLUSTER_IDENTIFIER_KEY]
    conn_id_name = f'{redshift_cluster_identifier} - conn-id'

    ip_permission = {
        'FromPort': -1,
        'IpProtocol': 'All',
        'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'Test description'}],
    }
    sg_name = f'{redshift_cluster_identifier}-sg'
    task_setup_sc_group = setup_security_group(sc_group_name=sg_name, ip_permissions=[ip_permission])

    # [START howto_operator_redshift_cluster]
    task_create_cluster = RedshiftCreateClusterOperator(
        task_id="redshift_create_cluster",
        cluster_identifier=redshift_cluster_identifier,
        vpc_security_group_ids=[task_setup_sc_group['sc_group_id']],
        publicly_accessible=True,
        cluster_type="single-node",
        node_type="dc2.large",
        master_username=DB_LOGIN,
        master_user_password=DB_PASS,
    )
    # [END howto_operator_redshift_cluster]

    # [START howto_sensor_redshift_cluster]
    task_wait_cluster_available = RedshiftClusterSensor(
        task_id='sensor_redshift_cluster_available',
        cluster_identifier=redshift_cluster_identifier,
        target_status='available',
        poke_interval=5,
        timeout=60 * 15,
    )
    # [END howto_sensor_redshift_cluster]

    task_set_up_connection = set_up_connection(conn_id_name, cluster_id=redshift_cluster_identifier)

    task_create_table = RedshiftSQLOperator(
        task_id='create_table',
        redshift_conn_id=conn_id_name,
        sql="""
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """,
    )

    task_insert_data = RedshiftSQLOperator(
        task_id='task_insert_data',
        redshift_conn_id=conn_id_name,
        sql=[
            "INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');",
            "INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');",
            "INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');",
            "INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');",
            "INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');",
            "INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');",
        ],
    )

    # [START howto_operator_redshift_sql]
    task_select_data = RedshiftSQLOperator(
        task_id='task_get_all_table_data',
        redshift_conn_id=conn_id_name,
        sql="""CREATE TABLE more_fruit AS SELECT * FROM fruit;""",
    )
    # [END howto_operator_redshift_sql]

    # [START howto_operator_redshift_sql_with_params]
    task_select_filtered_data = RedshiftSQLOperator(
        task_id='task_get_filtered_table_data',
        redshift_conn_id=conn_id_name,
        sql="""CREATE TABLE filtered_fruit AS SELECT * FROM fruit WHERE color = '{{ params.color }}';""",
        params={'color': 'Red'},
    )
    # [END howto_operator_redshift_sql_with_params]

    task_drop_table = RedshiftSQLOperator(
        task_id='drop_table',
        redshift_conn_id=conn_id_name,
        sql='DROP TABLE IF EXISTS fruit',
        trigger_rule=TriggerRule.ALL_DONE,
    )
    task_delete_cluster = RedshiftDeleteClusterOperator(
        task_id='delete_redshift_cluster',
        cluster_identifier=redshift_cluster_identifier,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    task_delete_sc_group = delete_security_group(
        sc_group_id=task_setup_sc_group['sc_group_id'],
        sc_group_name=sg_name,
    )
    chain(
        # TEST SETUP
        test_context,
        task_setup_sc_group,
        task_create_cluster,
        task_wait_cluster_available,
        task_set_up_connection,
        task_create_table,
        task_insert_data,
        # TEST BODY
        [task_select_data, task_select_filtered_data],
        # TEST TEARDOWN
        task_drop_table,
        task_delete_cluster,
        task_delete_sc_group,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
