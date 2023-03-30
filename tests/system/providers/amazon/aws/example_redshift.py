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
    RedshiftCreateClusterSnapshotOperator,
    RedshiftDeleteClusterOperator,
    RedshiftDeleteClusterSnapshotOperator,
    RedshiftPauseClusterOperator,
    RedshiftResumeClusterOperator,
)
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.providers.amazon.aws.sensors.redshift_cluster import RedshiftClusterSensor
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_redshift"
DB_LOGIN = "adminuser"
DB_PASS = "MyAmazonPassword1"
DB_NAME = "dev"
POLL_INTERVAL = 10

IP_PERMISSION = {
    "FromPort": -1,
    "IpProtocol": "All",
    "IpRanges": [{"CidrIp": "0.0.0.0/0", "Description": "Test description"}],
}

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
    redshift_cluster_snapshot_identifier = f"{env_id}-snapshot"
    conn_id_name = f"{env_id}-conn-id"
    sg_name = f"{env_id}-sg"

    set_up_sg = setup_security_group(sec_group_name=sg_name, ip_permissions=[IP_PERMISSION])

    # [START howto_operator_redshift_cluster]
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
    # [END howto_operator_redshift_cluster]

    # [START howto_sensor_redshift_cluster]
    wait_cluster_available = RedshiftClusterSensor(
        task_id="wait_cluster_available",
        cluster_identifier=redshift_cluster_identifier,
        target_status="available",
        poke_interval=15,
        timeout=60 * 15,
    )
    # [END howto_sensor_redshift_cluster]

    # [START howto_operator_redshift_create_cluster_snapshot]
    create_cluster_snapshot = RedshiftCreateClusterSnapshotOperator(
        task_id="create_cluster_snapshot",
        cluster_identifier=redshift_cluster_identifier,
        snapshot_identifier=redshift_cluster_snapshot_identifier,
        poll_interval=30,
        max_attempt=100,
        retention_period=1,
        wait_for_completion=True,
    )
    # [END howto_operator_redshift_create_cluster_snapshot]

    wait_cluster_available_before_pause = RedshiftClusterSensor(
        task_id="wait_cluster_available_before_pause",
        cluster_identifier=redshift_cluster_identifier,
        target_status="available",
        poke_interval=15,
        timeout=60 * 15,
    )

    # [START howto_operator_redshift_pause_cluster]
    pause_cluster = RedshiftPauseClusterOperator(
        task_id="pause_cluster",
        cluster_identifier=redshift_cluster_identifier,
    )
    # [END howto_operator_redshift_pause_cluster]

    wait_cluster_paused = RedshiftClusterSensor(
        task_id="wait_cluster_paused",
        cluster_identifier=redshift_cluster_identifier,
        target_status="paused",
        poke_interval=15,
        timeout=60 * 15,
    )

    # [START howto_operator_redshift_resume_cluster]
    resume_cluster = RedshiftResumeClusterOperator(
        task_id="resume_cluster",
        cluster_identifier=redshift_cluster_identifier,
    )
    # [END howto_operator_redshift_resume_cluster]

    wait_cluster_available_after_resume = RedshiftClusterSensor(
        task_id="wait_cluster_available_after_resume",
        cluster_identifier=redshift_cluster_identifier,
        target_status="available",
        poke_interval=15,
        timeout=60 * 15,
    )

    set_up_connection = create_connection(conn_id_name, cluster_id=redshift_cluster_identifier)

    # [START howto_operator_redshift_data]
    create_table_redshift_data = RedshiftDataOperator(
        task_id="create_table_redshift_data",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql="""
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """,
        poll_interval=POLL_INTERVAL,
        wait_for_completion=True,
    )
    # [END howto_operator_redshift_data]

    insert_data = RedshiftDataOperator(
        task_id="insert_data",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql="""
            INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');
            INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');
            INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');
            INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');
            INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');
            INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');
        """,
        poll_interval=POLL_INTERVAL,
        wait_for_completion=True,
    )

    drop_table = SQLExecuteQueryOperator(
        task_id="drop_table",
        conn_id=conn_id_name,
        sql="DROP TABLE IF EXISTS fruit",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # [START howto_operator_redshift_delete_cluster]
    delete_cluster = RedshiftDeleteClusterOperator(
        task_id="delete_cluster",
        cluster_identifier=redshift_cluster_identifier,
    )
    # [END howto_operator_redshift_delete_cluster]
    delete_cluster.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_redshift_delete_cluster_snapshot]
    delete_cluster_snapshot = RedshiftDeleteClusterSnapshotOperator(
        task_id="delete_cluster_snapshot",
        cluster_identifier=redshift_cluster_identifier,
        snapshot_identifier=redshift_cluster_snapshot_identifier,
    )
    # [END howto_operator_redshift_delete_cluster_snapshot]

    delete_sg = delete_security_group(
        sec_group_id=set_up_sg,
        sec_group_name=sg_name,
    )
    chain(
        # TEST SETUP
        test_context,
        set_up_sg,
        # TEST BODY
        create_cluster,
        wait_cluster_available,
        create_cluster_snapshot,
        wait_cluster_available_before_pause,
        pause_cluster,
        wait_cluster_paused,
        resume_cluster,
        wait_cluster_available_after_resume,
        set_up_connection,
        create_table_redshift_data,
        insert_data,
        drop_table,
        delete_cluster_snapshot,
        delete_cluster,
        # TEST TEARDOWN
        delete_sg,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
