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

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
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
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import ENV_ID_KEY, SystemTestContextBuilder

DAG_ID = "example_redshift"

# Externally fetched variables:
SECURITY_GROUP_KEY = "SECURITY_GROUP"
CLUSTER_SUBNET_GROUP_KEY = "CLUSTER_SUBNET_GROUP"

sys_test_context_task = (
    SystemTestContextBuilder()
    .add_variable(SECURITY_GROUP_KEY)
    .add_variable(CLUSTER_SUBNET_GROUP_KEY)
    .build()
)

DB_LOGIN = "adminuser"
DB_PASS = "MyAmazonPassword1"
DB_NAME = "dev"
POLL_INTERVAL = 10

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
    redshift_cluster_snapshot_identifier = f"{env_id}-snapshot"
    sg_name = f"{env_id}-sg"

    # [START howto_operator_redshift_cluster]
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
    # [END howto_operator_redshift_cluster]

    # [START howto_sensor_redshift_cluster]
    wait_cluster_available = RedshiftClusterSensor(
        task_id="wait_cluster_available",
        cluster_identifier=redshift_cluster_identifier,
        target_status="available",
        poke_interval=15,
        timeout=60 * 30,
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
        timeout=60 * 30,
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
        poke_interval=30,
        timeout=60 * 30,
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
        poke_interval=30,
        timeout=60 * 30,
    )

    # [START howto_operator_redshift_data]
    create_table_redshift_data = RedshiftDataOperator(
        task_id="create_table_redshift_data",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql=[
            """
            CREATE TABLE IF NOT EXISTS fruit (
            fruit_id INTEGER,
            name VARCHAR NOT NULL,
            color VARCHAR NOT NULL
            );
        """
        ],
        poll_interval=POLL_INTERVAL,
        wait_for_completion=True,
    )
    # [END howto_operator_redshift_data]

    insert_data = RedshiftDataOperator(
        task_id="insert_data",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql=[
            "INSERT INTO fruit VALUES ( 1, 'Banana', 'Yellow');",
            "INSERT INTO fruit VALUES ( 2, 'Apple', 'Red');",
            "INSERT INTO fruit VALUES ( 3, 'Lemon', 'Yellow');",
            "INSERT INTO fruit VALUES ( 4, 'Grape', 'Purple');",
            "INSERT INTO fruit VALUES ( 5, 'Pear', 'Green');",
            "INSERT INTO fruit VALUES ( 6, 'Strawberry', 'Red');",
        ],
        poll_interval=POLL_INTERVAL,
        wait_for_completion=True,
    )

    # [START howto_operator_redshift_data_session_reuse]
    create_tmp_table_data_api = RedshiftDataOperator(
        task_id="create_tmp_table_data_api",
        cluster_identifier=redshift_cluster_identifier,
        database=DB_NAME,
        db_user=DB_LOGIN,
        sql=[
            """
            CREATE TEMPORARY TABLE tmp_people (
            id INTEGER,
            first_name VARCHAR(100),
            age INTEGER
            );
        """
        ],
        poll_interval=POLL_INTERVAL,
        wait_for_completion=True,
        session_keep_alive_seconds=600,
    )

    insert_data_reuse_session = RedshiftDataOperator(
        task_id="insert_data_reuse_session",
        sql=[
            "INSERT INTO tmp_people VALUES ( 1, 'Bob', 30);",
            "INSERT INTO tmp_people VALUES ( 2, 'Alice', 35);",
            "INSERT INTO tmp_people VALUES ( 3, 'Charlie', 40);",
        ],
        poll_interval=POLL_INTERVAL,
        wait_for_completion=True,
        session_id="{{ task_instance.xcom_pull(task_ids='create_tmp_table_data_api', key='session_id') }}",
    )
    # [END howto_operator_redshift_data_session_reuse]

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

    chain(
        # TEST SETUP
        test_context,
        # TEST BODY
        create_cluster,
        wait_cluster_available,
        create_cluster_snapshot,
        wait_cluster_available_before_pause,
        pause_cluster,
        wait_cluster_paused,
        resume_cluster,
        wait_cluster_available_after_resume,
        create_table_redshift_data,
        insert_data,
        delete_cluster_snapshot,
        delete_cluster,
    )

    # Test session reuse in parallel
    chain(
        wait_cluster_available_after_resume,
        create_tmp_table_data_api,
        insert_data_reuse_session,
        delete_cluster_snapshot,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
