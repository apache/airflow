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
"""
Example Airflow DAG that creates, updates, queries and deletes a Cloud Spanner instance.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.spanner import (
    SpannerDeleteDatabaseInstanceOperator,
    SpannerDeleteInstanceOperator,
    SpannerDeployDatabaseInstanceOperator,
    SpannerDeployInstanceOperator,
    SpannerQueryDatabaseInstanceOperator,
    SpannerUpdateDatabaseInstanceOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "spanner"

GCP_SPANNER_INSTANCE_ID = f"instance-{DAG_ID}-{ENV_ID}"
GCP_SPANNER_DATABASE_ID = f"database-{DAG_ID}-{ENV_ID}"
GCP_SPANNER_CONFIG_NAME = f"projects/{PROJECT_ID}/instanceConfigs/regional-europe-west3"
GCP_SPANNER_NODE_COUNT = 1
GCP_SPANNER_DISPLAY_NAME = "InstanceSpanner"
# OPERATION_ID should be unique per operation
OPERATION_ID = "unique_operation_id"


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "spanner"],
) as dag:
    # Create
    # [START howto_operator_spanner_deploy]
    spanner_instance_create_task = SpannerDeployInstanceOperator(
        project_id=PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        configuration_name=GCP_SPANNER_CONFIG_NAME,
        node_count=GCP_SPANNER_NODE_COUNT,
        display_name=GCP_SPANNER_DISPLAY_NAME,
        task_id="spanner_instance_create_task",
    )
    spanner_instance_update_task = SpannerDeployInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        configuration_name=GCP_SPANNER_CONFIG_NAME,
        node_count=GCP_SPANNER_NODE_COUNT + 1,
        display_name=GCP_SPANNER_DISPLAY_NAME + "_updated",
        task_id="spanner_instance_update_task",
    )
    # [END howto_operator_spanner_deploy]

    # [START howto_operator_spanner_database_deploy]
    spanner_database_deploy_task = SpannerDeployDatabaseInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        ddl_statements=[
            "CREATE TABLE my_table1 (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
            "CREATE TABLE my_table2 (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
        ],
        task_id="spanner_database_deploy_task",
    )
    # [END howto_operator_spanner_database_deploy]

    # [START howto_operator_spanner_database_update]
    spanner_database_update_task = SpannerUpdateDatabaseInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        ddl_statements=[
            "CREATE TABLE my_table3 (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
        ],
        task_id="spanner_database_update_task",
    )
    # [END howto_operator_spanner_database_update]

    # [START howto_operator_spanner_database_update_idempotent]
    spanner_database_update_idempotent1_task = SpannerUpdateDatabaseInstanceOperator(
        project_id=PROJECT_ID,
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        operation_id=OPERATION_ID,
        ddl_statements=[
            "CREATE TABLE my_table_unique (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
        ],
        task_id="spanner_database_update_idempotent1_task",
    )
    spanner_database_update_idempotent2_task = SpannerUpdateDatabaseInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        operation_id=OPERATION_ID,
        ddl_statements=[
            "CREATE TABLE my_table_unique (id INT64, name STRING(MAX)) PRIMARY KEY (id)",
        ],
        task_id="spanner_database_update_idempotent2_task",
    )
    # [END howto_operator_spanner_database_update_idempotent]

    # [START howto_operator_spanner_query]
    spanner_instance_query_task = SpannerQueryDatabaseInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        query=["DELETE FROM my_table2 WHERE true"],
        task_id="spanner_instance_query_task",
    )
    # [END howto_operator_spanner_query]

    # [START howto_operator_spanner_database_delete]
    spanner_database_delete_task = SpannerDeleteDatabaseInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID,
        database_id=GCP_SPANNER_DATABASE_ID,
        task_id="spanner_database_delete_task",
    )
    # [END howto_operator_spanner_database_delete]
    spanner_database_delete_task.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_operator_spanner_delete]
    spanner_instance_delete_task = SpannerDeleteInstanceOperator(
        instance_id=GCP_SPANNER_INSTANCE_ID, task_id="spanner_instance_delete_task"
    )
    # [END howto_operator_spanner_delete]
    spanner_instance_delete_task.trigger_rule = TriggerRule.ALL_DONE

    (
        spanner_instance_create_task
        >> spanner_instance_update_task
        >> spanner_database_deploy_task
        >> spanner_database_update_task
        >> spanner_database_update_idempotent1_task
        >> spanner_database_update_idempotent2_task
        >> spanner_instance_query_task
        >> spanner_database_delete_task
        >> spanner_instance_delete_task
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
