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
Example Airflow Dag that performs a query in a Postgres Cloud SQL instance with proxy IAM authentication.
"""

from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime
from typing import Any

from googleapiclient import discovery

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceOperator,
    CloudSQLExecuteQueryOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
from system.google.gcp_api_client_helpers import create_airflow_connection, delete_airflow_connection

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
DAG_ID = "cloudsql_query_proxy_iam"
REGION = "us-central1"
IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))

CLOUD_SQL_INSTANCE_NAME = f"{ENV_ID}-{DAG_ID}-postgres".replace("_", "-")
CLOUD_SQL_DATABASE_NAME = "test_db"
CLOUD_IAM_SA = os.environ.get("SYSTEM_TESTS_CLOUDSQL_SA", "test_iam_sa")
CLOUD_SQL_IAM_SA = CLOUD_IAM_SA.split(".gserviceaccount.com")[0]
CLOUD_SQL_IP_ADDRESS = "127.0.0.1"
CLOUD_SQL_PUBLIC_PORT = 5432
CONNECTION_PROXY_IAM_ID = f"{DAG_ID}_{ENV_ID}_proxy_iam"

CLOUD_SQL_INSTANCE_CREATE_BODY: dict[str, Any] = {
    "name": CLOUD_SQL_INSTANCE_NAME,
    "settings": {
        "tier": "db-custom-1-3840",
        "dataDiskSizeGb": 30,
        "pricingPlan": "PER_USE",
        "ipConfiguration": {"ipv4Enabled": True},
        "databaseFlags": [{"name": "cloudsql.iam_authentication", "value": "on"}],
    },
    "databaseVersion": "POSTGRES_15",
    "region": REGION,
}

# [START howto_operator_cloudsql_proxy_iam_connections]
CONNECTION_WITH_PROXY_IAM_KWARGS = {
    "conn_type": "gcpcloudsql",
    "login": CLOUD_IAM_SA,
    "password": "",
    "host": CLOUD_SQL_IP_ADDRESS,
    "port": CLOUD_SQL_PUBLIC_PORT,
    "schema": CLOUD_SQL_DATABASE_NAME,
    "extra": {
        "database_type": "postgres",
        "project_id": PROJECT_ID,
        "location": REGION,
        "instance": CLOUD_SQL_INSTANCE_NAME,
        "use_proxy": "True",
        "sql_proxy_use_tcp": "True",
        "sql_proxy_enable_iam_login": "True",
    },
}
# [END howto_operator_cloudsql_proxy_iam_connections]


def cloud_sql_database_create_body(instance: str) -> dict[str, Any]:
    """Generates a Cloud SQL database creation body."""
    return {
        "instance": instance,
        "name": CLOUD_SQL_DATABASE_NAME,
        "project": PROJECT_ID,
    }


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2026, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "cloudsql", "postgres"],
) as dag:
    create_cloud_sql_instance = CloudSQLCreateInstanceOperator(
        task_id="create_cloud_sql_instance_postgres",
        project_id=PROJECT_ID,
        instance=CLOUD_SQL_INSTANCE_NAME,
        body=CLOUD_SQL_INSTANCE_CREATE_BODY,
    )

    create_database = CloudSQLCreateInstanceDatabaseOperator(
        task_id="create_database_postgres",
        body=cloud_sql_database_create_body(instance=CLOUD_SQL_INSTANCE_NAME),
        instance=CLOUD_SQL_INSTANCE_NAME,
    )

    @task(task_id="create_user_postgres")
    def create_user(instance: str, service_account: str) -> None:
        with discovery.build("sqladmin", "v1beta4") as service:
            request = service.users().insert(
                project=PROJECT_ID,
                instance=instance,
                body={
                    "name": service_account,
                    "type": "CLOUD_IAM_SERVICE_ACCOUNT",
                },
            )
            request.execute()

    create_user_task = create_user(instance=CLOUD_SQL_INSTANCE_NAME, service_account=CLOUD_SQL_IAM_SA)

    @task(task_id="create_connection_postgres")
    def create_connection(connection_id: str, instance: str) -> str:
        connection: dict[str, Any] = deepcopy(CONNECTION_WITH_PROXY_IAM_KWARGS)
        connection["extra"]["instance"] = instance
        connection["extra"] = json.dumps(connection["extra"])
        create_airflow_connection(
            connection_id=connection_id, connection_conf=connection, is_composer=IS_COMPOSER
        )
        return connection_id

    create_connection_task = create_connection(
        connection_id=CONNECTION_PROXY_IAM_ID,
        instance=CLOUD_SQL_INSTANCE_NAME,
    )

    query_task = CloudSQLExecuteQueryOperator(
        gcp_cloudsql_conn_id=CONNECTION_PROXY_IAM_ID,
        task_id="example_cloud_sql_query_proxy_iam_postgres",
        sql=["SELECT 1"],
    )

    delete_instance = CloudSQLDeleteInstanceOperator(
        task_id="delete_cloud_sql_instance_postgres",
        project_id=PROJECT_ID,
        instance=CLOUD_SQL_INSTANCE_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(task_id="delete_connection_postgres")
    def delete_connection(connection_id: str) -> None:
        delete_airflow_connection(connection_id=connection_id, is_composer=IS_COMPOSER)

    delete_connection_task = delete_connection(connection_id=CONNECTION_PROXY_IAM_ID)

    (
        # TEST SETUP
        create_cloud_sql_instance
        >> [create_database, create_user_task]
        >> create_connection_task
        # TEST BODY
        >> query_task
        # TEST TEARDOWN
        >> [delete_instance, delete_connection_task]
    )

    # ### Everything below this line is not part of example ###
    # ### Just for system tests purpose ###
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the Dag
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example Dag with pytest (see: contributing-docs/testing/system_tests.rst)
test_run = get_test_run(dag)
