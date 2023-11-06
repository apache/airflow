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
Example Airflow DAG that performs query in a Cloud SQL instance for Postgres.
"""
from __future__ import annotations

import logging
import os
from collections import namedtuple
from copy import deepcopy
from datetime import datetime

from googleapiclient import discovery

from airflow import settings
from airflow.decorators import task, task_group
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceOperator,
    CloudSQLExecuteQueryOperator,
)
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = "cloudsql-query-pg"
REGION = "us-central1"

CLOUD_SQL_INSTANCE_NAME = f"instance-{ENV_ID}-{DAG_ID}".replace("_", "-")
CLOUD_SQL_DATABASE_NAME = "test_db"
CLOUD_SQL_USER = "test_user"
CLOUD_SQL_PASSWORD = "JoxHlwrPzwch0gz9"
CLOUD_SQL_PUBLIC_IP = "127.0.0.1"
CLOUD_SQL_PUBLIC_PORT = 5432
CLOUD_SQL_DATABASE_CREATE_BODY = {
    "instance": CLOUD_SQL_INSTANCE_NAME,
    "name": CLOUD_SQL_DATABASE_NAME,
    "project": PROJECT_ID,
}

CLOUD_SQL_INSTANCE_CREATION_BODY = {
    "name": CLOUD_SQL_INSTANCE_NAME,
    "settings": {
        "tier": "db-custom-1-3840",
        "dataDiskSizeGb": 30,
        "ipConfiguration": {
            "ipv4Enabled": True,
            "requireSsl": False,
            # Consider specifying your network mask
            # for allowing requests only from the trusted sources, not from anywhere
            "authorizedNetworks": [
                {"value": "0.0.0.0/0"},
            ],
        },
        "pricingPlan": "PER_USE",
    },
    # For using a different database version please check the link below
    # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1/SqlDatabaseVersion
    "databaseVersion": "POSTGRES_15",
    "region": REGION,
}

SQL = [
    "CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)",
    "CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)",
    "INSERT INTO TABLE_TEST VALUES (0)",
    "CREATE TABLE IF NOT EXISTS TABLE_TEST2 (I INTEGER)",
    "DROP TABLE TABLE_TEST",
    "DROP TABLE TABLE_TEST2",
]

# [START howto_operator_cloudsql_query_connections]

# Postgres: connect via proxy over TCP
CONNECTION_PROXY_TCP_KWARGS = {
    "conn_type": "gcpcloudsql",
    "login": CLOUD_SQL_USER,
    "password": CLOUD_SQL_PASSWORD,
    "host": CLOUD_SQL_PUBLIC_IP,
    "port": CLOUD_SQL_PUBLIC_PORT,
    "schema": CLOUD_SQL_DATABASE_NAME,
    "extra": {
        "database_type": "postgres",
        "project_id": PROJECT_ID,
        "location": REGION,
        "instance": CLOUD_SQL_INSTANCE_NAME,
        "use_proxy": "True",
        "sql_proxy_use_tcp": "True",
    },
}

# Postgres: connect via proxy over UNIX socket (specific proxy version)
CONNECTION_PROXY_SOCKET_KWARGS = {
    "conn_type": "gcpcloudsql",
    "login": CLOUD_SQL_USER,
    "password": CLOUD_SQL_PASSWORD,
    "host": CLOUD_SQL_PUBLIC_IP,
    "port": CLOUD_SQL_PUBLIC_PORT,
    "schema": CLOUD_SQL_DATABASE_NAME,
    "extra": {
        "database_type": "postgres",
        "project_id": PROJECT_ID,
        "location": REGION,
        "instance": CLOUD_SQL_INSTANCE_NAME,
        "use_proxy": "True",
        "sql_proxy_version": "v1.33.9",
        "sql_proxy_use_tcp": "False",
    },
}

# Postgres: connect directly via TCP (non-SSL)
CONNECTION_PUBLIC_TCP_KWARGS = {
    "conn_type": "gcpcloudsql",
    "login": CLOUD_SQL_USER,
    "password": CLOUD_SQL_PASSWORD,
    "host": CLOUD_SQL_PUBLIC_IP,
    "port": CLOUD_SQL_PUBLIC_PORT,
    "schema": CLOUD_SQL_DATABASE_NAME,
    "extra": {
        "database_type": "postgres",
        "project_id": PROJECT_ID,
        "location": REGION,
        "instance": CLOUD_SQL_INSTANCE_NAME,
        "use_proxy": "False",
        "use_ssl": "False",
    },
}

# [END howto_operator_cloudsql_query_connections]

CONNECTION_PROXY_TCP_ID = f"connection_{DAG_ID}_{ENV_ID}_proxy_tcp"
CONNECTION_PUBLIC_TCP_ID = f"connection_{DAG_ID}_{ENV_ID}_public_tcp"
CONNECTION_PROXY_SOCKET_ID = f"connection_{DAG_ID}_{ENV_ID}_proxy_socket"

ConnectionConfig = namedtuple("ConnectionConfig", "id kwargs use_public_ip")
CONNECTIONS = [
    ConnectionConfig(id=CONNECTION_PROXY_TCP_ID, kwargs=CONNECTION_PROXY_TCP_KWARGS, use_public_ip=False),
    ConnectionConfig(
        id=CONNECTION_PROXY_SOCKET_ID, kwargs=CONNECTION_PROXY_SOCKET_KWARGS, use_public_ip=False
    ),
    ConnectionConfig(id=CONNECTION_PUBLIC_TCP_ID, kwargs=CONNECTION_PUBLIC_TCP_KWARGS, use_public_ip=True),
]

log = logging.getLogger(__name__)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "cloudsql", "postgres"],
) as dag:
    create_cloud_sql_instance = CloudSQLCreateInstanceOperator(
        task_id="create_cloud_sql_instance",
        project_id=PROJECT_ID,
        instance=CLOUD_SQL_INSTANCE_NAME,
        body=CLOUD_SQL_INSTANCE_CREATION_BODY,
    )

    create_database = CloudSQLCreateInstanceDatabaseOperator(
        task_id="create_database", body=CLOUD_SQL_DATABASE_CREATE_BODY, instance=CLOUD_SQL_INSTANCE_NAME
    )

    @task
    def create_user() -> None:
        with discovery.build("sqladmin", "v1beta4") as service:
            request = service.users().insert(
                project=PROJECT_ID,
                instance=CLOUD_SQL_INSTANCE_NAME,
                body={
                    "name": CLOUD_SQL_USER,
                    "password": CLOUD_SQL_PASSWORD,
                },
            )
            request.execute()

    @task
    def get_public_ip() -> str | None:
        with discovery.build("sqladmin", "v1beta4") as service:
            request = service.connect().get(
                project=PROJECT_ID, instance=CLOUD_SQL_INSTANCE_NAME, fields="ipAddresses"
            )
            response = request.execute()
            for ip_item in response.get("ipAddresses", []):
                if ip_item["type"] == "PRIMARY":
                    return ip_item["ipAddress"]
            return None

    @task
    def create_connection(connection_id: str, connection_kwargs: dict, use_public_ip: bool, **kwargs) -> None:
        session = settings.Session()
        if session.query(Connection).filter(Connection.conn_id == connection_id).first():
            log.warning("Connection '%s' already exists", connection_id)
            return None
        _connection_kwargs = deepcopy(connection_kwargs)
        if use_public_ip:
            public_ip = kwargs["ti"].xcom_pull(task_ids="get_public_ip")
            _connection_kwargs["host"] = public_ip
        connection = Connection(conn_id=connection_id, **_connection_kwargs)
        session.add(connection)
        session.commit()
        log.info("Connection created: '%s'", connection_id)

    @task_group(group_id="create_connections")
    def create_connections():
        for con in CONNECTIONS:
            create_connection(
                connection_id=con.id,
                connection_kwargs=con.kwargs,
                use_public_ip=con.use_public_ip,
            )

    @task_group(group_id="execute_queries")
    def execute_queries():
        prev_task = None
        for conn in CONNECTIONS:
            connection_id = conn.id
            task_id = "execute_query_" + conn.id

            # [START howto_operator_cloudsql_query_operators]
            query_task = CloudSQLExecuteQueryOperator(
                gcp_cloudsql_conn_id=connection_id,
                task_id=task_id,
                sql=SQL,
            )
            # [END howto_operator_cloudsql_query_operators]

            if prev_task:
                prev_task >> query_task
            prev_task = query_task

    @task_group(group_id="teardown")
    def teardown():
        CloudSQLDeleteInstanceOperator(
            task_id="delete_cloud_sql_instance",
            project_id=PROJECT_ID,
            instance=CLOUD_SQL_INSTANCE_NAME,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        for con in CONNECTIONS:
            BashOperator(
                task_id=f"delete_connection_{con.id}",
                bash_command=f"airflow connections delete {con.id}",
                trigger_rule=TriggerRule.ALL_DONE,
            )

    (
        # TEST SETUP
        create_cloud_sql_instance
        >> [create_database, create_user(), get_public_ip()]
        >> create_connections()
        # TEST BODY
        >> execute_queries()
        # TEST TEARDOWN
        >> teardown()
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
