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
Example Airflow DAG that performs query in a Cloud SQL instance.
"""

from __future__ import annotations

import logging
import os
from collections import namedtuple
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from googleapiclient import discovery

from airflow import settings
from airflow.decorators import task, task_group
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceOperator,
    CloudSQLExecuteQueryOperator,
)
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
DAG_ID = "cloudsql_query"
REGION = "us-central1"
HOME_DIR = Path.home()

COMPOSER_ENVIRONMENT = os.environ.get("COMPOSER_ENVIRONMENT", "")


def run_in_composer():
    return bool(COMPOSER_ENVIRONMENT)


CLOUD_SQL_INSTANCE_NAME_TEMPLATE = f"{ENV_ID}-{DAG_ID}".replace("_", "-")
CLOUD_SQL_INSTANCE_CREATE_BODY_TEMPLATE: dict[str, Any] = {
    "name": CLOUD_SQL_INSTANCE_NAME_TEMPLATE,
    "settings": {
        "tier": "db-custom-1-3840",
        "dataDiskSizeGb": 30,
        "pricingPlan": "PER_USE",
        "ipConfiguration": {},
    },
    # For using a different database version please check the link below.
    # https://cloud.google.com/sql/docs/mysql/admin-api/rest/v1/SqlDatabaseVersion
    "databaseVersion": "1.2.3",
    "region": REGION,
    "ipConfiguration": {
        "ipv4Enabled": True,
        "requireSsl": False,
        "authorizedNetworks": [
            {"value": "0.0.0.0/0"},
        ],
    },
}
DB_PROVIDERS: Iterable[dict[str, str]] = (
    {
        "database_type": "postgres",
        "port": "5432",
        "database_version": "POSTGRES_15",
        "cloud_sql_instance_name": f"{CLOUD_SQL_INSTANCE_NAME_TEMPLATE}-postgres",
    },
    {
        "database_type": "mysql",
        "port": "3306",
        "database_version": "MYSQL_8_0",
        "cloud_sql_instance_name": f"{CLOUD_SQL_INSTANCE_NAME_TEMPLATE}-mysql",
    },
)


def ip_configuration() -> dict[str, Any]:
    """Generates an ip configuration for a CloudSQL instance creation body"""
    if run_in_composer():
        # Use connection to Cloud SQL instance via Private IP within the Cloud Composer's network.
        return {
            "ipv4Enabled": True,
            "requireSsl": False,
            "enablePrivatePathForGoogleCloudServices": True,
            "privateNetwork": f"projects/{PROJECT_ID}/global/networks/default",
        }
    else:
        # Use connection to Cloud SQL instance via Public IP from anywhere (mask 0.0.0.0/0).
        # Consider specifying your network mask
        # for allowing requests only from the trusted sources, not from anywhere.
        return {
            "ipv4Enabled": True,
            "requireSsl": False,
            "authorizedNetworks": [
                {"value": "0.0.0.0/0"},
            ],
        }


def cloud_sql_instance_create_body(database_provider: dict[str, Any]) -> dict[str, Any]:
    """Generates a CloudSQL instance creation body"""
    create_body: dict[str, Any] = deepcopy(CLOUD_SQL_INSTANCE_CREATE_BODY_TEMPLATE)
    create_body["name"] = database_provider["cloud_sql_instance_name"]
    create_body["databaseVersion"] = database_provider["database_version"]
    create_body["settings"]["ipConfiguration"] = ip_configuration()
    return create_body


CLOUD_SQL_DATABASE_NAME = "test_db"
CLOUD_SQL_USER = "test_user"
CLOUD_SQL_PASSWORD = "JoxHlwrPzwch0gz9"
CLOUD_SQL_IP_ADDRESS = "127.0.0.1"
CLOUD_SQL_PUBLIC_PORT = 5432


def cloud_sql_database_create_body(instance: str) -> dict[str, Any]:
    """Generates a CloudSQL database creation body"""
    return {
        "instance": instance,
        "name": CLOUD_SQL_DATABASE_NAME,
        "project": PROJECT_ID,
    }


CLOUD_SQL_INSTANCE_NAME = ""
DATABASE_TYPE = ""  # "postgres|mysql|mssql"


# [START howto_operator_cloudsql_query_connections]
# Connect via proxy over TCP
CONNECTION_PROXY_TCP_KWARGS = {
    "conn_type": "gcpcloudsql",
    "login": CLOUD_SQL_USER,
    "password": CLOUD_SQL_PASSWORD,
    "host": CLOUD_SQL_IP_ADDRESS,
    "port": CLOUD_SQL_PUBLIC_PORT,
    "schema": CLOUD_SQL_DATABASE_NAME,
    "extra": {
        "database_type": DATABASE_TYPE,
        "project_id": PROJECT_ID,
        "location": REGION,
        "instance": CLOUD_SQL_INSTANCE_NAME,
        "use_proxy": "True",
        "sql_proxy_use_tcp": "True",
    },
}

# Connect via proxy over UNIX socket (specific proxy version)
CONNECTION_PROXY_SOCKET_KWARGS = {
    "conn_type": "gcpcloudsql",
    "login": CLOUD_SQL_USER,
    "password": CLOUD_SQL_PASSWORD,
    "host": CLOUD_SQL_IP_ADDRESS,
    "port": CLOUD_SQL_PUBLIC_PORT,
    "schema": CLOUD_SQL_DATABASE_NAME,
    "extra": {
        "database_type": DATABASE_TYPE,
        "project_id": PROJECT_ID,
        "location": REGION,
        "instance": CLOUD_SQL_INSTANCE_NAME,
        "use_proxy": "True",
        "sql_proxy_version": "v1.33.9",
        "sql_proxy_use_tcp": "False",
    },
}

# Connect directly via TCP (non-SSL)
CONNECTION_PUBLIC_TCP_KWARGS = {
    "conn_type": "gcpcloudsql",
    "login": CLOUD_SQL_USER,
    "password": CLOUD_SQL_PASSWORD,
    "host": CLOUD_SQL_IP_ADDRESS,
    "port": CLOUD_SQL_PUBLIC_PORT,
    "schema": CLOUD_SQL_DATABASE_NAME,
    "extra": {
        "database_type": DATABASE_TYPE,
        "project_id": PROJECT_ID,
        "location": REGION,
        "instance": CLOUD_SQL_INSTANCE_NAME,
        "use_proxy": "False",
        "use_ssl": "False",
    },
}

# [END howto_operator_cloudsql_query_connections]


CONNECTION_PROXY_SOCKET_ID = f"{DAG_ID}_{ENV_ID}_proxy_socket"
CONNECTION_PROXY_TCP_ID = f"{DAG_ID}_{ENV_ID}_proxy_tcp"
CONNECTION_PUBLIC_TCP_ID = f"{DAG_ID}_{ENV_ID}_public_tcp"

ConnectionConfig = namedtuple("ConnectionConfig", "id kwargs")
CONNECTIONS = [
    ConnectionConfig(id=CONNECTION_PROXY_SOCKET_ID, kwargs=CONNECTION_PROXY_SOCKET_KWARGS),
    ConnectionConfig(id=CONNECTION_PROXY_TCP_ID, kwargs=CONNECTION_PROXY_TCP_KWARGS),
    ConnectionConfig(id=CONNECTION_PUBLIC_TCP_ID, kwargs=CONNECTION_PUBLIC_TCP_KWARGS),
]

SQL = [
    "CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)",
    "CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)",
    "INSERT INTO TABLE_TEST VALUES (0)",
    "CREATE TABLE IF NOT EXISTS TABLE_TEST2 (I INTEGER)",
    "DROP TABLE TABLE_TEST",
    "DROP TABLE TABLE_TEST2",
]

# [START howto_operator_cloudsql_query_connections_env]

# The connections below are created using one of the standard approaches - via environment
# variables named AIRFLOW_CONN_* . The connections can also be created in the database
# of AIRFLOW (using command line or UI).

postgres_kwargs = {
    "user": "user",
    "password": "password",
    "public_ip": "public_ip",
    "public_port": "public_port",
    "database": "database",
    "project_id": "project_id",
    "location": "location",
    "instance": "instance",
    "client_cert_file": "client_cert_file",
    "client_key_file": "client_key_file",
    "server_ca_file": "server_ca_file",
}

# Postgres: connect via proxy over TCP
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_TCP"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=postgres&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=True&"
    "sql_proxy_use_tcp=True".format(**postgres_kwargs)
)

# Postgres: connect via proxy over UNIX socket (specific proxy version)
os.environ["AIRFLOW_CONN_PROXY_POSTGRES_SOCKET"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=postgres&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=True&"
    "sql_proxy_version=v1.13&"
    "sql_proxy_use_tcp=False".format(**postgres_kwargs)
)

# Postgres: connect directly via TCP (non-SSL)
os.environ["AIRFLOW_CONN_PUBLIC_POSTGRES_TCP"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=postgres&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=False&"
    "use_ssl=False".format(**postgres_kwargs)
)

# Postgres: connect directly via TCP (SSL)
os.environ["AIRFLOW_CONN_PUBLIC_POSTGRES_TCP_SSL"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=postgres&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=False&"
    "use_ssl=True&"
    "sslcert={client_cert_file}&"
    "sslkey={client_key_file}&"
    "sslrootcert={server_ca_file}".format(**postgres_kwargs)
)

mysql_kwargs = {
    "user": "user",
    "password": "password",
    "public_ip": "public_ip",
    "public_port": "public_port",
    "database": "database",
    "project_id": "project_id",
    "location": "location",
    "instance": "instance",
    "client_cert_file": "client_cert_file",
    "client_key_file": "client_key_file",
    "server_ca_file": "server_ca_file",
}

# MySQL: connect via proxy over TCP (specific proxy version)
os.environ["AIRFLOW_CONN_PROXY_MYSQL_TCP"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=mysql&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=True&"
    "sql_proxy_version=v1.13&"
    "sql_proxy_use_tcp=True".format(**mysql_kwargs)
)

# MySQL: connect via proxy over UNIX socket using pre-downloaded Cloud Sql Proxy binary
os.environ["AIRFLOW_CONN_PROXY_MYSQL_SOCKET"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=mysql&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=True&"
    "sql_proxy_use_tcp=False".format(**mysql_kwargs)
)

# MySQL: connect directly via TCP (non-SSL)
os.environ["AIRFLOW_CONN_PUBLIC_MYSQL_TCP"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=mysql&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=False&"
    "use_ssl=False".format(**mysql_kwargs)
)

# MySQL: connect directly via TCP (SSL) and with fixed Cloud Sql Proxy binary path
os.environ["AIRFLOW_CONN_PUBLIC_MYSQL_TCP_SSL"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=mysql&"
    "project_id={project_id}&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=False&"
    "use_ssl=True&"
    "sslcert={client_cert_file}&"
    "sslkey={client_key_file}&"
    "sslrootcert={server_ca_file}".format(**mysql_kwargs)
)

# Special case: MySQL: connect directly via TCP (SSL) and with fixed Cloud Sql
# Proxy binary path AND with missing project_id
os.environ["AIRFLOW_CONN_PUBLIC_MYSQL_TCP_SSL_NO_PROJECT_ID"] = (
    "gcpcloudsql://{user}:{password}@{public_ip}:{public_port}/{database}?"
    "database_type=mysql&"
    "location={location}&"
    "instance={instance}&"
    "use_proxy=False&"
    "use_ssl=True&"
    "sslcert={client_cert_file}&"
    "sslkey={client_key_file}&"
    "sslrootcert={server_ca_file}".format(**mysql_kwargs)
)
# [END howto_operator_cloudsql_query_connections_env]


log = logging.getLogger(__name__)


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "cloudsql", "postgres"],
) as dag:
    for db_provider in DB_PROVIDERS:
        database_type: str = db_provider["database_type"]
        cloud_sql_instance_name: str = db_provider["cloud_sql_instance_name"]

        create_cloud_sql_instance = CloudSQLCreateInstanceOperator(
            task_id=f"create_cloud_sql_instance_{database_type}",
            project_id=PROJECT_ID,
            instance=cloud_sql_instance_name,
            body=cloud_sql_instance_create_body(database_provider=db_provider),
        )

        create_database = CloudSQLCreateInstanceDatabaseOperator(
            task_id=f"create_database_{database_type}",
            body=cloud_sql_database_create_body(instance=cloud_sql_instance_name),
            instance=cloud_sql_instance_name,
        )

        @task(task_id=f"create_user_{database_type}")
        def create_user(instance: str) -> None:
            with discovery.build("sqladmin", "v1beta4") as service:
                request = service.users().insert(
                    project=PROJECT_ID,
                    instance=instance,
                    body={
                        "name": CLOUD_SQL_USER,
                        "password": CLOUD_SQL_PASSWORD,
                    },
                )
                request.execute()
            return None

        create_user_task = create_user(instance=cloud_sql_instance_name)

        @task(task_id=f"get_ip_address_{database_type}")
        def get_ip_address(instance: str) -> str | None:
            """Returns a Cloud SQL instance IP address.

            If the test is running in Cloud Composer, the Private IP address is used, otherwise Public IP."""
            with discovery.build("sqladmin", "v1beta4") as service:
                request = service.connect().get(
                    project=PROJECT_ID,
                    instance=instance,
                    fields="ipAddresses",
                )
                response = request.execute()
                for ip_item in response.get("ipAddresses", []):
                    if run_in_composer():
                        if ip_item["type"] == "PRIVATE":
                            return ip_item["ipAddress"]
                    else:
                        if ip_item["type"] == "PRIMARY":
                            return ip_item["ipAddress"]
                return None

        get_ip_address_task = get_ip_address(instance=cloud_sql_instance_name)

        @task(task_id=f"create_connection_{database_type}")
        def create_connection(
            connection_id: str,
            instance: str,
            db_type: str,
            ip_address: str,
            port: str,
            kwargs: dict[str, Any],
        ) -> str | None:
            session = settings.Session()
            log.info("Removing connection %s if it exists", connection_id)
            query = session.query(Connection).filter(Connection.conn_id == connection_id)
            query.delete()

            connection: dict[str, Any] = deepcopy(kwargs)
            connection["extra"]["instance"] = instance
            connection["host"] = ip_address
            connection["extra"]["database_type"] = db_type
            connection["port"] = port
            _conn = Connection(conn_id=connection_id, **connection)
            session.add(_conn)
            session.commit()
            log.info("Connection created: '%s'", connection_id)
            return connection_id

        @task_group(group_id=f"create_connections_{database_type}")
        def create_connections(instance: str, db_type: str, ip_address: str, port: str):
            for conn in CONNECTIONS:
                conn_id = f"{conn.id}_{database_type}"
                create_connection(
                    connection_id=conn_id,
                    instance=instance,
                    db_type=db_type,
                    ip_address=ip_address,
                    port=port,
                    kwargs=conn.kwargs,
                )

        create_connections_task = create_connections(
            instance=cloud_sql_instance_name,
            db_type=database_type,
            ip_address=get_ip_address_task,
            port=db_provider["port"],
        )

        @task_group(group_id=f"execute_queries_{database_type}")
        def execute_queries(db_type: str):
            prev_task = None
            for conn in CONNECTIONS:
                connection_id = f"{conn.id}_{db_type}"
                task_id = f"execute_query_{connection_id}"

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

        execute_queries_task = execute_queries(db_type=database_type)

        @task()
        def delete_connection(connection_id: str) -> None:
            session = Session()
            log.info("Removing connection %s", connection_id)
            query = session.query(Connection).filter(Connection.conn_id == connection_id)
            query.delete()
            session.commit()

        delete_connections_task = delete_connection.expand(
            connection_id=[f"{conn.id}_{database_type}" for conn in CONNECTIONS]
        )

        delete_instance = CloudSQLDeleteInstanceOperator(
            task_id=f"delete_cloud_sql_instance_{database_type}",
            project_id=PROJECT_ID,
            instance=cloud_sql_instance_name,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        (
            # TEST SETUP
            create_cloud_sql_instance
            >> [
                create_database,
                create_user_task,
                get_ip_address_task,
            ]
            >> create_connections_task
            # TEST BODY
            >> execute_queries_task
            # TEST TEARDOWN
            >> [delete_instance, delete_connections_task]
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
