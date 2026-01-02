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
Example Airflow DAG that performs query in a Cloud SQL instance with SSL support.
"""

from __future__ import annotations

import base64
import json
import logging
import os
import random
import string
from collections.abc import Iterable
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any

from googleapiclient import discovery

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.dag import DAG
from airflow.providers.google.cloud.hooks.cloud_sql import CloudSQLHook
from airflow.providers.google.cloud.hooks.secret_manager import GoogleCloudSecretManagerHook
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

from airflow.providers.google.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
from airflow.providers.google.tests.system.google.gcp_api_client_helpers import (
    create_airflow_connection,
    delete_airflow_connection,
)

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
DAG_ID = "cloudsql_query_ssl"
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
        "requireSsl": True,
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
            "sslMode": "ENCRYPTED_ONLY",
            "enablePrivatePathForGoogleCloudServices": True,
            "privateNetwork": f"projects/{PROJECT_ID}/global/networks/default",
        }
    # Use connection to Cloud SQL instance via Public IP from anywhere (mask 0.0.0.0/0).
    # Consider specifying your network mask
    # for allowing requests only from the trusted sources, not from anywhere.
    return {
        "ipv4Enabled": True,
        "requireSsl": False,
        "sslMode": "ENCRYPTED_ONLY",
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
# Connect directly via TCP (SSL)
CONNECTION_PUBLIC_TCP_SSL_KWARGS = {
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
        "use_ssl": "True",
    },
}
# [END howto_operator_cloudsql_query_connections]

CONNECTION_PUBLIC_TCP_SSL_ID = f"{DAG_ID}_{ENV_ID}_tcp_ssl"

SQL = [
    "CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)",
    "CREATE TABLE IF NOT EXISTS TABLE_TEST (I INTEGER)",
    "INSERT INTO TABLE_TEST VALUES (0)",
    "CREATE TABLE IF NOT EXISTS TABLE_TEST2 (I INTEGER)",
    "DROP TABLE TABLE_TEST",
    "DROP TABLE TABLE_TEST2",
]

SSL_PATH = f"/{DAG_ID}/{ENV_ID}"
SSL_LOCAL_PATH_PREFIX = "/tmp"
SSL_COMPOSER_PATH_PREFIX = "/home/airflow/gcs/data"
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

# MySQL: connect directly via TCP (SSL)
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

        conn_id = f"{CONNECTION_PUBLIC_TCP_SSL_ID}_{database_type}"

        @task(task_id=f"create_connection_{database_type}")
        def create_connection(
            connection_id: str, instance: str, db_type: str, ip_address: str, port: str
        ) -> str | None:
            connection: dict[str, Any] = deepcopy(CONNECTION_PUBLIC_TCP_SSL_KWARGS)
            connection["extra"]["instance"] = instance
            connection["host"] = ip_address
            connection["extra"]["database_type"] = db_type
            connection["port"] = port
            connection["extra"] = json.dumps(connection["extra"])
            create_airflow_connection(
                connection_id=connection_id,
                connection_conf=connection,
                is_composer=run_in_composer(),
            )
            return connection_id

        create_connection_task = create_connection(
            connection_id=conn_id,
            instance=cloud_sql_instance_name,
            db_type=database_type,
            ip_address=get_ip_address_task,
            port=db_provider["port"],
        )

        @task(task_id=f"create_ssl_certificates_{database_type}")
        def create_ssl_certificate(instance: str, connection_id: str) -> dict[str, Any]:
            hook = CloudSQLHook(api_version="v1", gcp_conn_id=connection_id)
            certificate_name = f"test_cert_{''.join(random.choice(string.ascii_letters) for _ in range(8))}"
            response = hook.create_ssl_certificate(
                instance=instance,
                body={"common_name": certificate_name},
                project_id=PROJECT_ID,
            )
            return response

        create_ssl_certificate_task = create_ssl_certificate(
            instance=cloud_sql_instance_name, connection_id=create_connection_task
        )

        @task(task_id=f"save_ssl_cert_locally_{database_type}")
        def save_ssl_cert_locally(ssl_cert: dict[str, Any], db_type: str) -> dict[str, str]:
            folder = SSL_COMPOSER_PATH_PREFIX if run_in_composer() else SSL_LOCAL_PATH_PREFIX
            folder += f"/certs/{db_type}/{ssl_cert['operation']['name']}"
            if not os.path.exists(folder):
                os.makedirs(folder)
            _ssl_root_cert_path = f"{folder}/sslrootcert.pem"
            _ssl_cert_path = f"{folder}/sslcert.pem"
            _ssl_key_path = f"{folder}/sslkey.pem"
            with open(_ssl_root_cert_path, "w") as ssl_root_cert_file:
                ssl_root_cert_file.write(ssl_cert["serverCaCert"]["cert"])
            with open(_ssl_cert_path, "w") as ssl_cert_file:
                ssl_cert_file.write(ssl_cert["clientCert"]["certInfo"]["cert"])
            with open(_ssl_key_path, "w") as ssl_key_file:
                ssl_key_file.write(ssl_cert["clientCert"]["certPrivateKey"])
            return {
                "sslrootcert": _ssl_root_cert_path,
                "sslcert": _ssl_cert_path,
                "sslkey": _ssl_key_path,
            }

        save_ssl_cert_locally_task = save_ssl_cert_locally(
            ssl_cert=create_ssl_certificate_task, db_type=database_type
        )

        @task(task_id=f"save_ssl_cert_to_secret_manager_{database_type}")
        def save_ssl_cert_to_secret_manager(ssl_cert: dict[str, Any], db_type: str) -> str:
            hook = GoogleCloudSecretManagerHook()
            payload = {
                "sslrootcert": ssl_cert["serverCaCert"]["cert"],
                "sslcert": ssl_cert["clientCert"]["certInfo"]["cert"],
                "sslkey": ssl_cert["clientCert"]["certPrivateKey"],
            }
            _secret_id = f"secret_{DAG_ID}_{ENV_ID}_{db_type}"

            if not hook.secret_exists(project_id=PROJECT_ID, secret_id=_secret_id):
                hook.create_secret(
                    secret_id=_secret_id,
                    project_id=PROJECT_ID,
                )

            hook.add_secret_version(
                project_id=PROJECT_ID,
                secret_id=_secret_id,
                secret_payload=dict(data=base64.b64encode(json.dumps(payload).encode("ascii"))),
            )

            return _secret_id

        save_ssl_cert_to_secret_manager_task = save_ssl_cert_to_secret_manager(
            ssl_cert=create_ssl_certificate_task, db_type=database_type
        )

        task_id = f"example_cloud_sql_query_ssl_{database_type}"
        ssl_server_cert_path = (
            f"{{{{ task_instance.xcom_pull('save_ssl_cert_locally_{database_type}')['sslrootcert'] }}}}"
        )
        ssl_cert_path = (
            f"{{{{ task_instance.xcom_pull('save_ssl_cert_locally_{database_type}')['sslcert'] }}}}"
        )
        ssl_key_path = f"{{{{ task_instance.xcom_pull('save_ssl_cert_locally_{database_type}')['sslkey'] }}}}"

        # [START howto_operator_cloudsql_query_operators_ssl]
        query_task = CloudSQLExecuteQueryOperator(
            gcp_cloudsql_conn_id=conn_id,
            task_id=task_id,
            sql=SQL,
            ssl_client_cert=ssl_cert_path,
            ssl_server_cert=ssl_server_cert_path,
            ssl_client_key=ssl_key_path,
        )
        # [END howto_operator_cloudsql_query_operators_ssl]

        task_id = f"example_cloud_sql_query_ssl_secret_{database_type}"
        secret_id = f"{{{{ task_instance.xcom_pull('save_ssl_cert_to_secret_manager_{database_type}') }}}}"

        # [START howto_operator_cloudsql_query_operators_ssl_secret_id]
        query_task_secret = CloudSQLExecuteQueryOperator(
            gcp_cloudsql_conn_id=conn_id,
            task_id=task_id,
            sql=SQL,
            ssl_secret_id=secret_id,
        )
        # [END howto_operator_cloudsql_query_operators_ssl_secret_id]

        delete_instance = CloudSQLDeleteInstanceOperator(
            task_id=f"delete_cloud_sql_instance_{database_type}",
            project_id=PROJECT_ID,
            instance=cloud_sql_instance_name,
            trigger_rule=TriggerRule.ALL_DONE,
        )

        @task(task_id=f"delete_connection_{database_type}")
        def delete_connection(connection_id: str) -> None:
            delete_airflow_connection(connection_id=connection_id, is_composer=run_in_composer())

        delete_connection_task = delete_connection(connection_id=conn_id)

        @task(task_id=f"delete_secret_{database_type}")
        def delete_secret(ssl_secret_id, db_type: str) -> None:
            hook = GoogleCloudSecretManagerHook()
            if hook.secret_exists(project_id=PROJECT_ID, secret_id=ssl_secret_id):
                hook.delete_secret(secret_id=ssl_secret_id, project_id=PROJECT_ID)

        delete_secret_task = delete_secret(
            ssl_secret_id=save_ssl_cert_to_secret_manager_task, db_type=database_type
        )

        (
            # TEST SETUP
            create_cloud_sql_instance
            >> [create_database, create_user_task, get_ip_address_task]
            >> create_connection_task
            >> create_ssl_certificate_task
            >> [save_ssl_cert_locally_task, save_ssl_cert_to_secret_manager_task]
            # TEST BODY
            >> query_task
            >> query_task_secret
            # TEST TEARDOWN
            >> [delete_instance, delete_connection_task, delete_secret_task]
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
