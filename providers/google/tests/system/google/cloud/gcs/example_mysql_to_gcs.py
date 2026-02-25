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
Example DAG using MySQLToGCSOperator.

This DAG relies on the following OS environment variables

* AIRFLOW__API__GOOGLE_KEY_PATH - Path to service account key file. Note, you can skip this variable if you
  run this DAG in a Composer environment.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from airflow.models.dag import DAG

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.operators.bash import BashOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google.gcp_api_client_helpers import create_airflow_connection, delete_airflow_connection
from system.openlineage.operator import OpenLineageTestOperator

try:
    from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
except ImportError:
    pytest.skip("MySQL not available", allow_module_level=True)

DAG_ID = "mysql_to_gcs"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "example-project")

IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))

REGION = "europe-west2"
ZONE = REGION + "-a"
NETWORK = "default"
CONNECTION_ID = f"mysql_{DAG_ID}_{ENV_ID}".replace("-", "_")
CONNECTION_TYPE = "mysql"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "result.json"

DB_NAME = "testdb"
DB_PORT = 3306
DB_USER_NAME = "root"
DB_USER_PASSWORD = "demo_password"
SETUP_MYSQL_COMMAND = f"""
sudo apt update &&
sudo apt install -y docker.io &&
sudo docker run -d -p {DB_PORT}:{DB_PORT} --name {DB_NAME} \
    -e MYSQL_ROOT_PASSWORD={DB_USER_PASSWORD} \
    -e MYSQL_DATABASE={DB_NAME} \
    mysql:8.1.0
"""
SQL_TABLE = "test_table"
SQL_CREATE = f"CREATE TABLE IF NOT EXISTS {DB_NAME}.{SQL_TABLE} (col_1 INT, col_2 VARCHAR(8))"
SQL_INSERT = f"INSERT INTO {DB_NAME}.{SQL_TABLE} (col_1, col_2) VALUES (1, 'one'), (2, 'two')"
SQL_SELECT = f"SELECT * FROM {DB_NAME}.{SQL_TABLE}"

GCE_MACHINE_TYPE = "n1-standard-1"
GCE_INSTANCE_NAME = f"instance-{DAG_ID}-{ENV_ID}".replace("_", "-")
GCE_INSTANCE_BODY = {
    "name": GCE_INSTANCE_NAME,
    "machine_type": f"zones/{ZONE}/machineTypes/{GCE_MACHINE_TYPE}",
    "disks": [
        {
            "boot": True,
            "auto_delete": True,
            "device_name": GCE_INSTANCE_NAME,
            "initialize_params": {
                "disk_size_gb": "10",
                "disk_type": f"zones/{ZONE}/diskTypes/pd-balanced",
                "source_image": "projects/debian-cloud/global/images/debian-12-bookworm-v20240611",
            },
        }
    ],
    "network_interfaces": [
        {
            "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
            "stack_type": "IPV4_ONLY",
            "subnetwork": f"regions/{REGION}/subnetworks/default",
        }
    ],
}
FIREWALL_RULE_NAME = f"allow-http-{DB_PORT}-{DAG_ID}-{ENV_ID}".replace("_", "-")
CREATE_FIREWALL_RULE_COMMAND = f"""
if [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then \
 gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; \
fi;

if [ -z $(gcloud compute firewall-rules list --filter=name:{FIREWALL_RULE_NAME} --format="value(name)" --project={PROJECT_ID}) ]; then \
    gcloud compute firewall-rules create {FIREWALL_RULE_NAME} \
      --project={PROJECT_ID} \
      --direction=INGRESS \
      --priority=100 \
      --network={NETWORK} \
      --action=ALLOW \
      --rules=tcp:{DB_PORT} \
      --source-ranges=0.0.0.0/0
else
    echo "Firewall rule {FIREWALL_RULE_NAME} already exists."
fi
"""
DELETE_FIREWALL_RULE_COMMAND = f"""
if [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then \
 gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; \
fi; \
if [ $(gcloud compute firewall-rules list --filter=name:{FIREWALL_RULE_NAME} --format="value(name)" --project={PROJECT_ID}) ]; then \
    gcloud compute firewall-rules delete {FIREWALL_RULE_NAME} --project={PROJECT_ID} --quiet; \
fi;
"""

log = logging.getLogger(__name__)

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "mysql", "gcs"],
) as dag:
    create_gce_instance = ComputeEngineInsertInstanceOperator(
        task_id="create_gce_instance",
        project_id=PROJECT_ID,
        zone=ZONE,
        body=GCE_INSTANCE_BODY,
    )

    create_firewall_rule = BashOperator(
        task_id="create_firewall_rule",
        bash_command=CREATE_FIREWALL_RULE_COMMAND,
    )

    setup_mysql = SSHOperator(
        task_id="setup_mysql",
        ssh_hook=ComputeEngineSSHHook(
            user="username",
            instance_name=GCE_INSTANCE_NAME,
            zone=ZONE,
            project_id=PROJECT_ID,
            use_oslogin=False,
            use_iap_tunnel=False,
            cmd_timeout=180,
        ),
        command=SETUP_MYSQL_COMMAND,
        retries=2,
    )

    @task
    def get_public_ip() -> str:
        hook = ComputeEngineHook()
        address = hook.get_instance_address(resource_id=GCE_INSTANCE_NAME, zone=ZONE, project_id=PROJECT_ID)
        return address

    get_public_ip_task = get_public_ip()

    @task
    def create_connection(connection_id: str, ip_address: str) -> None:
        connection: dict[str, Any] = {
            "description": "Example connection",
            "conn_type": CONNECTION_TYPE,
            "host": ip_address,
            "login": DB_USER_NAME,
            "password": DB_USER_PASSWORD,
            "port": DB_PORT,
        }
        create_airflow_connection(
            connection_id=connection_id,
            connection_conf=connection,
            is_composer=IS_COMPOSER,
        )

    create_connection_task = create_connection(connection_id=CONNECTION_ID, ip_address=get_public_ip_task)

    create_sql_table = SQLExecuteQueryOperator(
        task_id="create_sql_table",
        conn_id=CONNECTION_ID,
        sql=SQL_CREATE,
        retries=4,
    )

    insert_sql_data = SQLExecuteQueryOperator(
        task_id="insert_sql_data",
        conn_id=CONNECTION_ID,
        sql=SQL_INSERT,
    )

    create_gcs_bucket = GCSCreateBucketOperator(
        task_id="create_gcs_bucket",
        bucket_name=BUCKET_NAME,
    )

    # [START howto_operator_mysql_to_gcs]
    mysql_to_gcs = MySQLToGCSOperator(
        task_id="mysql_to_gcs",
        mysql_conn_id=CONNECTION_ID,
        sql=SQL_SELECT,
        bucket=BUCKET_NAME,
        filename=FILE_NAME,
        export_format="csv",
    )
    # [END howto_operator_mysql_to_gcs]

    delete_gcs_bucket = GCSDeleteBucketOperator(
        task_id="delete_gcs_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_firewall_rule = BashOperator(
        task_id="delete_firewall_rule",
        bash_command=DELETE_FIREWALL_RULE_COMMAND,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_gce_instance = ComputeEngineDeleteInstanceOperator(
        task_id="delete_gce_instance",
        resource_id=GCE_INSTANCE_NAME,
        zone=ZONE,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(task_id="delete_connection")
    def delete_connection(connection_id: str) -> None:
        delete_airflow_connection(connection_id=connection_id, is_composer=IS_COMPOSER)

    delete_connection_task = delete_connection(connection_id=CONNECTION_ID)

    check_openlineage_events = OpenLineageTestOperator(
        task_id="check_openlineage_events",
        file_path=str(Path(__file__).parent / "resources" / "openlineage" / "mysql_to_gcs.json"),
    )

    (
        # TEST SETUP
        create_gce_instance
        >> setup_mysql
        >> create_firewall_rule
        >> get_public_ip_task
        >> create_connection_task
        >> create_sql_table
        >> insert_sql_data
        >> create_gcs_bucket
        # TEST BODY
        >> mysql_to_gcs
    )

    # TEST TEARDOWN
    mysql_to_gcs >> [
        delete_gcs_bucket,
        delete_firewall_rule,
        delete_gce_instance,
        delete_connection_task,
        check_openlineage_events,
    ]

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
