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
Example DAG using SQLToGoogleSheetsOperator.

This DAG relies on the following OS environment variables

* AIRFLOW__API__GOOGLE_KEY_PATH - Path to service account key file. Note, you can skip this variable if you
  run this DAG in a Composer environment.

"""

from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
)
from airflow.providers.google.suite.operators.sheets import GoogleSheetsCreateSpreadsheetOperator
from airflow.providers.google.suite.transfers.sql_to_sheets import SQLToGoogleSheetsOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.standard.core.operators.bash import BashOperator
from airflow.settings import Session, json
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "sql_to_sheets"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "example-project")

REGION = "europe-west2"
ZONE = REGION + "-a"
NETWORK = "default"
CONNECTION_ID = f"pg_{DAG_ID}_{ENV_ID}".replace("-", "_")
CONNECTION_TYPE = "postgres"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "result.json"

DB_NAME = "testdb"
DB_PORT = 5432
DB_USER_NAME = "root"
DB_USER_PASSWORD = "demo_password"
SETUP_POSTGRES_COMMAND = f"""
sudo apt update &&
sudo apt install -y docker.io &&
sudo docker run -d -p {DB_PORT}:{DB_PORT} --name {DB_NAME} \
    -e PGUSER={DB_USER_NAME} \
    -e POSTGRES_USER={DB_USER_NAME} \
    -e POSTGRES_PASSWORD={DB_USER_PASSWORD} \
    -e POSTGRES_DB={DB_NAME} \
    postgres
"""
SQL_TABLE = "test_table"
SQL_CREATE = f"CREATE TABLE IF NOT EXISTS {SQL_TABLE} (col_1 INT, col_2 VARCHAR(8))"
SQL_INSERT = f"INSERT INTO {SQL_TABLE} (col_1, col_2) VALUES (1, 'one'), (2, 'two')"
SQL_SELECT = f"SELECT * FROM {SQL_TABLE}"

GCE_MACHINE_TYPE = "n1-standard-1"
GCE_INSTANCE_NAME = f"instance-{DAG_ID}-{ENV_ID}".replace("_", "-")
GCE_INSTANCE_BODY = {
    "name": GCE_INSTANCE_NAME,
    "machine_type": f"zones/{ZONE}/machineTypes/{GCE_MACHINE_TYPE}",
    "disks": [
        {
            "boot": True,
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
DELETE_PERSISTENT_DISK_COMMAND = f"""
if [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then \
 gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; \
fi;

gcloud compute disks delete {GCE_INSTANCE_NAME} --project={PROJECT_ID} --zone={ZONE} --quiet
"""

SHEETS_CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"
SPREADSHEET = {
    "properties": {"title": "Test1"},
    "sheets": [{"properties": {"title": "Sheet1"}}],
}


log = logging.getLogger(__name__)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "postgres", "gcs"],
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

    setup_postgres = SSHOperator(
        task_id="setup_postgres",
        ssh_hook=ComputeEngineSSHHook(
            user="username",
            instance_name=GCE_INSTANCE_NAME,
            zone=ZONE,
            project_id=PROJECT_ID,
            use_oslogin=False,
            use_iap_tunnel=False,
            cmd_timeout=180,
        ),
        command=SETUP_POSTGRES_COMMAND,
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
        connection = Connection(
            conn_id=connection_id,
            description="Example connection",
            conn_type=CONNECTION_TYPE,
            schema=DB_NAME,
            host=ip_address,
            login=DB_USER_NAME,
            password=DB_USER_PASSWORD,
            port=DB_PORT,
        )
        session = Session()
        log.info("Removing connection %s if it exists", connection_id)
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()

        session.add(connection)
        session.commit()
        log.info("Connection %s created", connection_id)

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

    @task
    def setup_sheets_connection():
        conn = Connection(
            conn_id=SHEETS_CONNECTION_ID,
            conn_type="google_cloud_platform",
        )
        conn_extra = {
            "scope": "https://www.googleapis.com/auth/spreadsheets,https://www.googleapis.com/auth/cloud-platform",
            "project": PROJECT_ID,
            "keyfile_dict": "",  # Override to match your needs
        }
        conn_extra_json = json.dumps(conn_extra)
        conn.set_extra(conn_extra_json)

        session = Session()
        session.add(conn)
        session.commit()

    setup_sheets_connection_task = setup_sheets_connection()

    create_spreadsheet = GoogleSheetsCreateSpreadsheetOperator(
        task_id="create_spreadsheet", spreadsheet=SPREADSHEET, gcp_conn_id=SHEETS_CONNECTION_ID
    )

    # [START upload_sql_to_sheets]
    upload_sql_to_sheet = SQLToGoogleSheetsOperator(
        task_id="upload_sql_to_sheet",
        sql=SQL_SELECT,
        sql_conn_id=CONNECTION_ID,
        database=DB_NAME,
        spreadsheet_id="{{ task_instance.xcom_pull(task_ids='create_spreadsheet', "
        "key='spreadsheet_id') }}",
        gcp_conn_id=SHEETS_CONNECTION_ID,
    )
    # [END upload_sql_to_sheets]

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

    delete_persistent_disk = BashOperator(
        task_id="delete_persistent_disk",
        bash_command=DELETE_PERSISTENT_DISK_COMMAND,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(task_id="delete_connection")
    def delete_connection(connection_id: str) -> None:
        session = Session()
        log.info("Removing connection %s", connection_id)
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()
        session.commit()

    delete_connection_task = delete_connection(connection_id=CONNECTION_ID)
    delete_connection_sheets_task = delete_connection(connection_id=SHEETS_CONNECTION_ID)

    # TEST SETUP
    create_gce_instance >> get_public_ip_task >> create_connection_task
    [create_gce_instance, create_firewall_rule] >> setup_postgres
    [setup_postgres, create_connection_task, create_firewall_rule] >> create_sql_table >> insert_sql_data
    setup_sheets_connection_task >> create_spreadsheet

    (
        [create_spreadsheet, insert_sql_data]
        # TEST BODY
        >> upload_sql_to_sheet
    )

    # TEST TEARDOWN
    upload_sql_to_sheet >> [
        delete_firewall_rule,
        delete_gce_instance,
        delete_connection_task,
        delete_connection_sheets_task,
    ]
    delete_gce_instance >> delete_persistent_disk

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
