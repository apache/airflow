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
First, you need a db instance that is accessible from the Airflow environment.
You can, for example, create a Cloud SQL instance and connect to it from
within breeze with Cloud SQL proxy:
https://cloud.google.com/sql/docs/postgres/connect-instance-auth-proxy

# DB setup
Create db:
```
CREATE DATABASE test_db;
```

Switch to db:
```
\c test_db
```

Create table and insert some rows
```
CREATE TABLE test_table (col1 INT, col2 INT);
INSERT INTO test_table VALUES (1,2), (3,4), (5,6), (7,8);
```

# Setup connections
db connection:
In airflow UI, set one db connection, for example "postgres_default"
and make sure the "Test" at the bottom succeeds

google cloud connection:
We need additional scopes for this test
scopes: https://www.googleapis.com/auth/spreadsheets, https://www.googleapis.com/auth/cloud-platform

# Sheet
Finally, you need a Google Sheet you have access to, for testing you can
create a public sheet and get its ID.

# Tear Down
You can delete the db with
```
DROP DATABASE test_db;
```
"""
from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
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
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "example_sql_to_sheets"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

REGION = "europe-west2"
ZONE = REGION + "-a"
NETWORK = "default"

SHEETS_CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"
SPREADSHEET = {
    "properties": {"title": "Test1"},
    "sheets": [{"properties": {"title": "Sheet1"}}],
}

DB_NAME = f"{DAG_ID}-{ENV_ID}-db".replace("-", "_")
DB_PORT = 5432
DB_USER_NAME = "demo_user"
DB_USER_PASSWORD = "demo_password"
DB_CONNECTION_ID = f"postgres_{DAG_ID}_{ENV_ID}".replace("-", "_")

SHORT_MACHINE_TYPE_NAME = "n1-standard-1"
DB_INSTANCE_NAME = f"instance-{DAG_ID}-{ENV_ID}".replace("_", "-")
GCE_INSTANCE_BODY = {
    "name": DB_INSTANCE_NAME,
    "machine_type": f"zones/{ZONE}/machineTypes/{SHORT_MACHINE_TYPE_NAME}",
    "disks": [
        {
            "boot": True,
            "device_name": DB_INSTANCE_NAME,
            "initialize_params": {
                "disk_size_gb": "10",
                "disk_type": f"zones/{ZONE}/diskTypes/pd-balanced",
                "source_image": "projects/debian-cloud/global/images/debian-11-bullseye-v20220621",
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
DELETE_PERSISTENT_DISK = f"""
if [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then \
 gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; \
fi;
gcloud compute disks delete {DB_INSTANCE_NAME} --project={PROJECT_ID} --zone={ZONE} --quiet
"""

SETUP_POSTGRES = f"""
sudo apt update &&
sudo apt install -y docker.io &&
sudo docker run -d -p {DB_PORT}:{DB_PORT} --name {DB_NAME} \
    -e POSTGRES_USER={DB_USER_NAME} \
    -e POSTGRES_PASSWORD={DB_USER_PASSWORD} \
    -e POSTGRES_DB={DB_NAME} \
    postgres
"""

FIREWALL_RULE_NAME = f"allow-http-{DB_PORT}"
CREATE_FIREWALL_RULE = f"""
if [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then \
 gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; \
fi;
gcloud compute firewall-rules create {FIREWALL_RULE_NAME} \
  --project={PROJECT_ID} \
  --direction=INGRESS \
  --priority=100 \
  --network={NETWORK} \
  --action=ALLOW \
  --rules=tcp:{DB_PORT} \
  --source-ranges=0.0.0.0/0
"""
DELETE_FIREWALL_RULE = f"""
if [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then \
 gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; \
fi;
gcloud compute firewall-rules delete {FIREWALL_RULE_NAME} --project={PROJECT_ID} --quiet
"""

SQL_TABLE = "test_table"
SQL_CREATE = f"CREATE TABLE IF NOT EXISTS {SQL_TABLE} (col_1 INT, col_2 VARCHAR(8))"
SQL_INSERT = f"INSERT INTO {SQL_TABLE} (col_1, col_2) VALUES (1, 'one'), (2, 'two')"
SQL_SELECT = f"SELECT * FROM {SQL_TABLE}"

log = logging.getLogger(__name__)


with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",  # Override to match your needs
    catchup=False,
    tags=["example", "sql"],
) as dag:
    create_instance = ComputeEngineInsertInstanceOperator(
        task_id="create_instance",
        project_id=PROJECT_ID,
        zone=ZONE,
        body=GCE_INSTANCE_BODY,
    )

    create_firewall_rule = BashOperator(
        task_id="create_firewall_rule",
        bash_command=CREATE_FIREWALL_RULE,
    )

    setup_postgres = SSHOperator(
        task_id="setup_postgres",
        ssh_hook=ComputeEngineSSHHook(
            user="username",
            instance_name=DB_INSTANCE_NAME,
            zone=ZONE,
            project_id=PROJECT_ID,
            use_oslogin=False,
            use_iap_tunnel=False,
            cmd_timeout=180,
        ),
        command=SETUP_POSTGRES,
        retries=2,
        retry_delay=30,
    )

    @task
    def get_public_ip() -> str:
        hook = ComputeEngineHook()
        address = hook.get_instance_address(resource_id=DB_INSTANCE_NAME, zone=ZONE, project_id=PROJECT_ID)
        return address

    get_public_ip_task = get_public_ip()

    @task
    def setup_postgres_connection(**kwargs) -> None:
        public_ip = kwargs["ti"].xcom_pull(task_ids="get_public_ip")
        connection = Connection(
            conn_id=DB_CONNECTION_ID,
            description="Example PostgreSQL connection",
            conn_type="postgres",
            host=public_ip,
            login=DB_USER_NAME,
            password=DB_USER_PASSWORD,
            schema=DB_NAME,
            port=DB_PORT,
        )
        session = Session()
        if session.query(Connection).filter(Connection.conn_id == DB_CONNECTION_ID).first():
            log.warning("Connection %s already exists", DB_CONNECTION_ID)
            return None

        session.add(connection)
        session.commit()

    setup_postgres_connection_task = setup_postgres_connection()

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

    create_sql_table = SQLExecuteQueryOperator(
        task_id="create_sql_table",
        conn_id=DB_CONNECTION_ID,
        sql=SQL_CREATE,
    )

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id=DB_CONNECTION_ID,
        sql=SQL_INSERT,
    )

    create_spreadsheet = GoogleSheetsCreateSpreadsheetOperator(
        task_id="create_spreadsheet", spreadsheet=SPREADSHEET, gcp_conn_id=SHEETS_CONNECTION_ID
    )

    # [START upload_sql_to_sheets]
    upload_sql_to_sheet = SQLToGoogleSheetsOperator(
        task_id="upload_sql_to_sheet",
        sql=SQL_SELECT,
        sql_conn_id=DB_CONNECTION_ID,
        database=DB_NAME,
        spreadsheet_id="{{ task_instance.xcom_pull(task_ids='create_spreadsheet', "
        "key='spreadsheet_id') }}",
        gcp_conn_id=SHEETS_CONNECTION_ID,
    )
    # [END upload_sql_to_sheets]

    delete_postgres_connection = BashOperator(
        task_id="delete_postgres_connection",
        bash_command=f"airflow connections delete {DB_CONNECTION_ID}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_sheets_connection = BashOperator(
        task_id="delete_temp_sheets_connection",
        bash_command=f"airflow connections delete {SHEETS_CONNECTION_ID}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_instance = ComputeEngineDeleteInstanceOperator(
        task_id="delete_instance",
        resource_id=DB_INSTANCE_NAME,
        zone=ZONE,
        project_id=PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_firewall_rule = BashOperator(
        task_id="delete_firewall_rule",
        bash_command=DELETE_FIREWALL_RULE,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_persistent_disk = BashOperator(
        task_id="delete_persistent_disk",
        bash_command=DELETE_PERSISTENT_DISK,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # TEST SETUP
    create_instance >> setup_postgres
    (create_instance >> get_public_ip_task >> setup_postgres_connection_task)
    (
        [setup_postgres, setup_postgres_connection_task, create_firewall_rule]
        >> create_sql_table
        >> insert_data
    )
    (
        [insert_data, setup_sheets_connection_task >> create_spreadsheet]
        # TEST BODY
        >> upload_sql_to_sheet
        # TEST TEARDOWN
        >> [delete_instance, delete_postgres_connection, delete_sheets_connection, delete_firewall_rule]
    )
    delete_instance >> delete_persistent_disk

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
