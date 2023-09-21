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

from googleapiclient import discovery

from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceOperator,
)
from airflow.providers.google.suite.operators.sheets import GoogleSheetsCreateSpreadsheetOperator
from airflow.providers.google.suite.transfers.sql_to_sheets import SQLToGoogleSheetsOperator
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "example_sql_to_sheets"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")

SHEETS_CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"
SPREADSHEET = {
    "properties": {"title": "Test1"},
    "sheets": [{"properties": {"title": "Sheet1"}}],
}

CLOUD_SQL_INSTANCE = f"cloud-sql-{DAG_ID}-{ENV_ID}".replace("_", "-")
CLOUD_SQL_INSTANCE_CREATION_BODY = {
    "name": CLOUD_SQL_INSTANCE,
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
    "databaseVersion": "POSTGRES_15",
    "region": "us-central1",
}
DB_NAME = f"{DAG_ID}-{ENV_ID}-db".replace("-", "_")
DB_PORT = 5432
DB_CREATE_BODY = {"instance": CLOUD_SQL_INSTANCE, "name": DB_NAME, "project": PROJECT_ID}
DB_USER_NAME = "demo_user"
DB_USER_PASSWORD = "demo_password"
DB_CONNECTION_ID = f"postgres_{DAG_ID}_{ENV_ID}".replace("-", "_")

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
    create_cloud_sql_instance = CloudSQLCreateInstanceOperator(
        task_id="create_cloud_sql_instance",
        project_id=PROJECT_ID,
        instance=CLOUD_SQL_INSTANCE,
        body=CLOUD_SQL_INSTANCE_CREATION_BODY,
    )

    create_database = CloudSQLCreateInstanceDatabaseOperator(
        task_id="create_database", body=DB_CREATE_BODY, instance=CLOUD_SQL_INSTANCE
    )

    @task
    def create_user() -> None:
        with discovery.build("sqladmin", "v1beta4") as service:
            request = service.users().insert(
                project=PROJECT_ID,
                instance=CLOUD_SQL_INSTANCE,
                body={
                    "name": DB_USER_NAME,
                    "password": DB_USER_PASSWORD,
                },
            )
            request.execute()

    create_user_task = create_user()

    @task
    def get_public_ip() -> str | None:
        with discovery.build("sqladmin", "v1beta4") as service:
            request = service.connect().get(
                project=PROJECT_ID, instance=CLOUD_SQL_INSTANCE, fields="ipAddresses"
            )
            response = request.execute()
            for ip_item in response.get("ipAddresses", []):
                if ip_item["type"] == "PRIMARY":
                    return ip_item["ipAddress"]

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
        session: Session = Session()
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

        session: Session = Session()
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

    delete_cloud_sql_instance = CloudSQLDeleteInstanceOperator(
        task_id="delete_cloud_sql_instance",
        project_id=PROJECT_ID,
        instance=CLOUD_SQL_INSTANCE,
        trigger_rule=TriggerRule.ALL_DONE,
    )

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

    # TEST SETUP
    create_cloud_sql_instance >> [create_database, create_user_task, get_public_ip_task]
    [create_user_task, get_public_ip_task] >> setup_postgres_connection_task
    create_database >> setup_postgres_connection_task >> create_sql_table >> insert_data
    (
        [insert_data, setup_sheets_connection_task >> create_spreadsheet]
        # TEST BODY
        >> upload_sql_to_sheet
        # TEST TEARDOWN
        >> [delete_cloud_sql_instance, delete_postgres_connection, delete_sheets_connection]
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
