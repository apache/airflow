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
Example DAG using PostgresToGoogleCloudStorageOperator.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

from googleapiclient import discovery

from airflow import models
from airflow.decorators import task
from airflow.models import Connection
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.operators.cloud_sql import (
    CloudSQLCreateInstanceDatabaseOperator,
    CloudSQLCreateInstanceOperator,
    CloudSQLDeleteInstanceOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "example_postgres_to_gcs"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "example-project")

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
CONNECTION_ID = f"postgres_{DAG_ID}_{ENV_ID}".replace("-", "_")

BUCKET_NAME = f"{DAG_ID}_{ENV_ID}_bucket"
FILE_NAME = "result.json"

SQL_TABLE = "test_table"
SQL_CREATE = f"CREATE TABLE IF NOT EXISTS {SQL_TABLE} (col_1 INT, col_2 VARCHAR(8))"
SQL_INSERT = f"INSERT INTO {SQL_TABLE} (col_1, col_2) VALUES (1, 'one'), (2, 'two')"
SQL_SELECT = f"SELECT * FROM {SQL_TABLE}"

log = logging.getLogger(__name__)


with models.DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "postgres", "gcs"],
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
            conn_id=CONNECTION_ID,
            description="Example PostgreSQL connection",
            conn_type="postgres",
            host=public_ip,
            login=DB_USER_NAME,
            password=DB_USER_PASSWORD,
            schema=DB_NAME,
            port=DB_PORT,
        )
        session: Session = Session()
        if session.query(Connection).filter(Connection.conn_id == CONNECTION_ID).first():
            log.warning("Connection %s already exists", CONNECTION_ID)
            return None

        session.add(connection)
        session.commit()

    setup_postgres_connection_task = setup_postgres_connection()

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
    )

    create_sql_table = SQLExecuteQueryOperator(
        task_id="create_sql_table",
        conn_id=CONNECTION_ID,
        sql=SQL_CREATE,
    )

    insert_data = SQLExecuteQueryOperator(
        task_id="insert_data",
        conn_id=CONNECTION_ID,
        sql=SQL_INSERT,
    )

    # [START howto_operator_postgres_to_gcs]
    get_data = PostgresToGCSOperator(
        task_id="get_data",
        postgres_conn_id=CONNECTION_ID,
        sql=SQL_SELECT,
        bucket=BUCKET_NAME,
        filename=FILE_NAME,
        gzip=False,
    )
    # [END howto_operator_postgres_to_gcs]

    delete_cloud_sql_instance = CloudSQLDeleteInstanceOperator(
        task_id="delete_cloud_sql_instance",
        project_id=PROJECT_ID,
        instance=CLOUD_SQL_INSTANCE,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_postgres_connection = BashOperator(
        task_id="delete_postgres_connection",
        bash_command=f"airflow connections delete {CONNECTION_ID}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # TEST SETUP
    create_cloud_sql_instance >> [create_database, create_user_task, get_public_ip_task]
    [create_user_task, get_public_ip_task] >> setup_postgres_connection_task
    create_database >> setup_postgres_connection_task >> create_sql_table >> insert_data
    (
        [insert_data, create_bucket]
        # TEST BODY
        >> get_data
        # TEST TEARDOWN
        >> [delete_cloud_sql_instance, delete_postgres_connection, delete_bucket]
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
