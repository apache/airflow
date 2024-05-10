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
Example Airflow DAG for Google BigQuery service.

This DAG relies on the following OS environment variables

* AIRFLOW__API__GOOGLE_KEY_PATH - Path to service account key file. Note, you can skip this variable if you
  run this DAG in a Composer environment.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

from pendulum import duration

from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.google.cloud.hooks.compute import ComputeEngineHook
from airflow.providers.google.cloud.hooks.compute_ssh import ComputeEngineSSHHook
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineDeleteInstanceOperator,
    ComputeEngineInsertInstanceOperator,
)
from airflow.providers.google.cloud.transfers.bigquery_to_postgres import BigQueryToPostgresOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "example-project")
DAG_ID = "example_bigquery_to_postgres"

REGION = "us-central1"
ZONE = REGION + "-a"
NETWORK = "default"
CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}".replace("-", "_")
CONNECTION_TYPE = "postgres"

BIGQUERY_DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
BIGQUERY_TABLE = "test_table"
SOURCE_OBJECT_NAME = "gs://airflow-system-tests-resources/bigquery/salaries_1k.csv"
BATCH_SIZE = 500
UPLOAD_DATA_TO_BIGQUERY = f"""
if [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then \
 gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; \
fi;

bq load --project_id={PROJECT_ID} --location={REGION} \
    --source_format=CSV {BIGQUERY_DATASET_NAME}.{BIGQUERY_TABLE} {SOURCE_OBJECT_NAME} \
    emp_name:STRING,salary:FLOAT
"""

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
SQL_CREATE_TABLE = f"CREATE TABLE IF NOT EXISTS {SQL_TABLE} (emp_name VARCHAR(64), salary FLOAT)"

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
FIREWALL_RULE_NAME = f"allow-http-{DB_PORT}"
CREATE_FIREWALL_RULE_COMMAND = f"""
if [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then \
 gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; \
fi;

if [ -z gcloud compute firewall-rules list --filter=name:{FIREWALL_RULE_NAME} --format="value(name)" ]; then \
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
if [ gcloud compute firewall-rules list --filter=name:{FIREWALL_RULE_NAME} --format="value(name)" ]; then \
    gcloud compute firewall-rules delete {FIREWALL_RULE_NAME} --project={PROJECT_ID} --quiet; \
fi;
"""
DELETE_PERSISTENT_DISK_COMMAND = f"""
if [ $AIRFLOW__API__GOOGLE_KEY_PATH ]; then \
 gcloud auth activate-service-account --key-file=$AIRFLOW__API__GOOGLE_KEY_PATH; \
fi;

gcloud compute disks delete {GCE_INSTANCE_NAME} --project={PROJECT_ID} --zone={ZONE} --quiet
"""


log = logging.getLogger(__name__)


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bigquery"],
) as dag:
    create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bigquery_dataset",
        dataset_id=BIGQUERY_DATASET_NAME,
        location=REGION,
    )

    create_bigquery_table = BigQueryCreateEmptyTableOperator(
        task_id="create_bigquery_table",
        dataset_id=BIGQUERY_DATASET_NAME,
        location=REGION,
        table_id=BIGQUERY_TABLE,
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "salary", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    insert_bigquery_data = BashOperator(
        task_id="insert_bigquery_data",
        bash_command=UPLOAD_DATA_TO_BIGQUERY,
    )

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
        retries=4,
    )

    @task
    def get_public_ip() -> str:
        hook = ComputeEngineHook()
        address = hook.get_instance_address(resource_id=GCE_INSTANCE_NAME, zone=ZONE, project_id=PROJECT_ID)
        return address

    get_public_ip_task = get_public_ip()

    @task
    def setup_connection(ip_address: str) -> None:
        connection = Connection(
            conn_id=CONNECTION_ID,
            description="Example connection",
            conn_type=CONNECTION_TYPE,
            host=ip_address,
            schema=DB_NAME,
            login=DB_USER_NAME,
            password=DB_USER_PASSWORD,
            port=DB_PORT,
        )
        session = Session()
        log.info("Removing connection %s if it exists", CONNECTION_ID)
        query = session.query(Connection).filter(Connection.conn_id == CONNECTION_ID)
        query.delete()

        session.add(connection)
        session.commit()
        log.info("Connection %s created", CONNECTION_ID)

    setup_connection_task = setup_connection(get_public_ip_task)

    create_sql_table = SQLExecuteQueryOperator(
        task_id="create_sql_table",
        conn_id=CONNECTION_ID,
        sql=SQL_CREATE_TABLE,
        retries=4,
        retry_delay=duration(seconds=20),
        retry_exponential_backoff=False,
    )

    # [START howto_operator_bigquery_to_postgres]
    bigquery_to_postgres = BigQueryToPostgresOperator(
        task_id="bigquery_to_postgres",
        postgres_conn_id=CONNECTION_ID,
        dataset_table=f"{BIGQUERY_DATASET_NAME}.{BIGQUERY_TABLE}",
        target_table_name=SQL_TABLE,
        batch_size=BATCH_SIZE,
        replace=False,
    )
    # [END howto_operator_bigquery_to_postgres]

    delete_bigquery_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_bigquery_dataset",
        dataset_id=BIGQUERY_DATASET_NAME,
        delete_contents=True,
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

    delete_persistent_disk = BashOperator(
        task_id="delete_persistent_disk",
        bash_command=DELETE_PERSISTENT_DISK_COMMAND,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_connection = BashOperator(
        task_id="delete_connection",
        bash_command=f"airflow connections delete {CONNECTION_ID}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # TEST SETUP
    create_bigquery_dataset >> create_bigquery_table >> insert_bigquery_data
    create_gce_instance >> setup_postgres
    create_gce_instance >> get_public_ip_task >> setup_connection_task
    [setup_postgres, setup_connection_task, create_firewall_rule] >> create_sql_table

    (
        [insert_bigquery_data, create_sql_table]
        # TEST BODY
        >> bigquery_to_postgres
    )

    # TEST TEARDOWN
    bigquery_to_postgres >> [
        delete_bigquery_dataset,
        delete_firewall_rule,
        delete_gce_instance,
        delete_connection,
    ]
    delete_gce_instance >> delete_persistent_disk

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
