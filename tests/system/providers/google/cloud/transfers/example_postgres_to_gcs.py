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
Example DAG using PostgresToGCSOperator.

This DAG relies on the following OS environment variables

* AIRFLOW__API__GOOGLE_KEY_PATH - Path to service account key file. Note, you can skip this variable if you
  run this DAG in a Composer environment.
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

from airflow.decorators import task
from airflow.models.connection import Connection
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
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
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule

DAG_ID = "example_postgres_to_gcs"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "example-project")

REGION = "europe-west2"
ZONE = REGION + "-a"
NETWORK = "default"

DB_NAME = "testdb"
DB_PORT = 5432
DB_USER_NAME = "demo_user"
DB_USER_PASSWORD = "demo_password"

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

CONNECTION_ID = f"postgres_{DAG_ID}_{ENV_ID}".replace("-", "_")

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
FILE_NAME = "result.json"

SQL_TABLE = "test_table"
SQL_CREATE = f"CREATE TABLE IF NOT EXISTS {SQL_TABLE} (col_1 INT, col_2 VARCHAR(8))"
SQL_INSERT = f"INSERT INTO {SQL_TABLE} (col_1, col_2) VALUES (1, 'one'), (2, 'two')"
SQL_SELECT = f"SELECT * FROM {SQL_TABLE}"


log = logging.getLogger(__name__)


with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "postgres", "gcs"],
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
            conn_id=CONNECTION_ID,
            description="Example PostgreSQL connection",
            conn_type="postgres",
            host=public_ip,
            login=DB_USER_NAME,
            password=DB_USER_PASSWORD,
            schema=DB_NAME,
            port=DB_PORT,
        )
        session = Session()
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
        [insert_data, create_bucket]
        # TEST BODY
        >> get_data
        # TEST TEARDOWN
        >> [delete_instance, delete_bucket, delete_postgres_connection, delete_firewall_rule]
    )
    delete_instance >> delete_persistent_disk

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
