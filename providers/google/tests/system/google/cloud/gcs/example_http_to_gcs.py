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
Example Airflow DAG for HTTP to Google Cloud Storage transfer operators.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS

if AIRFLOW_V_3_0_PLUS:
    from airflow.sdk import task
else:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.http_to_gcs import HttpToGCSOperator
from airflow.providers.standard.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
from tests_common.test_utils.api_client_helpers import create_airflow_connection, delete_airflow_connection

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "example_http_to_gcs"
BUCKET_NAME = f"bucket-{DAG_ID}-{ENV_ID}"

cmd = """
#!/bin/bash

echo 'foo' > /tmp/test_file

cd /tmp

nohup python3 -m http.server 8083 > /dev/null 2>&1 &

echo $!
sleep 2
exit 0
"""

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    conn_id_name = f"{ENV_ID}-http-conn-id"

    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    @task(task_id="create_connection")
    def create_connection(conn_id_name: str):
        connection: dict[str, Any] = {"conn_type": "http", "host": "localhost", "port": 8083}
        create_airflow_connection(
            connection_id=conn_id_name,
            connection_conf=connection,
        )

    set_up_connection = create_connection(conn_id_name)

    start_server = BashOperator(bash_command=cmd, task_id="start_server")

    # [START howto_transfer_http_to_gcs]
    http_to_gcs_task = HttpToGCSOperator(
        task_id="http_to_gcs_task",
        http_conn_id=conn_id_name,
        endpoint="/test_file",
        bucket_name=BUCKET_NAME,
        object_name="test_file",
    )
    # [END howto_transfer_http_to_gcs]

    stop_server = BashOperator(
        task_id="stop_simple_http_server",
        bash_command='kill {{ti.xcom_pull(task_ids="start_server")}}',
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    @task(task_id="delete_connection", trigger_rule=TriggerRule.ALL_DONE)
    def delete_connection(connection_id: str) -> None:
        delete_airflow_connection(connection_id=connection_id)

    delete_connection_task = delete_connection(connection_id=conn_id_name)

    chain(
        # TEST SETUP
        create_bucket,
        set_up_connection,
        start_server,
        # TEST BODY
        http_to_gcs_task,
        # TEST TEARDOWN
        stop_server,
        delete_bucket,
        delete_connection_task,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
