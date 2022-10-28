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
Example Airflow DAG that shows how to use Dataplex.
"""
from __future__ import annotations

import datetime
import os
from pathlib import Path

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataplex import (
    DataplexCreateLakeOperator,
    DataplexCreateTaskOperator,
    DataplexDeleteLakeOperator,
    DataplexDeleteTaskOperator,
    DataplexGetTaskOperator,
    DataplexListTasksOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.dataplex import DataplexTaskStateSensor
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "example_dataplex"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

SPARK_FILE_NAME = "spark_example_pi.py"
CURRENT_FOLDER = Path(__file__).parent
FILE_LOCAL_PATH = str(Path(CURRENT_FOLDER) / "resources" / SPARK_FILE_NAME)

LAKE_ID = f"test-lake-{ENV_ID}"
REGION = "us-central1"

SERVICE_ACC = os.environ.get("GCP_DATAPLEX_SERVICE_ACC")

SPARK_FILE_FULL_PATH = f"gs://{BUCKET_NAME}/{SPARK_FILE_NAME}"
DATAPLEX_TASK_ID = f"test-task-{ENV_ID}"
TRIGGER_SPEC_TYPE = "ON_DEMAND"

# [START howto_dataplex_configuration]
EXAMPLE_TASK_BODY = {
    "trigger_spec": {"type_": TRIGGER_SPEC_TYPE},
    "execution_spec": {"service_account": SERVICE_ACC},
    "spark": {"python_script_file": SPARK_FILE_FULL_PATH},
}
# [END howto_dataplex_configuration]

# [START howto_dataplex_lake_configuration]
EXAMPLE_LAKE_BODY = {
    "display_name": "test_display_name",
    "labels": [],
    "description": "test_description",
    "metastore": {"service": ""},
}
# [END howto_dataplex_lake_configuration]


with models.DAG(
    DAG_ID,
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@once",
    tags=["example", "dataplex"],
) as dag:

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file",
        src=FILE_LOCAL_PATH,
        dst=SPARK_FILE_NAME,
        bucket=BUCKET_NAME,
    )
    # [START howto_dataplex_create_lake_operator]
    create_lake = DataplexCreateLakeOperator(
        project_id=PROJECT_ID, region=REGION, body=EXAMPLE_LAKE_BODY, lake_id=LAKE_ID, task_id="create_lake"
    )
    # [END howto_dataplex_create_lake_operator]

    # [START howto_dataplex_create_task_operator]
    create_dataplex_task = DataplexCreateTaskOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        body=EXAMPLE_TASK_BODY,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="create_dataplex_task",
    )
    # [END howto_dataplex_create_task_operator]

    # [START howto_dataplex_async_create_task_operator]
    create_dataplex_task_async = DataplexCreateTaskOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        body=EXAMPLE_TASK_BODY,
        dataplex_task_id=f"{DATAPLEX_TASK_ID}-1",
        asynchronous=True,
        task_id="create_dataplex_task_async",
    )
    # [END howto_dataplex_async_create_task_operator]

    # [START howto_dataplex_delete_task_operator]
    delete_dataplex_task_async = DataplexDeleteTaskOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=f"{DATAPLEX_TASK_ID}-1",
        task_id="delete_dataplex_task_async",
    )
    # [END howto_dataplex_delete_task_operator]

    # [START howto_dataplex_list_tasks_operator]
    list_dataplex_task = DataplexListTasksOperator(
        project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, task_id="list_dataplex_task"
    )
    # [END howto_dataplex_list_tasks_operator]

    # [START howto_dataplex_get_task_operator]
    get_dataplex_task = DataplexGetTaskOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="get_dataplex_task",
    )
    # [END howto_dataplex_get_task_operator]

    # [START howto_dataplex_task_state_sensor]
    dataplex_task_state = DataplexTaskStateSensor(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="dataplex_task_state",
    )
    # [END howto_dataplex_task_state_sensor]

    # [START howto_dataplex_delete_task_operator]
    delete_dataplex_task = DataplexDeleteTaskOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        dataplex_task_id=DATAPLEX_TASK_ID,
        task_id="delete_dataplex_task",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_dataplex_delete_task_operator]

    # [START howto_dataplex_delete_lake_operator]
    delete_lake = DataplexDeleteLakeOperator(
        project_id=PROJECT_ID,
        region=REGION,
        lake_id=LAKE_ID,
        task_id="delete_lake",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_dataplex_delete_lake_operator]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    chain(
        # TEST SETUP
        create_bucket,
        upload_file,
        # TEST BODY
        create_lake,
        create_dataplex_task,
        get_dataplex_task,
        list_dataplex_task,
        create_dataplex_task_async,
        delete_dataplex_task_async,
        dataplex_task_state,
        # TEST TEARDOWN
        delete_dataplex_task,
        delete_lake,
        delete_bucket,
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
