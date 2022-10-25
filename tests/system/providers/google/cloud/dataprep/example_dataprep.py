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
Example Airflow DAG that shows how to use Google Dataprep.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.dataprep import (
    DataprepCopyFlowOperator,
    DataprepDeleteFlowOperator,
    DataprepGetJobGroupOperator,
    DataprepGetJobsForJobGroupOperator,
    DataprepRunFlowOperator,
    DataprepRunJobGroupOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.sensors.dataprep import DataprepJobGroupIsFinishedSensor
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_dataprep"

GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
GCS_BUCKET_NAME = f"dataprep-bucket-heorhi-{DAG_ID}-{ENV_ID}"
GCS_BUCKET_PATH = f"gs://{GCS_BUCKET_NAME}/task_results/"

FLOW_ID = os.environ.get("FLOW_ID", "")
RECIPE_ID = os.environ.get("RECIPE_ID")
RECIPE_NAME = os.environ.get("RECIPE_NAME")
WRITE_SETTINGS = (
    {
        "writesettings": [
            {
                "path": GCS_BUCKET_PATH,
                "action": "create",
                "format": "csv",
            }
        ],
    },
)

with models.DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),  # Override to match your needs
    catchup=False,
    tags=["example", "dataprep"],
    render_template_as_native_obj=True,
) as dag:
    create_bucket_task = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=GCS_BUCKET_NAME,
        project_id=GCP_PROJECT_ID,
    )

    # [START how_to_dataprep_run_job_group_operator]
    run_job_group_task = DataprepRunJobGroupOperator(
        task_id="run_job_group",
        project_id=GCP_PROJECT_ID,
        body_request={
            "wrangledDataset": {"id": RECIPE_ID},
            "overrides": WRITE_SETTINGS,
        },
    )
    # [END how_to_dataprep_run_job_group_operator]

    # [START how_to_dataprep_copy_flow_operator]
    copy_task = DataprepCopyFlowOperator(
        task_id="copy_flow",
        project_id=GCP_PROJECT_ID,
        flow_id=FLOW_ID,
        name=f"dataprep_example_flow_{DAG_ID}_{ENV_ID}",
    )
    # [END how_to_dataprep_copy_flow_operator]

    # [START how_to_dataprep_dataprep_run_flow_operator]
    run_flow_task = DataprepRunFlowOperator(
        task_id="run_flow",
        project_id=GCP_PROJECT_ID,
        flow_id="{{ task_instance.xcom_pull('copy_flow')['id'] }}",
        body_request={
            "overrides": {
                RECIPE_NAME: WRITE_SETTINGS,
            },
        },
    )
    # [END how_to_dataprep_dataprep_run_flow_operator]

    # [START how_to_dataprep_get_job_group_operator]
    get_job_group_task = DataprepGetJobGroupOperator(
        task_id="get_job_group",
        project_id=GCP_PROJECT_ID,
        job_group_id="{{ task_instance.xcom_pull('run_flow')['data'][0]['id'] }}",
        embed="",
        include_deleted=False,
    )
    # [END how_to_dataprep_get_job_group_operator]

    # [START how_to_dataprep_get_jobs_for_job_group_operator]
    get_jobs_for_job_group_task = DataprepGetJobsForJobGroupOperator(
        task_id="get_jobs_for_job_group",
        job_group_id="{{ task_instance.xcom_pull('run_flow')['data'][0]['id'] }}",
    )
    # [END how_to_dataprep_get_jobs_for_job_group_operator]

    # [START how_to_dataprep_job_group_finished_sensor]
    check_flow_status_sensor = DataprepJobGroupIsFinishedSensor(
        task_id="check_flow_status",
        job_group_id="{{ task_instance.xcom_pull('run_flow')['data'][0]['id'] }}",
    )
    # [END how_to_dataprep_job_group_finished_sensor]

    # [START how_to_dataprep_job_group_finished_sensor]
    check_job_group_status_sensor = DataprepJobGroupIsFinishedSensor(
        task_id="check_job_group_status",
        job_group_id="{{ task_instance.xcom_pull('run_job_group')['id'] }}",
    )
    # [END how_to_dataprep_job_group_finished_sensor]

    # [START how_to_dataprep_delete_flow_operator]
    delete_flow_task = DataprepDeleteFlowOperator(
        task_id="delete_flow",
        flow_id="{{ task_instance.xcom_pull('copy_flow')['id'] }}",
    )
    # [END how_to_dataprep_delete_flow_operator]
    delete_flow_task.trigger_rule = TriggerRule.ALL_DONE

    delete_bucket_task = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=GCS_BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    (
        # TEST SETUP
        create_bucket_task
        >> copy_task
        # TEST BODY
        >> [run_job_group_task, run_flow_task]
        >> get_job_group_task
        >> get_jobs_for_job_group_task
        # TEST TEARDOWN
        >> check_flow_status_sensor
        >> [delete_flow_task, check_job_group_status_sensor]
        >> delete_bucket_task
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
