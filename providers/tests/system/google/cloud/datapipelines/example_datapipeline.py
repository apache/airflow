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
Example Airflow DAG for testing Google DataPipelines Create Data Pipeline Operator.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowDeletePipelineOperator
from airflow.providers.google.cloud.operators.datapipeline import (
    CreateDataPipelineOperator,
    RunDataPipelineOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
    GCSSynchronizeBucketsOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "datapipeline"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
GCP_PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
GCP_LOCATION = "us-central1"

PIPELINE_NAME = f"{DAG_ID}-{ENV_ID}".replace("_", "-")
PIPELINE_JOB_NAME = f"{DAG_ID}-{ENV_ID}-job".replace("_", "-")
PIPELINE_TYPE = "PIPELINE_TYPE_BATCH"

RESOURCE_DATA_BUCKET = "airflow-system-tests-resources"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("-", "_")

FILE_NAME = "kinglear.txt"
TEMPLATE_FILE = "word-count.json"
TEMP_LOCATION = f"gs://{BUCKET_NAME}/temp"

GCS_PATH = f"gs://{BUCKET_NAME}/dataflow/{TEMPLATE_FILE}"
INPUT_FILE = f"gs://{BUCKET_NAME}/dataflow/{FILE_NAME}"
OUTPUT = f"gs://{BUCKET_NAME}/results/hello"

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "datapipeline"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    move_files_to_bucket = GCSSynchronizeBucketsOperator(
        task_id="move_files_to_bucket",
        source_bucket=RESOURCE_DATA_BUCKET,
        source_object="dataflow/pipelines",
        destination_bucket=BUCKET_NAME,
        destination_object="dataflow",
        recursive=True,
    )

    # [START howto_operator_create_data_pipeline]
    create_data_pipeline = CreateDataPipelineOperator(
        task_id="create_data_pipeline",
        project_id=GCP_PROJECT_ID,
        location=GCP_LOCATION,
        body={
            "name": f"projects/{GCP_PROJECT_ID}/locations/{GCP_LOCATION}/pipelines/{PIPELINE_NAME}",
            "type": PIPELINE_TYPE,
            "workload": {
                "dataflowFlexTemplateRequest": {
                    "launchParameter": {
                        "containerSpecGcsPath": GCS_PATH,
                        "jobName": PIPELINE_JOB_NAME,
                        "environment": {"tempLocation": TEMP_LOCATION},
                        "parameters": {
                            "inputFile": INPUT_FILE,
                            "output": OUTPUT,
                        },
                    },
                    "projectId": GCP_PROJECT_ID,
                    "location": GCP_LOCATION,
                }
            },
        },
    )
    # [END howto_operator_create_data_pipeline]

    # [START howto_operator_run_data_pipeline]
    run_data_pipeline = RunDataPipelineOperator(
        task_id="run_data_pipeline",
        data_pipeline_name=PIPELINE_NAME,
        project_id=GCP_PROJECT_ID,
    )
    # [END howto_operator_run_data_pipeline]

    # [START howto_operator_delete_dataflow_pipeline]
    delete_pipeline = DataflowDeletePipelineOperator(
        task_id="delete_data_pipeline",
        pipeline_name=PIPELINE_NAME,
        project_id=GCP_PROJECT_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_operator_delete_dataflow_pipeline]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> move_files_to_bucket
        # TEST BODY
        >> create_data_pipeline
        >> run_data_pipeline
        # TEST TEARDOWN
        >> delete_pipeline
        >> delete_bucket
    )

    from dev.tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from dev.tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
