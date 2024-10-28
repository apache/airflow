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
Example Airflow DAG for testing Google Dataflow.

:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator` operator.
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator,
    DataflowTemplatedJobStartOperator,
)
from airflow.providers.google.cloud.operators.gcs import (
    GCSCreateBucketOperator,
    GCSDeleteBucketOperator,
)
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = (
    os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
)
DAG_ID = "dataflow_template"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}".replace("_", "-")

CSV_FILE_NAME = "input.csv"
AVRO_FILE_NAME = "output.avro"
AVRO_SCHEMA = "schema.json"
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"
PYTHON_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / CSV_FILE_NAME)
SCHEMA_LOCAL_PATH = str(Path(__file__).parent / "resources" / AVRO_SCHEMA)
LOCATION = "europe-west3"

default_args = {
    "dataflow_default_options": {
        "tempLocation": GCS_TMP,
        "stagingLocation": GCS_STAGING,
    }
}
BODY = {
    "launchParameter": {
        "jobName": "test-flex-template",
        "parameters": {
            "inputFileSpec": f"gs://{BUCKET_NAME}/{CSV_FILE_NAME}",
            "outputBucket": f"gs://{BUCKET_NAME}/output/{AVRO_FILE_NAME}",
            "outputFileFormat": "avro",
            "inputFileFormat": "csv",
            "schema": f"gs://{BUCKET_NAME}/{AVRO_SCHEMA}",
        },
        "environment": {},
        "containerSpecGcsPath": "gs://dataflow-templates/latest/flex/File_Format_Conversion",
    },
}

with DAG(
    DAG_ID,
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataflow"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME
    )

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=PYTHON_FILE_LOCAL_PATH,
        dst=CSV_FILE_NAME,
        bucket=BUCKET_NAME,
    )

    upload_schema = LocalFilesystemToGCSOperator(
        task_id="upload_schema_to_bucket",
        src=SCHEMA_LOCAL_PATH,
        dst=AVRO_SCHEMA,
        bucket=BUCKET_NAME,
    )

    # [START howto_operator_start_template_job]
    start_template_job = DataflowTemplatedJobStartOperator(
        task_id="start_template_job",
        project_id=PROJECT_ID,
        template="gs://dataflow-templates/latest/Word_Count",
        parameters={
            "inputFile": f"gs://{BUCKET_NAME}/{CSV_FILE_NAME}",
            "output": GCS_OUTPUT,
        },
        location=LOCATION,
        wait_until_finished=True,
    )
    # [END howto_operator_start_template_job]

    # [START howto_operator_start_flex_template_job]
    start_flex_template_job = DataflowStartFlexTemplateOperator(
        task_id="start_flex_template_job",
        project_id=PROJECT_ID,
        body=BODY,
        location=LOCATION,
        append_job_name=False,
        wait_until_finished=True,
    )
    # [END howto_operator_start_flex_template_job]

    # [START howto_operator_start_template_job_deferrable]
    start_template_job_deferrable = DataflowTemplatedJobStartOperator(
        task_id="start_template_job_deferrable",
        project_id=PROJECT_ID,
        template="gs://dataflow-templates/latest/Word_Count",
        parameters={
            "inputFile": f"gs://{BUCKET_NAME}/{CSV_FILE_NAME}",
            "output": GCS_OUTPUT,
        },
        location=LOCATION,
        deferrable=True,
    )
    # [END howto_operator_start_template_job_deferrable]

    # [START howto_operator_start_flex_template_job_deferrable]
    start_flex_template_job_deferrable = DataflowStartFlexTemplateOperator(
        task_id="start_flex_template_job_deferrable",
        project_id=PROJECT_ID,
        body=BODY,
        location=LOCATION,
        append_job_name=False,
        deferrable=True,
    )
    # [END howto_operator_start_flex_template_job_deferrable]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        create_bucket,
        upload_file,
        upload_schema,
        [start_template_job, start_flex_template_job],
        [start_template_job_deferrable, start_flex_template_job_deferrable],
        delete_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
