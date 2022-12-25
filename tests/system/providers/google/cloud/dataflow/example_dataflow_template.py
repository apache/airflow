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
Example Airflow DAG for testing Google Dataflow
:class:`~airflow.providers.google.cloud.operators.dataflow.DataflowTemplatedJobStartOperator` operator.
"""
from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

from airflow import models
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "dataflow_template"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

FILE_NAME = "text.txt"
GCS_TMP = f"gs://{BUCKET_NAME}/temp/"
GCS_STAGING = f"gs://{BUCKET_NAME}/staging/"
GCS_OUTPUT = f"gs://{BUCKET_NAME}/output"
PYTHON_FILE_LOCAL_PATH = str(Path(__file__).parent / "resources" / FILE_NAME)
LOCATION = "europe-west3"

default_args = {
    "dataflow_default_options": {
        "tempLocation": GCS_TMP,
        "stagingLocation": GCS_STAGING,
    }
}

with models.DAG(
    DAG_ID,
    default_args=default_args,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "dataflow"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    upload_file = LocalFilesystemToGCSOperator(
        task_id="upload_file_to_bucket",
        src=PYTHON_FILE_LOCAL_PATH,
        dst=FILE_NAME,
        bucket=BUCKET_NAME,
    )

    # [START howto_operator_start_template_job]
    start_template_job = DataflowTemplatedJobStartOperator(
        task_id="start_template_job",
        template="gs://dataflow-templates/latest/Word_Count",
        parameters={"inputFile": f"gs://{BUCKET_NAME}/{FILE_NAME}", "output": GCS_OUTPUT},
        location=LOCATION,
    )
    # [END howto_operator_start_template_job]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    create_bucket >> upload_file >> start_template_job >> delete_bucket

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
