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

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.sheets_to_gcs import GoogleSheetsToGCSOperator
from airflow.providers.google.suite.transfers.gcs_to_sheets import GCSToGoogleSheetsOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")
DAG_ID = "example_gcs_to_sheets"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "example-spreadsheetID")
NEW_SPREADSHEET_ID = os.environ.get("NEW_SPREADSHEET_ID", "1234567890qwerty")

with models.DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule='@once',  # Override to match your needs
    catchup=False,
    tags=["example", "gcs"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    upload_sheet_to_gcs = GoogleSheetsToGCSOperator(
        task_id="upload_sheet_to_gcs",
        destination_bucket=BUCKET_NAME,
        spreadsheet_id=SPREADSHEET_ID,
    )

    # [START upload_gcs_to_sheets]
    upload_gcs_to_sheet = GCSToGoogleSheetsOperator(
        task_id="upload_gcs_to_sheet",
        bucket_name=BUCKET_NAME,
        object_name="{{ task_instance.xcom_pull('upload_sheet_to_gcs')[0] }}",
        spreadsheet_id=NEW_SPREADSHEET_ID,
    )
    # [END upload_gcs_to_sheets]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> upload_sheet_to_gcs
        # TEST BODY
        >> upload_gcs_to_sheet
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
