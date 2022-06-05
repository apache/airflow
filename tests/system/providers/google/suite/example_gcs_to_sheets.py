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
from airflow.providers.google.cloud.transfers.sheets_to_gcs import GoogleSheetsToGCSOperator
from airflow.providers.google.suite.transfers.gcs_to_sheets import GCSToGoogleSheetsOperator
from airflow.utils.trigger_rule import TriggerRule

BUCKET = os.environ.get("GCP_GCS_BUCKET", "example-test-bucket3")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "example-spreadsheetID")
NEW_SPREADSHEET_ID = os.environ.get("NEW_SPREADSHEET_ID", "1234567890qwerty")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_gcs_to_sheets"


with models.DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule_interval='@once',  # Override to match your needs
    catchup=False,
    tags=["example"],
) as dag:

    upload_sheet_to_gcs = GoogleSheetsToGCSOperator(
        task_id="upload_sheet_to_gcs",
        destination_bucket=BUCKET,
        spreadsheet_id=SPREADSHEET_ID,
    )

    # [START upload_gcs_to_sheets]
    upload_gcs_to_sheet = GCSToGoogleSheetsOperator(
        task_id="upload_gcs_to_sheet",
        bucket_name=BUCKET,
        object_name="{{ task_instance.xcom_pull('upload_sheet_to_gcs')[0] }}",
        spreadsheet_id=NEW_SPREADSHEET_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END upload_gcs_to_sheets]

    upload_sheet_to_gcs >> upload_gcs_to_sheet

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
