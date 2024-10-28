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
This is a basic example dag for using `GoogleApiToS3Operator` to retrieve Google Sheets data
You need to set all env variables to request the data.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.amazon.aws.operators.s3 import (
    S3CreateBucketOperator,
    S3DeleteBucketOperator,
)
from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator
from airflow.utils.trigger_rule import TriggerRule

from providers.tests.system.amazon.aws.utils import SystemTestContextBuilder

sys_test_context_task = SystemTestContextBuilder().build()

DAG_ID = "example_google_api_sheets_to_s3"

GOOGLE_SHEET_ID = os.getenv("GOOGLE_SHEET_ID", "test-google-sheet-id")
GOOGLE_SHEET_RANGE = os.getenv("GOOGLE_SHEET_RANGE", "test-google-sheet-range")
S3_DESTINATION_KEY = os.getenv("S3_DESTINATION_KEY", "s3://test-bucket/key.json")

with DAG(
    dag_id=DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    test_context = sys_test_context_task()
    env_id = test_context["ENV_ID"]

    s3_bucket = f"{env_id}-google-api-sheets"
    s3_key = f"{env_id}-google-api-key"

    create_s3_bucket = S3CreateBucketOperator(
        task_id="create_s3_bucket", bucket_name=s3_bucket
    )

    # [START howto_transfer_google_api_sheets_to_s3]
    task_google_sheets_values_to_s3 = GoogleApiToS3Operator(
        task_id="google_sheet_data_to_s3",
        google_api_service_name="sheets",
        google_api_service_version="v4",
        google_api_endpoint_path="sheets.spreadsheets.values.get",
        google_api_endpoint_params={
            "spreadsheetId": GOOGLE_SHEET_ID,
            "range": GOOGLE_SHEET_RANGE,
        },
        s3_destination_key=f"s3://{s3_bucket}/{s3_key}",
    )
    # [END howto_transfer_google_api_sheets_to_s3]

    delete_s3_bucket = S3DeleteBucketOperator(
        task_id="delete_s3_bucket",
        bucket_name=s3_bucket,
        force_delete=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        test_context,
        create_s3_bucket,
        # TEST BODY
        task_google_sheets_values_to_s3,
        # TEST TEARDOWN
        delete_s3_bucket,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
