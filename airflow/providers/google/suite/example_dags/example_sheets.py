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

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.sheets_to_gcs import GoogleSheetsToGCSOperator
from airflow.providers.google.suite.operators.gcs_to_sheets import GCSToGoogleSheetsOperator
from airflow.providers.google.suite.operators.sheets import GoogleSheetsCreateSpreadsheet
from airflow.utils.dates import days_ago

GCS_BUCKET = os.environ.get("SHEETS_GCS_BUCKET", "test28397ye")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1234567890qwerty")
NEW_SPREADSHEET_ID = os.environ.get("NEW_SPREADSHEET_ID", "1234567890qwerty")

SPREADSHEET = {
    "properties": {"title": "Test1"},
    "sheets": [{"properties": {"title": "Sheet1"}}],
}

default_args = {"start_date": days_ago(1)}

with models.DAG(
    "example_sheets_gcs",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
    tags=["example"],
) as dag:
    # [START upload_sheet_to_gcs]
    upload_sheet_to_gcs = GoogleSheetsToGCSOperator(
        task_id="upload_sheet_to_gcs",
        destination_bucket=GCS_BUCKET,
        spreadsheet_id=SPREADSHEET_ID,
    )
    # [END upload_sheet_to_gcs]

    # [START create_spreadsheet]
    create_spreadsheet = GoogleSheetsCreateSpreadsheet(
        task_id="create_spreadsheet", spreadsheet=SPREADSHEET
    )
    # [END create_spreadsheet]

    # [START print_spreadsheet_url]
    print_spreadsheet_url = BashOperator(
        task_id="print_spreadsheet_url",
        bash_command="echo {{ task_instance.xcom_pull('create_spreadsheet', key='spreadsheet_url') }}",
    )
    # [END print_spreadsheet_url]

    # [START upload_gcs_to_sheet]
    upload_gcs_to_sheet = GCSToGoogleSheetsOperator(
        task_id="upload_gcs_to_sheet",
        bucket_name=GCS_BUCKET,
        object_name="{{ task_instance.xcom_pull('upload_sheet_to_gcs')[0] }}",
        spreadsheet_id=NEW_SPREADSHEET_ID,
    )
    # [END upload_gcs_to_sheet]

    create_spreadsheet >> print_spreadsheet_url
    upload_sheet_to_gcs >> upload_gcs_to_sheet
