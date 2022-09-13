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

from datetime import datetime
from os import getenv

from airflow import DAG
from airflow.providers.amazon.aws.transfers.google_api_to_s3 import GoogleApiToS3Operator

GOOGLE_SHEET_ID = getenv("GOOGLE_SHEET_ID", "test-google-sheet-id")
GOOGLE_SHEET_RANGE = getenv("GOOGLE_SHEET_RANGE", "test-google-sheet-range")
S3_DESTINATION_KEY = getenv("S3_DESTINATION_KEY", "s3://test-bucket/key.json")

with DAG(
    dag_id="example_google_api_sheets_to_s3",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    # [START howto_transfer_google_api_sheets_to_s3]
    task_google_sheets_values_to_s3 = GoogleApiToS3Operator(
        task_id='google_sheet_data_to_s3',
        google_api_service_name='sheets',
        google_api_service_version='v4',
        google_api_endpoint_path='sheets.spreadsheets.values.get',
        google_api_endpoint_params={'spreadsheetId': GOOGLE_SHEET_ID, 'range': GOOGLE_SHEET_RANGE},
        s3_destination_key=S3_DESTINATION_KEY,
    )
    # [END howto_transfer_google_api_sheets_to_s3]
