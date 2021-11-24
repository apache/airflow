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

BUCKET = os.environ.get("GCP_GCS_BUCKET", "test28397yeo")
SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1234567890qwerty")

with models.DAG(
    "example_sheets_to_gcs",
    schedule_interval='@once',  # Override to match your needs
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    # [START upload_sheet_to_gcs]
    upload_sheet_to_gcs = GoogleSheetsToGCSOperator(
        task_id="upload_sheet_to_gcs",
        destination_bucket=BUCKET,
        spreadsheet_id=SPREADSHEET_ID,
    )
    # [END upload_sheet_to_gcs]
