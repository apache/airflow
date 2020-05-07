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
This is a basic example dag for using `GoogleApiToS3Transfer` to retrieve Google Sheets data:

You need to set all env variables to request the data.
"""

from os import getenv

from airflow import DAG
from airflow.providers.amazon.aws.operators.google_api_to_s3_transfer import GoogleApiToS3Transfer
from airflow.utils.dates import days_ago

# [START howto_operator_google_api_to_s3_transfer_basic_env_variables]
GOOGLE_SHEET_ID = getenv("GOOGLE_SHEET_ID")
GOOGLE_SHEET_RANGE = getenv("GOOGLE_SHEET_RANGE")
S3_DESTINATION_KEY = getenv("S3_DESTINATION_KEY", "s3://bucket/key.json")
# [END howto_operator_google_api_to_s3_transfer_basic_env_variables]

default_args = {"start_date": days_ago(1)}

with DAG(
    dag_id="example_google_api_to_s3_transfer_basic",
    default_args=default_args,
    schedule_interval=None,
    tags=['example']
) as dag:
    # [START howto_operator_google_api_to_s3_transfer_basic_task_1]
    task_google_sheets_values_to_s3 = GoogleApiToS3Transfer(
        google_api_service_name='sheets',
        google_api_service_version='v4',
        google_api_endpoint_path='sheets.spreadsheets.values.get',
        google_api_endpoint_params={
            'spreadsheetId': GOOGLE_SHEET_ID,
            'range': GOOGLE_SHEET_RANGE
        },
        s3_destination_key=S3_DESTINATION_KEY,
        task_id='google_sheets_values_to_s3',
        dag=dag
    )
    # [END howto_operator_google_api_to_s3_transfer_basic_task_1]
