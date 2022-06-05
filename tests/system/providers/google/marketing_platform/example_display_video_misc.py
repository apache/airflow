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
Example Airflow DAG that shows how to use DisplayVideo.
"""
import os
from datetime import datetime
from typing import Dict

from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook
from airflow.providers.google.marketing_platform.operators.display_video import (
    GoogleDisplayVideo360DownloadLineItemsOperator,
    GoogleDisplayVideo360UploadLineItemsOperator,
)

# [START howto_display_video_env_variables]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_display_video_misc"
BUCKET = os.environ.get("GMP_DISPLAY_VIDEO_BUCKET", "gs://INVALID BUCKET NAME")
ADVERTISER_ID = os.environ.get("GMP_ADVERTISER_ID", 1234567)
OBJECT_NAME = os.environ.get("GMP_OBJECT_NAME", "files/report.csv")
PATH_TO_UPLOAD_FILE = os.environ.get("GCP_GCS_PATH_TO_UPLOAD_FILE", "test-gcs-example.txt")
PATH_TO_SAVED_FILE = os.environ.get("GCP_GCS_PATH_TO_SAVED_FILE", "test-gcs-example-download.txt")
BUCKET_FILE_LOCATION = PATH_TO_UPLOAD_FILE.rpartition("/")[-1]
SDF_VERSION = os.environ.get("GMP_SDF_VERSION", "SDF_VERSION_5_1")
BQ_DATA_SET = os.environ.get("GMP_BQ_DATA_SET", "airflow_test")
GMP_PARTNER_ID = os.environ.get("GMP_PARTNER_ID", 123)
ENTITY_TYPE = os.environ.get("GMP_ENTITY_TYPE", "LineItem")
ERF_SOURCE_OBJECT = GoogleDisplayVideo360Hook.erf_uri(GMP_PARTNER_ID, ENTITY_TYPE)

REPORT = {
    "kind": "doubleclickbidmanager#query",
    "metadata": {
        "title": "Polidea Test Report",
        "dataRange": "LAST_7_DAYS",
        "format": "CSV",
        "sendNotification": False,
    },
    "params": {
        "type": "TYPE_GENERAL",
        "groupBys": ["FILTER_DATE", "FILTER_PARTNER"],
        "filters": [{"type": "FILTER_PARTNER", "value": 1486931}],
        "metrics": ["METRIC_IMPRESSIONS", "METRIC_CLICKS"],
        "includeInviteData": True,
    },
    "schedule": {"frequency": "ONE_TIME"},
}

PARAMETERS = {"dataRange": "LAST_14_DAYS", "timezoneCode": "America/New_York"}

CREATE_SDF_DOWNLOAD_TASK_BODY_REQUEST: Dict = {
    "version": SDF_VERSION,
    "advertiserId": ADVERTISER_ID,
    "inventorySourceFilter": {"inventorySourceIds": []},
}

DOWNLOAD_LINE_ITEMS_REQUEST: Dict = {"filterType": ADVERTISER_ID, "format": "CSV", "fileSpec": "EWF"}
# [END howto_display_video_env_variables]

START_DATE = datetime(2021, 1, 1)

with models.DAG(
    DAG_ID,
    schedule_interval='@once',  # Override to match your needs,
    start_date=START_DATE,
    catchup=False,
) as dag:
    # [START howto_google_display_video_upload_multiple_entity_read_files_to_big_query]
    upload_erf_to_bq = GCSToBigQueryOperator(
        task_id='upload_erf_to_bq',
        bucket=BUCKET,
        source_objects=ERF_SOURCE_OBJECT,
        destination_project_dataset_table=f"{BQ_DATA_SET}.gcs_to_bq_table",
        write_disposition='WRITE_TRUNCATE',
    )
    # [END howto_google_display_video_upload_multiple_entity_read_files_to_big_query]

    # [START howto_google_display_video_download_line_items_operator]
    download_line_items = GoogleDisplayVideo360DownloadLineItemsOperator(
        task_id="download_line_items",
        request_body=DOWNLOAD_LINE_ITEMS_REQUEST,
        bucket_name=BUCKET,
        object_name=OBJECT_NAME,
        gzip=False,
    )
    # [END howto_google_display_video_download_line_items_operator]

    # [START howto_google_display_video_upload_line_items_operator]
    upload_line_items = GoogleDisplayVideo360UploadLineItemsOperator(
        task_id="upload_line_items",
        bucket_name=BUCKET,
        object_name=BUCKET_FILE_LOCATION,
    )
    # [END howto_google_display_video_upload_line_items_operator]


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
