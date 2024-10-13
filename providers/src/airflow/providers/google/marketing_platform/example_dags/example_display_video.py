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

from __future__ import annotations

import os
from datetime import datetime
from typing import cast

from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook
from airflow.providers.google.marketing_platform.operators.display_video import (
    GoogleDisplayVideo360CreateQueryOperator,
    GoogleDisplayVideo360CreateSDFDownloadTaskOperator,
    GoogleDisplayVideo360DeleteReportOperator,
    GoogleDisplayVideo360DownloadLineItemsOperator,
    GoogleDisplayVideo360DownloadReportV2Operator,
    GoogleDisplayVideo360RunQueryOperator,
    GoogleDisplayVideo360SDFtoGCSOperator,
    GoogleDisplayVideo360UploadLineItemsOperator,
)
from airflow.providers.google.marketing_platform.sensors.display_video import (
    GoogleDisplayVideo360GetSDFDownloadOperationSensor,
    GoogleDisplayVideo360RunQuerySensor,
)

# [START howto_display_video_env_variables]
BUCKET = os.environ.get("GMP_DISPLAY_VIDEO_BUCKET", "gs://INVALID BUCKET NAME")
ADVERTISER_ID = os.environ.get("GMP_ADVERTISER_ID", 1234567)
OBJECT_NAME = os.environ.get("GMP_OBJECT_NAME", "files/report.csv")
PATH_TO_UPLOAD_FILE = os.environ.get("GCP_GCS_PATH_TO_UPLOAD_FILE", "test-gcs-example.txt")
PATH_TO_SAVED_FILE = os.environ.get("GCP_GCS_PATH_TO_SAVED_FILE", "test-gcs-example-download.txt")
BUCKET_FILE_LOCATION = PATH_TO_UPLOAD_FILE.rpartition("/")[-1]
SDF_VERSION = os.environ.get("GMP_SDF_VERSION", "SDF_VERSION_5_5")
BQ_DATA_SET = os.environ.get("GMP_BQ_DATA_SET", "airflow_test")
GMP_PARTNER_ID = os.environ.get("GMP_PARTNER_ID", 123)
ENTITY_TYPE = os.environ.get("GMP_ENTITY_TYPE", "LineItem")
ERF_SOURCE_OBJECT = GoogleDisplayVideo360Hook.erf_uri(GMP_PARTNER_ID, ENTITY_TYPE)

REPORT_V2 = {
    "metadata": {
        "title": "Airflow Test Report",
        "dataRange": {"range": "LAST_7_DAYS"},
        "format": "CSV",
        "sendNotification": False,
    },
    "params": {
        "type": "STANDARD",
        "groupBys": ["FILTER_DATE", "FILTER_PARTNER"],
        "filters": [{"type": "FILTER_PARTNER", "value": ADVERTISER_ID}],
        "metrics": ["METRIC_IMPRESSIONS", "METRIC_CLICKS"],
    },
    "schedule": {"frequency": "ONE_TIME"},
}

PARAMETERS = {
    "dataRange": {"range": "LAST_7_DAYS"},
}

CREATE_SDF_DOWNLOAD_TASK_BODY_REQUEST: dict = {
    "version": SDF_VERSION,
    "advertiserId": ADVERTISER_ID,
    "inventorySourceFilter": {"inventorySourceIds": []},
}

DOWNLOAD_LINE_ITEMS_REQUEST: dict = {"filterType": ADVERTISER_ID, "format": "CSV", "fileSpec": "EWF"}
# [END howto_display_video_env_variables]

START_DATE = datetime(2021, 1, 1)

with DAG(
    "example_display_video_misc",
    start_date=START_DATE,
    catchup=False,
) as dag_example_display_video_misc:
    # [START howto_google_display_video_upload_multiple_entity_read_files_to_big_query]
    upload_erf_to_bq = GCSToBigQueryOperator(
        task_id="upload_erf_to_bq",
        bucket=BUCKET,
        source_objects=ERF_SOURCE_OBJECT,
        destination_project_dataset_table=f"{BQ_DATA_SET}.gcs_to_bq_table",
        write_disposition="WRITE_TRUNCATE",
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

with DAG(
    "example_display_video_sdf",
    start_date=START_DATE,
    catchup=False,
) as dag_example_display_video_sdf:
    # [START howto_google_display_video_create_sdf_download_task_operator]
    create_sdf_download_task = GoogleDisplayVideo360CreateSDFDownloadTaskOperator(
        task_id="create_sdf_download_task", body_request=CREATE_SDF_DOWNLOAD_TASK_BODY_REQUEST
    )
    operation_name = '{{ task_instance.xcom_pull("create_sdf_download_task")["name"] }}'
    # [END howto_google_display_video_create_sdf_download_task_operator]

    # [START howto_google_display_video_wait_for_operation_sensor]
    wait_for_operation = GoogleDisplayVideo360GetSDFDownloadOperationSensor(
        task_id="wait_for_operation",
        operation_name=operation_name,
    )
    # [END howto_google_display_video_wait_for_operation_sensor]

    # [START howto_google_display_video_save_sdf_in_gcs_operator]
    save_sdf_in_gcs = GoogleDisplayVideo360SDFtoGCSOperator(
        task_id="save_sdf_in_gcs",
        operation_name=operation_name,
        bucket_name=BUCKET,
        object_name=BUCKET_FILE_LOCATION,
        gzip=False,
    )
    # [END howto_google_display_video_save_sdf_in_gcs_operator]

    # [START howto_google_display_video_gcs_to_big_query_operator]
    upload_sdf_to_big_query = GCSToBigQueryOperator(
        task_id="upload_sdf_to_big_query",
        bucket=BUCKET,
        source_objects=[save_sdf_in_gcs.output],
        destination_project_dataset_table=f"{BQ_DATA_SET}.gcs_to_bq_table",
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    # [END howto_google_display_video_gcs_to_big_query_operator]

    create_sdf_download_task >> wait_for_operation >> save_sdf_in_gcs

    # Task dependency created via `XComArgs`:
    #   save_sdf_in_gcs >> upload_sdf_to_big_query

with DAG(
    "example_display_video_v2",
    start_date=START_DATE,
    catchup=False,
) as dag:
    # [START howto_google_display_video_create_query_operator]
    create_query_v2 = GoogleDisplayVideo360CreateQueryOperator(body=REPORT_V2, task_id="create_query")

    query_id = cast(str, XComArg(create_query_v2, key="query_id"))
    # [END howto_google_display_video_create_query_operator]

    # [START howto_google_display_video_run_query_report_operator]
    run_query_v2 = GoogleDisplayVideo360RunQueryOperator(
        query_id=query_id, parameters=PARAMETERS, task_id="run_report"
    )

    query_id = cast(str, XComArg(run_query_v2, key="query_id"))
    report_id = cast(str, XComArg(run_query_v2, key="report_id"))
    # [END howto_google_display_video_run_query_report_operator]

    # [START howto_google_display_video_wait_run_query_sensor]
    wait_for_query = GoogleDisplayVideo360RunQuerySensor(
        task_id="wait_for_query",
        query_id=query_id,
        report_id=report_id,
    )
    # [END howto_google_display_video_wait_run_query_sensor]

    # [START howto_google_display_video_get_report_operator]
    get_report_v2 = GoogleDisplayVideo360DownloadReportV2Operator(
        query_id=query_id,
        report_id=report_id,
        task_id="get_report",
        bucket_name=BUCKET,
        report_name="test1.csv",
    )
    # # [END howto_google_display_video_get_report_operator]
    # # [START howto_google_display_video_delete_query_report_operator]
    delete_report_v2 = GoogleDisplayVideo360DeleteReportOperator(report_id=report_id, task_id="delete_report")
    # # [END howto_google_display_video_delete_query_report_operator]

    create_query_v2 >> run_query_v2 >> wait_for_query >> get_report_v2 >> delete_report_v2
