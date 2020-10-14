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
from typing import Dict

from airflow import models
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook
from airflow.providers.google.marketing_platform.operators.display_video import (
    GoogleDisplayVideo360CreateReportOperator,
    GoogleDisplayVideo360CreateSDFDownloadTaskOperator,
    GoogleDisplayVideo360DeleteReportOperator,
    GoogleDisplayVideo360DownloadLineItemsOperator,
    GoogleDisplayVideo360DownloadReportOperator,
    GoogleDisplayVideo360RunReportOperator,
    GoogleDisplayVideo360SDFtoGCSOperator,
    GoogleDisplayVideo360UploadLineItemsOperator,
)
from airflow.providers.google.marketing_platform.sensors.display_video import (
    GoogleDisplayVideo360GetSDFDownloadOperationSensor,
    GoogleDisplayVideo360ReportSensor,
)
from airflow.utils import dates

# [START howto_display_video_env_variables]
BUCKET = os.environ.get("GMP_DISPLAY_VIDEO_BUCKET", "gs://test-display-video-bucket")
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

PARAMS = {"dataRange": "LAST_14_DAYS", "timezoneCode": "America/New_York"}

CREATE_SDF_DOWNLOAD_TASK_BODY_REQUEST: Dict = {
    "version": SDF_VERSION,
    "advertiserId": ADVERTISER_ID,
    "inventorySourceFilter": {"inventorySourceIds": []},
}

DOWNLOAD_LINE_ITEMS_REQUEST: Dict = {"filterType": ADVERTISER_ID, "format": "CSV", "fileSpec": "EWF"}
# [END howto_display_video_env_variables]

with models.DAG(
    "example_display_video",
    schedule_interval=None,  # Override to match your needs,
    start_date=dates.days_ago(1),
) as dag1:
    # [START howto_google_display_video_createquery_report_operator]
    create_report = GoogleDisplayVideo360CreateReportOperator(body=REPORT, task_id="create_report")
    report_id = "{{ task_instance.xcom_pull('create_report', key='report_id') }}"
    # [END howto_google_display_video_createquery_report_operator]

    # [START howto_google_display_video_runquery_report_operator]
    run_report = GoogleDisplayVideo360RunReportOperator(
        report_id=report_id, params=PARAMS, task_id="run_report"
    )
    # [END howto_google_display_video_runquery_report_operator]

    # [START howto_google_display_video_wait_report_operator]
    wait_for_report = GoogleDisplayVideo360ReportSensor(task_id="wait_for_report", report_id=report_id)
    # [END howto_google_display_video_wait_report_operator]

    # [START howto_google_display_video_getquery_report_operator]
    get_report = GoogleDisplayVideo360DownloadReportOperator(
        report_id=report_id,
        task_id="get_report",
        bucket_name=BUCKET,
        report_name="test1.csv",
    )
    # [END howto_google_display_video_getquery_report_operator]

    # [START howto_google_display_video_deletequery_report_operator]
    delete_report = GoogleDisplayVideo360DeleteReportOperator(report_id=report_id, task_id="delete_report")
    # [END howto_google_display_video_deletequery_report_operator]

    create_report >> run_report >> wait_for_report >> get_report >> delete_report

with models.DAG(
    "example_display_video_misc",
    schedule_interval=None,  # Override to match your needs,
    start_date=dates.days_ago(1),
) as dag2:
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

with models.DAG(
    "example_display_video_sdf",
    schedule_interval=None,  # Override to match your needs,
    start_date=dates.days_ago(1),
) as dag3:
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
        source_objects=['{{ task_instance.xcom_pull("upload_sdf_to_bigquery")}}'],
        destination_project_dataset_table=f"{BQ_DATA_SET}.gcs_to_bq_table",
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "post_abbr", "type": "STRING", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
    )
    # [END howto_google_display_video_gcs_to_big_query_operator]

    create_sdf_download_task >> wait_for_operation >> save_sdf_in_gcs >> upload_sdf_to_big_query
