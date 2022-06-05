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
from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook
from airflow.providers.google.marketing_platform.operators.display_video import (
    GoogleDisplayVideo360CreateReportOperator,
    GoogleDisplayVideo360DeleteReportOperator,
    GoogleDisplayVideo360DownloadReportOperator,
    GoogleDisplayVideo360RunReportOperator,
)
from airflow.providers.google.marketing_platform.sensors.display_video import (
    GoogleDisplayVideo360ReportSensor,
)
from airflow.utils.trigger_rule import TriggerRule

# [START howto_display_video_env_variables]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_display_video"
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
    # [START howto_google_display_video_createquery_report_operator]
    create_report = GoogleDisplayVideo360CreateReportOperator(body=REPORT, task_id="create_report")
    report_id = create_report.output["report_id"]
    # [END howto_google_display_video_createquery_report_operator]

    # [START howto_google_display_video_runquery_report_operator]
    run_report = GoogleDisplayVideo360RunReportOperator(
        report_id=report_id, parameters=PARAMETERS, task_id="run_report"
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
    delete_report = GoogleDisplayVideo360DeleteReportOperator(
        report_id=report_id,
        task_id="delete_report",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_google_display_video_deletequery_report_operator]

    run_report >> wait_for_report >> get_report >> delete_report

    # Task dependencies created via `XComArgs`:
    #   create_report >> run_report
    #   create_report >> wait_for_report
    #   create_report >> get_report
    #   create_report >> delete_report

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
