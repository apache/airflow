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

from airflow import models
from airflow.providers.google.marketing_platform.operators.display_video import (
    GoogleDisplayVideo360CreateReportOperator, GoogleDisplayVideo360DeleteReportOperator,
    GoogleDisplayVideo360DownloadReportOperator, GoogleDisplayVideo360RunReportOperator,
)
from airflow.providers.google.marketing_platform.sensors.display_video import (
    GoogleDisplayVideo360ReportSensor,
)
from airflow.utils import dates

# [START howto_display_video_env_variables]
BUCKET = os.environ.get("GMP_DISPLAY_VIDEO_BUCKET", "gs://test-display-video-bucket")
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
# [END howto_display_video_env_variables]

default_args = {"start_date": dates.days_ago(1)}

with models.DAG(
    "example_display_video",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
) as dag:
    # [START howto_google_display_video_createquery_report_operator]
    create_report = GoogleDisplayVideo360CreateReportOperator(
        body=REPORT, task_id="create_report"
    )
    report_id = "{{ task_instance.xcom_pull('create_report', key='report_id') }}"
    # [END howto_google_display_video_createquery_report_operator]

    # [START howto_google_display_video_runquery_report_operator]
    run_report = GoogleDisplayVideo360RunReportOperator(
        report_id=report_id, params=PARAMS, task_id="run_report"
    )
    # [END howto_google_display_video_runquery_report_operator]

    # [START howto_google_display_video_wait_report_operator]
    wait_for_report = GoogleDisplayVideo360ReportSensor(
        task_id="wait_for_report", report_id=report_id
    )
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
        report_id=report_id, task_id="delete_report"
    )
    # [END howto_google_display_video_deletequery_report_operator]

    create_report >> run_report >> wait_for_report >> get_report >> delete_report
