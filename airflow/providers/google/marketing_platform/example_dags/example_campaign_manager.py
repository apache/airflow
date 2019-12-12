# -*- coding: utf-8 -*-
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
Example Airflow DAG that shows how to use CampaignManager.
"""
import os

from airflow import models
from airflow.providers.google.marketing_platform.operators.campaign_manager import (
    GoogleCampaignManagerDeleteReportOperator, GoogleCampaignManagerDownloadReportOperator,
    GoogleCampaignManagerInsertReportOperator, GoogleCampaignManagerRunReportOperator,
)
from airflow.providers.google.marketing_platform.sensors.campaign_manager import (
    GoogleCampaignManagerReportSensor,
)
from airflow.utils import dates

PROFILE_ID = os.environ.get("MARKETING_PROFILE_ID", "123456789")
BUCKET = os.environ.get("MARKETING_BUCKET", "test-cm-bucket")
REPORT_NAME = "test-report"
REPORT = {
    "type": "STANDARD",
    "name": REPORT_NAME,
    "criteria": {
        "dateRange": {
            "kind": "dfareporting#dateRange",
            "relativeDateRange": "LAST_365_DAYS",
        },
        "dimensions": [
            {"kind": "dfareporting#sortedDimension", "name": "dfa:advertiser"}
        ],
        "metricNames": ["dfa:activeViewImpressionDistributionViewable"],
    },
}


default_args = {"start_date": dates.days_ago(1)}

with models.DAG(
    "example_campaign_manager",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
) as dag:
    # [START howto_campaign_manager_insert_report_operator]
    create_report = GoogleCampaignManagerInsertReportOperator(
        profile_id=PROFILE_ID, report=REPORT, task_id="create_report"
    )
    report_id = "{{ task_instance.xcom_pull('create_report')['id'] }}"
    # [END howto_campaign_manager_insert_report_operator]

    # [START howto_campaign_manager_run_report_operator]
    run_report = GoogleCampaignManagerRunReportOperator(
        profile_id=PROFILE_ID, report_id=report_id, task_id="run_report"
    )
    file_id = "{{ task_instance.xcom_pull('run_report')['id'] }}"
    # [END howto_campaign_manager_run_report_operator]

    # [START howto_campaign_manager_wait_for_operation]
    wait_for_report = GoogleCampaignManagerReportSensor(
        task_id="wait_for_report",
        profile_id=PROFILE_ID,
        report_id=report_id,
        file_id=file_id,
    )
    # [END howto_campaign_manager_wait_for_operation]

    # [START howto_campaign_manager_get_report_operator]
    get_report = GoogleCampaignManagerDownloadReportOperator(
        task_id="get_report",
        profile_id=PROFILE_ID,
        report_id=report_id,
        file_id=file_id,
        report_name="test_report.csv",
        bucket_name=BUCKET,
    )
    # [END howto_campaign_manager_get_report_operator]

    # [START howto_campaign_manager_delete_report_operator]
    delete_report = GoogleCampaignManagerDeleteReportOperator(
        profile_id=PROFILE_ID, report_name=REPORT_NAME, task_id="delete_report"
    )
    # [END howto_campaign_manager_delete_report_operator]

    create_report >> run_report >> wait_for_report >> get_report >> delete_report
