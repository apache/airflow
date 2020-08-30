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
import time

from airflow import models
from airflow.providers.google.marketing_platform.operators.campaign_manager import (
    GoogleCampaignManagerBatchInsertConversionsOperator,
    GoogleCampaignManagerBatchUpdateConversionsOperator,
    GoogleCampaignManagerDeleteReportOperator,
    GoogleCampaignManagerDownloadReportOperator,
    GoogleCampaignManagerInsertReportOperator,
    GoogleCampaignManagerRunReportOperator,
)
from airflow.providers.google.marketing_platform.sensors.campaign_manager import (
    GoogleCampaignManagerReportSensor,
)
from airflow.utils import dates
from airflow.utils.state import State

PROFILE_ID = os.environ.get("MARKETING_PROFILE_ID", "123456789")
FLOODLIGHT_ACTIVITY_ID = int(os.environ.get("FLOODLIGHT_ACTIVITY_ID", 12345))
FLOODLIGHT_CONFIGURATION_ID = int(os.environ.get("FLOODLIGHT_CONFIGURATION_ID", 12345))
ENCRYPTION_ENTITY_ID = int(os.environ.get("ENCRYPTION_ENTITY_ID", 12345))
DEVICE_ID = os.environ.get("DEVICE_ID", "12345")
BUCKET = os.environ.get("MARKETING_BUCKET", "test-cm-bucket")
REPORT_NAME = "test-report"
REPORT = {
    "type": "STANDARD",
    "name": REPORT_NAME,
    "criteria": {
        "dateRange": {"kind": "dfareporting#dateRange", "relativeDateRange": "LAST_365_DAYS",},
        "dimensions": [{"kind": "dfareporting#sortedDimension", "name": "dfa:advertiser"}],
        "metricNames": ["dfa:activeViewImpressionDistributionViewable"],
    },
}

CONVERSION = {
    "kind": "dfareporting#conversion",
    "floodlightActivityId": FLOODLIGHT_ACTIVITY_ID,
    "floodlightConfigurationId": FLOODLIGHT_CONFIGURATION_ID,
    "mobileDeviceId": DEVICE_ID,
    "ordinal": "0",
    "quantity": 42,
    "value": 123.4,
    "timestampMicros": int(time.time()) * 1000000,
    "customVariables": [{"kind": "dfareporting#customFloodlightVariable", "type": "U4", "value": "value",}],
}

CONVERSION_UPDATE = {
    "kind": "dfareporting#conversion",
    "floodlightActivityId": FLOODLIGHT_ACTIVITY_ID,
    "floodlightConfigurationId": FLOODLIGHT_CONFIGURATION_ID,
    "mobileDeviceId": DEVICE_ID,
    "ordinal": "0",
    "quantity": 42,
    "value": 123.4,
}

with models.DAG(
    "example_campaign_manager",
    schedule_interval=None,  # Override to match your needs,
    start_date=dates.days_ago(1),
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
        task_id="wait_for_report", profile_id=PROFILE_ID, report_id=report_id, file_id=file_id,
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

    # [START howto_campaign_manager_insert_conversions]
    insert_conversion = GoogleCampaignManagerBatchInsertConversionsOperator(
        task_id="insert_conversion",
        profile_id=PROFILE_ID,
        conversions=[CONVERSION],
        encryption_source="AD_SERVING",
        encryption_entity_type="DCM_ADVERTISER",
        encryption_entity_id=ENCRYPTION_ENTITY_ID,
    )
    # [END howto_campaign_manager_insert_conversions]

    # [START howto_campaign_manager_update_conversions]
    update_conversion = GoogleCampaignManagerBatchUpdateConversionsOperator(
        task_id="update_conversion",
        profile_id=PROFILE_ID,
        conversions=[CONVERSION_UPDATE],
        encryption_source="AD_SERVING",
        encryption_entity_type="DCM_ADVERTISER",
        encryption_entity_id=ENCRYPTION_ENTITY_ID,
        max_failed_updates=1,
    )
    # [END howto_campaign_manager_update_conversions]

    insert_conversion >> update_conversion

if __name__ == "__main__":
    dag.clear(dag_run_state=State.NONE)
    dag.run()
