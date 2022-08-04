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
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
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
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "example_campaign_manager"

PROFILE_ID = os.environ.get("MARKETING_PROFILE_ID", "123456789")
FLOODLIGHT_ACTIVITY_ID = int(os.environ.get("FLOODLIGHT_ACTIVITY_ID", 12345))
FLOODLIGHT_CONFIGURATION_ID = int(os.environ.get("FLOODLIGHT_CONFIGURATION_ID", 12345))
ENCRYPTION_ENTITY_ID = int(os.environ.get("ENCRYPTION_ENTITY_ID", 12345))
DEVICE_ID = os.environ.get("DEVICE_ID", "12345")
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
REPORT_NAME = f"report_{DAG_ID}_{ENV_ID}"
REPORT = {
    "type": "STANDARD",
    "name": REPORT_NAME,
    "criteria": {
        "dateRange": {
            "kind": "dfareporting#dateRange",
            "relativeDateRange": "LAST_365_DAYS",
        },
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
    "customVariables": [
        {
            "kind": "dfareporting#customFloodlightVariable",
            "type": "U4",
            "value": "value",
        }
    ],
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
    DAG_ID,
    schedule_interval='@once',  # Override to match your needs,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "campaign"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    # [START howto_campaign_manager_insert_report_operator]
    create_report = GoogleCampaignManagerInsertReportOperator(
        profile_id=PROFILE_ID, report=REPORT, task_id="create_report"
    )
    report_id = create_report.output["report_id"]
    # [END howto_campaign_manager_insert_report_operator]

    # [START howto_campaign_manager_run_report_operator]
    run_report = GoogleCampaignManagerRunReportOperator(
        profile_id=PROFILE_ID, report_id=report_id, task_id="run_report"
    )
    file_id = run_report.output["file_id"]
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
        bucket_name=BUCKET_NAME,
    )
    # [END howto_campaign_manager_get_report_operator]

    # [START howto_campaign_manager_delete_report_operator]
    delete_report = GoogleCampaignManagerDeleteReportOperator(
        profile_id=PROFILE_ID,
        report_name=REPORT_NAME,
        task_id="delete_report",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_campaign_manager_delete_report_operator]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

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

    (
        # TEST SETUP
        create_bucket
        >> create_report
        # TEST BODY
        >> run_report
        >> wait_for_report
        >> get_report
        >> insert_conversion
        >> update_conversion
        # TEST TEARDOWN
        >> delete_report
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
