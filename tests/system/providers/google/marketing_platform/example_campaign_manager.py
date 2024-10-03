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

This system test relies on a service account with proper settings in Campaign Manager 360.
That's why before running this system test locally, make sure your service account corresponds all the
secrets that the DAG reads. If your service account doesn't have access but you know another one which has
then simply specify it in the environment variable CM360_IMPERSONATION_CHAIN.
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from datetime import datetime
from typing import cast

from google.api_core.exceptions import NotFound

from airflow.decorators import task
from airflow.models import Connection
from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.hooks.secret_manager import GoogleCloudSecretManagerHook
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
from airflow.settings import Session
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
CM360_IMPERSONATION_CHAIN = os.environ.get("IMPERSONATION_CHAIN", None)

DAG_ID = "campaign_manager"

SECRET_ACCOUNT_ID = "cm360_account_id"
SECRET_DCLID = "cm360_dclid"
SECRET_ENCRYPTION_ENTITY_ID = "cm360_encryption_entity_id"
SECRET_FLOODLIGHT_ACTIVITY_ID = "cm360_floodlight_activity_id"
SECRET_FLOODLIGHT_CONFIGURATION_ID = "cm360_floodlight_configuration_id"
SECRET_USER_PROFILE_ID = "cm360_user_profile_id"

ACCOUNT_ID = "{{ task_instance.xcom_pull('get_account_id') }}"
DCLID = "{{ task_instance.xcom_pull('get_dclid') }}"
ENCRYPTION_ENTITY_ID = "{{ task_instance.xcom_pull('get_encryption_entity_id') }}"
FLOODLIGHT_ACTIVITY_ID = "{{ task_instance.xcom_pull('get_floodlight_activity_id') }}"
FLOODLIGHT_CONFIGURATION_ID = "{{ task_instance.xcom_pull('get_floodlight_configuration_id') }}"
USER_PROFILE_ID = "{{ task_instance.xcom_pull('get_user_profile_id') }}"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
REPORT_NAME = f"report_{DAG_ID}_{ENV_ID}"
FILE_NAME = f"file_{DAG_ID}_{ENV_ID}"
FORMAT = "CSV"
CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"


# For more information, please check
# https://developers.google.com/doubleclick-advertisers/rest/v4/reports#type
REPORT = {
    "kind": "dfareporting#report",
    "type": "STANDARD",
    "name": REPORT_NAME,
    "fileName": FILE_NAME,
    "accountId": ACCOUNT_ID,
    "format": FORMAT,
    "criteria": {
        "dateRange": {
            "kind": "dfareporting#dateRange",
            "relativeDateRange": "LAST_365_DAYS",
        },
        "dimensions": [{"kind": "dfareporting#sortedDimension", "name": "campaign"}],
        "metricNames": ["activeViewImpressionDistributionViewable"],
    },
}

# For more information, please check
# https://developers.google.com/doubleclick-advertisers/rest/v4/Conversion
CONVERSION = {
    "kind": "dfareporting#conversion",
    "floodlightActivityId": FLOODLIGHT_ACTIVITY_ID,
    "floodlightConfigurationId": FLOODLIGHT_CONFIGURATION_ID,
    "dclid": DCLID,
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
    "dclid": DCLID,
    "ordinal": "0",
    "quantity": 42,
    "value": 123.4,
}


log = logging.getLogger(__name__)


def get_secret(secret_id: str) -> str:
    hook = GoogleCloudSecretManagerHook()
    if hook.secret_exists(secret_id=secret_id):
        return hook.access_secret(secret_id=secret_id).payload.data.decode().strip()
    raise NotFound("The secret '%s' not found", secret_id)


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "campaign"],
) as dag:

    @task
    def create_connection(connection_id: str) -> None:
        connection = Connection(
            conn_id=connection_id,
            conn_type="google_cloud_platform",
        )
        extras = {
            "scope": "https://www.googleapis.com/auth/cloud-platform,"
            "https://www.googleapis.com/auth/ddmconversions,"
            "https://www.googleapis.com/auth/dfareporting",
        }
        if CM360_IMPERSONATION_CHAIN:
            extras["impersonation_chain"] = CM360_IMPERSONATION_CHAIN

        conn_extra_json = json.dumps(extras)
        connection.set_extra(conn_extra_json)

        session = Session()
        log.info("Removing connection %s if it exists", connection_id)
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()

        session.add(connection)
        session.commit()
        log.info("Connection %s created", CONNECTION_ID)

    @task
    def get_account_id():
        return get_secret(secret_id=SECRET_ACCOUNT_ID)

    @task
    def get_dclid():
        return get_secret(secret_id=SECRET_DCLID)

    @task
    def get_encryption_entity_id():
        return get_secret(secret_id=SECRET_ENCRYPTION_ENTITY_ID)

    @task
    def get_floodlight_activity_id():
        return get_secret(secret_id=SECRET_FLOODLIGHT_ACTIVITY_ID)

    @task
    def get_floodlight_configuration_id():
        return get_secret(secret_id=SECRET_FLOODLIGHT_CONFIGURATION_ID)

    @task
    def get_user_profile_id():
        return get_secret(secret_id=SECRET_USER_PROFILE_ID)

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    # [START howto_campaign_manager_insert_report_operator]
    create_report = GoogleCampaignManagerInsertReportOperator(
        profile_id=USER_PROFILE_ID,
        report=REPORT,
        task_id="create_report",
        gcp_conn_id=CONNECTION_ID,
    )
    report_id = cast(str, XComArg(create_report, key="report_id"))
    # [END howto_campaign_manager_insert_report_operator]

    # [START howto_campaign_manager_run_report_operator]
    run_report = GoogleCampaignManagerRunReportOperator(
        profile_id=USER_PROFILE_ID,
        report_id=report_id,
        task_id="run_report",
        gcp_conn_id=CONNECTION_ID,
    )
    file_id = cast(str, XComArg(run_report, key="file_id"))
    # [END howto_campaign_manager_run_report_operator]

    # [START howto_campaign_manager_wait_for_operation]
    wait_for_report = GoogleCampaignManagerReportSensor(
        task_id="wait_for_report",
        profile_id=USER_PROFILE_ID,
        report_id=report_id,
        file_id=file_id,
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_campaign_manager_wait_for_operation]

    # [START howto_campaign_manager_get_report_operator]
    report_name = f"reports/report_{str(uuid.uuid1())}"
    get_report = GoogleCampaignManagerDownloadReportOperator(
        task_id="get_report",
        profile_id=USER_PROFILE_ID,
        report_id=report_id,
        file_id=file_id,
        report_name=report_name,
        bucket_name=BUCKET_NAME,
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_campaign_manager_get_report_operator]

    # [START howto_campaign_manager_delete_report_operator]
    delete_report = GoogleCampaignManagerDeleteReportOperator(
        profile_id=USER_PROFILE_ID,
        report_name=REPORT_NAME,
        task_id="delete_report",
        trigger_rule=TriggerRule.ALL_DONE,
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_campaign_manager_delete_report_operator]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    # [START howto_campaign_manager_insert_conversions]
    insert_conversion = GoogleCampaignManagerBatchInsertConversionsOperator(
        task_id="insert_conversion",
        profile_id=USER_PROFILE_ID,
        conversions=[CONVERSION],
        encryption_source="AD_SERVING",
        encryption_entity_type="DCM_ADVERTISER",
        encryption_entity_id=ENCRYPTION_ENTITY_ID,  # type: ignore[arg-type]
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_campaign_manager_insert_conversions]

    # [START howto_campaign_manager_update_conversions]
    update_conversion = GoogleCampaignManagerBatchUpdateConversionsOperator(
        task_id="update_conversion",
        profile_id=USER_PROFILE_ID,
        conversions=[CONVERSION_UPDATE],
        encryption_source="AD_SERVING",
        encryption_entity_type="DCM_ADVERTISER",
        encryption_entity_id=ENCRYPTION_ENTITY_ID,  # type: ignore[arg-type]
        max_failed_updates=1,
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_campaign_manager_update_conversions]

    @task(task_id="delete_connection")
    def delete_connection(connection_id: str) -> None:
        session = Session()
        log.info("Removing connection %s", connection_id)
        query = session.query(Connection).filter(Connection.conn_id == connection_id)
        query.delete()
        session.commit()

    (
        # TEST SETUP
        create_connection(connection_id=CONNECTION_ID)
        >> [
            get_account_id(),
            get_dclid(),
            get_encryption_entity_id(),
            get_floodlight_activity_id(),
            get_floodlight_configuration_id(),
            get_user_profile_id(),
        ]
        >> create_bucket
        # TEST BODY
        >> create_report
        >> run_report
        >> wait_for_report
        >> get_report
        >> insert_conversion
        >> update_conversion
        # TEST TEARDOWN
        >> delete_report
        >> delete_bucket
        >> delete_connection(connection_id=CONNECTION_ID)
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
