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
Example Airflow DAG that shows how to use Bid Manager API from GoogleDisplayVideo360.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import cast

from airflow.models.dag import DAG
from airflow.models.xcom_arg import XComArg
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.marketing_platform.operators.bid_manager import (
    GoogleBidManagerCreateQueryOperator,
    GoogleBidManagerDeleteQueryOperator,
    GoogleBidManagerDownloadReportOperator,
    GoogleBidManagerRunQueryOperator,
)
from airflow.providers.google.marketing_platform.sensors.bid_manager import (
    GoogleBidManagerRunQuerySensor,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]
from google.cloud.exceptions import NotFound

try:
    from airflow.sdk import task
except ImportError:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.providers.google.cloud.hooks.secret_manager import (
    GoogleCloudSecretManagerHook,
)

from system.google.gcp_api_client_helpers import create_airflow_connection, delete_airflow_connection

DAG_ID = "bid_manager"
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
CONNECTION_TYPE = "google_cloud_platform"
CONN_ID = "google_display_video_default"
DISPLAY_VIDEO_SERVICE_ACCOUNT_KEY = "google_display_video_service_account_key"
IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))


PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
ADVERTISER_ID = os.environ.get("GMP_ADVERTISER_ID", "1234567")

REPORT = {
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


def get_secret(secret_id: str) -> str:
    hook = GoogleCloudSecretManagerHook()
    if hook.secret_exists(secret_id=secret_id):
        return hook.access_secret(secret_id=secret_id).payload.data.decode()
    raise NotFound(f"The secret {secret_id} not found")


with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "bid_manager"],
    schedule="@once",
) as dag:

    @task
    def get_display_video_service_account_key():
        return get_secret(secret_id=DISPLAY_VIDEO_SERVICE_ACCOUNT_KEY)

    get_display_video_service_account_key_task = get_display_video_service_account_key()

    @task
    def create_connection_display_video(connection_id: str, key) -> None:
        conn_extra_json = json.dumps(
            {
                "keyfile_dict": key,
                "project": PROJECT_ID,
                "scope": "https://www.googleapis.com/auth/display-video, https://www.googleapis.com/auth/cloud-platform, https://www.googleapis.com/auth/doubleclickbidmanager",
            }
        )
        create_airflow_connection(
            connection_id=connection_id,
            connection_conf={"conn_type": CONNECTION_TYPE, "extra": conn_extra_json},
            is_composer=IS_COMPOSER,
        )

    create_connection_display_video_task = create_connection_display_video(
        connection_id=CONN_ID, key=get_display_video_service_account_key_task
    )

    @task(task_id="delete_connection_task")
    def delete_connection_display_video(connection_id: str) -> None:
        delete_airflow_connection(connection_id=connection_id, is_composer=IS_COMPOSER)

    delete_connection_task = delete_connection_display_video(connection_id=CONN_ID)

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID, gcp_conn_id=CONN_ID
    )
    # [START howto_google_bid_manager_create_query_operator]
    create_query = GoogleBidManagerCreateQueryOperator(
        body=REPORT, task_id="create_query", gcp_conn_id=CONN_ID
    )

    query_id = cast("str", XComArg(create_query, key="query_id"))
    # [END howto_google_bid_manager_create_query_operator]

    # [START howto_google_bid_manager_run_query_report_operator]
    run_query = GoogleBidManagerRunQueryOperator(
        query_id=query_id, parameters=PARAMETERS, task_id="run_report", gcp_conn_id=CONN_ID
    )

    query_id = cast("str", XComArg(run_query, key="query_id"))
    report_id = cast("str", XComArg(run_query, key="report_id"))
    # [END howto_google_bid_manager_run_query_report_operator]

    # [START howto_google_bid_manager_wait_run_query_sensor]
    wait_for_query = GoogleBidManagerRunQuerySensor(
        task_id="wait_for_query",
        query_id=query_id,
        report_id=report_id,
        gcp_conn_id=CONN_ID,
    )
    # [END howto_google_bid_manager_wait_run_query_sensor]

    # [START howto_google_bid_manager_get_report_operator]
    get_report = GoogleBidManagerDownloadReportOperator(
        query_id=query_id,
        report_id=report_id,
        task_id="get_report",
        bucket_name=BUCKET_NAME,
        report_name="test1.csv",
        gcp_conn_id=CONN_ID,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        gcp_conn_id=CONN_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END howto_google_bid_manager_get_report_operator]

    # [START howto_google_bid_manager_delete_query_operator]
    delete_query = GoogleBidManagerDeleteQueryOperator(
        query_id=query_id, task_id="delete_query", trigger_rule=TriggerRule.ALL_DONE, gcp_conn_id=CONN_ID
    )
    # [END howto_google_bid_manager_delete_query_operator]

    (
        get_display_video_service_account_key_task
        >> create_connection_display_video_task  # type: ignore
        >> create_bucket
        >> create_query
        >> run_query
        >> wait_for_query
        >> get_report
        >> delete_query
        >> delete_bucket
        >> delete_connection_task
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
