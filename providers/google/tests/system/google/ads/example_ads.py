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
Example Airflow DAG that shows how to use GoogleAdsToGcsOperator.

In order to run this test, make sure you followed steps:
1. In your GCP project create a service account that will be used to operate on Google Ads.
The name should be in format `google-ads-service-account@{PROJECT_ID}.iam.gserviceaccount.com`
2. Generate a key for this service account and store it in the Secret Manager
under the name `google_ads_service_account_key`.
3. Give this service account Editor permissions.
4. Make sure Google Ads API is enabled in your GCP project.
5. Login to https://ads.google.com
6. In the Admin section go to Access and Security and give your GCP service account Admin permissions.
7. Store values of your developer token and client ID to Secret Manager under names `google_ads_client_id`
and `google_ads_developer_token`.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

from airflow.providers.google.cloud.exceptions import NotFound

try:
    from airflow.sdk import task
except ImportError:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.dag import DAG
from airflow.providers.google.ads.operators.ads import GoogleAdsListAccountsOperator
from airflow.providers.google.ads.transfers.ads_to_gcs import GoogleAdsToGcsOperator
from airflow.providers.google.cloud.hooks.secret_manager import GoogleCloudSecretManagerHook
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google.gcp_api_client_helpers import (
    create_airflow_connection,
    delete_airflow_connection,
)

# [START howto_google_ads_env_variables]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
API_VERSION = "v21"

DAG_ID = "google_ads"

IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))

GOOGLE_ADS_CLIENT_ID = "google_ads_client_id"
GOOGLE_ADS_SERVICE_ACCOUNT_KEY = "google_ads_service_account_key"
GOOGLE_ADS_DEVELOPER_TOKEN = "google_ads_developer_token"

BUCKET_NAME = f"bucket_ads_{ENV_ID}"
GCS_OBJ_PATH = f"gs://{BUCKET_NAME}/google-ads-api-results.csv"
GCS_ACCOUNTS_CSV = "accounts.csv"
QUERY = """
    SELECT
        segments.date,
        customer.id,
        campaign.id,
        ad_group.id,
        ad_group_ad.ad.id,
        metrics.impressions,
        metrics.clicks,
        metrics.conversions,
        metrics.all_conversions,
        metrics.cost_micros
    FROM
        ad_group_ad
    WHERE
        segments.date >= '2020-02-01'
        AND segments.date <= '2020-02-29'
    """
CONNECTION_GLOUD_ID = f"connection_cloud_{DAG_ID}_{ENV_ID}"
CONNECTION_ADS_ID = "google_ads_default"
CONNECTION_TYPE = "google_cloud_platform"

FIELDS_TO_EXTRACT = [
    "segments.date.value",
    "customer.id.value",
    "campaign.id.value",
    "ad_group.id.value",
    "ad_group_ad.ad.id.value",
    "metrics.impressions.value",
    "metrics.clicks.value",
    "metrics.conversions.value",
    "metrics.all_conversions.value",
    "metrics.cost_micros.value",
]
# [END howto_google_ads_env_variables]

log = logging.getLogger(__name__)


def get_secret(secret_id: str) -> str:
    hook = GoogleCloudSecretManagerHook()
    if hook.secret_exists(secret_id=secret_id):
        return hook.access_secret(secret_id=secret_id).payload.data.decode()
    raise NotFound("The secret '%s' not found", secret_id)


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "ads"],
    render_template_as_native_obj=True,
) as dag:

    @task
    def get_google_ads_client_id():
        return get_secret(secret_id=GOOGLE_ADS_CLIENT_ID).strip()

    get_google_ads_client_id_task = get_google_ads_client_id()

    @task
    def get_google_ads_service_account_key():
        return get_secret(secret_id=GOOGLE_ADS_SERVICE_ACCOUNT_KEY)

    get_google_ads_service_account_key_task = get_google_ads_service_account_key()

    @task
    def get_google_ads_developer_token():
        return get_secret(secret_id=GOOGLE_ADS_DEVELOPER_TOKEN).strip()

    get_google_ads_developer_token_task = get_google_ads_developer_token()

    @task
    def create_connection_gcloud_for_ads(connection_id: str, key) -> None:
        conn_extra_json = json.dumps(
            {
                "keyfile_dict": key,
                "project": PROJECT_ID,
                "scope": "https://www.googleapis.com/auth/adwords, https://www.googleapis.com/auth/cloud-platform",
            }
        )
        create_airflow_connection(
            connection_id=connection_id,
            connection_conf={"conn_type": CONNECTION_TYPE, "extra": conn_extra_json},
            is_composer=IS_COMPOSER,
        )

    create_connection_gcloud_for_ads = create_connection_gcloud_for_ads(
        connection_id=CONNECTION_GLOUD_ID, key=get_google_ads_service_account_key_task
    )

    @task
    def create_connection_ads(connection_id: str, token) -> None:
        conn_extra_json = json.dumps(
            {
                "google_ads_client": {
                    "developer_token": token,
                    # this parameter is required to be not None, but the actual content will be overwritten, so can be some dummy string
                    "json_key_file_path": "some_string",
                    "impersonated_email": f"google-ads-service-account@{PROJECT_ID}.iam.gserviceaccount.com",
                    "use_proto_plus": False,
                },
                "project": PROJECT_ID,
                "scope": "https://www.googleapis.com/auth/adwords, https://www.googleapis.com/auth/cloud-platform",
            }
        )
        create_airflow_connection(
            connection_id=connection_id,
            connection_conf={"conn_type": CONNECTION_TYPE, "extra": conn_extra_json},
            is_composer=IS_COMPOSER,
        )

    create_connection_ads_task = create_connection_ads(
        connection_id=CONNECTION_ADS_ID, token=get_google_ads_developer_token_task
    )

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
        gcp_conn_id=CONNECTION_GLOUD_ID,
    )

    # [START howto_google_ads_to_gcs_operator]
    run_operator = GoogleAdsToGcsOperator(
        client_ids=[get_google_ads_client_id_task],
        query=QUERY,
        attributes=FIELDS_TO_EXTRACT,
        obj=GCS_OBJ_PATH,
        bucket=BUCKET_NAME,
        task_id="run_operator",
        api_version=API_VERSION,
        gcp_conn_id=CONNECTION_GLOUD_ID,
    )
    # [END howto_google_ads_to_gcs_operator]

    # [START howto_ads_list_accounts_operator]
    list_accounts = GoogleAdsListAccountsOperator(
        task_id="list_accounts",
        bucket=BUCKET_NAME,
        object_name=GCS_ACCOUNTS_CSV,
        api_version=API_VERSION,
        gcp_conn_id=CONNECTION_GLOUD_ID,
    )
    # [END howto_ads_list_accounts_operator]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        gcp_conn_id=CONNECTION_GLOUD_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    @task(task_id="delete_connection_gloud")
    def delete_connection_gloud(connection_id: str) -> None:
        delete_airflow_connection(connection_id=connection_id, is_composer=IS_COMPOSER)

    delete_connection_gloud_task = delete_connection_gloud(connection_id=CONNECTION_GLOUD_ID)

    @task(task_id="delete_connection_ads")
    def delete_connection_ads(connection_id: str) -> None:
        delete_airflow_connection(connection_id=connection_id, is_composer=IS_COMPOSER)

    delete_connection_ads_task = delete_connection_ads(connection_id=CONNECTION_ADS_ID)

    (
        # TEST SETUP
        [
            get_google_ads_client_id_task,
            get_google_ads_service_account_key_task,
            get_google_ads_developer_token_task,
        ]
        >> create_connection_gcloud_for_ads  # type: ignore
        >> create_connection_ads_task
        >> create_bucket
        # TEST BODY
        >> run_operator
        >> list_accounts
        # TEST TEARDOWN
        >> delete_bucket
        >> [delete_connection_gloud_task, delete_connection_ads_task]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
