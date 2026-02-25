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
Example Airflow DAG that shows how to use Google Analytics (GA4) Admin Operators.

This DAG relies on the following OS environment variables

* GA_ACCOUNT_ID - Google Analytics account id.
* GA_GOOGLE_ADS_PROPERTY_ID - Google Analytics property's id associated with Google Ads Link.

In order to run this test, make sure you followed steps:
1. Login to https://analytics.google.com
2. In the settings section create an account and save its ID in the Google Cloud Secret Manager with id
saved in the constant GOOGLE_ANALYTICS_ACCOUNT_SECRET_ID.
3. In the settings section go to the Property access management page and add your service account email with
Editor permissions. This service account should be created on behalf of the account from the step 1.
4. Make sure Google Analytics Admin API is enabled in your GCP project.
5. Create Google Ads account and link it to your Google Analytics account in the GA admin panel.
6. Associate the Google Ads account with a property, and save this property's id in the Google Cloud Secret
Manager with id saved in the constant GOOGLE_ADS_PROPERTY_ID.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Any

from google.analytics import admin_v1beta as google_analytics

try:
    from airflow.sdk import task
except ImportError:
    # Airflow 2 path
    from airflow.decorators import task  # type: ignore[attr-defined,no-redef]
from airflow.models.dag import DAG
from airflow.providers.google.common.utils.get_secret import get_secret
from airflow.providers.google.marketing_platform.operators.analytics_admin import (
    GoogleAnalyticsAdminCreateDataStreamOperator,
    GoogleAnalyticsAdminCreatePropertyOperator,
    GoogleAnalyticsAdminDeleteDataStreamOperator,
    GoogleAnalyticsAdminDeletePropertyOperator,
    GoogleAnalyticsAdminGetGoogleAdsLinkOperator,
    GoogleAnalyticsAdminListAccountsOperator,
    GoogleAnalyticsAdminListGoogleAdsLinksOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google.gcp_api_client_helpers import create_airflow_connection, delete_airflow_connection

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "google_analytics_admin"
IS_COMPOSER = bool(os.environ.get("COMPOSER_ENVIRONMENT", ""))

CONNECTION_ID = f"connection_{DAG_ID}_{ENV_ID}"
GOOGLE_ANALYTICS_ACCOUNT_SECRET_ID = "google_analytics_account_id"
GOOGLE_ADS_PROPERTY_SECRET_ID = "google_ads_property_id"
PROPERTY_ID = "{{ task_instance.xcom_pull('create_property')['name'].split('/')[-1] }}"
DATA_STREAM_ID = "{{ task_instance.xcom_pull('create_data_stream')['name'].split('/')[-1] }}"
GA_ADS_LINK_ID = "{{ task_instance.xcom_pull('list_google_ads_links')[0]['name'].split('/')[-1] }}"

log = logging.getLogger(__name__)


with DAG(
    DAG_ID,
    schedule="@once",  # Override to match your needs,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "analytics"],
) as dag:

    @task
    def create_connection(connection_id: str) -> None:
        conn_extra_json = json.dumps(
            {
                "scope": "https://www.googleapis.com/auth/analytics.edit,"
                "https://www.googleapis.com/auth/analytics.readonly",
            }
        )
        connection: dict[str, Any] = {"conn_type": "google_cloud_platform", "extra": conn_extra_json}
        create_airflow_connection(
            connection_id=connection_id,
            connection_conf=connection,
            is_composer=IS_COMPOSER,
        )

    create_connection_task = create_connection(connection_id=CONNECTION_ID)

    @task
    def get_google_analytics_account_id():
        return get_secret(secret_id=GOOGLE_ANALYTICS_ACCOUNT_SECRET_ID)

    get_google_analytics_account_id_task = get_google_analytics_account_id()

    @task
    def get_google_ads_property_id():
        return get_secret(secret_id=GOOGLE_ADS_PROPERTY_SECRET_ID)

    get_google_ads_property_id_task = get_google_ads_property_id()

    # [START howto_marketing_platform_list_accounts_operator]
    list_accounts = GoogleAnalyticsAdminListAccountsOperator(
        task_id="list_account",
        gcp_conn_id=CONNECTION_ID,
        show_deleted=True,
    )
    # [END howto_marketing_platform_list_accounts_operator]

    # [START howto_marketing_platform_create_property_operator]
    create_property = GoogleAnalyticsAdminCreatePropertyOperator(
        task_id="create_property",
        analytics_property={
            "parent": f"accounts/{get_google_analytics_account_id_task}",
            "display_name": "Test display name",
            "time_zone": "America/Los_Angeles",
        },
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_marketing_platform_create_property_operator]

    # [START howto_marketing_platform_create_data_stream_operator]
    create_data_stream = GoogleAnalyticsAdminCreateDataStreamOperator(
        task_id="create_data_stream",
        property_id=PROPERTY_ID,
        data_stream={
            "display_name": "Test data stream",
            "web_stream_data": {
                "default_uri": "www.example.com",
            },
            "type_": google_analytics.DataStream.DataStreamType.WEB_DATA_STREAM,
        },
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_marketing_platform_create_data_stream_operator]

    # [START howto_marketing_platform_delete_data_stream_operator]
    delete_data_stream = GoogleAnalyticsAdminDeleteDataStreamOperator(
        task_id="delete_datastream",
        property_id=PROPERTY_ID,
        data_stream_id=DATA_STREAM_ID,
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_marketing_platform_delete_data_stream_operator]

    # [START howto_marketing_platform_delete_property_operator]
    delete_property = GoogleAnalyticsAdminDeletePropertyOperator(
        task_id="delete_property",
        property_id=PROPERTY_ID,
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_marketing_platform_delete_property_operator]
    delete_property.trigger_rule = TriggerRule.ALL_DONE

    # [START howto_marketing_platform_list_google_ads_links]
    list_google_ads_links = GoogleAnalyticsAdminListGoogleAdsLinksOperator(
        task_id="list_google_ads_links",
        property_id=get_google_ads_property_id_task,
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_marketing_platform_list_google_ads_links]

    # [START howto_marketing_platform_get_google_ad_link]
    get_ad_link = GoogleAnalyticsAdminGetGoogleAdsLinkOperator(
        task_id="get_ad_link",
        property_id=get_google_ads_property_id_task,
        google_ads_link_id=GA_ADS_LINK_ID,
        gcp_conn_id=CONNECTION_ID,
    )
    # [END howto_marketing_platform_get_google_ad_link]

    @task(task_id="delete_connection")
    def delete_connection(connection_id: str) -> None:
        delete_airflow_connection(connection_id=connection_id, is_composer=IS_COMPOSER)

    delete_connection_task = delete_connection(connection_id=CONNECTION_ID)

    (
        # TEST SETUP
        [create_connection_task, get_google_analytics_account_id_task, get_google_ads_property_id_task]
        # TEST BODY
        >> list_accounts
        >> create_property
        >> create_data_stream
        >> delete_data_stream
        >> delete_property
        >> list_google_ads_links
        >> get_ad_link
        # TEST TEARDOWN
        >> delete_connection_task
    )
    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
