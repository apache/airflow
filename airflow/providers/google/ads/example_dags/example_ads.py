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
"""
import os

from airflow import models
from airflow.providers.google.ads.operators.ads import GoogleAdsListAccountsOperator
from airflow.providers.google.ads.transfers.ads_to_gcs import GoogleAdsToGcsOperator
from airflow.utils import dates

# [START howto_google_ads_env_variables]
CLIENT_IDS = ["1111111111", "2222222222"]
BUCKET = os.environ.get("GOOGLE_ADS_BUCKET", "gs://test-google-ads-bucket")
GCS_OBJ_PATH = "folder_name/google-ads-api-results.csv"
GCS_ACCOUNTS_CSV = "folder_name/accounts.csv"
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

with models.DAG(
    "example_google_ads",
    schedule_interval=None,  # Override to match your needs
    start_date=dates.days_ago(1),
) as dag:
    # [START howto_google_ads_to_gcs_operator]
    run_operator = GoogleAdsToGcsOperator(
        client_ids=CLIENT_IDS,
        query=QUERY,
        attributes=FIELDS_TO_EXTRACT,
        obj=GCS_OBJ_PATH,
        bucket=BUCKET,
        task_id="run_operator",
    )
    # [END howto_google_ads_to_gcs_operator]

    # [START howto_ads_list_accounts_operator]
    list_accounts = GoogleAdsListAccountsOperator(
        task_id="list_accounts", bucket=BUCKET, object_name=GCS_ACCOUNTS_CSV
    )
    # [END howto_ads_list_accounts_operator]
