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
from datetime import datetime

from airflow import models
from airflow.providers.google.ads.operators.ads import GoogleAdsListAccountsOperator
from airflow.providers.google.ads.transfers.ads_to_gcs import GoogleAdsToGcsOperator
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.utils.trigger_rule import TriggerRule

# [START howto_google_ads_env_variables]
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "example_google_ads"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
CLIENT_IDS = ["1111111111", "2222222222"]
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
    DAG_ID,
    schedule_interval='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "ads"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID
    )

    # [START howto_google_ads_to_gcs_operator]
    run_operator = GoogleAdsToGcsOperator(
        client_ids=CLIENT_IDS,
        query=QUERY,
        attributes=FIELDS_TO_EXTRACT,
        obj=GCS_OBJ_PATH,
        bucket=BUCKET_NAME,
        task_id="run_operator",
    )
    # [END howto_google_ads_to_gcs_operator]

    # [START howto_ads_list_accounts_operator]
    list_accounts = GoogleAdsListAccountsOperator(
        task_id="list_accounts", bucket=BUCKET_NAME, object_name=GCS_ACCOUNTS_CSV
    )
    # [END howto_ads_list_accounts_operator]

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        # TEST BODY
        >> run_operator
        >> list_accounts
        # TEST TEARDOWN
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
