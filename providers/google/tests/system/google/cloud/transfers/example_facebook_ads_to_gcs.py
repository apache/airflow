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
Example Airflow DAG that shows how to use FacebookAdsReportToGcsOperator.
"""

from __future__ import annotations

import os
from datetime import datetime

from facebook_business.adobjects.adsinsights import AdsInsights

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.facebook_ads_to_gcs import FacebookAdsReportToGcsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from airflow.providers.google.tests.system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "facebook_ads_to_gcs"

DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
TABLE_NAME = "airflow_test_datatable"
GCS_OBJ_PATH = "Temp/this_is_my_report_csv.csv"

FIELDS = [
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.clicks,
    AdsInsights.Field.impressions,
]
PARAMETERS = {"level": "ad", "date_preset": "yesterday"}

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "facebook_ads_to_gcs"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_NAME,
    )

    create_table = BigQueryCreateTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        table_resource={
            "schema": {
                "fields": [
                    {"name": "campaign_name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "campaign_id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "ad_id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "clicks", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "impressions", "type": "STRING", "mode": "NULLABLE"},
                ],
            },
        },
    )

    # [START howto_operator_facebook_ads_to_gcs]
    run_operator = FacebookAdsReportToGcsOperator(
        task_id="run_fetch_data",
        owner="airflow",
        bucket_name=BUCKET_NAME,
        parameters=PARAMETERS,
        fields=FIELDS,
        object_name=GCS_OBJ_PATH,
    )
    # [END howto_operator_facebook_ads_to_gcs]

    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bq_example",
        bucket=BUCKET_NAME,
        source_objects=[GCS_OBJ_PATH],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition="WRITE_TRUNCATE",
    )

    read_data_from_gcs_many_chunks = BigQueryInsertJobOperator(
        task_id="read_data_from_gcs_many_chunks",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`",
                "useLegacySql": False,
            }
        },
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    chain(
        # TEST SETUP
        create_bucket,
        create_dataset,
        create_table,
        # TEST BODY
        run_operator,
        load_csv,
        read_data_from_gcs_many_chunks,
        # TEST TEARDOWN
        delete_bucket,
        delete_dataset,
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
