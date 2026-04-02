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
Example Airflow DAG for Google Vertex AI Feature Store operations.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from google.cloud.aiplatform_v1beta1 import FeatureOnlineStore, FeatureView, FeatureViewDataKey

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.feature_store import (
    CreateFeatureOnlineStoreOperator,
    CreateFeatureViewOperator,
    DeleteFeatureOnlineStoreOperator,
    DeleteFeatureViewOperator,
    FetchFeatureValuesOperator,
    GetFeatureOnlineStoreOperator,
    GetFeatureViewSyncOperator,
    SyncFeatureViewOperator,
)
from airflow.providers.google.cloud.sensors.vertex_ai.feature_store import FeatureViewSyncSensor

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
DAG_ID = "vertex_ai_feature_store_dag"
REGION = "us-central1"

BQ_LOCATION = "US"
BQ_DATASET_ID = "bq_ds_featurestore_demo"
BQ_VIEW_ID = "product_features_view"
BQ_VIEW_FQN = f"{PROJECT_ID}.{BQ_DATASET_ID}.{BQ_VIEW_ID}"

# Please take into consideration that max ID length is 60 symbols
FEATURE_ONLINE_STORE_ID = f"{ENV_ID}_fo_id".replace("-", "_")
FEATURE_VIEW_ID = "feature_view_product"
FEATURE_VIEW_DATA_KEY = {"key": "28098"}

FEATURE_EXTRACT_QUERY = """
       WITH
        product_order_agg AS (
          SELECT cast(product_id as string) as entity_id,
            countif(status in ("Shipped", "Complete")) as good_order_count,
            countif(status in ("Returned", "Cancelled")) as bad_order_count
          FROM `bigquery-public-data.thelook_ecommerce.order_items`
          WHERE
            timestamp_trunc(created_at, day) >= timestamp_trunc(timestamp_sub(CURRENT_TIMESTAMP(), interval 30 day), day) and
            timestamp_trunc(created_at, day) < timestamp_trunc(CURRENT_TIMESTAMP(), day)
          group by 1
          order by entity_id),
        product_basic AS (
          SELECT cast(id as string) AS entity_id,
            lower(name) as name,
            lower(category) as category,
            lower(brand) as brand,
            cost,
            retail_price
          FROM   bigquery-public-data.thelook_ecommerce.products)
       SELECT *, current_timestamp() as feature_timestamp
       FROM product_basic
       LEFT OUTER JOIN product_order_agg
       USING (entity_id)
       """


with DAG(
    dag_id=DAG_ID,
    description="Sample DAG with Vertex AI Feature Store operations.",
    schedule="@once",
    start_date=datetime(2025, 6, 1),
    catchup=False,
    tags=["example", "vertex_ai", "feature_store"],
) as dag:
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=BQ_DATASET_ID,
        project_id=PROJECT_ID,
        location=BQ_LOCATION,
    )

    create_bq_table_view = BigQueryCreateTableOperator(
        task_id="create_bq_table_view",
        project_id=PROJECT_ID,
        location=BQ_LOCATION,
        dataset_id=BQ_DATASET_ID,
        table_id=BQ_VIEW_ID,
        table_resource={
            "view": {
                "query": FEATURE_EXTRACT_QUERY,
                "useLegacySql": False,
            }
        },
    )
    # [START how_to_cloud_vertex_ai_create_feature_online_store_operator]
    create_feature_online_store = CreateFeatureOnlineStoreOperator(
        task_id="create_feature_online_store",
        project_id=PROJECT_ID,
        location=REGION,
        feature_online_store_id=FEATURE_ONLINE_STORE_ID,
        feature_online_store=FeatureOnlineStore(optimized=FeatureOnlineStore.Optimized()),
    )
    # [END how_to_cloud_vertex_ai_create_feature_online_store_operator]

    # [START how_to_cloud_vertex_ai_create_feature_view_store_operator]
    create_feature_view = CreateFeatureViewOperator(
        task_id="create_feature_view",
        project_id=PROJECT_ID,
        location=REGION,
        feature_online_store_id=FEATURE_ONLINE_STORE_ID,
        feature_view_id=FEATURE_VIEW_ID,
        feature_view=FeatureView(
            big_query_source=FeatureView.BigQuerySource(
                uri=f"bq://{BQ_VIEW_FQN}",
                entity_id_columns=["entity_id"],
            ),
            sync_config=FeatureView.SyncConfig(cron="TZ=America/Los_Angeles 56 * * * *"),
        ),
    )
    # [END how_to_cloud_vertex_ai_create_feature_view_store_operator]

    # [START how_to_cloud_vertex_ai_get_feature_online_store_operator]
    get_feature_online_store = GetFeatureOnlineStoreOperator(
        task_id="get_feature_online_store",
        project_id=PROJECT_ID,
        location=REGION,
        feature_online_store_id=FEATURE_ONLINE_STORE_ID,
    )
    # [END how_to_cloud_vertex_ai_get_feature_online_store_operator]

    # [START how_to_cloud_vertex_ai_feature_store_sync_feature_view_operator]
    sync_task = SyncFeatureViewOperator(
        task_id="sync_task",
        project_id=PROJECT_ID,
        location=REGION,
        feature_online_store_id=FEATURE_ONLINE_STORE_ID,
        feature_view_id=FEATURE_VIEW_ID,
    )
    # [END how_to_cloud_vertex_ai_feature_store_sync_feature_view_operator]

    # [START how_to_cloud_vertex_ai_feature_store_feature_view_sync_sensor]
    wait_for_sync = FeatureViewSyncSensor(
        task_id="wait_for_sync",
        location=REGION,
        feature_view_sync_name="{{ task_instance.xcom_pull(task_ids='sync_task', key='return_value')}}",
        poke_interval=60,  # Check every minute
        timeout=1200,  # Timeout after 20 minutes
        mode="reschedule",
    )
    # [END how_to_cloud_vertex_ai_feature_store_feature_view_sync_sensor]

    # [START how_to_cloud_vertex_ai_feature_store_get_feature_view_sync_operator]
    get_task = GetFeatureViewSyncOperator(
        task_id="get_task",
        location=REGION,
        feature_view_sync_name="{{ task_instance.xcom_pull(task_ids='sync_task', key='return_value')}}",
    )
    # [END how_to_cloud_vertex_ai_feature_store_get_feature_view_sync_operator]

    # [START how_to_cloud_vertex_ai_fetch_feature_values_operator]
    fetch_feature_data = FetchFeatureValuesOperator(
        task_id="fetch_feature_data",
        project_id=PROJECT_ID,
        location=REGION,
        feature_online_store_id=FEATURE_ONLINE_STORE_ID,
        feature_view_id=FEATURE_VIEW_ID,
        data_key=FeatureViewDataKey(FEATURE_VIEW_DATA_KEY),
        retries=3,
        retry_delay=timedelta(minutes=3),
    )
    # [END how_to_cloud_vertex_ai_fetch_feature_values_operator]

    # [START how_to_cloud_vertex_ai_delete_feature_view_operator]
    delete_feature_view = DeleteFeatureViewOperator(
        task_id="delete_feature_view",
        project_id=PROJECT_ID,
        location=REGION,
        feature_online_store_id=FEATURE_ONLINE_STORE_ID,
        feature_view_id=FEATURE_VIEW_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_feature_view_operator]

    # [START how_to_cloud_vertex_ai_delete_feature_online_store_operator]
    delete_feature_online_store = DeleteFeatureOnlineStoreOperator(
        task_id="delete_feature_online_store",
        project_id=PROJECT_ID,
        location=REGION,
        feature_online_store_id=FEATURE_ONLINE_STORE_ID,
    )
    # [END how_to_cloud_vertex_ai_delete_feature_online_store_operator]

    delete_bq_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_bq_dataset",
        dataset_id=BQ_DATASET_ID,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # TEST SETUP
    (
        create_bq_dataset
        >> create_bq_table_view
        # TEST BODY
        >> create_feature_online_store
        >> get_feature_online_store
        >> create_feature_view
        >> sync_task
        >> wait_for_sync
        >> get_task
        >> fetch_feature_data
        # TEST TEARDOWN
        >> delete_feature_view
        >> delete_feature_online_store
        >> delete_bq_dataset
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
