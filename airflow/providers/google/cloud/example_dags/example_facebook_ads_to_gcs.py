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

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.facebook_ads_to_gcs import FacebookAdsReportToGcsOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# [START howto_GCS_env_variables]
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "free-tier-1997")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "airflow_bucket_fb")
GCS_OBJ_PATH = os.environ.get("GCS_OBJ_PATH", "Temp/this_is_my_report_csv.csv")
GCS_CONN_ID = os.environ.get("GCS_CONN_ID", "google_cloud_default")
DATASET_NAME = os.environ.get("DATASET_NAME", "airflow_test_dataset")
TABLE_NAME = os.environ.get("FB_TABLE_NAME", "airflow_test_datatable")
# [END howto_GCS_env_variables]

# [START howto_FB_ADS_variables]
FIELDS = [
    AdsInsights.Field.campaign_name,
    AdsInsights.Field.campaign_id,
    AdsInsights.Field.ad_id,
    AdsInsights.Field.clicks,
    AdsInsights.Field.impressions,
]
PARAMETERS = {'level': 'ad', 'date_preset': 'yesterday'}
# [END howto_FB_ADS_variables]

with models.DAG(
    "example_facebook_ads_to_gcs",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:

    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=GCS_BUCKET,
        project_id=GCP_PROJECT_ID,
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_NAME,
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        schema_fields=[
            {'name': 'campaign_name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'campaign_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'ad_id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'clicks', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'impressions', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
    )

    # [START howto_operator_facebook_ads_to_gcs]
    run_operator = FacebookAdsReportToGcsOperator(
        task_id='run_fetch_data',
        owner='airflow',
        bucket_name=GCS_BUCKET,
        parameters=PARAMETERS,
        fields=FIELDS,
        gcp_conn_id=GCS_CONN_ID,
        object_name=GCS_OBJ_PATH,
    )
    # [END howto_operator_facebook_ads_to_gcs]

    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bq_example',
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJ_PATH],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition='WRITE_TRUNCATE',
    )

    read_data_from_gcs_many_chunks = BigQueryInsertJobOperator(
        task_id="read_data_from_gcs_many_chunks",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`",
                "useLegacySql": False,
            }
        },
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=GCS_BUCKET,
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        project_id=GCP_PROJECT_ID,
        dataset_id=DATASET_NAME,
        delete_contents=True,
    )

    chain(
        create_bucket,
        create_dataset,
        create_table,
        run_operator,
        load_csv,
        read_data_from_gcs_many_chunks,
        delete_bucket,
        delete_dataset,
    )
