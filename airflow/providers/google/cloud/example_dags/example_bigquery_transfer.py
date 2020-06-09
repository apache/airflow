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
Example Airflow DAG for Google BigQuery service.
"""
import os

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.dates import days_ago

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "test_dataset_transfer")
DATA_EXPORT_BUCKET_NAME = os.environ.get(
    "GCP_BIGQUERY_EXPORT_BUCKET_NAME", "test-bigquery-sample-data"
)
ORIGIN = "origin"
TARGET = "target"

default_args = {"start_date": days_ago(1)}

with models.DAG(
    "example_bigquery_transfer",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
    tags=["example"],
) as dag:
    copy_selected_data = BigQueryToBigQueryOperator(
        task_id="copy_selected_data",
        source_project_dataset_tables=f"{DATASET_NAME}.{ORIGIN}",
        destination_project_dataset_table=f"{DATASET_NAME}.{TARGET}",
    )

    bigquery_to_gcs = BigQueryToGCSOperator(
        task_id="bigquery_to_gcs",
        source_project_dataset_table=f"{DATASET_NAME}.{ORIGIN}",
        destination_cloud_storage_uris=[
            f"gs://{DATA_EXPORT_BUCKET_NAME}/export-bigquery.csv"
        ],
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME
    )

    for table in [ORIGIN, TARGET]:
        create_table = BigQueryCreateEmptyTableOperator(
            task_id=f"create_{table}_table",
            dataset_id=DATASET_NAME,
            table_id=table,
            schema_fields=[
                {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
                {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
            ],
        )
        create_dataset >> create_table >> [copy_selected_data, bigquery_to_gcs]

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )

    [copy_selected_data, bigquery_to_gcs] >> delete_dataset
