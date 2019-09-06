# -*- coding: utf-8 -*-
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
Example Airflow DAG for Google Big Query service
"""
import os
from urllib.parse import urlparse

import airflow
from airflow import models
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.contrib.operators.bigquery_operator import (
    BigQueryOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryGetDatasetOperator,
    BigQueryPatchDatasetOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryCreateExternalTableOperator,
)
from airflow.contrib.operators.bigquery_table_delete_operator import BigQueryTableDeleteOperator
from airflow.contrib.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.operators.bash_operator import BashOperator

# 0x06012c8cf97BEaD5deAe237070F9587f8E7A266d = CryptoKitties contract address
WALLET_ADDRESS = os.environ.get("GCP_ETH_WALLET_ADDRESS", "0x06012c8cf97BEaD5deAe237070F9587f8E7A266d")

default_args = {"start_date": airflow.utils.dates.days_ago(1)}

MOST_VALUABLE_INCOMING_TRANSACTIONS = """
SELECT
  value, to_address
FROM
  `bigquery-public-data.ethereum_blockchain.transactions`
WHERE
  1 = 1
  AND DATE(block_timestamp) = "{{ ds }}"
  AND to_address = LOWER(@to_address)
ORDER BY value DESC
LIMIT 1000
"""

MOST_ACTIVE_PLAYERS = """
SELECT
  COUNT(from_address)
  , from_address
FROM
  `bigquery-public-data.ethereum_blockchain.transactions`
WHERE
  1 = 1
  AND DATE(block_timestamp) = "{{ ds }}"
  AND to_address = LOWER(@to_address)
GROUP BY from_address
ORDER BY COUNT(from_address) DESC
LIMIT 1000
"""

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")

DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "test_dataset")

DATA_SAMPLE_GCS_URL = os.environ.get(
    "GCP_BIGQUERY_DATA_GCS_URL", "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
)

DATA_SAMPLE_GCS_URL_PARTS = urlparse(DATA_SAMPLE_GCS_URL)
DATA_SAMPLE_GCS_BUCKET_NAME = DATA_SAMPLE_GCS_URL_PARTS.netloc
DATA_SAMPLE_GCS_OBJECT_NAME = DATA_SAMPLE_GCS_URL_PARTS.path[1:]

DATA_EXPORT_BUCKET_NAME = os.environ.get("GCP_BIGQUERY_EXPORT_BUCKET_NAME", "test-bigquery-sample-data")


with models.DAG(
    "example_bigquery", default_args=default_args, schedule_interval=None  # Override to match your needs
) as dag:

    execute_query = BigQueryOperator(
        task_id="execute-query",
        sql=MOST_VALUABLE_INCOMING_TRANSACTIONS,
        use_legacy_sql=False,
        query_params=[
            {
                "name": "to_address",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": WALLET_ADDRESS},
            }
        ],
    )

    bigquery_execute_multi_query = BigQueryOperator(
        task_id="execute-multi-query",
        sql=[MOST_VALUABLE_INCOMING_TRANSACTIONS, MOST_ACTIVE_PLAYERS],
        use_legacy_sql=False,
        query_params=[
            {
                "name": "to_address",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": WALLET_ADDRESS},
            }
        ],
    )

    execute_query_save = BigQueryOperator(
        task_id="execute-query-save",
        sql=MOST_VALUABLE_INCOMING_TRANSACTIONS,
        use_legacy_sql=False,
        destination_dataset_table="{}.save_query_result".format(DATASET_NAME),
        query_params=[
            {
                "name": "to_address",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": WALLET_ADDRESS},
            }
        ],
    )

    get_data = BigQueryGetDataOperator(
        task_id="get-data",
        dataset_id=DATASET_NAME,
        table_id="save_query_result",
        max_results="10",
        selected_fields="value,to_address",
    )

    get_data_result = BashOperator(
        task_id="get-data-result", bash_command="echo \"{{ task_instance.xcom_pull('get-data') }}\""
    )

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create-external-table",
        bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        source_objects=[DATA_SAMPLE_GCS_OBJECT_NAME],
        destination_project_dataset_table="{}.external_table".format(DATASET_NAME),
        skip_leading_rows=1,
        schema_fields=[{"name": "name", "type": "STRING"}, {"name": "post_abbr", "type": "STRING"}],
    )

    execute_query_external_table = BigQueryOperator(
        task_id="execute-query-external-table",
        destination_dataset_table="{}.selected_data_from_external_table".format(DATASET_NAME),
        sql='SELECT * FROM `{}.external_table` WHERE name LIKE "W%"'.format(DATASET_NAME),
        use_legacy_sql=False,
    )

    copy_from_selected_data = BigQueryToBigQueryOperator(
        task_id="copy-from-selected-data",
        source_project_dataset_tables="{}.selected_data_from_external_table".format(DATASET_NAME),
        destination_project_dataset_table="{}.copy_of_selected_data_from_external_table".format(DATASET_NAME),
    )

    bigquery_to_gcs = BigQueryToCloudStorageOperator(
        task_id="bigquery-to-gcs",
        source_project_dataset_table="{}.selected_data_from_external_table".format(DATASET_NAME),
        destination_cloud_storage_uris=["gs://{}/export-bigquery.csv".format(DATA_EXPORT_BUCKET_NAME)],
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create-dataset", dataset_id=DATASET_NAME)

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create-table",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    delete_table = BigQueryTableDeleteOperator(
        task_id="delete-table", deletion_dataset_table="{}.test_table".format(DATASET_NAME)
    )

    get_dataset = BigQueryGetDatasetOperator(task_id="get-dataset", dataset_id=DATASET_NAME)

    get_dataset_result = BashOperator(
        task_id="get-dataset-result",
        bash_command="echo \"{{ task_instance.xcom_pull('get-dataset')['id'] }}\"",
    )

    patch_dataset = BigQueryPatchDatasetOperator(
        task_id="patch-dataset",
        dataset_id=DATASET_NAME,
        dataset_resource={"friendlyName": "Patchet Dataset", "description": "Patched dataset"},
    )

    update_dataset = BigQueryUpdateDatasetOperator(
        task_id="update-dataset", dataset_id=DATASET_NAME, dataset_resource={"description": "Updated dataset"}
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete-dataset", dataset_id=DATASET_NAME, delete_contents=True
    )

    create_dataset >> execute_query_save >> delete_dataset
    create_dataset >> create_table >> delete_dataset
    create_dataset >> get_dataset >> delete_dataset
    create_dataset >> patch_dataset >> update_dataset >> delete_dataset
    execute_query_save >> get_data >> get_dataset_result
    get_data >> delete_dataset
    create_dataset >> create_external_table >> execute_query_external_table >> \
        copy_from_selected_data >> delete_dataset
    execute_query_external_table >> bigquery_to_gcs >> delete_dataset
    create_table >> delete_table >> delete_dataset
