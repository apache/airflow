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
from urllib.parse import urlparse

from airflow import models
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator, BigQueryDeleteTableOperator, BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator, BigQueryGetDatasetOperator, BigQueryGetDatasetTablesOperator,
    BigQueryPatchDatasetOperator, BigQueryUpdateDatasetOperator, BigQueryUpsertTableOperator,
)
from airflow.providers.google.cloud.operators.bigquery_to_bigquery import BigQueryToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.dates import days_ago

# 0x06012c8cf97BEaD5deAe237070F9587f8E7A266d = CryptoKitties contract address
WALLET_ADDRESS = os.environ.get("GCP_ETH_WALLET_ADDRESS", "0x06012c8cf97BEaD5deAe237070F9587f8E7A266d")

default_args = {"start_date": days_ago(1)}

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
BQ_LOCATION = "europe-north1"

DATASET_NAME = os.environ.get("GCP_BIGQUERY_DATASET_NAME", "test_dataset")
LOCATION_DATASET_NAME = "{}_location".format(DATASET_NAME)
DATA_SAMPLE_GCS_URL = os.environ.get(
    "GCP_BIGQUERY_DATA_GCS_URL", "gs://cloud-samples-data/bigquery/us-states/us-states.csv"
)

DATA_SAMPLE_GCS_URL_PARTS = urlparse(DATA_SAMPLE_GCS_URL)
DATA_SAMPLE_GCS_BUCKET_NAME = DATA_SAMPLE_GCS_URL_PARTS.netloc
DATA_SAMPLE_GCS_OBJECT_NAME = DATA_SAMPLE_GCS_URL_PARTS.path[1:]

DATA_EXPORT_BUCKET_NAME = os.environ.get("GCP_BIGQUERY_EXPORT_BUCKET_NAME", "test-bigquery-sample-data")


with models.DAG(
    "example_bigquery",
    default_args=default_args,
    schedule_interval=None,  # Override to match your needs
    tags=['example'],
) as dag:

    execute_query = BigQueryExecuteQueryOperator(
        task_id="execute_query",
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

    bigquery_execute_multi_query = BigQueryExecuteQueryOperator(
        task_id="execute_multi_query",
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

    execute_query_save = BigQueryExecuteQueryOperator(
        task_id="execute_query_save",
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
        task_id="get_data",
        dataset_id=DATASET_NAME,
        table_id="save_query_result",
        max_results="10",
        selected_fields="value,to_address",
    )

    get_data_result = BashOperator(
        task_id="get_data_result", bash_command="echo \"{{ task_instance.xcom_pull('get-data') }}\""
    )

    create_external_table = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        bucket=DATA_SAMPLE_GCS_BUCKET_NAME,
        source_objects=[DATA_SAMPLE_GCS_OBJECT_NAME],
        destination_project_dataset_table="{}.external_table".format(DATASET_NAME),
        skip_leading_rows=1,
        schema_fields=[{"name": "name", "type": "STRING"}, {"name": "post_abbr", "type": "STRING"}],
    )

    execute_query_external_table = BigQueryExecuteQueryOperator(
        task_id="execute_query_external_table",
        destination_dataset_table="{}.selected_data_from_external_table".format(DATASET_NAME),
        sql='SELECT * FROM `{}.external_table` WHERE name LIKE "W%"'.format(DATASET_NAME),
        use_legacy_sql=False,
    )

    copy_from_selected_data = BigQueryToBigQueryOperator(
        task_id="copy_from_selected_data",
        source_project_dataset_tables="{}.selected_data_from_external_table".format(DATASET_NAME),
        destination_project_dataset_table="{}.copy_of_selected_data_from_external_table".format(DATASET_NAME),
    )

    bigquery_to_gcs = BigQueryToGCSOperator(
        task_id="bigquery_to_gcs",
        source_project_dataset_table="{}.selected_data_from_external_table".format(DATASET_NAME),
        destination_cloud_storage_uris=["gs://{}/export-bigquery.csv".format(DATA_EXPORT_BUCKET_NAME)],
    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create-dataset", dataset_id=DATASET_NAME)

    create_dataset_with_location = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset_with_location",
        dataset_id=LOCATION_DATASET_NAME,
        location=BQ_LOCATION
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    create_table_with_location = BigQueryCreateEmptyTableOperator(
        task_id="create_table_with_location",
        dataset_id=LOCATION_DATASET_NAME,
        table_id="test_table",
        schema_fields=[
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "salary", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    create_view = BigQueryCreateEmptyTableOperator(
        task_id="create_view",
        dataset_id=LOCATION_DATASET_NAME,
        table_id="test_view",
        view={
            "query": "SELECT * FROM `{}.test_table`".format(DATASET_NAME),
            "useLegacySql": False
        }
    )

    get_empty_dataset_tables = BigQueryGetDatasetTablesOperator(
        task_id="get_empty_dataset_tables",
        dataset_id=DATASET_NAME
    )

    get_dataset_tables = BigQueryGetDatasetTablesOperator(
        task_id="get_dataset_tables",
        dataset_id=DATASET_NAME
    )

    delete_view = BigQueryDeleteTableOperator(
        task_id="delete_view", deletion_dataset_table="{}.test_view".format(DATASET_NAME)
    )

    delete_table = BigQueryDeleteTableOperator(
        task_id="delete_table", deletion_dataset_table="{}.test_table".format(DATASET_NAME)
    )

    get_dataset = BigQueryGetDatasetOperator(task_id="get-dataset", dataset_id=DATASET_NAME)

    get_dataset_result = BashOperator(
        task_id="get_dataset_result",
        bash_command="echo \"{{ task_instance.xcom_pull('get-dataset')['id'] }}\"",
    )

    patch_dataset = BigQueryPatchDatasetOperator(
        task_id="patch_dataset",
        dataset_id=DATASET_NAME,
        dataset_resource={"friendlyName": "Patched Dataset", "description": "Patched dataset"},
    )

    update_dataset = BigQueryUpdateDatasetOperator(
        task_id="update_dataset", dataset_id=DATASET_NAME, dataset_resource={"description": "Updated dataset"}
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )

    delete_dataset_with_location = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset_with_location",
        dataset_id=LOCATION_DATASET_NAME,
        delete_contents=True
    )

    update_table = BigQueryUpsertTableOperator(
        task_id="update_table", dataset_id=DATASET_NAME, table_resource={
            "tableReference": {
                "tableId": "test-table-id"
            },
            "expirationTime": 12345678
        }
    )

    create_dataset >> execute_query_save >> delete_dataset
    create_dataset >> get_empty_dataset_tables >> create_table >> get_dataset_tables >> delete_dataset
    create_dataset >> get_dataset >> delete_dataset
    create_dataset >> patch_dataset >> update_dataset >> delete_dataset
    execute_query_save >> get_data >> get_dataset_result
    get_data >> delete_dataset
    create_dataset >> create_external_table >> execute_query_external_table >> \
        copy_from_selected_data >> delete_dataset
    execute_query_external_table >> bigquery_to_gcs >> delete_dataset
    create_table >> create_view >> delete_view >> delete_table >> delete_dataset
    create_dataset_with_location >> create_table_with_location >> delete_dataset_with_location
    create_dataset >> create_table >> update_table >> delete_table >> delete_dataset
