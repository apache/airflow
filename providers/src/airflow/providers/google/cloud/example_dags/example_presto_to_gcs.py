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
Example DAG using PrestoToGCSOperator.
"""

from __future__ import annotations

import os
import re
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.presto_to_gcs import PrestoToGCSOperator

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCS_BUCKET = os.environ.get("GCP_PRESTO_TO_GCS_BUCKET_NAME", "INVALID BUCKET NAME")
DATASET_NAME = os.environ.get("GCP_PRESTO_TO_GCS_DATASET_NAME", "test_presto_to_gcs_dataset")

SOURCE_MULTIPLE_TYPES = "memory.default.test_multiple_types"
SOURCE_CUSTOMER_TABLE = "tpch.sf1.customer"


def safe_name(s: str) -> str:
    """
    Remove invalid characters for filename
    """
    return re.sub("[^0-9a-zA-Z_]+", "_", s)


with DAG(
    dag_id="example_presto_to_gcs",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(task_id="create-dataset", dataset_id=DATASET_NAME)

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, delete_contents=True
    )

    # [START howto_operator_presto_to_gcs_basic]
    presto_to_gcs_basic = PrestoToGCSOperator(
        task_id="presto_to_gcs_basic",
        sql=f"select * from {SOURCE_MULTIPLE_TYPES}",
        bucket=GCS_BUCKET,
        filename=f"{safe_name(SOURCE_MULTIPLE_TYPES)}.{{}}.json",
    )
    # [END howto_operator_presto_to_gcs_basic]

    # [START howto_operator_presto_to_gcs_multiple_types]
    presto_to_gcs_multiple_types = PrestoToGCSOperator(
        task_id="presto_to_gcs_multiple_types",
        sql=f"select * from {SOURCE_MULTIPLE_TYPES}",
        bucket=GCS_BUCKET,
        filename=f"{safe_name(SOURCE_MULTIPLE_TYPES)}.{{}}.json",
        schema_filename=f"{safe_name(SOURCE_MULTIPLE_TYPES)}-schema.json",
        gzip=False,
    )
    # [END howto_operator_presto_to_gcs_multiple_types]

    # [START howto_operator_create_external_table_multiple_types]
    create_external_table_multiple_types = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_multiple_types",
        bucket=GCS_BUCKET,
        source_objects=[f"{safe_name(SOURCE_MULTIPLE_TYPES)}.*.json"],
        table_resource={
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": f"{safe_name(SOURCE_MULTIPLE_TYPES)}",
            },
            "schema": {
                "fields": [
                    {"name": "name", "type": "STRING"},
                    {"name": "post_abbr", "type": "STRING"},
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "compression": "NONE",
                "csvOptions": {"skipLeadingRows": 1},
            },
        },
        schema_object=f"{safe_name(SOURCE_MULTIPLE_TYPES)}-schema.json",
    )
    # [END howto_operator_create_external_table_multiple_types]

    read_data_from_gcs_multiple_types = BigQueryInsertJobOperator(
        task_id="read_data_from_gcs_multiple_types",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{DATASET_NAME}."
                f"{safe_name(SOURCE_MULTIPLE_TYPES)}`",
                "useLegacySql": False,
            }
        },
    )

    # [START howto_operator_presto_to_gcs_many_chunks]
    presto_to_gcs_many_chunks = PrestoToGCSOperator(
        task_id="presto_to_gcs_many_chunks",
        sql=f"select * from {SOURCE_CUSTOMER_TABLE}",
        bucket=GCS_BUCKET,
        filename=f"{safe_name(SOURCE_CUSTOMER_TABLE)}.{{}}.json",
        schema_filename=f"{safe_name(SOURCE_CUSTOMER_TABLE)}-schema.json",
        approx_max_file_size_bytes=10_000_000,
        gzip=False,
    )
    # [END howto_operator_presto_to_gcs_many_chunks]

    create_external_table_many_chunks = BigQueryCreateExternalTableOperator(
        task_id="create_external_table_many_chunks",
        bucket=GCS_BUCKET,
        table_resource={
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": f"{safe_name(SOURCE_CUSTOMER_TABLE)}",
            },
            "schema": {
                "fields": [
                    {"name": "name", "type": "STRING"},
                    {"name": "post_abbr", "type": "STRING"},
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "compression": "NONE",
                "csvOptions": {"skipLeadingRows": 1},
            },
        },
        source_objects=[f"{safe_name(SOURCE_CUSTOMER_TABLE)}.*.json"],
        schema_object=f"{safe_name(SOURCE_CUSTOMER_TABLE)}-schema.json",
    )

    # [START howto_operator_read_data_from_gcs_many_chunks]
    read_data_from_gcs_many_chunks = BigQueryInsertJobOperator(
        task_id="read_data_from_gcs_many_chunks",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{DATASET_NAME}."
                f"{safe_name(SOURCE_CUSTOMER_TABLE)}`",
                "useLegacySql": False,
            }
        },
    )
    # [END howto_operator_read_data_from_gcs_many_chunks]

    # [START howto_operator_presto_to_gcs_csv]
    presto_to_gcs_csv = PrestoToGCSOperator(
        task_id="presto_to_gcs_csv",
        sql=f"select * from {SOURCE_MULTIPLE_TYPES}",
        bucket=GCS_BUCKET,
        filename=f"{safe_name(SOURCE_MULTIPLE_TYPES)}.{{}}.csv",
        schema_filename=f"{safe_name(SOURCE_MULTIPLE_TYPES)}-schema.json",
        export_format="csv",
    )
    # [END howto_operator_presto_to_gcs_csv]

    create_dataset >> presto_to_gcs_basic
    create_dataset >> presto_to_gcs_multiple_types
    create_dataset >> presto_to_gcs_many_chunks
    create_dataset >> presto_to_gcs_csv

    presto_to_gcs_multiple_types >> create_external_table_multiple_types >> read_data_from_gcs_multiple_types
    presto_to_gcs_many_chunks >> create_external_table_many_chunks >> read_data_from_gcs_many_chunks

    presto_to_gcs_basic >> delete_dataset
    presto_to_gcs_csv >> delete_dataset
    read_data_from_gcs_multiple_types >> delete_dataset
    read_data_from_gcs_many_chunks >> delete_dataset
