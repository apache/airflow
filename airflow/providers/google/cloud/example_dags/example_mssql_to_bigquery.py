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
Example Airflow DAG that shows how to transfer data from MsSQL to BigQuery by utilizing MSSQLToGCSOperator.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryGetDataOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.mssql_to_gcs import MSSQLToGCSOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "mssql-to-airflow-bucket")
GCS_CONN_ID = os.environ.get("GCS_CONN_ID", "google_cloud_default")
MSSQL_CONN_ID = os.environ.get("MSSQL_CONN_ID", "airflow_mssql")
DATASET_NAME = "numbers_dataset"
TABLE_NAME = "FloatingNumbers"

with models.DAG(
    "example_mssql_to_bigquery", schedule="@once", start_date=datetime(2021, 1, 1), catchup=False
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=GCS_BUCKET, project_id=GCP_PROJECT_ID, gcp_conn_id=GCS_CONN_ID
    )

    create_mssql_table = MsSqlOperator(
        task_id="create_mssql_table",
        mssql_conn_id=MSSQL_CONN_ID,
        sql=f"""
        CREATE TABLE {TABLE_NAME} (
            number FLOAT
        );
        """,
        dag=dag,
    )

    insert_rows_to_mssql_table = MsSqlOperator(
        task_id="insert_rows_to_mssql_table",
        mssql_conn_id=MSSQL_CONN_ID,
        sql=f"""
        INSERT INTO {TABLE_NAME} VALUES (7.9), (8.8), (10.1)
        """,
    )

    export_mssql_table_to_json_in_gcs = MSSQLToGCSOperator(
        task_id="export_mssql_table_to_json_in_gcs",
        mssql_conn_id=MSSQL_CONN_ID,
        sql=f"SELECT * FROM {TABLE_NAME};",
        bucket=GCS_BUCKET,
        filename="floating_numbers.json",
        export_format="json",
    )

    drop_mssql_table = MsSqlOperator(
        task_id="drop_mssql_table",
        mssql_conn_id=MSSQL_CONN_ID,
        sql=f"""
        DROP TABLE {TABLE_NAME};
        """,
        dag=dag,
    )

    create_bigquery_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bigquery_dataset", dataset_id=DATASET_NAME, gcp_conn_id=GCS_CONN_ID
    )

    upload_json_from_gcs_to_bq = GCSToBigQueryOperator(
        task_id="upload_json_from_gcs_to_bq",
        bucket=GCS_BUCKET,
        source_objects="floating_numbers.json",
        schema_fields=[{"name": "number", "type": "FLOAT"}],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        source_format="NEWLINE_DELIMITED_JSON",
        gcp_conn_id=GCS_CONN_ID,
    )

    select_bigquery_table_contents = BigQueryGetDataOperator(
        task_id="select_bigquery_table_contents",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        selected_fields="number",
        gcp_conn_id=GCS_CONN_ID,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=GCS_BUCKET, gcp_conn_id=GCS_CONN_ID
    )

    delete_bigquery_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_bigquery_dataset",
        dataset_id=DATASET_NAME,
        delete_contents=True,
        gcp_conn_id=GCS_CONN_ID,
    )

    (
        create_bucket
        >> create_mssql_table
        >> insert_rows_to_mssql_table
        >> export_mssql_table_to_json_in_gcs
        >> [drop_mssql_table, create_bigquery_dataset]
    )
    (
        create_bigquery_dataset
        >> upload_json_from_gcs_to_bq
        >> select_bigquery_table_contents
        >> delete_bucket
        >> delete_bigquery_dataset
    )
