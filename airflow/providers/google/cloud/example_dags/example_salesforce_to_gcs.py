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
Example Airflow DAG that shows how to use SalesforceToGcsOperator.
"""
import os
from datetime import datetime

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.salesforce_to_gcs import SalesforceToGcsOperator

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
GCS_BUCKET = os.environ.get("GCS_BUCKET", "airflow-salesforce-bucket")
DATASET_NAME = os.environ.get("SALESFORCE_DATASET_NAME", "salesforce_test_dataset")
TABLE_NAME = os.environ.get("SALESFORCE_TABLE_NAME", "salesforce_test_datatable")
GCS_OBJ_PATH = os.environ.get("GCS_OBJ_PATH", "results.csv")
QUERY = "SELECT Id, Name, Company, Phone, Email, CreatedDate, LastModifiedDate, IsDeleted FROM Lead"
GCS_CONN_ID = os.environ.get("GCS_CONN_ID", "google_cloud_default")
SALESFORCE_CONN_ID = os.environ.get("SALESFORCE_CONN_ID", "salesforce_default")


with models.DAG(
    "example_salesforce_to_gcs",
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=GCS_BUCKET,
        project_id=GCP_PROJECT_ID,
        gcp_conn_id=GCS_CONN_ID,
    )

    # [START howto_operator_salesforce_to_gcs]
    gcs_upload_task = SalesforceToGcsOperator(
        query=QUERY,
        include_deleted=True,
        bucket_name=GCS_BUCKET,
        object_name=GCS_OBJ_PATH,
        salesforce_conn_id=SALESFORCE_CONN_ID,
        export_format='csv',
        coerce_to_timestamp=False,
        record_time_added=False,
        gcp_conn_id=GCS_CONN_ID,
        task_id="upload_to_gcs",
        dag=dag,
    )
    # [END howto_operator_salesforce_to_gcs]

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME, project_id=GCP_PROJECT_ID, gcp_conn_id=GCS_CONN_ID
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        schema_fields=[
            {'name': 'id', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'company', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'phone', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'createddate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'lastmodifieddate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'isdeleted', 'type': 'BOOL', 'mode': 'NULLABLE'},
        ],
    )

    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket=GCS_BUCKET,
        source_objects=[GCS_OBJ_PATH],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition='WRITE_TRUNCATE',
    )

    read_data_from_gcs = BigQueryInsertJobOperator(
        task_id="read_data_from_gcs",
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

    create_bucket >> gcs_upload_task >> load_csv
    create_dataset >> create_table >> load_csv
    load_csv >> read_data_from_gcs
    read_data_from_gcs >> delete_bucket
    read_data_from_gcs >> delete_dataset
