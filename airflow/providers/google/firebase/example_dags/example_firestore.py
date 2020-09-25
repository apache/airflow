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
Example Airflow DAG that shows interactions with Google Cloud Firestore.

Prerequisites
=============

This example uses two Google Cloud projects:

* ``GCP_PROJECT_ID`` - It contains a bucket and a firestore database.
* ``G_FIRESTORE_PROJECT_ID`` - it contains the Data Warehouse based on the BigQuery service.

Saving in a bucket should be possible from the ``G_FIRESTORE_PROJECT_ID`` project.
Reading from a bucket should be possible from the ``GCP_PROJECT_ID`` project.

The bucket and dataset should be located in the same region.

If you want to run this example, you must do the following:

1. Create Google Cloud project and enable the BigQuery API
2. Create the Firebase project
3. Create a bucket in the same location as the Firebase project
4. Grant Firebase admin account permissions to manage BigQuery. This is required to create a dataset.
5. Create a bucket in Firebase project and
6. Give read/write access for Firebase admin to bucket to step no. 5.
7. Create collection in the Firestore database.
"""

import os
from urllib.parse import urlparse

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryExecuteQueryOperator,
)
from airflow.providers.google.firebase.operators.firestore import CloudFirestoreExportDatabaseOperator
from airflow.utils import dates

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-gcp-project")
FIRESTORE_PROJECT_ID = os.environ.get("G_FIRESTORE_PROJECT_ID", "example-firebase-project")

EXPORT_DESTINATION_URL = os.environ.get("GCP_FIRESTORE_ARCHIVE_URL", "gs://airflow-firestore/namespace/")
BUCKET_NAME = urlparse(EXPORT_DESTINATION_URL).hostname
EXPORT_PREFIX = urlparse(EXPORT_DESTINATION_URL).path

EXPORT_COLLECTION_ID = os.environ.get("GCP_FIRESTORE_COLLECTION_ID", "firestore_collection_id")
DATASET_NAME = os.environ.get("GCP_FIRESTORE_DATASET_NAME", "test_firestore_export")
DATASET_LOCATION = os.environ.get("GCP_FIRESTORE_DATASET_LOCATION", "EU")

if BUCKET_NAME is None:
    raise ValueError("Bucket name is required. Please set GCP_FIRESTORE_ARCHIVE_URL env variable.")

with models.DAG(
    "example_google_firestore",
    default_args=dict(start_date=dates.days_ago(1)),
    schedule_interval=None,
    tags=["example"],
) as dag:
    # [START howto_operator_export_database_to_gcs]
    export_database_to_gcs = CloudFirestoreExportDatabaseOperator(
        task_id="export_database_to_gcs",
        project_id=FIRESTORE_PROJECT_ID,
        body={"outputUriPrefix": EXPORT_DESTINATION_URL, "collectionIds": [EXPORT_COLLECTION_ID]},
    )
    # [END howto_operator_export_database_to_gcs]

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_NAME,
        location=DATASET_LOCATION,
        project_id=GCP_PROJECT_ID,
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset", dataset_id=DATASET_NAME, project_id=GCP_PROJECT_ID, delete_contents=True
    )

    # [START howto_operator_create_external_table_multiple_types]
    create_external_table_multiple_types = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        bucket=BUCKET_NAME,
        source_objects=[
            f"{EXPORT_PREFIX}/all_namespaces/kind_{EXPORT_COLLECTION_ID}"
            f"/all_namespaces_kind_{EXPORT_COLLECTION_ID}.export_metadata"
        ],
        source_format="DATASTORE_BACKUP",
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{DATASET_NAME}.firestore_data",
    )
    # [END howto_operator_create_external_table_multiple_types]

    read_data_from_gcs_multiple_types = BigQueryExecuteQueryOperator(
        task_id="execute_query",
        sql=f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{DATASET_NAME}.firestore_data`",
        use_legacy_sql=False,
    )

    # Firestore
    export_database_to_gcs >> create_dataset

    # BigQuery
    create_dataset >> create_external_table_multiple_types
    create_external_table_multiple_types >> read_data_from_gcs_multiple_types
    read_data_from_gcs_multiple_types >> delete_dataset
