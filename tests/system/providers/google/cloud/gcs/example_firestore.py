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
from __future__ import annotations

import os
from datetime import datetime
from urllib.parse import urlparse

from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.firebase.operators.firestore import CloudFirestoreExportDatabaseOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "example_google_firestore"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-gcp-project")
FIRESTORE_PROJECT_ID = os.environ.get("G_FIRESTORE_PROJECT_ID", "example-firebase-project")

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
EXPORT_DESTINATION_URL = os.environ.get("GCP_FIRESTORE_ARCHIVE_URL", "gs://INVALID BUCKET NAME/namespace/")
EXPORT_PREFIX = urlparse(EXPORT_DESTINATION_URL).path
EXPORT_COLLECTION_ID = os.environ.get("GCP_FIRESTORE_COLLECTION_ID", "firestore_collection_id")
DATASET_LOCATION = os.environ.get("GCP_FIRESTORE_DATASET_LOCATION", "EU")

if BUCKET_NAME is None:
    raise ValueError("Bucket name is required. Please set GCP_FIRESTORE_ARCHIVE_URL env variable.")

with models.DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "firestore"],
) as dag:
    create_bucket = GCSCreateBucketOperator(task_id="create_bucket", bucket_name=BUCKET_NAME)

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_NAME,
        location=DATASET_LOCATION,
        project_id=GCP_PROJECT_ID,
    )

    # [START howto_operator_export_database_to_gcs]
    export_database_to_gcs = CloudFirestoreExportDatabaseOperator(
        task_id="export_database_to_gcs",
        project_id=FIRESTORE_PROJECT_ID,
        body={"outputUriPrefix": EXPORT_DESTINATION_URL, "collectionIds": [EXPORT_COLLECTION_ID]},
    )
    # [END howto_operator_export_database_to_gcs]

    # [START howto_operator_create_external_table_multiple_types]
    create_external_table_multiple_types = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        bucket=BUCKET_NAME,
        table_resource={
            "tableReference": {
                "projectId": GCP_PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": "firestore_data",
            },
            "schema": {
                "fields": [
                    {"name": "name", "type": "STRING"},
                    {"name": "post_abbr", "type": "STRING"},
                ]
            },
            "externalDataConfiguration": {
                "sourceFormat": "DATASTORE_BACKUP",
                "compression": "NONE",
                "csvOptions": {"skipLeadingRows": 1},
            },
        },
    )
    # [END howto_operator_create_external_table_multiple_types]

    read_data_from_gcs_multiple_types = BigQueryInsertJobOperator(
        task_id="execute_query",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) FROM `{GCP_PROJECT_ID}.{DATASET_NAME}.firestore_data`",
                "useLegacySql": False,
            }
        },
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        project_id=GCP_PROJECT_ID,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        create_bucket
        >> create_dataset
        # TEST BODY
        >> export_database_to_gcs
        >> create_external_table_multiple_types
        >> read_data_from_gcs_multiple_types
        # TEST TEARDOWN
        >> delete_dataset
        >> delete_bucket
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
