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
Example Airflow DAG showing export of database from Google Cloud Firestore to Cloud Storage.

This example creates collections in the default namespaces in Firestore, adds some data to the collection
and exports this database from Cloud Firestore to Cloud Storage.
It then creates a table from the exported data in Bigquery and checks that the data is in it.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.dataflow import DataflowTemplatedJobStartOperator
from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreCommitOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.firebase.operators.firestore import CloudFirestoreExportDatabaseOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT", "default")
DAG_ID = "firestore_to_gcp"

BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}".replace("-", "_")

EXPORT_DESTINATION_URL = f"gs://{BUCKET_NAME}/namespace"
EXPORT_COLLECTION_ID = f"collection_{DAG_ID}_{ENV_ID}".replace("-", "_")
EXTERNAL_TABLE_SOURCE_URI = (
    f"{EXPORT_DESTINATION_URL}/all_namespaces/kind_{EXPORT_COLLECTION_ID}"
    f"/all_namespaces_kind_{EXPORT_COLLECTION_ID}.export_metadata"
)
DATASET_LOCATION = "EU"
KEYS = {
    "partitionId": {"projectId": PROJECT_ID, "namespaceId": ""},
    "path": {"kind": f"{EXPORT_COLLECTION_ID}"},
}


with DAG(
    DAG_ID,
    start_date=datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "firestore"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, location=DATASET_LOCATION
    )
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=DATASET_NAME,
        location=DATASET_LOCATION,
        project_id=PROJECT_ID,
    )
    commit_task = CloudDatastoreCommitOperator(
        task_id="commit_task",
        body={
            "mode": "TRANSACTIONAL",
            "mutations": [
                {
                    "insert": {
                        "key": KEYS,
                        "properties": {"string": {"stringValue": "test!"}},
                    }
                }
            ],
            "singleUseTransaction": {"readWrite": {}},
        },
        project_id=PROJECT_ID,
    )
    # [START howto_operator_export_database_to_gcs]
    export_database_to_gcs = CloudFirestoreExportDatabaseOperator(
        task_id="export_database_to_gcs",
        project_id=PROJECT_ID,
        body={"outputUriPrefix": EXPORT_DESTINATION_URL, "collectionIds": [EXPORT_COLLECTION_ID]},
    )
    # [END howto_operator_export_database_to_gcs]
    # [START howto_operator_create_external_table_multiple_types]
    create_external_table_multiple_types = BigQueryCreateExternalTableOperator(
        task_id="create_external_table",
        bucket=BUCKET_NAME,
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": "firestore_data",
            },
            "externalDataConfiguration": {
                "sourceFormat": "DATASTORE_BACKUP",
                "compression": "NONE",
                "sourceUris": [EXTERNAL_TABLE_SOURCE_URI],
            },
        },
    )
    # [END howto_operator_create_external_table_multiple_types]
    read_data_from_gcs_multiple_types = BigQueryInsertJobOperator(
        task_id="execute_query",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_NAME}.firestore_data`",
                "useLegacySql": False,
            }
        },
    )
    delete_entity = DataflowTemplatedJobStartOperator(
        task_id="delete-entity-firestore",
        project_id=PROJECT_ID,
        template="gs://dataflow-templates/latest/Firestore_to_Firestore_Delete",
        parameters={
            "firestoreReadGqlQuery": f"SELECT * FROM {EXPORT_COLLECTION_ID}",
            "firestoreReadProjectId": PROJECT_ID,
            "firestoreDeleteProjectId": PROJECT_ID,
        },
        environment={
            "tempLocation": f"gs://{BUCKET_NAME}/tmp",
        },
        location="us-central1",
        append_job_name=False,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=DATASET_NAME,
        project_id=PROJECT_ID,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    (
        # TEST SETUP
        [create_bucket, create_dataset]
        >> commit_task
        # TEST BODY
        >> export_database_to_gcs
        >> create_external_table_multiple_types
        >> read_data_from_gcs_multiple_types
        # TEST TEARDOWN
        >> delete_entity
        >> [delete_dataset, delete_bucket]
    )

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
