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

from __future__ import annotations

import os
from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.salesforce_to_gcs import SalesforceToGcsOperator

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "salesforce_to_gcs"

DATASET_NAME = f"dataset_{DAG_ID}_{ENV_ID}"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"

TABLE_NAME = "salesforce_test_datatable"
GCS_OBJ_PATH = "results.csv"
QUERY = "SELECT Id, Name, Company, Phone, Email, CreatedDate, LastModifiedDate, IsDeleted FROM Lead"


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "salesforce_to_gcs"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket",
        bucket_name=BUCKET_NAME,
        project_id=PROJECT_ID,
    )

    # [START howto_operator_salesforce_to_gcs]
    gcs_upload_task = SalesforceToGcsOperator(
        query=QUERY,
        include_deleted=True,
        bucket_name=BUCKET_NAME,
        object_name=GCS_OBJ_PATH,
        export_format="csv",
        coerce_to_timestamp=False,
        record_time_added=False,
        task_id="upload_to_gcs",
        salesforce_conn_id="salesforce_conn_id",
    )
    # [END howto_operator_salesforce_to_gcs]

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset", dataset_id=DATASET_NAME, project_id=PROJECT_ID
    )

    create_table = BigQueryCreateTableOperator(
        task_id="create_table",
        dataset_id=DATASET_NAME,
        table_id=TABLE_NAME,
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": DATASET_NAME,
                "tableId": TABLE_NAME,
            },
            "schema": {
                "fields": [
                    {"name": "id", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "company", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "email", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "createddate", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "lastmodifieddate", "type": "STRING", "mode": "NULLABLE"},
                    {"name": "isdeleted", "type": "BOOL", "mode": "NULLABLE"},
                ],
            },
        },
    )

    load_csv = GCSToBigQueryOperator(
        task_id="gcs_to_bq",
        bucket=BUCKET_NAME,
        source_objects=[GCS_OBJ_PATH],
        destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
        write_disposition="WRITE_TRUNCATE",
    )

    read_data_from_gcs = BigQueryInsertJobOperator(
        task_id="read_data_from_gcs",
        configuration={
            "query": {
                "query": f"SELECT COUNT(*) FROM `{PROJECT_ID}.{DATASET_NAME}.{TABLE_NAME}`",
                "useLegacySql": False,
            }
        },
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket",
        bucket_name=BUCKET_NAME,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        project_id=PROJECT_ID,
        dataset_id=DATASET_NAME,
        delete_contents=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    # TEST SETUP
    create_bucket >> gcs_upload_task >> load_csv
    create_dataset >> create_table >> load_csv
    # TEST BODY
    load_csv >> read_data_from_gcs
    # TEST TEARDOWN
    read_data_from_gcs >> delete_bucket
    read_data_from_gcs >> delete_dataset

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
