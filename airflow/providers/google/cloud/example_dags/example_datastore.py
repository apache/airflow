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
Example Airflow DAG that shows how to use Datastore operators.

This example requires that your project contains Datastore instance.
"""

import os
from typing import Any, Dict

from airflow import models
from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreAllocateIdsOperator, CloudDatastoreBeginTransactionOperator, CloudDatastoreCommitOperator,
    CloudDatastoreExportEntitiesOperator, CloudDatastoreImportEntitiesOperator,
    CloudDatastoreRollbackOperator, CloudDatastoreRunQueryOperator,
)
from airflow.utils import dates

GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "example-project")
BUCKET = os.environ.get("GCP_DATASTORE_BUCKET", "datastore-system-test")

with models.DAG(
    "example_gcp_datastore",
    schedule_interval=None,  # Override to match your needs
    start_date=dates.days_ago(1),
    tags=["example"],
) as dag:
    # [START how_to_export_task]
    export_task = CloudDatastoreExportEntitiesOperator(
        task_id="export_task",
        bucket=BUCKET,
        project_id=GCP_PROJECT_ID,
        overwrite_existing=True,
    )
    # [END how_to_export_task]

    # [START how_to_import_task]
    import_task = CloudDatastoreImportEntitiesOperator(
        task_id="import_task",
        bucket="{{ task_instance.xcom_pull('export_task')['response']['outputUrl'].split('/')[2] }}",
        file="{{ '/'.join(task_instance.xcom_pull('export_task')['response']['outputUrl'].split('/')[3:]) }}",
        project_id=GCP_PROJECT_ID,
    )
    # [END how_to_import_task]

    export_task >> import_task

# [START how_to_keys_def]
KEYS = [
    {
        "partitionId": {"projectId": GCP_PROJECT_ID, "namespaceId": ""},
        "path": {"kind": "airflow"},
    }
]
# [END how_to_keys_def]

# [START how_to_transaction_def]
TRANSACTION_OPTIONS: Dict[str, Any] = {"readWrite": {}}
# [END how_to_transaction_def]

# [START how_to_commit_def]
COMMIT_BODY = {
    "mode": "TRANSACTIONAL",
    "mutations": [
        {
            "insert": {
                "key": KEYS[0],
                "properties": {"string": {"stringValue": "airflow is awesome!"}},
            }
        }
    ],
    "transaction": "{{ task_instance.xcom_pull('begin_transaction_commit') }}",
}
# [END how_to_commit_def]

# [START how_to_query_def]
QUERY = {
    "partitionId": {"projectId": GCP_PROJECT_ID, "namespaceId": ""},
    "readOptions": {
        "transaction": "{{ task_instance.xcom_pull('begin_transaction_query') }}"
    },
    "query": {},
}
# [END how_to_query_def]

with models.DAG(
    "example_gcp_datastore_operations",
    start_date=dates.days_ago(1),
    schedule_interval=None,  # Override to match your needs
    tags=["example"],
) as dag2:
    # [START how_to_allocate_ids]
    allocate_ids = CloudDatastoreAllocateIdsOperator(
        task_id="allocate_ids", partial_keys=KEYS, project_id=GCP_PROJECT_ID
    )
    # [END how_to_allocate_ids]

    # [START how_to_begin_transaction]
    begin_transaction_commit = CloudDatastoreBeginTransactionOperator(
        task_id="begin_transaction_commit",
        transaction_options=TRANSACTION_OPTIONS,
        project_id=GCP_PROJECT_ID,
    )
    # [END how_to_begin_transaction]

    # [START how_to_commit_task]
    commit_task = CloudDatastoreCommitOperator(
        task_id="commit_task", body=COMMIT_BODY, project_id=GCP_PROJECT_ID
    )
    # [END how_to_commit_task]

    allocate_ids >> begin_transaction_commit >> commit_task

    begin_transaction_query = CloudDatastoreBeginTransactionOperator(
        task_id="begin_transaction_query",
        transaction_options=TRANSACTION_OPTIONS,
        project_id=GCP_PROJECT_ID,
    )

    # [START how_to_run_query]
    run_query = CloudDatastoreRunQueryOperator(
        task_id="run_query", body=QUERY, project_id=GCP_PROJECT_ID
    )
    # [END how_to_run_query]

    allocate_ids >> begin_transaction_query >> run_query

    begin_transaction_to_rollback = CloudDatastoreBeginTransactionOperator(
        task_id="begin_transaction_to_rollback",
        transaction_options=TRANSACTION_OPTIONS,
        project_id=GCP_PROJECT_ID,
    )

    # [START how_to_rollback_transaction]
    rollback_transaction = CloudDatastoreRollbackOperator(
        task_id="rollback_transaction",
        transaction="{{ task_instance.xcom_pull('begin_transaction_to_rollback') }}",
    )
    begin_transaction_to_rollback >> rollback_transaction
    # [END how_to_rollback_transaction]
