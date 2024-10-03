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
Airflow System Test DAG that verifies Datastore commit operators.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any

from airflow.models.baseoperator import chain
from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreAllocateIdsOperator,
    CloudDatastoreBeginTransactionOperator,
    CloudDatastoreCommitOperator,
    CloudDatastoreDeleteOperationOperator,
    CloudDatastoreExportEntitiesOperator,
    CloudDatastoreGetOperationOperator,
    CloudDatastoreImportEntitiesOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.utils.trigger_rule import TriggerRule
from tests.system.providers.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "datastore_commit"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"
# [START how_to_keys_def]
KEYS = [
    {
        "partitionId": {"projectId": PROJECT_ID, "namespaceId": ""},
        "path": {"kind": "airflow"},
    }
]
# [END how_to_keys_def]

# [START how_to_transaction_def]
TRANSACTION_OPTIONS: dict[str, Any] = {"readWrite": {}}
# [END how_to_transaction_def]


with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example", "datastore"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID, location="EU"
    )

    # [START how_to_allocate_ids]
    allocate_ids = CloudDatastoreAllocateIdsOperator(
        task_id="allocate_ids", partial_keys=KEYS, project_id=PROJECT_ID
    )
    # [END how_to_allocate_ids]

    # [START how_to_begin_transaction]
    begin_transaction_commit = CloudDatastoreBeginTransactionOperator(
        task_id="begin_transaction_commit",
        transaction_options=TRANSACTION_OPTIONS,
        project_id=PROJECT_ID,
    )
    # [END how_to_begin_transaction]

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
        "singleUseTransaction": {"readWrite": {}},
    }
    # [END how_to_commit_def]

    # [START how_to_commit_task]
    commit_task = CloudDatastoreCommitOperator(task_id="commit_task", body=COMMIT_BODY, project_id=PROJECT_ID)
    # [END how_to_commit_task]

    # [START how_to_export_task]
    export_task = CloudDatastoreExportEntitiesOperator(
        task_id="export_task",
        bucket=BUCKET_NAME,
        project_id=PROJECT_ID,
        overwrite_existing=True,
    )
    # [END how_to_export_task]

    # [START how_to_import_task]
    import_task = CloudDatastoreImportEntitiesOperator(
        task_id="import_task",
        bucket="{{ task_instance.xcom_pull('export_task')['response']['outputUrl'].split('/')[2] }}",
        file="{{ '/'.join(task_instance.xcom_pull('export_task')['response']['outputUrl'].split('/')[3:]) }}",
        project_id=PROJECT_ID,
    )
    # [END how_to_import_task]

    # [START get_operation_state]
    get_operation = CloudDatastoreGetOperationOperator(
        task_id="get_operation", name="{{ task_instance.xcom_pull('export_task')['name'] }}"
    )
    # [END get_operation_state]

    # [START delete_operation]
    delete_export_operation = CloudDatastoreDeleteOperationOperator(
        task_id="delete_export_operation",
        name="{{ task_instance.xcom_pull('export_task')['name'] }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )
    # [END delete_operation]

    delete_import_operation = CloudDatastoreDeleteOperationOperator(
        task_id="delete_import_operation",
        name="{{ task_instance.xcom_pull('import_task')['name'] }}",
        trigger_rule=TriggerRule.ALL_DONE,
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    chain(
        # TEST SETUP
        create_bucket,
        # TEST BODY
        allocate_ids,
        begin_transaction_commit,
        commit_task,
        export_task,
        import_task,
        get_operation,
        # TEST TEARDOWN
        [delete_bucket, delete_export_operation, delete_import_operation],
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
