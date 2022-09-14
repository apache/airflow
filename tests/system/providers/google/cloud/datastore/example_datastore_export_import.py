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
Airflow System Test DAG that verifies Datastore export and import operators.
"""
from __future__ import annotations

import os
from datetime import datetime

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.datastore import (
    CloudDatastoreDeleteOperationOperator,
    CloudDatastoreExportEntitiesOperator,
    CloudDatastoreGetOperationOperator,
    CloudDatastoreImportEntitiesOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator, GCSDeleteBucketOperator
from airflow.utils.trigger_rule import TriggerRule

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT")

DAG_ID = "datastore_export_import"
BUCKET_NAME = f"bucket_{DAG_ID}_{ENV_ID}"


with models.DAG(
    DAG_ID,
    schedule='@once',
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["datastore", "example"],
) as dag:
    create_bucket = GCSCreateBucketOperator(
        task_id="create_bucket", bucket_name=BUCKET_NAME, project_id=PROJECT_ID, location="EU"
    )

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
        task_id='get_operation', name="{{ task_instance.xcom_pull('export_task')['name'] }}"
    )
    # [END get_operation_state]

    # [START delete_operation]
    delete_export_operation = CloudDatastoreDeleteOperationOperator(
        task_id='delete_export_operation',
        name="{{ task_instance.xcom_pull('export_task')['name'] }}",
    )
    # [END delete_operation]
    delete_export_operation.trigger_rule = TriggerRule.ALL_DONE

    delete_import_operation = CloudDatastoreDeleteOperationOperator(
        task_id='delete_import_operation',
        name="{{ task_instance.xcom_pull('export_task')['name'] }}",
    )

    delete_bucket = GCSDeleteBucketOperator(
        task_id="delete_bucket", bucket_name=BUCKET_NAME, trigger_rule=TriggerRule.ALL_DONE
    )

    chain(
        create_bucket,
        export_task,
        import_task,
        get_operation,
        [delete_bucket, delete_export_operation, delete_import_operation],
    )

    from tests.system.utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "tearDown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests.system.utils import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
