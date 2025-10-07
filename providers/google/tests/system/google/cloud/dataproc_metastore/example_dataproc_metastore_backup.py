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
Airflow System Test DAG that verifies Dataproc Metastore
operators for managing backups.
"""

from __future__ import annotations

import datetime
import os

from airflow.models.dag import DAG
from airflow.providers.google.cloud.operators.dataproc_metastore import (
    DataprocMetastoreCreateBackupOperator,
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreDeleteBackupOperator,
    DataprocMetastoreDeleteServiceOperator,
    DataprocMetastoreListBackupsOperator,
    DataprocMetastoreRestoreServiceOperator,
)

try:
    from airflow.sdk import TriggerRule
except ImportError:
    # Compatibility for Airflow < 3.1
    from airflow.utils.trigger_rule import TriggerRule  # type: ignore[no-redef,attr-defined]

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

DAG_ID = "dataproc_metastore_backup"

PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")

SERVICE_ID = f"{DAG_ID}-service-{ENV_ID}".replace("_", "-")
BACKUP_ID = f"{DAG_ID}-backup-{ENV_ID}".replace("_", "-")
REGION = "europe-west3"
# Service definition
SERVICE = {
    "name": "test-service",
}
# Backup definition
# [START how_to_cloud_dataproc_metastore_create_backup]
BACKUP = {
    "name": "test-backup",
}
# [END how_to_cloud_dataproc_metastore_create_backup]

with DAG(
    DAG_ID,
    start_date=datetime.datetime(2021, 1, 1),
    schedule="@once",
    catchup=False,
    tags=["example", "dataproc", "metastore"],
) as dag:
    create_service = DataprocMetastoreCreateServiceOperator(
        task_id="create_service",
        region=REGION,
        project_id=PROJECT_ID,
        service=SERVICE,
        service_id=SERVICE_ID,
    )
    # [START how_to_cloud_dataproc_metastore_create_backup_operator]
    backup_service = DataprocMetastoreCreateBackupOperator(
        task_id="create_backup",
        project_id=PROJECT_ID,
        region=REGION,
        service_id=SERVICE_ID,
        backup=BACKUP,
        backup_id=BACKUP_ID,
    )
    # [END how_to_cloud_dataproc_metastore_create_backup_operator]
    # [START how_to_cloud_dataproc_metastore_list_backups_operator]
    list_backups = DataprocMetastoreListBackupsOperator(
        task_id="list_backups",
        project_id=PROJECT_ID,
        region=REGION,
        service_id=SERVICE_ID,
    )
    # [END how_to_cloud_dataproc_metastore_list_backups_operator]
    # [START how_to_cloud_dataproc_metastore_delete_backup_operator]
    delete_backup = DataprocMetastoreDeleteBackupOperator(
        task_id="delete_backup",
        project_id=PROJECT_ID,
        region=REGION,
        service_id=SERVICE_ID,
        backup_id=BACKUP_ID,
    )
    # [END how_to_cloud_dataproc_metastore_delete_backup_operator]
    delete_backup.trigger_rule = TriggerRule.ALL_DONE
    # [START how_to_cloud_dataproc_metastore_restore_service_operator]
    restore_service = DataprocMetastoreRestoreServiceOperator(
        task_id="restore_metastore",
        region=REGION,
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        backup_id=BACKUP_ID,
        backup_region=REGION,
        backup_project_id=PROJECT_ID,
        backup_service_id=SERVICE_ID,
    )
    # [END how_to_cloud_dataproc_metastore_restore_service_operator]
    delete_service = DataprocMetastoreDeleteServiceOperator(
        task_id="delete_service",
        region=REGION,
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        trigger_rule=TriggerRule.ALL_DONE,
    )
    (create_service >> backup_service >> list_backups >> restore_service >> delete_backup >> delete_service)

    from tests_common.test_utils.watcher import watcher

    # This test needs watcher in order to properly mark success/failure
    # when "teardown" task with trigger rule is part of the DAG
    list(dag.tasks) >> watcher()


from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
