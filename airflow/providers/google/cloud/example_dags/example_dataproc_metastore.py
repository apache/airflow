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
Example Airflow DAG that show how to use various Dataproc Metastore
operators to manage a service.
"""

import datetime
import os

from airflow import models
from airflow.models.baseoperator import chain
from airflow.providers.google.cloud.operators.dataproc_metastore import (
    DataprocMetastoreCreateBackupOperator,
    DataprocMetastoreCreateMetadataImportOperator,
    DataprocMetastoreCreateServiceOperator,
    DataprocMetastoreDeleteBackupOperator,
    DataprocMetastoreDeleteServiceOperator,
    DataprocMetastoreExportMetadataOperator,
    DataprocMetastoreGetServiceOperator,
    DataprocMetastoreListBackupsOperator,
    DataprocMetastoreRestoreServiceOperator,
    DataprocMetastoreUpdateServiceOperator,
)

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "<PROJECT_ID>")
SERVICE_ID = os.environ.get("GCP_DATAPROC_METASTORE_SERVICE_ID", "dataproc-metastore-system-tests-service-1")
BACKUP_ID = os.environ.get("GCP_DATAPROC_METASTORE_BACKUP_ID", "dataproc-metastore-system-tests-backup-1")
REGION = os.environ.get("GCP_REGION", "<REGION>")
BUCKET = os.environ.get("GCP_DATAPROC_METASTORE_BUCKET", "INVALID BUCKET NAME")
METADATA_IMPORT_FILE = os.environ.get("GCS_METADATA_IMPORT_FILE", None)
GCS_URI = os.environ.get("GCS_URI", f"gs://{BUCKET}/data/hive.sql")
METADATA_IMPORT_ID = "dataproc-metastore-system-tests-metadata-import-1"
TIMEOUT = 1200
DB_TYPE = "MYSQL"
DESTINATION_GCS_FOLDER = f"gs://{BUCKET}/>"

# Service definition
# Docs: https://cloud.google.com/dataproc-metastore/docs/reference/rest/v1/projects.locations.services#Service
# [START how_to_cloud_dataproc_metastore_create_service]
SERVICE = {
    "name": "test-service",
}
# [END how_to_cloud_dataproc_metastore_create_service]

# Update service
# [START how_to_cloud_dataproc_metastore_update_service]
SERVICE_TO_UPDATE = {
    "labels": {
        "mylocalmachine": "mylocalmachine",
        "systemtest": "systemtest",
    }
}
UPDATE_MASK = {"paths": ["labels"]}
# [END how_to_cloud_dataproc_metastore_update_service]

# Backup definition
# [START how_to_cloud_dataproc_metastore_create_backup]
BACKUP = {
    "name": "test-backup",
}
# [END how_to_cloud_dataproc_metastore_create_backup]

# Metadata import definition
# [START how_to_cloud_dataproc_metastore_create_metadata_import]
METADATA_IMPORT = {
    "name": "test-metadata-import",
    "database_dump": {
        "gcs_uri": GCS_URI,
        "database_type": DB_TYPE,
    },
}
# [END how_to_cloud_dataproc_metastore_create_metadata_import]


with models.DAG(
    "example_gcp_dataproc_metastore", start_date=datetime.datetime(2021, 1, 1), schedule_interval="@once"
) as dag:
    # [START how_to_cloud_dataproc_metastore_create_service_operator]
    create_service = DataprocMetastoreCreateServiceOperator(
        task_id="create_service",
        region=REGION,
        project_id=PROJECT_ID,
        service=SERVICE,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_create_service_operator]

    # [START how_to_cloud_dataproc_metastore_get_service_operator]
    get_service_details = DataprocMetastoreGetServiceOperator(
        task_id="get_service",
        region=REGION,
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
    )
    # [END how_to_cloud_dataproc_metastore_get_service_operator]

    # [START how_to_cloud_dataproc_metastore_update_service_operator]
    update_service = DataprocMetastoreUpdateServiceOperator(
        task_id="update_service",
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        region=REGION,
        service=SERVICE_TO_UPDATE,
        update_mask=UPDATE_MASK,
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_update_service_operator]

    # [START how_to_cloud_dataproc_metastore_create_metadata_import_operator]
    import_metadata = DataprocMetastoreCreateMetadataImportOperator(
        task_id="create_metadata_import",
        project_id=PROJECT_ID,
        region=REGION,
        service_id=SERVICE_ID,
        metadata_import=METADATA_IMPORT,
        metadata_import_id=METADATA_IMPORT_ID,
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_create_metadata_import_operator]

    # [START how_to_cloud_dataproc_metastore_export_metadata_operator]
    export_metadata = DataprocMetastoreExportMetadataOperator(
        task_id="export_metadata",
        destination_gcs_folder=DESTINATION_GCS_FOLDER,
        project_id=PROJECT_ID,
        region=REGION,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_export_metadata_operator]

    # [START how_to_cloud_dataproc_metastore_create_backup_operator]
    backup_service = DataprocMetastoreCreateBackupOperator(
        task_id="create_backup",
        project_id=PROJECT_ID,
        region=REGION,
        service_id=SERVICE_ID,
        backup=BACKUP,
        backup_id=BACKUP_ID,
        timeout=TIMEOUT,
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
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_delete_backup_operator]

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
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_restore_service_operator]

    # [START how_to_cloud_dataproc_metastore_delete_service_operator]
    delete_service = DataprocMetastoreDeleteServiceOperator(
        task_id="delete_service",
        region=REGION,
        project_id=PROJECT_ID,
        service_id=SERVICE_ID,
        timeout=TIMEOUT,
    )
    # [END how_to_cloud_dataproc_metastore_delete_service_operator]

    chain(
        create_service,
        update_service,
        get_service_details,
        backup_service,
        list_backups,
        restore_service,
        delete_backup,
        export_metadata,
        import_metadata,
        delete_service,
    )
