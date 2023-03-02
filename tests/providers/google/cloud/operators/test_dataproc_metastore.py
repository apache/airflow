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
from __future__ import annotations

from unittest import mock

from google.api_core.gapic_v1.method import _MethodDefault
from google.api_core.retry import Retry
from google.protobuf.field_mask_pb2 import FieldMask

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

TASK_ID: str = "task_id"
GCP_LOCATION: str = "test-location"
GCP_PROJECT_ID: str = "test-project-id"

GCP_CONN_ID: str = "test-gcp-conn-id"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

TEST_SERVICE: dict = {"name": "test-service"}
TEST_SERVICE_ID: str = "test-service-id"

TEST_TIMEOUT = 120
TEST_RETRY: Retry | _MethodDefault = mock.MagicMock(Retry)
TEST_METADATA = [("key", "value")]
TEST_REQUEST_ID = "request_id_uuid"

TEST_BACKUP: dict = {"name": "test-backup"}
TEST_BACKUP_ID: str = "test-backup-id"
TEST_METADATA_IMPORT: dict = {
    "name": "test-metadata-import",
    "database_dump": {
        "gcs_uri": "gs://bucket_name/path_inside_bucket",
        "database_type": "MYSQL",
    },
}
TEST_METADATA_IMPORT_ID: str = "test-metadata-import-id"
TEST_SERVICE_TO_UPDATE = {
    "labels": {
        "first_key": "first_value",
        "second_key": "second_value",
    }
}
TEST_UPDATE_MASK: FieldMask = FieldMask(paths=["labels"])
TEST_DESTINATION_GCS_FOLDER: str = "gs://bucket_name/path_inside_bucket"


class TestDataprocMetastoreCreateBackupOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.Backup")
    def test_assert_valid_hook_call(self, mock_backup, mock_hook) -> None:
        task = DataprocMetastoreCreateBackupOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT_ID,
            region=GCP_LOCATION,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.wait_for_operation.return_value = None
        mock_backup.return_value.to_dict.return_value = None
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_backup.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            region=GCP_LOCATION,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
            service_id=TEST_SERVICE_ID,
            request_id=None,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreCreateMetadataImportOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.MetadataImport")
    def test_assert_valid_hook_call(self, mock_metadata_import, mock_hook) -> None:
        task = DataprocMetastoreCreateMetadataImportOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT_ID,
            region=GCP_LOCATION,
            service_id=TEST_SERVICE_ID,
            metadata_import=mock_metadata_import(name=TEST_METADATA_IMPORT),
            metadata_import_id=TEST_METADATA_IMPORT_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.wait_for_operation.return_value = None
        mock_metadata_import.return_value.to_dict.return_value = None
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_metadata_import.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            region=GCP_LOCATION,
            service_id=TEST_SERVICE_ID,
            metadata_import=mock_metadata_import(name=TEST_METADATA_IMPORT),
            metadata_import_id=TEST_METADATA_IMPORT_ID,
            request_id=None,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreCreateServiceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.Service")
    def test_execute(self, mock_service, mock_hook) -> None:
        task = DataprocMetastoreCreateServiceOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            service=TEST_SERVICE,
            service_id=TEST_SERVICE_ID,
            request_id=TEST_REQUEST_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.wait_for_operation.return_value = None
        mock_service.return_value.to_dict.return_value = None
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.create_service.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            service=TEST_SERVICE,
            service_id=TEST_SERVICE_ID,
            request_id=TEST_REQUEST_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreDeleteBackupOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreDeleteBackupOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT_ID,
            region=GCP_LOCATION,
            retry=TEST_RETRY,
            service_id=TEST_SERVICE_ID,
            backup_id=TEST_BACKUP_ID,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.wait_for_operation.return_value = None
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_backup.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            region=GCP_LOCATION,
            service_id=TEST_SERVICE_ID,
            backup_id=TEST_BACKUP_ID,
            request_id=None,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreDeleteServiceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook")
    def test_execute(self, mock_hook) -> None:
        task = DataprocMetastoreDeleteServiceOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.wait_for_operation.return_value = None
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.delete_service.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreExportMetadataOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.MetadataExport")
    @mock.patch(
        "airflow.providers.google.cloud.operators.dataproc_metastore"
        ".DataprocMetastoreExportMetadataOperator._wait_for_export_metadata"
    )
    def test_assert_valid_hook_call(self, mock_wait, mock_export_metadata, mock_hook) -> None:
        task = DataprocMetastoreExportMetadataOperator(
            task_id=TASK_ID,
            service_id=TEST_SERVICE_ID,
            destination_gcs_folder=TEST_DESTINATION_GCS_FOLDER,
            project_id=GCP_PROJECT_ID,
            region=GCP_LOCATION,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_wait.return_value = None
        mock_export_metadata.return_value.to_dict.return_value = None
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.export_metadata.assert_called_once_with(
            database_dump_type=None,
            destination_gcs_folder=TEST_DESTINATION_GCS_FOLDER,
            project_id=GCP_PROJECT_ID,
            region=GCP_LOCATION,
            service_id=TEST_SERVICE_ID,
            request_id=None,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreGetServiceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.Service")
    def test_execute(self, mock_service, mock_hook) -> None:
        task = DataprocMetastoreGetServiceOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.wait_for_operation.return_value = None
        mock_service.return_value.to_dict.return_value = None
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.get_service.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreListBackupsOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook")
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.Backup")
    def test_assert_valid_hook_call(self, mock_backup, mock_hook) -> None:
        task = DataprocMetastoreListBackupsOperator(
            task_id=TASK_ID,
            project_id=GCP_PROJECT_ID,
            region=GCP_LOCATION,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_hook.return_value.wait_for_operation.return_value = None
        mock_backup.return_value.to_dict.return_value = None
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.list_backups.assert_called_once_with(
            project_id=GCP_PROJECT_ID,
            region=GCP_LOCATION,
            service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            filter=None,
            order_by=None,
            page_size=None,
            page_token=None,
        )


class TestDataprocMetastoreRestoreServiceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook")
    @mock.patch(
        "airflow.providers.google.cloud.operators.dataproc_metastore"
        ".DataprocMetastoreRestoreServiceOperator._wait_for_restore_service"
    )
    def test_assert_valid_hook_call(self, mock_wait, mock_hook) -> None:
        task = DataprocMetastoreRestoreServiceOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            service_id=TEST_SERVICE_ID,
            backup_id=TEST_BACKUP_ID,
            backup_region=GCP_LOCATION,
            backup_project_id=GCP_PROJECT_ID,
            backup_service_id=TEST_SERVICE_ID,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        mock_wait.return_value = None
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.restore_service.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            service_id=TEST_SERVICE_ID,
            backup_id=TEST_BACKUP_ID,
            backup_region=GCP_LOCATION,
            backup_project_id=GCP_PROJECT_ID,
            backup_service_id=TEST_SERVICE_ID,
            restore_type=None,
            request_id=None,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )


class TestDataprocMetastoreUpdateServiceOperator:
    @mock.patch("airflow.providers.google.cloud.operators.dataproc_metastore.DataprocMetastoreHook")
    def test_assert_valid_hook_call(self, mock_hook) -> None:
        task = DataprocMetastoreUpdateServiceOperator(
            task_id=TASK_ID,
            region=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            service_id=TEST_SERVICE_ID,
            service=TEST_SERVICE_TO_UPDATE,
            update_mask=TEST_UPDATE_MASK,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
            gcp_conn_id=GCP_CONN_ID,
            impersonation_chain=IMPERSONATION_CHAIN,
        )
        task.execute(context=mock.MagicMock())
        mock_hook.assert_called_once_with(gcp_conn_id=GCP_CONN_ID, impersonation_chain=IMPERSONATION_CHAIN)
        mock_hook.return_value.update_service.assert_called_once_with(
            region=GCP_LOCATION,
            project_id=GCP_PROJECT_ID,
            service_id=TEST_SERVICE_ID,
            service=TEST_SERVICE_TO_UPDATE,
            update_mask=TEST_UPDATE_MASK,
            request_id=None,
            retry=TEST_RETRY,
            timeout=TEST_TIMEOUT,
            metadata=TEST_METADATA,
        )
