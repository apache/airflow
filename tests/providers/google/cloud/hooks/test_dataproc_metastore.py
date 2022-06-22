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
#

from unittest import TestCase, mock

from airflow.providers.google.cloud.hooks.dataproc_metastore import DataprocMetastoreHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_BACKUP: str = "test-backup"
TEST_BACKUP_ID: str = "test-backup-id"
TEST_METADATA_IMPORT: dict = {
    "name": "test-metadata-import",
    "database_dump": {
        "gcs_uri": "gs://bucket_name/path_inside_bucket",
        "database_type": "MYSQL",
    },
}
TEST_METADATA_IMPORT_ID: str = "test-metadata-import-id"
TEST_SERVICE: dict = {"name": "test-service"}
TEST_SERVICE_ID: str = "test-service-id"
TEST_SERVICE_TO_UPDATE = {
    "labels": {
        "first_key": "first_value",
        "second_key": "second_value",
    }
}
TEST_UPDATE_MASK: dict = {"paths": ["labels"]}
TEST_PARENT: str = "projects/{}/locations/{}"
TEST_PARENT_SERVICES: str = "projects/{}/locations/{}/services/{}"
TEST_PARENT_BACKUPS: str = "projects/{}/locations/{}/services/{}/backups"
TEST_NAME_BACKUPS: str = "projects/{}/locations/{}/services/{}/backups/{}"
TEST_DESTINATION_GCS_FOLDER: str = "gs://bucket_name/path_inside_bucket"

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
DATAPROC_METASTORE_STRING = "airflow.providers.google.cloud.hooks.dataproc_metastore.{}"


class TestDataprocMetastoreWithDefaultProjectIdHook(TestCase):
    def setUp(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = DataprocMetastoreHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_create_backup(self, mock_client) -> None:
        self.hook.create_backup(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_backup.assert_called_once_with(
            request=dict(
                parent=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                backup=TEST_BACKUP,
                backup_id=TEST_BACKUP_ID,
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_create_metadata_import(self, mock_client) -> None:
        self.hook.create_metadata_import(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
            metadata_import=TEST_METADATA_IMPORT,
            metadata_import_id=TEST_METADATA_IMPORT_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_metadata_import.assert_called_once_with(
            request=dict(
                parent=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                metadata_import=TEST_METADATA_IMPORT,
                metadata_import_id=TEST_METADATA_IMPORT_ID,
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_create_service(self, mock_client) -> None:
        self.hook.create_service(
            region=TEST_REGION,
            project_id=TEST_PROJECT_ID,
            service=TEST_SERVICE,
            service_id=TEST_SERVICE_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_service.assert_called_once_with(
            request=dict(
                parent=TEST_PARENT.format(TEST_PROJECT_ID, TEST_REGION),
                service_id=TEST_SERVICE_ID,
                service=TEST_SERVICE,
                request_id=None,
            ),
            metadata=(),
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_delete_backup(self, mock_client) -> None:
        self.hook.delete_backup(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
            backup_id=TEST_BACKUP_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_backup.assert_called_once_with(
            request=dict(
                name=TEST_NAME_BACKUPS.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID, TEST_BACKUP_ID),
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_delete_service(self, mock_client) -> None:
        self.hook.delete_service(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_service.assert_called_once_with(
            request=dict(
                name=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                request_id=None,
            ),
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_export_metadata(self, mock_client) -> None:
        self.hook.export_metadata(
            destination_gcs_folder=TEST_DESTINATION_GCS_FOLDER,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.export_metadata.assert_called_once_with(
            request=dict(
                destination_gcs_folder=TEST_DESTINATION_GCS_FOLDER,
                service=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                request_id=None,
                database_dump_type=None,
            ),
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_get_service(self, mock_client) -> None:
        self.hook.get_service(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.get_service.assert_called_once_with(
            request=dict(
                name=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_list_backups(self, mock_client) -> None:
        self.hook.list_backups(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.list_backups.assert_called_once_with(
            request=dict(
                parent=TEST_PARENT_BACKUPS.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                page_size=None,
                page_token=None,
                filter=None,
                order_by=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_restore_service(self, mock_client) -> None:
        self.hook.restore_service(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
            backup_project_id=TEST_PROJECT_ID,
            backup_region=TEST_REGION,
            backup_service_id=TEST_SERVICE_ID,
            backup_id=TEST_BACKUP_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.restore_service.assert_called_once_with(
            request=dict(
                service=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                backup=TEST_NAME_BACKUPS.format(
                    TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID, TEST_BACKUP_ID
                ),
                restore_type=None,
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_update_service(self, mock_client) -> None:
        self.hook.update_service(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
            service=TEST_SERVICE_TO_UPDATE,
            update_mask=TEST_UPDATE_MASK,
        )
        mock_client.assert_called_once()
        mock_client.return_value.update_service.assert_called_once_with(
            request=dict(
                service=TEST_SERVICE_TO_UPDATE,
                update_mask=TEST_UPDATE_MASK,
                request_id=None,
            ),
            retry=None,
            timeout=None,
            metadata=None,
        )


class TestDataprocMetastoreWithoutDefaultProjectIdHook(TestCase):
    def setUp(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = DataprocMetastoreHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_create_backup(self, mock_client) -> None:
        self.hook.create_backup(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
            backup=TEST_BACKUP,
            backup_id=TEST_BACKUP_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_backup.assert_called_once_with(
            request=dict(
                parent=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                backup=TEST_BACKUP,
                backup_id=TEST_BACKUP_ID,
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_create_metadata_import(self, mock_client) -> None:
        self.hook.create_metadata_import(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
            metadata_import=TEST_METADATA_IMPORT,
            metadata_import_id=TEST_METADATA_IMPORT_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_metadata_import.assert_called_once_with(
            request=dict(
                parent=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                metadata_import=TEST_METADATA_IMPORT,
                metadata_import_id=TEST_METADATA_IMPORT_ID,
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_create_service(self, mock_client) -> None:
        self.hook.create_service(
            region=TEST_REGION,
            project_id=TEST_PROJECT_ID,
            service=TEST_SERVICE,
            service_id=TEST_SERVICE_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.create_service.assert_called_once_with(
            request=dict(
                parent=TEST_PARENT.format(TEST_PROJECT_ID, TEST_REGION),
                service_id=TEST_SERVICE_ID,
                service=TEST_SERVICE,
                request_id=None,
            ),
            metadata=(),
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_delete_backup(self, mock_client) -> None:
        self.hook.delete_backup(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
            backup_id=TEST_BACKUP_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_backup.assert_called_once_with(
            request=dict(
                name=TEST_NAME_BACKUPS.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID, TEST_BACKUP_ID),
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_delete_service(self, mock_client) -> None:
        self.hook.delete_service(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.delete_service.assert_called_once_with(
            request=dict(
                name=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                request_id=None,
            ),
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_export_metadata(self, mock_client) -> None:
        self.hook.export_metadata(
            destination_gcs_folder=TEST_DESTINATION_GCS_FOLDER,
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.export_metadata.assert_called_once_with(
            request=dict(
                destination_gcs_folder=TEST_DESTINATION_GCS_FOLDER,
                service=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                request_id=None,
                database_dump_type=None,
            ),
            retry=None,
            timeout=None,
            metadata=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_get_service(self, mock_client) -> None:
        self.hook.get_service(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.get_service.assert_called_once_with(
            request=dict(
                name=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_list_backups(self, mock_client) -> None:
        self.hook.list_backups(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.list_backups.assert_called_once_with(
            request=dict(
                parent=TEST_PARENT_BACKUPS.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                page_size=None,
                page_token=None,
                filter=None,
                order_by=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_restore_service(self, mock_client) -> None:
        self.hook.restore_service(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
            backup_project_id=TEST_PROJECT_ID,
            backup_region=TEST_REGION,
            backup_service_id=TEST_SERVICE_ID,
            backup_id=TEST_BACKUP_ID,
        )
        mock_client.assert_called_once()
        mock_client.return_value.restore_service.assert_called_once_with(
            request=dict(
                service=TEST_PARENT_SERVICES.format(TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID),
                backup=TEST_NAME_BACKUPS.format(
                    TEST_PROJECT_ID, TEST_REGION, TEST_SERVICE_ID, TEST_BACKUP_ID
                ),
                restore_type=None,
                request_id=None,
            ),
            metadata=None,
            retry=None,
            timeout=None,
        )

    @mock.patch(DATAPROC_METASTORE_STRING.format("DataprocMetastoreHook.get_dataproc_metastore_client"))
    def test_update_service(self, mock_client) -> None:
        self.hook.update_service(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            service_id=TEST_SERVICE_ID,
            service=TEST_SERVICE_TO_UPDATE,
            update_mask=TEST_UPDATE_MASK,
        )
        mock_client.assert_called_once()
        mock_client.return_value.update_service.assert_called_once_with(
            request=dict(
                service=TEST_SERVICE_TO_UPDATE,
                update_mask=TEST_UPDATE_MASK,
                request_id=None,
            ),
            retry=None,
            timeout=None,
            metadata=None,
        )
