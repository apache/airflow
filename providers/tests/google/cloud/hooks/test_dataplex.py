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

from google.api_core.gapic_v1.method import DEFAULT
from google.protobuf.field_mask_pb2 import FieldMask

from airflow.providers.google.cloud.operators.dataplex import DataplexHook

from providers.tests.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
DATAPLEX_STRING = "airflow.providers.google.cloud.hooks.dataplex.{}"
DATAPLEX_HOOK_CLIENT = "airflow.providers.google.cloud.hooks.dataplex.DataplexHook.get_dataplex_client"
DATAPLEX_HOOK_DS_CLIENT = (
    "airflow.providers.google.cloud.hooks.dataplex.DataplexHook.get_dataplex_data_scan_client"
)
DATAPLEX_CATALOG_HOOK_CLIENT = (
    "airflow.providers.google.cloud.hooks.dataplex.DataplexHook.get_dataplex_catalog_client"
)

PROJECT_ID = "project-id"
REGION = "region"
LAKE_ID = "lake-id"
BODY = {"body": "test"}
DATAPLEX_TASK_ID = "testTask001"

GCP_CONN_ID = "google_cloud_default"
IMPERSONATION_CHAIN = ["ACCOUNT_1", "ACCOUNT_2", "ACCOUNT_3"]

DATA_SCAN_ID = "test-data-scan-id"
ASSET_ID = "test_asset_id"
ZONE_ID = "test_zone_id"
JOB_ID = "job_id"

LOCATION = "us-central1"
ENTRY_GROUP_ID = "entry-group-id"
ENTRY_GROUP_BODY = {"description": "Some descr"}
ENTRY_GROUP_UPDATED_BODY = {"description": "Some new descr"}
ENTRY_TYPE_ID = "entry-type-id"
ENTRY_TYPE_BODY = {"description": "Some descr"}
ENTRY_TYPE_UPDATED_BODY = {"description": "Some new descr"}
UPDATE_MASK = ["description"]

COMMON_PARENT = f"projects/{PROJECT_ID}/locations/{LOCATION}"
DATA_SCAN_NAME = f"projects/{PROJECT_ID}/locations/{REGION}/dataScans/{DATA_SCAN_ID}"
DATA_SCAN_JOB_NAME = f"projects/{PROJECT_ID}/locations/{REGION}/dataScans/{DATA_SCAN_ID}/jobs/{JOB_ID}"
ZONE_NAME = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}"
ZONE_PARENT = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/zones/{ZONE_ID}"
ASSET_PARENT = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/zones/{ZONE_ID}/assets/{ASSET_ID}"
DATASCAN_PARENT = f"projects/{PROJECT_ID}/locations/{REGION}"
ENTRY_GROUP_PARENT = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryGroup/{ENTRY_GROUP_ID}"
ENTRY_TYPE_PARENT = f"projects/{PROJECT_ID}/locations/{LOCATION}/entryType/{ENTRY_TYPE_ID}"


class TestDataplexHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"),
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = DataplexHook(
                gcp_conn_id=GCP_CONN_ID,
                impersonation_chain=IMPERSONATION_CHAIN,
            )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_create_task(self, mock_client):
        self.hook.create_task(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            body=BODY,
            dataplex_task_id=DATAPLEX_TASK_ID,
            validate_only=None,
        )

        parent = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}"
        mock_client.return_value.create_task.assert_called_once_with(
            request=dict(
                parent=parent,
                task_id=DATAPLEX_TASK_ID,
                task=BODY,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_delete_task(self, mock_client):
        self.hook.delete_task(
            project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, dataplex_task_id=DATAPLEX_TASK_ID
        )

        name = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/tasks/{DATAPLEX_TASK_ID}"
        mock_client.return_value.delete_task.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_list_tasks(self, mock_client):
        self.hook.list_tasks(project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID)

        parent = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}"
        mock_client.return_value.list_tasks.assert_called_once_with(
            request=dict(
                parent=parent,
                page_size=None,
                page_token=None,
                filter=None,
                order_by=None,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_get_task(self, mock_client):
        self.hook.get_task(
            project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, dataplex_task_id=DATAPLEX_TASK_ID
        )

        name = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/tasks/{DATAPLEX_TASK_ID}"
        mock_client.return_value.get_task.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_create_lake(self, mock_client):
        self.hook.create_lake(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            body=BODY,
            validate_only=None,
        )

        parent = f"projects/{PROJECT_ID}/locations/{REGION}"
        mock_client.return_value.create_lake.assert_called_once_with(
            request=dict(
                parent=parent,
                lake_id=LAKE_ID,
                lake=BODY,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_delete_lake(self, mock_client):
        self.hook.delete_lake(project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID)

        name = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}"
        mock_client.return_value.delete_lake.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_get_lake(self, mock_client):
        self.hook.get_lake(project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID)

        name = f"projects/{PROJECT_ID}/locations/{REGION}/lakes/{LAKE_ID}/"
        mock_client.return_value.get_lake.assert_called_once_with(
            request=dict(
                name=name,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_create_zone(self, mock_client):
        self.hook.create_zone(project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, zone_id=ZONE_ID, body={})

        mock_client.return_value.create_zone.assert_called_once_with(
            request=dict(
                parent=ZONE_NAME,
                zone_id=ZONE_ID,
                zone={},
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_delete_zone(self, mock_client):
        self.hook.delete_zone(project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, zone_id=ZONE_ID)

        mock_client.return_value.delete_zone.assert_called_once_with(
            request=dict(
                name=ZONE_PARENT,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_create_asset(self, mock_client):
        self.hook.create_asset(
            project_id=PROJECT_ID,
            region=REGION,
            lake_id=LAKE_ID,
            zone_id=ZONE_ID,
            asset_id=ASSET_ID,
            body={},
        )

        mock_client.return_value.create_asset.assert_called_once_with(
            request=dict(
                parent=ZONE_PARENT,
                asset={},
                asset_id=ASSET_ID,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_CLIENT)
    def test_delete_asset(self, mock_client):
        self.hook.delete_asset(
            project_id=PROJECT_ID, region=REGION, lake_id=LAKE_ID, zone_id=ZONE_ID, asset_id=ASSET_ID
        )

        mock_client.return_value.delete_asset.assert_called_once_with(
            request=dict(
                name=ASSET_PARENT,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_DS_CLIENT)
    def test_create_data_scan(self, mock_client):
        self.hook.create_data_scan(project_id=PROJECT_ID, region=REGION, data_scan_id=DATA_SCAN_ID, body={})

        mock_client.return_value.create_data_scan.assert_called_once_with(
            request=dict(parent=DATASCAN_PARENT, data_scan_id=DATA_SCAN_ID, data_scan={}),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_DS_CLIENT)
    def test_run_data_scan(self, mock_client):
        self.hook.run_data_scan(project_id=PROJECT_ID, region=REGION, data_scan_id=DATA_SCAN_ID)

        mock_client.return_value.run_data_scan.assert_called_once_with(
            request=dict(
                name=DATA_SCAN_NAME,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_DS_CLIENT)
    def test_get_data_scan_job(self, mock_client):
        self.hook.get_data_scan_job(
            project_id=PROJECT_ID, region=REGION, job_id=JOB_ID, data_scan_id=DATA_SCAN_ID
        )

        mock_client.return_value.get_data_scan_job.assert_called_once_with(
            request=dict(name=DATA_SCAN_JOB_NAME, view="FULL"),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_DS_CLIENT)
    def test_delete_data_scan(self, mock_client):
        self.hook.delete_data_scan(project_id=PROJECT_ID, region=REGION, data_scan_id=DATA_SCAN_ID)

        mock_client.return_value.delete_data_scan.assert_called_once_with(
            request=dict(
                name=DATA_SCAN_NAME,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_HOOK_DS_CLIENT)
    def test_get_data_scan(self, mock_client):
        self.hook.get_data_scan(project_id=PROJECT_ID, region=REGION, data_scan_id=DATA_SCAN_ID)

        mock_client.return_value.get_data_scan.assert_called_once_with(
            request=dict(name=DATA_SCAN_NAME, view="FULL"),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_CATALOG_HOOK_CLIENT)
    def test_create_entry_group(self, mock_client):
        mock_common_location_path = mock_client.return_value.common_location_path
        mock_common_location_path.return_value = COMMON_PARENT
        self.hook.create_entry_group(
            project_id=PROJECT_ID,
            location=LOCATION,
            entry_group_id=ENTRY_GROUP_ID,
            entry_group_configuration=ENTRY_GROUP_BODY,
            validate_only=False,
        )
        mock_client.return_value.create_entry_group.assert_called_once_with(
            request=dict(
                parent=COMMON_PARENT,
                entry_group_id=ENTRY_GROUP_ID,
                entry_group=ENTRY_GROUP_BODY,
                validate_only=False,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_CATALOG_HOOK_CLIENT)
    def test_delete_entry_group(self, mock_client):
        mock_common_location_path = mock_client.return_value.entry_group_path
        mock_common_location_path.return_value = ENTRY_GROUP_PARENT
        self.hook.delete_entry_group(project_id=PROJECT_ID, location=LOCATION, entry_group_id=ENTRY_GROUP_ID)

        mock_client.return_value.delete_entry_group.assert_called_once_with(
            request=dict(
                name=ENTRY_GROUP_PARENT,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_CATALOG_HOOK_CLIENT)
    def test_list_entry_groups(self, mock_client):
        mock_common_location_path = mock_client.return_value.common_location_path
        mock_common_location_path.return_value = COMMON_PARENT
        self.hook.list_entry_groups(
            project_id=PROJECT_ID,
            location=LOCATION,
            order_by="name",
            page_size=2,
            filter_by="'description' = 'Some descr'",
        )
        mock_client.return_value.list_entry_groups.assert_called_once_with(
            request=dict(
                parent=COMMON_PARENT,
                page_size=2,
                page_token=None,
                filter="'description' = 'Some descr'",
                order_by="name",
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_CATALOG_HOOK_CLIENT)
    def test_get_entry_group(self, mock_client):
        mock_common_location_path = mock_client.return_value.entry_group_path
        mock_common_location_path.return_value = ENTRY_GROUP_PARENT
        self.hook.get_entry_group(project_id=PROJECT_ID, location=LOCATION, entry_group_id=ENTRY_GROUP_ID)

        mock_client.return_value.get_entry_group.assert_called_once_with(
            request=dict(
                name=ENTRY_GROUP_PARENT,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_CATALOG_HOOK_CLIENT)
    def test_update_entry_group(self, mock_client):
        mock_common_location_path = mock_client.return_value.entry_group_path
        mock_common_location_path.return_value = ENTRY_GROUP_PARENT
        self.hook.update_entry_group(
            project_id=PROJECT_ID,
            location=LOCATION,
            entry_group_id=ENTRY_GROUP_ID,
            entry_group_configuration=ENTRY_GROUP_UPDATED_BODY,
            update_mask=UPDATE_MASK,
            validate_only=False,
        )

        mock_client.return_value.update_entry_group.assert_called_once_with(
            request=dict(
                entry_group={**ENTRY_GROUP_UPDATED_BODY, "name": ENTRY_GROUP_PARENT},
                update_mask=FieldMask(paths=UPDATE_MASK),
                validate_only=False,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_CATALOG_HOOK_CLIENT)
    def test_create_entry_type(self, mock_client):
        mock_common_location_path = mock_client.return_value.common_location_path
        mock_common_location_path.return_value = COMMON_PARENT
        self.hook.create_entry_type(
            project_id=PROJECT_ID,
            location=LOCATION,
            entry_type_id=ENTRY_TYPE_ID,
            entry_type_configuration=ENTRY_TYPE_BODY,
            validate_only=False,
        )
        mock_client.return_value.create_entry_type.assert_called_once_with(
            request=dict(
                parent=COMMON_PARENT,
                entry_type_id=ENTRY_TYPE_ID,
                entry_type=ENTRY_TYPE_BODY,
                validate_only=False,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_CATALOG_HOOK_CLIENT)
    def test_delete_entry_type(self, mock_client):
        mock_common_location_path = mock_client.return_value.entry_type_path
        mock_common_location_path.return_value = ENTRY_TYPE_PARENT
        self.hook.delete_entry_type(project_id=PROJECT_ID, location=LOCATION, entry_type_id=ENTRY_TYPE_ID)

        mock_client.return_value.delete_entry_type.assert_called_once_with(
            request=dict(
                name=ENTRY_TYPE_PARENT,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_CATALOG_HOOK_CLIENT)
    def test_list_entry_types(self, mock_client):
        mock_common_location_path = mock_client.return_value.common_location_path
        mock_common_location_path.return_value = COMMON_PARENT
        self.hook.list_entry_types(
            project_id=PROJECT_ID,
            location=LOCATION,
            order_by="name",
            page_size=2,
            filter_by="'description' = 'Some descr'",
        )
        mock_client.return_value.list_entry_types.assert_called_once_with(
            request=dict(
                parent=COMMON_PARENT,
                page_size=2,
                page_token=None,
                filter="'description' = 'Some descr'",
                order_by="name",
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_CATALOG_HOOK_CLIENT)
    def test_get_entry_type(self, mock_client):
        mock_common_location_path = mock_client.return_value.entry_type_path
        mock_common_location_path.return_value = ENTRY_TYPE_PARENT
        self.hook.get_entry_type(project_id=PROJECT_ID, location=LOCATION, entry_type_id=ENTRY_TYPE_ID)

        mock_client.return_value.get_entry_type.assert_called_once_with(
            request=dict(
                name=ENTRY_TYPE_PARENT,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )

    @mock.patch(DATAPLEX_CATALOG_HOOK_CLIENT)
    def test_update_entry_type(self, mock_client):
        mock_common_location_path = mock_client.return_value.entry_type_path
        mock_common_location_path.return_value = ENTRY_TYPE_PARENT
        self.hook.update_entry_type(
            project_id=PROJECT_ID,
            location=LOCATION,
            entry_type_id=ENTRY_TYPE_ID,
            entry_type_configuration=ENTRY_TYPE_UPDATED_BODY,
            update_mask=UPDATE_MASK,
            validate_only=False,
        )

        mock_client.return_value.update_entry_type.assert_called_once_with(
            request=dict(
                entry_type={**ENTRY_TYPE_UPDATED_BODY, "name": ENTRY_TYPE_PARENT},
                update_mask=FieldMask(paths=UPDATE_MASK),
                validate_only=False,
            ),
            retry=DEFAULT,
            timeout=None,
            metadata=(),
        )
