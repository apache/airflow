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
from __future__ import annotations

from unittest import mock

import pytest

# For no Pydantic environment, we need to skip the tests
pytest.importorskip("google.cloud.aiplatform_v1")

from google.api_core.gapic_v1.method import DEFAULT

from airflow.providers.google.cloud.hooks.vertex_ai.dataset import DatasetHook

from providers.tests.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_PIPELINE_JOB: dict = {}
TEST_PIPELINE_JOB_ID: str = "test-pipeline-job-id"
TEST_TRAINING_PIPELINE: dict = {}
TEST_TRAINING_PIPELINE_NAME: str = "test-training-pipeline"
TEST_DATASET: dict = {}
TEST_DATASET_ID: str = "test-dataset-id"
TEST_EXPORT_CONFIG: dict = {}
TEST_ANNOTATION_SPEC: str = "test-annotation-spec"
TEST_IMPORT_CONFIGS: dict = {}
TEST_DATA_ITEM: str = "test-data-item"
TEST_UPDATE_MASK: dict = {}

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
DATASET_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.dataset.{}"


class TestVertexAIWithDefaultProjectIdHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            DatasetHook(gcp_conn_id=TEST_GCP_CONN_ID, delegate_to="delegate_to")

    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = DatasetHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_create_dataset(self, mock_client) -> None:
        self.hook.create_dataset(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.create_dataset.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                dataset=TEST_DATASET,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_delete_dataset(self, mock_client) -> None:
        self.hook.delete_dataset(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_dataset.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.dataset_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_export_data(self, mock_client) -> None:
        self.hook.export_data(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
            export_config=TEST_EXPORT_CONFIG,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.export_data.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.dataset_path.return_value,
                export_config=TEST_EXPORT_CONFIG,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_get_annotation_spec(self, mock_client) -> None:
        self.hook.get_annotation_spec(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
            annotation_spec=TEST_ANNOTATION_SPEC,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_annotation_spec.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.annotation_spec_path.return_value,
                read_mask=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.annotation_spec_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID, TEST_ANNOTATION_SPEC
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_get_dataset(self, mock_client) -> None:
        self.hook.get_dataset(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_dataset.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.dataset_path.return_value,
                read_mask=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_import_data(self, mock_client) -> None:
        self.hook.import_data(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
            import_configs=TEST_IMPORT_CONFIGS,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.import_data.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.dataset_path.return_value,
                import_configs=TEST_IMPORT_CONFIGS,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_list_annotations(self, mock_client) -> None:
        self.hook.list_annotations(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
            data_item=TEST_DATA_ITEM,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_annotations.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.data_item_path.return_value,
                filter=None,
                page_size=None,
                page_token=None,
                read_mask=None,
                order_by=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.data_item_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID, TEST_DATA_ITEM
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_list_data_items(self, mock_client) -> None:
        self.hook.list_data_items(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_data_items.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.dataset_path.return_value,
                filter=None,
                page_size=None,
                page_token=None,
                read_mask=None,
                order_by=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_list_datasets(self, mock_client) -> None:
        self.hook.list_datasets(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_datasets.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                filter=None,
                page_size=None,
                page_token=None,
                read_mask=None,
                order_by=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_update_dataset(self, mock_client) -> None:
        self.hook.update_dataset(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset_id=TEST_DATASET_ID,
            dataset=TEST_DATASET,
            update_mask=TEST_UPDATE_MASK,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.update_dataset.assert_called_once_with(
            request=dict(
                dataset=TEST_DATASET,
                update_mask=TEST_UPDATE_MASK,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )


class TestVertexAIWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = DatasetHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_create_dataset(self, mock_client) -> None:
        self.hook.create_dataset(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.create_dataset.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                dataset=TEST_DATASET,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_delete_dataset(self, mock_client) -> None:
        self.hook.delete_dataset(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_dataset.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.dataset_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_export_data(self, mock_client) -> None:
        self.hook.export_data(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
            export_config=TEST_EXPORT_CONFIG,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.export_data.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.dataset_path.return_value,
                export_config=TEST_EXPORT_CONFIG,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_get_annotation_spec(self, mock_client) -> None:
        self.hook.get_annotation_spec(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
            annotation_spec=TEST_ANNOTATION_SPEC,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_annotation_spec.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.annotation_spec_path.return_value,
                read_mask=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.annotation_spec_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID, TEST_ANNOTATION_SPEC
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_get_dataset(self, mock_client) -> None:
        self.hook.get_dataset(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_dataset.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.dataset_path.return_value,
                read_mask=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_import_data(self, mock_client) -> None:
        self.hook.import_data(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
            import_configs=TEST_IMPORT_CONFIGS,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.import_data.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.dataset_path.return_value,
                import_configs=TEST_IMPORT_CONFIGS,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_list_annotations(self, mock_client) -> None:
        self.hook.list_annotations(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
            data_item=TEST_DATA_ITEM,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_annotations.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.data_item_path.return_value,
                filter=None,
                page_size=None,
                page_token=None,
                read_mask=None,
                order_by=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.data_item_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID, TEST_DATA_ITEM
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_list_data_items(self, mock_client) -> None:
        self.hook.list_data_items(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset=TEST_DATASET_ID,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_data_items.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.dataset_path.return_value,
                filter=None,
                page_size=None,
                page_token=None,
                read_mask=None,
                order_by=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_list_datasets(self, mock_client) -> None:
        self.hook.list_datasets(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_datasets.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                filter=None,
                page_size=None,
                page_token=None,
                read_mask=None,
                order_by=None,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)

    @mock.patch(DATASET_STRING.format("DatasetHook.get_dataset_service_client"))
    def test_update_dataset(self, mock_client) -> None:
        self.hook.update_dataset(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            dataset_id=TEST_DATASET_ID,
            dataset=TEST_DATASET,
            update_mask=TEST_UPDATE_MASK,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.update_dataset.assert_called_once_with(
            request=dict(
                dataset=TEST_DATASET,
                update_mask=TEST_UPDATE_MASK,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.dataset_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_DATASET_ID
        )
