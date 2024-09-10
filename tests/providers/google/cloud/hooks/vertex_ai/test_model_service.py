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

from airflow.providers.google.cloud.hooks.vertex_ai.model_service import ModelServiceHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

TEST_GCP_CONN_ID: str = "test-gcp-conn-id"
TEST_REGION: str = "test-region"
TEST_PROJECT_ID: str = "test-project-id"
TEST_MODEL = None
TEST_PARENT_MODEL = "test-parent-model"
TEST_MODEL_NAME: str = "test_model_name"
TEST_OUTPUT_CONFIG: dict = {}

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
MODEL_SERVICE_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.model_service.{}"
TEST_VERSION_ALIASES = ["new-alias"]


class TestModelServiceWithDefaultProjectIdHook:
    def test_delegate_to_runtime_error(self):
        with pytest.raises(RuntimeError):
            ModelServiceHook(gcp_conn_id=TEST_GCP_CONN_ID, delegate_to="delegate_to")

    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = ModelServiceHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_delete_model(self, mock_client) -> None:
        self.hook.delete_model(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            model=TEST_MODEL_NAME,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_model.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.model_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_REGION,
            TEST_MODEL_NAME,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_export_model(self, mock_client) -> None:
        self.hook.export_model(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            model=TEST_MODEL_NAME,
            output_config=TEST_OUTPUT_CONFIG,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.export_model.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
                output_config=TEST_OUTPUT_CONFIG,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.model_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_MODEL_NAME
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_list_models(self, mock_client) -> None:
        self.hook.list_models(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_models.assert_called_once_with(
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

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_upload_model(self, mock_client) -> None:
        self.hook.upload_model(project_id=TEST_PROJECT_ID, region=TEST_REGION, model=TEST_MODEL)
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.upload_model.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                model=TEST_MODEL,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_upload_model_with_parent_model(self, mock_client) -> None:
        self.hook.upload_model(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, model=TEST_MODEL, parent_model=TEST_PARENT_MODEL
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.upload_model.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                model=TEST_MODEL,
                parent_model=TEST_PARENT_MODEL,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_list_model_versions(self, mock_client) -> None:
        self.hook.list_model_versions(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, model_id=TEST_MODEL_NAME
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_model_versions.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_delete_model_version(self, mock_client) -> None:
        self.hook.delete_model_version(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, model_id=TEST_MODEL_NAME
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_model_version.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_get_model(self, mock_client) -> None:
        self.hook.get_model(project_id=TEST_PROJECT_ID, region=TEST_REGION, model_id=TEST_MODEL_NAME)
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_model.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_set_version_as_default(self, mock_client) -> None:
        self.hook.set_version_as_default(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, model_id=TEST_MODEL_NAME
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.merge_version_aliases.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
                version_aliases=["default"],
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_add_version_aliases(self, mock_client) -> None:
        self.hook.add_version_aliases(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            model_id=TEST_MODEL_NAME,
            version_aliases=TEST_VERSION_ALIASES,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.merge_version_aliases.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
                version_aliases=TEST_VERSION_ALIASES,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_delete_version_aliases(self, mock_client) -> None:
        self.hook.delete_version_aliases(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            model_id=TEST_MODEL_NAME,
            version_aliases=TEST_VERSION_ALIASES,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.merge_version_aliases.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
                version_aliases=["-" + alias for alias in TEST_VERSION_ALIASES],
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )


class TestModelServiceWithoutDefaultProjectIdHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_no_default_project_id
        ):
            self.hook = ModelServiceHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_delete_model(self, mock_client) -> None:
        self.hook.delete_model(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            model=TEST_MODEL_NAME,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_model.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.model_path.assert_called_once_with(
            TEST_PROJECT_ID,
            TEST_REGION,
            TEST_MODEL_NAME,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_export_model(self, mock_client) -> None:
        self.hook.export_model(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            model=TEST_MODEL_NAME,
            output_config=TEST_OUTPUT_CONFIG,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.export_model.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
                output_config=TEST_OUTPUT_CONFIG,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.model_path.assert_called_once_with(
            TEST_PROJECT_ID, TEST_REGION, TEST_MODEL_NAME
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_list_models(self, mock_client) -> None:
        self.hook.list_models(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_models.assert_called_once_with(
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

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_upload_model(self, mock_client) -> None:
        self.hook.upload_model(project_id=TEST_PROJECT_ID, region=TEST_REGION, model=TEST_MODEL)
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.upload_model.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                model=TEST_MODEL,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_upload_model_with_parent_model(self, mock_client) -> None:
        self.hook.upload_model(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, model=TEST_MODEL, parent_model=TEST_PARENT_MODEL
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.upload_model.assert_called_once_with(
            request=dict(
                parent=mock_client.return_value.common_location_path.return_value,
                model=TEST_MODEL,
                parent_model=TEST_PARENT_MODEL,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
        mock_client.return_value.common_location_path.assert_called_once_with(TEST_PROJECT_ID, TEST_REGION)

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_list_model_versions(self, mock_client) -> None:
        self.hook.list_model_versions(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, model_id=TEST_MODEL_NAME
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.list_model_versions.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_delete_model_version(self, mock_client) -> None:
        self.hook.delete_model_version(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, model_id=TEST_MODEL_NAME
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.delete_model_version.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_get_model(self, mock_client) -> None:
        self.hook.get_model(project_id=TEST_PROJECT_ID, region=TEST_REGION, model_id=TEST_MODEL_NAME)
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.get_model.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_set_version_as_default(self, mock_client) -> None:
        self.hook.set_version_as_default(
            project_id=TEST_PROJECT_ID, region=TEST_REGION, model_id=TEST_MODEL_NAME
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.merge_version_aliases.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
                version_aliases=["default"],
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_add_version_aliases(self, mock_client) -> None:
        self.hook.add_version_aliases(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            model_id=TEST_MODEL_NAME,
            version_aliases=TEST_VERSION_ALIASES,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.merge_version_aliases.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
                version_aliases=TEST_VERSION_ALIASES,
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )

    @mock.patch(MODEL_SERVICE_STRING.format("ModelServiceHook.get_model_service_client"))
    def test_delete_version_aliases(self, mock_client) -> None:
        self.hook.delete_version_aliases(
            project_id=TEST_PROJECT_ID,
            region=TEST_REGION,
            model_id=TEST_MODEL_NAME,
            version_aliases=TEST_VERSION_ALIASES,
        )
        mock_client.assert_called_once_with(TEST_REGION)
        mock_client.return_value.merge_version_aliases.assert_called_once_with(
            request=dict(
                name=mock_client.return_value.model_path.return_value,
                version_aliases=["-" + alias for alias in TEST_VERSION_ALIASES],
            ),
            metadata=(),
            retry=DEFAULT,
            timeout=None,
        )
