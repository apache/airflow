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

from airflow.providers.google.marketing_platform.hooks.display_video import GoogleDisplayVideo360Hook

from unit.google.cloud.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id

API_VERSION = "v4"
GCP_CONN_ID = "google_cloud_default"


class TestGoogleDisplayVideo360Hook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.hook = GoogleDisplayVideo360Hook(api_version=API_VERSION, gcp_conn_id=GCP_CONN_ID)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.display_video.GoogleDisplayVideo360Hook._authorize"
    )
    @mock.patch("airflow.providers.google.marketing_platform.hooks.display_video.build")
    def test_get_conn_to_display_video(self, mock_build, mock_authorize):
        result = self.hook.get_conn_to_display_video()
        mock_build.assert_called_once_with(
            "displayvideo",
            API_VERSION,
            http=mock_authorize.return_value,
            cache_discovery=False,
        )
        assert mock_build.return_value == result

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "display_video.GoogleDisplayVideo360Hook.get_conn_to_display_video"
    )
    def test_create_sdf_download_tasks_called_with_params(self, get_conn_to_display_video):
        body_request = {
            "version": "version",
            "partnerId": "partner_id",
            "advertiserId": "advertiser_id",
            "parentEntityFilter": "parent_entity_filter",
            "idFilter": "id_filter",
            "inventorySourceFilter": "inventory_source_filter",
        }

        self.hook.create_sdf_download_operation(body_request=body_request)

        get_conn_to_display_video.return_value.sdfdownloadtasks.return_value.create.assert_called_once_with(
            body=body_request
        )

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "display_video.GoogleDisplayVideo360Hook.get_conn_to_display_video"
    )
    def test_create_sdf_download_tasks_called_once(self, get_conn_to_display_video):
        body_request = {
            "version": "version",
            "partnerId": "partner_id",
            "advertiserId": "advertiser_id",
            "parentEntityFilter": "parent_entity_filter",
            "idFilter": "id_filter",
            "inventorySourceFilter": "inventory_source_filter",
        }

        self.hook.create_sdf_download_operation(body_request=body_request)

        get_conn_to_display_video.return_value.sdfdownloadtasks.return_value.create.assert_called_once()

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "display_video.GoogleDisplayVideo360Hook.get_conn_to_display_video"
    )
    def test_create_sdf_download_tasks_return_equal_values(self, get_conn_to_display_video):
        response = ["name"]
        body_request = {
            "version": "version",
            "partnerId": "partner_id",
            "advertiserId": "advertiser_id",
            "parentEntityFilter": "parent_entity_filter",
            "idFilter": "id_filter",
            "inventorySourceFilter": "inventory_source_filter",
        }

        # fmt: off
        get_conn_to_display_video.return_value. \
            sdfdownloadtasks.return_value. \
            create.return_value \
            .execute.return_value = response
        # fmt: on

        result = self.hook.create_sdf_download_operation(body_request=body_request)
        assert response == result

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "display_video.GoogleDisplayVideo360Hook.get_conn_to_display_video"
    )
    def test_get_sdf_download_tasks_called_with_params(self, get_conn_to_display_video):
        operation_name = "operation_name"
        self.hook.get_sdf_download_operation(operation_name=operation_name)
        # fmt: off
        get_conn_to_display_video.return_value. \
            sdfdownloadtasks.return_value. \
            operations.return_value. \
            get.assert_called_once_with(name=operation_name)
        # fmt: on

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "display_video.GoogleDisplayVideo360Hook.get_conn_to_display_video"
    )
    def test_get_sdf_download_tasks_called_once(self, get_conn_to_display_video):
        operation_name = "name"
        self.hook.get_sdf_download_operation(operation_name=operation_name)
        # fmt: off
        get_conn_to_display_video.return_value. \
            sdfdownloadtasks.return_value. \
            operations.return_value. \
            get.assert_called_once()
        # fmt: on

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "display_video.GoogleDisplayVideo360Hook.get_conn_to_display_video"
    )
    def get_sdf_download_tasks_return_equal_values(self, get_conn_to_display_video):
        operation_name = "operation"
        response = "response"

        get_conn_to_display_video.return_value.sdfdownloadtasks.return_value.operations.return_value.get = (
            response
        )

        result = self.hook.get_sdf_download_operation(operation_name=operation_name)

        assert operation_name == result

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "display_video.GoogleDisplayVideo360Hook.get_conn_to_display_video"
    )
    def test_download_media_called_once(self, get_conn_to_display_video):
        resource_name = "resource_name"

        self.hook.download_media(resource_name=resource_name)
        get_conn_to_display_video.return_value.media.return_value.download_media.assert_called_once()

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks."
        "display_video.GoogleDisplayVideo360Hook.get_conn_to_display_video"
    )
    def test_download_media_called_once_with_params(self, get_conn_to_display_video):
        resource_name = "resource_name"

        self.hook.download_media(resource_name=resource_name)
        get_conn_to_display_video.return_value.media.return_value.download_media.assert_called_once_with(
            resourceName=resource_name
        )


class TestGoogleDisplayVideo360v2Hook:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.api_version = "v2"
            self.hook = GoogleDisplayVideo360Hook(api_version=self.api_version, gcp_conn_id=GCP_CONN_ID)

    @mock.patch(
        "airflow.providers.google.marketing_platform.hooks.display_video.GoogleDisplayVideo360Hook._authorize"
    )
    @mock.patch("airflow.providers.google.marketing_platform.hooks.display_video.build")
    def test_get_conn_to_display_video(self, mock_build, mock_authorize):
        result = self.hook.get_conn_to_display_video()
        mock_build.assert_called_once_with(
            "displayvideo",
            self.api_version,
            http=mock_authorize.return_value,
            cache_discovery=False,
        )
        assert mock_build.return_value == result
