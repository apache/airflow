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

from airflow.providers.google.cloud.hooks.vertex_ai.feature_store import FeatureStoreHook

from unit.google.cloud.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id,
)

BASE_STRING = "airflow.providers.google.common.hooks.base_google.{}"
FEATURE_STORE_STRING = "airflow.providers.google.cloud.hooks.vertex_ai.feature_store.{}"

TEST_GCP_CONN_ID = "test-gcp-conn-id"
TEST_PROJECT_ID = "test-project"
TEST_LOCATION = "us-central1"
TEST_FEATURE_ONLINE_STORE_ID = "test-store"
TEST_FEATURE_VIEW_ID = "test-view"
TEST_FEATURE_VIEW = f"projects/{TEST_PROJECT_ID}/locations/{TEST_LOCATION}/featureOnlineStores/{TEST_FEATURE_ONLINE_STORE_ID}/featureViews/{TEST_FEATURE_VIEW_ID}"
TEST_FEATURE_VIEW_SYNC_NAME = f"{TEST_FEATURE_VIEW}/featureViewSyncs/sync123"


class TestFeatureStoreHook:
    def setup_method(self):
        with mock.patch(
            BASE_STRING.format("GoogleBaseHook.__init__"), new=mock_base_gcp_hook_default_project_id
        ):
            self.hook = FeatureStoreHook(gcp_conn_id=TEST_GCP_CONN_ID)

    @mock.patch(FEATURE_STORE_STRING.format("FeatureOnlineStoreAdminServiceClient"), autospec=True)
    @mock.patch(BASE_STRING.format("GoogleBaseHook.get_credentials"))
    def test_get_feature_online_store_admin_service_client(self, mock_get_credentials, mock_client):
        self.hook.get_feature_online_store_admin_service_client(location=TEST_LOCATION)
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value, client_info=mock.ANY, client_options=mock.ANY
        )
        client_options = mock_client.call_args[1]["client_options"]
        assert client_options.api_endpoint == f"{TEST_LOCATION}-aiplatform.googleapis.com:443"

        mock_client.reset_mock()
        self.hook.get_feature_online_store_admin_service_client()
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value, client_info=mock.ANY, client_options=mock.ANY
        )
        client_options = mock_client.call_args[1]["client_options"]
        assert not client_options.api_endpoint

    @mock.patch(FEATURE_STORE_STRING.format("FeatureStoreHook.get_feature_online_store_admin_service_client"))
    def test_get_feature_view_sync(self, mock_client_getter):
        mock_client = mock.MagicMock()
        mock_client_getter.return_value = mock_client

        # Create a mock response with the expected structure
        mock_response = mock.MagicMock()
        mock_response.run_time.start_time.seconds = 1
        mock_response.run_time.end_time.seconds = 1
        mock_response.sync_summary.row_synced = 1
        mock_response.sync_summary.total_slot = 1

        mock_client.get_feature_view_sync.return_value = mock_response

        expected_result = {
            "name": TEST_FEATURE_VIEW_SYNC_NAME,
            "start_time": 1,
            "end_time": 1,
            "sync_summary": {"row_synced": 1, "total_slot": 1},
        }

        result = self.hook.get_feature_view_sync(
            location=TEST_LOCATION,
            feature_view_sync_name=TEST_FEATURE_VIEW_SYNC_NAME,
        )

        mock_client.get_feature_view_sync.assert_called_once_with(name=TEST_FEATURE_VIEW_SYNC_NAME)
        assert result == expected_result

    @mock.patch(FEATURE_STORE_STRING.format("FeatureStoreHook.get_feature_online_store_admin_service_client"))
    def test_sync_feature_view(self, mock_client_getter):
        mock_client = mock.MagicMock()
        mock_client_getter.return_value = mock_client

        # Create a mock response with the expected structure
        mock_response = mock.MagicMock()
        mock_response.feature_view_sync = "test-sync-operation-name"
        mock_client.sync_feature_view.return_value = mock_response

        result = self.hook.sync_feature_view(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            feature_online_store_id=TEST_FEATURE_ONLINE_STORE_ID,
            feature_view_id=TEST_FEATURE_VIEW_ID,
        )

        mock_client.sync_feature_view.assert_called_once_with(feature_view=TEST_FEATURE_VIEW)
        assert result == "test-sync-operation-name"
