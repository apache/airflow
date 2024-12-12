from __future__ import annotations

from unittest import mock

import pytest
from google.api_core.client_options import ClientOptions
from google.cloud.aiplatform_v1beta1 import FeatureOnlineStoreAdminServiceClient

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.vertex_ai.feature_store import FeatureStoreHook
from airflow.providers.google.common.hooks.base_google import PROVIDE_PROJECT_ID
from airflow.providers.google.common.consts import CLIENT_INFO
from providers.tests.google.cloud.utils.base_gcp_mock import (
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
        # Test with location
        result = self.hook.get_feature_online_store_admin_service_client(location=TEST_LOCATION)
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=mock.ANY,
            client_options=mock.ANY
        )
        client_options = mock_client.call_args[1]["client_options"]
        assert client_options.api_endpoint == f"{TEST_LOCATION}-aiplatform.googleapis.com:443"

        # Test without location (global)
        mock_client.reset_mock()
        result = self.hook.get_feature_online_store_admin_service_client()
        mock_client.assert_called_once_with(
            credentials=mock_get_credentials.return_value,
            client_info=mock.ANY,
            client_options=mock.ANY
        )
        client_options = mock_client.call_args[1]["client_options"]
        assert not client_options.api_endpoint

    @mock.patch(FEATURE_STORE_STRING.format("FeatureStoreHook.get_feature_online_store_admin_service_client"))
    def test_get_feature_view_sync(self, mock_client_getter):
        mock_client = mock.MagicMock()
        mock_client_getter.return_value = mock_client
        mock_response = mock.MagicMock()
        mock_client.get_feature_view_sync.return_value = mock_response

        result = self.hook.get_feature_view_sync(
            location=TEST_LOCATION,
            feature_view_sync_name=TEST_FEATURE_VIEW_SYNC_NAME,
        )

        mock_client.get_feature_view_sync.assert_called_once_with(
            name=TEST_FEATURE_VIEW_SYNC_NAME
        )
        assert result == mock_response

    @mock.patch(FEATURE_STORE_STRING.format("FeatureStoreHook.get_feature_online_store_admin_service_client"))
    def test_sync_feature_view(self, mock_client_getter):
        mock_client = mock.MagicMock()
        mock_client_getter.return_value = mock_client
        mock_response = mock.MagicMock()
        mock_client.sync_feature_view.return_value = mock_response

        result = self.hook.sync_feature_view(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            feature_online_store_id=TEST_FEATURE_ONLINE_STORE_ID,
            feature_view_id=TEST_FEATURE_VIEW_ID,
        )

        mock_client.sync_feature_view.assert_called_once_with(
            feature_view=TEST_FEATURE_VIEW
        )
        assert result == mock_response

    @mock.patch(FEATURE_STORE_STRING.format("FeatureStoreHook.get_feature_online_store_admin_service_client"))
    def test_list_feature_view_syncs(self, mock_client_getter):
        mock_client = mock.MagicMock()
        mock_client_getter.return_value = mock_client
        mock_response = mock.MagicMock()
        mock_client.list_feature_views.return_value = mock_response

        result = self.hook.list_feature_view_syncs(
            project_id=TEST_PROJECT_ID,
            location=TEST_LOCATION,
            feature_online_store_id=TEST_FEATURE_ONLINE_STORE_ID,
            feature_view_id=TEST_FEATURE_VIEW_ID,
        )

        mock_client.list_feature_views.assert_called_once_with(
            parent=TEST_FEATURE_VIEW
        )
        assert result == mock_response
