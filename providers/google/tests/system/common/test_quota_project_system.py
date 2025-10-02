"""System test for Google Cloud quota project functionality."""
from __future__ import annotations

from unittest import mock

from google.auth.credentials import Credentials
from airflow.models.connection import Connection
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from tests.test_utils.db import clear_db_connections


class TestGoogleCloudQuotaProject:
    @classmethod
    def setup_class(cls):
        clear_db_connections()

    def test_quota_project_propagation(self):
        """Test that quota project is properly propagated to Google client libraries."""
        test_quota_project = "test-quota-project"
        mock_credentials = mock.MagicMock(spec=Credentials)
        
        with mock.patch(
            'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id',
            return_value=(mock_credentials, None)
        ):
            # Test with direct parameter
            hook = GoogleBaseHook(quota_project_id=test_quota_project)
            credentials = hook.get_credentials()
            mock_credentials.with_quota_project.assert_called_once_with(test_quota_project)

            # Test with connection extra
            connection_id = "google-cloud-quota-test"
            connection = Connection(
                conn_id=connection_id,
                conn_type="google-cloud-platform",
                extra={"quota_project_id": test_quota_project}
            )
            connection.save()

            hook = GoogleBaseHook(gcp_conn_id=connection_id)
            credentials = hook.get_credentials()
            assert mock_credentials.with_quota_project.call_count == 2