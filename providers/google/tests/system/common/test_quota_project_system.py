"""Tests for Google Cloud quota project functionality."""
from __future__ import annotations

from unittest import mock

import pytest
from google.auth.credentials import Credentials
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook
from tests.test_utils.db import clear_db_connections


class TestGoogleCloudQuotaProject:
    @classmethod
    def setup_class(cls):
        clear_db_connections()

    def setup_method(self):
        self.test_quota_project = "test-quota-project"
        self.mock_credentials = mock.MagicMock(spec=Credentials)
        self.mock_credentials.with_quota_project.return_value = self.mock_credentials

    def test_quota_project_from_param(self):
        """Test quota project specified via parameter."""
        with mock.patch(
            'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id',
            return_value=(self.mock_credentials, None)
        ):
            hook = GoogleBaseHook(quota_project_id=self.test_quota_project)
            hook.get_credentials()
            self.mock_credentials.with_quota_project.assert_called_once_with(self.test_quota_project)

    def test_quota_project_from_connection(self):
        """Test quota project specified via connection."""
        conn_id = "test-conn"
        conn = Connection(
            conn_id=conn_id,
            conn_type="google_cloud_platform",
            extra={"quota_project_id": self.test_quota_project}
        )
        conn.save()

        with mock.patch(
            'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id',
            return_value=(self.mock_credentials, None)
        ):
            hook = GoogleBaseHook(gcp_conn_id=conn_id)
            hook.get_credentials()
            self.mock_credentials.with_quota_project.assert_called_once_with(self.test_quota_project)

    def test_param_overrides_connection(self):
        """Test that parameter quota project overrides connection value."""
        conn_id = "test-conn"
        conn_quota = "connection-quota-project"
        param_quota = "parameter-quota-project"

        conn = Connection(
            conn_id=conn_id,
            conn_type="google_cloud_platform",
            extra={"quota_project_id": conn_quota}
        )
        conn.save()

        with mock.patch(
            'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id',
            return_value=(self.mock_credentials, None)
        ):
            hook = GoogleBaseHook(gcp_conn_id=conn_id, quota_project_id=param_quota)
            hook.get_credentials()
            self.mock_credentials.with_quota_project.assert_called_once_with(param_quota)

    def test_invalid_quota_project_format(self):
        """Test validation of quota project ID format."""
        invalid_ids = [
            "UPPERCASE",  # Must be lowercase
            "special@chars",  # Invalid characters
            "a",  # Too short
            "a" * 31,  # Too long
            "1starts-with-number",  # Must start with letter
            "",  # Empty string
            None,  # None value
        ]

        for invalid_id in invalid_ids:
            with mock.patch(
                'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id',
                return_value=(self.mock_credentials, None)
            ):
                hook = GoogleBaseHook(quota_project_id=invalid_id)
                with pytest.raises(AirflowException):
                    hook.get_credentials()

    def test_quota_project_credential_error(self):
        """Test handling of credential errors when setting quota project."""
        self.mock_credentials.with_quota_project.side_effect = Exception("Auth error")

        with mock.patch(
            'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.get_credentials_and_project_id',
            return_value=(self.mock_credentials, None)
        ):
            hook = GoogleBaseHook(quota_project_id=self.test_quota_project)
            with pytest.raises(AirflowException) as exc_info:
                hook.get_credentials()
            
            assert "Failed to configure quota project" in str(exc_info.value)
            assert "Auth error" in str(exc_info.value)
        
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