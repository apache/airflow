from __future__ import annotations

import json
import os
from unittest import mock

import google.auth
import pytest
from google.auth.environment_vars import CREDENTIALS
from google.oauth2.credentials import Credentials

from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

TEST_QUOTA_PROJECT_ID = "test-quota-project"
TEST_CONN_ID = "google-cloud-default"


class TestGoogleBaseHookQuotaProject:
    def setup_method(self):
        with mock.patch(
            "airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__",
            new=mock_init_no_args,
        ):
            self.hook = GoogleBaseHook(gcp_conn_id=TEST_CONN_ID)

    def test_quota_project_id_init(self):
        """Test that quota project ID is properly initialized."""
        hook = GoogleBaseHook(gcp_conn_id=TEST_CONN_ID, quota_project_id=TEST_QUOTA_PROJECT_ID)
        assert hook.quota_project_id == TEST_QUOTA_PROJECT_ID

    @mock.patch("google.auth._default._load_credentials_from_file")
    @mock.patch("os.environ", {CREDENTIALS: "/test/key-path"})
    def test_quota_project_id_from_connection(self, mock_load_creds):
        """Test that quota project ID from connection is applied to credentials."""
        mock_creds = mock.MagicMock()
        mock_creds.with_quota_project.return_value = mock_creds
        mock_load_creds.return_value = (mock_creds, "test-project")

        # Mock connection with quota_project_id in extras
        uri = f"google-cloud-platform://?extras={json.dumps({'quota_project_id': TEST_QUOTA_PROJECT_ID})}"
        with mock.patch.dict("os.environ", {"AIRFLOW_CONN_" + TEST_CONN_ID.upper(): uri}):
            creds, _ = self.hook.get_credentials_and_project_id()
            mock_creds.with_quota_project.assert_called_once_with(TEST_QUOTA_PROJECT_ID)

    @mock.patch("google.auth._default._load_credentials_from_file")
    @mock.patch("os.environ", {CREDENTIALS: "/test/key-path"})
    def test_quota_project_id_param_overrides_connection(self, mock_load_creds):
        """Test that quota project ID from param overrides connection value."""
        mock_creds = mock.MagicMock()
        mock_creds.with_quota_project.return_value = mock_creds
        mock_load_creds.return_value = (mock_creds, "test-project")

        # Connection with quota_project_id in extras
        conn_quota = "connection-quota-project"
        uri = f"google-cloud-platform://?extras={json.dumps({'quota_project_id': conn_quota})}"
        
        with mock.patch.dict("os.environ", {"AIRFLOW_CONN_" + TEST_CONN_ID.upper(): uri}):
            hook = GoogleBaseHook(gcp_conn_id=TEST_CONN_ID, quota_project_id=TEST_QUOTA_PROJECT_ID)
            creds, _ = hook.get_credentials_and_project_id()
            
            # Should use param quota project, not connection quota project
            mock_creds.with_quota_project.assert_called_once_with(TEST_QUOTA_PROJECT_ID)


def mock_init_no_args(self, **kwargs):
    pass