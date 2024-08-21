from __future__ import annotations

import json
import os
from unittest import mock

import pytest
import tenacity
from azure.core.credentials import AccessToken

from airflow.exceptions import AirflowException
from airflow.models import Connection
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.hooks.databricks_base import WORKLOAD_IDENTITY_SETTING_KEY
from airflow.utils.session import provide_session


def create_successful_response_mock(content):
    response = mock.MagicMock()
    response.json.return_value = content
    response.status_code = 200
    return response


def create_aad_token_for_resource() -> AccessToken:
    return AccessToken(expires_on=1575500666, token="sample-token")


HOST = "xx.cloud.databricks.com"
DEFAULT_CONN_ID = "databricks_default"
DEFAULT_RETRY_NUMBER = 3
DEFAULT_RETRY_ARGS = dict(
    wait=tenacity.wait_none(),
    stop=tenacity.stop_after_attempt(DEFAULT_RETRY_NUMBER),
)


@pytest.mark.db_test
class TestDatabricksHookAadTokenManagedIdentity:
    """
    Tests for DatabricksHook when auth is done with AAD leveraging Managed Identity authentication
    """

    _hook: DatabricksHook

    @provide_session
    def setup_method(self, method, session=None):
        conn = session.query(Connection).filter(Connection.conn_id == DEFAULT_CONN_ID).first()
        conn.host = HOST
        conn.extra = json.dumps(
            {
                WORKLOAD_IDENTITY_SETTING_KEY: True,
            }
        )
        session.commit()

        # This will use the default connection id (databricks_default)
        self._hook = DatabricksHook(retry_args=DEFAULT_RETRY_ARGS)


    def test_not_running_in_kubernetes(self):
        with pytest.raises(AirflowException) as e:
            self._hook.list_jobs()

        assert str(e.value) == "Workload identity authentication is only supporting when running in an Kubernetes cluster"

    @mock.patch.dict(
        os.environ,
        {
            "AZURE_CLIENT_ID": "fake-client-id",
            "AZURE_TENANT_ID": "fake-tenant-id",
            "AZURE_FEDERATED_TOKEN_FILE": "/badpath",
            "KUBERNETES_SERVICE_HOST": "fakeip"
        },
    )
    @mock.patch(
        "azure.identity.WorkloadIdentityCredential.get_token", return_value=create_aad_token_for_resource()
    )
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests.get")
    def test_one(self, requests_mock, get_token_mock: mock.MagicMock):
        requests_mock.return_value = create_successful_response_mock({"jobs": []})

        result = self._hook.list_jobs()

        assert result == []
