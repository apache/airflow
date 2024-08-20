import json
import os
from unittest import mock
import pytest
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.hooks.databricks_base import DEFAULT_DATABRICKS_SCOPE, BaseDatabricksHook
from airflow.utils.session import provide_session
from airflow.models import Connection
from azure.core.credentials import AccessToken
from azure.identity import WorkloadIdentityCredential
import tenacity

def create_successful_response_mock(content):
    response = mock.MagicMock()
    response.json.return_value = content
    response.status_code = 200
    return response

def create_aad_token_for_resource() -> AccessToken:
    return AccessToken(expires_on=1575500666, token='sample-token')


HOST = "xx.cloud.databricks.com"
DEFAULT_CONN_ID = "databricks_default"
DEFAULT_RETRY_NUMBER = 3
DEFAULT_RETRY_ARGS = dict(
    wait=tenacity.wait_none(),
    stop=tenacity.stop_after_attempt(DEFAULT_RETRY_NUMBER),
)

WORKLOAD_IDENTITY_SETTING_KEY = "use_azure_workload_identity"


class WorkloadIdentitySupportDatabricksHook(DatabricksHook):
    def _get_token(self, raise_error: bool = False) -> str | None:
        if self.databricks_conn.extra_dejson.get(WORKLOAD_IDENTITY_SETTING_KEY, False):
            self.log.debug("Using Azure Workload Identity authentication.")
            
            # This only works in an AKS Cluster given the following environment variables:
            # AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_FEDERATED_TOKEN_FILE
            credential = WorkloadIdentityCredential()
            
            token_obj: AccessToken = credential.get_token(DEFAULT_DATABRICKS_SCOPE)
            token: str = token_obj.token
            
            return token

        return None

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
        self._hook = WorkloadIdentitySupportDatabricksHook(retry_args=DEFAULT_RETRY_ARGS)
        
    @mock.patch.dict(os.environ, {"AZURE_CLIENT_ID": "fake-client-id", "AZURE_TENANT_ID": "fake-tenant-id", "AZURE_FEDERATED_TOKEN_FILE": '/badpath'})
    @mock.patch('azure.identity.WorkloadIdentityCredential.get_token')
    @mock.patch("airflow.providers.databricks.hooks.databricks_base.requests.get")

    def test_one(self, requests_mock, get_token_mock: mock.MagicMock):
        get_token_mock.return_value = create_aad_token_for_resource()
        requests_mock.return_value = create_successful_response_mock({"jobs": []})

        result = self._hook.list_jobs()
        
        assert result == []
