import unittest
from unittest import mock

from airflow.api.client import get_current_api_client
from test_utils.config import conf_vars


class TestGetCurrentApiClient(unittest.TestCase):

    @mock.patch("airflow.api.client.json_client.Client")
    @mock.patch("airflow.api.auth.backend.default.CLIENT_AUTH", "CLIENT_AUTH")
    @conf_vars({
        ("api", 'auth_backend'): 'airflow.api.auth.backend.default',
        ("cli", 'api_client'): 'airflow.api.client.json_client',
        ("cli", 'endpoint_url'): 'http://localhost:1234',
    })
    def test_should_create_cllient(self, mock_client):
        result = get_current_api_client()

        mock_client.assert_called_once_with(
            api_base_url='http://localhost:1234', auth='CLIENT_AUTH'
        )
        self.assertEqual(mock_client.return_value, result)

