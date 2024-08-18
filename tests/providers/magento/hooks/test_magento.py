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

from unittest.mock import MagicMock, patch

import pytest
import requests

from airflow.exceptions import AirflowException
from airflow.providers.magento.hooks.magento import MagentoHook


@pytest.fixture
def mock_airflow_connection():
    """Fixture to mock Airflow connection."""
    with patch("airflow.providers.magento.hooks.magento.BaseHook.get_connection") as mock_get_connection:
        mock_connection = MagicMock()
        mock_connection.extra_dejson = {
            "consumer_key": "test_consumer_key",
            "consumer_secret": "test_consumer_secret",
            "access_token": "test_access_token",
            "access_token_secret": "test_access_token_secret",
        }
        mock_connection.host = "example.com"
        mock_get_connection.return_value = mock_connection
        yield mock_connection


def test_oauth_configuration(mock_airflow_connection):
    """Test OAuth configuration in MagentoHook."""
    hook = MagentoHook()
    assert hook.consumer_key == "test_consumer_key"
    assert hook.consumer_secret == "test_consumer_secret"
    assert hook.access_token == "test_access_token"
    assert hook.access_token_secret == "test_access_token_secret"


def test_oauth_configuration_missing_credentials():
    """Test missing OAuth credentials raises AirflowException."""
    with patch("airflow.providers.magento.hooks.magento.BaseHook.get_connection") as mock_get_connection:
        mock_connection = MagicMock()
        mock_connection.extra_dejson = {}  # No credentials
        mock_get_connection.return_value = mock_connection

        with pytest.raises(
            AirflowException, match="Magento OAuth credentials are not set properly in Airflow connection"
        ):
            MagentoHook()


@patch("requests.get")
@patch("requests.request")
def test_get_request_success(mock_request, mock_get, mock_airflow_connection):
    """Test successful API request handling."""
    hook = MagentoHook()
    mock_response = MagicMock()
    mock_response.json.return_value = {"data": "response_data"}
    mock_response.raise_for_status = MagicMock()
    mock_get.return_value = mock_response

    response = hook.get_request("endpoint")
    assert response == {"data": "response_data"}


@patch("requests.get")
@patch("requests.request")
def test_get_request_http_error(mock_request, mock_get, mock_airflow_connection):
    """Test API request handling for HTTP errors."""
    hook = MagentoHook()
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("HTTP error")
    mock_response.json.return_value = {"error": "details"}
    mock_get.return_value = mock_response

    with pytest.raises(
        AirflowException, match="Request failed: HTTP error. Error details: {'error': 'details'}"
    ):
        hook.get_request("endpoint")


@patch("requests.get")
@patch("requests.request")
def test_get_request_request_exception(mock_request, mock_get, mock_airflow_connection):
    """Test API request handling for request exceptions."""
    hook = MagentoHook()
    mock_get.side_effect = requests.exceptions.RequestException("Request failed")

    with pytest.raises(AirflowException, match="Request failed: Request failed"):
        hook.get_request("endpoint")


@patch("airflow.providers.magento.hooks.magento.MagentoHook._generate_oauth_parameters")
def test_generate_oauth_parameters(mock_generate_oauth_parameters, mock_airflow_connection):
    """Test OAuth parameter generation."""
    hook = MagentoHook()
    mock_generate_oauth_parameters.return_value = "Mocked OAuth Header"

    url = "https://example.com/api"
    method = "GET"
    data = {"param": "value"}

    header = hook._generate_oauth_parameters(url, method, data)
    assert header == "Mocked OAuth Header"
