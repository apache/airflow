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

from unittest.mock import Mock, patch

import pytest

from airflow.exceptions import AirflowException
from airflow.providers.magento.hooks.magento import MagentoHook


@pytest.fixture
def magento_hook():
    return MagentoHook(magento_conn_id="magento_default")


@patch("airflow.hooks.base.BaseHook.get_connection")
def test_initialization(mock_get_connection, magento_hook):
    mock_conn = Mock()
    mock_conn.extra_dejson = {
        "consumer_key": "key",
        "consumer_secret": "secret",
        "access_token": "token",
        "access_token_secret": "token_secret",
    }
    mock_get_connection.return_value = mock_conn

    assert magento_hook.consumer_key == "key"
    assert magento_hook.consumer_secret == "secret"
    assert magento_hook.access_token == "token"
    assert magento_hook.access_token_secret == "token_secret"


@patch("airflow.hooks.base.BaseHook.get_connection")
def test_oauth_configuration_failure(mock_get_connection):
    mock_conn = Mock()
    mock_conn.extra_dejson = {
        "consumer_key": None,
        "consumer_secret": None,
        "access_token": None,
        "access_token_secret": None,
    }
    mock_get_connection.return_value = mock_conn

    with pytest.raises(
        AirflowException, match="Magento OAuth credentials are not set properly in Airflow connection"
    ):
        MagentoHook(magento_conn_id="magento_default")


@patch("airflow.providers.magento.hooks.magento.MagentoHook.requests.get")
@patch("airflow.providers.magento.hooks.magento.MagentoHook._generate_oauth_parameters")
def test_get_request(mock_generate_oauth, mock_requests_get, magento_hook):
    mock_generate_oauth.return_value = "OAuth header"
    mock_response = Mock()
    mock_response.json.return_value = {"data": "value"}
    mock_requests_get.return_value = mock_response

    response = magento_hook.get_request("endpoint")

    mock_generate_oauth.assert_called_once_with(
        "https://magento_default/rest/default/V1/endpoint", "GET", None
    )
    mock_requests_get.assert_called_once_with(
        "https://magento_default/rest/default/V1/endpoint",
        headers={"Content-Type": "application/json", "Authorization": "OAuth header"},
    )
    assert response == {"data": "value"}


@patch("airflow.providers.magento.hooks.magento.MagentoHook.requests.request")
@patch("airflow.providers.magento.hooks.magento.MagentoHook._generate_oauth_parameters")
def test_post_request(mock_generate_oauth, mock_requests_request, magento_hook):
    mock_generate_oauth.return_value = "OAuth header"
    mock_response = Mock()
    mock_response.json.return_value = {"result": "success"}
    mock_requests_request.return_value = mock_response

    response = magento_hook.get_request("endpoint", method="POST", data={"key": "value"})

    mock_generate_oauth.assert_called_once_with(
        "https://magento_default/rest/default/V1/endpoint?key=value", "POST", {"key": "value"}
    )
    mock_requests_request.assert_called_once_with(
        "https://magento_default/rest/default/V1/endpoint?key=value",
        headers={"Content-Type": "application/json", "Authorization": "OAuth header"},
        method="POST",
        json={"key": "value"},
    )
    assert response == {"result": "success"}


@patch("airflow.providers.magento.hooks.magento.MagentoHook.requests.request")
@patch("airflow.providers.magento.hooks.magento.MagentoHook._generate_oauth_parameters")
def test_request_failure(mock_generate_oauth, mock_requests_request, magento_hook):
    mock_generate_oauth.return_value = "OAuth header"
    mock_response = Mock()
    mock_response.json.return_value = {"error": "details"}
    mock_requests_request.side_effect = Exception("Network error")

    with pytest.raises(
        AirflowException, match="Request failed: Network error. Error details: {'error': 'details'}"
    ):
        magento_hook.get_request("endpoint", method="POST", data={"key": "value"})
