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

import json
import os
import shutil
import tempfile
from unittest.mock import MagicMock, patch

import httpx
import pytest
from httpx import URL

from airflowctl.api.client import Client, ClientKind, Credentials
from airflowctl.api.datamodels.generated import ConnectionBody, VariableBody
from airflowctl.api.operations import ServerResponseError
from airflowctl.exceptions import AirflowCtlNotFoundException


@pytest.fixture(autouse=True)
def unique_config_dir():
    temp_dir = tempfile.mkdtemp()
    try:
        with patch.dict(os.environ, {"AIRFLOW_HOME": temp_dir}, clear=True):
            yield
    finally:
        shutil.rmtree(temp_dir, ignore_errors=True)


class TestClient:
    def test_error_parsing(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            """
            A transport handle that always returns errors
            """

            return httpx.Response(422, json={"detail": [{"loc": ["#0"], "msg": "err", "type": "required"}]})

        client = Client(base_url="", token="", mounts={"'http://": httpx.MockTransport(handle_request)})

        with pytest.raises(ServerResponseError) as err:
            client.get("http://error")

        assert isinstance(err.value, ServerResponseError)
        assert err.value.args == (
            "Client error message: {'detail': [{'loc': ['#0'], 'msg': 'err', 'type': 'required'}]}",
        )

    def test_error_parsing_plain_text(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            """
            A transport handle that always returns errors
            """

            return httpx.Response(422, content=b"Internal Server Error")

        client = Client(base_url="", token="", mounts={"'http://": httpx.MockTransport(handle_request)})

        with pytest.raises(httpx.HTTPStatusError) as err:
            client.get("http://error")
        assert not isinstance(err.value, ServerResponseError)

    def test_error_parsing_other_json(self):
        def handle_request(request: httpx.Request) -> httpx.Response:
            # Some other json than an error body.
            return httpx.Response(404, json={"detail": "Not found"})

        client = Client(base_url="", token="", mounts={"'http://": httpx.MockTransport(handle_request)})

        with pytest.raises(ServerResponseError) as err:
            client.get("http://error")
        assert err.value.args == ("Client error message: {'detail': 'Not found'}",)

    @pytest.mark.parametrize(
        ("base_url", "client_kind", "expected_base_url"),
        [
            ("http://localhost:8080", ClientKind.CLI, "http://localhost:8080/api/v2/"),
            ("http://localhost:8080", ClientKind.AUTH, "http://localhost:8080/auth/"),
            ("https://example.com", ClientKind.CLI, "https://example.com/api/v2/"),
            ("https://example.com", ClientKind.AUTH, "https://example.com/auth/"),
        ],
    )
    def test_refresh_base_url(self, base_url, client_kind, expected_base_url):
        client = Client(base_url="", token="", mounts={})
        client.refresh_base_url(base_url=base_url, kind=client_kind)
        assert client.base_url == URL(expected_base_url)

    @pytest.mark.parametrize(
        "datamodel, input_data, expected_cleaned_data",
        [
            (
                VariableBody,
                {"key": "example_key", "value": None, "description": None},
                {"key": "example_key", "value": None},
            ),
            (
                VariableBody,
                {"key": "example_key", "value": None, "description": "A variable"},
                {"key": "example_key", "value": None, "description": "A variable"},
            ),
            (
                ConnectionBody,
                {"connection_id": "example_conn", "conn_type": "mysql", "host": "", "port": None},
                {"connection_id": "example_conn", "conn_type": "mysql", "host": ""},
            ),
            (
                ConnectionBody,
                {"connection_id": "example_conn", "conn_type": "mysql", "port": None},
                {"connection_id": "example_conn", "conn_type": "mysql"},
            ),
            (
                ConnectionBody,
                {"connection_id": "example_conn", "conn_type": "postgres", "host": "localhost", "port": 5432},
                {"connection_id": "example_conn", "conn_type": "postgres", "host": "localhost", "port": 5432},
            ),
        ],
    )
    def test_clean_json_content(self, datamodel, input_data, expected_cleaned_data):
        client = Client(base_url="", token="", mounts={})
        client.datamodel = [datamodel]

        cleaned_data = client._clean_empty_values(data=input_data)
        assert cleaned_data == expected_cleaned_data


class TestCredentials:
    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_SAVE"})
    @patch("airflowctl.api.client.keyring")
    def test_save(self, mock_keyring):
        mock_keyring.set_password.return_value = MagicMock()
        env = "TEST_SAVE"
        cli_client = ClientKind.CLI
        credentials = Credentials(
            api_url="http://localhost:8080", api_token="NO_TOKEN", api_environment=env, client_kind=cli_client
        )
        credentials.save()

        config_dir = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
        assert os.path.exists(config_dir)
        with open(os.path.join(config_dir, f"{env}.json")) as f:
            credentials = Credentials(client_kind=cli_client).load()
            assert json.load(f) == {
                "api_url": credentials.api_url,
            }

    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_LOAD"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch("airflowctl.api.client.keyring")
    def test_load_cli_kind(self, mock_keyring):
        mock_keyring.set_password.return_value = MagicMock()
        mock_keyring.get_password.return_value = "NO_TOKEN"
        env = "TEST_LOAD"
        cli_client = ClientKind.CLI
        credentials = Credentials(
            api_url="http://localhost:8080", api_token="NO_TOKEN", api_environment=env, client_kind=cli_client
        )
        credentials.save()
        config_dir = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
        with open(os.path.join(config_dir, f"{env}.json")) as f:
            credentials = Credentials(client_kind=cli_client).load()
            assert json.load(f) == {
                "api_url": credentials.api_url,
            }

    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_LOAD"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch("airflowctl.api.client.keyring")
    def test_load_auth_kind(self, mock_keyring):
        mock_keyring.set_password.return_value = MagicMock()
        mock_keyring.get_password.return_value = "NO_TOKEN"
        auth_client = ClientKind.AUTH
        credentials = Credentials(client_kind=auth_client)
        assert credentials.api_url is None

    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_NO_CREDENTIALS"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch("airflowctl.api.client.keyring")
    def test_load_no_credentials(self, mock_keyring):
        cli_client = ClientKind.CLI
        config_dir = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
        if os.path.exists(config_dir):
            shutil.rmtree(config_dir)
        with pytest.raises(AirflowCtlNotFoundException):
            Credentials(client_kind=cli_client).load()

        assert not os.path.exists(config_dir)
