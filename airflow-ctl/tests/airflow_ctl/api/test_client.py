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
from unittest.mock import MagicMock, patch

import httpx
import pytest
from platformdirs import user_config_path

from airflowctl.api.client import Client, Credentials
from airflowctl.api.operations import ServerResponseError
from airflowctl.exceptions import AirflowCtlNotFoundException


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


class TestCredentials:
    default_config_dir = user_config_path("airflow", "Apache Software Foundation")

    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_SAVE"})
    @patch("airflowctl.api.client.keyring")
    def test_save(self, mock_keyring):
        mock_keyring.set_password.return_value = MagicMock()
        env = "TEST_SAVE"

        credentials = Credentials(api_url="http://localhost:8080", api_token="NO_TOKEN", api_environment=env)
        credentials.save()

        assert os.path.exists(self.default_config_dir)
        with open(os.path.join(self.default_config_dir, f"{env}.json")) as f:
            credentials = Credentials().load()
            assert json.load(f) == {
                "api_url": credentials.api_url,
            }

    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_LOAD"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch("airflowctl.api.client.keyring")
    def test_load(self, mock_keyring):
        mock_keyring.set_password.return_value = MagicMock()
        mock_keyring.get_password.return_value = "NO_TOKEN"
        env = "TEST_LOAD"

        credentials = Credentials(api_url="http://localhost:8080", api_token="NO_TOKEN", api_environment=env)
        credentials.save()
        with open(os.path.join(self.default_config_dir, f"{env}.json")) as f:
            credentials = Credentials().load()
            assert json.load(f) == {
                "api_url": credentials.api_url,
            }

    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_NO_CREDENTIALS"})
    @patch.dict(os.environ, {"AIRFLOW_CLI_TOKEN": "TEST_TOKEN"})
    @patch("airflowctl.api.client.keyring")
    def test_load_no_credentials(self, mock_keyring):
        if os.path.exists(self.default_config_dir):
            shutil.rmtree(self.default_config_dir)
        with pytest.raises(AirflowCtlNotFoundException):
            Credentials().load()

        assert not os.path.exists(self.default_config_dir)
