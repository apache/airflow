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

from airflowctl.api.client import Client, ClientKind, Credentials, _bounded_get_new_password
from airflowctl.api.operations import ServerResponseError
from airflowctl.exceptions import AirflowCtlCredentialNotFoundException, AirflowCtlKeyringException


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
            ("http://localhost:8080/", ClientKind.CLI, "http://localhost:8080/api/v2/"),
            ("http://localhost:8080/", ClientKind.AUTH, "http://localhost:8080/auth/"),
            ("https://example.com/", ClientKind.CLI, "https://example.com/api/v2/"),
            ("https://example.com/", ClientKind.AUTH, "https://example.com/auth/"),
        ],
    )
    def test_refresh_base_url(self, base_url, client_kind, expected_base_url):
        client = Client(base_url="", token="", mounts={})
        client.refresh_base_url(base_url=base_url, kind=client_kind)
        assert client.base_url == URL(expected_base_url)


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

    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_SAVE_NO_KEYRING"})
    @patch("airflowctl.api.client.keyring")
    def test_save_no_keyring(self, mock_keyring):
        from keyring.errors import NoKeyringError

        cli_client = ClientKind.CLI
        mock_keyring.set_password.side_effect = NoKeyringError("no backend")

        with pytest.raises(AirflowCtlKeyringException, match="Keyring backend is not available"):
            Credentials(client_kind=cli_client).save()

    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_SAVE_SKIP_KEYRING"})
    @patch("airflowctl.api.client.keyring")
    def test_save_no_keyring_backend_skip_keyring(self, mock_keyring):

        env = "TEST_SAVE_SKIP_KEYRING"
        cli_client = ClientKind.CLI
        mock_keyring.set_password = MagicMock()
        mock_keyring.get_password = MagicMock()

        Credentials(client_kind=cli_client).save(skip_keyring=True)

        config_dir = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
        assert os.path.exists(config_dir)
        with open(os.path.join(config_dir, f"{env}.json")) as f:
            credentials = Credentials(client_kind=cli_client, api_token="TEST_TOKEN").load()
            assert json.load(f) == {
                "api_url": credentials.api_url,
            }
        mock_keyring.set_password.assert_not_called()
        mock_keyring.get_password.assert_not_called()

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
        with pytest.raises(AirflowCtlCredentialNotFoundException, match="No credentials file found"):
            Credentials(client_kind=cli_client).load()

        assert not os.path.exists(config_dir)

    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_KEYRING_VALUE_ERROR"})
    @patch("airflowctl.api.client.keyring")
    def test_load_incorrect_keyring_password(self, mock_keyring):
        cli_client = ClientKind.CLI
        config_dir = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
        os.makedirs(config_dir, exist_ok=True)
        with open(os.path.join(config_dir, "TEST_KEYRING_VALUE_ERROR.json"), "w") as f:
            json.dump({"api_url": "http://localhost:8080"}, f)
        mock_keyring.get_password.side_effect = ValueError("incorrect password")

        with pytest.raises(AirflowCtlKeyringException, match="Incorrect keyring password"):
            Credentials(client_kind=cli_client).load()

    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_NO_KEYRING_BACKEND"})
    @patch("airflowctl.api.client.keyring")
    def test_load_no_keyring_backend(self, mock_keyring):
        from keyring.errors import NoKeyringError

        cli_client = ClientKind.CLI
        config_dir = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
        os.makedirs(config_dir, exist_ok=True)
        with open(os.path.join(config_dir, "TEST_NO_KEYRING_BACKEND.json"), "w") as f:
            json.dump({"api_url": "http://localhost:8080"}, f)
        mock_keyring.get_password.side_effect = NoKeyringError("no backend")

        with pytest.raises(AirflowCtlKeyringException, match="Keyring backend is not available"):
            Credentials(client_kind=cli_client).load()

    @patch.dict(os.environ, {"AIRFLOW_CLI_ENVIRONMENT": "TEST_NO_KEYRING_BACKEND"})
    @patch("airflowctl.api.client.keyring")
    def test_load_no_keyring_backend_token_provided(self, mock_keyring):
        from keyring.errors import NoKeyringError

        cli_client = ClientKind.CLI
        config_dir = os.environ.get("AIRFLOW_HOME", os.path.expanduser("~/airflow"))
        os.makedirs(config_dir, exist_ok=True)
        with open(os.path.join(config_dir, "TEST_NO_KEYRING_BACKEND.json"), "w") as f:
            json.dump({"api_url": "http://localhost:8080"}, f)
        mock_keyring.get_password.side_effect = NoKeyringError("no backend")

        credentials = Credentials(client_kind=cli_client, api_token="TEST_TOKEN").load()
        assert credentials.api_token == "TEST_TOKEN"


class TestBoundedGetNewPassword:
    @patch("airflowctl.api.client.getpass.getpass")
    def test_success_on_first_attempt(self, mock_getpass):
        mock_getpass.side_effect = ["mypassword", "mypassword"]
        assert _bounded_get_new_password() == "mypassword"

    @patch("airflowctl.api.client.getpass.getpass")
    def test_success_after_mismatch(self, mock_getpass, capsys):
        mock_getpass.side_effect = ["wrong1", "wrong2", "mypassword", "mypassword"]
        assert _bounded_get_new_password() == "mypassword"
        assert "Your passwords didn't match" in capsys.readouterr().err

    @patch("airflowctl.api.client.getpass.getpass")
    def test_success_after_blank(self, mock_getpass, capsys):
        mock_getpass.side_effect = ["", "", "mypassword", "mypassword"]
        assert _bounded_get_new_password() == "mypassword"
        assert "Blank passwords aren't allowed" in capsys.readouterr().err

    @patch("airflowctl.api.client.getpass.getpass")
    def test_exhausts_attempts_on_mismatch(self, mock_getpass):
        mock_getpass.side_effect = ["a", "b", "c", "d", "e", "f"]
        with pytest.raises(
            AirflowCtlKeyringException, match="Failed to set keyring password after 3 attempts"
        ):
            _bounded_get_new_password()

    @patch("airflowctl.api.client.getpass.getpass")
    def test_exhausts_attempts_on_blank(self, mock_getpass):
        mock_getpass.side_effect = ["", "", "  ", "  ", " ", " "]
        with pytest.raises(
            AirflowCtlKeyringException, match="Failed to set keyring password after 3 attempts"
        ):
            _bounded_get_new_password()


class TestSaveKeyringPatching:
    @patch("airflowctl.api.client.keyring")
    def test_save_patches_direct_encrypted_keyring_backend(self, mock_keyring):
        mock_backend = MagicMock()
        mock_backend.backends = []  # no chained children
        mock_keyring.get_keyring.return_value = mock_backend
        mock_keyring.set_password.return_value = None

        Credentials(api_url="http://localhost:8080", api_token="token", client_kind=ClientKind.CLI).save()

        assert mock_backend._get_new_password == _bounded_get_new_password

    @patch("airflowctl.api.client.keyring")
    def test_save_patches_encrypted_keyring_inside_chainer(self, mock_keyring):
        encrypted_backend = MagicMock()  # has _get_new_password (MagicMock default)
        chainer = MagicMock(spec=object)  # no _get_new_password on chainer itself
        chainer.backends = [encrypted_backend]
        mock_keyring.get_keyring.return_value = chainer
        mock_keyring.set_password.return_value = None

        Credentials(api_url="http://localhost:8080", api_token="token", client_kind=ClientKind.CLI).save()

        assert not hasattr(chainer, "_get_new_password")
        assert encrypted_backend._get_new_password == _bounded_get_new_password

    @patch("airflowctl.api.client.keyring")
    def test_save_skips_patch_for_non_encrypted_backend(self, mock_keyring):
        mock_backend = MagicMock(spec=object)
        mock_keyring.get_keyring.return_value = mock_backend
        mock_keyring.set_password.return_value = None

        Credentials(api_url="http://localhost:8080", api_token="token", client_kind=ClientKind.CLI).save()

        assert not hasattr(mock_backend, "_get_new_password")
        mock_keyring.set_password.assert_called_once_with("airflowctl", "api_token_production", "token")
