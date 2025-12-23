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

from unittest.mock import Mock, mock_open, patch

import pytest
from github import BadCredentialsException, Github, NamedUser

from airflow.models import Connection
from airflow.providers.github.hooks.github import GithubHook

github_client_mock = Mock(name="github_client_for_test")
github_app_client_mock = Mock(name="github_app_client_for_test")


class TestGithubHook:
    # TODO: Potential performance issue, converted setup_class to a setup_connections function level fixture
    @pytest.fixture(autouse=True)
    def setup_connections(self, create_connection_without_db):
        create_connection_without_db(
            Connection(
                conn_id="github_default",
                conn_type="github",
                password="my-access-token",
                host="https://mygithub.com/api/v3",
            )
        )
        create_connection_without_db(
            Connection(
                conn_id="github_app_conn",
                conn_type="github",
                host="https://mygithub.com/api/v3",
                extra={
                    "app_id": "123456",
                    "installation_id": 654321,
                    "key_path": "FAKE_PRIVATE_KEY.pem",
                    "token_permissions": {"issues": "write", "pull_requests": "read"},
                },
            )
        )

    @patch(
        "airflow.providers.github.hooks.github.GithubClient", autospec=True, return_value=github_client_mock
    )
    def test_github_client_connection(self, github_mock):
        github_hook = GithubHook()

        assert github_mock.called
        assert isinstance(github_hook.client, Mock)
        assert github_hook.client.name == github_mock.return_value.name

    @pytest.mark.parametrize("conn_id", ["github_default", "github_app_conn"])
    @patch(
        "airflow.providers.github.hooks.github.open",
        new_callable=mock_open,
        read_data="FAKE_PRIVATE_KEY_CONTENT",
    )
    def test_connection_success(self, mock_file, conn_id):
        hook = GithubHook(github_conn_id=conn_id)
        hook.client = Mock(spec=Github)
        hook.client.get_user.return_value = NamedUser.NamedUser

        status, msg = hook.test_connection()

        assert status is True
        assert msg == "Successfully connected to GitHub."

    @pytest.mark.parametrize("conn_id", ["github_default", "github_app_conn"])
    @patch(
        "airflow.providers.github.hooks.github.open",
        new_callable=mock_open,
        read_data="FAKE_PRIVATE_KEY_CONTENT",
    )
    def test_connection_failure(self, mock_file, conn_id):
        hook = GithubHook(github_conn_id=conn_id)
        hook.client.get_user = Mock(
            side_effect=BadCredentialsException(
                status=401,
                data={"message": "Bad credentials"},
                headers={},
            )
        )
        status, msg = hook.test_connection()

        assert status is False
        assert msg == '401 {"message": "Bad credentials"}'

    @pytest.mark.parametrize(
        (
            "conn_id",
            "extra",
            "expected_error_message",
        ),
        [
            # Wrong key file extension
            (
                "invalid_key_path",
                {"app_id": "1", "installation_id": 1, "key_path": "wrong_ext.txt"},
                "Unrecognised key file: expected a .pem private key",
            ),
            # Missing key_path
            (
                "missing_key_path",
                {"app_id": "1", "installation_id": 1},
                "No key_path provided for GitHub App authentication.",
            ),
            # installation_id is not integer
            (
                "invalid_install_id",
                {"app_id": "1", "installation_id": "654321_string", "key_path": "key.pem"},
                "The provided installation_id should be integer.",
            ),
            # app_id is not integer or string
            (
                "invalid_app_id",
                {"app_id": ["123456_list"], "installation_id": 1, "key_path": "key.pem"},
                "The provided app_id should be integer or string.",
            ),
            # No access token or authentication method provided
            (
                "no_auth_conn",
                {},
                "No access token or authentication method provided.",
            ),
        ],
    )
    @patch("airflow.providers.github.hooks.github.GithubHook.get_connection")
    @patch(
        "airflow.providers.github.hooks.github.open",
        new_callable=mock_open,
        read_data="FAKE_PRIVATE_KEY_CONTENT",
    )
    def test_get_conn_value_error_cases(
        self,
        mock_file,
        get_connection_mock,
        conn_id,
        extra,
        expected_error_message,
    ):
        mock_conn = Connection(
            conn_id=conn_id,
            conn_type="github",
            extra=extra,
        )
        get_connection_mock.return_value = mock_conn

        with pytest.raises(ValueError, match=expected_error_message):
            GithubHook(github_conn_id=conn_id)
