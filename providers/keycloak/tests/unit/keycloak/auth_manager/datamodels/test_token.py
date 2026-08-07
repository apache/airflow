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

from unittest import mock

import pytest
from pydantic import ValidationError

from airflow.providers.keycloak.auth_manager.datamodels.token import (
    TokenBody,
    TokenClientCredentialsBody,
    TokenPasswordBody,
    TokenResponse,
)


class TestTokenResponse:
    def test_model_validate_and_dump(self):
        token = TokenResponse.model_validate({"access_token": "token"})

        assert token.access_token == "token"
        assert token.model_dump() == {"access_token": "token"}

    def test_requires_access_token(self):
        with pytest.raises(ValidationError):
            TokenResponse.model_validate({})


class TestTokenBody:
    @pytest.mark.parametrize(
        ("payload", "expected_type", "expected_dump"),
        [
            (
                {"username": "username", "password": "password"},
                TokenPasswordBody,
                {"grant_type": "password", "username": "username", "password": "password"},
            ),
            (
                {"grant_type": "password", "username": "username", "password": "password"},
                TokenPasswordBody,
                {"grant_type": "password", "username": "username", "password": "password"},
            ),
            (
                {
                    "grant_type": "client_credentials",
                    "client_id": "client_id",
                    "client_secret": "client_secret",
                },
                TokenClientCredentialsBody,
                {
                    "grant_type": "client_credentials",
                    "client_id": "client_id",
                    "client_secret": "client_secret",
                },
            ),
        ],
    )
    def test_model_validate_and_dump(self, payload, expected_type, expected_dump):
        token_body = TokenBody.model_validate(payload)

        assert isinstance(token_body.root, expected_type)
        assert token_body.model_dump() == expected_dump

    @pytest.mark.parametrize(
        "payload",
        [
            {},
            {"username": None, "password": "password"},
            {"grant_type": "unsupported", "username": "username", "password": "password"},
            {"username": "username", "password": "password", "extra": "value"},
            {"grant_type": "client_credentials", "client_secret": "client_secret"},
        ],
    )
    def test_rejects_invalid_payload(self, payload):
        with pytest.raises(ValidationError):
            TokenBody.model_validate(payload)


class TestTokenPasswordBody:
    @mock.patch("airflow.providers.keycloak.auth_manager.datamodels.token.create_token_for", autospec=True)
    def test_create_token(self, mock_create_token_for):
        mock_create_token_for.return_value = "token"
        body = TokenPasswordBody(username="username", password="password")

        assert body.create_token(expiration_time_in_seconds=60) == "token"
        mock_create_token_for.assert_called_once_with("username", "password", expiration_time_in_seconds=60)


class TestTokenClientCredentialsBody:
    @mock.patch(
        "airflow.providers.keycloak.auth_manager.datamodels.token.create_client_credentials_token",
        autospec=True,
    )
    def test_create_token(self, mock_create_client_credentials_token):
        mock_create_client_credentials_token.return_value = "token"
        body = TokenClientCredentialsBody(
            grant_type="client_credentials",
            client_id="client_id",
            client_secret="client_secret",
        )

        assert body.create_token(expiration_time_in_seconds=60) == "token"
        mock_create_client_credentials_token.assert_called_once_with(
            "client_id", "client_secret", expiration_time_in_seconds=60
        )
