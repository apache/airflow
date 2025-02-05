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

from unittest.mock import ANY

import pytest
import requests_mock
from google.auth.exceptions import RefreshError

from airflow.providers.google.cloud.utils.external_token_supplier import (
    ClientCredentialsGrantFlowTokenSupplier,
)

MOCK_URL1 = "http://mock-idp/token1"
MOCK_URL2 = "http://mock-idp/token2"
MOCK_URL3 = "http://mock-idp/token3"
MOCK_URL4 = "http://mock-idp/token4"
CLIENT_ID = "test-client-id"
CLIENT_ID2 = "test-client-id2"
CLIENT_ID3 = "test-client-id3"
CLIENT_SECRET = "test-client-secret"
CLIENT_SECRET2 = "test-client-secret2"


class TestClientCredentialsGrantFlowTokenSupplier:
    def test_get_subject_token_success(self):
        token_supplier = ClientCredentialsGrantFlowTokenSupplier(
            oidc_issuer_url=MOCK_URL1,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        with requests_mock.Mocker() as m:
            m.post(MOCK_URL1, json={"access_token": "mock-token", "expires_in": 3600})
            token = token_supplier.get_subject_token(ANY, ANY)

        assert token == "mock-token"

    def test_cache_token_decorator(self):
        token_supplier = ClientCredentialsGrantFlowTokenSupplier(
            oidc_issuer_url=MOCK_URL2,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        with requests_mock.Mocker() as m:
            m.post(MOCK_URL2, json={"access_token": "mock-token", "expires_in": 3600})
            token = token_supplier.get_subject_token(ANY, ANY)

        assert token == "mock-token"

        # instances with same credentials and url should get previous token
        token_supplier2 = ClientCredentialsGrantFlowTokenSupplier(
            oidc_issuer_url=MOCK_URL2,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        with requests_mock.Mocker() as m2:
            m2.post(MOCK_URL2, json={"access_token": "mock-token2", "expires_in": 3600})
            token = token_supplier2.get_subject_token(ANY, ANY)

        assert token == "mock-token"

    def test_cache_token_decorator_diff_credentials(self):
        token_supplier = ClientCredentialsGrantFlowTokenSupplier(
            oidc_issuer_url=MOCK_URL3,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        with requests_mock.Mocker() as m:
            m.post(MOCK_URL3, json={"access_token": "mock-token", "expires_in": 3600})
            token = token_supplier.get_subject_token(ANY, ANY)

        assert token == "mock-token"

        # instances with different credentials and same url should get different tokens
        token_supplier2 = ClientCredentialsGrantFlowTokenSupplier(
            oidc_issuer_url=MOCK_URL3,
            client_id=CLIENT_ID2,
            client_secret=CLIENT_SECRET2,
        )

        with requests_mock.Mocker() as m2:
            m2.post(MOCK_URL3, json={"access_token": "mock-token2", "expires_in": 3600})
            token = token_supplier2.get_subject_token(ANY, ANY)

        assert token == "mock-token2"

    def test_cache_token_expiration_date(self):
        token_supplier = ClientCredentialsGrantFlowTokenSupplier(
            oidc_issuer_url=MOCK_URL4,
            client_id=CLIENT_ID,
            client_secret=CLIENT_SECRET,
        )

        with requests_mock.Mocker() as m:
            m.post(MOCK_URL4, json={"access_token": "mock-token", "expires_in": -1})
            token = token_supplier.get_subject_token(ANY, ANY)

        assert token == "mock-token"

        with requests_mock.Mocker() as m2:
            m2.post(MOCK_URL4, json={"access_token": "mock-token2", "expires_in": 3600})
            token = token_supplier.get_subject_token(ANY, ANY)

        assert token == "mock-token2"

    def test_get_subject_token_failure(self):
        token_supplier = ClientCredentialsGrantFlowTokenSupplier(
            oidc_issuer_url=MOCK_URL4,
            client_id=CLIENT_ID3,
            client_secret=CLIENT_SECRET,
        )
        with requests_mock.Mocker() as m:
            m.post(MOCK_URL4, status_code=400)

        with pytest.raises(RefreshError):
            token_supplier.get_subject_token(ANY, ANY)
