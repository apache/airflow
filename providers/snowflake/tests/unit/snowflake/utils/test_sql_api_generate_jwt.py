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

import pytest
from cryptography.hazmat.backends import default_backend as crypto_default_backend
from cryptography.hazmat.primitives import serialization as crypto_serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from airflow.providers.snowflake.utils.sql_api_generate_jwt import JWTGenerator

_PASSWORD = "snowflake42"

key = rsa.generate_private_key(backend=crypto_default_backend(), public_exponent=65537, key_size=2048)

private_key = key.private_bytes(
    crypto_serialization.Encoding.PEM,
    crypto_serialization.PrivateFormat.PKCS8,
    crypto_serialization.NoEncryption(),
)


class TestJWTGenerator:
    @pytest.mark.parametrize(
        "account_name, expected_account_name",
        [("test.us-east-1", "TEST"), ("test.global", "TEST.GLOBAL"), ("test", "TEST")],
    )
    def test_prepare_account_name_for_jwt(self, account_name, expected_account_name):
        """
        Test prepare_account_name_for_jwt by passing the account identifier and
        get the proper account name in caps
        """
        jwt_generator = JWTGenerator(account_name, "test_user", private_key)
        response = jwt_generator.prepare_account_name_for_jwt(account_name)
        assert response == expected_account_name

    def test_calculate_public_key_fingerprint(self):
        """Asserting get_token and calculate_public_key_fingerprint by passing key and generating token"""
        jwt_generator = JWTGenerator("test.us-east-1", "test_user", key)
        assert jwt_generator.get_token()
