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

import unittest
from unittest import mock

from flask_login import current_user
from google.auth.exceptions import GoogleAuthError
from parameterized import parameterized

from airflow.www.app import create_app
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_pools


class TestGoogleOpenID(unittest.TestCase):
    def setUp(self) -> None:
        with conf_vars(
            {
                ("api", "auth_backend"): "airflow.providers.google.common.auth_backend.google_openid",
                ('api', 'enable_experimental_api'): 'true',
            }
        ):
            self.app = create_app(testing=True)

        self.appbuilder = self.app.appbuilder  # pylint: disable=no-member
        role_admin = self.appbuilder.sm.find_role("Admin")
        tester = self.appbuilder.sm.find_user(username="test")
        if not tester:
            self.appbuilder.sm.add_user(
                username="test",
                first_name="test",
                last_name="test",
                email="test@fab.org",
                role=role_admin,
                password="test",
            )

    @mock.patch("google.oauth2.id_token.verify_token")
    def test_success(self, mock_verify_token):
        clear_db_pools()
        mock_verify_token.return_value = {
            "iss": "accounts.google.com",
            "email_verified": True,
            "email": "test@fab.org",
        }

        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/experimental/pools", headers={"Authorization": "bearer JWT_TOKEN"}
            )
            self.assertEqual("test@fab.org", current_user.email)

        self.assertEqual(200, response.status_code)
        self.assertIn("Default pool", str(response.json))

    @parameterized.expand([("bearer",), ("JWT_TOKEN",), ("bearer ",)])
    @mock.patch("google.oauth2.id_token.verify_token")
    def test_malformed_headers(self, auth_header, mock_verify_token):
        mock_verify_token.return_value = {
            "iss": "accounts.google.com",
            "email_verified": True,
            "email": "test@fab.org",
        }

        with self.app.test_client() as test_client:
            response = test_client.get("/api/experimental/pools", headers={"Authorization": auth_header})

        self.assertEqual(403, response.status_code)
        self.assertEqual("Forbidden", response.data.decode())

    @mock.patch("google.oauth2.id_token.verify_token")
    def test_invalid_iss_in_jwt_token(self, mock_verify_token):
        mock_verify_token.return_value = {
            "iss": "INVALID",
            "email_verified": True,
            "email": "test@fab.org",
        }

        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/experimental/pools", headers={"Authorization": "bearer JWT_TOKEN"}
            )

        self.assertEqual(403, response.status_code)
        self.assertEqual("Forbidden", response.data.decode())

    @mock.patch("google.oauth2.id_token.verify_token")
    def test_user_not_exists(self, mock_verify_token):
        mock_verify_token.return_value = {
            "iss": "accounts.google.com",
            "email_verified": True,
            "email": "invalid@fab.org",
        }

        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/experimental/pools", headers={"Authorization": "bearer JWT_TOKEN"}
            )

        self.assertEqual(403, response.status_code)
        self.assertEqual("Forbidden", response.data.decode())

    @conf_vars({("api", "auth_backend"): "airflow.providers.google.common.auth_backend.google_openid"})
    def test_missing_id_token(self):
        with self.app.test_client() as test_client:
            response = test_client.get("/api/experimental/pools")

        self.assertEqual(403, response.status_code)
        self.assertEqual("Forbidden", response.data.decode())

    @conf_vars({("api", "auth_backend"): "airflow.providers.google.common.auth_backend.google_openid"})
    @mock.patch("google.oauth2.id_token.verify_token")
    def test_invalid_id_token(self, mock_verify_token):
        mock_verify_token.side_effect = GoogleAuthError("Invalid token")

        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/experimental/pools", headers={"Authorization": "bearer JWT_TOKEN"}
            )

        self.assertEqual(403, response.status_code)
        self.assertEqual("Forbidden", response.data.decode())
