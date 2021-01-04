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

from airflow.www.app import create_app
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_pools


class TestGoogleOpenID(unittest.TestCase):
    def setUp(self) -> None:
        with conf_vars(
            {("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}
        ), mock.patch.dict('os.environ', SKIP_DAGS_PARSING='True'), conf_vars(
            {('api', 'enable_experimental_api'): 'true'}
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

    def test_success_using_username(self):
        clear_db_pools()

        with self.app.test_client() as test_client:
            response = test_client.get("/api/experimental/pools", environ_overrides={'REMOTE_USER': "test"})
            self.assertEqual("test@fab.org", current_user.email)

        self.assertEqual(200, response.status_code)
        self.assertIn("Default pool", str(response.json))

    def test_success_using_email(self):
        clear_db_pools()

        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/experimental/pools", environ_overrides={'REMOTE_USER': "test@fab.org"}
            )
            self.assertEqual("test@fab.org", current_user.email)

        self.assertEqual(200, response.status_code)
        self.assertIn("Default pool", str(response.json))

    def test_user_not_exists(self):
        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/experimental/pools", environ_overrides={'REMOTE_USER': "INVALID"}
            )

        self.assertEqual(403, response.status_code)
        self.assertEqual("Forbidden", response.data.decode())

    def test_missing_remote_user(self):
        with self.app.test_client() as test_client:
            response = test_client.get("/api/experimental/pools")

        self.assertEqual(403, response.status_code)
        self.assertEqual("Forbidden", response.data.decode())

    def test_user_should_be_logged_temporary(self):
        with self.app.test_client() as test_client:
            response = test_client.get(
                "/api/experimental/pools", environ_overrides={'REMOTE_USER': "INVALID"}
            )

        self.assertEqual(403, response.status_code)
        self.assertEqual("Forbidden", response.data.decode())
