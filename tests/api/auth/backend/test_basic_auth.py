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
from base64 import b64encode

from flask_login import current_user
from parameterized import parameterized

from airflow.www.app import create_app
from tests.test_utils.api_connexion_utils import assert_401
from tests.test_utils.config import conf_vars
from tests.test_utils.db import clear_db_pools


class TestBasicAuth(unittest.TestCase):
    def setUp(self) -> None:
        with conf_vars(
            {
                ("api", "auth_backend"): "airflow.api.auth.backend.basic_auth",
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

    def test_success(self):
        token = "Basic " + b64encode(b"test:test").decode()
        clear_db_pools()

        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": token})
            assert current_user.email == "test@fab.org"

        assert response.status_code == 200
        assert response.json == {
            "pools": [
                {
                    "name": "default_pool",
                    "slots": 128,
                    "occupied_slots": 0,
                    "running_slots": 0,
                    "queued_slots": 0,
                    "open_slots": 128,
                },
            ],
            "total_entries": 1,
        }

    @parameterized.expand(
        [
            ("basic",),
            ("basic ",),
            ("bearer",),
            ("test:test",),
            (b64encode(b"test:test").decode(),),
            ("bearer ",),
            ("basic: ",),
            ("basic 123",),
        ]
    )
    def test_malformed_headers(self, token):
        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": token})
            assert response.status_code == 401
            assert response.headers["Content-Type"] == "application/problem+json"
            assert response.headers["WWW-Authenticate"] == "Basic"
            assert_401(response)

    @parameterized.expand(
        [
            ("basic " + b64encode(b"test").decode(),),
            ("basic " + b64encode(b"test:").decode(),),
            ("basic " + b64encode(b"test:123").decode(),),
            ("basic " + b64encode(b"test test").decode(),),
        ]
    )
    def test_invalid_auth_header(self, token):
        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": token})
            assert response.status_code == 401
            assert response.headers["Content-Type"] == "application/problem+json"
            assert response.headers["WWW-Authenticate"] == "Basic"
            assert_401(response)

    @conf_vars({('api', 'enable_experimental_api'): 'true'})
    def test_experimental_api(self):
        with self.app.test_client() as test_client:
            response = test_client.get("/api/experimental/pools", headers={"Authorization": "Basic"})
            assert response.status_code == 401
            assert response.headers["WWW-Authenticate"] == "Basic"
            assert response.data == b'Unauthorized'

            clear_db_pools()
            response = test_client.get(
                "/api/experimental/pools",
                headers={"Authorization": "Basic " + b64encode(b"test:test").decode()},
            )
            assert response.status_code == 200
            assert response.json[0]["pool"] == 'default_pool'
