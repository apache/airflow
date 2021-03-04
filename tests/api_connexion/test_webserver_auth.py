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
from datetime import datetime, timedelta

import jwt
from flask_jwt_extended import create_access_token
from parameterized import parameterized

from airflow.security import permissions
from airflow.www.app import create_app
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.config import conf_vars


class TestWebserverAuth(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = create_app(testing=True)
        cls.appbuilder = cls.app.appbuilder  # pylint: disable=no-member
        create_user(
            cls.app,  # type: ignore
            username="test",
            role_name="Admin",
            permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_POOL)],
        )
        cls.tester = cls.appbuilder.sm.find_user(username="test")

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()

    def test_basic_auth_can_get_token(self):
        # For most users that would use username and password especially in local
        # deployment, we check that on a successful login, a token is generated
        token = "Basic " + b64encode(b"test:test").decode()
        with self.app.test_client() as test_client:
            test_client.post("api/v1/login", headers={"Authorization": token})
            cookie = next(
                (cookie for cookie in test_client.cookie_jar if cookie.name == "access_token_cookie"), None
            )
        assert isinstance(cookie.value, str)

    def test_token_in_cookie_can_view_endpoints(self):
        with self.app.app_context():
            token = create_access_token(self.tester.id)
            self.client.set_cookie("localhost", 'access_token_cookie', token)
            response = self.client.get("/api/v1/pools")
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

    def test_raises_for_the_none_algorithm(self):
        payload = {
            'exp': datetime.utcnow() + timedelta(minutes=10),
            'iat': datetime.utcnow(),
            'sub': self.tester.id,
        }
        forgedtoken = jwt.encode(payload, key=None, algorithm=None).decode()
        self.client.set_cookie("localhost", 'access_token_cookie', forgedtoken)
        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools")
        assert response.status_code == 401

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
            assert_401(response)

    @parameterized.expand(
        [
            ("bearer " + b64encode(b"test").decode(),),
            ("basic " + b64encode(b"test:").decode(),),
            ("bearer " + b64encode(b"test:123").decode(),),
            ("basic " + b64encode(b"test test").decode(),),
        ]
    )
    def test_invalid_auth_header(self, token):
        with self.app.test_client() as test_client:
            response = test_client.get("/api/v1/pools", headers={"Authorization": token})
            assert response.status_code == 401
            assert response.headers["Content-Type"] == "application/problem+json"
            assert_401(response)

    @parameterized.expand(
        [("access_token_cookies"), ("access_token_cook"), ("access_token"), ("csrf_access_token")]
    )
    def test_invalid_cookie_name_fails(self, name):
        with self.app.app_context():
            token = create_access_token(self.tester.id)
            self.client.set_cookie("localhost", name, token)
            response = self.client.get("/api/v1/pools")
        assert response.status_code == 401
        assert response.headers["Content-Type"] == "application/problem+json"
        assert_401(response)
