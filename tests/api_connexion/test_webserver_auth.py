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

from flask_login import current_user

from airflow.www.app import create_app
from tests.test_utils.config import conf_vars


class TestWebserverAuth(unittest.TestCase):
    def setUp(self) -> None:
        with conf_vars({("webserver", "session_lifetime_minutes"): "1"}):
            self.app = create_app(testing=True)

        self.appbuilder = self.app.appbuilder  # pylint: disable=no-member
        role_admin = self.appbuilder.sm.find_role("Admin")
        self.tester = self.appbuilder.sm.find_user(username="test")
        if not self.tester:
            self.appbuilder.sm.add_user(
                username="test",
                first_name="test",
                last_name="test",
                email="test@fab.org",
                role=role_admin,
                password="test",
            )

    def test_successful_login(self):
        payload = {"username": "test", "password": "test"}
        with self.app.test_client() as test_client:
            response = test_client.post("api/v1/login", json=payload)
        assert isinstance(response.json["token"], str)
        assert response.json["user"]['email'] == "test@fab.org"

    def test_can_view_other_endpoints(self):
        payload = {"username": "test", "password": "test"}
        with self.app.test_client() as test_client:
            response = test_client.post("api/v1/login", json=payload)
            assert current_user.email == "test@fab.org"
            token = response.json["token"]
            response2 = test_client.get("/api/v1/pools", headers={"Authorization": "Bearer " + token})
        assert response2.status_code == 200
        assert response2.json == {
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
