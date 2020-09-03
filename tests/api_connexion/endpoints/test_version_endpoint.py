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

from airflow.www import app
from tests.test_utils.api_connexion_utils import create_role, create_user, delete_user
from tests.test_utils.config import conf_vars


class TestGetHealthTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars({("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}):
            cls.app = app.create_app(testing=True)  # type:ignore

        create_role(
            cls.app,  # type: ignore
            name="Test",
            permissions=[
                ("can_read", "Version"),
            ],
        )
        create_user(cls.app, username="test", role="Test")  # type: ignore
        create_role(cls.app, name="TestNoPermissions", permissions=[])  # type: ignore
        create_user(cls.app, username="test_no_permissions", role="TestNoPermissions")  # type: ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore
        cls.app.appbuilder.sm.delete_role("Test")  # type: ignore  # pylint: disable=no-member
        delete_user(cls.app, username="test_no_permissions")  # type: ignore
        cls.app.appbuilder.sm.delete_role("TestNoPermissions")  # type: ignore  # pylint: disable=no-member

    @mock.patch("airflow.api_connexion.endpoints.version_endpoint.airflow.__version__", "MOCK_VERSION")
    @mock.patch(
        "airflow.api_connexion.endpoints.version_endpoint.get_airflow_git_version", return_value="GIT_COMMIT"
    )
    def test_should_response_200(self, mock_get_airflow_get_commit):
        response = self.client.get("/api/v1/version", environ_overrides={'REMOTE_USER': "test"})

        self.assertEqual(200, response.status_code)
        self.assertEqual({'git_version': 'GIT_COMMIT', 'version': 'MOCK_VERSION'}, response.json)
        mock_get_airflow_get_commit.assert_called_once_with()

    def test_should_response_403_unauthorized(self):
        response = self.client.get(
            "/api/v1/version", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )

        self.assertEqual(403, response.status_code)
