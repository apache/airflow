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

import pytest

from airflow.www import app
from tests.test_utils.api_connexion_utils import create_user, delete_user
from tests.test_utils.config import conf_vars


class TestTaskInstanceEndpoint(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        super().setUpClass()
        with conf_vars(
            {("api", "auth_backend"): "tests.test_utils.remote_user_api_auth_backend"}
        ):
            cls.app = app.create_app(testing=True)  # type:ignore
        # TODO: Add new role for each view to test permission.
        create_user(cls.app, username="test", role="Admin")  # type: ignore

    @classmethod
    def tearDownClass(cls) -> None:
        delete_user(cls.app, username="test")  # type: ignore

    def setUp(self) -> None:
        self.client = self.app.test_client()  # type:ignore


class TestGetTaskInstance(TestTaskInstanceEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.get(
            "/api/v1/dags/TEST_DG_ID/dagRuns/TEST_DAG_RUN_ID/taskInstances/TEST_TASK_ID",
            environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200


class TestGetTaskInstances(TestTaskInstanceEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.get(
            "/api/v1/dags/TEST_DAG_ID/taskInstances", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200


class TestGetTaskInstancesBatch(TestTaskInstanceEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.post(
            "/api/v1/dags/~/taskInstances/list", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200


class TestPostClearTaskInstances(TestTaskInstanceEndpoint):
    @pytest.mark.skip(reason="Not implemented yet")
    def test_should_response_200(self):
        response = self.client.post(
            "/api/v1/dags/clearTaskInstances", environ_overrides={'REMOTE_USER': "test"}
        )
        assert response.status_code == 200
