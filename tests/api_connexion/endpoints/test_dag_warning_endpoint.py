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

from airflow.models.dag import DagModel
from airflow.models.dagwarning import DagWarning
from airflow.security import permissions
from airflow.utils.session import create_session
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.db import clear_db_dag_warnings, clear_db_dags


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type:ignore
        username="test",
        role_name="Test",
        permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_WARNING)],  # type: ignore
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield minimal_app_for_api

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestBaseDagWarning:
    timestamp = "2020-06-10T12:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore

    def teardown_method(self) -> None:
        clear_db_dag_warnings()
        clear_db_dags()

    @staticmethod
    def _normalize_dag_warnings(dag_warnings):
        for i, dag_warning in enumerate(dag_warnings, 1):
            dag_warning["dag_warning_id"] = i


class TestGetDagWarningEndpoint(TestBaseDagWarning):
    def setup_class(self):
        clear_db_dag_warnings()
        clear_db_dags()

    def setup_method(self):
        with create_session() as session:
            session.add(DagModel(dag_id='dag1'))
            session.add(DagModel(dag_id='dag2'))
            session.add(DagModel(dag_id='dag3'))
            session.add(DagWarning('dag1', 'non-existent pool', 'test message'))
            session.add(DagWarning('dag2', 'non-existent pool', 'test message'))
            session.commit()

    def test_response_one(self):
        response = self.client.get(
            "/api/v1/dagWarnings",
            environ_overrides={'REMOTE_USER': "test"},
            query_string={'dag_id': 'dag1', 'warning_type': 'non-existent pool'},
        )
        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            'dag_warnings': [
                {
                    'dag_id': 'dag1',
                    'message': 'test message',
                    'timestamp': ANY,
                    'warning_type': 'non-existent pool',
                }
            ],
            'total_entries': 1,
        }

    def test_response_some(self):
        response = self.client.get(
            "/api/v1/dagWarnings",
            environ_overrides={'REMOTE_USER': "test"},
            query_string={'warning_type': 'non-existent pool'},
        )
        assert response.status_code == 200
        response_data = response.json
        assert len(response_data['dag_warnings']) == 2
        assert response_data == {
            'dag_warnings': ANY,
            'total_entries': 2,
        }

    def test_response_none(self, session):
        response = self.client.get(
            "/api/v1/dagWarnings",
            environ_overrides={'REMOTE_USER': "test"},
            query_string={'dag_id': 'missing_dag'},
        )
        assert response.status_code == 200
        response_data = response.json
        assert response_data == {
            'dag_warnings': [],
            'total_entries': 0,
        }

    def test_response_all(self):
        response = self.client.get(
            "/api/v1/dagWarnings",
            environ_overrides={'REMOTE_USER': "test"},
        )

        assert response.status_code == 200
        response_data = response.json
        assert len(response_data['dag_warnings']) == 2
        assert response_data == {
            'dag_warnings': ANY,
            'total_entries': 2,
        }

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get("/api/v1/dagWarnings")
        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            "/api/v1/dagWarnings", environ_overrides={'REMOTE_USER': "test_no_permissions"}
        )
        assert response.status_code == 403
