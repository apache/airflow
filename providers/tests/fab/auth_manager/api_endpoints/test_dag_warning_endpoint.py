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

from airflow.models.dag import DagModel
from airflow.models.dagwarning import DagWarning
from airflow.security import permissions
from airflow.utils.session import create_session

from dev.tests_common.test_utils.compat import AIRFLOW_V_3_0_PLUS
from dev.tests_common.test_utils.db import clear_db_dag_warnings, clear_db_dags
from providers.tests.fab.auth_manager.api_endpoints.api_connexion_utils import create_user, delete_user

pytestmark = [
    pytest.mark.db_test,
    pytest.mark.skip_if_database_isolation_mode,
    pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+"),
]


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_auth_api):
    app = minimal_app_for_auth_api
    create_user(
        app,  # type:ignore
        username="test_with_dag2_read",
        role_name="TestWithDag2Read",
        permissions=[
            (permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_WARNING),
            (permissions.ACTION_CAN_READ, f"{permissions.RESOURCE_DAG_PREFIX}dag2"),
        ],
    )

    yield app

    delete_user(app, username="test_with_dag2_read")


class TestBaseDagWarning:
    timestamp = "2020-06-10T12:00"

    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore

    def teardown_method(self) -> None:
        clear_db_dag_warnings()
        clear_db_dags()


class TestGetDagWarningEndpoint(TestBaseDagWarning):
    def setup_class(self):
        clear_db_dag_warnings()
        clear_db_dags()

    def setup_method(self):
        with create_session() as session:
            session.add(DagModel(dag_id="dag1"))
            session.add(DagWarning("dag1", "non-existent pool", "test message"))
            session.commit()

    def test_should_raise_403_forbidden_when_user_has_no_dag_read_permission(self):
        response = self.client.get(
            "/api/v1/dagWarnings",
            environ_overrides={"REMOTE_USER": "test_with_dag2_read"},
            query_string={"dag_id": "dag1"},
        )
        assert response.status_code == 403
