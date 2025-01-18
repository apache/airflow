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

import os

import pytest
from sqlalchemy import select

from airflow.models import DagBag
from airflow.models.dagbag import DagPriorityParsingRequest

from tests_common.test_utils.api_connexion_utils import create_user, delete_user
from tests_common.test_utils.db import clear_db_dag_parsing_requests, parse_and_sync_to_db

pytestmark = pytest.mark.db_test


ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
EXAMPLE_DAG_FILE = os.path.join("airflow", "example_dags", "example_bash_operator.py")
TEST_DAG_ID = "example_bash_operator"
NOT_READABLE_DAG_ID = "latest_only_with_trigger"
TEST_MULTIPLE_DAGS_ID = "asset_produces_1"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,
        username="test",
        role_name="admin",
    )
    create_user(app, username="test_no_permissions", role_name=None)

    yield app

    delete_user(app, username="test")
    delete_user(app, username="test_no_permissions")


class TestDagParsingRequest:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        self.clear_db()

    def teardown_method(self) -> None:
        self.clear_db()

    @staticmethod
    def clear_db():
        clear_db_dag_parsing_requests()

    def test_201_and_400_requests(self, url_safe_serializer, session):
        parse_and_sync_to_db(EXAMPLE_DAG_FILE)
        dagbag = DagBag(read_dags_from_db=True)
        test_dag = dagbag.get_dag(TEST_DAG_ID)

        url = f"/api/v1/parseDagFile/{url_safe_serializer.dumps(test_dag.fileloc)}"
        response = self.client.put(
            url, headers={"Accept": "application/json"}, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 201
        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert parsing_requests[0].fileloc == test_dag.fileloc

        # Duplicate file parsing request
        response = self.client.put(
            url, headers={"Accept": "application/json"}, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 201
        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert parsing_requests[0].fileloc == test_dag.fileloc

    def test_bad_file_request(self, url_safe_serializer, session):
        url = f"/api/v1/parseDagFile/{url_safe_serializer.dumps('/some/random/file.py')}"
        response = self.client.put(
            url, headers={"Accept": "application/json"}, environ_overrides={"REMOTE_USER": "test"}
        )
        assert response.status_code == 404

        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert parsing_requests == []

    def test_bad_user_request(self, url_safe_serializer, session):
        url = f"/api/v1/parseDagFile/{url_safe_serializer.dumps('/some/random/file.py')}"
        response = self.client.put(
            url,
            headers={"Accept": "application/json"},
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403

        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert parsing_requests == []
