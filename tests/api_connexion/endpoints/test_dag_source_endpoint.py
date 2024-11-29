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
from sqlalchemy import select

from airflow.models import DagBag
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel

from tests_common.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests_common.test_utils.db import clear_db_dags

pytestmark = pytest.mark.db_test


EXAMPLE_DAG_ID = "example_bash_operator"
TEST_DAG_ID = "latest_only"


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


@pytest.fixture
def test_dag():
    dagbag = DagBag(include_examples=True)
    dagbag.sync_to_db()
    return dagbag.dags[TEST_DAG_ID]


class TestGetSource:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        clear_db_dags()

    def teardown_method(self):
        clear_db_dags()

    @staticmethod
    def _get_dag_file_code(fileloc: str) -> str | None:
        with open(fileloc) as f:
            file_contents = f.read()
        return file_contents

    def test_should_respond_200_text(self, test_dag):
        dag_content = self._get_dag_file_code(test_dag.fileloc)

        url = f"/api/v1/dagSources/{TEST_DAG_ID}"
        response = self.client.get(
            url, headers={"Accept": "text/plain"}, environ_overrides={"REMOTE_USER": "test"}
        )

        assert 200 == response.status_code
        assert dag_content == response.data.decode()
        assert "text/plain" == response.headers["Content-Type"]

    def test_should_respond_200_json(self, session, test_dag):
        dag_content = self._get_dag_file_code(test_dag.fileloc)

        url = f"/api/v1/dagSources/{TEST_DAG_ID}"
        response = self.client.get(
            url, headers={"Accept": "application/json"}, environ_overrides={"REMOTE_USER": "test"}
        )

        assert 200 == response.status_code
        assert response.json == {
            "content": dag_content,
            "dag_id": TEST_DAG_ID,
            "version_number": 1,
        }
        assert "application/json" == response.headers["Content-Type"]

    @pytest.mark.parametrize("accept", ["application/json", "text/plain"])
    def test_should_respond_200_version(self, accept, session, test_dag):
        dag_content = self._get_dag_file_code(test_dag.fileloc)
        # force reserialization
        SerializedDagModel.write_dag(test_dag, processor_subdir="/tmp")
        dagcode = (
            session.query(DagCode)
            .filter(DagCode.fileloc == test_dag.fileloc)
            .order_by(DagCode.id.desc())
            .first()
        )
        assert dagcode.dag_version.version_number == 2
        # populate the latest dagcode with a value
        dag_content2 = "new source code"
        dagcode.source_code = dag_content2
        session.merge(dagcode)
        session.commit()

        dagcodes = session.scalars(select(DagCode).where(DagCode.fileloc == test_dag.fileloc)).all()
        assert len(dagcodes) == 2
        url = f"/api/v1/dagSources/{TEST_DAG_ID}"
        response = self.client.get(url, headers={"Accept": accept}, environ_overrides={"REMOTE_USER": "test"})

        assert 200 == response.status_code
        if accept == "text/plain":
            assert dag_content2 == response.data.decode()
            assert dag_content != response.data.decode()
            assert "text/plain" == response.headers["Content-Type"]
        else:
            assert dag_content2 == response.json["content"]
            assert dag_content != response.json["content"]
            assert "application/json" == response.headers["Content-Type"]
            assert response.json == {
                "content": dag_content2,
                "dag_id": TEST_DAG_ID,
                "version_number": 2,
            }

    def test_should_respond_404(self):
        non_existing_dag_id = "abcd1234"
        url = f"/api/v1/dagSources/{non_existing_dag_id}"
        response = self.client.get(
            url, headers={"Accept": "application/json"}, environ_overrides={"REMOTE_USER": "test"}
        )

        assert 404 == response.status_code

    def test_should_raises_401_unauthenticated(self):
        response = self.client.get(
            f"/api/v1/dagSources/{TEST_DAG_ID}",
            headers={"Accept": "text/plain"},
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self):
        response = self.client.get(
            f"/api/v1/dagSources/{TEST_DAG_ID}",
            headers={"Accept": "text/plain"},
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403
