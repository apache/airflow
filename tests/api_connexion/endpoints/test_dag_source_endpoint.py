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

import ast
import os
from typing import TYPE_CHECKING

import pytest

from airflow.models import DagBag
from airflow.security import permissions
from tests.test_utils.api_connexion_utils import assert_401, create_user, delete_user
from tests.test_utils.db import clear_db_dag_code, clear_db_dags, clear_db_serialized_dags

pytestmark = [pytest.mark.db_test, pytest.mark.skip_if_database_isolation_mode]

if TYPE_CHECKING:
    from airflow.models.dag import DAG

ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))
EXAMPLE_DAG_FILE = os.path.join("airflow", "example_dags", "example_bash_operator.py")
EXAMPLE_DAG_ID = "example_bash_operator"
TEST_DAG_ID = "latest_only"
NOT_READABLE_DAG_ID = "latest_only_with_trigger"
TEST_MULTIPLE_DAGS_ID = "dataset_produces_1"


@pytest.fixture(scope="module")
def configured_app(minimal_app_for_api):
    app = minimal_app_for_api
    create_user(
        app,  # type:ignore
        username="test",
        role_name="Test",
        permissions=[(permissions.ACTION_CAN_READ, permissions.RESOURCE_DAG_CODE)],  # type: ignore
    )
    app.appbuilder.sm.sync_perm_for_dag(  # type: ignore
        TEST_DAG_ID,
        access_control={"Test": [permissions.ACTION_CAN_READ]},
    )
    app.appbuilder.sm.sync_perm_for_dag(  # type: ignore
        EXAMPLE_DAG_ID,
        access_control={"Test": [permissions.ACTION_CAN_READ]},
    )
    app.appbuilder.sm.sync_perm_for_dag(  # type: ignore
        TEST_MULTIPLE_DAGS_ID,
        access_control={"Test": [permissions.ACTION_CAN_READ]},
    )
    create_user(app, username="test_no_permissions", role_name="TestNoPermissions")  # type: ignore

    yield app

    delete_user(app, username="test")  # type: ignore
    delete_user(app, username="test_no_permissions")  # type: ignore


class TestGetSource:
    @pytest.fixture(autouse=True)
    def setup_attrs(self, configured_app) -> None:
        self.app = configured_app
        self.client = self.app.test_client()  # type:ignore
        self.clear_db()

    def teardown_method(self) -> None:
        self.clear_db()

    @staticmethod
    def clear_db():
        clear_db_dags()
        clear_db_serialized_dags()
        clear_db_dag_code()

    @staticmethod
    def _get_dag_file_docstring(fileloc: str) -> str | None:
        with open(fileloc) as f:
            file_contents = f.read()
        module = ast.parse(file_contents)
        docstring = ast.get_docstring(module)
        return docstring

    def test_should_respond_200_text(self, url_safe_serializer):
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        test_dag: DAG = dagbag.dags[TEST_DAG_ID]
        dag_docstring = self._get_dag_file_docstring(test_dag.fileloc)

        url = f"/api/v1/dagSources/{url_safe_serializer.dumps(test_dag.fileloc)}"
        response = self.client.get(
            url, headers={"Accept": "text/plain"}, environ_overrides={"REMOTE_USER": "test"}
        )

        assert 200 == response.status_code
        assert dag_docstring in response.data.decode()
        assert "text/plain" == response.headers["Content-Type"]

    def test_should_respond_200_json(self, url_safe_serializer):
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        test_dag: DAG = dagbag.dags[TEST_DAG_ID]
        dag_docstring = self._get_dag_file_docstring(test_dag.fileloc)

        url = f"/api/v1/dagSources/{url_safe_serializer.dumps(test_dag.fileloc)}"
        response = self.client.get(
            url, headers={"Accept": "application/json"}, environ_overrides={"REMOTE_USER": "test"}
        )

        assert 200 == response.status_code
        assert dag_docstring in response.json["content"]
        assert "application/json" == response.headers["Content-Type"]

    def test_should_respond_406(self, url_safe_serializer):
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        test_dag: DAG = dagbag.dags[TEST_DAG_ID]

        url = f"/api/v1/dagSources/{url_safe_serializer.dumps(test_dag.fileloc)}"
        response = self.client.get(
            url, headers={"Accept": "image/webp"}, environ_overrides={"REMOTE_USER": "test"}
        )

        assert 406 == response.status_code

    def test_should_respond_404(self):
        wrong_fileloc = "abcd1234"
        url = f"/api/v1/dagSources/{wrong_fileloc}"
        response = self.client.get(
            url, headers={"Accept": "application/json"}, environ_overrides={"REMOTE_USER": "test"}
        )

        assert 404 == response.status_code

    def test_should_raises_401_unauthenticated(self, url_safe_serializer):
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        first_dag: DAG = next(iter(dagbag.dags.values()))

        response = self.client.get(
            f"/api/v1/dagSources/{url_safe_serializer.dumps(first_dag.fileloc)}",
            headers={"Accept": "text/plain"},
        )

        assert_401(response)

    def test_should_raise_403_forbidden(self, url_safe_serializer):
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        first_dag: DAG = next(iter(dagbag.dags.values()))

        response = self.client.get(
            f"/api/v1/dagSources/{url_safe_serializer.dumps(first_dag.fileloc)}",
            headers={"Accept": "text/plain"},
            environ_overrides={"REMOTE_USER": "test_no_permissions"},
        )
        assert response.status_code == 403

    def test_should_respond_403_not_readable(self, url_safe_serializer):
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        dag: DAG = dagbag.dags[NOT_READABLE_DAG_ID]

        response = self.client.get(
            f"/api/v1/dagSources/{url_safe_serializer.dumps(dag.fileloc)}",
            headers={"Accept": "text/plain"},
            environ_overrides={"REMOTE_USER": "test"},
        )
        read_dag = self.client.get(
            f"/api/v1/dags/{NOT_READABLE_DAG_ID}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 403
        assert read_dag.status_code == 403

    def test_should_respond_403_some_dags_not_readable_in_the_file(self, url_safe_serializer):
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        dag: DAG = dagbag.dags[TEST_MULTIPLE_DAGS_ID]

        response = self.client.get(
            f"/api/v1/dagSources/{url_safe_serializer.dumps(dag.fileloc)}",
            headers={"Accept": "text/plain"},
            environ_overrides={"REMOTE_USER": "test"},
        )

        read_dag = self.client.get(
            f"/api/v1/dags/{TEST_MULTIPLE_DAGS_ID}",
            environ_overrides={"REMOTE_USER": "test"},
        )
        assert response.status_code == 403
        assert read_dag.status_code == 200

    @pytest.mark.parametrize(
        "set_auto_role_public, expected_status_code",
        (("Public", 403), ("Admin", 200)),
        indirect=["set_auto_role_public"],
    )
    def test_with_auth_role_public_set(self, set_auto_role_public, expected_status_code, url_safe_serializer):
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        test_dag: DAG = dagbag.dags[TEST_DAG_ID]
        self._get_dag_file_docstring(test_dag.fileloc)

        url = f"/api/v1/dagSources/{url_safe_serializer.dumps(test_dag.fileloc)}"
        response = self.client.get(url, headers={"Accept": "text/plain"})

        assert response.status_code == expected_status_code
