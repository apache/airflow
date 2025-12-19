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

import json

import pendulum
import pytest
from httpx import Response
from sqlalchemy import select

from airflow.models.dagbag import DBDagBag
from airflow.models.dagcode import DagCode
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

from tests_common.test_utils.dag import sync_dag_to_db
from tests_common.test_utils.db import clear_db_dags, clear_db_runs, parse_and_sync_to_db
from unit.serialization.test_dag_serialization import AIRFLOW_REPO_ROOT_PATH

pytestmark = pytest.mark.db_test

API_PREFIX = "/dagSources"

# Example bash operator located here: airflow/example_dags/example_bash_operator.py
EXAMPLE_DAG_FILE = (
    AIRFLOW_REPO_ROOT_PATH / "airflow-core" / "src" / "airflow" / "example_dags" / "example_simplest_dag.py"
)
TEST_DAG_ID = "example_simplest_dag"
TEST_DAG_DISPLAY_NAME = "example_simplest_dag"


@pytest.fixture
def real_dag_bag():
    return parse_and_sync_to_db(EXAMPLE_DAG_FILE, include_examples=False)


@pytest.fixture
def test_dag(session, real_dag_bag):
    return DBDagBag().get_latest_version_of_dag(TEST_DAG_ID, session=session)


@pytest.fixture
def force_reserialization(real_dag_bag, session):
    def _force_reserialization(dag_id, bundle_name):
        dag = real_dag_bag.get_dag(dag_id, session=session)
        sync_dag_to_db(dag, bundle_name=bundle_name, session=session)

    return _force_reserialization


class TestGetDAGSource:
    @pytest.fixture(autouse=True)
    def setup(self, url_safe_serializer) -> None:
        self.clear_db()

    def teardown_method(self) -> None:
        self.clear_db()

    @staticmethod
    def _get_dag_file_code(fileloc: str) -> str | None:
        with open(fileloc) as f:
            file_contents = f.read()
        return file_contents

    def clear_db(self):
        clear_db_dags()
        clear_db_runs()

    def test_should_respond_200_text(self, test_client, test_dag):
        dag_content = self._get_dag_file_code(test_dag.fileloc)
        response: Response = test_client.get(f"{API_PREFIX}/{TEST_DAG_ID}", headers={"Accept": "text/plain"})

        assert isinstance(response, Response)
        assert response.status_code == 200
        assert dag_content == response.content.decode()
        with pytest.raises(json.JSONDecodeError):
            json.loads(response.content.decode())
        assert response.headers["Content-Type"].startswith("text/plain")

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get(
            f"{API_PREFIX}/{TEST_DAG_ID}", headers={"Accept": "text/plain"}
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get(
            f"{API_PREFIX}/{TEST_DAG_ID}", headers={"Accept": "text/plain"}
        )
        assert response.status_code == 403

    @pytest.mark.parametrize(
        "headers", [{"Accept": "application/json"}, {"Accept": "application/json; charset=utf-8"}, {}]
    )
    def test_should_respond_200_json(self, test_client, test_dag, headers):
        dag_content = self._get_dag_file_code(test_dag.fileloc)
        response: Response = test_client.get(
            f"{API_PREFIX}/{TEST_DAG_ID}",
            headers=headers,
        )
        assert isinstance(response, Response)
        assert response.status_code == 200
        assert response.json() == {
            "content": dag_content,
            "dag_id": TEST_DAG_ID,
            "version_number": 1,
            "dag_display_name": TEST_DAG_DISPLAY_NAME,
        }
        assert response.headers["Content-Type"].startswith("application/json")

    @pytest.mark.parametrize("accept", ["application/json", "text/plain"])
    def test_should_respond_200_version(self, test_client, accept, session, test_dag, force_reserialization):
        dag_content = self._get_dag_file_code(test_dag.fileloc)
        test_dag.create_dagrun(
            run_id="test1",
            run_after=pendulum.datetime(2025, 1, 1, tz="UTC"),
            state=DagRunState.QUEUED,
            triggered_by=DagRunTriggeredByType.TEST,
            run_type=DagRunType.MANUAL,
        )
        # force reserialization
        test_dag.doc_md = "new doc"
        force_reserialization(test_dag.dag_id, "dag-folder")
        dagcode = session.scalars(
            select(DagCode)
            .where(DagCode.fileloc == test_dag.fileloc)
            .order_by(DagCode.id.desc())
        ).first()
        assert dagcode.dag_version.version_number == 2
        # populate the latest dagcode with a value
        dag_content2 = "new source code"
        dagcode.source_code = dag_content2
        session.merge(dagcode)
        session.commit()

        dagcodes = session.scalars(select(DagCode).where(DagCode.fileloc == test_dag.fileloc)).all()
        assert len(dagcodes) == 2
        url = f"{API_PREFIX}/{TEST_DAG_ID}"
        response = test_client.get(url, headers={"Accept": accept})

        assert response.status_code == 200
        if accept == "text/plain":
            assert dag_content2 == response.content.decode()
            assert dag_content != response.content.decode()
            assert response.headers["Content-Type"].startswith("text/plain")
        else:
            assert dag_content2 == response.json()["content"]
            assert dag_content != response.json()["content"]
            assert response.headers["Content-Type"] == "application/json"
            assert response.json() == {
                "content": dag_content2,
                "dag_id": TEST_DAG_ID,
                "version_number": 2,
                "dag_display_name": TEST_DAG_DISPLAY_NAME,
            }

    def test_should_respond_406_unsupport_mime_type(self, test_client, test_dag):
        response = test_client.get(
            f"{API_PREFIX}/{TEST_DAG_ID}",
            headers={"Accept": "text/html"},
        )
        assert response.status_code == 406

    def test_should_respond_404(self, test_client):
        wrong_fileloc = "abcd1234"
        url = f"{API_PREFIX}/{wrong_fileloc}"
        response = test_client.get(url, headers={"Accept": "application/json"})
        assert response.status_code == 404
