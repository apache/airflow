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
import os

import pytest
from httpx import Response
from sqlalchemy import select

from airflow.models.dagbag import DagBag
from airflow.models.dagcode import DagCode
from airflow.models.serialized_dag import SerializedDagModel

from tests_common.test_utils.db import clear_db_dags

pytestmark = pytest.mark.db_test

API_PREFIX = "/public/dagSources"

# Example bash operator located here: airflow/example_dags/example_bash_operator.py
EXAMPLE_DAG_FILE = os.path.join("airflow", "example_dags", "example_bash_operator.py")
TEST_DAG_ID = "latest_only"


@pytest.fixture
def test_dag():
    dagbag = DagBag(include_examples=True)
    dagbag.sync_to_db()
    return dagbag.dags[TEST_DAG_ID]


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

    def test_should_respond_200_text(self, test_client, test_dag):
        dag_content = self._get_dag_file_code(test_dag.fileloc)
        response: Response = test_client.get(f"{API_PREFIX}/{TEST_DAG_ID}", headers={"Accept": "text/plain"})

        assert isinstance(response, Response)
        assert 200 == response.status_code
        assert dag_content == response.content.decode()
        with pytest.raises(json.JSONDecodeError):
            json.loads(response.content.decode())
        assert response.headers["Content-Type"].startswith("text/plain")

    @pytest.mark.parametrize(
        "headers", [{"Accept": "application/json"}, {"Accept": "application/json; charset=utf-8"}, {}]
    )
    def test_should_respond_200_json(self, test_client,test_dag, headers):
        dag_content = self._get_dag_file_code(test_dag.fileloc)
        response: Response = test_client.get(
            f"{API_PREFIX}/{TEST_DAG_ID}",
            headers=headers,
        )
        assert isinstance(response, Response)
        assert 200 == response.status_code
        assert response.json() == {
            "content": dag_content,
            "dag_id": TEST_DAG_ID,
            "version_number": 1,
        }
        assert response.headers["Content-Type"].startswith("application/json")

    @pytest.mark.parametrize("accept", ["application/json", "text/plain"])
    def test_should_respond_200_version(self, test_client, accept, session, test_dag):
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
        url = f"{API_PREFIX}/{TEST_DAG_ID}"
        response = test_client.get(url, headers={"Accept": accept})

        assert 200 == response.status_code
        if accept == "text/plain":
            assert dag_content2 == response.content.decode()
            assert dag_content != response.content.decode()
            assert response.headers["Content-Type"].startswith("text/plain")
        else:
            assert dag_content2 == response.json()["content"]
            assert dag_content != response.json()["content"]
            assert "application/json" == response.headers["Content-Type"]
            assert response.json() == {
                "content": dag_content2,
                "dag_id": TEST_DAG_ID,
                "version_number": 2,
            }

    def test_should_respond_406_unsupport_mime_type(self, test_client):
        dagbag = DagBag(include_examples=True)
        dagbag.sync_to_db()
        response = test_client.get(
            f"{API_PREFIX}/{TEST_DAG_ID}",
            headers={"Accept": "text/html"},
        )
        assert 406 == response.status_code

    def test_should_respond_404(self, test_client):
        wrong_fileloc = "abcd1234"
        url = f"{API_PREFIX}/{wrong_fileloc}"
        response = test_client.get(url, headers={"Accept": "application/json"})
        assert 404 == response.status_code
