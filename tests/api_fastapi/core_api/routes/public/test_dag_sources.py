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
import json
import os

import pytest
from httpx import Response

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag

from tests_common.test_utils.db import clear_db_dag_code, clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

API_PREFIX = "/public/dagSources"

# Example bash operator located here: airflow/example_dags/example_bash_operator.py
EXAMPLE_DAG_FILE = os.path.join("airflow", "example_dags", "example_bash_operator.py")
TEST_DAG_ID = "latest_only"


class TestGetDAGSource:
    @pytest.fixture(autouse=True)
    def setup(self, url_safe_serializer) -> None:
        self.clear_db()
        self.test_dag, self.dag_docstring = self.create_dag_source()
        fileloc = url_safe_serializer.dumps(self.test_dag.fileloc)
        self.dag_sources_url = f"{API_PREFIX}/{fileloc}"

    def teardown_method(self) -> None:
        self.clear_db()

    @staticmethod
    def _get_dag_file_docstring(fileloc: str) -> str | None:
        with open(fileloc) as f:
            file_contents = f.read()
        module = ast.parse(file_contents)
        docstring = ast.get_docstring(module)
        return docstring

    def create_dag_source(self) -> tuple[DAG, str | None]:
        dagbag = DagBag(dag_folder=EXAMPLE_DAG_FILE)
        dagbag.sync_to_db()
        test_dag: DAG = dagbag.dags[TEST_DAG_ID]
        return test_dag, self._get_dag_file_docstring(test_dag.fileloc)

    def clear_db(self):
        clear_db_dags()
        clear_db_serialized_dags()
        clear_db_dag_code()

    def test_should_respond_200_text(self, test_client):
        response: Response = test_client.get(self.dag_sources_url, headers={"Accept": "text/plain"})

        assert isinstance(response, Response)
        assert 200 == response.status_code
        assert len(self.dag_docstring) > 0
        assert self.dag_docstring in response.content.decode()
        with pytest.raises(json.JSONDecodeError):
            json.loads(response.content.decode())
        assert response.headers["Content-Type"].startswith("text/plain")

    @pytest.mark.parametrize("headers", [{"Accept": "application/json"}, {}])
    def test_should_respond_200_json(self, test_client, headers):
        response: Response = test_client.get(
            self.dag_sources_url,
            headers=headers,
        )
        assert isinstance(response, Response)
        assert 200 == response.status_code
        assert len(self.dag_docstring) > 0
        res_json = response.json()
        assert isinstance(res_json, dict)
        assert len(res_json.keys()) == 1
        assert len(res_json["content"]) > 0
        assert isinstance(res_json["content"], str)
        assert self.dag_docstring in res_json["content"]
        assert response.headers["Content-Type"].startswith("application/json")

    def test_should_respond_406_unsupport_mime_type(self, test_client):
        response = test_client.get(
            self.dag_sources_url,
            headers={"Accept": "text/html"},
        )
        assert 406 == response.status_code

    def test_should_respond_404(self, test_client):
        wrong_fileloc = "abcd1234"
        url = f"{API_PREFIX}/{wrong_fileloc}"
        response = test_client.get(url, headers={"Accept": "application/json"})
        assert 404 == response.status_code
