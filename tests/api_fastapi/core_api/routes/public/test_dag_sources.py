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

import pytest

from airflow.models.dag import DAG
from airflow.models.dagbag import DagBag

from tests_common.test_utils.db import clear_db_dag_code, clear_db_dags, clear_db_serialized_dags

pytestmark = pytest.mark.db_test

EXAMPLE_DAG_FILE = os.path.join("airflow", "example_dags", "example_bash_operator.py")
TEST_DAG_ID = "latest_only"
API_PREFIX = "/public/dagSources"


class TestGetDAGSource:
    @pytest.fixture(autouse=True)
    def setup(self, url_safe_serializer) -> None:
        clear_db_dag_code()
        self.test_dag, self.dag_docstring = self.create_dag_source()
        fileloc = url_safe_serializer.dumps(self.test_dag.fileloc)
        self.dag_sources_url = f"{API_PREFIX}/{fileloc}"

    def teardown_method(self) -> None:
        clear_db_dag_code()

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

    @staticmethod
    def clear_db():
        clear_db_dags()
        clear_db_serialized_dags()
        clear_db_dag_code()

    def test_should_respond_200_text(self, test_client):
        response = test_client.get(self.dag_sources_url, headers={"Accept": "text/plain"})
        assert 200 == response.status_code
        assert len(self.dag_docstring) > 0
        assert self.dag_docstring == response.data.decode()
        assert "text/plain" == response.headers["Content-Type"]

    def test_should_respond_200_json(self):
        response = self.client.get(self.dag_sources_url, headers={"Accept": "application/json"})
        assert 200 == response.status_code
        assert len(self.dag_docstring) > 0
        assert isinstance(response.json, dict)
        assert len(response.json["content"]) > 0
        assert self.dag_docstring == response.json["content"]
        assert "application/json" == response.headers["Content-Type"]

    def test_should_respond_404(self):
        wrong_fileloc = "abcd1234"
        url = f"{API_PREFIX}/{wrong_fileloc}"
        response = self.client.get(url, headers={"Accept": "application/json"})
        assert 404 == response.status_code
