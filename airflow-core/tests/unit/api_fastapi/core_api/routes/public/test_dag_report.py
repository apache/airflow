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
from unittest.mock import patch

import pytest

from airflow.utils.file import list_py_file_paths

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags, parse_and_sync_to_db

pytestmark = pytest.mark.db_test

TEST_DAG_FOLDER = os.environ["AIRFLOW__CORE__DAGS_FOLDER"]
TEST_DAG_FOLDER_WITH_SUBDIR = f"{TEST_DAG_FOLDER}/subdir2"
TEST_DAG_FOLDER_INVALID = "/invalid/path"
TEST_DAG_FOLDER_INVALID_2 = "/root/airflow/tests/dags/"


def get_corresponding_dag_file_count(dir: str, include_examples: bool = True) -> int:
    from airflow import example_dags

    return len(list_py_file_paths(directory=dir)) + (
        len(list_py_file_paths(next(iter(example_dags.__path__)))) if include_examples else 0
    )


class TestDagReportEndpoint:
    @pytest.fixture(autouse=True)
    def setup(self) -> None:
        self.clear_db()

    def teardown_method(self) -> None:
        self.clear_db()

    def clear_db(self):
        clear_db_dags()

    @pytest.mark.parametrize(
        "subdir,include_example,expected_total_entries",
        [
            pytest.param(
                TEST_DAG_FOLDER,
                True,
                get_corresponding_dag_file_count(TEST_DAG_FOLDER),
                id="dag_path_with_example",
            ),
            pytest.param(
                TEST_DAG_FOLDER,
                False,
                get_corresponding_dag_file_count(TEST_DAG_FOLDER, False),
                id="dag_path_without_example",
            ),
            pytest.param(
                TEST_DAG_FOLDER_WITH_SUBDIR,
                True,
                get_corresponding_dag_file_count(TEST_DAG_FOLDER_WITH_SUBDIR),
                id="dag_path_subdir_with_example",
            ),
            pytest.param(
                TEST_DAG_FOLDER_WITH_SUBDIR,
                False,
                get_corresponding_dag_file_count(TEST_DAG_FOLDER_WITH_SUBDIR, False),
                id="dag_path_subdir_without_example",
            ),
        ],
    )
    def test_should_response_200(self, test_client, subdir, include_example, expected_total_entries):
        with conf_vars({("core", "load_examples"): str(include_example)}):
            parse_and_sync_to_db(subdir, include_examples=include_example)
            response = test_client.get("/dagReports", params={"subdir": subdir})
            assert response.status_code == 200
            response_json = response.json()
            assert response_json["total_entries"] == expected_total_entries

    def test_should_response_200_with_empty_dagbag(self, test_client):
        # the constructor of DagBag will call `collect_dags` method and store the result in `dagbag_stats`
        def _mock_collect_dags(self, *args, **kwargs):
            self.dagbag_stats = []

        with patch("airflow.models.dagbag.DagBag.collect_dags", _mock_collect_dags):
            response = test_client.get("/dagReports", params={"subdir": TEST_DAG_FOLDER})
            assert response.status_code == 200
            assert response.json() == {"dag_reports": [], "total_entries": 0}

    @pytest.mark.parametrize(
        "subdir",
        [
            pytest.param(TEST_DAG_FOLDER_INVALID, id="invalid_dag_path"),
            pytest.param(TEST_DAG_FOLDER_INVALID_2, id="invalid_dag_path_2"),
        ],
    )
    def test_should_response_400(self, test_client, subdir):
        response = test_client.get("/dagReports", params={"subdir": subdir})
        assert response.status_code == 400
        assert response.json() == {"detail": "subdir should be subpath of DAGS_FOLDER settings"}

    def test_should_response_422(self, test_client):
        response = test_client.get("/dagReports")
        assert response.status_code == 422
        assert response.json() == {
            "detail": [
                {
                    "input": None,
                    "loc": ["query", "subdir"],
                    "msg": "Field required",
                    "type": "missing",
                }
            ]
        }

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.get("/dagReports")
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.get("/dagReports")
        assert response.status_code == 403
