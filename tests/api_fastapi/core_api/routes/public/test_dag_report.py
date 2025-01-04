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

from unittest.mock import patch

import pytest
import re2 as re

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import clear_db_dags

pytestmark = pytest.mark.db_test


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
            pytest.param("/root/airflow/dags", True, 58, id="dag_path_with_example"),
            pytest.param("/root/airflow/dags", False, 0, id="dag_path_without_example"),
        ],
    )
    def test_should_response_200(self, test_client, subdir, include_example, expected_total_entries):
        with conf_vars({("core", "load_examples"): str(include_example)}):
            params = {"subdir": subdir} if subdir else {}
            response = test_client.get("/public/dagReport", params=params)
            assert response.status_code == 200
            response_json = response.json()
            expected_keys = {"dag_num", "dags", "file", "task_num", "warning_num"}
            assert response_json["total_entries"] == expected_total_entries
            for dag_report in response_json["dag_reports"]:
                assert all(key in dag_report for key in expected_keys)
                # assert duration is in ISO8601 format ( PTnS or PTn.nS )
                assert re.match(r"^PT\d+(\.\d+)?S$", dag_report["duration"])

    def test_should_response_200_with_empty_dagbag(self, test_client):
        # the constructor of DagBag will call `collect_dags` method and store the result in `dagbag_stats`
        def _mock_collect_dags(self, *args, **kwargs):
            self.dagbag_stats = []

        with patch("airflow.models.dagbag.DagBag.collect_dags", _mock_collect_dags):
            response = test_client.get("/public/dagReport", params={"subdir": "airflow/example_dags"})
            assert response.status_code == 200
            assert response.json() == {"dag_reports": [], "total_entries": 0}

    def test_should_response_422(self, test_client):
        response = test_client.get("/public/dagReport")
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
