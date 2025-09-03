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

from airflow.models.dagbag import DagPriorityParsingRequest, DBDagBag

from tests_common.test_utils.api_fastapi import _check_last_log
from tests_common.test_utils.db import clear_db_dag_parsing_requests, clear_db_logs, parse_and_sync_to_db
from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

pytestmark = pytest.mark.db_test

EXAMPLE_DAG_FILE = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "example_dags" / "example_simplest_dag.py"
TEST_DAG_ID = "example_simplest_dag"
NOT_READABLE_DAG_ID = "latest_only_with_trigger"
TEST_MULTIPLE_DAGS_ID = "asset_produces_1"


class TestDagParsingEndpoint:
    @staticmethod
    def clear_db():
        clear_db_dag_parsing_requests()

    @pytest.fixture(autouse=True)
    def setup(self, session) -> None:
        self.clear_db()
        clear_db_logs()

    def test_201_and_400_requests(self, url_safe_serializer, session, test_client):
        parse_and_sync_to_db(EXAMPLE_DAG_FILE)
        test_dag = DBDagBag(load_op_links=False).get_latest_version_of_dag(TEST_DAG_ID, session=session)

        # grab the token
        token = test_client.get(f"/dags/{TEST_DAG_ID}").json()["file_token"]

        # First parsing request
        url = f"/parseDagFile/{token}"
        response = test_client.put(url, headers={"Accept": "application/json"})
        assert response.status_code == 201
        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert len(parsing_requests) == 1
        assert parsing_requests[0].bundle_name == "dags-folder"
        assert parsing_requests[0].relative_fileloc == test_dag.relative_fileloc
        _check_last_log(session, dag_id=None, event="reparse_dag_file", logical_date=None)

        # Duplicate file parsing request
        response = test_client.put(url, headers={"Accept": "application/json"})
        assert response.status_code == 409
        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert len(parsing_requests) == 1
        assert parsing_requests[0].bundle_name == "dags-folder"
        assert parsing_requests[0].relative_fileloc == test_dag.relative_fileloc
        _check_last_log(session, dag_id=None, event="reparse_dag_file", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.put(
            "/parseDagFile/token", headers={"Accept": "application/json"}
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.put("/parseDagFile/token", headers={"Accept": "application/json"})
        assert response.status_code == 403

    def test_bad_file_request(self, url_safe_serializer, session, test_client):
        payload = {"bundle_name": "some_bundle", "relative_fileloc": "/some/random/file.py"}
        url = f"/parseDagFile/{url_safe_serializer.dumps(payload)}"
        response = test_client.put(url, headers={"Accept": "application/json"})
        assert response.status_code == 404

        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert parsing_requests == []
