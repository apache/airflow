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
from fastapi import status
from sqlalchemy import select

from airflow.models import DagBag
from airflow.models.dagbag import DagPriorityParsingRequest

from tests_common.test_utils.api_fastapi import _check_last_log
from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_dag_bundles,
    clear_db_dag_parsing_requests,
    clear_db_dags,
    clear_db_logs,
    parse_and_sync_to_db,
)
from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH
from unit.models import TEST_DAGS_FOLDER

pytestmark = pytest.mark.db_test

EXAMPLE_DAG_FILE = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "example_dags" / "example_bash_operator.py"
TEST_DAG_ID = "example_bash_operator"
NOT_READABLE_DAG_ID = "latest_only_with_trigger"
TEST_MULTIPLE_DAGS_ID = "asset_produces_1"
API_PREFIX = "/api/v2/parseDagFile"


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
        dagbag = DagBag(read_dags_from_db=True)
        test_dag = dagbag.get_dag(TEST_DAG_ID)

        url = f"{API_PREFIX}/{url_safe_serializer.dumps(test_dag.fileloc)}"
        response = test_client.put(url, headers={"Accept": "application/json"})
        assert response.status_code == 201
        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert parsing_requests[0].fileloc == test_dag.fileloc
        _check_last_log(session, dag_id=None, event="reparse_dag_file", logical_date=None)

        # Duplicate file parsing request
        response = test_client.put(url, headers={"Accept": "application/json"})
        assert response.status_code == 409
        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert parsing_requests[0].fileloc == test_dag.fileloc
        _check_last_log(session, dag_id=None, event="reparse_dag_file", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.put(
            f"{API_PREFIX}/token", headers={"Accept": "application/json"}
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client):
        response = unauthorized_test_client.put(f"{API_PREFIX}/token", headers={"Accept": "application/json"})
        assert response.status_code == 403

    def test_bad_file_request(self, url_safe_serializer, session, test_client):
        url = f"{API_PREFIX}/{url_safe_serializer.dumps('/some/random/file.py')}"
        response = test_client.put(url, headers={"Accept": "application/json"})
        assert response.status_code == 404

        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert parsing_requests == []


class TestDagsReserializeAPI:
    BASE_URL = f"{API_PREFIX}/manage/reserialize"

    @pytest.fixture
    def test_bundles_config(self):
        """Fixture providing test bundle configuration"""
        return {
            "bundle1": TEST_DAGS_FOLDER / "test_example_bash_operator.py",
            "bundle2": TEST_DAGS_FOLDER / "test_sensor.py",
            "bundle3": TEST_DAGS_FOLDER / "test_dag_with_no_tags.py",
            "bundle4": TEST_DAGS_FOLDER.parent / "dags_with_system_exit" / "b_test_scheduler_dags.py",
        }

    @pytest.fixture(autouse=True)
    def setup_and_teardown(self, test_bundles_config, configure_dag_bundles):
        """Setup and teardown for each test."""
        clear_db_dag_parsing_requests()
        clear_db_logs()
        with configure_dag_bundles(test_bundles_config):
            for key, value in test_bundles_config.items():
                parse_and_sync_to_db(value, bundle_name=key)
            yield
        clear_db_dags()
        clear_db_dag_bundles()

    @pytest.mark.parametrize(
        "bundle_names,expected_count,expected_status",
        [
            ([], 4, status.HTTP_200_OK),  # All bundles
            (["bundle1"], 1, status.HTTP_200_OK),  # Single bundle
            (["bundle1", "bundle2"], 2, status.HTTP_200_OK),  # Multiple bundles
        ],
    )
    @conf_vars({("core", "load_examples"): "False"})
    def test_reserialize_bundles_success(
        self, test_client, session, test_bundles_config, bundle_names, expected_count, expected_status
    ):
        """Test successful bundle reserialize operations with various inputs."""
        response = test_client.post(self.BASE_URL, json={"bundle_names": bundle_names})
        assert response.status_code == expected_status

        # For empty list, all bundles should be processed
        expected_bundles = set(test_bundles_config.keys()) if not bundle_names else set(bundle_names)
        assert set(response.json()["processed_bundles"]) == expected_bundles

        # Verify parsing requests were created correctly
        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert len(parsing_requests) == expected_count

        expected_filelocs = {str(test_bundles_config[bundle]) for bundle in expected_bundles}
        assert {pr.fileloc for pr in parsing_requests} == expected_filelocs

        # Replace with actual method
        _check_last_log(session, dag_id=None, event="reserialize_dags", logical_date=None)

    @conf_vars({("core", "load_examples"): "False"})
    def test_reserialize_invalid_bundles(self, test_client, session):
        """Test invalid bundle names."""

        response = test_client.post(
            self.BASE_URL, json={"bundle_names": ["dags-folder", "nonexistent_bundle"]}
        )

        assert response.status_code == status.HTTP_400_BAD_REQUEST
        assert "Invalid bundle name" in response.json()["detail"]
        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert len(parsing_requests) == 0

    def test_should_respond_422(self, test_client):
        """Test empty json case"""

        response = test_client.post(self.BASE_URL, json={})
        assert response.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
