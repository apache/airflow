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

from unittest import mock

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select

from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.models.dagbag import DagPriorityParsingRequest, DBDagBag
from airflow.models.errors import ParseImportError

from tests_common.test_utils.api_fastapi import _check_last_log
from tests_common.test_utils.db import (
    clear_db_dag_parsing_requests,
    clear_db_import_errors,
    clear_db_logs,
    parse_and_sync_to_db,
)
from tests_common.test_utils.paths import AIRFLOW_CORE_SOURCES_PATH

pytestmark = pytest.mark.db_test

EXAMPLE_DAG_FILE = AIRFLOW_CORE_SOURCES_PATH / "airflow" / "example_dags" / "example_simplest_dag.py"
TEST_DAG_ID = "example_simplest_dag"
NOT_READABLE_DAG_ID = "latest_only_with_trigger"
TEST_MULTIPLE_DAGS_ID = "asset_produces_1"


@pytest.fixture
def dag_reader_test_client(test_client):
    """A caller who may read the Dags (and import errors) but not edit them: viewer is below the role edits require."""
    auth_manager = test_client.app.state.auth_manager
    token = auth_manager._get_token_signer().generate(
        auth_manager.serialize_user(SimpleAuthManagerUser(username="reader", role="viewer"))
    )
    with mock.patch("airflow.models.revoked_token.RevokedToken.is_revoked", return_value=False):
        yield TestClient(
            test_client.app,
            headers={"Authorization": f"Bearer {token}"},
            base_url=str(test_client.base_url),
        )


class TestDagParsingEndpoint:
    @staticmethod
    def clear_db():
        clear_db_dag_parsing_requests()
        clear_db_import_errors()

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
        assert parsing_requests[0].bundle_name == "example_dags"
        assert parsing_requests[0].relative_fileloc == test_dag.relative_fileloc
        _check_last_log(session, dag_id=None, event="reparse_dag_file", logical_date=None)

        # Duplicate file parsing request
        response = test_client.put(url, headers={"Accept": "application/json"})
        assert response.status_code == 409
        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert len(parsing_requests) == 1
        assert parsing_requests[0].bundle_name == "example_dags"
        assert parsing_requests[0].relative_fileloc == test_dag.relative_fileloc
        _check_last_log(session, dag_id=None, event="reparse_dag_file", logical_date=None)

    def test_should_respond_401(self, unauthenticated_test_client):
        response = unauthenticated_test_client.put(
            "/parseDagFile/token", headers={"Accept": "application/json"}
        )
        assert response.status_code == 401

    def test_should_respond_403(self, unauthorized_test_client, url_safe_serializer, session):
        parse_and_sync_to_db(EXAMPLE_DAG_FILE)
        test_dag = DBDagBag(load_op_links=False).get_latest_version_of_dag(TEST_DAG_ID, session=session)
        token = url_safe_serializer.dumps(
            {"bundle_name": "example_dags", "relative_fileloc": test_dag.relative_fileloc}
        )
        response = unauthorized_test_client.put(
            f"/parseDagFile/{token}", headers={"Accept": "application/json"}
        )
        assert response.status_code == 403

    def test_bad_file_request(self, url_safe_serializer, session, test_client):
        payload = {"bundle_name": "some_bundle", "relative_fileloc": "/some/random/file.py"}
        url = f"/parseDagFile/{url_safe_serializer.dumps(payload)}"
        response = test_client.put(url, headers={"Accept": "application/json"})
        assert response.status_code == 404

        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert parsing_requests == []

    def test_reparse_import_error_file(self, url_safe_serializer, session, test_client):
        # A file with an import error has no registered Dags, but reparse must still be allowed
        # so the user can retry after fixing the file.
        session.add(ParseImportError(bundle_name="some_bundle", filename="dags/broken.py", stacktrace="boom"))
        session.commit()
        token = url_safe_serializer.dumps(
            {"bundle_name": "some_bundle", "relative_fileloc": "dags/broken.py"}
        )

        response = test_client.put(f"/parseDagFile/{token}", headers={"Accept": "application/json"})

        assert response.status_code == 201
        parsing_requests = session.scalars(select(DagPriorityParsingRequest)).all()
        assert len(parsing_requests) == 1
        assert parsing_requests[0].bundle_name == "some_bundle"
        assert parsing_requests[0].relative_fileloc == "dags/broken.py"

    def test_reparse_import_error_file_forbidden(
        self, url_safe_serializer, session, unauthorized_test_client
    ):
        session.add(ParseImportError(bundle_name="some_bundle", filename="dags/broken.py", stacktrace="boom"))
        session.commit()
        token = url_safe_serializer.dumps(
            {"bundle_name": "some_bundle", "relative_fileloc": "dags/broken.py"}
        )

        response = unauthorized_test_client.put(
            f"/parseDagFile/{token}", headers={"Accept": "application/json"}
        )

        assert response.status_code == 403
        assert session.scalars(select(DagPriorityParsingRequest)).all() == []

    def test_reparse_import_error_file_requires_dag_edit_not_just_view(
        self, url_safe_serializer, session, dag_reader_test_client
    ):
        # Reparse is a write action: a caller who can view import errors but not edit Dags
        # (viewer) must not be able to trigger a reparse of an errored file.
        session.add(ParseImportError(bundle_name="some_bundle", filename="dags/broken.py", stacktrace="boom"))
        session.commit()
        token = url_safe_serializer.dumps(
            {"bundle_name": "some_bundle", "relative_fileloc": "dags/broken.py"}
        )

        response = dag_reader_test_client.put(
            f"/parseDagFile/{token}", headers={"Accept": "application/json"}
        )

        assert response.status_code == 403
        assert session.scalars(select(DagPriorityParsingRequest)).all() == []
