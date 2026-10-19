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
from fastapi import HTTPException
from sqlalchemy import select
from uuid6 import uuid7

from airflow.api_fastapi.auth.managers.simple.user import SimpleAuthManagerUser
from airflow.api_fastapi.execution_api.datamodels.dag_parsing import (
    ProcessorWorkAckBody,
    ProcessorWorkClaimBody,
)
from airflow.api_fastapi.execution_api.datamodels.token import DagProcessorClaims, DagProcessorToken
from airflow.api_fastapi.execution_api.routes.dag_parsing import acknowledge_work, claim_work
from airflow.jobs.job import Job, JobState
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
def dag_reader_headers(test_client):
    """A caller who may read the Dags (and import errors) but not edit them: viewer is below the role edits require."""
    auth_manager = test_client.app.state.auth_manager
    token = auth_manager._get_token_signer().generate(
        auth_manager.serialize_user(SimpleAuthManagerUser(username="reader", role="viewer"))
    )
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture
def processor_job(session):
    job = Job(job_type="DagProcessorJob", state=JobState.RUNNING)
    job.session_id = uuid7()
    job.registration_id = uuid7()
    job.bundle_names = ["example_dags"]
    session.add(job)
    session.commit()
    yield job
    session.rollback()
    session.delete(job)
    session.commit()


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

    def test_reparse_claimed_request_fences_previous_acknowledgment(
        self, session, test_client, processor_job
    ):
        parse_and_sync_to_db(EXAMPLE_DAG_FILE)
        file_token = test_client.get(f"/dags/{TEST_DAG_ID}").json()["file_token"]
        url = f"/parseDagFile/{file_token}"
        assert test_client.put(url).status_code == 201
        work = session.scalars(select(DagPriorityParsingRequest)).one()
        key = work.id
        previous_claim_id = uuid7()
        token = DagProcessorToken(
            id=processor_job.session_id,
            claims=DagProcessorClaims(
                exp=1, job_id=processor_job.id, dag_bundles=frozenset(processor_job.bundle_names)
            ),
        )
        previous_claim = ProcessorWorkClaimBody(
            claim_id=previous_claim_id, claim_sequence=1, bundle_names=["example_dags"]
        )
        claimed = claim_work(processor_job.id, "priority", previous_claim, session=session, token=token)
        assert [item.id for item in claimed] == [key]
        session.commit()

        assert test_client.put(url).status_code == 201

        session.expire_all()
        work = session.scalars(select(DagPriorityParsingRequest)).one()
        assert work.id == key
        assert work.processor_job_id is None
        assert work.processor_claim_id is None
        assert test_client.put(url).status_code == 409
        previous_ack = ProcessorWorkAckBody(claim_id=previous_claim_id)
        with pytest.raises(HTTPException) as error:
            acknowledge_work(processor_job.id, "priority", key, previous_ack, session=session, token=token)
        assert error.value.status_code == 409
        session.rollback()
        assert claim_work(processor_job.id, "priority", previous_claim, session=session, token=token) == []

        next_claim_id = uuid7()
        claimed = claim_work(
            processor_job.id,
            "priority",
            ProcessorWorkClaimBody(claim_id=next_claim_id, claim_sequence=2, bundle_names=["example_dags"]),
            session=session,
            token=token,
        )
        assert len(claimed) == 1
        assert claimed[0].id == key
        assert claimed[0].claim_id == next_claim_id
        session.commit()
        with pytest.raises(HTTPException) as error:
            acknowledge_work(processor_job.id, "priority", key, previous_ack, session=session, token=token)
        assert error.value.status_code == 409
        session.rollback()
        acknowledge_work(
            processor_job.id,
            "priority",
            key,
            ProcessorWorkAckBody(claim_id=next_claim_id),
            session=session,
            token=token,
        )
        session.commit()
        assert session.get(DagPriorityParsingRequest, key) is None

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

    def test_reparse_import_error_file_forbidden_for_basic_import_errors_viewer(
        self, url_safe_serializer, session, dag_reader_headers, test_client
    ):
        # Reparsing a file with no registered Dag requires the dedicated REPARSE_ALL permission
        # (admin-by-default), so a caller who can view the import-errors list (basic IMPORT_ERRORS)
        # but lacks REPARSE_ALL must not be able to reparse it.
        session.add(ParseImportError(bundle_name="some_bundle", filename="dags/broken.py", stacktrace="boom"))
        session.commit()
        token = url_safe_serializer.dumps(
            {"bundle_name": "some_bundle", "relative_fileloc": "dags/broken.py"}
        )

        response = test_client.put(
            f"/parseDagFile/{token}", headers={"Accept": "application/json", **dag_reader_headers}
        )

        assert response.status_code == 403
        assert (
            response.json()["detail"] == "You do not have permission to reparse files with no registered Dag"
        )
        assert session.scalars(select(DagPriorityParsingRequest)).all() == []
