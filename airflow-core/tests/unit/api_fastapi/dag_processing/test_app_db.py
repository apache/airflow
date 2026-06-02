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
"""Real-database tests for the DAG Processing API persistence endpoints.

These complement ``test_app.py`` (which mocks ``create_session``/``with_row_locks``)
by running the endpoints against a real session, so the actual SQL -- ordering,
limit, WHERE clauses, lock, and delete -- is exercised end to end.
"""

from __future__ import annotations

import json
from datetime import timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import select, update

from airflow._shared.timezones import timezone
from airflow.api_fastapi.dag_processing.app import create_dag_processing_api_app
from airflow.api_fastapi.dag_processing.security import require_dag_processing_auth
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.models.dagbundle import DagBundleModel
from airflow.models.db_callback_request import DbCallbackRequest
from airflow.models.errors import ParseImportError
from airflow.models.serialized_dag import SerializedDagModel

from tests_common.test_utils.db import (
    clear_db_callbacks,
    clear_db_dag_bundles,
    clear_db_dag_parsing_requests,
    clear_db_dags,
    clear_db_import_errors,
    clear_db_serialized_dags,
)

pytestmark = pytest.mark.db_test


@pytest.fixture
def client():
    # The token check is covered in test_app.py; here we bypass it and exercise the real DB.
    app = create_dag_processing_api_app()
    app.dependency_overrides[require_dag_processing_auth] = lambda: None
    return TestClient(app)


def _make_callback(*, bundle_name: str, run_id: str) -> DagCallbackRequest:
    return DagCallbackRequest(
        dag_id="test_dag",
        bundle_name=bundle_name,
        bundle_version=None,
        filepath="some_dag.py",
        is_failure_callback=True,
        run_id=run_id,
    )


class TestClaimCallbacks:
    def setup_method(self):
        clear_db_callbacks()

    def teardown_method(self):
        clear_db_callbacks()

    def test_claim_orders_by_priority_honors_limit_filters_bundle_and_deletes(self, client, session):
        """POST /callbacks/claim returns the owned bundle's callbacks ordered by priority_weight
        DESC up to ``limit``, deletes the claimed rows, and leaves the other bundle's row and the
        rows beyond the limit untouched."""
        # Arrange: three callbacks for the owned bundle (varying priority) + one for another bundle.
        session.add(
            DbCallbackRequest(callback=_make_callback(bundle_name="owned", run_id="low"), priority_weight=1)
        )
        session.add(
            DbCallbackRequest(callback=_make_callback(bundle_name="owned", run_id="high"), priority_weight=10)
        )
        session.add(
            DbCallbackRequest(callback=_make_callback(bundle_name="owned", run_id="mid"), priority_weight=5)
        )
        session.add(
            DbCallbackRequest(
                callback=_make_callback(bundle_name="other", run_id="other"), priority_weight=100
            )
        )
        session.commit()

        # Act: claim from the owned bundle with a limit of 2.
        resp = client.post("/callbacks/claim", json={"bundle_names": ["owned"], "limit": 2})

        # Assert: highest-priority owned callbacks first, count capped at the limit.
        assert resp.status_code == 200
        claimed = resp.json()["callbacks"]
        assert len(claimed) == 2
        run_ids = [json.loads(cb["req_data"])["run_id"] for cb in claimed]
        assert run_ids == ["high", "mid"]

        # The claimed rows are deleted; the lower-priority owned row and the other bundle's row remain.
        remaining = session.scalars(select(DbCallbackRequest)).all()
        remaining_run_ids = {json.loads(cb.data["req_data"])["run_id"] for cb in remaining}
        assert remaining_run_ids == {"low", "other"}


class TestDeactivateStaleDags:
    def setup_method(self):
        clear_db_serialized_dags()
        clear_db_dags()
        clear_db_dag_bundles()

    def teardown_method(self):
        clear_db_serialized_dags()
        clear_db_dags()
        clear_db_dag_bundles()

    def test_marks_inactive_bundle_and_past_threshold_dags_keeps_fresh_and_serialized(
        self, client, dag_maker, session
    ):
        """POST /stale-dags marks the inactive-bundle DAG and the past-threshold DAG stale, leaves
        the freshly parsed DAG active, and preserves the SerializedDagModel row."""
        now = timezone.utcnow()
        threshold = 60

        # A freshly parsed DAG with a real SerializedDagModel, in an active bundle.
        with dag_maker(dag_id="fresh_dag", bundle_name="live", serialized=True, session=session):
            pass
        dag_maker.dag_model.last_parsed_time = now
        dag_maker.dag_model.relative_fileloc = "fresh.py"
        dag_maker.dag_model.is_stale = False
        session.merge(dag_maker.dag_model)

        # An inactive bundle plus a DAG that lives in it.
        session.add(DagBundleModel(name="gone-bundle"))
        session.flush()
        session.execute(
            update(DagBundleModel).where(DagBundleModel.name == "gone-bundle").values(active=False)
        )
        session.add(
            DagModel(
                dag_id="dag_in_inactive_bundle",
                bundle_name="gone-bundle",
                relative_fileloc="inactive.py",
                last_parsed_time=now,
                is_stale=False,
            )
        )
        # A DAG in the active bundle whose last parse predates last_finish_time by more than the threshold.
        session.add(
            DagModel(
                dag_id="dag_past_threshold",
                bundle_name="live",
                relative_fileloc="stale.py",
                last_parsed_time=now - timedelta(hours=2),
                is_stale=False,
            )
        )
        session.commit()

        # Act: last_finish_time for the live-bundle files is "now"; fresh.py's gap is under the
        # threshold, stale.py's gap (2h) is over it.
        body = {
            "stale_dag_threshold": threshold,
            "last_parsed": [
                {"bundle_name": "live", "relative_fileloc": "fresh.py", "last_finish_time": now.isoformat()},
                {"bundle_name": "live", "relative_fileloc": "stale.py", "last_finish_time": now.isoformat()},
            ],
        }
        resp = client.post("/stale-dags", json=body)

        # Assert: two DAGs deactivated; the fresh DAG stays active.
        assert resp.status_code == 200
        assert resp.json()["deactivated"] == 2

        is_stale_by_dag = dict(
            session.execute(
                select(DagModel.dag_id, DagModel.is_stale).where(
                    DagModel.dag_id.in_(["fresh_dag", "dag_in_inactive_bundle", "dag_past_threshold"])
                )
            ).all()
        )
        assert is_stale_by_dag == {
            "fresh_dag": False,
            "dag_in_inactive_bundle": True,
            "dag_past_threshold": True,
        }

        # Deactivating DagModel rows must not delete the SerializedDagModel history.
        serialized_count = session.scalar(
            select(SerializedDagModel.dag_id).where(SerializedDagModel.dag_id == "fresh_dag")
        )
        assert serialized_count == "fresh_dag"


class TestReconcileImportErrors:
    def setup_method(self):
        clear_db_import_errors()
        clear_db_serialized_dags()
        clear_db_dags()

    def teardown_method(self):
        clear_db_import_errors()
        clear_db_serialized_dags()
        clear_db_dags()

    def test_deletes_absent_file_error_but_keeps_observed_and_zip_inner_errors(self, client, session):
        """POST /bundles/{name}/reconcile deletes the import error for a file no longer observed,
        while retaining the error for a still-observed file and for a zip inner path that is
        observed (e.g. ``test_zip.zip/broken_dag.py``)."""
        bundle = "testing"
        # One error for a normal file that is still present, one zip-inner error that is still
        # observed, and one for a file that is now absent from the bundle.
        session.add(
            ParseImportError(
                filename="present_dag.py",
                bundle_name=bundle,
                timestamp=timezone.utcnow(),
                stacktrace="present error",
            )
        )
        session.add(
            ParseImportError(
                filename="test_zip.zip/broken_dag.py",
                bundle_name=bundle,
                timestamp=timezone.utcnow(),
                stacktrace="zip import error",
            )
        )
        session.add(
            ParseImportError(
                filename="absent_dag.py",
                bundle_name=bundle,
                timestamp=timezone.utcnow(),
                stacktrace="absent error",
            )
        )
        session.commit()

        # Act: observed set includes the normal file and the zip inner path, but not absent_dag.py.
        resp = client.post(
            f"/bundles/{bundle}/reconcile",
            json={"observed_filelocs": ["present_dag.py", "test_zip.zip/broken_dag.py"]},
        )

        # Assert: only the absent file's error is removed.
        assert resp.status_code == 200
        remaining = {
            err.filename
            for err in session.scalars(select(ParseImportError).where(ParseImportError.bundle_name == bundle))
        }
        assert remaining == {"present_dag.py", "test_zip.zip/broken_dag.py"}


class TestClaimPriorityParseRequests:
    def setup_method(self):
        clear_db_dag_parsing_requests()

    def teardown_method(self):
        clear_db_dag_parsing_requests()

    def test_claims_owned_bundle_request_and_leaves_other_bundle_request(self, client, session):
        """POST /priority-parse-requests/claim returns and deletes the owned bundle's request while
        leaving a request that belongs to a bundle this processor does not own."""
        session.add(DagPriorityParsingRequest(bundle_name="owned", relative_fileloc="owned_file.py"))
        session.add(DagPriorityParsingRequest(bundle_name="other", relative_fileloc="other_file.py"))
        session.commit()

        # Act: claim only the owned bundle.
        resp = client.post("/priority-parse-requests/claim", json={"bundle_names": ["owned"]})

        # Assert: the owned request is returned and deleted; the other bundle's request remains.
        assert resp.status_code == 200
        assert resp.json()["claimed"] == [{"bundle_name": "owned", "relative_fileloc": "owned_file.py"}]

        remaining = session.scalars(select(DagPriorityParsingRequest)).all()
        assert len(remaining) == 1
        assert remaining[0].bundle_name == "other"
        assert remaining[0].relative_fileloc == "other_file.py"
