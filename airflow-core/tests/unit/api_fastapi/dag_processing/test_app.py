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

from airflow.api_fastapi.dag_processing.app import create_dag_processing_api_app
from airflow.api_fastapi.dag_processing.security import require_dag_processing_auth

from tests_common.test_utils.config import conf_vars

APP = "airflow.api_fastapi.dag_processing.app"


@pytest.fixture
def client():
    # Endpoint-logic tests bypass the token check; auth itself is covered separately below.
    app = create_dag_processing_api_app()
    app.dependency_overrides[require_dag_processing_auth] = lambda: None
    return TestClient(app)


def test_health(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "healthy"}


def test_persistence_endpoint_requires_a_token():
    """A persistence endpoint rejects an unauthenticated call (no token)."""
    unauth = TestClient(create_dag_processing_api_app(), raise_server_exceptions=False)
    resp = unauth.post("/bundles/sync")
    assert resp.status_code == 401


def test_persistence_endpoint_rejects_invalid_token():
    """A persistence endpoint rejects a malformed/garbage token."""
    unauth = TestClient(create_dag_processing_api_app(), raise_server_exceptions=False)
    resp = unauth.post("/bundles/sync", headers={"Authorization": "Bearer not-a-real-token"})
    assert resp.status_code == 403


def test_health_does_not_require_a_token():
    """``/health`` stays open so external readiness probes work."""
    unauth = TestClient(create_dag_processing_api_app())
    assert unauth.get("/health").status_code == 200


def test_persistence_endpoint_accepts_a_valid_token():
    """A token signed with the deployment key for the DAG-processing audience is accepted."""
    from airflow.api_fastapi.auth.tokens import JWTGenerator, get_signing_args
    from airflow.api_fastapi.dag_processing.security import _token_validator
    from airflow.configuration import conf

    with conf_vars({("api_auth", "jwt_secret"): "dag-processing-test-secret"}):
        _token_validator.cache_clear()
        # A trusted component (not the DAG processor) mints the token the processor presents.
        # Mirror the audience/issuer the validator reads so the token is accepted.
        token = JWTGenerator(
            valid_for=300,
            audience=conf.get_mandatory_list_value("dag_processor", "jwt_audience")[0],
            issuer=conf.get("api_auth", "jwt_issuer", fallback=None),
            **get_signing_args(make_secret_key_if_needed=False),
        ).generate({"sub": "dag-processor"})
        authed = TestClient(create_dag_processing_api_app(), raise_server_exceptions=False)
        with mock.patch(f"{APP}.DagBundlesManager"):
            resp = authed.post("/bundles/sync", headers={"Authorization": f"Bearer {token}"})
    _token_validator.cache_clear()
    assert resp.status_code == 200


def test_persist_parsing_results_forwards_to_update_db(client):
    with (
        mock.patch(f"{APP}.update_dag_parsing_results_in_db") as update_db,
        mock.patch(f"{APP}.LazyDeserializedDAG") as lazy_dag,
        mock.patch(f"{APP}.create_session"),
    ):
        lazy_dag.model_validate.side_effect = lambda d: d
        body = {
            "bundle_name": "b1",
            "bundle_version": "v1",
            "relative_fileloc": "dags/a.py",
            "run_duration": 1.5,
            "serialized_dags": [{"data": {"dag": {"dag_id": "x"}}}],
            "import_errors": {"dags/a.py": "boom"},
            "warnings": [],
        }
        resp = client.post("/parsing-results", json=body)

    assert resp.status_code == 201
    update_db.assert_called_once()
    kwargs = update_db.call_args.kwargs
    assert kwargs["bundle_name"] == "b1"
    assert kwargs["bundle_version"] == "v1"
    # import errors are re-keyed to (bundle_name, relative_fileloc) tuples
    assert kwargs["import_errors"] == {("b1", "dags/a.py"): "boom"}
    # files_parsed includes the parsed file plus any import-error files
    assert ("b1", "dags/a.py") in kwargs["files_parsed"]


def test_persist_parsing_results_without_fileloc_has_no_files_parsed(client):
    with (
        mock.patch(f"{APP}.update_dag_parsing_results_in_db") as update_db,
        mock.patch(f"{APP}.LazyDeserializedDAG"),
        mock.patch(f"{APP}.create_session"),
    ):
        resp = client.post("/parsing-results", json={"bundle_name": "b1", "serialized_dags": []})

    assert resp.status_code == 201
    assert update_db.call_args.kwargs["files_parsed"] is None


def test_persist_parsing_results_invalid_payload_returns_422(client):
    with (
        mock.patch(f"{APP}.LazyDeserializedDAG") as lazy_dag,
        mock.patch(f"{APP}.update_dag_parsing_results_in_db") as update_db,
    ):
        lazy_dag.model_validate.side_effect = ValueError("bad blob")
        resp = client.post("/parsing-results", json={"bundle_name": "b1", "serialized_dags": [{"data": {}}]})

    assert resp.status_code == 422
    update_db.assert_not_called()


def test_reconcile_deactivates_and_removes_references(client):
    with (
        mock.patch(f"{APP}.DagModel") as dag_model,
        mock.patch(f"{APP}.remove_references_to_deleted_dags") as remove_refs,
        mock.patch(f"{APP}.create_session") as create_session,
    ):
        dag_model.deactivate_deleted_dags.return_value = True
        create_session.return_value.__enter__.return_value.scalars.return_value = []
        resp = client.post("/bundles/b1/reconcile", json={"observed_filelocs": ["dags/a.py"]})

    assert resp.status_code == 200
    assert resp.json() == {"bundle_name": "b1", "deactivated": True}
    dag_model.deactivate_deleted_dags.assert_called_once()
    remove_refs.assert_called_once()


def test_reconcile_skips_remove_references_when_nothing_deactivated(client):
    with (
        mock.patch(f"{APP}.DagModel") as dag_model,
        mock.patch(f"{APP}.remove_references_to_deleted_dags") as remove_refs,
        mock.patch(f"{APP}.create_session") as create_session,
    ):
        dag_model.deactivate_deleted_dags.return_value = False
        create_session.return_value.__enter__.return_value.scalars.return_value = []
        resp = client.post("/bundles/b1/reconcile", json={"observed_filelocs": []})

    assert resp.status_code == 200
    assert resp.json()["deactivated"] is False
    remove_refs.assert_not_called()


def test_reconcile_import_error_cleanup_failure_is_swallowed(client):
    """A cleanup failure must not fail the request nor (being a separate txn) undo deactivation."""
    with (
        mock.patch(f"{APP}.DagModel") as dag_model,
        mock.patch(f"{APP}.remove_references_to_deleted_dags"),
        mock.patch(f"{APP}.create_session") as create_session,
    ):
        dag_model.deactivate_deleted_dags.return_value = True
        # The import-error cleanup block calls session.scalars(); make it blow up.
        create_session.return_value.__enter__.return_value.scalars.side_effect = RuntimeError("db down")
        resp = client.post("/bundles/b1/reconcile", json={"observed_filelocs": ["dags/a.py"]})

    assert resp.status_code == 200
    # Deactivation still ran (in its own, earlier transaction).
    dag_model.deactivate_deleted_dags.assert_called_once()


def test_get_bundle_state_found(client):
    from datetime import datetime, timezone

    row = mock.MagicMock(last_refreshed=datetime(2024, 1, 1, tzinfo=timezone.utc), version="v1")
    with (
        mock.patch(f"{APP}.create_session") as create_session,
    ):
        create_session.return_value.__enter__.return_value.scalar.return_value = row
        resp = client.get("/bundles/b1/state")

    assert resp.status_code == 200
    body = resp.json()
    assert body["found"] is True
    assert body["version"] == "v1"


def test_get_bundle_state_not_found(client):
    with (
        mock.patch(f"{APP}.create_session") as create_session,
    ):
        create_session.return_value.__enter__.return_value.scalar.return_value = None
        resp = client.get("/bundles/missing/state")

    assert resp.status_code == 200
    assert resp.json() == {"found": False, "last_refreshed": None, "version": None}


def test_update_bundle_state_includes_version_when_provided(client):
    with (
        mock.patch(f"{APP}.update") as update_stmt,
        mock.patch(f"{APP}.create_session") as create_session,
    ):
        resp = client.patch(
            "/bundles/b1/state",
            json={"last_refreshed": "2024-06-01T08:00:00+00:00", "version": "abc"},
        )

    assert resp.status_code == 200
    # version supplied -> both columns written
    update_stmt.return_value.where.return_value.values.assert_called_once_with(
        last_refreshed=mock.ANY, version="abc"
    )
    create_session.return_value.__enter__.return_value.execute.assert_called_once()


def test_update_bundle_state_omits_version_when_null(client):
    with (
        mock.patch(f"{APP}.update") as update_stmt,
        mock.patch(f"{APP}.create_session"),
    ):
        resp = client.patch(
            "/bundles/b1/state",
            json={"last_refreshed": "2024-06-01T08:00:00+00:00", "version": None},
        )

    assert resp.status_code == 200
    # version omitted -> only last_refreshed written (stored version left unchanged)
    update_stmt.return_value.where.return_value.values.assert_called_once_with(last_refreshed=mock.ANY)


def test_sync_bundles_triggers_server_side_sync(client):
    with mock.patch(f"{APP}.DagBundlesManager") as dbm:
        resp = client.post("/bundles/sync")

    assert resp.status_code == 200
    dbm.return_value.sync_bundles_to_db.assert_called_once_with()


def test_deactivate_stale_dags_inactive_bundle_and_threshold(client):
    from datetime import datetime, timedelta, timezone

    now = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
    r_inactive = mock.MagicMock(
        dag_id="d_inactive", bundle_name="dead", last_parsed_time=now, relative_fileloc="a.py"
    )
    r_stale = mock.MagicMock(
        dag_id="d_stale",
        bundle_name="live",
        last_parsed_time=now - timedelta(hours=2),
        relative_fileloc="b.py",
    )
    r_fresh = mock.MagicMock(
        dag_id="d_fresh", bundle_name="live", last_parsed_time=now, relative_fileloc="c.py"
    )
    with mock.patch(f"{APP}.create_session") as create_session:
        session = create_session.return_value.__enter__.return_value
        session.scalars.return_value.all.return_value = ["dead"]
        session.execute.side_effect = [[r_inactive, r_stale, r_fresh], mock.MagicMock(rowcount=2)]
        body = {
            "stale_dag_threshold": 60,
            "last_parsed": [
                {"bundle_name": "live", "relative_fileloc": "b.py", "last_finish_time": now.isoformat()},
                {"bundle_name": "live", "relative_fileloc": "c.py", "last_finish_time": now.isoformat()},
            ],
        }
        resp = client.post("/stale-dags", json=body)

    assert resp.status_code == 200
    # d_inactive (dead bundle) + d_stale (past threshold); d_fresh stays.
    assert resp.json()["deactivated"] == 2


def test_purge_warnings_endpoint(client):
    with mock.patch(f"{APP}.DagWarning") as dag_warning:
        resp = client.post("/purge-warnings")

    assert resp.status_code == 200
    dag_warning.purge_inactive_dag_warnings.assert_called_once_with()


def test_claim_priority_parse_requests(client):
    req = mock.MagicMock(bundle_name="b1", relative_fileloc="dags/a.py")
    with mock.patch(f"{APP}.create_session") as create_session:
        session = create_session.return_value.__enter__.return_value
        session.scalars.return_value = [req]
        resp = client.post("/priority-parse-requests/claim", json={"bundle_names": ["b1"]})

    assert resp.status_code == 200
    assert resp.json()["claimed"] == [{"bundle_name": "b1", "relative_fileloc": "dags/a.py"}]
    session.delete.assert_called_once_with(req)


def test_claim_callbacks_skip_locked_and_delete(client):
    cb = mock.MagicMock(data={"req_class": "DagCallbackRequest", "req_data": "{}"})
    with (
        mock.patch(f"{APP}.with_row_locks"),
        mock.patch(f"{APP}.prohibit_commit") as prohibit,
        mock.patch(f"{APP}.create_session") as create_session,
    ):
        session = create_session.return_value.__enter__.return_value
        session.scalars.return_value = [cb]
        resp = client.post("/callbacks/claim", json={"bundle_names": ["b1"], "limit": 5})

    assert resp.status_code == 200
    assert resp.json()["callbacks"] == [{"req_class": "DagCallbackRequest", "req_data": "{}"}]
    session.delete.assert_called_once_with(cb)
    # the claim is committed via the prohibit_commit guard
    prohibit.return_value.__enter__.return_value.commit.assert_called_once()


def test_register_job(client):
    job = mock.MagicMock(id=42)
    with mock.patch(f"{APP}.Job", return_value=job), mock.patch(f"{APP}.create_session"):
        resp = client.post("/jobs", json={"job_type": "DagProcessorJob"})

    assert resp.status_code == 201
    assert resp.json() == {"job_id": 42}
    job.prepare_for_execution.assert_called_once()


def test_job_heartbeat_updates_latest_heartbeat(client):
    job = mock.MagicMock()
    with mock.patch(f"{APP}.create_session") as create_session:
        create_session.return_value.__enter__.return_value.get.return_value = job
        resp = client.post("/jobs/7/heartbeat")

    assert resp.status_code == 200
    assert resp.json() == {"alive": True}
    assert job.latest_heartbeat is not None


def test_job_heartbeat_missing_returns_404(client):
    with mock.patch(f"{APP}.create_session") as create_session:
        create_session.return_value.__enter__.return_value.get.return_value = None
        resp = client.post("/jobs/7/heartbeat")

    assert resp.status_code == 404


def test_complete_job_sets_state(client):
    job = mock.MagicMock()
    with mock.patch(f"{APP}.create_session") as create_session:
        create_session.return_value.__enter__.return_value.get.return_value = job
        resp = client.post("/jobs/7/complete", json={"state": "success"})

    assert resp.status_code == 200
    assert job.state == "success"
