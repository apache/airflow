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

import json
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import Barrier, Event
from unittest import mock
from uuid import UUID

import pytest
from fastapi import Request
from sqlalchemy import delete, select
from sqlalchemy.exc import OperationalError
from uuid6 import uuid7

from airflow._shared.timezones import timezone
from airflow.api_fastapi.execution_api.datamodels.token import TIClaims, TIToken
from airflow.api_fastapi.execution_api.security import require_auth
from airflow.callbacks.callback_requests import DagCallbackRequest
from airflow.dag_processing.bundles.local import LocalDagBundle
from airflow.dag_processing.collection import DagModelOperation, update_dag_parsing_results_in_db
from airflow.dag_processing.manager import DagFileProcessorManager
from airflow.jobs.job import Job, JobState
from airflow.models.callback import DagProcessorCallback
from airflow.models.dag import DagModel
from airflow.models.dag_parse_checkpoint import DagParseCheckpoint
from airflow.models.dag_version import DagVersion
from airflow.models.dagbag import DagPriorityParsingRequest
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagcode import DagCode
from airflow.models.errors import ParseImportError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.sdk import DAG
from airflow.serialization.serialized_objects import LazyDeserializedDAG

from tests_common.test_utils.config import conf_vars
from tests_common.test_utils.db import (
    clear_db_callbacks,
    clear_db_dag_bundles,
    clear_db_dags,
    clear_db_import_errors,
    clear_db_jobs,
)

pytestmark = pytest.mark.db_test
SESSION_ID = UUID("00000000-0000-0000-0000-0000000000aa")


@pytest.fixture(autouse=True)
def clean_db(time_machine, session):
    time_machine.move_to("2026-10-05T12:00:00Z", tick=False)
    clear_db_jobs()
    clear_db_callbacks()
    session.execute(delete(DagPriorityParsingRequest))
    session.commit()
    clear_db_dags()
    clear_db_import_errors()
    clear_db_dag_bundles()
    yield
    clear_db_jobs()
    clear_db_callbacks()
    session.execute(delete(DagPriorityParsingRequest))
    session.commit()
    clear_db_dags()
    clear_db_import_errors()
    clear_db_dag_bundles()


@pytest.fixture
def job(session, exec_app):
    job = Job(job_type="DagProcessorJob", state=JobState.RUNNING)
    job.session_id = SESSION_ID
    job.registration_id = uuid7()
    job.bundle_names = ["bundle"]
    session.add_all([job, DagBundleModel(name="bundle"), DagBundleModel(name="other")])
    session.commit()

    async def authenticate(request: Request):
        return TIToken(
            id=SESSION_ID,
            claims=TIClaims(scope="dag_processor", job_id=job.id, dag_bundles=frozenset({"bundle"})),
        )

    exec_app.dependency_overrides[require_auth] = authenticate
    return job


@pytest.fixture
def body():
    dag = DAG("published", schedule=None)
    dag.fileloc = "/worker-only/dags/a.py"
    dag.relative_fileloc = "a.py"
    return {
        "attempt_id": str(uuid7()),
        "dispatch_sequence": 1,
        "bundle_name": "bundle",
        "relative_fileloc": "a.py",
        "bundle_version": "v1",
        "version_data": {"ref": "v1"},
        "parse_duration": 0.5,
        "serialized_dags": [LazyDeserializedDAG.from_dag(dag).data],
        "source_codes": {dag.fileloc: {"source_code": "# captured on worker", "language": "python"}},
    }


@pytest.mark.parametrize("source", ["# captured on worker", "", None])
@mock.patch.object(DagCode, "code", autospec=True, side_effect=AssertionError("Server read source"))
def test_publishes_worker_source_without_filesystem_access(mock_code, client, session, job, body, source):
    body["source_codes"]["/worker-only/dags/a.py"]["source_code"] = source
    response = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert response.status_code == 200, response.json()
    assert session.get(DagModel, "published").relative_fileloc == "a.py"
    assert session.scalar(select(SerializedDagModel)).dag_id == "published"
    assert session.scalar(select(DagCode)).source_code == (
        source if source is not None else "Source unavailable"
    )
    assert session.scalar(select(DagParseCheckpoint)).attempt_id == UUID(body["attempt_id"])
    mock_code.assert_not_called()


def test_replay_returns_receipt_without_rewriting_metadata(client, session, job, body, time_machine):
    first = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert first.status_code == 200, first.json()
    parsed_at = session.get(DagModel, "published").last_parsed_time
    time_machine.shift(10)
    replay = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert replay.json() == first.json()
    session.expire_all()
    assert session.get(DagModel, "published").last_parsed_time == parsed_at
    assert len(session.scalars(select(DagParseCheckpoint)).all()) == 1


@pytest.mark.parametrize("paused", [True, False])
def test_publication_preserves_pause_on_creation_and_existing_pause_state(client, session, job, body, paused):
    body["serialized_dags"][0]["dag"]["is_paused_upon_creation"] = paused
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 200
    assert session.get(DagModel, "published").is_paused is paused
    body.update(attempt_id=str(uuid7()), dispatch_sequence=2)
    body["serialized_dags"][0]["dag"]["is_paused_upon_creation"] = not paused
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 200
    session.expire_all()
    assert session.get(DagModel, "published").is_paused is paused


@pytest.mark.parametrize(
    ("change", "reason"), [("payload", "publication_conflict"), ("attempt", "publication_superseded")]
)
def test_rejects_conflicting_or_superseded_publication(client, session, job, body, change, reason):
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 200
    if change == "payload":
        body["parse_duration"] = 99
    else:
        body["attempt_id"] = str(uuid7())
    response = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert response.status_code == 409
    assert response.json()["detail"]["reason"] == reason
    assert session.get(DagModel, "published").last_parse_duration == 0.5


@conf_vars({("core", "min_serialized_dag_update_interval"): "0"})
def test_newer_result_supersedes_old_attempt(client, session, job, body):
    original = deepcopy(body)
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 200
    body.update(attempt_id=str(uuid7()), dispatch_sequence=2, parse_duration=2.0)
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 200
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=original).status_code == 409
    session.expire_all()
    assert session.get(DagModel, "published").last_parse_duration == 2.0
    assert len(session.scalars(select(DagParseCheckpoint)).all()) == 1


@pytest.mark.parametrize("same_bundle", [True, False])
def test_allows_file_relocation_but_not_cross_bundle_takeover(client, session, job, body, same_bundle):
    session.add(
        DagModel(
            dag_id="published", bundle_name="bundle" if same_bundle else "other", relative_fileloc="old.py"
        )
    )
    session.commit()
    response = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert response.status_code == (200 if same_bundle else 409), response.json()
    session.expire_all()
    assert session.get(DagModel, "published").relative_fileloc == ("a.py" if same_bundle else "old.py")


@pytest.mark.parametrize(
    "failure", ["bundle", "job", "closed", "replaced", "inactive", "deleted", "missing_bundle"]
)
def test_rejects_unauthorized_publication(client, session, job, body, failure):
    job_id = job.id
    if failure == "bundle":
        body["bundle_name"] = "other"
    elif failure == "job":
        job_id += 1000
    elif failure == "closed":
        job.end_date = job.start_date
    elif failure == "replaced":
        job.session_id = None
    elif failure == "deleted":
        session.delete(job)
    elif failure == "missing_bundle":
        session.delete(session.get(DagBundleModel, "bundle"))
    else:
        session.get(DagBundleModel, "bundle").active = False
    session.commit()
    response = client.post(f"/execution/jobs/{job_id}/parse-results", json=body)
    assert response.status_code == {"job": 404, "inactive": 409, "missing_bundle": 409}.get(failure, 403)
    assert session.get(DagModel, "published") is None
    assert session.scalar(select(DagParseCheckpoint)) is None


@pytest.mark.parametrize(
    "failure",
    [
        "schema",
        "version",
        "source",
        "path",
        "error_path",
        "warning",
        "duplicate",
        "dag_id",
        "relative_fileloc",
        "fileloc",
        "long_path",
    ],
)
def test_invalid_result_is_rejected_without_writes(client, session, job, body, failure):
    dag = body["serialized_dags"][0]
    if failure == "schema":
        dag["dag"]["tasks"] = "invalid"
    elif failure == "version":
        dag["__version"] = 999
    elif failure == "source":
        body["source_codes"] = {}
    elif failure == "path":
        dag["dag"]["relative_fileloc"] = "../other.py"
    elif failure == "error_path":
        body["import_errors"] = {"other.py": "error"}
    elif failure == "warning":
        body["warnings"] = [{"dag_id": "other", "warning_type": "duplicate dag id", "message": "error"}]
    elif failure == "duplicate":
        body["serialized_dags"].append(deepcopy(dag))
    elif failure == "long_path":
        body["parsed_definitions"] = ["a.py/" + "x" * 2000]
    else:
        dag["dag"].pop(failure)
    response = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert response.status_code == 422, response.json()
    assert session.get(DagModel, "published") is None
    assert session.scalar(select(DagParseCheckpoint)) is None


@mock.patch("airflow.api_fastapi.execution_api.routes.dag_parsing.MAX_PARSE_RESULT_BYTES", 1)
def test_rejects_oversized_result(client, session, job, body):
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 413
    assert session.scalar(select(DagParseCheckpoint)) is None


def test_rejects_non_json_version_metadata(client, session, job, body):
    body["version_data"] = {"invalid": float("nan")}
    response = client.post(f"/execution/jobs/{job.id}/parse-results", content=json.dumps(body))
    assert response.status_code == 422
    assert session.scalar(select(DagParseCheckpoint)) is None


@mock.patch(
    "airflow.dag_processing.collection._update_import_errors",
    autospec=True,
    side_effect=RuntimeError("write failed"),
)
def test_diagnostic_failure_rolls_back_metadata_and_receipt(mock_update, client, session, job, body):
    with pytest.raises(RuntimeError, match="write failed"):
        client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert session.get(DagModel, "published") is None
    assert session.scalar(select(SerializedDagModel)) is None
    assert session.scalar(select(DagParseCheckpoint)) is None


def test_empty_result_clears_file_errors_and_has_replay_receipt(client, session, job, body):
    body.update(serialized_dags=[], source_codes={}, import_errors={"a.py": "broken"})
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 200
    assert session.scalar(select(ParseImportError)) is not None
    body.update(attempt_id=str(uuid7()), dispatch_sequence=2, import_errors={})
    result = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert result.status_code == 200, result.json()
    assert session.scalar(select(ParseImportError)) is None
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).json() == result.json()


def test_container_result_keeps_member_errors_and_source(client, session, job, body):
    body["relative_fileloc"] = "bundle.zip"
    body["serialized_dags"][0]["dag"]["relative_fileloc"] = "bundle.zip/a.py"
    body["parsed_definitions"] = ["bundle.zip/a.py", "bundle.zip/b.py"]
    body["import_errors"] = {"bundle.zip/b.py": "bad member"}
    response = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert response.status_code == 200, response.json()
    assert session.scalar(select(ParseImportError)).filename == "bundle.zip/b.py"


@mock.patch(
    "airflow.api_fastapi.execution_api.routes.dag_parsing.DagSerialization.validate_serialized_dag",
    autospec=True,
)
@mock.patch(
    "airflow.api_fastapi.execution_api.routes.dag_parsing._reject_other_teams_plugin_classes",
    autospec=True,
    return_value=[],
)
def test_plugin_authorization_precedes_deserialization(reject, deserialize, client, session, job, body):
    response = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert response.status_code == 403
    deserialize.assert_not_called()
    assert session.scalar(select(DagParseCheckpoint)) is None


@pytest.mark.parametrize(
    ("stage", "error"),
    [
        ("_build_duplicate_dag_id_warnings", ValueError("failed")),
        ("_update_dag_warnings", ValueError("failed")),
        ("SerializedDagModel.write_dag", ValueError("failed")),
        ("SerializedDAG.bulk_write_to_db", OperationalError(None, None, RuntimeError("failed"))),
    ],
)
def test_persistence_errors_do_not_leave_accepted_partial_results(
    stage, error, client, session, job, body, mocker
):
    write = mocker.patch(f"airflow.dag_processing.collection.{stage}", autospec=True, side_effect=error)
    if isinstance(error, OperationalError):
        response = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
        assert response.status_code == 500
    else:
        with pytest.raises(type(error), match="failed"):
            client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    write.assert_called_once()
    assert session.get(DagModel, "published") is None
    assert session.scalar(select(DagParseCheckpoint)) is None


def test_other_job_publication_is_not_overwritten_by_receipt_recovery(client, session, exec_app, job, body):
    first = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert first.status_code == 200
    other = Job(job_type="DagProcessorJob", state=JobState.RUNNING)
    other.session_id = uuid7()
    other.registration_id = uuid7()
    other.bundle_names = ["bundle"]
    session.add(other)
    session.commit()
    identities = {job.id: SESSION_ID, other.id: other.session_id}

    async def authenticate(request: Request):
        job_id = int(request.path_params["job_id"])
        return TIToken(
            id=identities[job_id],
            claims=TIClaims(scope="dag_processor", job_id=job_id, dag_bundles=frozenset({"bundle"})),
        )

    exec_app.dependency_overrides[require_auth] = authenticate
    newer = {**body, "attempt_id": str(uuid7()), "parse_duration": 3.0}
    assert client.post(f"/execution/jobs/{other.id}/parse-results", json=newer).status_code == 200
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).json() == first.json()
    session.expire_all()
    assert session.get(DagModel, "published").last_parse_duration == 3.0
    assert len(session.scalars(select(DagParseCheckpoint)).all()) == 2


def test_job_cleanup_removes_its_checkpoint(client, session, job, body):
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 200
    session.delete(job)
    session.commit()
    assert session.scalar(select(DagParseCheckpoint)) is None
    assert session.get(DagModel, "published") is not None


@pytest.mark.backend("postgres")
def test_concurrent_replay_commits_one_publication(client, session, job, body):
    ready = Barrier(2)
    url = f"/execution/jobs/{job.id}/parse-results"

    def publish():
        ready.wait(timeout=10)
        return client.post(url, json=body)

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(publish) for _ in range(2)]
        responses = [future.result(timeout=30) for future in futures]
    assert [response.status_code for response in responses] == [200, 200]
    assert responses[0].json() == responses[1].json()
    assert len(session.scalars(select(DagParseCheckpoint)).all()) == 1


@conf_vars({("core", "auth_manager"): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"})
@pytest.mark.parametrize("fail", [False, True])
@mock.patch("airflow.dag_processing.collection._update_import_errors", autospec=True)
def test_fab_permissions_share_publication_transaction(update_errors, client, session, job, body, fail):
    security = pytest.importorskip("airflow.providers.fab.www.security_appless")
    dag_id = f"published_{uuid7().hex}"
    body["serialized_dags"][0]["dag"]["dag_id"] = dag_id
    if fail:
        update_errors.side_effect = RuntimeError("failure after Dag permissions")
        with pytest.raises(RuntimeError, match="failure after Dag permissions"):
            client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    else:
        response = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
        assert response.status_code == 200, response.json()
    session.expire_all()
    assert (session.get(DagModel, dag_id) is None) == fail
    assert (session.scalar(select(SerializedDagModel)) is None) == fail
    assert (session.scalar(select(DagParseCheckpoint)) is None) == fail
    manager = security.ApplessAirflowSecurityManager(session=session)
    assert (manager.get_resource(f"DAG:{dag_id}") is None) == fail


@pytest.mark.parametrize("ineligible", [None, "active_bundle", "active_dag", "file_location", "version"])
def test_recovers_only_orphaned_legacy_dags(client, session, job, body, ineligible):
    previous_bundle = session.get(DagBundleModel, "other")
    previous_bundle.active = ineligible == "active_bundle"
    session.add(
        DagModel(
            dag_id="published",
            bundle_name="other",
            relative_fileloc="old.py" if ineligible == "file_location" else None,
            fileloc="/old-host/dags/a.py",
            is_stale=ineligible != "active_dag",
        )
    )
    session.flush()
    if ineligible == "version":
        session.add(DagVersion(dag_id="published", bundle_name="other"))
    session.commit()
    response = client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    assert response.status_code == (409 if ineligible else 200), response.json()
    session.expire_all()
    dag = session.get(DagModel, "published")
    assert dag.bundle_name == ("other" if ineligible else "bundle")
    if ineligible is None:
        assert dag.relative_fileloc == "a.py"
        assert not dag.is_stale


@conf_vars({("core", "auth_manager"): "airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager"})
def test_failed_publication_rolls_back_permissions_when_dag_metadata_is_unchanged(
    client, session, job, body, mocker
):
    security = pytest.importorskip("airflow.providers.fab.www.security_appless")
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 200
    original_attempt = UUID(body["attempt_id"])
    manager = security.ApplessAirflowSecurityManager(session=session)
    role_name = f"publication_{uuid7().hex}"
    role = manager.add_role(role_name)
    dag = DAG("published", schedule=None, access_control={role_name: {"can_read"}})
    body["serialized_dags"][0]["dag"]["access_control"] = LazyDeserializedDAG.from_dag(dag).data["dag"][
        "access_control"
    ]
    body.update(attempt_id=str(uuid7()), dispatch_sequence=2)
    mocker.patch(
        "airflow.dag_processing.collection._update_import_errors",
        autospec=True,
        side_effect=RuntimeError("failure after permissions"),
    )
    with pytest.raises(RuntimeError, match="failure after permissions"):
        client.post(f"/execution/jobs/{job.id}/parse-results", json=body)
    session.expire_all()
    assert not role.permissions
    assert session.scalar(select(DagParseCheckpoint)).attempt_id == original_attempt
    session.delete(role)
    session.commit()


@pytest.mark.backend("postgres")
@pytest.mark.parametrize("race_stage", ["before_persistence", "missing_row"])
def test_concurrent_creation_cannot_overwrite_another_bundle(
    client, session, exec_app, job, body, mocker, race_stage
):
    other = Job(job_type="DagProcessorJob", state=JobState.RUNNING)
    other.session_id = uuid7()
    other.registration_id = uuid7()
    other.bundle_names = ["other"]
    session.add(other)
    session.commit()
    identities = {job.id: (job.session_id, "bundle"), other.id: (other.session_id, "other")}

    async def authenticate(request: Request):
        job_id = int(request.path_params["job_id"])
        session_id, bundle = identities[job_id]
        return TIToken(
            id=session_id,
            claims=TIClaims(scope="dag_processor", job_id=job_id, dag_bundles=frozenset({bundle})),
        )

    exec_app.dependency_overrides[require_auth] = authenticate
    checked_ownership = Event()
    second_published = Event()

    def wait_for_competitor():
        checked_ownership.set()
        assert second_published.wait(timeout=30)

    if race_stage == "before_persistence":

        def persist(**kwargs):
            if kwargs["bundle_name"] == "bundle":
                wait_for_competitor()
            return update_dag_parsing_results_in_db(**kwargs)

        mocker.patch(
            "airflow.api_fastapi.execution_api.routes.dag_parsing.update_dag_parsing_results_in_db",
            autospec=True,
            side_effect=persist,
        )
    else:
        find_orm_dags = DagModelOperation.find_orm_dags

        def find(op, *, session):
            dags = find_orm_dags(op, session=session)
            if op.bundle_name == "bundle" and not dags:
                wait_for_competitor()
            return dags

        mocker.patch.object(DagModelOperation, "find_orm_dags", autospec=True, side_effect=find)

    with ThreadPoolExecutor(max_workers=1) as executor:
        first = executor.submit(client.post, f"/execution/jobs/{job.id}/parse-results", json=body)
        try:
            assert checked_ownership.wait(timeout=30)
            second = client.post(
                f"/execution/jobs/{other.id}/parse-results", json={**body, "bundle_name": "other"}
            )
            assert second.status_code == 200, second.json()
        finally:
            second_published.set()
        first_response = first.result(timeout=30)
    assert first_response.status_code == 409, first_response.json()
    session.expire_all()
    assert session.get(DagModel, "published").bundle_name == "other"
    assert [row.job_id for row in session.scalars(select(DagParseCheckpoint))] == [other.id]


def test_complete_inventory_reconciles_only_its_bundle_and_replays(client, session, job, body):
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 200
    session.add(DagModel(dag_id="other_bundle", bundle_name="other", relative_fileloc="a.py", is_stale=False))
    session.commit()
    inventory = {
        "attempt_id": str(uuid7()),
        "dispatch_sequence": 2,
        "expected_revision": None,
        "version": "v2",
        "files": [],
    }
    url = f"/execution/jobs/{job.id}/bundles/bundle/inventory"
    response = client.post(url, json=inventory)
    assert response.status_code == 200, response.json()
    assert client.post(url, json=inventory).json() == response.json()
    session.expire_all()
    assert session.get(DagModel, "published").is_stale
    assert not session.get(DagModel, "other_bundle").is_stale
    body.update(attempt_id=str(uuid7()), dispatch_sequence=3)
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 409
    assert client.post(url.replace("bundle/inventory", "other/inventory"), json=inventory).status_code == 403


def test_inventory_revision_fences_late_refreshes(client, session, job):
    url = f"/execution/jobs/{job.id}/bundles/bundle/inventory"
    inventory = {
        "attempt_id": str(uuid7()),
        "dispatch_sequence": 1,
        "expected_revision": None,
        "version": "v1",
        "files": ["a.py"],
    }
    first = client.post(url, json=inventory)
    assert first.status_code == 200, first.json()
    retry = {**inventory, "attempt_id": str(uuid7()), "dispatch_sequence": 2, "version": "v0"}
    assert client.post(url, json=retry).status_code == 409
    retry.update(expected_revision=first.json()["revision"], version="v1")
    current = client.post(url, json=retry)
    assert current.status_code == 200
    assert current.json()["revision"] == first.json()["revision"]
    session.expire_all()
    assert session.get(DagBundleModel, "bundle").version == "v1"


@pytest.mark.parametrize("files", [["../a.py"], ["a.py", "a.py"], ["/a.py"]])
def test_invalid_inventory_cannot_remove_dags(client, session, job, body, files):
    assert client.post(f"/execution/jobs/{job.id}/parse-results", json=body).status_code == 200
    response = client.post(
        f"/execution/jobs/{job.id}/bundles/bundle/inventory",
        json={
            "attempt_id": str(uuid7()),
            "dispatch_sequence": 2,
            "expected_revision": None,
            "files": files,
        },
    )
    assert response.status_code == 422
    session.expire_all()
    assert not session.get(DagModel, "published").is_stale


@pytest.mark.parametrize("rejected", [False, True])
def test_empty_publication_deactivates_only_after_acceptance(client, session, job, body, rejected):
    url = f"/execution/jobs/{job.id}/parse-results"
    assert client.post(url, json=body).status_code == 200
    body.update(attempt_id=str(uuid7()), dispatch_sequence=2, serialized_dags=[], source_codes={})
    if rejected:
        body["bundle_revision"] = str(uuid7())
    response = client.post(url, json=body)
    assert response.status_code == (409 if rejected else 200)
    session.expire_all()
    assert session.get(DagModel, "published").is_stale is not rejected


@pytest.mark.parametrize("kind", ["callbacks", "priority"])
def test_requested_work_claim_survives_response_loss_and_ack_replay(client, session, job, kind):
    if kind == "callbacks":
        work = DagProcessorCallback(
            priority_weight=5,
            callback=DagCallbackRequest(
                filepath="a.py",
                bundle_name="bundle",
                bundle_version="v1",
                dag_id="published",
                run_id="run",
            ),
        )
    else:
        work = DagPriorityParsingRequest(bundle_name="bundle", relative_fileloc="a.py")
    session.add(work)
    session.commit()
    base = f"/execution/jobs/{job.id}/requested-work/{kind}"
    claim = {"claim_id": str(uuid7()), "bundle_names": ["bundle"], "limit": 1}
    first = client.post(f"{base}/claim", json=claim)
    assert first.status_code == 200, first.json()
    assert len(first.json()) == 1
    assert client.post(f"{base}/claim", json=claim).json() == first.json()
    assert client.post(f"{base}/claim", json={**claim, "claim_id": str(uuid7())}).json() == []
    ack = {"claim_id": claim["claim_id"], "state": "success"}
    key = work.id
    url = f"{base}/{key}/ack"
    assert client.post(url, json={**ack, "claim_id": str(uuid7())}).status_code == 409
    assert client.post(url, json=ack).status_code == 204
    assert client.post(url, json=ack).status_code == 204
    session.expire_all()
    model = DagProcessorCallback if kind == "callbacks" else DagPriorityParsingRequest
    assert session.get(model, key) is None


@pytest.mark.parametrize("retired", [False, True])
def test_priority_claim_preserves_live_owner_and_recovers_retired_owner(client, session, job, retired):
    owner = Job(job_type="DagProcessorJob", state=JobState.RUNNING)
    session.add(owner)
    session.flush()
    work = DagPriorityParsingRequest(bundle_name="bundle", relative_fileloc="recover.py")
    work.processor_job_id = owner.id
    work.processor_claim_id = uuid7()
    if retired:
        owner.end_date = timezone.utcnow()
    session.add(work)
    session.commit()
    response = client.post(
        f"/execution/jobs/{job.id}/requested-work/priority/claim",
        json={
            "claim_id": str(uuid7()),
            "bundle_names": ["bundle"],
            "limit": 1,
        },
    )
    assert response.status_code == 200, response.json()
    assert len(response.json()) == int(retired)
    session.delete(work)
    session.commit()


@pytest.mark.backend("postgres", "mysql")
@pytest.mark.parametrize("kind", ["callbacks", "priority"])
def test_competing_jobs_claim_requested_work_once(client, exec_app, session, job, kind):
    second = Job(job_type="DagProcessorJob", state=JobState.RUNNING)
    second.session_id = uuid7()
    second.registration_id = uuid7()
    second.bundle_names = ["bundle"]
    session.add(second)
    if kind == "callbacks":
        work = DagProcessorCallback(
            priority_weight=1,
            callback=DagCallbackRequest(
                filepath="a.py", bundle_name="bundle", bundle_version=None, dag_id="dag", run_id="run"
            ),
        )
    else:
        work = DagPriorityParsingRequest(bundle_name="bundle", relative_fileloc="a.py")
    session.add(work)
    session.commit()
    identities = {job.id: job.session_id, second.id: second.session_id}

    async def authenticate(request: Request):
        job_id = int(request.path_params["job_id"])
        return TIToken(
            id=identities[job_id],
            claims=TIClaims(scope="dag_processor", job_id=job_id, dag_bundles=frozenset({"bundle"})),
        )

    exec_app.dependency_overrides[require_auth] = authenticate
    ready = Barrier(2)

    def claim(job_id):
        ready.wait(timeout=10)
        return client.post(
            f"/execution/jobs/{job_id}/requested-work/{kind}/claim",
            json={"claim_id": str(uuid7()), "bundle_names": ["bundle"]},
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(claim, job_id) for job_id in identities]
        responses = [future.result(timeout=30) for future in futures]
    assert [response.status_code for response in responses] == [200, 200]
    assert sorted(len(response.json()) for response in responses) == [0, 1]


def test_catalog_and_inventory_support_granted_bundle_with_slash(client, exec_app, session, job):
    session.add(DagBundleModel(name="team/bundle"))
    session.commit()

    async def authenticate(request: Request):
        return TIToken(
            id=SESSION_ID,
            claims=TIClaims(scope="dag_processor", job_id=job.id, dag_bundles=frozenset({"team/bundle"})),
        )

    exec_app.dependency_overrides[require_auth] = authenticate
    response = client.get(f"/execution/jobs/{job.id}/bundles")
    assert response.status_code == 200
    assert [bundle["name"] for bundle in response.json()] == ["team/bundle"]
    response = client.post(
        f"/execution/jobs/{job.id}/bundles/team%2Fbundle/inventory",
        json={"attempt_id": str(uuid7()), "dispatch_sequence": 1, "expected_revision": None, "files": []},
    )
    assert response.status_code == 200, response.json()


@pytest.mark.parametrize("kind", ["callbacks", "priority"])
@pytest.mark.parametrize("retired", [False, True])
def test_direct_mode_recovers_retired_api_claims_only(session, job, tmp_path, kind, retired):
    if kind == "callbacks":
        work = DagProcessorCallback(
            priority_weight=1,
            callback=DagCallbackRequest(
                filepath="a.py", bundle_name="bundle", bundle_version=None, dag_id="dag", run_id="run"
            ),
        )
    else:
        work = DagPriorityParsingRequest(bundle_name="bundle", relative_fileloc="a.py")
    work.processor_job_id = job.id
    work.processor_claim_id = uuid7()
    if retired:
        job.end_date = timezone.utcnow()
    session.add(work)
    session.commit()
    manager = DagFileProcessorManager(max_runs=1)
    manager._dag_bundles = [LocalDagBundle(name="bundle", path=tmp_path)]
    result = manager.fetch_callbacks() if kind == "callbacks" else manager.claim_priority_files()
    assert len(result) == int(retired)
