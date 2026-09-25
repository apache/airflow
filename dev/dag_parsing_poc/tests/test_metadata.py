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
# ruff: noqa: S101
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from unittest import mock
from uuid import uuid4

import pytest
import time_machine
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from fastapi.testclient import TestClient
from sqlalchemy import func, select
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import Session

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.parsing import DagDefinitionAttempt, DagDefinitionResult, ParseDagDefinitions
from airflow.models import import_all_models
from airflow.models.base import Base
from airflow.models.dag import DagModel
from airflow.models.dag_version import DagVersion
from airflow.models.dagbundle import DagBundleModel
from airflow.models.dagcode import DagCode
from airflow.models.errors import ParseImportError
from airflow.models.serialized_dag import SerializedDagModel
from airflow.sdk import DAG, task
from airflow.serialization.serialized_objects import LazyDeserializedDAG
from airflow.utils.db import add_default_pool_if_not_exists, synchronize_log_template

from dev.dag_parsing_poc.api import create_app
from dev.dag_parsing_poc.metadata import MetadataReceiptStore
from tests_common.test_utils.config import conf_vars

pytestmark = pytest.mark.db_test
NOW = datetime(2026, 9, 25, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def clock():
    with time_machine.travel(NOW, tick=False) as clock:
        yield clock


@pytest.fixture
def store(tmp_path):
    store = MetadataReceiptStore(tmp_path / "metadata.sqlite")
    import_all_models()
    Base.metadata.create_all(store.engine)
    with Session(store.engine) as session:
        add_default_pool_if_not_exists(session=session)
        synchronize_log_template(session=session)
        session.add(DagBundleModel(name="poc", version="v1"))
        session.commit()
    yield store
    store.engine.dispose()


@pytest.fixture
def build_result():
    def build(*, revision=1, path="example.py", dag_id="remote_metadata"):
        source = f"# worker source revision {revision}\n"
        with DAG(
            dag_id,
            schedule="@once",
            start_date=NOW - timedelta(days=1),
            is_paused_upon_creation=False,
            doc_md=f"Revision {revision}",
        ) as dag:

            @task
            def sample():
                return 1

            sample()
        dag.fileloc = f"/inaccessible-worker/{path}"
        dag.relative_fileloc = path
        definition = DagDefinitionAttempt(
            attempt_id=uuid4(),
            relative_path=path,
            source_revision=hashlib.sha256(source.encode()).hexdigest(),
            timeout_seconds=10,
        )
        workload = ParseDagDefinitions(
            workload_id=uuid4(),
            bundle_info=BundleInfo(name="poc", version="v1"),
            definitions=(definition,),
            start_deadline=NOW + timedelta(seconds=60),
            stop_deadline=NOW + timedelta(seconds=120),
            token="unissued",
            queue="poc-parsing",
        )
        result = DagDefinitionResult(
            attempt_id=definition.attempt_id,
            relative_path=path,
            source_revision=definition.source_revision,
            source_code=source,
            outcome="success",
            serialized_dags=[json.loads(json.dumps(LazyDeserializedDAG.from_dag(dag).data))],
            duration_seconds=0.1,
        )
        return workload, result

    return build


@pytest.fixture
def client(tmp_path, store):
    key = Ed25519PrivateKey.generate()
    public = tmp_path / "public.pem"
    public.write_bytes(key.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo))
    generator = JWTGenerator(
        private_key=key,
        kid="dag-parsing-poc",
        issuer="dag-parsing-poc",
        audience="dag-parsing-poc",
        algorithm="EdDSA",
        valid_for=600,
    )
    with TestClient(
        create_app(store.path, public, persist_metadata=True), raise_server_exceptions=False
    ) as client:

        def publish(workload, result, *, execution_id=None):
            execution_id = execution_id or uuid4()
            headers = {
                "Authorization": "Bearer "
                + generator.generate(
                    {
                        "sub": str(workload.workload_id),
                        "scope": "dag-parsing-poc",
                        "attempt_ids": [str(item.attempt_id) for item in workload.definitions],
                    }
                )
            }
            route = f"/execution/poc/parsing/workloads/{workload.workload_id}/attempts/{result.attempt_id}"
            claim = {"execution_id": str(execution_id)}
            response = client.post(f"{route}/claim", headers=headers, json=claim)
            assert response.status_code == 200, response.text
            return client.post(
                f"{route}/result", headers=headers, json=claim | {"result": result.model_dump(mode="json")}
            ), execution_id

        yield publish


@mock.patch.object(
    DagCode, "get_code_from_file", autospec=True, side_effect=AssertionError("API read worker path")
)
def test_publish_persists_metadata_and_code_without_source_files(read_file, client, store, build_result):
    workload, result = build_result()
    workload.bundle_info.version_data = {"source": "registered-bundle-revision"}
    store.register_workload(workload)
    response, execution = client(workload, result)
    assert response.status_code == 200, response.text
    with Session(store.engine) as session:
        dag = session.get(DagModel, "remote_metadata")
        assert dag.bundle_name == "poc"
        assert dag.relative_fileloc == "example.py"
        assert dag.next_dagrun is not None
        assert session.scalar(select(SerializedDagModel)).data["dag"]["doc_md"] == "Revision 1"
        assert session.scalar(select(DagCode)).source_code == result.source_code
        assert session.scalar(select(func.count()).select_from(DagVersion)) == 1
        assert session.scalar(select(DagVersion)).version_data == workload.bundle_info.version_data
    replay, _ = client(workload, result, execution_id=execution)
    assert replay.json() == response.json()
    assert len(store.get_results(workload.workload_id)) == 1
    with Session(store.engine) as session:
        assert session.scalar(select(func.count()).select_from(DagVersion)) == 1
    read_file.assert_not_called()


@pytest.mark.parametrize(
    "stage", ["duplicate_warning", "bulk_write", "serialized", "diagnostics", "warnings", "after_receipt"]
)
def test_metadata_and_receipt_roll_back_together(client, store, build_result, stage):
    workload, result = build_result()
    store.register_workload(workload)
    if stage == "after_receipt":
        with store._open_transaction() as connection:
            connection.execute(
                "CREATE TRIGGER reject_receipt AFTER UPDATE OF result_json ON attempts "
                "WHEN NEW.result_json IS NOT NULL BEGIN SELECT RAISE(ABORT, 'receipt failed'); END"
            )
        response, execution = client(workload, result)
        with store._open_transaction() as connection:
            connection.execute("DROP TRIGGER reject_receipt")
    else:
        target = {
            "duplicate_warning": "_build_duplicate_dag_id_warnings",
            "bulk_write": "SerializedDAG.bulk_write_to_db",
            "serialized": "SerializedDagModel.write_dag",
            "diagnostics": "_update_import_errors",
            "warnings": "_update_dag_warnings",
        }[stage]
        error = (
            OperationalError("injected", {}, RuntimeError("database unavailable"))
            if stage == "bulk_write"
            else RuntimeError("injected persistence failure")
        )
        with mock.patch(
            f"airflow.dag_processing.collection.{target}", autospec=True, side_effect=error
        ) as fail:
            response, execution = client(workload, result)
        fail.assert_called_once()
    assert response.status_code == 500
    assert store.get_results(workload.workload_id) == []
    assert store.get_attempts(workload.workload_id)[0]["status"] == "claimed"
    with Session(store.engine) as session:
        for model in (DagModel, SerializedDagModel, DagVersion, DagCode):
            assert session.scalar(select(func.count()).select_from(model)) == 0
    retry, _ = client(workload, result, execution_id=execution)
    assert retry.status_code == 200, retry.text


@conf_vars({("core", "min_serialized_dag_update_interval"): "0"})
@pytest.mark.parametrize("first_accepted", [True, False])
def test_new_registration_fences_stale_result_but_preserves_accepted_replay(
    client, store, build_result, first_accepted, clock
):
    old, old_result = build_result()
    store.register_workload(old)
    execution = uuid4()
    if first_accepted:
        response, _ = client(old, old_result, execution_id=execution)
        assert response.status_code == 200, response.text
    clock.shift(timedelta(seconds=1))
    new, new_result = build_result(revision=2)
    store.register_workload(new)
    response, _ = client(new, new_result)
    assert response.status_code == 200, response.text
    store.register_workload(old)  # An idempotent registration cannot regain authority.
    response, _ = client(old, old_result, execution_id=execution)
    assert response.status_code == (200 if first_accepted else 409), response.text
    with Session(store.engine) as session:
        assert session.scalar(select(SerializedDagModel)).data["dag"]["doc_md"] == "Revision 2"
        assert session.scalar(select(DagCode)).source_code == new_result.source_code


@pytest.mark.parametrize("invalid", ["source", "path", "duplicate_id", "warning", "error_path", "bundle"])
def test_rejects_invalid_metadata_without_accepting_receipt(client, store, build_result, invalid):
    workload, result = build_result()
    store.register_workload(workload)
    if invalid == "source":
        result.source_code = "unregistered source"
    elif invalid == "path":
        result.serialized_dags[0]["dag"]["relative_fileloc"] = "another.py"
    elif invalid == "duplicate_id":
        result.serialized_dags *= 2
    elif invalid == "warning":
        result.warnings = [{"dag_id": "another", "warning_type": "non-existent pool", "message": "bad"}]
    elif invalid == "error_path":
        result.import_errors = {"another.py": "error"}
    else:
        with Session(store.engine) as session:
            session.get(DagBundleModel, "poc").version = "v2"
            session.commit()
    response, _ = client(workload, result)
    assert response.status_code == (409 if invalid == "bundle" else 422), response.text
    assert store.get_results(workload.workload_id) == []


def test_import_error_persists_and_success_clears_it(client, store, build_result):
    workload, result = build_result()
    result.outcome = "import_error"
    result.serialized_dags = []
    result.import_errors = {"example.py": "SyntaxError: broken definition"}
    store.register_workload(workload)
    response, _ = client(workload, result)
    assert response.status_code == 200, response.text
    with Session(store.engine) as session:
        error = session.scalar(select(ParseImportError))
        assert error.bundle_name == "poc"
        assert error.filename == "example.py"
    workload, result = build_result(revision=2)
    store.register_workload(workload)
    response, _ = client(workload, result)
    assert response.status_code == 200, response.text
    with Session(store.engine) as session:
        assert session.scalar(select(ParseImportError)) is None


@pytest.mark.parametrize("superseded", [True, False])
def test_recovery_only_retries_current_definitions(store, build_result, superseded):
    old, _ = build_result()
    store.reserve_workload(old, route=old.queue, capacity=2)
    execution = uuid4()
    store.claim(old.workload_id, old.definitions[0].attempt_id, execution)
    if superseded:
        new, _ = build_result(revision=2)
        store.register_workload(new)
    decision = store.retire_and_replace(
        old.workload_id,
        termination={
            "kind": "confirmed_worker_termination",
            "workload_id": str(old.workload_id),
            "execution_ids": [str(execution)],
            "evidence": {"container_exited": True},
        },
        start_deadline=NOW + timedelta(seconds=10),
        stop_deadline=NOW + timedelta(seconds=50),
    )
    assert store.get_attempts(old.workload_id)[0]["status"] == "retired"
    if superseded:
        assert decision["replacement"] is None
        expected = str(new.workload_id)
        assert [item["workload_id"] for item in store.get_admissions(old.queue)] == [expected]
    else:
        expected = decision["replacement"]["workload_id"]
        assert store.get_admissions(old.queue)[0]["workload_id"] == expected
    with store._open_transaction() as connection:
        assert connection.execute("SELECT workload_id FROM current_definitions").fetchone()[0] == expected


def test_lost_acknowledgment_can_be_recovered_after_execution_deadline(client, store, build_result, clock):
    workload, result = build_result()
    store.register_workload(workload)
    response, execution = client(workload, result)
    assert response.status_code == 200, response.text
    clock.shift(timedelta(seconds=121))
    with mock.patch.object(
        MetadataReceiptStore,
        "_persist_result",
        autospec=True,
        side_effect=AssertionError("replayed metadata write"),
    ):
        recovered, _ = client(workload, result, execution_id=execution)
    assert recovered.json() == response.json()


@conf_vars({("core", "min_serialized_dag_update_interval"): "0"})
def test_source_only_change_updates_code_without_new_serialized_version(client, store, build_result, clock):
    old, old_result = build_result()
    store.register_workload(old)
    response, _ = client(old, old_result)
    assert response.status_code == 200, response.text
    clock.shift(timedelta(seconds=1))
    new, new_result = build_result(revision=2)
    new_result.serialized_dags = old_result.serialized_dags
    store.register_workload(new)
    response, _ = client(new, new_result)
    assert response.status_code == 200, response.text
    with Session(store.engine) as session:
        assert session.scalar(select(DagCode)).source_code == new_result.source_code
        assert session.scalar(select(SerializedDagModel)).data["dag"]["doc_md"] == "Revision 1"
        assert session.scalar(select(func.count()).select_from(DagVersion)) == 1


def test_definition_cannot_overwrite_dag_id_from_another_definition(client, store, build_result):
    old, old_result = build_result()
    store.register_workload(old)
    response, _ = client(old, old_result)
    assert response.status_code == 200, response.text
    new, new_result = build_result(path="other.py")
    store.register_workload(new)
    response, _ = client(new, new_result)
    assert response.status_code == 409, response.text
    assert store.get_results(new.workload_id) == []
