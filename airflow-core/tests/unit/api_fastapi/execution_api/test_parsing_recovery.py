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

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier
from types import SimpleNamespace
from uuid import uuid4

import pytest
import time_machine
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from fastapi.testclient import TestClient

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.execution_api.parsing import create_app
from airflow.dag_processing.parsing_state import ReceiptStore
from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.parsing import DagDefinitionAttempt, DagDefinitionResult, ParseDagDefinitions

NOW = datetime(2026, 9, 25, tzinfo=timezone.utc)


@pytest.fixture
def recovery_api(tmp_path):
    with time_machine.travel(NOW, tick=False):
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
        workload = ParseDagDefinitions(
            workload_id=uuid4(),
            bundle_info=BundleInfo(name="recovery", version="v1"),
            definitions=tuple(
                DagDefinitionAttempt(
                    attempt_id=uuid4(),
                    relative_path=f"{i}.py",
                    source_revision=f"revision-{i}",
                    timeout_seconds=5,
                )
                for i in range(2)
            ),
            start_deadline=NOW + timedelta(seconds=30),
            stop_deadline=NOW + timedelta(seconds=60),
            queue="recovery",
            token="not-stored",
        )
        store = ReceiptStore(tmp_path / "receipts.sqlite")
        store.reserve_workload(workload, route="recovery", capacity=1)
        store.mark_submitted(workload.workload_id)
        execution_id = str(uuid4())
        with TestClient(create_app(store.path, public)) as client:
            yield SimpleNamespace(
                workload=workload,
                store=store,
                client=client,
                generator=generator,
                execution_id=execution_id,
                termination={
                    "kind": "confirmed_worker_termination",
                    "workload_id": str(workload.workload_id),
                    "execution_ids": [execution_id],
                    "evidence": {"source": "synthetic termination fixture; no live worker"},
                },
            )


def build_headers(generator, manifest):
    token = generator.generate(
        {
            "sub": manifest["workload_id"],
            "scope": "dag-parsing-poc",
            "attempt_ids": [item["attempt_id"] for item in manifest["definitions"]],
        }
    )
    return {"Authorization": f"Bearer {token}"}


def build_route(manifest, index):
    return (
        f"/execution/poc/parsing/workloads/{manifest['workload_id']}"
        f"/attempts/{manifest['definitions'][index]['attempt_id']}"
    )


def build_result(definition):
    return DagDefinitionResult(
        attempt_id=definition["attempt_id"],
        relative_path=definition["relative_path"],
        source_revision=definition["source_revision"],
        outcome="success",
        serialized_dags=[{"dag": {"dag_id": definition["relative_path"]}}],
        duration_seconds=0.1,
    ).model_dump(mode="json")


def retire(recovery):
    return recovery.store.retire_and_replace(
        recovery.workload.workload_id,
        termination=recovery.termination,
        start_deadline=NOW + timedelta(seconds=10),
        stop_deadline=NOW + timedelta(seconds=40),
    )


def test_retirement_fences_old_tokens_without_losing_accepted_sibling(recovery_api):
    r = recovery_api
    original = r.store.get_manifest(r.workload.workload_id)
    headers = build_headers(r.generator, original)
    claim = {"execution_id": r.execution_id}
    for index in (0, 1):
        assert (
            r.client.post(f"{build_route(original, index)}/claim", json=claim, headers=headers).status_code
            == 200
        )
    accepted_body = claim | {"result": build_result(original["definitions"][0])}
    accepted = r.client.post(f"{build_route(original, 0)}/result", json=accepted_body, headers=headers)
    assert accepted.status_code == 200

    decision = retire(r)
    replacement = decision["replacement"]
    assert len(replacement["definitions"]) == 1
    assert replacement["definitions"][0]["relative_path"] == original["definitions"][1]["relative_path"]
    assert replacement["definitions"][0]["attempt_id"] != original["definitions"][1]["attempt_id"]
    assert (
        r.client.post(f"{build_route(original, 0)}/result", json=accepted_body, headers=headers).json()
        == accepted.json()
    )
    for execution in (r.execution_id, str(uuid4())):
        assert (
            r.client.post(
                f"{build_route(original, 1)}/claim", json={"execution_id": execution}, headers=headers
            ).status_code
            == 409
        )
        assert (
            r.client.post(
                f"{build_route(original, 1)}/result",
                json={"execution_id": execution, "result": build_result(original["definitions"][1])},
                headers=headers,
            ).status_code
            == 409
        )
    new_route = build_route(replacement, 0)
    new_claim = {"execution_id": str(uuid4())}
    assert r.client.post(f"{new_route}/claim", json=new_claim, headers=headers).status_code == 403
    replacement_headers = build_headers(r.generator, replacement)
    assert r.client.post(f"{new_route}/claim", json=new_claim, headers=replacement_headers).status_code == 200
    assert (
        r.client.post(
            f"{new_route}/result",
            json=new_claim | {"result": build_result(replacement["definitions"][0])},
            headers=replacement_headers,
        ).status_code
        == 200
    )
    assert retire(r) == decision
    reopened = ReceiptStore(r.store.path)
    assert reopened.get_results(original["workload_id"]) == [accepted_body["result"]]
    assert len(reopened.restore_admissions("recovery")) == 1


def test_http_publication_and_retirement_have_one_atomic_winner(recovery_api):
    r = recovery_api
    original = r.store.get_manifest(r.workload.workload_id)
    headers = build_headers(r.generator, original)
    claim = {"execution_id": r.execution_id}
    route = build_route(original, 1)
    assert r.client.post(f"{route}/claim", json=claim, headers=headers).status_code == 200
    barrier = Barrier(2)

    def publish():
        barrier.wait(timeout=5)
        return r.client.post(
            f"{route}/result",
            json=claim | {"result": build_result(original["definitions"][1])},
            headers=headers,
        )

    def recover():
        barrier.wait(timeout=5)
        return retire(r)

    with ThreadPoolExecutor(max_workers=2) as pool:
        publication = pool.submit(publish)
        retirement = pool.submit(recover)
        response, decision = publication.result(timeout=10), retirement.result(timeout=10)
    retried_paths = {item["relative_path"] for item in decision["replacement"]["definitions"]}
    if response.status_code == 200:
        assert original["definitions"][1]["relative_path"] not in retried_paths
        assert len(r.store.get_results(original["workload_id"])) == 1
    else:
        assert response.status_code == 409
        assert original["definitions"][1]["relative_path"] in retried_paths
        assert r.store.get_results(original["workload_id"]) == []
