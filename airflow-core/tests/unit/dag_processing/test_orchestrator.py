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

import hashlib
import json
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from unittest import mock
from uuid import uuid4
from zipfile import ZipFile

import httpx
import pytest
import time_machine
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import Encoding, PublicFormat
from fastapi.testclient import TestClient

from airflow.api_fastapi.auth.tokens import JWTGenerator
from airflow.api_fastapi.execution_api.parsing import create_app
from airflow.dag_processing.discovery import discover_python_bundle
from airflow.dag_processing.executor_runner import ParsingExecutorRunner
from airflow.dag_processing.executor_worker import (
    ParsingAPIClient,
    ParsingPublicationError,
    supervise_dag_parse,
)
from airflow.dag_processing.orchestrator import DiscoveredDefinition, OrchestrationStore, ParseOrchestrator
from airflow.dag_processing.parsing_state import ReceiptConflictError
from airflow.executors.base_executor import BaseExecutor
from airflow.executors.local_executor import LocalExecutor
from airflow.executors.workloads import BundleInfo, WorkloadType
from airflow.executors.workloads.parsing import (
    DagDefinitionResult,
    ParseDagDefinitions,
    ParseDagDefinitionsState,
)

NOW = datetime(2026, 9, 25, tzinfo=timezone.utc)
BUNDLE = BundleInfo(name="poc", version="v1")
ROUTE = "orchestration-only"


@pytest.fixture(autouse=True)
def clock():
    with time_machine.travel(NOW, tick=False) as traveller:
        yield traveller


@pytest.fixture
def store(tmp_path):
    return OrchestrationStore(tmp_path / "receipts.sqlite")


def create_orchestrator(store, **kwargs):
    return ParseOrchestrator(store, route=ROUTE, bundle=BUNDLE.name, **kwargs)


def inventory(*names, revision="v1"):
    return [DiscoveredDefinition(relative_path=name, source_revision=revision) for name in names]


@mock.patch(
    "airflow.dag_processing.discovery.compute_source_revision",
    autospec=True,
    side_effect=["before", "after"],
)
def test_discovery_rejects_archive_changed_during_scan(revision, tmp_path):
    with ZipFile(tmp_path / "bundle.zip", "w") as archive:
        archive.writestr("dag.py", "from airflow.sdk import DAG\ndag = DAG('example', schedule=None)\n")
    with pytest.raises(ValueError, match="Archive changed during discovery"):
        discover_python_bundle(tmp_path)
    assert revision.call_count == 2


def get_workload(store, workload_id):
    return ParseDagDefinitions.model_validate(store.get_manifest(workload_id) | {"token": "not-issued"})


def accept(store, workload, index=0, outcome="success"):
    definition = workload.definitions[index]
    execution_id = uuid4()
    store.claim(workload.workload_id, definition.attempt_id, execution_id)
    result = DagDefinitionResult(
        attempt_id=definition.attempt_id,
        relative_path=definition.relative_path,
        source_revision=definition.source_revision,
        outcome=outcome,
        duration_seconds=0,
    )
    store.accept_result(workload.workload_id, definition.attempt_id, execution_id, result)
    return execution_id, result


def retire(store, workload):
    return store.retire_and_replace(
        workload.workload_id,
        termination={
            "kind": "confirmed_worker_termination",
            "workload_id": str(workload.workload_id),
            "execution_ids": [
                row["execution_id"] for row in store.get_attempts(workload.workload_id) if row["execution_id"]
            ],
            "evidence": {"external_termination": "fixture"},
        },
        start_deadline=NOW + timedelta(seconds=10),
        stop_deadline=NOW + timedelta(seconds=20),
    )


def test_step_filters_eligible_paths_before_batch_limit(store):
    orchestrator = create_orchestrator(store, batch_size=1)
    orchestrator.update_inventory(BUNDLE, inventory("a.py", "b.py", "c.py"))
    workload = get_workload(store, orchestrator.step(eligible_paths={"c.py"}).workload_id)
    assert [definition.relative_path for definition in workload.definitions] == ["c.py"]
    assert store.get_sources(ROUTE, BUNDLE.name)[0]["active_workload"] is None


def test_empty_eligibility_still_reconciles_released_attempts(store):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    accept(store, workload)
    retire(store, workload)
    result = orchestrator.step(eligible_paths=set())
    assert result.reconciled == 1
    assert result.workload_id is None
    assert store.get_sources(ROUTE, BUNDLE.name)[0]["active_workload"] is None


def test_restart_restores_inventory_admission_and_due_time(store, clock):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py", "b.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    restarted_store = OrchestrationStore(store.path)
    restarted = create_orchestrator(restarted_store)
    restarted.update_inventory(BUNDLE, inventory("a.py", "b.py"))
    assert restarted.step().workload_id is None
    assert len(store.get_admissions(ROUTE)) == 1
    accept(store, workload)
    accept(store, workload, 1, "import_error")
    assert restarted.step().reconciled == 0  # Receipt acceptance does not release capacity.
    retire(store, workload)
    assert restarted.step().reconciled == 2
    clock.move_to(NOW + timedelta(seconds=29))
    assert restarted.step().workload_id is None
    clock.move_to(NOW + timedelta(seconds=30))
    second = get_workload(store, restarted.step().workload_id)
    assert [d.relative_path for d in second.definitions] == ["a.py", "b.py"]
    assert {d.attempt_id for d in second.definitions}.isdisjoint(d.attempt_id for d in workload.definitions)


def test_changed_definition_is_due_without_reimporting_unchanged_sibling(store):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py", "b.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    accept(store, workload)
    accept(store, workload, 1)
    retire(store, workload)
    orchestrator.update_inventory(BUNDLE, inventory("a.py", revision="v2") + inventory("b.py"))
    second = get_workload(store, orchestrator.step().workload_id)
    assert [(d.relative_path, d.source_revision) for d in second.definitions] == [("a.py", "v2")]
    assert store.get_sources(ROUTE, "poc")[1]["accepted_count"] == 1


@pytest.mark.parametrize("change", ["revision", "version", "remove", "aba", "remove_readd", "archive"])
def test_inventory_changes_fence_claims_results_and_recovery(store, change):
    original = inventory("a.py")
    if change == "archive":
        original = [
            DiscoveredDefinition(
                relative_path="d.zip/a.py",
                source_revision="v1",
                archive_path="d.zip",
                archive_revision="zip1",
            )
        ]
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, original)
    workload = get_workload(store, orchestrator.step().workload_id)
    definition = workload.definitions[0]
    execution_id = uuid4()
    store.claim(workload.workload_id, definition.attempt_id, execution_id)
    if change in {"remove", "remove_readd"}:
        orchestrator.update_inventory(BUNDLE, [])
        if change == "remove_readd":
            orchestrator.update_inventory(BUNDLE, original)
    elif change == "version":
        orchestrator.update_inventory(BundleInfo(name="poc", version="v2"), original)
    elif change == "archive":
        orchestrator.update_inventory(BUNDLE, [original[0].model_copy(update={"archive_revision": "zip2"})])
    else:
        orchestrator.update_inventory(BUNDLE, inventory("a.py", revision="v2"))
        if change == "aba":
            orchestrator.update_inventory(BUNDLE, original)
    with pytest.raises(ReceiptConflictError, match="superseded"):
        store.claim(workload.workload_id, definition.attempt_id, execution_id)
    result = DagDefinitionResult(
        attempt_id=definition.attempt_id,
        relative_path=definition.relative_path,
        source_revision=definition.source_revision,
        outcome="success",
        duration_seconds=0,
    )
    with pytest.raises(ReceiptConflictError, match="superseded"):
        store.accept_result(workload.workload_id, definition.attempt_id, execution_id, result)
    assert orchestrator.step().workload_id is None
    assert len(store.get_admissions(ROUTE)) == 1
    assert retire(store, workload)["replacement"] is None
    next_step = orchestrator.step()
    assert bool(next_step.workload_id) is (change != "remove")
    assert store.get_results(workload.workload_id) == []


def test_accepted_replay_survives_inventory_change_without_refreshing_due_time(store, clock):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    execution_id, result = accept(store, workload)
    orchestrator.update_inventory(BUNDLE, inventory("a.py", revision="v2"))
    before = store.get_sources(ROUTE, "poc")
    clock.move_to(NOW + timedelta(days=1))
    assert store.claim(workload.workload_id, result.attempt_id, uuid4())["status"] == "accepted"
    assert (
        store.accept_result(workload.workload_id, result.attempt_id, execution_id, result)["status"]
        == "accepted"
    )
    assert store.get_sources(ROUTE, "poc") == before


@pytest.mark.parametrize("outcome", ["timeout", "worker_error", "missing"])
def test_failed_and_unfinished_definitions_retry_after_backoff_only(store, clock, outcome):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py", "b.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    accept(store, workload)
    if outcome != "missing":
        accept(store, workload, 1, outcome)
    else:
        store.claim(workload.workload_id, workload.definitions[1].attempt_id, uuid4())
    decision = retire(store, workload)
    assert decision["replacement"] is None
    assert bool(decision["retired_attempt_ids"]) is (outcome == "missing")
    assert orchestrator.step().workload_id is None
    clock.move_to(NOW + timedelta(seconds=5))
    replacement = get_workload(store, orchestrator.step().workload_id)
    assert [d.relative_path for d in replacement.definitions] == ["b.py"]
    assert replacement.definitions[0].attempt_id != workload.definitions[1].attempt_id


def test_step_bounds_and_capacity_do_not_depend_on_history(store):
    orchestrator = create_orchestrator(store, batch_size=2)
    orchestrator.update_inventory(BUNDLE, inventory("a.py", "b.py", "c.py"))
    workload = get_workload(store, orchestrator.step(limit=1).workload_id)
    assert len(workload.definitions) == 1
    assert orchestrator.step().capacity_blocked
    accept(store, workload)
    retire(store, workload)
    result = orchestrator.step(limit=1)
    assert result.reconciled == 1
    assert len(get_workload(store, result.workload_id).definitions) == 1


def test_reservation_and_schedule_roll_back_together(store, monkeypatch):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    original = store._reserve_workload

    def crash(*args, **kwargs):
        original(*args, **kwargs)
        raise RuntimeError("before scheduling commit")

    monkeypatch.setattr(store, "_reserve_workload", mock.create_autospec(original, side_effect=crash))
    with pytest.raises(RuntimeError, match="scheduling commit"):
        orchestrator.step()
    assert store.get_admissions(ROUTE) == []
    assert store.get_sources(ROUTE, "poc")[0]["active_workload"] is None
    restarted = create_orchestrator(OrchestrationStore(store.path))
    assert restarted.step().workload_id is not None


def test_competing_steps_cannot_admit_same_definition(store):
    orchestrator = create_orchestrator(store, capacity=2)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                lambda _: create_orchestrator(OrchestrationStore(store.path), capacity=2).step(), range(2)
            )
        )
    assert sum(result.workload_id is not None for result in results) == 1
    assert len(store.get_admissions(ROUTE)) == 1


@pytest.mark.parametrize(
    "options",
    [
        {"capacity": 0},
        {"capacity": True},
        {"batch_size": 101},
        {"batch_size": 0},
        {"parse_interval": 0},
        {"retry_interval": float("nan")},
        {"start_window": float("inf")},
        {"execution_window": -1},
    ],
)
def test_invalid_scheduling_config(store, options):
    with pytest.raises(ValueError, match="positive|between"):
        create_orchestrator(store, **options)


@pytest.mark.parametrize("limit", [0, True, 101, 1.5])
def test_invalid_step_limit(store, limit):
    with pytest.raises(ValueError, match="Step limit"):
        create_orchestrator(store).step(limit=limit)


def test_invalid_snapshot_leaves_inventory_unchanged(store):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    before = store.get_sources(ROUTE, "poc")
    with pytest.raises(ValueError, match="duplicate"):
        orchestrator.update_inventory(BUNDLE, inventory("a.py", "a.py"))
    with pytest.raises(ValueError, match="another bundle"):
        orchestrator.update_inventory(BundleInfo(name="wrong", version="v1"), [])
    assert store.get_sources(ROUTE, "poc") == before


@pytest.fixture
def local_runner(store):
    executor = mock.create_autospec(LocalExecutor, instance=True)
    executor.parallelism = 1
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    executor.get_event_buffer.return_value = {}
    runner = ParsingExecutorRunner(store, executor, route=ROUTE, token_issuer=lambda _: "in-memory-token")
    runner.start()
    return runner


@pytest.mark.parametrize(
    ("state", "outcome", "release"),
    [
        (ParseDagDefinitionsState.SUCCESS, "success", True),
        (ParseDagDefinitionsState.SUCCESS, "import_error", True),
        (ParseDagDefinitionsState.SUCCESS, "timeout", False),
        (ParseDagDefinitionsState.SUCCESS, None, False),
        (ParseDagDefinitionsState.FAILED, "success", False),
        (ParseDagDefinitionsState.FAILED, None, False),
    ],
)
def test_remote_runner_requires_success_and_complete_importer_receipts(store, state, outcome, release):
    executor = mock.create_autospec(BaseExecutor, instance=True)
    executor.parallelism = 1
    executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})
    executor.get_event_buffer.return_value = {}
    runner = ParsingExecutorRunner(store, executor, route=ROUTE, token_issuer=lambda _: "fixture")
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    runner.start()
    runner.tick()
    if outcome is not None:
        accept(store, workload, outcome=outcome)
    executor.get_event_buffer.return_value = {
        workload.key: (state, ParsingPublicationError("remote evidence is insufficient", str(uuid4())))
    }
    runner.tick()
    assert (store.get_admissions(ROUTE) == []) is release
    runner.close()


def test_runner_dispatches_reserved_work_after_orchestrator_restart(store, local_runner):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    local_runner.tick()
    assert store.get_admissions(ROUTE)[0]["state"] == "submitted"
    assert "token" not in store.get_manifest(workload.workload_id)
    restarted = create_orchestrator(OrchestrationStore(store.path))
    assert restarted.step().workload_id is None
    local_runner.tick()
    local_runner.executor.queue_workload.assert_called_once()
    assert local_runner.executor.queue_workload.call_args.args[0].token == "in-memory-token"
    assert local_runner.executor.queue_workload.call_args.kwargs["session"].bind is None


@pytest.mark.parametrize("complete", [True, False])
def test_local_return_retires_before_retry_and_does_not_reparse_accepted_siblings(
    store, local_runner, complete
):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py", "b.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    local_runner.tick()
    accept(store, workload)
    if complete:
        accept(store, workload, 1)
    local_runner.executor.get_event_buffer.return_value = {
        workload.key: (
            ParseDagDefinitionsState.SUCCESS if complete else ParseDagDefinitionsState.FAILED,
            None,
        )
    }
    local_runner.tick()
    if not complete:
        assert len(store.get_admissions(ROUTE)) == 1
        assert store.get_attempts(workload.workload_id)[1]["status"] == "pending"
        retire(store, workload)
    assert store.get_admissions(ROUTE) == []
    assert store.get_attempts(workload.workload_id)[1]["status"] == ("accepted" if complete else "retired")
    assert orchestrator.step().workload_id is None


def test_restarted_runner_does_not_republish_or_release_old_submissions(store, local_runner):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    store.mark_submitted(workload.workload_id)
    accept(store, workload)
    local_runner.executor.get_event_buffer.return_value = {
        workload.key: (ParseDagDefinitionsState.SUCCESS, None)
    }
    local_runner.tick()
    assert len(store.get_admissions(ROUTE)) == 1
    local_runner.executor.queue_workload.assert_not_called()


@mock.patch("airflow.dag_processing.executor_worker.get_bundle_root", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.ParsingAPIClient", autospec=True)
@mock.patch("airflow.dag_processing.executor_worker.parse_definition", autospec=True)
@pytest.mark.parametrize("accepted_before_disconnect", [False, True])
@pytest.mark.parametrize("failed_definition", [0, 1])
def test_local_publication_failure_retires_only_unaccepted_attempts(
    parse, factory, root, store, local_runner, clock, tmp_path, accepted_before_disconnect, failed_definition
):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py", "b.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    local_runner.tick()
    root.return_value = tmp_path
    results = [
        DagDefinitionResult(
            attempt_id=d.attempt_id,
            relative_path=d.relative_path,
            source_revision=d.source_revision,
            outcome="success",
            duration_seconds=0,
        )
        for d in workload.definitions
    ]
    parse.side_effect = results

    def handle(request):
        payload = json.loads(request.content)
        attempt = request.url.path.split("/")[-2]
        if request.url.path.endswith("/claim"):
            response = store.claim(workload.workload_id, attempt, payload["execution_id"])
            return httpx.Response(200, json=response)
        fails = attempt == str(workload.definitions[failed_definition].attempt_id)
        if not fails or accepted_before_disconnect:
            store.accept_result(
                workload.workload_id,
                attempt,
                payload["execution_id"],
                DagDefinitionResult.model_validate(payload["result"]),
            )
        return httpx.Response(503 if fails else 200, json={"status": "accepted"})

    factory.return_value = ParsingAPIClient(
        base_url="http://poc.invalid/execution/", token="fixture", transport=httpx.MockTransport(handle)
    )
    with pytest.raises(ParsingPublicationError) as captured:
        supervise_dag_parse(workload, server="http://poc.invalid/execution/")
    assert parse.call_count == failed_definition + 1
    local_runner.executor.get_event_buffer.return_value = {
        workload.key: (ParseDagDefinitionsState.FAILED, captured.value)
    }
    local_runner.tick()
    assert store.get_admissions(ROUTE) == []
    accepted = failed_definition + int(accepted_before_disconnect)
    assert [a["status"] for a in store.get_attempts(workload.workload_id)] == (
        ["accepted"] * accepted + ["retired"] * (2 - accepted)
    )
    assert orchestrator.step().workload_id is None
    clock.move_to(NOW + timedelta(seconds=6))
    retry = orchestrator.step().workload_id
    if accepted == len(workload.definitions):
        assert retry is None
    else:
        assert [d.relative_path for d in get_workload(store, retry).definitions] == ["a.py", "b.py"][
            accepted:
        ]
    if not accepted_before_disconnect:
        with pytest.raises(ReceiptConflictError, match="removed or superseded"):
            store.accept_result(
                workload.workload_id,
                results[failed_definition].attempt_id,
                captured.value.execution_id,
                results[failed_definition],
            )


def test_publication_evidence_cannot_retire_another_execution(store, local_runner):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    local_runner.tick()
    store.claim(workload.workload_id, workload.definitions[0].attempt_id, uuid4())
    local_runner.executor.get_event_buffer.return_value = {
        workload.key: (ParseDagDefinitionsState.FAILED, ParsingPublicationError("unavailable", str(uuid4())))
    }
    local_runner.tick()
    assert store.get_attempts(workload.workload_id)[0]["status"] == "claimed"
    assert store.get_admissions(ROUTE)[0]["state"] == "submitted"


def test_unsent_expiry_is_retired_and_backed_off(store, local_runner, clock):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    clock.move_to(NOW + timedelta(seconds=61))
    local_runner.tick()
    local_runner.executor.queue_workload.assert_not_called()
    assert store.get_attempts(workload.workload_id)[0]["status"] == "retired"
    assert orchestrator.step().workload_id is None
    clock.move_to(NOW + timedelta(seconds=66))
    assert orchestrator.step().workload_id is not None


def test_runner_retries_reconciliation_without_dispatching_again(store, local_runner, monkeypatch):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    local_runner.tick()
    accept(store, workload)
    local_runner.executor.get_event_buffer.return_value = {
        workload.key: (ParseDagDefinitionsState.SUCCESS, None)
    }
    original = store.retire_and_replace
    patched = mock.create_autospec(original, side_effect=OSError("store unavailable"))
    monkeypatch.setattr(store, "retire_and_replace", patched)
    with pytest.raises(OSError, match="store unavailable"):
        local_runner.tick()
    monkeypatch.setattr(store, "retire_and_replace", original)
    local_runner.executor.get_event_buffer.return_value = {}
    local_runner.tick()
    assert store.get_admissions(ROUTE) == []
    local_runner.executor.queue_workload.assert_called_once()


def test_orchestration_api_enforces_current_inventory(store, tmp_path):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    key = Ed25519PrivateKey.generate()
    public = tmp_path / "public.pem"
    public.write_bytes(key.public_key().public_bytes(Encoding.PEM, PublicFormat.SubjectPublicKeyInfo))
    generator = JWTGenerator(
        private_key=key,
        kid="dag-parsing-poc",
        issuer="dag-parsing-poc",
        audience="dag-parsing-poc",
        algorithm="EdDSA",
        valid_for=120,
    )
    token = generator.generate(
        {
            "sub": str(workload.workload_id),
            "scope": "dag-parsing-poc",
            "attempt_ids": [str(d.attempt_id) for d in workload.definitions],
        }
    )
    orchestrator.update_inventory(BUNDLE, [])
    with TestClient(create_app(store.path, public, orchestrated=True)) as client:
        response = client.post(
            f"/execution/poc/parsing/workloads/{workload.workload_id}/attempts/{workload.definitions[0].attempt_id}/claim",
            json={"execution_id": str(uuid4())},
            headers={"Authorization": f"Bearer {token}"},
        )
        assert response.status_code == 409


@pytest.mark.parametrize(
    ("route", "bundle"), [("", "poc"), ("default", "poc"), ("celery", "poc"), (ROUTE, " ")]
)
def test_invalid_route_or_bundle(store, route, bundle):
    with pytest.raises(ValueError, match="explicit parsing route and bundle"):
        ParseOrchestrator(store, route=route, bundle=bundle)


@pytest.mark.parametrize("path", ["a//b.py", "./a.py", "../a.py"])
def test_definition_references_reject_aliases_and_traversal(path):
    with pytest.raises(ValueError, match="canonical|relative POSIX"):
        DiscoveredDefinition(relative_path=path, source_revision="v1")


@pytest.mark.parametrize("invalid", ["capabilities", "route", "capacity"])
def test_runner_rejects_unsupported_configuration(store, local_runner, invalid):
    executor = local_runner.executor
    route = ROUTE
    if invalid == "capabilities":
        executor.supported_workload_types = frozenset({WorkloadType.EXECUTE_TASK})
    elif invalid == "route":
        route = "default"
    else:
        executor.parallelism = 0
    with pytest.raises(ValueError, match="dedicated|explicit"):
        ParsingExecutorRunner(store, executor, route=route, token_issuer=lambda _: "token")


def test_runner_start_and_graceful_drain(store, local_runner):
    with pytest.raises(RuntimeError, match="already started"):
        local_runner.start()
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    local_runner.tick()
    local_runner.executor.get_event_buffer.return_value = {
        workload.key: (ParseDagDefinitionsState.RUNNING, None)
    }
    local_runner.tick()
    assert len(store.get_admissions(ROUTE)) == 1
    accept(store, workload)
    local_runner.executor.get_event_buffer.return_value = {
        workload.key: (ParseDagDefinitionsState.SUCCESS, None)
    }
    local_runner.close()
    local_runner.close()
    local_runner.executor.end.assert_called_once()
    assert store.get_admissions(ROUTE) == []
    with pytest.raises(RuntimeError, match="Start the runner"):
        local_runner.tick()


def test_runner_keeps_unknown_submission_charged_before_dispatching_more(store, local_runner):
    orchestrator = create_orchestrator(store, capacity=2, batch_size=1)
    orchestrator.update_inventory(BUNDLE, inventory("a.py", "b.py"))
    first = orchestrator.step().workload_id
    store.mark_submitted(first)
    second = orchestrator.step().workload_id
    local_runner.tick()
    local_runner.executor.queue_workload.assert_not_called()
    assert [row["state"] for row in store.get_admissions(ROUTE)] == ["submitted", "reserved"]
    assert store.get_manifest(second)["definitions"][0]["relative_path"] == "b.py"


def test_deadline_expiring_during_signing_retires_without_publication(store, local_runner, clock):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)

    def issue_late(manifest):
        clock.move_to(NOW + timedelta(seconds=61))
        return "token"

    local_runner.token_issuer = issue_late
    local_runner.tick()
    local_runner.executor.queue_workload.assert_not_called()
    assert store.get_attempts(workload.workload_id)[0]["status"] == "retired"


def test_uncertain_local_submission_is_not_repeated(store, local_runner):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    orchestrator.step()
    local_runner.executor.queue_workload.side_effect = OSError("submission uncertain")
    with pytest.raises(OSError, match="submission uncertain"):
        local_runner.tick()
    assert store.get_admissions(ROUTE)[0]["state"] == "submitted"
    local_runner.executor.queue_workload.side_effect = None
    local_runner.tick()
    local_runner.executor.queue_workload.assert_called_once()


def test_discovery_reads_sdk_files_and_archive_members_without_importing(tmp_path):
    source = b"from airflow.sdk import DAG\nraise RuntimeError('must not import during discovery')\n"
    (tmp_path / "a.py").write_bytes(source)
    with ZipFile(tmp_path / "bundle.zip", "w") as archive:
        archive.writestr("nested/b.py", source)
    found = {definition.relative_path: definition for definition in discover_python_bundle(tmp_path)}
    assert set(found) == {"a.py", "bundle.zip/nested/b.py"}
    assert {definition.source_revision for definition in found.values()} == {
        hashlib.sha256(source).hexdigest()
    }
    assert (
        found["bundle.zip/nested/b.py"].archive_revision
        == hashlib.sha256((tmp_path / "bundle.zip").read_bytes()).hexdigest()
    )
    (tmp_path / "a.py").write_bytes(source + b"changed\n")
    updated = {definition.relative_path: definition for definition in discover_python_bundle(tmp_path)}
    assert updated["a.py"].source_revision != found["a.py"].source_revision
    assert updated["bundle.zip/nested/b.py"] == found["bundle.zip/nested/b.py"]


def test_failed_discovery_cannot_apply_partial_inventory(store, tmp_path):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("old.py"))
    before = store.get_sources(ROUTE, "poc")
    (tmp_path / "a.py").write_text("from airflow.sdk import DAG\n")
    (tmp_path / "broken.zip").write_bytes(b"invalid archive")
    with pytest.raises(ValueError, match="Discovery failed"):
        orchestrator.update_inventory(BUNDLE, discover_python_bundle(tmp_path))
    assert store.get_sources(ROUTE, "poc") == before


def test_discovery_rejects_outside_symlink(tmp_path):
    root = tmp_path / "bundle"
    root.mkdir()
    outside = tmp_path / "outside.py"
    outside.write_text("from airflow.sdk import DAG\n")
    (root / "alias.py").symlink_to(outside)
    with pytest.raises(ValueError, match="escapes"):
        discover_python_bundle(root)


@pytest.mark.parametrize("outcome", ["timeout", "worker_error"])
def test_uncertain_importer_termination_requires_external_recovery(store, local_runner, outcome, clock):
    orchestrator = create_orchestrator(store)
    orchestrator.update_inventory(BUNDLE, inventory("a.py"))
    workload = get_workload(store, orchestrator.step().workload_id)
    local_runner.tick()
    accept(store, workload, outcome=outcome)
    local_runner.executor.get_event_buffer.return_value = {
        workload.key: (ParseDagDefinitionsState.SUCCESS, None)
    }
    clock.move_to(NOW + timedelta(seconds=400))
    local_runner.tick()
    assert len(store.get_admissions(ROUTE)) == 1
    assert orchestrator.step().workload_id is None
    retire(store, workload)
    local_runner.executor.get_event_buffer.return_value = {}
    local_runner.tick()
    assert local_runner._submitted == {}
    assert local_runner._terminal == {}
    assert orchestrator.step().workload_id is not None
