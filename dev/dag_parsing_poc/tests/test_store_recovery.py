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

import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from threading import Barrier, Event
from unittest import mock
from uuid import uuid4

import pytest
import time_machine

from airflow.executors.workloads.base import BundleInfo
from airflow.executors.workloads.parsing import DagDefinitionAttempt, DagDefinitionResult, ParseDagDefinitions

from dev.dag_parsing_poc.store import (
    ReceiptCapacityError,
    ReceiptConflictError,
    ReceiptExpiredError,
    ReceiptNotFoundError,
    ReceiptStore,
)

NOW = datetime(2026, 9, 25, tzinfo=timezone.utc)
ROUTE = "parsing-only"


@pytest.fixture(autouse=True)
def clock():
    with time_machine.travel(NOW, tick=False) as traveller:
        yield traveller


@pytest.fixture
def store(tmp_path):
    return ReceiptStore(tmp_path / "recovery.sqlite")


def _build_workload(*, queue=ROUTE):
    return ParseDagDefinitions(
        workload_id=uuid4(),
        bundle_info=BundleInfo(name="recovery", version="revision-v1"),
        definitions=tuple(
            DagDefinitionAttempt(
                attempt_id=uuid4(),
                relative_path=f"definition_{index}.py",
                source_revision=f"source-{index}",
                timeout_seconds=5 + index,
            )
            for index in range(2)
        ),
        start_deadline=NOW + timedelta(seconds=30),
        stop_deadline=NOW + timedelta(seconds=60),
        queue=queue,
        token="private-workload-token-must-not-be-stored",
    )


def _build_result(definition):
    return DagDefinitionResult(
        attempt_id=definition.attempt_id,
        relative_path=definition.relative_path,
        source_revision=definition.source_revision,
        outcome="success",
        serialized_dags=[{"dag": {"dag_id": definition.relative_path}}],
        duration_seconds=0.1,
    )


def _build_termination(workload, *owners):
    return {
        "kind": "confirmed_worker_termination",
        "workload_id": str(workload.workload_id),
        "execution_ids": [str(owner) for owner in owners],
        "evidence": {"container_id": "verified-by-trusted-coordinator", "exited": True},
    }


def _recover(store, workload, termination, **deadlines):
    return store.retire_and_replace(
        workload.workload_id,
        termination=termination,
        **{
            "start_deadline": NOW + timedelta(seconds=10),
            "stop_deadline": NOW + timedelta(seconds=30),
        }
        | deadlines,
    )


def _prepare_partial_batch(store):
    workload = _build_workload()
    owner = uuid4()
    store.reserve_workload(workload, route=ROUTE, capacity=1)
    store.mark_submitted(workload.workload_id)
    for definition in workload.definitions:
        store.claim(workload.workload_id, definition.attempt_id, owner)
    accepted = store.accept_result(
        workload.workload_id,
        workload.definitions[0].attempt_id,
        owner,
        _build_result(workload.definitions[0]),
    )
    return workload, owner, accepted


def _read_database(store):
    with sqlite3.connect(store.path) as connection:
        return {
            table: connection.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            for table in (
                "workloads",
                "attempts",
                "admissions",
                "retired_attempts",
                "recovery_decisions",
                "pending_admissions",
            )
        }


def test_admission_is_idempotent_and_survives_reopen_without_tokens(store):
    workload = _build_workload()
    reserved = store.reserve_workload(workload, route=ROUTE, capacity=1)
    assert reserved == {
        "workload_id": str(workload.workload_id),
        "route": ROUTE,
        "state": "reserved",
        "manifest": workload.model_dump(mode="json", exclude={"token"}),
    }
    refreshed_token = workload.model_copy(update={"token": "refreshed-token"})
    assert store.reserve_workload(refreshed_token, route=ROUTE, capacity=1) == reserved
    reopened = ReceiptStore(store.path)
    assert reopened.restore_admissions(ROUTE) == [reserved]
    submitted = reopened.mark_submitted(workload.workload_id)
    assert submitted == reserved | {"state": "submitted"}
    assert ReceiptStore(store.path).mark_submitted(workload.workload_id) == submitted
    assert reopened.get_manifest(workload.workload_id) == reserved["manifest"]
    assert workload.token not in json.dumps(_read_database(store))
    assert "refreshed-token" not in json.dumps(_read_database(store))


@pytest.mark.parametrize("capacity", [0, -1, True, 1.5])
def test_admission_rejects_invalid_capacity(store, capacity):
    with pytest.raises(ValueError, match="positive integer"):
        store.reserve_workload(_build_workload(), route=ROUTE, capacity=capacity)
    assert not _read_database(store)["workloads"]


@pytest.mark.parametrize("queue", [None, "", " ", "another-route"])
def test_admission_rejects_missing_or_different_explicit_queue(store, queue):
    with pytest.raises(ReceiptConflictError, match="explicit queue"):
        store.reserve_workload(_build_workload(queue=queue), route=ROUTE, capacity=1)
    assert store.get_admissions(ROUTE) == []


@pytest.mark.parametrize("changed", ["queue", "bundle_info", "definitions"])
def test_admission_identity_cannot_change_manifest_or_route(store, changed):
    workload = _build_workload()
    reserved = store.reserve_workload(workload, route=ROUTE, capacity=1)
    replacement = _build_workload(queue="different-route")
    altered = workload.model_copy(update={changed: getattr(replacement, changed)})
    if changed == "bundle_info":
        altered.bundle_info = BundleInfo(name="changed", version="v2")
    with pytest.raises(ReceiptConflictError):
        store.reserve_workload(altered, route=altered.queue, capacity=1)
    assert store.get_admissions(ROUTE) == [reserved]


def test_restore_charges_every_legacy_manifest_even_accepted_and_above_new_limit(store):
    workloads = [_build_workload(), _build_workload(), _build_workload(queue="other-route")]
    for workload in workloads:
        store.register_workload(workload)
    owner = uuid4()
    completed = workloads[0]
    for definition in completed.definitions:
        store.claim(completed.workload_id, definition.attempt_id, owner)
        store.accept_result(completed.workload_id, definition.attempt_id, owner, _build_result(definition))
    assert store.get_admissions(ROUTE) == []
    restored = ReceiptStore(store.path).restore_admissions(ROUTE)
    assert {row["workload_id"] for row in restored} == {str(item.workload_id) for item in workloads[:2]}
    assert {row["state"] for row in restored} == {"submitted"}
    assert store.restore_admissions(ROUTE) == restored
    assert store.get_admissions("other-route") == []
    with pytest.raises(ReceiptCapacityError):
        store.reserve_workload(_build_workload(), route=ROUTE, capacity=1)
    assert store.get_admissions(ROUTE) == restored
    assert store.reserve_workload(completed, route=ROUTE, capacity=1) == restored[0]


def test_reserve_accounts_for_legacy_manifests_without_explicit_restore(store):
    legacy = _build_workload()
    store.register_workload(legacy)
    fresh = _build_workload()
    with pytest.raises(ReceiptCapacityError):
        store.reserve_workload(fresh, route=ROUTE, capacity=1)
    with pytest.raises(ReceiptNotFoundError):
        store.get_manifest(fresh.workload_id)
    assert len(store.restore_admissions(ROUTE)) == 1


def test_upgrade_indexes_only_legacy_work_without_admissions(store):
    legacy = _build_workload()
    reserved = _build_workload()
    store.register_workload(legacy)
    store.register_workload(_build_workload(queue=None))
    store.reserve_workload(reserved, route=ROUTE, capacity=2)
    # Recreate the previous schema, where new legacy registrations had no pending index.
    later_legacy = _build_workload(queue="another-route")
    store.register_workload(later_legacy)
    with sqlite3.connect(store.path) as connection:
        connection.execute("DROP TABLE pending_admissions")
    reopened = ReceiptStore(store.path)
    with sqlite3.connect(store.path) as connection:
        assert connection.execute("SELECT workload_id, route FROM pending_admissions").fetchall() == [
            (str(later_legacy.workload_id), "another-route")
        ]
    assert [row["state"] for row in reopened.restore_admissions(ROUTE)] == ["submitted", "reserved"]
    assert reopened.restore_admissions("another-route")[0]["state"] == "submitted"
    assert not _read_database(reopened)["pending_admissions"]


def test_admission_query_work_does_not_grow_with_completed_history(store, monkeypatch):
    connect = sqlite3.connect
    steps = 0

    def count_step():
        nonlocal steps
        steps += 1
        return 0

    def traced_connect(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connection.set_progress_handler(count_step, 1)
        return connection

    monkeypatch.setattr(sqlite3, "connect", mock.create_autospec(connect, side_effect=traced_connect))
    first = _build_workload()
    store.reserve_workload(first, route=ROUTE, capacity=2)
    baseline = steps
    with connect(store.path) as connection:
        manifest = first.model_dump(mode="json", exclude={"token"})
        history = [(str(uuid4()), manifest | {"queue": ROUTE}) for _ in range(1000)]
        connection.executemany(
            "INSERT INTO workloads VALUES (?, ?)",
            [(key, json.dumps(value | {"workload_id": key})) for key, value in history],
        )
        connection.executemany(
            "INSERT INTO admissions VALUES (?, ?, 'released')", [(key, ROUTE) for key, _ in history]
        )
    steps = 0
    store.reserve_workload(_build_workload(), route=ROUTE, capacity=2)
    # SQLite VM instructions measure query work without wall-clock timing noise.
    assert steps < baseline * 2


@pytest.mark.parametrize("elapsed", [30, 61])
def test_expired_unsent_retirement_releases_and_fences_atomically(store, clock, elapsed):
    workload = _build_workload()
    store.reserve_workload(workload, route=ROUTE, capacity=1)
    assert not store.retire_expired_reservation(workload.workload_id)
    clock.move_to(NOW + timedelta(seconds=elapsed))
    with pytest.raises(ReceiptExpiredError):
        store.mark_submitted(workload.workload_id)
    assert store.get_admissions(ROUTE)[0]["state"] == "reserved"
    assert store.retire_expired_reservation(workload.workload_id)
    assert not store.retire_expired_reservation(workload.workload_id)
    reopened = ReceiptStore(store.path)
    assert reopened.restore_admissions(ROUTE) == []
    assert {row["status"] for row in reopened.get_attempts(workload.workload_id)} == {"retired"}
    for definition in workload.definitions:
        with pytest.raises(ReceiptConflictError, match="retired"):
            reopened.claim(workload.workload_id, definition.attempt_id, uuid4())
    with pytest.raises(ReceiptConflictError, match="reuse"):
        reopened.reserve_workload(workload, route=ROUTE, capacity=1)


@pytest.mark.parametrize("state", ["submitted", "claimed"])
def test_expiry_cannot_release_possibly_executing_work(store, clock, state):
    workload = _build_workload()
    store.reserve_workload(workload, route=ROUTE, capacity=1)
    if state == "submitted":
        store.mark_submitted(workload.workload_id)
    else:
        store.claim(workload.workload_id, workload.definitions[0].attempt_id, uuid4())
    before = _read_database(store)
    clock.move_to(NOW + timedelta(seconds=120))
    if state == "submitted":
        assert not store.retire_expired_reservation(workload.workload_id)
    else:
        with pytest.raises(ReceiptConflictError, match="confirmed termination"):
            store.retire_expired_reservation(workload.workload_id)
    assert _read_database(store) == before


def test_unsent_retirement_rolls_back_fencing_if_release_fails(store, clock):
    workload = _build_workload()
    store.reserve_workload(workload, route=ROUTE, capacity=1)
    before = _read_database(store)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "CREATE TRIGGER reject_release BEFORE UPDATE OF state ON admissions "
            "BEGIN SELECT RAISE(ABORT, 'simulated release failure'); END"
        )
    clock.move_to(NOW + timedelta(seconds=30))
    with pytest.raises(sqlite3.IntegrityError, match="simulated release failure"):
        store.retire_expired_reservation(workload.workload_id)
    assert _read_database(store) == before


def test_unsent_retirement_requires_an_admission(store):
    with pytest.raises(ReceiptNotFoundError, match="no admission"):
        store.retire_expired_reservation(uuid4())


def test_route_capacity_check_is_atomic_under_concurrent_admission(store):
    barrier = Barrier(2)
    workloads = [_build_workload(), _build_workload()]

    def reserve(workload):
        barrier.wait(timeout=5)
        try:
            return store.reserve_workload(workload, route=ROUTE, capacity=1)
        except ReceiptCapacityError:
            return None

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, workloads))
    accepted = [result for result in results if result]
    assert len(accepted) == 1
    assert store.get_admissions(ROUTE) == accepted
    assert len(_read_database(store)["workloads"]) == 1
    assert store.reserve_workload(_build_workload(queue="other-route"), route="other-route", capacity=1)


def test_admission_registration_rolls_back_if_reservation_write_fails(store):
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "CREATE TRIGGER reject_admission BEFORE INSERT ON admissions "
            "BEGIN SELECT RAISE(ABORT, 'simulated admission failure'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="simulated admission failure"):
        store.reserve_workload(_build_workload(), route=ROUTE, capacity=1)
    assert all(not rows for rows in _read_database(store).values())


def test_recovery_preserves_receipts_replaces_only_unfinished_and_fences_old_authority(store, clock):
    workload, owner, accepted = _prepare_partial_batch(store)
    first, second = workload.definitions
    decision = _recover(store, workload, _build_termination(workload, owner))
    assert decision["workload_id"] == str(workload.workload_id)
    assert decision["retired_attempt_ids"] == [str(second.attempt_id)]
    replacement = decision["replacement"]
    assert replacement["workload_id"] != str(workload.workload_id)
    assert replacement["bundle_info"] == workload.bundle_info.model_dump(mode="json")
    assert replacement["queue"] == ROUTE
    assert len(replacement["definitions"]) == 1
    new_definition = replacement["definitions"][0]
    assert new_definition["attempt_id"] not in {str(first.attempt_id), str(second.attempt_id)}
    assert {key: value for key, value in new_definition.items() if key != "attempt_id"} == second.model_dump(
        mode="json", exclude={"attempt_id"}
    )
    assert "token" not in replacement
    assert store.get_results(workload.workload_id) == [_build_result(first).model_dump(mode="json")]
    assert [row["status"] for row in store.get_attempts(workload.workload_id)] == ["accepted", "retired"]
    assert store.get_admissions(ROUTE) == [
        {
            "workload_id": replacement["workload_id"],
            "route": ROUTE,
            "state": "reserved",
            "manifest": replacement,
        }
    ]
    assert [row["state"] for row in store.get_admissions(ROUTE, include_released=True)] == [
        "released",
        "reserved",
    ]
    with pytest.raises(ReceiptCapacityError):
        store.reserve_workload(_build_workload(), route=ROUTE, capacity=1)
    for execution in (owner, uuid4()):
        with pytest.raises(ReceiptConflictError, match="retired"):
            store.claim(workload.workload_id, second.attempt_id, execution)
    with pytest.raises(ReceiptConflictError, match="retired"):
        store.accept_result(workload.workload_id, second.attempt_id, owner, _build_result(second))
    clock.move_to(NOW + timedelta(minutes=5))
    assert store.claim(workload.workload_id, first.attempt_id, uuid4())["status"] == "accepted"
    assert (
        store.accept_result(workload.workload_id, first.attempt_id, owner, _build_result(first)) == accepted
    )
    conflicting = _build_result(first).model_copy(update={"diagnostics": ["changed"]})
    with pytest.raises(ReceiptConflictError, match="cannot be changed"):
        store.accept_result(workload.workload_id, first.attempt_id, owner, conflicting)


def test_recovery_replay_survives_restart_and_does_not_extend_deadlines(store, clock):
    workload, owner, _ = _prepare_partial_batch(store)
    termination = _build_termination(workload, owner)
    decision = _recover(store, workload, termination)
    before = _read_database(store)
    clock.move_to(NOW + timedelta(days=1))
    reopened = ReceiptStore(store.path)
    assert (
        _recover(
            reopened,
            workload,
            termination,
            start_deadline=NOW + timedelta(days=1, seconds=10),
            stop_deadline=NOW + timedelta(days=1, seconds=30),
        )
        == decision
    )
    assert _read_database(reopened) == before
    assert len(reopened.restore_admissions(ROUTE)) == 1
    with pytest.raises(ReceiptConflictError, match="reuse"):
        reopened.reserve_workload(workload, route=ROUTE, capacity=1)
    with pytest.raises(ReceiptConflictError, match="submitted again"):
        reopened.mark_submitted(workload.workload_id)


def test_all_accepted_recovery_releases_capacity_without_replacement(store):
    workload, owner, _ = _prepare_partial_batch(store)
    definition = workload.definitions[1]
    store.accept_result(workload.workload_id, definition.attempt_id, owner, _build_result(definition))
    decision = _recover(store, workload, _build_termination(workload, owner))
    assert decision == {
        "workload_id": str(workload.workload_id),
        "replacement": None,
        "retired_attempt_ids": [],
    }
    assert not store.get_admissions(ROUTE)
    assert len(store.get_results(workload.workload_id)) == 2
    assert store.reserve_workload(_build_workload(), route=ROUTE, capacity=1)["state"] == "reserved"


def test_recovery_rechecks_new_claim_owners_and_allows_accepted_snapshot_owners(store):
    workload = _build_workload()
    store.reserve_workload(workload, route=ROUTE, capacity=1)
    first, second = workload.definitions
    first_owner, second_owner = uuid4(), uuid4()
    store.claim(workload.workload_id, first.attempt_id, first_owner)
    stale_evidence = _build_termination(workload, first_owner)
    store.claim(workload.workload_id, second.attempt_id, second_owner)
    before = _read_database(store)
    with pytest.raises(ReceiptConflictError, match="every unfinished execution"):
        _recover(store, workload, stale_evidence)
    assert _read_database(store) == before
    evidence = _build_termination(workload, first_owner, second_owner)
    store.accept_result(workload.workload_id, second.attempt_id, second_owner, _build_result(second))
    decision = _recover(store, workload, evidence)
    assert decision["retired_attempt_ids"] == [str(first.attempt_id)]
    assert len(store.get_results(workload.workload_id)) == 1


def test_recovery_of_unclaimed_legacy_batch_registers_and_fences_every_replacement(store):
    workload = _build_workload()
    store.register_workload(workload)
    decision = _recover(store, workload, _build_termination(workload))
    replacement = decision["replacement"]
    assert len(replacement["definitions"]) == len(workload.definitions)
    assert decision["retired_attempt_ids"] == [str(item.attempt_id) for item in workload.definitions]
    assert store.get_manifest(replacement["workload_id"]) == replacement
    assert {row["status"] for row in store.get_attempts(workload.workload_id)} == {"retired"}
    assert {row["status"] for row in store.get_attempts(replacement["workload_id"])} == {"pending"}
    for definition in workload.definitions:
        with pytest.raises(ReceiptConflictError, match="retired"):
            store.claim(workload.workload_id, definition.attempt_id, uuid4())
    assert len(store.restore_admissions(ROUTE)) == 1


@pytest.mark.parametrize(
    "invalid",
    [
        {"kind": "backend_missing"},
        {"kind": "deadline_elapsed"},
        {"workload_id": "wrong-workload"},
        {"execution_ids": []},
        {"execution_ids": "not-a-list"},
        {"execution_ids": [None]},
        {"evidence": {}},
        {"evidence": None},
    ],
)
def test_recovery_requires_matching_termination_evidence_covering_current_owners(store, invalid):
    workload, owner, _ = _prepare_partial_batch(store)
    before = _read_database(store)
    with pytest.raises(ReceiptConflictError):
        _recover(store, workload, _build_termination(workload, owner) | invalid)
    assert _read_database(store) == before


@pytest.mark.parametrize(
    ("start", "stop"),
    [
        (NOW, NOW + timedelta(seconds=10)),
        (NOW + timedelta(seconds=31), NOW + timedelta(seconds=40)),
        (NOW + timedelta(seconds=10), NOW + timedelta(seconds=41)),
        (NOW + timedelta(seconds=10), NOW + timedelta(seconds=10)),
        ((NOW + timedelta(seconds=10)).replace(tzinfo=None), NOW + timedelta(seconds=30)),
        (NOW + timedelta(seconds=10), (NOW + timedelta(seconds=30)).replace(tzinfo=None)),
    ],
)
def test_recovery_rejects_unbounded_or_invalid_deadlines_without_changes(store, start, stop):
    workload, owner, _ = _prepare_partial_batch(store)
    before = _read_database(store)
    with pytest.raises(ReceiptConflictError, match="future, aware and bounded"):
        _recover(
            store, workload, _build_termination(workload, owner), start_deadline=start, stop_deadline=stop
        )
    assert _read_database(store) == before


def test_recovery_rolls_back_retirement_replacement_and_release_on_final_write_failure(store):
    workload, owner, _ = _prepare_partial_batch(store)
    before = _read_database(store)
    with sqlite3.connect(store.path) as connection:
        connection.execute(
            "CREATE TRIGGER reject_recovery BEFORE INSERT ON recovery_decisions "
            "BEGIN SELECT RAISE(ABORT, 'simulated recovery failure'); END"
        )
    with pytest.raises(sqlite3.IntegrityError, match="simulated recovery failure"):
        _recover(store, workload, _build_termination(workload, owner))
    assert _read_database(ReceiptStore(store.path)) == before
    definition = workload.definitions[1]
    store.accept_result(workload.workload_id, definition.attempt_id, owner, _build_result(definition))


@pytest.mark.parametrize("winner", ["publication", "retirement"])
def test_publication_and_retirement_serialize_in_one_transaction(store, monkeypatch, winner):
    workload, owner, _ = _prepare_partial_batch(store)
    definition = workload.definitions[1]
    inside_transaction, contender_started, release = Event(), Event(), Event()
    hook_name = "_get_attempt" if winner == "publication" else "_register_workload"
    original = getattr(store, hook_name)

    def pause_transaction(*args):
        value = original(*args)
        inside_transaction.set()
        assert release.wait(timeout=5)
        return value

    monkeypatch.setattr(store, hook_name, pause_transaction)

    def publish():
        return store.accept_result(
            workload.workload_id, definition.attempt_id, owner, _build_result(definition)
        )

    def recover():
        return _recover(store, workload, _build_termination(workload, owner))

    def contend(action):
        contender_started.set()
        return action()

    first, second = (publish, recover) if winner == "publication" else (recover, publish)
    with ThreadPoolExecutor(max_workers=2) as pool:
        leader = pool.submit(first)
        assert inside_transaction.wait(timeout=5)
        contender = pool.submit(contend, second)
        assert contender_started.wait(timeout=5)
        release.set()
        leading_result = leader.result(timeout=5)
        if winner == "publication":
            decision = contender.result(timeout=5)
            assert decision["replacement"] is None
            assert len(store.get_results(workload.workload_id)) == 2
        else:
            assert leading_result["retired_attempt_ids"] == [str(definition.attempt_id)]
            with pytest.raises(ReceiptConflictError, match="retired"):
                contender.result(timeout=5)
            assert len(store.get_results(workload.workload_id)) == 1


def test_concurrent_recovery_creates_exactly_one_replacement(store):
    workload, owner, _ = _prepare_partial_batch(store)
    barrier = Barrier(2)

    def recover():
        barrier.wait(timeout=5)
        return _recover(store, workload, _build_termination(workload, owner))

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(recover) for _ in range(2)]
        decisions = [future.result(timeout=5) for future in futures]
    assert decisions[0] == decisions[1]
    assert len(_read_database(store)["workloads"]) == 2
    assert len(store.get_admissions(ROUTE)) == 1
