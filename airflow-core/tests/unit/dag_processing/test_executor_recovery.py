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

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest import mock
from uuid import uuid4

import pytest
import time_machine
from celery import Celery, states

from airflow.dag_processing.executor_recovery import ParsingRecoveryCoordinator, PublicationOutcome
from airflow.dag_processing.parsing_state import ReceiptCapacityError, ReceiptStore
from airflow.executors.workloads import BundleInfo, WorkloadType
from airflow.executors.workloads.parsing import DagDefinitionAttempt, DagDefinitionResult, ParseDagDefinitions
from airflow.providers.celery.executors import celery_executor_utils
from airflow.providers.celery.executors.celery_executor import CeleryExecutor

NOW = datetime(2026, 9, 25, tzinfo=timezone.utc)
ROUTE = "recovery-only"


def _issue_token(manifest):
    return f"fresh-token-{manifest['workload_id']}"


def _validate_termination(termination):
    return None


def _create_workload(*, queue=ROUTE):
    return ParseDagDefinitions(
        workload_id=uuid4(),
        bundle_info=BundleInfo(name="poc", version="v1"),
        definitions=tuple(
            DagDefinitionAttempt(
                attempt_id=uuid4(), relative_path=f"{index}.py", source_revision="v1", timeout_seconds=5
            )
            for index in range(2)
        ),
        start_deadline=NOW + timedelta(seconds=30),
        stop_deadline=NOW + timedelta(seconds=60),
        queue=queue,
        token="old-token-never-persist",
    )


def _accept(store, workload, definition, execution_id):
    store.claim(workload.workload_id, definition.attempt_id, execution_id)
    store.accept_result(
        workload.workload_id,
        definition.attempt_id,
        execution_id,
        DagDefinitionResult(
            attempt_id=definition.attempt_id,
            relative_path=definition.relative_path,
            source_revision=definition.source_revision,
            outcome="success",
            duration_seconds=0,
        ),
    )


def _termination(workload, execution_ids):
    return {
        "kind": "confirmed_worker_termination",
        "workload_id": str(workload.workload_id),
        "execution_ids": list(map(str, execution_ids)),
        "evidence": {"inspected_container": "original-stopped-container"},
    }


@pytest.fixture(autouse=True)
def clock():
    with time_machine.travel(NOW, tick=False):
        yield


@pytest.fixture
def setup(tmp_path, monkeypatch):
    app = Celery(
        f"coordinator-{uuid4()}", broker="memory://", backend="cache+memory://", set_as_current=False
    )
    # BulkStateFetcher expects Redis's ordered MGET values; the memory cache returns a mapping.
    monkeypatch.setattr(
        app.backend,
        "mget",
        mock.create_autospec(
            app.backend.mget, side_effect=lambda keys: [app.backend.get(key) for key in keys]
        ),
    )
    create_app = mock.create_autospec(celery_executor_utils.create_celery_app, return_value=app)
    monkeypatch.setattr(celery_executor_utils, "create_celery_app", create_app)
    store = ReceiptStore(tmp_path / "receipts.sqlite")
    issuer = mock.create_autospec(_issue_token, side_effect=_issue_token)
    validator = mock.create_autospec(_validate_termination)
    sends = []

    def create_coordinator(*, capacity=1, route=ROUTE):
        executor = CeleryExecutor(parallelism=capacity)
        executor.supported_workload_types = frozenset({WorkloadType.PARSE_DAG_DEFINITIONS})

        def send(workloads):
            for _, workload, _, _ in workloads:
                admission = store.get_admissions(route)[0]
                assert admission["state"] == "submitted"
                assert "token" not in admission["manifest"]
                sends.append(workload)
            return [
                (key, workload, app.AsyncResult(str(workload.workload_id)))
                for key, workload, _, _ in workloads
            ]

        publisher = mock.create_autospec(executor._send_workloads_to_celery, side_effect=send)
        monkeypatch.setattr(executor, "_send_workloads_to_celery", publisher)
        return ParsingRecoveryCoordinator(
            store,
            executor,
            route=route,
            capacity=capacity,
            token_issuer=issuer,
            termination_validator=validator,
        )

    yield SimpleNamespace(
        store=store, app=app, issuer=issuer, validator=validator, create=create_coordinator, sends=sends
    )
    app.close()


@pytest.mark.parametrize("operation", ["admit", "dispatch", "sync", "recover", "capacity"])
def test_startup_is_required_before_any_coordination(setup, operation):
    coordinator = setup.create()
    workload = _create_workload()
    operations = {
        "admit": lambda: coordinator.admit(workload),
        "dispatch": coordinator.dispatch_reserved,
        "sync": coordinator.sync,
        "capacity": lambda: coordinator.available_slots,
        "recover": lambda: coordinator.recover(
            workload.workload_id,
            termination=_termination(workload, []),
            start_deadline=workload.start_deadline,
            stop_deadline=workload.stop_deadline,
        ),
    }
    with pytest.raises(RuntimeError, match="Restore durable admissions"):
        operations[operation]()
    assert setup.store.get_admissions(ROUTE) == []


@pytest.mark.parametrize("legacy", [True, False])
@pytest.mark.parametrize("backend_state", [states.PENDING, states.STARTED, states.SUCCESS, states.FAILURE])
def test_restart_restores_charge_despite_missing_or_terminal_provider_state(setup, legacy, backend_state):
    workload = _create_workload()
    if legacy:
        setup.store.register_workload(workload)
    else:
        setup.store.reserve_workload(workload, route=ROUTE, capacity=1)
        setup.store.mark_submitted(workload.workload_id)
    if backend_state != states.PENDING:
        result = ValueError("provider failure") if backend_state == states.FAILURE else None
        setup.app.backend.store_result(str(workload.workload_id), result, backend_state)
    coordinator = setup.create()
    coordinator.start()
    assert coordinator.executor.running == {workload.key}
    assert coordinator.executor.workloads[workload.key].task_id == str(workload.workload_id)
    events = coordinator.sync()
    if backend_state == states.SUCCESS:
        assert events[workload.key][0] == workload.success_state
    elif backend_state == states.FAILURE:
        assert events[workload.key][0] == workload.failure_state
    else:
        assert events == {}
    assert coordinator.executor.running == {workload.key}
    assert coordinator.executor.slots_available == coordinator.available_slots == 0
    with pytest.raises(ReceiptCapacityError):
        coordinator.admit(_create_workload())
    assert coordinator.dispatch_reserved() == []
    assert not setup.sends
    setup.issuer.assert_not_called()


def test_reserved_work_is_signed_and_marked_submitted_before_provider_io(setup):
    workload = _create_workload()
    coordinator = setup.create()
    coordinator.start()
    coordinator.admit(workload)
    assert coordinator.available_slots == 0
    assert coordinator.executor.slots_available == 1
    assert not any(coordinator.executor.executor_queues.values())
    setup.issuer.assert_not_called()
    assert coordinator.dispatch_reserved() == [PublicationOutcome(str(workload.workload_id), "published")]
    assert setup.sends[0].token == f"fresh-token-{workload.workload_id}"
    assert setup.sends[0].queue == ROUTE
    assert coordinator.executor.running == {workload.key}
    assert not coordinator.executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS]
    restarted = setup.create()
    restarted.start()
    assert restarted.dispatch_reserved() == []
    assert len(setup.sends) == 1
    assert setup.issuer.call_count == 1


@pytest.mark.parametrize("published", [False, True])
def test_uncertain_submission_is_never_republished_after_crash(setup, published):
    workload = _create_workload()
    coordinator = setup.create()
    coordinator.start()
    coordinator.admit(workload)
    sender = coordinator.executor._send_workloads_to_celery
    original_send = sender.side_effect

    def crash(workloads):
        assert setup.store.get_admissions(ROUTE)[0]["state"] == "submitted"
        if published:
            original_send(workloads)
        raise SystemExit("coordinator crashed before broker acknowledgment")

    sender.side_effect = crash
    with pytest.raises(SystemExit, match="coordinator crashed"):
        coordinator.dispatch_reserved()
    assert coordinator.executor.running == {workload.key}
    restarted = setup.create()
    restarted.start()
    assert restarted.dispatch_reserved() == []
    assert restarted.available_slots == 0
    assert len(setup.sends) == int(published)


def test_restart_preserves_unsent_reservation_and_task_executor_capacity(setup):
    task_executor = CeleryExecutor(parallelism=2)
    original_types = task_executor.supported_workload_types
    coordinator = setup.create()
    coordinator.start()
    workload = _create_workload()
    coordinator.admit(workload)
    restarted = setup.create()
    restarted.start()
    setup.issuer.assert_not_called()
    assert restarted.dispatch_reserved() == [PublicationOutcome(str(workload.workload_id), "published")]
    assert task_executor.slots_available == 2
    assert task_executor.supported_workload_types == original_types
    assert WorkloadType.PARSE_DAG_DEFINITIONS not in original_types


def test_lower_capacity_restores_all_charges_and_blocks_admission(setup):
    for _ in range(2):
        setup.store.register_workload(_create_workload())
    coordinator = setup.create(capacity=1)
    coordinator.start()
    assert len(coordinator.executor.running) == 2
    assert coordinator.available_slots == 0
    with pytest.raises(ReceiptCapacityError):
        coordinator.admit(_create_workload())
    assert coordinator.dispatch_reserved() == []


@pytest.mark.parametrize("restart", ["before_recovery", "after_commit"])
def test_atomic_recovery_survives_restart_without_reimporting_accepted_definition(
    setup, monkeypatch, restart
):
    workload = _create_workload()
    coordinator = setup.create()
    coordinator.start()
    coordinator.admit(workload)
    coordinator.dispatch_reserved()
    execution_id = uuid4()
    _accept(setup.store, workload, workload.definitions[0], execution_id)
    setup.store.claim(workload.workload_id, workload.definitions[1].attempt_id, execution_id)
    accepted = setup.store.get_results(workload.workload_id)
    termination = _termination(workload, [execution_id])
    kwargs = {
        "termination": termination,
        "start_deadline": NOW + timedelta(seconds=10),
        "stop_deadline": NOW + timedelta(seconds=40),
    }
    if restart == "before_recovery":
        coordinator = setup.create()
        coordinator.start()
        decision = coordinator.recover(workload.workload_id, **kwargs)
    else:
        monkeypatch.setattr(
            coordinator,
            "_restore_tracking",
            mock.create_autospec(coordinator._restore_tracking, side_effect=SystemExit("after commit")),
        )
        with pytest.raises(SystemExit, match="after commit"):
            coordinator.recover(workload.workload_id, **kwargs)
        coordinator = setup.create()
        coordinator.start()
        decision = coordinator.recover(workload.workload_id, **kwargs)
    replacement = decision["replacement"]
    assert [definition["relative_path"] for definition in replacement["definitions"]] == ["1.py"]
    assert replacement["workload_id"] != str(workload.workload_id)
    assert coordinator.available_slots == 0
    assert coordinator.recover(workload.workload_id, **kwargs) == decision
    assert coordinator.dispatch_reserved() == [PublicationOutcome(replacement["workload_id"], "published")]
    restarted = setup.create()
    restarted.start()
    assert restarted.recover(workload.workload_id, **kwargs) == decision
    assert restarted.dispatch_reserved() == []
    assert len(setup.sends) == 2
    assert setup.store.get_results(workload.workload_id) == accepted


def test_all_accepted_work_releases_only_after_validated_recovery(setup):
    workload = _create_workload()
    coordinator = setup.create()
    coordinator.start()
    coordinator.admit(workload)
    coordinator.dispatch_reserved()
    for definition in workload.definitions:
        _accept(setup.store, workload, definition, uuid4())
    assert coordinator.available_slots == 0
    decision = coordinator.recover(
        workload.workload_id,
        termination=_termination(workload, []),
        start_deadline=NOW + timedelta(seconds=10),
        stop_deadline=NOW + timedelta(seconds=40),
    )
    assert decision["replacement"] is None
    assert coordinator.executor.slots_available == coordinator.available_slots == 1
    assert coordinator.dispatch_reserved() == []
    assert len(setup.sends) == 1


def test_wrong_route_is_rejected_before_recovery_validation(setup):
    workload = _create_workload(queue="other-parsing-route")
    setup.store.register_workload(workload)
    coordinator = setup.create()
    coordinator.start()
    with pytest.raises(ValueError, match="another parsing route"):
        coordinator.admit(workload)
    with pytest.raises(ValueError, match="another parsing route"):
        coordinator.recover(
            workload.workload_id,
            termination=_termination(workload, []),
            start_deadline=workload.start_deadline,
            stop_deadline=workload.stop_deadline,
        )
    setup.validator.assert_not_called()
    assert setup.store.get_admissions(ROUTE) == []


def test_unconfirmed_termination_preserves_original_charge_and_claim(setup):
    workload = _create_workload()
    coordinator = setup.create()
    coordinator.start()
    coordinator.admit(workload)
    coordinator.dispatch_reserved()
    execution_id = uuid4()
    setup.store.claim(workload.workload_id, workload.definitions[0].attempt_id, execution_id)
    before = setup.store.get_attempts(workload.workload_id)
    setup.validator.side_effect = ValueError("Container termination was not established")
    with pytest.raises(ValueError, match="not established"):
        coordinator.recover(
            workload.workload_id,
            termination=_termination(workload, [execution_id]),
            start_deadline=NOW + timedelta(seconds=10),
            stop_deadline=NOW + timedelta(seconds=40),
        )
    assert setup.store.get_attempts(workload.workload_id) == before
    assert coordinator.executor.running == {workload.key}
    assert coordinator.available_slots == 0


@pytest.mark.parametrize(
    "invalid", ["zero", "boolean", "mismatch", "empty_route", "default_route", "task_executor", "busy"]
)
def test_coordinator_requires_fresh_dedicated_executor_and_matching_capacity(setup, invalid):
    executor = setup.create().executor
    capacity, route = 1, ROUTE
    if invalid == "zero":
        capacity = 0
    elif invalid == "boolean":
        capacity = True
    elif invalid == "mismatch":
        capacity = 2
    elif invalid == "empty_route":
        route = " "
    elif invalid == "default_route":
        route = "celery"
    elif invalid == "task_executor":
        executor.supported_workload_types = CeleryExecutor.supported_workload_types
    else:
        executor.queue_workload(_create_workload(), session=None)
    with pytest.raises(ValueError, match="capacity matching|dedicated parsing-only|fresh executor"):
        ParsingRecoveryCoordinator(
            setup.store,
            executor,
            route=route,
            capacity=capacity,
            token_issuer=setup.issuer,
            termination_validator=setup.validator,
        )


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_dispatch_rejects_invalid_bounds(setup, limit):
    coordinator = setup.create()
    coordinator.start()
    with pytest.raises(ValueError, match="positive integer"):
        coordinator.dispatch_reserved(limit=limit)


def test_dispatch_respects_batch_bound_and_retained_running_capacity(setup):
    coordinator = setup.create(capacity=2)
    coordinator.start()
    workloads = [_create_workload(), _create_workload()]
    for workload in workloads:
        coordinator.admit(workload)
    assert coordinator.dispatch_reserved(limit=1) == [
        PublicationOutcome(str(workloads[0].workload_id), "published")
    ]
    assert len(coordinator.executor.running) == 1
    assert not any(coordinator.executor.executor_queues.values())
    assert coordinator.executor.slots_available == 1
    assert coordinator.available_slots == 0
    assert coordinator.dispatch_reserved(limit=2) == [
        PublicationOutcome(str(workloads[1].workload_id), "published")
    ]
    assert len(coordinator.executor.running) == 2


def test_token_failure_does_not_mark_or_publish_reserved_work(setup):
    coordinator = setup.create()
    coordinator.start()
    workload = _create_workload()
    coordinator.admit(workload)
    setup.issuer.side_effect = ValueError("signer unavailable")
    with pytest.raises(ValueError, match="signer unavailable"):
        coordinator.dispatch_reserved()
    assert setup.store.get_admissions(ROUTE)[0]["state"] == "reserved"
    assert not setup.sends
    assert coordinator.available_slots == 0


def test_crash_immediately_after_submission_commit_keeps_tracking_and_no_republish(setup, monkeypatch):
    coordinator = setup.create()
    coordinator.start()
    workload = _create_workload()
    coordinator.admit(workload)
    mark_submitted = setup.store.mark_submitted

    def crash(workload_id):
        mark_submitted(workload_id)
        raise SystemExit("committed before publication")

    monkeypatch.setattr(
        setup.store, "mark_submitted", mock.create_autospec(mark_submitted, side_effect=crash)
    )
    with pytest.raises(SystemExit, match="committed before publication"):
        coordinator.dispatch_reserved()
    assert coordinator.executor.running == {workload.key}
    restarted = setup.create()
    restarted.start()
    assert restarted.dispatch_reserved() == []
    assert not setup.sends


def test_coordinator_cannot_start_twice(setup):
    coordinator = setup.create()
    coordinator.start()
    with pytest.raises(RuntimeError, match="already started"):
        coordinator.start()


def test_termination_for_another_workload_is_rejected_before_validation(setup):
    coordinator = setup.create()
    coordinator.start()
    workload = _create_workload()
    coordinator.admit(workload)
    with pytest.raises(ValueError, match="another workload"):
        coordinator.recover(
            workload.workload_id,
            termination=_termination(_create_workload(), []),
            start_deadline=workload.start_deadline,
            stop_deadline=workload.stop_deadline,
        )
    setup.validator.assert_not_called()
    assert setup.store.get_admissions(ROUTE)[0]["state"] == "reserved"


@pytest.mark.parametrize("timeout", [False, True])
def test_provider_publish_failure_cannot_leave_a_republishable_queue(setup, timeout):
    coordinator = setup.create()
    coordinator.start()
    workload = _create_workload()
    coordinator.admit(workload)
    error = (
        celery_executor_utils.AirflowTaskTimeout("unknown publication")
        if timeout
        else RuntimeError("unknown publication")
    )
    sender = coordinator.executor._send_workloads_to_celery
    sender.side_effect = None
    sender.return_value = [(workload.key, workload, celery_executor_utils.ExceptionWithTraceback(error, ""))]
    assert coordinator.dispatch_reserved() == [PublicationOutcome(str(workload.workload_id), "uncertain")]
    assert coordinator.executor.running == {workload.key}
    assert not coordinator.executor.executor_queues[WorkloadType.PARSE_DAG_DEFINITIONS]
    assert not coordinator.executor.workload_publish_retries
    assert setup.store.get_admissions(ROUTE)[0]["state"] == "submitted"
    assert coordinator.dispatch_reserved() == []
    assert sender.call_count == 1


@pytest.mark.parametrize("elapsed", [30, 61])
def test_restart_retires_expired_unsent_work_and_allows_fresh_admission(setup, elapsed):
    workload = _create_workload()
    coordinator = setup.create()
    coordinator.start()
    coordinator.admit(workload)
    with time_machine.travel(NOW + timedelta(seconds=elapsed), tick=False):
        restarted = setup.create()
        restarted.start()
        assert restarted.dispatch_reserved() == [PublicationOutcome(str(workload.workload_id), "expired")]
        assert restarted.available_slots == 1
        assert restarted.dispatch_reserved() == []
        assert {row["status"] for row in setup.store.get_attempts(workload.workload_id)} == {"retired"}
        fresh = _create_workload().model_copy(
            update={
                "start_deadline": NOW + timedelta(seconds=120),
                "stop_deadline": NOW + timedelta(seconds=150),
            }
        )
        assert restarted.admit(fresh)["state"] == "reserved"
        assert not setup.sends
        setup.issuer.assert_not_called()
        assert restarted.dispatch_reserved() == [PublicationOutcome(str(fresh.workload_id), "published")]


def test_expiry_during_token_issuance_does_not_publish(setup):
    coordinator = setup.create()
    coordinator.start()
    workload = _create_workload()
    coordinator.admit(workload)
    with time_machine.travel(NOW, tick=False) as clock:

        def slow_issuer(manifest):
            clock.shift(timedelta(seconds=30))
            return _issue_token(manifest)

        setup.issuer.side_effect = slow_issuer
        assert coordinator.dispatch_reserved() == [PublicationOutcome(str(workload.workload_id), "expired")]
    assert not setup.sends
    assert coordinator.available_slots == 1


@pytest.mark.parametrize("restart", [False, True])
@mock.patch("airflow.executors.base_executor.BaseExecutor._emit_metrics", autospec=True)
def test_executor_heartbeat_cannot_dispatch_unsigned_reservations(mock_metrics, setup, restart):
    coordinator = setup.create()
    coordinator.start()
    workload = _create_workload()
    coordinator.admit(workload)
    if restart:
        coordinator = setup.create()
        coordinator.start()
    coordinator.executor.heartbeat()
    assert not setup.sends
    setup.issuer.assert_not_called()
    assert setup.store.get_admissions(ROUTE)[0]["state"] == "reserved"
    assert coordinator.available_slots == 0
    assert coordinator.dispatch_reserved() == [PublicationOutcome(str(workload.workload_id), "published")]
    assert len(setup.sends) == 1


def test_expiry_of_submitted_work_never_releases_capacity(setup):
    coordinator = setup.create()
    coordinator.start()
    workload = _create_workload()
    coordinator.admit(workload)
    coordinator.dispatch_reserved()
    with time_machine.travel(NOW + timedelta(seconds=120), tick=False):
        restarted = setup.create()
        restarted.start()
        assert restarted.dispatch_reserved() == []
        assert restarted.available_slots == 0
        assert setup.store.get_admissions(ROUTE)[0]["state"] == "submitted"


def test_expired_reservations_are_retired_even_when_submissions_exceed_reduced_capacity(setup):
    coordinator = setup.create(capacity=2)
    coordinator.start()
    workloads = [_create_workload(), _create_workload()]
    for workload in workloads:
        coordinator.admit(workload)
    coordinator.dispatch_reserved()
    with time_machine.travel(NOW + timedelta(seconds=120), tick=False):
        restarted = setup.create(capacity=1)
        restarted.start()
        assert restarted.dispatch_reserved() == [PublicationOutcome(str(workloads[1].workload_id), "expired")]
        assert len(setup.store.get_admissions(ROUTE)) == 1
        assert restarted.available_slots == 0


def test_reserved_work_waits_when_reduced_capacity_is_fully_submitted(setup):
    coordinator = setup.create(capacity=2)
    coordinator.start()
    for _ in range(2):
        coordinator.admit(_create_workload())
    coordinator.dispatch_reserved()
    restarted = setup.create(capacity=1)
    restarted.start()
    assert restarted.dispatch_reserved() == []
    assert len(setup.sends) == 1
    assert [row["state"] for row in setup.store.get_admissions(ROUTE)] == ["submitted", "reserved"]
