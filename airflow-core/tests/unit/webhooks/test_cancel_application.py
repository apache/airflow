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
"""How the scheduler applies webhook cancel requests to the runs and tasks about their correlation keys."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any
from unittest import mock

import pytest
from sqlalchemy import select, update

from airflow._shared.webhook_naming.matching import compute_key_digest, compute_match_digest
from airflow.api_fastapi.execution_api.datamodels.taskinstance import TaskInstance as TIDataModel
from airflow.callbacks.callback_requests import DagCallbackRequest, DagRunContext
from airflow.models.asset import AssetEvent, AssetModel
from airflow.models.dagbag import DBDagBag
from airflow.models.dagrun import DagRun
from airflow.models.hitl import HITLDetail
from airflow.models.inbound_wait import InboundWait, WaitStatus
from airflow.models.log import Log
from airflow.models.taskinstance import TaskInstance
from airflow.models.webhook_cancel_request import CancelRequestStatus, WebhookCancelRequest
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.session import create_session
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.webhooks import cancel_application
from airflow.webhooks.cancel_application import (
    CancelledTaskInstance,
    CancelOutcome,
    RunKeySource,
    apply_cancel_request,
    apply_earlier_cancel_requests,
    claim_pending_cancel_request,
    extract_webhook_correlation,
    find_runs_about_key,
    mark_cancel_request_failed,
    notify_cancel_listeners,
)

from tests_common.test_utils.asserts import assert_queries_count
from tests_common.test_utils.db import (
    clear_db_assets,
    clear_db_dags,
    clear_db_logs,
    clear_db_runs,
    clear_db_webhook_cancel_requests,
)
from tests_common.test_utils.webhooks import team_bundle as team_bundle

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

NOW = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)
#: When the events in these tests were recorded: before the requests, which are received at NOW.
EVENT_TIME = NOW - timedelta(minutes=5)
DAG_ID = "cancellable"
KEY = "my-org/web#42"
NOTE = "Cancelled by webhook github delivery delivery-1"
#: The extra of a topic event that a GitHub delivery about pull request 42 recorded.
DELIVERY_EXTRA = {
    "from_webhook": True,
    "webhook": "github",
    "source_type": "github",
    "delivery_id": "delivery-1",
    "inbound_delivery_id": "0199f8a0-0000-7000-8000-000000000001",
    "correlation_key": KEY,
    "payload": None,
}
#: A request whose Dag-level condition matched, naming no conf key.
RUN_TARGETS: dict[str, Any] = {"run": {"correlation_key_confs": []}, "tasks": []}


@pytest.fixture(autouse=True)
def frozen_time(time_machine):
    time_machine.move_to(NOW, tick=False)


@pytest.fixture
def clean_db():
    def clean() -> None:
        clear_db_runs()
        clear_db_webhook_cancel_requests()
        clear_db_dags()
        clear_db_logs()
        clear_db_assets()

    clean()
    yield
    clean()


def build_task_targets(*targets: tuple[str, str, list[str]], run: list[str] | None = None) -> dict[str, Any]:
    """Return the targets of a request for ``(task_id, cancel_state, correlation_key_confs)`` and the run."""
    return {
        "run": None if run is None else {"correlation_key_confs": run},
        "tasks": [
            {"task_id": task_id, "cancel_state": state, "correlation_key_confs": confs}
            for task_id, state, confs in targets
        ],
    }


def declare(dag_maker, *task_ids: str, dag_id: str = DAG_ID, **dag_kwargs: Any) -> None:
    """Add Dag ``dag_id`` with an empty task for each of ``task_ids``."""
    with dag_maker(dag_id, schedule=None, **dag_kwargs):
        for task_id in task_ids or ("task",):
            EmptyOperator(task_id=task_id)


def declare_mapped(dag_maker, **dag_kwargs: Any) -> None:
    """Add Dag DAG_ID with task ``deploy`` and task ``e2e``, mapped over two indexes."""
    with dag_maker(DAG_ID, schedule=None, **dag_kwargs):
        EmptyOperator(task_id="deploy")
        PythonOperator.partial(task_id="e2e", python_callable=list).expand(op_kwargs=[{}, {}])


def create_run(
    dag_maker,
    session: Session,
    run_id: str = "run",
    *,
    state: DagRunState = DagRunState.RUNNING,
    queued_at: datetime = EVENT_TIME,
    states: dict[str, TaskInstanceState | None] | None = None,
    **kwargs: Any,
) -> DagRun:
    """Create a run of the last Dag declared, queued at ``queued_at``, with its task instances in ``states``."""
    dag_run = dag_maker.create_dagrun(run_id=run_id, state=state, logical_date=None, **kwargs)
    dag_run.queued_at = queued_at
    for ti in dag_run.task_instances:
        if states is not None and ti.task_id in states:
            ti.state = states[ti.task_id]
    session.commit()
    return dag_run


def add_webhook_event(
    session: Session, dag_run: DagRun, *, timestamp: datetime = EVENT_TIME, **overrides: Any
) -> AssetEvent:
    """Add an event that a delivery recorded to the events ``dag_run`` consumed; ``overrides`` change it."""
    asset = session.scalar(select(AssetModel).where(AssetModel.name == "topic"))
    if asset is None:
        asset = AssetModel(name="topic")
        session.add(asset)
        session.flush()
    source_dag_id = overrides.pop("source_dag_id", None)
    event = AssetEvent(
        asset_id=asset.id,
        source_dag_id=source_dag_id,
        extra={**DELIVERY_EXTRA, **overrides},
        timestamp=timestamp,
    )
    session.add(event)
    dag_run.consumed_asset_events.append(event)
    session.commit()
    return event


def add_request(
    session: Session,
    targets: dict[str, Any],
    *,
    received_at: datetime = NOW,
    status: CancelRequestStatus = CancelRequestStatus.PENDING,
    dag_id: str = DAG_ID,
    correlation_key: str = KEY,
    **overrides: Any,
) -> WebhookCancelRequest:
    request = WebhookCancelRequest(
        **{
            "inbound_delivery_id": uuid.uuid4(),
            "delivery_id": "delivery-1",
            "webhook": "github",
            **overrides,
        },
        correlation_key=correlation_key,
        received_at=received_at,
        dag_id=dag_id,
        targets=targets,
        status=status,
        retain_until=received_at + timedelta(days=7),
    )
    session.add(request)
    session.commit()
    return request


def apply(
    session: Session,
    request: WebhookCancelRequest,
    *,
    dag_bag: DBDagBag | None = None,
    multi_team: bool = False,
) -> CancelOutcome:
    """Apply ``request`` as the scheduler does, and commit."""
    outcome = apply_cancel_request(
        request, dag_bag=dag_bag or DBDagBag(), multi_team=multi_team, session=session
    )
    session.commit()
    return outcome


def get_states(session: Session, dag_run: DagRun) -> dict[tuple[str, int], str | None]:
    session.expire_all()
    return {
        (ti.task_id, ti.map_index): ti.state
        for ti in session.scalars(select(TaskInstance).where(TaskInstance.run_id == dag_run.run_id))
    }


def get_cancel_logs(session: Session) -> list[Log]:
    return list(session.scalars(select(Log).where(Log.event == "webhook_cancel").order_by(Log.id)))


def build_result(**entries: Any) -> dict[str, Any]:
    return {"dag_runs": [], "task_instances": [], "skipped_runs": [], "error": None, **entries}


def add_wait(
    session: Session, ti_id: uuid.UUID, *, number: int, is_open: bool = True, **overrides: Any
) -> InboundWait:
    match = {"number": number}
    wait = InboundWait(
        ti_id=ti_id,
        webhook="github",
        events=None,
        match=match,
        key_digest=compute_key_digest(match),
        match_digest=compute_match_digest(None, match),
        deadline=NOW + timedelta(hours=1),
        **overrides,
    )
    session.add(wait)
    session.flush()
    # Set after the insert: the ORM inserts a column's default in place of None.
    wait.open = True if is_open else None
    session.commit()
    return wait


class TestExtractWebhookCorrelation:
    def test_returns_webhook_and_key_of_a_delivery_event(self):
        assert extract_webhook_correlation(source_dag_id=None, extra=DELIVERY_EXTRA) == ("github", KEY)

    @pytest.mark.parametrize(
        ("source_dag_id", "extra"),
        [
            pytest.param("producer", DELIVERY_EXTRA, id="task-emitted"),
            pytest.param(None, {**DELIVERY_EXTRA, "from_rest_api": True}, id="rest-api"),
            pytest.param(None, {**DELIVERY_EXTRA, "from_trigger": True}, id="trigger"),
            pytest.param(
                None,
                {key: value for key, value in DELIVERY_EXTRA.items() if key != "from_webhook"},
                id="no-from-webhook",
            ),
            pytest.param(None, {**DELIVERY_EXTRA, "from_webhook": "true"}, id="from-webhook-string"),
            pytest.param(None, {**DELIVERY_EXTRA, "correlation_key": None}, id="no-key"),
            pytest.param(None, {**DELIVERY_EXTRA, "correlation_key": ""}, id="empty-key"),
            pytest.param(None, {**DELIVERY_EXTRA, "correlation_key": 42}, id="key-not-a-string"),
            pytest.param(None, {**DELIVERY_EXTRA, "webhook": None}, id="no-webhook"),
            pytest.param(None, list(DELIVERY_EXTRA), id="extra-not-a-dict"),
        ],
    )
    def test_refuses_events_not_recorded_by_a_delivery(self, source_dag_id, extra):
        assert extract_webhook_correlation(source_dag_id=source_dag_id, extra=extra) is None


@pytest.mark.db_test
@pytest.mark.usefixtures("clean_db")
class TestFindRunsAboutKey:
    @pytest.mark.parametrize(
        ("event", "state", "found"),
        [
            pytest.param({}, DagRunState.RUNNING, True, id="earlier-event"),
            pytest.param({}, DagRunState.QUEUED, True, id="queued-run"),
            pytest.param({"timestamp": NOW}, DagRunState.RUNNING, False, id="event-as-old-as-the-request"),
            pytest.param(
                {"timestamp": NOW + timedelta(seconds=1)}, DagRunState.RUNNING, False, id="later-event"
            ),
            pytest.param({"webhook": "slack"}, DagRunState.RUNNING, False, id="other-webhook"),
            pytest.param({"correlation_key": "my-org/web#57"}, DagRunState.RUNNING, False, id="other-key"),
            pytest.param({"source_dag_id": "producer"}, DagRunState.RUNNING, False, id="task-emitted-event"),
            pytest.param({"from_rest_api": True}, DagRunState.RUNNING, False, id="rest-api-event"),
            pytest.param({}, DagRunState.SUCCESS, False, id="finished-run"),
        ],
    )
    def test_finds_runs_by_an_earlier_delivery_event(self, dag_maker, session, event, state, found):
        declare(dag_maker)
        dag_run = create_run(dag_maker, session, state=state)
        add_webhook_event(session, dag_run, **event)
        request = add_request(session, RUN_TARGETS)

        runs = find_runs_about_key(request, session=session)

        expected = [("run", RunKeySource(by_event=True))] if found else []
        assert [(run.run_id, source) for run, source in runs] == expected

    @pytest.mark.parametrize(
        ("conf", "correlation_key", "targets", "queued_at", "conf_names"),
        [
            pytest.param({"pr": KEY}, KEY, build_task_targets(run=["pr"]), EVENT_TIME, {"pr"}, id="string"),
            pytest.param({"pr": 42}, "42", build_task_targets(run=["pr"]), EVENT_TIME, {"pr"}, id="integer"),
            pytest.param(
                {"pr": KEY},
                KEY,
                build_task_targets(("task", "skipped", ["pr"]), run=[]),
                EVENT_TIME,
                {"pr"},
                id="named-by-a-task-target",
            ),
            pytest.param(
                {"pr": KEY, "ticket": KEY},
                KEY,
                build_task_targets(("task", "skipped", ["ticket"]), run=["pr", "other"]),
                EVENT_TIME,
                {"pr", "ticket"},
                id="named-twice",
            ),
            pytest.param(
                {"pr": True}, "True", build_task_targets(run=["pr"]), EVENT_TIME, None, id="boolean"
            ),
            pytest.param({"pr": None}, "None", build_task_targets(run=["pr"]), EVENT_TIME, None, id="null"),
            pytest.param({"pr": [KEY]}, KEY, build_task_targets(run=["pr"]), EVENT_TIME, None, id="list"),
            pytest.param(
                {"ticket": KEY}, KEY, build_task_targets(run=["pr"]), EVENT_TIME, None, id="other-conf"
            ),
            pytest.param({"pr": KEY}, KEY, RUN_TARGETS, EVENT_TIME, None, id="no-conf-key-named"),
            pytest.param({"pr": KEY}, KEY, build_task_targets(run=["pr"]), NOW, None, id="queued-at-receipt"),
            pytest.param(
                {"pr": KEY},
                KEY,
                build_task_targets(run=["pr"]),
                NOW + timedelta(seconds=1),
                None,
                id="queued-after-the-request",
            ),
        ],
    )
    def test_finds_runs_queued_earlier_by_their_conf(
        self, dag_maker, session, conf, correlation_key, targets, queued_at, conf_names
    ):
        declare(dag_maker)
        create_run(dag_maker, session, conf=conf, queued_at=queued_at)
        request = add_request(session, targets, correlation_key=correlation_key)

        runs = find_runs_about_key(request, session=session)

        expected = [("run", RunKeySource(conf_names=frozenset(conf_names)))] if conf_names else []
        assert [(run.run_id, source) for run, source in runs] == expected

    def test_returns_a_run_about_the_key_both_ways_once_with_its_note(self, dag_maker, session):
        declare(dag_maker)
        dag_run = create_run(dag_maker, session, conf={"pr": KEY})
        add_webhook_event(session, dag_run)
        add_webhook_event(session, dag_run, timestamp=EVENT_TIME + timedelta(seconds=1))
        create_run(dag_maker, session, "unrelated")
        request = add_request(session, build_task_targets(run=["pr"]))

        runs = find_runs_about_key(request, session=session)

        assert [(run.run_id, source) for run, source in runs] == [
            ("run", RunKeySource(by_event=True, conf_names=frozenset({"pr"})))
        ]
        assert "dag_run_note" in runs[0][0].__dict__

    @pytest.mark.parametrize(
        ("by_event", "change", "expected"),
        [
            pytest.param(False, {"queued_at": NOW}, [], id="cleared"),
            pytest.param(
                True, {"queued_at": NOW}, [("run", RunKeySource(by_event=True))], id="cleared-keyed-by-event"
            ),
            pytest.param(True, {"state": DagRunState.SUCCESS}, [], id="finished"),
        ],
    )
    @mock.patch.object(cancel_application, "lock_dag_runs", autospec=True)
    def test_reads_the_runs_again_once_locked(
        self, lock_dag_runs, dag_maker, session, by_event, change, expected
    ):
        declare(dag_maker)
        dag_run = create_run(dag_maker, session, conf={"pr": KEY})
        if by_event:
            add_webhook_event(session, dag_run)
        request = add_request(session, build_task_targets(run=["pr"]))

        def change_while_locking(session: Session, run_keys) -> None:
            session.execute(
                update(DagRun.__table__).where(DagRun.__table__.c.run_id == "run").values(**change)
            )

        lock_dag_runs.side_effect = change_while_locking

        runs = find_runs_about_key(request, session=session)

        lock_dag_runs.assert_called_once_with(session, mock.ANY)
        assert set(lock_dag_runs.call_args.args[1]) == {(DAG_ID, "run")}
        assert [(run.run_id, source) for run, source in runs] == expected


@pytest.mark.db_test
@pytest.mark.usefixtures("clean_db")
class TestApplyCancelRequestRunScope:
    @pytest.mark.parametrize(
        ("state", "expected"),
        [
            pytest.param(TaskInstanceState.RUNNING, TaskInstanceState.FAILED, id="running"),
            pytest.param(TaskInstanceState.DEFERRED, TaskInstanceState.FAILED, id="deferred"),
            pytest.param(
                TaskInstanceState.UP_FOR_RESCHEDULE, TaskInstanceState.FAILED, id="up-for-reschedule"
            ),
            pytest.param(TaskInstanceState.AWAITING_INPUT, TaskInstanceState.FAILED, id="awaiting-input"),
            pytest.param(None, TaskInstanceState.SKIPPED, id="none"),
            pytest.param(TaskInstanceState.SCHEDULED, TaskInstanceState.SKIPPED, id="scheduled"),
            pytest.param(TaskInstanceState.QUEUED, TaskInstanceState.SKIPPED, id="queued"),
            pytest.param(TaskInstanceState.UP_FOR_RETRY, TaskInstanceState.SKIPPED, id="up-for-retry"),
            pytest.param(TaskInstanceState.RESTARTING, TaskInstanceState.SKIPPED, id="restarting"),
            pytest.param(TaskInstanceState.SUCCESS, TaskInstanceState.SUCCESS, id="success"),
            pytest.param(TaskInstanceState.FAILED, TaskInstanceState.FAILED, id="failed"),
            pytest.param(TaskInstanceState.SKIPPED, TaskInstanceState.SKIPPED, id="skipped"),
            pytest.param(
                TaskInstanceState.UPSTREAM_FAILED, TaskInstanceState.UPSTREAM_FAILED, id="upstream-failed"
            ),
            pytest.param(TaskInstanceState.REMOVED, TaskInstanceState.REMOVED, id="removed"),
        ],
    )
    def test_fails_the_run_and_ends_its_unfinished_task_instances(self, dag_maker, session, state, expected):
        declare(dag_maker)
        dag_run = create_run(dag_maker, session, states={"task": state})
        add_webhook_event(session, dag_run)
        request = add_request(session, RUN_TARGETS)

        outcome = apply(session, request)

        assert get_states(session, dag_run) == {("task", -1): expected}
        dag_run = session.scalars(select(DagRun).where(DagRun.run_id == "run")).one()
        assert (dag_run.state, dag_run.end_date) == (DagRunState.FAILED, NOW)
        assert [
            (cancelled.ti.task_id, cancelled.previous_state, cancelled.state)
            for cancelled in outcome.task_instances
        ] == ([("task", state, expected)] if state != expected else [])
        assert (outcome.runs_cancelled, outcome.tis_cancelled) == (1, 0)
        assert [run.run_id for run in outcome.failed_runs] == ["run"]

    def test_leaves_the_finished_map_indexes_of_a_running_mapped_task(self, dag_maker, session):
        declare_mapped(dag_maker)
        dag_run = create_run(dag_maker, session)
        tis = {ti.map_index: ti for ti in dag_run.task_instances if ti.task_id == "e2e"}
        tis[0].state = TaskInstanceState.RUNNING
        tis[1].state = TaskInstanceState.SUCCESS
        session.commit()
        add_webhook_event(session, dag_run)

        apply(session, add_request(session, RUN_TARGETS))

        assert get_states(session, dag_run) == {
            ("deploy", -1): TaskInstanceState.SKIPPED,
            ("e2e", 0): TaskInstanceState.FAILED,
            ("e2e", 1): TaskInstanceState.SUCCESS,
        }

    @pytest.mark.parametrize(
        ("work_state", "work_cancelled_state", "final_state"),
        [
            pytest.param(
                TaskInstanceState.RUNNING, TaskInstanceState.FAILED, DagRunState.FAILED, id="running"
            ),
            pytest.param(
                TaskInstanceState.SCHEDULED, TaskInstanceState.SKIPPED, DagRunState.SUCCESS, id="scheduled"
            ),
        ],
    )
    def test_leaves_a_teardown_to_finish_the_run(
        self, dag_maker, session, work_state, work_cancelled_state, final_state
    ):
        with dag_maker(DAG_ID, schedule=None, on_failure_callback=lambda context: None):
            EmptyOperator(task_id="work") >> EmptyOperator(task_id="teardown").as_teardown()
        dag_run = create_run(dag_maker, session, states={"work": work_state})
        add_webhook_event(session, dag_run)
        request = add_request(session, RUN_TARGETS)

        outcome = apply(session, request)

        assert get_states(session, dag_run) == {("work", -1): work_cancelled_state, ("teardown", -1): None}
        dag_run = session.scalars(select(DagRun).where(DagRun.run_id == "run")).one()
        assert (dag_run.state, dag_run.note) == (DagRunState.RUNNING, NOTE)
        assert (outcome.failed_runs, outcome.callbacks, outcome.runs_cancelled) == ([], [], 1)
        assert len(get_cancel_logs(session)) == 1
        assert request.result == build_result(dag_runs=[{"run_id": "run", "state": "running"}])

        # The teardown ends the run when it finishes, as for a run marked failed.
        dag_run.get_task_instance("teardown", session=session).state = TaskInstanceState.SUCCESS
        session.flush()
        dag_run.dag = dag_maker.serialized_dag
        dag_run.update_state(session=session, execute_callbacks=False)
        assert dag_run.state == final_state

    def test_leaves_tasks_missing_from_the_runs_version(self, dag_maker, session):
        declare(dag_maker, "kept", "gone")
        dag_run = create_run(
            dag_maker, session, states={"kept": TaskInstanceState.RUNNING, "gone": TaskInstanceState.RUNNING}
        )
        add_webhook_event(session, dag_run)
        declare(dag_maker, "kept")

        apply(session, add_request(session, RUN_TARGETS))

        assert get_states(session, dag_run) == {
            ("kept", -1): TaskInstanceState.FAILED,
            ("gone", -1): TaskInstanceState.RUNNING,
        }

    def test_ends_the_open_waits_of_the_task_instances_it_changes(self, dag_maker, session):
        declare(dag_maker, "parked", "running", "done")
        dag_run = create_run(
            dag_maker,
            session,
            states={
                "parked": TaskInstanceState.AWAITING_INPUT,
                "running": TaskInstanceState.RUNNING,
                "done": TaskInstanceState.SUCCESS,
            },
        )
        tis = {ti.task_id: ti for ti in dag_run.task_instances}
        closed_at = NOW - timedelta(minutes=1)
        closed = add_wait(
            session,
            tis["parked"].id,
            number=1,
            is_open=False,
            status=WaitStatus.TIMED_OUT,
            ended_at=closed_at,
        )
        parked = add_wait(session, tis["parked"].id, number=2)
        received = add_wait(session, tis["running"].id, number=3, status=WaitStatus.RECEIVED)
        of_finished = add_wait(session, tis["done"].id, number=4)
        session.add(
            HITLDetail(
                ti_id=tis["parked"].id,
                options=["Approve", "Reject"],
                subject="approve?",
                defaults=["Approve"],
                multiple=False,
                params={},
            )
        )
        session.flush()
        add_webhook_event(session, dag_run)

        apply(session, add_request(session, RUN_TARGETS))

        session.expire_all()
        waits = {
            wait.id: (wait.open, wait.status, wait.ended_at) for wait in session.scalars(select(InboundWait))
        }
        assert waits == {
            closed.id: (None, WaitStatus.TIMED_OUT, closed_at),
            parked.id: (None, WaitStatus.ENDED, NOW),
            received.id: (None, WaitStatus.ENDED, NOW),
            of_finished.id: (True, WaitStatus.REGISTERED, None),
        }
        hitl = session.get_one(HITLDetail, tis["parked"].id)
        assert (hitl.responded_at, hitl.responded_by, hitl.chosen_options, hitl.params_input) == (
            None,
            None,
            None,
            {},
        )

    @pytest.mark.parametrize(
        ("note", "delivery_id", "expected"),
        [
            pytest.param(None, "delivery-1", (NOTE, None), id="no-note"),
            pytest.param(
                None,
                None,
                ("Cancelled by webhook github delivery 0199f8a0-0000-7000-8000-0000000000aa", None),
                id="no-note-nor-sender-id",
            ),
            pytest.param(("Looking into it", "admin"), "delivery-1", ("Looking into it", "admin"), id="note"),
        ],
    )
    def test_writes_a_note_only_when_the_run_has_none(self, dag_maker, session, note, delivery_id, expected):
        declare(dag_maker)
        dag_run = create_run(dag_maker, session)
        if note is not None:
            dag_run.note = note
            session.commit()
        add_webhook_event(session, dag_run)
        request = add_request(
            session,
            RUN_TARGETS,
            delivery_id=delivery_id,
            inbound_delivery_id=uuid.UUID("0199f8a0-0000-7000-8000-0000000000aa"),
        )

        apply(session, request)

        session.expire_all()
        dag_run = session.scalars(select(DagRun).where(DagRun.run_id == "run")).one()
        assert (dag_run.dag_run_note.content, dag_run.dag_run_note.user_id) == expected

    def test_writes_an_audit_log_entry_for_each_cancelled_run(self, dag_maker, session):
        declare(dag_maker)
        for run_id in ("first", "second"):
            add_webhook_event(session, create_run(dag_maker, session, run_id))
        request = add_request(session, RUN_TARGETS)

        apply(session, request)

        logs = get_cancel_logs(session)
        assert [
            (log.dag_id, log.run_id, log.task_id, log.owner, log.owner_display_name, log.team_name)
            for log in logs
        ] == [
            (DAG_ID, "first", None, "webhook:github", "Webhook github", None),
            (DAG_ID, "second", None, "webhook:github", "Webhook github", None),
        ]
        assert {log.extra for log in logs} == {
            json.dumps({"delivery_id": "delivery-1", "request_id": str(request.id), "scope": "run"})
        }

    @pytest.mark.parametrize(
        ("has_run", "expected"),
        [
            pytest.param(True, build_result(dag_runs=[{"run_id": "run", "state": "failed"}]), id="a-run"),
            pytest.param(False, build_result(), id="no-run"),
        ],
    )
    def test_marks_the_request_applied_with_its_result(self, dag_maker, session, has_run, expected):
        declare(dag_maker)
        dag_run = create_run(dag_maker, session)
        if has_run:
            add_webhook_event(session, dag_run)
        request = add_request(session, RUN_TARGETS)

        apply(session, request)

        session.expire_all()
        request = session.get_one(WebhookCancelRequest, request.id)
        assert (request.status, request.applied_at, request.result) == (
            CancelRequestStatus.APPLIED,
            NOW,
            expected,
        )

    def test_adds_to_the_result_that_a_run_created_since_recorded(self, dag_maker, session):
        declare(dag_maker)
        add_webhook_event(session, create_run(dag_maker, session))
        recorded = {"run_id": "created", "state": "failed"}
        request = add_request(session, RUN_TARGETS, result=build_result(dag_runs=[recorded]))

        apply(session, request)

        session.expire_all()
        assert session.get_one(WebhookCancelRequest, request.id).result == build_result(
            dag_runs=[recorded, {"run_id": "run", "state": "failed"}]
        )

    @pytest.mark.parametrize("has_callback", [True, False])
    def test_produces_a_failure_callback_with_reason_cancelled(self, dag_maker, session, has_callback):
        declare(
            dag_maker,
            "first",
            "last",
            "pending",
            **({"on_failure_callback": lambda context: None} if has_callback else {}),
        )
        dag_run = create_run(
            dag_maker,
            session,
            states={"first": TaskInstanceState.RUNNING, "last": TaskInstanceState.DEFERRED},
        )
        tis = {ti.task_id: ti for ti in dag_run.task_instances}
        tis["first"].start_date = NOW - timedelta(minutes=2)
        tis["last"].start_date = NOW - timedelta(minutes=1)
        tis["pending"].start_date = None
        session.commit()
        add_webhook_event(session, dag_run)

        outcome = apply(session, add_request(session, RUN_TARGETS))

        if not has_callback:
            assert outcome.callbacks == []
            return
        dag_run = session.scalars(select(DagRun).where(DagRun.run_id == "run")).one()
        last = dag_run.get_task_instance("last", session=session)
        expected = DagCallbackRequest(
            filepath=dag_maker.serialized_dag.relative_fileloc,
            dag_id=DAG_ID,
            is_failure_callback=True,
            run_id="run",
            msg="cancelled",
            bundle_name="dag_maker",
            bundle_version=None,
            context_from_server=DagRunContext(
                dag_run=dag_run,
                last_ti=TIDataModel.model_validate(last, from_attributes=True).model_copy(
                    update={"region_id": None, "region_index": None}
                ),
            ),
        )
        assert [(run.run_id, callback) for run, callback in outcome.callbacks] == [("run", expected)]

    def test_skips_a_run_whose_dag_version_is_gone(self, dag_maker, session):
        declare(dag_maker)
        dag_run = create_run(dag_maker, session, states={"task": TaskInstanceState.RUNNING})
        add_webhook_event(session, dag_run)
        request = add_request(session, RUN_TARGETS)
        dag_bag = mock.create_autospec(DBDagBag, instance=True)
        dag_bag.get_dag_for_run.return_value = None

        outcome = apply(session, request, dag_bag=dag_bag)

        assert request.result == build_result(skipped_runs=[{"run_id": "run", "reason": "dag_not_found"}])
        assert get_states(session, dag_run) == {("task", -1): TaskInstanceState.RUNNING}
        assert (outcome.runs_cancelled, get_cancel_logs(session)) == (0, [])

    @pytest.mark.parametrize(
        ("multi_team", "expected_state", "expected_result"),
        [
            pytest.param(
                True,
                DagRunState.RUNNING,
                build_result(skipped_runs=[{"run_id": "run", "reason": "dag_has_team"}]),
                id="multi-team",
            ),
            pytest.param(
                False,
                DagRunState.FAILED,
                build_result(dag_runs=[{"run_id": "run", "state": "failed"}]),
                id="single-team",
            ),
        ],
    )
    def test_under_multi_team_leaves_the_runs_of_a_dag_of_a_team(
        self, team_bundle, dag_maker, session, multi_team, expected_state, expected_result
    ):
        declare(dag_maker, bundle_name=team_bundle)
        dag_run = create_run(dag_maker, session)
        add_webhook_event(session, dag_run)
        request = add_request(session, RUN_TARGETS)

        apply(session, request, multi_team=multi_team)

        assert request.result == expected_result
        session.expire_all()
        assert session.scalars(select(DagRun.state)).one() == expected_state


@pytest.mark.db_test
@pytest.mark.usefixtures("clean_db")
class TestApplyCancelRequestTaskScope:
    @pytest.mark.parametrize("cancel_state", [TaskInstanceState.FAILED, TaskInstanceState.SKIPPED])
    @pytest.mark.parametrize("state", [TaskInstanceState.RUNNING, TaskInstanceState.SCHEDULED, None])
    def test_gives_every_task_instance_of_its_tasks_their_cancel_state(
        self, dag_maker, session, cancel_state, state
    ):
        declare_mapped(dag_maker)
        dag_run = create_run(dag_maker, session, states={"deploy": TaskInstanceState.RUNNING, "e2e": state})
        add_webhook_event(session, dag_run)
        request = add_request(session, build_task_targets(("e2e", cancel_state.value, [])))

        outcome = apply(session, request)

        assert get_states(session, dag_run) == {
            ("deploy", -1): TaskInstanceState.RUNNING,
            ("e2e", 0): cancel_state,
            ("e2e", 1): cancel_state,
        }
        assert session.scalars(select(DagRun.state)).one() == DagRunState.RUNNING
        tis = sorted(
            (ti for ti in session.scalars(select(TaskInstance)) if ti.task_id == "e2e"),
            key=lambda ti: ti.map_index,
        )
        assert request.result == build_result(
            task_instances=[
                {
                    "ti_id": str(ti.id),
                    "dag_id": DAG_ID,
                    "run_id": "run",
                    "task_id": "e2e",
                    "map_index": ti.map_index,
                    "region_id": str(ti.region_id),
                    "region_index": ti.map_index,
                    "state": cancel_state.value,
                }
                for ti in tis
            ]
        )
        assert (outcome.runs_cancelled, outcome.tis_cancelled, outcome.failed_runs) == (0, 2, [])
        assert [(c.ti.map_index, c.previous_state, c.state) for c in outcome.task_instances] == [
            (0, state, cancel_state),
            (1, state, cancel_state),
        ]

    def test_writes_an_audit_log_entry_for_each_cancelled_task_instance(self, dag_maker, session):
        declare(dag_maker, "e2e", "deploy")
        dag_run = create_run(dag_maker, session, states={"e2e": TaskInstanceState.RUNNING})
        add_webhook_event(session, dag_run)
        request = add_request(session, build_task_targets(("e2e", "skipped", [])))

        apply(session, request)

        [log] = get_cancel_logs(session)
        e2e = dag_run.get_task_instance("e2e", session=session)
        assert (log.dag_id, log.run_id, log.task_id, log.map_index, log.task_instance_id, log.owner) == (
            DAG_ID,
            "run",
            "e2e",
            -1,
            e2e.id,
            "webhook:github",
        )
        assert (log.owner_display_name, log.team_name, json.loads(log.extra)) == (
            "Webhook github",
            None,
            {"delivery_id": "delivery-1", "request_id": str(request.id), "scope": "task", "state": "skipped"},
        )
        session.expire_all()
        assert session.scalars(select(DagRun)).one().dag_run_note is None

    @pytest.mark.parametrize(
        ("conf", "by_event", "targets", "expected"),
        [
            pytest.param(
                None,
                True,
                build_task_targets(("e2e", "skipped", []), run=[]),
                {("e2e", -1): TaskInstanceState.FAILED, ("deploy", -1): TaskInstanceState.SKIPPED},
                id="the-run-by-its-event",
            ),
            pytest.param(
                {"pr": KEY},
                False,
                build_task_targets(("e2e", "skipped", ["pr"]), run=["pr"]),
                {("e2e", -1): TaskInstanceState.FAILED, ("deploy", -1): TaskInstanceState.SKIPPED},
                id="the-run-by-the-conf-it-names",
            ),
            pytest.param(
                {"pr": KEY},
                False,
                build_task_targets(("e2e", "skipped", ["pr"]), run=[]),
                {("e2e", -1): TaskInstanceState.SKIPPED, ("deploy", -1): None},
                id="the-tasks-by-the-conf-only-they-name",
            ),
            pytest.param(
                {"ticket": KEY},
                False,
                build_task_targets(("e2e", "skipped", ["ticket"]), ("deploy", "failed", ["pr"]), run=["pr"]),
                {("e2e", -1): TaskInstanceState.SKIPPED, ("deploy", -1): None},
                id="the-task-by-the-conf-it-names",
            ),
        ],
    )
    def test_cancels_the_run_if_its_target_applies_and_otherwise_the_tasks_whose_targets_do(
        self, dag_maker, session, conf, by_event, targets, expected
    ):
        declare(dag_maker, "e2e", "deploy")
        dag_run = create_run(dag_maker, session, conf=conf, states={"e2e": TaskInstanceState.RUNNING})
        if by_event:
            add_webhook_event(session, dag_run)

        apply(session, add_request(session, targets))

        assert get_states(session, dag_run) == expected

    @pytest.mark.parametrize(
        "targets",
        [
            pytest.param(build_task_targets(("work", "skipped", [])), id="task"),
            pytest.param(RUN_TARGETS, id="run-held-by-a-teardown"),
        ],
    )
    def test_applying_again_changes_and_records_nothing(self, dag_maker, session, targets):
        with dag_maker(DAG_ID, schedule=None):
            EmptyOperator(task_id="work") >> EmptyOperator(task_id="teardown").as_teardown()
        dag_run = create_run(dag_maker, session, states={"work": TaskInstanceState.RUNNING})
        add_webhook_event(session, dag_run)
        apply(session, add_request(session, targets))
        logs = get_cancel_logs(session)
        again = add_request(session, targets)

        outcome = apply(session, again)

        assert again.result == build_result()
        assert (outcome.runs_cancelled, outcome.tis_cancelled, outcome.task_instances) == (0, 0, [])
        assert get_cancel_logs(session) == logs


@pytest.mark.db_test
@pytest.mark.usefixtures("clean_db")
class TestApplyEarlierCancelRequests:
    @staticmethod
    def apply_to_new_run(
        dag_maker, session: Session, *, events: list[AssetEvent] | None = None, multi_team: bool = False
    ) -> list[CancelOutcome]:
        dag_run = session.scalars(select(DagRun).where(DagRun.run_id == "new")).one()
        outcomes = apply_earlier_cancel_requests(
            dag=dag_maker.serialized_dag,
            dag_run=dag_run,
            events=dag_run.consumed_asset_events if events is None else events,
            multi_team=multi_team,
            session=session,
        )
        session.commit()
        return outcomes

    @pytest.mark.parametrize(
        ("status", "result"),
        [
            pytest.param(CancelRequestStatus.PENDING, None, id="pending"),
            pytest.param(
                CancelRequestStatus.APPLIED,
                build_result(dag_runs=[{"run_id": "older", "state": "failed"}]),
                id="applied",
            ),
        ],
    )
    def test_cancels_a_new_run_whose_event_is_older_than_a_request(self, dag_maker, session, status, result):
        declare(dag_maker, on_failure_callback=lambda context: None)
        dag_run = create_run(dag_maker, session, "new", state=DagRunState.QUEUED)
        add_webhook_event(session, dag_run)
        request = add_request(
            session, RUN_TARGETS, received_at=EVENT_TIME + timedelta(seconds=1), status=status, result=result
        )

        [outcome] = self.apply_to_new_run(dag_maker, session)

        assert get_states(session, dag_run) == {("task", -1): TaskInstanceState.SKIPPED}
        dag_run = session.scalars(select(DagRun)).one()
        assert (dag_run.state, dag_run.note) == (DagRunState.FAILED, NOTE)
        assert [log.run_id for log in get_cancel_logs(session)] == ["new"]
        request = session.get_one(WebhookCancelRequest, request.id)
        earlier = result["dag_runs"] if result else []
        assert (request.status, request.result) == (
            status,
            build_result(dag_runs=[*earlier, {"run_id": "new", "state": "failed"}]),
        )
        assert (outcome.request_id, [run.run_id for run in outcome.failed_runs]) == (request.id, ["new"])
        assert [(run.run_id, callback.msg) for run, callback in outcome.callbacks] == [("new", "cancelled")]

    @pytest.mark.parametrize("later_first", [False, True], ids=["earlier-first", "later-first"])
    def test_compares_a_request_with_the_earliest_delivery_event_of_a_new_run(
        self, dag_maker, session, later_first
    ):
        declare(dag_maker)
        dag_run = create_run(dag_maker, session, "new", state=DagRunState.QUEUED)
        earlier = add_webhook_event(session, dag_run, timestamp=EVENT_TIME)
        later = add_webhook_event(session, dag_run, timestamp=NOW + timedelta(seconds=1))
        # Received between the two events.
        add_request(session, RUN_TARGETS)

        # Passed explicitly, since the order of the run's consumed events is not guaranteed.
        [outcome] = self.apply_to_new_run(
            dag_maker, session, events=[later, earlier] if later_first else [earlier, later]
        )

        assert session.scalars(select(DagRun.state)).one() == DagRunState.FAILED
        assert outcome.runs_cancelled == 1

    def test_gives_the_tasks_of_a_new_run_their_cancel_state(self, dag_maker, session):
        declare(dag_maker, "e2e", "deploy")
        dag_run = create_run(dag_maker, session, "new", state=DagRunState.QUEUED)
        add_webhook_event(session, dag_run)
        add_request(session, build_task_targets(("e2e", "skipped", [])))

        [outcome] = self.apply_to_new_run(dag_maker, session)

        assert get_states(session, dag_run) == {("e2e", -1): TaskInstanceState.SKIPPED, ("deploy", -1): None}
        assert session.scalars(select(DagRun.state)).one() == DagRunState.QUEUED
        assert outcome.tis_cancelled == 1

    @pytest.mark.parametrize(
        "request_kwargs",
        [
            pytest.param({"received_at": EVENT_TIME}, id="as-old-as-the-event"),
            pytest.param({"received_at": EVENT_TIME - timedelta(seconds=1)}, id="older-than-the-event"),
            pytest.param({"status": CancelRequestStatus.FAILED}, id="failed"),
            pytest.param({"webhook": "slack"}, id="other-webhook"),
            pytest.param({"correlation_key": "my-org/web#57"}, id="other-key"),
            pytest.param({"dag_id": "other"}, id="other-dag"),
        ],
    )
    def test_leaves_a_new_run_that_no_request_is_about(self, dag_maker, session, request_kwargs):
        declare(dag_maker, dag_id="other")
        declare(dag_maker)
        dag_run = create_run(dag_maker, session, "new", state=DagRunState.QUEUED)
        add_webhook_event(session, dag_run)
        add_request(session, RUN_TARGETS, **request_kwargs)

        assert self.apply_to_new_run(dag_maker, session) == []

        assert session.scalars(select(DagRun.state)).one() == DagRunState.QUEUED

    def test_reads_no_request_for_a_run_of_no_delivery_event(self, dag_maker, session):
        declare(dag_maker)
        dag_run = create_run(dag_maker, session, "new", state=DagRunState.QUEUED)
        add_webhook_event(session, dag_run, source_dag_id="producer")
        add_request(session, RUN_TARGETS)
        events = list(dag_run.consumed_asset_events)

        with assert_queries_count(0):
            outcomes = apply_earlier_cancel_requests(
                dag=dag_maker.serialized_dag,
                dag_run=dag_run,
                events=events,
                multi_team=False,
                session=session,
            )

        assert outcomes == []

    def test_stops_once_a_request_failed_the_run(self, dag_maker, session):
        declare(dag_maker)
        dag_run = create_run(dag_maker, session, "new", state=DagRunState.QUEUED)
        add_webhook_event(session, dag_run)
        first = add_request(session, RUN_TARGETS, received_at=NOW - timedelta(seconds=2))
        second = add_request(session, RUN_TARGETS)

        outcomes = self.apply_to_new_run(dag_maker, session)

        assert [outcome.request_id for outcome in outcomes] == [first.id]
        assert session.get_one(WebhookCancelRequest, second.id).result is None

    def test_under_multi_team_leaves_a_new_run_of_a_dag_of_a_team(self, team_bundle, dag_maker, session):
        declare(dag_maker, bundle_name=team_bundle)
        dag_run = create_run(dag_maker, session, "new", state=DagRunState.QUEUED)
        add_webhook_event(session, dag_run)
        add_request(session, RUN_TARGETS)

        assert self.apply_to_new_run(dag_maker, session, multi_team=True) == []

        assert session.scalars(select(DagRun.state)).one() == DagRunState.QUEUED

    def test_does_not_wait_to_record_the_run_on_a_request_another_scheduler_holds(self, dag_maker, session):
        if session.bind.dialect.name == "sqlite":
            pytest.skip("SQLite has no row locks")
        declare(dag_maker)
        dag_run = create_run(dag_maker, session, "new", state=DagRunState.QUEUED)
        add_webhook_event(session, dag_run)
        request = add_request(session, RUN_TARGETS)

        with create_session(scoped=False) as locker:
            locker.scalar(
                select(WebhookCancelRequest.id).where(WebhookCancelRequest.id == request.id).with_for_update()
            )
            try:
                [outcome] = self.apply_to_new_run(dag_maker, session)
            finally:
                locker.rollback()

        assert outcome.runs_cancelled == 1
        session.expire_all()
        assert session.scalars(select(DagRun.state)).one() == DagRunState.FAILED
        assert session.get_one(WebhookCancelRequest, request.id).result is None


@pytest.mark.db_test
@pytest.mark.usefixtures("clean_db")
class TestClaimPendingCancelRequest:
    def test_claims_the_pending_request_received_first(self, dag_maker, session):
        declare(dag_maker)
        add_request(
            session, RUN_TARGETS, received_at=NOW - timedelta(minutes=9), status=CancelRequestStatus.APPLIED
        )
        add_request(
            session, RUN_TARGETS, received_at=NOW - timedelta(minutes=9), status=CancelRequestStatus.FAILED
        )
        add_request(session, RUN_TARGETS, received_at=NOW - timedelta(minutes=1))
        first = add_request(session, RUN_TARGETS, received_at=NOW - timedelta(minutes=2), id=uuid.UUID(int=1))
        add_request(session, RUN_TARGETS, received_at=NOW - timedelta(minutes=2), id=uuid.UUID(int=2))

        assert claim_pending_cancel_request(session=session) is first

    def test_returns_none_without_a_pending_request(self, dag_maker, session):
        declare(dag_maker)
        add_request(session, RUN_TARGETS, status=CancelRequestStatus.APPLIED)

        assert claim_pending_cancel_request(session=session) is None


@pytest.mark.db_test
@pytest.mark.usefixtures("clean_db")
class TestMarkCancelRequestFailed:
    def test_marks_it_failed_with_the_error_and_keeps_what_it_recorded(self, dag_maker, session):
        declare(dag_maker)
        recorded = {"run_id": "created", "state": "failed"}
        request = add_request(session, RUN_TARGETS, result=build_result(dag_runs=[recorded]))

        mark_cancel_request_failed(request, error="RuntimeError")
        session.commit()

        session.expire_all()
        request = session.get_one(WebhookCancelRequest, request.id)
        assert (request.status, request.applied_at, request.result) == (
            CancelRequestStatus.FAILED,
            NOW,
            build_result(dag_runs=[recorded], error="RuntimeError"),
        )


class TestNotifyCancelListeners:
    @staticmethod
    def build_outcome() -> tuple[CancelOutcome, list[mock.Mock], DagRun]:
        tis = [mock.create_autospec(TaskInstance, instance=True) for _ in range(3)]
        dag_run = DagRun(dag_id=DAG_ID, run_id="run", state=DagRunState.FAILED)
        outcome = CancelOutcome(
            request_id=uuid.UUID(int=1),
            webhook="github",
            delivery_id="delivery-1",
            dag_id=DAG_ID,
            failed_runs=[dag_run],
            task_instances=[
                CancelledTaskInstance(tis[0], TaskInstanceState.RUNNING, TaskInstanceState.FAILED),
                CancelledTaskInstance(tis[1], TaskInstanceState.DEFERRED, TaskInstanceState.SKIPPED),
                CancelledTaskInstance(tis[2], TaskInstanceState.SCHEDULED, TaskInstanceState.SKIPPED),
            ],
        )
        return outcome, tis, dag_run

    @mock.patch("airflow.models.dagrun.get_listener_manager", autospec=True)
    @mock.patch.object(cancel_application, "get_listener_manager", autospec=True)
    def test_calls_the_listeners_of_the_task_instances_it_stopped_and_the_runs_it_failed(
        self, ti_listener_manager, run_listener_manager
    ):
        outcome, tis, dag_run = self.build_outcome()

        notify_cancel_listeners(outcome)

        hook = ti_listener_manager.return_value.hook
        hook.on_task_instance_failed.assert_called_once_with(
            previous_state=TaskInstanceState.RUNNING, task_instance=tis[0], error=NOTE
        )
        hook.on_task_instance_skipped.assert_called_once_with(
            previous_state=TaskInstanceState.DEFERRED, task_instance=tis[1]
        )
        run_listener_manager.return_value.hook.on_dag_run_failed.assert_called_once_with(
            dag_run=dag_run, msg="cancelled"
        )

    @mock.patch("airflow.models.dagrun.get_listener_manager", autospec=True)
    @mock.patch.object(cancel_application, "get_listener_manager", autospec=True)
    def test_logs_a_listener_that_raises(self, ti_listener_manager, run_listener_manager, caplog):
        outcome, tis, dag_run = self.build_outcome()
        hook = ti_listener_manager.return_value.hook
        hook.on_task_instance_failed.side_effect = RuntimeError("listener failed")

        notify_cancel_listeners(outcome)

        assert {"event": "Error calling a task instance listener", "ti_id": str(tis[0].id)} in caplog
        hook.on_task_instance_skipped.assert_called_once()
        run_listener_manager.return_value.hook.on_dag_run_failed.assert_called_once()
