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
"""
Apply cancel requests: cancel the runs and tasks that are about a request's correlation key.

The scheduler applies each pending request on its own: it fails the runs of the request's Dag that are
about the key and older than the delivery when a Dag-level condition matched, and otherwise gives the
matched tasks their ``cancel_state``. A run is about the key if it consumed an event that a delivery with
the key recorded, or if its conf holds the key under a conf key that a matched condition names. When the
scheduler creates a run from an event older than a request, it applies the request to that run too.

Locks are taken in this order: the request, the runs, their task instances, then the task instances'
waits, as the scheduler and the Execution API lock runs before task instances. A run created in the same
transaction is not locked, and the requests applied to it are only updated if no other scheduler holds
them, so that run creation never waits for a request. With more than one scheduler, a request committed
after a creating scheduler read the requests, and applied by another scheduler before the new run
commits, is not applied to that run; neither is a request applied while the API transaction of a run
triggered with the key in its conf is still open. Nothing here commits.

.. note:: |experimental|
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any

import structlog
from sqlalchemy import or_, select, tuple_
from sqlalchemy.orm import selectinload

from airflow._shared.timezones import timezone
from airflow.api.common.mark_tasks import RUNNING_STATES
from airflow.listeners.listener import get_listener_manager
from airflow.models.asset import AssetEvent, association_table
from airflow.models.dag import DagModel
from airflow.models.dagrun import DagRun, lock_dag_runs
from airflow.models.log import Log
from airflow.models.task_coordinates import build_task_instance_coordinates
from airflow.models.taskinstance import TaskInstance
from airflow.models.webhook_cancel_request import CancelRequestStatus, WebhookCancelRequest
from airflow.utils.sqlalchemy import with_row_locks
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.webhooks.cancel_requests import CancelScope
from airflow.webhooks.topics import build_dag_has_team_clause
from airflow.webhooks.wait_resumption import end_open_waits

if TYPE_CHECKING:
    from collections.abc import Callable, Collection, Iterable
    from uuid import UUID

    from sqlalchemy.orm import Session

    from airflow.callbacks.callback_requests import DagCallbackRequest
    from airflow.models.dagbag import DBDagBag
    from airflow.serialization.definitions.dag import SerializedDAG

log = structlog.get_logger(__name__)

#: The reason a cancelled run's failure callback and run listeners are given.
CANCEL_REASON = "cancelled"
#: The event of the audit log entries of what a request cancelled.
CANCEL_LOG_EVENT = "webhook_cancel"
#: Why a run about the key was left alone: its Dag version cannot be loaded.
SKIPPED_DAG_NOT_FOUND = "dag_not_found"
#: Why a run about the key was left alone: under multi-team, its Dag has a team, and webhooks have none.
SKIPPED_DAG_HAS_TEAM = "dag_has_team"


@dataclass(frozen=True)
class RunKeySource:
    """
    Why a run is about a request's correlation key.

    :param by_event: The run consumed an event that a delivery with the key recorded before the request's.
    :param conf_names: The conf keys under which the run's conf holds the key, if it was queued before the
        request's delivery was received.
    """

    by_event: bool = False
    conf_names: frozenset[str] = frozenset()

    def is_targeted_by(self, target: Mapping[str, Any]) -> bool:
        """Whether a target of a request applies to the run, through its event or a conf key it names."""
        return self.by_event or not self.conf_names.isdisjoint(target.get("correlation_key_confs") or ())


@dataclass(frozen=True)
class CancelledTaskInstance:
    """A task instance that a request changed, and its state before and after."""

    ti: TaskInstance
    previous_state: TaskInstanceState | None
    state: TaskInstanceState


@dataclass
class CancelOutcome:
    """
    What applying a request changed, for what the scheduler does once it has committed.

    :param failed_runs: The runs the request failed, for their run listeners.
    :param callbacks: The failure callback of each failed run whose Dag has one, to send after commit.
    :param task_instances: Every task instance the request changed, for their task instance listeners.
    :param runs_cancelled: The runs a Dag-level condition cancelled, as ``webhooks.cancels`` counts them.
    :param tis_cancelled: The task instances that task-level conditions cancelled.
    """

    request_id: UUID
    webhook: str
    delivery_id: str
    dag_id: str
    failed_runs: list[DagRun] = field(default_factory=list)
    callbacks: list[tuple[DagRun, DagCallbackRequest]] = field(default_factory=list)
    task_instances: list[CancelledTaskInstance] = field(default_factory=list)
    runs_cancelled: int = 0
    tis_cancelled: int = 0

    @classmethod
    def from_request(cls, request: WebhookCancelRequest) -> CancelOutcome:
        return cls(
            request_id=request.id,
            webhook=request.webhook,
            delivery_id=get_request_delivery_id(request),
            dag_id=request.dag_id,
        )


def get_request_delivery_id(request: WebhookCancelRequest) -> str:
    """Return the id by which a request's note and audit entries name its delivery."""
    return request.delivery_id or str(request.inbound_delivery_id)


def build_cancel_note(webhook: str, delivery_id: str) -> str:
    """Return the note of a run that a request cancelled, which is written only if the run has none."""
    return f"Cancelled by webhook {webhook} delivery {delivery_id}"


def build_empty_result() -> dict[str, Any]:
    """
    Return the result of a request that changed nothing.

    ``dag_runs`` lists ``{"run_id", "state"}`` of each run that a Dag-level condition cancelled, which a
    teardown can keep running; ``task_instances`` lists the coordinates and new ``state`` of each task
    instance that a task-level condition cancelled; ``skipped_runs`` lists ``{"run_id", "reason"}`` of each
    run about the key that was left alone; ``error`` names the exception that failed the request.
    """
    return {"dag_runs": [], "task_instances": [], "skipped_runs": [], "error": None}


def merge_results(existing: Mapping[str, Any] | None, new: Mapping[str, Any]) -> dict[str, Any]:
    """Return ``existing`` with the entries of ``new`` added, and with the error of ``new`` if it names one."""
    merged = build_empty_result()
    for result in (existing or {}, new):
        for key in ("dag_runs", "task_instances", "skipped_runs"):
            merged[key].extend(result.get(key) or ())
        if result.get("error") is not None:
            merged["error"] = result["error"]
    return merged


def extract_webhook_correlation(*, source_dag_id: str | None, extra: Any) -> tuple[str, str] | None:
    """
    Return the webhook and correlation key of an asset event that a delivery recorded.

    Anyone who may create asset events can set the same extra, so an event counts only if it is not a task's
    outlet event and neither the REST API nor a trigger created it, as each of those marks its events.

    :return: ``(webhook, correlation_key)``, or None if the event does not say both, or another source could
        have created it.
    """
    if source_dag_id is not None or not isinstance(extra, Mapping):
        return None
    if extra.get("from_webhook") is not True or "from_rest_api" in extra or "from_trigger" in extra:
        return None
    webhook = extra.get("webhook")
    correlation_key = extra.get("correlation_key")
    if isinstance(webhook, str) and webhook and isinstance(correlation_key, str) and correlation_key:
        return webhook, correlation_key
    return None


def _is_conf_about_key(conf: Any, name: str, correlation_key: str) -> bool:
    # A conf value of None or True must never match a key such as "None" or "True".
    if not isinstance(conf, Mapping):
        return False
    value = conf.get(name)
    return isinstance(value, (str, int)) and not isinstance(value, bool) and str(value) == correlation_key


def _find_conf_names(targets: Mapping[str, Any]) -> set[str]:
    run_target = targets.get("run")
    names = set(run_target.get("correlation_key_confs") or ()) if run_target is not None else set()
    for task_target in targets.get("tasks") or ():
        names.update(task_target.get("correlation_key_confs") or ())
    return names


def find_runs_about_key(
    request: WebhookCancelRequest, *, session: Session
) -> list[tuple[DagRun, RunKeySource]]:
    """
    Find, lock and return the unfinished runs of a request's Dag that are about its key and predate it.

    A run is about the key through an event older than the request that a delivery with the key recorded,
    or, if it was queued before the request, through a conf key that a target of the request names. A run
    cleared since is queued again, so a request older than the clear leaves it alone. The runs are locked
    in the order every path that changes task instances under their runs locks them, and those that
    finished meanwhile are left out.

    :return: Each run, by run id, with why it is about the key, its note loaded.
    """
    sources: dict[tuple[str, str], RunKeySource] = {}
    for row in session.execute(
        select(DagRun.dag_id, DagRun.run_id, AssetEvent.source_dag_id, AssetEvent.extra)
        .join(association_table, association_table.c.dag_run_id == DagRun.id)
        .join(AssetEvent, AssetEvent.id == association_table.c.event_id)
        .where(
            DagRun.dag_id == request.dag_id,
            DagRun.state.in_(State.unfinished_dr_states),
            AssetEvent.source_dag_id.is_(None),
            AssetEvent.timestamp < request.received_at,
        )
    ):
        correlation = extract_webhook_correlation(source_dag_id=row.source_dag_id, extra=row.extra)
        if correlation == (request.webhook, request.correlation_key):
            sources[(row.dag_id, row.run_id)] = RunKeySource(by_event=True)
    if names := _find_conf_names(request.targets):
        for row in session.execute(
            select(DagRun.dag_id, DagRun.run_id, DagRun.conf).where(
                DagRun.dag_id == request.dag_id,
                DagRun.state.in_(State.unfinished_dr_states),
                DagRun.queued_at < request.received_at,
            )
        ):
            conf_names = frozenset(
                name for name in names if _is_conf_about_key(row.conf, name, request.correlation_key)
            )
            if conf_names:
                key = (row.dag_id, row.run_id)
                sources[key] = RunKeySource(by_event=key in sources, conf_names=conf_names)
    if not sources:
        return []
    lock_dag_runs(session, sources)
    runs: list[tuple[DagRun, RunKeySource]] = []
    for dag_run in session.scalars(
        select(DagRun)
        .where(
            tuple_(DagRun.dag_id, DagRun.run_id).in_(sorted(sources)),
            DagRun.state.in_(State.unfinished_dr_states),
        )
        .order_by(DagRun.run_id)
        .options(selectinload(DagRun.dag_run_note))
        .execution_options(populate_existing=True)
    ):
        source = sources[(dag_run.dag_id, dag_run.run_id)]
        if source.conf_names and not (dag_run.queued_at and dag_run.queued_at < request.received_at):
            # Cleared after it was read: the request is older than its queueing now.
            if not source.by_event:
                continue
            source = RunKeySource(by_event=True)
        runs.append((dag_run, source))
    return runs


def _cancel_task_instances(
    dag_run: DagRun,
    dag: SerializedDAG,
    *,
    task_ids: Collection[str] | None,
    get_state: Callable[[TaskInstance], TaskInstanceState],
    session: Session,
) -> tuple[list[CancelledTaskInstance], bool]:
    """
    Lock and change the unfinished task instances of a run, and end the open waits of those changed.

    Tasks missing from the run's Dag version are left alone, and so are teardowns, which hold the run.

    :param task_ids: The tasks to change, or None for all.
    :param get_state: The state to give a task instance, from its current state.
    :return: The task instances changed, and whether an unfinished teardown holds the run.
    """
    query = (
        select(TaskInstance)
        .where(
            TaskInstance.dag_id == dag_run.dag_id,
            TaskInstance.run_id == dag_run.run_id,
            or_(TaskInstance.state.is_(None), TaskInstance.state.in_(State.unfinished)),
        )
        .order_by(TaskInstance.id)
        .execution_options(populate_existing=True)
    )
    if task_ids is not None:
        query = query.where(TaskInstance.task_id.in_(task_ids))
    changed: list[CancelledTaskInstance] = []
    held_by_teardown = False
    for ti in session.scalars(with_row_locks(query, of=TaskInstance, session=session)).all():
        task = dag.task_dict.get(ti.task_id)
        if task is None:
            continue
        if task.is_teardown:
            held_by_teardown = True
            continue
        previous_state = ti.state
        state = get_state(ti)
        if ti.set_state(state, session=session):
            changed.append(CancelledTaskInstance(ti, previous_state, state))
    end_open_waits([cancelled.ti.id for cancelled in changed], session=session)
    return changed, held_by_teardown


def _get_run_scope_state(ti: TaskInstance) -> TaskInstanceState:
    return TaskInstanceState.FAILED if ti.state in RUNNING_STATES else TaskInstanceState.SKIPPED


def _produce_failure_callback(
    dag_run: DagRun,
    dag: SerializedDAG,
    killed: Iterable[CancelledTaskInstance],
    *,
    session: Session,
) -> DagCallbackRequest | None:
    # As for a run that timed out, the context loads the run's asset events, so they are loaded at once.
    session.flush()
    dag_run = session.scalars(
        select(DagRun)
        .where(DagRun.id == dag_run.id)
        .options(
            selectinload(DagRun.consumed_asset_events).selectinload(AssetEvent.asset),
            selectinload(DagRun.consumed_asset_events).selectinload(AssetEvent.source_aliases),
        )
    ).one()
    relevant_ti = max(
        (cancelled.ti for cancelled in killed),
        key=lambda ti: ti.start_date or timezone.make_aware(datetime.min),
        default=None,
    )
    return dag_run.produce_dag_callback(
        dag=dag,
        success=False,
        relevant_ti=relevant_ti,
        reason=CANCEL_REASON,
        execute=False,
        session=session,
    )


def _build_audit_log(
    outcome: CancelOutcome, scope: CancelScope, *, extra: Mapping[str, Any] | None = None, **kwargs: Any
) -> Log:
    return Log(
        event=CANCEL_LOG_EVENT,
        owner=f"webhook:{outcome.webhook}",
        owner_display_name=f"Webhook {outcome.webhook}",
        extra=json.dumps(
            {
                "delivery_id": outcome.delivery_id,
                "request_id": str(outcome.request_id),
                "scope": scope.value,
                **(extra or {}),
            }
        ),
        team_name=None,
        **kwargs,
    )


def _cancel_run(
    dag_run: DagRun,
    dag: SerializedDAG,
    *,
    outcome: CancelOutcome,
    result: dict[str, Any],
    session: Session,
) -> None:
    changed, held_by_teardown = _cancel_task_instances(
        dag_run, dag, task_ids=None, get_state=_get_run_scope_state, session=session
    )
    if held_by_teardown and not changed:
        return
    # As when a run is marked failed: a teardown left to finish ends the run when it does.
    if not held_by_teardown:
        dag_run.set_state(DagRunState.FAILED)
    outcome.runs_cancelled += 1
    outcome.task_instances.extend(changed)
    if dag_run.dag_run_note is None:
        dag_run.note = (build_cancel_note(outcome.webhook, outcome.delivery_id), None)
    session.add(_build_audit_log(outcome, CancelScope.RUN, dag_id=dag_run.dag_id, run_id=dag_run.run_id))
    result["dag_runs"].append({"run_id": dag_run.run_id, "state": str(dag_run.state)})
    if held_by_teardown:
        return
    outcome.failed_runs.append(dag_run)
    if dag.has_on_failure_callback:
        killed = [cancelled for cancelled in changed if cancelled.previous_state in RUNNING_STATES]
        if callback := _produce_failure_callback(dag_run, dag, killed, session=session):
            outcome.callbacks.append((dag_run, callback))


def _cancel_tasks(
    dag_run: DagRun,
    dag: SerializedDAG,
    task_targets: Iterable[Mapping[str, Any]],
    *,
    outcome: CancelOutcome,
    result: dict[str, Any],
    session: Session,
) -> None:
    states = {target["task_id"]: TaskInstanceState(target["cancel_state"]) for target in task_targets}
    if not states:
        return
    changed, _ = _cancel_task_instances(
        dag_run, dag, task_ids=states, get_state=lambda ti: states[ti.task_id], session=session
    )
    for cancelled in changed:
        session.add(
            _build_audit_log(
                outcome, CancelScope.TASK, extra={"state": cancelled.state.value}, task_instance=cancelled.ti
            )
        )
        result["task_instances"].append(
            {**build_task_instance_coordinates(cancelled.ti, session=session), "state": cancelled.state.value}
        )
    outcome.task_instances.extend(changed)
    outcome.tis_cancelled += len(changed)


def _apply_targets_to_run(
    request: WebhookCancelRequest,
    dag_run: DagRun,
    dag: SerializedDAG,
    source: RunKeySource,
    *,
    outcome: CancelOutcome,
    result: dict[str, Any],
    session: Session,
) -> None:
    """
    Cancel the run if the request's Dag-level target applies to it, and otherwise the tasks whose targets do.

    Only the runs and task instances that change are recorded, so applying a request to a run again records
    nothing.
    """
    run_target = request.targets.get("run")
    if run_target is not None and source.is_targeted_by(run_target):
        _cancel_run(dag_run, dag, outcome=outcome, result=result, session=session)
        return
    _cancel_tasks(
        dag_run,
        dag,
        [target for target in request.targets.get("tasks") or () if source.is_targeted_by(target)],
        outcome=outcome,
        result=result,
        session=session,
    )


def _has_team(dag_id: str, *, session: Session) -> bool:
    return (
        session.scalar(select(DagModel.dag_id).where(DagModel.dag_id == dag_id, build_dag_has_team_clause()))
        is not None
    )


def claim_pending_cancel_request(*, session: Session) -> WebhookCancelRequest | None:
    """
    Lock and return the pending request received first that no other scheduler holds, or None.

    One request is claimed per transaction: a commit releases every lock the transaction holds, so the
    requests of a batch locked together could be claimed again by another scheduler after the first commit.
    """
    query = (
        select(WebhookCancelRequest)
        .where(WebhookCancelRequest.status == CancelRequestStatus.PENDING)
        .order_by(WebhookCancelRequest.received_at, WebhookCancelRequest.id)
        .limit(1)
    )
    return session.scalar(
        with_row_locks(query, of=WebhookCancelRequest, session=session, skip_locked=True, key_share=False)
    )


def apply_cancel_request(
    request: WebhookCancelRequest, *, dag_bag: DBDagBag, multi_team: bool, session: Session
) -> CancelOutcome:
    """
    Apply a claimed request to the runs about its key, and mark it ``applied`` with what it changed.

    Each run is cancelled with its own Dag version, so a target task missing from that version is left
    alone. A run cancelled by a Dag-level condition is failed, with its running task instances, and its
    others skipped, unless a teardown still has to run; it gets the note naming the delivery if it has no
    note. Task-level conditions give their tasks' task instances the condition's ``cancel_state``. Under
    multi-team, the runs of a Dag with a team are left alone.

    :param multi_team: Whether ``[core] multi_team`` is on.
    """
    outcome = CancelOutcome.from_request(request)
    result = build_empty_result()
    dag_has_team = multi_team and _has_team(request.dag_id, session=session)
    for dag_run, source in find_runs_about_key(request, session=session):
        if dag_has_team:
            result["skipped_runs"].append({"run_id": dag_run.run_id, "reason": SKIPPED_DAG_HAS_TEAM})
            continue
        if (dag := dag_bag.get_dag_for_run(dag_run=dag_run, session=session)) is None:
            log.warning(
                "The Dag version of a run that a webhook cancel request is about was not found",
                request_id=str(request.id),
                dag_id=dag_run.dag_id,
                run_id=dag_run.run_id,
            )
            result["skipped_runs"].append({"run_id": dag_run.run_id, "reason": SKIPPED_DAG_NOT_FOUND})
            continue
        dag_run.dag = dag
        _apply_targets_to_run(request, dag_run, dag, source, outcome=outcome, result=result, session=session)
    request.status = CancelRequestStatus.APPLIED
    request.applied_at = timezone.utcnow()
    # The result is replaced rather than changed in place, which the JSON column would not notice.
    request.result = merge_results(request.result, result)
    return outcome


def mark_cancel_request_failed(request: WebhookCancelRequest, *, error: str) -> None:
    """Mark a request that could not be applied ``failed``, so that it leaves the queue, naming the error."""
    request.status = CancelRequestStatus.FAILED
    request.applied_at = timezone.utcnow()
    request.result = merge_results(request.result, {"error": error})


def apply_earlier_cancel_requests(
    *,
    dag: SerializedDAG,
    dag_run: DagRun,
    events: Iterable[AssetEvent],
    multi_team: bool,
    session: Session,
) -> list[CancelOutcome]:
    """
    Apply to a run just created the requests received after the delivery events it consumed.

    The scheduler holds an asset-triggered run back while its Dag is at ``max_active_runs``, and creates an
    asset-gated run only when its schedule is due, so a request about its key can be applied before the run
    exists. Pending and applied requests apply; a failed one does not. A request's result gets the run's
    entries only if no other scheduler is applying it, since creating a run must not wait for a request: a
    scheduler applying it holds its row and may wait for the Dag's row, which the creating scheduler holds.

    :param dag: The Dag version the run was created with.
    :param events: The asset events the run consumed.
    :param multi_team: Whether ``[core] multi_team`` is on; a Dag with a team then gets no cancel.
    :return: What each request changed.
    """
    earliest: dict[tuple[str, str], datetime] = {}
    for event in events:
        correlation = extract_webhook_correlation(source_dag_id=event.source_dag_id, extra=event.extra)
        if correlation is not None and (
            correlation not in earliest or event.timestamp < earliest[correlation]
        ):
            earliest[correlation] = event.timestamp
    if not earliest:
        return []
    requests = [
        request
        for request in session.scalars(
            select(WebhookCancelRequest)
            .where(
                WebhookCancelRequest.dag_id == dag.dag_id,
                WebhookCancelRequest.correlation_key.in_(sorted({key for _, key in earliest})),
                WebhookCancelRequest.status.in_((CancelRequestStatus.PENDING, CancelRequestStatus.APPLIED)),
            )
            .order_by(WebhookCancelRequest.received_at, WebhookCancelRequest.id)
        )
        if (timestamp := earliest.get((request.webhook, request.correlation_key))) is not None
        and request.received_at > timestamp
    ]
    if not requests or (multi_team and _has_team(dag.dag_id, session=session)):
        return []
    # The run's task instances are read again under their locks.
    session.flush()
    dag_run.dag = dag
    source = RunKeySource(by_event=True)
    outcomes: list[CancelOutcome] = []
    results: dict[UUID, dict[str, Any]] = {}
    for request in requests:
        if dag_run.state == DagRunState.FAILED:
            break
        outcome = CancelOutcome.from_request(request)
        result = build_empty_result()
        _apply_targets_to_run(request, dag_run, dag, source, outcome=outcome, result=result, session=session)
        if outcome.runs_cancelled or outcome.tis_cancelled:
            outcomes.append(outcome)
            results[request.id] = result
    if results:
        for request in session.scalars(
            with_row_locks(
                select(WebhookCancelRequest)
                .where(WebhookCancelRequest.id.in_(list(results)))
                .execution_options(populate_existing=True),
                of=WebhookCancelRequest,
                session=session,
                skip_locked=True,
                key_share=False,
            )
        ):
            request.result = merge_results(request.result, results[request.id])
    return outcomes


def notify_cancel_listeners(outcome: CancelOutcome) -> None:
    """
    Call the listeners of the task instances a request stopped, and of the runs it failed, after commit.

    As when a run is marked failed, task instances that had not started get no listener call, and a
    listener that raises is logged.
    """
    error = build_cancel_note(outcome.webhook, outcome.delivery_id)
    for cancelled in outcome.task_instances:
        if cancelled.previous_state not in RUNNING_STATES:
            continue
        try:
            if cancelled.state == TaskInstanceState.FAILED:
                get_listener_manager().hook.on_task_instance_failed(
                    previous_state=cancelled.previous_state, task_instance=cancelled.ti, error=error
                )
            else:
                get_listener_manager().hook.on_task_instance_skipped(
                    previous_state=cancelled.previous_state, task_instance=cancelled.ti
                )
        except Exception:
            log.exception("Error calling a task instance listener", ti_id=str(cancelled.ti.id))
    for dag_run in outcome.failed_runs:
        dag_run.notify_dagrun_state_changed(msg=CANCEL_REASON)
