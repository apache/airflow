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
Cancel requests: what a delivery asks the scheduler to cancel, for each Dag whose ``cancel_on`` it matches.

A delivery with a correlation key records one request per Dag with a matching condition on its webhook, in
its own transaction, and looks for no run: the scheduler applies the request to the runs about the key.
Nothing here loads a Dag or writes a run or a task instance.

.. note:: |experimental|
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, TypedDict

import uuid6
from sqlalchemy import select

from airflow._shared.webhook_naming.matching import evaluate
from airflow.configuration import conf
from airflow.models.dag import DagModel
from airflow.models.dag_cancel_condition import DAG_LEVEL_TASK_ID, DagCancelCondition
from airflow.models.webhook_cancel_request import WebhookCancelRequest
from airflow.webhooks.topics import build_dag_has_team_clause
from airflow.webhooks.wait_registration import EVENT_ATTRIBUTE

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping
    from datetime import datetime
    from uuid import UUID

    from sqlalchemy import Row
    from sqlalchemy.orm import Session

    from airflow.webhooks.base import WebhookDelivery


class CancelScope(str, Enum):
    """What a cancel request asks to cancel, as ``actions["cancel_requests"]`` lists it."""

    #: A Dag-level condition matched, so the request can fail whole runs.
    RUN = "run"
    #: Only task-level conditions matched, so the request ends only those tasks.
    TASK = "task"

    def __str__(self) -> str:
        return self.value


class RecordedCancelRequest(TypedDict):
    """A request a delivery recorded, as listed in ``actions["cancel_requests"]`` of its record."""

    #: The id of the WebhookCancelRequest row.
    request_id: str
    dag_id: str
    scope: CancelScope


def build_cancel_request_action(
    request_id: UUID, dag_id: str, targets: Mapping[str, Any]
) -> RecordedCancelRequest:
    """Return what ``actions["cancel_requests"]`` lists of a request with ``targets``."""
    scope = CancelScope.RUN if targets.get("run") is not None else CancelScope.TASK
    return {"request_id": str(request_id), "dag_id": dag_id, "scope": scope}


@dataclass(frozen=True)
class CancelRequests:
    """
    The cancel requests of a delivery.

    :param kept: The requests that an earlier copy of the delivery recorded, as its actions list them.
    :param new: A request for each other Dag whose ``cancel_on`` the delivery matches. They are not in the
        session: the caller adds them once the delivery's record is written.
    """

    kept: list[RecordedCancelRequest] = field(default_factory=list)
    new: list[WebhookCancelRequest] = field(default_factory=list)

    @property
    def new_actions(self) -> list[RecordedCancelRequest]:
        """What ``actions["cancel_requests"]`` lists of the new requests."""
        return [
            build_cancel_request_action(request.id, request.dag_id, request.targets) for request in self.new
        ]

    @property
    def actions(self) -> list[RecordedCancelRequest]:
        """What ``actions["cancel_requests"]`` lists: the kept requests, then the new ones."""
        return [*self.kept, *self.new_actions]


def find_matching_cancel_conditions(
    webhook: str, attributes: Mapping[str, Any] | None, *, multi_team: bool, session: Session
) -> list[Row[Any]]:
    """
    Return the ``cancel_on`` conditions on ``webhook`` that a delivery with ``attributes`` matches.

    Each distinct events and match is evaluated once, as a condition that a Dag's ``default_args`` gives
    is stored for each of its tasks. Stale, paused and draining Dags keep their conditions, since a cancel
    only reduces work. Under multi-team, the conditions of Dags of a team are left out: webhooks have no
    team, and cancel nothing of a team's Dags.

    :return: The conditions' ``dag_id``, ``task_id``, ``events``, ``match``, ``correlation_key_conf`` and
        ``cancel_state``, in Dag and task order.
    """
    query = select(
        DagCancelCondition.dag_id,
        DagCancelCondition.task_id,
        DagCancelCondition.events,
        DagCancelCondition.match,
        DagCancelCondition.correlation_key_conf,
        DagCancelCondition.cancel_state,
    ).where(DagCancelCondition.webhook == webhook)
    if multi_team:
        query = query.join(DagModel, DagModel.dag_id == DagCancelCondition.dag_id).where(
            ~build_dag_has_team_clause()
        )
    attributes = attributes or {}
    event = attributes.get(EVENT_ATTRIBUTE)
    matched: dict[tuple[str, str], bool] = {}

    def is_matched(condition: Row[Any]) -> bool:
        key = (json.dumps(condition.events), json.dumps(condition.match, sort_keys=True))
        if key not in matched:
            matched[key] = evaluate(
                events=condition.events, match=condition.match, event=event, attributes=attributes
            ).matched
        return matched[key]

    query = query.order_by(
        DagCancelCondition.dag_id, DagCancelCondition.task_id, DagCancelCondition.condition_digest
    )
    return [condition for condition in session.execute(query) if is_matched(condition)]


def build_targets(conditions: Iterable[Row[Any]]) -> dict[str, Any]:
    """
    Return the targets of a request for the conditions of one Dag that a delivery matches.

    The run is a target if a Dag-level condition matched, and so is each task whose condition matched. Each
    target lists the conf keys that its matched conditions name, sorted and without repeats, through which
    it applies to a run triggered with a conf. The scheduler decides between the run and the tasks for each
    run, as a Dag-level condition need not apply to every run that a task's condition applies to.

    :return: ``{"run": {"correlation_key_confs": [...]} | None, "tasks": [{"task_id", "cancel_state",
        "correlation_key_confs"}]}``, the tasks sorted by id.
    """
    run_confs: set[str] | None = None
    task_states: dict[str, str | None] = {}
    task_confs: dict[str, set[str]] = defaultdict(set)
    for condition in conditions:
        if condition.task_id == DAG_LEVEL_TASK_ID:
            if run_confs is None:
                run_confs = set()
            confs = run_confs
        else:
            task_states.setdefault(condition.task_id, condition.cancel_state)
            confs = task_confs[condition.task_id]
        if condition.correlation_key_conf is not None:
            confs.add(condition.correlation_key_conf)
    return {
        "run": None if run_confs is None else {"correlation_key_confs": sorted(run_confs)},
        "tasks": [
            {
                "task_id": task_id,
                "cancel_state": cancel_state,
                "correlation_key_confs": sorted(task_confs[task_id]),
            }
            for task_id, cancel_state in sorted(task_states.items())
        ],
    }


def build_cancel_requests(
    webhook: str,
    delivery: WebhookDelivery,
    *,
    inbound_delivery_id: UUID,
    received_at: datetime,
    retain_until: datetime,
    reprocessed: bool,
    session: Session,
) -> CancelRequests:
    """
    Build a request for each Dag with a ``cancel_on`` condition on ``webhook`` that a delivery matches.

    Only a delivery with a correlation key requests a cancel, since a request applies to the runs about
    its key; one without reads nothing. A delivery processed again keeps the requests that an earlier copy
    recorded, and gets a new one only for a Dag that has none, such as one that declared ``cancel_on``
    since, so that it never requests twice for a Dag.

    :param inbound_delivery_id: The id of the delivery's record.
    :param received_at: When the delivery was first received.
    :param retain_until: When the delivery's record may be removed, and its requests with it.
    :param reprocessed: Whether the delivery repeats a recorded one, which may have recorded requests.
    """
    if delivery.correlation_key is None:
        return CancelRequests()
    kept: list[RecordedCancelRequest] = []
    if reprocessed:
        kept = [
            build_cancel_request_action(request.id, request.dag_id, request.targets)
            for request in session.execute(
                select(WebhookCancelRequest.id, WebhookCancelRequest.dag_id, WebhookCancelRequest.targets)
                .where(WebhookCancelRequest.inbound_delivery_id == inbound_delivery_id)
                .order_by(WebhookCancelRequest.id)
            )
        ]
    kept_dag_ids = {request["dag_id"] for request in kept}
    conditions_by_dag: dict[str, list[Row[Any]]] = defaultdict(list)
    for condition in find_matching_cancel_conditions(
        webhook, delivery.attributes, multi_team=conf.getboolean("core", "multi_team"), session=session
    ):
        if condition.dag_id not in kept_dag_ids:
            conditions_by_dag[condition.dag_id].append(condition)
    new = [
        WebhookCancelRequest(
            # Chosen now, so that the delivery's actions can name the request before it is inserted.
            id=uuid6.uuid7(),
            inbound_delivery_id=inbound_delivery_id,
            delivery_id=delivery.delivery_id,
            webhook=webhook,
            correlation_key=delivery.correlation_key,
            received_at=received_at,
            dag_id=dag_id,
            targets=build_targets(conditions),
            retain_until=retain_until,
        )
        for dag_id, conditions in conditions_by_dag.items()
    ]
    return CancelRequests(kept=kept, new=new)
