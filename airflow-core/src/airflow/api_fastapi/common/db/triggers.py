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
What the API works out for a trigger from what refers to it: its kind, its owner's Dag and its status.

Each is given in Python, for a loaded trigger, and in SQL, to filter and sort on; the two are kept in step.
"""

from __future__ import annotations

import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import and_, case, or_, select
from sqlalchemy.sql.functions import coalesce

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.jobs.job import Job
from airflow.models.callback import Callback
from airflow.models.dagrun import DagRun
from airflow.models.deadline import Deadline
from airflow.models.taskinstance import TaskInstance
from airflow.models.trigger import Trigger

if TYPE_CHECKING:
    from sqlalchemy.sql import ColumnElement


class TriggerKind(str, Enum):
    """What a trigger runs for, worked out from its owner rather than stored."""

    TASK = "task"
    CALLBACK = "callback"
    ASSET_WATCHER = "asset_watcher"


class TriggerStatus(str, Enum):
    """How a trigger is doing, worked out the same way for every kind from its row and its triggerer's heartbeat."""

    # pending: not picked up yet. running: on a live triggerer. orphaned: its triggerer stopped sending
    # heartbeats; another triggerer will take it.
    PENDING = "pending"
    RUNNING = "running"
    ORPHANED = "orphaned"


def get_trigger_kind(trigger: Trigger) -> TriggerKind | None:
    """
    Return what the trigger runs for, or ``None`` once nothing refers to it.

    A trigger nothing refers to (a task that resumed or failed, a finished callback, a removed watcher) is
    deleted on the triggerer's next cleanup. Keep in step with :func:`build_trigger_kind_expression`.
    """
    if trigger.task_instance is not None:
        return TriggerKind.TASK
    if trigger.callback is not None:
        return TriggerKind.CALLBACK
    if trigger.asset_watchers:
        return TriggerKind.ASSET_WATCHER
    return None


def build_trigger_kind_expression() -> ColumnElement[str | None]:
    """Return the SQL for :func:`get_trigger_kind`."""
    return case(
        (Trigger.task_instance.has(), TriggerKind.TASK.value),
        (Trigger.callback.has(), TriggerKind.CALLBACK.value),
        (Trigger.asset_watchers.any(), TriggerKind.ASSET_WATCHER.value),
        else_=None,
    )


def build_has_owner_expression() -> ColumnElement[bool]:
    """Whether anything still refers to the trigger, that is :func:`get_trigger_kind` is not ``None``."""
    return or_(Trigger.task_instance.has(), Trigger.callback.has(), Trigger.asset_watchers.any())


def build_trigger_dag_id_expression() -> ColumnElement[str | None]:
    """
    Return the SQL for the Dag of the task instance or callback the trigger runs for; ``NULL`` for a watcher.

    A callback names its Dag run only through its deadline. Keep in step with ``_build_response`` in the
    triggers routes.
    """
    task_dag_id = (
        select(TaskInstance.dag_id).where(TaskInstance.trigger_id == Trigger.id).limit(1).scalar_subquery()
    )
    callback_dag_id = (
        select(DagRun.dag_id)
        .join(Deadline, Deadline.dagrun_id == DagRun.id)
        .join(Callback, Callback.id == Deadline.callback_id)
        .where(Callback.trigger_id == Trigger.id)
        .limit(1)
        .scalar_subquery()
    )
    return coalesce(task_dag_id, callback_dag_id)


def get_trigger_status(trigger: Trigger) -> TriggerStatus:
    """
    Return how the trigger is doing; needs ``Trigger.triggerer_job`` loaded.

    A triggerer holds its triggers while it has not ended and its heartbeat is recent, the rule
    ``Trigger.assign_unassigned`` uses. Keep in step with :func:`build_trigger_status_expression`.
    """
    if trigger.triggerer_id is None:
        return TriggerStatus.PENDING
    job = trigger.triggerer_job
    if (
        job is not None
        and job.end_date is None
        and job.latest_heartbeat is not None
        and job.latest_heartbeat > _get_heartbeat_cutoff()
    ):
        return TriggerStatus.RUNNING
    return TriggerStatus.ORPHANED


def build_trigger_status_expression() -> ColumnElement[str]:
    """Return the SQL for :func:`get_trigger_status`; needs ``Trigger.triggerer_job`` outer-joined."""
    # Positive conditions only: a NULL from the outer join (job row gone, no heartbeat) is not true, so it
    # falls through to "orphaned" as it does in Python.
    alive = and_(Job.end_date.is_(None), Job.latest_heartbeat > _get_heartbeat_cutoff())
    return case(
        (Trigger.triggerer_id.is_(None), TriggerStatus.PENDING.value),
        (alive, TriggerStatus.RUNNING.value),
        else_=TriggerStatus.ORPHANED.value,
    )


def _get_heartbeat_cutoff() -> datetime.datetime:
    threshold = conf.getfloat("triggerer", "triggerer_health_check_threshold")
    return timezone.utcnow() - datetime.timedelta(seconds=threshold)
