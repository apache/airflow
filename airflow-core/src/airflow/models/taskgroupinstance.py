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
"""Persist retry state for TaskGroups within a DagRun."""

from __future__ import annotations

import logging
from collections.abc import Callable, Iterator, Mapping, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Protocol

from sqlalchemy import Index, Integer
from sqlalchemy.orm import Mapped, mapped_column

from airflow._shared.module_loading import import_string
from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.operator_helpers import determine_kwargs
from airflow.utils.sqlalchemy import UtcDateTime
from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.models.taskinstance import TaskInstance

TASK_GROUP_RETRY_CONDITION_ANY_FAILED = "any_failed"
TASK_GROUP_RETRY_CONDITION_ALL_FAILED = "all_failed"
TaskGroupRetryCondition = Callable[..., bool] | str

log = logging.getLogger(__name__)


class _TaskLike(Protocol):
    @property
    def task_id(self) -> str: ...

    @property
    def is_teardown(self) -> bool: ...


class _TaskGroupLike(Protocol):
    @property
    def group_id(self) -> str | None: ...


class _TaskGroupRetryLike(_TaskGroupLike, Protocol):
    @property
    def is_root(self) -> bool: ...

    @property
    def parent_group(self) -> _TaskGroupRetryLike | None: ...

    @property
    def retries(self) -> int: ...

    @property
    def retry_condition(self) -> TaskGroupRetryCondition | None: ...

    def iter_tasks(self) -> Iterator[_TaskLike]: ...


def iter_task_group_ancestors(task_group: _TaskGroupRetryLike | None) -> Iterator[_TaskGroupRetryLike]:
    """Yield TaskGroup ancestors from inner to outer, excluding the root."""
    group = task_group
    while group is not None and not group.is_root:
        yield group
        group = group.parent_group


def iter_retryable_task_groups(task_group: _TaskGroupRetryLike | None) -> Iterator[_TaskGroupRetryLike]:
    """Yield TaskGroup ancestors that have group-level retries configured."""
    for group in iter_task_group_ancestors(task_group):
        if group.retries and group.group_id:
            yield group


def iter_task_group_ids(task_group: _TaskGroupRetryLike | None) -> Iterator[str]:
    """Yield all ancestor TaskGroup IDs from inner to outer."""
    for group in iter_task_group_ancestors(task_group):
        group_id = group.group_id
        if group_id:
            yield group_id


def iter_retryable_task_group_ids(task_group: _TaskGroupRetryLike | None) -> Iterator[str]:
    """Yield IDs of retryable TaskGroup ancestors from inner to outer."""
    for group in iter_retryable_task_groups(task_group):
        group_id = group.group_id
        if group_id:
            yield group_id


def collect_task_group_tis(
    task_group: _TaskGroupRetryLike,
    tis_by_task_id: Mapping[str, Sequence[TaskInstance]],
) -> list[TaskInstance] | None:
    """Return all TaskInstances for a group, or ``None`` if any group task is missing."""
    group_tis: list[TaskInstance] = []
    for task in task_group.iter_tasks():
        task_tis = tis_by_task_id.get(task.task_id)
        if not task_tis:
            return None
        group_tis.extend(task_tis)
    return group_tis


def should_task_group_retry(
    *,
    task_group: _TaskGroupRetryLike,
    task_instances: Sequence[TaskInstance],
    context: object | None = None,
    ti: TaskInstance | None = None,
) -> bool:
    """Evaluate the configured retry condition for a TaskGroup."""
    retry_condition = task_group.retry_condition

    def any_failed() -> bool:
        return any(task_instance.state in State.failed_states for task_instance in task_instances)

    def all_failed() -> bool:
        if not task_instances:
            return False
        return all(task_instance.state in State.failed_states for task_instance in task_instances)

    if not retry_condition or retry_condition == TASK_GROUP_RETRY_CONDITION_ANY_FAILED:
        return any_failed()

    if isinstance(retry_condition, str):
        normalized = retry_condition.strip()
        normalized_lower = normalized.lower()
        if normalized_lower == TASK_GROUP_RETRY_CONDITION_ANY_FAILED:
            return any_failed()
        if normalized_lower == TASK_GROUP_RETRY_CONDITION_ALL_FAILED:
            return all_failed()
        callable_path = normalized
        if normalized.startswith("<callable ") and normalized.endswith(">"):
            callable_path = normalized[len("<callable ") : -1].strip()
        try:
            retry_condition = import_string(callable_path)
        except Exception:
            log.warning(
                "Failed to import TaskGroup retry_condition %r; falling back to %r.",
                retry_condition,
                TASK_GROUP_RETRY_CONDITION_ANY_FAILED,
                exc_info=True,
            )
            return any_failed()

    if callable(retry_condition):
        kwargs = {
            "task_instances": task_instances,
            "task_group": task_group,
            "task_group_id": task_group.group_id,
            "context": context,
            "ti": ti,
        }
        call_kwargs = dict(determine_kwargs(retry_condition, (), kwargs))
        return bool(retry_condition(**call_kwargs))

    normalized = str(retry_condition).strip().lower()
    if normalized == TASK_GROUP_RETRY_CONDITION_ALL_FAILED:
        return all_failed()
    if normalized == TASK_GROUP_RETRY_CONDITION_ANY_FAILED:
        return any_failed()
    log.warning(
        "Unknown TaskGroup retry_condition %r; falling back to %r.",
        retry_condition,
        TASK_GROUP_RETRY_CONDITION_ANY_FAILED,
    )
    return any_failed()


class TaskGroupInstance(Base):
    """Persist retry state for a TaskGroup within a DagRun."""

    __tablename__ = "task_group_instance"

    dag_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    run_id: Mapped[str] = mapped_column(StringID(), primary_key=True)
    task_group_id: Mapped[str] = mapped_column(StringID(), primary_key=True)

    try_number: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    next_retry_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, nullable=False, default=timezone.utcnow, onupdate=timezone.utcnow
    )

    __table_args__ = (Index("idx_task_group_instance_dag_run", "dag_id", "run_id"),)

    def __repr__(self) -> str:
        return (
            "<TaskGroupInstance "
            f"{self.dag_id}.{self.run_id}.{self.task_group_id} "
            f"try_number={self.try_number} next_retry_at={self.next_retry_at}>"
        )
