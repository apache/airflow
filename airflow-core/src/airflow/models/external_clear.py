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
"""Follow the cross-Dag clear targets that task instances recorded when they ran."""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, NamedTuple

from sqlalchemy import select

from airflow.exceptions import AirflowException, DagNotFound
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XComModel, build_xcom_read_query, xcom_entity

if TYPE_CHECKING:
    from collections.abc import Iterable

    from sqlalchemy.orm import Session

    from airflow.models.dagbag import DBDagBag

EXTERNAL_CLEAR_TARGET_XCOM_KEY = "_external_clear_target"
"""XCom key the Task SDK's task runner records ``ExternalClearTarget.serialize()`` under."""


class ExternalClearDepthExceeded(AirflowException):
    """A clear followed more levels of cross-Dag clear targets than the starting task allows."""


class _ClearTarget(NamedTuple):
    dag_id: str
    task_id: str
    logical_date: datetime
    recursion_depth: int

    @classmethod
    def from_record(cls, data: dict[str, Any]) -> _ClearTarget:
        return cls(
            dag_id=data["dag_id"],
            task_id=data["task_id"],
            logical_date=datetime.fromisoformat(data["logical_date"]),
            recursion_depth=data["recursion_depth"],
        )


def _recorded_targets(ti_ids: Iterable, *, session: Session) -> list[_ClearTarget]:
    read = build_xcom_read_query(
        producer_ids=select(TaskInstance.id).where(TaskInstance.id.in_(ti_ids)),
        key=EXTERNAL_CLEAR_TARGET_XCOM_KEY,
    )
    rows = session.execute(read.with_only_columns(xcom_entity(read).value))
    return [_ClearTarget.from_record(XComModel.deserialize_value(row)) for row in rows]


def find_external_clear_task_instances(
    task_instances: Iterable[TaskInstance],
    *,
    dag_bag: DBDagBag,
    only_failed: bool = False,
    only_running: bool = False,
    session: Session,
) -> list[TaskInstance]:
    """
    Return the task instances in other Dags that clearing ``task_instances`` should also clear.

    A task instance that recorded an :class:`~airflow.sdk.ExternalClearTarget` when it ran pulls in the
    target task and its downstream tasks, in the target Dag's run for that logical date. Those can record
    targets of their own, which are followed in turn. The first level's largest ``recursion_depth`` caps how
    many levels are followed, which stops a cycle of targets.

    :raises ExternalClearDepthExceeded: when the targets go deeper than that cap.
    :raises DagNotFound: when a target's Dag run has no serialized Dag to read its downstream tasks from.
    """
    seen = {ti.id for ti in task_instances}
    frontier = set(seen)
    found: list[TaskInstance] = []
    max_depth: int | None = None
    depth = 0
    while frontier and (targets := _recorded_targets(frontier, session=session)):
        if max_depth is None:
            max_depth = max(target.recursion_depth for target in targets)
        depth += 1
        if depth > max_depth:
            raise ExternalClearDepthExceeded(
                f"Clearing followed more than {max_depth} levels of external clear targets, "
                f"the last into Dag {targets[0].dag_id!r}. There may be a cycle between Dags."
            )
        frontier = set()
        for target in targets:
            target_tis = session.scalars(
                select(TaskInstance)
                .join(TaskInstance.dag_run)
                .where(
                    TaskInstance.dag_id == target.dag_id,
                    TaskInstance.task_id == target.task_id,
                    DagRun.logical_date == target.logical_date,
                )
            )
            for target_ti in target_tis:
                if (dag := dag_bag.get_dag_for_run(target_ti.dag_run, session=session)) is None:
                    raise DagNotFound(f"Could not find Dag {target_ti.dag_id!r}")
                downstream = dag.partial_subset(
                    task_ids=[target_ti.task_id], include_downstream=True, include_upstream=False
                )
                for ti in dag.clear(
                    task_ids=list(downstream.task_dict),
                    run_id=target_ti.run_id,
                    only_failed=only_failed,
                    only_running=only_running,
                    dry_run=True,
                    session=session,
                ):
                    if ti.id not in seen:
                        seen.add(ti.id)
                        found.append(ti)
                        frontier.add(ti.id)
    return found
