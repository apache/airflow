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

from typing import TYPE_CHECKING

from sqlalchemy import select

from airflow.models.taskinstance import TaskInstance
from airflow.models.xcom import XCOM_RETURN_KEY, XComModel

from tests_common.test_utils.version_compat import AIRFLOW_V_3_4_PLUS

if TYPE_CHECKING:
    from collections.abc import Collection, Sequence

    from sqlalchemy.orm import Session

    from airflow.serialization.definitions.mappedoperator import Operator


def push_mapped_length(ti: TaskInstance, value: Collection, *, session: Session) -> None:
    """Record ``value`` as ``ti``'s return value, usable as an expansion input."""
    # A range has to be materialised to store it, but a dict must keep its shape: the real
    # push path stores the value unchanged, and expansion then hands a mapped task the
    # ``(key, value)`` tuple rather than the key on its own.
    stored = value if isinstance(value, (list, dict)) else list(value)
    if AIRFLOW_V_3_4_PLUS:
        XComModel.set(
            key=XCOM_RETURN_KEY,
            value=stored,
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index,
            mapped_length=len(value),
            session=session,
        )
        return

    # Before 3.4 the length lived in its own table, so it is imported here rather than at
    # module scope, where it would fail against the version that dropped it.
    from airflow.models.taskmap import TaskMap

    XComModel.set(
        key=XCOM_RETURN_KEY,
        value=stored,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        map_index=ti.map_index,
        session=session,
    )
    session.add(
        TaskMap(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index,
            length=len(value),
            keys=None,
        )
    )
    session.flush()


def expand_mapped_task_instances(
    mapped: Operator,
    run_id: str,
    *,
    session: Session,
) -> tuple[Sequence[TaskInstance], int]:
    # map_index -1 sorts first, so the unmapped TI wins when it is still around; tests that
    # already removed it drive expansion off an existing mapped index instead.
    ti = session.scalars(
        select(TaskInstance)
        .where(
            TaskInstance.dag_id == mapped.dag_id,
            TaskInstance.task_id == mapped.task_id,
            TaskInstance.run_id == run_id,
        )
        .order_by(TaskInstance.map_index)
        .limit(1)
    ).one()
    ti.task = mapped
    return ti.expand_mapped_task(session=session)


def expand_mapped_task(
    mapped: Operator,
    run_id: str,
    upstream_task_id: str,
    length: int,
    session: Session,
):
    upstream_ti = session.scalars(
        select(TaskInstance).where(
            TaskInstance.dag_id == mapped.dag_id,
            TaskInstance.task_id == upstream_task_id,
            TaskInstance.run_id == run_id,
            TaskInstance.map_index == -1,
        )
    ).one()
    push_mapped_length(upstream_ti, list(range(length)), session=session)
    expand_mapped_task_instances(mapped, run_id, session=session)
