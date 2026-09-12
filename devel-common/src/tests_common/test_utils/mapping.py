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

if TYPE_CHECKING:
    from collections.abc import Collection, Sequence

    from sqlalchemy.orm import Session

    from airflow.serialization.definitions.mappedoperator import Operator


def push_mapped_length(ti: TaskInstance, value: Collection, *, session: Session) -> None:
    """Record ``value`` as ``ti``'s return value, usable as an expansion input."""
    XComModel.set(
        key=XCOM_RETURN_KEY,
        value=list(value),
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=ti.run_id,
        map_index=ti.map_index,
        mapped_length=len(value),
        session=session,
    )


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
    ).first()
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
    push_mapped_length(upstream_ti, range(length), session=session)
    expand_mapped_task_instances(mapped, run_id, session=session)
