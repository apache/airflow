#
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
"""Table to store information about mapped task instances (AIP-42)."""

from __future__ import annotations

import collections.abc
import enum
from collections.abc import Collection, Iterable, Sequence
from typing import TYPE_CHECKING, Any

from sqlalchemy import CheckConstraint, ForeignKeyConstraint, Integer, String, func, or_, select
from sqlalchemy.orm import Mapped

from airflow.models.base import COLLATION_ARGS, ID_LEN, TaskInstanceDependencies
from airflow.models.dag_version import DagVersion
from airflow.utils.db import exists_query
from airflow.utils.sqlalchemy import ExtendedJSON, mapped_column, with_row_locks
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.mappedoperator import MappedOperator
    from airflow.models.taskinstance import TaskInstance
    from airflow.serialization.serialized_objects import SerializedBaseOperator


class TaskMapVariant(enum.Enum):
    """
    Task map variant.

    Possible values are **dict** (for a key-value mapping) and **list** (for an
    ordered value sequence).
    """

    DICT = "dict"
    LIST = "list"


class TaskMap(TaskInstanceDependencies):
    """
    Model to track dynamic task-mapping information.

    This is currently only populated by an upstream TaskInstance pushing an
    XCom that's pulled by a downstream for mapping purposes.
    """

    __tablename__ = "task_map"

    # Link to upstream TaskInstance creating this dynamic mapping information.
    dag_id: Mapped[str] = mapped_column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    task_id: Mapped[str] = mapped_column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    run_id: Mapped[str] = mapped_column(String(ID_LEN, **COLLATION_ARGS), primary_key=True)
    map_index: Mapped[int] = mapped_column(Integer, primary_key=True)

    length: Mapped[int] = mapped_column(Integer, nullable=False)
    keys: Mapped[list | None] = mapped_column(ExtendedJSON, nullable=True)

    __table_args__ = (
        CheckConstraint(length >= 0, name="task_map_length_not_negative"),
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_map_task_instance_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )

    def __init__(
        self,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int,
        length: int,
        keys: list[Any] | None,
    ) -> None:
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index
        self.length = length
        self.keys = keys

    @classmethod
    def from_task_instance_xcom(cls, ti: TaskInstance, value: Collection) -> TaskMap:
        if ti.run_id is None:
            raise ValueError("cannot record task map for unrun task instance")
        return cls(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index,
            length=len(value),
            keys=(list(value) if isinstance(value, collections.abc.Mapping) else None),
        )

    @property
    def variant(self) -> TaskMapVariant:
        if self.keys is None:
            return TaskMapVariant.LIST
        return TaskMapVariant.DICT

    @classmethod
    def expand_mapped_task(
        cls,
        task: SerializedBaseOperator | MappedOperator,
        run_id: str,
        *,
        session: Session,
    ) -> tuple[Sequence[TaskInstance], int]:
        """
        Create the mapped task instances for mapped task.

        :raise NotMapped: If this task does not need expansion.
        :return: The newly created mapped task instances (if any) in ascending
            order by map index, and the maximum map index value.
        """
        from airflow.models.expandinput import NotFullyPopulated
        from airflow.models.mappedoperator import MappedOperator, get_mapped_ti_count
        from airflow.models.taskinstance import TaskInstance
        from airflow.serialization.serialized_objects import SerializedBaseOperator
        from airflow.settings import task_instance_mutation_hook

        if not isinstance(task, (MappedOperator, SerializedBaseOperator)):
            raise RuntimeError(
                f"cannot expand unrecognized operator type {type(task).__module__}.{type(task).__name__}"
            )

        try:
            total_length: int | None = get_mapped_ti_count(task, run_id, session=session)
        except NotFullyPopulated as e:
            if not task.dag or not task.dag.partial:
                task.log.error(
                    "Cannot expand %r for run %s; missing upstream values: %s",
                    task,
                    run_id,
                    sorted(e.missing),
                )
            total_length = None

        state: str | None = None
        unmapped_ti: TaskInstance | None = session.scalars(
            select(TaskInstance).where(
                TaskInstance.dag_id == task.dag_id,
                TaskInstance.task_id == task.task_id,
                TaskInstance.run_id == run_id,
                TaskInstance.map_index == -1,
                or_(TaskInstance.state.in_(State.unfinished), TaskInstance.state.is_(None)),
            )
        ).one_or_none()

        all_expanded_tis: list[TaskInstance] = []

        if unmapped_ti:
            if TYPE_CHECKING:
                assert task.dag is None

            # The unmapped task instance still exists and is unfinished, i.e. we
            # haven't tried to run it before.
            if total_length is None:
                # If the DAG is partial, it's likely that the upstream tasks
                # are not done yet, so the task can't fail yet.
                if not task.dag or not task.dag.partial:
                    unmapped_ti.state = TaskInstanceState.UPSTREAM_FAILED
            elif total_length < 1:
                # If the upstream maps this to a zero-length value, simply mark
                # the unmapped task instance as SKIPPED (if needed).
                task.log.info(
                    "Marking %s as SKIPPED since the map has %d values to expand",
                    unmapped_ti,
                    total_length,
                )
                unmapped_ti.state = TaskInstanceState.SKIPPED
            else:
                zero_index_ti_exists = exists_query(
                    TaskInstance.dag_id == task.dag_id,
                    TaskInstance.task_id == task.task_id,
                    TaskInstance.run_id == run_id,
                    TaskInstance.map_index == 0,
                    session=session,
                )
                if not zero_index_ti_exists:
                    # Otherwise convert this into the first mapped index, and create
                    # TaskInstance for other indexes.
                    unmapped_ti.map_index = 0
                    task.log.debug("Updated in place to become %s", unmapped_ti)
                    all_expanded_tis.append(unmapped_ti)
                    # execute hook for task instance map index 0
                    task_instance_mutation_hook(unmapped_ti)
                    session.flush()
                else:
                    task.log.debug("Deleting the original task instance: %s", unmapped_ti)
                    session.delete(unmapped_ti)
                state = unmapped_ti.state
            dag_version_id = unmapped_ti.dag_version_id

        if total_length is None or total_length < 1:
            # Nothing to fixup.
            indexes_to_map: Iterable[int] = ()
        else:
            # Only create "missing" ones.
            current_max_mapping = (
                session.scalar(
                    select(func.max(TaskInstance.map_index)).where(
                        TaskInstance.dag_id == task.dag_id,
                        TaskInstance.task_id == task.task_id,
                        TaskInstance.run_id == run_id,
                    )
                )
                or 0
            )
            indexes_to_map = range(current_max_mapping + 1, total_length)

        if unmapped_ti:
            dag_version_id = unmapped_ti.dag_version_id
        elif dag_version := DagVersion.get_latest_version(task.dag_id, session=session):
            dag_version_id = dag_version.id
        else:
            dag_version_id = None

        for index in indexes_to_map:
            # TODO: Make more efficient with bulk_insert_mappings/bulk_save_mappings.
            ti = TaskInstance(
                task,
                run_id=run_id,
                map_index=index,
                state=state,
                dag_version_id=dag_version_id,
            )
            task.log.debug("Expanding TIs upserted %s", ti)
            task_instance_mutation_hook(ti)
            ti = session.merge(ti)
            ti.refresh_from_task(task)  # session.merge() loses task information.
            all_expanded_tis.append(ti)

        # Coerce the None case to 0 -- these two are almost treated identically,
        # except the unmapped ti (if exists) is marked to different states.
        total_expanded_ti_count = total_length or 0

        # Any (old) task instances with inapplicable indexes (>= the total
        # number we need) are set to "REMOVED".
        query = select(TaskInstance).where(
            TaskInstance.dag_id == task.dag_id,
            TaskInstance.task_id == task.task_id,
            TaskInstance.run_id == run_id,
            TaskInstance.map_index >= total_expanded_ti_count,
        )
        to_update = session.scalars(with_row_locks(query, of=TaskInstance, session=session, skip_locked=True))
        for ti in to_update:
            ti.state = TaskInstanceState.REMOVED
        session.flush()
        return all_expanded_tis, total_expanded_ti_count - 1
