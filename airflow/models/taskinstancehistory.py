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
from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKeyConstraint, Integer, UniqueConstraint, func, select, text

from airflow.models.base import Base, StringID
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic


class TaskInstanceHistory(Base):
    """
    Store old tries of TaskInstances.

    :meta private:
    """

    __tablename__ = "task_instance_history"
    id = Column(Integer(), primary_key=True, autoincrement=True)
    task_id = Column(StringID(), nullable=False)
    dag_id = Column(StringID(), nullable=False)
    run_id = Column(StringID(), nullable=False)
    map_index = Column(Integer, nullable=False, server_default=text("-1"))
    try_number = Column(Integer, nullable=False)
    # The rest of the columns are kept in sync with TaskInstance, added at the bottom of this file

    def __init__(
        self,
        ti: TaskInstance | TaskInstancePydantic,
        state: str | None = None,
    ):
        super().__init__()
        for column in self.__table__.columns:
            if column.name == "id":
                continue
            setattr(self, column.name, getattr(ti, column.name))

        if state:
            self.state = state

    __table_args__ = (
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_instance_history_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
        UniqueConstraint(
            "dag_id",
            "task_id",
            "run_id",
            "map_index",
            "try_number",
            name="task_instance_history_dtrt_uq",
        ),
    )

    @staticmethod
    @provide_session
    def record_ti(ti: TaskInstance, session: NEW_SESSION = None) -> None:
        """Record a TaskInstance to TaskInstanceHistory."""
        exists_q = session.scalar(
            select(func.count(TaskInstanceHistory.task_id)).where(
                TaskInstanceHistory.dag_id == ti.dag_id,
                TaskInstanceHistory.task_id == ti.task_id,
                TaskInstanceHistory.run_id == ti.run_id,
                TaskInstanceHistory.map_index == ti.map_index,
                TaskInstanceHistory.try_number == ti.try_number,
            )
        )
        if exists_q:
            return
        ti_history_state = ti.state
        if ti.state not in State.finished:
            ti_history_state = TaskInstanceState.FAILED
            ti.end_date = timezone.utcnow()
            ti.set_duration()
        ti_history = TaskInstanceHistory(ti, state=ti_history_state)
        session.add(ti_history)


def _copy_column(column):
    return Column(
        column.type,
        nullable=column.nullable,
        default=column.default,
        autoincrement=column.autoincrement,
        unique=column.unique,
        index=column.index,
        primary_key=None,
        server_default=column.server_default,
        server_onupdate=column.server_onupdate,
        doc=column.doc,
        comment=column.comment,
        info=column.info,
    )


# Add remaining columns from TaskInstance to TaskInstanceHistory, as we want to keep them in sync
for column in TaskInstance.__table__.columns:
    if column.name not in TaskInstanceHistory.__table__.columns:
        setattr(TaskInstanceHistory, column.name, _copy_column(column))
