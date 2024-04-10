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
"""Taskfail tracks the failed run durations of each task instance."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKeyConstraint, Index, Integer, text
from sqlalchemy.orm import relationship

from airflow.models.base import StringID, TaskInstanceDependencies
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Mapped

    from airflow.models.dagrun import DagRun
    from airflow.models.taskinstance import TaskInstance


class TaskFail(TaskInstanceDependencies):
    """TaskFail tracks the failed run durations of each task instance."""

    __tablename__ = "task_fail"

    id: Mapped[int] = Column(Integer, primary_key=True)
    task_id: Mapped[str] = Column(StringID(), nullable=False)
    dag_id: Mapped[str] = Column(StringID(), nullable=False)
    run_id: Mapped[str] = Column(StringID(), nullable=False)
    map_index: Mapped[int] = Column(Integer, nullable=False, server_default=text("-1"))
    start_date: Mapped[datetime | None] = Column(UtcDateTime)
    end_date: Mapped[datetime | None] = Column(UtcDateTime)
    duration: Mapped[int | None] = Column(Integer)

    __table_args__ = (
        Index("idx_task_fail_task_instance", dag_id, task_id, run_id, map_index),
        ForeignKeyConstraint(
            [dag_id, task_id, run_id, map_index],
            [
                "task_instance.dag_id",
                "task_instance.task_id",
                "task_instance.run_id",
                "task_instance.map_index",
            ],
            name="task_fail_ti_fkey",
            ondelete="CASCADE",
        ),
    )

    # We don't need a DB level FK here, as we already have that to TI (which has one to DR) but by defining
    # the relationship we can more easily find the execution date for these rows
    dag_run: Mapped[DagRun] = relationship(
        "DagRun",
        primaryjoin="""and_(
            TaskFail.dag_id == foreign(DagRun.dag_id),
            TaskFail.run_id == foreign(DagRun.run_id),
        )""",
        viewonly=True,
    )

    def __init__(self, ti: TaskInstance):
        self.dag_id = ti.dag_id
        self.task_id = ti.task_id
        self.run_id = ti.run_id
        self.map_index = ti.map_index
        self.start_date = ti.start_date
        self.end_date = ti.end_date
        if self.end_date and self.start_date:
            self.duration = int((self.end_date - self.start_date).total_seconds())
        else:
            self.duration = None

    def __repr__(self):
        prefix = f"<{self.__class__.__name__}: {self.dag_id}.{self.task_id} {self.run_id}"
        if self.map_index != -1:
            prefix += f" map_index={self.map_index}"
        return prefix + ">"
