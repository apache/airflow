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

from sqlalchemy import (
    Column,
    ForeignKeyConstraint,
    Integer,
    PrimaryKeyConstraint,
    text,
)

from airflow.models.base import Base, StringID
from airflow.models.taskinstance import TaskInstance

if TYPE_CHECKING:
    from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic


class TaskInstanceHistory(Base):
    """
    Store old tries of TaskInstances.

    :meta private:
    """

    __tablename__ = "task_instance_history"
    task_id = Column(StringID(), primary_key=True, nullable=False)
    dag_id = Column(StringID(), primary_key=True, nullable=False)
    run_id = Column(StringID(), primary_key=True, nullable=False)
    map_index = Column(Integer, primary_key=True, nullable=False, server_default=text("-1"))
    try_number = Column(Integer, primary_key=True, default=0)
    # The rest of the columns are kept in sync with TaskInstance, added below

    __table_args__ = (
        PrimaryKeyConstraint(
            "dag_id", "task_id", "run_id", "map_index", "try_number", name="task_instance_history_pkey"
        ),
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
    )

    def __init__(
        self,
        ti: TaskInstance | TaskInstancePydantic,
        state: str | None = None,
    ):
        super().__init__()
        for column in self.__table__.columns:
            setattr(self, column.name, getattr(ti, column.name))

        if state:
            self.state = state


# Add remaining columns from TaskInstance to TaskInstanceHistory, as we want to keep them in sync
for column in TaskInstance.__table__.columns:
    if column.name not in TaskInstanceHistory.__table__.columns:
        setattr(TaskInstanceHistory, column.name, column.copy())
