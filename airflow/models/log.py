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

from sqlalchemy import Column, Index, Integer, String, Text

from airflow.models.base import Base, Hint, StringID
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from datetime import datetime

    from sqlalchemy.orm import Mapped

    from airflow.models.taskinstance import TaskInstance
    from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic


class Log(Base):
    """Used to actively log events to the database."""

    __tablename__ = "log"

    id: Mapped[int] = Hint.col | Column(Integer, primary_key=True)
    dttm: Mapped[datetime | None] = Hint.col | Column(UtcDateTime)
    dag_id: Mapped[str | None] = Hint.col | Column(StringID())
    task_id: Mapped[str | None] = Hint.col | Column(StringID())
    map_index: Mapped[int | None] = Hint.col | Column(Integer)
    event: Mapped[str | None] = Hint.col | Column(String(60))
    execution_date: Mapped[datetime | None] = Hint.col | Column(UtcDateTime)
    run_id: Mapped[str | None] = Hint.col | Column(StringID())
    owner: Mapped[str | None] = Hint.col | Column(String(500))
    owner_display_name: Mapped[str | None] = Hint.col | Column(String(500))
    extra: Mapped[str | None] = Hint.col | Column(Text)

    __table_args__ = (
        Index("idx_log_dag", dag_id),
        Index("idx_log_dttm", dttm),
        Index("idx_log_event", event),
    )

    def __init__(
        self,
        event,
        task_instance: TaskInstance | TaskInstancePydantic | None = None,
        owner=None,
        owner_display_name=None,
        extra=None,
        **kwargs,
    ):
        self.dttm = timezone.utcnow()
        self.event = event
        self.extra = extra

        task_owner = None

        if task_instance:
            self.dag_id = task_instance.dag_id
            self.task_id = task_instance.task_id
            self.execution_date = task_instance.execution_date
            self.run_id = task_instance.run_id
            self.map_index = task_instance.map_index

            task = getattr(task_instance, "task", None)
            if task:
                task_owner = task.owner

        if "task_id" in kwargs:
            self.task_id = kwargs["task_id"]
        if "dag_id" in kwargs:
            self.dag_id = kwargs["dag_id"]
        if kwargs.get("execution_date"):
            self.execution_date = kwargs["execution_date"]
        if kwargs.get("run_id"):
            self.run_id = kwargs["run_id"]
        if "map_index" in kwargs:
            self.map_index = kwargs["map_index"]

        self.owner = owner or task_owner
        self.owner_display_name = owner_display_name or None

    def __str__(self) -> str:
        return f"Log({self.event}, {self.task_id}, {self.owner}, {self.owner_display_name}, {self.extra})"
