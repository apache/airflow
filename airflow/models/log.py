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

from airflow.models.base import Base, StringID
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
    _table_args_ = lambda: (
        Column("id", Integer(), primary_key=True),
        dttm := Column("dttm", UtcDateTime()),
        dag_id := Column("dag_id", StringID()),
        Column("task_id", StringID()),
        Column("map_index", Integer()),
        event := Column("event", String(60)),
        Column("execution_date", UtcDateTime()),
        Column("run_id", StringID()),
        Column("owner", String(500)),
        Column("owner_display_name", String(500)),
        Column("extra", Text()),
        Index("idx_log_dag", dag_id),
        Index("idx_log_dttm", dttm),
        Index("idx_log_event", event),
    )

    id: Mapped[int]
    dttm: Mapped[datetime | None]
    dag_id: Mapped[str | None]
    task_id: Mapped[str | None]
    map_index: Mapped[int | None]
    event: Mapped[str | None]
    execution_date: Mapped[datetime | None]
    run_id: Mapped[str | None]
    owner: Mapped[str | None]
    owner_display_name: Mapped[str | None]
    extra: Mapped[str | None]

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
