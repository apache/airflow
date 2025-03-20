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
"""TaskReschedule tracks rescheduled task instances."""

from __future__ import annotations

import uuid
from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    ForeignKey,
    Integer,
    String,
    asc,
    desc,
    select,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import relationship

from airflow.models.base import Base
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    import datetime

    from sqlalchemy.sql import Select

    from airflow.models.taskinstance import TaskInstance


class TaskReschedule(Base):
    """TaskReschedule tracks rescheduled task instances."""

    __tablename__ = "task_reschedule"
    id = Column(Integer, primary_key=True)
    ti_id = Column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        ForeignKey("task_instance.id", ondelete="CASCADE", name="task_reschedule_ti_fkey"),
        nullable=False,
    )
    try_number = Column(Integer, nullable=False)
    start_date = Column(UtcDateTime, nullable=False)
    end_date = Column(UtcDateTime, nullable=False)
    duration = Column(Integer, nullable=False)
    reschedule_date = Column(UtcDateTime, nullable=False)

    task_instance = relationship(
        "TaskInstance", primaryjoin="TaskReschedule.ti_id == foreign(TaskInstance.id)", uselist=False
    )

    def __init__(
        self,
        task_instance_id: uuid.UUID,
        try_number: int,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        reschedule_date: datetime.datetime,
    ) -> None:
        self.ti_id = task_instance_id
        self.try_number = try_number
        self.start_date = start_date
        self.end_date = end_date
        self.reschedule_date = reschedule_date
        self.duration = (self.end_date - self.start_date).total_seconds()

    @classmethod
    def stmt_for_task_instance(
        cls,
        ti: TaskInstance,
        *,
        try_number: int | None = None,
        descending: bool = False,
    ) -> Select:
        """
        Statement for task reschedules for a given task instance.

        :param ti: the task instance to find task reschedules for
        :param descending: If True then records are returned in descending order
        :param try_number: Look for TaskReschedule of the given try_number. Default is None which
            looks for the same try_number of the given task_instance.
        :meta private:
        """
        if try_number is None:
            try_number = ti.try_number
        return (
            select(cls)
            .where(cls.ti_id == ti.id, cls.try_number == try_number)
            .order_by(desc(cls.id) if descending else asc(cls.id))
        )
