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

import datetime
import uuid
from typing import TYPE_CHECKING

from sqlalchemy import (
    ForeignKey,
    Integer,
    String,
    asc,
    desc,
    select,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.orm import Mapped, relationship

from airflow.models.base import Base
from airflow.utils.sqlalchemy import UtcDateTime, mapped_column

if TYPE_CHECKING:
    import datetime

    from sqlalchemy.sql import Select

    from airflow.models.taskinstance import TaskInstance


class TaskReschedule(Base):
    """TaskReschedule tracks rescheduled task instances."""

    __tablename__ = "task_reschedule"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ti_id: Mapped[str] = mapped_column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        ForeignKey("task_instance.id", ondelete="CASCADE", name="task_reschedule_ti_fkey"),
        nullable=False,
    )
    start_date: Mapped[datetime.datetime] = mapped_column(UtcDateTime, nullable=False)
    end_date: Mapped[datetime.datetime] = mapped_column(UtcDateTime, nullable=False)
    duration: Mapped[int] = mapped_column(Integer, nullable=False)
    reschedule_date: Mapped[datetime.datetime] = mapped_column(UtcDateTime, nullable=False)

    task_instance = relationship(
        "TaskInstance", primaryjoin="TaskReschedule.ti_id == foreign(TaskInstance.id)", uselist=False
    )

    def __init__(
        self,
        ti_id: uuid.UUID | str,
        start_date: datetime.datetime,
        end_date: datetime.datetime,
        reschedule_date: datetime.datetime,
    ) -> None:
        self.ti_id = str(ti_id)
        self.start_date = start_date
        self.end_date = end_date
        self.reschedule_date = reschedule_date
        self.duration = int((self.end_date - self.start_date).total_seconds())

    @classmethod
    def stmt_for_task_instance(
        cls,
        ti: TaskInstance,
        *,
        descending: bool = False,
    ) -> Select:
        """
        Statement for task reschedules for a given task instance.

        :param ti: the task instance to find task reschedules for
        :param descending: If True then records are returned in descending order
        :meta private:
        """
        return select(cls).where(cls.ti_id == ti.id).order_by(desc(cls.id) if descending else asc(cls.id))
