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

import dill
from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
    select,
    text,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import relationship
from sqlalchemy_utils import UUIDType

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.span_status import SpanStatus
from airflow.utils.sqlalchemy import (
    ExecutorConfigType,
    ExtendedJSON,
    UtcDateTime,
)
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session

    from airflow.models import DagRun
    from airflow.models.taskinstance import TaskInstance


class TaskInstanceHistory(Base):
    """
    Store old tries of TaskInstances.

    :meta private:
    """

    __tablename__ = "task_instance_history"
    task_instance_id = Column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        nullable=False,
        primary_key=True,
    )
    task_id = Column(StringID(), nullable=False)
    dag_id = Column(StringID(), nullable=False)
    run_id = Column(StringID(), nullable=False)
    map_index = Column(Integer, nullable=False, server_default=text("-1"))
    try_number = Column(Integer, nullable=False)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    max_tries = Column(Integer, server_default=text("-1"))
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    pool = Column(String(256), nullable=False)
    pool_slots = Column(Integer, default=1, nullable=False)
    queue = Column(String(256))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    custom_operator_name = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    scheduled_dttm = Column(UtcDateTime)
    queued_by_job_id = Column(Integer)
    pid = Column(Integer)
    executor = Column(String(1000))
    executor_config = Column(ExecutorConfigType(pickler=dill))
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow)
    rendered_map_index = Column(String(250))
    context_carrier = Column(MutableDict.as_mutable(ExtendedJSON))
    span_status = Column(String(250), server_default=SpanStatus.NOT_STARTED, nullable=False)

    external_executor_id = Column(StringID())
    trigger_id = Column(Integer)
    trigger_timeout = Column(DateTime)
    next_method = Column(String(1000))
    next_kwargs = Column(MutableDict.as_mutable(ExtendedJSON))

    task_display_name = Column(String(2000), nullable=True)
    dag_version_id = Column(UUIDType(binary=False))

    dag_version = relationship(
        "DagVersion",
        primaryjoin="TaskInstanceHistory.dag_version_id == DagVersion.id",
        viewonly=True,
        foreign_keys=[dag_version_id],
    )

    dag_run = relationship(
        "DagRun",
        primaryjoin="and_(TaskInstanceHistory.run_id == DagRun.run_id, DagRun.dag_id == TaskInstanceHistory.dag_id)",
        viewonly=True,
        foreign_keys=[run_id, dag_id],
    )

    def __init__(
        self,
        ti: TaskInstance,
        state: str | None = None,
    ):
        super().__init__()
        for column in self.__table__.columns:
            if column.name == "id":
                continue
            if column.name == "task_instance_id":
                setattr(self, column.name, ti.id)
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
        Index("idx_tih_dag_run", dag_id, run_id),
    )

    @property
    def id(self) -> str:
        """Alias for primary key field to support TaskInstance."""
        return self.task_instance_id

    @staticmethod
    @provide_session
    def record_ti(ti: TaskInstance, session: Session = NEW_SESSION) -> None:
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

    @provide_session
    def get_dagrun(self, session: Session = NEW_SESSION) -> DagRun:
        """Return the DagRun for this TaskInstanceHistory, matching TaskInstance."""
        return self.dag_run
