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

from datetime import datetime
from typing import TYPE_CHECKING

import dill
from sqlalchemy import (
    JSON,
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
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy_utils import UUIDType

from airflow._shared.timezones import timezone
from airflow.models.base import Base, StringID
from airflow.models.hitl import HITLDetail
from airflow.models.hitl_history import HITLDetailHistory
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.span_status import SpanStatus
from airflow.utils.sqlalchemy import (
    ExecutorConfigType,
    ExtendedJSON,
    UtcDateTime,
    mapped_column,
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
    task_instance_id: Mapped[str] = mapped_column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        nullable=False,
        primary_key=True,
    )
    task_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    run_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    map_index: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("-1"))
    try_number: Mapped[int] = mapped_column(Integer, nullable=False)
    start_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    end_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    duration: Mapped[float | None] = mapped_column(Float, nullable=True)
    state: Mapped[str | None] = mapped_column(String(20), nullable=True)
    max_tries: Mapped[int | None] = mapped_column(Integer, server_default=text("-1"), nullable=True)
    hostname: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    unixname: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    pool: Mapped[str] = mapped_column(String(256), nullable=False)
    pool_slots: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    queue: Mapped[str | None] = mapped_column(String(256), nullable=True)
    priority_weight: Mapped[int | None] = mapped_column(Integer, nullable=True)
    operator: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    custom_operator_name: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    queued_dttm: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    scheduled_dttm: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    queued_by_job_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    executor: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    executor_config: Mapped[dict | None] = mapped_column(ExecutorConfigType(pickler=dill), nullable=True)
    updated_at: Mapped[datetime | None] = mapped_column(
        UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=True
    )
    rendered_map_index: Mapped[str | None] = mapped_column(String(250), nullable=True)
    context_carrier: Mapped[dict | None] = mapped_column(MutableDict.as_mutable(ExtendedJSON), nullable=True)
    span_status: Mapped[str] = mapped_column(
        String(250), server_default=SpanStatus.NOT_STARTED, nullable=False
    )

    external_executor_id: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    trigger_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    trigger_timeout: Mapped[DateTime | None] = mapped_column(DateTime, nullable=True)
    next_method: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    next_kwargs: Mapped[dict | None] = mapped_column(
        MutableDict.as_mutable(JSON().with_variant(postgresql.JSONB, "postgresql")), nullable=True
    )

    task_display_name: Mapped[str | None] = mapped_column(String(2000), nullable=True)
    dag_version_id: Mapped[str | None] = mapped_column(UUIDType(binary=False), nullable=True)

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

    hitl_detail = relationship("HITLDetailHistory", lazy="noload", uselist=False)

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

        ti_hitl_detail = session.scalar(select(HITLDetail).where(HITLDetail.ti_id == ti.id))
        if ti_hitl_detail is not None:
            session.add(HITLDetailHistory(ti_hitl_detail))

    @provide_session
    def get_dagrun(self, session: Session = NEW_SESSION) -> DagRun:
        """Return the DagRun for this TaskInstanceHistory, matching TaskInstance."""
        return self.dag_run
