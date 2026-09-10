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

from sqlalchemy import Index, Integer, String, Text, event
from sqlalchemy.orm import Mapped, Session, mapped_column, relationship

from airflow._shared.timezones import timezone
from airflow.configuration import conf
from airflow.models.base import Base, StringID
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from airflow.models.dag import DagModel
    from airflow.models.taskinstance import TaskInstance
    from airflow.models.taskinstancekey import TaskInstanceKey


class Log(Base):
    """Used to actively log events to the database."""

    __tablename__ = "log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dttm: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    dag_id: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    task_id: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    map_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    event: Mapped[str] = mapped_column(String(60), nullable=False)
    logical_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    run_id: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    owner: Mapped[str | None] = mapped_column(String(500), nullable=True)
    owner_display_name: Mapped[str | None] = mapped_column(String(500), nullable=True)
    extra: Mapped[str | None] = mapped_column(Text, nullable=True)
    try_number: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Team this event belongs to, recorded when the row is written: either passed in by a caller
    # acting on a team-scoped resource that owns no Dag (a triggerer started with ``--team-name``,
    # a team-scoped pool, the ``teams`` commands), or resolved from ``dag_id`` on insert. Stamped
    # rather than resolved on read so an event keeps the team that owned the resource at the time,
    # even if the Dag's bundle is later reassigned. Deliberately not a foreign key to ``team.name``,
    # for the same reason ``dag_id`` is not one to ``dag.dag_id``: deleting a team must not rewrite
    # or delete the audit trail of what was done to it.
    team_name: Mapped[str | None] = mapped_column(String(50), nullable=True)

    dag_model: Mapped[DagModel | None] = relationship(
        "DagModel",
        viewonly=True,
        foreign_keys=[dag_id],
        primaryjoin="Log.dag_id == DagModel.dag_id",
    )

    task_instance: Mapped[TaskInstance | None] = relationship(
        "TaskInstance",
        viewonly=True,
        foreign_keys=[dag_id, task_id, run_id, map_index],
        primaryjoin="and_(Log.dag_id == TaskInstance.dag_id, Log.task_id == TaskInstance.task_id, Log.run_id == TaskInstance.run_id, Log.map_index == TaskInstance.map_index)",
        lazy="raise",
    )

    __table_args__ = (
        Index("idx_log_dttm", dttm),
        Index("idx_log_event", event),
        Index("idx_log_task_instance", dag_id, task_id, run_id, map_index, try_number),
        Index("idx_log_team_name", team_name),
    )

    def __init__(
        self,
        event,
        task_instance: TaskInstance | TaskInstanceKey | None = None,
        owner=None,
        owner_display_name=None,
        extra=None,
        **kwargs,
    ):
        self.dttm = timezone.utcnow()
        self.event = event
        self.extra = extra

        task_owner = None

        self.logical_date = None
        if task_instance:
            self.dag_id = task_instance.dag_id
            self.task_id = task_instance.task_id
            self.run_id = task_instance.run_id
            if logical_date := getattr(task_instance, "logical_date", None):
                self.logical_date = logical_date
            self.try_number = task_instance.try_number
            self.map_index = task_instance.map_index
            if task := getattr(task_instance, "task", None):
                task_owner = task.owner

        if "task_id" in kwargs:
            self.task_id = kwargs["task_id"]
        if "dag_id" in kwargs:
            self.dag_id = kwargs["dag_id"]
        if kwargs.get("logical_date"):
            self.logical_date = kwargs["logical_date"]
        if kwargs.get("run_id"):
            self.run_id = kwargs["run_id"]
        if "map_index" in kwargs:
            self.map_index = kwargs["map_index"]
        if "try_number" in kwargs:
            self.try_number = kwargs["try_number"]
        if "team_name" in kwargs:
            self.team_name = kwargs["team_name"]

        self.owner = owner or task_owner
        self.owner_display_name = owner_display_name or None

    def __str__(self) -> str:
        return f"Log({self.event}, {self.task_id}, {self.owner}, {self.owner_display_name}, {self.extra})"


def resolve_team_name(dag_id: str | None, *, session: Session) -> str | None:
    """
    Return the team owning ``dag_id``, or ``None`` outside multi-team mode and for no Dag.

    Callers that insert :class:`Log` rows without the ORM must stamp ``team_name`` with this;
    the ``before_insert`` hook below does it for every other write path.
    """
    if not dag_id or not conf.getboolean("core", "multi_team"):
        return None

    from airflow.models.dag import DagModel

    return DagModel.get_team_name(dag_id, session=session)


@event.listens_for(Log, "before_insert")
def _stamp_team_name(mapper, connection, target: Log) -> None:
    """
    Record the team owning the event's Dag, unless the caller already named a team.

    Done here rather than at each of the many places that log an event, so that none of them --
    including ones added later -- can leave a Dag event unattributed. Bulk inserts bypass ORM
    events, so those few paths call :func:`resolve_team_name` themselves.
    """
    if target.team_name or not target.dag_id:
        return
    # ``Session(bind=connection)`` runs the lookup inside the flush's own transaction. Cheap in
    # practice: ``get_team_name`` is cached per Dag, so a warm cache emits no query at all.
    with Session(bind=connection) as session:
        target.team_name = resolve_team_name(target.dag_id, session=session)
