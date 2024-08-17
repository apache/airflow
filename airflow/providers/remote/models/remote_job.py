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

from ast import literal_eval
from datetime import datetime
from typing import TYPE_CHECKING, List, Optional

from sqlalchemy import (
    Column,
    Index,
    Integer,
    String,
    select,
    text,
)

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.models.base import Base, StringID
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.serialization.serialized_objects import add_pydantic_class_type_mapping
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.pydantic import BaseModel as BaseModelPydantic, ConfigDict, is_pydantic_2_installed
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, with_row_locks
from airflow.utils.state import TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


class RemoteJobModel(Base, LoggingMixin):
    """
    A job which is queued, waiting or running on a Remote Worker.

    Each tuple in the database represents and describes the state of one job.
    """

    __tablename__ = "remote_job"
    dag_id = Column(StringID(), primary_key=True, nullable=False)
    task_id = Column(StringID(), primary_key=True, nullable=False)
    run_id = Column(StringID(), primary_key=True, nullable=False)
    map_index = Column(Integer, primary_key=True, nullable=False, server_default=text("-1"))
    try_number = Column(Integer, primary_key=True, default=0)
    state = Column(String(20))
    queue = Column(String(256))
    command = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    remote_worker = Column(String(64))
    last_update = Column(UtcDateTime)

    def __init__(
        self,
        dag_id: str,
        task_id: str,
        run_id: str,
        map_index: int,
        try_number: int,
        state: str,
        queue: str,
        command: str,
        queued_dttm: datetime | None = None,
        remote_worker: str | None = None,
        last_update: datetime | None = None,
    ):
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index
        self.try_number = try_number
        self.state = state
        self.queue = queue
        self.command = command
        self.queued_dttm = queued_dttm or timezone.utcnow()
        self.remote_worker = remote_worker
        self.last_update = last_update
        super().__init__()

    __table_args__ = (Index("rj_order", state, queued_dttm, queue),)

    @property
    def key(self):
        return TaskInstanceKey(
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            try_number=self.try_number,
            map_index=self.map_index,
        )


class RemoteJob(BaseModelPydantic, LoggingMixin):
    """Accessor for remote jobs as logical model."""

    dag_id: str
    task_id: str
    run_id: str
    map_index: int
    try_number: int
    state: TaskInstanceState
    queue: str
    command: List[str]  # noqa: UP006 - prevent Sphinx failing
    queued_dttm: datetime
    remote_worker: Optional[str]  # noqa: UP007 - prevent Sphinx failing
    last_update: Optional[datetime]  # noqa: UP007 - prevent Sphinx failing
    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    @property
    def key(self) -> TaskInstanceKey:
        return TaskInstanceKey(self.dag_id, self.task_id, self.run_id, self.try_number, self.map_index)

    @staticmethod
    @internal_api_call
    @provide_session
    def reserve_task(
        worker_name: str, queues: list[str] | None = None, session: Session = NEW_SESSION
    ) -> RemoteJob | None:
        query = (
            select(RemoteJobModel)
            .where(RemoteJobModel.state == TaskInstanceState.QUEUED)
            .order_by(RemoteJobModel.queued_dttm)
        )
        if queues:
            query = query.where(RemoteJobModel.queue.in_(queues))
        query = query.limit(1)
        query = with_row_locks(query, of=RemoteJobModel, session=session, skip_locked=True)
        job: RemoteJobModel = session.scalar(query)
        if not job:
            return None
        job.state = TaskInstanceState.RUNNING
        job.remote_worker = worker_name
        job.last_update = timezone.utcnow()
        session.commit()
        return RemoteJob(
            dag_id=job.dag_id,
            task_id=job.task_id,
            run_id=job.run_id,
            map_index=job.map_index,
            try_number=job.try_number,
            state=job.state,
            queue=job.queue,
            command=literal_eval(job.command),
            queued_dttm=job.queued_dttm,
            remote_worker=job.remote_worker,
            last_update=job.last_update,
        )

    @staticmethod
    @internal_api_call
    @provide_session
    def set_state(task: TaskInstanceKey | tuple, state: TaskInstanceState, session: Session = NEW_SESSION):
        if isinstance(task, tuple):
            task = TaskInstanceKey(*task)
        query = select(RemoteJobModel).where(
            RemoteJobModel.dag_id == task.dag_id,
            RemoteJobModel.task_id == task.task_id,
            RemoteJobModel.run_id == task.run_id,
            RemoteJobModel.map_index == task.map_index,
            RemoteJobModel.try_number == task.try_number,
        )
        job: RemoteJobModel = session.scalar(query)
        job.state = state
        job.last_update = timezone.utcnow()
        session.commit()

    def __hash__(self):
        return f"{self.dag_id}|{self.task_id}|{self.run_id}|{self.map_index}|{self.try_number}".__hash__()


if is_pydantic_2_installed():
    RemoteJob.model_rebuild()

add_pydantic_class_type_mapping("remote_job", RemoteJobModel, RemoteJob)
