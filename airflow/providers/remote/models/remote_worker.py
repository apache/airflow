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

import json
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import (
    Column,
    Integer,
    String,
    select,
)

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.models.base import Base
from airflow.serialization.serialized_objects import add_pydantic_class_type_mapping
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.pydantic import BaseModel as BaseModelPydantic, ConfigDict, is_pydantic_2_installed
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


class RemoteWorkerModel(Base, LoggingMixin):
    """A Remote Worker instance which reports the state and health."""

    __tablename__ = "remote_worker"
    worker_name = Column(String(64), primary_key=True, nullable=False)
    state = Column(String(20))
    queues = Column(String(256))
    first_online = Column(UtcDateTime)
    last_update = Column(UtcDateTime)
    jobs_active = Column(Integer, default=0)
    jobs_taken = Column(Integer, default=0)
    jobs_success = Column(Integer, default=0)
    jobs_failed = Column(Integer, default=0)
    sysinfo = Column(String(256))

    def __init__(
        self,
        worker_name: str,
        state: str,
        queues: list[str] | None,
        first_online: datetime | None = None,
        last_update: datetime | None = None,
    ):
        self.worker_name = worker_name
        self.state = state
        self.queues = ", ".join(queues) if queues else None
        self.first_online = first_online or timezone.utcnow()
        self.last_update = last_update
        super().__init__()


class RemoteWorkerState(str, Enum):
    """Status of a remote worker instance."""

    STARTING = "starting"
    """Remote worker is in initialization."""
    RUNNING = "running"
    """Remote worker is actively running a task."""
    IDLE = "idle"
    """Remote worker is active and waiting for a task."""
    TERMINATING = "terminating"
    """Remote worker is completing work and stopping."""
    OFFLINE = "offline"
    """Remote worker was show down."""
    UNKNOWN = "unknown"
    """No heartbeat signal from worker for some time, remote worker probably down."""


class RemoteWorker(BaseModelPydantic, LoggingMixin):
    """Accessor for remote worker instances as logical model."""

    worker_name: str
    state: RemoteWorkerState
    queues: list[str] | None
    first_online: datetime
    last_update: datetime | None = None
    jobs_active: int
    jobs_taken: int
    jobs_success: int
    jobs_failed: int
    sysinfo: str
    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    @staticmethod
    @internal_api_call
    @provide_session
    def register_worker(
        worker_name: str, state: RemoteWorkerState, queues: list[str] | None, session: Session = NEW_SESSION
    ) -> RemoteWorker:
        query = select(RemoteWorkerModel).where(RemoteWorkerModel.worker_name == worker_name)
        worker: RemoteWorkerModel = session.scalar(query)
        if not worker:
            worker = RemoteWorkerModel(worker_name=worker_name, state=state, queues=queues)
        worker.state = state
        worker.queues = queues
        worker.last_update = timezone.utcnow()
        session.add(worker)
        return RemoteWorker(
            worker_name=worker_name,
            state=state,
            queues=worker.queues,
            first_online=worker.first_online,
            last_update=worker.last_update,
            jobs_active=worker.jobs_active or 0,
            jobs_taken=worker.jobs_taken or 0,
            jobs_success=worker.jobs_success or 0,
            jobs_failed=worker.jobs_failed or 0,
            sysinfo=worker.sysinfo or "{}",
        )

    @staticmethod
    @internal_api_call
    @provide_session
    def set_state(
        worker_name: str,
        state: RemoteWorkerState,
        jobs_active: int,
        sysinfo: dict[str, str],
        session: Session = NEW_SESSION,
    ):
        query = select(RemoteWorkerModel).where(RemoteWorkerModel.worker_name == worker_name)
        worker: RemoteWorkerModel = session.scalar(query)
        worker.state = state
        worker.jobs_active = jobs_active
        worker.sysinfo = json.dumps(sysinfo)
        worker.last_update = timezone.utcnow()
        session.commit()


if is_pydantic_2_installed():
    RemoteWorker.model_rebuild()

add_pydantic_class_type_mapping("remote_worker", RemoteWorkerModel, RemoteWorker)
