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
from typing import TYPE_CHECKING, List, Optional

from pydantic import BaseModel, ConfigDict
from sqlalchemy import (
    Column,
    Integer,
    String,
    select,
)

from airflow.api_internal.internal_api_call import internal_api_call
from airflow.exceptions import AirflowException
from airflow.models.base import Base
from airflow.serialization.serialized_objects import add_pydantic_class_type_mapping
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


class EdgeWorkerVersionException(AirflowException):
    """Signal a version mismatch between core and Edge Site."""

    pass


class EdgeWorkerState(str, Enum):
    """Status of a Edge Worker instance."""

    STARTING = "starting"
    """Edge Worker is in initialization."""
    RUNNING = "running"
    """Edge Worker is actively running a task."""
    IDLE = "idle"
    """Edge Worker is active and waiting for a task."""
    TERMINATING = "terminating"
    """Edge Worker is completing work and stopping."""
    OFFLINE = "offline"
    """Edge Worker was show down."""
    UNKNOWN = "unknown"
    """No heartbeat signal from worker for some time, Edge Worker probably down."""


class EdgeWorkerModel(Base, LoggingMixin):
    """A Edge Worker instance which reports the state and health."""

    __tablename__ = "edge_worker"
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

    @property
    def sysinfo_json(self) -> dict:
        return json.loads(self.sysinfo) if self.sysinfo else None


class EdgeWorker(BaseModel, LoggingMixin):
    """Accessor for Edge Worker instances as logical model."""

    worker_name: str
    state: EdgeWorkerState
    queues: Optional[List[str]]  # noqa: UP006,UP007 - prevent Sphinx failing
    first_online: datetime
    last_update: Optional[datetime] = None  # noqa: UP007 - prevent Sphinx failing
    jobs_active: int
    jobs_taken: int
    jobs_success: int
    jobs_failed: int
    sysinfo: str
    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    @staticmethod
    def assert_version(sysinfo: dict[str, str]) -> None:
        """Check if the Edge Worker version matches the central API site."""
        from airflow import __version__ as airflow_version
        from airflow.providers.edge import __version__ as edge_provider_version

        # Note: In future, more stable versions we might be more liberate, for the
        #       moment we require exact version match for Edge Worker and core version
        if "airflow_version" in sysinfo:
            airflow_on_worker = sysinfo["airflow_version"]
            if airflow_on_worker != airflow_version:
                raise EdgeWorkerVersionException(
                    f"Edge Worker runs on Airflow {airflow_on_worker} "
                    f"and the core runs on {airflow_version}. Rejecting access due to difference."
                )
        else:
            raise EdgeWorkerVersionException("Edge Worker does not specify the version it is running on.")

        if "edge_provider_version" in sysinfo:
            provider_on_worker = sysinfo["edge_provider_version"]
            if provider_on_worker != edge_provider_version:
                raise EdgeWorkerVersionException(
                    f"Edge Worker runs on Edge Provider {provider_on_worker} "
                    f"and the core runs on {edge_provider_version}. Rejecting access due to difference."
                )
        else:
            raise EdgeWorkerVersionException(
                "Edge Worker does not specify the provider version it is running on."
            )

    @staticmethod
    @internal_api_call
    @provide_session
    def register_worker(
        worker_name: str,
        state: EdgeWorkerState,
        queues: list[str] | None,
        sysinfo: dict[str, str],
        session: Session = NEW_SESSION,
    ) -> EdgeWorker:
        EdgeWorker.assert_version(sysinfo)
        query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
        worker: EdgeWorkerModel = session.scalar(query)
        if not worker:
            worker = EdgeWorkerModel(worker_name=worker_name, state=state, queues=queues)
        worker.state = state
        worker.queues = queues
        worker.sysinfo = json.dumps(sysinfo)
        worker.last_update = timezone.utcnow()
        session.add(worker)
        return EdgeWorker(
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
        state: EdgeWorkerState,
        jobs_active: int,
        sysinfo: dict[str, str],
        session: Session = NEW_SESSION,
    ):
        EdgeWorker.assert_version(sysinfo)
        query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
        worker: EdgeWorkerModel = session.scalar(query)
        worker.state = state
        worker.jobs_active = jobs_active
        worker.sysinfo = json.dumps(sysinfo)
        worker.last_update = timezone.utcnow()
        session.commit()


EdgeWorker.model_rebuild()

add_pydantic_class_type_mapping("edge_worker", EdgeWorkerModel, EdgeWorker)
