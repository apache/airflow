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

import ast
import json
import logging
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING

from sqlalchemy import Integer, String, delete, select
from sqlalchemy.orm import Mapped

from airflow.exceptions import AirflowException
from airflow.models.base import Base
from airflow.providers.common.compat.sdk import Stats, timezone
from airflow.providers.common.compat.sqlalchemy.orm import mapped_column
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.providers_configuration_loader import providers_configuration_loaded
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class EdgeWorkerVersionException(AirflowException):
    """Signal a version mismatch between core and Edge Site."""

    pass


class EdgeWorkerDuplicateException(AirflowException):
    """Signal that a worker with the same name is already active."""

    pass


class EdgeWorkerState(str, Enum):
    """Status of a Edge Worker instance."""

    STARTING = "starting"
    """Edge Worker is in initialization."""
    RUNNING = "running"
    """Edge Worker is actively running a task."""
    IDLE = "idle"
    """Edge Worker is active and waiting for a task."""
    SHUTDOWN_REQUEST = "shutdown request"
    """Request to shutdown Edge Worker is issued. It will be picked-up on the next heartbeat, tasks will drain and then worker will terminate."""
    TERMINATING = "terminating"
    """Edge Worker is completing work (draining running tasks) and stopping."""
    OFFLINE = "offline"
    """Edge Worker was shut down."""
    UNKNOWN = "unknown"
    """No heartbeat signal from worker for some time, Edge Worker probably down or got disconnected."""
    MAINTENANCE_REQUEST = "maintenance request"
    """Worker was requested to enter maintenance mode. Once worker receives this message it will pause fetching tasks and drain tasks."""
    MAINTENANCE_PENDING = "maintenance pending"
    """Edge Worker received the request for maintenance, waiting for tasks to finish. Once tasks are finished will move to 'maintenance mode'."""
    MAINTENANCE_MODE = "maintenance mode"
    """Edge Worker is in maintenance mode. It is online but pauses fetching tasks."""
    MAINTENANCE_EXIT = "maintenance exit"
    """Request Worker is requested to exit maintenance mode. Once the worker receives this state it will un-pause and fetch new tasks."""
    OFFLINE_MAINTENANCE = "offline maintenance"
    """Worker was shut down in maintenance mode. It will be in maintenance mode when restarted."""


class EdgeWorkerModel(Base, LoggingMixin):
    """A Edge Worker instance which reports the state and health."""

    __tablename__ = "edge_worker"
    worker_name: Mapped[str] = mapped_column(String(64), primary_key=True, nullable=False)
    state: Mapped[EdgeWorkerState] = mapped_column(String(20))
    maintenance_comment: Mapped[str | None] = mapped_column(String(1024))
    _queues: Mapped[str | None] = mapped_column("queues", String(256))
    first_online: Mapped[datetime | None] = mapped_column(UtcDateTime)
    last_update: Mapped[datetime | None] = mapped_column(UtcDateTime)
    jobs_active: Mapped[int] = mapped_column(Integer, default=0)
    jobs_taken: Mapped[int] = mapped_column(Integer, default=0)
    jobs_success: Mapped[int] = mapped_column(Integer, default=0)
    jobs_failed: Mapped[int] = mapped_column(Integer, default=0)
    sysinfo: Mapped[str | None] = mapped_column(String(256))

    def __init__(
        self,
        worker_name: str,
        state: str | EdgeWorkerState,
        queues: list[str] | None,
        first_online: datetime | None = None,
        last_update: datetime | None = None,
        maintenance_comment: str | None = None,
    ):
        self.worker_name = worker_name
        self.state = EdgeWorkerState(state)
        self.queues = queues
        self.first_online = first_online or timezone.utcnow()
        self.last_update = last_update
        self.maintenance_comment = maintenance_comment
        super().__init__()

    @property
    def sysinfo_json(self) -> dict | None:
        return json.loads(self.sysinfo) if self.sysinfo else None

    @property
    def queues(self) -> list[str] | None:
        """Return list of queues which are stored in queues field."""
        if self._queues:
            return ast.literal_eval(self._queues)
        return None

    @queues.setter
    def queues(self, queues: list[str] | None) -> None:
        """Set all queues of list into queues field."""
        self._queues = str(queues) if queues else None

    def add_queues(self, new_queues: list[str]) -> None:
        """Add new queue to the queues field."""
        queues = self.queues if self.queues else []
        queues.extend(new_queues)
        # remove duplicated items
        self.queues = list(set(queues))

    def remove_queues(self, remove_queues: list[str]) -> None:
        """Remove queue from queues field."""
        queues = self.queues if self.queues else []
        for queue_name in remove_queues:
            if queue_name in queues:
                queues.remove(queue_name)
        self.queues = queues

    def update_state(self, state: str | EdgeWorkerState) -> None:
        """Update state field."""
        self.state = EdgeWorkerState(state)


def set_metrics(
    worker_name: str,
    state: str | EdgeWorkerState,
    jobs_active: int,
    concurrency: int,
    free_concurrency: int,
    queues: list[str] | None,
) -> None:
    """Set metric of edge worker."""
    queues = queues if queues else []
    connected = state not in (
        EdgeWorkerState.UNKNOWN,
        EdgeWorkerState.OFFLINE,
        EdgeWorkerState.OFFLINE_MAINTENANCE,
    )
    maintenance = state in (
        EdgeWorkerState.MAINTENANCE_MODE,
        EdgeWorkerState.MAINTENANCE_EXIT,
        EdgeWorkerState.OFFLINE_MAINTENANCE,
    )

    Stats.gauge(f"edge_worker.connected.{worker_name}", int(connected))
    Stats.gauge("edge_worker.connected", int(connected), tags={"worker_name": worker_name})

    Stats.gauge(f"edge_worker.maintenance.{worker_name}", int(maintenance))
    Stats.gauge("edge_worker.maintenance", int(maintenance), tags={"worker_name": worker_name})

    Stats.gauge(f"edge_worker.jobs_active.{worker_name}", jobs_active)
    Stats.gauge("edge_worker.jobs_active", jobs_active, tags={"worker_name": worker_name})

    Stats.gauge(f"edge_worker.concurrency.{worker_name}", concurrency)
    Stats.gauge("edge_worker.concurrency", concurrency, tags={"worker_name": worker_name})

    Stats.gauge(f"edge_worker.free_concurrency.{worker_name}", free_concurrency)
    Stats.gauge("edge_worker.free_concurrency", free_concurrency, tags={"worker_name": worker_name})

    Stats.gauge(f"edge_worker.num_queues.{worker_name}", len(queues))
    Stats.gauge(
        "edge_worker.num_queues",
        len(queues),
        tags={"worker_name": worker_name, "queues": ",".join(queues)},
    )


def reset_metrics(worker_name: str) -> None:
    """Reset metrics of worker."""
    set_metrics(
        worker_name=worker_name,
        state=EdgeWorkerState.UNKNOWN,
        jobs_active=0,
        concurrency=0,
        free_concurrency=-1,
        queues=None,
    )


@providers_configuration_loaded
@provide_session
def _fetch_edge_hosts_from_db(
    hostname: str | None = None, states: list | None = None, session: Session = NEW_SESSION
) -> Sequence[EdgeWorkerModel]:
    query = select(EdgeWorkerModel)
    if states:
        query = query.where(EdgeWorkerModel.state.in_(states))
    if hostname:
        query = query.where(EdgeWorkerModel.worker_name == hostname)
    query = query.order_by(EdgeWorkerModel.worker_name)
    return session.scalars(query).all()


@providers_configuration_loaded
@provide_session
def get_registered_edge_hosts(states: list | None = None, session: Session = NEW_SESSION):
    return _fetch_edge_hosts_from_db(states=states, session=session)


@provide_session
def request_maintenance(
    worker_name: str, maintenance_comment: str | None, session: Session = NEW_SESSION
) -> None:
    """Write maintenance request to the db."""
    query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker: EdgeWorkerModel | None = session.scalar(query)
    if not worker:
        raise ValueError(f"Edge Worker {worker_name} not found in list of registered workers")
    worker.state = EdgeWorkerState.MAINTENANCE_REQUEST
    worker.maintenance_comment = maintenance_comment


@provide_session
def exit_maintenance(worker_name: str, session: Session = NEW_SESSION) -> None:
    """Write maintenance exit to the db."""
    query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker: EdgeWorkerModel | None = session.scalar(query)
    if not worker:
        raise ValueError(f"Edge Worker {worker_name} not found in list of registered workers")
    worker.state = EdgeWorkerState.MAINTENANCE_EXIT
    worker.maintenance_comment = None


@provide_session
def remove_worker(worker_name: str, session: Session = NEW_SESSION) -> None:
    """Remove a worker that is offline or just gone from DB."""
    query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker: EdgeWorkerModel | None = session.scalar(query)
    if not worker:
        raise ValueError(f"Edge Worker {worker_name} not found in list of registered workers")
    if worker.state in (
        EdgeWorkerState.OFFLINE,
        EdgeWorkerState.OFFLINE_MAINTENANCE,
        EdgeWorkerState.UNKNOWN,
    ):
        session.execute(delete(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name))
    else:
        error_message = f"Cannot remove edge worker {worker_name} as it is in {worker.state} state!"
        logger.error(error_message)
        raise TypeError(error_message)


@provide_session
def change_maintenance_comment(
    worker_name: str, maintenance_comment: str | None, session: Session = NEW_SESSION
) -> None:
    """Write maintenance comment in the db."""
    query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker: EdgeWorkerModel | None = session.scalar(query)
    if not worker:
        raise ValueError(f"Edge Worker {worker_name} not found in list of registered workers")
    if worker.state in (
        EdgeWorkerState.MAINTENANCE_MODE,
        EdgeWorkerState.MAINTENANCE_PENDING,
        EdgeWorkerState.MAINTENANCE_REQUEST,
        EdgeWorkerState.OFFLINE_MAINTENANCE,
    ):
        worker.maintenance_comment = maintenance_comment
    else:
        error_message = f"Cannot change maintenance comment as {worker_name} is not in maintenance!"
        logger.error(error_message)
        raise TypeError(error_message)


@provide_session
def request_shutdown(worker_name: str, session: Session = NEW_SESSION) -> None:
    """Request to shutdown the edge worker."""
    query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker: EdgeWorkerModel | None = session.scalar(query)
    if not worker:
        raise ValueError(f"Edge Worker {worker_name} not found in list of registered workers")
    if worker.state not in (
        EdgeWorkerState.OFFLINE,
        EdgeWorkerState.OFFLINE_MAINTENANCE,
        EdgeWorkerState.UNKNOWN,
    ):
        worker.state = EdgeWorkerState.SHUTDOWN_REQUEST


@provide_session
def add_worker_queues(worker_name: str, queues: list[str], session: Session = NEW_SESSION) -> None:
    """Add queues to an edge worker."""
    query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker: EdgeWorkerModel | None = session.scalar(query)
    if not worker:
        raise ValueError(f"Edge Worker {worker_name} not found in list of registered workers")
    if worker.state in (
        EdgeWorkerState.OFFLINE,
        EdgeWorkerState.OFFLINE_MAINTENANCE,
        EdgeWorkerState.UNKNOWN,
    ):
        error_message = f"Cannot add queues to edge worker {worker_name} as it is in {worker.state} state!"
        logger.error(error_message)
        raise TypeError(error_message)
    worker.add_queues(queues)


@provide_session
def remove_worker_queues(worker_name: str, queues: list[str], session: Session = NEW_SESSION) -> None:
    """Remove queues from an edge worker."""
    query = select(EdgeWorkerModel).where(EdgeWorkerModel.worker_name == worker_name)
    worker: EdgeWorkerModel | None = session.scalar(query)
    if not worker:
        raise ValueError(f"Edge Worker {worker_name} not found in list of registered workers")
    if worker.state in (
        EdgeWorkerState.OFFLINE,
        EdgeWorkerState.OFFLINE_MAINTENANCE,
        EdgeWorkerState.UNKNOWN,
    ):
        error_message = (
            f"Cannot remove queues from edge worker {worker_name} as it is in {worker.state} state!"
        )
        logger.error(error_message)
        raise TypeError(error_message)
    worker.remove_queues(queues)
