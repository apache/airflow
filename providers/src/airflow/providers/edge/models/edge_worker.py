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
from datetime import datetime
from enum import Enum

from sqlalchemy import Column, Integer, String

from airflow.exceptions import AirflowException
from airflow.models.base import Base
from airflow.stats import Stats
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.sqlalchemy import UtcDateTime


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
    """Edge Worker was shut down."""
    UNKNOWN = "unknown"
    """No heartbeat signal from worker for some time, Edge Worker probably down."""


class EdgeWorkerModel(Base, LoggingMixin):
    """A Edge Worker instance which reports the state and health."""

    __tablename__ = "edge_worker"
    worker_name = Column(String(64), primary_key=True, nullable=False)
    state = Column(String(20))
    _queues = Column("queues", String(256))
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
        self.queues = queues
        self.first_online = first_online or timezone.utcnow()
        self.last_update = last_update
        super().__init__()

    @property
    def sysinfo_json(self) -> dict:
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


def set_metrics(
    worker_name: str,
    state: EdgeWorkerState,
    jobs_active: int,
    concurrency: int,
    free_concurrency: int,
    queues: list[str] | None,
) -> None:
    """Set metric of edge worker."""
    queues = queues if queues else []
    connected = state not in (EdgeWorkerState.UNKNOWN, EdgeWorkerState.OFFLINE)

    Stats.gauge(f"edge_worker.state.{worker_name}", int(connected))
    Stats.gauge(
        "edge_worker.state",
        int(connected),
        tags={"name": worker_name, "state": state},
    )

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
