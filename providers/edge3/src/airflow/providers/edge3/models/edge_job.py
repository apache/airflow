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

from sqlalchemy import (
    Column,
    Index,
    Integer,
    String,
    text,
)

from airflow.models.base import Base, StringID
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.sqlalchemy import UtcDateTime


class EdgeJobModel(Base, LoggingMixin):
    """
    A job which is queued, waiting or running on a Edge Worker.

    Each tuple in the database represents and describes the state of one job.
    """

    __tablename__ = "edge_job"
    dag_id = Column(StringID(), primary_key=True, nullable=False)
    task_id = Column(StringID(), primary_key=True, nullable=False)
    run_id = Column(StringID(), primary_key=True, nullable=False)
    map_index = Column(Integer, primary_key=True, nullable=False, server_default=text("-1"))
    try_number = Column(Integer, primary_key=True, default=0)
    state = Column(String(20))
    queue = Column(String(256))
    concurrency_slots = Column(Integer)
    command = Column(String(2048))
    queued_dttm = Column(UtcDateTime)
    edge_worker = Column(String(64))
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
        concurrency_slots: int,
        command: str,
        queued_dttm: datetime | None = None,
        edge_worker: str | None = None,
        last_update: datetime | None = None,
    ):
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index
        self.try_number = try_number
        self.state = state
        self.queue = queue
        self.concurrency_slots = concurrency_slots
        self.command = command
        self.queued_dttm = queued_dttm or timezone.utcnow()
        self.edge_worker = edge_worker
        self.last_update = last_update
        super().__init__()

    __table_args__ = (Index("rj_order", state, queued_dttm, queue),)

    @property
    def key(self):
        return TaskInstanceKey(self.dag_id, self.task_id, self.run_id, self.try_number, self.map_index)

    @property
    def last_update_t(self) -> float:
        return self.last_update.timestamp()
