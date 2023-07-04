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

from sqlalchemy import Column, Index, Integer, String, Text

from airflow.models.base import Base, StringID
from airflow.utils import timezone
from airflow.utils.sqlalchemy import UtcDateTime


class Log(Base):
    """Used to actively log events to the database."""

    __tablename__ = "log"

    id = Column(Integer, primary_key=True)
    dttm = Column(UtcDateTime)
    dag_id = Column(StringID())
    task_id = Column(StringID())
    map_index = Column(Integer)
    event = Column(String(30))
    execution_date = Column(UtcDateTime)
    owner = Column(String(500))
    extra = Column(Text)

    __table_args__ = (
        Index("idx_log_dag", dag_id),
        Index("idx_log_dttm", dttm),
        Index("idx_log_event", event),
    )

    def __init__(self, event, task_instance=None, owner=None, extra=None, **kwargs):
        self.dttm = timezone.utcnow()
        self.event = event
        self.extra = extra

        task_owner = None

        if task_instance:
            self.dag_id = task_instance.dag_id
            self.task_id = task_instance.task_id
            self.execution_date = task_instance.execution_date
            self.map_index = task_instance.map_index
            if getattr(task_instance, "task", None):
                task_owner = task_instance.task.owner

        if "task_id" in kwargs:
            self.task_id = kwargs["task_id"]
        if "dag_id" in kwargs:
            self.dag_id = kwargs["dag_id"]
        if kwargs.get("execution_date"):
            self.execution_date = kwargs["execution_date"]
        if "map_index" in kwargs:
            self.map_index = kwargs["map_index"]

        self.owner = owner or task_owner

    def __str__(self) -> str:
        return f"Log({self.event}, {self.task_id}, {self.owner}, {self.extra})"
