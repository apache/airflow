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

from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from airflow.models import Base
from airflow.models.base import StringID
from airflow.utils import timezone
from airflow.utils.session import provide_session


class DagRunStateModel(Base):
    """A class to represent the state of a DagRun."""
    __tablename__ = "dagrun_state"
    id = Column(Integer, primary_key=True)
    type = Column(StringID())
    name = Column(String(20))
    timestamp = Column(DateTime, default=timezone.utcnow())
    message = Column(StringID())
    dag_run_id = Column(Integer, ForeignKey("dag_run.id"))
    dag_run = relationship("DagRun", back_populates="dag_run_states")

    def __init__(self, type, name, message, timestamp=timezone.utcnow(), dagrun=None):
        self.type = type
        self.name = name
        self.message = message
        self.timestamp = timestamp
        self.dag_run = dagrun

    @staticmethod
    @provide_session
    def add_state(type, name, message="", dagrun=None, session=None):
        exists = (
            session.query(DagRunStateModel)
            .filter(
                DagRunStateModel.type == type,
                DagRunStateModel.name == name,
                DagRunStateModel.dag_run_id == dagrun.id,
            )
            .first()
        )
        if not exists:
            state = DagRunStateModel(type=type, name=name, message=message, dagrun=dagrun)
            return state
