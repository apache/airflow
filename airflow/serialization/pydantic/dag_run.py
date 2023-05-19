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
from typing import Iterable

from pendulum import DateTime
from pydantic import BaseModel as BaseModelPydantic
from sqlalchemy import PickleType
from sqlalchemy.orm import Session

from airflow import DAG
from airflow.jobs.scheduler_job_runner import TI
from airflow.models.dagrun import DagRun, _get_previous_dagrun, _get_previous_scheduled_dagrun
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState, TaskInstanceState


class DagRunPydantic(BaseModelPydantic):
    """Serializable representation of the DagRun ORM SqlAlchemyModel used by internal API."""

    id: int
    dag_id: str
    queued_at: datetime | None
    execution_date: DateTime
    start_date: datetime | None
    end_date: datetime | None
    state: str
    run_id: str
    creating_job_id: int | None
    external_trigger: bool
    run_type: str
    conf: PickleType
    data_interval_start: datetime | None
    data_interval_end: datetime | None
    last_scheduling_decision: datetime | None
    dag_hash: str | None
    updated_at: datetime
    dag: DAG | None

    class Config:
        """Make sure it deals automatically with SQLAlchemy ORM classes."""

        orm_mode = True

    @provide_session
    def get_task_instances(
        self,
        state: Iterable[TaskInstanceState | None] | None = None,
        session: Session = NEW_SESSION,
    ) -> list[TI]:
        """
        Returns the task instances for this dag run

        TODO: make it works for AIP-44
        """
        raise NotImplementedError()

    @provide_session
    def get_previous_scheduled_dagrun(self, session: Session = NEW_SESSION) -> DagRun | None:
        """
        The previous, SCHEDULED DagRun, if there is one.

        :param session: SQLAlchemy ORM Session
        """
        return _get_previous_scheduled_dagrun(self, session)

    @provide_session
    def get_previous_dagrun(
        self, state: DagRunState | None = None, session: Session = NEW_SESSION
    ) -> DagRun | None:
        """
        The previous DagRun, if there is one.

        :param session: SQLAlchemy ORM Session
        :param state: the dag run state
        """
        return _get_previous_dagrun(self, state, session)
