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
from typing import TYPE_CHECKING, Iterable, List, Optional

from pydantic import BaseModel as BaseModelPydantic, ConfigDict

from airflow.serialization.pydantic.dag import PydanticDag
from airflow.serialization.pydantic.dataset import DatasetEventPydantic
from airflow.utils.session import NEW_SESSION, provide_session

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.jobs.scheduler_job_runner import TI
    from airflow.serialization.pydantic.taskinstance import TaskInstancePydantic
    from airflow.utils.state import TaskInstanceState


class DagRunPydantic(BaseModelPydantic):
    """Serializable representation of the DagRun ORM SqlAlchemyModel used by internal API."""

    id: int
    dag_id: str
    queued_at: Optional[datetime]
    execution_date: datetime
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    state: str
    run_id: str
    creating_job_id: Optional[int]
    external_trigger: bool
    run_type: str
    conf: dict
    data_interval_start: Optional[datetime]
    data_interval_end: Optional[datetime]
    last_scheduling_decision: Optional[datetime]
    dag_hash: Optional[str]
    updated_at: Optional[datetime]
    dag: Optional[PydanticDag]
    consumed_dataset_events: List[DatasetEventPydantic]  # noqa
    log_template_id: Optional[int]

    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    @property
    def logical_date(self) -> datetime:
        return self.execution_date

    @provide_session
    def get_task_instances(
        self,
        state: Iterable[TaskInstanceState | None] | None = None,
        session: Session = NEW_SESSION,
    ) -> list[TI]:
        """
        Return the task instances for this dag run.

        TODO: make it works for AIP-44
        """
        raise NotImplementedError()

    @provide_session
    def get_task_instance(
        self,
        task_id: str,
        session: Session = NEW_SESSION,
        *,
        map_index: int = -1,
    ) -> TI | TaskInstancePydantic | None:
        """
        Return the task instance specified by task_id for this dag run.

        :param task_id: the task id
        :param session: Sqlalchemy ORM Session
        """
        from airflow.models.dagrun import DagRun

        return DagRun.fetch_task_instance(
            dag_id=self.dag_id,
            dag_run_id=self.run_id,
            task_id=task_id,
            session=session,
            map_index=map_index,
        )


DagRunPydantic.model_rebuild()
