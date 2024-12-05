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

from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Annotated, Any, Optional
from uuid import UUID

from pydantic import (
    BaseModel as BaseModelPydantic,
    ConfigDict,
    PlainSerializer,
    PlainValidator,
)

from airflow.exceptions import AirflowRescheduleException
from airflow.models import Operator
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import (
    TaskInstance,
    _handle_reschedule,
    _set_ti_attrs,
)
from airflow.serialization.pydantic.dag import DagModelPydantic
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.utils import timezone
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    from pydantic import ValidationInfo
    from sqlalchemy.orm import Session

    from airflow.models.dagrun import DagRun
    from airflow.utils.context import Context


def serialize_operator(x: Operator | None) -> dict | None:
    if x:
        from airflow.serialization.serialized_objects import BaseSerialization

        return BaseSerialization.serialize(x, use_pydantic_models=True)
    return None


def validated_operator(x: dict[str, Any] | Operator, _info: ValidationInfo) -> Any:
    from airflow.models.mappedoperator import MappedOperator

    if isinstance(x, BaseOperator) or isinstance(x, MappedOperator) or x is None:
        return x
    from airflow.serialization.serialized_objects import BaseSerialization

    return BaseSerialization.deserialize(x, use_pydantic_models=True)


PydanticOperator = Annotated[
    Operator,
    PlainValidator(validated_operator),
    PlainSerializer(serialize_operator, return_type=dict),
]


class TaskInstancePydantic(BaseModelPydantic, LoggingMixin):
    """Serializable representation of the TaskInstance ORM SqlAlchemyModel used by internal API."""

    id: str
    task_id: str
    dag_id: str
    run_id: str
    map_index: int
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    logical_date: Optional[datetime]
    duration: Optional[float]
    state: Optional[str]
    try_number: int
    max_tries: int
    hostname: str
    unixname: str
    pool: str
    pool_slots: int
    queue: str
    priority_weight: Optional[int]
    operator: str
    custom_operator_name: Optional[str]
    queued_dttm: Optional[datetime]
    queued_by_job_id: Optional[int]
    last_heartbeat_at: Optional[datetime] = None
    pid: Optional[int]
    executor: Optional[str]
    executor_config: Any
    updated_at: Optional[datetime]
    rendered_map_index: Optional[str]
    external_executor_id: Optional[str]
    trigger_id: Optional[int]
    trigger_timeout: Optional[datetime]
    next_method: Optional[str]
    next_kwargs: Optional[dict]
    dag_version_id: Optional[UUID]
    run_as_user: Optional[str]
    task: Optional[PydanticOperator]
    test_mode: bool
    dag_run: Optional[DagRunPydantic]
    dag_model: Optional[DagModelPydantic]
    raw: Optional[bool]
    is_trigger_log_context: Optional[bool]
    context_carrier: Optional[dict]
    span_status: Optional[str]
    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    @property
    def _logger_name(self):
        return "airflow.task"

    def _run_execute_callback(self, context, task):
        TaskInstance._run_execute_callback(self=self, context=context, task=task)  # type: ignore[arg-type]

    def render_templates(self, context: Context | None = None, jinja_env=None):
        return TaskInstance.render_templates(self=self, context=context, jinja_env=jinja_env)  # type: ignore[arg-type]

    def init_run_context(self, raw: bool = False) -> None:
        """Set the log context."""
        self.raw = raw
        self._set_context(self)

    def xcom_pull(
        self,
        task_ids: str | Iterable[str] | None = None,
        dag_id: str | None = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        session: Session | None = None,
        *,
        map_indexes: int | Iterable[int] | None = None,
        default: Any = None,
    ) -> Any:
        """
        Pull an XCom value for this task instance.

        :param task_ids: task id or list of task ids, if None, the task_id of the current task is used
        :param dag_id: dag id, if None, the dag_id of the current task is used
        :param key: the key to identify the XCom value
        :param include_prior_dates: whether to include prior logical dates
        :param session: the sqlalchemy session
        :param map_indexes: map index or list of map indexes, if None, the map_index of the current task
            is used
        :param default: the default value to return if the XCom value does not exist
        :return: Xcom value
        """
        return TaskInstance.xcom_pull(
            self=self,  # type: ignore[arg-type]
            task_ids=task_ids,
            dag_id=dag_id,
            key=key,
            include_prior_dates=include_prior_dates,
            map_indexes=map_indexes,
            default=default,
            session=session,
        )

    def xcom_push(
        self,
        key: str,
        value: Any,
        session: Session | None = None,
    ) -> None:
        """
        Push an XCom value for this task instance.

        :param key: the key to identify the XCom value
        :param value: the value of the XCom
        """
        return TaskInstance.xcom_push(
            self=self,  # type: ignore[arg-type]
            key=key,
            value=value,
            session=session,
        )

    def get_dagrun(self, session: Session | None = None) -> DagRun:
        """
        Return the DagRun for this TaskInstance.

        :param session: SQLAlchemy ORM Session

        :return: Pydantic serialized version of DagRun
        """
        return TaskInstance._get_dagrun(dag_id=self.dag_id, run_id=self.run_id, session=session)

    def _execute_task(self, context, task_orig):
        """
        Execute Task (optionally with a Timeout) and push Xcom results.

        :param context: Jinja2 context
        :param task_orig: origin task
        """
        from airflow.models.taskinstance import _execute_task

        return _execute_task(task_instance=self, context=context, task_orig=task_orig)

    def update_heartbeat(self):
        """Update the recorded heartbeat for this task to "now"."""
        from airflow.models.taskinstance import _update_ti_heartbeat

        return _update_ti_heartbeat(self.id, timezone.utcnow())

    def is_eligible_to_retry(self):
        """Is task instance is eligible for retry."""
        from airflow.models.taskinstance import _is_eligible_to_retry

        return _is_eligible_to_retry(task_instance=self)

    def _register_asset_changes(self, *, events, session: Session | None = None) -> None:
        TaskInstance._register_asset_changes(self=self, events=events, session=session)  # type: ignore[arg-type]

    def _handle_reschedule(
        self,
        actual_start_date: datetime,
        reschedule_exception: AirflowRescheduleException,
        test_mode: bool = False,
        session: Session | None = None,
    ):
        updated_ti = _handle_reschedule(
            ti=self,
            actual_start_date=actual_start_date,
            reschedule_exception=reschedule_exception,
            test_mode=test_mode,
            session=session,
        )
        _set_ti_attrs(self, updated_ti)  # _handle_reschedule is a remote call that mutates the TI


TaskInstancePydantic.model_rebuild()
