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
from typing import TYPE_CHECKING, Any, Iterable, Optional

from pydantic import (
    BaseModel as BaseModelPydantic,
    ConfigDict,
    PlainSerializer,
    PlainValidator,
)
from typing_extensions import Annotated

from airflow.exceptions import AirflowRescheduleException, TaskDeferred
from airflow.models import Operator
from airflow.models.baseoperator import BaseOperator
from airflow.models.taskinstance import (
    TaskInstance,
    TaskReturnCode,
    _defer_task,
    _handle_reschedule,
    _run_raw_task,
    _set_ti_attrs,
)
from airflow.serialization.pydantic.dag import DagModelPydantic
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    import pendulum
    from pydantic import ValidationInfo
    from sqlalchemy.orm import Session

    from airflow.models.dagrun import DagRun
    from airflow.utils.context import Context
    from airflow.utils.state import DagRunState


def serialize_operator(x: Operator | None) -> dict | None:
    if x:
        from airflow.serialization.serialized_objects import BaseSerialization

        return BaseSerialization.serialize(x, use_pydantic_models=True)
    return None


def validated_operator(x: dict[str, Any] | Operator, _info: ValidationInfo) -> Any:
    from airflow.models.baseoperator import BaseOperator
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

    task_id: str
    dag_id: str
    run_id: str
    map_index: int
    start_date: Optional[datetime]
    end_date: Optional[datetime]
    execution_date: Optional[datetime]
    duration: Optional[float]
    state: Optional[str]
    try_number: int
    max_tries: int
    hostname: str
    unixname: str
    job_id: Optional[int]
    pool: str
    pool_slots: int
    queue: str
    priority_weight: Optional[int]
    operator: str
    custom_operator_name: Optional[str]
    queued_dttm: Optional[datetime]
    queued_by_job_id: Optional[int]
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
    run_as_user: Optional[str]
    task: Optional[PydanticOperator]
    test_mode: bool
    dag_run: Optional[DagRunPydantic]
    dag_model: Optional[DagModelPydantic]
    raw: Optional[bool]
    is_trigger_log_context: Optional[bool]
    model_config = ConfigDict(from_attributes=True, arbitrary_types_allowed=True)

    @property
    def _logger_name(self):
        return "airflow.task"

    def clear_xcom_data(self, session: Session | None = None):
        TaskInstance._clear_xcom_data(ti=self, session=session)

    def set_state(self, state, session: Session | None = None) -> bool:
        return TaskInstance._set_state(ti=self, state=state, session=session)

    def _run_raw_task(
        self,
        mark_success: bool = False,
        test_mode: bool = False,
        job_id: str | None = None,
        pool: str | None = None,
        raise_on_defer: bool = False,
        session: Session | None = None,
    ) -> TaskReturnCode | None:
        return _run_raw_task(
            ti=self,
            mark_success=mark_success,
            test_mode=test_mode,
            job_id=job_id,
            pool=pool,
            raise_on_defer=raise_on_defer,
            session=session,
        )

    def _run_execute_callback(self, context, task):
        TaskInstance._run_execute_callback(self=self, context=context, task=task)  # type: ignore[arg-type]

    def render_templates(self, context: Context | None = None, jinja_env=None):
        return TaskInstance.render_templates(
            self=self, context=context, jinja_env=jinja_env
        )  # type: ignore[arg-type]

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
        :param include_prior_dates: whether to include prior execution dates
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

    def get_dagrun(self, session: Session | None = None) -> DagRunPydantic:
        """
        Return the DagRun for this TaskInstance.

        :param session: SQLAlchemy ORM Session

        :return: Pydantic serialized version of DagRun
        """
        return TaskInstance._get_dagrun(
            dag_id=self.dag_id, run_id=self.run_id, session=session
        )

    def _execute_task(self, context, task_orig):
        """
        Execute Task (optionally with a Timeout) and push Xcom results.

        :param context: Jinja2 context
        :param task_orig: origin task
        """
        from airflow.models.taskinstance import _execute_task

        return _execute_task(task_instance=self, context=context, task_orig=task_orig)

    def refresh_from_db(
        self, session: Session | None = None, lock_for_update: bool = False
    ) -> None:
        """
        Refresh the task instance from the database based on the primary key.

        :param session: SQLAlchemy ORM Session
        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        """
        from airflow.models.taskinstance import _refresh_from_db

        _refresh_from_db(
            task_instance=self, session=session, lock_for_update=lock_for_update
        )

    def set_duration(self) -> None:
        """Set task instance duration."""
        from airflow.models.taskinstance import _set_duration

        _set_duration(task_instance=self)

    @property
    def stats_tags(self) -> dict[str, str]:
        """Return task instance tags."""
        from airflow.models.taskinstance import _stats_tags

        return _stats_tags(task_instance=self)

    def clear_next_method_args(self) -> None:
        """Ensure we unset next_method and next_kwargs to ensure that any retries don't reuse them."""
        from airflow.models.taskinstance import _clear_next_method_args

        _clear_next_method_args(task_instance=self)

    def get_template_context(
        self,
        session: Session | None = None,
        ignore_param_exceptions: bool = True,
    ) -> Context:
        """
        Return TI Context.

        :param session: SQLAlchemy ORM Session
        :param ignore_param_exceptions: flag to suppress value exceptions while initializing the ParamsDict
        """
        from airflow.models.taskinstance import _get_template_context

        if TYPE_CHECKING:
            assert self.task
            assert self.task.dag
        return _get_template_context(
            task_instance=self,
            dag=self.task.dag,
            session=session,
            ignore_param_exceptions=ignore_param_exceptions,
        )

    def is_eligible_to_retry(self):
        """Is task instance is eligible for retry."""
        from airflow.models.taskinstance import _is_eligible_to_retry

        return _is_eligible_to_retry(task_instance=self)

    def handle_failure(
        self,
        error: None | str | BaseException,
        test_mode: bool | None = None,
        context: Context | None = None,
        force_fail: bool = False,
        session: Session | None = None,
    ) -> None:
        """
        Handle Failure for a task instance.

        :param error: if specified, log the specific exception if thrown
        :param session: SQLAlchemy ORM Session
        :param test_mode: doesn't record success or failure in the DB if True
        :param context: Jinja2 context
        :param force_fail: if True, task does not retry
        """
        from airflow.models.taskinstance import _handle_failure

        if TYPE_CHECKING:
            assert self.task
            assert self.task.dag
        try:
            fail_stop = self.task.dag.fail_stop
        except Exception:
            fail_stop = False
        _handle_failure(
            task_instance=self,
            error=error,
            session=session,
            test_mode=test_mode,
            context=context,
            force_fail=force_fail,
            fail_stop=fail_stop,
        )

    def refresh_from_task(self, task: Operator, pool_override: str | None = None) -> None:
        """
        Copy common attributes from the given task.

        :param task: The task object to copy from
        :param pool_override: Use the pool_override instead of task's pool
        """
        from airflow.models.taskinstance import _refresh_from_task

        _refresh_from_task(task_instance=self, task=task, pool_override=pool_override)

    def get_previous_dagrun(
        self,
        state: DagRunState | None = None,
        session: Session | None = None,
    ) -> DagRun | None:
        """
        Return the DagRun that ran before this task instance's DagRun.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session.
        """
        from airflow.models.taskinstance import _get_previous_dagrun

        return _get_previous_dagrun(task_instance=self, state=state, session=session)

    def get_previous_execution_date(
        self,
        state: DagRunState | None = None,
        session: Session | None = None,
    ) -> pendulum.DateTime | None:
        """
        Return the execution date from property previous_ti_success.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session
        """
        from airflow.models.taskinstance import _get_previous_execution_date

        return _get_previous_execution_date(
            task_instance=self, state=state, session=session
        )

    def get_previous_start_date(
        self,
        state: DagRunState | None = None,
        session: Session | None = None,
    ) -> pendulum.DateTime | None:
        """
        Return the execution date from property previous_ti_success.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session
        """
        from airflow.models.taskinstance import _get_previous_start_date

        return _get_previous_start_date(task_instance=self, state=state, session=session)

    def email_alert(self, exception, task: BaseOperator) -> None:
        """
        Send alert email with exception information.

        :param exception: the exception
        :param task: task related to the exception
        """
        from airflow.models.taskinstance import _email_alert

        _email_alert(task_instance=self, exception=exception, task=task)

    def get_email_subject_content(
        self, exception: BaseException, task: BaseOperator | None = None
    ) -> tuple[str, str, str]:
        """
        Get the email subject content for exceptions.

        :param exception: the exception sent in the email
        :param task:
        """
        from airflow.models.taskinstance import _get_email_subject_content

        return _get_email_subject_content(
            task_instance=self, exception=exception, task=task
        )

    def get_previous_ti(
        self,
        state: DagRunState | None = None,
        session: Session | None = None,
    ) -> TaskInstance | TaskInstancePydantic | None:
        """
        Return the task instance for the task that ran before this task instance.

        :param session: SQLAlchemy ORM Session
        :param state: If passed, it only take into account instances of a specific state.
        """
        from airflow.models.taskinstance import _get_previous_ti

        return _get_previous_ti(task_instance=self, state=state, session=session)

    def check_and_change_state_before_execution(
        self,
        verbose: bool = True,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        test_mode: bool = False,
        job_id: str | None = None,
        pool: str | None = None,
        external_executor_id: str | None = None,
        session: Session | None = None,
    ) -> bool:
        return TaskInstance._check_and_change_state_before_execution(
            task_instance=self,
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            test_mode=test_mode,
            hostname=get_hostname(),
            job_id=job_id,
            pool=pool,
            external_executor_id=external_executor_id,
            session=session,
        )

    def schedule_downstream_tasks(
        self, session: Session | None = None, max_tis_per_query: int | None = None
    ):
        """
        Schedule downstream tasks of this task instance.

        :meta: private
        """
        # we should not schedule downstream tasks with Pydantic model because it will not be able to
        # get the DAG object (we do not serialize it currently).
        return

    def command_as_list(
        self,
        mark_success: bool = False,
        ignore_all_deps: bool = False,
        ignore_task_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_ti_state: bool = False,
        local: bool = False,
        pickle_id: int | None = None,
        raw: bool = False,
        job_id: str | None = None,
        pool: str | None = None,
        cfg_path: str | None = None,
    ) -> list[str]:
        """
        Return a command that can be executed anywhere where airflow is installed.

        This command is part of the message sent to executors by the orchestrator.
        """
        return TaskInstance._command_as_list(
            ti=self,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path,
        )

    def _register_asset_changes(self, *, events, session: Session | None = None) -> None:
        TaskInstance._register_asset_changes(self=self, events=events, session=session)  # type: ignore[arg-type]

    def defer_task(self, exception: TaskDeferred, session: Session | None = None):
        """Defer task."""
        updated_ti = _defer_task(ti=self, exception=exception, session=session)
        _set_ti_attrs(self, updated_ti)

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
        _set_ti_attrs(
            self, updated_ti
        )  # _handle_reschedule is a remote call that mutates the TI

    def get_relevant_upstream_map_indexes(
        self,
        upstream: Operator,
        ti_count: int | None,
        *,
        session: Session | None = None,
    ) -> int | range | None:
        return TaskInstance.get_relevant_upstream_map_indexes(
            self=self,  # type: ignore[arg-type]
            upstream=upstream,
            ti_count=ti_count,
            session=session,
        )


TaskInstancePydantic.model_rebuild()
