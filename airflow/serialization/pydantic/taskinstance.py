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
from typing import TYPE_CHECKING, Any, Iterable

import dill
import pendulum
from pydantic import BaseModel as BaseModelPydantic
from sqlalchemy.orm import Session

from airflow.models import DagRun, Operator
from airflow.models.taskinstance import (
    TaskInstance,
    _clear_next_method_args,
    _email_alert,
    _execute_task,
    _get_email_subject_content,
    _get_previous_dagrun,
    _get_previous_execution_date,
    _get_previous_ti,
    _get_template_context,
    _get_try_number,
    _handle_failure,
    _is_eligible_to_retry,
    _refresh_from_db,
    _refresh_from_task,
    _set_duration,
    _set_try_number,
    _stats_tags,
    hybrid_property,
)
from airflow.serialization.pydantic.dag_run import DagRunPydantic
from airflow.utils.context import Context
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import ExecutorConfigType
from airflow.utils.state import DagRunState
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator


class TaskInstancePydantic(BaseModelPydantic):
    """Serializable representation of the TaskInstance ORM SqlAlchemyModel used by internal API."""

    task_id: str
    dag_id: str
    run_id: str
    map_index: int
    start_date: datetime
    end_date: datetime | None
    execution_date: datetime
    duration: float | None
    state: str | None
    _try_number: int
    max_tries: int
    hostname: str
    unixname: str
    job_id: int | None
    pool: str
    pool_slots: int
    queue: str
    priority_weight: int | None
    operator: str
    queued_dttm: str | None
    queued_by_job_id: int | None
    pid: int | None
    executor_config = ExecutorConfigType(pickler=dill)
    updated_at: datetime | None
    external_executor_id: str | None
    trigger_id: int | None
    trigger_timeout: datetime | None
    next_method: str | None
    next_kwargs: dict | None
    run_as_user: str | None
    task: Operator
    test_mode: bool

    class Config:
        """Make sure it deals automatically with SQLAlchemy ORM classes."""

        orm_mode = True

    def xcom_pull(
        self,
        task_ids: str | Iterable[str] | None = None,
        dag_id: str | None = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        *,
        map_indexes: int | Iterable[int] | None = None,
        default: Any = None,
    ) -> Any:
        """
        Pull an XCom value for this task instance.

        TODO: make it works for AIP-44
        :param task_ids: task id or list of task ids, if None, the task_id of the current task is used
        :param dag_id: dag id, if None, the dag_id of the current task is used
        :param key: the key to identify the XCom value
        :param include_prior_dates: whether to include prior execution dates
        :param map_indexes: map index or list of map indexes, if None, the map_index of the current task
            is used
        :param default: the default value to return if the XCom value does not exist
        :return: Xcom value
        """
        return None

    @provide_session
    def xcom_push(
        self,
        key: str,
        value: Any,
        execution_date: datetime | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Push an XCom value for this task instance.

        TODO: make it works for AIP-44
        :param key: the key to identify the XCom value
        :param value: the value of the XCom
        :param execution_date: the execution date to push the XCom for
        """
        pass

    @provide_session
    def get_dagrun(self, session: Session = NEW_SESSION) -> DagRunPydantic:
        """
        Returns the DagRun for this TaskInstance

        :param session: SQLAlchemy ORM Session

        TODO: make it works for AIP-44

        :return: Pydantic serialized version of DaGrun
        """
        raise NotImplementedError()

    def _execute_task(self, context, task_orig):
        """
        Executes Task (optionally with a Timeout) and pushes Xcom results.

        :param context: Jinja2 context
        :param task_orig: origin task
        """
        return _execute_task(self, context, task_orig)

    @provide_session
    def refresh_from_db(self, session: Session = NEW_SESSION, lock_for_update: bool = False) -> None:
        """
        Refreshes the task instance from the database based on the primary key

        :param session: SQLAlchemy ORM Session
        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        """
        _refresh_from_db(self, session, lock_for_update)

    def set_duration(self) -> None:
        """Set task instance duration."""
        _set_duration(self)

    @property
    def stats_tags(self) -> dict[str, str]:
        """Returns task instance tags."""
        return _stats_tags(self)

    def clear_next_method_args(self) -> None:
        """Ensure we unset next_method and next_kwargs to ensure that any retries don't re-use them."""
        _clear_next_method_args(self)

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
        return _get_template_context(
            task_instance=self,
            session=session,
            ignore_param_exceptions=ignore_param_exceptions,
        )

    def is_eligible_to_retry(self):
        """Is task instance is eligible for retry."""
        return _is_eligible_to_retry(self)

    @provide_session
    def handle_failure(
        self,
        error: None | str | Exception | KeyboardInterrupt,
        test_mode: bool | None = None,
        context: Context | None = None,
        force_fail: bool = False,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Handle Failure for a task instance.

        :param error: if specified, log the specific exception if thrown
        :param session: SQLAlchemy ORM Session
        :param test_mode: doesn't record success or failure in the DB if True
        :param context: Jinja2 context
        :param force_fail: if True, task does not retry
        """
        _handle_failure(
            task_instance=self,
            error=error,
            session=session,
            test_mode=test_mode,
            context=context,
            force_fail=force_fail,
        )

    @hybrid_property
    def try_number(self):
        """
        Return the try number that a task number will be when it is actually run.

        If the TaskInstance is currently running, this will match the column in the
        database, in all other cases this will be incremented.

        This is designed so that task logs end up in the right file.
        """
        return _get_try_number(self)

    @try_number.setter
    def try_number(self, value: int) -> None:
        """
        Set a task try number.

        :param value: the try number
        """
        _set_try_number(self, value)

    def refresh_from_task(self, task: Operator, pool_override: str | None = None) -> None:
        """
        Copy common attributes from the given task.

        :param task: The task object to copy from
        :param pool_override: Use the pool_override instead of task's pool
        """
        _refresh_from_task(self, task, pool_override)

    @provide_session
    def get_previous_dagrun(
        self,
        state: DagRunState | None = None,
        session: Session | None = None,
    ) -> DagRun | None:
        """
        The DagRun that ran before this task instance's DagRun.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session.
        """
        return _get_previous_dagrun(self, state, session)

    @provide_session
    def get_previous_execution_date(
        self,
        state: DagRunState | None = None,
        session: Session = NEW_SESSION,
    ) -> pendulum.DateTime | None:
        """
        The execution date from property previous_ti_success.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session
        """
        return _get_previous_execution_date(self, state, session)

    def email_alert(self, exception, task: BaseOperator) -> None:
        """
        Send alert email with exception information.

        :param exception: the exception
        :param task: task related to the exception
        """
        _email_alert(self, exception, task)

    def get_email_subject_content(
        self, exception: BaseException, task: BaseOperator | None = None
    ) -> tuple[str, str, str]:
        """
        Get the email subject content for exceptions.

        :param exception: the exception sent in the email
        :param task:
        """
        return _get_email_subject_content(self, exception, task)

    @provide_session
    def get_previous_ti(
        self,
        state: DagRunState | None = None,
        session: Session = NEW_SESSION,
    ) -> TaskInstance | None:
        """
        The task instance for the task that ran before this task instance.

        :param session: SQLAlchemy ORM Session
        :param state: If passed, it only take into account instances of a specific state.
        """
        return _get_previous_ti(self, state, session)
