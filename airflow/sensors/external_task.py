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

import datetime
import os
import warnings
from typing import TYPE_CHECKING, Any, Callable, Collection, FrozenSet, Iterable, Optional, Union

import attr
from sqlalchemy import func

from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.baseoperator import BaseOperatorLink
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.helpers import build_airflow_url_with_query
from airflow.utils.session import provide_session
from airflow.utils.state import State


class ExternalDagLink(BaseOperatorLink):
    """
    Operator link for ExternalTaskSensor and ExternalTaskMarker.
    It allows users to access DAG waited with ExternalTaskSensor or cleared by ExternalTaskMarker.
    """

    name = 'External DAG'

    def get_link(self, operator, dttm):
        ti = TaskInstance(task=operator, execution_date=dttm)
        operator.render_template_fields(ti.get_template_context())
        query = {"dag_id": operator.external_dag_id, "execution_date": dttm.isoformat()}
        return build_airflow_url_with_query(query)


class ExternalTaskSensor(BaseSensorOperator):
    """
    Waits for a different DAG or a task in a different DAG to complete for a
    specific logical date.

    By default the ExternalTaskSensor will wait for the external task to
    succeed, at which point it will also succeed. However, by default it will
    *not* fail if the external task fails, but will continue to check the status
    until the sensor times out (thus giving you time to retry the external task
    without also having to clear the sensor).

    It is possible to alter the default behavior by setting states which
    cause the sensor to fail, e.g. by setting ``allowed_states=[State.FAILED]``
    and ``failed_states=[State.SUCCESS]`` you will flip the behaviour to get a
    sensor which goes green when the external task *fails* and immediately goes
    red if the external task *succeeds*!

    Note that ``soft_fail`` is respected when examining the failed_states. Thus
    if the external task enters a failed state and ``soft_fail == True`` the
    sensor will _skip_ rather than fail. As a result, setting ``soft_fail=True``
    and ``failed_states=[State.SKIPPED]`` will result in the sensor skipping if
    the external task skips.

    :param external_dag_id: The dag_id that contains the task you want to
        wait for
    :param external_task_id: The task_id that contains the task you want to
        wait for. If ``None`` (default value) the sensor waits for the DAG
    :param external_task_ids: The list of task_ids that you want to wait for.
        If ``None`` (default value) the sensor waits for the DAG. Either
        external_task_id or external_task_ids can be passed to
        ExternalTaskSensor, but not both.
    :param allowed_states: Iterable of allowed states, default is ``['success']``
    :param failed_states: Iterable of failed or dis-allowed states, default is ``None``
    :param execution_delta: time difference with the previous execution to
        look at, the default is the same logical date as the current task or DAG.
        For yesterday, use [positive!] datetime.timedelta(days=1). Either
        execution_delta or execution_date_fn can be passed to
        ExternalTaskSensor, but not both.
    :param execution_date_fn: function that receives the current execution's logical date as the first
        positional argument and optionally any number of keyword arguments available in the
        context dictionary, and returns the desired logical dates to query.
        Either execution_delta or execution_date_fn can be passed to ExternalTaskSensor,
        but not both.
    :param check_existence: Set to `True` to check if the external task exists (when
        external_task_id is not None) or check if the DAG to wait for exists (when
        external_task_id is None), and immediately cease waiting if the external task
        or DAG does not exist (default value: False).
    """

    template_fields = ['external_dag_id', 'external_task_id', 'external_task_ids']
    ui_color = '#19647e'

    @property
    def operator_extra_links(self):
        """Return operator extra links"""
        return [ExternalDagLink()]

    def __init__(
        self,
        *,
        external_dag_id: str,
        external_task_id: Optional[str] = None,
        external_task_ids: Optional[Collection[str]] = None,
        allowed_states: Optional[Iterable[str]] = None,
        failed_states: Optional[Iterable[str]] = None,
        execution_delta: Optional[datetime.timedelta] = None,
        execution_date_fn: Optional[Callable] = None,
        check_existence: bool = False,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.allowed_states = list(allowed_states) if allowed_states else [State.SUCCESS]
        self.failed_states = list(failed_states) if failed_states else []

        total_states = set(self.allowed_states + self.failed_states)

        if set(self.failed_states).intersection(set(self.allowed_states)):
            raise AirflowException(
                f"Duplicate values provided as allowed "
                f"`{self.allowed_states}` and failed states `{self.failed_states}`"
            )

        if external_task_id is not None and external_task_ids is not None:
            raise ValueError(
                'Only one of `external_task_id` or `external_task_ids` may '
                'be provided to ExternalTaskSensor; not both.'
            )

        if external_task_id is not None:
            external_task_ids = [external_task_id]

        if external_task_ids:
            if not total_states <= set(State.task_states):
                raise ValueError(
                    f'Valid values for `allowed_states` and `failed_states` '
                    f'when `external_task_id` or `external_task_ids` is not `None`: {State.task_states}'
                )
            if len(external_task_ids) > len(set(external_task_ids)):
                raise ValueError('Duplicate task_ids passed in external_task_ids parameter')
        elif not total_states <= set(State.dag_states):
            raise ValueError(
                f'Valid values for `allowed_states` and `failed_states` '
                f'when `external_task_id` is `None`: {State.dag_states}'
            )

        if execution_delta is not None and execution_date_fn is not None:
            raise ValueError(
                'Only one of `execution_delta` or `execution_date_fn` may '
                'be provided to ExternalTaskSensor; not both.'
            )

        self.execution_delta = execution_delta
        self.execution_date_fn = execution_date_fn
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.external_task_ids = external_task_ids
        self.check_existence = check_existence
        self._has_checked_existence = False

    @provide_session
    def poke(self, context, session=None):
        if self.execution_delta:
            dttm = context['logical_date'] - self.execution_delta
        elif self.execution_date_fn:
            dttm = self._handle_execution_date_fn(context=context)
        else:
            dttm = context['logical_date']

        dttm_filter = dttm if isinstance(dttm, list) else [dttm]
        serialized_dttm_filter = ','.join(dt.isoformat() for dt in dttm_filter)

        self.log.info(
            'Poking for tasks %s in dag %s on %s ... ',
            self.external_task_ids,
            self.external_dag_id,
            serialized_dttm_filter,
        )

        # In poke mode this will check dag existence only once
        if self.check_existence and not self._has_checked_existence:
            self._check_for_existence(session=session)

        count_allowed = self.get_count(dttm_filter, session, self.allowed_states)

        count_failed = -1
        if self.failed_states:
            count_failed = self.get_count(dttm_filter, session, self.failed_states)

        if count_failed == len(dttm_filter):
            if self.external_task_ids:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f'Some of the external tasks {self.external_task_ids} '
                        f'in DAG {self.external_dag_id} failed. Skipping due to soft_fail.'
                    )
                raise AirflowException(
                    f'Some of the external tasks {self.external_task_ids} '
                    f'in DAG {self.external_dag_id} failed.'
                )
            else:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f'The external DAG {self.external_dag_id} failed. Skipping due to soft_fail.'
                    )
                raise AirflowException(f'The external DAG {self.external_dag_id} failed.')

        return count_allowed == len(dttm_filter)

    def _check_for_existence(self, session) -> None:
        dag_to_wait = session.query(DagModel).filter(DagModel.dag_id == self.external_dag_id).first()

        if not dag_to_wait:
            raise AirflowException(f'The external DAG {self.external_dag_id} does not exist.')

        if not os.path.exists(dag_to_wait.fileloc):
            raise AirflowException(f'The external DAG {self.external_dag_id} was deleted.')

        if self.external_task_ids:
            refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(self.external_dag_id)
            for external_task_id in self.external_task_ids:
                if not refreshed_dag_info.has_task(external_task_id):
                    raise AirflowException(
                        f'The external task {external_task_id} in '
                        f'DAG {self.external_dag_id} does not exist.'
                    )
        self._has_checked_existence = True

    def get_count(self, dttm_filter, session, states) -> int:
        """
        Get the count of records against dttm filter and states

        :param dttm_filter: date time filter for execution date
        :param session: airflow session object
        :param states: task or dag states
        :return: count of record against the filters
        """
        TI = TaskInstance
        DR = DagRun
        if not dttm_filter:
            return 0

        if self.external_task_ids:
            count = (
                session.query(func.count())  # .count() is inefficient
                .filter(
                    TI.dag_id == self.external_dag_id,
                    TI.task_id.in_(self.external_task_ids),
                    TI.state.in_(states),
                    TI.execution_date.in_(dttm_filter),
                )
                .scalar()
            )
            count = count / len(self.external_task_ids)
        else:
            count = (
                session.query(func.count())
                .filter(
                    DR.dag_id == self.external_dag_id,
                    DR.state.in_(states),
                    DR.execution_date.in_(dttm_filter),
                )
                .scalar()
            )
        return count

    def _handle_execution_date_fn(self, context) -> Any:
        """
        This function is to handle backwards compatibility with how this operator was
        previously where it only passes the execution date, but also allow for the newer
        implementation to pass all context variables as keyword arguments, to allow
        for more sophisticated returns of dates to return.
        """
        from airflow.utils.operator_helpers import make_kwargs_callable

        # Remove "logical_date" because it is already a mandatory positional argument
        logical_date = context["logical_date"]
        kwargs = {k: v for k, v in context.items() if k not in {"execution_date", "logical_date"}}
        # Add "context" in the kwargs for backward compatibility (because context used to be
        # an acceptable argument of execution_date_fn)
        kwargs["context"] = context
        if TYPE_CHECKING:
            assert self.execution_date_fn is not None
        kwargs_callable = make_kwargs_callable(self.execution_date_fn)
        return kwargs_callable(logical_date, **kwargs)


class ExternalTaskMarker(EmptyOperator):
    """
    Use this operator to indicate that a task on a different DAG depends on this task.
    When this task is cleared with "Recursive" selected, Airflow will clear the task on
    the other DAG and its downstream tasks recursively. Transitive dependencies are followed
    until the recursion_depth is reached.

    :param external_dag_id: The dag_id that contains the dependent task that needs to be cleared.
    :param external_task_id: The task_id of the dependent task that needs to be cleared.
    :param execution_date: The logical date of the dependent task execution that needs to be cleared.
    :param recursion_depth: The maximum level of transitive dependencies allowed. Default is 10.
        This is mostly used for preventing cyclic dependencies. It is fine to increase
        this number if necessary. However, too many levels of transitive dependencies will make
        it slower to clear tasks in the web UI.
    """

    template_fields = ['external_dag_id', 'external_task_id', 'execution_date']
    ui_color = '#19647e'

    # The _serialized_fields are lazily loaded when get_serialized_fields() method is called
    __serialized_fields: Optional[FrozenSet[str]] = None

    @property
    def operator_extra_links(self):
        """Return operator extra links"""
        return [ExternalDagLink()]

    def __init__(
        self,
        *,
        external_dag_id: str,
        external_task_id: str,
        execution_date: Optional[Union[str, datetime.datetime]] = "{{ logical_date.isoformat() }}",
        recursion_depth: int = 10,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        if isinstance(execution_date, datetime.datetime):
            self.execution_date = execution_date.isoformat()
        elif isinstance(execution_date, str):
            self.execution_date = execution_date
        else:
            raise TypeError(
                f'Expected str or datetime.datetime type for execution_date. Got {type(execution_date)}'
            )

        if recursion_depth <= 0:
            raise ValueError("recursion_depth should be a positive integer")
        self.recursion_depth = recursion_depth

    @classmethod
    def get_serialized_fields(cls):
        """Serialized ExternalTaskMarker contain exactly these fields + templated_fields ."""
        if not cls.__serialized_fields:
            cls.__serialized_fields = frozenset(super().get_serialized_fields() | {"recursion_depth"})
        return cls.__serialized_fields


@attr.s(auto_attribs=True)
class ExternalTaskSensorLink(ExternalDagLink):
    """
    This external link is deprecated.
    Please use :class:`airflow.sensors.external_task.ExternalDagLink`.
    """

    def __attrs_post_init__(self):
        warnings.warn(
            "This external link is deprecated. "
            "Please use :class:`airflow.sensors.external_task.ExternalDagLink`.",
            DeprecationWarning,
            stacklevel=2,
        )
