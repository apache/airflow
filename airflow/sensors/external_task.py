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

import datetime
import os
import warnings
from typing import TYPE_CHECKING, Any, Callable, Collection, Iterable

from airflow.configuration import conf
from airflow.exceptions import AirflowException, AirflowSkipException
from airflow.models.baseoperatorlink import BaseOperatorLink
from airflow.models.dag import DagModel
from airflow.models.dagbag import DagBag
from airflow.models.taskinstance import TaskInstance
from airflow.operators.empty import EmptyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.triggers.external_task import WorkflowTrigger
from airflow.utils.file import correct_maybe_zipped
from airflow.utils.helpers import build_airflow_url_with_query
from airflow.utils.sensor_helper import _get_count, _get_external_task_group_task_ids
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import State, TaskInstanceState

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.baseoperator import BaseOperator
    from airflow.models.taskinstancekey import TaskInstanceKey
    from airflow.utils.context import Context


class ExternalDagLink(BaseOperatorLink):
    """
    Operator link for ExternalTaskSensor and ExternalTaskMarker.

    It allows users to access DAG waited with ExternalTaskSensor or cleared by ExternalTaskMarker.
    """

    name = "External DAG"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey) -> str:
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        ti = TaskInstance.get_task_instance(
            dag_id=ti_key.dag_id, run_id=ti_key.run_id, task_id=ti_key.task_id, map_index=ti_key.map_index
        )

        if TYPE_CHECKING:
            assert ti is not None

        template_fields = RenderedTaskInstanceFields.get_templated_fields(ti)
        external_dag_id = (
            template_fields["external_dag_id"] if template_fields else operator.external_dag_id  # type: ignore[attr-defined]
        )
        query = {
            "dag_id": external_dag_id,
            "execution_date": ti.execution_date.isoformat(),  # type: ignore[union-attr]
        }

        return build_airflow_url_with_query(query)


class ExternalTaskSensor(BaseSensorOperator):
    """
    Waits for a different DAG, task group, or task to complete for a specific logical date.

    If both `external_task_group_id` and `external_task_id` are ``None`` (default), the sensor
    waits for the DAG.
    Values for `external_task_group_id` and `external_task_id` can't be set at the same time.

    By default, the ExternalTaskSensor will wait for the external task to
    succeed, at which point it will also succeed. However, by default it will
    *not* fail if the external task fails, but will continue to check the status
    until the sensor times out (thus giving you time to retry the external task
    without also having to clear the sensor).

    By default, the ExternalTaskSensor will not skip if the external task skips.
    To change this, simply set ``skipped_states=[TaskInstanceState.SKIPPED]``.
    Note that if you are monitoring multiple tasks, and one enters error state
    and the other enters a skipped state, then the external task will react to
    whichever one it sees first. If both happen together, then the failed state
    takes priority.

    It is possible to alter the default behavior by setting states which
    cause the sensor to fail, e.g. by setting ``allowed_states=[DagRunState.FAILED]``
    and ``failed_states=[DagRunState.SUCCESS]`` you will flip the behaviour to
    get a sensor which goes green when the external task *fails* and immediately
    goes red if the external task *succeeds*!

    Note that ``soft_fail`` is respected when examining the failed_states. Thus
    if the external task enters a failed state and ``soft_fail == True`` the
    sensor will _skip_ rather than fail. As a result, setting ``soft_fail=True``
    and ``failed_states=[DagRunState.SKIPPED]`` will result in the sensor
    skipping if the external task skips. However, this is a contrived
    example---consider using ``skipped_states`` if you would like this
    behaviour. Using ``skipped_states`` allows the sensor to skip if the target
    fails, but still enter failed state on timeout. Using ``soft_fail == True``
    as above will cause the sensor to skip if the target fails, but also if it
    times out.

    :param external_dag_id: The dag_id that contains the task you want to
        wait for. (templated)
    :param external_task_id: The task_id that contains the task you want to
        wait for. (templated)
    :param external_task_ids: The list of task_ids that you want to wait for. (templated)
        If ``None`` (default value) the sensor waits for the DAG. Either
        external_task_id or external_task_ids can be passed to
        ExternalTaskSensor, but not both.
    :param external_task_group_id: The task_group_id that contains the task you want to
        wait for. (templated)
    :param allowed_states: Iterable of allowed states, default is ``['success']``
    :param skipped_states: Iterable of states to make this task mark as skipped, default is ``None``
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
    :param poll_interval: polling period in seconds to check for the status
    :param deferrable: Run sensor in deferrable mode
    """

    template_fields = ["external_dag_id", "external_task_id", "external_task_ids", "external_task_group_id"]
    ui_color = "#4db7db"
    operator_extra_links = [ExternalDagLink()]

    def __init__(
        self,
        *,
        external_dag_id: str,
        external_task_id: str | None = None,
        external_task_ids: Collection[str] | None = None,
        external_task_group_id: str | None = None,
        allowed_states: Iterable[str] | None = None,
        skipped_states: Iterable[str] | None = None,
        failed_states: Iterable[str] | None = None,
        execution_delta: datetime.timedelta | None = None,
        execution_date_fn: Callable | None = None,
        check_existence: bool = False,
        poll_interval: float = 2.0,
        deferrable: bool = conf.getboolean("operators", "default_deferrable", fallback=False),
        **kwargs,
    ):
        super().__init__(**kwargs)

        self.allowed_states = list(allowed_states) if allowed_states else [TaskInstanceState.SUCCESS.value]
        self.skipped_states = list(skipped_states) if skipped_states else []
        self.failed_states = list(failed_states) if failed_states else []

        total_states = set(self.allowed_states + self.skipped_states + self.failed_states)

        if len(total_states) != len(self.allowed_states) + len(self.skipped_states) + len(self.failed_states):
            raise AirflowException(
                "Duplicate values provided across allowed_states, skipped_states and failed_states."
            )

        # convert [] to None
        if not external_task_ids:
            external_task_ids = None

        # can't set both single task id and a list of task ids
        if external_task_id is not None and external_task_ids is not None:
            raise ValueError(
                "Only one of `external_task_id` or `external_task_ids` may "
                "be provided to ExternalTaskSensor; "
                "use external_task_id or external_task_ids or external_task_group_id."
            )

        # since both not set, convert the single id to a 1-elt list - from here on, we only consider the list
        if external_task_id is not None:
            external_task_ids = [external_task_id]

        if external_task_group_id is not None and external_task_ids is not None:
            raise ValueError(
                "Only one of `external_task_group_id` or `external_task_ids` may "
                "be provided to ExternalTaskSensor; "
                "use external_task_id or external_task_ids or external_task_group_id."
            )

        # check the requested states are all valid states for the target type, be it dag or task
        if external_task_ids or external_task_group_id:
            if not total_states <= set(State.task_states):
                raise ValueError(
                    "Valid values for `allowed_states`, `skipped_states` and `failed_states` "
                    "when `external_task_id` or `external_task_ids` or `external_task_group_id` "
                    f"is not `None`: {State.task_states}"
                )

        elif not total_states <= set(State.dag_states):
            raise ValueError(
                "Valid values for `allowed_states`, `skipped_states` and `failed_states` "
                f"when `external_task_id` and `external_task_group_id` is `None`: {State.dag_states}"
            )

        if execution_delta is not None and execution_date_fn is not None:
            raise ValueError(
                "Only one of `execution_delta` or `execution_date_fn` may "
                "be provided to ExternalTaskSensor; not both."
            )

        self.execution_delta = execution_delta
        self.execution_date_fn = execution_date_fn
        self.external_dag_id = external_dag_id
        self.external_task_id = external_task_id
        self.external_task_ids = external_task_ids
        self.external_task_group_id = external_task_group_id
        self.check_existence = check_existence
        self._has_checked_existence = False
        self.deferrable = deferrable
        self.poll_interval = poll_interval

    def _get_dttm_filter(self, context):
        if self.execution_delta:
            dttm = context["logical_date"] - self.execution_delta
        elif self.execution_date_fn:
            dttm = self._handle_execution_date_fn(context=context)
        else:
            dttm = context["logical_date"]
        return dttm if isinstance(dttm, list) else [dttm]

    @provide_session
    def poke(self, context: Context, session: Session = NEW_SESSION) -> bool:
        # delay check to poke rather than __init__ in case it was supplied as XComArgs
        if self.external_task_ids and len(self.external_task_ids) > len(set(self.external_task_ids)):
            raise ValueError("Duplicate task_ids passed in external_task_ids parameter")

        dttm_filter = self._get_dttm_filter(context)
        serialized_dttm_filter = ",".join(dt.isoformat() for dt in dttm_filter)

        if self.external_task_ids:
            self.log.info(
                "Poking for tasks %s in dag %s on %s ... ",
                self.external_task_ids,
                self.external_dag_id,
                serialized_dttm_filter,
            )

        if self.external_task_group_id:
            self.log.info(
                "Poking for task_group '%s' in dag '%s' on %s ... ",
                self.external_task_group_id,
                self.external_dag_id,
                serialized_dttm_filter,
            )

        if self.external_dag_id and not self.external_task_group_id and not self.external_task_ids:
            self.log.info(
                "Poking for DAG '%s' on %s ... ",
                self.external_dag_id,
                serialized_dttm_filter,
            )

        # In poke mode this will check dag existence only once
        if self.check_existence and not self._has_checked_existence:
            self._check_for_existence(session=session)

        count_failed = -1
        if self.failed_states:
            count_failed = self.get_count(dttm_filter, session, self.failed_states)

        # Fail if anything in the list has failed.
        if count_failed > 0:
            if self.external_task_ids:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f"Some of the external tasks {self.external_task_ids} "
                        f"in DAG {self.external_dag_id} failed. Skipping due to soft_fail."
                    )
                raise AirflowException(
                    f"Some of the external tasks {self.external_task_ids} "
                    f"in DAG {self.external_dag_id} failed."
                )
            elif self.external_task_group_id:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f"The external task_group '{self.external_task_group_id}' "
                        f"in DAG '{self.external_dag_id}' failed. Skipping due to soft_fail."
                    )
                raise AirflowException(
                    f"The external task_group '{self.external_task_group_id}' "
                    f"in DAG '{self.external_dag_id}' failed."
                )

            else:
                if self.soft_fail:
                    raise AirflowSkipException(
                        f"The external DAG {self.external_dag_id} failed. Skipping due to soft_fail."
                    )
                raise AirflowException(f"The external DAG {self.external_dag_id} failed.")

        count_skipped = -1
        if self.skipped_states:
            count_skipped = self.get_count(dttm_filter, session, self.skipped_states)

        # Skip if anything in the list has skipped. Note if we are checking multiple tasks and one skips
        # before another errors, we'll skip first.
        if count_skipped > 0:
            if self.external_task_ids:
                raise AirflowSkipException(
                    f"Some of the external tasks {self.external_task_ids} "
                    f"in DAG {self.external_dag_id} reached a state in our states-to-skip-on list. Skipping."
                )
            elif self.external_task_group_id:
                raise AirflowSkipException(
                    f"The external task_group '{self.external_task_group_id}' "
                    f"in DAG {self.external_dag_id} reached a state in our states-to-skip-on list. Skipping."
                )
            else:
                raise AirflowSkipException(
                    f"The external DAG {self.external_dag_id} reached a state in our states-to-skip-on list. "
                    "Skipping."
                )

        # only go green if every single task has reached an allowed state
        count_allowed = self.get_count(dttm_filter, session, self.allowed_states)
        return count_allowed == len(dttm_filter)

    def execute(self, context: Context) -> None:
        """Run on the worker and defer using the triggers if deferrable is set to True."""
        if not self.deferrable:
            super().execute(context)
        else:
            self.defer(
                timeout=self.execution_timeout,
                trigger=WorkflowTrigger(
                    external_dag_id=self.external_dag_id,
                    external_task_group_id=self.external_task_group_id,
                    external_task_ids=self.external_task_ids,
                    execution_dates=self._get_dttm_filter(context),
                    allowed_states=self.allowed_states,
                    poke_interval=self.poll_interval,
                    soft_fail=self.soft_fail,
                ),
                method_name="execute_complete",
            )

    def execute_complete(self, context, event=None):
        """Execute when the trigger fires - return immediately."""
        if event["status"] == "success":
            self.log.info("External tasks %s has executed successfully.", self.external_task_ids)
        elif event["status"] == "skipped":
            raise AirflowSkipException("External job has skipped skipping.")
        else:
            if self.soft_fail:
                raise AirflowSkipException("External job has failed skipping.")
            else:
                raise AirflowException(
                    "Error occurred while trying to retrieve task status. Please, check the "
                    "name of executed task and Dag."
                )

    def _check_for_existence(self, session) -> None:
        dag_to_wait = DagModel.get_current(self.external_dag_id, session)

        if not dag_to_wait:
            raise AirflowException(f"The external DAG {self.external_dag_id} does not exist.")

        if not os.path.exists(correct_maybe_zipped(dag_to_wait.fileloc)):
            raise AirflowException(f"The external DAG {self.external_dag_id} was deleted.")

        if self.external_task_ids:
            refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(self.external_dag_id)
            for external_task_id in self.external_task_ids:
                if not refreshed_dag_info.has_task(external_task_id):
                    raise AirflowException(
                        f"The external task {external_task_id} in "
                        f"DAG {self.external_dag_id} does not exist."
                    )

        if self.external_task_group_id:
            refreshed_dag_info = DagBag(dag_to_wait.fileloc).get_dag(self.external_dag_id)
            if not refreshed_dag_info.has_task_group(self.external_task_group_id):
                raise AirflowException(
                    f"The external task group '{self.external_task_group_id}' in "
                    f"DAG '{self.external_dag_id}' does not exist."
                )

        self._has_checked_existence = True

    def get_count(self, dttm_filter, session, states) -> int:
        """
        Get the count of records against dttm filter and states.

        :param dttm_filter: date time filter for execution date
        :param session: airflow session object
        :param states: task or dag states
        :return: count of record against the filters
        """
        warnings.warn(
            "This method is deprecated and will be removed in future.", DeprecationWarning, stacklevel=2
        )
        return _get_count(
            dttm_filter,
            self.external_task_ids,
            self.external_task_group_id,
            self.external_dag_id,
            states,
            session,
        )

    def get_external_task_group_task_ids(self, session, dttm_filter):
        warnings.warn(
            "This method is deprecated and will be removed in future.", DeprecationWarning, stacklevel=2
        )
        return _get_external_task_group_task_ids(
            dttm_filter, self.external_task_group_id, self.external_dag_id, session
        )

    def _handle_execution_date_fn(self, context) -> Any:
        """
        Handle backward compatibility.

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

    template_fields = ["external_dag_id", "external_task_id", "execution_date"]
    ui_color = "#4db7db"
    operator_extra_links = [ExternalDagLink()]

    # The _serialized_fields are lazily loaded when get_serialized_fields() method is called
    __serialized_fields: frozenset[str] | None = None

    def __init__(
        self,
        *,
        external_dag_id: str,
        external_task_id: str,
        execution_date: str | datetime.datetime | None = "{{ logical_date.isoformat() }}",
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
                f"Expected str or datetime.datetime type for execution_date. Got {type(execution_date)}"
            )

        if recursion_depth <= 0:
            raise ValueError("recursion_depth should be a positive integer")
        self.recursion_depth = recursion_depth

    @classmethod
    def get_serialized_fields(cls):
        """Serialize ExternalTaskMarker to contain exactly these fields + templated_fields ."""
        if not cls.__serialized_fields:
            cls.__serialized_fields = frozenset(super().get_serialized_fields() | {"recursion_depth"})
        return cls.__serialized_fields
