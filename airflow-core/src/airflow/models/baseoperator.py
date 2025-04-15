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
"""
Base operator for all operators.

:sphinx-autoapi-skip:
"""

from __future__ import annotations

import functools
import logging
import operator
from collections.abc import Collection, Iterable, Iterator
from datetime import datetime
from functools import singledispatchmethod
from typing import TYPE_CHECKING, Any

import pendulum
from sqlalchemy import select
from sqlalchemy.orm.exc import NoResultFound

# Keeping this file at all is a temp thing as we migrate the repo to the task sdk as the base, but to keep
# main working and useful for others to develop against we use the TaskSDK here but keep this file around
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.sdk.bases.operator import (
    BaseOperator as TaskSDKBaseOperator,
    # Re-export for compat
    chain as chain,
    chain_linear as chain_linear,
    cross_downstream as cross_downstream,
    get_merged_defaults as get_merged_defaults,
)
from airflow.sdk.definitions._internal.abstractoperator import (
    AbstractOperator as TaskSDKAbstractOperator,
    NotMapped,
)
from airflow.sdk.definitions.mappedoperator import MappedOperator
from airflow.sdk.definitions.taskgroup import MappedTaskGroup, TaskGroup
from airflow.serialization.enums import DagAttributeTypes
from airflow.ti_deps.deps.mapped_task_upstream_dep import MappedTaskUpstreamDep
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.not_previously_skipped_dep import NotPreviouslySkippedDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils import timezone
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState
from airflow.utils.types import DagRunTriggeredByType
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from airflow.models.dag import DAG as SchedulerDAG
    from airflow.models.operator import Operator
    from airflow.sdk import BaseOperatorLink, Context
    from airflow.sdk.definitions._internal.node import DAGNode
    from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
    from airflow.triggers.base import StartTriggerArgs

logger = logging.getLogger("airflow.models.baseoperator.BaseOperator")


class BaseOperator(TaskSDKBaseOperator):
    r"""
    Abstract base class for all operators.

    Since operators create objects that become nodes in the DAG, BaseOperator
    contains many recursive methods for DAG crawling behavior. To derive from
    this class, you are expected to override the constructor and the 'execute'
    method.

    Operators derived from this class should perform or trigger certain tasks
    synchronously (wait for completion). Example of operators could be an
    operator that runs a Pig job (PigOperator), a sensor operator that
    waits for a partition to land in Hive (HiveSensorOperator), or one that
    moves data from Hive to MySQL (Hive2MySqlOperator). Instances of these
    operators (tasks) target specific operations, running specific scripts,
    functions or data transfers.

    This class is abstract and shouldn't be instantiated. Instantiating a
    class derived from this one results in the creation of a task object,
    which ultimately becomes a node in DAG objects. Task dependencies should
    be set by using the set_upstream and/or set_downstream methods.

    :param task_id: a unique, meaningful id for the task
    :param owner: the owner of the task. Using a meaningful description
        (e.g. user/person/team/role name) to clarify ownership is recommended.
    :param email: the 'to' email address(es) used in email alerts. This can be a
        single email or multiple ones. Multiple addresses can be specified as a
        comma or semicolon separated string or by passing a list of strings.
    :param email_on_retry: Indicates whether email alerts should be sent when a
        task is retried
    :param email_on_failure: Indicates whether email alerts should be sent when
        a task failed
    :param retries: the number of retries that should be performed before
        failing the task
    :param retry_delay: delay between retries, can be set as ``timedelta`` or
        ``float`` seconds, which will be converted into ``timedelta``,
        the default is ``timedelta(seconds=300)``.
    :param retry_exponential_backoff: allow progressively longer waits between
        retries by using exponential backoff algorithm on retry delay (delay
        will be converted into seconds)
    :param max_retry_delay: maximum delay interval between retries, can be set as
        ``timedelta`` or ``float`` seconds, which will be converted into ``timedelta``.
    :param start_date: The ``start_date`` for the task, determines
        the ``logical_date`` for the first task instance. The best practice
        is to have the start_date rounded
        to your DAG's schedule. Daily jobs have their start_date
        some day at 00:00:00, hourly jobs have their start_date at 00:00
        of a specific hour. Note that Airflow simply looks at the latest
        ``logical_date`` and adds the schedule to determine
        the next ``logical_date``. It is also very important
        to note that different tasks' dependencies
        need to line up in time. If task A depends on task B and their
        start_date are offset in a way that their logical_date don't line
        up, A's dependencies will never be met. If you are looking to delay
        a task, for example running a daily task at 2AM, look into the
        ``TimeSensor`` and ``TimeDeltaSensor``. We advise against using
        dynamic ``start_date`` and recommend using fixed ones. Read the
        FAQ entry about start_date for more information.
    :param end_date: if specified, the scheduler won't go beyond this date
    :param depends_on_past: when set to true, task instances will run
        sequentially and only if the previous instance has succeeded or has been skipped.
        The task instance for the start_date is allowed to run.
    :param wait_for_past_depends_before_skipping: when set to true, if the task instance
        should be marked as skipped, and depends_on_past is true, the ti will stay on None state
        waiting the task of the previous run
    :param wait_for_downstream: when set to true, an instance of task
        X will wait for tasks immediately downstream of the previous instance
        of task X to finish successfully or be skipped before it runs. This is useful if the
        different instances of a task X alter the same asset, and this asset
        is used by tasks downstream of task X. Note that depends_on_past
        is forced to True wherever wait_for_downstream is used. Also note that
        only tasks *immediately* downstream of the previous task instance are waited
        for; the statuses of any tasks further downstream are ignored.
    :param dag: a reference to the dag the task is attached to (if any)
    :param priority_weight: priority weight of this task against other task.
        This allows the executor to trigger higher priority tasks before
        others when things get backed up. Set priority_weight as a higher
        number for more important tasks.
        As not all database engines support 64-bit integers, values are capped with 32-bit.
        Valid range is from -2,147,483,648 to 2,147,483,647.
    :param weight_rule: weighting method used for the effective total
        priority weight of the task. Options are:
        ``{ downstream | upstream | absolute }`` default is ``downstream``
        When set to ``downstream`` the effective weight of the task is the
        aggregate sum of all downstream descendants. As a result, upstream
        tasks will have higher weight and will be scheduled more aggressively
        when using positive weight values. This is useful when you have
        multiple dag run instances and desire to have all upstream tasks to
        complete for all runs before each dag can continue processing
        downstream tasks. When set to ``upstream`` the effective weight is the
        aggregate sum of all upstream ancestors. This is the opposite where
        downstream tasks have higher weight and will be scheduled more
        aggressively when using positive weight values. This is useful when you
        have multiple dag run instances and prefer to have each dag complete
        before starting upstream tasks of other dags.  When set to
        ``absolute``, the effective weight is the exact ``priority_weight``
        specified without additional weighting. You may want to do this when
        you know exactly what priority weight each task should have.
        Additionally, when set to ``absolute``, there is bonus effect of
        significantly speeding up the task creation process as for very large
        DAGs. Options can be set as string or using the constants defined in
        the static class ``airflow.utils.WeightRule``.
        Irrespective of the weight rule, resulting priority values are capped with 32-bit.
        |experimental|
        Since 2.9.0, Airflow allows to define custom priority weight strategy,
        by creating a subclass of
        ``airflow.task.priority_strategy.PriorityWeightStrategy`` and registering
        in a plugin, then providing the class path or the class instance via
        ``weight_rule`` parameter. The custom priority weight strategy will be
        used to calculate the effective total priority weight of the task instance.
    :param queue: which queue to target when running this job. Not
        all executors implement queue management, the CeleryExecutor
        does support targeting specific queues.
    :param pool: the slot pool this task should run in, slot pools are a
        way to limit concurrency for certain tasks
    :param pool_slots: the number of pool slots this task should use (>= 1)
        Values less than 1 are not allowed.
    :param sla: DEPRECATED - The SLA feature is removed in Airflow 3.0, to be replaced with a new implementation in 3.1
    :param execution_timeout: max time allowed for the execution of
        this task instance, if it goes beyond it will raise and fail.
    :param on_failure_callback: a function or list of functions to be called when a task instance
        of this task fails. a context dictionary is passed as a single
        parameter to this function. Context contains references to related
        objects to the task instance and is documented under the macros
        section of the API.
    :param on_execute_callback: much like the ``on_failure_callback`` except
        that it is executed right before the task is executed.
    :param on_retry_callback: much like the ``on_failure_callback`` except
        that it is executed when retries occur.
    :param on_success_callback: much like the ``on_failure_callback`` except
        that it is executed when the task succeeds.
    :param on_skipped_callback: much like the ``on_failure_callback`` except
        that it is executed when skipped occur; this callback will be called only if AirflowSkipException get raised.
        Explicitly it is NOT called if a task is not started to be executed because of a preceding branching
        decision in the DAG or a trigger rule which causes execution to skip so that the task execution
        is never scheduled.
    :param pre_execute: a function to be called immediately before task
        execution, receiving a context dictionary; raising an exception will
        prevent the task from being executed.

        |experimental|
    :param post_execute: a function to be called immediately after task
        execution, receiving a context dictionary and task result; raising an
        exception will prevent the task from succeeding.

        |experimental|
    :param trigger_rule: defines the rule by which dependencies are applied
        for the task to get triggered. Options are:
        ``{ all_success | all_failed | all_done | all_skipped | one_success | one_done |
        one_failed | none_failed | none_failed_min_one_success | none_skipped | always}``
        default is ``all_success``. Options can be set as string or
        using the constants defined in the static class
        ``airflow.utils.TriggerRule``
    :param resources: A map of resource parameter names (the argument names of the
        Resources constructor) to their values.
    :param run_as_user: unix username to impersonate while running the task
    :param max_active_tis_per_dag: When set, a task will be able to limit the concurrent
        runs across logical_dates.
    :param max_active_tis_per_dagrun: When set, a task will be able to limit the concurrent
        task instances per DAG run.
    :param executor: Which executor to target when running this task. NOT YET SUPPORTED
    :param executor_config: Additional task-level configuration parameters that are
        interpreted by a specific executor. Parameters are namespaced by the name of
        executor.

        **Example**: to run this task in a specific docker container through
        the KubernetesExecutor ::

            MyOperator(..., executor_config={"KubernetesExecutor": {"image": "myCustomDockerImage"}})

    :param do_xcom_push: if True, an XCom is pushed containing the Operator's
        result
    :param multiple_outputs: if True and do_xcom_push is True, pushes multiple XComs, one for each
        key in the returned dictionary result. If False and do_xcom_push is True, pushes a single XCom.
    :param task_group: The TaskGroup to which the task should belong. This is typically provided when not
        using a TaskGroup as a context manager.
    :param doc: Add documentation or notes to your Task objects that is visible in
        Task Instance details View in the Webserver
    :param doc_md: Add documentation (in Markdown format) or notes to your Task objects
        that is visible in Task Instance details View in the Webserver
    :param doc_rst: Add documentation (in RST format) or notes to your Task objects
        that is visible in Task Instance details View in the Webserver
    :param doc_json: Add documentation (in JSON format) or notes to your Task objects
        that is visible in Task Instance details View in the Webserver
    :param doc_yaml: Add documentation (in YAML format) or notes to your Task objects
        that is visible in Task Instance details View in the Webserver
    :param task_display_name: The display name of the task which appears on the UI.
    :param logger_name: Name of the logger used by the Operator to emit logs.
        If set to `None` (default), the logger name will fall back to
        `airflow.task.operators.{class.__module__}.{class.__name__}` (e.g. SimpleHttpOperator will have
        *airflow.task.operators.airflow.providers.http.operators.http.SimpleHttpOperator* as logger).
    :param allow_nested_operators: if True, when an operator is executed within another one a warning message
        will be logged. If False, then an exception will be raised if the operator is badly used (e.g. nested
        within another one). In future releases of Airflow this parameter will be removed and an exception
        will always be thrown when operators are nested within each other (default is True).

        **Example**: example of a bad operator mixin usage::

            @task(provide_context=True)
            def say_hello_world(**context):
                hello_world_task = BashOperator(
                    task_id="hello_world_task",
                    bash_command="python -c \"print('Hello, world!')\"",
                    dag=dag,
                )
                hello_world_task.execute(context)
    """

    def __init__(self, **kwargs):
        if start_date := kwargs.get("start_date", None):
            kwargs["start_date"] = timezone.convert_to_utc(start_date)
        if end_date := kwargs.get("end_date", None):
            kwargs["end_date"] = timezone.convert_to_utc(end_date)
        super().__init__(**kwargs)

    # Defines the operator level extra links
    operator_extra_links: Collection[BaseOperatorLink] = ()

    if TYPE_CHECKING:

        @property  # type: ignore[override]
        def dag(self) -> SchedulerDAG:  # type: ignore[override]
            return super().dag  # type: ignore[return-value]

        @dag.setter
        def dag(self, val: SchedulerDAG):
            # For type checking only
            ...

    def get_inlet_defs(self):
        """
        Get inlet definitions on this task.

        :meta private:
        """
        return self.inlets

    def get_outlet_defs(self):
        """
        Get outlet definitions on this task.

        :meta private:
        """
        return self.outlets

    deps: frozenset[BaseTIDep] = frozenset(
        {
            NotInRetryPeriodDep(),
            PrevDagrunDep(),
            TriggerRuleDep(),
            NotPreviouslySkippedDep(),
            MappedTaskUpstreamDep(),
        }
    )
    """
    Returns the set of dependencies for the operator. These differ from execution
    context dependencies in that they are specific to tasks and can be
    extended/overridden by subclasses.
    """

    def execute(self, context: Context) -> Any:
        """
        Derive when creating an operator.

        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        raise NotImplementedError()

    @provide_session
    def clear(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        upstream: bool = False,
        downstream: bool = False,
        session: Session = NEW_SESSION,
    ):
        """Clear the state of task instances associated with the task, following the parameters specified."""
        qry = select(TaskInstance).where(TaskInstance.dag_id == self.dag_id)

        if start_date:
            qry = qry.where(TaskInstance.logical_date >= start_date)
        if end_date:
            qry = qry.where(TaskInstance.logical_date <= end_date)

        tasks = [self.task_id]

        if upstream:
            tasks += [t.task_id for t in self.get_flat_relatives(upstream=True)]

        if downstream:
            tasks += [t.task_id for t in self.get_flat_relatives(upstream=False)]

        qry = qry.where(TaskInstance.task_id.in_(tasks))
        results = session.scalars(qry).all()
        count = len(results)

        if TYPE_CHECKING:
            # TODO: Task-SDK: We need to set this to the scheduler DAG until we fully separate scheduling and
            # definition code
            assert isinstance(self.dag, SchedulerDAG)

        clear_task_instances(results, session, dag=self.dag)
        session.commit()
        return count

    @provide_session
    def get_task_instances(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        session: Session = NEW_SESSION,
    ) -> list[TaskInstance]:
        """Get task instances related to this task for a specific date range."""
        from airflow.models import DagRun

        query = (
            select(TaskInstance)
            .join(TaskInstance.dag_run)
            .where(TaskInstance.dag_id == self.dag_id)
            .where(TaskInstance.task_id == self.task_id)
        )
        if start_date:
            query = query.where(DagRun.logical_date >= start_date)
        if end_date:
            query = query.where(DagRun.logical_date <= end_date)
        return session.scalars(query.order_by(DagRun.logical_date)).all()

    @provide_session
    def run(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        ignore_first_depends_on_past: bool = True,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        test_mode: bool = False,
        session: Session = NEW_SESSION,
    ) -> None:
        """Run a set of task instances for a date range."""
        from airflow.models import DagRun
        from airflow.utils.types import DagRunType

        # Assertions for typing -- we need a dag, for this function, and when we have a DAG we are
        # _guaranteed_ to have start_date (else we couldn't have been added to a DAG)
        if TYPE_CHECKING:
            assert self.start_date

            # TODO: Task-SDK: We need to set this to the scheduler DAG until we fully separate scheduling and
            # definition code
            assert isinstance(self.dag, SchedulerDAG)

        start_date = pendulum.instance(start_date or self.start_date)
        end_date = pendulum.instance(end_date or self.end_date or timezone.utcnow())

        for info in self.dag.iter_dagrun_infos_between(start_date, end_date, align=False):
            ignore_depends_on_past = info.logical_date == start_date and ignore_first_depends_on_past
            try:
                dag_run = session.scalars(
                    select(DagRun).where(
                        DagRun.dag_id == self.dag_id,
                        DagRun.logical_date == info.logical_date,
                    )
                ).one()
                ti = TaskInstance(self, run_id=dag_run.run_id)
            except NoResultFound:
                # This is _mostly_ only used in tests
                dr = DagRun(
                    dag_id=self.dag_id,
                    run_id=DagRun.generate_run_id(
                        run_type=DagRunType.MANUAL,
                        logical_date=info.logical_date,
                        run_after=info.run_after,
                    ),
                    run_type=DagRunType.MANUAL,
                    logical_date=info.logical_date,
                    data_interval=info.data_interval,
                    run_after=info.run_after,
                    triggered_by=DagRunTriggeredByType.TEST,
                    state=DagRunState.RUNNING,
                )
                ti = TaskInstance(self, run_id=dr.run_id)
                ti.dag_run = dr
                session.add(dr)
                session.flush()

            ti.run(
                mark_success=mark_success,
                ignore_depends_on_past=ignore_depends_on_past,
                wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
                ignore_ti_state=ignore_ti_state,
                test_mode=test_mode,
                session=session,
            )

    def dry_run(self) -> None:
        """Perform dry run for the operator - just render template fields."""
        self.log.info("Dry run")
        for field in self.template_fields:
            try:
                content = getattr(self, field)
            except AttributeError:
                raise AttributeError(
                    f"{field!r} is configured as a template field "
                    f"but {self.task_type} does not have this attribute."
                )

            if content and isinstance(content, str):
                self.log.info("Rendering template for %s", field)
                self.log.info(content)

    def get_direct_relatives(self, upstream: bool = False) -> Iterable[Operator]:
        """Get list of the direct relatives to the current task, upstream or downstream."""
        if upstream:
            return self.upstream_list
        return self.downstream_list

    @staticmethod
    def xcom_push(
        context: Any,
        key: str,
        value: Any,
    ) -> None:
        """
        Make an XCom available for tasks to pull.

        :param context: Execution Context Dictionary
        :param key: A key for the XCom
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        """
        context["ti"].xcom_push(key=key, value=value)

    @staticmethod
    @provide_session
    def xcom_pull(
        context: Any,
        task_ids: str | list[str] | None = None,
        dag_id: str | None = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool | None = None,
        session: Session = NEW_SESSION,
    ) -> Any:
        """
        Pull XComs that optionally meet certain criteria.

        The default value for `key` limits the search to XComs
        that were returned by other tasks (as opposed to those that were pushed
        manually). To remove this filter, pass key=None (or any desired value).

        If a single task_id string is provided, the result is the value of the
        most recent matching XCom from that task_id. If multiple task_ids are
        provided, a tuple of matching values is returned. None is returned
        whenever no matches are found.

        :param context: Execution Context Dictionary
        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is 'return_value', also
            available as a constant XCOM_RETURN_KEY. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass key=None.
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :param dag_id: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :param include_prior_dates: If False, only XComs from the current
            logical_date are returned. If True, XComs from previous dates
            are returned as well.
        """
        return context["ti"].xcom_pull(
            key=key,
            task_ids=task_ids,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates,
            session=session,
        )

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Serialize; required by DAGNode."""
        return DagAttributeTypes.OP, self.task_id

    def unmap(self, resolve: None | dict[str, Any] | tuple[Context, Session]) -> BaseOperator:
        """
        Get the "normal" operator from the current operator.

        Since a BaseOperator is not mapped to begin with, this simply returns
        the original operator.

        :meta private:
        """
        return self

    def expand_start_from_trigger(self, *, context: Context, session: Session) -> bool:
        """
        Get the start_from_trigger value of the current abstract operator.

        Since a BaseOperator is not mapped to begin with, this simply returns
        the original value of start_from_trigger.

        :meta private:
        """
        return self.start_from_trigger

    def expand_start_trigger_args(self, *, context: Context, session: Session) -> StartTriggerArgs | None:
        """
        Get the start_trigger_args value of the current abstract operator.

        Since a BaseOperator is not mapped to begin with, this simply returns
        the original value of start_trigger_args.

        :meta private:
        """
        return self.start_trigger_args

    if TYPE_CHECKING:

        @classmethod
        def get_mapped_ti_count(
            cls, node: DAGNode | MappedTaskGroup, run_id: str, *, session: Session
        ) -> int:
            """
            Return the number of mapped TaskInstances that can be created at run time.

            This considers both literal and non-literal mapped arguments, and the
            result is therefore available when all depended tasks have finished. The
            return value should be identical to ``parse_time_mapped_ti_count`` if
            all mapped arguments are literal.

            :raise NotFullyPopulated: If upstream tasks are not all complete yet.
            :raise NotMapped: If the operator is neither mapped, nor has any parent
                mapped task groups.
            :return: Total number of mapped TIs this task should have.
            """
    else:

        @singledispatchmethod
        @classmethod
        def get_mapped_ti_count(cls, task: DAGNode, run_id: str, *, session: Session) -> int:
            raise NotImplementedError(f"Not implemented for {type(task)}")

        # https://github.com/python/cpython/issues/86153
        # WHile we support Python 3.9 we can't rely on the type hint, we need to pass the type explicitly to
        # register.
        @get_mapped_ti_count.register(TaskSDKAbstractOperator)
        @classmethod
        def _(cls, task: TaskSDKAbstractOperator, run_id: str, *, session: Session) -> int:
            group = task.get_closest_mapped_task_group()
            if group is None:
                raise NotMapped()
            return cls.get_mapped_ti_count(group, run_id, session=session)

        @get_mapped_ti_count.register(MappedOperator)
        @classmethod
        def _(cls, task: MappedOperator, run_id: str, *, session: Session) -> int:
            from airflow.serialization.serialized_objects import BaseSerialization, _ExpandInputRef

            exp_input = task._get_specified_expand_input()
            if isinstance(exp_input, _ExpandInputRef):
                exp_input = exp_input.deref(task.dag)
            # TODO: TaskSDK This is only needed to support `dag.test()` etc until we port it over to use the
            # task sdk runner.
            if not hasattr(exp_input, "get_total_map_length"):
                exp_input = _ExpandInputRef(
                    type(exp_input).EXPAND_INPUT_TYPE,
                    BaseSerialization.deserialize(BaseSerialization.serialize(exp_input.value)),
                )
                exp_input = exp_input.deref(task.dag)

            current_count = exp_input.get_total_map_length(run_id, session=session)

            group = task.get_closest_mapped_task_group()
            if group is None:
                return current_count
            parent_count = cls.get_mapped_ti_count(group, run_id, session=session)
            return parent_count * current_count

        @get_mapped_ti_count.register(TaskGroup)
        @classmethod
        def _(cls, group: TaskGroup, run_id: str, *, session: Session) -> int:
            """
            Return the number of instances a task in this group should be mapped to at run time.

            This considers both literal and non-literal mapped arguments, and the
            result is therefore available when all depended tasks have finished. The
            return value should be identical to ``parse_time_mapped_ti_count`` if
            all mapped arguments are literal.

            If this group is inside mapped task groups, all the nested counts are
            multiplied and accounted.

            :raise NotFullyPopulated: If upstream tasks are not all complete yet.
            :return: Total number of mapped TIs this task should have.
            """
            from airflow.serialization.serialized_objects import BaseSerialization, _ExpandInputRef

            def iter_mapped_task_group_lengths(group) -> Iterator[int]:
                while group is not None:
                    if isinstance(group, MappedTaskGroup):
                        exp_input = group._expand_input
                        # TODO: TaskSDK This is only needed to support `dag.test()` etc until we port it over to use the
                        # task sdk runner.
                        if not hasattr(exp_input, "get_total_map_length"):
                            exp_input = _ExpandInputRef(
                                type(exp_input).EXPAND_INPUT_TYPE,
                                BaseSerialization.deserialize(BaseSerialization.serialize(exp_input.value)),
                            )
                            exp_input = exp_input.deref(group.dag)
                        yield exp_input.get_total_map_length(run_id, session=session)
                    group = group.parent_group

            return functools.reduce(operator.mul, iter_mapped_task_group_lengths(group))
