# -*- coding: utf-8 -*-
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
"""
import copy
import functools
import logging
import sys
import warnings
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Type, Union

import jinja2
from cached_property import cached_property
from dateutil.relativedelta import relativedelta

from airflow import settings
from airflow.configuration import conf
from airflow.exceptions import AirflowException
from airflow.lineage import DataSet, apply_lineage, prepare_lineage
from airflow.models.dag import DAG
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils import timezone
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.helpers import validate_key
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.operator_resources import Resources
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weight_rule import WeightRule

ScheduleInterval = Union[str, timedelta, relativedelta]


# pylint: disable=too-many-instance-attributes,too-many-public-methods
@functools.total_ordering
class BaseOperator(LoggingMixin):
    """
    Abstract base class for all operators. Since operators create objects that
    become nodes in the dag, BaseOperator contains many recursive methods for
    dag crawling behavior. To derive this class, you are expected to override
    the constructor as well as the 'execute' method.

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
    :type task_id: str
    :param owner: the owner of the task, using the unix username is recommended
    :type owner: str
    :param email: the 'to' email address(es) used in email alerts. This can be a
        single email or multiple ones. Multiple addresses can be specified as a
        comma or semi-colon separated string or by passing a list of strings.
    :type email: str or list[str]
    :param email_on_retry: Indicates whether email alerts should be sent when a
        task is retried
    :type email_on_retry: bool
    :param email_on_failure: Indicates whether email alerts should be sent when
        a task failed
    :type email_on_failure: bool
    :param retries: the number of retries that should be performed before
        failing the task
    :type retries: int
    :param retry_delay: delay between retries
    :type retry_delay: datetime.timedelta
    :param retry_exponential_backoff: allow progressive longer waits between
        retries by using exponential backoff algorithm on retry delay (delay
        will be converted into seconds)
    :type retry_exponential_backoff: bool
    :param max_retry_delay: maximum delay interval between retries
    :type max_retry_delay: datetime.timedelta
    :param start_date: The ``start_date`` for the task, determines
        the ``execution_date`` for the first task instance. The best practice
        is to have the start_date rounded
        to your DAG's ``schedule_interval``. Daily jobs have their start_date
        some day at 00:00:00, hourly jobs have their start_date at 00:00
        of a specific hour. Note that Airflow simply looks at the latest
        ``execution_date`` and adds the ``schedule_interval`` to determine
        the next ``execution_date``. It is also very important
        to note that different tasks' dependencies
        need to line up in time. If task A depends on task B and their
        start_date are offset in a way that their execution_date don't line
        up, A's dependencies will never be met. If you are looking to delay
        a task, for example running a daily task at 2AM, look into the
        ``TimeSensor`` and ``TimeDeltaSensor``. We advise against using
        dynamic ``start_date`` and recommend using fixed ones. Read the
        FAQ entry about start_date for more information.
    :type start_date: datetime.datetime
    :param end_date: if specified, the scheduler won't go beyond this date
    :type end_date: datetime.datetime
    :param depends_on_past: when set to true, task instances will run
        sequentially while relying on the previous task's schedule to
        succeed. The task instance for the start_date is allowed to run.
    :type depends_on_past: bool
    :param wait_for_downstream: when set to true, an instance of task
        X will wait for tasks immediately downstream of the previous instance
        of task X to finish successfully before it runs. This is useful if the
        different instances of a task X alter the same asset, and this asset
        is used by tasks downstream of task X. Note that depends_on_past
        is forced to True wherever wait_for_downstream is used.
    :type wait_for_downstream: bool
    :param queue: which queue to target when running this job. Not
        all executors implement queue management, the CeleryExecutor
        does support targeting specific queues.
    :type queue: str
    :param dag: a reference to the dag the task is attached to (if any)
    :type dag: airflow.models.DAG
    :param priority_weight: priority weight of this task against other task.
        This allows the executor to trigger higher priority tasks before
        others when things get backed up. Set priority_weight as a higher
        number for more important tasks.
    :type priority_weight: int
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
        DAGS. Options can be set as string or using the constants defined in
        the static class ``airflow.utils.WeightRule``
    :type weight_rule: str
    :param queue: specifies which task queue to use
    :type queue: str
    :param pool: the slot pool this task should run in, slot pools are a
        way to limit concurrency for certain tasks
    :type pool: str
    :param sla: time by which the job is expected to succeed. Note that
        this represents the ``timedelta`` after the period is closed. For
        example if you set an SLA of 1 hour, the scheduler would send an email
        soon after 1:00AM on the ``2016-01-02`` if the ``2016-01-01`` instance
        has not succeeded yet.
        The scheduler pays special attention for jobs with an SLA and
        sends alert
        emails for sla misses. SLA misses are also recorded in the database
        for future reference. All tasks that share the same SLA time
        get bundled in a single email, sent soon after that time. SLA
        notification are sent once and only once for each task instance.
    :type sla: datetime.timedelta
    :param execution_timeout: max time allowed for the execution of
        this task instance, if it goes beyond it will raise and fail.
    :type execution_timeout: datetime.timedelta
    :param on_failure_callback: a function to be called when a task instance
        of this task fails. a context dictionary is passed as a single
        parameter to this function. Context contains references to related
        objects to the task instance and is documented under the macros
        section of the API.
    :type on_failure_callback: callable
    :param on_retry_callback: much like the ``on_failure_callback`` except
        that it is executed when retries occur.
    :type on_retry_callback: callable
    :param on_success_callback: much like the ``on_failure_callback`` except
        that it is executed when the task succeeds.
    :type on_success_callback: callable
    :param trigger_rule: defines the rule by which dependencies are applied
        for the task to get triggered. Options are:
        ``{ all_success | all_failed | all_done | one_success |
        one_failed | none_failed | none_skipped | dummy}``
        default is ``all_success``. Options can be set as string or
        using the constants defined in the static class
        ``airflow.utils.TriggerRule``
    :type trigger_rule: str
    :param resources: A map of resource parameter names (the argument names of the
        Resources constructor) to their values.
    :type resources: dict
    :param run_as_user: unix username to impersonate while running the task
    :type run_as_user: str
    :param task_concurrency: When set, a task will be able to limit the concurrent
        runs across execution_dates
    :type task_concurrency: int
    :param executor_config: Additional task-level configuration parameters that are
        interpreted by a specific executor. Parameters are namespaced by the name of
        executor.

        **Example**: to run this task in a specific docker container through
        the KubernetesExecutor ::

            MyOperator(...,
                executor_config={
                "KubernetesExecutor":
                    {"image": "myCustomDockerImage"}
                    }
            )

    :type executor_config: dict
    :param do_xcom_push: if True, an XCom is pushed containing the Operator's
        result
    :type do_xcom_push: bool
    """

    # For derived classes to define which fields will get jinjaified
    template_fields = []  # type: Iterable[str]
    # Defines which files extensions to look for in the templated fields
    template_ext = []  # type: Iterable[str]
    # Defines the color in the UI
    ui_color = '#fff'  # type str
    ui_fgcolor = '#000'  # type str

    # base list which includes all the attrs that don't need deep copy.
    _base_operator_shallow_copy_attrs = ('user_defined_macros',
                                         'user_defined_filters',
                                         'params',
                                         '_log',)  # type: Iterable[str]

    # each operator should override this class attr for shallow copy attrs.
    shallow_copy_attrs = ()  # type: Iterable[str]

    # Defines the operator level extra links
    operator_extra_links = ()  # type: Iterable[BaseOperatorLink]

    _comps = {
        'task_id',
        'dag_id',
        'owner',
        'email',
        'email_on_retry',
        'retry_delay',
        'retry_exponential_backoff',
        'max_retry_delay',
        'start_date',
        'depends_on_past',
        'wait_for_downstream',
        'priority_weight',
        'sla',
        'execution_timeout',
        'on_failure_callback',
        'on_success_callback',
        'on_retry_callback',
        'do_xcom_push',
    }

    # noinspection PyUnusedLocal
    # pylint: disable=too-many-arguments,too-many-locals
    @apply_defaults
    def __init__(
        self,
        task_id: str,
        owner: str = conf.get('operators', 'DEFAULT_OWNER'),
        email: Optional[Union[str, Iterable[str]]] = None,
        email_on_retry: bool = True,
        email_on_failure: bool = True,
        retries: Optional[int] = conf.getint('core', 'default_task_retries', fallback=0),
        retry_delay: timedelta = timedelta(seconds=300),
        retry_exponential_backoff: bool = False,
        max_retry_delay: Optional[datetime] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        depends_on_past: bool = False,
        wait_for_downstream: bool = False,
        dag: Optional[DAG] = None,
        params: Optional[Dict] = None,
        default_args: Optional[Dict] = None,  # pylint: disable=unused-argument
        priority_weight: int = 1,
        weight_rule: str = WeightRule.DOWNSTREAM,
        queue: str = conf.get('celery', 'default_queue'),
        pool: str = Pool.DEFAULT_POOL_NAME,
        sla: Optional[timedelta] = None,
        execution_timeout: Optional[timedelta] = None,
        on_failure_callback: Optional[Callable] = None,
        on_success_callback: Optional[Callable] = None,
        on_retry_callback: Optional[Callable] = None,
        trigger_rule: str = TriggerRule.ALL_SUCCESS,
        resources: Optional[Dict] = None,
        run_as_user: Optional[str] = None,
        task_concurrency: Optional[int] = None,
        executor_config: Optional[Dict] = None,
        do_xcom_push: bool = True,
        inlets: Optional[Dict] = None,
        outlets: Optional[Dict] = None,
        *args,
        **kwargs
    ):

        if args or kwargs:
            # TODO remove *args and **kwargs in Airflow 2.0
            warnings.warn(
                'Invalid arguments were passed to {c} (task_id: {t}). '
                'Support for passing such arguments will be dropped in '
                'Airflow 2.0. Invalid arguments were:'
                '\n*args: {a}\n**kwargs: {k}'.format(
                    c=self.__class__.__name__, a=args, k=kwargs, t=task_id),
                category=PendingDeprecationWarning,
                stacklevel=3
            )
        validate_key(task_id)
        self.task_id = task_id
        self.owner = owner
        self.email = email
        self.email_on_retry = email_on_retry
        self.email_on_failure = email_on_failure

        self.start_date = start_date
        if start_date and not isinstance(start_date, datetime):
            self.log.warning("start_date for %s isn't datetime.datetime", self)
        elif start_date:
            self.start_date = timezone.convert_to_utc(start_date)

        self.end_date = end_date
        if end_date:
            self.end_date = timezone.convert_to_utc(end_date)

        if not TriggerRule.is_valid(trigger_rule):
            raise AirflowException(
                "The trigger_rule must be one of {all_triggers},"
                "'{d}.{t}'; received '{tr}'."
                .format(all_triggers=TriggerRule.all_triggers(),
                        d=dag.dag_id if dag else "", t=task_id, tr=trigger_rule))

        self.trigger_rule = trigger_rule
        self.depends_on_past = depends_on_past
        self.wait_for_downstream = wait_for_downstream
        if wait_for_downstream:
            self.depends_on_past = True

        self.retries = retries
        self.queue = queue
        self.pool = pool
        self.sla = sla
        self.execution_timeout = execution_timeout
        self.on_failure_callback = on_failure_callback
        self.on_success_callback = on_success_callback
        self.on_retry_callback = on_retry_callback

        if isinstance(retry_delay, timedelta):
            self.retry_delay = retry_delay
        else:
            self.log.debug("Retry_delay isn't timedelta object, assuming secs")
            self.retry_delay = timedelta(seconds=retry_delay)
        self.retry_exponential_backoff = retry_exponential_backoff
        self.max_retry_delay = max_retry_delay
        self.params = params or {}  # Available in templates!
        self.priority_weight = priority_weight
        if not WeightRule.is_valid(weight_rule):
            raise AirflowException(
                "The weight_rule must be one of {all_weight_rules},"
                "'{d}.{t}'; received '{tr}'."
                .format(all_weight_rules=WeightRule.all_weight_rules,
                        d=dag.dag_id if dag else "", t=task_id, tr=weight_rule))
        self.weight_rule = weight_rule

        self.resources = Resources(*resources) if resources is not None else None
        self.run_as_user = run_as_user
        self.task_concurrency = task_concurrency
        self.executor_config = executor_config or {}
        self.do_xcom_push = do_xcom_push

        # Private attributes
        self._upstream_task_ids = set()  # type: Set[str]
        self._downstream_task_ids = set()  # type: Set[str]

        if not dag and settings.CONTEXT_MANAGER_DAG:
            dag = settings.CONTEXT_MANAGER_DAG
        if dag:
            self.dag = dag

        self._log = logging.getLogger("airflow.task.operators")

        # lineage
        self.inlets = []   # type: List[DataSet]
        self.outlets = []  # type: List[DataSet]
        self.lineage_data = None

        self._inlets = {
            "auto": False,
            "task_ids": [],
            "datasets": [],
        }

        self._outlets = {
            "datasets": [],
        }  # type: Dict

        if inlets:
            self._inlets.update(inlets)

        if outlets:
            self._outlets.update(outlets)

    def __eq__(self, other):
        if (type(self) == type(other) and  # pylint: disable=unidiomatic-typecheck
                self.task_id == other.task_id):
            return all(self.__dict__.get(c, None) == other.__dict__.get(c, None) for c in self._comps)
        return False

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.task_id < other.task_id

    def __hash__(self):
        hash_components = [type(self)]
        for component in self._comps:
            val = getattr(self, component, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    # Composing Operators -----------------------------------------------

    def __rshift__(self, other):
        """
        Implements Self >> Other == self.set_downstream(other)

        If "Other" is a DAG, the DAG is assigned to the Operator.
        """
        if isinstance(other, DAG):
            # if this dag is already assigned, do nothing
            # otherwise, do normal dag assignment
            if not (self.has_dag() and self.dag is other):
                self.dag = other
        else:
            self.set_downstream(other)
        return other

    def __lshift__(self, other):
        """
        Implements Self << Other == self.set_upstream(other)

        If "Other" is a DAG, the DAG is assigned to the Operator.
        """
        if isinstance(other, DAG):
            # if this dag is already assigned, do nothing
            # otherwise, do normal dag assignment
            if not (self.has_dag() and self.dag is other):
                self.dag = other
        else:
            self.set_upstream(other)
        return other

    def __rrshift__(self, other):
        """
        Called for [DAG] >> [Operator] because DAGs don't have
        __rshift__ operators.
        """
        self.__lshift__(other)
        return self

    def __rlshift__(self, other):
        """
        Called for [DAG] << [Operator] because DAGs don't have
        __lshift__ operators.
        """
        self.__rshift__(other)
        return self

    # /Composing Operators ---------------------------------------------

    @property
    def dag(self):
        """
        Returns the Operator's DAG if set, otherwise raises an error
        """
        if self.has_dag():
            return self._dag
        else:
            raise AirflowException(
                'Operator {} has not been assigned to a DAG yet'.format(self))

    @dag.setter
    def dag(self, dag):
        """
        Operators can be assigned to one DAG, one time. Repeat assignments to
        that same DAG are ok.
        """
        if not isinstance(dag, DAG):
            raise TypeError(
                'Expected DAG; received {}'.format(dag.__class__.__name__))
        elif self.has_dag() and self.dag is not dag:
            raise AirflowException(
                "The DAG assigned to {} can not be changed.".format(self))
        elif self.task_id not in dag.task_dict:
            dag.add_task(self)

        self._dag = dag  # pylint: disable=attribute-defined-outside-init

    def has_dag(self):
        """
        Returns True if the Operator has been assigned to a DAG.
        """
        return getattr(self, '_dag', None) is not None

    @property
    def dag_id(self):
        """Returns dag id if it has one or an adhoc + owner"""
        if self.has_dag():
            return self.dag.dag_id
        else:
            return 'adhoc_' + self.owner

    @property
    def deps(self):
        """
        Returns the list of dependencies for the operator. These differ from execution
        context dependencies in that they are specific to tasks and can be
        extended/overridden by subclasses.
        """
        return {
            NotInRetryPeriodDep(),
            PrevDagrunDep(),
            TriggerRuleDep(),
        }

    @property
    def priority_weight_total(self):
        """
        Total priority weight for the task. It might include all upstream or downstream tasks.
        depending on the weight rule.

          - WeightRule.ABSOLUTE - only own weight
          - WeightRule.DOWNSTREAM - adds priority weight of all downstream tasks
          - WeightRule.UPSTREAM - adds priority weight of all upstream tasks

        """
        if self.weight_rule == WeightRule.ABSOLUTE:
            return self.priority_weight
        elif self.weight_rule == WeightRule.DOWNSTREAM:
            upstream = False
        elif self.weight_rule == WeightRule.UPSTREAM:
            upstream = True
        else:
            upstream = False

        return self.priority_weight + sum(
            map(lambda task_id: self._dag.task_dict[task_id].priority_weight,
                self.get_flat_relative_ids(upstream=upstream))
        )

    @cached_property
    def operator_extra_link_dict(self):
        """Returns dictionary of all extra links for the operator"""
        from airflow.plugins_manager import operator_extra_links

        op_extra_links_from_plugin = {}
        for ope in operator_extra_links:
            if ope.operators and self.__class__ in ope.operators:
                op_extra_links_from_plugin.update({ope.name: ope})

        operator_extra_links_all = {
            link.name: link for link in self.operator_extra_links
        }
        # Extra links defined in Plugins overrides operator links defined in operator
        operator_extra_links_all.update(op_extra_links_from_plugin)

        return operator_extra_links_all

    @cached_property
    def global_operator_extra_link_dict(self):
        """Returns dictionary of all global extra links"""
        from airflow.plugins_manager import global_operator_extra_links
        return {link.name: link for link in global_operator_extra_links}

    @prepare_lineage
    def pre_execute(self, context):
        """
        This hook is triggered right before self.execute() is called.
        """

    def execute(self, context):
        """
        This is the main method to derive when creating an operator.
        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        raise NotImplementedError()

    @apply_lineage
    def post_execute(self, context, result=None):
        """
        This hook is triggered right after self.execute() is called.
        It is passed the execution context and any results returned by the
        operator.
        """

    def on_kill(self):
        """
        Override this method to cleanup subprocesses when a task instance
        gets killed. Any use of the threading, subprocess or multiprocessing
        module within an operator needs to be cleaned up or it will leave
        ghost processes behind.
        """

    def __deepcopy__(self, memo):
        """
        Hack sorting double chained task lists by task_id to avoid hitting
        max_depth on deepcopy operations.
        """
        sys.setrecursionlimit(5000)  # TODO fix this in a better way
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        # noinspection PyProtectedMember
        shallow_copy = cls.shallow_copy_attrs + \
            cls._base_operator_shallow_copy_attrs  # pylint: disable=protected-access

        for k, v in self.__dict__.items():
            if k not in shallow_copy:
                setattr(result, k, copy.deepcopy(v, memo))
            else:
                setattr(result, k, copy.copy(v))
        return result

    def __getstate__(self):
        state = dict(self.__dict__)
        del state['_log']

        return state

    def __setstate__(self, state):
        self.__dict__ = state  # pylint: disable=attribute-defined-outside-init
        self._log = logging.getLogger("airflow.task.operators")

    def render_template_fields(self, context: Dict, jinja_env: Optional[jinja2.Environment] = None) -> None:
        """
        Template all attributes listed in template_fields. Note this operation is irreversible.

        :param context: Dict with values to apply on content
        :type context: dict
        :param jinja_env: Jinja environment
        :type jinja_env: jinja2.Environment
        """

        if not jinja_env:
            jinja_env = self.get_template_env()

        self._do_render_template_fields(self, self.template_fields, context, jinja_env, set())

    def _do_render_template_fields(
        self, parent: Any, template_fields: Iterable[str], context: Dict, jinja_env: jinja2.Environment,
        seen_oids: Set
    ) -> None:
        for attr_name in template_fields:
            content = getattr(parent, attr_name)
            if content:
                rendered_content = self.render_template(content, context, jinja_env, seen_oids)
                setattr(parent, attr_name, rendered_content)

    def render_template(      # pylint: disable=too-many-return-statements
        self, content: Any, context: Dict, jinja_env: Optional[jinja2.Environment] = None,
        seen_oids: Optional[Set] = None
    ) -> Any:
        """
        Render a templated string. The content can be a collection holding multiple templated strings and will
        be templated recursively.

        :param content: Content to template. Only strings can be templated (may be inside collection).
        :type content: Any
        :param context: Dict with values to apply on templated content
        :type context: dict
        :param jinja_env: Jinja environment. Can be provided to avoid re-creating Jinja environments during
            recursion.
        :type jinja_env: jinja2.Environment
        :param seen_oids: template fields already rendered (to avoid RecursionError on circular dependencies)
        :type seen_oids: set
        :return: Templated content
        """

        if not jinja_env:
            jinja_env = self.get_template_env()

        if isinstance(content, str):
            if any(content.endswith(ext) for ext in self.template_ext):
                # Content contains a filepath
                return jinja_env.get_template(content).render(**context)
            else:
                return jinja_env.from_string(content).render(**context)

        if isinstance(content, tuple):
            if type(content) is not tuple:  # pylint: disable=unidiomatic-typecheck
                # Special case for named tuples
                return content.__class__(
                    *(self.render_template(element, context, jinja_env) for element in content)
                )
            else:
                return tuple(self.render_template(element, context, jinja_env) for element in content)

        elif isinstance(content, list):
            return [self.render_template(element, context, jinja_env) for element in content]

        elif isinstance(content, dict):
            return {key: self.render_template(value, context, jinja_env) for key, value in content.items()}

        elif isinstance(content, set):
            return {self.render_template(element, context, jinja_env) for element in content}

        else:
            if seen_oids is None:
                seen_oids = set()
            self._render_nested_template_fields(content, context, jinja_env, seen_oids)
            return content

    def _render_nested_template_fields(
        self, content: Any, context: Dict, jinja_env: jinja2.Environment, seen_oids: Set
    ) -> None:
        if id(content) not in seen_oids:
            seen_oids.add(id(content))
            try:
                nested_template_fields = content.template_fields
            except AttributeError:
                # content has no inner template fields
                return

            self._do_render_template_fields(content, nested_template_fields, context, jinja_env, seen_oids)

    def get_template_env(self) -> jinja2.Environment:
        """Fetch a Jinja template environment from the DAG or instantiate empty environment if no DAG."""
        return self.dag.get_template_env() if self.has_dag() else jinja2.Environment(cache_size=0)

    def prepare_template(self):
        """
        Hook that is triggered after the templated fields get replaced
        by their content. If you need your operator to alter the
        content of the file before the template is rendered,
        it should override this method to do so.
        """

    def resolve_template_files(self):
        """Getting the content of files for template_field / template_ext"""
        if self.template_ext:  # pylint: disable=too-many-nested-blocks
            for attr in self.template_fields:
                content = getattr(self, attr, None)
                if content is None:
                    continue
                elif isinstance(content, str) and \
                        any([content.endswith(ext) for ext in self.template_ext]):
                    env = self.get_template_env()
                    try:
                        setattr(self, attr, env.loader.get_source(env, content)[0])
                    except Exception as e:  # pylint: disable=broad-except
                        self.log.exception(e)
                elif isinstance(content, list):
                    env = self.dag.get_template_env()
                    for i in range(len(content)):  # pylint: disable=consider-using-enumerate
                        if isinstance(content[i], str) and \
                                any([content[i].endswith(ext) for ext in self.template_ext]):
                            try:
                                content[i] = env.loader.get_source(env, content[i])[0]
                            except Exception as e:  # pylint: disable=broad-except
                                self.log.exception(e)
        self.prepare_template()

    @property
    def upstream_list(self):
        """@property: list of tasks directly upstream"""
        return [self.dag.get_task(tid) for tid in self._upstream_task_ids]

    @property
    def upstream_task_ids(self):
        """@property: list of ids of tasks directly upstream"""
        return self._upstream_task_ids

    @property
    def downstream_list(self):
        """@property: list of tasks directly downstream"""
        return [self.dag.get_task(tid) for tid in self._downstream_task_ids]

    @property
    def downstream_task_ids(self):
        """@property: list of ids of tasks directly downstream"""
        return self._downstream_task_ids

    @provide_session
    def clear(self,
              start_date=None,
              end_date=None,
              upstream=False,
              downstream=False,
              session=None):
        """
        Clears the state of task instances associated with the task, following
        the parameters specified.
        """
        TI = TaskInstance
        qry = session.query(TI).filter(TI.dag_id == self.dag_id)

        if start_date:
            qry = qry.filter(TI.execution_date >= start_date)
        if end_date:
            qry = qry.filter(TI.execution_date <= end_date)

        tasks = [self.task_id]

        if upstream:
            tasks += [
                t.task_id for t in self.get_flat_relatives(upstream=True)]

        if downstream:
            tasks += [
                t.task_id for t in self.get_flat_relatives(upstream=False)]

        qry = qry.filter(TI.task_id.in_(tasks))

        count = qry.count()

        clear_task_instances(qry.all(), session, dag=self.dag)

        session.commit()

        return count

    @provide_session
    def get_task_instances(self, start_date=None, end_date=None, session=None):
        """
        Get a set of task instance related to this task for a specific date
        range.
        """
        end_date = end_date or timezone.utcnow()
        return session.query(TaskInstance)\
            .filter(TaskInstance.dag_id == self.dag_id)\
            .filter(TaskInstance.task_id == self.task_id)\
            .filter(TaskInstance.execution_date >= start_date)\
            .filter(TaskInstance.execution_date <= end_date)\
            .order_by(TaskInstance.execution_date)\
            .all()

    def get_flat_relative_ids(self, upstream=False, found_descendants=None):
        """
        Get a flat list of relatives' ids, either upstream or downstream.
        """

        if not found_descendants:
            found_descendants = set()
        relative_ids = self.get_direct_relative_ids(upstream)

        for relative_id in relative_ids:
            if relative_id not in found_descendants:
                found_descendants.add(relative_id)
                relative_task = self._dag.task_dict[relative_id]
                relative_task.get_flat_relative_ids(upstream,
                                                    found_descendants)

        return found_descendants

    def get_flat_relatives(self, upstream=False):
        """
        Get a flat list of relatives, either upstream or downstream.
        """
        return list(map(lambda task_id: self._dag.task_dict[task_id],
                        self.get_flat_relative_ids(upstream)))

    def run(
            self,
            start_date=None,
            end_date=None,
            ignore_first_depends_on_past=False,
            ignore_ti_state=False,
            mark_success=False):
        """
        Run a set of task instances for a date range.
        """
        start_date = start_date or self.start_date
        end_date = end_date or self.end_date or timezone.utcnow()

        for execution_date in self.dag.date_range(start_date, end_date=end_date):
            TaskInstance(self, execution_date).run(
                mark_success=mark_success,
                ignore_depends_on_past=(
                    execution_date == start_date and ignore_first_depends_on_past),
                ignore_ti_state=ignore_ti_state)

    def dry_run(self):
        """Performs dry run for the operator - just render template fields."""
        self.log.info('Dry run')
        for attr in self.template_fields:
            content = getattr(self, attr)
            if content and isinstance(content, str):
                self.log.info('Rendering template for %s', attr)
                self.log.info(content)

    def get_direct_relative_ids(self, upstream=False):
        """
        Get the direct relative ids to the current task, upstream or
        downstream.
        """
        if upstream:
            return self._upstream_task_ids
        else:
            return self._downstream_task_ids

    def get_direct_relatives(self, upstream=False):
        """
        Get the direct relatives to the current task, upstream or
        downstream.
        """
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def __repr__(self):
        return "<Task({self.__class__.__name__}): {self.task_id}>".format(
            self=self)

    @property
    def task_type(self):
        """@property: type of the task"""
        return self.__class__.__name__

    def add_only_new(self, item_set, item):
        """Adds only new items to item set"""
        if item in item_set:
            self.log.warning(
                'Dependency %s, %s already registered', self, item)
        else:
            item_set.add(item)

    def _set_relatives(self, task_or_task_list, upstream=False):
        """Sets relatives for the task."""
        try:
            task_list = list(task_or_task_list)
        except TypeError:
            task_list = [task_or_task_list]

        for task in task_list:
            if not isinstance(task, BaseOperator):
                raise AirflowException(
                    "Relationships can only be set between "
                    "Operators; received {}".format(task.__class__.__name__))

        # relationships can only be set if the tasks share a single DAG. Tasks
        # without a DAG are assigned to that DAG.
        dags = {
            task._dag.dag_id: task._dag  # pylint: disable=protected-access
            for task in [self] + task_list if task.has_dag()}

        if len(dags) > 1:
            raise AirflowException(
                'Tried to set relationships between tasks in '
                'more than one DAG: {}'.format(dags.values()))
        elif len(dags) == 1:
            dag = dags.popitem()[1]
        else:
            raise AirflowException(
                "Tried to create relationships between tasks that don't have "
                "DAGs yet. Set the DAG for at least one "
                "task  and try again: {}".format([self] + task_list))

        if dag and not self.has_dag():
            self.dag = dag

        for task in task_list:
            if dag and not task.has_dag():
                task.dag = dag
            if upstream:
                task.add_only_new(task.get_direct_relative_ids(upstream=False), self.task_id)
                self.add_only_new(self._upstream_task_ids, task.task_id)
            else:
                self.add_only_new(self._downstream_task_ids, task.task_id)
                task.add_only_new(task.get_direct_relative_ids(upstream=True), self.task_id)

    def set_downstream(self, task_or_task_list):
        """
        Set a task or a task list to be directly downstream from the current
        task.
        """
        self._set_relatives(task_or_task_list, upstream=False)

    def set_upstream(self, task_or_task_list):
        """
        Set a task or a task list to be directly upstream from the current
        task.
        """
        self._set_relatives(task_or_task_list, upstream=True)

    def xcom_push(
            self,
            context,
            key,
            value,
            execution_date=None):
        """
        See TaskInstance.xcom_push()
        """
        context['ti'].xcom_push(
            key=key,
            value=value,
            execution_date=execution_date)

    def xcom_pull(
            self,
            context,
            task_ids=None,
            dag_id=None,
            key=XCOM_RETURN_KEY,
            include_prior_dates=None):
        """
        See TaskInstance.xcom_pull()
        """
        return context['ti'].xcom_pull(
            key=key,
            task_ids=task_ids,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates)

    @cached_property
    def extra_links(self) -> Iterable[str]:
        """@property: extra links for the task. """
        return list(set(self.operator_extra_link_dict.keys())
                    .union(self.global_operator_extra_link_dict.keys()))

    def get_extra_links(self, dttm, link_name):
        """
        For an operator, gets the URL that the external links specified in
        `extra_links` should point to.

        :raise ValueError: The error message of a ValueError will be passed on through to
            the fronted to show up as a tooltip on the disabled link
        :param dttm: The datetime parsed execution date for the URL being searched for
        :param link_name: The name of the link we're looking for the URL for. Should be
            one of the options specified in `extra_links`
        :return: A URL
        """
        if link_name in self.operator_extra_link_dict:
            return self.operator_extra_link_dict[link_name].get_link(self, dttm)
        elif link_name in self.global_operator_extra_link_dict:
            return self.global_operator_extra_link_dict[link_name].get_link(self, dttm)
        else:
            return None


class BaseOperatorLink(metaclass=ABCMeta):
    """
    Abstract base class that defines how we get an operator link.
    """

    operators = []   # type: List[Type[BaseOperator]]
    """
    This property will be used by Airflow Plugins to find the Operators to which you want
    to assign this Operator Link

    :return: List of Operator classes used by task for which you want to create extra link
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """
        Name of the link. This will be the button name on the task UI.

        :return: link name
        """

    @abstractmethod
    def get_link(self, operator: BaseOperator, dttm: datetime) -> str:
        """
        Link to external system.

        :param operator: airflow operator
        :param dttm: datetime
        :return: link to external system
        """
