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
"""Base operator for all operators."""
import abc
import collections
import collections.abc
import contextlib
import copy
import functools
import logging
import sys
import warnings
from abc import ABCMeta, abstractmethod
from datetime import datetime, timedelta
from inspect import signature
from types import FunctionType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Collection,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    TypeVar,
    Union,
    cast,
)

import attr
import pendulum
from dateutil.relativedelta import relativedelta
from sqlalchemy.orm import Session
from sqlalchemy.orm.exc import NoResultFound

from airflow.configuration import conf
from airflow.exceptions import AirflowException, RemovedInAirflow3Warning, TaskDeferred
from airflow.lineage import apply_lineage, prepare_lineage
from airflow.models.abstractoperator import (
    DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST,
    DEFAULT_OWNER,
    DEFAULT_POOL_SLOTS,
    DEFAULT_PRIORITY_WEIGHT,
    DEFAULT_QUEUE,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
    DEFAULT_TASK_EXECUTION_TIMEOUT,
    DEFAULT_TRIGGER_RULE,
    DEFAULT_WEIGHT_RULE,
    AbstractOperator,
    TaskStateChangeCallback,
)
from airflow.models.mappedoperator import OperatorPartial, validate_mapping_kwargs
from airflow.models.param import ParamsDict
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.models.taskmixin import DAGNode, DependencyMixin
from airflow.models.xcom import XCOM_RETURN_KEY
from airflow.serialization.enums import DagAttributeTypes
from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.not_previously_skipped_dep import NotPreviouslySkippedDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.triggers.base import BaseTrigger
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.helpers import validate_key
from airflow.utils.operator_resources import Resources
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weight_rule import WeightRule

if TYPE_CHECKING:
    import jinja2  # Slow import.

    from airflow.models.dag import DAG
    from airflow.models.taskinstance import TaskInstanceKey
    from airflow.models.xcom_arg import XComArg
    from airflow.utils.task_group import TaskGroup

ScheduleInterval = Union[str, timedelta, relativedelta]

TaskPreExecuteHook = Callable[[Context], None]
TaskPostExecuteHook = Callable[[Context, Any], None]

T = TypeVar('T', bound=FunctionType)

logger = logging.getLogger("airflow.models.baseoperator.BaseOperator")


def parse_retries(retries: Any) -> Optional[int]:
    if retries is None or isinstance(retries, int):
        return retries
    try:
        parsed_retries = int(retries)
    except (TypeError, ValueError):
        raise AirflowException(f"'retries' type must be int, not {type(retries).__name__}")
    logger.warning("Implicitly converting 'retries' from %r to int", retries)
    return parsed_retries


def coerce_timedelta(value: Union[float, timedelta], *, key: str) -> timedelta:
    if isinstance(value, timedelta):
        return value
    logger.debug("%s isn't a timedelta object, assuming secs", key)
    return timedelta(seconds=value)


def coerce_resources(resources: Optional[Dict[str, Any]]) -> Optional[Resources]:
    if resources is None:
        return None
    return Resources(**resources)


def _get_parent_defaults(dag: Optional["DAG"], task_group: Optional["TaskGroup"]) -> Tuple[dict, ParamsDict]:
    if not dag:
        return {}, ParamsDict()
    dag_args = copy.copy(dag.default_args)
    dag_params = copy.deepcopy(dag.params)
    if task_group:
        if task_group.default_args and not isinstance(task_group.default_args, collections.abc.Mapping):
            raise TypeError("default_args must be a mapping")
        dag_args.update(task_group.default_args)
    return dag_args, dag_params


def get_merged_defaults(
    dag: Optional["DAG"],
    task_group: Optional["TaskGroup"],
    task_params: Optional[dict],
    task_default_args: Optional[dict],
) -> Tuple[dict, ParamsDict]:
    args, params = _get_parent_defaults(dag, task_group)
    if task_params:
        if not isinstance(task_params, collections.abc.Mapping):
            raise TypeError("params must be a mapping")
        params.update(task_params)
    if task_default_args:
        if not isinstance(task_default_args, collections.abc.Mapping):
            raise TypeError("default_args must be a mapping")
        args.update(task_default_args)
        with contextlib.suppress(KeyError):
            params.update(task_default_args["params"] or {})
    return args, params


class _PartialDescriptor:
    """A descriptor that guards against ``.partial`` being called on Task objects."""

    class_method = None

    def __get__(
        self, obj: "BaseOperator", cls: "Optional[Type[BaseOperator]]" = None
    ) -> Callable[..., OperatorPartial]:
        # Call this "partial" so it looks nicer in stack traces.
        def partial(**kwargs):
            raise TypeError("partial can only be called on Operator classes, not Tasks themselves")

        if obj is not None:
            return partial
        return self.class_method.__get__(cls, cls)


# This is what handles the actual mapping.
def partial(
    operator_class: Type["BaseOperator"],
    *,
    task_id: str,
    dag: Optional["DAG"] = None,
    task_group: Optional["TaskGroup"] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    owner: str = DEFAULT_OWNER,
    email: Union[None, str, Iterable[str]] = None,
    params: Optional[dict] = None,
    resources: Optional[Dict[str, Any]] = None,
    trigger_rule: str = DEFAULT_TRIGGER_RULE,
    depends_on_past: bool = False,
    ignore_first_depends_on_past: bool = DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST,
    wait_for_downstream: bool = False,
    retries: Optional[int] = DEFAULT_RETRIES,
    queue: str = DEFAULT_QUEUE,
    pool: Optional[str] = None,
    pool_slots: int = DEFAULT_POOL_SLOTS,
    execution_timeout: Optional[timedelta] = DEFAULT_TASK_EXECUTION_TIMEOUT,
    max_retry_delay: Union[None, timedelta, float] = None,
    retry_delay: Union[timedelta, float] = DEFAULT_RETRY_DELAY,
    retry_exponential_backoff: bool = False,
    priority_weight: int = DEFAULT_PRIORITY_WEIGHT,
    weight_rule: str = DEFAULT_WEIGHT_RULE,
    sla: Optional[timedelta] = None,
    max_active_tis_per_dag: Optional[int] = None,
    on_execute_callback: Optional[TaskStateChangeCallback] = None,
    on_failure_callback: Optional[TaskStateChangeCallback] = None,
    on_success_callback: Optional[TaskStateChangeCallback] = None,
    on_retry_callback: Optional[TaskStateChangeCallback] = None,
    run_as_user: Optional[str] = None,
    executor_config: Optional[Dict] = None,
    inlets: Optional[Any] = None,
    outlets: Optional[Any] = None,
    doc: Optional[str] = None,
    doc_md: Optional[str] = None,
    doc_json: Optional[str] = None,
    doc_yaml: Optional[str] = None,
    doc_rst: Optional[str] = None,
    **kwargs,
) -> OperatorPartial:
    from airflow.models.dag import DagContext
    from airflow.utils.task_group import TaskGroupContext

    validate_mapping_kwargs(operator_class, "partial", kwargs)

    dag = dag or DagContext.get_current_dag()
    if dag:
        task_group = TaskGroupContext.get_current_task_group(dag)
    if task_group:
        task_id = task_group.child_id(task_id)

    # Merge DAG and task group level defaults into user-supplied values.
    partial_kwargs, default_params = get_merged_defaults(
        dag=dag,
        task_group=task_group,
        task_params=params,
        task_default_args=kwargs.pop("default_args", None),
    )
    partial_kwargs.update(kwargs)

    # Always fully populate partial kwargs to exclude them from map().
    partial_kwargs.setdefault("dag", dag)
    partial_kwargs.setdefault("task_group", task_group)
    partial_kwargs.setdefault("task_id", task_id)
    partial_kwargs.setdefault("start_date", start_date)
    partial_kwargs.setdefault("end_date", end_date)
    partial_kwargs.setdefault("owner", owner)
    partial_kwargs.setdefault("email", email)
    partial_kwargs.setdefault("params", default_params)
    partial_kwargs.setdefault("trigger_rule", trigger_rule)
    partial_kwargs.setdefault("depends_on_past", depends_on_past)
    partial_kwargs.setdefault("ignore_first_depends_on_past", ignore_first_depends_on_past)
    partial_kwargs.setdefault("wait_for_downstream", wait_for_downstream)
    partial_kwargs.setdefault("retries", retries)
    partial_kwargs.setdefault("queue", queue)
    partial_kwargs.setdefault("pool", pool)
    partial_kwargs.setdefault("pool_slots", pool_slots)
    partial_kwargs.setdefault("execution_timeout", execution_timeout)
    partial_kwargs.setdefault("max_retry_delay", max_retry_delay)
    partial_kwargs.setdefault("retry_delay", retry_delay)
    partial_kwargs.setdefault("retry_exponential_backoff", retry_exponential_backoff)
    partial_kwargs.setdefault("priority_weight", priority_weight)
    partial_kwargs.setdefault("weight_rule", weight_rule)
    partial_kwargs.setdefault("sla", sla)
    partial_kwargs.setdefault("max_active_tis_per_dag", max_active_tis_per_dag)
    partial_kwargs.setdefault("on_execute_callback", on_execute_callback)
    partial_kwargs.setdefault("on_failure_callback", on_failure_callback)
    partial_kwargs.setdefault("on_retry_callback", on_retry_callback)
    partial_kwargs.setdefault("on_success_callback", on_success_callback)
    partial_kwargs.setdefault("run_as_user", run_as_user)
    partial_kwargs.setdefault("executor_config", executor_config)
    partial_kwargs.setdefault("inlets", inlets)
    partial_kwargs.setdefault("outlets", outlets)
    partial_kwargs.setdefault("resources", resources)
    partial_kwargs.setdefault("doc", doc)
    partial_kwargs.setdefault("doc_json", doc_json)
    partial_kwargs.setdefault("doc_md", doc_md)
    partial_kwargs.setdefault("doc_rst", doc_rst)
    partial_kwargs.setdefault("doc_yaml", doc_yaml)

    # Post-process arguments. Should be kept in sync with _TaskDecorator.expand().
    if "task_concurrency" in kwargs:  # Reject deprecated option.
        raise TypeError("unexpected argument: task_concurrency")
    if partial_kwargs["wait_for_downstream"]:
        partial_kwargs["depends_on_past"] = True
    partial_kwargs["start_date"] = timezone.convert_to_utc(partial_kwargs["start_date"])
    partial_kwargs["end_date"] = timezone.convert_to_utc(partial_kwargs["end_date"])
    if partial_kwargs["pool"] is None:
        partial_kwargs["pool"] = Pool.DEFAULT_POOL_NAME
    partial_kwargs["retries"] = parse_retries(partial_kwargs["retries"])
    partial_kwargs["retry_delay"] = coerce_timedelta(partial_kwargs["retry_delay"], key="retry_delay")
    if partial_kwargs["max_retry_delay"] is not None:
        partial_kwargs["max_retry_delay"] = coerce_timedelta(
            partial_kwargs["max_retry_delay"],
            key="max_retry_delay",
        )
    partial_kwargs["executor_config"] = partial_kwargs["executor_config"] or {}
    partial_kwargs["resources"] = coerce_resources(partial_kwargs["resources"])

    return OperatorPartial(operator_class=operator_class, kwargs=partial_kwargs)


class BaseOperatorMeta(abc.ABCMeta):
    """Metaclass of BaseOperator."""

    @classmethod
    def _apply_defaults(cls, func: T) -> T:
        """
        Function decorator that Looks for an argument named "default_args", and
        fills the unspecified arguments from it.

        Since python2.* isn't clear about which arguments are missing when
        calling a function, and that this can be quite confusing with multi-level
        inheritance and argument defaults, this decorator also alerts with
        specific information about the missing arguments.
        """
        # Cache inspect.signature for the wrapper closure to avoid calling it
        # at every decorated invocation. This is separate sig_cache created
        # per decoration, i.e. each function decorated using apply_defaults will
        # have a different sig_cache.
        sig_cache = signature(func)
        non_variadic_params = {
            name: param
            for (name, param) in sig_cache.parameters.items()
            if param.name != 'self' and param.kind not in (param.VAR_POSITIONAL, param.VAR_KEYWORD)
        }
        non_optional_args = {
            name
            for name, param in non_variadic_params.items()
            if param.default == param.empty and name != "task_id"
        }

        class autostacklevel_warn:
            def __init__(self):
                self.warnings = __import__('warnings')

            def __getattr__(self, name):
                return getattr(self.warnings, name)

            def __dir__(self):
                return dir(self.warnings)

            def warn(self, message, category=None, stacklevel=1, source=None):
                self.warnings.warn(message, category, stacklevel + 2, source)

        if func.__globals__.get('warnings') is sys.modules['warnings']:
            # Yes, this is slightly hacky, but it _automatically_ sets the right
            # stacklevel parameter to `warnings.warn` to ignore the decorator. Now
            # that the decorator is applied automatically, this makes the needed
            # stacklevel parameter less confusing.
            func.__globals__['warnings'] = autostacklevel_warn()

        @functools.wraps(func)
        def apply_defaults(self: "BaseOperator", *args: Any, **kwargs: Any) -> Any:
            from airflow.models.dag import DagContext
            from airflow.utils.task_group import TaskGroupContext

            if len(args) > 0:
                raise AirflowException("Use keyword arguments when initializing operators")

            instantiated_from_mapped = kwargs.pop(
                "_airflow_from_mapped",
                getattr(self, "_BaseOperator__from_mapped", False),
            )

            dag: Optional[DAG] = kwargs.get('dag') or DagContext.get_current_dag()
            task_group: Optional[TaskGroup] = kwargs.get('task_group')
            if dag and not task_group:
                task_group = TaskGroupContext.get_current_task_group(dag)

            default_args, merged_params = get_merged_defaults(
                dag=dag,
                task_group=task_group,
                task_params=kwargs.pop("params", None),
                task_default_args=kwargs.pop("default_args", None),
            )

            for arg in sig_cache.parameters:
                if arg not in kwargs and arg in default_args:
                    kwargs[arg] = default_args[arg]

            missing_args = non_optional_args - set(kwargs)
            if len(missing_args) == 1:
                raise AirflowException(f"missing keyword argument {missing_args.pop()!r}")
            elif missing_args:
                display = ", ".join(repr(a) for a in sorted(missing_args))
                raise AirflowException(f"missing keyword arguments {display}")

            if merged_params:
                kwargs["params"] = merged_params

            hook = getattr(self, '_hook_apply_defaults', None)
            if hook:
                args, kwargs = hook(**kwargs, default_args=default_args)
                default_args = kwargs.pop('default_args', {})

            if not hasattr(self, '_BaseOperator__init_kwargs'):
                self._BaseOperator__init_kwargs = {}
            self._BaseOperator__from_mapped = instantiated_from_mapped

            result = func(self, **kwargs, default_args=default_args)

            # Store the args passed to init -- we need them to support task.map serialzation!
            self._BaseOperator__init_kwargs.update(kwargs)  # type: ignore

            if not instantiated_from_mapped:
                # Set upstream task defined by XComArgs passed to template fields of the operator.
                self.set_xcomargs_dependencies()
                # Mark instance as instantiated.
                self._BaseOperator__instantiated = True

            return result

        apply_defaults.__non_optional_args = non_optional_args  # type: ignore
        apply_defaults.__param_names = set(non_variadic_params)  # type: ignore

        return cast(T, apply_defaults)

    def __new__(cls, name, bases, namespace, **kwargs):
        new_cls = super().__new__(cls, name, bases, namespace, **kwargs)
        with contextlib.suppress(KeyError):
            # Update the partial descriptor with the class method so it call call the actual function (but let
            # subclasses override it if they need to)
            partial_desc = vars(new_cls)["partial"]
            if isinstance(partial_desc, _PartialDescriptor):
                partial_desc.class_method = classmethod(partial)
        new_cls.__init__ = cls._apply_defaults(new_cls.__init__)
        return new_cls


@functools.total_ordering
class BaseOperator(AbstractOperator, metaclass=BaseOperatorMeta):
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
    :param owner: the owner of the task. Using a meaningful description
        (e.g. user/person/team/role name) to clarify ownership is recommended.
    :param email: the 'to' email address(es) used in email alerts. This can be a
        single email or multiple ones. Multiple addresses can be specified as a
        comma or semi-colon separated string or by passing a list of strings.
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
    :param end_date: if specified, the scheduler won't go beyond this date
    :param depends_on_past: when set to true, task instances will run
        sequentially and only if the previous instance has succeeded or has been skipped.
        The task instance for the start_date is allowed to run.
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
        the static class ``airflow.utils.WeightRule``
    :param queue: which queue to target when running this job. Not
        all executors implement queue management, the CeleryExecutor
        does support targeting specific queues.
    :param pool: the slot pool this task should run in, slot pools are a
        way to limit concurrency for certain tasks
    :param pool_slots: the number of pool slots this task should use (>= 1)
        Values less than 1 are not allowed.
    :param sla: time by which the job is expected to succeed. Note that
        this represents the ``timedelta`` after the period is closed. For
        example if you set an SLA of 1 hour, the scheduler would send an email
        soon after 1:00AM on the ``2016-01-02`` if the ``2016-01-01`` instance
        has not succeeded yet.
        The scheduler pays special attention for jobs with an SLA and
        sends alert
        emails for SLA misses. SLA misses are also recorded in the database
        for future reference. All tasks that share the same SLA time
        get bundled in a single email, sent soon after that time. SLA
        notification are sent once and only once for each task instance.
    :param execution_timeout: max time allowed for the execution of
        this task instance, if it goes beyond it will raise and fail.
    :param on_failure_callback: a function to be called when a task instance
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
        ``{ all_success | all_failed | all_done | all_skipped | one_success |
        one_failed | none_failed | none_failed_min_one_success | none_skipped | always}``
        default is ``all_success``. Options can be set as string or
        using the constants defined in the static class
        ``airflow.utils.TriggerRule``
    :param resources: A map of resource parameter names (the argument names of the
        Resources constructor) to their values.
    :param run_as_user: unix username to impersonate while running the task
    :param max_active_tis_per_dag: When set, a task will be able to limit the concurrent
        runs across execution_dates.
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

    :param do_xcom_push: if True, an XCom is pushed containing the Operator's
        result
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
    """

    # Implementing Operator.
    template_fields: Sequence[str] = ()
    template_ext: Sequence[str] = ()

    template_fields_renderers: Dict[str, str] = {}

    # Defines the color in the UI
    ui_color: str = '#fff'
    ui_fgcolor: str = '#000'

    pool: str = ""

    # base list which includes all the attrs that don't need deep copy.
    _base_operator_shallow_copy_attrs: Tuple[str, ...] = (
        'user_defined_macros',
        'user_defined_filters',
        'params',
        '_log',
    )

    # each operator should override this class attr for shallow copy attrs.
    shallow_copy_attrs: Sequence[str] = ()

    # Defines the operator level extra links
    operator_extra_links: Collection['BaseOperatorLink'] = ()

    # The _serialized_fields are lazily loaded when get_serialized_fields() method is called
    __serialized_fields: Optional[FrozenSet[str]] = None

    partial: Callable[..., OperatorPartial] = _PartialDescriptor()  # type: ignore

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
        'end_date',
        'depends_on_past',
        'wait_for_downstream',
        'priority_weight',
        'sla',
        'execution_timeout',
        'on_execute_callback',
        'on_failure_callback',
        'on_success_callback',
        'on_retry_callback',
        'do_xcom_push',
    }

    # Defines if the operator supports lineage without manual definitions
    supports_lineage = False

    # If True then the class constructor was called
    __instantiated = False
    # List of args as passed to `init()`, after apply_defaults() has been updated. Used to "recreate" the task
    # when mapping
    __init_kwargs: Dict[str, Any]

    # Set to True before calling execute method
    _lock_for_execution = False

    _dag: Optional["DAG"] = None
    task_group: Optional["TaskGroup"] = None

    # subdag parameter is only set for SubDagOperator.
    # Setting it to None by default as other Operators do not have that field
    subdag: Optional["DAG"] = None

    start_date: Optional[pendulum.DateTime] = None
    end_date: Optional[pendulum.DateTime] = None

    # Set to True for an operator instantiated by a mapped operator.
    __from_mapped = False

    def __init__(
        self,
        task_id: str,
        owner: str = DEFAULT_OWNER,
        email: Optional[Union[str, Iterable[str]]] = None,
        email_on_retry: bool = conf.getboolean('email', 'default_email_on_retry', fallback=True),
        email_on_failure: bool = conf.getboolean('email', 'default_email_on_failure', fallback=True),
        retries: Optional[int] = DEFAULT_RETRIES,
        retry_delay: Union[timedelta, float] = DEFAULT_RETRY_DELAY,
        retry_exponential_backoff: bool = False,
        max_retry_delay: Optional[Union[timedelta, float]] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        depends_on_past: bool = False,
        ignore_first_depends_on_past: bool = DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST,
        wait_for_downstream: bool = False,
        dag: Optional['DAG'] = None,
        params: Optional[Dict] = None,
        default_args: Optional[Dict] = None,
        priority_weight: int = DEFAULT_PRIORITY_WEIGHT,
        weight_rule: str = DEFAULT_WEIGHT_RULE,
        queue: str = DEFAULT_QUEUE,
        pool: Optional[str] = None,
        pool_slots: int = DEFAULT_POOL_SLOTS,
        sla: Optional[timedelta] = None,
        execution_timeout: Optional[timedelta] = DEFAULT_TASK_EXECUTION_TIMEOUT,
        on_execute_callback: Optional[TaskStateChangeCallback] = None,
        on_failure_callback: Optional[TaskStateChangeCallback] = None,
        on_success_callback: Optional[TaskStateChangeCallback] = None,
        on_retry_callback: Optional[TaskStateChangeCallback] = None,
        pre_execute: Optional[TaskPreExecuteHook] = None,
        post_execute: Optional[TaskPostExecuteHook] = None,
        trigger_rule: str = DEFAULT_TRIGGER_RULE,
        resources: Optional[Dict[str, Any]] = None,
        run_as_user: Optional[str] = None,
        task_concurrency: Optional[int] = None,
        max_active_tis_per_dag: Optional[int] = None,
        executor_config: Optional[Dict] = None,
        do_xcom_push: bool = True,
        inlets: Optional[Any] = None,
        outlets: Optional[Any] = None,
        task_group: Optional["TaskGroup"] = None,
        doc: Optional[str] = None,
        doc_md: Optional[str] = None,
        doc_json: Optional[str] = None,
        doc_yaml: Optional[str] = None,
        doc_rst: Optional[str] = None,
        **kwargs,
    ):
        from airflow.models.dag import DagContext
        from airflow.utils.task_group import TaskGroupContext

        self.__init_kwargs = {}

        super().__init__()

        kwargs.pop("_airflow_mapped_validation_only", None)
        if kwargs:
            if not conf.getboolean('operators', 'ALLOW_ILLEGAL_ARGUMENTS'):
                raise AirflowException(
                    f"Invalid arguments were passed to {self.__class__.__name__} (task_id: {task_id}). "
                    f"Invalid arguments were:\n**kwargs: {kwargs}",
                )
            warnings.warn(
                f'Invalid arguments were passed to {self.__class__.__name__} (task_id: {task_id}). '
                'Support for passing such arguments will be dropped in future. '
                f'Invalid arguments were:\n**kwargs: {kwargs}',
                category=RemovedInAirflow3Warning,
                stacklevel=3,
            )
        validate_key(task_id)

        dag = dag or DagContext.get_current_dag()
        task_group = task_group or TaskGroupContext.get_current_task_group(dag)

        self.task_id = task_group.child_id(task_id) if task_group else task_id
        if not self.__from_mapped and task_group:
            task_group.add(self)

        self.owner = owner
        self.email = email
        self.email_on_retry = email_on_retry
        self.email_on_failure = email_on_failure

        if execution_timeout is not None and not isinstance(execution_timeout, timedelta):
            raise ValueError(
                f'execution_timeout must be timedelta object but passed as type: {type(execution_timeout)}'
            )
        self.execution_timeout = execution_timeout

        self.on_execute_callback = on_execute_callback
        self.on_failure_callback = on_failure_callback
        self.on_success_callback = on_success_callback
        self.on_retry_callback = on_retry_callback
        self._pre_execute_hook = pre_execute
        self._post_execute_hook = post_execute

        if start_date and not isinstance(start_date, datetime):
            self.log.warning("start_date for %s isn't datetime.datetime", self)
        elif start_date:
            self.start_date = timezone.convert_to_utc(start_date)

        if end_date:
            self.end_date = timezone.convert_to_utc(end_date)

        self.executor_config = executor_config or {}
        self.run_as_user = run_as_user
        self.retries = parse_retries(retries)
        self.queue = queue
        self.pool = Pool.DEFAULT_POOL_NAME if pool is None else pool
        self.pool_slots = pool_slots
        if self.pool_slots < 1:
            dag_str = f" in dag {dag.dag_id}" if dag else ""
            raise ValueError(f"pool slots for {self.task_id}{dag_str} cannot be less than 1")
        self.sla = sla

        if trigger_rule == "dummy":
            warnings.warn(
                "dummy Trigger Rule is deprecated. Please use `TriggerRule.ALWAYS`.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            trigger_rule = TriggerRule.ALWAYS

        if trigger_rule == "none_failed_or_skipped":
            warnings.warn(
                "none_failed_or_skipped Trigger Rule is deprecated. "
                "Please use `none_failed_min_one_success`.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            trigger_rule = TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

        if not TriggerRule.is_valid(trigger_rule):
            raise AirflowException(
                f"The trigger_rule must be one of {TriggerRule.all_triggers()},"
                f"'{dag.dag_id if dag else ''}.{task_id}'; received '{trigger_rule}'."
            )

        self.trigger_rule = TriggerRule(trigger_rule)
        self.depends_on_past: bool = depends_on_past
        self.ignore_first_depends_on_past: bool = ignore_first_depends_on_past
        self.wait_for_downstream: bool = wait_for_downstream
        if wait_for_downstream:
            self.depends_on_past = True

        self.retry_delay = coerce_timedelta(retry_delay, key="retry_delay")
        self.retry_exponential_backoff = retry_exponential_backoff
        self.max_retry_delay = (
            max_retry_delay
            if max_retry_delay is None
            else coerce_timedelta(max_retry_delay, key="max_retry_delay")
        )

        # At execution_time this becomes a normal dict
        self.params: Union[ParamsDict, dict] = ParamsDict(params)
        if priority_weight is not None and not isinstance(priority_weight, int):
            raise AirflowException(
                f"`priority_weight` for task '{self.task_id}' only accepts integers, "
                f"received '{type(priority_weight)}'."
            )
        self.priority_weight = priority_weight
        if not WeightRule.is_valid(weight_rule):
            raise AirflowException(
                f"The weight_rule must be one of "
                f"{WeightRule.all_weight_rules},'{dag.dag_id if dag else ''}.{task_id}'; "
                f"received '{weight_rule}'."
            )
        self.weight_rule = weight_rule
        self.resources = coerce_resources(resources)
        if task_concurrency and not max_active_tis_per_dag:
            # TODO: Remove in Airflow 3.0
            warnings.warn(
                "The 'task_concurrency' parameter is deprecated. Please use 'max_active_tis_per_dag'.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            max_active_tis_per_dag = task_concurrency
        self.max_active_tis_per_dag: Optional[int] = max_active_tis_per_dag
        self.do_xcom_push = do_xcom_push

        self.doc_md = doc_md
        self.doc_json = doc_json
        self.doc_yaml = doc_yaml
        self.doc_rst = doc_rst
        self.doc = doc

        self.upstream_task_ids: Set[str] = set()
        self.downstream_task_ids: Set[str] = set()

        if dag:
            self.dag = dag

        self._log = logging.getLogger("airflow.task.operators")

        # Lineage
        self.inlets: List = []
        self.outlets: List = []

        if inlets:
            self.inlets = (
                inlets
                if isinstance(inlets, list)
                else [
                    inlets,
                ]
            )

        if outlets:
            self.outlets = (
                outlets
                if isinstance(outlets, list)
                else [
                    outlets,
                ]
            )

        if isinstance(self.template_fields, str):
            warnings.warn(
                f"The `template_fields` value for {self.task_type} is a string "
                "but should be a list or tuple of string. Wrapping it in a list for execution. "
                f"Please update {self.task_type} accordingly.",
                UserWarning,
                stacklevel=2,
            )
            self.template_fields = [self.template_fields]

    def __eq__(self, other):
        if type(self) is type(other):
            # Use getattr() instead of __dict__ as __dict__ doesn't return
            # correct values for properties.
            return all(getattr(self, c, None) == getattr(other, c, None) for c in self._comps)
        return False

    def __ne__(self, other):
        return not self == other

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

    # including lineage information
    def __or__(self, other):
        """
        Called for [This Operator] | [Operator], The inlets of other
        will be set to pickup the outlets from this operator. Other will
        be set as a downstream task of this operator.
        """
        if isinstance(other, BaseOperator):
            if not self.outlets and not self.supports_lineage:
                raise ValueError("No outlets defined for this operator")
            other.add_inlets([self.task_id])
            self.set_downstream(other)
        else:
            raise TypeError(f"Right hand side ({other}) is not an Operator")

        return self

    # /Composing Operators ---------------------------------------------

    def __gt__(self, other):
        """
        Called for [Operator] > [Outlet], so that if other is an attr annotated object
        it is set as an outlet of this Operator.
        """
        if not isinstance(other, Iterable):
            other = [other]

        for obj in other:
            if not attr.has(obj):
                raise TypeError(f"Left hand side ({obj}) is not an outlet")
        self.add_outlets(other)

        return self

    def __lt__(self, other):
        """
        Called for [Inlet] > [Operator] or [Operator] < [Inlet], so that if other is
        an attr annotated object it is set as an inlet to this operator
        """
        if not isinstance(other, Iterable):
            other = [other]

        for obj in other:
            if not attr.has(obj):
                raise TypeError(f"{obj} cannot be an inlet")
        self.add_inlets(other)

        return self

    def __setattr__(self, key, value):
        super().__setattr__(key, value)
        if self.__from_mapped or self._lock_for_execution:
            return  # Skip any custom behavior for validation and during execute.
        if key in self.__init_kwargs:
            self.__init_kwargs[key] = value
        if self.__instantiated and key in self.template_fields:
            # Resolve upstreams set by assigning an XComArg after initializing
            # an operator, example:
            #   op = BashOperator()
            #   op.bash_command = "sleep 1"
            self.set_xcomargs_dependencies()

    def add_inlets(self, inlets: Iterable[Any]):
        """Sets inlets to this operator"""
        self.inlets.extend(inlets)

    def add_outlets(self, outlets: Iterable[Any]):
        """Defines the outlets of this operator"""
        self.outlets.extend(outlets)

    def get_inlet_defs(self):
        """:meta private:"""
        return self.inlets

    def get_outlet_defs(self):
        """:meta private:"""
        return self.outlets

    def get_dag(self) -> "Optional[DAG]":
        return self._dag

    @property  # type: ignore[override]
    def dag(self) -> 'DAG':  # type: ignore[override]
        """Returns the Operator's DAG if set, otherwise raises an error"""
        if self._dag:
            return self._dag
        else:
            raise AirflowException(f'Operator {self} has not been assigned to a DAG yet')

    @dag.setter
    def dag(self, dag: Optional['DAG']):
        """
        Operators can be assigned to one DAG, one time. Repeat assignments to
        that same DAG are ok.
        """
        from airflow.models.dag import DAG

        if dag is None:
            self._dag = None
            return
        if not isinstance(dag, DAG):
            raise TypeError(f'Expected DAG; received {dag.__class__.__name__}')
        elif self.has_dag() and self.dag is not dag:
            raise AirflowException(f"The DAG assigned to {self} can not be changed.")

        if self.__from_mapped:
            pass  # Don't add to DAG -- the mapped task takes the place.
        elif self.task_id not in dag.task_dict:
            dag.add_task(self)
        elif self.task_id in dag.task_dict and dag.task_dict[self.task_id] is not self:
            dag.add_task(self)

        self._dag = dag

    def has_dag(self):
        """Returns True if the Operator has been assigned to a DAG."""
        return self._dag is not None

    deps: FrozenSet[BaseTIDep] = frozenset(
        {
            NotInRetryPeriodDep(),
            PrevDagrunDep(),
            TriggerRuleDep(),
            NotPreviouslySkippedDep(),
        }
    )
    """
    Returns the set of dependencies for the operator. These differ from execution
    context dependencies in that they are specific to tasks and can be
    extended/overridden by subclasses.
    """

    def prepare_for_execution(self) -> "BaseOperator":
        """
        Lock task for execution to disable custom action in __setattr__ and
        returns a copy of the task
        """
        other = copy.copy(self)
        other._lock_for_execution = True
        return other

    def set_xcomargs_dependencies(self) -> None:
        """
        Resolves upstream dependencies of a task. In this way passing an ``XComArg``
        as value for a template field will result in creating upstream relation between
        two tasks.

        **Example**: ::

            with DAG(...):
                generate_content = GenerateContentOperator(task_id="generate_content")
                send_email = EmailOperator(..., html_content=generate_content.output)

            # This is equivalent to
            with DAG(...):
                generate_content = GenerateContentOperator(task_id="generate_content")
                send_email = EmailOperator(
                    ..., html_content="{{ task_instance.xcom_pull('generate_content') }}"
                )
                generate_content >> send_email

        """
        from airflow.models.xcom_arg import XComArg

        for field in self.template_fields:
            if hasattr(self, field):
                arg = getattr(self, field)
                XComArg.apply_upstream_relationship(self, arg)

    @prepare_lineage
    def pre_execute(self, context: Any):
        """This hook is triggered right before self.execute() is called."""
        if self._pre_execute_hook is not None:
            self._pre_execute_hook(context)

    def execute(self, context: Context) -> Any:
        """
        This is the main method to derive when creating an operator.
        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        raise NotImplementedError()

    @apply_lineage
    def post_execute(self, context: Any, result: Any = None):
        """
        This hook is triggered right after self.execute() is called.
        It is passed the execution context and any results returned by the
        operator.
        """
        if self._post_execute_hook is not None:
            self._post_execute_hook(context, result)

    def on_kill(self) -> None:
        """
        Override this method to cleanup subprocesses when a task instance
        gets killed. Any use of the threading, subprocess or multiprocessing
        module within an operator needs to be cleaned up or it will leave
        ghost processes behind.
        """

    def __deepcopy__(self, memo):
        # Hack sorting double chained task lists by task_id to avoid hitting
        # max_depth on deepcopy operations.
        sys.setrecursionlimit(5000)  # TODO fix this in a better way

        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        shallow_copy = cls.shallow_copy_attrs + cls._base_operator_shallow_copy_attrs

        for k, v in self.__dict__.items():
            if k == "_BaseOperator__instantiated":
                # Don't set this until the _end_, as it changes behaviour of __setattr__
                continue
            if k not in shallow_copy:
                setattr(result, k, copy.deepcopy(v, memo))
            else:
                setattr(result, k, copy.copy(v))
        result.__instantiated = self.__instantiated
        return result

    def __getstate__(self):
        state = dict(self.__dict__)
        del state['_log']

        return state

    def __setstate__(self, state):
        self.__dict__ = state
        self._log = logging.getLogger("airflow.task.operators")

    def render_template_fields(
        self,
        context: Context,
        jinja_env: Optional["jinja2.Environment"] = None,
    ) -> Optional["BaseOperator"]:
        """Template all attributes listed in template_fields.

        This mutates the attributes in-place and is irreversible.

        :param context: Dict with values to apply on content
        :param jinja_env: Jinja environment
        """
        if not jinja_env:
            jinja_env = self.get_template_env()
        self._do_render_template_fields(self, self.template_fields, context, jinja_env, set())
        return self

    @provide_session
    def clear(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        upstream: bool = False,
        downstream: bool = False,
        session: Session = NEW_SESSION,
    ):
        """
        Clears the state of task instances associated with the task, following
        the parameters specified.
        """
        qry = session.query(TaskInstance).filter(TaskInstance.dag_id == self.dag_id)

        if start_date:
            qry = qry.filter(TaskInstance.execution_date >= start_date)
        if end_date:
            qry = qry.filter(TaskInstance.execution_date <= end_date)

        tasks = [self.task_id]

        if upstream:
            tasks += [t.task_id for t in self.get_flat_relatives(upstream=True)]

        if downstream:
            tasks += [t.task_id for t in self.get_flat_relatives(upstream=False)]

        qry = qry.filter(TaskInstance.task_id.in_(tasks))
        results = qry.all()
        count = len(results)
        clear_task_instances(results, session, dag=self.dag)
        session.commit()
        return count

    @provide_session
    def get_task_instances(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        session: Session = NEW_SESSION,
    ) -> List[TaskInstance]:
        """Get task instances related to this task for a specific date range."""
        from airflow.models import DagRun

        end_date = end_date or timezone.utcnow()
        return (
            session.query(TaskInstance)
            .join(TaskInstance.dag_run)
            .filter(TaskInstance.dag_id == self.dag_id)
            .filter(TaskInstance.task_id == self.task_id)
            .filter(DagRun.execution_date >= start_date)
            .filter(DagRun.execution_date <= end_date)
            .order_by(DagRun.execution_date)
            .all()
        )

    @provide_session
    def run(
        self,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        ignore_first_depends_on_past: bool = True,
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

        start_date = pendulum.instance(start_date or self.start_date)
        end_date = pendulum.instance(end_date or self.end_date or timezone.utcnow())

        for info in self.dag.iter_dagrun_infos_between(start_date, end_date, align=False):
            ignore_depends_on_past = info.logical_date == start_date and ignore_first_depends_on_past
            try:
                dag_run = (
                    session.query(DagRun)
                    .filter(
                        DagRun.dag_id == self.dag_id,
                        DagRun.execution_date == info.logical_date,
                    )
                    .one()
                )
                ti = TaskInstance(self, run_id=dag_run.run_id)
            except NoResultFound:
                # This is _mostly_ only used in tests
                dr = DagRun(
                    dag_id=self.dag_id,
                    run_id=DagRun.generate_run_id(DagRunType.MANUAL, info.logical_date),
                    run_type=DagRunType.MANUAL,
                    execution_date=info.logical_date,
                    data_interval=info.data_interval,
                )
                ti = TaskInstance(self, run_id=dr.run_id)
                ti.dag_run = dr
                session.add(dr)
                session.flush()

            ti.run(
                mark_success=mark_success,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_ti_state=ignore_ti_state,
                test_mode=test_mode,
                session=session,
            )

    def dry_run(self) -> None:
        """Performs dry run for the operator - just render template fields."""
        self.log.info('Dry run')
        for field in self.template_fields:
            try:
                content = getattr(self, field)
            except AttributeError:
                raise AttributeError(
                    f"{field!r} is configured as a template field "
                    f"but {self.task_type} does not have this attribute."
                )

            if content and isinstance(content, str):
                self.log.info('Rendering template for %s', field)
                self.log.info(content)

    def get_direct_relatives(self, upstream: bool = False) -> Iterable["DAGNode"]:
        """
        Get list of the direct relatives to the current task, upstream or
        downstream.
        """
        if upstream:
            return self.upstream_list
        else:
            return self.downstream_list

    def __repr__(self):
        return "<Task({self.task_type}): {self.task_id}>".format(self=self)

    @property
    def operator_class(self) -> Type["BaseOperator"]:  # type: ignore[override]
        return self.__class__

    @property
    def task_type(self) -> str:
        """@property: type of the task"""
        return self.__class__.__name__

    @property
    def operator_name(self) -> str:
        """@property: use a more friendly display name for the operator, if set"""
        try:
            return self.custom_operator_name  # type: ignore
        except AttributeError:
            return self.task_type

    @property
    def roots(self) -> List["BaseOperator"]:
        """Required by DAGNode."""
        return [self]

    @property
    def leaves(self) -> List["BaseOperator"]:
        """Required by DAGNode."""
        return [self]

    @property
    def output(self) -> "XComArg":
        """Returns reference to XCom pushed by current operator"""
        from airflow.models.xcom_arg import XComArg

        return XComArg(operator=self)

    @staticmethod
    def xcom_push(
        context: Any,
        key: str,
        value: Any,
        execution_date: Optional[datetime] = None,
    ) -> None:
        """
        Make an XCom available for tasks to pull.

        :param context: Execution Context Dictionary
        :param key: A key for the XCom
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        :param execution_date: if provided, the XCom will not be visible until
            this date. This can be used, for example, to send a message to a
            task on a future date without it being immediately visible.
        """
        context['ti'].xcom_push(key=key, value=value, execution_date=execution_date)

    @staticmethod
    def xcom_pull(
        context: Any,
        task_ids: Optional[Union[str, List[str]]] = None,
        dag_id: Optional[str] = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: Optional[bool] = None,
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
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        """
        return context['ti'].xcom_pull(
            key=key, task_ids=task_ids, dag_id=dag_id, include_prior_dates=include_prior_dates
        )

    @classmethod
    def get_serialized_fields(cls):
        """Stringified DAGs and operators contain exactly these fields."""
        if not cls.__serialized_fields:
            from airflow.models.dag import DagContext

            # make sure the following dummy task is not added to current active
            # dag in context, otherwise, it will result in
            # `RuntimeError: dictionary changed size during iteration`
            # Exception in SerializedDAG.serialize_dag() call.
            DagContext.push_context_managed_dag(None)
            cls.__serialized_fields = frozenset(
                vars(BaseOperator(task_id='test')).keys()
                - {
                    'upstream_task_ids',
                    'default_args',
                    'dag',
                    '_dag',
                    'label',
                    '_BaseOperator__instantiated',
                    '_BaseOperator__init_kwargs',
                    '_BaseOperator__from_mapped',
                }
                | {  # Class level defaults need to be added to this list
                    'start_date',
                    'end_date',
                    '_task_type',
                    '_operator_name',
                    'subdag',
                    'ui_color',
                    'ui_fgcolor',
                    'template_ext',
                    'template_fields',
                    'template_fields_renderers',
                    'params',
                }
            )
            DagContext.pop_context_managed_dag()

        return cls.__serialized_fields

    def serialize_for_task_group(self) -> Tuple[DagAttributeTypes, Any]:
        """Required by DAGNode."""
        return DagAttributeTypes.OP, self.task_id

    is_mapped: ClassVar[bool] = False

    @property
    def inherits_from_empty_operator(self):
        """Used to determine if an Operator is inherited from EmptyOperator"""
        # This looks like `isinstance(self, EmptyOperator) would work, but this also
        # needs to cope when `self` is a Serialized instance of a EmptyOperator or one
        # of its sub-classes (which don't inherit from anything but BaseOperator).
        return getattr(self, '_is_empty', False)

    def defer(
        self,
        *,
        trigger: BaseTrigger,
        method_name: str,
        kwargs: Optional[Dict[str, Any]] = None,
        timeout: Optional[timedelta] = None,
    ):
        """
        Marks this Operator as being "deferred" - that is, suspending its
        execution until the provided trigger fires an event.

        This is achieved by raising a special exception (TaskDeferred)
        which is caught in the main _execute_task wrapper.
        """
        raise TaskDeferred(trigger=trigger, method_name=method_name, kwargs=kwargs, timeout=timeout)

    def unmap(self, resolve: Union[None, Dict[str, Any], Tuple[Context, Session]]) -> "BaseOperator":
        """:meta private:"""
        return self


# TODO: Deprecate for Airflow 3.0
Chainable = Union[DependencyMixin, Sequence[DependencyMixin]]


def chain(*tasks: Union[DependencyMixin, Sequence[DependencyMixin]]) -> None:
    r"""
    Given a number of tasks, builds a dependency chain.

    This function accepts values of BaseOperator (aka tasks), EdgeModifiers (aka Labels), XComArg, TaskGroups,
    or lists containing any mix of these types (or a mix in the same list). If you want to chain between two
    lists you must ensure they have the same length.

    Using classic operators/sensors:

    .. code-block:: python

        chain(t1, [t2, t3], [t4, t5], t6)

    is equivalent to::

          / -> t2 -> t4 \
        t1               -> t6
          \ -> t3 -> t5 /

    .. code-block:: python

        t1.set_downstream(t2)
        t1.set_downstream(t3)
        t2.set_downstream(t4)
        t3.set_downstream(t5)
        t4.set_downstream(t6)
        t5.set_downstream(t6)

    Using task-decorated functions aka XComArgs:

    .. code-block:: python

        chain(x1(), [x2(), x3()], [x4(), x5()], x6())

    is equivalent to::

          / -> x2 -> x4 \
        x1               -> x6
          \ -> x3 -> x5 /

    .. code-block:: python

        x1 = x1()
        x2 = x2()
        x3 = x3()
        x4 = x4()
        x5 = x5()
        x6 = x6()
        x1.set_downstream(x2)
        x1.set_downstream(x3)
        x2.set_downstream(x4)
        x3.set_downstream(x5)
        x4.set_downstream(x6)
        x5.set_downstream(x6)

    Using TaskGroups:

    .. code-block:: python

        chain(t1, task_group1, task_group2, t2)

        t1.set_downstream(task_group1)
        task_group1.set_downstream(task_group2)
        task_group2.set_downstream(t2)


    It is also possible to mix between classic operator/sensor, EdgeModifiers, XComArg, and TaskGroups:

    .. code-block:: python

        chain(t1, [Label("branch one"), Label("branch two")], [x1(), x2()], task_group1, t2())

    is equivalent to::

          / "branch one" -> x1 \
        t1                      -> t2 -> x3
          \ "branch two" -> x2 /

    .. code-block:: python

        x1 = x1()
        x2 = x2()
        x3 = x3()
        label1 = Label("branch one")
        label2 = Label("branch two")
        t1.set_downstream(label1)
        label1.set_downstream(x1)
        t2.set_downstream(label2)
        label2.set_downstream(x2)
        x1.set_downstream(task_group1)
        x2.set_downstream(task_group1)
        task_group1.set_downstream(x3)

        # or

        x1 = x1()
        x2 = x2()
        x3 = x3()
        t1.set_downstream(x1, edge_modifier=Label("branch one"))
        t1.set_downstream(x2, edge_modifier=Label("branch two"))
        x1.set_downstream(task_group1)
        x2.set_downstream(task_group1)
        task_group1.set_downstream(x3)


    :param tasks: Individual and/or list of tasks, EdgeModifiers, XComArgs, or TaskGroups to set dependencies
    """
    for index, up_task in enumerate(tasks[:-1]):
        down_task = tasks[index + 1]
        if isinstance(up_task, DependencyMixin):
            up_task.set_downstream(down_task)
            continue
        if isinstance(down_task, DependencyMixin):
            down_task.set_upstream(up_task)
            continue
        if not isinstance(up_task, Sequence) or not isinstance(down_task, Sequence):
            raise TypeError(f'Chain not supported between instances of {type(up_task)} and {type(down_task)}')
        up_task_list = up_task
        down_task_list = down_task
        if len(up_task_list) != len(down_task_list):
            raise AirflowException(
                f'Chain not supported for different length Iterable. '
                f'Got {len(up_task_list)} and {len(down_task_list)}.'
            )
        for up_t, down_t in zip(up_task_list, down_task_list):
            up_t.set_downstream(down_t)


def cross_downstream(
    from_tasks: Sequence[DependencyMixin],
    to_tasks: Union[DependencyMixin, Sequence[DependencyMixin]],
):
    r"""
    Set downstream dependencies for all tasks in from_tasks to all tasks in to_tasks.

    Using classic operators/sensors:

    .. code-block:: python

        cross_downstream(from_tasks=[t1, t2, t3], to_tasks=[t4, t5, t6])

    is equivalent to::

        t1 ---> t4
           \ /
        t2 -X -> t5
           / \
        t3 ---> t6

    .. code-block:: python

        t1.set_downstream(t4)
        t1.set_downstream(t5)
        t1.set_downstream(t6)
        t2.set_downstream(t4)
        t2.set_downstream(t5)
        t2.set_downstream(t6)
        t3.set_downstream(t4)
        t3.set_downstream(t5)
        t3.set_downstream(t6)

    Using task-decorated functions aka XComArgs:

    .. code-block:: python

        cross_downstream(from_tasks=[x1(), x2(), x3()], to_tasks=[x4(), x5(), x6()])

    is equivalent to::

        x1 ---> x4
           \ /
        x2 -X -> x5
           / \
        x3 ---> x6

    .. code-block:: python

        x1 = x1()
        x2 = x2()
        x3 = x3()
        x4 = x4()
        x5 = x5()
        x6 = x6()
        x1.set_downstream(x4)
        x1.set_downstream(x5)
        x1.set_downstream(x6)
        x2.set_downstream(x4)
        x2.set_downstream(x5)
        x2.set_downstream(x6)
        x3.set_downstream(x4)
        x3.set_downstream(x5)
        x3.set_downstream(x6)

    It is also possible to mix between classic operator/sensor and XComArg tasks:

    .. code-block:: python

        cross_downstream(from_tasks=[t1, x2(), t3], to_tasks=[x1(), t2, x3()])

    is equivalent to::

        t1 ---> x1
           \ /
        x2 -X -> t2
           / \
        t3 ---> x3

    .. code-block:: python

        x1 = x1()
        x2 = x2()
        x3 = x3()
        t1.set_downstream(x1)
        t1.set_downstream(t2)
        t1.set_downstream(x3)
        x2.set_downstream(x1)
        x2.set_downstream(t2)
        x2.set_downstream(x3)
        t3.set_downstream(x1)
        t3.set_downstream(t2)
        t3.set_downstream(x3)

    :param from_tasks: List of tasks or XComArgs to start from.
    :param to_tasks: List of tasks or XComArgs to set as downstream dependencies.
    """
    for task in from_tasks:
        task.set_downstream(to_tasks)


@attr.s(auto_attribs=True)
class BaseOperatorLink(metaclass=ABCMeta):
    """Abstract base class that defines how we get an operator link."""

    operators: ClassVar[List[Type[BaseOperator]]] = []
    """
    This property will be used by Airflow Plugins to find the Operators to which you want
    to assign this Operator Link

    :return: List of Operator classes used by task for which you want to create extra link
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Name of the link. This will be the button name on the task UI."""

    @abstractmethod
    def get_link(self, operator: BaseOperator, *, ti_key: "TaskInstanceKey") -> str:
        """Link to external system.

        Note: The old signature of this function was ``(self, operator, dttm: datetime)``. That is still
        supported at runtime but is deprecated.

        :param operator: The Airflow operator object this link is associated to.
        :param ti_key: TaskInstance ID to return link for.
        :return: link to external system
        """
