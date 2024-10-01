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

import abc
import collections.abc
import contextlib
import copy
import functools
import inspect
import logging
import sys
import warnings
from datetime import datetime, timedelta
from functools import total_ordering, wraps
from types import FunctionType
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Iterable,
    NoReturn,
    Sequence,
    TypeVar,
    Union,
    cast,
)

import attr
import pendulum
from sqlalchemy import select
from sqlalchemy.orm.exc import NoResultFound

from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    FailStopDagInvalidTriggerRule,
    TaskDeferralError,
    TaskDeferred,
)
from airflow.lineage import apply_lineage, prepare_lineage
from airflow.models.abstractoperator import (
    DEFAULT_EXECUTOR,
    DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST,
    DEFAULT_OWNER,
    DEFAULT_POOL_SLOTS,
    DEFAULT_PRIORITY_WEIGHT,
    DEFAULT_QUEUE,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
    DEFAULT_TASK_EXECUTION_TIMEOUT,
    DEFAULT_TRIGGER_RULE,
    DEFAULT_WAIT_FOR_PAST_DEPENDS_BEFORE_SKIPPING,
    DEFAULT_WEIGHT_RULE,
    AbstractOperator,
)
from airflow.models.base import _sentinel
from airflow.models.mappedoperator import OperatorPartial, validate_mapping_kwargs
from airflow.models.param import ParamsDict
from airflow.models.pool import Pool
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.models.taskmixin import DependencyMixin
from airflow.serialization.enums import DagAttributeTypes
from airflow.task.priority_strategy import PriorityWeightStrategy, validate_and_load_priority_weight_strategy
from airflow.ti_deps.deps.mapped_task_upstream_dep import MappedTaskUpstreamDep
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.not_previously_skipped_dep import NotPreviouslySkippedDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep
from airflow.utils import timezone
from airflow.utils.context import Context, context_get_outlet_events
from airflow.utils.decorators import fixup_decorator_warning_stack
from airflow.utils.edgemodifier import EdgeModifier
from airflow.utils.helpers import validate_instance_args, validate_key
from airflow.utils.operator_helpers import ExecutionCallableRunner
from airflow.utils.operator_resources import Resources
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.setup_teardown import SetupTeardownContext
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import NOTSET, AttributeRemoved, DagRunTriggeredByType
from airflow.utils.xcom import XCOM_RETURN_KEY

if TYPE_CHECKING:
    from types import ClassMethodDescriptorType

    import jinja2  # Slow import.
    from sqlalchemy.orm import Session

    from airflow.models.abstractoperator import TaskStateChangeCallback
    from airflow.models.baseoperatorlink import BaseOperatorLink
    from airflow.models.dag import DAG
    from airflow.models.operator import Operator
    from airflow.models.xcom_arg import XComArg
    from airflow.ti_deps.deps.base_ti_dep import BaseTIDep
    from airflow.triggers.base import BaseTrigger, StartTriggerArgs
    from airflow.utils.task_group import TaskGroup
    from airflow.utils.types import ArgNotSet

TaskPreExecuteHook = Callable[[Context], None]
TaskPostExecuteHook = Callable[[Context, Any], None]

T = TypeVar("T", bound=FunctionType)

logger = logging.getLogger("airflow.models.baseoperator.BaseOperator")


def parse_retries(retries: Any) -> int | None:
    if retries is None or type(retries) == int:  # noqa: E721
        return retries
    try:
        parsed_retries = int(retries)
    except (TypeError, ValueError):
        raise AirflowException(f"'retries' type must be int, not {type(retries).__name__}")
    logger.warning("Implicitly converting 'retries' from %r to int", retries)
    return parsed_retries


def coerce_timedelta(value: float | timedelta, *, key: str) -> timedelta:
    if isinstance(value, timedelta):
        return value
    logger.debug("%s isn't a timedelta object, assuming secs", key)
    return timedelta(seconds=value)


def coerce_resources(resources: dict[str, Any] | None) -> Resources | None:
    if resources is None:
        return None
    return Resources(**resources)


def _get_parent_defaults(dag: DAG | None, task_group: TaskGroup | None) -> tuple[dict, ParamsDict]:
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
    dag: DAG | None,
    task_group: TaskGroup | None,
    task_params: collections.abc.MutableMapping | None,
    task_default_args: dict | None,
) -> tuple[dict, ParamsDict]:
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

    class_method: ClassMethodDescriptorType | None = None

    def __get__(
        self, obj: BaseOperator, cls: type[BaseOperator] | None = None
    ) -> Callable[..., OperatorPartial]:
        # Call this "partial" so it looks nicer in stack traces.
        def partial(**kwargs):
            raise TypeError("partial can only be called on Operator classes, not Tasks themselves")

        if obj is not None:
            return partial
        return self.class_method.__get__(cls, cls)


_PARTIAL_DEFAULTS: dict[str, Any] = {
    "map_index_template": None,
    "owner": DEFAULT_OWNER,
    "trigger_rule": DEFAULT_TRIGGER_RULE,
    "depends_on_past": False,
    "ignore_first_depends_on_past": DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST,
    "wait_for_past_depends_before_skipping": DEFAULT_WAIT_FOR_PAST_DEPENDS_BEFORE_SKIPPING,
    "wait_for_downstream": False,
    "retries": DEFAULT_RETRIES,
    "executor": DEFAULT_EXECUTOR,
    "queue": DEFAULT_QUEUE,
    "pool_slots": DEFAULT_POOL_SLOTS,
    "execution_timeout": DEFAULT_TASK_EXECUTION_TIMEOUT,
    "retry_delay": DEFAULT_RETRY_DELAY,
    "retry_exponential_backoff": False,
    "priority_weight": DEFAULT_PRIORITY_WEIGHT,
    "weight_rule": DEFAULT_WEIGHT_RULE,
    "inlets": [],
    "outlets": [],
    "allow_nested_operators": True,
}


# This is what handles the actual mapping.
def partial(
    operator_class: type[BaseOperator],
    *,
    task_id: str,
    dag: DAG | None = None,
    task_group: TaskGroup | None = None,
    start_date: datetime | ArgNotSet = NOTSET,
    end_date: datetime | ArgNotSet = NOTSET,
    owner: str | ArgNotSet = NOTSET,
    email: None | str | Iterable[str] | ArgNotSet = NOTSET,
    params: collections.abc.MutableMapping | None = None,
    resources: dict[str, Any] | None | ArgNotSet = NOTSET,
    trigger_rule: str | ArgNotSet = NOTSET,
    depends_on_past: bool | ArgNotSet = NOTSET,
    ignore_first_depends_on_past: bool | ArgNotSet = NOTSET,
    wait_for_past_depends_before_skipping: bool | ArgNotSet = NOTSET,
    wait_for_downstream: bool | ArgNotSet = NOTSET,
    retries: int | None | ArgNotSet = NOTSET,
    queue: str | ArgNotSet = NOTSET,
    pool: str | ArgNotSet = NOTSET,
    pool_slots: int | ArgNotSet = NOTSET,
    execution_timeout: timedelta | None | ArgNotSet = NOTSET,
    max_retry_delay: None | timedelta | float | ArgNotSet = NOTSET,
    retry_delay: timedelta | float | ArgNotSet = NOTSET,
    retry_exponential_backoff: bool | ArgNotSet = NOTSET,
    priority_weight: int | ArgNotSet = NOTSET,
    weight_rule: str | PriorityWeightStrategy | ArgNotSet = NOTSET,
    sla: timedelta | None | ArgNotSet = NOTSET,
    map_index_template: str | None | ArgNotSet = NOTSET,
    max_active_tis_per_dag: int | None | ArgNotSet = NOTSET,
    max_active_tis_per_dagrun: int | None | ArgNotSet = NOTSET,
    on_execute_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] | ArgNotSet = NOTSET,
    on_failure_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] | ArgNotSet = NOTSET,
    on_success_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] | ArgNotSet = NOTSET,
    on_retry_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] | ArgNotSet = NOTSET,
    on_skipped_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] | ArgNotSet = NOTSET,
    run_as_user: str | None | ArgNotSet = NOTSET,
    executor: str | None | ArgNotSet = NOTSET,
    executor_config: dict | None | ArgNotSet = NOTSET,
    inlets: Any | None | ArgNotSet = NOTSET,
    outlets: Any | None | ArgNotSet = NOTSET,
    doc: str | None | ArgNotSet = NOTSET,
    doc_md: str | None | ArgNotSet = NOTSET,
    doc_json: str | None | ArgNotSet = NOTSET,
    doc_yaml: str | None | ArgNotSet = NOTSET,
    doc_rst: str | None | ArgNotSet = NOTSET,
    task_display_name: str | None | ArgNotSet = NOTSET,
    logger_name: str | None | ArgNotSet = NOTSET,
    allow_nested_operators: bool = True,
    **kwargs,
) -> OperatorPartial:
    from airflow.models.dag import DagContext
    from airflow.utils.task_group import TaskGroupContext

    validate_mapping_kwargs(operator_class, "partial", kwargs)

    dag = dag or DagContext.get_current_dag()
    if dag:
        task_group = task_group or TaskGroupContext.get_current_task_group(dag)
    if task_group:
        task_id = task_group.child_id(task_id)

    # Merge DAG and task group level defaults into user-supplied values.
    dag_default_args, partial_params = get_merged_defaults(
        dag=dag,
        task_group=task_group,
        task_params=params,
        task_default_args=kwargs.pop("default_args", None),
    )

    # Create partial_kwargs from args and kwargs
    partial_kwargs: dict[str, Any] = {
        **kwargs,
        "dag": dag,
        "task_group": task_group,
        "task_id": task_id,
        "map_index_template": map_index_template,
        "start_date": start_date,
        "end_date": end_date,
        "owner": owner,
        "email": email,
        "trigger_rule": trigger_rule,
        "depends_on_past": depends_on_past,
        "ignore_first_depends_on_past": ignore_first_depends_on_past,
        "wait_for_past_depends_before_skipping": wait_for_past_depends_before_skipping,
        "wait_for_downstream": wait_for_downstream,
        "retries": retries,
        "queue": queue,
        "pool": pool,
        "pool_slots": pool_slots,
        "execution_timeout": execution_timeout,
        "max_retry_delay": max_retry_delay,
        "retry_delay": retry_delay,
        "retry_exponential_backoff": retry_exponential_backoff,
        "priority_weight": priority_weight,
        "weight_rule": weight_rule,
        "sla": sla,
        "max_active_tis_per_dag": max_active_tis_per_dag,
        "max_active_tis_per_dagrun": max_active_tis_per_dagrun,
        "on_execute_callback": on_execute_callback,
        "on_failure_callback": on_failure_callback,
        "on_retry_callback": on_retry_callback,
        "on_success_callback": on_success_callback,
        "on_skipped_callback": on_skipped_callback,
        "run_as_user": run_as_user,
        "executor": executor,
        "executor_config": executor_config,
        "inlets": inlets,
        "outlets": outlets,
        "resources": resources,
        "doc": doc,
        "doc_json": doc_json,
        "doc_md": doc_md,
        "doc_rst": doc_rst,
        "doc_yaml": doc_yaml,
        "task_display_name": task_display_name,
        "logger_name": logger_name,
        "allow_nested_operators": allow_nested_operators,
    }

    # Inject DAG-level default args into args provided to this function.
    partial_kwargs.update((k, v) for k, v in dag_default_args.items() if partial_kwargs.get(k) is NOTSET)

    # Fill fields not provided by the user with default values.
    partial_kwargs = {k: _PARTIAL_DEFAULTS.get(k) if v is NOTSET else v for k, v in partial_kwargs.items()}

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

    return OperatorPartial(
        operator_class=operator_class,
        kwargs=partial_kwargs,
        params=partial_params,
    )


class ExecutorSafeguard:
    """
    The ExecutorSafeguard decorator.

    Checks if the execute method of an operator isn't manually called outside
    the TaskInstance as we want to avoid bad mixing between decorated and
    classic operators.
    """

    test_mode = conf.getboolean("core", "unit_test_mode")

    @classmethod
    def decorator(cls, func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            from airflow.decorators.base import DecoratedOperator
            from airflow.models.streamedoperator import StreamedOperator

            sentinel = kwargs.pop(f"{self.__class__.__name__}__sentinel", None)

            if (
                not cls.test_mode
                and not sentinel == _sentinel
                and not isinstance(self, DecoratedOperator)
                and not isinstance(self, StreamedOperator)
            ):
                message = f"{self.__class__.__name__}.{func.__name__} cannot be called outside TaskInstance!"
                if not self.allow_nested_operators:
                    raise AirflowException(message)
                self.log.warning(message)
            return func(self, *args, **kwargs)

        return wrapper


class BaseOperatorMeta(abc.ABCMeta):
    """Metaclass of BaseOperator."""

    @classmethod
    def _apply_defaults(cls, func: T) -> T:
        """
        Look for an argument named "default_args", and fill the unspecified arguments from it.

        Since python2.* isn't clear about which arguments are missing when
        calling a function, and that this can be quite confusing with multi-level
        inheritance and argument defaults, this decorator also alerts with
        specific information about the missing arguments.
        """
        # Cache inspect.signature for the wrapper closure to avoid calling it
        # at every decorated invocation. This is separate sig_cache created
        # per decoration, i.e. each function decorated using apply_defaults will
        # have a different sig_cache.
        sig_cache = inspect.signature(func)
        non_variadic_params = {
            name: param
            for (name, param) in sig_cache.parameters.items()
            if param.name != "self" and param.kind not in (param.VAR_POSITIONAL, param.VAR_KEYWORD)
        }
        non_optional_args = {
            name
            for name, param in non_variadic_params.items()
            if param.default == param.empty and name != "task_id"
        }

        fixup_decorator_warning_stack(func)

        @wraps(func)
        def apply_defaults(self: BaseOperator, *args: Any, **kwargs: Any) -> Any:
            from airflow.models.dag import DagContext
            from airflow.utils.task_group import TaskGroupContext

            if args:
                raise AirflowException("Use keyword arguments when initializing operators")

            instantiated_from_mapped = kwargs.pop(
                "_airflow_from_mapped",
                getattr(self, "_BaseOperator__from_mapped", False),
            )

            dag: DAG | None = kwargs.get("dag") or DagContext.get_current_dag()
            task_group: TaskGroup | None = kwargs.get("task_group")
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

            missing_args = non_optional_args.difference(kwargs)
            if len(missing_args) == 1:
                raise AirflowException(f"missing keyword argument {missing_args.pop()!r}")
            elif missing_args:
                display = ", ".join(repr(a) for a in sorted(missing_args))
                raise AirflowException(f"missing keyword arguments {display}")

            if merged_params:
                kwargs["params"] = merged_params

            hook = getattr(self, "_hook_apply_defaults", None)
            if hook:
                args, kwargs = hook(**kwargs, default_args=default_args)
                default_args = kwargs.pop("default_args", {})

            if not hasattr(self, "_BaseOperator__init_kwargs"):
                self._BaseOperator__init_kwargs = {}
            self._BaseOperator__from_mapped = instantiated_from_mapped

            result = func(self, **kwargs, default_args=default_args)

            # Store the args passed to init -- we need them to support task.map serialization!
            self._BaseOperator__init_kwargs.update(kwargs)  # type: ignore

            # Set upstream task defined by XComArgs passed to template fields of the operator.
            # BUT: only do this _ONCE_, not once for each class in the hierarchy
            if not instantiated_from_mapped and func == self.__init__.__wrapped__:  # type: ignore[misc]
                self.set_xcomargs_dependencies()
                # Mark instance as instantiated.
                self._BaseOperator__instantiated = True

            return result

        apply_defaults.__non_optional_args = non_optional_args  # type: ignore
        apply_defaults.__param_names = set(non_variadic_params)  # type: ignore

        return cast(T, apply_defaults)

    def __new__(cls, name, bases, namespace, **kwargs):
        execute_method = namespace.get("execute")
        if callable(execute_method) and not getattr(execute_method, "__isabstractmethod__", False):
            namespace["execute"] = ExecutorSafeguard().decorator(execute_method)
        new_cls = super().__new__(cls, name, bases, namespace, **kwargs)
        with contextlib.suppress(KeyError):
            # Update the partial descriptor with the class method, so it calls the actual function
            # (but let subclasses override it if they need to)
            partial_desc = vars(new_cls)["partial"]
            if isinstance(partial_desc, _PartialDescriptor):
                partial_desc.class_method = classmethod(partial)

        # We patch `__init__` only if the class defines it.
        if inspect.getmro(new_cls)[1].__init__ is not new_cls.__init__:
            new_cls.__init__ = cls._apply_defaults(new_cls.__init__)

        return new_cls


# TODO: The following mapping is used to validate that the arguments passed to the BaseOperator are of the
#  correct type. This is a temporary solution until we find a more sophisticated method for argument
#  validation. One potential method is to use `get_type_hints` from the typing module. However, this is not
#  fully compatible with future annotations for Python versions below 3.10. Once we require a minimum Python
#  version that supports `get_type_hints` effectively or find a better approach, we can replace this
#  manual type-checking method.
BASEOPERATOR_ARGS_EXPECTED_TYPES = {
    "task_id": str,
    "email": (str, Iterable),
    "email_on_retry": bool,
    "email_on_failure": bool,
    "retries": int,
    "retry_exponential_backoff": bool,
    "depends_on_past": bool,
    "ignore_first_depends_on_past": bool,
    "wait_for_past_depends_before_skipping": bool,
    "wait_for_downstream": bool,
    "priority_weight": int,
    "queue": str,
    "pool": str,
    "pool_slots": int,
    "trigger_rule": str,
    "run_as_user": str,
    "task_concurrency": int,
    "map_index_template": str,
    "max_active_tis_per_dag": int,
    "max_active_tis_per_dagrun": int,
    "executor": str,
    "do_xcom_push": bool,
    "multiple_outputs": bool,
    "doc": str,
    "doc_md": str,
    "doc_json": str,
    "doc_yaml": str,
    "doc_rst": str,
    "task_display_name": str,
    "logger_name": str,
    "allow_nested_operators": bool,
}


@total_ordering
class BaseOperator(AbstractOperator, metaclass=BaseOperatorMeta):
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
        the ``execution_date`` for the first task instance. The best practice
        is to have the start_date rounded
        to your DAG's schedule. Daily jobs have their start_date
        some day at 00:00:00, hourly jobs have their start_date at 00:00
        of a specific hour. Note that Airflow simply looks at the latest
        ``execution_date`` and adds the schedule to determine
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
        runs across execution_dates.
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

    # Implementing Operator.
    template_fields: Sequence[str] = ()
    template_ext: Sequence[str] = ()

    template_fields_renderers: dict[str, str] = {}

    # Defines the color in the UI
    ui_color: str = "#fff"
    ui_fgcolor: str = "#000"

    pool: str = ""

    # base list which includes all the attrs that don't need deep copy.
    _base_operator_shallow_copy_attrs: tuple[str, ...] = (
        "user_defined_macros",
        "user_defined_filters",
        "params",
    )

    # each operator should override this class attr for shallow copy attrs.
    shallow_copy_attrs: Sequence[str] = ()

    # Defines the operator level extra links
    operator_extra_links: Collection[BaseOperatorLink] = ()

    # The _serialized_fields are lazily loaded when get_serialized_fields() method is called
    __serialized_fields: frozenset[str] | None = None

    partial: Callable[..., OperatorPartial] = _PartialDescriptor()  # type: ignore

    _comps = {
        "task_id",
        "dag_id",
        "owner",
        "email",
        "email_on_retry",
        "retry_delay",
        "retry_exponential_backoff",
        "max_retry_delay",
        "start_date",
        "end_date",
        "depends_on_past",
        "wait_for_downstream",
        "priority_weight",
        "sla",
        "execution_timeout",
        "on_execute_callback",
        "on_failure_callback",
        "on_success_callback",
        "on_retry_callback",
        "on_skipped_callback",
        "do_xcom_push",
        "multiple_outputs",
        "allow_nested_operators",
        "executor",
    }

    # Defines if the operator supports lineage without manual definitions
    supports_lineage = False

    # If True then the class constructor was called
    __instantiated = False
    # List of args as passed to `init()`, after apply_defaults() has been updated. Used to "recreate" the task
    # when mapping
    __init_kwargs: dict[str, Any]

    # Set to True before calling execute method
    _lock_for_execution = False

    _dag: DAG | None = None
    task_group: TaskGroup | None = None

    start_date: pendulum.DateTime | None = None
    end_date: pendulum.DateTime | None = None

    # Set to True for an operator instantiated by a mapped operator.
    __from_mapped = False

    start_trigger_args: StartTriggerArgs | None = None
    start_from_trigger: bool = False

    def __init__(
        self,
        task_id: str,
        owner: str = DEFAULT_OWNER,
        email: str | Iterable[str] | None = None,
        email_on_retry: bool = conf.getboolean("email", "default_email_on_retry", fallback=True),
        email_on_failure: bool = conf.getboolean("email", "default_email_on_failure", fallback=True),
        retries: int | None = DEFAULT_RETRIES,
        retry_delay: timedelta | float = DEFAULT_RETRY_DELAY,
        retry_exponential_backoff: bool = False,
        max_retry_delay: timedelta | float | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        depends_on_past: bool = False,
        ignore_first_depends_on_past: bool = DEFAULT_IGNORE_FIRST_DEPENDS_ON_PAST,
        wait_for_past_depends_before_skipping: bool = DEFAULT_WAIT_FOR_PAST_DEPENDS_BEFORE_SKIPPING,
        wait_for_downstream: bool = False,
        dag: DAG | None = None,
        params: collections.abc.MutableMapping | None = None,
        default_args: dict | None = None,
        priority_weight: int = DEFAULT_PRIORITY_WEIGHT,
        weight_rule: str | PriorityWeightStrategy = DEFAULT_WEIGHT_RULE,
        queue: str = DEFAULT_QUEUE,
        pool: str | None = None,
        pool_slots: int = DEFAULT_POOL_SLOTS,
        sla: timedelta | None = None,
        execution_timeout: timedelta | None = DEFAULT_TASK_EXECUTION_TIMEOUT,
        on_execute_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None,
        on_failure_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None,
        on_success_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None,
        on_retry_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None,
        on_skipped_callback: None | TaskStateChangeCallback | list[TaskStateChangeCallback] = None,
        pre_execute: TaskPreExecuteHook | None = None,
        post_execute: TaskPostExecuteHook | None = None,
        trigger_rule: str = DEFAULT_TRIGGER_RULE,
        resources: dict[str, Any] | None = None,
        run_as_user: str | None = None,
        map_index_template: str | None = None,
        max_active_tis_per_dag: int | None = None,
        max_active_tis_per_dagrun: int | None = None,
        executor: str | None = None,
        executor_config: dict | None = None,
        do_xcom_push: bool = True,
        multiple_outputs: bool = False,
        inlets: Any | None = None,
        outlets: Any | None = None,
        task_group: TaskGroup | None = None,
        doc: str | None = None,
        doc_md: str | None = None,
        doc_json: str | None = None,
        doc_yaml: str | None = None,
        doc_rst: str | None = None,
        task_display_name: str | None = None,
        logger_name: str | None = None,
        allow_nested_operators: bool = True,
        **kwargs,
    ):
        from airflow.models.dag import DagContext
        from airflow.utils.task_group import TaskGroupContext

        self.__init_kwargs = {}

        super().__init__()

        kwargs.pop("_airflow_mapped_validation_only", None)
        if kwargs:
            raise AirflowException(
                f"Invalid arguments were passed to {self.__class__.__name__} (task_id: {task_id}). "
                f"Invalid arguments were:\n**kwargs: {kwargs}",
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
                f"execution_timeout must be timedelta object but passed as type: {type(execution_timeout)}"
            )
        self.execution_timeout = execution_timeout

        self.on_execute_callback = on_execute_callback
        self.on_failure_callback = on_failure_callback
        self.on_success_callback = on_success_callback
        self.on_retry_callback = on_retry_callback
        self.on_skipped_callback = on_skipped_callback
        self._pre_execute_hook = pre_execute
        self._post_execute_hook = post_execute

        if start_date and not isinstance(start_date, datetime):
            self.log.warning("start_date for %s isn't datetime.datetime", self)
        elif start_date:
            self.start_date = timezone.convert_to_utc(start_date)

        if end_date:
            self.end_date = timezone.convert_to_utc(end_date)

        self.executor = executor
        self.executor_config = executor_config or {}
        self.run_as_user = run_as_user
        self.retries = parse_retries(retries)
        self.queue = queue
        self.pool = Pool.DEFAULT_POOL_NAME if pool is None else pool
        self.pool_slots = pool_slots
        if self.pool_slots < 1:
            dag_str = f" in dag {dag.dag_id}" if dag else ""
            raise ValueError(f"pool slots for {self.task_id}{dag_str} cannot be less than 1")

        if sla:
            self.log.warning(
                "The SLA feature is removed in Airflow 3.0, to be replaced with a new implementation in 3.1"
            )

        if not TriggerRule.is_valid(trigger_rule):
            raise AirflowException(
                f"The trigger_rule must be one of {TriggerRule.all_triggers()},"
                f"'{dag.dag_id if dag else ''}.{task_id}'; received '{trigger_rule}'."
            )

        self.trigger_rule: TriggerRule = TriggerRule(trigger_rule)
        FailStopDagInvalidTriggerRule.check(dag=dag, trigger_rule=self.trigger_rule)

        self.depends_on_past: bool = depends_on_past
        self.ignore_first_depends_on_past: bool = ignore_first_depends_on_past
        self.wait_for_past_depends_before_skipping: bool = wait_for_past_depends_before_skipping
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
        self.params: ParamsDict | dict = ParamsDict(params)
        if priority_weight is not None and not isinstance(priority_weight, int):
            raise AirflowException(
                f"`priority_weight` for task '{self.task_id}' only accepts integers, "
                f"received '{type(priority_weight)}'."
            )
        self.priority_weight = priority_weight
        self.weight_rule = validate_and_load_priority_weight_strategy(weight_rule)
        self.resources = coerce_resources(resources)
        self.max_active_tis_per_dag: int | None = max_active_tis_per_dag
        self.max_active_tis_per_dagrun: int | None = max_active_tis_per_dagrun
        self.do_xcom_push: bool = do_xcom_push
        self.map_index_template: str | None = map_index_template
        self.multiple_outputs: bool = multiple_outputs

        self.doc_md = doc_md
        self.doc_json = doc_json
        self.doc_yaml = doc_yaml
        self.doc_rst = doc_rst
        self.doc = doc
        # Populate the display field only if provided and different from task id
        self._task_display_property_value = (
            task_display_name if task_display_name and task_display_name != task_id else None
        )

        self.upstream_task_ids: set[str] = set()
        self.downstream_task_ids: set[str] = set()

        if dag:
            self.dag = dag

        self._log_config_logger_name = "airflow.task.operators"
        self._logger_name = logger_name
        self.allow_nested_operators: bool = allow_nested_operators

        # Lineage
        self.inlets: list = []
        self.outlets: list = []

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

        self._is_setup = False
        self._is_teardown = False
        if SetupTeardownContext.active:
            SetupTeardownContext.update_context_map(self)

        validate_instance_args(self, BASEOPERATOR_ARGS_EXPECTED_TYPES)

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
        Return [This Operator] | [Operator].

        The inlets of other will be set to pick up the outlets from this operator.
        Other will be set as a downstream task of this operator.
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
        Return [Operator] > [Outlet].

        If other is an attr annotated object it is set as an outlet of this Operator.
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
        Return [Inlet] > [Operator] or [Operator] < [Inlet].

        If other is an attr annotated object it is set as an inlet to this operator.
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
        """Set inlets to this operator."""
        self.inlets.extend(inlets)

    def add_outlets(self, outlets: Iterable[Any]):
        """Define the outlets of this operator."""
        self.outlets.extend(outlets)

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

    def get_dag(self) -> DAG | None:
        return self._dag

    @property  # type: ignore[override]
    def dag(self) -> DAG:  # type: ignore[override]
        """Returns the Operator's DAG if set, otherwise raises an error."""
        if self._dag:
            return self._dag
        else:
            raise AirflowException(f"Operator {self} has not been assigned to a DAG yet")

    @dag.setter
    def dag(self, dag: DAG | None):
        """Operators can be assigned to one DAG, one time. Repeat assignments to that same DAG are ok."""
        if dag is None:
            self._dag = None
            return

        # if set to removed, then just set and exit
        if self._dag.__class__ is AttributeRemoved:
            self._dag = dag
            return
        # if setting to removed, then just set and exit
        if dag.__class__ is AttributeRemoved:
            self._dag = AttributeRemoved("_dag")  # type: ignore[assignment]
            return

        from airflow.models.dag import DAG

        if not isinstance(dag, DAG):
            raise TypeError(f"Expected DAG; received {dag.__class__.__name__}")
        elif self.has_dag() and self.dag is not dag:
            raise AirflowException(f"The DAG assigned to {self} can not be changed.")

        if self.__from_mapped:
            pass  # Don't add to DAG -- the mapped task takes the place.
        elif dag.task_dict.get(self.task_id) is not self:
            dag.add_task(self)

        self._dag = dag

    @property
    def task_display_name(self) -> str:
        return self._task_display_property_value or self.task_id

    def has_dag(self):
        """Return True if the Operator has been assigned to a DAG."""
        return self._dag is not None

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

    def prepare_for_execution(self) -> BaseOperator:
        """Lock task for execution to disable custom action in ``__setattr__`` and return a copy."""
        other = copy.copy(self)
        other._lock_for_execution = True
        return other

    def set_xcomargs_dependencies(self) -> None:
        """
        Resolve upstream dependencies of a task.

        In this way passing an ``XComArg`` as value for a template field
        will result in creating upstream relation between two tasks.

        **Example**: ::

            with DAG(...):
                generate_content = GenerateContentOperator(task_id="generate_content")
                send_email = EmailOperator(..., html_content=generate_content.output)

            # This is equivalent to
            with DAG(...):
                generate_content = GenerateContentOperator(task_id="generate_content")
                send_email = EmailOperator(..., html_content="{{ task_instance.xcom_pull('generate_content') }}")
                generate_content >> send_email

        """
        from airflow.models.xcom_arg import XComArg

        for field in self.template_fields:
            if hasattr(self, field):
                arg = getattr(self, field)
                XComArg.apply_upstream_relationship(self, arg)

    @prepare_lineage
    def pre_execute(self, context: Any):
        """Execute right before self.execute() is called."""
        if self._pre_execute_hook is None:
            return
        ExecutionCallableRunner(
            self._pre_execute_hook,
            context_get_outlet_events(context),
            logger=self.log,
        ).run(context)

    def execute(self, context: Context) -> Any:
        """
        Derive when creating an operator.

        Context is the same dictionary used as when rendering jinja templates.

        Refer to get_template_context for more context.
        """
        raise NotImplementedError()

    @apply_lineage
    def post_execute(self, context: Any, result: Any = None):
        """
        Execute right after self.execute() is called.

        It is passed the execution context and any results returned by the operator.
        """
        if self._post_execute_hook is None:
            return
        ExecutionCallableRunner(
            self._post_execute_hook,
            context_get_outlet_events(context),
            logger=self.log,
        ).run(context, result)

    def on_kill(self) -> None:
        """
        Override this method to clean up subprocesses when a task instance gets killed.

        Any use of the threading, subprocess or multiprocessing module within an
        operator needs to be cleaned up, or it will leave ghost processes behind.
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
        if self._log:
            del state["_log"]

        return state

    def __setstate__(self, state):
        self.__dict__ = state

    def render_template_fields(
        self,
        context: Context,
        jinja_env: jinja2.Environment | None = None,
    ) -> None:
        """
        Template all attributes listed in *self.template_fields*.

        This mutates the attributes in-place and is irreversible.

        :param context: Context dict with values to apply on content.
        :param jinja_env: Jinja's environment to use for rendering.
        """
        if not jinja_env:
            jinja_env = self.get_template_env()
        self._do_render_template_fields(self, self.template_fields, context, jinja_env, set())

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
            qry = qry.where(TaskInstance.execution_date >= start_date)
        if end_date:
            qry = qry.where(TaskInstance.execution_date <= end_date)

        tasks = [self.task_id]

        if upstream:
            tasks += [t.task_id for t in self.get_flat_relatives(upstream=True)]

        if downstream:
            tasks += [t.task_id for t in self.get_flat_relatives(upstream=False)]

        qry = qry.where(TaskInstance.task_id.in_(tasks))
        results = session.scalars(qry).all()
        count = len(results)
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
            query = query.where(DagRun.execution_date >= start_date)
        if end_date:
            query = query.where(DagRun.execution_date <= end_date)
        return session.scalars(query.order_by(DagRun.execution_date)).all()

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

        start_date = pendulum.instance(start_date or self.start_date)
        end_date = pendulum.instance(end_date or self.end_date or timezone.utcnow())

        for info in self.dag.iter_dagrun_infos_between(start_date, end_date, align=False):
            ignore_depends_on_past = info.logical_date == start_date and ignore_first_depends_on_past
            try:
                dag_run = session.scalars(
                    select(DagRun).where(
                        DagRun.dag_id == self.dag_id,
                        DagRun.execution_date == info.logical_date,
                    )
                ).one()
                ti = TaskInstance(self, run_id=dag_run.run_id)
            except NoResultFound:
                # This is _mostly_ only used in tests
                dr = DagRun(
                    dag_id=self.dag_id,
                    run_id=DagRun.generate_run_id(DagRunType.MANUAL, info.logical_date),
                    run_type=DagRunType.MANUAL,
                    execution_date=info.logical_date,
                    data_interval=info.data_interval,
                    triggered_by=DagRunTriggeredByType.TEST,
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
        else:
            return self.downstream_list

    def __repr__(self):
        return f"<Task({self.task_type}): {self.task_id}>"

    @property
    def operator_class(self) -> type[BaseOperator]:  # type: ignore[override]
        return self.__class__

    @property
    def task_type(self) -> str:
        """@property: type of the task."""
        return self.__class__.__name__

    @property
    def operator_name(self) -> str:
        """@property: use a more friendly display name for the operator, if set."""
        try:
            return self.custom_operator_name  # type: ignore
        except AttributeError:
            return self.task_type

    @property
    def roots(self) -> list[BaseOperator]:
        """Required by DAGNode."""
        return [self]

    @property
    def leaves(self) -> list[BaseOperator]:
        """Required by DAGNode."""
        return [self]

    @property
    def output(self) -> XComArg:
        """Returns reference to XCom pushed by current operator."""
        from airflow.models.xcom_arg import XComArg

        return XComArg(operator=self)

    @property
    def is_setup(self) -> bool:
        """
        Whether the operator is a setup task.

        :meta private:
        """
        return self._is_setup

    @is_setup.setter
    def is_setup(self, value: bool) -> None:
        """
        Setter for is_setup property.

        :meta private:
        """
        if self.is_teardown and value:
            raise ValueError(f"Cannot mark task '{self.task_id}' as setup; task is already a teardown.")
        self._is_setup = value

    @property
    def is_teardown(self) -> bool:
        """
        Whether the operator is a teardown task.

        :meta private:
        """
        return self._is_teardown

    @is_teardown.setter
    def is_teardown(self, value: bool) -> None:
        """
        Setter for is_teardown property.

        :meta private:
        """
        if self.is_setup and value:
            raise ValueError(f"Cannot mark task '{self.task_id}' as teardown; task is already a setup.")
        self._is_teardown = value

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
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        """
        return context["ti"].xcom_pull(
            key=key,
            task_ids=task_ids,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates,
            session=session,
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
                vars(BaseOperator(task_id="test")).keys()
                - {
                    "upstream_task_ids",
                    "default_args",
                    "dag",
                    "_dag",
                    "label",
                    "_BaseOperator__instantiated",
                    "_BaseOperator__init_kwargs",
                    "_BaseOperator__from_mapped",
                    "_is_setup",
                    "_is_teardown",
                    "_on_failure_fail_dagrun",
                }
                | {  # Class level defaults need to be added to this list
                    "start_date",
                    "end_date",
                    "_task_type",
                    "_operator_name",
                    "ui_color",
                    "ui_fgcolor",
                    "template_ext",
                    "template_fields",
                    "template_fields_renderers",
                    "params",
                    "is_setup",
                    "is_teardown",
                    "on_failure_fail_dagrun",
                    "map_index_template",
                    "start_trigger_args",
                    "_needs_expansion",
                    "start_from_trigger",
                }
            )
            DagContext.pop_context_managed_dag()

        return cls.__serialized_fields

    def serialize_for_task_group(self) -> tuple[DagAttributeTypes, Any]:
        """Serialize; required by DAGNode."""
        return DagAttributeTypes.OP, self.task_id

    @property
    def inherits_from_empty_operator(self):
        """Used to determine if an Operator is inherited from EmptyOperator."""
        # This looks like `isinstance(self, EmptyOperator) would work, but this also
        # needs to cope when `self` is a Serialized instance of a EmptyOperator or one
        # of its subclasses (which don't inherit from anything but BaseOperator).
        return getattr(self, "_is_empty", False)

    def defer(
        self,
        *,
        trigger: BaseTrigger,
        method_name: str,
        kwargs: dict[str, Any] | None = None,
        timeout: timedelta | None = None,
    ) -> NoReturn:
        """
        Mark this Operator "deferred", suspending its execution until the provided trigger fires an event.

        This is achieved by raising a special exception (TaskDeferred)
        which is caught in the main _execute_task wrapper. Triggers can send execution back to task or end
        the task instance directly. If the trigger will end the task instance itself, ``method_name`` should
        be None; otherwise, provide the name of the method that should be used when resuming execution in
        the task.
        """
        raise TaskDeferred(trigger=trigger, method_name=method_name, kwargs=kwargs, timeout=timeout)

    @classmethod
    def next_callable(cls, operator, next_method, next_kwargs) -> Callable[..., Any]:
        """Get the next callable from given operator."""
        # __fail__ is a special signal value for next_method that indicates
        # this task was scheduled specifically to fail.
        if next_method == "__fail__":
            next_kwargs = next_kwargs or {}
            traceback = next_kwargs.get("traceback")
            if traceback is not None:
                cls.logger().error("Trigger failed:\n%s", "\n".join(traceback))
            raise TaskDeferralError(next_kwargs.get("error", "Unknown"))
        # Grab the callable off the Operator/Task and add in any kwargs
        execute_callable = getattr(operator, next_method)
        if next_kwargs:
            execute_callable = functools.partial(execute_callable, **next_kwargs)
        return execute_callable

    def resume_execution(self, next_method: str, next_kwargs: dict[str, Any] | None, context: Context):
        """Call this method when a deferred task is resumed."""
        execute_callable = self.next_callable(self, next_method, next_kwargs)
        return execute_callable(context)

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


# TODO: Deprecate for Airflow 3.0
Chainable = Union[DependencyMixin, Sequence[DependencyMixin]]


def chain(*tasks: DependencyMixin | Sequence[DependencyMixin]) -> None:
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

        chain(t1, [Label("branch one"), Label("branch two")], [x1(), x2()], task_group1, x3())

    is equivalent to::

          / "branch one" -> x1 \
        t1                      -> task_group1 -> x3
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
    for up_task, down_task in zip(tasks, tasks[1:]):
        if isinstance(up_task, DependencyMixin):
            up_task.set_downstream(down_task)
            continue
        if isinstance(down_task, DependencyMixin):
            down_task.set_upstream(up_task)
            continue
        if not isinstance(up_task, Sequence) or not isinstance(down_task, Sequence):
            raise TypeError(f"Chain not supported between instances of {type(up_task)} and {type(down_task)}")
        up_task_list = up_task
        down_task_list = down_task
        if len(up_task_list) != len(down_task_list):
            raise AirflowException(
                f"Chain not supported for different length Iterable. "
                f"Got {len(up_task_list)} and {len(down_task_list)}."
            )
        for up_t, down_t in zip(up_task_list, down_task_list):
            up_t.set_downstream(down_t)


def cross_downstream(
    from_tasks: Sequence[DependencyMixin],
    to_tasks: DependencyMixin | Sequence[DependencyMixin],
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


def chain_linear(*elements: DependencyMixin | Sequence[DependencyMixin]):
    """
    Simplify task dependency definition.

    E.g.: suppose you want precedence like so::

            ╭─op2─╮ ╭─op4─╮
        op1─┤     ├─├─op5─┤─op7
            ╰-op3─╯ ╰-op6─╯

    Then you can accomplish like so::

        chain_linear(op1, [op2, op3], [op4, op5, op6], op7)

    :param elements: a list of operators / lists of operators
    """
    if not elements:
        raise ValueError("No tasks provided; nothing to do.")
    prev_elem = None
    deps_set = False
    for curr_elem in elements:
        if isinstance(curr_elem, EdgeModifier):
            raise ValueError("Labels are not supported by chain_linear")
        if prev_elem is not None:
            for task in prev_elem:
                task >> curr_elem
                if not deps_set:
                    deps_set = True
        prev_elem = [curr_elem] if isinstance(curr_elem, DependencyMixin) else curr_elem
    if not deps_set:
        raise ValueError("No dependencies were set. Did you forget to expand with `*`?")
