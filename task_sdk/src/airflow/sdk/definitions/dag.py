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

import copy
import functools
import itertools
import logging
import os
import sys
import weakref
from collections import abc
from collections.abc import Collection, Iterable, Iterator, MutableSet
from datetime import datetime, timedelta
from inspect import signature
from re import Pattern
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Union,
    cast,
)
from urllib.parse import urlsplit

import attrs
import jinja2
import re2
from dateutil.relativedelta import relativedelta

from airflow import settings
from airflow.assets import Asset, AssetAlias, BaseAsset
from airflow.exceptions import (
    DuplicateTaskIdFound,
    FailStopDagInvalidTriggerRule,
    ParamValidationError,
    TaskNotFound,
)
from airflow.models.param import DagParam, ParamsDict
from airflow.sdk.definitions.abstractoperator import AbstractOperator
from airflow.sdk.definitions.baseoperator import BaseOperator
from airflow.sdk.types import NOTSET
from airflow.timetables.base import Timetable
from airflow.timetables.simple import (
    AssetTriggeredTimetable,
    ContinuousTimetable,
    NullTimetable,
    OnceTimetable,
)
from airflow.utils.dag_cycle_tester import check_cycle
from airflow.utils.decorators import fixup_decorator_warning_stack
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import EdgeInfoType

if TYPE_CHECKING:
    # TODO: Task-SDK: Remove pendulum core dep
    from pendulum.tz.timezone import FixedTimezone, Timezone

    from airflow.decorators import TaskDecoratorCollection
    from airflow.models.operator import Operator
    from airflow.sdk.definitions.taskgroup import TaskGroup
    from airflow.typing_compat import Self


log = logging.getLogger(__name__)

TAG_MAX_LEN = 100

__all__ = [
    "DAG",
    "dag",
]


# TODO: Task-SDK
class Context: ...


DagStateChangeCallback = Callable[[Context], None]
ScheduleInterval = Union[None, str, timedelta, relativedelta]

ScheduleArg = Union[
    ScheduleInterval,
    Timetable,
    BaseAsset,
    Collection[Union["Asset", "AssetAlias"]],
]


_DAG_HASH_ATTRS = frozenset(
    {
        "dag_id",
        "task_ids",
        "start_date",
        "end_date",
        "fileloc",
        "template_searchpath",
        "last_loaded",
        "schedule",
        # TODO: Task-SDK: we should be hashing on timetable now, not scheulde!
        # "timetable",
    }
)


def _create_timetable(interval: ScheduleInterval, timezone: Timezone | FixedTimezone) -> Timetable:
    """Create a Timetable instance from a plain ``schedule`` value."""
    from airflow.configuration import conf as airflow_conf
    from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
    from airflow.timetables.trigger import CronTriggerTimetable

    if interval is None:
        return NullTimetable()
    if interval == "@once":
        return OnceTimetable()
    if interval == "@continuous":
        return ContinuousTimetable()
    if isinstance(interval, (timedelta, relativedelta)):
        return DeltaDataIntervalTimetable(interval)
    if isinstance(interval, str):
        if airflow_conf.getboolean("scheduler", "create_cron_data_intervals"):
            return CronDataIntervalTimetable(interval, timezone)
        else:
            return CronTriggerTimetable(interval, timezone=timezone)
    raise ValueError(f"{interval!r} is not a valid schedule.")


def _convert_params(val: abc.MutableMapping | None, self_: DAG) -> ParamsDict:
    """
    Convert the plain dict into a ParamsDict.

    This will also merge in params from default_args
    """
    val = val or {}

    # merging potentially conflicting default_args['params'] into params
    if "params" in self_.default_args:
        val.update(self_.default_args["params"])
        del self_.default_args["params"]

    params = ParamsDict(val)
    object.__setattr__(self_, "params", params)

    return params


def _convert_str_to_tuple(val: str | Iterable[str] | None) -> Iterable[str] | None:
    if isinstance(val, str):
        return (val,)
    return val


def _convert_tags(tags: Collection[str] | None) -> MutableSet[str]:
    return set(tags or [])


def _convert_access_control(value, self_: DAG):
    if hasattr(self_, "_upgrade_outdated_dag_access_control"):
        return self_._upgrade_outdated_dag_access_control(value)
    else:
        return value


def _all_after_dag_id_to_kw_only(cls, fields: list[attrs.Attribute]):
    i = iter(fields)
    f = next(i)
    if f.name != "dag_id":
        raise RuntimeError("dag_id was not the first field")
    yield f

    for f in i:
        yield f.evolve(kw_only=True)


if TYPE_CHECKING:
    # Given this attrs field:
    #
    #   default_args: dict[str, Any] = attrs.field(factory=dict, converter=copy.copy)
    #
    # mypy ignores the type of the attrs and works out the type as the converter function. However it doesn't
    # cope with generics properly and errors with 'incompatible type "dict[str, object]"; expected "_T"'
    #
    # https://github.com/python/mypy/issues/8625
    def dict_copy(_: dict[str, Any]) -> dict[str, Any]: ...
else:
    dict_copy = copy.copy


# TODO: Task-SDK: look at re-enabling slots after we remove pickling
@attrs.define(repr=False, field_transformer=_all_after_dag_id_to_kw_only, slots=False)
class DAG:
    """
    A dag (directed acyclic graph) is a collection of tasks with directional dependencies.

    A dag also has a schedule, a start date and an end date (optional).  For each schedule,
    (say daily or hourly), the DAG needs to run each individual tasks as their dependencies
    are met. Certain tasks have the property of depending on their own past, meaning that
    they can't run until their previous schedule (and upstream tasks) are completed.

    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.

    Note that if you plan to use time zones all the dates provided should be pendulum
    dates. See :ref:`timezone_aware_dags`.

    .. versionadded:: 2.4
        The *schedule* argument to specify either time-based scheduling logic
        (timetable), or dataset-driven triggers.

    .. versionchanged:: 3.0
        The default value of *schedule* has been changed to *None* (no schedule).
        The previous default was ``timedelta(days=1)``.

    :param dag_id: The id of the DAG; must consist exclusively of alphanumeric
        characters, dashes, dots and underscores (all ASCII)
    :param description: The description for the DAG to e.g. be shown on the webserver
    :param schedule: If provided, this defines the rules according to which DAG
        runs are scheduled. Possible values include a cron expression string,
        timedelta object, Timetable, or list of Asset objects.
        See also :doc:`/howto/timetable`.
    :param start_date: The timestamp from which the scheduler will
        attempt to backfill. If this is not provided, backfilling must be done
        manually with an explicit time range.
    :param end_date: A date beyond which your DAG won't run, leave to None
        for open-ended scheduling.
    :param template_searchpath: This list of folders (non-relative)
        defines where jinja will look for your templates. Order matters.
        Note that jinja/airflow includes the path of your DAG file by
        default
    :param template_undefined: Template undefined type.
    :param user_defined_macros: a dictionary of macros that will be exposed
        in your jinja templates. For example, passing ``dict(foo='bar')``
        to this argument allows you to ``{{ foo }}`` in all jinja
        templates related to this DAG. Note that you can pass any
        type of object here.
    :param user_defined_filters: a dictionary of filters that will be exposed
        in your jinja templates. For example, passing
        ``dict(hello=lambda name: 'Hello %s' % name)`` to this argument allows
        you to ``{{ 'world' | hello }}`` in all jinja templates related to
        this DAG.
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in the operator's call
        `default_args`, the actual value will be `False`.
    :param params: a dictionary of DAG level parameters that are made
        accessible in templates, namespaced under `params`. These
        params can be overridden at the task level.
    :param max_active_tasks: the number of task instances allowed to run
        concurrently
    :param max_active_runs: maximum number of active DAG runs, beyond this
        number of DAG runs in a running state, the scheduler won't create
        new active DAG runs
    :param max_consecutive_failed_dag_runs: (experimental) maximum number of consecutive failed DAG runs,
        beyond this the scheduler will disable the DAG
    :param dagrun_timeout: Specify the duration a DagRun should be allowed to run before it times out or
        fails. Task instances that are running when a DagRun is timed out will be marked as skipped.
    :param sla_miss_callback: DEPRECATED - The SLA feature is removed in Airflow 3.0, to be replaced with a new implementation in 3.1
    :param catchup: Perform scheduler catchup (or only run latest)? Defaults to True
    :param on_failure_callback: A function or list of functions to be called when a DagRun of this dag fails.
        A context dictionary is passed as a single parameter to this function.
    :param on_success_callback: Much like the ``on_failure_callback`` except
        that it is executed when the dag succeeds.
    :param access_control: Specify optional DAG-level actions, e.g.,
        "{'role1': {'can_read'}, 'role2': {'can_read', 'can_edit', 'can_delete'}}"
        or it can specify the resource name if there is a DAGs Run resource, e.g.,
        "{'role1': {'DAG Runs': {'can_create'}}, 'role2': {'DAGs': {'can_read', 'can_edit', 'can_delete'}}"
    :param is_paused_upon_creation: Specifies if the dag is paused when created for the first time.
        If the dag exists already, this flag will be ignored. If this optional parameter
        is not specified, the global config setting will be used.
    :param jinja_environment_kwargs: additional configuration options to be passed to Jinja
        ``Environment`` for template rendering

        **Example**: to avoid Jinja from removing a trailing newline from template strings ::

            DAG(
                dag_id="my-dag",
                jinja_environment_kwargs={
                    "keep_trailing_newline": True,
                    # some other jinja2 Environment options here
                },
            )

        **See**: `Jinja Environment documentation
        <https://jinja.palletsprojects.com/en/2.11.x/api/#jinja2.Environment>`_

    :param render_template_as_native_obj: If True, uses a Jinja ``NativeEnvironment``
        to render templates as native Python types. If False, a Jinja
        ``Environment`` is used to render templates as string values.
    :param tags: List of tags to help filtering DAGs in the UI.
    :param owner_links: Dict of owners and their links, that will be clickable on the DAGs view UI.
        Can be used as an HTTP link (for example the link to your Slack channel), or a mailto link.
        e.g: {"dag_owner": "https://airflow.apache.org/"}
    :param auto_register: Automatically register this DAG when it is used in a ``with`` block
    :param fail_stop: Fails currently running tasks when task in DAG fails.
        **Warning**: A fail stop dag can only have tasks with the default trigger rule ("all_success").
        An exception will be thrown if any task in a fail stop dag has a non default trigger rule.
    :param dag_display_name: The display name of the DAG which appears on the UI.
    """

    __serialized_fields: ClassVar[frozenset[str] | None] = None

    # NOTE: When updating arguments here, please also keep arguments in @dag()
    # below in sync. (Search for 'def dag(' in this file.)
    dag_id: str = attrs.field(kw_only=False, validator=attrs.validators.instance_of(str))
    description: str | None = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(str)),
    )
    default_args: dict[str, Any] = attrs.field(
        factory=dict, validator=attrs.validators.instance_of(dict), converter=dict_copy
    )
    start_date: datetime | None = attrs.field()
    end_date: datetime | None = None
    timezone: FixedTimezone | Timezone = attrs.field(init=False)
    schedule: ScheduleArg = attrs.field(default=None, on_setattr=attrs.setters.frozen)
    timetable: Timetable = attrs.field(init=False)
    full_filepath: str | None = None
    template_searchpath: str | Iterable[str] | None = attrs.field(
        default=None, converter=_convert_str_to_tuple
    )
    # TODO: Task-SDK: Work out how to not import jinj2 until we need it! It's expensive
    template_undefined: type[jinja2.StrictUndefined] = jinja2.StrictUndefined
    user_defined_macros: dict | None = None
    user_defined_filters: dict | None = None
    concurrency: int | None = None
    max_active_tasks: int = attrs.field(default=16, validator=attrs.validators.instance_of(int))
    max_active_runs: int = attrs.field(default=16, validator=attrs.validators.instance_of(int))
    max_consecutive_failed_dag_runs: int = attrs.field(
        default=-1, validator=attrs.validators.instance_of(int)
    )
    dagrun_timeout: timedelta | None = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(timedelta)),
    )
    # sla_miss_callback: None | SLAMissCallback | list[SLAMissCallback] = None
    catchup: bool = attrs.field(default=True, converter=bool)
    # on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None
    # on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None
    doc_md: str | None = None
    params: ParamsDict = attrs.field(
        # mypy doesn't really like passing the Converter object
        default=None,
        converter=attrs.Converter(_convert_params, takes_self=True),  # type: ignore[misc, call-overload]
    )
    access_control: dict | None = attrs.field(
        default=None, converter=attrs.Converter(_convert_access_control, takes_self=True)
    )
    is_paused_upon_creation: bool | None = None
    jinja_environment_kwargs: dict | None = None
    render_template_as_native_obj: bool = attrs.field(default=False, converter=bool)
    tags: MutableSet[str] = attrs.field(factory=set, converter=_convert_tags)
    owner_links: dict[str, str] = attrs.field(factory=dict)
    auto_register: bool = attrs.field(default=True, converter=bool)
    fail_stop: bool = attrs.field(default=True, converter=bool)
    dag_display_name: str = attrs.field(validator=attrs.validators.instance_of(str))

    task_dict: dict[str, Operator] = attrs.field(factory=dict, init=False)

    task_group: TaskGroup = attrs.field(on_setattr=attrs.setters.frozen)

    fileloc: str = attrs.field(init=False)
    partial: bool = attrs.field(init=False, default=False)

    edge_info: dict[str, dict[str, EdgeInfoType]] = attrs.field(init=False, factory=dict)

    def __attrs_post_init__(self):
        from airflow.utils import timezone

        # Apply the timezone we settled on to end_date if it wasn't supplied
        if isinstance(_end_date := self.default_args.get("end_date"), str):
            self.default_args["end_date"] = timezone.parse(_end_date, timezone=self.timezone)

        self.start_date = timezone.convert_to_utc(self.start_date)
        self.end_date = timezone.convert_to_utc(self.end_date)

    @fileloc.default
    def _default_fileloc(self) -> str:
        # Skip over this frame, and the 'attrs generated init'
        back = sys._getframe().f_back
        if not back or not (back := back.f_back):
            # We expect two frames back, if not we don't know where we are
            return ""
        return back.f_code.co_filename if back else ""

    @dag_display_name.default
    def _default_dag_display_name(self) -> str:
        return self.dag_id

    @task_group.default
    def _default_task_group(self) -> TaskGroup:
        from airflow.sdk.definitions.taskgroup import TaskGroup

        return TaskGroup.create_root(dag=self)

    @timetable.default
    def _default_timetable(self):
        from airflow.assets import AssetAll

        schedule = self.schedule
        # TODO: Once
        # delattr(self, "schedule")
        if isinstance(schedule, Timetable):
            return schedule
        elif isinstance(schedule, BaseAsset):
            return AssetTriggeredTimetable(schedule)
        elif isinstance(schedule, Collection) and not isinstance(schedule, str):
            if not all(isinstance(x, (Asset, AssetAlias)) for x in schedule):
                raise ValueError("All elements in 'schedule' should be assets or asset aliases")
            return AssetTriggeredTimetable(AssetAll(*schedule))
        else:
            return _create_timetable(schedule, self.timezone)

    @start_date.default
    def _default_start_date(self):
        # Find start date inside default_args for compat with Airflow 2.
        from airflow.utils import timezone

        if date := self.default_args.get("start_date"):
            if not isinstance(date, datetime):
                date = timezone.parse(date)
                self.default_args["start_date"] = date
            return date
        return None

    @timezone.default
    def _extract_tz(self):
        import pendulum

        from airflow.utils import timezone

        # TODO: Task-SDK: get default dag tz from settings
        tz = timezone.utc
        if self.start_date and (tzinfo := self.start_date.tzinfo):
            tzinfo = None if tzinfo else tz
            tz = pendulum.instance(self.start_date, tz=tzinfo).timezone
        return tz

    @params.validator
    def _validate_params(self, _, params: ParamsDict):
        """
        Validate Param values when the DAG has schedule defined.

        Raise exception if there are any Params which can not be resolved by their schema definition.
        """
        if not self.timetable or not self.timetable.can_be_scheduled:
            return

        try:
            params.validate()
        except ParamValidationError as pverr:
            raise ValueError(
                f"DAG {self.dag_id!r} is not allowed to define a Schedule, "
                "as there are required params without default values, or the default values are not valid."
            ) from pverr

    @catchup.validator
    def _validate_catchup(self, _, catchup: bool):
        requires_automatic_backfilling = self.timetable.can_be_scheduled and catchup
        if requires_automatic_backfilling and not ("start_date" in self.default_args or self.start_date):
            raise ValueError("start_date is required when catchup=True")

    @tags.validator
    def _validate_tags(self, _, tags: Collection[str]):
        if tags and any(len(tag) > TAG_MAX_LEN for tag in tags):
            raise ValueError(f"tag cannot be longer than {TAG_MAX_LEN} characters")

    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    def __eq__(self, other: Self | Any):
        # TODO: This subclassing behaviour seems wrong, but it's what Airflow has done for ~ever.
        if type(self) is not type(other):
            return False
        return all(getattr(self, c, None) == getattr(other, c, None) for c in _DAG_HASH_ATTRS)

    def __ne__(self, other: Any):
        return not self == other

    def __lt__(self, other):
        return self.dag_id < other.dag_id

    def __hash__(self):
        hash_components: list[Any] = [type(self)]
        for c in _DAG_HASH_ATTRS:
            # task_ids returns a list and lists can't be hashed
            if c == "task_ids":
                val = tuple(self.task_dict)
            else:
                val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    def __enter__(self) -> Self:
        from airflow.sdk.definitions.contextmanager import DagContext

        DagContext.push(self)
        return self

    def __exit__(self, _type, _value, _tb):
        from airflow.sdk.definitions.contextmanager import DagContext

        _ = DagContext.pop()

    def get_doc_md(self, doc_md: str | None) -> str | None:
        if doc_md is None:
            return doc_md

        if doc_md.endswith(".md"):
            try:
                return open(doc_md).read()
            except FileNotFoundError:
                return doc_md

        return doc_md

    def validate(self):
        """
        Validate the DAG has a coherent setup.

        This is called by the DAG bag before bagging the DAG.
        """
        self.timetable.validate()
        self.validate_setup_teardown()

    def validate_setup_teardown(self):
        """
        Validate that setup and teardown tasks are configured properly.

        :meta private:
        """
        for task in self.tasks:
            if task.is_setup:
                for down_task in task.downstream_list:
                    if not down_task.is_teardown and down_task.trigger_rule != TriggerRule.ALL_SUCCESS:
                        # todo: we can relax this to allow out-of-scope tasks to have other trigger rules
                        # this is required to ensure consistent behavior of dag
                        # when clearing an indirect setup
                        raise ValueError("Setup tasks must be followed with trigger rule ALL_SUCCESS.")

    def param(self, name: str, default: Any = NOTSET) -> DagParam:
        """
        Return a DagParam object for current dag.

        :param name: dag parameter name.
        :param default: fallback value for dag parameter.
        :return: DagParam instance for specified name and current dag.
        """
        return DagParam(current_dag=self, name=name, default=default)

    @property
    def tasks(self) -> list[Operator]:
        return list(self.task_dict.values())

    @tasks.setter
    def tasks(self, val):
        raise AttributeError("DAG.tasks can not be modified. Use dag.add_task() instead.")

    @property
    def task_ids(self) -> list[str]:
        return list(self.task_dict)

    @property
    def teardowns(self) -> list[Operator]:
        return [task for task in self.tasks if getattr(task, "is_teardown", None)]

    @property
    def tasks_upstream_of_teardowns(self) -> list[Operator]:
        upstream_tasks = [t.upstream_list for t in self.teardowns]
        return [val for sublist in upstream_tasks for val in sublist if not getattr(val, "is_teardown", None)]

    @property
    def folder(self) -> str:
        """Folder location of where the DAG object is instantiated."""
        return os.path.dirname(self.fileloc)

    @property
    def owner(self) -> str:
        """
        Return list of all owners found in DAG tasks.

        :return: Comma separated list of owners in DAG tasks
        """
        return ", ".join({t.owner for t in self.tasks})

    @property
    def allow_future_exec_dates(self) -> bool:
        return settings.ALLOW_FUTURE_EXEC_DATES and not self.timetable.can_be_scheduled

    def resolve_template_files(self):
        for t in self.tasks:
            t.resolve_template_files()

    def get_template_env(self, *, force_sandboxed: bool = False) -> jinja2.Environment:
        """Build a Jinja2 environment."""
        import airflow.templates

        # Collect directories to search for template files
        searchpath = [self.folder]
        if self.template_searchpath:
            searchpath += self.template_searchpath

        # Default values (for backward compatibility)
        jinja_env_options = {
            "loader": jinja2.FileSystemLoader(searchpath),
            "undefined": self.template_undefined,
            "extensions": ["jinja2.ext.do"],
            "cache_size": 0,
        }
        if self.jinja_environment_kwargs:
            jinja_env_options.update(self.jinja_environment_kwargs)
        env: jinja2.Environment
        if self.render_template_as_native_obj and not force_sandboxed:
            env = airflow.templates.NativeEnvironment(**jinja_env_options)
        else:
            env = airflow.templates.SandboxedEnvironment(**jinja_env_options)

        # Add any user defined items. Safe to edit globals as long as no templates are rendered yet.
        # http://jinja.pocoo.org/docs/2.10/api/#jinja2.Environment.globals
        if self.user_defined_macros:
            env.globals.update(self.user_defined_macros)
        if self.user_defined_filters:
            env.filters.update(self.user_defined_filters)

        return env

    def set_dependency(self, upstream_task_id, downstream_task_id):
        """Set dependency between two tasks that already have been added to the DAG using add_task()."""
        self.get_task(upstream_task_id).set_downstream(self.get_task(downstream_task_id))

    @property
    def roots(self) -> list[Operator]:
        """Return nodes with no parents. These are first to execute and are called roots or root nodes."""
        return [task for task in self.tasks if not task.upstream_list]

    @property
    def leaves(self) -> list[Operator]:
        """Return nodes with no children. These are last to execute and are called leaves or leaf nodes."""
        return [task for task in self.tasks if not task.downstream_list]

    def topological_sort(self):
        """
        Sorts tasks in topographical order, such that a task comes after any of its upstream dependencies.

        Deprecated in place of ``task_group.topological_sort``
        """
        from airflow.sdk.definitions.taskgroup import TaskGroup

        # TODO: Remove in RemovedInAirflow3Warning
        def nested_topo(group):
            for node in group.topological_sort():
                if isinstance(node, TaskGroup):
                    yield from nested_topo(node)
                else:
                    yield node

        return tuple(nested_topo(self.task_group))

    def __deepcopy__(self, memo: dict[int, Any]):
        # Switcharoo to go around deepcopying objects coming through the
        # backdoor
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k not in ("user_defined_macros", "user_defined_filters", "_log"):
                object.__setattr__(result, k, copy.deepcopy(v, memo))

        result.user_defined_macros = self.user_defined_macros
        result.user_defined_filters = self.user_defined_filters
        if hasattr(self, "_log"):
            result._log = self._log  # type: ignore[attr-defined]
        return result

    def partial_subset(
        self,
        task_ids_or_regex: str | Pattern | Iterable[str],
        include_downstream=False,
        include_upstream=True,
        include_direct_upstream=False,
    ):
        """
        Return a subset of the current dag based on regex matching one or more tasks.

        Returns a subset of the current dag as a deep copy of the current dag
        based on a regex that should match one or many tasks, and includes
        upstream and downstream neighbours based on the flag passed.

        :param task_ids_or_regex: Either a list of task_ids, or a regex to
            match against task ids (as a string, or compiled regex pattern).
        :param include_downstream: Include all downstream tasks of matched
            tasks, in addition to matched tasks.
        :param include_upstream: Include all upstream tasks of matched tasks,
            in addition to matched tasks.
        :param include_direct_upstream: Include all tasks directly upstream of matched
            and downstream (if include_downstream = True) tasks
        """
        from airflow.models.mappedoperator import MappedOperator

        # deep-copying self.task_dict and self.task_group takes a long time, and we don't want all
        # the tasks anyway, so we copy the tasks manually later
        memo = {id(self.task_dict): None, id(self.task_group): None}
        dag = copy.deepcopy(self, memo)  # type: ignore

        if isinstance(task_ids_or_regex, (str, Pattern)):
            matched_tasks = [t for t in self.tasks if re2.findall(task_ids_or_regex, t.task_id)]
        else:
            matched_tasks = [t for t in self.tasks if t.task_id in task_ids_or_regex]

        also_include_ids: set[str] = set()
        for t in matched_tasks:
            if include_downstream:
                for rel in t.get_flat_relatives(upstream=False):
                    also_include_ids.add(rel.task_id)
                    if rel not in matched_tasks:  # if it's in there, we're already processing it
                        # need to include setups and teardowns for tasks that are in multiple
                        # non-collinear setup/teardown paths
                        if not rel.is_setup and not rel.is_teardown:
                            also_include_ids.update(
                                x.task_id for x in rel.get_upstreams_only_setups_and_teardowns()
                            )
            if include_upstream:
                also_include_ids.update(x.task_id for x in t.get_upstreams_follow_setups())
            else:
                if not t.is_setup and not t.is_teardown:
                    also_include_ids.update(x.task_id for x in t.get_upstreams_only_setups_and_teardowns())
            if t.is_setup and not include_downstream:
                also_include_ids.update(x.task_id for x in t.downstream_list if x.is_teardown)

        also_include: list[Operator] = [self.task_dict[x] for x in also_include_ids]
        direct_upstreams: list[Operator] = []
        if include_direct_upstream:
            for t in itertools.chain(matched_tasks, also_include):
                upstream = (u for u in t.upstream_list if isinstance(u, (BaseOperator, MappedOperator)))
                direct_upstreams.extend(upstream)

        # Make sure to not recursively deepcopy the dag or task_group while copying the task.
        # task_group is reset later
        def _deepcopy_task(t) -> Operator:
            memo.setdefault(id(t.task_group), None)
            return copy.deepcopy(t, memo)

        # Compiling the unique list of tasks that made the cut
        dag.task_dict = {
            t.task_id: _deepcopy_task(t)
            for t in itertools.chain(matched_tasks, also_include, direct_upstreams)
        }

        def filter_task_group(group, parent_group):
            """Exclude tasks not included in the subdag from the given TaskGroup."""
            # We want to deepcopy _most but not all_ attributes of the task group, so we create a shallow copy
            # and then manually deep copy the instances. (memo argument to deepcopy only works for instances
            # of classes, not "native" properties of an instance)
            copied = copy.copy(group)

            memo[id(group.children)] = {}
            if parent_group:
                memo[id(group.parent_group)] = parent_group
            for attr in type(group).__slots__:
                value = getattr(group, attr)
                value = copy.deepcopy(value, memo)
                object.__setattr__(copied, attr, value)

            proxy = weakref.proxy(copied)

            for child in group.children.values():
                if isinstance(child, AbstractOperator):
                    if child.task_id in dag.task_dict:
                        task = copied.children[child.task_id] = dag.task_dict[child.task_id]
                        task.task_group = proxy
                    else:
                        copied.used_group_ids.discard(child.task_id)
                else:
                    filtered_child = filter_task_group(child, proxy)

                    # Only include this child TaskGroup if it is non-empty.
                    if filtered_child.children:
                        copied.children[child.group_id] = filtered_child

            return copied

        object.__setattr__(dag, "task_group", filter_task_group(self.task_group, None))

        # Removing upstream/downstream references to tasks and TaskGroups that did not make
        # the cut.
        subdag_task_groups = dag.task_group.get_task_group_dict()
        for group in subdag_task_groups.values():
            group.upstream_group_ids.intersection_update(subdag_task_groups)
            group.downstream_group_ids.intersection_update(subdag_task_groups)
            group.upstream_task_ids.intersection_update(dag.task_dict)
            group.downstream_task_ids.intersection_update(dag.task_dict)

        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # make the cut
            t.upstream_task_ids.intersection_update(dag.task_dict)
            t.downstream_task_ids.intersection_update(dag.task_dict)

        if len(dag.tasks) < len(self.tasks):
            dag.partial = True

        return dag

    def has_task(self, task_id: str):
        return task_id in self.task_dict

    def has_task_group(self, task_group_id: str) -> bool:
        return task_group_id in self.task_group_dict

    @functools.cached_property
    def task_group_dict(self):
        return {k: v for k, v in self.task_group.get_task_group_dict().items() if k is not None}

    def get_task(self, task_id: str) -> Operator:
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        raise TaskNotFound(f"Task {task_id} not found")

    @property
    def task(self) -> TaskDecoratorCollection:
        from airflow.decorators import task

        return cast("TaskDecoratorCollection", functools.partial(task, dag=self))

    def add_task(self, task: Operator) -> None:
        """
        Add a task to the DAG.

        :param task: the task you want to add
        """
        # FailStopDagInvalidTriggerRule.check(dag=self, trigger_rule=task.trigger_rule)

        from airflow.sdk.definitions.contextmanager import TaskGroupContext

        # if the task has no start date, assign it the same as the DAG
        if not task.start_date:
            task.start_date = self.start_date
        # otherwise, the task will start on the later of its own start date and
        # the DAG's start date
        elif self.start_date:
            task.start_date = max(task.start_date, self.start_date)

        # if the task has no end date, assign it the same as the dag
        if not task.end_date:
            task.end_date = self.end_date
        # otherwise, the task will end on the earlier of its own end date and
        # the DAG's end date
        elif task.end_date and self.end_date:
            task.end_date = min(task.end_date, self.end_date)

        task_id = task.node_id
        if not task.task_group:
            task_group = TaskGroupContext.get_current(self)
            if task_group:
                task_id = task_group.child_id(task_id)
                task_group.add(task)

        if (
            task_id in self.task_dict and self.task_dict[task_id] is not task
        ) or task_id in self.task_group.used_group_ids:
            raise DuplicateTaskIdFound(f"Task id '{task_id}' has already been added to the DAG")
        else:
            self.task_dict[task_id] = task
            # TODO: Task-SDK: this type ignore shouldn't be needed!
            task.dag = self  # type: ignore[assignment]
            # Add task_id to used_group_ids to prevent group_id and task_id collisions.
            self.task_group.used_group_ids.add(task_id)

        FailStopDagInvalidTriggerRule.check(fail_stop=self.fail_stop, trigger_rule=task.trigger_rule)

    def add_tasks(self, tasks: Iterable[Operator]) -> None:
        """
        Add a list of tasks to the DAG.

        :param tasks: a lit of tasks you want to add
        """
        for task in tasks:
            self.add_task(task)

    def _remove_task(self, task_id: str) -> None:
        # This is "private" as removing could leave a hole in dependencies if done incorrectly, and this
        # doesn't guard against that
        task = self.task_dict.pop(task_id)
        tg = getattr(task, "task_group", None)
        if tg:
            tg._remove(task)

    def cli(self):
        """Exposes a CLI specific to this DAG."""
        check_cycle(self)

        from airflow.cli import cli_parser

        parser = cli_parser.get_parser(dag_parser=True)
        args = parser.parse_args()
        args.func(args, self)

    @classmethod
    def get_serialized_fields(cls):
        """Stringified DAGs and operators contain exactly these fields."""
        if not cls.__serialized_fields:
            exclusion_list = {
                "schedule_asset_references",
                "schedule_asset_alias_references",
                "task_outlet_asset_references",
                "_old_context_manager_dags",
                "safe_dag_id",
                "last_loaded",
                "user_defined_filters",
                "user_defined_macros",
                "partial",
                "params",
                "_pickle_id",
                "_log",
                "task_dict",
                "template_searchpath",
                "sla_miss_callback",
                "on_success_callback",
                "on_failure_callback",
                "template_undefined",
                "jinja_environment_kwargs",
                # has_on_*_callback are only stored if the value is True, as the default is False
                "has_on_success_callback",
                "has_on_failure_callback",
                "auto_register",
                "fail_stop",
                "schedule",
            }
            cls.__serialized_fields = frozenset(vars(DAG(dag_id="test", schedule=None))) - exclusion_list
        return cls.__serialized_fields

    def get_edge_info(self, upstream_task_id: str, downstream_task_id: str) -> EdgeInfoType:
        """Return edge information for the given pair of tasks or an empty edge if there is no information."""
        # Note - older serialized DAGs may not have edge_info being a dict at all
        empty = cast(EdgeInfoType, {})
        if self.edge_info:
            return self.edge_info.get(upstream_task_id, {}).get(downstream_task_id, empty)
        else:
            return empty

    def set_edge_info(self, upstream_task_id: str, downstream_task_id: str, info: EdgeInfoType):
        """
        Set the given edge information on the DAG.

        Note that this will overwrite, rather than merge with, existing info.
        """
        self.edge_info.setdefault(upstream_task_id, {})[downstream_task_id] = info

    def iter_invalid_owner_links(self) -> Iterator[tuple[str, str]]:
        """
        Parse a given link, and verifies if it's a valid URL, or a 'mailto' link.

        Returns an iterator of invalid (owner, link) pairs.
        """
        for owner, link in self.owner_links.items():
            result = urlsplit(link)
            if result.scheme == "mailto":
                # netloc is not existing for 'mailto' link, so we are checking that the path is parsed
                if not result.path:
                    yield result.path, link
            elif not result.scheme or not result.netloc:
                yield owner, link


if TYPE_CHECKING:
    # NOTE: Please keep the list of arguments in sync with DAG.__init__.
    # Only exception: dag_id here should have a default value, but not in DAG.
    def dag(
        dag_id: str = "",
        *,
        description: str | None = None,
        schedule: ScheduleArg = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        template_searchpath: str | Iterable[str] | None = None,
        template_undefined: type[jinja2.StrictUndefined] = jinja2.StrictUndefined,
        user_defined_macros: dict | None = None,
        user_defined_filters: dict | None = None,
        default_args: dict | None = None,
        max_active_tasks: int = ...,
        max_active_runs: int = ...,
        max_consecutive_failed_dag_runs: int = ...,
        dagrun_timeout: timedelta | None = None,
        sla_miss_callback: Any = None,
        catchup: bool = ...,
        on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None,
        on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None,
        doc_md: str | None = None,
        params: abc.MutableMapping | None = None,
        access_control: dict[str, dict[str, Collection[str]]] | dict[str, Collection[str]] | None = None,
        is_paused_upon_creation: bool | None = None,
        jinja_environment_kwargs: dict | None = None,
        render_template_as_native_obj: bool = False,
        tags: Collection[str] | None = None,
        owner_links: dict[str, str] | None = None,
        auto_register: bool = True,
        fail_stop: bool = False,
        dag_display_name: str | None = None,
    ) -> Callable[[Callable], Callable[..., DAG]]:
        """
        Python dag decorator which wraps a function into an Airflow DAG.

        Accepts kwargs for operator kwarg. Can be used to parameterize DAGs.

        :param dag_args: Arguments for DAG object
        :param dag_kwargs: Kwargs for DAG object.
        """
else:

    def dag(dag_id="", __DAG_class=DAG, __warnings_stacklevel_delta=2, **decorator_kwargs):
        # TODO: Task-SDK: remove __DAG_class
        # __DAG_class is a temporary hack to allow the dag decorator in airflow.models.dag to continue to
        # return SchedulerDag objects
        DAG = __DAG_class

        def wrapper(f: Callable) -> Callable[..., DAG]:
            @functools.wraps(f)
            def factory(*args, **kwargs):
                # Generate signature for decorated function and bind the arguments when called
                # we do this to extract parameters, so we can annotate them on the DAG object.
                # In addition, this fails if we are missing any args/kwargs with TypeError as expected.
                f_sig = signature(f).bind(*args, **kwargs)
                # Apply defaults to capture default values if set.
                f_sig.apply_defaults()

                # Initialize DAG with bound arguments
                with DAG(dag_id or f.__name__, **decorator_kwargs) as dag_obj:
                    # Set DAG documentation from function documentation if it exists and doc_md is not set.
                    if f.__doc__ and not dag_obj.doc_md:
                        dag_obj.doc_md = f.__doc__

                    # Generate DAGParam for each function arg/kwarg and replace it for calling the function.
                    # All args/kwargs for function will be DAGParam object and replaced on execution time.
                    f_kwargs = {}
                    for name, value in f_sig.arguments.items():
                        f_kwargs[name] = dag_obj.param(name, value)

                    # set file location to caller source path
                    back = sys._getframe().f_back
                    dag_obj.fileloc = back.f_code.co_filename if back else ""

                    # Invoke function to create operators in the DAG scope.
                    f(**f_kwargs)

                # Return dag object such that it's accessible in Globals.
                return dag_obj

            # Ensure that warnings from inside DAG() are emitted from the caller, not here
            fixup_decorator_warning_stack(factory)
            return factory

        return wrapper
