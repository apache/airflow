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
import json
import logging
import os
import sys
import warnings
import weakref
from collections import abc, defaultdict, deque
from collections.abc import Callable, Collection, Iterable, MutableSet
from datetime import datetime, timedelta
from inspect import signature
from typing import TYPE_CHECKING, Any, ClassVar, TypeGuard, Union, cast, overload
from urllib.parse import urlsplit
from uuid import UUID

import attrs
import jinja2
from dateutil.relativedelta import relativedelta

from airflow import settings
from airflow.sdk import TaskInstanceState, TriggerRule
from airflow.sdk.bases.operator import BaseOperator
from airflow.sdk.bases.timetable import BaseTimetable
from airflow.sdk.definitions._internal.node import validate_key
from airflow.sdk.definitions._internal.types import NOTSET, ArgNotSet, is_arg_set
from airflow.sdk.definitions.asset import AssetAll, BaseAsset
from airflow.sdk.definitions.context import Context
from airflow.sdk.definitions.deadline import DeadlineAlert
from airflow.sdk.definitions.param import DagParam, ParamsDict
from airflow.sdk.definitions.timetables.assets import AssetTriggeredTimetable
from airflow.sdk.definitions.timetables.simple import ContinuousTimetable, NullTimetable, OnceTimetable
from airflow.sdk.exceptions import (
    AirflowDagCycleException,
    DuplicateTaskIdFound,
    FailFastDagInvalidTriggerRule,
    ParamValidationError,
    RemovedInAirflow4Warning,
    TaskNotFound,
)

if TYPE_CHECKING:
    from re import Pattern
    from typing import TypeAlias

    from pendulum.tz.timezone import FixedTimezone, Timezone
    from typing_extensions import Self, TypeIs

    from airflow.models.taskinstance import TaskInstance as SchedulerTaskInstance
    from airflow.sdk.definitions.decorators import TaskDecoratorCollection
    from airflow.sdk.definitions.edges import EdgeInfoType
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.sdk.definitions.taskgroup import TaskGroup
    from airflow.sdk.execution_time.supervisor import TaskRunResult
    from airflow.timetables.base import DataInterval, Timetable as CoreTimetable

    Operator: TypeAlias = BaseOperator | MappedOperator

log = logging.getLogger(__name__)

TAG_MAX_LEN = 100

__all__ = [
    "DAG",
    "dag",
]

FINISHED_STATES = frozenset(
    [
        TaskInstanceState.SUCCESS,
        TaskInstanceState.FAILED,
        TaskInstanceState.SKIPPED,
        TaskInstanceState.UPSTREAM_FAILED,
        TaskInstanceState.REMOVED,
    ]
)

DagStateChangeCallback = Callable[[Context], None]
ScheduleInterval = None | str | timedelta | relativedelta

ScheduleArg = Union[ScheduleInterval, BaseTimetable, "CoreTimetable", BaseAsset, Collection[BaseAsset]]


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
        # TODO: Task-SDK: we should be hashing on timetable now, not schedule!
        # "timetable",
    }
)


def _is_core_timetable(schedule: ScheduleArg) -> TypeIs[CoreTimetable]:
    try:
        from airflow.timetables.base import Timetable
    except ImportError:
        return False
    return isinstance(schedule, Timetable)


def _create_timetable(interval: ScheduleInterval, timezone: Timezone | FixedTimezone) -> BaseTimetable:
    """Create a Timetable instance from a plain ``schedule`` value."""
    from airflow.sdk.configuration import conf as airflow_conf
    from airflow.sdk.definitions.timetables.interval import (
        CronDataIntervalTimetable,
        DeltaDataIntervalTimetable,
    )
    from airflow.sdk.definitions.timetables.trigger import CronTriggerTimetable, DeltaTriggerTimetable

    if interval is None:
        return NullTimetable()
    if interval == "@once":
        return OnceTimetable()
    if interval == "@continuous":
        return ContinuousTimetable()
    if isinstance(interval, timedelta | relativedelta):
        if airflow_conf.getboolean("scheduler", "create_cron_data_intervals"):
            return DeltaDataIntervalTimetable(interval)
        return DeltaTriggerTimetable(interval)
    if isinstance(interval, str):
        if airflow_conf.getboolean("scheduler", "create_cron_data_intervals"):
            return CronDataIntervalTimetable(interval, timezone)
        return CronTriggerTimetable(interval, timezone=timezone)
    raise ValueError(f"{interval!r} is not a valid schedule.")


def _config_bool_factory(section: str, key: str) -> Callable[[], bool]:
    from airflow.sdk.configuration import conf

    return functools.partial(conf.getboolean, section, key)


def _config_int_factory(section: str, key: str) -> Callable[[], int]:
    from airflow.sdk.configuration import conf

    return functools.partial(conf.getint, section, key)


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


def _convert_access_control(access_control):
    if access_control is None:
        return None
    updated_access_control = {}
    for role, perms in access_control.items():
        updated_access_control[role] = updated_access_control.get(role, {})
        if isinstance(perms, set | list):
            # Support for old-style access_control where only the actions are specified
            updated_access_control[role]["DAGs"] = set(perms)
        else:
            updated_access_control[role] = perms
    return updated_access_control


def _convert_deadline(deadline: list[DeadlineAlert] | DeadlineAlert | None) -> list[DeadlineAlert] | None:
    """Convert deadline parameter to a list of DeadlineAlert objects."""
    if deadline is None:
        return None
    if isinstance(deadline, DeadlineAlert):
        return [deadline]
    return list(deadline)


def _convert_doc_md(doc_md: str | None) -> str | None:
    if doc_md is None:
        return doc_md

    if doc_md.endswith(".md"):
        try:
            with open(doc_md) as fh:
                return fh.read()
        except FileNotFoundError:
            return doc_md

    return doc_md


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


def _default_start_date(instance: DAG):
    # Find start date inside default_args for compat with Airflow 2.
    from airflow.sdk import timezone

    if date := instance.default_args.get("start_date"):
        if not isinstance(date, datetime):
            date = timezone.parse(date)
            instance.default_args["start_date"] = date
        return date
    return None


def _default_dag_display_name(instance: DAG) -> str:
    return instance.dag_id


def _default_fileloc() -> str:
    # Skip over this frame, and the 'attrs generated init'
    back = sys._getframe().f_back
    if not back or not (back := back.f_back):
        # We expect two frames back, if not we don't know where we are
        return ""
    return back.f_code.co_filename if back else ""


def _default_task_group(instance: DAG) -> TaskGroup:
    from airflow.sdk.definitions.taskgroup import TaskGroup

    return TaskGroup.create_root(dag=instance)


# TODO: Task-SDK: look at re-enabling slots after we remove pickling
@attrs.define(repr=False, field_transformer=_all_after_dag_id_to_kw_only, slots=False)
class DAG:
    """
    A dag is a collection of tasks with directional dependencies.

    A dag also has a schedule, a start date and an end date (optional).  For each schedule,
    (say daily or hourly), the DAG needs to run each individual tasks as their dependencies
    are met. Certain tasks have the property of depending on their own past, meaning that
    they can't run until their previous schedule (and upstream tasks) are completed.

    Dags essentially act as namespaces for tasks. A task_id can only be
    added once to a Dag.

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
        See also :external:doc:`howto/timetable`.
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
    :param sla_miss_callback: DEPRECATED - The SLA feature is removed in Airflow 3.0, to be replaced with DeadlineAlerts in 3.1
    :param deadline: An optional DeadlineAlert for the Dag.
    :param catchup: Perform scheduler catchup (or only run latest)? Defaults to False
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
    :param tags: List of tags to help filtering Dags in the UI.
    :param owner_links: Dict of owners and their links, that will be clickable on the Dags view UI.
        Can be used as an HTTP link (for example the link to your Slack channel), or a mailto link.
        e.g: ``{"dag_owner": "https://airflow.apache.org/"}``
    :param auto_register: Automatically register this DAG when it is used in a ``with`` block
    :param fail_fast: Fails currently running tasks when task in Dag fails.
        **Warning**: A fail stop dag can only have tasks with the default trigger rule ("all_success").
        An exception will be thrown if any task in a fail stop dag has a non default trigger rule.
    :param dag_display_name: The display name of the Dag which appears on the UI.
    """

    __serialized_fields: ClassVar[frozenset[str]]

    # Note: mypy gets very confused about the use of `@${attr}.default` for attrs without init=False -- and it
    # doesn't correctly track/notice that they have default values (it gives errors about `Missing positional
    # argument "description" in call to "DAG"`` etc), so for init=True args we use the `default=Factory()`
    # style

    def __rich_repr__(self):
        yield "dag_id", self.dag_id
        yield "schedule", self.schedule
        yield "#tasks", len(self.tasks)

    __rich_repr__.angular = True  # type: ignore[attr-defined]

    # NOTE: When updating arguments here, please also keep arguments in @dag()
    # below in sync. (Search for 'def dag(' in this file.)
    dag_id: str = attrs.field(kw_only=False, validator=lambda i, a, v: validate_key(v))
    description: str | None = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(str)),
    )
    default_args: dict[str, Any] = attrs.field(
        factory=dict, validator=attrs.validators.instance_of(dict), converter=dict_copy
    )
    start_date: datetime | None = attrs.field(
        default=attrs.Factory(_default_start_date, takes_self=True),
    )

    end_date: datetime | None = None
    timezone: FixedTimezone | Timezone = attrs.field(init=False)
    schedule: ScheduleArg = attrs.field(default=None, on_setattr=attrs.setters.frozen)
    timetable: BaseTimetable | CoreTimetable = attrs.field(init=False)
    template_searchpath: str | Iterable[str] | None = attrs.field(
        default=None, converter=_convert_str_to_tuple
    )
    # TODO: Task-SDK: Work out how to not import jinj2 until we need it! It's expensive
    template_undefined: type[jinja2.StrictUndefined] = jinja2.StrictUndefined
    user_defined_macros: dict | None = None
    user_defined_filters: dict | None = None
    max_active_tasks: int = attrs.field(
        factory=_config_int_factory("core", "max_active_tasks_per_dag"),
        converter=attrs.converters.default_if_none(  # type: ignore[misc]
            # attrs only supports named callables or lambdas, but partial works
            # OK here too. This is a false positive from attrs's Mypy plugin.
            factory=_config_int_factory("core", "max_active_tasks_per_dag"),
        ),
        validator=attrs.validators.instance_of(int),
    )
    max_active_runs: int = attrs.field(
        factory=_config_int_factory("core", "max_active_runs_per_dag"),
        converter=attrs.converters.default_if_none(  # type: ignore[misc]
            # attrs only supports named callables or lambdas, but partial works
            # OK here too. This is a false positive from attrs's Mypy plugin.
            factory=_config_int_factory("core", "max_active_runs_per_dag"),
        ),
        validator=attrs.validators.instance_of(int),
    )
    max_consecutive_failed_dag_runs: int = attrs.field(
        factory=_config_int_factory("core", "max_consecutive_failed_dag_runs_per_dag"),
        converter=attrs.converters.default_if_none(  # type: ignore[misc]
            # attrs only supports named callables or lambdas, but partial works
            # OK here too. This is a false positive from attrs's Mypy plugin.
            factory=_config_int_factory("core", "max_consecutive_failed_dag_runs_per_dag"),
        ),
        validator=attrs.validators.instance_of(int),
    )
    dagrun_timeout: timedelta | None = attrs.field(
        default=None,
        validator=attrs.validators.optional(attrs.validators.instance_of(timedelta)),
    )
    deadline: list[DeadlineAlert] | DeadlineAlert | None = attrs.field(
        default=None,
        converter=_convert_deadline,
        validator=attrs.validators.optional(
            attrs.validators.deep_iterable(
                member_validator=attrs.validators.instance_of(DeadlineAlert),
                iterable_validator=attrs.validators.instance_of(list),
            )
        ),
    )

    sla_miss_callback: None = attrs.field(default=None)
    catchup: bool = attrs.field(
        factory=_config_bool_factory("scheduler", "catchup_by_default"),
    )
    on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None
    on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None
    doc_md: str | None = attrs.field(default=None, converter=_convert_doc_md)
    params: ParamsDict = attrs.field(
        # mypy doesn't really like passing the Converter object
        default=None,
        converter=attrs.Converter(_convert_params, takes_self=True),  # type: ignore[misc, call-overload]
    )
    access_control: dict[str, dict[str, Collection[str]]] | None = attrs.field(
        default=None,
        converter=attrs.Converter(_convert_access_control),  # type: ignore[misc, call-overload]
    )
    is_paused_upon_creation: bool | None = None
    jinja_environment_kwargs: dict | None = None
    render_template_as_native_obj: bool = attrs.field(default=False, converter=bool)
    tags: MutableSet[str] = attrs.field(factory=set, converter=_convert_tags)
    owner_links: dict[str, str] = attrs.field(factory=dict)
    auto_register: bool = attrs.field(default=True, converter=bool)
    fail_fast: bool = attrs.field(default=False, converter=bool)
    dag_display_name: str = attrs.field(
        default=attrs.Factory(_default_dag_display_name, takes_self=True),
        validator=attrs.validators.instance_of(str),
    )

    task_dict: dict[str, Operator] = attrs.field(factory=dict, init=False)

    task_group: TaskGroup = attrs.field(
        on_setattr=attrs.setters.frozen, default=attrs.Factory(_default_task_group, takes_self=True)
    )

    fileloc: str = attrs.field(init=False, factory=_default_fileloc)
    relative_fileloc: str | None = attrs.field(init=False, default=None)
    partial: bool = attrs.field(init=False, default=False)

    edge_info: dict[str, dict[str, EdgeInfoType]] = attrs.field(init=False, factory=dict)

    has_on_success_callback: bool = attrs.field(init=False)
    has_on_failure_callback: bool = attrs.field(init=False)
    disable_bundle_versioning: bool = attrs.field(
        factory=_config_bool_factory("dag_processor", "disable_bundle_versioning")
    )

    # TODO (GH-52141): This is never used in the sdk dag (it only makes sense
    # after this goes through the dag processor), but various parts of the code
    # depends on its existence. We should remove this after completely splitting
    # DAG classes in the SDK and scheduler.
    last_loaded: datetime | None = attrs.field(init=False, default=None)

    def __attrs_post_init__(self):
        from airflow.sdk import timezone

        # Apply the timezone we settled on to start_date, end_date if it wasn't supplied
        if isinstance(_start_date := self.default_args.get("start_date"), str):
            self.default_args["start_date"] = timezone.parse(_start_date, timezone=self.timezone)
        if isinstance(_end_date := self.default_args.get("end_date"), str):
            self.default_args["end_date"] = timezone.parse(_end_date, timezone=self.timezone)

        self.start_date = timezone.convert_to_utc(self.start_date)
        self.end_date = timezone.convert_to_utc(self.end_date)
        if start_date := self.default_args.get("start_date", None):
            self.default_args["start_date"] = timezone.convert_to_utc(start_date)
        if end_date := self.default_args.get("end_date", None):
            self.default_args["end_date"] = timezone.convert_to_utc(end_date)
        if self.access_control is not None:
            warnings.warn(
                "The airflow.security.permissions module is deprecated; please see https://airflow.apache.org/docs/apache-airflow/stable/security/deprecated_permissions.html",
                RemovedInAirflow4Warning,
                stacklevel=2,
            )
        if (
            active_runs_limit := self.timetable.active_runs_limit
        ) is not None and active_runs_limit < self.max_active_runs:
            raise ValueError(
                f"Invalid max_active_runs: {type(self.timetable)} "
                f"requires max_active_runs <= {active_runs_limit}"
            )

    @params.validator
    def _validate_params(self, _, params: ParamsDict):
        """
        Validate Param values when the Dag has schedule defined.

        Raise exception if there are any Params which can not be resolved by their schema definition.
        """
        if not self.timetable or not self.timetable.can_be_scheduled:
            return

        try:
            params.validate()
        except ParamValidationError as pverr:
            raise ValueError(
                f"Dag {self.dag_id!r} is not allowed to define a Schedule, "
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

    @max_active_runs.validator
    def _validate_max_active_runs(self, _, max_active_runs):
        if self.timetable.active_runs_limit is not None:
            if self.timetable.active_runs_limit < self.max_active_runs:
                raise ValueError(
                    f"Invalid max_active_runs: {type(self.timetable).__name__} "
                    f"requires max_active_runs <= {self.timetable.active_runs_limit}"
                )

    @timetable.default
    def _default_timetable(instance: DAG) -> BaseTimetable | CoreTimetable:
        schedule = instance.schedule
        # TODO: Once
        # delattr(self, "schedule")
        if _is_core_timetable(schedule):
            return schedule
        if isinstance(schedule, BaseTimetable):
            return schedule
        if isinstance(schedule, BaseAsset):
            return AssetTriggeredTimetable(schedule)
        if isinstance(schedule, Collection) and not isinstance(schedule, str):
            if not all(isinstance(x, BaseAsset) for x in schedule):
                raise ValueError(
                    "All elements in 'schedule' should be either assets, asset references, or asset aliases"
                )
            return AssetTriggeredTimetable(AssetAll(*schedule))
        return _create_timetable(schedule, instance.timezone)

    @timezone.default
    def _extract_tz(instance):
        import pendulum

        from airflow.sdk import timezone

        start_date = instance.start_date or instance.default_args.get("start_date")

        if start_date:
            if not isinstance(start_date, datetime):
                start_date = timezone.parse(start_date)
            tzinfo = start_date.tzinfo or settings.TIMEZONE
            tz = pendulum.instance(start_date, tz=tzinfo).timezone
        else:
            tz = settings.TIMEZONE

        return tz

    @has_on_success_callback.default
    def _has_on_success_callback(self) -> bool:
        return self.on_success_callback is not None

    @has_on_failure_callback.default
    def _has_on_failure_callback(self) -> bool:
        return self.on_failure_callback is not None

    @sla_miss_callback.validator
    def _validate_sla_miss_callback(self, _, value):
        if value is not None:
            warnings.warn(
                "The SLA feature is removed in Airflow 3.0, and replaced with a Deadline Alerts in >=3.1",
                stacklevel=2,
            )
        return value

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
            # If it is a list, convert to tuple because lists can't be hashed
            if isinstance(getattr(self, c, None), list):
                val = tuple(getattr(self, c))
            else:
                val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    def __enter__(self) -> Self:
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        DagContext.push(self)
        return self

    def __exit__(self, _type, _value, _tb):
        from airflow.sdk.definitions._internal.contextmanager import DagContext

        _ = DagContext.pop()

    def validate(self):
        """
        Validate the Dag has a coherent setup.

        This is called by the Dag bag before bagging the Dag.
        """
        self.timetable.validate()
        self.validate_setup_teardown()

        # We validate owner links on set, but since it's a dict it could be mutated without calling the
        # setter. Validate again here
        self._validate_owner_links(None, self.owner_links)

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
        """Folder location of where the Dag object is instantiated."""
        return os.path.dirname(self.fileloc)

    @property
    def owner(self) -> str:
        """
        Return list of all owners found in Dag tasks.

        :return: Comma separated list of owners in Dag tasks
        """
        return ", ".join({t.owner for t in self.tasks})

    def resolve_template_files(self):
        for t in self.tasks:
            # TODO: TaskSDK: move this on to BaseOperator and remove the check?
            if hasattr(t, "resolve_template_files"):
                t.resolve_template_files()

    def get_template_env(self, *, force_sandboxed: bool = False) -> jinja2.Environment:
        """Build a Jinja2 environment."""
        from airflow.sdk.definitions._internal.templater import NativeEnvironment, SandboxedEnvironment

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
            env = NativeEnvironment(**jinja_env_options)
        else:
            env = SandboxedEnvironment(**jinja_env_options)

        # Add any user defined items. Safe to edit globals as long as no templates are rendered yet.
        # http://jinja.pocoo.org/docs/2.10/api/#jinja2.Environment.globals
        if self.user_defined_macros:
            env.globals.update(self.user_defined_macros)
        if self.user_defined_filters:
            env.filters.update(self.user_defined_filters)

        return env

    def set_dependency(self, upstream_task_id, downstream_task_id):
        """Set dependency between two tasks that already have been added to the Dag using add_task()."""
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
        task_ids: str | Iterable[str],
        include_downstream=False,
        include_upstream=True,
        include_direct_upstream=False,
    ):
        """
        Return a subset of the current dag based on regex matching one or more tasks.

        Returns a subset of the current dag as a deep copy of the current dag
        based on a regex that should match one or many tasks, and includes
        upstream and downstream neighbours based on the flag passed.

        :param task_ids: Either a list of task_ids, or a string task_id
        :param include_downstream: Include all downstream tasks of matched
            tasks, in addition to matched tasks.
        :param include_upstream: Include all upstream tasks of matched tasks,
            in addition to matched tasks.
        :param include_direct_upstream: Include all tasks directly upstream of matched
            and downstream (if include_downstream = True) tasks
        """
        from airflow.sdk.definitions.mappedoperator import MappedOperator

        def is_task(obj) -> TypeGuard[Operator]:
            return isinstance(obj, BaseOperator | MappedOperator)

        # deep-copying self.task_dict and self.task_group takes a long time, and we don't want all
        # the tasks anyway, so we copy the tasks manually later
        memo = {id(self.task_dict): None, id(self.task_group): None}
        dag = copy.deepcopy(self, memo)

        if isinstance(task_ids, str):
            matched_tasks = [t for t in self.tasks if task_ids in t.task_id]
        else:
            matched_tasks = [t for t in self.tasks if t.task_id in task_ids]

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
                direct_upstreams.extend(u for u in t.upstream_list if is_task(u))

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
            """Exclude tasks not included in the partial dag from the given TaskGroup."""
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
                if is_task(child):
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
        groups = dag.task_group.get_task_group_dict()
        for g in groups.values():
            g.upstream_group_ids.intersection_update(groups)
            g.downstream_group_ids.intersection_update(groups)
            g.upstream_task_ids.intersection_update(dag.task_dict)
            g.downstream_task_ids.intersection_update(dag.task_dict)

        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # make the cut
            t.upstream_task_ids.intersection_update(dag.task_dict)
            t.downstream_task_ids.intersection_update(dag.task_dict)

        dag.partial = len(dag.tasks) < len(self.tasks)

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
        from airflow.sdk.definitions.decorators import task

        return cast("TaskDecoratorCollection", functools.partial(task, dag=self))

    def add_task(self, task: Operator) -> None:
        """
        Add a task to the Dag.

        :param task: the task you want to add
        """
        # FailStopDagInvalidTriggerRule.check(dag=self, trigger_rule=task.trigger_rule)

        from airflow.sdk.definitions._internal.contextmanager import TaskGroupContext

        # if the task has no start date, assign it the same as the Dag
        if not task.start_date:
            task.start_date = self.start_date
        # otherwise, the task will start on the later of its own start date and
        # the Dag's start date
        elif self.start_date:
            task.start_date = max(task.start_date, self.start_date)

        # if the task has no end date, assign it the same as the dag
        if not task.end_date:
            task.end_date = self.end_date
        # otherwise, the task will end on the earlier of its own end date and
        # the Dag's end date
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
        self.task_dict[task_id] = task

        task.dag = self
        # Add task_id to used_group_ids to prevent group_id and task_id collisions.
        self.task_group.used_group_ids.add(task_id)

        FailFastDagInvalidTriggerRule.check(fail_fast=self.fail_fast, trigger_rule=task.trigger_rule)

    def add_tasks(self, tasks: Iterable[Operator]) -> None:
        """
        Add a list of tasks to the Dag.

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

    def check_cycle(self) -> None:
        """
        Check to see if there are any cycles in the Dag.

        :raises AirflowDagCycleException: If cycle is found in the Dag.
        """
        # default of int is 0 which corresponds to CYCLE_NEW
        CYCLE_NEW = 0
        CYCLE_IN_PROGRESS = 1
        CYCLE_DONE = 2

        visited: dict[str, int] = defaultdict(int)
        path_stack: deque[str] = deque()
        task_dict = self.task_dict

        def _check_adjacent_tasks(task_id, current_task):
            """Return first untraversed child task, else None if all tasks traversed."""
            for adjacent_task in current_task.get_direct_relative_ids():
                if visited[adjacent_task] == CYCLE_IN_PROGRESS:
                    msg = f"Cycle detected in Dag: {self.dag_id}. Faulty task: {task_id}"
                    raise AirflowDagCycleException(msg)
                if visited[adjacent_task] == CYCLE_NEW:
                    return adjacent_task
            return None

        for dag_task_id in self.task_dict.keys():
            if visited[dag_task_id] == CYCLE_DONE:
                continue
            path_stack.append(dag_task_id)
            while path_stack:
                current_task_id = path_stack[-1]
                if visited[current_task_id] == CYCLE_NEW:
                    visited[current_task_id] = CYCLE_IN_PROGRESS
                task = task_dict[current_task_id]
                child_to_check = _check_adjacent_tasks(current_task_id, task)
                if not child_to_check:
                    visited[current_task_id] = CYCLE_DONE
                    path_stack.pop()
                else:
                    path_stack.append(child_to_check)

    def cli(self):
        """Exposes a CLI specific to this Dag."""
        self.check_cycle()

        from airflow.cli import cli_parser

        parser = cli_parser.get_parser(dag_parser=True)
        args = parser.parse_args()
        args.func(args, self)

    @classmethod
    def get_serialized_fields(cls):
        """Stringified Dags and operators contain exactly these fields."""
        return cls.__serialized_fields

    def get_edge_info(self, upstream_task_id: str, downstream_task_id: str) -> EdgeInfoType:
        """Return edge information for the given pair of tasks or an empty edge if there is no information."""
        empty = cast("EdgeInfoType", {})
        if self.edge_info:
            return self.edge_info.get(upstream_task_id, {}).get(downstream_task_id, empty)
        return empty

    def set_edge_info(self, upstream_task_id: str, downstream_task_id: str, info: EdgeInfoType):
        """
        Set the given edge information on the Dag.

        Note that this will overwrite, rather than merge with, existing info.
        """
        self.edge_info.setdefault(upstream_task_id, {})[downstream_task_id] = info

    @owner_links.validator
    def _validate_owner_links(self, _, owner_links):
        wrong_links = {}

        for owner, link in owner_links.items():
            result = urlsplit(link)
            if result.scheme == "mailto":
                # netloc is not existing for 'mailto' link, so we are checking that the path is parsed
                if not result.path:
                    wrong_links[result.path] = link
            elif not result.scheme or not result.netloc:
                wrong_links[owner] = link
        if wrong_links:
            raise ValueError(
                "Wrong link format was used for the owner. Use a valid link \n"
                f"Bad formatted links are: {wrong_links}"
            )

    def test(
        self,
        run_after: datetime | None = None,
        logical_date: datetime | None | ArgNotSet = NOTSET,
        run_conf: dict[str, Any] | None = None,
        conn_file_path: str | None = None,
        variable_file_path: str | None = None,
        use_executor: bool = False,
        mark_success_pattern: Pattern | str | None = None,
    ):
        """
        Execute one single DagRun for a given Dag and logical date.

        :param run_after: the datetime before which to Dag cannot run.
        :param logical_date: logical date for the Dag run
        :param run_conf: configuration to pass to newly created dagrun
        :param conn_file_path: file path to a connection file in either yaml or json
        :param variable_file_path: file path to a variable file in either yaml or json
        :param use_executor: if set, uses an executor to test the Dag
        :param mark_success_pattern: regex of task_ids to mark as success instead of running
        """
        import re
        import time
        from contextlib import ExitStack
        from unittest.mock import patch

        from airflow import settings
        from airflow.models.dagrun import DagRun, get_or_create_dagrun
        from airflow.sdk import DagRunState, timezone
        from airflow.serialization.definitions.dag import SerializedDAG
        from airflow.serialization.encoders import coerce_to_core_timetable
        from airflow.serialization.serialized_objects import DagSerialization
        from airflow.utils.types import DagRunTriggeredByType, DagRunType

        exit_stack = ExitStack()

        if conn_file_path or variable_file_path:
            backend_kwargs = {}
            if conn_file_path:
                backend_kwargs["connections_file_path"] = conn_file_path
            if variable_file_path:
                backend_kwargs["variables_file_path"] = variable_file_path

            exit_stack.enter_context(
                patch.dict(
                    os.environ,
                    {
                        "AIRFLOW__SECRETS__BACKEND": "airflow.secrets.local_filesystem.LocalFilesystemBackend",
                        "AIRFLOW__SECRETS__BACKEND_KWARGS": json.dumps(backend_kwargs),
                    },
                )
            )

        if settings.Session is None:
            raise RuntimeError("Session not configured. Call configure_orm() first.")
        session = settings.Session()

        with exit_stack:
            self.validate()
            scheduler_dag = DagSerialization.deserialize_dag(DagSerialization.serialize_dag(self))

            # Allow users to explicitly pass None. If it isn't set, we default to current time.
            logical_date = logical_date if is_arg_set(logical_date) else timezone.utcnow()

            log.debug("Clearing existing task instances for logical date %s", logical_date)
            # TODO: Replace with calling client.dag_run.clear in Execution API at some point
            SerializedDAG.clear_dags(
                dags=[scheduler_dag],
                start_date=logical_date,
                end_date=logical_date,
                dag_run_state=False,
            )

            log.debug("Getting dagrun for dag %s", self.dag_id)
            logical_date = timezone.coerce_datetime(logical_date)
            run_after = timezone.coerce_datetime(run_after) or timezone.coerce_datetime(timezone.utcnow())
            if logical_date is None:
                data_interval: DataInterval | None = None
            else:
                timetable = coerce_to_core_timetable(self.timetable)
                data_interval = timetable.infer_manual_data_interval(run_after=logical_date)
            from airflow.models.dag_version import DagVersion

            version = DagVersion.get_version(self.dag_id)
            if not version:
                from airflow.dag_processing.bundles.manager import DagBundlesManager
                from airflow.dag_processing.dagbag import DagBag, sync_bag_to_db
                from airflow.sdk.definitions._internal.dag_parsing_context import (
                    _airflow_parsing_context_manager,
                )

                manager = DagBundlesManager()
                manager.sync_bundles_to_db(session=session)
                session.commit()
                # sync all bundles? or use the dags-folder bundle?
                # What if the test dag is in a different bundle?
                for bundle in manager.get_all_dag_bundles():
                    if not bundle.is_initialized:
                        bundle.initialize()
                    with _airflow_parsing_context_manager(dag_id=self.dag_id):
                        dagbag = DagBag(
                            dag_folder=bundle.path, bundle_path=bundle.path, include_examples=False
                        )
                        sync_bag_to_db(dagbag, bundle.name, bundle.version)
                    version = DagVersion.get_version(self.dag_id)
                    if version:
                        break

            # Preserve callback functions from original Dag since they're lost during serialization
            # and yes it is a hack for now! It is a tradeoff for code simplicity.
            # Without it, we need "Scheduler Dag" (Serialized dag) for the scheduler bits
            #   -- dep check, scheduling tis
            # and need real dag to get and run callbacks without having to load the dag model

            # Scheduler DAG shouldn't have these attributes, but assigning them
            # here is an easy hack to get this test() thing working.
            scheduler_dag.on_success_callback = self.on_success_callback  # type: ignore[attr-defined, union-attr]
            scheduler_dag.on_failure_callback = self.on_failure_callback  # type: ignore[attr-defined, union-attr]

            dr: DagRun = get_or_create_dagrun(
                dag=scheduler_dag,
                start_date=logical_date or run_after,
                logical_date=logical_date,
                data_interval=data_interval,
                run_after=run_after,
                run_id=DagRun.generate_run_id(
                    run_type=DagRunType.MANUAL,
                    logical_date=logical_date,
                    run_after=run_after,
                ),
                session=session,
                conf=run_conf,
                triggered_by=DagRunTriggeredByType.TEST,
                triggering_user_name="dag_test",
            )
            # Start a mock span so that one is present and not started downstream. We
            # don't care about otel in dag.test and starting the span during dagrun update
            # is not functioning properly in this context anyway.
            dr.start_dr_spans_if_needed(tis=[])

            log.debug("starting dagrun")
            # Instead of starting a scheduler, we run the minimal loop possible to check
            # for task readiness and dependency management.
            # Instead of starting a scheduler, we run the minimal loop possible to check
            # for task readiness and dependency management.

            # ``Dag.test()`` works in two different modes depending on ``use_executor``:
            # - if ``use_executor`` is False, runs the task locally with no executor using ``_run_task``
            # - if ``use_executor`` is True, sends workloads to the executor with
            #   ``BaseExecutor.queue_workload``
            if use_executor:
                from airflow.executors.base_executor import ExecutorLoader

                executor = ExecutorLoader.get_default_executor()
                executor.start()

            while dr.state == DagRunState.RUNNING:
                session.expire_all()
                schedulable_tis, _ = dr.update_state(session=session)
                for s in schedulable_tis:
                    if s.state != TaskInstanceState.UP_FOR_RESCHEDULE:
                        s.try_number += 1
                    s.state = TaskInstanceState.SCHEDULED
                    s.scheduled_dttm = timezone.utcnow()
                session.commit()
                # triggerer may mark tasks scheduled so we read from DB
                all_tis = set(dr.get_task_instances(session=session))
                scheduled_tis = {x for x in all_tis if x.state == TaskInstanceState.SCHEDULED}
                ids_unrunnable = {x for x in all_tis if x.state not in FINISHED_STATES} - scheduled_tis
                if not scheduled_tis and ids_unrunnable:
                    log.warning("No tasks to run. unrunnable tasks: %s", ids_unrunnable)
                    time.sleep(1)

                for ti in scheduled_tis:
                    task = self.task_dict[ti.task_id]

                    mark_success = (
                        re.compile(mark_success_pattern).fullmatch(ti.task_id) is not None
                        if mark_success_pattern is not None
                        else False
                    )

                    if use_executor:
                        if executor.has_task(ti):
                            continue

                        from pathlib import Path

                        from airflow.executors import workloads
                        from airflow.executors.base_executor import ExecutorLoader
                        from airflow.executors.workloads import BundleInfo

                        workload = workloads.ExecuteTask.make(
                            ti,
                            dag_rel_path=Path(self.fileloc),
                            generator=executor.jwt_generator,
                            sentry_integration=executor.sentry_integration,
                            # For the system test/debug purpose, we use the default bundle which uses
                            # local file system. If it turns out to be a feature people want, we could
                            # plumb the Bundle to use as a parameter to dag.test
                            bundle_info=BundleInfo(name="dags-folder"),
                        )
                        executor.queue_workload(workload, session=session)
                        ti.state = TaskInstanceState.QUEUED
                        session.commit()
                    else:
                        # Run the task locally
                        try:
                            if mark_success:
                                ti.set_state(TaskInstanceState.SUCCESS)
                                log.info("[DAG TEST] Marking success for %s on %s", task, ti.logical_date)
                            else:
                                _run_task(ti=ti, task=task, run_triggerer=True)
                        except Exception:
                            log.exception("Task failed; ti=%s", ti)
                if use_executor:
                    executor.heartbeat()
                    session.expire_all()

                    from airflow.jobs.scheduler_job_runner import SchedulerJobRunner
                    from airflow.models.dagbag import DBDagBag

                    SchedulerJobRunner.process_executor_events(
                        executor=executor, job_id=None, scheduler_dag_bag=DBDagBag(), session=session
                    )
            if use_executor:
                executor.end()
        return dr


def _run_task(
    *,
    ti: SchedulerTaskInstance,
    task: Operator,
    run_triggerer: bool = False,
) -> TaskRunResult | None:
    """
    Run a single task instance, and push result to Xcom for downstream tasks.

    Bypasses a lot of extra steps used in `task.run` to keep our local running as fast as
    possible.  This function is only meant for the `dag.test` function as a helper function.
    """
    from airflow.sdk._shared.module_loading import import_string
    from airflow.sdk.serde import deserialize, serialize
    from airflow.utils.session import create_session

    taskrun_result: TaskRunResult | None
    log.info("[DAG TEST] starting task_id=%s map_index=%s", ti.task_id, ti.map_index)
    while True:
        try:
            log.info("[DAG TEST] running task %s", ti)

            from airflow.sdk.api.datamodels._generated import TaskInstance as TaskInstanceSDK
            from airflow.sdk.execution_time.comms import DeferTask
            from airflow.sdk.execution_time.supervisor import run_task_in_process
            from airflow.serialization.serialized_objects import create_scheduler_operator

            # The API Server expects the task instance to be in QUEUED state before
            # it is run.
            ti.set_state(TaskInstanceState.QUEUED)
            task_sdk_ti = TaskInstanceSDK(
                id=UUID(str(ti.id)),
                task_id=ti.task_id,
                dag_id=ti.dag_id,
                run_id=ti.run_id,
                try_number=ti.try_number,
                map_index=ti.map_index,
                dag_version_id=UUID(str(ti.dag_version_id)),
            )

            taskrun_result = run_task_in_process(ti=task_sdk_ti, task=task)
            msg = taskrun_result.msg
            ti.set_state(taskrun_result.ti.state)
            ti.task = create_scheduler_operator(taskrun_result.ti.task)

            if ti.state == TaskInstanceState.DEFERRED and isinstance(msg, DeferTask) and run_triggerer:
                # API Server expects the task instance to be in QUEUED state before
                # resuming from deferral.
                ti.set_state(TaskInstanceState.QUEUED)

                log.info("[DAG TEST] running trigger in line")
                # trigger_kwargs need to be deserialized before passing to the
                # trigger class since they are in serde encoded format.
                # Ignore needed to convince mypy that trigger_kwargs is a dict
                # or a str because its unable to infer JsonValue.
                kwargs = deserialize(msg.trigger_kwargs)  # type: ignore[type-var]
                if TYPE_CHECKING:
                    assert isinstance(kwargs, dict)
                trigger = import_string(msg.classpath)(**kwargs)
                event = _run_inline_trigger(trigger, task_sdk_ti)
                ti.next_method = msg.next_method
                ti.next_kwargs = {"event": serialize(event.payload)} if event else msg.next_kwargs
                log.info("[DAG TEST] Trigger completed")

                # Set the state to SCHEDULED so that the task can be resumed.
                with create_session() as session:
                    ti.state = TaskInstanceState.SCHEDULED
                    session.add(ti)
                continue

            break
        except Exception:
            log.exception("[DAG TEST] Error running task %s", ti)
            if ti.state not in FINISHED_STATES:
                ti.set_state(TaskInstanceState.FAILED)
                taskrun_result = None
                break
            raise

    log.info("[DAG TEST] end task task_id=%s map_index=%s", ti.task_id, ti.map_index)
    return taskrun_result


def _run_inline_trigger(trigger, task_sdk_ti):
    from airflow.sdk.execution_time.supervisor import InProcessTestSupervisor

    return InProcessTestSupervisor.run_trigger_in_process(trigger=trigger, ti=task_sdk_ti)


# Since we define all the attributes of the class with attrs, we can compute this statically at parse time
DAG._DAG__serialized_fields = frozenset(a.name for a in attrs.fields(DAG)) - {  # type: ignore[attr-defined]
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
    "schedule",
}

if TYPE_CHECKING:
    # NOTE: Please keep the list of arguments in sync with DAG.__init__.
    # Only exception: dag_id here should have a default value, but not in DAG.
    @overload
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
        default_args: dict[str, Any] | None = None,
        max_active_tasks: int = ...,
        max_active_runs: int = ...,
        max_consecutive_failed_dag_runs: int = ...,
        dagrun_timeout: timedelta | None = None,
        catchup: bool = ...,
        on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None,
        on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None,
        deadline: list[DeadlineAlert] | DeadlineAlert | None = None,
        doc_md: str | None = None,
        params: ParamsDict | dict[str, Any] | None = None,
        access_control: dict[str, dict[str, Collection[str]]] | dict[str, Collection[str]] | None = None,
        is_paused_upon_creation: bool | None = None,
        jinja_environment_kwargs: dict | None = None,
        render_template_as_native_obj: bool = False,
        tags: Collection[str] | None = None,
        owner_links: dict[str, str] | None = None,
        auto_register: bool = True,
        fail_fast: bool = False,
        dag_display_name: str | None = None,
        disable_bundle_versioning: bool = False,
    ) -> Callable[[Callable], Callable[..., DAG]]:
        """
        Python dag decorator which wraps a function into an Airflow Dag.

        Accepts kwargs for operator kwarg. Can be used to parameterize Dags.

        :param dag_args: Arguments for DAG object
        :param dag_kwargs: Kwargs for DAG object.
        """

    @overload
    def dag(func: Callable[..., DAG]) -> Callable[..., DAG]:
        """Python dag decorator to use without any arguments."""


def dag(dag_id_or_func=None, __DAG_class=DAG, __warnings_stacklevel_delta=2, **decorator_kwargs):
    from airflow.sdk.definitions._internal.decorators import fixup_decorator_warning_stack

    # TODO: Task-SDK: remove __DAG_class
    # __DAG_class is a temporary hack to allow the dag decorator in airflow.models.dag to continue to
    # return SchedulerDag objects
    DAG = __DAG_class

    def wrapper(f: Callable) -> Callable[..., DAG]:
        # Determine dag_id: prioritize keyword arg, then positional string, fallback to function name
        if "dag_id" in decorator_kwargs:
            dag_id = decorator_kwargs.pop("dag_id", "")
        elif isinstance(dag_id_or_func, str) and dag_id_or_func.strip():
            dag_id = dag_id_or_func
        else:
            dag_id = f.__name__

        @functools.wraps(f)
        def factory(*args, **kwargs):
            # Generate signature for decorated function and bind the arguments when called
            # we do this to extract parameters, so we can annotate them on the DAG object.
            # In addition, this fails if we are missing any args/kwargs with TypeError as expected.
            f_sig = signature(f).bind(*args, **kwargs)
            # Apply defaults to capture default values if set.
            f_sig.apply_defaults()

            # Initialize Dag with bound arguments
            with DAG(dag_id, **decorator_kwargs) as dag_obj:
                # Set Dag documentation from function documentation if it exists and doc_md is not set.
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

                # Invoke function to create operators in the Dag scope.
                f(**f_kwargs)

            # Return dag object such that it's accessible in Globals.
            return dag_obj

        # Ensure that warnings from inside DAG() are emitted from the caller, not here
        fixup_decorator_warning_stack(factory)
        return factory

    if callable(dag_id_or_func) and not isinstance(dag_id_or_func, str):
        return wrapper(dag_id_or_func)

    return wrapper
