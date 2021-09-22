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

import copy
import functools
import logging
import os
import pathlib
import pickle
import re
import sys
import traceback
import warnings
from collections import OrderedDict
from datetime import datetime, timedelta, tzinfo
from inspect import signature
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Dict,
    FrozenSet,
    Iterable,
    List,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
    overload,
)

import jinja2
import pendulum
from dateutil.relativedelta import relativedelta
from jinja2.nativetypes import NativeEnvironment
from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, String, Text, func, or_
from sqlalchemy.orm import backref, joinedload, relationship
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import expression

import airflow.templates
from airflow import settings, utils
from airflow.compat.functools import cached_property
from airflow.configuration import conf
from airflow.exceptions import AirflowException, DuplicateTaskIdFound, TaskNotFound
from airflow.models.base import ID_LEN, Base
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagbag import DagBag
from airflow.models.dagcode import DagCode
from airflow.models.dagpickle import DagPickle
from airflow.models.dagrun import DagRun
from airflow.models.param import DagParam, ParamsDict
from airflow.models.taskinstance import Context, TaskInstance, TaskInstanceKey, clear_task_instances
from airflow.security import permissions
from airflow.stats import Stats
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
from airflow.timetables.simple import NullTimetable, OnceTimetable
from airflow.typing_compat import Literal, RePatternType
from airflow.utils import timezone
from airflow.utils.dag_cycle_tester import check_cycle
from airflow.utils.dates import cron_presets, date_range as utils_date_range
from airflow.utils.file import correct_maybe_zipped
from airflow.utils.helpers import validate_key
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import Interval, UtcDateTime, skip_locked, with_row_locks
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType, EdgeInfoType

if TYPE_CHECKING:
    from airflow.utils.task_group import TaskGroup


log = logging.getLogger(__name__)

DEFAULT_VIEW_PRESETS = ['tree', 'graph', 'duration', 'gantt', 'landing_times']
ORIENTATION_PRESETS = ['LR', 'TB', 'RL', 'BT']

ScheduleIntervalArgNotSet = type("ScheduleIntervalArgNotSet", (), {})

DagStateChangeCallback = Callable[[Context], None]
ScheduleInterval = Union[str, timedelta, relativedelta]
ScheduleIntervalArg = Union[ScheduleInterval, None, Type[ScheduleIntervalArgNotSet]]


# Backward compatibility: If neither schedule_interval nor timetable is
# *provided by the user*, default to a one-day interval.
DEFAULT_SCHEDULE_INTERVAL = timedelta(days=1)


class InconsistentDataInterval(AirflowException):
    """Exception raised when a model populates data interval fields incorrectly.

    The data interval fields should either both be None (for runs scheduled
    prior to AIP-39), or both be datetime (for runs scheduled after AIP-39 is
    implemented). This is raised if exactly one of the fields is None.
    """

    _template = (
        "Inconsistent {cls}: {start[0]}={start[1]!r}, {end[0]}={end[1]!r}, "
        "they must be either both None or both datetime"
    )

    def __init__(self, instance: Any, start_field_name: str, end_field_name: str) -> None:
        self._class_name = type(instance).__name__
        self._start_field = (start_field_name, getattr(instance, start_field_name))
        self._end_field = (end_field_name, getattr(instance, end_field_name))

    def __str__(self) -> str:
        return self._template.format(cls=self._class_name, start=self._start_field, end=self._end_field)


def _get_model_data_interval(
    instance: Any,
    start_field_name: str,
    end_field_name: str,
) -> Optional[DataInterval]:
    start = timezone.coerce_datetime(getattr(instance, start_field_name))
    end = timezone.coerce_datetime(getattr(instance, end_field_name))
    if start is None:
        if end is not None:
            raise InconsistentDataInterval(instance, start_field_name, end_field_name)
        return None
    elif end is None:
        raise InconsistentDataInterval(instance, start_field_name, end_field_name)
    return DataInterval(start, end)


def create_timetable(interval: ScheduleIntervalArg, timezone: tzinfo) -> Timetable:
    """Create a Timetable instance from a ``schedule_interval`` argument."""
    if interval is ScheduleIntervalArgNotSet:
        return DeltaDataIntervalTimetable(DEFAULT_SCHEDULE_INTERVAL)
    if interval is None:
        return NullTimetable()
    if interval == "@once":
        return OnceTimetable()
    if isinstance(interval, (timedelta, relativedelta)):
        return DeltaDataIntervalTimetable(interval)
    if isinstance(interval, str):
        return CronDataIntervalTimetable(interval, timezone)
    raise ValueError(f"{interval!r} is not a valid schedule_interval.")


def get_last_dagrun(dag_id, session, include_externally_triggered=False):
    """
    Returns the last dag run for a dag, None if there was none.
    Last dag run can be any type of run eg. scheduled or backfilled.
    Overridden DagRuns are ignored.
    """
    DR = DagRun
    query = session.query(DR).filter(DR.dag_id == dag_id)
    if not include_externally_triggered:
        query = query.filter(DR.external_trigger == expression.false())
    query = query.order_by(DR.execution_date.desc())
    return query.first()


@functools.total_ordering
class DAG(LoggingMixin):
    """
    A dag (directed acyclic graph) is a collection of tasks with directional
    dependencies. A dag also has a schedule, a start date and an end date
    (optional). For each schedule, (say daily or hourly), the DAG needs to run
    each individual tasks as their dependencies are met. Certain tasks have
    the property of depending on their own past, meaning that they can't run
    until their previous schedule (and upstream tasks) are completed.

    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.

    :param dag_id: The id of the DAG; must consist exclusively of alphanumeric
        characters, dashes, dots and underscores (all ASCII)
    :type dag_id: str
    :param description: The description for the DAG to e.g. be shown on the webserver
    :type description: str
    :param schedule_interval: Defines how often that DAG runs, this
        timedelta object gets added to your latest task instance's
        execution_date to figure out the next schedule
    :type schedule_interval: datetime.timedelta or
        dateutil.relativedelta.relativedelta or str that acts as a cron
        expression
    :param start_date: The timestamp from which the scheduler will
        attempt to backfill
    :type start_date: datetime.datetime
    :param end_date: A date beyond which your DAG won't run, leave to None
        for open ended scheduling
    :type end_date: datetime.datetime
    :param template_searchpath: This list of folders (non relative)
        defines where jinja will look for your templates. Order matters.
        Note that jinja/airflow includes the path of your DAG file by
        default
    :type template_searchpath: str or list[str]
    :param template_undefined: Template undefined type.
    :type template_undefined: jinja2.StrictUndefined
    :param user_defined_macros: a dictionary of macros that will be exposed
        in your jinja templates. For example, passing ``dict(foo='bar')``
        to this argument allows you to ``{{ foo }}`` in all jinja
        templates related to this DAG. Note that you can pass any
        type of object here.
    :type user_defined_macros: dict
    :param user_defined_filters: a dictionary of filters that will be exposed
        in your jinja templates. For example, passing
        ``dict(hello=lambda name: 'Hello %s' % name)`` to this argument allows
        you to ``{{ 'world' | hello }}`` in all jinja templates related to
        this DAG.
    :type user_defined_filters: dict
    :param default_args: A dictionary of default parameters to be used
        as constructor keyword parameters when initialising operators.
        Note that operators have the same hook, and precede those defined
        here, meaning that if your dict contains `'depends_on_past': True`
        here and `'depends_on_past': False` in the operator's call
        `default_args`, the actual value will be `False`.
    :type default_args: dict
    :param params: a dictionary of DAG level parameters that are made
        accessible in templates, namespaced under `params`. These
        params can be overridden at the task level.
    :type params: dict
    :param max_active_tasks: the number of task instances allowed to run
        concurrently
    :type max_active_tasks: int
    :param max_active_runs: maximum number of active DAG runs, beyond this
        number of DAG runs in a running state, the scheduler won't create
        new active DAG runs
    :type max_active_runs: int
    :param dagrun_timeout: specify how long a DagRun should be up before
        timing out / failing, so that new DagRuns can be created. The timeout
        is only enforced for scheduled DagRuns.
    :type dagrun_timeout: datetime.timedelta
    :param sla_miss_callback: specify a function to call when reporting SLA
        timeouts. See :ref:`sla_miss_callback<concepts:sla_miss_callback>` for
        more information about the function signature and parameters that are
        passed to the callback.
    :type sla_miss_callback: callable
    :param default_view: Specify DAG default view (tree, graph, duration,
                                                   gantt, landing_times), default tree
    :type default_view: str
    :param orientation: Specify DAG orientation in graph view (LR, TB, RL, BT), default LR
    :type orientation: str
    :param catchup: Perform scheduler catchup (or only run latest)? Defaults to True
    :type catchup: bool
    :param on_failure_callback: A function to be called when a DagRun of this dag fails.
        A context dictionary is passed as a single parameter to this function.
    :type on_failure_callback: callable
    :param on_success_callback: Much like the ``on_failure_callback`` except
        that it is executed when the dag succeeds.
    :type on_success_callback: callable
    :param access_control: Specify optional DAG-level actions, e.g.,
        "{'role1': {'can_read'}, 'role2': {'can_read', 'can_edit'}}"
    :type access_control: dict
    :param is_paused_upon_creation: Specifies if the dag is paused when created for the first time.
        If the dag exists already, this flag will be ignored. If this optional parameter
        is not specified, the global config setting will be used.
    :type is_paused_upon_creation: bool or None
    :param jinja_environment_kwargs: additional configuration options to be passed to Jinja
        ``Environment`` for template rendering

        **Example**: to avoid Jinja from removing a trailing newline from template strings ::

            DAG(dag_id='my-dag',
                jinja_environment_kwargs={
                    'keep_trailing_newline': True,
                    # some other jinja2 Environment options here
                }
            )

        **See**: `Jinja Environment documentation
        <https://jinja.palletsprojects.com/en/2.11.x/api/#jinja2.Environment>`_

    :type jinja_environment_kwargs: dict
    :param render_template_as_native_obj: If True, uses a Jinja ``NativeEnvironment``
        to render templates as native Python types. If False, a Jinja
        ``Environment`` is used to render templates as string values.
    :type render_template_as_native_obj: bool
    :param tags: List of tags to help filtering DAGS in the UI.
    :type tags: List[str]
    """

    _comps = {
        'dag_id',
        'task_ids',
        'parent_dag',
        'start_date',
        'schedule_interval',
        'fileloc',
        'template_searchpath',
        'last_loaded',
    }

    __serialized_fields: Optional[FrozenSet[str]] = None

    fileloc: str
    """
    File path that needs to be imported to load this DAG or subdag.

    This may not be an actual file on disk in the case when this DAG is loaded
    from a ZIP file or other DAG distribution format.
    """

    def __init__(
        self,
        dag_id: str,
        description: Optional[str] = None,
        schedule_interval: ScheduleIntervalArg = ScheduleIntervalArgNotSet,
        timetable: Optional[Timetable] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        full_filepath: Optional[str] = None,
        template_searchpath: Optional[Union[str, Iterable[str]]] = None,
        template_undefined: Type[jinja2.StrictUndefined] = jinja2.StrictUndefined,
        user_defined_macros: Optional[Dict] = None,
        user_defined_filters: Optional[Dict] = None,
        default_args: Optional[Dict] = None,
        concurrency: Optional[int] = None,
        max_active_tasks: int = conf.getint('core', 'max_active_tasks_per_dag'),
        max_active_runs: int = conf.getint('core', 'max_active_runs_per_dag'),
        dagrun_timeout: Optional[timedelta] = None,
        sla_miss_callback: Optional[Callable[["DAG", str, str, List[str], List[TaskInstance]], None]] = None,
        default_view: str = conf.get('webserver', 'dag_default_view').lower(),
        orientation: str = conf.get('webserver', 'dag_orientation'),
        catchup: bool = conf.getboolean('scheduler', 'catchup_by_default'),
        on_success_callback: Optional[DagStateChangeCallback] = None,
        on_failure_callback: Optional[DagStateChangeCallback] = None,
        doc_md: Optional[str] = None,
        params: Optional[Dict] = None,
        access_control: Optional[Dict] = None,
        is_paused_upon_creation: Optional[bool] = None,
        jinja_environment_kwargs: Optional[Dict] = None,
        render_template_as_native_obj: bool = False,
        tags: Optional[List[str]] = None,
    ):
        from airflow.utils.task_group import TaskGroup

        self.user_defined_macros = user_defined_macros
        self.user_defined_filters = user_defined_filters
        self.default_args = copy.deepcopy(default_args or {})
        self.params = params or {}

        # merging potentially conflicting default_args['params'] into params
        if 'params' in self.default_args:
            self.params.update(self.default_args['params'])
            del self.default_args['params']

        # check self.params and convert them into ParamsDict
        self.params = ParamsDict(self.params)

        if full_filepath:
            warnings.warn(
                "Passing full_filepath to DAG() is deprecated and has no effect",
                DeprecationWarning,
                stacklevel=2,
            )

        validate_key(dag_id)

        self._dag_id = dag_id
        if concurrency and not max_active_tasks:
            # TODO: Remove in Airflow 3.0
            warnings.warn(
                "The 'concurrency' parameter is deprecated. Please use 'max_active_tasks'.",
                DeprecationWarning,
                stacklevel=2,
            )
            max_active_tasks = concurrency
        self._max_active_tasks = max_active_tasks
        self._pickle_id: Optional[int] = None

        self._description = description
        # set file location to caller source path
        back = sys._getframe().f_back
        self.fileloc = back.f_code.co_filename if back else ""
        self.task_dict: Dict[str, BaseOperator] = {}

        # set timezone from start_date
        if start_date and start_date.tzinfo:
            self.timezone = start_date.tzinfo
        elif 'start_date' in self.default_args and self.default_args['start_date']:
            if isinstance(self.default_args['start_date'], str):
                self.default_args['start_date'] = timezone.parse(self.default_args['start_date'])
            self.timezone = self.default_args['start_date'].tzinfo

        if not hasattr(self, 'timezone') or not self.timezone:
            self.timezone = settings.TIMEZONE

        # Apply the timezone we settled on to end_date if it wasn't supplied
        if 'end_date' in self.default_args and self.default_args['end_date']:
            if isinstance(self.default_args['end_date'], str):
                self.default_args['end_date'] = timezone.parse(
                    self.default_args['end_date'], timezone=self.timezone
                )

        self.start_date = timezone.convert_to_utc(start_date)
        self.end_date = timezone.convert_to_utc(end_date)

        # also convert tasks
        if 'start_date' in self.default_args:
            self.default_args['start_date'] = timezone.convert_to_utc(self.default_args['start_date'])
        if 'end_date' in self.default_args:
            self.default_args['end_date'] = timezone.convert_to_utc(self.default_args['end_date'])

        # Calculate the DAG's timetable.
        if timetable is None:
            self.timetable = create_timetable(schedule_interval, self.timezone)
            if schedule_interval is ScheduleIntervalArgNotSet:
                schedule_interval = DEFAULT_SCHEDULE_INTERVAL
            self.schedule_interval: ScheduleInterval = schedule_interval
        elif schedule_interval is ScheduleIntervalArgNotSet:
            self.timetable = timetable
            self.schedule_interval = self.timetable.summary
        else:
            raise TypeError("cannot specify both 'schedule_interval' and 'timetable'")

        if isinstance(template_searchpath, str):
            template_searchpath = [template_searchpath]
        self.template_searchpath = template_searchpath
        self.template_undefined = template_undefined
        self.parent_dag: Optional[DAG] = None  # Gets set when DAGs are loaded
        self.last_loaded = timezone.utcnow()
        self.safe_dag_id = dag_id.replace('.', '__dot__')
        self.max_active_runs = max_active_runs
        self.dagrun_timeout = dagrun_timeout
        self.sla_miss_callback = sla_miss_callback
        if default_view in DEFAULT_VIEW_PRESETS:
            self._default_view: str = default_view
        else:
            raise AirflowException(
                f'Invalid values of dag.default_view: only support '
                f'{DEFAULT_VIEW_PRESETS}, but get {default_view}'
            )
        if orientation in ORIENTATION_PRESETS:
            self.orientation = orientation
        else:
            raise AirflowException(
                f'Invalid values of dag.orientation: only support '
                f'{ORIENTATION_PRESETS}, but get {orientation}'
            )
        self.catchup = catchup
        self.is_subdag = False  # DagBag.bag_dag() will set this to True if appropriate

        self.partial = False
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback

        # Keeps track of any extra edge metadata (sparse; will not contain all
        # edges, so do not iterate over it for that). Outer key is upstream
        # task ID, inner key is downstream task ID.
        self.edge_info: Dict[str, Dict[str, EdgeInfoType]] = {}

        # To keep it in parity with Serialized DAGs
        # and identify if DAG has on_*_callback without actually storing them in Serialized JSON
        self.has_on_success_callback = self.on_success_callback is not None
        self.has_on_failure_callback = self.on_failure_callback is not None

        self.doc_md = doc_md

        self._access_control = DAG._upgrade_outdated_dag_access_control(access_control)
        self.is_paused_upon_creation = is_paused_upon_creation

        self.jinja_environment_kwargs = jinja_environment_kwargs
        self.render_template_as_native_obj = render_template_as_native_obj
        self.tags = tags
        self._task_group = TaskGroup.create_root(self)
        self.validate_schedule_and_params()

    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    def __eq__(self, other):
        if type(self) == type(other):
            # Use getattr() instead of __dict__ as __dict__ doesn't return
            # correct values for properties.
            return all(getattr(self, c, None) == getattr(other, c, None) for c in self._comps)
        return False

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.dag_id < other.dag_id

    def __hash__(self):
        hash_components = [type(self)]
        for c in self._comps:
            # task_ids returns a list and lists can't be hashed
            if c == 'task_ids':
                val = tuple(self.task_dict.keys())
            else:
                val = getattr(self, c, None)
            try:
                hash(val)
                hash_components.append(val)
            except TypeError:
                hash_components.append(repr(val))
        return hash(tuple(hash_components))

    # Context Manager -----------------------------------------------
    def __enter__(self):
        DagContext.push_context_managed_dag(self)
        return self

    def __exit__(self, _type, _value, _tb):
        DagContext.pop_context_managed_dag()

    # /Context Manager ----------------------------------------------

    @staticmethod
    def _upgrade_outdated_dag_access_control(access_control=None):
        """
        Looks for outdated dag level actions (can_dag_read and can_dag_edit) in DAG
        access_controls (for example, {'role1': {'can_dag_read'}, 'role2': {'can_dag_read', 'can_dag_edit'}})
        and replaces them with updated actions (can_read and can_edit).
        """
        if not access_control:
            return None
        new_perm_mapping = {
            permissions.DEPRECATED_ACTION_CAN_DAG_READ: permissions.ACTION_CAN_READ,
            permissions.DEPRECATED_ACTION_CAN_DAG_EDIT: permissions.ACTION_CAN_EDIT,
        }
        updated_access_control = {}
        for role, perms in access_control.items():
            updated_access_control[role] = {new_perm_mapping.get(perm, perm) for perm in perms}

        if access_control != updated_access_control:
            warnings.warn(
                "The 'can_dag_read' and 'can_dag_edit' permissions are deprecated. "
                "Please use 'can_read' and 'can_edit', respectively.",
                DeprecationWarning,
                stacklevel=3,
            )

        return updated_access_control

    def date_range(
        self,
        start_date: datetime,
        num: Optional[int] = None,
        end_date: Optional[datetime] = timezone.utcnow(),
    ) -> List[datetime]:
        message = "`DAG.date_range()` is deprecated."
        if num is not None:
            result = utils_date_range(start_date=start_date, num=num)
        else:
            message += " Please use `DAG.iter_dagrun_infos_between(..., align=False)` instead."
            result = [
                info.logical_date
                for info in self.iter_dagrun_infos_between(start_date, end_date, align=False)
            ]
        warnings.warn(message, category=DeprecationWarning, stacklevel=2)
        return result

    def is_fixed_time_schedule(self):
        warnings.warn(
            "`DAG.is_fixed_time_schedule()` is deprecated.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        try:
            return not self.timetable._should_fix_dst
        except AttributeError:
            return True

    def following_schedule(self, dttm):
        """
        Calculates the following schedule for this dag in UTC.

        :param dttm: utc datetime
        :return: utc datetime
        """
        warnings.warn(
            "`DAG.following_schedule()` is deprecated. Use `DAG.next_dagrun_info(restricted=False)` instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        data_interval = self.infer_automated_data_interval(timezone.coerce_datetime(dttm))
        next_info = self.next_dagrun_info(data_interval, restricted=False)
        if next_info is None:
            return None
        return next_info.data_interval.start

    def previous_schedule(self, dttm):
        from airflow.timetables.interval import _DataIntervalTimetable

        warnings.warn(
            "`DAG.previous_schedule()` is deprecated.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        if not isinstance(self.timetable, _DataIntervalTimetable):
            return None
        return self.timetable._get_prev(timezone.coerce_datetime(dttm))

    def get_next_data_interval(self, dag_model: "DagModel") -> DataInterval:
        """Get the data interval of the next scheduled run.

        For compatibility, this method infers the data interval from the DAG's
        schedule if the run does not have an explicit one set, which is possible for
        runs created prior to AIP-39.

        This function is private to Airflow core and should not be depended as a
        part of the Python API.

        :meta private:
        """
        if self.dag_id != dag_model.dag_id:
            raise ValueError(f"Arguments refer to different DAGs: {self.dag_id} != {dag_model.dag_id}")
        data_interval = dag_model.next_dagrun_data_interval
        if data_interval is not None:
            return data_interval
        # Compatibility: runs scheduled before AIP-39 implementation don't have an
        # explicit data interval. Try to infer from the logical date.
        return self.infer_automated_data_interval(dag_model.next_dagrun)

    def get_run_data_interval(self, run: DagRun) -> DataInterval:
        """Get the data interval of this run.

        For compatibility, this method infers the data interval from the DAG's
        schedule if the run does not have an explicit one set, which is possible for
        runs created prior to AIP-39.

        This function is private to Airflow core and should not be depended as a
        part of the Python API.

        :meta private:
        """
        if run.dag_id is not None and run.dag_id != self.dag_id:
            raise ValueError(f"Arguments refer to different DAGs: {self.dag_id} != {run.dag_id}")
        data_interval = _get_model_data_interval(run, "data_interval_start", "data_interval_end")
        if data_interval is not None:
            return data_interval
        # Compatibility: runs created before AIP-39 implementation don't have an
        # explicit data interval. Try to infer from the logical date.
        return self.infer_automated_data_interval(run.execution_date)

    def infer_automated_data_interval(self, logical_date: datetime) -> DataInterval:
        """Infer a data interval for a run against this DAG.

        This method is used to bridge runs created prior to AIP-39
        implementation, which do not have an explicit data interval. Therefore,
        this method only considers ``schedule_interval`` values valid prior to
        Airflow 2.2.

        DO NOT use this method is there is a known data interval.
        """
        timetable_type = type(self.timetable)
        if issubclass(timetable_type, (NullTimetable, OnceTimetable)):
            return DataInterval.exact(timezone.coerce_datetime(logical_date))
        start = timezone.coerce_datetime(logical_date)
        if issubclass(timetable_type, CronDataIntervalTimetable):
            end = cast(CronDataIntervalTimetable, self.timetable)._get_next(start)
        elif issubclass(timetable_type, DeltaDataIntervalTimetable):
            end = cast(DeltaDataIntervalTimetable, self.timetable)._get_next(start)
        else:
            raise ValueError(f"Not a valid timetable: {self.timetable!r}")
        return DataInterval(start, end)

    def next_dagrun_info(
        self,
        last_automated_dagrun: Union[None, datetime, DataInterval],
        *,
        restricted: bool = True,
    ) -> Optional[DagRunInfo]:
        """Get information about the next DagRun of this dag after ``date_last_automated_dagrun``.

        This calculates what time interval the next DagRun should operate on
        (its execution date), and when it can be scheduled, , according to the
        dag's timetable, start_date, end_date, etc. This doesn't check max
        active run or any other "max_active_tasks" type limits, but only
        performs calculations based on the various date and interval fields of
        this dag and its tasks.

        :param date_last_automated_dagrun: The ``max(execution_date)`` of
            existing "automated" DagRuns for this dag (scheduled or backfill,
            but not manual).
        :param restricted: If set to *False* (default is *True*), ignore
            ``start_date``, ``end_date``, and ``catchup`` specified on the DAG
            or tasks.
        :return: DagRunInfo of the next dagrun, or None if a dagrun is not
            going to be scheduled.
        """
        # Never schedule a subdag. It will be scheduled by its parent dag.
        if self.is_subdag:
            return None
        if isinstance(last_automated_dagrun, datetime):
            warnings.warn(
                "Passing a datetime to DAG.next_dagrun_info is deprecated. Use a DataInterval instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            data_interval = self.infer_automated_data_interval(
                timezone.coerce_datetime(last_automated_dagrun)
            )
        else:
            data_interval = last_automated_dagrun
        if restricted:
            restriction = self._time_restriction
        else:
            restriction = TimeRestriction(earliest=None, latest=None, catchup=True)
        return self.timetable.next_dagrun_info(
            last_automated_data_interval=data_interval,
            restriction=restriction,
        )

    def next_dagrun_after_date(self, date_last_automated_dagrun: Optional[pendulum.DateTime]):
        warnings.warn(
            "`DAG.next_dagrun_after_date()` is deprecated. Please use `DAG.next_dagrun_info()` instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        if date_last_automated_dagrun is None:
            data_interval = None
        else:
            data_interval = self.infer_automated_data_interval(date_last_automated_dagrun)
        info = self.next_dagrun_info(data_interval)
        if info is None:
            return None
        return info.run_after

    @cached_property
    def _time_restriction(self) -> TimeRestriction:
        start_dates = [t.start_date for t in self.tasks if t.start_date]
        if self.start_date is not None:
            start_dates.append(self.start_date)
        if start_dates:
            earliest = timezone.coerce_datetime(min(start_dates))
        else:
            earliest = None
        end_dates = [t.end_date for t in self.tasks if t.end_date]
        if self.end_date is not None:
            end_dates.append(self.end_date)
        if end_dates:
            latest = timezone.coerce_datetime(max(end_dates))
        else:
            latest = None
        return TimeRestriction(earliest, latest, self.catchup)

    def iter_dagrun_infos_between(
        self,
        earliest: Optional[pendulum.DateTime],
        latest: pendulum.DateTime,
        *,
        align: bool = True,
    ) -> Iterable[DagRunInfo]:
        """Yield DagRunInfo using this DAG's timetable between given interval.

        DagRunInfo instances yielded if their ``logical_date`` is not earlier
        than ``earliest``, nor later than ``latest``. The instances are ordered
        by their ``logical_date`` from earliest to latest.

        If ``align`` is ``False``, the first run will happen immediately on
        ``earliest``, even if it does not fall on the logical timetable schedule.
        The default is ``True``, but subdags will ignore this value and always
        behave as if this is set to ``False`` for backward compatibility.

        Example: A DAG is scheduled to run every midnight (``0 0 * * *``). If
        ``earliest`` is ``2021-06-03 23:00:00``, the first DagRunInfo would be
        ``2021-06-03 23:00:00`` if ``align=False``, and ``2021-06-04 00:00:00``
        if ``align=True``.
        """
        if earliest is None:
            earliest = self._time_restriction.earliest
        earliest = timezone.coerce_datetime(earliest)
        latest = timezone.coerce_datetime(latest)

        restriction = TimeRestriction(earliest, latest, catchup=True)

        # HACK: Sub-DAGs are currently scheduled differently. For example, say
        # the schedule is @daily and start is 2021-06-03 22:16:00, a top-level
        # DAG should be first scheduled to run on midnight 2021-06-04, but a
        # sub-DAG should be first scheduled to run RIGHT NOW. We can change
        # this, but since sub-DAGs are going away in 3.0 anyway, let's keep
        # compatibility for now and remove this entirely later.
        if self.is_subdag:
            align = False

        info = self.timetable.next_dagrun_info(last_automated_data_interval=None, restriction=restriction)
        if info is None:
            # No runs to be scheduled between the user-supplied timeframe. But
            # if align=False, "invent" a data interval for the timeframe itself.
            if not align:
                yield DagRunInfo.interval(earliest, latest)
            return

        # If align=False and earliest does not fall on the timetable's logical
        # schedule, "invent" a data interval for it.
        if not align and info.logical_date != earliest:
            yield DagRunInfo.interval(earliest, info.data_interval.start)

        # Generate naturally according to schedule.
        while info is not None:
            yield info
            info = self.timetable.next_dagrun_info(
                last_automated_data_interval=info.data_interval,
                restriction=restriction,
            )

    def get_run_dates(self, start_date, end_date=None):
        """
        Returns a list of dates between the interval received as parameter using this
        dag's schedule interval. Returned dates can be used for execution dates.

        :param start_date: The start date of the interval.
        :type start_date: datetime
        :param end_date: The end date of the interval. Defaults to ``timezone.utcnow()``.
        :type end_date: datetime
        :return: A list of dates within the interval following the dag's schedule.
        :rtype: list
        """
        warnings.warn(
            "`DAG.get_run_dates()` is deprecated. Please use `DAG.iter_dagrun_infos_between()` instead.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        earliest = timezone.coerce_datetime(start_date)
        if end_date is None:
            latest = pendulum.now(timezone.utc)
        else:
            latest = timezone.coerce_datetime(end_date)
        return [info.logical_date for info in self.iter_dagrun_infos_between(earliest, latest)]

    def normalize_schedule(self, dttm):
        warnings.warn(
            "`DAG.normalize_schedule()` is deprecated.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            following = self.following_schedule(dttm)
        if not following:  # in case of @once
            return dttm
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            previous_of_following = self.previous_schedule(following)
        if previous_of_following != dttm:
            return following
        return dttm

    @provide_session
    def get_last_dagrun(self, session=None, include_externally_triggered=False):
        return get_last_dagrun(
            self.dag_id, session=session, include_externally_triggered=include_externally_triggered
        )

    @provide_session
    def has_dag_runs(self, session=None, include_externally_triggered=True) -> bool:
        return (
            get_last_dagrun(
                self.dag_id, session=session, include_externally_triggered=include_externally_triggered
            )
            is not None
        )

    @property
    def dag_id(self) -> str:
        return self._dag_id

    @dag_id.setter
    def dag_id(self, value: str) -> None:
        self._dag_id = value

    @property
    def full_filepath(self) -> str:
        """:meta private:"""
        warnings.warn(
            "DAG.full_filepath is deprecated in favour of fileloc",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.fileloc

    @full_filepath.setter
    def full_filepath(self, value) -> None:
        warnings.warn(
            "DAG.full_filepath is deprecated in favour of fileloc",
            DeprecationWarning,
            stacklevel=2,
        )
        self.fileloc = value

    @property
    def concurrency(self) -> int:
        # TODO: Remove in Airflow 3.0
        warnings.warn(
            "The 'DAG.concurrency' attribute is deprecated. Please use 'DAG.max_active_tasks'.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self._max_active_tasks

    @concurrency.setter
    def concurrency(self, value: int):
        self._max_active_tasks = value

    @property
    def max_active_tasks(self) -> int:
        return self._max_active_tasks

    @max_active_tasks.setter
    def max_active_tasks(self, value: int):
        self._max_active_tasks = value

    @property
    def access_control(self):
        return self._access_control

    @access_control.setter
    def access_control(self, value):
        self._access_control = DAG._upgrade_outdated_dag_access_control(value)

    @property
    def description(self) -> Optional[str]:
        return self._description

    @property
    def default_view(self) -> str:
        return self._default_view

    @property
    def pickle_id(self) -> Optional[int]:
        return self._pickle_id

    @pickle_id.setter
    def pickle_id(self, value: int) -> None:
        self._pickle_id = value

    def param(self, name: str, default=None) -> DagParam:
        """
        Return a DagParam object for current dag.

        :param name: dag parameter name.
        :param default: fallback value for dag parameter.
        :return: DagParam instance for specified name and current dag.
        """
        return DagParam(current_dag=self, name=name, default=default)

    @property
    def tasks(self) -> List[BaseOperator]:
        return list(self.task_dict.values())

    @tasks.setter
    def tasks(self, val):
        raise AttributeError('DAG.tasks can not be modified. Use dag.add_task() instead.')

    @property
    def task_ids(self) -> List[str]:
        return list(self.task_dict.keys())

    @property
    def task_group(self) -> "TaskGroup":
        return self._task_group

    @property
    def filepath(self) -> str:
        """:meta private:"""
        warnings.warn(
            "filepath is deprecated, use relative_fileloc instead", DeprecationWarning, stacklevel=2
        )
        return str(self.relative_fileloc)

    @property
    def relative_fileloc(self) -> pathlib.Path:
        """File location of the importable dag 'file' relative to the configured DAGs folder."""
        path = pathlib.Path(self.fileloc)
        try:
            return path.relative_to(settings.DAGS_FOLDER)
        except ValueError:
            # Not relative to DAGS_FOLDER.
            return path

    @property
    def folder(self) -> str:
        """Folder location of where the DAG object is instantiated."""
        return os.path.dirname(self.fileloc)

    @property
    def owner(self) -> str:
        """
        Return list of all owners found in DAG tasks.

        :return: Comma separated list of owners in DAG tasks
        :rtype: str
        """
        return ", ".join({t.owner for t in self.tasks})

    @property
    def allow_future_exec_dates(self) -> bool:
        return settings.ALLOW_FUTURE_EXEC_DATES and not self.timetable.can_run

    @provide_session
    def get_concurrency_reached(self, session=None) -> bool:
        """
        Returns a boolean indicating whether the max_active_tasks limit for this DAG
        has been reached
        """
        TI = TaskInstance
        qry = session.query(func.count(TI.task_id)).filter(
            TI.dag_id == self.dag_id,
            TI.state == State.RUNNING,
        )
        return qry.scalar() >= self.max_active_tasks

    @property
    def concurrency_reached(self):
        """This attribute is deprecated. Please use `airflow.models.DAG.get_concurrency_reached` method."""
        warnings.warn(
            "This attribute is deprecated. Please use `airflow.models.DAG.get_concurrency_reached` method.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_concurrency_reached()

    @provide_session
    def get_is_active(self, session=None) -> Optional[None]:
        """Returns a boolean indicating whether this DAG is active"""
        qry = session.query(DagModel).filter(DagModel.dag_id == self.dag_id)
        return qry.value(DagModel.is_active)

    @provide_session
    def get_is_paused(self, session=None) -> Optional[None]:
        """Returns a boolean indicating whether this DAG is paused"""
        qry = session.query(DagModel).filter(DagModel.dag_id == self.dag_id)
        return qry.value(DagModel.is_paused)

    @property
    def is_paused(self):
        """This attribute is deprecated. Please use `airflow.models.DAG.get_is_paused` method."""
        warnings.warn(
            "This attribute is deprecated. Please use `airflow.models.DAG.get_is_paused` method.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_is_paused()

    @property
    def normalized_schedule_interval(self) -> Optional[ScheduleInterval]:
        warnings.warn(
            "DAG.normalized_schedule_interval() is deprecated.",
            category=DeprecationWarning,
            stacklevel=2,
        )
        if isinstance(self.schedule_interval, str) and self.schedule_interval in cron_presets:
            _schedule_interval = cron_presets.get(self.schedule_interval)  # type: Optional[ScheduleInterval]
        elif self.schedule_interval == '@once':
            _schedule_interval = None
        else:
            _schedule_interval = self.schedule_interval
        return _schedule_interval

    @provide_session
    def handle_callback(self, dagrun, success=True, reason=None, session=None):
        """
        Triggers the appropriate callback depending on the value of success, namely the
        on_failure_callback or on_success_callback. This method gets the context of a
        single TaskInstance part of this DagRun and passes that to the callable along
        with a 'reason', primarily to differentiate DagRun failures.

        .. note: The logs end up in
            ``$AIRFLOW_HOME/logs/scheduler/latest/PROJECT/DAG_FILE.py.log``

        :param dagrun: DagRun object
        :param success: Flag to specify if failure or success callback should be called
        :param reason: Completion reason
        :param session: Database session
        """
        callback = self.on_success_callback if success else self.on_failure_callback
        if callback:
            self.log.info('Executing dag callback function: %s', callback)
            tis = dagrun.get_task_instances(session=session)
            ti = tis[-1]  # get first TaskInstance of DagRun
            ti.task = self.get_task(ti.task_id)
            context = ti.get_template_context(session=session)
            context.update({'reason': reason})
            try:
                callback(context)
            except Exception:
                self.log.exception("failed to invoke dag state update callback")
                Stats.incr("dag.callback_exceptions")

    def get_active_runs(self):
        """
        Returns a list of dag run execution dates currently running

        :return: List of execution dates
        """
        runs = DagRun.find(dag_id=self.dag_id, state=State.RUNNING)

        active_dates = []
        for run in runs:
            active_dates.append(run.execution_date)

        return active_dates

    @provide_session
    def get_num_active_runs(self, external_trigger=None, session=None):
        """
        Returns the number of active "running" dag runs

        :param external_trigger: True for externally triggered active dag runs
        :type external_trigger: bool
        :param session:
        :return: number greater than 0 for active dag runs
        """
        # .count() is inefficient
        query = (
            session.query(func.count())
            .filter(DagRun.dag_id == self.dag_id)
            .filter(DagRun.state == State.RUNNING)
        )

        if external_trigger is not None:
            query = query.filter(
                DagRun.external_trigger == (expression.true() if external_trigger else expression.false())
            )

        return query.scalar()

    @provide_session
    def get_dagrun(
        self,
        execution_date: Optional[str] = None,
        run_id: Optional[str] = None,
        session: Optional[Session] = None,
    ):
        """
        Returns the dag run for a given execution date or run_id if it exists, otherwise
        none.

        :param execution_date: The execution date of the DagRun to find.
        :param run_id: The run_id of the DagRun to find.
        :param session:
        :return: The DagRun if found, otherwise None.
        """
        if not (execution_date or run_id):
            raise TypeError("You must provide either the execution_date or the run_id")
        query = session.query(DagRun)
        if execution_date:
            query = query.filter(DagRun.dag_id == self.dag_id, DagRun.execution_date == execution_date)
        if run_id:
            query = query.filter(DagRun.dag_id == self.dag_id, DagRun.run_id == run_id)
        return query.first()

    @provide_session
    def get_dagruns_between(self, start_date, end_date, session=None):
        """
        Returns the list of dag runs between start_date (inclusive) and end_date (inclusive).

        :param start_date: The starting execution date of the DagRun to find.
        :param end_date: The ending execution date of the DagRun to find.
        :param session:
        :return: The list of DagRuns found.
        """
        dagruns = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date >= start_date,
                DagRun.execution_date <= end_date,
            )
            .all()
        )

        return dagruns

    @provide_session
    def get_latest_execution_date(self, session=None):
        """Returns the latest date for which at least one dag run exists"""
        return session.query(func.max(DagRun.execution_date)).filter(DagRun.dag_id == self.dag_id).scalar()

    @property
    def latest_execution_date(self):
        """This attribute is deprecated. Please use `airflow.models.DAG.get_latest_execution_date` method."""
        warnings.warn(
            "This attribute is deprecated. Please use `airflow.models.DAG.get_latest_execution_date` method.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_latest_execution_date()

    @property
    def subdags(self):
        """Returns a list of the subdag objects associated to this DAG"""
        # Check SubDag for class but don't check class directly
        from airflow.operators.subdag import SubDagOperator

        subdag_lst = []
        for task in self.tasks:
            if (
                isinstance(task, SubDagOperator)
                or
                # TODO remove in Airflow 2.0
                type(task).__name__ == 'SubDagOperator'
                or task.task_type == 'SubDagOperator'
            ):
                subdag_lst.append(task.subdag)
                subdag_lst += task.subdag.subdags
        return subdag_lst

    def resolve_template_files(self):
        for t in self.tasks:
            t.resolve_template_files()

    def get_template_env(self) -> jinja2.Environment:
        """Build a Jinja2 environment."""
        # Collect directories to search for template files
        searchpath = [self.folder]
        if self.template_searchpath:
            searchpath += self.template_searchpath

        # Default values (for backward compatibility)
        jinja_env_options = {
            'loader': jinja2.FileSystemLoader(searchpath),
            'undefined': self.template_undefined,
            'extensions': ["jinja2.ext.do"],
            'cache_size': 0,
        }
        if self.jinja_environment_kwargs:
            jinja_env_options.update(self.jinja_environment_kwargs)
        if self.render_template_as_native_obj:
            env = NativeEnvironment(**jinja_env_options)
        else:
            env = airflow.templates.SandboxedEnvironment(**jinja_env_options)  # type: ignore

        # Add any user defined items. Safe to edit globals as long as no templates are rendered yet.
        # http://jinja.pocoo.org/docs/2.10/api/#jinja2.Environment.globals
        if self.user_defined_macros:
            env.globals.update(self.user_defined_macros)
        if self.user_defined_filters:
            env.filters.update(self.user_defined_filters)

        return env

    def set_dependency(self, upstream_task_id, downstream_task_id):
        """
        Simple utility method to set dependency between two tasks that
        already have been added to the DAG using add_task()
        """
        self.get_task(upstream_task_id).set_downstream(self.get_task(downstream_task_id))

    @provide_session
    def get_task_instances_before(
        self,
        base_date: datetime,
        num: int,
        *,
        session: Session,
    ) -> List[TaskInstance]:
        """Get ``num`` task instances before (including) ``base_date``.

        The returned list may contain exactly ``num`` task instances. It can
        have less if there are less than ``num`` scheduled DAG runs before
        ``base_date``, or more if there are manual task runs between the
        requested period, which does not count toward ``num``.
        """
        min_date = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date <= base_date,
                DagRun.run_type != DagRunType.MANUAL,
            )
            .order_by(DagRun.execution_date.desc())
            .offset(num)
            .first()
        )
        if min_date is None:
            min_date = timezone.utc_epoch()
        return self.get_task_instances(start_date=min_date, end_date=base_date, session=session)

    @provide_session
    def get_task_instances(
        self, start_date=None, end_date=None, state=None, session=None
    ) -> List[TaskInstance]:
        if not start_date:
            start_date = (timezone.utcnow() - timedelta(30)).date()
            start_date = timezone.make_aware(datetime.combine(start_date, datetime.min.time()))

        return (
            self._get_task_instances(
                task_ids=None,
                start_date=start_date,
                end_date=end_date,
                run_id=None,
                state=state,
                include_subdags=False,
                include_parentdag=False,
                include_dependent_dags=False,
                exclude_task_ids=[],
                as_pk_tuple=False,
                session=session,
            )
            .join(TaskInstance.dag_run)
            .order_by(DagRun.execution_date)
            .all()
        )

    @overload
    def _get_task_instances(
        self,
        *,
        task_ids,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        run_id: None,
        state: Union[str, List[str]],
        include_subdags: bool,
        include_parentdag: bool,
        include_dependent_dags: bool,
        exclude_task_ids: Collection[str],
        as_pk_tuple: Literal[True],
        session: Session,
        dag_bag: "DagBag" = None,
        recursion_depth: int = 0,
        max_recursion_depth: int = None,
        visited_external_tis: Set[Tuple[str, str, datetime]] = None,
    ) -> Set["TaskInstanceKey"]:
        ...  # pragma: no cover

    @overload
    def _get_task_instances(
        self,
        *,
        task_ids,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        run_id: Optional[str],
        state: Union[str, List[str]],
        include_subdags: bool,
        include_parentdag: bool,
        include_dependent_dags: bool,
        as_pk_tuple: Literal[False],
        exclude_task_ids: Collection[str],
        session: Session,
        dag_bag: "DagBag" = None,
        recursion_depth: int = 0,
        max_recursion_depth: int = None,
        visited_external_tis: Set[Tuple[str, str, datetime]] = None,
    ) -> Iterable[TaskInstance]:
        ...  # pragma: no cover

    def _get_task_instances(
        self,
        *,
        task_ids,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        run_id: Optional[str],
        state: Union[str, List[str]],
        include_subdags: bool,
        include_parentdag: bool,
        include_dependent_dags: bool,
        as_pk_tuple: bool,
        exclude_task_ids: Collection[str],
        session: Session,
        dag_bag: "DagBag" = None,
        recursion_depth: int = 0,
        max_recursion_depth: int = None,
        visited_external_tis: Set[Tuple[str, str, datetime]] = None,
    ) -> Union[Iterable[TaskInstance], Set[TaskInstanceKey]]:
        TI = TaskInstance

        # If we are looking at subdags/dependent dags we want to avoid UNION calls
        # in SQL (it doesn't play nice with fields that have no equality operator,
        # like JSON types), we instead build our result set separately.
        #
        # This will be empty if we are only looking at one dag, in which case
        # we can return the filtered TI query object directly.
        result: Set[TaskInstanceKey] = set()

        # Do we want full objects, or just the primary columns?
        if as_pk_tuple:
            tis = session.query(TI.dag_id, TI.task_id, TI.run_id)
        else:
            tis = session.query(TaskInstance)
        tis = tis.join(TaskInstance.dag_run)

        if include_subdags:
            # Crafting the right filter for dag_id and task_ids combo
            conditions = []
            for dag in self.subdags + [self]:
                conditions.append(
                    (TaskInstance.dag_id == dag.dag_id) & TaskInstance.task_id.in_(dag.task_ids)
                )
            tis = tis.filter(or_(*conditions))
        else:
            tis = tis.filter(TaskInstance.dag_id == self.dag_id, TaskInstance.task_id.in_(self.task_ids))
        if run_id:
            tis = tis.filter(TaskInstance.run_id == run_id)
        if start_date:
            tis = tis.filter(DagRun.execution_date >= start_date)
        if task_ids:
            tis = tis.filter(TaskInstance.task_id.in_(task_ids))

        # This allows allow_trigger_in_future config to take affect, rather than mandating exec_date <= UTC
        if end_date or not self.allow_future_exec_dates:
            end_date = end_date or timezone.utcnow()
            tis = tis.filter(DagRun.execution_date <= end_date)

        if state:
            if isinstance(state, str):
                tis = tis.filter(TaskInstance.state == state)
            elif len(state) == 1:
                tis = tis.filter(TaskInstance.state == state[0])
            else:
                # this is required to deal with NULL values
                if None in state:
                    if all(x is None for x in state):
                        tis = tis.filter(TaskInstance.state.is_(None))
                    else:
                        not_none_state = [s for s in state if s]
                        tis = tis.filter(
                            or_(TaskInstance.state.in_(not_none_state), TaskInstance.state.is_(None))
                        )
                else:
                    tis = tis.filter(TaskInstance.state.in_(state))

        # Next, get any of them from our parent DAG (if there is one)
        if include_parentdag and self.is_subdag and self.parent_dag is not None:
            p_dag = self.parent_dag.partial_subset(
                task_ids_or_regex=r"^{}$".format(self.dag_id.split('.')[1]),
                include_upstream=False,
                include_downstream=True,
            )
            result.update(
                p_dag._get_task_instances(
                    task_ids=task_ids,
                    start_date=start_date,
                    end_date=end_date,
                    run_id=None,
                    state=state,
                    include_subdags=include_subdags,
                    include_parentdag=False,
                    include_dependent_dags=include_dependent_dags,
                    as_pk_tuple=True,
                    exclude_task_ids=exclude_task_ids,
                    session=session,
                    dag_bag=dag_bag,
                    recursion_depth=recursion_depth,
                    max_recursion_depth=max_recursion_depth,
                    visited_external_tis=visited_external_tis,
                )
            )

        if include_dependent_dags:
            # Recursively find external tasks indicated by ExternalTaskMarker
            from airflow.sensors.external_task import ExternalTaskMarker

            query = tis
            if as_pk_tuple:
                condition = TI.filter_for_tis(TaskInstanceKey(*cols) for cols in tis.all())
                if condition is not None:
                    query = session.query(TI).filter(condition)

            if visited_external_tis is None:
                visited_external_tis = set()

            for ti in query.filter(TI.operator == ExternalTaskMarker.__name__):
                ti_key = ti.key.primary
                if ti_key in visited_external_tis:
                    continue

                visited_external_tis.add(ti_key)

                task: ExternalTaskMarker = cast(ExternalTaskMarker, copy.copy(self.get_task(ti.task_id)))
                ti.task = task

                if max_recursion_depth is None:
                    # Maximum recursion depth allowed is the recursion_depth of the first
                    # ExternalTaskMarker in the tasks to be visited.
                    max_recursion_depth = task.recursion_depth

                if recursion_depth + 1 > max_recursion_depth:
                    # Prevent cycles or accidents.
                    raise AirflowException(
                        "Maximum recursion depth {} reached for {} {}. "
                        "Attempted to clear too many tasks "
                        "or there may be a cyclic dependency.".format(
                            max_recursion_depth, ExternalTaskMarker.__name__, ti.task_id
                        )
                    )
                ti.render_templates()
                external_tis = (
                    session.query(TI)
                    .join(TI.dag_run)
                    .filter(
                        TI.dag_id == task.external_dag_id,
                        TI.task_id == task.external_task_id,
                        DagRun.execution_date == pendulum.parse(task.execution_date),
                    )
                )

                for tii in external_tis:
                    if not dag_bag:
                        dag_bag = DagBag(read_dags_from_db=True)
                    external_dag = dag_bag.get_dag(tii.dag_id, session=session)
                    if not external_dag:
                        raise AirflowException(f"Could not find dag {tii.dag_id}")
                    downstream = external_dag.partial_subset(
                        task_ids_or_regex=[tii.task_id],
                        include_upstream=False,
                        include_downstream=True,
                    )
                    result.update(
                        downstream._get_task_instances(
                            task_ids=None,
                            run_id=tii.run_id,
                            start_date=None,
                            end_date=None,
                            state=state,
                            include_subdags=include_subdags,
                            include_dependent_dags=include_dependent_dags,
                            include_parentdag=False,
                            as_pk_tuple=True,
                            exclude_task_ids=exclude_task_ids,
                            dag_bag=dag_bag,
                            session=session,
                            recursion_depth=recursion_depth + 1,
                            max_recursion_depth=max_recursion_depth,
                            visited_external_tis=visited_external_tis,
                        )
                    )

        if result or as_pk_tuple:
            # Only execute the `ti` query if we have also collected some other results (i.e. subdags etc.)
            if as_pk_tuple:
                result.update(TaskInstanceKey(*cols) for cols in tis.all())
            else:
                result.update(ti.key for ti in tis.all())

            if exclude_task_ids:
                result = set(
                    filter(
                        lambda key: key.task_id not in exclude_task_ids,
                        result,
                    )
                )

        if as_pk_tuple:
            return result
        elif result:
            # We've been asked for objects, lets combine it all back in to a result set
            tis = tis.with_entities(TI.dag_id, TI.task_id, TI.run_id)

            tis = session.query(TI).filter(TI.filter_for_tis(result))
        elif exclude_task_ids:
            tis = tis.filter(TI.task_id.notin_(list(exclude_task_ids)))

        return tis

    @provide_session
    def set_task_instance_state(
        self,
        task_id: str,
        execution_date: datetime,
        state: State,
        upstream: Optional[bool] = False,
        downstream: Optional[bool] = False,
        future: Optional[bool] = False,
        past: Optional[bool] = False,
        commit: Optional[bool] = True,
        session=None,
    ) -> List[TaskInstance]:
        """
        Set the state of a TaskInstance to the given state, and clear its downstream tasks that are
        in failed or upstream_failed state.

        :param task_id: Task ID of the TaskInstance
        :type task_id: str
        :param execution_date: execution_date of the TaskInstance
        :type execution_date: datetime
        :param state: State to set the TaskInstance to
        :type state: State
        :param upstream: Include all upstream tasks of the given task_id
        :type upstream: bool
        :param downstream: Include all downstream tasks of the given task_id
        :type downstream: bool
        :param future: Include all future TaskInstances of the given task_id
        :type future: bool
        :param commit: Commit changes
        :type commit: bool
        :param past: Include all past TaskInstances of the given task_id
        :type past: bool
        """
        from airflow.api.common.experimental.mark_tasks import set_state

        task = self.get_task(task_id)
        task.dag = self

        altered = set_state(
            tasks=[task],
            execution_date=execution_date,
            upstream=upstream,
            downstream=downstream,
            future=future,
            past=past,
            state=state,
            commit=commit,
            session=session,
        )

        if not commit:
            return altered

        # Clear downstream tasks that are in failed/upstream_failed state to resume them.
        # Flush the session so that the tasks marked success are reflected in the db.
        session.flush()
        subdag = self.partial_subset(
            task_ids_or_regex={task_id},
            include_downstream=True,
            include_upstream=False,
        )

        end_date = execution_date if not future else None
        start_date = execution_date if not past else None

        subdag.clear(
            start_date=start_date,
            end_date=end_date,
            include_subdags=True,
            include_parentdag=True,
            only_failed=True,
            session=session,
            # Exclude the task itself from being cleared
            exclude_task_ids={task_id},
        )

        return altered

    @property
    def roots(self) -> List[BaseOperator]:
        """Return nodes with no parents. These are first to execute and are called roots or root nodes."""
        return [task for task in self.tasks if not task.upstream_list]

    @property
    def leaves(self) -> List[BaseOperator]:
        """Return nodes with no children. These are last to execute and are called leaves or leaf nodes."""
        return [task for task in self.tasks if not task.downstream_list]

    def topological_sort(self, include_subdag_tasks: bool = False):
        """
        Sorts tasks in topographical order, such that a task comes after any of its
        upstream dependencies.

        Heavily inspired by:
        http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

        :param include_subdag_tasks: whether to include tasks in subdags, default to False
        :return: list of tasks in topological order
        """
        from airflow.operators.subdag import SubDagOperator  # Avoid circular import

        # convert into an OrderedDict to speedup lookup while keeping order the same
        graph_unsorted = OrderedDict((task.task_id, task) for task in self.tasks)

        graph_sorted = []  # type: List[BaseOperator]

        # special case
        if len(self.tasks) == 0:
            return tuple(graph_sorted)

        # Run until the unsorted graph is empty.
        while graph_unsorted:
            # Go through each of the node/edges pairs in the unsorted
            # graph. If a set of edges doesn't contain any nodes that
            # haven't been resolved, that is, that are still in the
            # unsorted graph, remove the pair from the unsorted graph,
            # and append it to the sorted graph. Note here that by using
            # using the items() method for iterating, a copy of the
            # unsorted graph is used, allowing us to modify the unsorted
            # graph as we move through it. We also keep a flag for
            # checking that graph is acyclic, which is true if any
            # nodes are resolved during each pass through the graph. If
            # not, we need to exit as the graph therefore can't be
            # sorted.
            acyclic = False
            for node in list(graph_unsorted.values()):
                for edge in node.upstream_list:
                    if edge.task_id in graph_unsorted:
                        break
                # no edges in upstream tasks
                else:
                    acyclic = True
                    del graph_unsorted[node.task_id]
                    graph_sorted.append(node)
                    if include_subdag_tasks and isinstance(node, SubDagOperator):
                        graph_sorted.extend(node.subdag.topological_sort(include_subdag_tasks=True))

            if not acyclic:
                raise AirflowException(f"A cyclic dependency occurred in dag: {self.dag_id}")

        return tuple(graph_sorted)

    @provide_session
    def set_dag_runs_state(
        self,
        state: str = State.RUNNING,
        session: Session = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        dag_ids: List[str] = None,
    ) -> None:
        warnings.warn(
            "This method is deprecated and will be removed in a future version.",
            DeprecationWarning,
            stacklevel=3,
        )
        dag_ids = dag_ids or [self.dag_id]
        query = session.query(DagRun).filter(DagRun.dag_id.in_(dag_ids))
        if start_date:
            query = query.filter(DagRun.execution_date >= start_date)
        if end_date:
            query = query.filter(DagRun.execution_date <= end_date)
        query.update({DagRun.state: state}, synchronize_session='fetch')

    @provide_session
    def clear(
        self,
        task_ids=None,
        start_date=None,
        end_date=None,
        only_failed=False,
        only_running=False,
        confirm_prompt=False,
        include_subdags=True,
        include_parentdag=True,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run=False,
        session=None,
        get_tis=False,
        recursion_depth=0,
        max_recursion_depth=None,
        dag_bag=None,
        exclude_task_ids: FrozenSet[str] = frozenset({}),
    ):
        """
        Clears a set of task instances associated with the current dag for
        a specified date range.

        :param task_ids: List of task ids to clear
        :type task_ids: List[str]
        :param start_date: The minimum execution_date to clear
        :type start_date: datetime.datetime or None
        :param end_date: The maximum execution_date to clear
        :type end_date: datetime.datetime or None
        :param only_failed: Only clear failed tasks
        :type only_failed: bool
        :param only_running: Only clear running tasks.
        :type only_running: bool
        :param confirm_prompt: Ask for confirmation
        :type confirm_prompt: bool
        :param include_subdags: Clear tasks in subdags and clear external tasks
            indicated by ExternalTaskMarker
        :type include_subdags: bool
        :param include_parentdag: Clear tasks in the parent dag of the subdag.
        :type include_parentdag: bool
        :param dag_run_state: state to set DagRun to. If set to False, dagrun state will not
            be changed.
        :param dry_run: Find the tasks to clear but don't clear them.
        :type dry_run: bool
        :param session: The sqlalchemy session to use
        :type session: sqlalchemy.orm.session.Session
        :param dag_bag: The DagBag used to find the dags subdags (Optional)
        :type dag_bag: airflow.models.dagbag.DagBag
        :param exclude_task_ids: A set of ``task_id`` that should not be cleared
        :type exclude_task_ids: frozenset
        """
        if get_tis:
            warnings.warn(
                "Passing `get_tis` to dag.clear() is deprecated. Use `dry_run` parameter instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            dry_run = True

        if recursion_depth:
            warnings.warn(
                "Passing `recursion_depth` to dag.clear() is deprecated.",
                DeprecationWarning,
                stacklevel=2,
            )
        if max_recursion_depth:
            warnings.warn(
                "Passing `max_recursion_depth` to dag.clear() is deprecated.",
                DeprecationWarning,
                stacklevel=2,
            )

        state = []
        if only_failed:
            state += [State.FAILED, State.UPSTREAM_FAILED]
            only_failed = None
        if only_running:
            # Yes, having `+=` doesn't make sense, but this was the existing behaviour
            state += [State.RUNNING]
            only_running = None

        tis = self._get_task_instances(
            task_ids=task_ids,
            start_date=start_date,
            end_date=end_date,
            run_id=None,
            state=state,
            include_subdags=include_subdags,
            include_parentdag=include_parentdag,
            include_dependent_dags=include_subdags,  # compat, yes this is not a typo
            as_pk_tuple=False,
            session=session,
            dag_bag=dag_bag,
            exclude_task_ids=exclude_task_ids,
        )

        if dry_run:
            return tis

        tis = tis.all()

        count = len(tis)
        do_it = True
        if count == 0:
            return 0
        if confirm_prompt:
            ti_list = "\n".join(str(t) for t in tis)
            question = (
                "You are about to delete these {count} tasks:\n{ti_list}\n\nAre you sure? (yes/no): "
            ).format(count=count, ti_list=ti_list)
            do_it = utils.helpers.ask_yesno(question)

        if do_it:
            clear_task_instances(
                tis,
                session,
                dag=self,
                dag_run_state=dag_run_state,
            )
        else:
            count = 0
            print("Cancelled, nothing was cleared.")

        session.flush()
        return count

    @classmethod
    def clear_dags(
        cls,
        dags,
        start_date=None,
        end_date=None,
        only_failed=False,
        only_running=False,
        confirm_prompt=False,
        include_subdags=True,
        include_parentdag=False,
        dag_run_state=DagRunState.QUEUED,
        dry_run=False,
    ):
        all_tis = []
        for dag in dags:
            tis = dag.clear(
                start_date=start_date,
                end_date=end_date,
                only_failed=only_failed,
                only_running=only_running,
                confirm_prompt=False,
                include_subdags=include_subdags,
                include_parentdag=include_parentdag,
                dag_run_state=dag_run_state,
                dry_run=True,
            )
            all_tis.extend(tis)

        if dry_run:
            return all_tis

        count = len(all_tis)
        do_it = True
        if count == 0:
            print("Nothing to clear.")
            return 0
        if confirm_prompt:
            ti_list = "\n".join(str(t) for t in all_tis)
            question = f"You are about to delete these {count} tasks:\n{ti_list}\n\nAre you sure? (yes/no): "
            do_it = utils.helpers.ask_yesno(question)

        if do_it:
            for dag in dags:
                dag.clear(
                    start_date=start_date,
                    end_date=end_date,
                    only_failed=only_failed,
                    only_running=only_running,
                    confirm_prompt=False,
                    include_subdags=include_subdags,
                    dag_run_state=dag_run_state,
                    dry_run=False,
                )
        else:
            count = 0
            print("Cancelled, nothing was cleared.")
        return count

    def __deepcopy__(self, memo):
        # Switcharoo to go around deepcopying objects coming through the
        # backdoor
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in self.__dict__.items():
            if k not in ('user_defined_macros', 'user_defined_filters', 'params', '_log'):
                setattr(result, k, copy.deepcopy(v, memo))

        result.user_defined_macros = self.user_defined_macros
        result.user_defined_filters = self.user_defined_filters
        result.params = self.params
        if hasattr(self, '_log'):
            result._log = self._log
        return result

    def sub_dag(self, *args, **kwargs):
        """This method is deprecated in favor of partial_subset"""
        warnings.warn(
            "This method is deprecated and will be removed in a future version. Please use partial_subset",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.partial_subset(*args, **kwargs)

    def partial_subset(
        self,
        task_ids_or_regex: Union[str, RePatternType, Iterable[str]],
        include_downstream=False,
        include_upstream=True,
        include_direct_upstream=False,
    ):
        """
        Returns a subset of the current dag as a deep copy of the current dag
        based on a regex that should match one or many tasks, and includes
        upstream and downstream neighbours based on the flag passed.

        :param task_ids_or_regex: Either a list of task_ids, or a regex to
            match against task ids (as a string, or compiled regex pattern).
        :type task_ids_or_regex: [str] or str or re.Pattern
        :param include_downstream: Include all downstream tasks of matched
            tasks, in addition to matched tasks.
        :param include_upstream: Include all upstream tasks of matched tasks,
            in addition to matched tasks.
        """
        # deep-copying self.task_dict and self._task_group takes a long time, and we don't want all
        # the tasks anyway, so we copy the tasks manually later
        memo = {id(self.task_dict): None, id(self._task_group): None}
        dag = copy.deepcopy(self, memo)  # type: ignore

        if isinstance(task_ids_or_regex, (str, RePatternType)):
            matched_tasks = [t for t in self.tasks if re.findall(task_ids_or_regex, t.task_id)]
        else:
            matched_tasks = [t for t in self.tasks if t.task_id in task_ids_or_regex]

        also_include = []
        for t in matched_tasks:
            if include_downstream:
                also_include += t.get_flat_relatives(upstream=False)
            if include_upstream:
                also_include += t.get_flat_relatives(upstream=True)
            elif include_direct_upstream:
                also_include += t.upstream_list

        # Compiling the unique list of tasks that made the cut
        # Make sure to not recursively deepcopy the dag while copying the task
        dag.task_dict = {
            t.task_id: copy.deepcopy(t, {id(t.dag): dag})  # type: ignore
            for t in matched_tasks + also_include
        }

        def filter_task_group(group, parent_group):
            """Exclude tasks not included in the subdag from the given TaskGroup."""
            copied = copy.copy(group)
            copied.used_group_ids = set(copied.used_group_ids)
            copied._parent_group = parent_group

            copied.children = {}

            for child in group.children.values():
                if isinstance(child, BaseOperator):
                    if child.task_id in dag.task_dict:
                        copied.children[child.task_id] = dag.task_dict[child.task_id]
                    else:
                        copied.used_group_ids.discard(child.task_id)
                else:
                    filtered_child = filter_task_group(child, copied)

                    # Only include this child TaskGroup if it is non-empty.
                    if filtered_child.children:
                        copied.children[child.group_id] = filtered_child

            return copied

        dag._task_group = filter_task_group(self._task_group, None)

        # Removing upstream/downstream references to tasks and TaskGroups that did not make
        # the cut.
        subdag_task_groups = dag.task_group.get_task_group_dict()
        for group in subdag_task_groups.values():
            group.upstream_group_ids = group.upstream_group_ids.intersection(subdag_task_groups.keys())
            group.downstream_group_ids = group.downstream_group_ids.intersection(subdag_task_groups.keys())
            group.upstream_task_ids = group.upstream_task_ids.intersection(dag.task_dict.keys())
            group.downstream_task_ids = group.downstream_task_ids.intersection(dag.task_dict.keys())

        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # make the cut
            t._upstream_task_ids = t.upstream_task_ids.intersection(dag.task_dict.keys())
            t._downstream_task_ids = t.downstream_task_ids.intersection(dag.task_dict.keys())

        if len(dag.tasks) < len(self.tasks):
            dag.partial = True

        return dag

    def has_task(self, task_id: str):
        return task_id in (t.task_id for t in self.tasks)

    def get_task(self, task_id: str, include_subdags: bool = False) -> BaseOperator:
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        if include_subdags:
            for dag in self.subdags:
                if task_id in dag.task_dict:
                    return dag.task_dict[task_id]
        raise TaskNotFound(f"Task {task_id} not found")

    def pickle_info(self):
        d = {}
        d['is_picklable'] = True
        try:
            dttm = timezone.utcnow()
            pickled = pickle.dumps(self)
            d['pickle_len'] = len(pickled)
            d['pickling_duration'] = str(timezone.utcnow() - dttm)
        except Exception as e:
            self.log.debug(e)
            d['is_picklable'] = False
            d['stacktrace'] = traceback.format_exc()
        return d

    @provide_session
    def pickle(self, session=None) -> DagPickle:
        dag = session.query(DagModel).filter(DagModel.dag_id == self.dag_id).first()
        dp = None
        if dag and dag.pickle_id:
            dp = session.query(DagPickle).filter(DagPickle.id == dag.pickle_id).first()
        if not dp or dp.pickle != self:
            dp = DagPickle(dag=self)
            session.add(dp)
            self.last_pickled = timezone.utcnow()
            session.commit()
            self.pickle_id = dp.id

        return dp

    def tree_view(self) -> None:
        """Print an ASCII tree representation of the DAG."""

        def get_downstream(task, level=0):
            print((" " * level * 4) + str(task))
            level += 1
            for t in task.downstream_list:
                get_downstream(t, level)

        for t in self.roots:
            get_downstream(t)

    @property
    def task(self):
        from airflow.decorators import task

        return functools.partial(task, dag=self)

    def add_task(self, task):
        """
        Add a task to the DAG

        :param task: the task you want to add
        :type task: task
        """
        if not self.start_date and not task.start_date:
            raise AirflowException("Task is missing the start_date parameter")
        # if the task has no start date, assign it the same as the DAG
        elif not task.start_date:
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

        if (
            task.task_id in self.task_dict and self.task_dict[task.task_id] is not task
        ) or task.task_id in self._task_group.used_group_ids:
            raise DuplicateTaskIdFound(f"Task id '{task.task_id}' has already been added to the DAG")
        else:
            self.task_dict[task.task_id] = task
            task.dag = self
            # Add task_id to used_group_ids to prevent group_id and task_id collisions.
            self._task_group.used_group_ids.add(task.task_id)

        self.task_count = len(self.task_dict)

    def add_tasks(self, tasks):
        """
        Add a list of tasks to the DAG

        :param tasks: a lit of tasks you want to add
        :type tasks: list of tasks
        """
        for task in tasks:
            self.add_task(task)

    def run(
        self,
        start_date=None,
        end_date=None,
        mark_success=False,
        local=False,
        executor=None,
        donot_pickle=conf.getboolean('core', 'donot_pickle'),
        ignore_task_deps=False,
        ignore_first_depends_on_past=True,
        pool=None,
        delay_on_limit_secs=1.0,
        verbose=False,
        conf=None,
        rerun_failed_tasks=False,
        run_backwards=False,
    ):
        """
        Runs the DAG.

        :param start_date: the start date of the range to run
        :type start_date: datetime.datetime
        :param end_date: the end date of the range to run
        :type end_date: datetime.datetime
        :param mark_success: True to mark jobs as succeeded without running them
        :type mark_success: bool
        :param local: True to run the tasks using the LocalExecutor
        :type local: bool
        :param executor: The executor instance to run the tasks
        :type executor: airflow.executor.base_executor.BaseExecutor
        :param donot_pickle: True to avoid pickling DAG object and send to workers
        :type donot_pickle: bool
        :param ignore_task_deps: True to skip upstream tasks
        :type ignore_task_deps: bool
        :param ignore_first_depends_on_past: True to ignore depends_on_past
            dependencies for the first set of tasks only
        :type ignore_first_depends_on_past: bool
        :param pool: Resource pool to use
        :type pool: str
        :param delay_on_limit_secs: Time in seconds to wait before next attempt to run
            dag run when max_active_runs limit has been reached
        :type delay_on_limit_secs: float
        :param verbose: Make logging output more verbose
        :type verbose: bool
        :param conf: user defined dictionary passed from CLI
        :type conf: dict
        :param rerun_failed_tasks:
        :type: bool
        :param run_backwards:
        :type: bool

        """
        from airflow.jobs.backfill_job import BackfillJob

        if not executor and local:
            from airflow.executors.local_executor import LocalExecutor

            executor = LocalExecutor()
        elif not executor:
            from airflow.executors.executor_loader import ExecutorLoader

            executor = ExecutorLoader.get_default_executor()
        job = BackfillJob(
            self,
            start_date=start_date,
            end_date=end_date,
            mark_success=mark_success,
            executor=executor,
            donot_pickle=donot_pickle,
            ignore_task_deps=ignore_task_deps,
            ignore_first_depends_on_past=ignore_first_depends_on_past,
            pool=pool,
            delay_on_limit_secs=delay_on_limit_secs,
            verbose=verbose,
            conf=conf,
            rerun_failed_tasks=rerun_failed_tasks,
            run_backwards=run_backwards,
        )
        job.run()

    def cli(self):
        """Exposes a CLI specific to this DAG"""
        check_cycle(self)

        from airflow.cli import cli_parser

        parser = cli_parser.get_parser(dag_parser=True)
        args = parser.parse_args()
        args.func(args, self)

    @provide_session
    def create_dagrun(
        self,
        state: DagRunState,
        execution_date: Optional[datetime] = None,
        run_id: Optional[str] = None,
        start_date: Optional[datetime] = None,
        external_trigger: Optional[bool] = False,
        conf: Optional[dict] = None,
        run_type: Optional[DagRunType] = None,
        session=None,
        dag_hash: Optional[str] = None,
        creating_job_id: Optional[int] = None,
        data_interval: Optional[Tuple[datetime, datetime]] = None,
    ):
        """
        Creates a dag run from this dag including the tasks associated with this dag.
        Returns the dag run.

        :param run_id: defines the run id for this dag run
        :type run_id: str
        :param run_type: type of DagRun
        :type run_type: airflow.utils.types.DagRunType
        :param execution_date: the execution date of this dag run
        :type execution_date: datetime.datetime
        :param state: the state of the dag run
        :type state: airflow.utils.state.DagRunState
        :param start_date: the date this dag run should be evaluated
        :type start_date: datetime
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param conf: Dict containing configuration/parameters to pass to the DAG
        :type conf: dict
        :param creating_job_id: id of the job creating this DagRun
        :type creating_job_id: int
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param dag_hash: Hash of Serialized DAG
        :type dag_hash: str
        :param data_interval: Data interval of the DagRun
        :type data_interval: tuple[datetime, datetime] | None
        """
        if run_id:  # Infer run_type from run_id if needed.
            if not isinstance(run_id, str):
                raise ValueError(f"`run_id` expected to be a str is {type(run_id)}")
            if not run_type:
                run_type = DagRunType.from_run_id(run_id)
        elif run_type and execution_date is not None:  # Generate run_id from run_type and execution_date.
            if not isinstance(run_type, DagRunType):
                raise ValueError(f"`run_type` expected to be a DagRunType is {type(run_type)}")
            run_id = DagRun.generate_run_id(run_type, execution_date)
        else:
            raise AirflowException(
                "Creating DagRun needs either `run_id` or both `run_type` and `execution_date`"
            )

        logical_date = timezone.coerce_datetime(execution_date)
        if data_interval is None and logical_date is not None:
            warnings.warn(
                "Calling `DAG.create_dagrun()` without an explicit data interval is deprecated",
                DeprecationWarning,
                stacklevel=3,
            )
            if run_type == DagRunType.MANUAL:
                data_interval = self.timetable.infer_manual_data_interval(run_after=logical_date)
            else:
                data_interval = self.infer_automated_data_interval(logical_date)

        # create a copy of params before validating
        copied_params = copy.deepcopy(self.params)
        copied_params.update(conf or {})
        copied_params.validate()

        run = DagRun(
            dag_id=self.dag_id,
            run_id=run_id,
            execution_date=logical_date,
            start_date=start_date,
            external_trigger=external_trigger,
            conf=conf,
            state=state,
            run_type=run_type,
            dag_hash=dag_hash,
            creating_job_id=creating_job_id,
            data_interval=data_interval,
        )
        session.add(run)
        session.flush()

        run.dag = self

        # create the associated task instances
        # state is None at the moment of creation
        run.verify_integrity(session=session)

        return run

    @classmethod
    @provide_session
    def bulk_sync_to_db(cls, dags: Collection["DAG"], session=None):
        """This method is deprecated in favor of bulk_write_to_db"""
        warnings.warn(
            "This method is deprecated and will be removed in a future version. Please use bulk_write_to_db",
            DeprecationWarning,
            stacklevel=2,
        )
        return cls.bulk_write_to_db(dags, session)

    @classmethod
    @provide_session
    def bulk_write_to_db(cls, dags: Collection["DAG"], session=None):
        """
        Ensure the DagModel rows for the given dags are up-to-date in the dag table in the DB, including
        calculated fields.

        Note that this method can be called for both DAGs and SubDAGs. A SubDag is actually a SubDagOperator.

        :param dags: the DAG objects to save to the DB
        :type dags: List[airflow.models.dag.DAG]
        :return: None
        """
        if not dags:
            return

        log.info("Sync %s DAGs", len(dags))
        dag_by_ids = {dag.dag_id: dag for dag in dags}
        dag_ids = set(dag_by_ids.keys())
        query = (
            session.query(DagModel)
            .options(joinedload(DagModel.tags, innerjoin=False))
            .filter(DagModel.dag_id.in_(dag_ids))
        )
        orm_dags: List[DagModel] = with_row_locks(query, of=DagModel, session=session).all()

        existing_dag_ids = {orm_dag.dag_id for orm_dag in orm_dags}
        missing_dag_ids = dag_ids.difference(existing_dag_ids)

        for missing_dag_id in missing_dag_ids:
            orm_dag = DagModel(dag_id=missing_dag_id)
            dag = dag_by_ids[missing_dag_id]
            if dag.is_paused_upon_creation is not None:
                orm_dag.is_paused = dag.is_paused_upon_creation
            orm_dag.tags = []
            log.info("Creating ORM DAG for %s", dag.dag_id)
            session.add(orm_dag)
            orm_dags.append(orm_dag)

        # Get the latest dag run for each existing dag as a single query (avoid n+1 query)
        most_recent_subq = (
            session.query(DagRun.dag_id, func.max(DagRun.execution_date).label("max_execution_date"))
            .filter(
                DagRun.dag_id.in_(existing_dag_ids),
                or_(DagRun.run_type == DagRunType.BACKFILL_JOB, DagRun.run_type == DagRunType.SCHEDULED),
            )
            .group_by(DagRun.dag_id)
            .subquery()
        )
        most_recent_runs_iter = session.query(DagRun).filter(
            DagRun.dag_id == most_recent_subq.c.dag_id,
            DagRun.execution_date == most_recent_subq.c.max_execution_date,
        )
        most_recent_runs = {run.dag_id: run for run in most_recent_runs_iter}

        filelocs = []

        for orm_dag in sorted(orm_dags, key=lambda d: d.dag_id):
            dag = dag_by_ids[orm_dag.dag_id]
            filelocs.append(dag.fileloc)
            if dag.is_subdag:
                orm_dag.is_subdag = True
                orm_dag.fileloc = dag.parent_dag.fileloc  # type: ignore
                orm_dag.root_dag_id = dag.parent_dag.dag_id  # type: ignore
                orm_dag.owners = dag.parent_dag.owner  # type: ignore
            else:
                orm_dag.is_subdag = False
                orm_dag.fileloc = dag.fileloc
                orm_dag.owners = dag.owner
            orm_dag.is_active = True
            orm_dag.last_parsed_time = timezone.utcnow()
            orm_dag.default_view = dag.default_view
            orm_dag.description = dag.description
            orm_dag.schedule_interval = dag.schedule_interval
            orm_dag.max_active_tasks = dag.max_active_tasks
            orm_dag.max_active_runs = dag.max_active_runs
            orm_dag.has_task_concurrency_limits = any(t.max_active_tis_per_dag is not None for t in dag.tasks)

            run: Optional[DagRun] = most_recent_runs.get(dag.dag_id)
            if run is None:
                data_interval = None
            else:
                data_interval = dag.get_run_data_interval(run)
            orm_dag.calculate_dagrun_date_fields(dag, data_interval)

            for orm_tag in list(orm_dag.tags):
                if orm_tag.name not in set(dag.tags):
                    session.delete(orm_tag)
                    orm_dag.tags.remove(orm_tag)
            if dag.tags:
                orm_tag_names = [t.name for t in orm_dag.tags]
                for dag_tag in set(dag.tags):
                    if dag_tag not in orm_tag_names:
                        dag_tag_orm = DagTag(name=dag_tag, dag_id=dag.dag_id)
                        orm_dag.tags.append(dag_tag_orm)
                        session.add(dag_tag_orm)

        DagCode.bulk_sync_to_db(filelocs, session=session)

        # Issue SQL/finish "Unit of Work", but let @provide_session commit (or if passed a session, let caller
        # decide when to commit
        session.flush()

        for dag in dags:
            cls.bulk_write_to_db(dag.subdags, session=session)

    @provide_session
    def sync_to_db(self, session=None):
        """
        Save attributes about this DAG to the DB. Note that this method
        can be called for both DAGs and SubDAGs. A SubDag is actually a
        SubDagOperator.

        :return: None
        """
        self.bulk_write_to_db([self], session)

    def get_default_view(self):
        """This is only there for backward compatible jinja2 templates"""
        if self.default_view is None:
            return conf.get('webserver', 'dag_default_view').lower()
        else:
            return self.default_view

    @staticmethod
    @provide_session
    def deactivate_unknown_dags(active_dag_ids, session=None):
        """
        Given a list of known DAGs, deactivate any other DAGs that are
        marked as active in the ORM

        :param active_dag_ids: list of DAG IDs that are active
        :type active_dag_ids: list[unicode]
        :return: None
        """
        if len(active_dag_ids) == 0:
            return
        for dag in session.query(DagModel).filter(~DagModel.dag_id.in_(active_dag_ids)).all():
            dag.is_active = False
            session.merge(dag)
        session.commit()

    @staticmethod
    @provide_session
    def deactivate_stale_dags(expiration_date, session=None):
        """
        Deactivate any DAGs that were last touched by the scheduler before
        the expiration date. These DAGs were likely deleted.

        :param expiration_date: set inactive DAGs that were touched before this
            time
        :type expiration_date: datetime
        :return: None
        """
        for dag in (
            session.query(DagModel)
            .filter(DagModel.last_parsed_time < expiration_date, DagModel.is_active)
            .all()
        ):
            log.info(
                "Deactivating DAG ID %s since it was last touched by the scheduler at %s",
                dag.dag_id,
                dag.last_parsed_time.isoformat(),
            )
            dag.is_active = False
            session.merge(dag)
            session.commit()

    @staticmethod
    @provide_session
    def get_num_task_instances(dag_id, task_ids=None, states=None, session=None):
        """
        Returns the number of task instances in the given DAG.

        :param session: ORM session
        :param dag_id: ID of the DAG to get the task concurrency of
        :type dag_id: unicode
        :param task_ids: A list of valid task IDs for the given DAG
        :type task_ids: list[unicode]
        :param states: A list of states to filter by if supplied
        :type states: list[state]
        :return: The number of running tasks
        :rtype: int
        """
        qry = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == dag_id,
        )
        if task_ids:
            qry = qry.filter(
                TaskInstance.task_id.in_(task_ids),
            )

        if states:
            if None in states:
                if all(x is None for x in states):
                    qry = qry.filter(TaskInstance.state.is_(None))
                else:
                    not_none_states = [state for state in states if state]
                    qry = qry.filter(
                        or_(TaskInstance.state.in_(not_none_states), TaskInstance.state.is_(None))
                    )
            else:
                qry = qry.filter(TaskInstance.state.in_(states))
        return qry.scalar()

    @classmethod
    def get_serialized_fields(cls):
        """Stringified DAGs and operators contain exactly these fields."""
        if not cls.__serialized_fields:
            cls.__serialized_fields = frozenset(vars(DAG(dag_id='test')).keys()) - {
                'parent_dag',
                '_old_context_manager_dags',
                'safe_dag_id',
                'last_loaded',
                'user_defined_filters',
                'user_defined_macros',
                'partial',
                'params',
                '_pickle_id',
                '_log',
                'is_subdag',
                'task_dict',
                'template_searchpath',
                'sla_miss_callback',
                'on_success_callback',
                'on_failure_callback',
                'template_undefined',
                'jinja_environment_kwargs',
                # has_on_*_callback are only stored if the value is True, as the default is False
                'has_on_success_callback',
                'has_on_failure_callback',
            }
        return cls.__serialized_fields

    def get_edge_info(self, upstream_task_id: str, downstream_task_id: str) -> EdgeInfoType:
        """
        Returns edge information for the given pair of tasks if present, and
        None if there is no information.
        """
        # Note - older serialized DAGs may not have edge_info being a dict at all
        if self.edge_info:
            return self.edge_info.get(upstream_task_id, {}).get(downstream_task_id, {})
        else:
            return {}

    def set_edge_info(self, upstream_task_id: str, downstream_task_id: str, info: EdgeInfoType):
        """
        Sets the given edge information on the DAG. Note that this will overwrite,
        rather than merge with, existing info.
        """
        self.edge_info.setdefault(upstream_task_id, {})[downstream_task_id] = info

    def validate_schedule_and_params(self):
        """
        Validates & raise exception if there are any Params in the DAG which neither have a default value nor
        have the null in schema['type'] list, but the DAG have a schedule_interval which is not None.
        """
        if not self.timetable.can_run:
            return

        for k, v in self.params.items():
            # As type can be an array, we would check if `null` is a allowed type or not
            if v.default is None and ("type" not in v.schema or "null" not in v.schema["type"]):
                raise AirflowException(
                    "DAG Schedule must be None, if there are any required params without default values"
                )


class DagTag(Base):
    """A tag name per dag, to allow quick filtering in the DAG view."""

    __tablename__ = "dag_tag"
    name = Column(String(100), primary_key=True)
    dag_id = Column(String(ID_LEN), ForeignKey('dag.dag_id'), primary_key=True)

    def __repr__(self):
        return self.name


class DagModel(Base):
    """Table containing DAG properties"""

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = Column(String(ID_LEN), primary_key=True)
    root_dag_id = Column(String(ID_LEN))
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused_at_creation = conf.getboolean('core', 'dags_are_paused_at_creation')
    is_paused = Column(Boolean, default=is_paused_at_creation)
    # Whether the DAG is a subdag
    is_subdag = Column(Boolean, default=False)
    # Whether that DAG was seen on the last DagBag load
    is_active = Column(Boolean, default=False)
    # Last time the scheduler started
    last_parsed_time = Column(UtcDateTime)
    # Last time this DAG was pickled
    last_pickled = Column(UtcDateTime)
    # Time when the DAG last received a refresh signal
    # (e.g. the DAG's "refresh" button was clicked in the web UI)
    last_expired = Column(UtcDateTime)
    # Whether (one  of) the scheduler is scheduling this DAG at the moment
    scheduler_lock = Column(Boolean)
    # Foreign key to the latest pickle_id
    pickle_id = Column(Integer)
    # The location of the file containing the DAG object
    # Note: Do not depend on fileloc pointing to a file; in the case of a
    # packaged DAG, it will point to the subpath of the DAG within the
    # associated zip.
    fileloc = Column(String(2000))
    # String representing the owners
    owners = Column(String(2000))
    # Description of the dag
    description = Column(Text)
    # Default view of the inside the webserver
    default_view = Column(String(25))
    # Schedule interval
    schedule_interval = Column(Interval)
    # Tags for view filter
    tags = relationship('DagTag', cascade='all,delete-orphan', backref=backref('dag'))

    max_active_tasks = Column(Integer, nullable=False)
    max_active_runs = Column(Integer, nullable=True)

    has_task_concurrency_limits = Column(Boolean, nullable=False)

    # The logical date of the next dag run.
    next_dagrun = Column(UtcDateTime)

    # Must be either both NULL or both datetime.
    next_dagrun_data_interval_start = Column(UtcDateTime)
    next_dagrun_data_interval_end = Column(UtcDateTime)

    # Earliest time at which this ``next_dagrun`` can be created.
    next_dagrun_create_after = Column(UtcDateTime)

    __table_args__ = (
        Index('idx_root_dag_id', root_dag_id, unique=False),
        Index('idx_next_dagrun_create_after', next_dagrun_create_after, unique=False),
    )

    parent_dag = relationship(
        "DagModel", remote_side=[dag_id], primaryjoin=root_dag_id == dag_id, foreign_keys=[root_dag_id]
    )

    NUM_DAGS_PER_DAGRUN_QUERY = conf.getint('scheduler', 'max_dagruns_to_create_per_loop', fallback=10)

    def __init__(self, concurrency=None, **kwargs):
        super().__init__(**kwargs)
        if self.max_active_tasks is None:
            if concurrency:
                warnings.warn(
                    "The 'DagModel.concurrency' parameter is deprecated. Please use 'max_active_tasks'.",
                    DeprecationWarning,
                    stacklevel=2,
                )
                self.max_active_tasks = concurrency
            else:
                self.max_active_tasks = conf.getint('core', 'max_active_tasks_per_dag')
        if self.max_active_runs is None:
            self.max_active_runs = conf.getint('core', 'max_active_runs_per_dag')
        if self.has_task_concurrency_limits is None:
            # Be safe -- this will be updated later once the DAG is parsed
            self.has_task_concurrency_limits = True

    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    @property
    def next_dagrun_data_interval(self) -> Optional[DataInterval]:
        return _get_model_data_interval(
            self,
            "next_dagrun_data_interval_start",
            "next_dagrun_data_interval_end",
        )

    @next_dagrun_data_interval.setter
    def next_dagrun_data_interval(self, value: Optional[Tuple[datetime, datetime]]) -> None:
        if value is None:
            self.next_dagrun_data_interval_start = self.next_dagrun_data_interval_end = None
        else:
            self.next_dagrun_data_interval_start, self.next_dagrun_data_interval_end = value

    @property
    def timezone(self):
        return settings.TIMEZONE

    @staticmethod
    @provide_session
    def get_dagmodel(dag_id, session=None):
        return session.query(DagModel).options(joinedload(DagModel.parent_dag)).get(dag_id)

    @classmethod
    @provide_session
    def get_current(cls, dag_id, session=None):
        return session.query(cls).filter(cls.dag_id == dag_id).first()

    @provide_session
    def get_last_dagrun(self, session=None, include_externally_triggered=False):
        return get_last_dagrun(
            self.dag_id, session=session, include_externally_triggered=include_externally_triggered
        )

    @staticmethod
    @provide_session
    def get_paused_dag_ids(dag_ids: List[str], session: Session = None) -> Set[str]:
        """
        Given a list of dag_ids, get a set of Paused Dag Ids

        :param dag_ids: List of Dag ids
        :param session: ORM Session
        :return: Paused Dag_ids
        """
        paused_dag_ids = (
            session.query(DagModel.dag_id)
            .filter(DagModel.is_paused == expression.true())
            .filter(DagModel.dag_id.in_(dag_ids))
            .all()
        )

        paused_dag_ids = {paused_dag_id for paused_dag_id, in paused_dag_ids}
        return paused_dag_ids

    def get_default_view(self) -> str:
        """
        Get the Default DAG View, returns the default config value if DagModel does not
        have a value
        """
        # This is for backwards-compatibility with old dags that don't have None as default_view
        return self.default_view or conf.get('webserver', 'dag_default_view').lower()

    @property
    def safe_dag_id(self):
        return self.dag_id.replace('.', '__dot__')

    @property
    def relative_fileloc(self) -> Optional[pathlib.Path]:
        """File location of the importable dag 'file' relative to the configured DAGs folder."""
        if self.fileloc is None:
            return None
        path = pathlib.Path(self.fileloc)
        try:
            return path.relative_to(settings.DAGS_FOLDER)
        except ValueError:
            # Not relative to DAGS_FOLDER.
            return path

    @provide_session
    def set_is_paused(self, is_paused: bool, including_subdags: bool = True, session=None) -> None:
        """
        Pause/Un-pause a DAG.

        :param is_paused: Is the DAG paused
        :param including_subdags: whether to include the DAG's subdags
        :param session: session
        """
        filter_query = [
            DagModel.dag_id == self.dag_id,
        ]
        if including_subdags:
            filter_query.append(DagModel.root_dag_id == self.dag_id)
        session.query(DagModel).filter(or_(*filter_query)).update(
            {DagModel.is_paused: is_paused}, synchronize_session='fetch'
        )
        session.commit()

    @classmethod
    @provide_session
    def deactivate_deleted_dags(cls, alive_dag_filelocs: List[str], session=None):
        """
        Set ``is_active=False`` on the DAGs for which the DAG files have been removed.

        :param alive_dag_filelocs: file paths of alive DAGs
        :param session: ORM Session
        """
        log.debug("Deactivating DAGs (for which DAG files are deleted) from %s table ", cls.__tablename__)

        dag_models = session.query(cls).all()
        for dag_model in dag_models:
            if dag_model.fileloc is not None:
                if correct_maybe_zipped(dag_model.fileloc) not in alive_dag_filelocs:
                    dag_model.is_active = False
            else:
                continue

    @classmethod
    def dags_needing_dagruns(cls, session: Session):
        """
        Return (and lock) a list of Dag objects that are due to create a new DagRun.

        This will return a resultset of rows  that is row-level-locked with a "SELECT ... FOR UPDATE" query,
        you should ensure that any scheduling decisions are made in a single transaction -- as soon as the
        transaction is committed it will be unlocked.
        """
        # TODO[HA]: Bake this query, it is run _A lot_
        # We limit so that _one_ scheduler doesn't try to do all the creation
        # of dag runs
        query = (
            session.query(cls)
            .filter(
                cls.is_paused == expression.false(),
                cls.is_active == expression.true(),
                cls.next_dagrun_create_after <= func.now(),
            )
            .order_by(cls.next_dagrun_create_after)
            .limit(cls.NUM_DAGS_PER_DAGRUN_QUERY)
        )

        return with_row_locks(query, of=cls, session=session, **skip_locked(session=session))

    def calculate_dagrun_date_fields(
        self,
        dag: DAG,
        most_recent_dag_run: Union[None, datetime, DataInterval],
    ) -> None:
        """
        Calculate ``next_dagrun`` and `next_dagrun_create_after``

        :param dag: The DAG object
        :param most_recent_dag_run: DateTime of most recent run of this dag, or none if not yet scheduled.
        """
        if isinstance(most_recent_dag_run, datetime):
            warnings.warn(
                "Passing a datetime to `DagModel.calculate_dagrun_date_fields` is deprecated. "
                "Provide a data interval instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            most_recent_data_interval = dag.infer_automated_data_interval(most_recent_dag_run)
        else:
            most_recent_data_interval = most_recent_dag_run
        next_dagrun_info = dag.next_dagrun_info(most_recent_data_interval)
        if next_dagrun_info is None:
            self.next_dagrun_data_interval = self.next_dagrun = self.next_dagrun_create_after = None
        else:
            self.next_dagrun_data_interval = next_dagrun_info.data_interval
            self.next_dagrun = next_dagrun_info.logical_date
            self.next_dagrun_create_after = next_dagrun_info.run_after

        log.info("Setting next_dagrun for %s to %s", dag.dag_id, self.next_dagrun)


def dag(*dag_args, **dag_kwargs):
    """
    Python dag decorator. Wraps a function into an Airflow DAG.
    Accepts kwargs for operator kwarg. Can be used to parametrize DAGs.

    :param dag_args: Arguments for DAG object
    :type dag_args: Any
    :param dag_kwargs: Kwargs for DAG object.
    :type dag_kwargs: Any
    """

    def wrapper(f: Callable):
        # Get dag initializer signature and bind it to validate that dag_args, and dag_kwargs are correct
        dag_sig = signature(DAG.__init__)
        dag_bound_args = dag_sig.bind_partial(*dag_args, **dag_kwargs)

        @functools.wraps(f)
        def factory(*args, **kwargs):
            # Generate signature for decorated function and bind the arguments when called
            # we do this to extract parameters so we can annotate them on the DAG object.
            # In addition, this fails if we are missing any args/kwargs with TypeError as expected.
            f_sig = signature(f).bind(*args, **kwargs)
            # Apply defaults to capture default values if set.
            f_sig.apply_defaults()

            # Set function name as dag_id if not set
            dag_id = dag_bound_args.arguments.get('dag_id', f.__name__)
            dag_bound_args.arguments['dag_id'] = dag_id

            # Initialize DAG with bound arguments
            with DAG(*dag_bound_args.args, **dag_bound_args.kwargs) as dag_obj:
                # Set DAG documentation from function documentation.
                if f.__doc__:
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

        return factory

    return wrapper


STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover

    from airflow.models.serialized_dag import SerializedDagModel

    DagModel.serialized_dag = relationship(SerializedDagModel)


class DagContext:
    """
    DAG context is used to keep the current DAG when DAG is used as ContextManager.

    You can use DAG as context:

    .. code-block:: python

        with DAG(
            dag_id="example_dag",
            default_args=default_args,
            schedule_interval="0 0 * * *",
            dagrun_timeout=timedelta(minutes=60),
        ) as dag:
            ...

    If you do this the context stores the DAG and whenever new task is created, it will use
    such stored DAG as the parent DAG.

    """

    _context_managed_dag: Optional[DAG] = None
    _previous_context_managed_dags: List[DAG] = []

    @classmethod
    def push_context_managed_dag(cls, dag: DAG):
        if cls._context_managed_dag:
            cls._previous_context_managed_dags.append(cls._context_managed_dag)
        cls._context_managed_dag = dag

    @classmethod
    def pop_context_managed_dag(cls) -> Optional[DAG]:
        old_dag = cls._context_managed_dag
        if cls._previous_context_managed_dags:
            cls._context_managed_dag = cls._previous_context_managed_dags.pop()
        else:
            cls._context_managed_dag = None
        return old_dag

    @classmethod
    def get_current_dag(cls) -> Optional[DAG]:
        return cls._context_managed_dag
