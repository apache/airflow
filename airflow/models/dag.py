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

import collections
import collections.abc
import copy
import functools
import itertools
import logging
import os
import pathlib
import pickle
import sys
import traceback
import warnings
import weakref
from collections import deque
from datetime import datetime, timedelta
from inspect import signature
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    Container,
    Iterable,
    Iterator,
    List,
    Pattern,
    Sequence,
    Union,
    cast,
    overload,
)
from urllib.parse import urlsplit

import jinja2
import pendulum
import re2
from dateutil.relativedelta import relativedelta
from pendulum.tz.timezone import Timezone
from sqlalchemy import (
    Boolean,
    Column,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    and_,
    case,
    func,
    not_,
    or_,
    select,
    update,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import backref, joinedload, relationship
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import Session
from sqlalchemy.sql import Select, expression

import airflow.templates
from airflow import settings, utils
from airflow.api_internal.internal_api_call import internal_api_call
from airflow.configuration import conf as airflow_conf, secrets_backend_list
from airflow.exceptions import (
    AirflowDagInconsistent,
    AirflowException,
    AirflowSkipException,
    DuplicateTaskIdFound,
    FailStopDagInvalidTriggerRule,
    ParamValidationError,
    RemovedInAirflow3Warning,
    TaskNotFound,
)
from airflow.jobs.job import run_job
from airflow.models.abstractoperator import AbstractOperator
from airflow.models.base import Base, StringID
from airflow.models.baseoperator import BaseOperator
from airflow.models.dagcode import DagCode
from airflow.models.dagpickle import DagPickle
from airflow.models.dagrun import RUN_ID_REGEX, DagRun
from airflow.models.operator import Operator
from airflow.models.param import DagParam, ParamsDict
from airflow.models.taskinstance import (
    Context,
    TaskInstance,
    TaskInstanceKey,
    TaskReturnCode,
    clear_task_instances,
)
from airflow.secrets.local_filesystem import LocalFilesystemBackend
from airflow.security import permissions
from airflow.stats import Stats
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
from airflow.timetables.simple import (
    ContinuousTimetable,
    DatasetTriggeredTimetable,
    NullTimetable,
    OnceTimetable,
)
from airflow.typing_compat import Literal
from airflow.utils import timezone
from airflow.utils.dag_cycle_tester import check_cycle
from airflow.utils.dates import cron_presets, date_range as utils_date_range
from airflow.utils.decorators import fixup_decorator_warning_stack
from airflow.utils.helpers import at_most_one, exactly_one, validate_key
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import (
    Interval,
    UtcDateTime,
    lock_rows,
    skip_locked,
    tuple_in_condition,
    with_row_locks,
)
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.types import NOTSET, ArgNotSet, DagRunType, EdgeInfoType

if TYPE_CHECKING:
    from types import ModuleType

    from airflow.datasets import Dataset
    from airflow.decorators import TaskDecoratorCollection
    from airflow.models.dagbag import DagBag
    from airflow.models.slamiss import SlaMiss
    from airflow.utils.task_group import TaskGroup

log = logging.getLogger(__name__)

DEFAULT_VIEW_PRESETS = ["grid", "graph", "duration", "gantt", "landing_times"]
ORIENTATION_PRESETS = ["LR", "TB", "RL", "BT"]

TAG_MAX_LEN = 100

DagStateChangeCallback = Callable[[Context], None]
ScheduleInterval = Union[None, str, timedelta, relativedelta]

# FIXME: Ideally this should be Union[Literal[NOTSET], ScheduleInterval],
# but Mypy cannot handle that right now. Track progress of PEP 661 for progress.
# See also: https://discuss.python.org/t/9126/7
ScheduleIntervalArg = Union[ArgNotSet, ScheduleInterval]
ScheduleArg = Union[ArgNotSet, ScheduleInterval, Timetable, Collection["Dataset"]]

SLAMissCallback = Callable[["DAG", str, str, List["SlaMiss"], List[TaskInstance]], None]

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
) -> DataInterval | None:
    start = timezone.coerce_datetime(getattr(instance, start_field_name))
    end = timezone.coerce_datetime(getattr(instance, end_field_name))
    if start is None:
        if end is not None:
            raise InconsistentDataInterval(instance, start_field_name, end_field_name)
        return None
    elif end is None:
        raise InconsistentDataInterval(instance, start_field_name, end_field_name)
    return DataInterval(start, end)


def create_timetable(interval: ScheduleIntervalArg, timezone: Timezone) -> Timetable:
    """Create a Timetable instance from a ``schedule_interval`` argument."""
    if interval is NOTSET:
        return DeltaDataIntervalTimetable(DEFAULT_SCHEDULE_INTERVAL)
    if interval is None:
        return NullTimetable()
    if interval == "@once":
        return OnceTimetable()
    if interval == "@continuous":
        return ContinuousTimetable()
    if isinstance(interval, (timedelta, relativedelta)):
        return DeltaDataIntervalTimetable(interval)
    if isinstance(interval, str):
        return CronDataIntervalTimetable(interval, timezone)
    raise ValueError(f"{interval!r} is not a valid schedule_interval.")


def get_last_dagrun(dag_id, session, include_externally_triggered=False):
    """
    Return the last dag run for a dag, None if there was none.

    Last dag run can be any type of run e.g. scheduled or backfilled.
    Overridden DagRuns are ignored.
    """
    DR = DagRun
    query = select(DR).where(DR.dag_id == dag_id)
    if not include_externally_triggered:
        query = query.where(DR.external_trigger == expression.false())
    query = query.order_by(DR.execution_date.desc())
    return session.scalar(query.limit(1))


def get_dataset_triggered_next_run_info(
    dag_ids: list[str], *, session: Session
) -> dict[str, dict[str, int | str]]:
    """
    Get next run info for a list of dag_ids.

    Given a list of dag_ids, get string representing how close any that are dataset triggered are
    their next run, e.g. "1 of 2 datasets updated".
    """
    from airflow.models.dataset import DagScheduleDatasetReference, DatasetDagRunQueue as DDRQ, DatasetModel

    return {
        x.dag_id: {
            "uri": x.uri,
            "ready": x.ready,
            "total": x.total,
        }
        for x in session.execute(
            select(
                DagScheduleDatasetReference.dag_id,
                # This is a dirty hack to workaround group by requiring an aggregate,
                # since grouping by dataset is not what we want to do here...but it works
                case((func.count() == 1, func.max(DatasetModel.uri)), else_="").label("uri"),
                func.count().label("total"),
                func.sum(case((DDRQ.target_dag_id.is_not(None), 1), else_=0)).label("ready"),
            )
            .join(
                DDRQ,
                and_(
                    DDRQ.dataset_id == DagScheduleDatasetReference.dataset_id,
                    DDRQ.target_dag_id == DagScheduleDatasetReference.dag_id,
                ),
                isouter=True,
            )
            .join(DatasetModel, DatasetModel.id == DagScheduleDatasetReference.dataset_id)
            .group_by(DagScheduleDatasetReference.dag_id)
            .where(DagScheduleDatasetReference.dag_id.in_(dag_ids))
        ).all()
    }


class _StopDagTest(Exception):
    """
    Raise when DAG.test should stop immediately.

    :meta private:
    """


@functools.total_ordering
class DAG(LoggingMixin):
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

    .. deprecated:: 2.4
        The arguments *schedule_interval* and *timetable*. Their functionalities
        are merged into the new *schedule* argument.

    :param dag_id: The id of the DAG; must consist exclusively of alphanumeric
        characters, dashes, dots and underscores (all ASCII)
    :param description: The description for the DAG to e.g. be shown on the webserver
    :param schedule: Defines the rules according to which DAG runs are scheduled. Can
        accept cron string, timedelta object, Timetable, or list of Dataset objects.
        If this is not provided, the DAG will be set to the default
        schedule ``timedelta(days=1)``. See also :doc:`/howto/timetable`.
    :param start_date: The timestamp from which the scheduler will
        attempt to backfill
    :param end_date: A date beyond which your DAG won't run, leave to None
        for open-ended scheduling
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
    :param dagrun_timeout: specify how long a DagRun should be up before
        timing out / failing, so that new DagRuns can be created.
    :param sla_miss_callback: specify a function or list of functions to call when reporting SLA
        timeouts. See :ref:`sla_miss_callback<concepts:sla_miss_callback>` for
        more information about the function signature and parameters that are
        passed to the callback.
    :param default_view: Specify DAG default view (grid, graph, duration,
                                                   gantt, landing_times), default grid
    :param orientation: Specify DAG orientation in graph view (LR, TB, RL, BT), default LR
    :param catchup: Perform scheduler catchup (or only run latest)? Defaults to True
    :param on_failure_callback: A function or list of functions to be called when a DagRun of this dag fails.
        A context dictionary is passed as a single parameter to this function.
    :param on_success_callback: Much like the ``on_failure_callback`` except
        that it is executed when the dag succeeds.
    :param access_control: Specify optional DAG-level actions, e.g.,
        "{'role1': {'can_read'}, 'role2': {'can_read', 'can_edit', 'can_delete'}}"
    :param is_paused_upon_creation: Specifies if the dag is paused when created for the first time.
        If the dag exists already, this flag will be ignored. If this optional parameter
        is not specified, the global config setting will be used.
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
    """

    _comps = {
        "dag_id",
        "task_ids",
        "parent_dag",
        "start_date",
        "end_date",
        "schedule_interval",
        "fileloc",
        "template_searchpath",
        "last_loaded",
    }

    __serialized_fields: frozenset[str] | None = None

    fileloc: str
    """
    File path that needs to be imported to load this DAG or subdag.

    This may not be an actual file on disk in the case when this DAG is loaded
    from a ZIP file or other DAG distribution format.
    """

    parent_dag: DAG | None = None  # Gets set when DAGs are loaded

    # NOTE: When updating arguments here, please also keep arguments in @dag()
    # below in sync. (Search for 'def dag(' in this file.)
    def __init__(
        self,
        dag_id: str,
        description: str | None = None,
        schedule: ScheduleArg = NOTSET,
        schedule_interval: ScheduleIntervalArg = NOTSET,
        timetable: Timetable | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        full_filepath: str | None = None,
        template_searchpath: str | Iterable[str] | None = None,
        template_undefined: type[jinja2.StrictUndefined] = jinja2.StrictUndefined,
        user_defined_macros: dict | None = None,
        user_defined_filters: dict | None = None,
        default_args: dict | None = None,
        concurrency: int | None = None,
        max_active_tasks: int = airflow_conf.getint("core", "max_active_tasks_per_dag"),
        max_active_runs: int = airflow_conf.getint("core", "max_active_runs_per_dag"),
        dagrun_timeout: timedelta | None = None,
        sla_miss_callback: None | SLAMissCallback | list[SLAMissCallback] = None,
        default_view: str = airflow_conf.get_mandatory_value("webserver", "dag_default_view").lower(),
        orientation: str = airflow_conf.get_mandatory_value("webserver", "dag_orientation"),
        catchup: bool = airflow_conf.getboolean("scheduler", "catchup_by_default"),
        on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None,
        on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None,
        doc_md: str | None = None,
        params: collections.abc.MutableMapping | None = None,
        access_control: dict | None = None,
        is_paused_upon_creation: bool | None = None,
        jinja_environment_kwargs: dict | None = None,
        render_template_as_native_obj: bool = False,
        tags: list[str] | None = None,
        owner_links: dict[str, str] | None = None,
        auto_register: bool = True,
        fail_stop: bool = False,
    ):
        from airflow.utils.task_group import TaskGroup

        if tags and any(len(tag) > TAG_MAX_LEN for tag in tags):
            raise AirflowException(f"tag cannot be longer than {TAG_MAX_LEN} characters")

        self.owner_links = owner_links if owner_links else {}
        self.user_defined_macros = user_defined_macros
        self.user_defined_filters = user_defined_filters
        if default_args and not isinstance(default_args, dict):
            raise TypeError("default_args must be a dict")
        self.default_args = copy.deepcopy(default_args or {})
        params = params or {}

        # merging potentially conflicting default_args['params'] into params
        if "params" in self.default_args:
            params.update(self.default_args["params"])
            del self.default_args["params"]

        # check self.params and convert them into ParamsDict
        self.params = ParamsDict(params)

        if full_filepath:
            warnings.warn(
                "Passing full_filepath to DAG() is deprecated and has no effect",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )

        validate_key(dag_id)

        self._dag_id = dag_id
        if concurrency:
            # TODO: Remove in Airflow 3.0
            warnings.warn(
                "The 'concurrency' parameter is deprecated. Please use 'max_active_tasks'.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            max_active_tasks = concurrency
        self._max_active_tasks = max_active_tasks
        self._pickle_id: int | None = None

        self._description = description
        # set file location to caller source path
        back = sys._getframe().f_back
        self.fileloc = back.f_code.co_filename if back else ""
        self.task_dict: dict[str, Operator] = {}

        # set timezone from start_date
        tz = None
        if start_date and start_date.tzinfo:
            tzinfo = None if start_date.tzinfo else settings.TIMEZONE
            tz = pendulum.instance(start_date, tz=tzinfo).timezone
        elif "start_date" in self.default_args and self.default_args["start_date"]:
            date = self.default_args["start_date"]
            if not isinstance(date, datetime):
                date = timezone.parse(date)
                self.default_args["start_date"] = date
                start_date = date

            tzinfo = None if date.tzinfo else settings.TIMEZONE
            tz = pendulum.instance(date, tz=tzinfo).timezone
        self.timezone: Timezone = tz or settings.TIMEZONE

        # Apply the timezone we settled on to end_date if it wasn't supplied
        if "end_date" in self.default_args and self.default_args["end_date"]:
            if isinstance(self.default_args["end_date"], str):
                self.default_args["end_date"] = timezone.parse(
                    self.default_args["end_date"], timezone=self.timezone
                )

        self.start_date = timezone.convert_to_utc(start_date)
        self.end_date = timezone.convert_to_utc(end_date)

        # also convert tasks
        if "start_date" in self.default_args:
            self.default_args["start_date"] = timezone.convert_to_utc(self.default_args["start_date"])
        if "end_date" in self.default_args:
            self.default_args["end_date"] = timezone.convert_to_utc(self.default_args["end_date"])

        # sort out DAG's scheduling behavior
        scheduling_args = [schedule_interval, timetable, schedule]
        if not at_most_one(*scheduling_args):
            raise ValueError("At most one allowed for args 'schedule_interval', 'timetable', and 'schedule'.")
        if schedule_interval is not NOTSET:
            warnings.warn(
                "Param `schedule_interval` is deprecated and will be removed in a future release. "
                "Please use `schedule` instead. ",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
        if timetable is not None:
            warnings.warn(
                "Param `timetable` is deprecated and will be removed in a future release. "
                "Please use `schedule` instead. ",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )

        self.timetable: Timetable
        self.schedule_interval: ScheduleInterval
        self.dataset_triggers: Collection[Dataset] = []

        if isinstance(schedule, Collection) and not isinstance(schedule, str):
            from airflow.datasets import Dataset

            if not all(isinstance(x, Dataset) for x in schedule):
                raise ValueError("All elements in 'schedule' should be datasets")
            self.dataset_triggers = list(schedule)
        elif isinstance(schedule, Timetable):
            timetable = schedule
        elif schedule is not NOTSET:
            schedule_interval = schedule

        if self.dataset_triggers:
            self.timetable = DatasetTriggeredTimetable()
            self.schedule_interval = self.timetable.summary
        elif timetable:
            self.timetable = timetable
            self.schedule_interval = self.timetable.summary
        else:
            if isinstance(schedule_interval, ArgNotSet):
                schedule_interval = DEFAULT_SCHEDULE_INTERVAL
            self.schedule_interval = schedule_interval
            self.timetable = create_timetable(schedule_interval, self.timezone)

        if isinstance(template_searchpath, str):
            template_searchpath = [template_searchpath]
        self.template_searchpath = template_searchpath
        self.template_undefined = template_undefined
        self.last_loaded: datetime = timezone.utcnow()
        self.safe_dag_id = dag_id.replace(".", "__dot__")
        self.max_active_runs = max_active_runs
        if self.timetable.active_runs_limit is not None:
            if self.timetable.active_runs_limit < self.max_active_runs:
                raise AirflowException(
                    f"Invalid max_active_runs: {type(self.timetable)} "
                    f"requires max_active_runs <= {self.timetable.active_runs_limit}"
                )
        self.dagrun_timeout = dagrun_timeout
        self.sla_miss_callback = sla_miss_callback
        if default_view in DEFAULT_VIEW_PRESETS:
            self._default_view: str = default_view
        elif default_view == "tree":
            warnings.warn(
                "`default_view` of 'tree' has been renamed to 'grid' -- please update your DAG",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            self._default_view = "grid"
        else:
            raise AirflowException(
                f"Invalid values of dag.default_view: only support "
                f"{DEFAULT_VIEW_PRESETS}, but get {default_view}"
            )
        if orientation in ORIENTATION_PRESETS:
            self.orientation = orientation
        else:
            raise AirflowException(
                f"Invalid values of dag.orientation: only support "
                f"{ORIENTATION_PRESETS}, but get {orientation}"
            )
        self.catchup: bool = catchup

        self.partial: bool = False
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback

        # Keeps track of any extra edge metadata (sparse; will not contain all
        # edges, so do not iterate over it for that). Outer key is upstream
        # task ID, inner key is downstream task ID.
        self.edge_info: dict[str, dict[str, EdgeInfoType]] = {}

        # To keep it in parity with Serialized DAGs
        # and identify if DAG has on_*_callback without actually storing them in Serialized JSON
        self.has_on_success_callback: bool = self.on_success_callback is not None
        self.has_on_failure_callback: bool = self.on_failure_callback is not None

        self._access_control = DAG._upgrade_outdated_dag_access_control(access_control)
        self.is_paused_upon_creation = is_paused_upon_creation
        self.auto_register = auto_register

        self.fail_stop: bool = fail_stop

        self.jinja_environment_kwargs = jinja_environment_kwargs
        self.render_template_as_native_obj = render_template_as_native_obj

        self.doc_md = self.get_doc_md(doc_md)

        self.tags = tags or []
        self._task_group = TaskGroup.create_root(self)
        self.validate_schedule_and_params()
        wrong_links = dict(self.iter_invalid_owner_links())
        if wrong_links:
            raise AirflowException(
                "Wrong link format was used for the owner. Use a valid link \n"
                f"Bad formatted links are: {wrong_links}"
            )

        # this will only be set at serialization time
        # it's only use is for determining the relative
        # fileloc based only on the serialize dag
        self._processor_dags_folder = None

    def get_doc_md(self, doc_md: str | None) -> str | None:
        if doc_md is None:
            return doc_md

        env = self.get_template_env(force_sandboxed=True)

        if not doc_md.endswith(".md"):
            template = jinja2.Template(doc_md)
        else:
            try:
                template = env.get_template(doc_md)
            except jinja2.exceptions.TemplateNotFound:
                return f"""
                # Templating Error!
                Not able to find the template file: `{doc_md}`.
                """

        return template.render()

    def _check_schedule_interval_matches_timetable(self) -> bool:
        """Check ``schedule_interval`` and ``timetable`` match.

        This is done as a part of the DAG validation done before it's bagged, to
        guard against the DAG's ``timetable`` (or ``schedule_interval``) from
        being changed after it's created, e.g.

        .. code-block:: python

            dag1 = DAG("d1", timetable=MyTimetable())
            dag1.schedule_interval = "@once"

            dag2 = DAG("d2", schedule="@once")
            dag2.timetable = MyTimetable()

        Validation is done by creating a timetable and check its summary matches
        ``schedule_interval``. The logic is not bullet-proof, especially if a
        custom timetable does not provide a useful ``summary``. But this is the
        best we can do.
        """
        if self.schedule_interval == self.timetable.summary:
            return True
        try:
            timetable = create_timetable(self.schedule_interval, self.timezone)
        except ValueError:
            return False
        return timetable.summary == self.timetable.summary

    def validate(self):
        """Validate the DAG has a coherent setup.

        This is called by the DAG bag before bagging the DAG.
        """
        if not self._check_schedule_interval_matches_timetable():
            raise AirflowDagInconsistent(
                f"inconsistent schedule: timetable {self.timetable.summary!r} "
                f"does not match schedule_interval {self.schedule_interval!r}",
            )
        self.params.validate()
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
            FailStopDagInvalidTriggerRule.check(dag=self, trigger_rule=task.trigger_rule)

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
        Look for outdated dag level actions in DAG access_controls and replace them with updated actions.

        For example, in DAG access_control {'role1': {'can_dag_read'}} 'can_dag_read'
        will be replaced with 'can_read', in {'role2': {'can_dag_read', 'can_dag_edit'}}
        'can_dag_edit' will be replaced with 'can_edit', etc.
        """
        if access_control is None:
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
                RemovedInAirflow3Warning,
                stacklevel=3,
            )

        return updated_access_control

    def date_range(
        self,
        start_date: pendulum.DateTime,
        num: int | None = None,
        end_date: datetime | None = None,
    ) -> list[datetime]:
        message = "`DAG.date_range()` is deprecated."
        if num is not None:
            warnings.warn(message, category=RemovedInAirflow3Warning, stacklevel=2)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RemovedInAirflow3Warning)
                return utils_date_range(
                    start_date=start_date, num=num, delta=self.normalized_schedule_interval
                )
        message += " Please use `DAG.iter_dagrun_infos_between(..., align=False)` instead."
        warnings.warn(message, category=RemovedInAirflow3Warning, stacklevel=2)
        if end_date is None:
            coerced_end_date = timezone.utcnow()
        else:
            coerced_end_date = end_date
        it = self.iter_dagrun_infos_between(start_date, pendulum.instance(coerced_end_date), align=False)
        return [info.logical_date for info in it]

    def is_fixed_time_schedule(self):
        warnings.warn(
            "`DAG.is_fixed_time_schedule()` is deprecated.",
            category=RemovedInAirflow3Warning,
            stacklevel=2,
        )
        try:
            return not self.timetable._should_fix_dst
        except AttributeError:
            return True

    def following_schedule(self, dttm):
        """
        Calculate the following schedule for this dag in UTC.

        :param dttm: utc datetime
        :return: utc datetime
        """
        warnings.warn(
            "`DAG.following_schedule()` is deprecated. Use `DAG.next_dagrun_info(restricted=False)` instead.",
            category=RemovedInAirflow3Warning,
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
            category=RemovedInAirflow3Warning,
            stacklevel=2,
        )
        if not isinstance(self.timetable, _DataIntervalTimetable):
            return None
        return self.timetable._get_prev(timezone.coerce_datetime(dttm))

    def get_next_data_interval(self, dag_model: DagModel) -> DataInterval | None:
        """Get the data interval of the next scheduled run.

        For compatibility, this method infers the data interval from the DAG's
        schedule if the run does not have an explicit one set, which is possible
        for runs created prior to AIP-39.

        This function is private to Airflow core and should not be depended on as a
        part of the Python API.

        :meta private:
        """
        if self.dag_id != dag_model.dag_id:
            raise ValueError(f"Arguments refer to different DAGs: {self.dag_id} != {dag_model.dag_id}")
        if dag_model.next_dagrun is None:  # Next run not scheduled.
            return None
        data_interval = dag_model.next_dagrun_data_interval
        if data_interval is not None:
            return data_interval

        # Compatibility: A run was scheduled without an explicit data interval.
        # This means the run was scheduled before AIP-39 implementation. Try to
        # infer from the logical date.
        return self.infer_automated_data_interval(dag_model.next_dagrun)

    def get_run_data_interval(self, run: DagRun) -> DataInterval:
        """Get the data interval of this run.

        For compatibility, this method infers the data interval from the DAG's
        schedule if the run does not have an explicit one set, which is possible for
        runs created prior to AIP-39.

        This function is private to Airflow core and should not be depended on as a
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

        DO NOT call this method if there is a known data interval.

        :meta private:
        """
        timetable_type = type(self.timetable)
        if issubclass(timetable_type, (NullTimetable, OnceTimetable, DatasetTriggeredTimetable)):
            return DataInterval.exact(timezone.coerce_datetime(logical_date))
        start = timezone.coerce_datetime(logical_date)
        if issubclass(timetable_type, CronDataIntervalTimetable):
            end = cast(CronDataIntervalTimetable, self.timetable)._get_next(start)
        elif issubclass(timetable_type, DeltaDataIntervalTimetable):
            end = cast(DeltaDataIntervalTimetable, self.timetable)._get_next(start)
        # Contributors: When the exception below is raised, you might want to
        # add an 'elif' block here to handle custom timetables. Stop! The bug
        # you're looking for is instead at when the DAG run (represented by
        # logical_date) was created. See GH-31969 for an example:
        # * Wrong fix: GH-32074 (modifies this function).
        # * Correct fix: GH-32118 (modifies the DAG run creation code).
        else:
            raise ValueError(f"Not a valid timetable: {self.timetable!r}")
        return DataInterval(start, end)

    def next_dagrun_info(
        self,
        last_automated_dagrun: None | datetime | DataInterval,
        *,
        restricted: bool = True,
    ) -> DagRunInfo | None:
        """Get information about the next DagRun of this dag after ``date_last_automated_dagrun``.

        This calculates what time interval the next DagRun should operate on
        (its execution date) and when it can be scheduled, according to the
        dag's timetable, start_date, end_date, etc. This doesn't check max
        active run or any other "max_active_tasks" type limits, but only
        performs calculations based on the various date and interval fields of
        this dag and its tasks.

        :param last_automated_dagrun: The ``max(execution_date)`` of
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

        data_interval = None
        if isinstance(last_automated_dagrun, datetime):
            warnings.warn(
                "Passing a datetime to DAG.next_dagrun_info is deprecated. Use a DataInterval instead.",
                RemovedInAirflow3Warning,
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
        try:
            info = self.timetable.next_dagrun_info(
                last_automated_data_interval=data_interval,
                restriction=restriction,
            )
        except Exception:
            self.log.exception(
                "Failed to fetch run info after data interval %s for DAG %r",
                data_interval,
                self.dag_id,
            )
            info = None
        return info

    def next_dagrun_after_date(self, date_last_automated_dagrun: pendulum.DateTime | None):
        warnings.warn(
            "`DAG.next_dagrun_after_date()` is deprecated. Please use `DAG.next_dagrun_info()` instead.",
            category=RemovedInAirflow3Warning,
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

    @functools.cached_property
    def _time_restriction(self) -> TimeRestriction:
        start_dates = [t.start_date for t in self.tasks if t.start_date]
        if self.start_date is not None:
            start_dates.append(self.start_date)
        earliest = None
        if start_dates:
            earliest = timezone.coerce_datetime(min(start_dates))
        latest = self.end_date
        end_dates = [t.end_date for t in self.tasks if t.end_date]
        if len(end_dates) == len(self.tasks):  # not exists null end_date
            if self.end_date is not None:
                end_dates.append(self.end_date)
            if end_dates:
                latest = timezone.coerce_datetime(max(end_dates))
        return TimeRestriction(earliest, latest, self.catchup)

    def iter_dagrun_infos_between(
        self,
        earliest: pendulum.DateTime | None,
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
        if earliest is None:
            raise ValueError("earliest was None and we had no value in time_restriction to fallback on")
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

        try:
            info = self.timetable.next_dagrun_info(
                last_automated_data_interval=None,
                restriction=restriction,
            )
        except Exception:
            self.log.exception(
                "Failed to fetch run info after data interval %s for DAG %r",
                None,
                self.dag_id,
            )
            info = None

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
            try:
                info = self.timetable.next_dagrun_info(
                    last_automated_data_interval=info.data_interval,
                    restriction=restriction,
                )
            except Exception:
                self.log.exception(
                    "Failed to fetch run info after data interval %s for DAG %r",
                    info.data_interval if info else "<NONE>",
                    self.dag_id,
                )
                break

    def get_run_dates(self, start_date, end_date=None) -> list:
        """
        Return a list of dates between the interval received as parameter using this dag's schedule interval.

        Returned dates can be used for execution dates.

        :param start_date: The start date of the interval.
        :param end_date: The end date of the interval. Defaults to ``timezone.utcnow()``.
        :return: A list of dates within the interval following the dag's schedule.
        """
        warnings.warn(
            "`DAG.get_run_dates()` is deprecated. Please use `DAG.iter_dagrun_infos_between()` instead.",
            category=RemovedInAirflow3Warning,
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
            category=RemovedInAirflow3Warning,
            stacklevel=2,
        )
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RemovedInAirflow3Warning)
            following = self.following_schedule(dttm)
        if not following:  # in case of @once
            return dttm
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RemovedInAirflow3Warning)
            previous_of_following = self.previous_schedule(following)
        if previous_of_following != dttm:
            return following
        return dttm

    @provide_session
    def get_last_dagrun(self, session=NEW_SESSION, include_externally_triggered=False):
        return get_last_dagrun(
            self.dag_id, session=session, include_externally_triggered=include_externally_triggered
        )

    @provide_session
    def has_dag_runs(self, session=NEW_SESSION, include_externally_triggered=True) -> bool:
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
    def is_subdag(self) -> bool:
        return self.parent_dag is not None

    @property
    def full_filepath(self) -> str:
        """Full file path to the DAG.

        :meta private:
        """
        warnings.warn(
            "DAG.full_filepath is deprecated in favour of fileloc",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return self.fileloc

    @full_filepath.setter
    def full_filepath(self, value) -> None:
        warnings.warn(
            "DAG.full_filepath is deprecated in favour of fileloc",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        self.fileloc = value

    @property
    def concurrency(self) -> int:
        # TODO: Remove in Airflow 3.0
        warnings.warn(
            "The 'DAG.concurrency' attribute is deprecated. Please use 'DAG.max_active_tasks'.",
            RemovedInAirflow3Warning,
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
    def description(self) -> str | None:
        return self._description

    @property
    def default_view(self) -> str:
        return self._default_view

    @property
    def pickle_id(self) -> int | None:
        return self._pickle_id

    @pickle_id.setter
    def pickle_id(self, value: int) -> None:
        self._pickle_id = value

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
    def task_group(self) -> TaskGroup:
        return self._task_group

    @property
    def filepath(self) -> str:
        """Relative file path to the DAG.

        :meta private:
        """
        warnings.warn(
            "filepath is deprecated, use relative_fileloc instead",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return str(self.relative_fileloc)

    @property
    def relative_fileloc(self) -> pathlib.Path:
        """File location of the importable dag 'file' relative to the configured DAGs folder."""
        path = pathlib.Path(self.fileloc)
        try:
            rel_path = path.relative_to(self._processor_dags_folder or settings.DAGS_FOLDER)
            if rel_path == pathlib.Path("."):
                return path
            else:
                return rel_path
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
        """
        return ", ".join({t.owner for t in self.tasks})

    @property
    def allow_future_exec_dates(self) -> bool:
        return settings.ALLOW_FUTURE_EXEC_DATES and not self.timetable.can_be_scheduled

    @provide_session
    def get_concurrency_reached(self, session=NEW_SESSION) -> bool:
        """Return a boolean indicating whether the max_active_tasks limit for this DAG has been reached."""
        TI = TaskInstance
        total_tasks = session.scalar(
            select(func.count(TI.task_id)).where(
                TI.dag_id == self.dag_id,
                TI.state == TaskInstanceState.RUNNING,
            )
        )
        return total_tasks >= self.max_active_tasks

    @property
    def concurrency_reached(self):
        """Use `airflow.models.DAG.get_concurrency_reached`, this attribute is deprecated."""
        warnings.warn(
            "This attribute is deprecated. Please use `airflow.models.DAG.get_concurrency_reached` method.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return self.get_concurrency_reached()

    @provide_session
    def get_is_active(self, session=NEW_SESSION) -> None:
        """Return a boolean indicating whether this DAG is active."""
        return session.scalar(select(DagModel.is_active).where(DagModel.dag_id == self.dag_id))

    @provide_session
    def get_is_paused(self, session=NEW_SESSION) -> None:
        """Return a boolean indicating whether this DAG is paused."""
        return session.scalar(select(DagModel.is_paused).where(DagModel.dag_id == self.dag_id))

    @property
    def is_paused(self):
        """Use `airflow.models.DAG.get_is_paused`, this attribute is deprecated."""
        warnings.warn(
            "This attribute is deprecated. Please use `airflow.models.DAG.get_is_paused` method.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return self.get_is_paused()

    @property
    def normalized_schedule_interval(self) -> ScheduleInterval:
        warnings.warn(
            "DAG.normalized_schedule_interval() is deprecated.",
            category=RemovedInAirflow3Warning,
            stacklevel=2,
        )
        if isinstance(self.schedule_interval, str) and self.schedule_interval in cron_presets:
            _schedule_interval: ScheduleInterval = cron_presets.get(self.schedule_interval)
        elif self.schedule_interval == "@once":
            _schedule_interval = None
        else:
            _schedule_interval = self.schedule_interval
        return _schedule_interval

    @provide_session
    def handle_callback(self, dagrun, success=True, reason=None, session=NEW_SESSION):
        """
        Triggers on_failure_callback or on_success_callback as appropriate.

        This method gets the context of a single TaskInstance part of this DagRun
        and passes that to the callable along with a 'reason', primarily to
        differentiate DagRun failures.

        .. note: The logs end up in
            ``$AIRFLOW_HOME/logs/scheduler/latest/PROJECT/DAG_FILE.py.log``

        :param dagrun: DagRun object
        :param success: Flag to specify if failure or success callback should be called
        :param reason: Completion reason
        :param session: Database session
        """
        callbacks = self.on_success_callback if success else self.on_failure_callback
        if callbacks:
            callbacks = callbacks if isinstance(callbacks, list) else [callbacks]
            tis = dagrun.get_task_instances(session=session)
            ti = tis[-1]  # get first TaskInstance of DagRun
            ti.task = self.get_task(ti.task_id)
            context = ti.get_template_context(session=session)
            context.update({"reason": reason})
            for callback in callbacks:
                self.log.info("Executing dag callback function: %s", callback)
                try:
                    callback(context)
                except Exception:
                    self.log.exception("failed to invoke dag state update callback")
                    Stats.incr("dag.callback_exceptions", tags={"dag_id": dagrun.dag_id})

    def get_active_runs(self):
        """
        Return a list of dag run execution dates currently running.

        :return: List of execution dates
        """
        runs = DagRun.find(dag_id=self.dag_id, state=DagRunState.RUNNING)

        active_dates = []
        for run in runs:
            active_dates.append(run.execution_date)

        return active_dates

    @provide_session
    def get_num_active_runs(self, external_trigger=None, only_running=True, session=NEW_SESSION):
        """
        Return the number of active "running" dag runs.

        :param external_trigger: True for externally triggered active dag runs
        :param session:
        :return: number greater than 0 for active dag runs
        """
        query = select(func.count()).where(DagRun.dag_id == self.dag_id)
        if only_running:
            query = query.where(DagRun.state == DagRunState.RUNNING)
        else:
            query = query.where(DagRun.state.in_({DagRunState.RUNNING, DagRunState.QUEUED}))

        if external_trigger is not None:
            query = query.where(
                DagRun.external_trigger == (expression.true() if external_trigger else expression.false())
            )

        return session.scalar(query)

    @provide_session
    def get_dagrun(
        self,
        execution_date: datetime | None = None,
        run_id: str | None = None,
        session: Session = NEW_SESSION,
    ):
        """
        Return the dag run for a given execution date or run_id if it exists, otherwise none.

        :param execution_date: The execution date of the DagRun to find.
        :param run_id: The run_id of the DagRun to find.
        :param session:
        :return: The DagRun if found, otherwise None.
        """
        if not (execution_date or run_id):
            raise TypeError("You must provide either the execution_date or the run_id")
        query = select(DagRun)
        if execution_date:
            query = query.where(DagRun.dag_id == self.dag_id, DagRun.execution_date == execution_date)
        if run_id:
            query = query.where(DagRun.dag_id == self.dag_id, DagRun.run_id == run_id)
        return session.scalar(query)

    @provide_session
    def get_dagruns_between(self, start_date, end_date, session=NEW_SESSION):
        """
        Return the list of dag runs between start_date (inclusive) and end_date (inclusive).

        :param start_date: The starting execution date of the DagRun to find.
        :param end_date: The ending execution date of the DagRun to find.
        :param session:
        :return: The list of DagRuns found.
        """
        dagruns = session.scalars(
            select(DagRun).where(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date >= start_date,
                DagRun.execution_date <= end_date,
            )
        ).all()

        return dagruns

    @provide_session
    def get_latest_execution_date(self, session: Session = NEW_SESSION) -> pendulum.DateTime | None:
        """Return the latest date for which at least one dag run exists."""
        return session.scalar(select(func.max(DagRun.execution_date)).where(DagRun.dag_id == self.dag_id))

    @property
    def latest_execution_date(self):
        """Use `airflow.models.DAG.get_latest_execution_date`, this attribute is deprecated."""
        warnings.warn(
            "This attribute is deprecated. Please use `airflow.models.DAG.get_latest_execution_date`.",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return self.get_latest_execution_date()

    @property
    def subdags(self):
        """Return a list of the subdag objects associated to this DAG."""
        # Check SubDag for class but don't check class directly
        from airflow.operators.subdag import SubDagOperator

        subdag_lst = []
        for task in self.tasks:
            if (
                isinstance(task, SubDagOperator)
                or
                # TODO remove in Airflow 2.0
                type(task).__name__ == "SubDagOperator"
                or task.task_type == "SubDagOperator"
            ):
                subdag_lst.append(task.subdag)
                subdag_lst += task.subdag.subdags
        return subdag_lst

    def resolve_template_files(self):
        for t in self.tasks:
            t.resolve_template_files()

    def get_template_env(self, *, force_sandboxed: bool = False) -> jinja2.Environment:
        """Build a Jinja2 environment."""
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

    @provide_session
    def get_task_instances_before(
        self,
        base_date: datetime,
        num: int,
        *,
        session: Session = NEW_SESSION,
    ) -> list[TaskInstance]:
        """Get ``num`` task instances before (including) ``base_date``.

        The returned list may contain exactly ``num`` task instances
        corresponding to any DagRunType. It can have less if there are
        less than ``num`` scheduled DAG runs before ``base_date``.
        """
        execution_dates: list[Any] = session.execute(
            select(DagRun.execution_date)
            .where(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date <= base_date,
            )
            .order_by(DagRun.execution_date.desc())
            .limit(num)
        ).all()

        if not execution_dates:
            return self.get_task_instances(start_date=base_date, end_date=base_date, session=session)

        min_date: datetime | None = execution_dates[-1]._mapping.get(
            "execution_date"
        )  # getting the last value from the list

        return self.get_task_instances(start_date=min_date, end_date=base_date, session=session)

    @provide_session
    def get_task_instances(
        self,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        state: list[TaskInstanceState] | None = None,
        session: Session = NEW_SESSION,
    ) -> list[TaskInstance]:
        if not start_date:
            start_date = (timezone.utcnow() - timedelta(30)).replace(
                hour=0, minute=0, second=0, microsecond=0
            )

        query = self._get_task_instances(
            task_ids=None,
            start_date=start_date,
            end_date=end_date,
            run_id=None,
            state=state or (),
            include_subdags=False,
            include_parentdag=False,
            include_dependent_dags=False,
            exclude_task_ids=(),
            session=session,
        )
        return session.scalars(cast(Select, query).order_by(DagRun.execution_date)).all()

    @overload
    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        start_date: datetime | None,
        end_date: datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        include_subdags: bool,
        include_parentdag: bool,
        include_dependent_dags: bool,
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        session: Session,
        dag_bag: DagBag | None = ...,
    ) -> Iterable[TaskInstance]:
        ...  # pragma: no cover

    @overload
    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        as_pk_tuple: Literal[True],
        start_date: datetime | None,
        end_date: datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        include_subdags: bool,
        include_parentdag: bool,
        include_dependent_dags: bool,
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        session: Session,
        dag_bag: DagBag | None = ...,
        recursion_depth: int = ...,
        max_recursion_depth: int = ...,
        visited_external_tis: set[TaskInstanceKey] = ...,
    ) -> set[TaskInstanceKey]:
        ...  # pragma: no cover

    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        as_pk_tuple: Literal[True, None] = None,
        start_date: datetime | None,
        end_date: datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        include_subdags: bool,
        include_parentdag: bool,
        include_dependent_dags: bool,
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        session: Session,
        dag_bag: DagBag | None = None,
        recursion_depth: int = 0,
        max_recursion_depth: int | None = None,
        visited_external_tis: set[TaskInstanceKey] | None = None,
    ) -> Iterable[TaskInstance] | set[TaskInstanceKey]:
        TI = TaskInstance

        # If we are looking at subdags/dependent dags we want to avoid UNION calls
        # in SQL (it doesn't play nice with fields that have no equality operator,
        # like JSON types), we instead build our result set separately.
        #
        # This will be empty if we are only looking at one dag, in which case
        # we can return the filtered TI query object directly.
        result: set[TaskInstanceKey] = set()

        # Do we want full objects, or just the primary columns?
        if as_pk_tuple:
            tis = select(TI.dag_id, TI.task_id, TI.run_id, TI.map_index)
        else:
            tis = select(TaskInstance)
        tis = tis.join(TaskInstance.dag_run)

        if include_subdags:
            # Crafting the right filter for dag_id and task_ids combo
            conditions = []
            for dag in [*self.subdags, self]:
                conditions.append(
                    (TaskInstance.dag_id == dag.dag_id) & TaskInstance.task_id.in_(dag.task_ids)
                )
            tis = tis.where(or_(*conditions))
        elif self.partial:
            tis = tis.where(TaskInstance.dag_id == self.dag_id, TaskInstance.task_id.in_(self.task_ids))
        else:
            tis = tis.where(TaskInstance.dag_id == self.dag_id)
        if run_id:
            tis = tis.where(TaskInstance.run_id == run_id)
        if start_date:
            tis = tis.where(DagRun.execution_date >= start_date)
        if task_ids is not None:
            tis = tis.where(TaskInstance.ti_selector_condition(task_ids))

        # This allows allow_trigger_in_future config to take affect, rather than mandating exec_date <= UTC
        if end_date or not self.allow_future_exec_dates:
            end_date = end_date or timezone.utcnow()
            tis = tis.where(DagRun.execution_date <= end_date)

        if state:
            if isinstance(state, (str, TaskInstanceState)):
                tis = tis.where(TaskInstance.state == state)
            elif len(state) == 1:
                tis = tis.where(TaskInstance.state == state[0])
            else:
                # this is required to deal with NULL values
                if None in state:
                    if all(x is None for x in state):
                        tis = tis.where(TaskInstance.state.is_(None))
                    else:
                        not_none_state = [s for s in state if s]
                        tis = tis.where(
                            or_(TaskInstance.state.in_(not_none_state), TaskInstance.state.is_(None))
                        )
                else:
                    tis = tis.where(TaskInstance.state.in_(state))

        # Next, get any of them from our parent DAG (if there is one)
        if include_parentdag and self.parent_dag is not None:
            if visited_external_tis is None:
                visited_external_tis = set()

            p_dag = self.parent_dag.partial_subset(
                task_ids_or_regex=r"^{}$".format(self.dag_id.split(".")[1]),
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
                all_tis = session.execute(query).all()
                condition = TI.filter_for_tis(TaskInstanceKey(*cols) for cols in all_tis)
                if condition is not None:
                    query = select(TI).where(condition)

            if visited_external_tis is None:
                visited_external_tis = set()

            external_tasks = session.scalars(query.where(TI.operator == ExternalTaskMarker.__name__))

            for ti in external_tasks:
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
                        f"Maximum recursion depth {max_recursion_depth} reached for "
                        f"{ExternalTaskMarker.__name__} {ti.task_id}. "
                        f"Attempted to clear too many tasks or there may be a cyclic dependency."
                    )
                ti.render_templates()
                external_tis = session.scalars(
                    select(TI)
                    .join(TI.dag_run)
                    .where(
                        TI.dag_id == task.external_dag_id,
                        TI.task_id == task.external_task_id,
                        DagRun.execution_date == pendulum.parse(task.execution_date),
                    )
                )

                for tii in external_tis:
                    if not dag_bag:
                        from airflow.models.dagbag import DagBag

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
                tis_query = session.execute(tis).all()
                result.update(TaskInstanceKey(**cols._mapping) for cols in tis_query)
            else:
                result.update(ti.key for ti in session.scalars(tis))

            if exclude_task_ids is not None:
                result = {
                    task
                    for task in result
                    if task.task_id not in exclude_task_ids
                    and (task.task_id, task.map_index) not in exclude_task_ids
                }

        if as_pk_tuple:
            return result
        if result:
            # We've been asked for objects, lets combine it all back in to a result set
            ti_filters = TI.filter_for_tis(result)
            if ti_filters is not None:
                tis = select(TI).where(ti_filters)
        elif exclude_task_ids is None:
            pass  # Disable filter if not set.
        elif isinstance(next(iter(exclude_task_ids), None), str):
            tis = tis.where(TI.task_id.notin_(exclude_task_ids))
        else:
            tis = tis.where(not_(tuple_in_condition((TI.task_id, TI.map_index), exclude_task_ids)))

        return tis

    @provide_session
    def set_task_instance_state(
        self,
        *,
        task_id: str,
        map_indexes: Collection[int] | None = None,
        execution_date: datetime | None = None,
        run_id: str | None = None,
        state: TaskInstanceState,
        upstream: bool = False,
        downstream: bool = False,
        future: bool = False,
        past: bool = False,
        commit: bool = True,
        session=NEW_SESSION,
    ) -> list[TaskInstance]:
        """
        Set the state of a TaskInstance and clear downstream tasks in failed or upstream_failed state.

        :param task_id: Task ID of the TaskInstance
        :param map_indexes: Only set TaskInstance if its map_index matches.
            If None (default), all mapped TaskInstances of the task are set.
        :param execution_date: Execution date of the TaskInstance
        :param run_id: The run_id of the TaskInstance
        :param state: State to set the TaskInstance to
        :param upstream: Include all upstream tasks of the given task_id
        :param downstream: Include all downstream tasks of the given task_id
        :param future: Include all future TaskInstances of the given task_id
        :param commit: Commit changes
        :param past: Include all past TaskInstances of the given task_id
        """
        from airflow.api.common.mark_tasks import set_state

        if not exactly_one(execution_date, run_id):
            raise ValueError("Exactly one of execution_date or run_id must be provided")

        task = self.get_task(task_id)
        task.dag = self

        tasks_to_set_state: list[Operator | tuple[Operator, int]]
        if map_indexes is None:
            tasks_to_set_state = [task]
        else:
            tasks_to_set_state = [(task, map_index) for map_index in map_indexes]

        altered = set_state(
            tasks=tasks_to_set_state,
            execution_date=execution_date,
            run_id=run_id,
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

        if execution_date is None:
            dag_run = session.scalars(
                select(DagRun).where(DagRun.run_id == run_id, DagRun.dag_id == self.dag_id)
            ).one()  # Raises an error if not found
            resolve_execution_date = dag_run.execution_date
        else:
            resolve_execution_date = execution_date

        end_date = resolve_execution_date if not future else None
        start_date = resolve_execution_date if not past else None

        subdag.clear(
            start_date=start_date,
            end_date=end_date,
            include_subdags=True,
            include_parentdag=True,
            only_failed=True,
            session=session,
            # Exclude the task itself from being cleared
            exclude_task_ids=frozenset({task_id}),
        )

        return altered

    @provide_session
    def set_task_group_state(
        self,
        *,
        group_id: str,
        execution_date: datetime | None = None,
        run_id: str | None = None,
        state: TaskInstanceState,
        upstream: bool = False,
        downstream: bool = False,
        future: bool = False,
        past: bool = False,
        commit: bool = True,
        session: Session = NEW_SESSION,
    ) -> list[TaskInstance]:
        """
        Set TaskGroup to the given state and clear downstream tasks in failed or upstream_failed state.

        :param group_id: The group_id of the TaskGroup
        :param execution_date: Execution date of the TaskInstance
        :param run_id: The run_id of the TaskInstance
        :param state: State to set the TaskInstance to
        :param upstream: Include all upstream tasks of the given task_id
        :param downstream: Include all downstream tasks of the given task_id
        :param future: Include all future TaskInstances of the given task_id
        :param commit: Commit changes
        :param past: Include all past TaskInstances of the given task_id
        :param session: new session
        """
        from airflow.api.common.mark_tasks import set_state

        if not exactly_one(execution_date, run_id):
            raise ValueError("Exactly one of execution_date or run_id must be provided")

        tasks_to_set_state: list[BaseOperator | tuple[BaseOperator, int]] = []
        task_ids: list[str] = []

        if execution_date is None:
            dag_run = session.scalars(
                select(DagRun).where(DagRun.run_id == run_id, DagRun.dag_id == self.dag_id)
            ).one()  # Raises an error if not found
            resolve_execution_date = dag_run.execution_date
        else:
            resolve_execution_date = execution_date

        end_date = resolve_execution_date if not future else None
        start_date = resolve_execution_date if not past else None

        task_group_dict = self.task_group.get_task_group_dict()
        task_group = task_group_dict.get(group_id)
        if task_group is None:
            raise ValueError("TaskGroup {group_id} could not be found")
        tasks_to_set_state = [task for task in task_group.iter_tasks() if isinstance(task, BaseOperator)]
        task_ids = [task.task_id for task in task_group.iter_tasks()]

        dag_runs_query = session.query(DagRun.id).where(DagRun.dag_id == self.dag_id)
        if start_date is None and end_date is None:
            dag_runs_query = dag_runs_query.where(DagRun.execution_date == start_date)
        else:
            if start_date is not None:
                dag_runs_query = dag_runs_query.where(DagRun.execution_date >= start_date)
            if end_date is not None:
                dag_runs_query = dag_runs_query.where(DagRun.execution_date <= end_date)

        with lock_rows(dag_runs_query, session):
            altered = set_state(
                tasks=tasks_to_set_state,
                execution_date=execution_date,
                run_id=run_id,
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
            task_subset = self.partial_subset(
                task_ids_or_regex=task_ids,
                include_downstream=True,
                include_upstream=False,
            )

            task_subset.clear(
                start_date=start_date,
                end_date=end_date,
                include_subdags=True,
                include_parentdag=True,
                only_failed=True,
                session=session,
                # Exclude the task from the current group from being cleared
                exclude_task_ids=frozenset(task_ids),
            )

        return altered

    @property
    def roots(self) -> list[Operator]:
        """Return nodes with no parents. These are first to execute and are called roots or root nodes."""
        return [task for task in self.tasks if not task.upstream_list]

    @property
    def leaves(self) -> list[Operator]:
        """Return nodes with no children. These are last to execute and are called leaves or leaf nodes."""
        return [task for task in self.tasks if not task.downstream_list]

    def topological_sort(self, include_subdag_tasks: bool = False):
        """
        Sorts tasks in topographical order, such that a task comes after any of its upstream dependencies.

        Deprecated in place of ``task_group.topological_sort``
        """
        from airflow.utils.task_group import TaskGroup

        def nested_topo(group):
            for node in group.topological_sort(_include_subdag_tasks=include_subdag_tasks):
                if isinstance(node, TaskGroup):
                    yield from nested_topo(node)
                else:
                    yield node

        return tuple(nested_topo(self.task_group))

    @provide_session
    def set_dag_runs_state(
        self,
        state: DagRunState = DagRunState.RUNNING,
        session: Session = NEW_SESSION,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        dag_ids: list[str] = [],
    ) -> None:
        warnings.warn(
            "This method is deprecated and will be removed in a future version.",
            RemovedInAirflow3Warning,
            stacklevel=3,
        )
        dag_ids = dag_ids or [self.dag_id]
        query = update(DagRun).where(DagRun.dag_id.in_(dag_ids))
        if start_date:
            query = query.where(DagRun.execution_date >= start_date)
        if end_date:
            query = query.where(DagRun.execution_date <= end_date)
        session.execute(query.values(state=state).execution_options(synchronize_session="fetch"))

    @provide_session
    def clear(
        self,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        confirm_prompt: bool = False,
        include_subdags: bool = True,
        include_parentdag: bool = True,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: bool = False,
        session: Session = NEW_SESSION,
        get_tis: bool = False,
        recursion_depth: int = 0,
        max_recursion_depth: int | None = None,
        dag_bag: DagBag | None = None,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
    ) -> int | Iterable[TaskInstance]:
        """
        Clear a set of task instances associated with the current dag for a specified date range.

        :param task_ids: List of task ids or (``task_id``, ``map_index``) tuples to clear
        :param start_date: The minimum execution_date to clear
        :param end_date: The maximum execution_date to clear
        :param only_failed: Only clear failed tasks
        :param only_running: Only clear running tasks.
        :param confirm_prompt: Ask for confirmation
        :param include_subdags: Clear tasks in subdags and clear external tasks
            indicated by ExternalTaskMarker
        :param include_parentdag: Clear tasks in the parent dag of the subdag.
        :param dag_run_state: state to set DagRun to. If set to False, dagrun state will not
            be changed.
        :param dry_run: Find the tasks to clear but don't clear them.
        :param session: The sqlalchemy session to use
        :param dag_bag: The DagBag used to find the dags subdags (Optional)
        :param exclude_task_ids: A set of ``task_id`` or (``task_id``, ``map_index``)
            tuples that should not be cleared
        """
        if get_tis:
            warnings.warn(
                "Passing `get_tis` to dag.clear() is deprecated. Use `dry_run` parameter instead.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
            dry_run = True

        if recursion_depth:
            warnings.warn(
                "Passing `recursion_depth` to dag.clear() is deprecated.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )
        if max_recursion_depth:
            warnings.warn(
                "Passing `max_recursion_depth` to dag.clear() is deprecated.",
                RemovedInAirflow3Warning,
                stacklevel=2,
            )

        state: list[TaskInstanceState] = []
        if only_failed:
            state += [TaskInstanceState.FAILED, TaskInstanceState.UPSTREAM_FAILED]
        if only_running:
            # Yes, having `+=` doesn't make sense, but this was the existing behaviour
            state += [TaskInstanceState.RUNNING]

        tis = self._get_task_instances(
            task_ids=task_ids,
            start_date=start_date,
            end_date=end_date,
            run_id=None,
            state=state,
            include_subdags=include_subdags,
            include_parentdag=include_parentdag,
            include_dependent_dags=include_subdags,  # compat, yes this is not a typo
            session=session,
            dag_bag=dag_bag,
            exclude_task_ids=exclude_task_ids,
        )

        if dry_run:
            return session.scalars(tis).all()

        tis = session.scalars(tis).all()

        count = len(list(tis))
        do_it = True
        if count == 0:
            return 0
        if confirm_prompt:
            ti_list = "\n".join(str(t) for t in tis)
            question = f"You are about to delete these {count} tasks:\n{ti_list}\n\nAre you sure? [y/n]"
            do_it = utils.helpers.ask_yesno(question)

        if do_it:
            clear_task_instances(
                list(tis),
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
            question = f"You are about to delete these {count} tasks:\n{ti_list}\n\nAre you sure? [y/n]"
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
            if k not in ("user_defined_macros", "user_defined_filters", "_log"):
                setattr(result, k, copy.deepcopy(v, memo))

        result.user_defined_macros = self.user_defined_macros
        result.user_defined_filters = self.user_defined_filters
        if hasattr(self, "_log"):
            result._log = self._log
        return result

    def sub_dag(self, *args, **kwargs):
        """Use `airflow.models.DAG.partial_subset`, this method is deprecated."""
        warnings.warn(
            "This method is deprecated and will be removed in a future version. Please use partial_subset",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return self.partial_subset(*args, **kwargs)

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
        from airflow.models.baseoperator import BaseOperator
        from airflow.models.mappedoperator import MappedOperator

        # deep-copying self.task_dict and self._task_group takes a long time, and we don't want all
        # the tasks anyway, so we copy the tasks manually later
        memo = {id(self.task_dict): None, id(self._task_group): None}
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

        # Compiling the unique list of tasks that made the cut
        # Make sure to not recursively deepcopy the dag or task_group while copying the task.
        # task_group is reset later
        def _deepcopy_task(t) -> Operator:
            memo.setdefault(id(t.task_group), None)
            return copy.deepcopy(t, memo)

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
            for attr, value in copied.__dict__.items():
                if id(value) in memo:
                    value = memo[id(value)]
                else:
                    value = copy.deepcopy(value, memo)
                copied.__dict__[attr] = value

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

        dag._task_group = filter_task_group(self.task_group, None)

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
        return {k: v for k, v in self._task_group.get_task_group_dict().items() if k is not None}

    def get_task(self, task_id: str, include_subdags: bool = False) -> Operator:
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        if include_subdags:
            for dag in self.subdags:
                if task_id in dag.task_dict:
                    return dag.task_dict[task_id]
        raise TaskNotFound(f"Task {task_id} not found")

    def pickle_info(self):
        d = {}
        d["is_picklable"] = True
        try:
            dttm = timezone.utcnow()
            pickled = pickle.dumps(self)
            d["pickle_len"] = len(pickled)
            d["pickling_duration"] = str(timezone.utcnow() - dttm)
        except Exception as e:
            self.log.debug(e)
            d["is_picklable"] = False
            d["stacktrace"] = traceback.format_exc()
        return d

    @provide_session
    def pickle(self, session=NEW_SESSION) -> DagPickle:
        dag = session.scalar(select(DagModel).where(DagModel.dag_id == self.dag_id).limit(1))
        dp = None
        if dag and dag.pickle_id:
            dp = session.scalar(select(DagPickle).where(DagPickle.id == dag.pickle_id).limit(1))
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
    def task(self) -> TaskDecoratorCollection:
        from airflow.decorators import task

        return cast("TaskDecoratorCollection", functools.partial(task, dag=self))

    def add_task(self, task: Operator) -> None:
        """
        Add a task to the DAG.

        :param task: the task you want to add
        """
        FailStopDagInvalidTriggerRule.check(dag=self, trigger_rule=task.trigger_rule)

        from airflow.utils.task_group import TaskGroupContext

        if not self.start_date and not task.start_date:
            raise AirflowException("DAG is missing the start_date parameter")
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

        task_id = task.task_id
        if not task.task_group:
            task_group = TaskGroupContext.get_current_task_group(self)
            if task_group:
                task_id = task_group.child_id(task_id)
                task_group.add(task)

        if (
            task_id in self.task_dict and self.task_dict[task_id] is not task
        ) or task_id in self._task_group.used_group_ids:
            raise DuplicateTaskIdFound(f"Task id '{task_id}' has already been added to the DAG")
        else:
            self.task_dict[task_id] = task
            task.dag = self
            # Add task_id to used_group_ids to prevent group_id and task_id collisions.
            self._task_group.used_group_ids.add(task_id)

        self.task_count = len(self.task_dict)

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

        self.task_count = len(self.task_dict)

    def run(
        self,
        start_date=None,
        end_date=None,
        mark_success=False,
        local=False,
        executor=None,
        donot_pickle=airflow_conf.getboolean("core", "donot_pickle"),
        ignore_task_deps=False,
        ignore_first_depends_on_past=True,
        pool=None,
        delay_on_limit_secs=1.0,
        verbose=False,
        conf=None,
        rerun_failed_tasks=False,
        run_backwards=False,
        run_at_least_once=False,
        continue_on_failures=False,
        disable_retry=False,
    ):
        """
        Run the DAG.

        :param start_date: the start date of the range to run
        :param end_date: the end date of the range to run
        :param mark_success: True to mark jobs as succeeded without running them
        :param local: True to run the tasks using the LocalExecutor
        :param executor: The executor instance to run the tasks
        :param donot_pickle: True to avoid pickling DAG object and send to workers
        :param ignore_task_deps: True to skip upstream tasks
        :param ignore_first_depends_on_past: True to ignore depends_on_past
            dependencies for the first set of tasks only
        :param pool: Resource pool to use
        :param delay_on_limit_secs: Time in seconds to wait before next attempt to run
            dag run when max_active_runs limit has been reached
        :param verbose: Make logging output more verbose
        :param conf: user defined dictionary passed from CLI
        :param rerun_failed_tasks:
        :param run_backwards:
        :param run_at_least_once: If true, always run the DAG at least once even
            if no logical run exists within the time range.
        """
        from airflow.jobs.backfill_job_runner import BackfillJobRunner

        if not executor and local:
            from airflow.executors.local_executor import LocalExecutor

            executor = LocalExecutor()
        elif not executor:
            from airflow.executors.executor_loader import ExecutorLoader

            executor = ExecutorLoader.get_default_executor()
        from airflow.jobs.job import Job

        job = Job(executor=executor)
        job_runner = BackfillJobRunner(
            job=job,
            dag=self,
            start_date=start_date,
            end_date=end_date,
            mark_success=mark_success,
            donot_pickle=donot_pickle,
            ignore_task_deps=ignore_task_deps,
            ignore_first_depends_on_past=ignore_first_depends_on_past,
            pool=pool,
            delay_on_limit_secs=delay_on_limit_secs,
            verbose=verbose,
            conf=conf,
            rerun_failed_tasks=rerun_failed_tasks,
            run_backwards=run_backwards,
            run_at_least_once=run_at_least_once,
            continue_on_failures=continue_on_failures,
            disable_retry=disable_retry,
        )
        run_job(job=job, execute_callable=job_runner._execute)

    def cli(self):
        """Exposes a CLI specific to this DAG."""
        check_cycle(self)

        from airflow.cli import cli_parser

        parser = cli_parser.get_parser(dag_parser=True)
        args = parser.parse_args()
        args.func(args, self)

    @provide_session
    def test(
        self,
        execution_date: datetime | None = None,
        run_conf: dict[str, Any] | None = None,
        conn_file_path: str | None = None,
        variable_file_path: str | None = None,
        session: Session = NEW_SESSION,
    ) -> DagRun:
        """
        Execute one single DagRun for a given DAG and execution date.

        :param execution_date: execution date for the DAG run
        :param run_conf: configuration to pass to newly created dagrun
        :param conn_file_path: file path to a connection file in either yaml or json
        :param variable_file_path: file path to a variable file in either yaml or json
        :param session: database connection (optional)
        """

        def add_logger_if_needed(ti: TaskInstance):
            """Add a formatted logger to the task instance.

            This allows all logs to surface to the command line, instead of into
            a task file. Since this is a local test run, it is much better for
            the user to see logs in the command line, rather than needing to
            search for a log file.

            :param ti: The task instance that will receive a logger.
            """
            format = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
            handler = logging.StreamHandler(sys.stdout)
            handler.level = logging.INFO
            handler.setFormatter(format)
            # only add log handler once
            if not any(isinstance(h, logging.StreamHandler) for h in ti.log.handlers):
                self.log.debug("Adding Streamhandler to taskinstance %s", ti.task_id)
                ti.log.addHandler(handler)

        if conn_file_path or variable_file_path:
            local_secrets = LocalFilesystemBackend(
                variables_file_path=variable_file_path, connections_file_path=conn_file_path
            )
            secrets_backend_list.insert(0, local_secrets)

        execution_date = execution_date or timezone.utcnow()
        self.validate()
        self.log.debug("Clearing existing task instances for execution date %s", execution_date)
        self.clear(
            start_date=execution_date,
            end_date=execution_date,
            dag_run_state=False,  # type: ignore
            session=session,
        )
        self.log.debug("Getting dagrun for dag %s", self.dag_id)
        logical_date = timezone.coerce_datetime(execution_date)
        data_interval = self.timetable.infer_manual_data_interval(run_after=logical_date)
        dr: DagRun = _get_or_create_dagrun(
            dag=self,
            start_date=execution_date,
            execution_date=execution_date,
            run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
            session=session,
            conf=run_conf,
            data_interval=data_interval,
        )

        tasks = self.task_dict
        self.log.debug("starting dagrun")
        # Instead of starting a scheduler, we run the minimal loop possible to check
        # for task readiness and dependency management. This is notably faster
        # than creating a BackfillJob and allows us to surface logs to the user
        while dr.state == DagRunState.RUNNING:
            schedulable_tis, _ = dr.update_state(session=session)
            for ti in schedulable_tis:
                try:
                    add_logger_if_needed(ti)
                    ti.task = tasks[ti.task_id]
                    ret = _run_task(ti, session=session)
                    if ret is TaskReturnCode.DEFERRED:
                        if not _triggerer_is_healthy():
                            raise _StopDagTest(
                                "Task has deferred but triggerer component is not running. "
                                "You can start the triggerer by running `airflow triggerer` in a terminal."
                            )
                except _StopDagTest:
                    # Let this exception bubble out and not be swallowed by the
                    # except block below.
                    raise
                except Exception:
                    self.log.exception("Task failed; ti=%s", ti)
        if conn_file_path or variable_file_path:
            # Remove the local variables we have added to the secrets_backend_list
            secrets_backend_list.pop(0)
        return dr

    @provide_session
    def create_dagrun(
        self,
        state: DagRunState,
        execution_date: datetime | None = None,
        run_id: str | None = None,
        start_date: datetime | None = None,
        external_trigger: bool | None = False,
        conf: dict | None = None,
        run_type: DagRunType | None = None,
        session: Session = NEW_SESSION,
        dag_hash: str | None = None,
        creating_job_id: int | None = None,
        data_interval: tuple[datetime, datetime] | None = None,
    ):
        """
        Create a dag run from this dag including the tasks associated with this dag.

        Returns the dag run.

        :param run_id: defines the run id for this dag run
        :param run_type: type of DagRun
        :param execution_date: the execution date of this dag run
        :param state: the state of the dag run
        :param start_date: the date this dag run should be evaluated
        :param external_trigger: whether this dag run is externally triggered
        :param conf: Dict containing configuration/parameters to pass to the DAG
        :param creating_job_id: id of the job creating this DagRun
        :param session: database session
        :param dag_hash: Hash of Serialized DAG
        :param data_interval: Data interval of the DagRun
        """
        logical_date = timezone.coerce_datetime(execution_date)

        if data_interval and not isinstance(data_interval, DataInterval):
            data_interval = DataInterval(*map(timezone.coerce_datetime, data_interval))

        if data_interval is None and logical_date is not None:
            warnings.warn(
                "Calling `DAG.create_dagrun()` without an explicit data interval is deprecated",
                RemovedInAirflow3Warning,
                stacklevel=3,
            )
            if run_type == DagRunType.MANUAL:
                data_interval = self.timetable.infer_manual_data_interval(run_after=logical_date)
            else:
                data_interval = self.infer_automated_data_interval(logical_date)

        if run_type is None or isinstance(run_type, DagRunType):
            pass
        elif isinstance(run_type, str):  # Compatibility: run_type used to be a str.
            run_type = DagRunType(run_type)
        else:
            raise ValueError(f"`run_type` should be a DagRunType, not {type(run_type)}")

        if run_id:  # Infer run_type from run_id if needed.
            if not isinstance(run_id, str):
                raise ValueError(f"`run_id` should be a str, not {type(run_id)}")
            inferred_run_type = DagRunType.from_run_id(run_id)
            if run_type is None:
                # No explicit type given, use the inferred type.
                run_type = inferred_run_type
            elif run_type == DagRunType.MANUAL and inferred_run_type != DagRunType.MANUAL:
                # Prevent a manual run from using an ID that looks like a scheduled run.
                raise ValueError(
                    f"A {run_type.value} DAG run cannot use ID {run_id!r} since it "
                    f"is reserved for {inferred_run_type.value} runs"
                )
        elif run_type and logical_date is not None:  # Generate run_id from run_type and execution_date.
            run_id = self.timetable.generate_run_id(
                run_type=run_type, logical_date=logical_date, data_interval=data_interval
            )
        else:
            raise AirflowException(
                "Creating DagRun needs either `run_id` or both `run_type` and `execution_date`"
            )

        regex = airflow_conf.get("scheduler", "allowed_run_id_pattern")

        if run_id and not re2.match(RUN_ID_REGEX, run_id):
            if not regex.strip() or not re2.match(regex.strip(), run_id):
                raise AirflowException(
                    f"The provided run ID '{run_id}' is invalid. It does not match either "
                    f"the configured pattern: '{regex}' or the built-in pattern: '{RUN_ID_REGEX}'"
                )

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
    def bulk_sync_to_db(
        cls,
        dags: Collection[DAG],
        session=NEW_SESSION,
    ):
        """Use `airflow.models.DAG.bulk_write_to_db`, this method is deprecated."""
        warnings.warn(
            "This method is deprecated and will be removed in a future version. Please use bulk_write_to_db",
            RemovedInAirflow3Warning,
            stacklevel=2,
        )
        return cls.bulk_write_to_db(dags=dags, session=session)

    @classmethod
    @provide_session
    def bulk_write_to_db(
        cls,
        dags: Collection[DAG],
        processor_subdir: str | None = None,
        session=NEW_SESSION,
    ):
        """
        Ensure the DagModel rows for the given dags are up-to-date in the dag table in the DB.

        Note that this method can be called for both DAGs and SubDAGs. A SubDag is actually a SubDagOperator.

        :param dags: the DAG objects to save to the DB
        :return: None
        """
        if not dags:
            return

        log.info("Sync %s DAGs", len(dags))
        dag_by_ids = {dag.dag_id: dag for dag in dags}

        dag_ids = set(dag_by_ids)
        query = (
            select(DagModel)
            .options(joinedload(DagModel.tags, innerjoin=False))
            .where(DagModel.dag_id.in_(dag_ids))
            .options(joinedload(DagModel.schedule_dataset_references))
            .options(joinedload(DagModel.task_outlet_dataset_references))
        )
        query = with_row_locks(query, of=DagModel, session=session)
        orm_dags: list[DagModel] = session.scalars(query).unique().all()
        existing_dags = {orm_dag.dag_id: orm_dag for orm_dag in orm_dags}
        missing_dag_ids = dag_ids.difference(existing_dags)

        for missing_dag_id in missing_dag_ids:
            orm_dag = DagModel(dag_id=missing_dag_id)
            dag = dag_by_ids[missing_dag_id]
            if dag.is_paused_upon_creation is not None:
                orm_dag.is_paused = dag.is_paused_upon_creation
            orm_dag.tags = []
            log.info("Creating ORM DAG for %s", dag.dag_id)
            session.add(orm_dag)
            orm_dags.append(orm_dag)

        most_recent_runs: dict[str, DagRun] = {}
        num_active_runs: dict[str, int] = {}
        # Skip these queries entirely if no DAGs can be scheduled to save time.
        if any(dag.timetable.can_be_scheduled for dag in dags):
            # Get the latest dag run for each existing dag as a single query (avoid n+1 query)
            most_recent_subq = (
                select(DagRun.dag_id, func.max(DagRun.execution_date).label("max_execution_date"))
                .where(
                    DagRun.dag_id.in_(existing_dags),
                    or_(DagRun.run_type == DagRunType.BACKFILL_JOB, DagRun.run_type == DagRunType.SCHEDULED),
                )
                .group_by(DagRun.dag_id)
                .subquery()
            )
            most_recent_runs_iter = session.scalars(
                select(DagRun).where(
                    DagRun.dag_id == most_recent_subq.c.dag_id,
                    DagRun.execution_date == most_recent_subq.c.max_execution_date,
                )
            )
            most_recent_runs = {run.dag_id: run for run in most_recent_runs_iter}

            # Get number of active dagruns for all dags we are processing as a single query.
            num_active_runs = DagRun.active_runs_of_dags(dag_ids=existing_dags, session=session)

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
            orm_dag.has_import_errors = False
            orm_dag.last_parsed_time = timezone.utcnow()
            orm_dag.default_view = dag.default_view
            orm_dag.description = dag.description
            orm_dag.max_active_tasks = dag.max_active_tasks
            orm_dag.max_active_runs = dag.max_active_runs
            orm_dag.has_task_concurrency_limits = any(
                t.max_active_tis_per_dag is not None or t.max_active_tis_per_dagrun is not None
                for t in dag.tasks
            )
            orm_dag.schedule_interval = dag.schedule_interval
            orm_dag.timetable_description = dag.timetable.description
            orm_dag.processor_subdir = processor_subdir

            run: DagRun | None = most_recent_runs.get(dag.dag_id)
            if run is None:
                data_interval = None
            else:
                data_interval = dag.get_run_data_interval(run)
            if num_active_runs.get(dag.dag_id, 0) >= orm_dag.max_active_runs:
                orm_dag.next_dagrun_create_after = None
            else:
                orm_dag.calculate_dagrun_date_fields(dag, data_interval)

            dag_tags = set(dag.tags or {})
            orm_dag_tags = list(orm_dag.tags or [])
            for orm_tag in orm_dag_tags:
                if orm_tag.name not in dag_tags:
                    session.delete(orm_tag)
                    orm_dag.tags.remove(orm_tag)
            orm_tag_names = {t.name for t in orm_dag_tags}
            for dag_tag in dag_tags:
                if dag_tag not in orm_tag_names:
                    dag_tag_orm = DagTag(name=dag_tag, dag_id=dag.dag_id)
                    orm_dag.tags.append(dag_tag_orm)
                    session.add(dag_tag_orm)

            orm_dag_links = orm_dag.dag_owner_links or []
            for orm_dag_link in orm_dag_links:
                if orm_dag_link not in dag.owner_links:
                    session.delete(orm_dag_link)
            for owner_name, owner_link in dag.owner_links.items():
                dag_owner_orm = DagOwnerAttributes(dag_id=dag.dag_id, owner=owner_name, link=owner_link)
                session.add(dag_owner_orm)

        DagCode.bulk_sync_to_db(filelocs, session=session)

        from airflow.datasets import Dataset
        from airflow.models.dataset import (
            DagScheduleDatasetReference,
            DatasetModel,
            TaskOutletDatasetReference,
        )

        dag_references = collections.defaultdict(set)
        outlet_references = collections.defaultdict(set)
        # We can't use a set here as we want to preserve order
        outlet_datasets: dict[Dataset, None] = {}
        input_datasets: dict[Dataset, None] = {}

        # here we go through dags and tasks to check for dataset references
        # if there are now None and previously there were some, we delete them
        # if there are now *any*, we add them to the above data structures, and
        # later we'll persist them to the database.
        for dag in dags:
            curr_orm_dag = existing_dags.get(dag.dag_id)
            if not dag.dataset_triggers:
                if curr_orm_dag and curr_orm_dag.schedule_dataset_references:
                    curr_orm_dag.schedule_dataset_references = []
            for dataset in dag.dataset_triggers:
                dag_references[dag.dag_id].add(dataset.uri)
                input_datasets[DatasetModel.from_public(dataset)] = None
            curr_outlet_references = curr_orm_dag and curr_orm_dag.task_outlet_dataset_references
            for task in dag.tasks:
                dataset_outlets = [x for x in task.outlets or [] if isinstance(x, Dataset)]
                if not dataset_outlets:
                    if curr_outlet_references:
                        this_task_outlet_refs = [
                            x
                            for x in curr_outlet_references
                            if x.dag_id == dag.dag_id and x.task_id == task.task_id
                        ]
                        for ref in this_task_outlet_refs:
                            curr_outlet_references.remove(ref)
                for d in dataset_outlets:
                    outlet_references[(task.dag_id, task.task_id)].add(d.uri)
                    outlet_datasets[DatasetModel.from_public(d)] = None
        all_datasets = outlet_datasets
        all_datasets.update(input_datasets)

        # store datasets
        stored_datasets = {}
        for dataset in all_datasets:
            stored_dataset = session.scalar(
                select(DatasetModel).where(DatasetModel.uri == dataset.uri).limit(1)
            )
            if stored_dataset:
                # Some datasets may have been previously unreferenced, and therefore orphaned by the
                # scheduler. But if we're here, then we have found that dataset again in our DAGs, which
                # means that it is no longer an orphan, so set is_orphaned to False.
                stored_dataset.is_orphaned = expression.false()
                stored_datasets[stored_dataset.uri] = stored_dataset
            else:
                session.add(dataset)
                stored_datasets[dataset.uri] = dataset

        session.flush()  # this is required to ensure each dataset has its PK loaded

        del all_datasets

        # reconcile dag-schedule-on-dataset references
        for dag_id, uri_list in dag_references.items():
            dag_refs_needed = {
                DagScheduleDatasetReference(dataset_id=stored_datasets[uri].id, dag_id=dag_id)
                for uri in uri_list
            }
            dag_refs_stored = set(
                existing_dags.get(dag_id)
                and existing_dags.get(dag_id).schedule_dataset_references  # type: ignore
                or []
            )
            dag_refs_to_add = {x for x in dag_refs_needed if x not in dag_refs_stored}
            session.bulk_save_objects(dag_refs_to_add)
            for obj in dag_refs_stored - dag_refs_needed:
                session.delete(obj)

        existing_task_outlet_refs_dict = collections.defaultdict(set)
        for dag_id, orm_dag in existing_dags.items():
            for todr in orm_dag.task_outlet_dataset_references:
                existing_task_outlet_refs_dict[(dag_id, todr.task_id)].add(todr)

        # reconcile task-outlet-dataset references
        for (dag_id, task_id), uri_list in outlet_references.items():
            task_refs_needed = {
                TaskOutletDatasetReference(dataset_id=stored_datasets[uri].id, dag_id=dag_id, task_id=task_id)
                for uri in uri_list
            }
            task_refs_stored = existing_task_outlet_refs_dict[(dag_id, task_id)]
            task_refs_to_add = {x for x in task_refs_needed if x not in task_refs_stored}
            session.bulk_save_objects(task_refs_to_add)
            for obj in task_refs_stored - task_refs_needed:
                session.delete(obj)

        # Issue SQL/finish "Unit of Work", but let @provide_session commit (or if passed a session, let caller
        # decide when to commit
        session.flush()

        for dag in dags:
            cls.bulk_write_to_db(dag.subdags, processor_subdir=processor_subdir, session=session)

    @provide_session
    def sync_to_db(self, processor_subdir: str | None = None, session=NEW_SESSION):
        """
        Save attributes about this DAG to the DB.

        Note that this method can be called for both DAGs and SubDAGs. A SubDag is actually a SubDagOperator.

        :return: None
        """
        self.bulk_write_to_db([self], processor_subdir=processor_subdir, session=session)

    def get_default_view(self):
        """Allow backward compatible jinja2 templates."""
        if self.default_view is None:
            return airflow_conf.get("webserver", "dag_default_view").lower()
        else:
            return self.default_view

    @staticmethod
    @provide_session
    def deactivate_unknown_dags(active_dag_ids, session=NEW_SESSION):
        """
        Given a list of known DAGs, deactivate any other DAGs that are marked as active in the ORM.

        :param active_dag_ids: list of DAG IDs that are active
        :return: None
        """
        if not active_dag_ids:
            return
        for dag in session.scalars(select(DagModel).where(~DagModel.dag_id.in_(active_dag_ids))).all():
            dag.is_active = False
            session.merge(dag)
        session.commit()

    @staticmethod
    @provide_session
    def deactivate_stale_dags(expiration_date, session=NEW_SESSION):
        """
        Deactivate any DAGs that were last touched by the scheduler before the expiration date.

        These DAGs were likely deleted.

        :param expiration_date: set inactive DAGs that were touched before this time
        :return: None
        """
        for dag in session.scalars(
            select(DagModel).where(DagModel.last_parsed_time < expiration_date, DagModel.is_active)
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
    def get_num_task_instances(dag_id, run_id=None, task_ids=None, states=None, session=NEW_SESSION) -> int:
        """
        Return the number of task instances in the given DAG.

        :param session: ORM session
        :param dag_id: ID of the DAG to get the task concurrency of
        :param run_id: ID of the DAG run to get the task concurrency of
        :param task_ids: A list of valid task IDs for the given DAG
        :param states: A list of states to filter by if supplied
        :return: The number of running tasks
        """
        qry = select(func.count(TaskInstance.task_id)).where(
            TaskInstance.dag_id == dag_id,
        )
        if run_id:
            qry = qry.where(
                TaskInstance.run_id == run_id,
            )
        if task_ids:
            qry = qry.where(
                TaskInstance.task_id.in_(task_ids),
            )

        if states:
            if None in states:
                if all(x is None for x in states):
                    qry = qry.where(TaskInstance.state.is_(None))
                else:
                    not_none_states = [state for state in states if state]
                    qry = qry.where(
                        or_(TaskInstance.state.in_(not_none_states), TaskInstance.state.is_(None))
                    )
            else:
                qry = qry.where(TaskInstance.state.in_(states))
        return session.scalar(qry)

    @classmethod
    def get_serialized_fields(cls):
        """Stringified DAGs and operators contain exactly these fields."""
        if not cls.__serialized_fields:
            exclusion_list = {
                "parent_dag",
                "schedule_dataset_references",
                "task_outlet_dataset_references",
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
            }
            cls.__serialized_fields = frozenset(vars(DAG(dag_id="test"))) - exclusion_list
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

    def validate_schedule_and_params(self):
        """
        Validate Param values when the DAG has schedule defined.

        Raise exception if there are any Params which can not be resolved by their schema definition.
        """
        if not self.timetable.can_be_scheduled:
            return

        try:
            self.params.validate()
        except ParamValidationError as pverr:
            raise AirflowException(
                "DAG is not allowed to define a Schedule, "
                "if there are any required params without default values or default values are not valid."
            ) from pverr

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


class DagTag(Base):
    """A tag name per dag, to allow quick filtering in the DAG view."""

    __tablename__ = "dag_tag"
    name = Column(String(TAG_MAX_LEN), primary_key=True)
    dag_id = Column(
        StringID(),
        ForeignKey("dag.dag_id", name="dag_tag_dag_id_fkey", ondelete="CASCADE"),
        primary_key=True,
    )

    def __repr__(self):
        return self.name


class DagOwnerAttributes(Base):
    """
    Table defining different owner attributes.

    For example, a link for an owner that will be passed as a hyperlink to the "DAGs" view.
    """

    __tablename__ = "dag_owner_attributes"
    dag_id = Column(
        StringID(),
        ForeignKey("dag.dag_id", name="dag.dag_id", ondelete="CASCADE"),
        nullable=False,
        primary_key=True,
    )
    owner = Column(String(500), primary_key=True, nullable=False)
    link = Column(String(500), nullable=False)

    def __repr__(self):
        return f"<DagOwnerAttributes: dag_id={self.dag_id}, owner={self.owner}, link={self.link}>"

    @classmethod
    def get_all(cls, session) -> dict[str, dict[str, str]]:
        dag_links: dict = collections.defaultdict(dict)
        for obj in session.scalars(select(cls)):
            dag_links[obj.dag_id].update({obj.owner: obj.link})
        return dag_links


class DagModel(Base):
    """Table containing DAG properties."""

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = Column(StringID(), primary_key=True)
    root_dag_id = Column(StringID())
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused_at_creation = airflow_conf.getboolean("core", "dags_are_paused_at_creation")
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
    # The base directory used by Dag Processor that parsed this dag.
    processor_subdir = Column(String(2000), nullable=True)
    # String representing the owners
    owners = Column(String(2000))
    # Description of the dag
    description = Column(Text)
    # Default view of the DAG inside the webserver
    default_view = Column(String(25))
    # Schedule interval
    schedule_interval = Column(Interval)
    # Timetable/Schedule Interval description
    timetable_description = Column(String(1000), nullable=True)
    # Tags for view filter
    tags = relationship("DagTag", cascade="all, delete, delete-orphan", backref=backref("dag"))
    # Dag owner links for DAGs view
    dag_owner_links = relationship(
        "DagOwnerAttributes", cascade="all, delete, delete-orphan", backref=backref("dag")
    )

    max_active_tasks = Column(Integer, nullable=False)
    max_active_runs = Column(Integer, nullable=True)

    has_task_concurrency_limits = Column(Boolean, nullable=False)
    has_import_errors = Column(Boolean(), default=False, server_default="0")

    # The logical date of the next dag run.
    next_dagrun = Column(UtcDateTime)

    # Must be either both NULL or both datetime.
    next_dagrun_data_interval_start = Column(UtcDateTime)
    next_dagrun_data_interval_end = Column(UtcDateTime)

    # Earliest time at which this ``next_dagrun`` can be created.
    next_dagrun_create_after = Column(UtcDateTime)

    __table_args__ = (
        Index("idx_root_dag_id", root_dag_id, unique=False),
        Index("idx_next_dagrun_create_after", next_dagrun_create_after, unique=False),
    )

    parent_dag = relationship(
        "DagModel", remote_side=[dag_id], primaryjoin=root_dag_id == dag_id, foreign_keys=[root_dag_id]
    )
    schedule_dataset_references = relationship(
        "DagScheduleDatasetReference",
        cascade="all, delete, delete-orphan",
    )
    schedule_datasets = association_proxy("schedule_dataset_references", "dataset")
    task_outlet_dataset_references = relationship(
        "TaskOutletDatasetReference",
        cascade="all, delete, delete-orphan",
    )
    NUM_DAGS_PER_DAGRUN_QUERY = airflow_conf.getint(
        "scheduler", "max_dagruns_to_create_per_loop", fallback=10
    )

    def __init__(self, concurrency=None, **kwargs):
        super().__init__(**kwargs)
        if self.max_active_tasks is None:
            if concurrency:
                warnings.warn(
                    "The 'DagModel.concurrency' parameter is deprecated. Please use 'max_active_tasks'.",
                    RemovedInAirflow3Warning,
                    stacklevel=2,
                )
                self.max_active_tasks = concurrency
            else:
                self.max_active_tasks = airflow_conf.getint("core", "max_active_tasks_per_dag")

        if self.max_active_runs is None:
            self.max_active_runs = airflow_conf.getint("core", "max_active_runs_per_dag")

        if self.has_task_concurrency_limits is None:
            # Be safe -- this will be updated later once the DAG is parsed
            self.has_task_concurrency_limits = True

    def __repr__(self):
        return f"<DAG: {self.dag_id}>"

    @property
    def next_dagrun_data_interval(self) -> DataInterval | None:
        return _get_model_data_interval(
            self,
            "next_dagrun_data_interval_start",
            "next_dagrun_data_interval_end",
        )

    @next_dagrun_data_interval.setter
    def next_dagrun_data_interval(self, value: tuple[datetime, datetime] | None) -> None:
        if value is None:
            self.next_dagrun_data_interval_start = self.next_dagrun_data_interval_end = None
        else:
            self.next_dagrun_data_interval_start, self.next_dagrun_data_interval_end = value

    @property
    def timezone(self):
        return settings.TIMEZONE

    @staticmethod
    @provide_session
    def get_dagmodel(dag_id: str, session: Session = NEW_SESSION) -> DagModel | None:
        return session.get(
            DagModel,
            dag_id,
            options=[joinedload(DagModel.parent_dag)],
        )

    @classmethod
    @provide_session
    def get_current(cls, dag_id, session=NEW_SESSION):
        return session.scalar(select(cls).where(cls.dag_id == dag_id))

    @provide_session
    def get_last_dagrun(self, session=NEW_SESSION, include_externally_triggered=False):
        return get_last_dagrun(
            self.dag_id, session=session, include_externally_triggered=include_externally_triggered
        )

    def get_is_paused(self, *, session: Session | None = None) -> bool:
        """Provide interface compatibility to 'DAG'."""
        return self.is_paused

    @staticmethod
    @internal_api_call
    @provide_session
    def get_paused_dag_ids(dag_ids: list[str], session: Session = NEW_SESSION) -> set[str]:
        """
        Given a list of dag_ids, get a set of Paused Dag Ids.

        :param dag_ids: List of Dag ids
        :param session: ORM Session
        :return: Paused Dag_ids
        """
        paused_dag_ids = session.execute(
            select(DagModel.dag_id)
            .where(DagModel.is_paused == expression.true())
            .where(DagModel.dag_id.in_(dag_ids))
        )

        paused_dag_ids = {paused_dag_id for paused_dag_id, in paused_dag_ids}
        return paused_dag_ids

    def get_default_view(self) -> str:
        """Get the Default DAG View, returns the default config value if DagModel does not have a value."""
        # This is for backwards-compatibility with old dags that don't have None as default_view
        return self.default_view or airflow_conf.get_mandatory_value("webserver", "dag_default_view").lower()

    @property
    def safe_dag_id(self):
        return self.dag_id.replace(".", "__dot__")

    @property
    def relative_fileloc(self) -> pathlib.Path | None:
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
    def set_is_paused(self, is_paused: bool, including_subdags: bool = True, session=NEW_SESSION) -> None:
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
        session.execute(
            update(DagModel)
            .where(or_(*filter_query))
            .values(is_paused=is_paused)
            .execution_options(synchronize_session="fetch")
        )
        session.commit()

    @classmethod
    @internal_api_call
    @provide_session
    def deactivate_deleted_dags(
        cls,
        alive_dag_filelocs: Container[str],
        processor_subdir: str,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Set ``is_active=False`` on the DAGs for which the DAG files have been removed.

        :param alive_dag_filelocs: file paths of alive DAGs
        :param processor_subdir: dag processor subdir
        :param session: ORM Session
        """
        log.debug("Deactivating DAGs (for which DAG files are deleted) from %s table ", cls.__tablename__)
        dag_models = session.scalars(
            select(cls).where(
                cls.fileloc.is_not(None),
                or_(
                    cls.processor_subdir.is_(None),
                    cls.processor_subdir == processor_subdir,
                ),
            )
        )

        for dag_model in dag_models:
            if dag_model.fileloc not in alive_dag_filelocs:
                dag_model.is_active = False

    @classmethod
    def dags_needing_dagruns(cls, session: Session) -> tuple[Query, dict[str, tuple[datetime, datetime]]]:
        """
        Return (and lock) a list of Dag objects that are due to create a new DagRun.

        This will return a resultset of rows that is row-level-locked with a "SELECT ... FOR UPDATE" query,
        you should ensure that any scheduling decisions are made in a single transaction -- as soon as the
        transaction is committed it will be unlocked.
        """
        from airflow.models.dataset import DagScheduleDatasetReference, DatasetDagRunQueue as DDRQ

        # these dag ids are triggered by datasets, and they are ready to go.
        dataset_triggered_dag_info = {
            x.dag_id: (x.first_queued_time, x.last_queued_time)
            for x in session.execute(
                select(
                    DagScheduleDatasetReference.dag_id,
                    func.max(DDRQ.created_at).label("last_queued_time"),
                    func.min(DDRQ.created_at).label("first_queued_time"),
                )
                .join(DagScheduleDatasetReference.queue_records, isouter=True)
                .group_by(DagScheduleDatasetReference.dag_id)
                .having(func.count() == func.sum(case((DDRQ.target_dag_id.is_not(None), 1), else_=0)))
            )
        }
        dataset_triggered_dag_ids = set(dataset_triggered_dag_info)
        if dataset_triggered_dag_ids:
            exclusion_list = set(
                session.scalars(
                    select(DagModel.dag_id)
                    .join(DagRun.dag_model)
                    .where(DagRun.state.in_((DagRunState.QUEUED, DagRunState.RUNNING)))
                    .where(DagModel.dag_id.in_(dataset_triggered_dag_ids))
                    .group_by(DagModel.dag_id)
                    .having(func.count() >= func.max(DagModel.max_active_runs))
                )
            )
            if exclusion_list:
                dataset_triggered_dag_ids -= exclusion_list
                dataset_triggered_dag_info = {
                    k: v for k, v in dataset_triggered_dag_info.items() if k not in exclusion_list
                }

        # We limit so that _one_ scheduler doesn't try to do all the creation of dag runs
        query = (
            select(cls)
            .where(
                cls.is_paused == expression.false(),
                cls.is_active == expression.true(),
                cls.has_import_errors == expression.false(),
                or_(
                    cls.next_dagrun_create_after <= func.now(),
                    cls.dag_id.in_(dataset_triggered_dag_ids),
                ),
            )
            .order_by(cls.next_dagrun_create_after)
            .limit(cls.NUM_DAGS_PER_DAGRUN_QUERY)
        )

        return (
            session.scalars(with_row_locks(query, of=cls, session=session, **skip_locked(session=session))),
            dataset_triggered_dag_info,
        )

    def calculate_dagrun_date_fields(
        self,
        dag: DAG,
        most_recent_dag_run: None | datetime | DataInterval,
    ) -> None:
        """
        Calculate ``next_dagrun`` and `next_dagrun_create_after``.

        :param dag: The DAG object
        :param most_recent_dag_run: DataInterval (or datetime) of most recent run of this dag, or none
            if not yet scheduled.
        """
        most_recent_data_interval: DataInterval | None
        if isinstance(most_recent_dag_run, datetime):
            warnings.warn(
                "Passing a datetime to `DagModel.calculate_dagrun_date_fields` is deprecated. "
                "Provide a data interval instead.",
                RemovedInAirflow3Warning,
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

        log.info(
            "Setting next_dagrun for %s to %s, run_after=%s",
            dag.dag_id,
            self.next_dagrun,
            self.next_dagrun_create_after,
        )

    @provide_session
    def get_dataset_triggered_next_run_info(self, *, session=NEW_SESSION) -> dict[str, int | str] | None:
        if self.schedule_interval != "Dataset":
            return None
        return get_dataset_triggered_next_run_info([self.dag_id], session=session)[self.dag_id]


# NOTE: Please keep the list of arguments in sync with DAG.__init__.
# Only exception: dag_id here should have a default value, but not in DAG.
def dag(
    dag_id: str = "",
    description: str | None = None,
    schedule: ScheduleArg = NOTSET,
    schedule_interval: ScheduleIntervalArg = NOTSET,
    timetable: Timetable | None = None,
    start_date: datetime | None = None,
    end_date: datetime | None = None,
    full_filepath: str | None = None,
    template_searchpath: str | Iterable[str] | None = None,
    template_undefined: type[jinja2.StrictUndefined] = jinja2.StrictUndefined,
    user_defined_macros: dict | None = None,
    user_defined_filters: dict | None = None,
    default_args: dict | None = None,
    concurrency: int | None = None,
    max_active_tasks: int = airflow_conf.getint("core", "max_active_tasks_per_dag"),
    max_active_runs: int = airflow_conf.getint("core", "max_active_runs_per_dag"),
    dagrun_timeout: timedelta | None = None,
    sla_miss_callback: None | SLAMissCallback | list[SLAMissCallback] = None,
    default_view: str = airflow_conf.get_mandatory_value("webserver", "dag_default_view").lower(),
    orientation: str = airflow_conf.get_mandatory_value("webserver", "dag_orientation"),
    catchup: bool = airflow_conf.getboolean("scheduler", "catchup_by_default"),
    on_success_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None,
    on_failure_callback: None | DagStateChangeCallback | list[DagStateChangeCallback] = None,
    doc_md: str | None = None,
    params: collections.abc.MutableMapping | None = None,
    access_control: dict | None = None,
    is_paused_upon_creation: bool | None = None,
    jinja_environment_kwargs: dict | None = None,
    render_template_as_native_obj: bool = False,
    tags: list[str] | None = None,
    owner_links: dict[str, str] | None = None,
    auto_register: bool = True,
    fail_stop: bool = False,
) -> Callable[[Callable], Callable[..., DAG]]:
    """
    Python dag decorator which wraps a function into an Airflow DAG.

    Accepts kwargs for operator kwarg. Can be used to parameterize DAGs.

    :param dag_args: Arguments for DAG object
    :param dag_kwargs: Kwargs for DAG object.
    """

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
            with DAG(
                dag_id or f.__name__,
                description=description,
                schedule_interval=schedule_interval,
                timetable=timetable,
                start_date=start_date,
                end_date=end_date,
                full_filepath=full_filepath,
                template_searchpath=template_searchpath,
                template_undefined=template_undefined,
                user_defined_macros=user_defined_macros,
                user_defined_filters=user_defined_filters,
                default_args=default_args,
                concurrency=concurrency,
                max_active_tasks=max_active_tasks,
                max_active_runs=max_active_runs,
                dagrun_timeout=dagrun_timeout,
                sla_miss_callback=sla_miss_callback,
                default_view=default_view,
                orientation=orientation,
                catchup=catchup,
                on_success_callback=on_success_callback,
                on_failure_callback=on_failure_callback,
                doc_md=doc_md,
                params=params,
                access_control=access_control,
                is_paused_upon_creation=is_paused_upon_creation,
                jinja_environment_kwargs=jinja_environment_kwargs,
                render_template_as_native_obj=render_template_as_native_obj,
                tags=tags,
                schedule=schedule,
                owner_links=owner_links,
                auto_register=auto_register,
                fail_stop=fail_stop,
            ) as dag_obj:
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


STATICA_HACK = True
globals()["kcah_acitats"[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.models.serialized_dag import SerializedDagModel

    DagModel.serialized_dag = relationship(SerializedDagModel)
    """:sphinx-autoapi-skip:"""


class DagContext:
    """
    DAG context is used to keep the current DAG when DAG is used as ContextManager.

    You can use DAG as context:

    .. code-block:: python

        with DAG(
            dag_id="example_dag",
            default_args=default_args,
            schedule="0 0 * * *",
            dagrun_timeout=timedelta(minutes=60),
        ) as dag:
            ...

    If you do this the context stores the DAG and whenever new task is created, it will use
    such stored DAG as the parent DAG.

    """

    _context_managed_dags: collections.deque[DAG] = deque()
    autoregistered_dags: set[tuple[DAG, ModuleType]] = set()
    current_autoregister_module_name: str | None = None

    @classmethod
    def push_context_managed_dag(cls, dag: DAG):
        cls._context_managed_dags.appendleft(dag)

    @classmethod
    def pop_context_managed_dag(cls) -> DAG | None:
        dag = cls._context_managed_dags.popleft()

        # In a few cases around serialization we explicitly push None in to the stack
        if cls.current_autoregister_module_name is not None and dag and dag.auto_register:
            mod = sys.modules[cls.current_autoregister_module_name]
            cls.autoregistered_dags.add((dag, mod))

        return dag

    @classmethod
    def get_current_dag(cls) -> DAG | None:
        try:
            return cls._context_managed_dags[0]
        except IndexError:
            return None


def _triggerer_is_healthy():
    from airflow.jobs.triggerer_job_runner import TriggererJobRunner

    job = TriggererJobRunner.most_recent_job()
    return job and job.is_alive()


def _run_task(ti: TaskInstance, session) -> TaskReturnCode | None:
    """
    Run a single task instance, and push result to Xcom for downstream tasks.

    Bypasses a lot of extra steps used in `task.run` to keep our local running as fast as
    possible.  This function is only meant for the `dag.test` function as a helper function.

    Args:
        ti: TaskInstance to run
    """
    ret = None
    log.info("*****************************************************")
    if ti.map_index > 0:
        log.info("Running task %s index %d", ti.task_id, ti.map_index)
    else:
        log.info("Running task %s", ti.task_id)
    try:
        ret = ti._run_raw_task(session=session)
        session.flush()
        log.info("%s ran successfully!", ti.task_id)
    except AirflowSkipException:
        log.info("Task Skipped, continuing")
    log.info("*****************************************************")
    return ret


def _get_or_create_dagrun(
    dag: DAG,
    conf: dict[Any, Any] | None,
    start_date: datetime,
    execution_date: datetime,
    run_id: str,
    session: Session,
    data_interval: tuple[datetime, datetime] | None = None,
) -> DagRun:
    """Create a DAG run, replacing an existing instance if needed to prevent collisions.

    This function is only meant to be used by :meth:`DAG.test` as a helper function.

    :param dag: DAG to be used to find run.
    :param conf: Configuration to pass to newly created run.
    :param start_date: Start date of new run.
    :param execution_date: Logical date for finding an existing run.
    :param run_id: Run ID for the new DAG run.

    :return: The newly created DAG run.
    """
    log.info("dagrun id: %s", dag.dag_id)
    dr: DagRun = session.scalar(
        select(DagRun).where(DagRun.dag_id == dag.dag_id, DagRun.execution_date == execution_date)
    )
    if dr:
        session.delete(dr)
        session.commit()
    dr = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_id=run_id,
        start_date=start_date or execution_date,
        session=session,
        conf=conf,
        data_interval=data_interval,
    )
    log.info("created dagrun %s", dr)
    return dr
