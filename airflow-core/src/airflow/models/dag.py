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
import logging
import re
from collections import defaultdict
from collections.abc import Collection, Generator, Iterable, Sequence
from datetime import datetime, timedelta
from functools import cache
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    TypeVar,
    Union,
    cast,
    overload,
)

import attrs
import methodtools
import pendulum
import sqlalchemy_jsonfield
from dateutil.relativedelta import relativedelta
from packaging import version as packaging_version
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
    or_,
    select,
    tuple_,
    update,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import backref, load_only, relationship
from sqlalchemy.sql import Select, expression

from airflow import settings, utils
from airflow.assets.evaluation import AssetEvaluator
from airflow.configuration import conf as airflow_conf
from airflow.exceptions import (
    AirflowException,
    UnknownExecutorException,
)
from airflow.executors.executor_loader import ExecutorLoader
from airflow.models.asset import (
    AssetDagRunQueue,
    AssetModel,
)
from airflow.models.base import Base, StringID
from airflow.models.baseoperator import BaseOperator
from airflow.models.dag_version import DagVersion
from airflow.models.dagrun import RUN_ID_REGEX, DagRun
from airflow.models.taskinstance import (
    TaskInstance,
    TaskInstanceKey,
    clear_task_instances,
)
from airflow.models.tasklog import LogTemplate
from airflow.sdk import TaskGroup
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetUniqueKey, BaseAsset
from airflow.sdk.definitions.dag import DAG as TaskSDKDag, dag as task_sdk_dag_decorator
from airflow.settings import json
from airflow.timetables.base import DagRunInfo, DataInterval, TimeRestriction, Timetable
from airflow.timetables.interval import CronDataIntervalTimetable, DeltaDataIntervalTimetable
from airflow.timetables.simple import (
    AssetTriggeredTimetable,
    NullTimetable,
    OnceTimetable,
)
from airflow.utils import timezone
from airflow.utils.context import Context
from airflow.utils.dag_cycle_tester import check_cycle
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import UtcDateTime, lock_rows, with_row_locks
from airflow.utils.state import DagRunState, TaskInstanceState
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from pydantic import NonNegativeInt
    from sqlalchemy.orm.query import Query
    from sqlalchemy.orm.session import Session

    from airflow.models.dagbag import DagBag
    from airflow.models.operator import Operator
    from airflow.serialization.serialized_objects import MaybeSerializedDAG
    from airflow.typing_compat import Literal

log = logging.getLogger(__name__)

AssetT = TypeVar("AssetT", bound=BaseAsset)

TAG_MAX_LEN = 100

DagStateChangeCallback = Callable[[Context], None]
ScheduleInterval = Union[None, str, timedelta, relativedelta]

ScheduleArg = Union[
    ScheduleInterval,
    Timetable,
    BaseAsset,
    Collection[Union["Asset", "AssetAlias"]],
]


class InconsistentDataInterval(AirflowException):
    """
    Exception raised when a model populates data interval fields incorrectly.

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
    if end is None:
        raise InconsistentDataInterval(instance, start_field_name, end_field_name)
    return DataInterval(start, end)


def get_last_dagrun(dag_id, session, include_manually_triggered=False):
    """
    Return the last dag run for a dag, None if there was none.

    Last dag run can be any type of run e.g. scheduled or backfilled.
    Overridden DagRuns are ignored.
    """
    DR = DagRun
    query = select(DR).where(DR.dag_id == dag_id, DR.logical_date.is_not(None))
    if not include_manually_triggered:
        query = query.where(DR.run_type != DagRunType.MANUAL)
    query = query.order_by(DR.logical_date.desc())
    return session.scalar(query.limit(1))


def get_asset_triggered_next_run_info(
    dag_ids: list[str], *, session: Session
) -> dict[str, dict[str, int | str]]:
    """
    Get next run info for a list of dag_ids.

    Given a list of dag_ids, get string representing how close any that are asset triggered are
    their next run, e.g. "1 of 2 assets updated".
    """
    from airflow.models.asset import AssetDagRunQueue as ADRQ, DagScheduleAssetReference

    return {
        x.dag_id: {
            "uri": x.uri,
            "ready": x.ready,
            "total": x.total,
        }
        for x in session.execute(
            select(
                DagScheduleAssetReference.dag_id,
                # This is a dirty hack to workaround group by requiring an aggregate,
                # since grouping by asset is not what we want to do here...but it works
                case((func.count() == 1, func.max(AssetModel.uri)), else_="").label("uri"),
                func.count().label("total"),
                func.sum(case((ADRQ.target_dag_id.is_not(None), 1), else_=0)).label("ready"),
            )
            .join(
                ADRQ,
                and_(
                    ADRQ.asset_id == DagScheduleAssetReference.asset_id,
                    ADRQ.target_dag_id == DagScheduleAssetReference.dag_id,
                ),
                isouter=True,
            )
            .join(AssetModel, AssetModel.id == DagScheduleAssetReference.asset_id)
            .group_by(DagScheduleAssetReference.dag_id)
            .where(DagScheduleAssetReference.dag_id.in_(dag_ids))
        ).all()
    }


def _triggerer_is_healthy(session: Session):
    from airflow.jobs.triggerer_job_runner import TriggererJobRunner

    job = TriggererJobRunner.most_recent_job(session=session)
    return job and job.is_alive()


@provide_session
def _create_orm_dagrun(
    *,
    dag: DAG,
    run_id: str,
    logical_date: datetime | None,
    data_interval: DataInterval | None,
    run_after: datetime,
    start_date: datetime | None,
    conf: Any,
    state: DagRunState | None,
    run_type: DagRunType,
    creating_job_id: int | None,
    backfill_id: NonNegativeInt | None,
    triggered_by: DagRunTriggeredByType,
    session: Session = NEW_SESSION,
) -> DagRun:
    bundle_version = None
    if not dag.disable_bundle_versioning:
        bundle_version = session.scalar(
            select(DagModel.bundle_version).where(DagModel.dag_id == dag.dag_id),
        )
    dag_version = DagVersion.get_latest_version(dag.dag_id, session=session)
    run = DagRun(
        dag_id=dag.dag_id,
        run_id=run_id,
        logical_date=logical_date,
        start_date=start_date,
        run_after=run_after,
        conf=conf,
        state=state,
        run_type=run_type,
        creating_job_id=creating_job_id,
        data_interval=data_interval,
        triggered_by=triggered_by,
        backfill_id=backfill_id,
        bundle_version=bundle_version,
    )
    # Load defaults into the following two fields to ensure result can be serialized detached
    run.log_template_id = int(session.scalar(select(func.max(LogTemplate.__table__.c.id))))
    run.created_dag_version = dag_version
    run.consumed_asset_events = []
    session.add(run)
    session.flush()
    run.dag = dag
    # create the associated task instances
    # state is None at the moment of creation
    run.verify_integrity(session=session, dag_version_id=dag_version.id if dag_version else None)
    return run


if TYPE_CHECKING:
    dag = task_sdk_dag_decorator
else:

    def dag(dag_id: str = "", **kwargs):
        return task_sdk_dag_decorator(dag_id, __DAG_class=DAG, __warnings_stacklevel_delta=3, **kwargs)


def _convert_max_consecutive_failed_dag_runs(val: int) -> int:
    if val == 0:
        val = airflow_conf.getint("core", "max_consecutive_failed_dag_runs_per_dag")
    if val < 0:
        raise ValueError(
            f"Invalid max_consecutive_failed_dag_runs: {val}. Requires max_consecutive_failed_dag_runs >= 0"
        )
    return val


@functools.total_ordering
@attrs.define(hash=False, repr=False, eq=False, slots=False)
class DAG(TaskSDKDag, LoggingMixin):
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
        (timetable), or asset-driven triggers.

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
    :param tags: List of tags to help filtering DAGs in the UI.
    :param owner_links: Dict of owners and their links, that will be clickable on the DAGs view UI.
        Can be used as an HTTP link (for example the link to your Slack channel), or a mailto link.
        e.g: {"dag_owner": "https://airflow.apache.org/"}
    :param auto_register: Automatically register this DAG when it is used in a ``with`` block
    :param fail_fast: Fails currently running tasks when task in DAG fails.
        **Warning**: A fail fast dag can only have tasks with the default trigger rule ("all_success").
        An exception will be thrown if any task in a fail fast dag has a non default trigger rule.
    :param dag_display_name: The display name of the DAG which appears on the UI.
    """

    partial: bool = False
    last_loaded: datetime | None = attrs.field(factory=timezone.utcnow)

    # this will only be set at serialization time
    # it's only use is for determining the relative fileloc based only on the serialize dag
    _processor_dags_folder: str | None = attrs.field(init=False, default=None)

    # Override the default from parent class to use config
    max_consecutive_failed_dag_runs: int = attrs.field(
        default=0,
        converter=_convert_max_consecutive_failed_dag_runs,
        validator=attrs.validators.instance_of(int),
    )

    @property
    def safe_dag_id(self):
        return self.dag_id.replace(".", "__dot__")

    def validate(self):
        super().validate()
        self.validate_executor_field()

    def validate_executor_field(self):
        for task in self.tasks:
            if task.executor:
                try:
                    ExecutorLoader.lookup_executor_name_by_str(task.executor)
                except UnknownExecutorException:
                    raise UnknownExecutorException(
                        f"The specified executor {task.executor} for task {task.task_id} is not "
                        "configured. Review the core.executors Airflow configuration to add it or "
                        "update the executor configuration for this task."
                    )

    @staticmethod
    def _upgrade_outdated_dag_access_control(access_control=None):
        """Look for outdated dag level actions in DAG access_controls and replace them with updated actions."""
        if access_control is None:
            return None

        from airflow.providers.fab import __version__ as FAB_VERSION
        from airflow.providers.fab.www.security import permissions

        updated_access_control = {}
        for role, perms in access_control.items():
            if packaging_version.parse(FAB_VERSION) >= packaging_version.parse("1.3.0"):
                updated_access_control[role] = updated_access_control.get(role, {})
                if isinstance(perms, (set, list)):
                    # Support for old-style access_control where only the actions are specified
                    updated_access_control[role][permissions.RESOURCE_DAG] = set(perms)
                else:
                    updated_access_control[role] = perms
            elif isinstance(perms, dict):
                # Not allow new access control format with old FAB versions
                raise AirflowException(
                    "Please upgrade the FAB provider to a version >= 1.3.0 to allow "
                    "use the Dag Level Access Control new format."
                )
            else:
                updated_access_control[role] = set(perms)

        return updated_access_control

    def get_next_data_interval(self, dag_model: DagModel) -> DataInterval | None:
        """
        Get the data interval of the next scheduled run.

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
        """
        Get the data interval of this run.

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
        return self.infer_automated_data_interval(run.logical_date)

    def infer_automated_data_interval(self, logical_date: datetime) -> DataInterval:
        """
        Infer a data interval for a run against this DAG.

        This method is used to bridge runs created prior to AIP-39
        implementation, which do not have an explicit data interval. Therefore,
        this method only considers ``schedule_interval`` values valid prior to
        Airflow 2.2.

        DO NOT call this method if there is a known data interval.

        :meta private:
        """
        timetable_type = type(self.timetable)
        if issubclass(timetable_type, (NullTimetable, OnceTimetable, AssetTriggeredTimetable)):
            return DataInterval.exact(timezone.coerce_datetime(logical_date))
        start = timezone.coerce_datetime(logical_date)
        if issubclass(timetable_type, CronDataIntervalTimetable):
            end = cast("CronDataIntervalTimetable", self.timetable)._get_next(start)
        elif issubclass(timetable_type, DeltaDataIntervalTimetable):
            end = cast("DeltaDataIntervalTimetable", self.timetable)._get_next(start)
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
        last_automated_dagrun: None | DataInterval,
        *,
        restricted: bool = True,
    ) -> DagRunInfo | None:
        """
        Get information about the next DagRun of this dag after ``date_last_automated_dagrun``.

        This calculates what time interval the next DagRun should operate on
        (its logical date) and when it can be scheduled, according to the
        dag's timetable, start_date, end_date, etc. This doesn't check max
        active run or any other "max_active_tasks" type limits, but only
        performs calculations based on the various date and interval fields of
        this dag and its tasks.

        :param last_automated_dagrun: The ``max(logical_date)`` of
            existing "automated" DagRuns for this dag (scheduled or backfill,
            but not manual).
        :param restricted: If set to *False* (default is *True*), ignore
            ``start_date``, ``end_date``, and ``catchup`` specified on the DAG
            or tasks.
        :return: DagRunInfo of the next dagrun, or None if a dagrun is not
            going to be scheduled.
        """
        data_interval = None
        if isinstance(last_automated_dagrun, datetime):
            raise ValueError(
                "Passing a datetime to DAG.next_dagrun_info is not supported anymore. Use a DataInterval instead."
            )
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

    @functools.cached_property
    def _time_restriction(self) -> TimeRestriction:
        start_dates = [t.start_date for t in self.tasks if t.start_date]
        if self.start_date is not None:
            start_dates.append(self.start_date)
        earliest = None
        if start_dates:
            earliest = timezone.coerce_datetime(min(start_dates))
        latest = timezone.coerce_datetime(self.end_date)
        end_dates = [t.end_date for t in self.tasks if t.end_date]
        if len(end_dates) == len(self.tasks):  # not exists null end_date
            if self.end_date is not None:
                end_dates.append(self.end_date)
            if end_dates:
                latest = timezone.coerce_datetime(max(end_dates))
        return TimeRestriction(earliest, latest, self.catchup)

    def iter_dagrun_infos_between(
        self,
        earliest: pendulum.DateTime | datetime | None,
        latest: pendulum.DateTime | datetime,
        *,
        align: bool = True,
    ) -> Iterable[DagRunInfo]:
        """
        Yield DagRunInfo using this DAG's timetable between given interval.

        DagRunInfo instances yielded if their ``logical_date`` is not earlier
        than ``earliest``, nor later than ``latest``. The instances are ordered
        by their ``logical_date`` from earliest to latest.

        If ``align`` is ``False``, the first run will happen immediately on
        ``earliest``, even if it does not fall on the logical timetable schedule.
        The default is ``True``.

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

    @provide_session
    def get_last_dagrun(self, session=NEW_SESSION, include_manually_triggered=False):
        return get_last_dagrun(
            self.dag_id, session=session, include_manually_triggered=include_manually_triggered
        )

    @provide_session
    def has_dag_runs(self, session=NEW_SESSION, include_manually_triggered=True) -> bool:
        return (
            get_last_dagrun(
                self.dag_id, session=session, include_manually_triggered=include_manually_triggered
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
    def timetable_summary(self) -> str:
        return self.timetable.summary

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

    @provide_session
    def get_is_active(self, session=NEW_SESSION) -> None:
        """Return a boolean indicating whether this DAG is active."""
        return session.scalar(select(~DagModel.is_stale).where(DagModel.dag_id == self.dag_id))

    @provide_session
    def get_is_stale(self, session=NEW_SESSION) -> None:
        """Return a boolean indicating whether this DAG is stale."""
        return session.scalar(select(DagModel.is_stale).where(DagModel.dag_id == self.dag_id))

    @provide_session
    def get_is_paused(self, session=NEW_SESSION) -> None:
        """Return a boolean indicating whether this DAG is paused."""
        return session.scalar(select(DagModel.is_paused).where(DagModel.dag_id == self.dag_id))

    @provide_session
    def get_bundle_name(self, session=NEW_SESSION) -> str | None:
        """Return the bundle name this DAG is in."""
        return session.scalar(select(DagModel.bundle_name).where(DagModel.dag_id == self.dag_id))

    @provide_session
    def get_bundle_version(self, session=NEW_SESSION) -> str | None:
        """Return the bundle version that was seen when this dag was processed."""
        return session.scalar(select(DagModel.bundle_version).where(DagModel.dag_id == self.dag_id))

    @methodtools.lru_cache(maxsize=None)
    @classmethod
    def get_serialized_fields(cls):
        """Stringified DAGs and operators contain exactly these fields."""
        return TaskSDKDag.get_serialized_fields() | {"_processor_dags_folder"}

    def get_active_runs(self):
        """
        Return a list of dag run logical dates currently running.

        :return: List of logical dates
        """
        runs = DagRun.find(dag_id=self.dag_id, state=DagRunState.RUNNING)

        active_dates = []
        for run in runs:
            active_dates.append(run.logical_date)

        return active_dates

    @staticmethod
    @provide_session
    def fetch_dagrun(dag_id: str, run_id: str, session: Session = NEW_SESSION) -> DagRun:
        """
        Return the dag run for a given run_id if it exists, otherwise none.

        :param dag_id: The dag_id of the DAG to find.
        :param run_id: The run_id of the DagRun to find.
        :param session:
        :return: The DagRun if found, otherwise None.
        """
        return session.scalar(select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id))

    @provide_session
    def get_dagrun(self, run_id: str, session: Session = NEW_SESSION) -> DagRun:
        return DAG.fetch_dagrun(dag_id=self.dag_id, run_id=run_id, session=session)

    @provide_session
    def get_dagruns_between(self, start_date, end_date, session=NEW_SESSION):
        """
        Return the list of dag runs between start_date (inclusive) and end_date (inclusive).

        :param start_date: The starting logical date of the DagRun to find.
        :param end_date: The ending logical date of the DagRun to find.
        :param session:
        :return: The list of DagRuns found.
        """
        dagruns = session.scalars(
            select(DagRun).where(
                DagRun.dag_id == self.dag_id,
                DagRun.logical_date >= start_date,
                DagRun.logical_date <= end_date,
            )
        ).all()

        return dagruns

    @provide_session
    def get_latest_logical_date(self, session: Session = NEW_SESSION) -> pendulum.DateTime | None:
        """Return the latest date for which at least one dag run exists."""
        return session.scalar(select(func.max(DagRun.logical_date)).where(DagRun.dag_id == self.dag_id))

    @provide_session
    def get_task_instances_before(
        self,
        base_date: datetime,
        num: int,
        *,
        session: Session = NEW_SESSION,
    ) -> list[TaskInstance]:
        """
        Get ``num`` task instances before (including) ``base_date``.

        The returned list may contain exactly ``num`` task instances
        corresponding to any DagRunType. It can have less if there are
        less than ``num`` scheduled DAG runs before ``base_date``.
        """
        logical_dates: list[Any] = session.execute(
            select(DagRun.logical_date)
            .where(
                DagRun.dag_id == self.dag_id,
                DagRun.logical_date <= base_date,
            )
            .order_by(DagRun.logical_date.desc())
            .limit(num)
        ).all()

        if not logical_dates:
            return self.get_task_instances(start_date=base_date, end_date=base_date, session=session)

        min_date: datetime | None = logical_dates[-1]._mapping.get(
            "logical_date"
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
            include_dependent_dags=False,
            exclude_task_ids=(),
            exclude_run_ids=None,
            session=session,
        )
        return session.scalars(cast("Select", query).order_by(DagRun.logical_date)).all()

    @overload
    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        start_date: datetime | None,
        end_date: datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        include_dependent_dags: bool,
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        exclude_run_ids: frozenset[str] | None,
        session: Session,
        dag_bag: DagBag | None = ...,
    ) -> Iterable[TaskInstance]: ...  # pragma: no cover

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
        include_dependent_dags: bool,
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        exclude_run_ids: frozenset[str] | None,
        session: Session,
        dag_bag: DagBag | None = ...,
        recursion_depth: int = ...,
        max_recursion_depth: int = ...,
        visited_external_tis: set[TaskInstanceKey] = ...,
    ) -> set[TaskInstanceKey]: ...  # pragma: no cover

    def _get_task_instances(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None,
        as_pk_tuple: Literal[True, None] = None,
        start_date: datetime | None,
        end_date: datetime | None,
        run_id: str | None,
        state: TaskInstanceState | Sequence[TaskInstanceState],
        include_dependent_dags: bool,
        exclude_task_ids: Collection[str | tuple[str, int]] | None,
        exclude_run_ids: frozenset[str] | None,
        session: Session,
        dag_bag: DagBag | None = None,
        recursion_depth: int = 0,
        max_recursion_depth: int | None = None,
        visited_external_tis: set[TaskInstanceKey] | None = None,
    ) -> Iterable[TaskInstance] | set[TaskInstanceKey]:
        TI = TaskInstance

        # If we are looking at dependent dags we want to avoid UNION calls
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

        if self.partial:
            tis = tis.where(TaskInstance.dag_id == self.dag_id, TaskInstance.task_id.in_(self.task_ids))
        else:
            tis = tis.where(TaskInstance.dag_id == self.dag_id)
        if run_id:
            tis = tis.where(TaskInstance.run_id == run_id)
        if start_date:
            tis = tis.where(DagRun.logical_date >= start_date)
        if task_ids is not None:
            tis = tis.where(TaskInstance.ti_selector_condition(task_ids))
        if end_date:
            tis = tis.where(DagRun.logical_date <= end_date)

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

        if exclude_run_ids:
            tis = tis.where(TaskInstance.run_id.not_in(exclude_run_ids))

        if include_dependent_dags:
            # Recursively find external tasks indicated by ExternalTaskMarker
            from airflow.providers.standard.sensors.external_task import ExternalTaskMarker

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

                task: ExternalTaskMarker = cast("ExternalTaskMarker", copy.copy(self.get_task(ti.task_id)))
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
                        DagRun.logical_date == pendulum.parse(task.logical_date),
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
                        task_ids=[tii.task_id],
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
                            include_dependent_dags=include_dependent_dags,
                            as_pk_tuple=True,
                            exclude_task_ids=exclude_task_ids,
                            exclude_run_ids=exclude_run_ids,
                            dag_bag=dag_bag,
                            session=session,
                            recursion_depth=recursion_depth + 1,
                            max_recursion_depth=max_recursion_depth,
                            visited_external_tis=visited_external_tis,
                        )
                    )

        if result or as_pk_tuple:
            # Only execute the `ti` query if we have also collected some other results
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
            tis = tis.where(tuple_(TI.task_id, TI.map_index).not_in(exclude_task_ids))

        return tis

    @provide_session
    def set_task_instance_state(
        self,
        *,
        task_id: str,
        map_indexes: Collection[int] | None = None,
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
        :param run_id: The run_id of the TaskInstance
        :param state: State to set the TaskInstance to
        :param upstream: Include all upstream tasks of the given task_id
        :param downstream: Include all downstream tasks of the given task_id
        :param future: Include all future TaskInstances of the given task_id
        :param commit: Commit changes
        :param past: Include all past TaskInstances of the given task_id
        """
        from airflow.api.common.mark_tasks import set_state

        task = self.get_task(task_id)
        task.dag = self

        tasks_to_set_state: list[Operator | tuple[Operator, int]]
        if map_indexes is None:
            tasks_to_set_state = [task]
        else:
            tasks_to_set_state = [(task, map_index) for map_index in map_indexes]

        altered = set_state(
            tasks=tasks_to_set_state,
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
        subset = self.partial_subset(
            task_ids={task_id},
            include_downstream=True,
            include_upstream=False,
        )

        # Raises an error if not found
        dr_id, logical_date = session.execute(
            select(DagRun.id, DagRun.logical_date).where(
                DagRun.run_id == run_id, DagRun.dag_id == self.dag_id
            )
        ).one()

        # Now we want to clear downstreams of tasks that had their state set...
        clear_kwargs = {
            "only_failed": True,
            "session": session,
            # Exclude the task itself from being cleared.
            "exclude_task_ids": frozenset((task_id,)),
        }
        if not future and not past:  # Simple case 1: we're only dealing with exactly one run.
            clear_kwargs["run_id"] = run_id
            subset.clear(**clear_kwargs)
        elif future and past:  # Simple case 2: we're clearing ALL runs.
            subset.clear(**clear_kwargs)
        else:  # Complex cases: we may have more than one run, based on a date range.
            # Make 'future' and 'past' make some sense when multiple runs exist
            # for the same logical date. We order runs by their id and only
            # clear runs have larger/smaller ids.
            exclude_run_id_stmt = select(DagRun.run_id).where(DagRun.logical_date == logical_date)
            if future:
                clear_kwargs["start_date"] = logical_date
                exclude_run_id_stmt = exclude_run_id_stmt.where(DagRun.id > dr_id)
            else:
                clear_kwargs["end_date"] = logical_date
                exclude_run_id_stmt = exclude_run_id_stmt.where(DagRun.id < dr_id)
            subset.clear(exclude_run_ids=frozenset(session.scalars(exclude_run_id_stmt)), **clear_kwargs)
        return altered

    @provide_session
    def set_task_group_state(
        self,
        *,
        group_id: str,
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

        tasks_to_set_state: list[BaseOperator | tuple[BaseOperator, int]] = []
        task_ids: list[str] = []

        task_group_dict = self.task_group.get_task_group_dict()
        task_group = task_group_dict.get(group_id)
        if task_group is None:
            raise ValueError("TaskGroup {group_id} could not be found")
        tasks_to_set_state = [task for task in task_group.iter_tasks() if isinstance(task, BaseOperator)]
        task_ids = [task.task_id for task in task_group.iter_tasks()]
        dag_runs_query = select(DagRun.id).where(DagRun.dag_id == self.dag_id)

        @cache
        def get_logical_date() -> datetime:
            stmt = select(DagRun.logical_date).where(DagRun.run_id == run_id, DagRun.dag_id == self.dag_id)
            return session.scalars(stmt).one()  # Raises an error if not found

        end_date = None if future else get_logical_date()
        start_date = None if past else get_logical_date()

        if future:
            dag_runs_query = dag_runs_query.where(DagRun.logical_date <= start_date)
        if past:
            dag_runs_query = dag_runs_query.where(DagRun.logical_date >= end_date)
        if not future and not past:
            dag_runs_query = dag_runs_query.where(DagRun.run_id == run_id)

        with lock_rows(dag_runs_query, session):
            altered = set_state(
                tasks=tasks_to_set_state,
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
            subset = self.partial_subset(
                task_ids=task_ids,
                include_downstream=True,
                include_upstream=False,
            )

            subset.clear(
                start_date=start_date,
                end_date=end_date,
                only_failed=True,
                session=session,
                # Exclude the task from the current group from being cleared
                exclude_task_ids=frozenset(task_ids),
            )

        return altered

    @overload
    def clear(
        self,
        *,
        dry_run: Literal[True],
        task_ids: Collection[str | tuple[str, int]] | None = None,
        run_id: str,
        only_failed: bool = False,
        only_running: bool = False,
        confirm_prompt: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        session: Session = NEW_SESSION,
        dag_bag: DagBag | None = None,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
    ) -> list[TaskInstance]: ...  # pragma: no cover

    @overload
    def clear(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        run_id: str,
        only_failed: bool = False,
        only_running: bool = False,
        confirm_prompt: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: Literal[False] = False,
        session: Session = NEW_SESSION,
        dag_bag: DagBag | None = None,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
    ) -> int: ...  # pragma: no cover

    @overload
    def clear(
        self,
        *,
        dry_run: Literal[True],
        task_ids: Collection[str | tuple[str, int]] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        confirm_prompt: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        session: Session = NEW_SESSION,
        dag_bag: DagBag | None = None,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
    ) -> list[TaskInstance]: ...  # pragma: no cover

    @overload
    def clear(
        self,
        *,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        confirm_prompt: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: Literal[False] = False,
        session: Session = NEW_SESSION,
        dag_bag: DagBag | None = None,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
    ) -> int: ...  # pragma: no cover

    @provide_session
    def clear(
        self,
        task_ids: Collection[str | tuple[str, int]] | None = None,
        *,
        run_id: str | None = None,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        only_failed: bool = False,
        only_running: bool = False,
        confirm_prompt: bool = False,
        dag_run_state: DagRunState = DagRunState.QUEUED,
        dry_run: bool = False,
        session: Session = NEW_SESSION,
        dag_bag: DagBag | None = None,
        exclude_task_ids: frozenset[str] | frozenset[tuple[str, int]] | None = frozenset(),
        exclude_run_ids: frozenset[str] | None = frozenset(),
    ) -> int | Iterable[TaskInstance]:
        """
        Clear a set of task instances associated with the current dag for a specified date range.

        :param task_ids: List of task ids or (``task_id``, ``map_index``) tuples to clear
        :param run_id: The run_id for which the tasks should be cleared
        :param start_date: The minimum logical_date to clear
        :param end_date: The maximum logical_date to clear
        :param only_failed: Only clear failed tasks
        :param only_running: Only clear running tasks.
        :param confirm_prompt: Ask for confirmation
        :param dag_run_state: state to set DagRun to. If set to False, dagrun state will not
            be changed.
        :param dry_run: Find the tasks to clear but don't clear them.
        :param session: The sqlalchemy session to use
        :param dag_bag: The DagBag used to find the dags (Optional)
        :param exclude_task_ids: A set of ``task_id`` or (``task_id``, ``map_index``)
            tuples that should not be cleared
        :param exclude_run_ids: A set of ``run_id`` or (``run_id``)
        """
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
            run_id=run_id,
            state=state,
            include_dependent_dags=True,
            session=session,
            dag_bag=dag_bag,
            exclude_task_ids=exclude_task_ids,
            exclude_run_ids=exclude_run_ids,
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
        dag_run_state=DagRunState.QUEUED,
        dry_run=False,
    ):
        all_tis = []
        for dag in dags:
            if not isinstance(dag, DAG):
                dag = DAG.from_sdk_dag(dag)
            tis = dag.clear(
                start_date=start_date,
                end_date=end_date,
                only_failed=only_failed,
                only_running=only_running,
                confirm_prompt=False,
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
                if not isinstance(dag, DAG):
                    dag = DAG.from_sdk_dag(dag)
                dag.clear(
                    start_date=start_date,
                    end_date=end_date,
                    only_failed=only_failed,
                    only_running=only_running,
                    confirm_prompt=False,
                    dag_run_state=dag_run_state,
                    dry_run=False,
                )
        else:
            count = 0
            print("Cancelled, nothing was cleared.")
        return count

    def cli(self):
        """Exposes a CLI specific to this DAG."""
        check_cycle(self)

        from airflow.cli import cli_parser

        parser = cli_parser.get_parser(dag_parser=True)
        args = parser.parse_args()
        args.func(args, self)

    @provide_session
    def create_dagrun(
        self,
        *,
        run_id: str,
        logical_date: datetime | None = None,
        data_interval: tuple[datetime, datetime] | None = None,
        run_after: datetime,
        conf: dict | None = None,
        run_type: DagRunType,
        triggered_by: DagRunTriggeredByType,
        state: DagRunState,
        start_date: datetime | None = None,
        creating_job_id: int | None = None,
        backfill_id: NonNegativeInt | None = None,
        session: Session = NEW_SESSION,
    ) -> DagRun:
        """
        Create a run for this DAG to run its tasks.

        :param start_date: the date this dag run should be evaluated
        :param conf: Dict containing configuration/parameters to pass to the DAG
        :param creating_job_id: ID of the job creating this DagRun
        :param backfill_id: ID of the backfill run if one exists
        :return: The created DAG run.

        :meta private:
        """
        logical_date = timezone.coerce_datetime(logical_date)
        # For manual runs where logical_date is None, ensure no data_interval is set.
        if logical_date is None and data_interval is not None:
            raise ValueError("data_interval must be None when logical_date is None")

        if data_interval and not isinstance(data_interval, DataInterval):
            data_interval = DataInterval(*map(timezone.coerce_datetime, data_interval))

        if isinstance(run_type, DagRunType):
            pass
        elif isinstance(run_type, str):  # Ensure the input value is valid.
            run_type = DagRunType(run_type)
        else:
            raise ValueError(f"run_type should be a DagRunType, not {type(run_type)}")

        if not isinstance(run_id, str):
            raise ValueError(f"`run_id` should be a str, not {type(run_id)}")

        # This is also done on the DagRun model class, but SQLAlchemy column
        # validator does not work well for some reason.
        if not re.match(RUN_ID_REGEX, run_id):
            regex = airflow_conf.get("scheduler", "allowed_run_id_pattern").strip()
            if not regex or not re.match(regex, run_id):
                raise ValueError(
                    f"The run_id provided '{run_id}' does not match regex pattern "
                    f"'{regex}' or '{RUN_ID_REGEX}'"
                )

        # Prevent a manual run from using an ID that looks like a scheduled run.
        if run_type == DagRunType.MANUAL:
            if (inferred_run_type := DagRunType.from_run_id(run_id)) != DagRunType.MANUAL:
                raise ValueError(
                    f"A {run_type.value} DAG run cannot use ID {run_id!r} since it "
                    f"is reserved for {inferred_run_type.value} runs"
                )

        # todo: AIP-78 add verification that if run type is backfill then we have a backfill id

        if TYPE_CHECKING:
            # TODO: Task-SDK: remove this assert
            assert self.params
        # create a copy of params before validating
        copied_params = copy.deepcopy(self.params)
        if conf:
            copied_params.update(conf)
        copied_params.validate()
        return _create_orm_dagrun(
            dag=self,
            run_id=run_id,
            logical_date=logical_date,
            data_interval=data_interval,
            run_after=timezone.coerce_datetime(run_after),
            start_date=timezone.coerce_datetime(start_date),
            conf=conf,
            state=state,
            run_type=run_type,
            creating_job_id=creating_job_id,
            backfill_id=backfill_id,
            triggered_by=triggered_by,
            session=session,
        )

    @classmethod
    @provide_session
    def bulk_write_to_db(
        cls,
        bundle_name: str,
        bundle_version: str | None,
        dags: Collection[MaybeSerializedDAG],
        session: Session = NEW_SESSION,
    ):
        """
        Ensure the DagModel rows for the given dags are up-to-date in the dag table in the DB.

        :param dags: the DAG objects to save to the DB
        :return: None
        """
        if not dags:
            return

        from airflow.dag_processing.collection import AssetModelOperation, DagModelOperation

        log.info("Sync %s DAGs", len(dags))
        dag_op = DagModelOperation(
            bundle_name=bundle_name, bundle_version=bundle_version, dags={d.dag_id: d for d in dags}
        )  # type: ignore[misc]

        orm_dags = dag_op.add_dags(session=session)
        dag_op.update_dags(orm_dags, session=session)

        asset_op = AssetModelOperation.collect(dag_op.dags)

        orm_assets = asset_op.sync_assets(session=session)
        orm_asset_aliases = asset_op.sync_asset_aliases(session=session)
        session.flush()  # This populates id so we can create fks in later calls.

        orm_dags = dag_op.find_orm_dags(session=session)  # Refetch so relationship is up to date.
        asset_op.add_dag_asset_references(orm_dags, orm_assets, session=session)
        asset_op.add_dag_asset_alias_references(orm_dags, orm_asset_aliases, session=session)
        asset_op.add_dag_asset_name_uri_references(session=session)
        asset_op.add_task_asset_references(orm_dags, orm_assets, session=session)
        asset_op.add_asset_trigger_references(orm_assets, session=session)
        asset_op.activate_assets_if_possible(orm_assets.values(), session=session)

        dag_op.update_dag_asset_expression(orm_dags=orm_dags, orm_assets=orm_assets)

        session.flush()

    @provide_session
    def sync_to_db(self, session=NEW_SESSION):
        """
        Save attributes about this DAG to the DB.

        :return: None
        """
        # TODO: AIP-66 should this be in the model?
        bundle_name = self.get_bundle_name(session=session)
        bundle_version = self.get_bundle_version(session=session)
        self.bulk_write_to_db(bundle_name, bundle_version, [self], session=session)

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
            dag.is_stale = True
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
            select(DagModel).where(DagModel.last_parsed_time < expiration_date, ~DagModel.is_stale)
        ):
            log.info(
                "Deactivating DAG ID %s since it was last touched by the scheduler at %s",
                dag.dag_id,
                dag.last_parsed_time.isoformat(),
            )
            dag.is_stale = True
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

    # "default has type "type[Asset]", argument has type "type[AssetT]")  [assignment]" :shrug:
    def get_task_assets(
        self,
        inlets: bool = True,
        outlets: bool = True,
        of_type: type[AssetT] = Asset,  # type: ignore[assignment]
    ) -> Generator[tuple[str, AssetT], None, None]:
        for task in self.task_dict.values():
            directions = ("inlets",) if inlets else ()
            if outlets:
                directions += ("outlets",)
            for direction in directions:
                if not (ports := getattr(task, direction, None)):
                    continue

                for port in ports:
                    if isinstance(port, of_type):
                        yield task.task_id, port

    @classmethod
    def from_sdk_dag(cls, dag: TaskSDKDag) -> DAG:
        """Create a new (Scheduler) DAG object from a TaskSDKDag."""
        if not isinstance(dag, TaskSDKDag):
            return dag

        fields = attrs.fields(dag.__class__)

        kwargs = {}
        for field in fields:
            # Skip fields that are:
            # 1. Initialized after creation (init=False)
            # 2. Internal state fields that shouldn't be copied
            if not field.init or field.name in ["edge_info"]:
                continue

            kwargs[field.name] = getattr(dag, field.name)

        new_dag = cls(**kwargs)

        task_group_map = {}

        def create_task_groups(task_group, parent_group=None):
            new_task_group = copy.deepcopy(task_group)

            new_task_group.dag = new_dag
            new_task_group.parent_group = parent_group
            new_task_group.children = {}

            task_group_map[task_group.group_id] = new_task_group

            for child in task_group.children.values():
                if isinstance(child, TaskGroup):
                    create_task_groups(child, new_task_group)

        create_task_groups(dag.task_group)

        def create_tasks(task):
            if isinstance(task, TaskGroup):
                return task_group_map[task.group_id]

            new_task = copy.deepcopy(task)

            # Only overwrite the specific attributes we want to change
            new_task.task_id = task.task_id
            new_task.dag = None  # Don't set dag yet
            new_task.task_group = task_group_map.get(task.task_group.group_id) if task.task_group else None

            return new_task

        # Process all tasks in the original DAG
        for task in dag.tasks:
            new_task = create_tasks(task)
            if not isinstance(new_task, TaskGroup):
                # Add the task to the DAG
                new_dag.task_dict[new_task.task_id] = new_task
                if new_task.task_group:
                    new_task.task_group.children[new_task.task_id] = new_task
                new_task.dag = new_dag

        new_dag.edge_info = dag.edge_info.copy()

        return new_dag


class DagTag(Base):
    """A tag name per dag, to allow quick filtering in the DAG view."""

    __tablename__ = "dag_tag"
    name = Column(String(TAG_MAX_LEN), primary_key=True)
    dag_id = Column(
        StringID(),
        ForeignKey("dag.dag_id", name="dag_tag_dag_id_fkey", ondelete="CASCADE"),
        primary_key=True,
    )

    __table_args__ = (Index("idx_dag_tag_dag_id", dag_id),)

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
        dag_links: dict = defaultdict(dict)
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
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused_at_creation = airflow_conf.getboolean("core", "dags_are_paused_at_creation")
    is_paused = Column(Boolean, default=is_paused_at_creation)
    # Whether that DAG was seen on the last DagBag load
    is_stale = Column(Boolean, default=True)
    # Last time the scheduler started
    last_parsed_time = Column(UtcDateTime)
    # Time when the DAG last received a refresh signal
    # (e.g. the DAG's "refresh" button was clicked in the web UI)
    last_expired = Column(UtcDateTime)
    # The location of the file containing the DAG object
    # Note: Do not depend on fileloc pointing to a file; in the case of a
    # packaged DAG, it will point to the subpath of the DAG within the
    # associated zip.
    fileloc = Column(String(2000))
    relative_fileloc = Column(String(2000))
    bundle_name = Column(StringID(), ForeignKey("dag_bundle.name"), nullable=True)
    # The version of the bundle the last time the DAG was processed
    bundle_version = Column(String(200), nullable=True)
    # String representing the owners
    owners = Column(String(2000))
    # Display name of the dag
    _dag_display_property_value = Column("dag_display_name", String(2000), nullable=True)
    # Description of the dag
    description = Column(Text)
    # Timetable summary
    timetable_summary = Column(Text, nullable=True)
    # Timetable description
    timetable_description = Column(String(1000), nullable=True)
    # Asset expression based on asset triggers
    asset_expression = Column(sqlalchemy_jsonfield.JSONField(json=json), nullable=True)
    # Tags for view filter
    tags = relationship("DagTag", cascade="all, delete, delete-orphan", backref=backref("dag"))
    # Dag owner links for DAGs view
    dag_owner_links = relationship(
        "DagOwnerAttributes", cascade="all, delete, delete-orphan", backref=backref("dag")
    )

    max_active_tasks = Column(Integer, nullable=False)
    max_active_runs = Column(Integer, nullable=True)  # todo: should not be nullable if we have a default
    max_consecutive_failed_dag_runs = Column(Integer, nullable=False)

    has_task_concurrency_limits = Column(Boolean, nullable=False)
    has_import_errors = Column(Boolean(), default=False, server_default="0")

    # The logical date of the next dag run.
    next_dagrun = Column(UtcDateTime)

    # Must be either both NULL or both datetime.
    next_dagrun_data_interval_start = Column(UtcDateTime)
    next_dagrun_data_interval_end = Column(UtcDateTime)

    # Earliest time at which this ``next_dagrun`` can be created.
    next_dagrun_create_after = Column(UtcDateTime)

    __table_args__ = (Index("idx_next_dagrun_create_after", next_dagrun_create_after, unique=False),)

    schedule_asset_references = relationship(
        "DagScheduleAssetReference",
        back_populates="dag",
        cascade="all, delete, delete-orphan",
    )
    schedule_asset_alias_references = relationship(
        "DagScheduleAssetAliasReference",
        back_populates="dag",
        cascade="all, delete, delete-orphan",
    )
    schedule_asset_name_references = relationship(
        "DagScheduleAssetNameReference",
        back_populates="dag",
        cascade="all, delete, delete-orphan",
    )
    schedule_asset_uri_references = relationship(
        "DagScheduleAssetUriReference",
        back_populates="dag",
        cascade="all, delete, delete-orphan",
    )
    schedule_assets = association_proxy("schedule_asset_references", "asset")
    task_outlet_asset_references = relationship(
        "TaskOutletAssetReference",
        cascade="all, delete, delete-orphan",
    )
    NUM_DAGS_PER_DAGRUN_QUERY = airflow_conf.getint(
        "scheduler", "max_dagruns_to_create_per_loop", fallback=10
    )
    dag_versions = relationship(
        "DagVersion", back_populates="dag_model", cascade="all, delete, delete-orphan"
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if self.max_active_tasks is None:
            self.max_active_tasks = airflow_conf.getint("core", "max_active_tasks_per_dag")

        if self.max_active_runs is None:
            self.max_active_runs = airflow_conf.getint("core", "max_active_runs_per_dag")

        if self.max_consecutive_failed_dag_runs is None:
            self.max_consecutive_failed_dag_runs = airflow_conf.getint(
                "core", "max_consecutive_failed_dag_runs_per_dag"
            )

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
        )

    @classmethod
    @provide_session
    def get_current(cls, dag_id: str, session=NEW_SESSION) -> DagModel:
        return session.scalar(select(cls).where(cls.dag_id == dag_id))

    @provide_session
    def get_last_dagrun(self, session=NEW_SESSION, include_manually_triggered=False):
        return get_last_dagrun(
            self.dag_id, session=session, include_manually_triggered=include_manually_triggered
        )

    def get_is_paused(self, *, session: Session | None = None) -> bool:
        """Provide interface compatibility to 'DAG'."""
        return self.is_paused

    def get_is_active(self, *, session: Session | None = None) -> bool:
        """Provide interface compatibility to 'DAG'."""
        return not self.is_stale

    @staticmethod
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

        paused_dag_ids = {paused_dag_id for (paused_dag_id,) in paused_dag_ids}
        return paused_dag_ids

    @property
    def safe_dag_id(self):
        return self.dag_id.replace(".", "__dot__")

    @provide_session
    def set_is_paused(self, is_paused: bool, session=NEW_SESSION) -> None:
        """
        Pause/Un-pause a DAG.

        :param is_paused: Is the DAG paused
        :param session: session
        """
        filter_query = [
            DagModel.dag_id == self.dag_id,
        ]

        session.execute(
            update(DagModel)
            .where(or_(*filter_query))
            .values(is_paused=is_paused)
            .execution_options(synchronize_session="fetch")
        )
        session.commit()

    @hybrid_property
    def dag_display_name(self) -> str:
        return self._dag_display_property_value or self.dag_id

    @dag_display_name.expression  # type: ignore[no-redef]
    def dag_display_name(self) -> str:
        """
        Expression part of the ``dag_display`` name hybrid property.

        :meta private:
        """
        return case(
            (self._dag_display_property_value.is_not(None), self._dag_display_property_value),
            else_=self.dag_id,
        )

    @classmethod
    @provide_session
    def deactivate_deleted_dags(
        cls,
        bundle_name: str,
        rel_filelocs: list[str],
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Set ``is_active=False`` on the DAGs for which the DAG files have been removed.

        :param bundle_name: bundle for filelocs
        :param rel_filelocs: relative filelocs for bundle
        :param session: ORM Session
        """
        log.debug("Deactivating DAGs (for which DAG files are deleted) from %s table ", cls.__tablename__)
        dag_models = session.scalars(
            select(cls)
            .where(
                cls.bundle_name == bundle_name,
            )
            .options(
                load_only(
                    cls.relative_fileloc,
                    cls.is_stale,
                ),
            )
        )

        for dm in dag_models:
            if dm.relative_fileloc not in rel_filelocs:
                dm.is_stale = True

    @classmethod
    def dags_needing_dagruns(cls, session: Session) -> tuple[Query, dict[str, datetime]]:
        """
        Return (and lock) a list of Dag objects that are due to create a new DagRun.

        This will return a resultset of rows that is row-level-locked with a "SELECT ... FOR UPDATE" query,
        you should ensure that any scheduling decisions are made in a single transaction -- as soon as the
        transaction is committed it will be unlocked.
        """
        from airflow.models.serialized_dag import SerializedDagModel

        evaluator = AssetEvaluator(session)

        def dag_ready(dag_id: str, cond: BaseAsset, statuses: dict[AssetUniqueKey, bool]) -> bool | None:
            # if dag was serialized before 2.9 and we *just* upgraded,
            # we may be dealing with old version.  In that case,
            # just wait for the dag to be reserialized.
            try:
                return evaluator.run(cond, statuses)
            except AttributeError:
                log.warning("dag '%s' has old serialization; skipping DAG run creation.", dag_id)
                return None

        # this loads all the ADRQ records.... may need to limit num dags
        adrq_by_dag: dict[str, list[AssetDagRunQueue]] = defaultdict(list)
        for r in session.scalars(select(AssetDagRunQueue)):
            adrq_by_dag[r.target_dag_id].append(r)

        dag_statuses: dict[str, dict[AssetUniqueKey, bool]] = {
            dag_id: {AssetUniqueKey.from_asset(adrq.asset): True for adrq in adrqs}
            for dag_id, adrqs in adrq_by_dag.items()
        }
        ser_dags = SerializedDagModel.get_latest_serialized_dags(dag_ids=list(dag_statuses), session=session)
        for ser_dag in ser_dags:
            dag_id = ser_dag.dag_id
            statuses = dag_statuses[dag_id]
            if not dag_ready(dag_id, cond=ser_dag.dag.timetable.asset_condition, statuses=statuses):
                del adrq_by_dag[dag_id]
                del dag_statuses[dag_id]
        del dag_statuses

        # triggered dates for asset triggered dags
        triggered_date_by_dag: dict[str, datetime] = {
            dag_id: max(adrq.created_at for adrq in adrqs) for dag_id, adrqs in adrq_by_dag.items()
        }
        del adrq_by_dag

        asset_triggered_dag_ids = set(triggered_date_by_dag.keys())
        if asset_triggered_dag_ids:
            # exclude as max active runs has been reached
            exclusion_list = set(
                session.scalars(
                    select(DagModel.dag_id)
                    .join(DagRun.dag_model)
                    .where(DagRun.state.in_((DagRunState.QUEUED, DagRunState.RUNNING)))
                    .where(DagModel.dag_id.in_(asset_triggered_dag_ids))
                    .group_by(DagModel.dag_id)
                    .having(func.count() >= func.max(DagModel.max_active_runs))
                )
            )
            if exclusion_list:
                asset_triggered_dag_ids -= exclusion_list
                triggered_date_by_dag = {
                    k: v for k, v in triggered_date_by_dag.items() if k not in exclusion_list
                }

        # We limit so that _one_ scheduler doesn't try to do all the creation of dag runs
        query = (
            select(cls)
            .where(
                cls.is_paused == expression.false(),
                cls.is_stale == expression.false(),
                cls.has_import_errors == expression.false(),
                or_(
                    cls.next_dagrun_create_after <= func.now(),
                    cls.dag_id.in_(asset_triggered_dag_ids),
                ),
            )
            .order_by(cls.next_dagrun_create_after)
            .limit(cls.NUM_DAGS_PER_DAGRUN_QUERY)
        )

        return (
            session.scalars(with_row_locks(query, of=cls, session=session, skip_locked=True)),
            triggered_date_by_dag,
        )

    def calculate_dagrun_date_fields(
        self,
        dag: DAG,
        last_automated_dag_run: None | DataInterval,
    ) -> None:
        """
        Calculate ``next_dagrun`` and `next_dagrun_create_after``.

        :param dag: The DAG object
        :param last_automated_dag_run: DataInterval (or datetime) of most recent run of this dag, or none
            if not yet scheduled.
        """
        last_automated_data_interval: DataInterval | None
        if isinstance(last_automated_dag_run, datetime):
            raise ValueError(
                "Passing a datetime to `DagModel.calculate_dagrun_date_fields` is not supported. "
                "Provide a data interval instead."
            )
        last_automated_data_interval = last_automated_dag_run
        next_dagrun_info = dag.next_dagrun_info(last_automated_data_interval)
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
    def get_asset_triggered_next_run_info(self, *, session=NEW_SESSION) -> dict[str, int | str] | None:
        if self.asset_expression is None:
            return None

        # When an asset alias does not resolve into assets, get_asset_triggered_next_run_info returns
        # an empty dict as there's no asset info to get. This method should thus return None.
        return get_asset_triggered_next_run_info([self.dag_id], session=session).get(self.dag_id, None)


STATICA_HACK = True
globals()["kcah_acitats"[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.models.serialized_dag import SerializedDagModel

    DagModel.serialized_dag = relationship(SerializedDagModel)
    """:sphinx-autoapi-skip:"""


def _get_or_create_dagrun(
    *,
    dag: DAG,
    run_id: str,
    logical_date: datetime | None,
    data_interval: tuple[datetime, datetime] | None,
    run_after: datetime,
    conf: dict | None,
    triggered_by: DagRunTriggeredByType,
    start_date: datetime,
    session: Session,
) -> DagRun:
    """
    Create a DAG run, replacing an existing instance if needed to prevent collisions.

    This function is only meant to be used by :meth:`DAG.test` as a helper function.

    :param dag: DAG to be used to find run.
    :param conf: Configuration to pass to newly created run.
    :param start_date: Start date of new run.
    :param logical_date: Logical date for finding an existing run.
    :param run_id: Run ID for the new DAG run.
    :param triggered_by: the entity which triggers the dag_run

    :return: The newly created DAG run.
    """
    dr: DagRun = session.scalar(
        select(DagRun).where(DagRun.dag_id == dag.dag_id, DagRun.logical_date == logical_date)
    )
    if dr:
        session.delete(dr)
        session.commit()
    dr = dag.create_dagrun(
        run_id=run_id,
        logical_date=logical_date,
        data_interval=data_interval,
        run_after=run_after,
        conf=conf,
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        triggered_by=triggered_by,
        start_date=start_date or logical_date,
        session=session,
    )
    log.info("created dagrun %s", dr)
    return dr
