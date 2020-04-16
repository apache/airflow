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

from __future__ import print_function

import copy
import functools
import os
import pickle
import re
import sys
import traceback
import warnings
from collections import OrderedDict, defaultdict
from datetime import timedelta, datetime
from typing import TYPE_CHECKING, Callable, Dict, FrozenSet, Iterable, List, Optional, Type, Union

import jinja2
import pendulum
import six
from croniter import croniter
from dateutil.relativedelta import relativedelta
from future.standard_library import install_aliases
from sqlalchemy import Boolean, Column, ForeignKey, Index, Integer, String, Text, func, or_
from sqlalchemy.orm import backref, relationship

from airflow import settings, utils
from airflow.configuration import conf
from airflow.dag.base_dag import BaseDag
from airflow.executors import LocalExecutor, get_default_executor
from airflow.models.base import Base, ID_LEN
from airflow.exceptions import (
    AirflowDagCycleException, AirflowException, DagNotFound, TaskNotFound,
)
from airflow.models.dagbag import DagBag
from airflow.models.dagcode import DagCode
from airflow.models.dagpickle import DagPickle
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance, clear_task_instances
from airflow.settings import STORE_SERIALIZED_DAGS, MIN_SERIALIZED_DAG_UPDATE_INTERVAL
from airflow.utils import timezone
from airflow.utils.dag_processing import correct_maybe_zipped
from airflow.utils.dates import cron_presets, date_range as utils_date_range
from airflow.utils.db import provide_session
from airflow.utils.helpers import validate_key
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.sqlalchemy import UtcDateTime, Interval
from airflow.utils.state import State

if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator  # Avoid circular dependency

install_aliases()

ScheduleInterval = Union[str, timedelta, relativedelta]


def get_last_dagrun(dag_id, session, include_externally_triggered=False):
    """
    Returns the last dag run for a dag, None if there was none.
    Last dag run can be any type of run eg. scheduled or backfilled.
    Overridden DagRuns are ignored.
    """
    DR = DagRun
    query = session.query(DR).filter(DR.dag_id == dag_id)
    if not include_externally_triggered:
        query = query.filter(DR.external_trigger == False)  # noqa
    query = query.order_by(DR.execution_date.desc())
    return query.first()


@functools.total_ordering
class DAG(BaseDag, LoggingMixin):
    """
    A dag (directed acyclic graph) is a collection of tasks with directional
    dependencies. A dag also has a schedule, a start date and an end date
    (optional). For each schedule, (say daily or hourly), the DAG needs to run
    each individual tasks as their dependencies are met. Certain tasks have
    the property of depending on their own past, meaning that they can't run
    until their previous schedule (and upstream tasks) are completed.

    DAGs essentially act as namespaces for tasks. A task_id can only be
    added once to a DAG.

    :param dag_id: The id of the DAG
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
    :type template_undefined: jinja2.Undefined
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
    :param concurrency: the number of task instances allowed to run
        concurrently
    :type concurrency: int
    :param max_active_runs: maximum number of active DAG runs, beyond this
        number of DAG runs in a running state, the scheduler won't create
        new active DAG runs
    :type max_active_runs: int
    :param dagrun_timeout: specify how long a DagRun should be up before
        timing out / failing, so that new DagRuns can be created. The timeout
        is only enforced for scheduled DagRuns, and only once the
        # of active DagRuns == max_active_runs.
    :type dagrun_timeout: datetime.timedelta
    :param sla_miss_callback: specify a function to call when reporting SLA
        timeouts.
    :type sla_miss_callback: types.FunctionType
    :param default_view: Specify DAG default view (tree, graph, duration,
                                                   gantt, landing_times)
    :type default_view: str
    :param orientation: Specify DAG orientation in graph view (LR, TB, RL, BT)
    :type orientation: str
    :param catchup: Perform scheduler catchup (or only run latest)? Defaults to True
    :type catchup: bool
    :param on_failure_callback: A function to be called when a DagRun of this dag fails.
        A context dictionary is passed as a single parameter to this function.
    :type on_failure_callback: callable
    :param on_success_callback: Much like the ``on_failure_callback`` except
        that it is executed when the dag succeeds.
    :type on_success_callback: callable
    :param access_control: Specify optional DAG-level permissions, e.g.,
        "{'role1': {'can_dag_read'}, 'role2': {'can_dag_read', 'can_dag_edit'}}"
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
        <https://jinja.palletsprojects.com/en/master/api/#jinja2.Environment>`_

    :type jinja_environment_kwargs: dict
    :param tags: List of tags to help filtering DAGS in the UI.
    :type tags: List[str]
    """

    _comps = {
        'dag_id',
        'task_ids',
        'parent_dag',
        'start_date',
        'schedule_interval',
        'full_filepath',
        'template_searchpath',
        'last_loaded',
    }

    __serialized_fields = None  # type: Optional[FrozenSet[str]]

    def __init__(
        self,
        dag_id,  # type: str
        description='',  # type: str
        schedule_interval=timedelta(days=1),  # type: Optional[ScheduleInterval]
        start_date=None,  # type: Optional[datetime]
        end_date=None,  # type: Optional[datetime]
        full_filepath=None,  # type: Optional[str]
        template_searchpath=None,  # type: Optional[Union[str, Iterable[str]]]
        template_undefined=jinja2.Undefined,  # type: Type[jinja2.Undefined]
        user_defined_macros=None,  # type: Optional[Dict]
        user_defined_filters=None,  # type: Optional[Dict]
        default_args=None,  # type: Optional[Dict]
        concurrency=conf.getint('core', 'dag_concurrency'),  # type: int
        max_active_runs=conf.getint(
            'core', 'max_active_runs_per_dag'),  # type: int
        dagrun_timeout=None,  # type: Optional[timedelta]
        sla_miss_callback=None,  # type: Optional[Callable]
        default_view=None,  # type: Optional[str]
        orientation=conf.get('webserver', 'dag_orientation'),  # type: str
        catchup=conf.getboolean('scheduler', 'catchup_by_default'),  # type: bool
        on_success_callback=None,  # type: Optional[Callable]
        on_failure_callback=None,  # type: Optional[Callable]
        doc_md=None,  # type: Optional[str]
        params=None,  # type: Optional[Dict]
        access_control=None,  # type: Optional[Dict]
        is_paused_upon_creation=None,  # type: Optional[bool]
        jinja_environment_kwargs=None,  # type: Optional[Dict]
        tags=None,  # type: Optional[List[str]]
    ):
        self.user_defined_macros = user_defined_macros
        self.user_defined_filters = user_defined_filters
        self.default_args = copy.deepcopy(default_args or {})
        self.params = params or {}

        # merging potentially conflicting default_args['params'] into params
        if 'params' in self.default_args:
            self.params.update(self.default_args['params'])
            del self.default_args['params']

        validate_key(dag_id)

        # Properties from BaseDag
        self._dag_id = dag_id
        self._full_filepath = full_filepath if full_filepath else ''
        self._concurrency = concurrency
        self._pickle_id = None

        self._description = description
        # set file location to caller source path
        self.fileloc = sys._getframe().f_back.f_code.co_filename
        self.task_dict = dict()  # type: Dict[str, BaseOperator]

        # set timezone from start_date
        if start_date and start_date.tzinfo:
            self.timezone = start_date.tzinfo
        elif 'start_date' in self.default_args and self.default_args['start_date']:
            if isinstance(self.default_args['start_date'], six.string_types):
                self.default_args['start_date'] = (
                    timezone.parse(self.default_args['start_date'])
                )
            self.timezone = self.default_args['start_date'].tzinfo

        if not hasattr(self, 'timezone') or not self.timezone:
            self.timezone = settings.TIMEZONE

        # Apply the timezone we settled on to end_date if it wasn't supplied
        if 'end_date' in self.default_args and self.default_args['end_date']:
            if isinstance(self.default_args['end_date'], six.string_types):
                self.default_args['end_date'] = (
                    timezone.parse(self.default_args['end_date'], timezone=self.timezone)
                )

        self.start_date = timezone.convert_to_utc(start_date)
        self.end_date = timezone.convert_to_utc(end_date)

        # also convert tasks
        if 'start_date' in self.default_args:
            self.default_args['start_date'] = (
                timezone.convert_to_utc(self.default_args['start_date'])
            )
        if 'end_date' in self.default_args:
            self.default_args['end_date'] = (
                timezone.convert_to_utc(self.default_args['end_date'])
            )

        self.schedule_interval = schedule_interval
        if isinstance(schedule_interval, six.string_types) and schedule_interval in cron_presets:
            self._schedule_interval = cron_presets.get(schedule_interval)  # type: Optional[ScheduleInterval]
        elif schedule_interval == '@once':
            self._schedule_interval = None
        else:
            self._schedule_interval = schedule_interval
        if isinstance(template_searchpath, six.string_types):
            template_searchpath = [template_searchpath]
        self.template_searchpath = template_searchpath
        self.template_undefined = template_undefined
        self.parent_dag = None  # Gets set when DAGs are loaded
        self.last_loaded = timezone.utcnow()
        self.safe_dag_id = dag_id.replace('.', '__dot__')
        self.max_active_runs = max_active_runs
        self.dagrun_timeout = dagrun_timeout
        self.sla_miss_callback = sla_miss_callback
        self._default_view = default_view
        self.orientation = orientation
        self.catchup = catchup
        self.is_subdag = False  # DagBag.bag_dag() will set this to True if appropriate

        self.partial = False
        self.on_success_callback = on_success_callback
        self.on_failure_callback = on_failure_callback
        self.doc_md = doc_md

        self._old_context_manager_dags = []  # type: Iterable[DAG]
        self._access_control = access_control
        self.is_paused_upon_creation = is_paused_upon_creation

        self.jinja_environment_kwargs = jinja_environment_kwargs
        self.tags = tags

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    def __eq__(self, other):
        if (type(self) == type(other) and
                self.dag_id == other.dag_id):

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
        self._old_context_manager_dags.append(settings.CONTEXT_MANAGER_DAG)
        settings.CONTEXT_MANAGER_DAG = self
        return self

    def __exit__(self, _type, _value, _tb):
        settings.CONTEXT_MANAGER_DAG = self._old_context_manager_dags.pop()

    # /Context Manager ----------------------------------------------

    def get_default_view(self):
        """This is only there for backward compatible jinja2 templates"""
        if self._default_view is None:
            return conf.get('webserver', 'dag_default_view').lower()
        else:
            return self._default_view

    def date_range(self, start_date, num=None, end_date=timezone.utcnow()):
        if num:
            end_date = None
        return utils_date_range(
            start_date=start_date, end_date=end_date,
            num=num, delta=self._schedule_interval)

    def is_fixed_time_schedule(self):
        """
        Figures out if the DAG schedule has a fixed time (e.g. 3 AM).

        :return: True if the schedule has a fixed time, False if not.
        """
        now = datetime.now()
        cron = croniter(self._schedule_interval, now)

        start = cron.get_next(datetime)
        cron_next = cron.get_next(datetime)

        if cron_next.minute == start.minute and cron_next.hour == start.hour:
            return True

        return False

    def following_schedule(self, dttm):
        """
        Calculates the following schedule for this dag in UTC.

        :param dttm: utc datetime
        :return: utc datetime
        """
        if isinstance(self._schedule_interval, six.string_types):
            # we don't want to rely on the transitions created by
            # croniter as they are not always correct
            dttm = pendulum.instance(dttm)
            naive = timezone.make_naive(dttm, self.timezone)
            cron = croniter(self._schedule_interval, naive)

            # We assume that DST transitions happen on the minute/hour
            if not self.is_fixed_time_schedule():
                # relative offset (eg. every 5 minutes)
                delta = cron.get_next(datetime) - naive
                following = dttm.in_timezone(self.timezone).add_timedelta(delta)
            else:
                # absolute (e.g. 3 AM)
                naive = cron.get_next(datetime)
                tz = pendulum.timezone(self.timezone.name)
                following = timezone.make_aware(naive, tz)
            return timezone.convert_to_utc(following)
        elif self._schedule_interval is not None:
            return dttm + self._schedule_interval

    def previous_schedule(self, dttm):
        """
        Calculates the previous schedule for this dag in UTC

        :param dttm: utc datetime
        :return: utc datetime
        """
        if isinstance(self._schedule_interval, six.string_types):
            # we don't want to rely on the transitions created by
            # croniter as they are not always correct
            dttm = pendulum.instance(dttm)
            naive = timezone.make_naive(dttm, self.timezone)
            cron = croniter(self._schedule_interval, naive)

            # We assume that DST transitions happen on the minute/hour
            if not self.is_fixed_time_schedule():
                # relative offset (eg. every 5 minutes)
                delta = naive - cron.get_prev(datetime)
                previous = dttm.in_timezone(self.timezone).subtract_timedelta(delta)
            else:
                # absolute (e.g. 3 AM)
                naive = cron.get_prev(datetime)
                tz = pendulum.timezone(self.timezone.name)
                previous = timezone.make_aware(naive, tz)
            return timezone.convert_to_utc(previous)
        elif self._schedule_interval is not None:
            return dttm - self._schedule_interval

    def get_run_dates(self, start_date, end_date=None):
        """
        Returns a list of dates between the interval received as parameter using this
        dag's schedule interval. Returned dates can be used for execution dates.

        :param start_date: the start date of the interval
        :type start_date: datetime
        :param end_date: the end date of the interval, defaults to timezone.utcnow()
        :type end_date: datetime
        :return: a list of dates within the interval following the dag's schedule
        :rtype: list
        """
        run_dates = []

        using_start_date = start_date
        using_end_date = end_date

        # dates for dag runs
        using_start_date = using_start_date or min([t.start_date for t in self.tasks])
        using_end_date = using_end_date or timezone.utcnow()

        # next run date for a subdag isn't relevant (schedule_interval for subdags
        # is ignored) so we use the dag run's start date in the case of a subdag
        next_run_date = (self.normalize_schedule(using_start_date)
                         if not self.is_subdag else using_start_date)

        while next_run_date and next_run_date <= using_end_date:
            run_dates.append(next_run_date)
            next_run_date = self.following_schedule(next_run_date)

        return run_dates

    def normalize_schedule(self, dttm):
        """
        Returns dttm + interval unless dttm is first interval then it returns dttm
        """
        following = self.following_schedule(dttm)

        # in case of @once
        if not following:
            return dttm
        if self.previous_schedule(following) != dttm:
            return following

        return dttm

    @provide_session
    def get_last_dagrun(self, session=None, include_externally_triggered=False):
        return get_last_dagrun(self.dag_id, session=session,
                               include_externally_triggered=include_externally_triggered)

    @property
    def dag_id(self):
        return self._dag_id

    @dag_id.setter
    def dag_id(self, value):
        self._dag_id = value

    @property
    def full_filepath(self):
        return self._full_filepath

    @full_filepath.setter
    def full_filepath(self, value):
        self._full_filepath = value

    @property
    def concurrency(self):
        return self._concurrency

    @concurrency.setter
    def concurrency(self, value):
        self._concurrency = value

    @property
    def access_control(self):
        return self._access_control

    @access_control.setter
    def access_control(self, value):
        self._access_control = value

    @property
    def description(self):
        return self._description

    @property
    def description_unicode(self):
        if six.PY2:
            try:
                # in Py2 when you parse python code which is UTF-8 encoded you get byte str
                # description here - but it is actually UTF-8 encoded, so you need to decode
                # it in order to be correctly converted to
                return self._description.decode("utf-8")
            except UnicodeDecodeError:
                pass
        return self._description

    @property
    def pickle_id(self):
        return self._pickle_id

    @pickle_id.setter
    def pickle_id(self, value):
        self._pickle_id = value

    @property
    def tasks(self):
        return list(self.task_dict.values())

    @tasks.setter
    def tasks(self, val):
        raise AttributeError(
            'DAG.tasks can not be modified. Use dag.add_task() instead.')

    @property
    def task_ids(self):
        return list(self.task_dict.keys())

    @property
    def filepath(self):
        """
        File location of where the dag object is instantiated
        """
        fn = self.full_filepath.replace(settings.DAGS_FOLDER + '/', '')
        fn = fn.replace(os.path.dirname(__file__) + '/', '')
        return fn

    @property
    def folder(self):
        """Folder location of where the DAG object is instantiated."""
        return os.path.dirname(self.full_filepath)

    @property
    def owner(self):
        """
        Return list of all owners found in DAG tasks.

        :return: Comma separated list of owners in DAG tasks
        :rtype: str
        """
        return ", ".join({t.owner for t in self.tasks})

    @property
    def allow_future_exec_dates(self):
        return conf.getboolean(
            'scheduler',
            'allow_trigger_in_future',
            fallback=False) and self.schedule_interval is None

    @provide_session
    def _get_concurrency_reached(self, session=None):
        TI = TaskInstance
        qry = session.query(func.count(TI.task_id)).filter(
            TI.dag_id == self.dag_id,
            TI.state == State.RUNNING,
        )
        return qry.scalar() >= self.concurrency

    @property
    def concurrency_reached(self):
        """
        Returns a boolean indicating whether the concurrency limit for this DAG
        has been reached
        """
        return self._get_concurrency_reached()

    @provide_session
    def _get_is_paused(self, session=None):
        qry = session.query(DagModel).filter(
            DagModel.dag_id == self.dag_id)
        return qry.value(DagModel.is_paused)

    @property
    def is_paused(self):
        """
        Returns a boolean indicating whether this DAG is paused
        """
        return self._get_is_paused()

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
            self.log.info('Executing dag callback function: {}'.format(callback))
            tis = dagrun.get_task_instances()
            ti = tis[-1]  # get first TaskInstance of DagRun
            ti.task = self.get_task(ti.task_id)
            context = ti.get_template_context(session=session)
            context.update({'reason': reason})
            callback(context)

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
        query = (session
                 .query(func.count())
                 .filter(DagRun.dag_id == self.dag_id)
                 .filter(DagRun.state == State.RUNNING))

        if external_trigger is not None:
            query = query.filter(DagRun.external_trigger == external_trigger)

        return query.scalar()

    @provide_session
    def get_dagrun(self, execution_date, session=None):
        """
        Returns the dag run for a given execution date if it exists, otherwise
        none.

        :param execution_date: The execution date of the DagRun to find.
        :param session:
        :return: The DagRun if found, otherwise None.
        """
        dagrun = (
            session.query(DagRun)
            .filter(
                DagRun.dag_id == self.dag_id,
                DagRun.execution_date == execution_date)
            .first())

        return dagrun

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
                DagRun.execution_date <= end_date)
            .all())

        return dagruns

    @provide_session
    def _get_latest_execution_date(self, session=None):
        return session.query(func.max(DagRun.execution_date)).filter(
            DagRun.dag_id == self.dag_id
        ).scalar()

    @property
    def latest_execution_date(self):
        """
        Returns the latest date for which at least one dag run exists
        """
        return self._get_latest_execution_date()

    @property
    def subdags(self):
        """
        Returns a list of the subdag objects associated to this DAG
        """
        # Check SubDag for class but don't check class directly
        from airflow.operators.subdag_operator import SubDagOperator
        subdag_lst = []
        for task in self.tasks:
            if (isinstance(task, SubDagOperator) or
                    # TODO remove in Airflow 2.0
                    type(task).__name__ == 'SubDagOperator' or
                    task.task_type == 'SubDagOperator'):
                subdag_lst.append(task.subdag)
                subdag_lst += task.subdag.subdags
        return subdag_lst

    def resolve_template_files(self):
        for t in self.tasks:
            t.resolve_template_files()

    def get_template_env(self):  # type: () -> jinja2.Environment
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
            'cache_size': 0
        }
        if self.jinja_environment_kwargs:
            jinja_env_options.update(self.jinja_environment_kwargs)

        env = jinja2.Environment(**jinja_env_options)  # type: ignore

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
        self.get_task(upstream_task_id).set_downstream(
            self.get_task(downstream_task_id))

    @provide_session
    def get_task_instances(
            self, start_date=None, end_date=None, state=None, session=None):
        if not start_date:
            start_date = (timezone.utcnow() - timedelta(30)).date()
            start_date = timezone.make_aware(
                datetime.combine(start_date, datetime.min.time()))

        tis = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.execution_date >= start_date,
            TaskInstance.task_id.in_([t.task_id for t in self.tasks]),
        )

        # This allows allow_trigger_in_future config to take affect, rather than mandating exec_date <= UTC
        if end_date or not self.allow_future_exec_dates:
            end_date = end_date or timezone.utcnow()
            tis = tis.filter(TaskInstance.execution_date <= end_date)

        if state:
            if isinstance(state, str):
                tis = tis.filter(TaskInstance.state == state)
            else:
                # this is required to deal with NULL values
                if None in state:
                    tis = tis.filter(
                        or_(TaskInstance.state.in_(state),
                            TaskInstance.state.is_(None))
                    )
                else:
                    tis = tis.filter(TaskInstance.state.in_(state))
        tis = tis.order_by(TaskInstance.execution_date).all()
        return tis

    @property
    def roots(self):
        """Return nodes with no parents. These are first to execute and are called roots or root nodes."""
        return [task for task in self.tasks if not task.upstream_list]

    @property
    def leaves(self):
        """Return nodes with no children. These are last to execute and are called leaves or leaf nodes."""
        return [task for task in self.tasks if not task.downstream_list]

    def topological_sort(self):
        """
        Sorts tasks in topographical order, such that a task comes after any of its
        upstream dependencies.

        Heavily inspired by:
        http://blog.jupo.org/2012/04/06/topological-sorting-acyclic-directed-graphs/

        :return: list of tasks in topological order
        """

        # convert into an OrderedDict to speedup lookup while keeping order the same
        graph_unsorted = OrderedDict((task.task_id, task) for task in self.tasks)

        graph_sorted = []

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
            # checking that that graph is acyclic, which is true if any
            # nodes are resolved during each pass through the graph. If
            # not, we need to bail out as the graph therefore can't be
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

            if not acyclic:
                raise AirflowException("A cyclic dependency occurred in dag: {}"
                                       .format(self.dag_id))

        return tuple(graph_sorted)

    @provide_session
    def set_dag_runs_state(
            self,
            state=State.RUNNING,
            session=None,
            start_date=None,
            end_date=None,
    ):
        query = session.query(DagRun).filter_by(dag_id=self.dag_id)
        if start_date:
            query = query.filter(DagRun.execution_date >= start_date)
        if end_date:
            query = query.filter(DagRun.execution_date <= end_date)
        drs = query.all()

        dirty_ids = []
        for dr in drs:
            dr.state = state
            dirty_ids.append(dr.dag_id)

    @provide_session
    def clear(
            self, start_date=None, end_date=None,
            only_failed=False,
            only_running=False,
            confirm_prompt=False,
            include_subdags=True,
            include_parentdag=True,
            reset_dag_runs=True,
            dry_run=False,
            session=None,
            get_tis=False,
            recursion_depth=0,
            max_recursion_depth=None,
            dag_bag=None,
    ):
        """
        Clears a set of task instances associated with the current dag for
        a specified date range.

        :param start_date: The minimum execution_date to clear
        :type start_date: datetime.datetime or None
        :param end_date: The maximum exeuction_date to clear
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
        :param reset_dag_runs: Set state of dag to RUNNING
        :type reset_dag_runs: bool
        :param dry_run: Find the tasks to clear but don't clear them.
        :type dry_run: bool
        :param session: The sqlalchemy session to use
        :type session: sqlalchemy.orm.session.Session
        :param get_tis: Return the sqlachemy query for finding the TaskInstance without clearing the tasks
        :type get_tis: bool
        :param recursion_depth: The recursion depth of nested calls to DAG.clear().
        :type recursion_depth: int
        :param max_recursion_depth: The maximum recusion depth allowed. This is determined by the
            first encountered ExternalTaskMarker. Default is None indicating no ExternalTaskMarker
            has been encountered.
        :type max_recursion_depth: int
        :param dag_bag: The DagBag used to find the dags
        :type dag_bag: airflow.models.dagbag.DagBag
        """
        TI = TaskInstance
        tis = session.query(TI)
        if include_subdags:
            # Crafting the right filter for dag_id and task_ids combo
            conditions = []
            for dag in self.subdags + [self]:
                conditions.append(
                    (TI.dag_id == dag.dag_id) &
                    TI.task_id.in_(dag.task_ids)
                )
            tis = tis.filter(or_(*conditions))
        else:
            tis = session.query(TI).filter(TI.dag_id == self.dag_id)
            tis = tis.filter(TI.task_id.in_(self.task_ids))

        if include_parentdag and self.is_subdag:

            p_dag = self.parent_dag.sub_dag(
                task_regex=r"^{}$".format(self.dag_id.split('.')[1]),
                include_upstream=False,
                include_downstream=True)

            tis = tis.union(p_dag.clear(
                start_date=start_date, end_date=end_date,
                only_failed=only_failed,
                only_running=only_running,
                confirm_prompt=confirm_prompt,
                include_subdags=include_subdags,
                include_parentdag=False,
                reset_dag_runs=reset_dag_runs,
                get_tis=True,
                session=session,
                recursion_depth=recursion_depth,
                max_recursion_depth=max_recursion_depth,
                dag_bag=dag_bag
            ))

        if start_date:
            tis = tis.filter(TI.execution_date >= start_date)
        if end_date:
            tis = tis.filter(TI.execution_date <= end_date)
        if only_failed:
            tis = tis.filter(or_(
                TI.state == State.FAILED,
                TI.state == State.UPSTREAM_FAILED))
        if only_running:
            tis = tis.filter(TI.state == State.RUNNING)

        if include_subdags:
            from airflow.sensors.external_task_sensor import ExternalTaskMarker

            # Recursively find external tasks indicated by ExternalTaskMarker
            instances = tis.all()
            for ti in instances:
                if ti.operator == ExternalTaskMarker.__name__:
                    ti.task = self.get_task(ti.task_id)

                    if recursion_depth == 0:
                        # Maximum recursion depth allowed is the recursion_depth of the first
                        # ExternalTaskMarker in the tasks to be cleared.
                        max_recursion_depth = ti.task.recursion_depth

                    if recursion_depth + 1 > max_recursion_depth:
                        # Prevent cycles or accidents.
                        raise AirflowException("Maximum recursion depth {} reached for {} {}. "
                                               "Attempted to clear too many tasks "
                                               "or there may be a cyclic dependency."
                                               .format(max_recursion_depth,
                                                       ExternalTaskMarker.__name__, ti.task_id))
                    ti.render_templates()
                    external_tis = session.query(TI).filter(TI.dag_id == ti.task.external_dag_id,
                                                            TI.task_id == ti.task.external_task_id,
                                                            TI.execution_date ==
                                                            pendulum.parse(ti.task.execution_date))

                    for tii in external_tis:
                        if not dag_bag:
                            dag_bag = DagBag()
                        external_dag = dag_bag.get_dag(tii.dag_id)
                        if not external_dag:
                            raise AirflowException("Could not find dag {}".format(tii.dag_id))
                        downstream = external_dag.sub_dag(
                            task_regex=r"^{}$".format(tii.task_id),
                            include_upstream=False,
                            include_downstream=True
                        )
                        tis = tis.union(downstream.clear(start_date=tii.execution_date,
                                                         end_date=tii.execution_date,
                                                         only_failed=only_failed,
                                                         only_running=only_running,
                                                         confirm_prompt=confirm_prompt,
                                                         include_subdags=include_subdags,
                                                         include_parentdag=False,
                                                         reset_dag_runs=reset_dag_runs,
                                                         get_tis=True,
                                                         session=session,
                                                         recursion_depth=recursion_depth + 1,
                                                         max_recursion_depth=max_recursion_depth,
                                                         dag_bag=dag_bag))

        if get_tis:
            return tis

        tis = tis.all()

        if dry_run:
            session.expunge_all()
            return tis

        # Do not use count() here, it's actually much slower than just retrieving all the rows when
        # tis has multiple UNION statements.
        count = len(tis)
        do_it = True
        if count == 0:
            return 0
        if confirm_prompt:
            ti_list = "\n".join([str(t) for t in tis])
            question = (
                "You are about to delete these {count} tasks:\n"
                "{ti_list}\n\n"
                "Are you sure? (yes/no): ").format(count=count, ti_list=ti_list)
            do_it = utils.helpers.ask_yesno(question)

        if do_it:
            clear_task_instances(tis,
                                 session,
                                 dag=self,
                                 )
            if reset_dag_runs:
                self.set_dag_runs_state(session=session,
                                        start_date=start_date,
                                        end_date=end_date,
                                        )
        else:
            count = 0
            print("Bail. Nothing was cleared.")

        session.commit()
        return count

    @classmethod
    def clear_dags(
            cls, dags,
            start_date=None,
            end_date=None,
            only_failed=False,
            only_running=False,
            confirm_prompt=False,
            include_subdags=True,
            include_parentdag=False,
            reset_dag_runs=True,
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
                reset_dag_runs=reset_dag_runs,
                dry_run=True)
            all_tis.extend(tis)

        if dry_run:
            return all_tis

        count = len(all_tis)
        do_it = True
        if count == 0:
            print("Nothing to clear.")
            return 0
        if confirm_prompt:
            ti_list = "\n".join([str(t) for t in all_tis])
            question = (
                "You are about to delete these {} tasks:\n"
                "{}\n\n"
                "Are you sure? (yes/no): ").format(count, ti_list)
            do_it = utils.helpers.ask_yesno(question)

        if do_it:
            for dag in dags:
                dag.clear(start_date=start_date,
                          end_date=end_date,
                          only_failed=only_failed,
                          only_running=only_running,
                          confirm_prompt=False,
                          include_subdags=include_subdags,
                          reset_dag_runs=reset_dag_runs,
                          dry_run=False,
                          )
        else:
            count = 0
            print("Bail. Nothing was cleared.")
        return count

    def __deepcopy__(self, memo):
        # Swiwtcharoo to go around deepcopying objects coming through the
        # backdoor
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result
        for k, v in list(self.__dict__.items()):
            if k not in ('user_defined_macros', 'user_defined_filters', 'params'):
                setattr(result, k, copy.deepcopy(v, memo))

        result.user_defined_macros = self.user_defined_macros
        result.user_defined_filters = self.user_defined_filters
        result.params = self.params
        return result

    def sub_dag(self, task_regex, include_downstream=False,
                include_upstream=True):
        """
        Returns a subset of the current dag as a deep copy of the current dag
        based on a regex that should match one or many tasks, and includes
        upstream and downstream neighbours based on the flag passed.
        """

        # deep-copying self.task_dict takes a long time, and we don't want all
        # the tasks anyway, so we copy the tasks manually later
        task_dict = self.task_dict
        self.task_dict = {}
        dag = copy.deepcopy(self)
        self.task_dict = task_dict

        regex_match = [
            t for t in self.tasks if re.findall(task_regex, t.task_id)]
        also_include = []
        for t in regex_match:
            if include_downstream:
                also_include += t.get_flat_relatives(upstream=False)
            if include_upstream:
                also_include += t.get_flat_relatives(upstream=True)

        # Compiling the unique list of tasks that made the cut
        # Make sure to not recursively deepcopy the dag while copying the task
        dag.task_dict = {t.task_id: copy.deepcopy(t, {id(t.dag): dag})
                         for t in regex_match + also_include}
        for t in dag.tasks:
            # Removing upstream/downstream references to tasks that did not
            # made the cut
            t._upstream_task_ids = t._upstream_task_ids.intersection(dag.task_dict.keys())
            t._downstream_task_ids = t._downstream_task_ids.intersection(
                dag.task_dict.keys())

        if len(dag.tasks) < len(self.tasks):
            dag.partial = True

        return dag

    def has_task(self, task_id):
        return task_id in (t.task_id for t in self.tasks)

    def get_task(self, task_id):
        if task_id in self.task_dict:
            return self.task_dict[task_id]
        raise TaskNotFound("Task {task_id} not found".format(task_id=task_id))

    def pickle_info(self):
        d = dict()
        d['is_picklable'] = True
        try:
            dttm = timezone.utcnow()
            pickled = pickle.dumps(self)
            d['pickle_len'] = len(pickled)
            d['pickling_duration'] = "{}".format(timezone.utcnow() - dttm)
        except Exception as e:
            self.log.debug(e)
            d['is_picklable'] = False
            d['stacktrace'] = traceback.format_exc()
        return d

    @provide_session
    def pickle(self, session=None):
        dag = session.query(
            DagModel).filter(DagModel.dag_id == self.dag_id).first()
        dp = None
        if dag and dag.pickle_id:
            dp = session.query(DagPickle).filter(
                DagPickle.id == dag.pickle_id).first()
        if not dp or dp.pickle != self:
            dp = DagPickle(dag=self)
            session.add(dp)
            self.last_pickled = timezone.utcnow()
            session.commit()
            self.pickle_id = dp.id

        return dp

    def tree_view(self):
        """Print an ASCII tree representation of the DAG."""
        def get_downstream(task, level=0):
            line = (" " * level * 4) + str(task)
            if six.PY2:
                line = line.decode("utf-8")
            print(line)
            level += 1
            for t in task.downstream_list:
                get_downstream(t, level)

        for t in self.roots:
            get_downstream(t)

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

        if task.task_id in self.task_dict:
            # TODO: raise an error in Airflow 2.0
            warnings.warn(
                'The requested task could not be added to the DAG because a '
                'task with task_id {} is already in the DAG. Starting in '
                'Airflow 2.0, trying to overwrite a task will raise an '
                'exception.'.format(task.task_id),
                category=PendingDeprecationWarning)
        else:
            self.task_dict[task.task_id] = task
            task.dag = self

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
            ignore_first_depends_on_past=False,
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
        :type executor: airflow.executor.BaseExecutor
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
        from airflow.jobs import BackfillJob
        if not executor and local:
            executor = LocalExecutor()
        elif not executor:
            executor = get_default_executor()
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
        """
        Exposes a CLI specific to this DAG
        """
        from airflow.bin import cli
        parser = cli.CLIFactory.get_parser(dag_parser=True)
        args = parser.parse_args()
        args.func(args, self)

    @provide_session
    def create_dagrun(self,
                      run_id,
                      state,
                      execution_date=None,
                      start_date=None,
                      external_trigger=False,
                      conf=None,
                      session=None):
        """
        Creates a dag run from this dag including the tasks associated with this dag.
        Returns the dag run.

        :param run_id: defines the the run id for this dag run
        :type run_id: str
        :param execution_date: the execution date of this dag run
        :type execution_date: datetime.datetime
        :param state: the state of the dag run
        :type state: airflow.utils.state.State
        :param start_date: the date this dag run should be evaluated
        :type start_date: datetime
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        run = DagRun(
            dag_id=self.dag_id,
            run_id=run_id,
            execution_date=execution_date,
            start_date=start_date,
            external_trigger=external_trigger,
            conf=conf,
            state=state
        )
        session.add(run)

        session.commit()

        run.dag = self

        # create the associated task instances
        # state is None at the moment of creation
        run.verify_integrity(session=session)

        run.refresh_from_db()

        return run

    @provide_session
    def sync_to_db(self, owner=None, sync_time=None, session=None):
        """
        Save attributes about this DAG to the DB. Note that this method
        can be called for both DAGs and SubDAGs. A SubDag is actually a
        SubDagOperator.

        :param dag: the DAG object to save to the DB
        :type dag: airflow.models.DAG
        :param sync_time: The time that the DAG should be marked as sync'ed
        :type sync_time: datetime
        :return: None
        """
        from airflow.models.serialized_dag import SerializedDagModel

        if owner is None:
            owner = self.owner
        if sync_time is None:
            sync_time = timezone.utcnow()

        orm_dag = session.query(
            DagModel).filter(DagModel.dag_id == self.dag_id).first()
        if not orm_dag:
            orm_dag = DagModel(dag_id=self.dag_id)
            if self.is_paused_upon_creation is not None:
                orm_dag.is_paused = self.is_paused_upon_creation
            self.log.info("Creating ORM DAG for %s", self.dag_id)
            session.add(orm_dag)
        if self.is_subdag:
            orm_dag.is_subdag = True
            orm_dag.fileloc = self.parent_dag.fileloc
            orm_dag.root_dag_id = self.parent_dag.dag_id
        else:
            orm_dag.is_subdag = False
            orm_dag.fileloc = self.fileloc
        orm_dag.owners = owner
        orm_dag.is_active = True
        orm_dag.last_scheduler_run = sync_time
        orm_dag.default_view = self._default_view
        orm_dag.description = self.description
        orm_dag.schedule_interval = self.schedule_interval
        orm_dag.tags = self.get_dagtags(session=session)

        if conf.getboolean('core', 'store_dag_code', fallback=False):
            DagCode.bulk_sync_to_db([orm_dag.fileloc])

        session.commit()

        for subdag in self.subdags:
            subdag.sync_to_db(owner=owner, sync_time=sync_time, session=session)

        # Write DAGs to serialized_dag table in DB.
        # subdags are not written into serialized_dag, because they are not displayed
        # in the DAG list on UI. They are included in the serialized parent DAG.
        if STORE_SERIALIZED_DAGS and not self.is_subdag:
            SerializedDagModel.write_dag(
                self,
                min_update_interval=MIN_SERIALIZED_DAG_UPDATE_INTERVAL,
                session=session
            )

    @provide_session
    def get_dagtags(self, session=None):
        """
        Creating a list of DagTags, if one is missing from the DB, will insert.

        :return: The DagTag list.
        :rtype: list
        """
        tags = []
        if not self.tags:
            return tags

        for name in set(self.tags):
            tag = session.query(
                DagTag).filter(DagTag.name == name).filter(DagTag.dag_id == self.dag_id).first()
            if not tag:
                tag = DagTag(name=name, dag_id=self.dag_id)
                session.add(tag)
            tags.append(tag)
        session.commit()
        return tags

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
        for dag in session.query(
                DagModel).filter(~DagModel.dag_id.in_(active_dag_ids)).all():
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
        log = LoggingMixin().log
        for dag in session.query(
                DagModel).filter(DagModel.last_scheduler_run < expiration_date,
                                 DagModel.is_active).all():
            log.info(
                "Deactivating DAG ID %s since it was last touched by the scheduler at %s",
                dag.dag_id, dag.last_scheduler_run.isoformat()
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

        if states is not None:
            if None in states:
                qry = qry.filter(or_(
                    TaskInstance.state.in_(states),
                    TaskInstance.state.is_(None)))
            else:
                qry = qry.filter(TaskInstance.state.in_(states))
        return qry.scalar()

    def test_cycle(self):
        """
        Check to see if there are any cycles in the DAG. Returns False if no cycle found,
        otherwise raises exception.
        """
        from airflow.models.dagbag import DagBag  # Avoid circular imports

        # default of int is 0 which corresponds to CYCLE_NEW
        visit_map = defaultdict(int)
        for task_id in self.task_dict.keys():
            # print('starting %s' % task_id)
            if visit_map[task_id] == DagBag.CYCLE_NEW:
                self._test_cycle_helper(visit_map, task_id)
        return False

    def _test_cycle_helper(self, visit_map, task_id):
        """
        Checks if a cycle exists from the input task using DFS traversal
        """
        from airflow.models.dagbag import DagBag  # Avoid circular imports

        # print('Inspecting %s' % task_id)
        if visit_map[task_id] == DagBag.CYCLE_DONE:
            return False

        visit_map[task_id] = DagBag.CYCLE_IN_PROGRESS

        task = self.task_dict[task_id]
        for descendant_id in task.get_direct_relative_ids():
            if visit_map[descendant_id] == DagBag.CYCLE_IN_PROGRESS:
                msg = "Cycle detected in DAG. Faulty task: {0} to {1}".format(
                    task_id, descendant_id)
                raise AirflowDagCycleException(msg)
            else:
                self._test_cycle_helper(visit_map, descendant_id)

        visit_map[task_id] = DagBag.CYCLE_DONE

    @classmethod
    def get_serialized_fields(cls):
        """Stringified DAGs and operators contain exactly these fields."""
        if not cls.__serialized_fields:
            cls.__serialized_fields = frozenset(vars(DAG(dag_id='test')).keys()) - {
                'parent_dag', '_old_context_manager_dags', 'safe_dag_id', 'last_loaded',
                '_full_filepath', 'user_defined_filters', 'user_defined_macros',
                '_schedule_interval', 'partial', '_old_context_manager_dags',
                '_pickle_id', '_log', 'is_subdag', 'task_dict', 'template_searchpath',
                'sla_miss_callback', 'on_success_callback', 'on_failure_callback',
                'template_undefined', 'jinja_environment_kwargs'
            }
        return cls.__serialized_fields


class DagTag(Base):
    """
    A tag name per dag, to allow quick filtering in the DAG view.
    """
    __tablename__ = "dag_tag"
    name = Column(String(100), primary_key=True)
    dag_id = Column(String(ID_LEN), ForeignKey('dag.dag_id'), primary_key=True)


class DagModel(Base):

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = Column(String(ID_LEN), primary_key=True)
    root_dag_id = Column(String(ID_LEN))
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused_at_creation = conf\
        .getboolean('core',
                    'dags_are_paused_at_creation')
    is_paused = Column(Boolean, default=is_paused_at_creation)
    # Whether the DAG is a subdag
    is_subdag = Column(Boolean, default=False)
    # Whether that DAG was seen on the last DagBag load
    is_active = Column(Boolean, default=False)
    # Last time the scheduler started
    last_scheduler_run = Column(UtcDateTime)
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

    __table_args__ = (
        Index('idx_root_dag_id', root_dag_id, unique=False),
    )

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    @property
    def timezone(self):
        return settings.TIMEZONE

    @staticmethod
    @provide_session
    def get_dagmodel(dag_id, session=None):
        return session.query(DagModel).filter(DagModel.dag_id == dag_id).first()

    @classmethod
    @provide_session
    def get_current(cls, dag_id, session=None):
        return session.query(cls).filter(cls.dag_id == dag_id).first()

    def get_default_view(self):
        if self.default_view is None:
            return conf.get('webserver', 'dag_default_view').lower()
        else:
            return self.default_view

    @provide_session
    def get_last_dagrun(self, session=None, include_externally_triggered=False):
        return get_last_dagrun(self.dag_id, session=session,
                               include_externally_triggered=include_externally_triggered)

    @property
    def safe_dag_id(self):
        return self.dag_id.replace('.', '__dot__')

    def get_dag(self, store_serialized_dags=False):
        """Creates a dagbag to load and return a DAG.
        Calling it from UI should set store_serialized_dags = STORE_SERIALIZED_DAGS.
        There may be a delay for scheduler to write serialized DAG into database,
        loads from file in this case.
        FIXME: remove it when webserver does not access to DAG folder in future.
        """
        dag = DagBag(
            dag_folder=self.fileloc, store_serialized_dags=store_serialized_dags).get_dag(self.dag_id)
        if store_serialized_dags and dag is None:
            dag = self.get_dag()
        return dag

    @provide_session
    def create_dagrun(self,
                      run_id,
                      state,
                      execution_date,
                      start_date=None,
                      external_trigger=False,
                      conf=None,
                      session=None):
        """
        Creates a dag run from this dag including the tasks associated with this dag.
        Returns the dag run.

        :param run_id: defines the the run id for this dag run
        :type run_id: str
        :param execution_date: the execution date of this dag run
        :type execution_date: datetime.datetime
        :param state: the state of the dag run
        :type state: airflow.utils.state.State
        :param start_date: the date this dag run should be evaluated
        :type start_date: datetime.datetime
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """

        return self.get_dag().create_dagrun(run_id=run_id,
                                            state=state,
                                            execution_date=execution_date,
                                            start_date=start_date,
                                            external_trigger=external_trigger,
                                            conf=conf,
                                            session=session)

    @provide_session
    def set_is_paused(self,
                      is_paused,  # type: bool
                      including_subdags=True,  # type: bool
                      store_serialized_dags=False,  # type: bool
                      session=None,
                      ):
        # type: (...) -> None
        """
        Pause/Un-pause a DAG.

        :param is_paused: Is the DAG paused
        :param including_subdags: whether to include the DAG's subdags
        :param store_serialized_dags: whether to serialize DAGs & store it in DB
        :param session: session
        """
        dag_ids = [self.dag_id]  # type: List[str]
        if including_subdags:
            dag = self.get_dag(store_serialized_dags)
            if dag is None:
                raise DagNotFound("Dag id {} not found".format(self.dag_id))
            subdags = dag.subdags
            dag_ids.extend([subdag.dag_id for subdag in subdags])
        dag_models = session.query(DagModel).filter(DagModel.dag_id.in_(dag_ids)).all()
        try:
            for dag_model in dag_models:
                dag_model.is_paused = is_paused
            session.commit()
        except Exception:
            session.rollback()
            raise

    @classmethod
    @provide_session
    def deactivate_deleted_dags(cls, alive_dag_filelocs, session=None):
        """
        Set ``is_active=False`` on the DAGs for which the DAG files have been removed.
        Additionally change ``is_active=False`` to ``True`` if the DAG file exists.

        :param alive_dag_filelocs: file paths of alive DAGs
        :param session: ORM Session
        """
        log = LoggingMixin().log
        log.debug("Deactivating DAGs (for which DAG files are deleted) from %s table ",
                  cls.__tablename__)
        dag_models = session.query(cls).all()
        try:
            for dag_model in dag_models:
                if dag_model.fileloc is not None:
                    if correct_maybe_zipped(dag_model.fileloc) not in alive_dag_filelocs:
                        dag_model.is_active = False
                    else:
                        # If is_active is set as False and the DAG File still exists
                        # Change is_active=True
                        if not dag_model.is_active:
                            dag_model.is_active = True
                else:
                    continue
            session.commit()
        except Exception:
            session.rollback()
            raise
