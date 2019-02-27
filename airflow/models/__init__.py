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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import copy
from collections import defaultdict, namedtuple, OrderedDict
from builtins import ImportError as BuiltinImportError, bytes, object, str
from future.standard_library import install_aliases

from airflow.models.base import Base, ID_LEN

try:
    # Fix Python > 3.7 deprecation
    from collections.abc import Hashable
except ImportError:
    # Preserve Python < 3.3 compatibility
    from collections import Hashable
from datetime import timedelta

import dill
import functools
import getpass
import imp
import importlib
import zipfile
import jinja2
import json
import logging
import os
import pendulum
import pickle
import re
import signal
import sys
import textwrap
import traceback
import warnings
import hashlib

from datetime import datetime
from urllib.parse import quote

from sqlalchemy import (
    Boolean, Column, DateTime, Float, Index, Integer, PickleType, String,
    Text, UniqueConstraint, and_, func, or_
)
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import reconstructor, synonym

from croniter import (
    croniter, CroniterBadCronError, CroniterBadDateError, CroniterNotAlphaError
)
import six

from airflow import settings, utils
from airflow.executors import get_default_executor, LocalExecutor
from airflow import configuration
from airflow.exceptions import (
    AirflowDagCycleException, AirflowException, AirflowSkipException, AirflowTaskTimeout,
    AirflowRescheduleException
)
from airflow.dag.base_dag import BaseDag, BaseDagBag
from airflow.lineage import apply_lineage, prepare_lineage
from airflow.models.dagpickle import DagPickle
from airflow.models.kubernetes import KubeWorkerIdentifier, KubeResourceVersion  # noqa: F401
from airflow.models.log import Log
from airflow.models.taskfail import TaskFail
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.xcom import XCom
from airflow.ti_deps.deps.not_in_retry_period_dep import NotInRetryPeriodDep
from airflow.ti_deps.deps.prev_dagrun_dep import PrevDagrunDep
from airflow.ti_deps.deps.trigger_rule_dep import TriggerRuleDep

from airflow.ti_deps.dep_context import DepContext, QUEUE_DEPS, RUN_DEPS
from airflow.utils import timezone
from airflow.utils.dag_processing import list_py_file_paths
from airflow.utils.dates import cron_presets, date_range as utils_date_range
from airflow.utils.db import provide_session
from airflow.utils.decorators import apply_defaults
from airflow.utils.email import send_email
from airflow.utils.helpers import is_container, validate_key, pprinttable
from airflow.utils.operator_resources import Resources
from airflow.utils.state import State
from airflow.utils.sqlalchemy import UtcDateTime, Interval
from airflow.utils.timeout import timeout
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.weight_rule import WeightRule
from airflow.utils.net import get_hostname
from airflow.utils.log.logging_mixin import LoggingMixin

install_aliases()

XCOM_RETURN_KEY = 'return_value'

Stats = settings.Stats


class InvalidFernetToken(Exception):
    # If Fernet isn't loaded we need a valid exception class to catch. If it is
    # loaded this will get reset to the actual class once get_fernet() is called
    pass


class NullFernet(object):
    """
    A "Null" encryptor class that doesn't encrypt or decrypt but that presents
    a similar interface to Fernet.

    The purpose of this is to make the rest of the code not have to know the
    difference, and to only display the message once, not 20 times when
    `airflow initdb` is ran.
    """
    is_encrypted = False

    def decrpyt(self, b):
        return b

    def encrypt(self, b):
        return b


_fernet = None


def get_fernet():
    """
    Deferred load of Fernet key.

    This function could fail either because Cryptography is not installed
    or because the Fernet key is invalid.

    :return: Fernet object
    :raises: airflow.exceptions.AirflowException if there's a problem trying to load Fernet
    """
    global _fernet
    log = LoggingMixin().log

    if _fernet:
        return _fernet
    try:
        from cryptography.fernet import Fernet, MultiFernet, InvalidToken
        global InvalidFernetToken
        InvalidFernetToken = InvalidToken

    except BuiltinImportError:
        log.warning(
            "cryptography not found - values will not be stored encrypted."
        )
        _fernet = NullFernet()
        return _fernet

    try:
        fernet_key = configuration.conf.get('core', 'FERNET_KEY')
        if not fernet_key:
            log.warning(
                "empty cryptography key - values will not be stored encrypted."
            )
            _fernet = NullFernet()
        else:
            _fernet = MultiFernet([
                Fernet(fernet_part.encode('utf-8'))
                for fernet_part in fernet_key.split(',')
            ])
            _fernet.is_encrypted = True
    except (ValueError, TypeError) as ve:
        raise AirflowException("Could not create Fernet object: {}".format(ve))

    return _fernet


# Used by DAG context_managers
_CONTEXT_MANAGER_DAG = None


def clear_task_instances(tis,
                         session,
                         activate_dag_runs=True,
                         dag=None,
                         ):
    """
    Clears a set of task instances, but makes sure the running ones
    get killed.

    :param tis: a list of task instances
    :param session: current session
    :param activate_dag_runs: flag to check for active dag run
    :param dag: DAG object
    """
    job_ids = []
    for ti in tis:
        if ti.state == State.RUNNING:
            if ti.job_id:
                ti.state = State.SHUTDOWN
                job_ids.append(ti.job_id)
        else:
            task_id = ti.task_id
            if dag and dag.has_task(task_id):
                task = dag.get_task(task_id)
                task_retries = task.retries
                ti.max_tries = ti.try_number + task_retries - 1
            else:
                # Ignore errors when updating max_tries if dag is None or
                # task not found in dag since database records could be
                # outdated. We make max_tries the maximum value of its
                # original max_tries or the current task try number.
                ti.max_tries = max(ti.max_tries, ti.try_number - 1)
            ti.state = State.NONE
            session.merge(ti)

    if job_ids:
        from airflow.jobs import BaseJob as BJ
        for job in session.query(BJ).filter(BJ.id.in_(job_ids)).all():
            job.state = State.SHUTDOWN

    if activate_dag_runs and tis:
        drs = session.query(DagRun).filter(
            DagRun.dag_id.in_({ti.dag_id for ti in tis}),
            DagRun.execution_date.in_({ti.execution_date for ti in tis}),
        ).all()
        for dr in drs:
            dr.state = State.RUNNING
            dr.start_date = timezone.utcnow()


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


class DagBag(BaseDagBag, LoggingMixin):
    """
    A dagbag is a collection of dags, parsed out of a folder tree and has high
    level configuration settings, like what database to use as a backend and
    what executor to use to fire off tasks. This makes it easier to run
    distinct environments for say production and development, tests, or for
    different teams or security profiles. What would have been system level
    settings are now dagbag level so that one system can run multiple,
    independent settings sets.

    :param dag_folder: the folder to scan to find DAGs
    :type dag_folder: unicode
    :param executor: the executor to use when executing task instances
        in this DagBag
    :param include_examples: whether to include the examples that ship
        with airflow or not
    :type include_examples: bool
    :param has_logged: an instance boolean that gets flipped from False to True after a
        file has been skipped. This is to prevent overloading the user with logging
        messages about skipped files. Therefore only once per DagBag is a file logged
        being skipped.
    """

    # static class variables to detetct dag cycle
    CYCLE_NEW = 0
    CYCLE_IN_PROGRESS = 1
    CYCLE_DONE = 2

    def __init__(
            self,
            dag_folder=None,
            executor=None,
            include_examples=configuration.conf.getboolean('core', 'LOAD_EXAMPLES'),
            safe_mode=configuration.conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE')):

        # do not use default arg in signature, to fix import cycle on plugin load
        if executor is None:
            executor = get_default_executor()
        dag_folder = dag_folder or settings.DAGS_FOLDER
        self.log.info("Filling up the DagBag from %s", dag_folder)
        self.dag_folder = dag_folder
        self.dags = {}
        # the file's last modified timestamp when we last read it
        self.file_last_changed = {}
        self.executor = executor
        self.import_errors = {}
        self.has_logged = False

        self.collect_dags(
            dag_folder=dag_folder,
            include_examples=include_examples,
            safe_mode=safe_mode)

    def size(self):
        """
        :return: the amount of dags contained in this dagbag
        """
        return len(self.dags)

    def get_dag(self, dag_id):
        """
        Gets the DAG out of the dictionary, and refreshes it if expired
        """
        # If asking for a known subdag, we want to refresh the parent
        root_dag_id = dag_id
        if dag_id in self.dags:
            dag = self.dags[dag_id]
            if dag.is_subdag:
                root_dag_id = dag.parent_dag.dag_id

        # If the dag corresponding to root_dag_id is absent or expired
        orm_dag = DagModel.get_current(root_dag_id)
        if orm_dag and (
                root_dag_id not in self.dags or
                (
                    orm_dag.last_expired and
                    dag.last_loaded < orm_dag.last_expired
                )
        ):
            # Reprocess source file
            found_dags = self.process_file(
                filepath=orm_dag.fileloc, only_if_updated=False)

            # If the source file no longer exports `dag_id`, delete it from self.dags
            if found_dags and dag_id in [found_dag.dag_id for found_dag in found_dags]:
                return self.dags[dag_id]
            elif dag_id in self.dags:
                del self.dags[dag_id]
        return self.dags.get(dag_id)

    def process_file(self, filepath, only_if_updated=True, safe_mode=True):
        """
        Given a path to a python module or zip file, this method imports
        the module and look for dag objects within it.
        """
        found_dags = []

        # if the source file no longer exists in the DB or in the filesystem,
        # return an empty list
        # todo: raise exception?
        if filepath is None or not os.path.isfile(filepath):
            return found_dags

        try:
            # This failed before in what may have been a git sync
            # race condition
            file_last_changed_on_disk = datetime.fromtimestamp(os.path.getmtime(filepath))
            if only_if_updated \
                    and filepath in self.file_last_changed \
                    and file_last_changed_on_disk == self.file_last_changed[filepath]:
                return found_dags

        except Exception as e:
            self.log.exception(e)
            return found_dags

        mods = []
        is_zipfile = zipfile.is_zipfile(filepath)
        if not is_zipfile:
            if safe_mode and os.path.isfile(filepath):
                with open(filepath, 'rb') as f:
                    content = f.read()
                    if not all([s in content for s in (b'DAG', b'airflow')]):
                        self.file_last_changed[filepath] = file_last_changed_on_disk
                        # Don't want to spam user with skip messages
                        if not self.has_logged:
                            self.has_logged = True
                            self.log.info(
                                "File %s assumed to contain no DAGs. Skipping.",
                                filepath)
                        return found_dags

            self.log.debug("Importing %s", filepath)
            org_mod_name, _ = os.path.splitext(os.path.split(filepath)[-1])
            mod_name = ('unusual_prefix_' +
                        hashlib.sha1(filepath.encode('utf-8')).hexdigest() +
                        '_' + org_mod_name)

            if mod_name in sys.modules:
                del sys.modules[mod_name]

            with timeout(configuration.conf.getint('core', "DAGBAG_IMPORT_TIMEOUT")):
                try:
                    m = imp.load_source(mod_name, filepath)
                    mods.append(m)
                except Exception as e:
                    self.log.exception("Failed to import: %s", filepath)
                    self.import_errors[filepath] = str(e)
                    self.file_last_changed[filepath] = file_last_changed_on_disk

        else:
            zip_file = zipfile.ZipFile(filepath)
            for mod in zip_file.infolist():
                head, _ = os.path.split(mod.filename)
                mod_name, ext = os.path.splitext(mod.filename)
                if not head and (ext == '.py' or ext == '.pyc'):
                    if mod_name == '__init__':
                        self.log.warning("Found __init__.%s at root of %s", ext, filepath)
                    if safe_mode:
                        with zip_file.open(mod.filename) as zf:
                            self.log.debug("Reading %s from %s", mod.filename, filepath)
                            content = zf.read()
                            if not all([s in content for s in (b'DAG', b'airflow')]):
                                self.file_last_changed[filepath] = (
                                    file_last_changed_on_disk)
                                # todo: create ignore list
                                # Don't want to spam user with skip messages
                                if not self.has_logged:
                                    self.has_logged = True
                                    self.log.info(
                                        "File %s assumed to contain no DAGs. Skipping.",
                                        filepath)

                    if mod_name in sys.modules:
                        del sys.modules[mod_name]

                    try:
                        sys.path.insert(0, filepath)
                        m = importlib.import_module(mod_name)
                        mods.append(m)
                    except Exception as e:
                        self.log.exception("Failed to import: %s", filepath)
                        self.import_errors[filepath] = str(e)
                        self.file_last_changed[filepath] = file_last_changed_on_disk

        for m in mods:
            for dag in list(m.__dict__.values()):
                if isinstance(dag, DAG):
                    if not dag.full_filepath:
                        dag.full_filepath = filepath
                        if dag.fileloc != filepath and not is_zipfile:
                            dag.fileloc = filepath
                    try:
                        dag.is_subdag = False
                        self.bag_dag(dag, parent_dag=dag, root_dag=dag)
                        if isinstance(dag._schedule_interval, six.string_types):
                            croniter(dag._schedule_interval)
                        found_dags.append(dag)
                        found_dags += dag.subdags
                    except (CroniterBadCronError,
                            CroniterBadDateError,
                            CroniterNotAlphaError) as cron_e:
                        self.log.exception("Failed to bag_dag: %s", dag.full_filepath)
                        self.import_errors[dag.full_filepath] = \
                            "Invalid Cron expression: " + str(cron_e)
                        self.file_last_changed[dag.full_filepath] = \
                            file_last_changed_on_disk
                    except AirflowDagCycleException as cycle_exception:
                        self.log.exception("Failed to bag_dag: %s", dag.full_filepath)
                        self.import_errors[dag.full_filepath] = str(cycle_exception)
                        self.file_last_changed[dag.full_filepath] = \
                            file_last_changed_on_disk

        self.file_last_changed[filepath] = file_last_changed_on_disk
        return found_dags

    @provide_session
    def kill_zombies(self, zombies, session=None):
        """
        Fail given zombie tasks, which are tasks that haven't
        had a heartbeat for too long, in the current DagBag.

        :param zombies: zombie task instances to kill.
        :type zombies: airflow.utils.dag_processing.SimpleTaskInstance
        :param session: DB session.
        :type session: sqlalchemy.orm.session.Session
        """
        for zombie in zombies:
            if zombie.dag_id in self.dags:
                dag = self.dags[zombie.dag_id]
                if zombie.task_id in dag.task_ids:
                    task = dag.get_task(zombie.task_id)
                    ti = TaskInstance(task, zombie.execution_date)
                    # Get properties needed for failure handling from SimpleTaskInstance.
                    ti.start_date = zombie.start_date
                    ti.end_date = zombie.end_date
                    ti.try_number = zombie.try_number
                    ti.state = zombie.state
                    ti.test_mode = configuration.getboolean('core', 'unit_test_mode')
                    ti.handle_failure("{} detected as zombie".format(ti),
                                      ti.test_mode, ti.get_template_context())
                    self.log.info(
                        'Marked zombie job %s as %s', ti, ti.state)
                    Stats.incr('zombies_killed')
        session.commit()

    def bag_dag(self, dag, parent_dag, root_dag):
        """
        Adds the DAG into the bag, recurses into sub dags.
        Throws AirflowDagCycleException if a cycle is detected in this dag or its subdags
        """

        dag.test_cycle()  # throws if a task cycle is found

        dag.resolve_template_files()
        dag.last_loaded = timezone.utcnow()

        for task in dag.tasks:
            settings.policy(task)

        subdags = dag.subdags

        try:
            for subdag in subdags:
                subdag.full_filepath = dag.full_filepath
                subdag.parent_dag = dag
                subdag.is_subdag = True
                self.bag_dag(subdag, parent_dag=dag, root_dag=root_dag)

            self.dags[dag.dag_id] = dag
            self.log.debug('Loaded DAG {dag}'.format(**locals()))
        except AirflowDagCycleException as cycle_exception:
            # There was an error in bagging the dag. Remove it from the list of dags
            self.log.exception('Exception bagging dag: {dag.dag_id}'.format(**locals()))
            # Only necessary at the root level since DAG.subdags automatically
            # performs DFS to search through all subdags
            if dag == root_dag:
                for subdag in subdags:
                    if subdag.dag_id in self.dags:
                        del self.dags[subdag.dag_id]
            raise cycle_exception

    def collect_dags(
            self,
            dag_folder=None,
            only_if_updated=True,
            include_examples=configuration.conf.getboolean('core', 'LOAD_EXAMPLES'),
            safe_mode=configuration.conf.getboolean('core', 'DAG_DISCOVERY_SAFE_MODE')):
        """
        Given a file path or a folder, this method looks for python modules,
        imports them and adds them to the dagbag collection.

        Note that if a ``.airflowignore`` file is found while processing
        the directory, it will behave much like a ``.gitignore``,
        ignoring files that match any of the regex patterns specified
        in the file.

        **Note**: The patterns in .airflowignore are treated as
        un-anchored regexes, not shell-like glob patterns.
        """
        start_dttm = timezone.utcnow()
        dag_folder = dag_folder or self.dag_folder

        # Used to store stats around DagBag processing
        stats = []
        FileLoadStat = namedtuple(
            'FileLoadStat', "file duration dag_num task_num dags")

        for filepath in list_py_file_paths(dag_folder, safe_mode=safe_mode,
                                           include_examples=include_examples):
            try:
                ts = timezone.utcnow()
                found_dags = self.process_file(
                    filepath, only_if_updated=only_if_updated,
                    safe_mode=safe_mode)

                td = timezone.utcnow() - ts
                td = td.total_seconds() + (
                    float(td.microseconds) / 1000000)
                stats.append(FileLoadStat(
                    filepath.replace(dag_folder, ''),
                    td,
                    len(found_dags),
                    sum([len(dag.tasks) for dag in found_dags]),
                    str([dag.dag_id for dag in found_dags]),
                ))
            except Exception as e:
                self.log.exception(e)
        Stats.gauge(
            'collect_dags', (timezone.utcnow() - start_dttm).total_seconds(), 1)
        Stats.gauge(
            'dagbag_size', len(self.dags), 1)
        Stats.gauge(
            'dagbag_import_errors', len(self.import_errors), 1)
        self.dagbag_stats = sorted(
            stats, key=lambda x: x.duration, reverse=True)

    def dagbag_report(self):
        """Prints a report around DagBag loading stats"""
        report = textwrap.dedent("""\n
        -------------------------------------------------------------------
        DagBag loading stats for {dag_folder}
        -------------------------------------------------------------------
        Number of DAGs: {dag_num}
        Total task number: {task_num}
        DagBag parsing time: {duration}
        {table}
        """)
        stats = self.dagbag_stats
        return report.format(
            dag_folder=self.dag_folder,
            duration=sum([o.duration for o in stats]),
            dag_num=sum([o.dag_num for o in stats]),
            task_num=sum([o.task_num for o in stats]),
            table=pprinttable(stats),
        )


class TaskInstance(Base, LoggingMixin):
    """
    Task instances store the state of a task instance. This table is the
    authority and single source of truth around what tasks have run and the
    state they are in.

    The SqlAlchemy model doesn't have a SqlAlchemy foreign key to the task or
    dag model deliberately to have more control over transactions.

    Database transactions on this table should insure double triggers and
    any confusion around what task instances are or aren't ready to run
    even while multiple schedulers may be firing task instances.
    """

    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN), primary_key=True)
    dag_id = Column(String(ID_LEN), primary_key=True)
    execution_date = Column(UtcDateTime, primary_key=True)
    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(50))
    queue = Column(String(50))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    pid = Column(Integer)
    executor_config = Column(PickleType(pickler=dill))

    __table_args__ = (
        Index('ti_dag_state', dag_id, state),
        Index('ti_dag_date', dag_id, execution_date),
        Index('ti_state', state),
        Index('ti_state_lkp', dag_id, task_id, execution_date, state),
        Index('ti_pool', pool, state, priority_weight),
        Index('ti_job_id', job_id),
    )

    def __init__(self, task, execution_date, state=None):
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.task = task
        self._log = logging.getLogger("airflow.task")

        # make sure we have a localized execution_date stored in UTC
        if execution_date and not timezone.is_localized(execution_date):
            self.log.warning("execution date %s has no timezone information. Using "
                             "default from dag or system", execution_date)
            if self.task.has_dag():
                execution_date = timezone.make_aware(execution_date,
                                                     self.task.dag.timezone)
            else:
                execution_date = timezone.make_aware(execution_date)

            execution_date = timezone.convert_to_utc(execution_date)

        self.execution_date = execution_date

        self.queue = task.queue
        self.pool = task.pool
        self.priority_weight = task.priority_weight_total
        self.try_number = 0
        self.max_tries = self.task.retries
        self.unixname = getpass.getuser()
        self.run_as_user = task.run_as_user
        if state:
            self.state = state
        self.hostname = ''
        self.executor_config = task.executor_config
        self.init_on_load()
        # Is this TaskInstance being currently running within `airflow run --raw`.
        # Not persisted to the database so only valid for the current process
        self.raw = False

    @reconstructor
    def init_on_load(self):
        """ Initialize the attributes that aren't stored in the DB. """
        self.test_mode = False  # can be changed when calling 'run'

    @property
    def try_number(self):
        """
        Return the try number that this task number will be when it is actually
        run.

        If the TI is currently running, this will match the column in the
        databse, in all othercases this will be incremenetd
        """
        # This is designed so that task logs end up in the right file.
        if self.state == State.RUNNING:
            return self._try_number
        return self._try_number + 1

    @try_number.setter
    def try_number(self, value):
        self._try_number = value

    @property
    def next_try_number(self):
        return self._try_number + 1

    def command(
            self,
            mark_success=False,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None,
            pool=None,
            cfg_path=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        return " ".join(self.command_as_list(
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path))

    def command_as_list(
            self,
            mark_success=False,
            ignore_all_deps=False,
            ignore_task_deps=False,
            ignore_depends_on_past=False,
            ignore_ti_state=False,
            local=False,
            pickle_id=None,
            raw=False,
            job_id=None,
            pool=None,
            cfg_path=None):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        dag = self.task.dag

        should_pass_filepath = not pickle_id and dag
        if should_pass_filepath and dag.full_filepath != dag.filepath:
            path = "DAGS_FOLDER/{}".format(dag.filepath)
        elif should_pass_filepath and dag.full_filepath:
            path = dag.full_filepath
        else:
            path = None

        return TaskInstance.generate_command(
            self.dag_id,
            self.task_id,
            self.execution_date,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_ti_state=ignore_ti_state,
            local=local,
            pickle_id=pickle_id,
            file_path=path,
            raw=raw,
            job_id=job_id,
            pool=pool,
            cfg_path=cfg_path)

    @staticmethod
    def generate_command(dag_id,
                         task_id,
                         execution_date,
                         mark_success=False,
                         ignore_all_deps=False,
                         ignore_depends_on_past=False,
                         ignore_task_deps=False,
                         ignore_ti_state=False,
                         local=False,
                         pickle_id=None,
                         file_path=None,
                         raw=False,
                         job_id=None,
                         pool=None,
                         cfg_path=None
                         ):
        """
        Generates the shell command required to execute this task instance.

        :param dag_id: DAG ID
        :type dag_id: unicode
        :param task_id: Task ID
        :type task_id: unicode
        :param execution_date: Execution date for the task
        :type execution_date: datetime
        :param mark_success: Whether to mark the task as successful
        :type mark_success: bool
        :param ignore_all_deps: Ignore all ignorable dependencies.
            Overrides the other ignore_* parameters.
        :type ignore_all_deps: bool
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
            (e.g. for Backfills)
        :type ignore_depends_on_past: bool
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :type ignore_task_deps: bool
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :type ignore_ti_state: bool
        :param local: Whether to run the task locally
        :type local: bool
        :param pickle_id: If the DAG was serialized to the DB, the ID
            associated with the pickled DAG
        :type pickle_id: unicode
        :param file_path: path to the file containing the DAG definition
        :param raw: raw mode (needs more details)
        :param job_id: job ID (needs more details)
        :param pool: the Airflow pool that the task should run in
        :type pool: unicode
        :param cfg_path: the Path to the configuration file
        :type cfg_path: basestring
        :return: shell command that can be used to run the task instance
        """
        iso = execution_date.isoformat()
        cmd = ["airflow", "run", str(dag_id), str(task_id), str(iso)]
        cmd.extend(["--mark_success"]) if mark_success else None
        cmd.extend(["--pickle", str(pickle_id)]) if pickle_id else None
        cmd.extend(["--job_id", str(job_id)]) if job_id else None
        cmd.extend(["-A"]) if ignore_all_deps else None
        cmd.extend(["-i"]) if ignore_task_deps else None
        cmd.extend(["-I"]) if ignore_depends_on_past else None
        cmd.extend(["--force"]) if ignore_ti_state else None
        cmd.extend(["--local"]) if local else None
        cmd.extend(["--pool", pool]) if pool else None
        cmd.extend(["--raw"]) if raw else None
        cmd.extend(["-sd", file_path]) if file_path else None
        cmd.extend(["--cfg_path", cfg_path]) if cfg_path else None
        return cmd

    @property
    def log_filepath(self):
        iso = self.execution_date.isoformat()
        log = os.path.expanduser(configuration.conf.get('core', 'BASE_LOG_FOLDER'))
        return (
            "{log}/{self.dag_id}/{self.task_id}/{iso}.log".format(**locals()))

    @property
    def log_url(self):
        iso = quote(self.execution_date.isoformat())
        base_url = configuration.conf.get('webserver', 'BASE_URL')
        return base_url + (
            "/log?"
            "execution_date={iso}"
            "&task_id={self.task_id}"
            "&dag_id={self.dag_id}"
        ).format(**locals())

    @property
    def mark_success_url(self):
        iso = quote(self.execution_date.isoformat())
        base_url = configuration.conf.get('webserver', 'BASE_URL')
        return base_url + (
            "/success"
            "?task_id={self.task_id}"
            "&dag_id={self.dag_id}"
            "&execution_date={iso}"
            "&upstream=false"
            "&downstream=false"
        ).format(**locals())

    @provide_session
    def current_state(self, session=None):
        """
        Get the very latest state from the database, if a session is passed,
        we use and looking up the state becomes part of the session, otherwise
        a new session is used.
        """
        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date,
        ).all()
        if ti:
            state = ti[0].state
        else:
            state = None
        return state

    @provide_session
    def error(self, session=None):
        """
        Forces the task instance's state to FAILED in the database.
        """
        self.log.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()

    @provide_session
    def refresh_from_db(self, session=None, lock_for_update=False):
        """
        Refreshes the task instance from the database based on the primary key

        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        """
        TI = TaskInstance

        qry = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date == self.execution_date)

        if lock_for_update:
            ti = qry.with_for_update().first()
        else:
            ti = qry.first()
        if ti:
            self.state = ti.state
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            # Get the raw value of try_number column, don't read through the
            # accessor here otherwise it will be incremeneted by one already.
            self.try_number = ti._try_number
            self.max_tries = ti.max_tries
            self.hostname = ti.hostname
            self.pid = ti.pid
            self.executor_config = ti.executor_config
        else:
            self.state = None

    @provide_session
    def clear_xcom_data(self, session=None):
        """
        Clears all XCom data from the database for the task instance
        """
        session.query(XCom).filter(
            XCom.dag_id == self.dag_id,
            XCom.task_id == self.task_id,
            XCom.execution_date == self.execution_date
        ).delete()
        session.commit()

    @property
    def key(self):
        """
        Returns a tuple that identifies the task instance uniquely
        """
        return self.dag_id, self.task_id, self.execution_date, self.try_number

    @provide_session
    def set_state(self, state, session=None):
        self.state = state
        self.start_date = timezone.utcnow()
        self.end_date = timezone.utcnow()
        session.merge(self)
        session.commit()

    @property
    def is_premature(self):
        """
        Returns whether a task is in UP_FOR_RETRY state and its retry interval
        has elapsed.
        """
        # is the task still in the retry waiting period?
        return self.state == State.UP_FOR_RETRY and not self.ready_for_retry()

    @provide_session
    def are_dependents_done(self, session=None):
        """
        Checks whether the dependents of this task instance have all succeeded.
        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.
        """
        task = self.task

        if not task.downstream_task_ids:
            return True

        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(task.downstream_task_ids),
            TaskInstance.execution_date == self.execution_date,
            TaskInstance.state == State.SUCCESS,
        )
        count = ti[0][0]
        return count == len(task.downstream_task_ids)

    @property
    @provide_session
    def previous_ti(self, session=None):
        """ The task instance for the task that ran before this task instance """

        dag = self.task.dag
        if dag:
            dr = self.get_dagrun(session=session)

            # LEGACY: most likely running from unit tests
            if not dr:
                # Means that this TI is NOT being run from a DR, but from a catchup
                previous_scheduled_date = dag.previous_schedule(self.execution_date)
                if not previous_scheduled_date:
                    return None

                return TaskInstance(task=self.task,
                                    execution_date=previous_scheduled_date)

            dr.dag = dag
            if dag.catchup:
                last_dagrun = dr.get_previous_scheduled_dagrun(session=session)
            else:
                last_dagrun = dr.get_previous_dagrun(session=session)

            if last_dagrun:
                return last_dagrun.get_task_instance(self.task_id, session=session)

        return None

    @provide_session
    def are_dependencies_met(
            self,
            dep_context=None,
            session=None,
            verbose=False):
        """
        Returns whether or not all the conditions are met for this task instance to be run
        given the context for the dependencies (e.g. a task instance being force run from
        the UI will ignore some dependencies).

        :param dep_context: The execution context that determines the dependencies that
            should be evaluated.
        :type dep_context: DepContext
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        :param verbose: whether log details on failed dependencies on
            info or debug log level
        :type verbose: bool
        """
        dep_context = dep_context or DepContext()
        failed = False
        verbose_aware_logger = self.log.info if verbose else self.log.debug
        for dep_status in self.get_failed_dep_statuses(
                dep_context=dep_context,
                session=session):
            failed = True

            verbose_aware_logger(
                "Dependencies not met for %s, dependency '%s' FAILED: %s",
                self, dep_status.dep_name, dep_status.reason
            )

        if failed:
            return False

        verbose_aware_logger("Dependencies all met for %s", self)
        return True

    @provide_session
    def get_failed_dep_statuses(
            self,
            dep_context=None,
            session=None):
        dep_context = dep_context or DepContext()
        for dep in dep_context.deps | self.task.deps:
            for dep_status in dep.get_dep_statuses(
                    self,
                    session,
                    dep_context):

                self.log.debug(
                    "%s dependency '%s' PASSED: %s, %s",
                    self, dep_status.dep_name, dep_status.passed, dep_status.reason
                )

                if not dep_status.passed:
                    yield dep_status

    def __repr__(self):
        return (
            "<TaskInstance: {ti.dag_id}.{ti.task_id} "
            "{ti.execution_date} [{ti.state}]>"
        ).format(ti=self)

    def next_retry_datetime(self):
        """
        Get datetime of the next retry if the task instance fails. For exponential
        backoff, retry_delay is used as base and will be converted to seconds.
        """
        delay = self.task.retry_delay
        if self.task.retry_exponential_backoff:
            min_backoff = int(delay.total_seconds() * (2 ** (self.try_number - 2)))
            # deterministic per task instance
            hash = int(hashlib.sha1("{}#{}#{}#{}".format(self.dag_id,
                                                         self.task_id,
                                                         self.execution_date,
                                                         self.try_number)
                                    .encode('utf-8')).hexdigest(), 16)
            # between 0.5 * delay * (2^retry_number) and 1.0 * delay * (2^retry_number)
            modded_hash = min_backoff + hash % min_backoff
            # timedelta has a maximum representable value. The exponentiation
            # here means this value can be exceeded after a certain number
            # of tries (around 50 if the initial delay is 1s, even fewer if
            # the delay is larger). Cap the value here before creating a
            # timedelta object so the operation doesn't fail.
            delay_backoff_in_seconds = min(
                modded_hash,
                timedelta.max.total_seconds() - 1
            )
            delay = timedelta(seconds=delay_backoff_in_seconds)
            if self.task.max_retry_delay:
                delay = min(self.task.max_retry_delay, delay)
        return self.end_date + delay

    def ready_for_retry(self):
        """
        Checks on whether the task instance is in the right state and timeframe
        to be retried.
        """
        return (self.state == State.UP_FOR_RETRY and
                self.next_retry_datetime() < timezone.utcnow())

    @provide_session
    def pool_full(self, session):
        """
        Returns a boolean as to whether the slot pool has room for this
        task to run
        """
        if not self.task.pool:
            return False

        pool = (
            session
            .query(Pool)
            .filter(Pool.pool == self.task.pool)
            .first()
        )
        if not pool:
            return False
        open_slots = pool.open_slots(session=session)

        return open_slots <= 0

    @provide_session
    def get_dagrun(self, session):
        """
        Returns the DagRun for this TaskInstance

        :param session:
        :return: DagRun
        """
        dr = session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == self.execution_date
        ).first()

        return dr

    @provide_session
    def _check_and_change_state_before_execution(
            self,
            verbose=True,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        """
        Checks dependencies and then sets state to RUNNING if they are met. Returns
        True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task

        :param verbose: whether to turn on more verbose logging
        :type verbose: bool
        :param ignore_all_deps: Ignore all of the non-critical dependencies, just runs
        :type ignore_all_deps: bool
        :param ignore_depends_on_past: Ignore depends_on_past DAG attribute
        :type ignore_depends_on_past: bool
        :param ignore_task_deps: Don't check the dependencies of this TI's task
        :type ignore_task_deps: bool
        :param ignore_ti_state: Disregards previous task instance state
        :type ignore_ti_state: bool
        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        :return: whether the state was changed to running or not
        :rtype: bool
        """
        task = self.task
        self.pool = pool or task.pool
        self.test_mode = test_mode
        self.refresh_from_db(session=session, lock_for_update=True)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.operator = task.__class__.__name__

        if not ignore_all_deps and not ignore_ti_state and self.state == State.SUCCESS:
            Stats.incr('previously_succeeded', 1, 1)

        queue_dep_context = DepContext(
            deps=QUEUE_DEPS,
            ignore_all_deps=ignore_all_deps,
            ignore_ti_state=ignore_ti_state,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps)
        if not self.are_dependencies_met(
                dep_context=queue_dep_context,
                session=session,
                verbose=True):
            session.commit()
            return False

        # TODO: Logging needs cleanup, not clear what is being printed
        hr = "\n" + ("-" * 80) + "\n"  # Line break

        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Attempt 1 instead of
        # Attempt 0 for the first attempt).
        msg = "Starting attempt {attempt} of {total}".format(
            attempt=self.try_number,
            total=self.max_tries + 1)

        # Set the task start date. In case it was re-scheduled use the initial
        # start date that is recorded in task_reschedule table
        self.start_date = timezone.utcnow()
        task_reschedules = TaskReschedule.find_for_task_instance(self, session)
        if task_reschedules:
            self.start_date = task_reschedules[0].start_date

        dep_context = DepContext(
            deps=RUN_DEPS - QUEUE_DEPS,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state)
        runnable = self.are_dependencies_met(
            dep_context=dep_context,
            session=session,
            verbose=True)

        if not runnable and not mark_success:
            # FIXME: we might have hit concurrency limits, which means we probably
            # have been running prematurely. This should be handled in the
            # scheduling mechanism.
            self.state = State.NONE
            msg = ("FIXME: Rescheduling due to concurrency limits reached at task "
                   "runtime. Attempt {attempt} of {total}. State set to NONE.").format(
                attempt=self.try_number,
                total=self.max_tries + 1)
            self.log.warning(hr + msg + hr)

            self.queued_dttm = timezone.utcnow()
            self.log.info("Queuing into pool %s", self.pool)
            session.merge(self)
            session.commit()
            return False

        # Another worker might have started running this task instance while
        # the current worker process was blocked on refresh_from_db
        if self.state == State.RUNNING:
            msg = "Task Instance already running {}".format(self)
            self.log.warning(msg)
            session.commit()
            return False

        # print status message
        self.log.info(hr + msg + hr)
        self._try_number += 1

        if not test_mode:
            session.add(Log(State.RUNNING, self))
        self.state = State.RUNNING
        self.pid = os.getpid()
        self.end_date = None
        if not test_mode:
            session.merge(self)
        session.commit()

        # Closing all pooled connections to prevent
        # "max number of connections reached"
        settings.engine.dispose()
        if verbose:
            if mark_success:
                msg = "Marking success for {} on {}".format(self.task,
                                                            self.execution_date)
                self.log.info(msg)
            else:
                msg = "Executing {} on {}".format(self.task, self.execution_date)
                self.log.info(msg)
        return True

    @provide_session
    def _run_raw_task(
            self,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        """
        Immediately runs the task (without checking or changing db state
        before execution) and then sets the appropriate final state after
        completion and runs any post-execute callbacks. Meant to be called
        only after another function changes the state to running.

        :param mark_success: Don't run the task, mark its state as success
        :type mark_success: bool
        :param test_mode: Doesn't record success or failure in the DB
        :type test_mode: bool
        :param pool: specifies the pool to use to run the task instance
        :type pool: str
        """
        task = self.task
        self.pool = pool or task.pool
        self.test_mode = test_mode
        self.refresh_from_db(session=session)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.operator = task.__class__.__name__

        context = {}
        actual_start_date = timezone.utcnow()
        try:
            if not mark_success:
                context = self.get_template_context()

                task_copy = copy.copy(task)
                self.task = task_copy

                def signal_handler(signum, frame):
                    self.log.error("Received SIGTERM. Terminating subprocesses.")
                    task_copy.on_kill()
                    raise AirflowException("Task received SIGTERM signal")
                signal.signal(signal.SIGTERM, signal_handler)

                # Don't clear Xcom until the task is certain to execute
                self.clear_xcom_data()

                self.render_templates()
                task_copy.pre_execute(context=context)

                # If a timeout is specified for the task, make it fail
                # if it goes beyond
                result = None
                if task_copy.execution_timeout:
                    try:
                        with timeout(int(
                                task_copy.execution_timeout.total_seconds())):
                            result = task_copy.execute(context=context)
                    except AirflowTaskTimeout:
                        task_copy.on_kill()
                        raise
                else:
                    result = task_copy.execute(context=context)

                # If the task returns a result, push an XCom containing it
                if task_copy.do_xcom_push and result is not None:
                    self.xcom_push(key=XCOM_RETURN_KEY, value=result)

                task_copy.post_execute(context=context, result=result)

                Stats.incr('operator_successes_{}'.format(
                    self.task.__class__.__name__), 1, 1)
                Stats.incr('ti_successes')
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SUCCESS
        except AirflowSkipException:
            self.refresh_from_db(lock_for_update=True)
            self.state = State.SKIPPED
        except AirflowRescheduleException as reschedule_exception:
            self.refresh_from_db()
            self._handle_reschedule(actual_start_date, reschedule_exception, test_mode, context)
            return
        except AirflowException as e:
            self.refresh_from_db()
            # for case when task is marked as success/failed externally
            # current behavior doesn't hit the success callback
            if self.state in {State.SUCCESS, State.FAILED}:
                return
            else:
                self.handle_failure(e, test_mode, context)
                raise
        except (Exception, KeyboardInterrupt) as e:
            self.handle_failure(e, test_mode, context)
            raise

        # Success callback
        try:
            if task.on_success_callback:
                task.on_success_callback(context)
        except Exception as e3:
            self.log.error("Failed when executing success callback")
            self.log.exception(e3)

        # Recording SUCCESS
        self.end_date = timezone.utcnow()
        self.set_duration()
        if not test_mode:
            session.add(Log(self.state, self))
            session.merge(self)
        session.commit()

    @provide_session
    def run(
            self,
            verbose=True,
            ignore_all_deps=False,
            ignore_depends_on_past=False,
            ignore_task_deps=False,
            ignore_ti_state=False,
            mark_success=False,
            test_mode=False,
            job_id=None,
            pool=None,
            session=None):
        res = self._check_and_change_state_before_execution(
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            test_mode=test_mode,
            job_id=job_id,
            pool=pool,
            session=session)
        if res:
            self._run_raw_task(
                mark_success=mark_success,
                test_mode=test_mode,
                job_id=job_id,
                pool=pool,
                session=session)

    def dry_run(self):
        task = self.task
        task_copy = copy.copy(task)
        self.task = task_copy

        self.render_templates()
        task_copy.dry_run()

    @provide_session
    def _handle_reschedule(self, actual_start_date, reschedule_exception, test_mode=False, context=None,
                           session=None):
        # Don't record reschedule request in test mode
        if test_mode:
            return

        self.end_date = timezone.utcnow()
        self.set_duration()

        # Log reschedule request
        session.add(TaskReschedule(self.task, self.execution_date, self._try_number,
                    actual_start_date, self.end_date,
                    reschedule_exception.reschedule_date))

        # set state
        self.state = State.UP_FOR_RESCHEDULE

        # Decrement try_number so subsequent runs will use the same try number and write
        # to same log file.
        self._try_number -= 1

        session.merge(self)
        session.commit()
        self.log.info('Rescheduling task, marking task as UP_FOR_RESCHEDULE')

    @provide_session
    def handle_failure(self, error, test_mode=False, context=None, session=None):
        self.log.exception(error)
        task = self.task
        self.end_date = timezone.utcnow()
        self.set_duration()
        Stats.incr('operator_failures_{}'.format(task.__class__.__name__), 1, 1)
        Stats.incr('ti_failures')
        if not test_mode:
            session.add(Log(State.FAILED, self))

        # Log failure duration
        session.add(TaskFail(task, self.execution_date, self.start_date, self.end_date))

        if context is not None:
            context['exception'] = error

        # Let's go deeper
        try:
            # Since this function is called only when the TI state is running,
            # try_number contains the current try_number (not the next). We
            # only mark task instance as FAILED if the next task instance
            # try_number exceeds the max_tries.
            if self.is_eligible_to_retry():
                self.state = State.UP_FOR_RETRY
                self.log.info('Marking task as UP_FOR_RETRY')
                if task.email_on_retry and task.email:
                    self.email_alert(error)
            else:
                self.state = State.FAILED
                if task.retries:
                    self.log.info('All retries failed; marking task as FAILED')
                else:
                    self.log.info('Marking task as FAILED.')
                if task.email_on_failure and task.email:
                    self.email_alert(error)
        except Exception as e2:
            self.log.error('Failed to send email to: %s', task.email)
            self.log.exception(e2)

        # Handling callbacks pessimistically
        try:
            if self.state == State.UP_FOR_RETRY and task.on_retry_callback:
                task.on_retry_callback(context)
            if self.state == State.FAILED and task.on_failure_callback:
                task.on_failure_callback(context)
        except Exception as e3:
            self.log.error("Failed at executing callback")
            self.log.exception(e3)

        if not test_mode:
            session.merge(self)
        session.commit()

    def is_eligible_to_retry(self):
        """Is task instance is eligible for retry"""
        return self.task.retries and self.try_number <= self.max_tries

    @provide_session
    def get_template_context(self, session=None):
        task = self.task
        from airflow import macros
        tables = None
        if 'tables' in task.params:
            tables = task.params['tables']

        params = {}
        run_id = ''
        dag_run = None
        if hasattr(task, 'dag'):
            if task.dag.params:
                params.update(task.dag.params)
            dag_run = (
                session.query(DagRun)
                .filter_by(
                    dag_id=task.dag.dag_id,
                    execution_date=self.execution_date)
                .first()
            )
            run_id = dag_run.run_id if dag_run else None
            session.expunge_all()
            session.commit()

        ds = self.execution_date.strftime('%Y-%m-%d')
        ts = self.execution_date.isoformat()
        yesterday_ds = (self.execution_date - timedelta(1)).strftime('%Y-%m-%d')
        tomorrow_ds = (self.execution_date + timedelta(1)).strftime('%Y-%m-%d')

        # For manually triggered dagruns that aren't run on a schedule, next/previous
        # schedule dates don't make sense, and should be set to execution date for
        # consistency with how execution_date is set for manually triggered tasks, i.e.
        # triggered_date == execution_date.
        if dag_run and dag_run.external_trigger:
            prev_execution_date = self.execution_date
            next_execution_date = self.execution_date
        else:
            prev_execution_date = task.dag.previous_schedule(self.execution_date)
            next_execution_date = task.dag.following_schedule(self.execution_date)

        next_ds = None
        next_ds_nodash = None
        if next_execution_date:
            next_ds = next_execution_date.strftime('%Y-%m-%d')
            next_ds_nodash = next_ds.replace('-', '')

        prev_ds = None
        prev_ds_nodash = None
        if prev_execution_date:
            prev_ds = prev_execution_date.strftime('%Y-%m-%d')
            prev_ds_nodash = prev_ds.replace('-', '')

        ds_nodash = ds.replace('-', '')
        ts_nodash = self.execution_date.strftime('%Y%m%dT%H%M%S')
        ts_nodash_with_tz = ts.replace('-', '').replace(':', '')
        yesterday_ds_nodash = yesterday_ds.replace('-', '')
        tomorrow_ds_nodash = tomorrow_ds.replace('-', '')

        ti_key_str = "{task.dag_id}__{task.task_id}__{ds_nodash}"
        ti_key_str = ti_key_str.format(**locals())

        if task.params:
            params.update(task.params)

        if configuration.getboolean('core', 'dag_run_conf_overrides_params'):
            self.overwrite_params_with_dag_run_conf(params=params, dag_run=dag_run)

        class VariableAccessor:
            """
            Wrapper around Variable. This way you can get variables in templates by using
            {var.value.your_variable_name}.
            """
            def __init__(self):
                self.var = None

            def __getattr__(self, item):
                self.var = Variable.get(item)
                return self.var

            def __repr__(self):
                return str(self.var)

        class VariableJsonAccessor:
            """
            Wrapper around deserialized Variables. This way you can get variables
            in templates by using {var.json.your_variable_name}.
            """
            def __init__(self):
                self.var = None

            def __getattr__(self, item):
                self.var = Variable.get(item, deserialize_json=True)
                return self.var

            def __repr__(self):
                return str(self.var)

        return {
            'dag': task.dag,
            'ds': ds,
            'next_ds': next_ds,
            'next_ds_nodash': next_ds_nodash,
            'prev_ds': prev_ds,
            'prev_ds_nodash': prev_ds_nodash,
            'ds_nodash': ds_nodash,
            'ts': ts,
            'ts_nodash': ts_nodash,
            'ts_nodash_with_tz': ts_nodash_with_tz,
            'yesterday_ds': yesterday_ds,
            'yesterday_ds_nodash': yesterday_ds_nodash,
            'tomorrow_ds': tomorrow_ds,
            'tomorrow_ds_nodash': tomorrow_ds_nodash,
            'END_DATE': ds,
            'end_date': ds,
            'dag_run': dag_run,
            'run_id': run_id,
            'execution_date': self.execution_date,
            'prev_execution_date': prev_execution_date,
            'next_execution_date': next_execution_date,
            'latest_date': ds,
            'macros': macros,
            'params': params,
            'tables': tables,
            'task': task,
            'task_instance': self,
            'ti': self,
            'task_instance_key_str': ti_key_str,
            'conf': configuration,
            'test_mode': self.test_mode,
            'var': {
                'value': VariableAccessor(),
                'json': VariableJsonAccessor()
            },
            'inlets': task.inlets,
            'outlets': task.outlets,
        }

    def overwrite_params_with_dag_run_conf(self, params, dag_run):
        if dag_run and dag_run.conf:
            params.update(dag_run.conf)

    def render_templates(self):
        task = self.task
        jinja_context = self.get_template_context()
        if hasattr(self, 'task') and hasattr(self.task, 'dag'):
            if self.task.dag.user_defined_macros:
                jinja_context.update(
                    self.task.dag.user_defined_macros)

        rt = self.task.render_template  # shortcut to method
        for attr in task.__class__.template_fields:
            content = getattr(task, attr)
            if content:
                rendered_content = rt(attr, content, jinja_context)
                setattr(task, attr, rendered_content)

    def email_alert(self, exception):
        exception_html = str(exception).replace('\n', '<br>')
        jinja_context = self.get_template_context()
        # This function is called after changing the state
        # from State.RUNNING so need to subtract 1 from self.try_number.
        jinja_context.update(dict(
            exception=exception,
            exception_html=exception_html,
            try_number=self.try_number - 1,
            max_tries=self.max_tries))

        jinja_env = self.task.get_template_env()

        default_subject = 'Airflow alert: {{ti}}'
        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Try 1 instead of
        # Try 0 for the first attempt).
        default_html_content = (
            'Try {{try_number}} out of {{max_tries + 1}}<br>'
            'Exception:<br>{{exception_html}}<br>'
            'Log: <a href="{{ti.log_url}}">Link</a><br>'
            'Host: {{ti.hostname}}<br>'
            'Log file: {{ti.log_filepath}}<br>'
            'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
        )

        def render(key, content):
            if configuration.has_option('email', key):
                path = configuration.get('email', key)
                with open(path) as f:
                    content = f.read()

            return jinja_env.from_string(content).render(**jinja_context)

        subject = render('subject_template', default_subject)
        html_content = render('html_content_template', default_html_content)
        send_email(self.task.email, subject, html_content)

    def set_duration(self):
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None

    def xcom_push(
            self,
            key,
            value,
            execution_date=None):
        """
        Make an XCom available for tasks to pull.

        :param key: A key for the XCom
        :type key: str
        :param value: A value for the XCom. The value is pickled and stored
            in the database.
        :type value: any pickleable object
        :param execution_date: if provided, the XCom will not be visible until
            this date. This can be used, for example, to send a message to a
            task on a future date without it being immediately visible.
        :type execution_date: datetime
        """

        if execution_date and execution_date < self.execution_date:
            raise ValueError(
                'execution_date can not be in the past (current '
                'execution_date is {}; received {})'.format(
                    self.execution_date, execution_date))

        XCom.set(
            key=key,
            value=value,
            task_id=self.task_id,
            dag_id=self.dag_id,
            execution_date=execution_date or self.execution_date)

    def xcom_pull(
            self,
            task_ids=None,
            dag_id=None,
            key=XCOM_RETURN_KEY,
            include_prior_dates=False):
        """
        Pull XComs that optionally meet certain criteria.

        The default value for `key` limits the search to XComs
        that were returned by other tasks (as opposed to those that were pushed
        manually). To remove this filter, pass key=None (or any desired value).

        If a single task_id string is provided, the result is the value of the
        most recent matching XCom from that task_id. If multiple task_ids are
        provided, a tuple of matching values is returned. None is returned
        whenever no matches are found.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is 'return_value', also
            available as a constant XCOM_RETURN_KEY. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass key=None.
        :type key: str
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Can pass None to remove the filter.
        :type task_ids: str or iterable of strings (representing task_ids)
        :param dag_id: If provided, only pulls XComs from this DAG.
            If None (default), the DAG of the calling task is used.
        :type dag_id: str
        :param include_prior_dates: If False, only XComs from the current
            execution_date are returned. If True, XComs from previous dates
            are returned as well.
        :type include_prior_dates: bool
        """

        if dag_id is None:
            dag_id = self.dag_id

        pull_fn = functools.partial(
            XCom.get_one,
            execution_date=self.execution_date,
            key=key,
            dag_id=dag_id,
            include_prior_dates=include_prior_dates)

        if is_container(task_ids):
            return tuple(pull_fn(task_id=t) for t in task_ids)
        else:
            return pull_fn(task_id=task_ids)

    @provide_session
    def get_num_running_task_instances(self, session):
        TI = TaskInstance
        return session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.state == State.RUNNING
        ).count()

    def init_run_context(self, raw=False):
        """
        Sets the log context.
        """
        self.raw = raw
        self._set_context(self)


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
        downtream tasks have higher weight and will be scheduled more
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
        one_failed | none_failed | dummy}``
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
    template_fields = []
    # Defines which files extensions to look for in the templated fields
    template_ext = []
    # Defines the color in the UI
    ui_color = '#fff'
    ui_fgcolor = '#000'

    # base list which includes all the attrs that don't need deep copy.
    _base_operator_shallow_copy_attrs = ('user_defined_macros',
                                         'user_defined_filters',
                                         'params',
                                         '_log',)

    # each operator should override this class attr for shallow copy attrs.
    shallow_copy_attrs = ()

    @apply_defaults
    def __init__(
            self,
            task_id,
            owner=configuration.conf.get('operators', 'DEFAULT_OWNER'),
            email=None,
            email_on_retry=True,
            email_on_failure=True,
            retries=0,
            retry_delay=timedelta(seconds=300),
            retry_exponential_backoff=False,
            max_retry_delay=None,
            start_date=None,
            end_date=None,
            schedule_interval=None,  # not hooked as of now
            depends_on_past=False,
            wait_for_downstream=False,
            dag=None,
            params=None,
            default_args=None,
            adhoc=False,
            priority_weight=1,
            weight_rule=WeightRule.DOWNSTREAM,
            queue=configuration.conf.get('celery', 'default_queue'),
            pool=None,
            sla=None,
            execution_timeout=None,
            on_failure_callback=None,
            on_success_callback=None,
            on_retry_callback=None,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            resources=None,
            run_as_user=None,
            task_concurrency=None,
            executor_config=None,
            do_xcom_push=True,
            inlets=None,
            outlets=None,
            *args,
            **kwargs):

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

        if schedule_interval:
            self.log.warning(
                "schedule_interval is used for %s, though it has "
                "been deprecated as a task parameter, you need to "
                "specify it as a DAG parameter instead",
                self
            )
        self._schedule_interval = schedule_interval
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
        self.adhoc = adhoc
        self.priority_weight = priority_weight
        if not WeightRule.is_valid(weight_rule):
            raise AirflowException(
                "The weight_rule must be one of {all_weight_rules},"
                "'{d}.{t}'; received '{tr}'."
                .format(all_weight_rules=WeightRule.all_weight_rules,
                        d=dag.dag_id if dag else "", t=task_id, tr=weight_rule))
        self.weight_rule = weight_rule

        self.resources = Resources(**(resources or {}))
        self.run_as_user = run_as_user
        self.task_concurrency = task_concurrency
        self.executor_config = executor_config or {}
        self.do_xcom_push = do_xcom_push

        # Private attributes
        self._upstream_task_ids = set()
        self._downstream_task_ids = set()

        if not dag and _CONTEXT_MANAGER_DAG:
            dag = _CONTEXT_MANAGER_DAG
        if dag:
            self.dag = dag

        self._log = logging.getLogger("airflow.task.operators")

        # lineage
        self.inlets = []
        self.outlets = []
        self.lineage_data = None

        self._inlets = {
            "auto": False,
            "task_ids": [],
            "datasets": [],
        }

        self._outlets = {
            "datasets": [],
        }

        if inlets:
            self._inlets.update(inlets)

        if outlets:
            self._outlets.update(outlets)

        self._comps = {
            'task_id',
            'dag_id',
            'owner',
            'email',
            'email_on_retry',
            'retry_delay',
            'retry_exponential_backoff',
            'max_retry_delay',
            'start_date',
            'schedule_interval',
            'depends_on_past',
            'wait_for_downstream',
            'adhoc',
            'priority_weight',
            'sla',
            'execution_timeout',
            'on_failure_callback',
            'on_success_callback',
            'on_retry_callback',
            'do_xcom_push',
        }

    def __eq__(self, other):
        if (type(self) == type(other) and
                self.task_id == other.task_id):
            return all(self.__dict__.get(c, None) == other.__dict__.get(c, None) for c in self._comps)
        return False

    def __ne__(self, other):
        return not self == other

    def __lt__(self, other):
        return self.task_id < other.task_id

    def __hash__(self):
        hash_components = [type(self)]
        for c in self._comps:
            val = getattr(self, c, None)
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

        self._dag = dag

    def has_dag(self):
        """
        Returns True if the Operator has been assigned to a DAG.
        """
        return getattr(self, '_dag', None) is not None

    @property
    def dag_id(self):
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
    def schedule_interval(self):
        """
        The schedule interval of the DAG always wins over individual tasks so
        that tasks within a DAG always line up. The task still needs a
        schedule_interval as it may not be attached to a DAG.
        """
        if self.has_dag():
            return self.dag._schedule_interval
        else:
            return self._schedule_interval

    @property
    def priority_weight_total(self):
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

    @prepare_lineage
    def pre_execute(self, context):
        """
        This hook is triggered right before self.execute() is called.
        """
        pass

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
        pass

    def on_kill(self):
        """
        Override this method to cleanup subprocesses when a task instance
        gets killed. Any use of the threading, subprocess or multiprocessing
        module within an operator needs to be cleaned up or it will leave
        ghost processes behind.
        """
        pass

    def __deepcopy__(self, memo):
        """
        Hack sorting double chained task lists by task_id to avoid hitting
        max_depth on deepcopy operations.
        """
        sys.setrecursionlimit(5000)  # TODO fix this in a better way
        cls = self.__class__
        result = cls.__new__(cls)
        memo[id(self)] = result

        shallow_copy = cls.shallow_copy_attrs + cls._base_operator_shallow_copy_attrs

        for k, v in list(self.__dict__.items()):
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
        self.__dict__ = state
        self._log = logging.getLogger("airflow.task.operators")

    def render_template_from_field(self, attr, content, context, jinja_env):
        """
        Renders a template from a field. If the field is a string, it will
        simply render the string and return the result. If it is a collection or
        nested set of collections, it will traverse the structure and render
        all elements in it. If the field has another type, it will return it as it is.
        """
        rt = self.render_template
        if isinstance(content, six.string_types):
            result = jinja_env.from_string(content).render(**context)
        elif isinstance(content, (list, tuple)):
            result = [rt(attr, e, context) for e in content]
        elif isinstance(content, dict):
            result = {
                k: rt("{}[{}]".format(attr, k), v, context)
                for k, v in list(content.items())}
        else:
            result = content
        return result

    def render_template(self, attr, content, context):
        """
        Renders a template either from a file or directly in a field, and returns
        the rendered result.
        """
        jinja_env = self.get_template_env()

        exts = self.__class__.template_ext
        if (
                isinstance(content, six.string_types) and
                any([content.endswith(ext) for ext in exts])):
            return jinja_env.get_template(content).render(**context)
        else:
            return self.render_template_from_field(attr, content, context, jinja_env)

    def get_template_env(self):
        return self.dag.get_template_env() \
            if hasattr(self, 'dag') \
            else jinja2.Environment(cache_size=0)

    def prepare_template(self):
        """
        Hook that is triggered after the templated fields get replaced
        by their content. If you need your operator to alter the
        content of the file before the template is rendered,
        it should override this method to do so.
        """
        pass

    def resolve_template_files(self):
        # Getting the content of files for template_field / template_ext
        for attr in self.template_fields:
            content = getattr(self, attr)
            if content is None:
                continue
            elif isinstance(content, six.string_types) and \
                    any([content.endswith(ext) for ext in self.template_ext]):
                env = self.get_template_env()
                try:
                    setattr(self, attr, env.loader.get_source(env, content)[0])
                except Exception as e:
                    self.log.exception(e)
            elif isinstance(content, list):
                env = self.dag.get_template_env()
                for i in range(len(content)):
                    if isinstance(content[i], six.string_types) and \
                            any([content[i].endswith(ext) for ext in self.template_ext]):
                        try:
                            content[i] = env.loader.get_source(env, content[i])[0]
                        except Exception as e:
                            self.log.exception(e)
        self.prepare_template()

    @property
    def upstream_list(self):
        """@property: list of tasks directly upstream"""
        return [self.dag.get_task(tid) for tid in self._upstream_task_ids]

    @property
    def upstream_task_ids(self):
        return self._upstream_task_ids

    @property
    def downstream_list(self):
        """@property: list of tasks directly downstream"""
        return [self.dag.get_task(tid) for tid in self._downstream_task_ids]

    @property
    def downstream_task_ids(self):
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

    def get_task_instances(self, session, start_date=None, end_date=None):
        """
        Get a set of task instance related to this task for a specific date
        range.
        """
        TI = TaskInstance
        end_date = end_date or timezone.utcnow()
        return session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.task_id == self.task_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
        ).order_by(TI.execution_date).all()

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

        for dt in self.dag.date_range(start_date, end_date=end_date):
            TaskInstance(self, dt).run(
                mark_success=mark_success,
                ignore_depends_on_past=(
                    dt == start_date and ignore_first_depends_on_past),
                ignore_ti_state=ignore_ti_state)

    def dry_run(self):
        self.log.info('Dry run')
        for attr in self.template_fields:
            content = getattr(self, attr)
            if content and isinstance(content, six.string_types):
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
        return self.__class__.__name__

    def add_only_new(self, item_set, item):
        if item in item_set:
            raise AirflowException(
                'Dependency {self}, {item} already registered'
                ''.format(**locals()))
        else:
            item_set.add(item)

    def _set_relatives(self, task_or_task_list, upstream=False):
        try:
            task_list = list(task_or_task_list)
        except TypeError:
            task_list = [task_or_task_list]

        for t in task_list:
            if not isinstance(t, BaseOperator):
                raise AirflowException(
                    "Relationships can only be set between "
                    "Operators; received {}".format(t.__class__.__name__))

        # relationships can only be set if the tasks share a single DAG. Tasks
        # without a DAG are assigned to that DAG.
        dags = {t._dag.dag_id: t._dag for t in [self] + task_list if t.has_dag()}

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
                task.add_only_new(task._downstream_task_ids, self.task_id)
                self.add_only_new(self._upstream_task_ids, task.task_id)
            else:
                self.add_only_new(self._downstream_task_ids, task.task_id)
                task.add_only_new(task._upstream_task_ids, self.task_id)

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


class DagModel(Base):

    __tablename__ = "dag"
    """
    These items are stored in the database for state related information
    """
    dag_id = Column(String(ID_LEN), primary_key=True)
    # A DAG can be paused from the UI / DB
    # Set this default value of is_paused based on a configuration value!
    is_paused_at_creation = configuration.conf\
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
    fileloc = Column(String(2000))
    # String representing the owners
    owners = Column(String(2000))
    # Description of the dag
    description = Column(Text)
    # Default view of the inside the webserver
    default_view = Column(String(25))
    # Schedule interval
    schedule_interval = Column(Interval)

    def __repr__(self):
        return "<DAG: {self.dag_id}>".format(self=self)

    @property
    def timezone(self):
        return settings.TIMEZONE

    @classmethod
    @provide_session
    def get_current(cls, dag_id, session=None):
        return session.query(cls).filter(cls.dag_id == dag_id).first()

    def get_default_view(self):
        if self.default_view is None:
            return configuration.conf.get('webserver', 'dag_default_view').lower()
        else:
            return self.default_view

    @provide_session
    def get_last_dagrun(self, session=None, include_externally_triggered=False):
        return get_last_dagrun(self.dag_id, session=session,
                               include_externally_triggered=include_externally_triggered)

    @property
    def safe_dag_id(self):
        return self.dag_id.replace('.', '__dot__')

    def get_dag(self):
        return DagBag(dag_folder=self.fileloc).get_dag(self.dag_id)

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
    """

    def __init__(
            self, dag_id,
            description='',
            schedule_interval=timedelta(days=1),
            start_date=None, end_date=None,
            full_filepath=None,
            template_searchpath=None,
            user_defined_macros=None,
            user_defined_filters=None,
            default_args=None,
            concurrency=configuration.conf.getint('core', 'dag_concurrency'),
            max_active_runs=configuration.conf.getint(
                'core', 'max_active_runs_per_dag'),
            dagrun_timeout=None,
            sla_miss_callback=None,
            default_view=None,
            orientation=configuration.conf.get('webserver', 'dag_orientation'),
            catchup=configuration.conf.getboolean('scheduler', 'catchup_by_default'),
            on_success_callback=None, on_failure_callback=None,
            params=None,
            access_control=None):

        self.user_defined_macros = user_defined_macros
        self.user_defined_filters = user_defined_filters
        self.default_args = default_args or {}
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
        self.task_dict = dict()

        # set timezone
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
        if isinstance(schedule_interval, Hashable) and schedule_interval in cron_presets:
            self._schedule_interval = cron_presets.get(schedule_interval)
        elif schedule_interval == '@once':
            self._schedule_interval = None
        else:
            self._schedule_interval = schedule_interval
        if isinstance(template_searchpath, six.string_types):
            template_searchpath = [template_searchpath]
        self.template_searchpath = template_searchpath
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

        self._old_context_manager_dags = []
        self._access_control = access_control

        self._comps = {
            'dag_id',
            'task_ids',
            'parent_dag',
            'start_date',
            'schedule_interval',
            'full_filepath',
            'template_searchpath',
            'last_loaded',
        }

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
        global _CONTEXT_MANAGER_DAG
        self._old_context_manager_dags.append(_CONTEXT_MANAGER_DAG)
        _CONTEXT_MANAGER_DAG = self
        return self

    def __exit__(self, _type, _value, _tb):
        global _CONTEXT_MANAGER_DAG
        _CONTEXT_MANAGER_DAG = self._old_context_manager_dags.pop()

    # /Context Manager ----------------------------------------------

    def get_default_view(self):
        """This is only there for backward compatible jinja2 templates"""
        if self._default_view is None:
            return configuration.conf.get('webserver', 'dag_default_view').lower()
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
    def active_task_ids(self):
        return list(k for k, v in self.task_dict.items() if not v.adhoc)

    @property
    def active_tasks(self):
        return [t for t in self.tasks if not t.adhoc]

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
        """
        Folder location of where the dag object is instantiated
        """
        return os.path.dirname(self.full_filepath)

    @property
    def owner(self):
        return ", ".join(list(set([t.owner for t in self.tasks])))

    @property
    @provide_session
    def concurrency_reached(self, session=None):
        """
        Returns a boolean indicating whether the concurrency limit for this DAG
        has been reached
        """
        TI = TaskInstance
        qry = session.query(func.count(TI.task_id)).filter(
            TI.dag_id == self.dag_id,
            TI.state == State.RUNNING,
        )
        return qry.scalar() >= self.concurrency

    @property
    @provide_session
    def is_paused(self, session=None):
        """
        Returns a boolean indicating whether this DAG is paused
        """
        qry = session.query(DagModel).filter(
            DagModel.dag_id == self.dag_id)
        return qry.value('is_paused')

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
            tis = dagrun.get_task_instances(session=session)
            ti = tis[-1]  # get first TaskInstance of DagRun
            ti.task = self.get_task(ti.task_id)
            context = ti.get_template_context(session=session)
            context.update({'reason': reason})
            callback(context)

    @provide_session
    def get_active_runs(self, session=None):
        """
        Returns a list of dag run execution dates currently running

        :param session:
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
        query = (session
                 .query(DagRun)
                 .filter(DagRun.dag_id == self.dag_id)
                 .filter(DagRun.state == State.RUNNING))

        if external_trigger is not None:
            query = query.filter(DagRun.external_trigger == external_trigger)

        return query.count()

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

    @property
    @provide_session
    def latest_execution_date(self, session=None):
        """
        Returns the latest date for which at least one dag run exists
        """
        execution_date = session.query(func.max(DagRun.execution_date)).filter(
            DagRun.dag_id == self.dag_id
        ).scalar()
        return execution_date

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
                    type(task).__name__ == 'SubDagOperator'):
                subdag_lst.append(task.subdag)
                subdag_lst += task.subdag.subdags
        return subdag_lst

    def resolve_template_files(self):
        for t in self.tasks:
            t.resolve_template_files()

    def get_template_env(self):
        """
        Returns a jinja2 Environment while taking into account the DAGs
        template_searchpath, user_defined_macros and user_defined_filters
        """
        searchpath = [self.folder]
        if self.template_searchpath:
            searchpath += self.template_searchpath

        env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(searchpath),
            extensions=["jinja2.ext.do"],
            cache_size=0)
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

    def get_task_instances(
            self, session, start_date=None, end_date=None, state=None):
        TI = TaskInstance
        if not start_date:
            start_date = (timezone.utcnow() - timedelta(30)).date()
            start_date = timezone.make_aware(
                datetime.combine(start_date, datetime.min.time()))
        end_date = end_date or timezone.utcnow()
        tis = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date >= start_date,
            TI.execution_date <= end_date,
            TI.task_id.in_([t.task_id for t in self.tasks]),
        )
        if state:
            tis = tis.filter(TI.state == state)
        tis = tis.order_by(TI.execution_date).all()
        return tis

    @property
    def roots(self):
        return [t for t in self.tasks if not t.downstream_list]

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
    ):
        """
        Clears a set of task instances associated with the current dag for
        a specified date range.
        """
        TI = TaskInstance
        tis = session.query(TI)
        if include_subdags:
            # Crafting the right filter for dag_id and task_ids combo
            conditions = []
            for dag in self.subdags + [self]:
                conditions.append(
                    TI.dag_id.like(dag.dag_id) &
                    TI.task_id.in_(dag.task_ids)
                )
            tis = tis.filter(or_(*conditions))
        else:
            tis = session.query(TI).filter(TI.dag_id == self.dag_id)
            tis = tis.filter(TI.task_id.in_(self.task_ids))

        if include_parentdag and self.is_subdag:

            p_dag = self.parent_dag.sub_dag(
                task_regex=self.dag_id.split('.')[1],
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

        if get_tis:
            return tis

        if dry_run:
            tis = tis.all()
            session.expunge_all()
            return tis

        count = tis.count()
        do_it = True
        if count == 0:
            return 0
        if confirm_prompt:
            ti_list = "\n".join([str(t) for t in tis])
            question = (
                "You are about to delete these {count} tasks:\n"
                "{ti_list}\n\n"
                "Are you sure? (yes/no): ").format(**locals())
            do_it = utils.helpers.ask_yesno(question)

        if do_it:
            clear_task_instances(tis.all(),
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
        dag.task_dict = {t.task_id: copy.deepcopy(t, {id(t.dag): t.dag})
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
        raise AirflowException("Task {task_id} not found".format(**locals()))

    @provide_session
    def pickle_info(self, session=None):
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
        """
        Shows an ascii tree representation of the DAG
        """
        def get_downstream(task, level=0):
            print((" " * level * 4) + str(task))
            level += 1
            for t in task.upstream_list:
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
            donot_pickle=configuration.conf.getboolean('core', 'donot_pickle'),
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

        if owner is None:
            owner = self.owner
        if sync_time is None:
            sync_time = timezone.utcnow()

        orm_dag = session.query(
            DagModel).filter(DagModel.dag_id == self.dag_id).first()
        if not orm_dag:
            orm_dag = DagModel(dag_id=self.dag_id)
            self.log.info("Creating ORM DAG for %s", self.dag_id)
        orm_dag.fileloc = self.fileloc
        orm_dag.is_subdag = self.is_subdag
        orm_dag.owners = owner
        orm_dag.is_active = True
        orm_dag.last_scheduler_run = sync_time
        orm_dag.default_view = self._default_view
        orm_dag.description = self.description
        orm_dag.schedule_interval = self.schedule_interval
        session.merge(orm_dag)
        session.commit()

        for subdag in self.subdags:
            subdag.sync_to_db(owner=owner, sync_time=sync_time, session=session)

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
    def get_num_task_instances(dag_id, task_ids, states=None, session=None):
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
            TaskInstance.task_id.in_(task_ids))
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


class Variable(Base, LoggingMixin):
    __tablename__ = "variable"

    id = Column(Integer, primary_key=True)
    key = Column(String(ID_LEN), unique=True)
    _val = Column('val', Text)
    is_encrypted = Column(Boolean, unique=False, default=False)

    def __repr__(self):
        # Hiding the value
        return '{} : {}'.format(self.key, self._val)

    def get_val(self):
        log = LoggingMixin().log
        if self._val and self.is_encrypted:
            try:
                fernet = get_fernet()
                return fernet.decrypt(bytes(self._val, 'utf-8')).decode()
            except InvalidFernetToken:
                log.error("Can't decrypt _val for key={}, invalid token "
                          "or value".format(self.key))
                return None
            except Exception:
                log.error("Can't decrypt _val for key={}, FERNET_KEY "
                          "configuration missing".format(self.key))
                return None
        else:
            return self._val

    def set_val(self, value):
        if value:
            fernet = get_fernet()
            self._val = fernet.encrypt(bytes(value, 'utf-8')).decode()
            self.is_encrypted = fernet.is_encrypted

    @declared_attr
    def val(cls):
        return synonym('_val',
                       descriptor=property(cls.get_val, cls.set_val))

    @classmethod
    def setdefault(cls, key, default, deserialize_json=False):
        """
        Like a Python builtin dict object, setdefault returns the current value
        for a key, and if it isn't there, stores the default value and returns it.

        :param key: Dict key for this Variable
        :type key: str
        :param default: Default value to set and return if the variable
            isn't already in the DB
        :type default: Mixed
        :param deserialize_json: Store this as a JSON encoded value in the DB
            and un-encode it when retrieving a value
        :return: Mixed
        """
        default_sentinel = object()
        obj = Variable.get(key, default_var=default_sentinel,
                           deserialize_json=deserialize_json)
        if obj is default_sentinel:
            if default is not None:
                Variable.set(key, default, serialize_json=deserialize_json)
                return default
            else:
                raise ValueError('Default Value must be set')
        else:
            return obj

    @classmethod
    @provide_session
    def get(cls, key, default_var=None, deserialize_json=False, session=None):
        obj = session.query(cls).filter(cls.key == key).first()
        if obj is None:
            if default_var is not None:
                return default_var
            else:
                raise KeyError('Variable {} does not exist'.format(key))
        else:
            if deserialize_json:
                return json.loads(obj.val)
            else:
                return obj.val

    @classmethod
    @provide_session
    def set(cls, key, value, serialize_json=False, session=None):

        if serialize_json:
            stored_value = json.dumps(value)
        else:
            stored_value = str(value)

        session.query(cls).filter(cls.key == key).delete()
        session.add(Variable(key=key, val=stored_value))
        session.flush()

    def rotate_fernet_key(self):
        fernet = get_fernet()
        if self._val and self.is_encrypted:
            self._val = fernet.rotate(self._val.encode('utf-8')).decode()


class DagRun(Base, LoggingMixin):
    """
    DagRun describes an instance of a Dag. It can be created
    by the scheduler (for regular runs) or by an external trigger
    """
    __tablename__ = "dag_run"

    ID_PREFIX = 'scheduled__'
    ID_FORMAT_PREFIX = ID_PREFIX + '{0}'

    id = Column(Integer, primary_key=True)
    dag_id = Column(String(ID_LEN))
    execution_date = Column(UtcDateTime, default=timezone.utcnow)
    start_date = Column(UtcDateTime, default=timezone.utcnow)
    end_date = Column(UtcDateTime)
    _state = Column('state', String(50), default=State.RUNNING)
    run_id = Column(String(ID_LEN))
    external_trigger = Column(Boolean, default=True)
    conf = Column(PickleType)

    dag = None

    __table_args__ = (
        Index('dag_id_state', dag_id, _state),
        UniqueConstraint('dag_id', 'execution_date'),
        UniqueConstraint('dag_id', 'run_id'),
    )

    def __repr__(self):
        return (
            '<DagRun {dag_id} @ {execution_date}: {run_id}, '
            'externally triggered: {external_trigger}>'
        ).format(
            dag_id=self.dag_id,
            execution_date=self.execution_date,
            run_id=self.run_id,
            external_trigger=self.external_trigger)

    def get_state(self):
        return self._state

    def set_state(self, state):
        if self._state != state:
            self._state = state
            self.end_date = timezone.utcnow() if self._state in State.finished() else None

    @declared_attr
    def state(self):
        return synonym('_state',
                       descriptor=property(self.get_state, self.set_state))

    @classmethod
    def id_for_date(cls, date, prefix=ID_FORMAT_PREFIX):
        return prefix.format(date.isoformat()[:19])

    @provide_session
    def refresh_from_db(self, session=None):
        """
        Reloads the current dagrun from the database
        :param session: database session
        """
        DR = DagRun

        exec_date = func.cast(self.execution_date, DateTime)

        dr = session.query(DR).filter(
            DR.dag_id == self.dag_id,
            func.cast(DR.execution_date, DateTime) == exec_date,
            DR.run_id == self.run_id
        ).one()

        self.id = dr.id
        self.state = dr.state

    @staticmethod
    @provide_session
    def find(dag_id=None, run_id=None, execution_date=None,
             state=None, external_trigger=None, no_backfills=False,
             session=None):
        """
        Returns a set of dag runs for the given search criteria.

        :param dag_id: the dag_id to find dag runs for
        :type dag_id: int, list
        :param run_id: defines the the run id for this dag run
        :type run_id: str
        :param execution_date: the execution date
        :type execution_date: datetime.datetime
        :param state: the state of the dag run
        :type state: airflow.utils.state.State
        :param external_trigger: whether this dag run is externally triggered
        :type external_trigger: bool
        :param no_backfills: return no backfills (True), return all (False).
            Defaults to False
        :type no_backfills: bool
        :param session: database session
        :type session: sqlalchemy.orm.session.Session
        """
        DR = DagRun

        qry = session.query(DR)
        if dag_id:
            qry = qry.filter(DR.dag_id == dag_id)
        if run_id:
            qry = qry.filter(DR.run_id == run_id)
        if execution_date:
            if isinstance(execution_date, list):
                qry = qry.filter(DR.execution_date.in_(execution_date))
            else:
                qry = qry.filter(DR.execution_date == execution_date)
        if state:
            qry = qry.filter(DR.state == state)
        if external_trigger is not None:
            qry = qry.filter(DR.external_trigger == external_trigger)
        if no_backfills:
            # in order to prevent a circular dependency
            from airflow.jobs import BackfillJob
            qry = qry.filter(DR.run_id.notlike(BackfillJob.ID_PREFIX + '%'))

        dr = qry.order_by(DR.execution_date).all()

        return dr

    @provide_session
    def get_task_instances(self, state=None, session=None):
        """
        Returns the task instances for this dag run
        """
        TI = TaskInstance
        tis = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
        )
        if state:
            if isinstance(state, six.string_types):
                tis = tis.filter(TI.state == state)
            else:
                # this is required to deal with NULL values
                if None in state:
                    tis = tis.filter(
                        or_(TI.state.in_(state),
                            TI.state.is_(None))
                    )
                else:
                    tis = tis.filter(TI.state.in_(state))

        if self.dag and self.dag.partial:
            tis = tis.filter(TI.task_id.in_(self.dag.task_ids))

        return tis.all()

    @provide_session
    def get_task_instance(self, task_id, session=None):
        """
        Returns the task instance specified by task_id for this dag run

        :param task_id: the task id
        """

        TI = TaskInstance
        ti = session.query(TI).filter(
            TI.dag_id == self.dag_id,
            TI.execution_date == self.execution_date,
            TI.task_id == task_id
        ).first()

        return ti

    def get_dag(self):
        """
        Returns the Dag associated with this DagRun.

        :return: DAG
        """
        if not self.dag:
            raise AirflowException("The DAG (.dag) for {} needs to be set"
                                   .format(self))

        return self.dag

    @provide_session
    def get_previous_dagrun(self, session=None):
        """The previous DagRun, if there is one"""

        return session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date < self.execution_date
        ).order_by(
            DagRun.execution_date.desc()
        ).first()

    @provide_session
    def get_previous_scheduled_dagrun(self, session=None):
        """The previous, SCHEDULED DagRun, if there is one"""
        dag = self.get_dag()

        return session.query(DagRun).filter(
            DagRun.dag_id == self.dag_id,
            DagRun.execution_date == dag.previous_schedule(self.execution_date)
        ).first()

    @provide_session
    def update_state(self, session=None):
        """
        Determines the overall state of the DagRun based on the state
        of its TaskInstances.

        :return: State
        """

        dag = self.get_dag()

        tis = self.get_task_instances(session=session)

        self.log.debug("Updating state for %s considering %s task(s)", self, len(tis))

        for ti in list(tis):
            # skip in db?
            if ti.state == State.REMOVED:
                tis.remove(ti)
            else:
                ti.task = dag.get_task(ti.task_id)

        # pre-calculate
        # db is faster
        start_dttm = timezone.utcnow()
        unfinished_tasks = self.get_task_instances(
            state=State.unfinished(),
            session=session
        )
        none_depends_on_past = all(not t.task.depends_on_past for t in unfinished_tasks)
        none_task_concurrency = all(t.task.task_concurrency is None
                                    for t in unfinished_tasks)
        # small speed up
        if unfinished_tasks and none_depends_on_past and none_task_concurrency:
            # todo: this can actually get pretty slow: one task costs between 0.01-015s
            no_dependencies_met = True
            for ut in unfinished_tasks:
                # We need to flag upstream and check for changes because upstream
                # failures/re-schedules can result in deadlock false positives
                old_state = ut.state
                deps_met = ut.are_dependencies_met(
                    dep_context=DepContext(
                        flag_upstream_failed=True,
                        ignore_in_retry_period=True,
                        ignore_in_reschedule_period=True),
                    session=session)
                if deps_met or old_state != ut.current_state(session=session):
                    no_dependencies_met = False
                    break

        duration = (timezone.utcnow() - start_dttm).total_seconds() * 1000
        Stats.timing("dagrun.dependency-check.{}".format(self.dag_id), duration)

        # future: remove the check on adhoc tasks (=active_tasks)
        if len(tis) == len(dag.active_tasks):
            root_ids = [t.task_id for t in dag.roots]
            roots = [t for t in tis if t.task_id in root_ids]

            # if all roots finished and at least one failed, the run failed
            if (not unfinished_tasks and
                    any(r.state in (State.FAILED, State.UPSTREAM_FAILED) for r in roots)):
                self.log.info('Marking run %s failed', self)
                self.set_state(State.FAILED)
                dag.handle_callback(self, success=False, reason='task_failure',
                                    session=session)

            # if all roots succeeded and no unfinished tasks, the run succeeded
            elif not unfinished_tasks and all(r.state in (State.SUCCESS, State.SKIPPED)
                                              for r in roots):
                self.log.info('Marking run %s successful', self)
                self.set_state(State.SUCCESS)
                dag.handle_callback(self, success=True, reason='success', session=session)

            # if *all tasks* are deadlocked, the run failed
            elif (unfinished_tasks and none_depends_on_past and
                  none_task_concurrency and no_dependencies_met):
                self.log.info('Deadlock; marking run %s failed', self)
                self.set_state(State.FAILED)
                dag.handle_callback(self, success=False, reason='all_tasks_deadlocked',
                                    session=session)

            # finally, if the roots aren't done, the dag is still running
            else:
                self.set_state(State.RUNNING)

        # todo: determine we want to use with_for_update to make sure to lock the run
        session.merge(self)
        session.commit()

        return self.state

    @provide_session
    def verify_integrity(self, session=None):
        """
        Verifies the DagRun by checking for removed tasks or tasks that are not in the
        database yet. It will set state to removed or add the task if required.
        """
        dag = self.get_dag()
        tis = self.get_task_instances(session=session)

        # check for removed or restored tasks
        task_ids = []
        for ti in tis:
            task_ids.append(ti.task_id)
            task = None
            try:
                task = dag.get_task(ti.task_id)
            except AirflowException:
                if ti.state == State.REMOVED:
                    pass  # ti has already been removed, just ignore it
                elif self.state is not State.RUNNING and not dag.partial:
                    self.log.warning("Failed to get task '{}' for dag '{}'. "
                                     "Marking it as removed.".format(ti, dag))
                    Stats.incr(
                        "task_removed_from_dag.{}".format(dag.dag_id), 1, 1)
                    ti.state = State.REMOVED

            is_task_in_dag = task is not None
            should_restore_task = is_task_in_dag and ti.state == State.REMOVED
            if should_restore_task:
                self.log.info("Restoring task '{}' which was previously "
                              "removed from DAG '{}'".format(ti, dag))
                Stats.incr("task_restored_to_dag.{}".format(dag.dag_id), 1, 1)
                ti.state = State.NONE

        # check for missing tasks
        for task in six.itervalues(dag.task_dict):
            if task.adhoc:
                continue
            if task.start_date > self.execution_date and not self.is_backfill:
                continue

            if task.task_id not in task_ids:
                Stats.incr(
                    "task_instance_created-{}".format(task.__class__.__name__),
                    1, 1)
                ti = TaskInstance(task, self.execution_date)
                session.add(ti)

        session.commit()

    @staticmethod
    def get_run(session, dag_id, execution_date):
        """
        :param dag_id: DAG ID
        :type dag_id: unicode
        :param execution_date: execution date
        :type execution_date: datetime
        :return: DagRun corresponding to the given dag_id and execution date
            if one exists. None otherwise.
        :rtype: DagRun
        """
        qry = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.external_trigger == False, # noqa
            DagRun.execution_date == execution_date,
        )
        return qry.first()

    @property
    def is_backfill(self):
        from airflow.jobs import BackfillJob
        return (
            self.run_id is not None and
            self.run_id.startswith(BackfillJob.ID_PREFIX)
        )

    @classmethod
    @provide_session
    def get_latest_runs(cls, session):
        """Returns the latest DagRun for each DAG. """
        subquery = (
            session
            .query(
                cls.dag_id,
                func.max(cls.execution_date).label('execution_date'))
            .group_by(cls.dag_id)
            .subquery()
        )
        dagruns = (
            session
            .query(cls)
            .join(subquery,
                  and_(cls.dag_id == subquery.c.dag_id,
                       cls.execution_date == subquery.c.execution_date))
            .all()
        )
        return dagruns


class Pool(Base):
    __tablename__ = "slot_pool"

    id = Column(Integer, primary_key=True)
    pool = Column(String(50), unique=True)
    slots = Column(Integer, default=0)
    description = Column(Text)

    def __repr__(self):
        return self.pool

    def to_json(self):
        return {
            'id': self.id,
            'pool': self.pool,
            'slots': self.slots,
            'description': self.description,
        }

    @provide_session
    def used_slots(self, session):
        """
        Returns the number of slots used at the moment
        """
        running = (
            session
            .query(TaskInstance)
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.RUNNING)
            .count()
        )
        return running

    @provide_session
    def queued_slots(self, session):
        """
        Returns the number of slots used at the moment
        """
        return (
            session
            .query(TaskInstance)
            .filter(TaskInstance.pool == self.pool)
            .filter(TaskInstance.state == State.QUEUED)
            .count()
        )

    @provide_session
    def open_slots(self, session):
        """
        Returns the number of slots open at the moment
        """
        used_slots = self.used_slots(session=session)
        queued_slots = self.queued_slots(session=session)
        return self.slots - used_slots - queued_slots
