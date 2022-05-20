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
import collections.abc
import contextlib
import hashlib
import logging
import math
import operator
import os
import pickle
import signal
import warnings
from collections import defaultdict
from datetime import datetime, timedelta
from functools import partial
from tempfile import NamedTemporaryFile
from types import TracebackType
from typing import (
    IO,
    TYPE_CHECKING,
    Any,
    Callable,
    Collection,
    ContextManager,
    Dict,
    Generator,
    Iterable,
    Iterator,
    List,
    NamedTuple,
    Optional,
    Set,
    Tuple,
    Union,
)
from urllib.parse import quote

import attr
import dill
import jinja2
import pendulum
from jinja2 import TemplateAssertionError, UndefinedError
from sqlalchemy import (
    Column,
    Float,
    ForeignKeyConstraint,
    Index,
    Integer,
    PickleType,
    String,
    and_,
    false,
    func,
    inspect,
    or_,
    text,
)
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import reconstructor, relationship
from sqlalchemy.orm.attributes import NO_VALUE, set_committed_value
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.query import Query
from sqlalchemy.orm.session import Session
from sqlalchemy.sql.elements import BooleanClauseList
from sqlalchemy.sql.expression import ColumnOperators
from sqlalchemy.sql.sqltypes import BigInteger

from airflow import settings
from airflow.compat.functools import cache
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowException,
    AirflowFailException,
    AirflowRescheduleException,
    AirflowSensorTimeout,
    AirflowSkipException,
    AirflowSmartSensorException,
    AirflowTaskTimeout,
    DagRunNotFound,
    TaskDeferralError,
    TaskDeferred,
    UnmappableXComLengthPushed,
    UnmappableXComTypePushed,
    XComForMappingNotPushed,
)
from airflow.models.base import COLLATION_ARGS, ID_LEN, Base
from airflow.models.log import Log
from airflow.models.param import ParamsDict
from airflow.models.taskfail import TaskFail
from airflow.models.taskmap import TaskMap
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.xcom import XCOM_RETURN_KEY, XCom
from airflow.plugins_manager import integrate_macros_plugins
from airflow.sentry import Sentry
from airflow.stats import Stats
from airflow.templates import SandboxedEnvironment
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.timetables.base import DataInterval
from airflow.typing_compat import Literal
from airflow.utils import timezone
from airflow.utils.context import ConnectionAccessor, Context, VariableAccessor, context_merge
from airflow.utils.email import send_email
from airflow.utils.helpers import render_template_to_string
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.operator_helpers import context_to_airflow_vars
from airflow.utils.platform import getuser
from airflow.utils.retries import run_with_db_retries
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.sqlalchemy import ExtendedJSON, UtcDateTime, tuple_in_condition, with_row_locks
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.timeout import timeout

TR = TaskReschedule

_CURRENT_CONTEXT: List[Context] = []
log = logging.getLogger(__name__)


if TYPE_CHECKING:
    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG, DagModel
    from airflow.models.dagrun import DagRun
    from airflow.models.operator import Operator


@contextlib.contextmanager
def set_current_context(context: Context) -> Iterator[Context]:
    """
    Sets the current execution context to the provided context object.
    This method should be called once per Task execution, before calling operator.execute.
    """
    _CURRENT_CONTEXT.append(context)
    try:
        yield context
    finally:
        expected_state = _CURRENT_CONTEXT.pop()
        if expected_state != context:
            log.warning(
                "Current context is not equal to the state at context stack. Expected=%s, got=%s",
                context,
                expected_state,
            )


def load_error_file(fd: IO[bytes]) -> Optional[Union[str, Exception]]:
    """Load and return error from error file"""
    if fd.closed:
        return None
    fd.seek(0, os.SEEK_SET)
    data = fd.read()
    if not data:
        return None
    try:
        return pickle.loads(data)
    except Exception:
        return "Failed to load task run error"


def set_error_file(error_file: str, error: Union[str, BaseException]) -> None:
    """Write error into error file by path"""
    with open(error_file, "wb") as fd:
        try:
            pickle.dump(error, fd)
        except Exception:
            # local class objects cannot be pickled, so we fallback
            # to store the string representation instead
            pickle.dump(str(error), fd)


def clear_task_instances(
    tis,
    session,
    activate_dag_runs=None,
    dag=None,
    dag_run_state: Union[DagRunState, Literal[False]] = DagRunState.QUEUED,
):
    """
    Clears a set of task instances, but makes sure the running ones
    get killed.

    :param tis: a list of task instances
    :param session: current session
    :param dag_run_state: state to set DagRun to. If set to False, dagrun state will not
        be changed.
    :param dag: DAG object
    :param activate_dag_runs: Deprecated parameter, do not pass
    """
    job_ids = []
    # Keys: dag_id -> run_id -> map_indexes -> try_numbers -> task_id
    task_id_by_key: Dict[str, Dict[str, Dict[int, Dict[int, Set[str]]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(set)))
    )
    for ti in tis:
        if ti.state == TaskInstanceState.RUNNING:
            if ti.job_id:
                # If a task is cleared when running, set its state to RESTARTING so that
                # the task is terminated and becomes eligible for retry.
                ti.state = TaskInstanceState.RESTARTING
                job_ids.append(ti.job_id)
        else:
            task_id = ti.task_id
            if dag and dag.has_task(task_id):
                task = dag.get_task(task_id)
                ti.refresh_from_task(task)
                task_retries = task.retries
                ti.max_tries = ti.try_number + task_retries - 1
            else:
                # Ignore errors when updating max_tries if dag is None or
                # task not found in dag since database records could be
                # outdated. We make max_tries the maximum value of its
                # original max_tries or the last attempted try number.
                ti.max_tries = max(ti.max_tries, ti.prev_attempted_tries)
            ti.state = None
            ti.external_executor_id = None
            session.merge(ti)

        task_id_by_key[ti.dag_id][ti.run_id][ti.map_index][ti.try_number].add(ti.task_id)

    if task_id_by_key:
        # Clear all reschedules related to the ti to clear

        # This is an optimization for the common case where all tis are for a small number
        # of dag_id, run_id, try_number, and map_index. Use a nested dict of dag_id,
        # run_id, try_number, map_index, and task_id to construct the where clause in a
        # hierarchical manner. This speeds up the delete statement by more than 40x for
        # large number of tis (50k+).
        conditions = or_(
            and_(
                TR.dag_id == dag_id,
                or_(
                    and_(
                        TR.run_id == run_id,
                        or_(
                            and_(
                                TR.map_index == map_index,
                                or_(
                                    and_(TR.try_number == try_number, TR.task_id.in_(task_ids))
                                    for try_number, task_ids in task_tries.items()
                                ),
                            )
                            for map_index, task_tries in map_indexes.items()
                        ),
                    )
                    for run_id, map_indexes in run_ids.items()
                ),
            )
            for dag_id, run_ids in task_id_by_key.items()
        )

        delete_qry = TR.__table__.delete().where(conditions)
        session.execute(delete_qry)

    if job_ids:
        from airflow.jobs.base_job import BaseJob

        for job in session.query(BaseJob).filter(BaseJob.id.in_(job_ids)).all():
            job.state = TaskInstanceState.RESTARTING

    if activate_dag_runs is not None:
        warnings.warn(
            "`activate_dag_runs` parameter to clear_task_instances function is deprecated. "
            "Please use `dag_run_state`",
            DeprecationWarning,
            stacklevel=2,
        )
        if not activate_dag_runs:
            dag_run_state = False

    if dag_run_state is not False and tis:
        from airflow.models.dagrun import DagRun  # Avoid circular import

        run_ids_by_dag_id = defaultdict(set)
        for instance in tis:
            run_ids_by_dag_id[instance.dag_id].add(instance.run_id)

        drs = (
            session.query(DagRun)
            .filter(
                or_(
                    and_(DagRun.dag_id == dag_id, DagRun.run_id.in_(run_ids))
                    for dag_id, run_ids in run_ids_by_dag_id.items()
                )
            )
            .all()
        )
        dag_run_state = DagRunState(dag_run_state)  # Validate the state value.
        for dr in drs:
            dr.state = dag_run_state
            dr.start_date = timezone.utcnow()
            if dag_run_state == DagRunState.QUEUED:
                dr.last_scheduling_decision = None
                dr.start_date = None


class _LazyXComAccessIterator(collections.abc.Iterator):
    __slots__ = ['_cm', '_it']

    def __init__(self, cm: ContextManager[Query]):
        self._cm = cm
        self._it = None

    def __del__(self):
        if self._it:
            self._cm.__exit__(None, None, None)

    def __iter__(self):
        return self

    def __next__(self):
        if not self._it:
            self._it = iter(self._cm.__enter__())
        return XCom.deserialize_value(next(self._it))


@attr.define
class _LazyXComAccess(collections.abc.Sequence):
    """Wrapper to lazily pull XCom with a sequence-like interface.

    Note that since the session bound to the parent query may have died when we
    actually access the sequence's content, we must create a new session
    for every function call with ``with_session()``.
    """

    dag_id: str
    run_id: str
    task_id: str
    _query: Query = attr.ib(repr=False)
    _len: Optional[int] = attr.ib(init=False, repr=False, default=None)

    @classmethod
    def build_from_single_xcom(cls, first: "XCom", query: Query) -> "_LazyXComAccess":
        return cls(
            dag_id=first.dag_id,
            run_id=first.run_id,
            task_id=first.task_id,
            query=query.with_entities(XCom.value)
            .filter(
                XCom.run_id == first.run_id,
                XCom.task_id == first.task_id,
                XCom.dag_id == first.dag_id,
                XCom.map_index >= 0,
            )
            .order_by(None)
            .order_by(XCom.map_index.asc()),
        )

    def __len__(self):
        if self._len is None:
            with self._get_bound_query() as query:
                self._len = query.count()
        return self._len

    def __iter__(self):
        return _LazyXComAccessIterator(self._get_bound_query())

    def __getitem__(self, key):
        if not isinstance(key, int):
            raise ValueError("only support index access for now")
        try:
            with self._get_bound_query() as query:
                r = query.offset(key).limit(1).one()
        except NoResultFound:
            raise IndexError(key) from None
        return XCom.deserialize_value(r)

    @contextlib.contextmanager
    def _get_bound_query(self) -> Generator[Query, None, None]:
        # Do we have a valid session already?
        if self._query.session and self._query.session.is_active:
            yield self._query
            return

        session = settings.Session()
        try:
            yield self._query.with_session(session)
        finally:
            session.close()


class TaskInstanceKey(NamedTuple):
    """Key used to identify task instance."""

    dag_id: str
    task_id: str
    run_id: str
    try_number: int = 1
    map_index: int = -1

    @property
    def primary(self) -> Tuple[str, str, str, int]:
        """Return task instance primary key part of the key"""
        return self.dag_id, self.task_id, self.run_id, self.map_index

    @property
    def reduced(self) -> 'TaskInstanceKey':
        """Remake the key by subtracting 1 from try number to match in memory information"""
        return TaskInstanceKey(
            self.dag_id, self.task_id, self.run_id, max(1, self.try_number - 1), self.map_index
        )

    def with_try_number(self, try_number: int) -> 'TaskInstanceKey':
        """Returns TaskInstanceKey with provided ``try_number``"""
        return TaskInstanceKey(self.dag_id, self.task_id, self.run_id, try_number, self.map_index)

    @property
    def key(self) -> "TaskInstanceKey":
        """For API-compatibly with TaskInstance.

        Returns self
        """
        return self


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

    A value of -1 in map_index represents any of: a TI without mapped tasks;
    a TI with mapped tasks that has yet to be expanded (state=pending);
    a TI with mapped tasks that expanded to an empty list (state=skipped).
    """

    __tablename__ = "task_instance"

    task_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True, nullable=False)
    dag_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True, nullable=False)
    run_id = Column(String(ID_LEN, **COLLATION_ARGS), primary_key=True, nullable=False)
    map_index = Column(Integer, primary_key=True, nullable=False, server_default=text("-1"))

    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    _try_number = Column('try_number', Integer, default=0)
    max_tries = Column(Integer)
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    job_id = Column(Integer)
    pool = Column(String(256), nullable=False)
    pool_slots = Column(Integer, default=1, nullable=False, server_default=text("1"))
    queue = Column(String(256))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    queued_by_job_id = Column(Integer)
    pid = Column(Integer)
    executor_config = Column(PickleType(pickler=dill))

    external_executor_id = Column(String(ID_LEN, **COLLATION_ARGS))

    # The trigger to resume on if we are in state DEFERRED
    trigger_id = Column(BigInteger)

    # Optional timeout datetime for the trigger (past this, we'll fail)
    trigger_timeout = Column(UtcDateTime)

    # The method to call next, and any extra arguments to pass to it.
    # Usually used when resuming from DEFERRED.
    next_method = Column(String(1000))
    next_kwargs = Column(MutableDict.as_mutable(ExtendedJSON))

    # If adding new fields here then remember to add them to
    # refresh_from_db() or they won't display in the UI correctly

    __table_args__ = (
        Index('ti_dag_state', dag_id, state),
        Index('ti_dag_run', dag_id, run_id),
        Index('ti_state', state),
        Index('ti_state_lkp', dag_id, task_id, run_id, state),
        Index('ti_pool', pool, state, priority_weight),
        Index('ti_job_id', job_id),
        Index('ti_trigger_id', trigger_id),
        ForeignKeyConstraint(
            [trigger_id],
            ['trigger.id'],
            name='task_instance_trigger_id_fkey',
            ondelete='CASCADE',
        ),
        ForeignKeyConstraint(
            [dag_id, run_id],
            ["dag_run.dag_id", "dag_run.run_id"],
            name='task_instance_dag_run_fkey',
            ondelete="CASCADE",
        ),
    )

    dag_model = relationship(
        "DagModel",
        primaryjoin="TaskInstance.dag_id == DagModel.dag_id",
        foreign_keys=dag_id,
        uselist=False,
        innerjoin=True,
        viewonly=True,
    )

    trigger = relationship(
        "Trigger",
        primaryjoin="TaskInstance.trigger_id == Trigger.id",
        foreign_keys=trigger_id,
        uselist=False,
        innerjoin=True,
    )

    dag_run = relationship("DagRun", back_populates="task_instances", lazy='joined', innerjoin=True)
    rendered_task_instance_fields = relationship("RenderedTaskInstanceFields", lazy='noload', uselist=False)

    execution_date = association_proxy("dag_run", "execution_date")

    task: "Operator"  # Not always set...

    def __init__(
        self,
        task: "Operator",
        execution_date: Optional[datetime] = None,
        run_id: Optional[str] = None,
        state: Optional[str] = None,
        map_index: int = -1,
    ):
        super().__init__()
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.map_index = map_index
        self.refresh_from_task(task)
        # init_on_load will config the log
        self.init_on_load()

        if run_id is None and execution_date is not None:
            from airflow.models.dagrun import DagRun  # Avoid circular import

            warnings.warn(
                "Passing an execution_date to `TaskInstance()` is deprecated in favour of passing a run_id",
                DeprecationWarning,
                # Stack level is 4 because SQLA adds some wrappers around the constructor
                stacklevel=4,
            )
            # make sure we have a localized execution_date stored in UTC
            if execution_date and not timezone.is_localized(execution_date):
                self.log.warning(
                    "execution date %s has no timezone information. Using default from dag or system",
                    execution_date,
                )
                if self.task.has_dag():
                    assert self.task.dag  # For Mypy.
                    execution_date = timezone.make_aware(execution_date, self.task.dag.timezone)
                else:
                    execution_date = timezone.make_aware(execution_date)

                execution_date = timezone.convert_to_utc(execution_date)
            with create_session() as session:
                run_id = (
                    session.query(DagRun.run_id)
                    .filter_by(dag_id=self.dag_id, execution_date=execution_date)
                    .scalar()
                )
                if run_id is None:
                    raise DagRunNotFound(
                        f"DagRun for {self.dag_id!r} with date {execution_date} not found"
                    ) from None

        self.run_id = run_id

        self.try_number = 0
        self.max_tries = self.task.retries
        self.unixname = getuser()
        if state:
            self.state = state
        self.hostname = ''
        # Is this TaskInstance being currently running within `airflow tasks run --raw`.
        # Not persisted to the database so only valid for the current process
        self.raw = False
        # can be changed when calling 'run'
        self.test_mode = False

    @staticmethod
    def insert_mapping(run_id: str, task: "Operator", map_index: int) -> dict:
        """:meta private:"""
        return {
            'dag_id': task.dag_id,
            'task_id': task.task_id,
            'run_id': run_id,
            '_try_number': 0,
            'hostname': '',
            'unixname': getuser(),
            'queue': task.queue,
            'pool': task.pool,
            'pool_slots': task.pool_slots,
            'priority_weight': task.priority_weight_total,
            'run_as_user': task.run_as_user,
            'max_tries': task.retries,
            'executor_config': task.executor_config,
            'operator': task.task_type,
            'map_index': map_index,
        }

    @reconstructor
    def init_on_load(self):
        """Initialize the attributes that aren't stored in the DB"""
        # correctly config the ti log
        self._log = logging.getLogger("airflow.task")
        self.test_mode = False  # can be changed when calling 'run'

    @property
    def try_number(self):
        """
        Return the try number that this task number will be when it is actually
        run.

        If the TaskInstance is currently running, this will match the column in the
        database, in all other cases this will be incremented.
        """
        # This is designed so that task logs end up in the right file.
        # TODO: whether we need sensing here or not (in sensor and task_instance state machine)
        if self.state in State.running:
            return self._try_number
        return self._try_number + 1

    @try_number.setter
    def try_number(self, value):
        self._try_number = value

    @property
    def prev_attempted_tries(self):
        """
        Based on this instance's try_number, this will calculate
        the number of previously attempted tries, defaulting to 0.
        """
        # Expose this for the Task Tries and Gantt graph views.
        # Using `try_number` throws off the counts for non-running tasks.
        # Also useful in error logging contexts to get
        # the try number for the last try that was attempted.
        # https://issues.apache.org/jira/browse/AIRFLOW-2143

        return self._try_number

    @property
    def next_try_number(self):
        """Setting Next Try Number"""
        return self._try_number + 1

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
        cfg_path=None,
    ):
        """
        Returns a command that can be executed anywhere where airflow is
        installed. This command is part of the message sent to executors by
        the orchestrator.
        """
        dag: Union["DAG", "DagModel"]
        # Use the dag if we have it, else fallback to the ORM dag_model, which might not be loaded
        if hasattr(self, 'task') and hasattr(self.task, 'dag'):
            dag = self.task.dag
        else:
            dag = self.dag_model

        should_pass_filepath = not pickle_id and dag
        path = None
        if should_pass_filepath:
            if dag.is_subdag:
                path = dag.parent_dag.relative_fileloc
            else:
                path = dag.relative_fileloc

            if path:
                if not path.is_absolute():
                    path = 'DAGS_FOLDER' / path
                path = str(path)

        return TaskInstance.generate_command(
            self.dag_id,
            self.task_id,
            run_id=self.run_id,
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
            cfg_path=cfg_path,
            map_index=self.map_index,
        )

    @staticmethod
    def generate_command(
        dag_id: str,
        task_id: str,
        run_id: str,
        mark_success: bool = False,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        local: bool = False,
        pickle_id: Optional[int] = None,
        file_path: Optional[str] = None,
        raw: bool = False,
        job_id: Optional[str] = None,
        pool: Optional[str] = None,
        cfg_path: Optional[str] = None,
        map_index: int = -1,
    ) -> List[str]:
        """
        Generates the shell command required to execute this task instance.

        :param dag_id: DAG ID
        :param task_id: Task ID
        :param run_id: The run_id of this task's DagRun
        :param mark_success: Whether to mark the task as successful
        :param ignore_all_deps: Ignore all ignorable dependencies.
            Overrides the other ignore_* parameters.
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
            (e.g. for Backfills)
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :param local: Whether to run the task locally
        :param pickle_id: If the DAG was serialized to the DB, the ID
            associated with the pickled DAG
        :param file_path: path to the file containing the DAG definition
        :param raw: raw mode (needs more details)
        :param job_id: job ID (needs more details)
        :param pool: the Airflow pool that the task should run in
        :param cfg_path: the Path to the configuration file
        :return: shell command that can be used to run the task instance
        :rtype: list[str]
        """
        cmd = ["airflow", "tasks", "run", dag_id, task_id, run_id]
        if mark_success:
            cmd.extend(["--mark-success"])
        if pickle_id:
            cmd.extend(["--pickle", str(pickle_id)])
        if job_id:
            cmd.extend(["--job-id", str(job_id)])
        if ignore_all_deps:
            cmd.extend(["--ignore-all-dependencies"])
        if ignore_task_deps:
            cmd.extend(["--ignore-dependencies"])
        if ignore_depends_on_past:
            cmd.extend(["--ignore-depends-on-past"])
        if ignore_ti_state:
            cmd.extend(["--force"])
        if local:
            cmd.extend(["--local"])
        if pool:
            cmd.extend(["--pool", pool])
        if raw:
            cmd.extend(["--raw"])
        if file_path:
            cmd.extend(["--subdir", file_path])
        if cfg_path:
            cmd.extend(["--cfg-path", cfg_path])
        if map_index != -1:
            cmd.extend(['--map-index', str(map_index)])
        return cmd

    @property
    def log_url(self):
        """Log URL for TaskInstance"""
        iso = quote(self.execution_date.isoformat())
        base_url = conf.get('webserver', 'BASE_URL')
        return base_url + f"/log?execution_date={iso}&task_id={self.task_id}&dag_id={self.dag_id}"

    @property
    def mark_success_url(self):
        """URL to mark TI success"""
        base_url = conf.get('webserver', 'BASE_URL')
        return base_url + (
            "/confirm"
            f"?task_id={self.task_id}"
            f"&dag_id={self.dag_id}"
            f"&dag_run_id={quote(self.run_id)}"
            "&upstream=false"
            "&downstream=false"
            "&state=success"
        )

    @provide_session
    def current_state(self, session=NEW_SESSION) -> str:
        """
        Get the very latest state from the database, if a session is passed,
        we use and looking up the state becomes part of the session, otherwise
        a new session is used.

        :param session: SQLAlchemy ORM Session
        """
        return (
            session.query(TaskInstance.state)
            .filter(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.run_id == self.run_id,
            )
            .scalar()
        )

    @provide_session
    def error(self, session=NEW_SESSION):
        """
        Forces the task instance's state to FAILED in the database.

        :param session: SQLAlchemy ORM Session
        """
        self.log.error("Recording the task instance as FAILED")
        self.state = State.FAILED
        session.merge(self)
        session.commit()

    @provide_session
    def refresh_from_db(self, session=NEW_SESSION, lock_for_update=False) -> None:
        """
        Refreshes the task instance from the database based on the primary key

        :param session: SQLAlchemy ORM Session
        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        """
        self.log.debug("Refreshing TaskInstance %s from DB", self)

        qry = session.query(TaskInstance).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.run_id == self.run_id,
            TaskInstance.map_index == self.map_index,
        )

        if lock_for_update:
            for attempt in run_with_db_retries(logger=self.log):
                with attempt:
                    ti: Optional[TaskInstance] = qry.with_for_update().first()
        else:
            ti = qry.first()
        if ti:
            # Fields ordered per model definition
            self.start_date = ti.start_date
            self.end_date = ti.end_date
            self.duration = ti.duration
            self.state = ti.state
            # Get the raw value of try_number column, don't read through the
            # accessor here otherwise it will be incremented by one already.
            self.try_number = ti._try_number
            self.max_tries = ti.max_tries
            self.hostname = ti.hostname
            self.unixname = ti.unixname
            self.job_id = ti.job_id
            self.pool = ti.pool
            self.pool_slots = ti.pool_slots or 1
            self.queue = ti.queue
            self.priority_weight = ti.priority_weight
            self.operator = ti.operator
            self.queued_dttm = ti.queued_dttm
            self.queued_by_job_id = ti.queued_by_job_id
            self.pid = ti.pid
            self.executor_config = ti.executor_config
            self.external_executor_id = ti.external_executor_id
            self.trigger_id = ti.trigger_id
            self.next_method = ti.next_method
            self.next_kwargs = ti.next_kwargs
        else:
            self.state = None

    def refresh_from_task(self, task: "Operator", pool_override=None):
        """
        Copy common attributes from the given task.

        :param task: The task object to copy from
        :param pool_override: Use the pool_override instead of task's pool
        """
        self.task = task
        self.queue = task.queue
        self.pool = pool_override or task.pool
        self.pool_slots = task.pool_slots
        self.priority_weight = task.priority_weight_total
        self.run_as_user = task.run_as_user
        # Do not set max_tries to task.retries here because max_tries is a cumulative
        # value that needs to be stored in the db.
        self.executor_config = task.executor_config
        self.operator = task.task_type

    @provide_session
    def clear_xcom_data(self, session: Session = NEW_SESSION):
        """Clear all XCom data from the database for the task instance.

        If the task is unmapped, all XComs matching this task ID in the same DAG
        run are removed. If the task is mapped, only the one with matching map
        index is removed.

        :param session: SQLAlchemy ORM Session
        """
        self.log.debug("Clearing XCom data")
        if self.map_index < 0:
            map_index: Optional[int] = None
        else:
            map_index = self.map_index
        XCom.clear(
            dag_id=self.dag_id,
            task_id=self.task_id,
            run_id=self.run_id,
            map_index=map_index,
            session=session,
        )

    @property
    def key(self) -> TaskInstanceKey:
        """Returns a tuple that identifies the task instance uniquely"""
        return TaskInstanceKey(self.dag_id, self.task_id, self.run_id, self.try_number, self.map_index)

    @provide_session
    def set_state(self, state: Optional[str], session=NEW_SESSION):
        """
        Set TaskInstance state.

        :param state: State to set for the TI
        :param session: SQLAlchemy ORM Session
        """
        current_time = timezone.utcnow()
        self.log.debug("Setting task state for %s to %s", self, state)
        self.state = state
        self.start_date = self.start_date or current_time
        if self.state in State.finished or self.state == State.UP_FOR_RETRY:
            self.end_date = self.end_date or current_time
            self.duration = (self.end_date - self.start_date).total_seconds()
        session.merge(self)

    @property
    def is_premature(self):
        """
        Returns whether a task is in UP_FOR_RETRY state and its retry interval
        has elapsed.
        """
        # is the task still in the retry waiting period?
        return self.state == State.UP_FOR_RETRY and not self.ready_for_retry()

    @provide_session
    def are_dependents_done(self, session=NEW_SESSION):
        """
        Checks whether the immediate dependents of this task instance have succeeded or have been skipped.
        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.

        :param session: SQLAlchemy ORM Session
        """
        task = self.task

        if not task.downstream_task_ids:
            return True

        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(task.downstream_task_ids),
            TaskInstance.run_id == self.run_id,
            TaskInstance.state.in_([State.SKIPPED, State.SUCCESS]),
        )
        count = ti[0][0]
        return count == len(task.downstream_task_ids)

    @provide_session
    def get_previous_dagrun(
        self,
        state: Optional[DagRunState] = None,
        session: Optional[Session] = None,
    ) -> Optional["DagRun"]:
        """The DagRun that ran before this task instance's DagRun.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session.
        """
        dag = self.task.dag
        if dag is None:
            return None

        dr = self.get_dagrun(session=session)
        dr.dag = dag

        # We always ignore schedule in dagrun lookup when `state` is given
        # or the DAG is never scheduled. For legacy reasons, when
        # `catchup=True`, we use `get_previous_scheduled_dagrun` unless
        # `ignore_schedule` is `True`.
        ignore_schedule = state is not None or not dag.timetable.can_run
        if dag.catchup is True and not ignore_schedule:
            last_dagrun = dr.get_previous_scheduled_dagrun(session=session)
        else:
            last_dagrun = dr.get_previous_dagrun(session=session, state=state)

        if last_dagrun:
            return last_dagrun

        return None

    @provide_session
    def get_previous_ti(
        self,
        state: Optional[DagRunState] = None,
        session: Session = NEW_SESSION,
    ) -> Optional['TaskInstance']:
        """
        The task instance for the task that ran before this task instance.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session
        """
        dagrun = self.get_previous_dagrun(state, session=session)
        if dagrun is None:
            return None
        return dagrun.get_task_instance(self.task_id, session=session)

    @property
    def previous_ti(self):
        """
        This attribute is deprecated.
        Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
        """
        warnings.warn(
            """
            This attribute is deprecated.
            Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_previous_ti()

    @property
    def previous_ti_success(self) -> Optional['TaskInstance']:
        """
        This attribute is deprecated.
        Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
        """
        warnings.warn(
            """
            This attribute is deprecated.
            Please use `airflow.models.taskinstance.TaskInstance.get_previous_ti` method.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_previous_ti(state=DagRunState.SUCCESS)

    @provide_session
    def get_previous_execution_date(
        self,
        state: Optional[DagRunState] = None,
        session: Session = NEW_SESSION,
    ) -> Optional[pendulum.DateTime]:
        """
        The execution date from property previous_ti_success.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session
        """
        self.log.debug("previous_execution_date was called")
        prev_ti = self.get_previous_ti(state=state, session=session)
        return prev_ti and pendulum.instance(prev_ti.execution_date)

    @provide_session
    def get_previous_start_date(
        self, state: Optional[DagRunState] = None, session: Session = NEW_SESSION
    ) -> Optional[pendulum.DateTime]:
        """
        The start date from property previous_ti_success.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session
        """
        self.log.debug("previous_start_date was called")
        prev_ti = self.get_previous_ti(state=state, session=session)
        # prev_ti may not exist and prev_ti.start_date may be None.
        return prev_ti and prev_ti.start_date and pendulum.instance(prev_ti.start_date)

    @property
    def previous_start_date_success(self) -> Optional[pendulum.DateTime]:
        """
        This attribute is deprecated.
        Please use `airflow.models.taskinstance.TaskInstance.get_previous_start_date` method.
        """
        warnings.warn(
            """
            This attribute is deprecated.
            Please use `airflow.models.taskinstance.TaskInstance.get_previous_start_date` method.
            """,
            DeprecationWarning,
            stacklevel=2,
        )
        return self.get_previous_start_date(state=DagRunState.SUCCESS)

    @provide_session
    def are_dependencies_met(self, dep_context=None, session=NEW_SESSION, verbose=False):
        """
        Returns whether or not all the conditions are met for this task instance to be run
        given the context for the dependencies (e.g. a task instance being force run from
        the UI will ignore some dependencies).

        :param dep_context: The execution context that determines the dependencies that
            should be evaluated.
        :param session: database session
        :param verbose: whether log details on failed dependencies on
            info or debug log level
        """
        dep_context = dep_context or DepContext()
        failed = False
        verbose_aware_logger = self.log.info if verbose else self.log.debug
        for dep_status in self.get_failed_dep_statuses(dep_context=dep_context, session=session):
            failed = True

            verbose_aware_logger(
                "Dependencies not met for %s, dependency '%s' FAILED: %s",
                self,
                dep_status.dep_name,
                dep_status.reason,
            )

        if failed:
            return False

        verbose_aware_logger("Dependencies all met for %s", self)
        return True

    @provide_session
    def get_failed_dep_statuses(self, dep_context=None, session=NEW_SESSION):
        """Get failed Dependencies"""
        dep_context = dep_context or DepContext()
        for dep in dep_context.deps | self.task.deps:
            for dep_status in dep.get_dep_statuses(self, session, dep_context):

                self.log.debug(
                    "%s dependency '%s' PASSED: %s, %s",
                    self,
                    dep_status.dep_name,
                    dep_status.passed,
                    dep_status.reason,
                )

                if not dep_status.passed:
                    yield dep_status

    def __repr__(self):
        prefix = f"<TaskInstance: {self.dag_id}.{self.task_id} {self.run_id} "
        if self.map_index != -1:
            prefix += f"map_index={self.map_index} "
        return prefix + f"[{self.state}]>"

    def next_retry_datetime(self):
        """
        Get datetime of the next retry if the task instance fails. For exponential
        backoff, retry_delay is used as base and will be converted to seconds.
        """
        delay = self.task.retry_delay
        if self.task.retry_exponential_backoff:
            # If the min_backoff calculation is below 1, it will be converted to 0 via int. Thus,
            # we must round up prior to converting to an int, otherwise a divide by zero error
            # will occur in the modded_hash calculation.
            min_backoff = int(math.ceil(delay.total_seconds() * (2 ** (self.try_number - 2))))

            # In the case when delay.total_seconds() is 0, min_backoff will not be rounded up to 1.
            # To address this, we impose a lower bound of 1 on min_backoff. This effectively makes
            # the ceiling function unnecessary, but the ceiling function was retained to avoid
            # introducing a breaking change.
            if min_backoff < 1:
                min_backoff = 1

            # deterministic per task instance
            ti_hash = int(
                hashlib.sha1(
                    f"{self.dag_id}#{self.task_id}#{self.execution_date}#{self.try_number}".encode()
                ).hexdigest(),
                16,
            )
            # between 1 and 1.0 * delay * (2^retry_number)
            modded_hash = min_backoff + ti_hash % min_backoff
            # timedelta has a maximum representable value. The exponentiation
            # here means this value can be exceeded after a certain number
            # of tries (around 50 if the initial delay is 1s, even fewer if
            # the delay is larger). Cap the value here before creating a
            # timedelta object so the operation doesn't fail.
            delay_backoff_in_seconds = min(modded_hash, timedelta.max.total_seconds() - 1)
            delay = timedelta(seconds=delay_backoff_in_seconds)
            if self.task.max_retry_delay:
                delay = min(self.task.max_retry_delay, delay)
        return self.end_date + delay

    def ready_for_retry(self):
        """
        Checks on whether the task instance is in the right state and timeframe
        to be retried.
        """
        return self.state == State.UP_FOR_RETRY and self.next_retry_datetime() < timezone.utcnow()

    @provide_session
    def get_dagrun(self, session: Session = NEW_SESSION) -> "DagRun":
        """
        Returns the DagRun for this TaskInstance

        :param session: SQLAlchemy ORM Session
        :return: DagRun
        """
        info = inspect(self)
        if info.attrs.dag_run.loaded_value is not NO_VALUE:
            return self.dag_run

        from airflow.models.dagrun import DagRun  # Avoid circular import

        dr = session.query(DagRun).filter(DagRun.dag_id == self.dag_id, DagRun.run_id == self.run_id).one()

        # Record it in the instance for next time. This means that `self.execution_date` will work correctly
        set_committed_value(self, 'dag_run', dr)

        return dr

    @provide_session
    def check_and_change_state_before_execution(
        self,
        verbose: bool = True,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        test_mode: bool = False,
        job_id: Optional[str] = None,
        pool: Optional[str] = None,
        external_executor_id: Optional[str] = None,
        session=NEW_SESSION,
    ) -> bool:
        """
        Checks dependencies and then sets state to RUNNING if they are met. Returns
        True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task

        :param verbose: whether to turn on more verbose logging
        :param ignore_all_deps: Ignore all of the non-critical dependencies, just runs
        :param ignore_depends_on_past: Ignore depends_on_past DAG attribute
        :param ignore_task_deps: Don't check the dependencies of this TaskInstance's task
        :param ignore_ti_state: Disregards previous task instance state
        :param mark_success: Don't run the task, mark its state as success
        :param test_mode: Doesn't record success or failure in the DB
        :param job_id: Job (BackfillJob / LocalTaskJob / SchedulerJob) ID
        :param pool: specifies the pool to use to run the task instance
        :param external_executor_id: The identifier of the celery executor
        :param session: SQLAlchemy ORM Session
        :return: whether the state was changed to running or not
        :rtype: bool
        """
        task = self.task
        self.refresh_from_task(task, pool_override=pool)
        self.test_mode = test_mode
        self.refresh_from_db(session=session, lock_for_update=True)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.pid = None

        if not ignore_all_deps and not ignore_ti_state and self.state == State.SUCCESS:
            Stats.incr('previously_succeeded', 1, 1)

        # TODO: Logging needs cleanup, not clear what is being printed
        hr_line_break = "\n" + ("-" * 80)  # Line break

        if not mark_success:
            # Firstly find non-runnable and non-requeueable tis.
            # Since mark_success is not set, we do nothing.
            non_requeueable_dep_context = DepContext(
                deps=RUNNING_DEPS - REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_ti_state=ignore_ti_state,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps,
            )
            if not self.are_dependencies_met(
                dep_context=non_requeueable_dep_context, session=session, verbose=True
            ):
                session.commit()
                return False

            # For reporting purposes, we report based on 1-indexed,
            # not 0-indexed lists (i.e. Attempt 1 instead of
            # Attempt 0 for the first attempt).
            # Set the task start date. In case it was re-scheduled use the initial
            # start date that is recorded in task_reschedule table
            # If the task continues after being deferred (next_method is set), use the original start_date
            self.start_date = self.start_date if self.next_method else timezone.utcnow()
            if self.state == State.UP_FOR_RESCHEDULE:
                task_reschedule: TR = TR.query_for_task_instance(self, session=session).first()
                if task_reschedule:
                    self.start_date = task_reschedule.start_date

            # Secondly we find non-runnable but requeueable tis. We reset its state.
            # This is because we might have hit concurrency limits,
            # e.g. because of backfilling.
            dep_context = DepContext(
                deps=REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_depends_on_past=ignore_depends_on_past,
                ignore_task_deps=ignore_task_deps,
                ignore_ti_state=ignore_ti_state,
            )
            if not self.are_dependencies_met(dep_context=dep_context, session=session, verbose=True):
                self.state = State.NONE
                self.log.warning(hr_line_break)
                self.log.warning(
                    "Rescheduling due to concurrency limits reached "
                    "at task runtime. Attempt %s of "
                    "%s. State set to NONE.",
                    self.try_number,
                    self.max_tries + 1,
                )
                self.log.warning(hr_line_break)
                self.queued_dttm = timezone.utcnow()
                session.merge(self)
                session.commit()
                return False

        # print status message
        self.log.info(hr_line_break)
        self.log.info("Starting attempt %s of %s", self.try_number, self.max_tries + 1)
        self.log.info(hr_line_break)
        self._try_number += 1

        if not test_mode:
            session.add(Log(State.RUNNING, self))
        self.state = State.RUNNING
        self.external_executor_id = external_executor_id
        self.end_date = None
        if not test_mode:
            session.merge(self).task = task
        session.commit()

        # Closing all pooled connections to prevent
        # "max number of connections reached"
        settings.engine.dispose()  # type: ignore
        if verbose:
            if mark_success:
                self.log.info("Marking success for %s on %s", self.task, self.execution_date)
            else:
                self.log.info("Executing %s on %s", self.task, self.execution_date)
        return True

    def _date_or_empty(self, attr: str):
        result: Optional[datetime] = getattr(self, attr, None)
        return result.strftime('%Y%m%dT%H%M%S') if result else ''

    def _log_state(self, lead_msg: str = ''):
        params = [
            lead_msg,
            str(self.state).upper(),
            self.dag_id,
            self.task_id,
        ]
        message = '%sMarking task as %s. dag_id=%s, task_id=%s, '
        if self.map_index >= 0:
            params.append(self.map_index)
            message += 'map_index=%d, '
        self.log.info(
            message + 'execution_date=%s, start_date=%s, end_date=%s',
            *params,
            self._date_or_empty('execution_date'),
            self._date_or_empty('start_date'),
            self._date_or_empty('end_date'),
        )

    # Ensure we unset next_method and next_kwargs to ensure that any
    # retries don't re-use them.
    def clear_next_method_args(self):
        self.log.debug("Clearing next_method and next_kwargs.")

        self.next_method = None
        self.next_kwargs = None

    @provide_session
    @Sentry.enrich_errors
    def _run_raw_task(
        self,
        mark_success: bool = False,
        test_mode: bool = False,
        job_id: Optional[str] = None,
        pool: Optional[str] = None,
        error_file: Optional[str] = None,
        session=NEW_SESSION,
    ) -> None:
        """
        Immediately runs the task (without checking or changing db state
        before execution) and then sets the appropriate final state after
        completion and runs any post-execute callbacks. Meant to be called
        only after another function changes the state to running.

        :param mark_success: Don't run the task, mark its state as success
        :param test_mode: Doesn't record success or failure in the DB
        :param pool: specifies the pool to use to run the task instance
        :param session: SQLAlchemy ORM Session
        """
        self.test_mode = test_mode
        self.refresh_from_task(self.task, pool_override=pool)
        self.refresh_from_db(session=session)
        self.job_id = job_id
        self.hostname = get_hostname()
        self.pid = os.getpid()
        if not test_mode:
            session.merge(self)
            session.commit()
        actual_start_date = timezone.utcnow()
        Stats.incr(f'ti.start.{self.task.dag_id}.{self.task.task_id}')
        # Initialize final state counters at zero
        for state in State.task_states:
            Stats.incr(f'ti.finish.{self.task.dag_id}.{self.task.task_id}.{state}', count=0)
        try:
            if not mark_success:
                self.task = self.task.prepare_for_execution()
                context = self.get_template_context(ignore_param_exceptions=False)
                self._execute_task_with_callbacks(context, test_mode)
            if not test_mode:
                self.refresh_from_db(lock_for_update=True, session=session)
            self.state = State.SUCCESS
        except TaskDeferred as defer:
            # The task has signalled it wants to defer execution based on
            # a trigger.
            self._defer_task(defer=defer, session=session)
            self.log.info(
                'Pausing task as DEFERRED. dag_id=%s, task_id=%s, execution_date=%s, start_date=%s',
                self.dag_id,
                self.task_id,
                self._date_or_empty('execution_date'),
                self._date_or_empty('start_date'),
            )
            if not test_mode:
                session.add(Log(self.state, self))
                session.merge(self)
                session.commit()
            return
        except AirflowSmartSensorException as e:
            self.log.info(e)
            return
        except AirflowSkipException as e:
            # Recording SKIP
            # log only if exception has any arguments to prevent log flooding
            if e.args:
                self.log.info(e)
            if not test_mode:
                self.refresh_from_db(lock_for_update=True, session=session)
            self.state = State.SKIPPED
        except AirflowRescheduleException as reschedule_exception:
            self._handle_reschedule(actual_start_date, reschedule_exception, test_mode, session=session)
            session.commit()
            return
        except (AirflowFailException, AirflowSensorTimeout) as e:
            # If AirflowFailException is raised, task should not retry.
            # If a sensor in reschedule mode reaches timeout, task should not retry.
            self.handle_failure(e, test_mode, force_fail=True, error_file=error_file, session=session)
            session.commit()
            raise
        except AirflowException as e:
            if not test_mode:
                self.refresh_from_db(lock_for_update=True, session=session)
            # for case when task is marked as success/failed externally
            # or dagrun timed out and task is marked as skipped
            # current behavior doesn't hit the callbacks
            if self.state in State.finished:
                self.clear_next_method_args()
                session.merge(self)
                session.commit()
                return
            else:
                self.handle_failure(e, test_mode, error_file=error_file, session=session)
                session.commit()
                raise
        except (Exception, KeyboardInterrupt) as e:
            self.handle_failure(e, test_mode, error_file=error_file, session=session)
            session.commit()
            raise
        finally:
            Stats.incr(f'ti.finish.{self.dag_id}.{self.task_id}.{self.state}')

        # Recording SKIPPED or SUCCESS
        self.clear_next_method_args()
        self.end_date = timezone.utcnow()
        self._log_state()
        self.set_duration()
        if not test_mode:
            session.add(Log(self.state, self))
            session.merge(self)

            session.commit()

    def _execute_task_with_callbacks(self, context, test_mode=False):
        """Prepare Task for Execution"""
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        parent_pid = os.getpid()

        def signal_handler(signum, frame):
            pid = os.getpid()

            # If a task forks during execution (from DAG code) for whatever
            # reason, we want to make sure that we react to the signal only in
            # the process that we've spawned ourselves (referred to here as the
            # parent process).
            if pid != parent_pid:
                os._exit(1)
                return
            self.log.error("Received SIGTERM. Terminating subprocesses.")
            self.task.on_kill()
            raise AirflowException("Task received SIGTERM signal")

        signal.signal(signal.SIGTERM, signal_handler)

        # Don't clear Xcom until the task is certain to execute, and check if we are resuming from deferral.
        if not self.next_method:
            self.clear_xcom_data()

        with Stats.timer(f'dag.{self.task.dag_id}.{self.task.task_id}.duration'):
            # Set the validated/merged params on the task object.
            self.task.params = context['params']

            task_orig = self.render_templates(context=context)
            if not test_mode:
                rtif = RenderedTaskInstanceFields(ti=self, render_templates=False)
                RenderedTaskInstanceFields.write(rtif)
                RenderedTaskInstanceFields.delete_old_records(self.task_id, self.dag_id)

            # Export context to make it available for operators to use.
            airflow_context_vars = context_to_airflow_vars(context, in_env_var_format=True)
            os.environ.update(airflow_context_vars)

            # Log context only for the default execution method, the assumption
            # being that otherwise we're resuming a deferred task (in which
            # case there's no need to log these again).
            if not self.next_method:
                self.log.info(
                    "Exporting the following env vars:\n%s",
                    '\n'.join(f"{k}={v}" for k, v in airflow_context_vars.items()),
                )

            # Run pre_execute callback
            self.task.pre_execute(context=context)

            # Run on_execute callback
            self._run_execute_callback(context, self.task)

            if self.task.is_smart_sensor_compatible():
                # Try to register it in the smart sensor service.
                registered = False
                try:
                    registered = self.task.register_in_sensor_service(self, context)
                except Exception:
                    self.log.warning(
                        "Failed to register in sensor service."
                        " Continue to run task in non smart sensor mode.",
                        exc_info=True,
                    )

                if registered:
                    # Will raise AirflowSmartSensorException to avoid long running execution.
                    self._update_ti_state_for_sensing()

            # Execute the task
            with set_current_context(context):
                result = self._execute_task(context, task_orig)

            # Run post_execute callback
            self.task.post_execute(context=context, result=result)

        Stats.incr(f'operator_successes_{self.task.task_type}', 1, 1)
        Stats.incr('ti_successes')

    @provide_session
    def _update_ti_state_for_sensing(self, session=NEW_SESSION):
        self.log.info('Submitting %s to sensor service', self)
        self.state = State.SENSING
        self.start_date = timezone.utcnow()
        session.merge(self)
        session.commit()
        # Raise exception for sensing state
        raise AirflowSmartSensorException("Task successfully registered in smart sensor.")

    def _execute_task(self, context, task_orig):
        """Executes Task (optionally with a Timeout) and pushes Xcom results"""
        task_to_execute = self.task
        # If the task has been deferred and is being executed due to a trigger,
        # then we need to pick the right method to come back to, otherwise
        # we go for the default execute
        if self.next_method:
            # __fail__ is a special signal value for next_method that indicates
            # this task was scheduled specifically to fail.
            if self.next_method == "__fail__":
                next_kwargs = self.next_kwargs or {}
                traceback = self.next_kwargs.get("traceback")
                if traceback is not None:
                    self.log.error("Trigger failed:\n%s", "\n".join(traceback))
                raise TaskDeferralError(next_kwargs.get("error", "Unknown"))
            # Grab the callable off the Operator/Task and add in any kwargs
            execute_callable = getattr(task_to_execute, self.next_method)
            if self.next_kwargs:
                execute_callable = partial(execute_callable, **self.next_kwargs)
        else:
            execute_callable = task_to_execute.execute
        # If a timeout is specified for the task, make it fail
        # if it goes beyond
        if task_to_execute.execution_timeout:
            # If we are coming in with a next_method (i.e. from a deferral),
            # calculate the timeout from our start_date.
            if self.next_method:
                timeout_seconds = (
                    task_to_execute.execution_timeout - (timezone.utcnow() - self.start_date)
                ).total_seconds()
            else:
                timeout_seconds = task_to_execute.execution_timeout.total_seconds()
            try:
                # It's possible we're already timed out, so fast-fail if true
                if timeout_seconds <= 0:
                    raise AirflowTaskTimeout()
                # Run task in timeout wrapper
                with timeout(timeout_seconds):
                    result = execute_callable(context=context)
            except AirflowTaskTimeout:
                task_to_execute.on_kill()
                raise
        else:
            result = execute_callable(context=context)
        with create_session() as session:
            if task_to_execute.do_xcom_push:
                xcom_value = result
            else:
                xcom_value = None
            if xcom_value is not None:  # If the task returns a result, push an XCom containing it.
                self.xcom_push(key=XCOM_RETURN_KEY, value=xcom_value, session=session)
            self._record_task_map_for_downstreams(task_orig, xcom_value, session=session)
        return result

    @provide_session
    def _defer_task(self, session, defer: TaskDeferred):
        """
        Marks the task as deferred and sets up the trigger that is needed
        to resume it.
        """
        from airflow.models.trigger import Trigger

        # First, make the trigger entry
        trigger_row = Trigger.from_object(defer.trigger)
        session.add(trigger_row)
        session.flush()

        # Then, update ourselves so it matches the deferral request
        # Keep an eye on the logic in `check_and_change_state_before_execution()`
        # depending on self.next_method semantics
        self.state = State.DEFERRED
        self.trigger_id = trigger_row.id
        self.next_method = defer.method_name
        self.next_kwargs = defer.kwargs or {}

        # Decrement try number so the next one is the same try
        self._try_number -= 1

        # Calculate timeout too if it was passed
        if defer.timeout is not None:
            self.trigger_timeout = timezone.utcnow() + defer.timeout
        else:
            self.trigger_timeout = None

        # If an execution_timeout is set, set the timeout to the minimum of
        # it and the trigger timeout
        execution_timeout = self.task.execution_timeout
        if execution_timeout:
            if self.trigger_timeout:
                self.trigger_timeout = min(self.start_date + execution_timeout, self.trigger_timeout)
            else:
                self.trigger_timeout = self.start_date + execution_timeout

    def _run_execute_callback(self, context: Context, task):
        """Functions that need to be run before a Task is executed"""
        try:
            if task.on_execute_callback:
                task.on_execute_callback(context)
        except Exception:
            self.log.exception("Failed when executing execute callback")

    def _run_finished_callback(self, error: Optional[Union[str, Exception]] = None) -> None:
        """
        Call callback defined for finished state change.

        NOTE: Only invoke this function from caller of self._run_raw_task or
        self.run
        """
        if self.state == State.FAILED:
            task = self.task
            if task.on_failure_callback is not None:
                context = self.get_template_context()
                context["exception"] = error
                try:
                    task.on_failure_callback(context)
                except Exception:
                    self.log.exception("Error when executing on_failure_callback")
        elif self.state == State.SUCCESS:
            task = self.task
            if task.on_success_callback is not None:
                context = self.get_template_context()
                try:
                    task.on_success_callback(context)
                except Exception:
                    self.log.exception("Error when executing on_success_callback")
        elif self.state == State.UP_FOR_RETRY:
            task = self.task
            if task.on_retry_callback is not None:
                context = self.get_template_context()
                context["exception"] = error
                try:
                    task.on_retry_callback(context)
                except Exception:
                    self.log.exception("Error when executing on_retry_callback")

    @provide_session
    def run(
        self,
        verbose: bool = True,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        test_mode: bool = False,
        job_id: Optional[str] = None,
        pool: Optional[str] = None,
        session=NEW_SESSION,
    ) -> None:
        """Run TaskInstance"""
        res = self.check_and_change_state_before_execution(
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            test_mode=test_mode,
            job_id=job_id,
            pool=pool,
            session=session,
        )
        if not res:
            return

        try:
            error_fd = NamedTemporaryFile(delete=True)
            self._run_raw_task(
                mark_success=mark_success,
                test_mode=test_mode,
                job_id=job_id,
                pool=pool,
                error_file=error_fd.name,
                session=session,
            )
        finally:
            error = None if self.state == State.SUCCESS else load_error_file(error_fd)
            error_fd.close()
            self._run_finished_callback(error=error)

    def dry_run(self):
        """Only Renders Templates for the TI"""
        from airflow.models.baseoperator import BaseOperator

        self.task = self.task.prepare_for_execution()
        self.render_templates()
        assert isinstance(self.task, BaseOperator)  # For Mypy.
        self.task.dry_run()

    @provide_session
    def _handle_reschedule(
        self, actual_start_date, reschedule_exception, test_mode=False, session=NEW_SESSION
    ):
        # Don't record reschedule request in test mode
        if test_mode:
            return

        from airflow.models.dagrun import DagRun  # Avoid circular import

        self.refresh_from_db(session)

        self.end_date = timezone.utcnow()
        self.set_duration()

        # Lock DAG run to be sure not to get into a deadlock situation when trying to insert
        # TaskReschedule which apparently also creates lock on corresponding DagRun entity
        with_row_locks(
            session.query(DagRun).filter_by(
                dag_id=self.dag_id,
                run_id=self.run_id,
            ),
            session=session,
        ).one()

        # Log reschedule request
        session.add(
            TaskReschedule(
                self.task,
                self.run_id,
                self._try_number,
                actual_start_date,
                self.end_date,
                reschedule_exception.reschedule_date,
            )
        )

        # set state
        self.state = State.UP_FOR_RESCHEDULE

        # Decrement try_number so subsequent runs will use the same try number and write
        # to same log file.
        self._try_number -= 1

        self.clear_next_method_args()

        session.merge(self)
        session.commit()
        self.log.info('Rescheduling task, marking task as UP_FOR_RESCHEDULE')

    @staticmethod
    def get_truncated_error_traceback(error: BaseException, truncate_to: Callable) -> Optional[TracebackType]:
        """
        Truncates the traceback of an exception to the first frame called from within a given function

        :param error: exception to get traceback from
        :param truncate_to: Function to truncate TB to. Must have a ``__code__`` attribute

        :meta private:
        """
        tb = error.__traceback__
        code = truncate_to.__func__.__code__  # type: ignore[attr-defined]
        while tb is not None:
            if tb.tb_frame.f_code is code:
                return tb.tb_next
            tb = tb.tb_next
        return tb or error.__traceback__

    @provide_session
    def handle_failure(
        self,
        error: Union[None, str, BaseException] = None,
        test_mode: Optional[bool] = None,
        force_fail: bool = False,
        error_file: Optional[str] = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """Handle Failure for the TaskInstance"""
        if test_mode is None:
            test_mode = self.test_mode

        if error:
            if isinstance(error, BaseException):
                tb = self.get_truncated_error_traceback(error, truncate_to=self._execute_task)
                self.log.error("Task failed with exception", exc_info=(type(error), error, tb))
            else:
                self.log.error("%s", error)
            # external monitoring process provides pickle file so _run_raw_task
            # can send its runtime errors for access by failure callback
            if error_file:
                set_error_file(error_file, error)
        if not test_mode:
            self.refresh_from_db(session)

        self.end_date = timezone.utcnow()
        self.set_duration()
        Stats.incr(f'operator_failures_{self.task.task_type}')
        Stats.incr('ti_failures')
        if not test_mode:
            session.add(Log(State.FAILED, self))

            # Log failure duration
            session.add(TaskFail(ti=self))

        self.clear_next_method_args()

        # Set state correctly and figure out how to log it and decide whether
        # to email

        # Note, callback invocation needs to be handled by caller of
        # _run_raw_task to avoid race conditions which could lead to duplicate
        # invocations or miss invocation.

        # Since this function is called only when the TaskInstance state is running,
        # try_number contains the current try_number (not the next). We
        # only mark task instance as FAILED if the next task instance
        # try_number exceeds the max_tries ... or if force_fail is truthy

        task = None
        try:
            task = self.task.unmap()
        except Exception:
            self.log.error("Unable to unmap task, can't determine if we need to send an alert email or not")

        if force_fail or not self.is_eligible_to_retry():
            self.state = State.FAILED
            email_for_state = operator.attrgetter('email_on_failure')
        else:
            if self.state == State.QUEUED:
                # We increase the try_number so as to fail the task if it fails to start after sometime
                self._try_number += 1
            self.state = State.UP_FOR_RETRY
            email_for_state = operator.attrgetter('email_on_retry')

        self._log_state('Immediate failure requested. ' if force_fail else '')
        if task and email_for_state(task) and task.email:
            try:
                self.email_alert(error, task)
            except Exception:
                self.log.exception('Failed to send email to: %s', task.email)

        if not test_mode:
            session.merge(self)
            session.flush()

    @provide_session
    def handle_failure_with_callback(
        self,
        error: Union[None, str, Exception],
        test_mode: Optional[bool] = None,
        force_fail: bool = False,
        session=NEW_SESSION,
    ) -> None:
        self.handle_failure(error=error, test_mode=test_mode, force_fail=force_fail, session=session)
        self._run_finished_callback(error=error)

    def is_eligible_to_retry(self):
        """Is task instance is eligible for retry"""
        if self.state == State.RESTARTING:
            # If a task is cleared when running, it goes into RESTARTING state and is always
            # eligible for retry
            return True

        return self.task.retries and self.try_number <= self.max_tries

    def get_template_context(
        self, session: Session = NEW_SESSION, ignore_param_exceptions: bool = True
    ) -> Context:
        """Return TI Context"""
        # Do not use provide_session here -- it expunges everything on exit!
        if not session:
            session = settings.Session()

        from airflow import macros

        integrate_macros_plugins()

        task = self.task
        assert task.dag  # For Mypy.
        dag: DAG = task.dag

        dag_run = self.get_dagrun(session)
        data_interval = dag.get_run_data_interval(dag_run)

        # Validates Params and convert them into a simple dict.
        params = ParamsDict(suppress_exception=ignore_param_exceptions)
        with contextlib.suppress(AttributeError):
            params.update(dag.params)
        if task.params:
            params.update(task.params)
        if conf.getboolean('core', 'dag_run_conf_overrides_params'):
            self.overwrite_params_with_dag_run_conf(params=params, dag_run=dag_run)
        validated_params = params.validate()

        logical_date = timezone.coerce_datetime(self.execution_date)
        ds = logical_date.strftime('%Y-%m-%d')
        ds_nodash = ds.replace('-', '')
        ts = logical_date.isoformat()
        ts_nodash = logical_date.strftime('%Y%m%dT%H%M%S')
        ts_nodash_with_tz = ts.replace('-', '').replace(':', '')

        @cache  # Prevent multiple database access.
        def _get_previous_dagrun_success() -> Optional["DagRun"]:
            return self.get_previous_dagrun(state=DagRunState.SUCCESS, session=session)

        def _get_previous_dagrun_data_interval_success() -> Optional["DataInterval"]:
            dagrun = _get_previous_dagrun_success()
            if dagrun is None:
                return None
            return dag.get_run_data_interval(dagrun)

        def get_prev_data_interval_start_success() -> Optional[pendulum.DateTime]:
            data_interval = _get_previous_dagrun_data_interval_success()
            if data_interval is None:
                return None
            return data_interval.start

        def get_prev_data_interval_end_success() -> Optional[pendulum.DateTime]:
            data_interval = _get_previous_dagrun_data_interval_success()
            if data_interval is None:
                return None
            return data_interval.end

        def get_prev_start_date_success() -> Optional[pendulum.DateTime]:
            dagrun = _get_previous_dagrun_success()
            if dagrun is None:
                return None
            return timezone.coerce_datetime(dagrun.start_date)

        @cache
        def get_yesterday_ds() -> str:
            return (logical_date - timedelta(1)).strftime('%Y-%m-%d')

        def get_yesterday_ds_nodash() -> str:
            return get_yesterday_ds().replace('-', '')

        @cache
        def get_tomorrow_ds() -> str:
            return (logical_date + timedelta(1)).strftime('%Y-%m-%d')

        def get_tomorrow_ds_nodash() -> str:
            return get_tomorrow_ds().replace('-', '')

        @cache
        def get_next_execution_date() -> Optional[pendulum.DateTime]:
            # For manually triggered dagruns that aren't run on a schedule,
            # the "next" execution date doesn't make sense, and should be set
            # to execution date for consistency with how execution_date is set
            # for manually triggered tasks, i.e. triggered_date == execution_date.
            if dag_run.external_trigger:
                return logical_date
            if dag is None:
                return None
            next_info = dag.next_dagrun_info(data_interval, restricted=False)
            if next_info is None:
                return None
            return timezone.coerce_datetime(next_info.logical_date)

        def get_next_ds() -> Optional[str]:
            execution_date = get_next_execution_date()
            if execution_date is None:
                return None
            return execution_date.strftime('%Y-%m-%d')

        def get_next_ds_nodash() -> Optional[str]:
            ds = get_next_ds()
            if ds is None:
                return ds
            return ds.replace('-', '')

        @cache
        def get_prev_execution_date():
            # For manually triggered dagruns that aren't run on a schedule,
            # the "previous" execution date doesn't make sense, and should be set
            # to execution date for consistency with how execution_date is set
            # for manually triggered tasks, i.e. triggered_date == execution_date.
            if dag_run.external_trigger:
                return logical_date
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                return dag.previous_schedule(logical_date)

        @cache
        def get_prev_ds() -> Optional[str]:
            execution_date = get_prev_execution_date()
            if execution_date is None:
                return None
            return execution_date.strftime(r'%Y-%m-%d')

        def get_prev_ds_nodash() -> Optional[str]:
            prev_ds = get_prev_ds()
            if prev_ds is None:
                return None
            return prev_ds.replace('-', '')

        # NOTE: If you add anything to this dict, make sure to also update the
        # definition in airflow/utils/context.pyi, and KNOWN_CONTEXT_KEYS in
        # airflow/utils/context.py!
        context = {
            'conf': conf,
            'dag': dag,
            'dag_run': dag_run,
            'data_interval_end': timezone.coerce_datetime(data_interval.end),
            'data_interval_start': timezone.coerce_datetime(data_interval.start),
            'ds': ds,
            'ds_nodash': ds_nodash,
            'execution_date': logical_date,
            'inlets': task.inlets,
            'logical_date': logical_date,
            'macros': macros,
            'next_ds': get_next_ds(),
            'next_ds_nodash': get_next_ds_nodash(),
            'next_execution_date': get_next_execution_date(),
            'outlets': task.outlets,
            'params': validated_params,
            'prev_data_interval_start_success': get_prev_data_interval_start_success(),
            'prev_data_interval_end_success': get_prev_data_interval_end_success(),
            'prev_ds': get_prev_ds(),
            'prev_ds_nodash': get_prev_ds_nodash(),
            'prev_execution_date': get_prev_execution_date(),
            'prev_execution_date_success': self.get_previous_execution_date(
                state=DagRunState.SUCCESS,
                session=session,
            ),
            'prev_start_date_success': get_prev_start_date_success(),
            'run_id': self.run_id,
            'task': task,
            'task_instance': self,
            'task_instance_key_str': f"{task.dag_id}__{task.task_id}__{ds_nodash}",
            'test_mode': self.test_mode,
            'ti': self,
            'tomorrow_ds': get_tomorrow_ds(),
            'tomorrow_ds_nodash': get_tomorrow_ds_nodash(),
            'ts': ts,
            'ts_nodash': ts_nodash,
            'ts_nodash_with_tz': ts_nodash_with_tz,
            'var': {
                'json': VariableAccessor(deserialize_json=True),
                'value': VariableAccessor(deserialize_json=False),
            },
            'conn': ConnectionAccessor(),
            'yesterday_ds': get_yesterday_ds(),
            'yesterday_ds_nodash': get_yesterday_ds_nodash(),
        }
        # Mypy doesn't like turning existing dicts in to a TypeDict -- and we "lie" in the type stub to say it
        # is one, but in practice it isn't. See https://github.com/python/mypy/issues/8890
        return Context(context)  # type: ignore

    @provide_session
    def get_rendered_template_fields(self, session: Session = NEW_SESSION) -> None:
        """Fetch rendered template fields from DB"""
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        rendered_task_instance_fields = RenderedTaskInstanceFields.get_templated_fields(self, session=session)
        if rendered_task_instance_fields:
            self.task = self.task.unmap()
            for field_name, rendered_value in rendered_task_instance_fields.items():
                setattr(self.task, field_name, rendered_value)
            return

        try:
            # Task was never executed. Initialize RenderedTaskInstanceFields
            # to render template and mask secrets. Set MASK_SECRETS_IN_LOGS
            # to True to enable masking similar to task run.
            original_value = settings.MASK_SECRETS_IN_LOGS
            settings.MASK_SECRETS_IN_LOGS = True
            rendered_task_instance = RenderedTaskInstanceFields(self)
            rendered_fields = rendered_task_instance.rendered_fields
            if rendered_fields:
                for field_name, rendered_value in rendered_fields.items():
                    setattr(self.task, field_name, rendered_value)
        except (TemplateAssertionError, UndefinedError) as e:
            raise AirflowException(
                "Webserver does not have access to User-defined Macros or Filters "
                "when Dag Serialization is enabled. Hence for the task that have not yet "
                "started running, please use 'airflow tasks render' for debugging the "
                "rendering of template_fields."
            ) from e
        finally:
            settings.MASK_SECRETS_IN_LOGS = original_value

    @provide_session
    def get_rendered_k8s_spec(self, session=NEW_SESSION):
        """Fetch rendered template fields from DB"""
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        rendered_k8s_spec = RenderedTaskInstanceFields.get_k8s_pod_yaml(self, session=session)
        if not rendered_k8s_spec:
            try:
                rendered_k8s_spec = self.render_k8s_pod_yaml()
            except (TemplateAssertionError, UndefinedError) as e:
                raise AirflowException(f"Unable to render a k8s spec for this taskinstance: {e}") from e
        return rendered_k8s_spec

    def overwrite_params_with_dag_run_conf(self, params, dag_run):
        """Overwrite Task Params with DagRun.conf"""
        if dag_run and dag_run.conf:
            self.log.debug("Updating task params (%s) with DagRun.conf (%s)", params, dag_run.conf)
            params.update(dag_run.conf)

    def render_templates(self, context: Optional[Context] = None) -> "Operator":
        """Render templates in the operator fields.

        If the task was originally mapped, this may replace ``self.task`` with
        the unmapped, fully rendered BaseOperator. The original ``self.task``
        before replacement is returned.
        """
        if not context:
            context = self.get_template_context()
        rendered_task = self.task.render_template_fields(context)
        if rendered_task is None:  # Compatibility -- custom renderer, assume unmapped.
            return self.task
        original_task, self.task = self.task, rendered_task
        return original_task

    def render_k8s_pod_yaml(self) -> Optional[dict]:
        """Render k8s pod yaml"""
        from kubernetes.client.api_client import ApiClient

        from airflow.kubernetes.kube_config import KubeConfig
        from airflow.kubernetes.kubernetes_helper_functions import create_pod_id  # Circular import
        from airflow.kubernetes.pod_generator import PodGenerator

        kube_config = KubeConfig()
        pod = PodGenerator.construct_pod(
            dag_id=self.dag_id,
            run_id=self.run_id,
            task_id=self.task_id,
            map_index=self.map_index,
            date=None,
            pod_id=create_pod_id(self.dag_id, self.task_id),
            try_number=self.try_number,
            kube_image=kube_config.kube_image,
            args=self.command_as_list(),
            pod_override_object=PodGenerator.from_obj(self.executor_config),
            scheduler_job_id="0",
            namespace=kube_config.executor_namespace,
            base_worker_pod=PodGenerator.deserialize_model_file(kube_config.pod_template_file),
        )
        settings.pod_mutation_hook(pod)
        sanitized_pod = ApiClient().sanitize_for_serialization(pod)
        return sanitized_pod

    def get_email_subject_content(
        self, exception: BaseException, task: Optional["BaseOperator"] = None
    ) -> Tuple[str, str, str]:
        """Get the email subject content for exceptions."""
        # For a ti from DB (without ti.task), return the default value
        # Reuse it for smart sensor to send default email alert
        if task is None:
            task = getattr(self, 'task')
        use_default = task is None
        exception_html = str(exception).replace('\n', '<br>')

        default_subject = 'Airflow alert: {{ti}}'
        # For reporting purposes, we report based on 1-indexed,
        # not 0-indexed lists (i.e. Try 1 instead of
        # Try 0 for the first attempt).
        default_html_content = (
            'Try {{try_number}} out of {{max_tries + 1}}<br>'
            'Exception:<br>{{exception_html}}<br>'
            'Log: <a href="{{ti.log_url}}">Link</a><br>'
            'Host: {{ti.hostname}}<br>'
            'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
        )

        default_html_content_err = (
            'Try {{try_number}} out of {{max_tries + 1}}<br>'
            'Exception:<br>Failed attempt to attach error logs<br>'
            'Log: <a href="{{ti.log_url}}">Link</a><br>'
            'Host: {{ti.hostname}}<br>'
            'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
        )

        # This function is called after changing the state from State.RUNNING,
        # so we need to subtract 1 from self.try_number here.
        current_try_number = self.try_number - 1
        additional_context: Dict[str, Any] = {
            "exception": exception,
            "exception_html": exception_html,
            "try_number": current_try_number,
            "max_tries": self.max_tries,
        }

        if use_default:
            default_context = {"ti": self, **additional_context}
            jinja_env = jinja2.Environment(
                loader=jinja2.FileSystemLoader(os.path.dirname(__file__)), autoescape=True
            )
            subject = jinja_env.from_string(default_subject).render(**default_context)
            html_content = jinja_env.from_string(default_html_content).render(**default_context)
            html_content_err = jinja_env.from_string(default_html_content_err).render(**default_context)

        else:
            # Use the DAG's get_template_env() to set force_sandboxed. Don't add
            # the flag to the function on task object -- that function can be
            # overridden, and adding a flag breaks backward compatibility.
            dag = self.task.get_dag()
            if dag:
                jinja_env = dag.get_template_env(force_sandboxed=True)
            else:
                jinja_env = SandboxedEnvironment(cache_size=0)
            jinja_context = self.get_template_context()
            context_merge(jinja_context, additional_context)

            def render(key: str, content: str) -> str:
                if conf.has_option('email', key):
                    path = conf.get_mandatory_value('email', key)
                    with open(path) as f:
                        content = f.read()
                return render_template_to_string(jinja_env.from_string(content), jinja_context)

            subject = render('subject_template', default_subject)
            html_content = render('html_content_template', default_html_content)
            html_content_err = render('html_content_template', default_html_content_err)

        return subject, html_content, html_content_err

    def email_alert(self, exception, task: "BaseOperator"):
        """Send alert email with exception information."""
        subject, html_content, html_content_err = self.get_email_subject_content(exception, task=task)
        assert task.email
        try:
            send_email(task.email, subject, html_content)
        except Exception:
            send_email(task.email, subject, html_content_err)

    def set_duration(self) -> None:
        """Set TI duration"""
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None
        self.log.debug("Task Duration set to %s", self.duration)

    def _record_task_map_for_downstreams(self, task: "Operator", value: Any, *, session: Session) -> None:
        # TODO: We don't push TaskMap for mapped task instances because it's not
        # currently possible for a downstream to depend on one individual mapped
        # task instance, only a task as a whole. This will change in AIP-42
        # Phase 2, and we'll need to further analyze the mapped task case.
        if next(task.iter_mapped_dependants(), None) is None:
            return
        if value is None:
            raise XComForMappingNotPushed()
        if task.is_mapped:
            return
        if not isinstance(value, collections.abc.Collection) or isinstance(value, (bytes, str)):
            raise UnmappableXComTypePushed(value)
        task_map = TaskMap.from_task_instance_xcom(self, value)
        max_map_length = conf.getint("core", "max_map_length", fallback=1024)
        if task_map.length > max_map_length:
            raise UnmappableXComLengthPushed(value, max_map_length)
        session.merge(task_map)

    @provide_session
    def xcom_push(
        self,
        key: str,
        value: Any,
        execution_date: Optional[datetime] = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Make an XCom available for tasks to pull.

        :param key: Key to store the value under.
        :param value: Value to store. What types are possible depends on whether
            ``enable_xcom_pickling`` is true or not. If so, this can be any
            picklable object; only be JSON-serializable may be used otherwise.
        :param execution_date: Deprecated parameter that has no effect.
        """
        if execution_date is not None:
            self_execution_date = self.get_dagrun(session).execution_date
            if execution_date < self_execution_date:
                raise ValueError(
                    f'execution_date can not be in the past (current execution_date is '
                    f'{self_execution_date}; received {execution_date})'
                )
            elif execution_date is not None:
                message = "Passing 'execution_date' to 'TaskInstance.xcom_push()' is deprecated."
                warnings.warn(message, DeprecationWarning, stacklevel=3)

        XCom.set(
            key=key,
            value=value,
            task_id=self.task_id,
            dag_id=self.dag_id,
            run_id=self.run_id,
            map_index=self.map_index,
            session=session,
        )

    @provide_session
    def xcom_pull(
        self,
        task_ids: Optional[Union[str, Iterable[str]]] = None,
        dag_id: Optional[str] = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        session: Session = NEW_SESSION,
        *,
        map_indexes: Optional[Union[int, Iterable[int]]] = None,
        default: Any = None,
    ) -> Any:
        """Pull XComs that optionally meet certain criteria.

        :param key: A key for the XCom. If provided, only XComs with matching
            keys will be returned. The default key is ``'return_value'``, also
            available as constant ``XCOM_RETURN_KEY``. This key is automatically
            given to XComs returned by tasks (as opposed to being pushed
            manually). To remove the filter, pass *None*.
        :param task_ids: Only XComs from tasks with matching ids will be
            pulled. Pass *None* to remove the filter.
        :param dag_id: If provided, only pulls XComs from this DAG. If *None*
            (default), the DAG of the calling task is used.
        :param map_indexes: If provided, only pull XComs with matching indexes.
            If *None* (default), this is inferred from the task(s) being pulled
            (see below for details).
        :param include_prior_dates: If False, only XComs from the current
            execution_date are returned. If *True*, XComs from previous dates
            are returned as well.

        When pulling one single task (``task_id`` is *None* or a str) without
        specifying ``map_indexes``, the return value is inferred from whether
        the specified task is mapped. If not, value from the one single task
        instance is returned. If the task to pull is mapped, an iterator (not a
        list) yielding XComs from mapped task instances is returned. In either
        case, ``default`` (*None* if not specified) is returned if no matching
        XComs are found.

        When pulling multiple tasks (i.e. either ``task_id`` or ``map_index`` is
        a non-str iterable), a list of matching XComs is returned. Elements in
        the list is ordered by item ordering in ``task_id`` and ``map_index``.
        """
        if dag_id is None:
            dag_id = self.dag_id

        query = XCom.get_many(
            key=key,
            run_id=self.run_id,
            dag_ids=dag_id,
            task_ids=task_ids,
            map_indexes=map_indexes,
            include_prior_dates=include_prior_dates,
            session=session,
        )

        # NOTE: Since we're only fetching the value field and not the whole
        # class, the @recreate annotation does not kick in. Therefore we need to
        # call XCom.deserialize_value() manually.

        # We are only pulling one single task.
        if (task_ids is None or isinstance(task_ids, str)) and not isinstance(map_indexes, Iterable):
            first = query.with_entities(
                XCom.run_id, XCom.task_id, XCom.dag_id, XCom.map_index, XCom.value
            ).first()
            if first is None:  # No matching XCom at all.
                return default
            if map_indexes is not None or first.map_index < 0:
                return XCom.deserialize_value(first)

            return _LazyXComAccess.build_from_single_xcom(first, query)

        # At this point either task_ids or map_indexes is explicitly multi-value.

        results = (
            (r.task_id, r.map_index, XCom.deserialize_value(r))
            for r in query.with_entities(XCom.task_id, XCom.map_index, XCom.value)
        )

        if task_ids is None:
            task_id_pos: Dict[str, int] = defaultdict(int)
        elif isinstance(task_ids, str):
            task_id_pos = {task_ids: 0}
        else:
            task_id_pos = {task_id: i for i, task_id in enumerate(task_ids)}
        if map_indexes is None:
            map_index_pos: Dict[int, int] = defaultdict(int)
        elif isinstance(map_indexes, int):
            map_index_pos = {map_indexes: 0}
        else:
            map_index_pos = {map_index: i for i, map_index in enumerate(map_indexes)}

        def _arg_pos(item: Tuple[str, int, Any]) -> Tuple[int, int]:
            task_id, map_index, _ = item
            return task_id_pos[task_id], map_index_pos[map_index]

        results_sorted_by_arg_pos = sorted(results, key=_arg_pos)
        return [value for _, _, value in results_sorted_by_arg_pos]

    @provide_session
    def get_num_running_task_instances(self, session):
        """Return Number of running TIs from the DB"""
        # .count() is inefficient
        return (
            session.query(func.count())
            .filter(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.state == State.RUNNING,
            )
            .scalar()
        )

    def init_run_context(self, raw=False):
        """Sets the log context."""
        self.raw = raw
        self._set_context(self)

    @staticmethod
    def filter_for_tis(tis: Iterable[Union["TaskInstance", TaskInstanceKey]]) -> Optional[BooleanClauseList]:
        """Returns SQLAlchemy filter to query selected task instances"""
        # DictKeys type, (what we often pass here from the scheduler) is not directly indexable :(
        # Or it might be a generator, but we need to be able to iterate over it more than once
        tis = list(tis)

        if not tis:
            return None

        first = tis[0]

        dag_id = first.dag_id
        run_id = first.run_id
        map_index = first.map_index
        first_task_id = first.task_id
        # Common path optimisations: when all TIs are for the same dag_id and run_id, or same dag_id
        # and task_id -- this can be over 150x faster for huge numbers of TIs (20k+)
        if all(t.dag_id == dag_id and t.run_id == run_id and t.map_index == map_index for t in tis):
            return and_(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.map_index == map_index,
                TaskInstance.task_id.in_(t.task_id for t in tis),
            )
        if all(t.dag_id == dag_id and t.task_id == first_task_id and t.map_index == map_index for t in tis):
            return and_(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id.in_(t.run_id for t in tis),
                TaskInstance.map_index == map_index,
                TaskInstance.task_id == first_task_id,
            )
        if all(t.dag_id == dag_id and t.run_id == run_id and t.task_id == first_task_id for t in tis):
            return and_(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.map_index.in_(t.map_index for t in tis),
                TaskInstance.task_id == first_task_id,
            )

        return tuple_in_condition(
            (TaskInstance.dag_id, TaskInstance.task_id, TaskInstance.run_id, TaskInstance.map_index),
            (ti.key.primary for ti in tis),
        )

    @classmethod
    def ti_selector_condition(cls, vals: Collection[Union[str, Tuple[str, int]]]) -> ColumnOperators:
        """
        Build an SQLAlchemy filter for a list where each element can contain
        whether a task_id, or a tuple of (task_id,map_index)

        :meta private:
        """
        # Compute a filter for TI.task_id and TI.map_index based on input values
        # For each item, it will either be a task_id, or (task_id, map_index)
        task_id_only = [v for v in vals if isinstance(v, str)]
        with_map_index = [v for v in vals if not isinstance(v, str)]

        filters: List[ColumnOperators] = []
        if task_id_only:
            filters.append(cls.task_id.in_(task_id_only))
        if with_map_index:
            filters.append(tuple_in_condition((cls.task_id, cls.map_index), with_map_index))

        if not filters:
            return false()
        if len(filters) == 1:
            return filters[0]
        return or_(*filters)


# State of the task instance.
# Stores string version of the task state.
TaskInstanceStateType = Tuple[TaskInstanceKey, str]


class SimpleTaskInstance:
    """
    Simplified Task Instance.

    Used to send data between processes via Queues.
    """

    def __init__(
        self,
        dag_id: str,
        task_id: str,
        run_id: str,
        start_date: Optional[datetime],
        end_date: Optional[datetime],
        try_number: int,
        map_index: int,
        state: str,
        executor_config: Any,
        pool: str,
        queue: str,
        key: TaskInstanceKey,
        run_as_user: Optional[str] = None,
        priority_weight: Optional[int] = None,
    ):
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index
        self.start_date = start_date
        self.end_date = end_date
        self.try_number = try_number
        self.state = state
        self.executor_config = executor_config
        self.run_as_user = run_as_user
        self.pool = pool
        self.priority_weight = priority_weight
        self.queue = queue
        self.key = key

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    @classmethod
    def from_ti(cls, ti: TaskInstance):
        return cls(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index,
            start_date=ti.start_date,
            end_date=ti.end_date,
            try_number=ti.try_number,
            state=ti.state,
            executor_config=ti.executor_config,
            pool=ti.pool,
            queue=ti.queue,
            key=ti.key,
            run_as_user=ti.run_as_user if hasattr(ti, 'run_as_user') else None,
            priority_weight=ti.priority_weight if hasattr(ti, 'priority_weight') else None,
        )

    @classmethod
    def from_dict(cls, obj_dict: dict):
        ti_key = TaskInstanceKey(*obj_dict.pop('key'))
        start_date = None
        end_date = None
        start_date_str: Optional[str] = obj_dict.pop('start_date')
        end_date_str: Optional[str] = obj_dict.pop('end_date')
        if start_date_str:
            start_date = timezone.parse(start_date_str)
        if end_date_str:
            end_date = timezone.parse(end_date_str)
        return cls(**obj_dict, start_date=start_date, end_date=end_date, key=ti_key)


STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.jobs.base_job import BaseJob

    TaskInstance.queued_by_job = relationship(BaseJob)
