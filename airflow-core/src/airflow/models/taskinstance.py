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

import contextlib
import hashlib
import itertools
import logging
import math
import operator
import os
from collections import defaultdict
from collections.abc import Collection, Generator, Iterable, Sequence
from datetime import timedelta
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import attrs
import dill
import jinja2
import lazy_object_proxy
import uuid6
from sqlalchemy import (
    Column,
    Float,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    and_,
    case,
    delete,
    extract,
    false,
    func,
    inspect,
    or_,
    select,
    text,
    tuple_,
    update,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import lazyload, reconstructor, relationship
from sqlalchemy.orm.attributes import NO_VALUE, set_committed_value
from sqlalchemy_utils import UUIDType

from airflow import settings
from airflow.assets.manager import asset_manager
from airflow.configuration import conf
from airflow.exceptions import (
    AirflowInactiveAssetInInletOrOutletException,
    TaskDeferralError,
    TaskDeferred,
)
from airflow.listeners.listener import get_listener_manager
from airflow.models.asset import AssetEvent, AssetModel
from airflow.models.base import Base, StringID, TaskInstanceDependencies
from airflow.models.log import Log
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.models.taskmap import TaskMap
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.xcom import LazyXComSelectSequence, XComModel
from airflow.plugins_manager import integrate_macros_plugins
from airflow.settings import task_instance_mutation_hook
from airflow.stats import Stats
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.utils import timezone
from airflow.utils.email import send_email
from airflow.utils.helpers import prune_dict, render_template_to_string
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.platform import getuser
from airflow.utils.retries import run_with_db_retries
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.span_status import SpanStatus
from airflow.utils.sqlalchemy import ExecutorConfigType, ExtendedJSON, UtcDateTime
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.xcom import XCOM_RETURN_KEY

TR = TaskReschedule

log = logging.getLogger(__name__)


if TYPE_CHECKING:
    from datetime import datetime
    from pathlib import PurePath

    import pendulum
    from sqlalchemy.engine import Connection as SAConnection, Engine
    from sqlalchemy.orm.session import Session
    from sqlalchemy.sql import Update
    from sqlalchemy.sql.elements import BooleanClauseList
    from sqlalchemy.sql.expression import ColumnOperators

    from airflow.models.baseoperator import BaseOperator
    from airflow.models.dag import DAG as SchedulerDAG, DagModel
    from airflow.models.dagrun import DagRun
    from airflow.sdk.api.datamodels._generated import AssetProfile
    from airflow.sdk.definitions._internal.abstractoperator import Operator, TaskStateChangeCallback
    from airflow.sdk.definitions.asset import AssetNameRef, AssetUniqueKey, AssetUriRef
    from airflow.sdk.definitions.dag import DAG
    from airflow.sdk.definitions.taskgroup import MappedTaskGroup
    from airflow.sdk.types import RuntimeTaskInstanceProtocol
    from airflow.typing_compat import Literal
    from airflow.utils.context import Context
    from airflow.utils.task_group import TaskGroup


PAST_DEPENDS_MET = "past_depends_met"


@provide_session
def _add_log(
    event,
    task_instance=None,
    owner=None,
    owner_display_name=None,
    extra=None,
    session: Session = NEW_SESSION,
    **kwargs,
):
    session.add(
        Log(
            event,
            task_instance,
            owner,
            owner_display_name,
            extra,
            **kwargs,
        )
    )


@contextlib.contextmanager
def set_current_context(context: Context) -> Generator[Context, None, None]:
    """
    Set the current execution context to the provided context object.

    This method should be called once per Task execution, before calling operator.execute.
    """
    from airflow.sdk.definitions._internal.contextmanager import _CURRENT_CONTEXT

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


def _stop_remaining_tasks(*, task_instance: TaskInstance, task_teardown_map=None, session: Session):
    """
    Stop non-teardown tasks in dag.

    :meta private:
    """
    if not task_instance.dag_run:
        raise ValueError("``task_instance`` must have ``dag_run`` set")
    tis = task_instance.dag_run.get_task_instances(session=session)
    if TYPE_CHECKING:
        assert task_instance.task
        assert isinstance(task_instance.task.dag, DAG)

    for ti in tis:
        if ti.task_id == task_instance.task_id or ti.state in (
            TaskInstanceState.SUCCESS,
            TaskInstanceState.FAILED,
        ):
            continue
        if task_teardown_map:
            teardown = task_teardown_map[ti.task_id]
        else:
            task = task_instance.task.dag.task_dict[ti.task_id]
            teardown = task.is_teardown
        if not teardown:
            if ti.state == TaskInstanceState.RUNNING:
                log.info("Forcing task %s to fail due to dag's `fail_fast` setting", ti.task_id)
                msg = "Forcing task to fail due to dag's `fail_fast` setting."
                session.add(Log(event="fail task", extra=msg, task_instance=ti.key))
                ti.error(session)
            else:
                log.info("Setting task %s to SKIPPED due to dag's `fail_fast` setting.", ti.task_id)
                msg = "Skipping task due to dag's `fail_fast` setting."
                session.add(Log(event="skip task", extra=msg, task_instance=ti.key))
                ti.set_state(state=TaskInstanceState.SKIPPED, session=session)
        else:
            log.info("Not skipping teardown task '%s'", ti.task_id)


def clear_task_instances(
    tis: list[TaskInstance],
    session: Session,
    dag_run_state: DagRunState | Literal[False] = DagRunState.QUEUED,
) -> None:
    """
    Clear a set of task instances, but make sure the running ones get killed.

    Also sets Dagrun's `state` to QUEUED and `start_date` to the time of execution.
    But only for finished DRs (SUCCESS and FAILED).
    Doesn't clear DR's `state` and `start_date`for running
    DRs (QUEUED and RUNNING) because clearing the state for already
    running DR is redundant and clearing `start_date` affects DR's duration.

    :param tis: a list of task instances
    :param session: current session
    :param dag_run_state: state to set finished DagRuns to.
        If set to False, DagRuns state will not be changed.

    :meta private:
    """
    task_instance_ids: list[str] = []
    from airflow.jobs.scheduler_job_runner import SchedulerDagBag

    scheduler_dagbag = SchedulerDagBag()

    for ti in tis:
        task_instance_ids.append(ti.id)
        ti.prepare_db_for_next_try(session)
        if ti.state == TaskInstanceState.RUNNING:
            # If a task is cleared when running, set its state to RESTARTING so that
            # the task is terminated and becomes eligible for retry.
            ti.state = TaskInstanceState.RESTARTING
        else:
            dr = ti.dag_run
            ti_dag = scheduler_dagbag.get_dag(dag_run=dr, session=session)
            if not ti_dag:
                log.warning("No serialized dag found for dag '%s'", dr.dag_id)
            task_id = ti.task_id
            if ti_dag and ti_dag.has_task(task_id):
                task = ti_dag.get_task(task_id)
                ti.refresh_from_task(task)
                if TYPE_CHECKING:
                    assert ti.task
                ti.max_tries = ti.try_number + task.retries
            else:
                # Ignore errors when updating max_tries if the DAG or
                # task are not found since database records could be
                # outdated. We make max_tries the maximum value of its
                # original max_tries or the last attempted try number.
                ti.max_tries = max(ti.max_tries, ti.try_number)
            ti.state = None
            ti.external_executor_id = None
            ti.clear_next_method_args()
            session.merge(ti)

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
            if dr.state in State.finished_dr_states:
                dr.state = dag_run_state
                dr.start_date = timezone.utcnow()
                dr_dag = scheduler_dagbag.get_dag(dag_run=dr, session=session)
                if not dr_dag:
                    log.warning("No serialized dag found for dag '%s'", dr.dag_id)
                if dr_dag and not dr_dag.disable_bundle_versioning:
                    bundle_version = dr.dag_model.bundle_version
                    if bundle_version is not None:
                        dr.bundle_version = bundle_version
                if dag_run_state == DagRunState.QUEUED:
                    dr.last_scheduling_decision = None
                    dr.start_date = None
                    dr.clear_number += 1
    session.flush()


def _creator_note(val):
    """Creator the ``note`` association proxy."""
    if isinstance(val, str):
        return TaskInstanceNote(content=val)
    if isinstance(val, dict):
        return TaskInstanceNote(**val)
    return TaskInstanceNote(*val)


def _get_email_subject_content(
    *,
    task_instance: TaskInstance | RuntimeTaskInstanceProtocol,
    exception: BaseException,
    task: BaseOperator | None = None,
) -> tuple[str, str, str]:
    """
    Get the email subject content for exceptions.

    :param task_instance: the task instance
    :param exception: the exception sent in the email
    :param task:

    :meta private:
    """
    # For a ti from DB (without ti.task), return the default value
    if task is None:
        task = getattr(task_instance, "task")
    use_default = task is None
    exception_html = str(exception).replace("\n", "<br>")

    default_subject = "Airflow alert: {{ti}}"
    # For reporting purposes, we report based on 1-indexed,
    # not 0-indexed lists (i.e. Try 1 instead of
    # Try 0 for the first attempt).
    default_html_content = (
        "Try {{try_number}} out of {{max_tries + 1}}<br>"
        "Exception:<br>{{exception_html}}<br>"
        'Log: <a href="{{ti.log_url}}">Link</a><br>'
        "Host: {{ti.hostname}}<br>"
        'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
    )

    default_html_content_err = (
        "Try {{try_number}} out of {{max_tries + 1}}<br>"
        "Exception:<br>Failed attempt to attach error logs<br>"
        'Log: <a href="{{ti.log_url}}">Link</a><br>'
        "Host: {{ti.hostname}}<br>"
        'Mark success: <a href="{{ti.mark_success_url}}">Link</a><br>'
    )

    additional_context: dict[str, Any] = {
        "exception": exception,
        "exception_html": exception_html,
        "try_number": task_instance.try_number,
        "max_tries": task_instance.max_tries,
    }

    if use_default:
        default_context = {"ti": task_instance, **additional_context}
        jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader(os.path.dirname(__file__)), autoescape=True
        )
        subject = jinja_env.from_string(default_subject).render(**default_context)
        html_content = jinja_env.from_string(default_html_content).render(**default_context)
        html_content_err = jinja_env.from_string(default_html_content_err).render(**default_context)

    else:
        from airflow.sdk.definitions._internal.templater import SandboxedEnvironment
        from airflow.utils.context import context_merge

        if TYPE_CHECKING:
            assert task_instance.task

        # Use the DAG's get_template_env() to set force_sandboxed. Don't add
        # the flag to the function on task object -- that function can be
        # overridden, and adding a flag breaks backward compatibility.
        dag = task_instance.task.get_dag()
        if dag:
            jinja_env = dag.get_template_env(force_sandboxed=True)
        else:
            jinja_env = SandboxedEnvironment(cache_size=0)
        jinja_context = task_instance.get_template_context()
        context_merge(jinja_context, additional_context)

        def render(key: str, content: str) -> str:
            if conf.has_option("email", key):
                path = conf.get_mandatory_value("email", key)
                try:
                    with open(path) as f:
                        content = f.read()
                except FileNotFoundError:
                    log.warning("Could not find email template file '%s'. Using defaults...", path)
                except OSError:
                    log.exception("Error while using email template %s. Using defaults...", path)
            return render_template_to_string(jinja_env.from_string(content), jinja_context)

        subject = render("subject_template", default_subject)
        html_content = render("html_content_template", default_html_content)
        html_content_err = render("html_content_template", default_html_content_err)

    return subject, html_content, html_content_err


def _run_finished_callback(
    *,
    callbacks: None | TaskStateChangeCallback | Sequence[TaskStateChangeCallback],
    context: Context,
) -> None:
    """
    Run callback after task finishes.

    :param callbacks: callbacks to run
    :param context: callbacks context

    :meta private:
    """
    if callbacks:
        callbacks = callbacks if isinstance(callbacks, Sequence) else [callbacks]

        def get_callback_representation(callback: TaskStateChangeCallback) -> Any:
            with contextlib.suppress(AttributeError):
                return callback.__name__
            with contextlib.suppress(AttributeError):
                return callback.__class__.__name__
            return callback

        for idx, callback in enumerate(callbacks):
            callback_repr = get_callback_representation(callback)
            log.info("Executing callback at index %d: %s", idx, callback_repr)
            try:
                callback(context)
            except Exception:
                log.exception("Error in callback at index %d: %s", idx, callback_repr)


def _log_state(*, task_instance: TaskInstance, lead_msg: str = "") -> None:
    """
    Log task state.

    :param task_instance: the task instance
    :param lead_msg: lead message

    :meta private:
    """
    params = [
        lead_msg,
        str(task_instance.state).upper(),
        task_instance.dag_id,
        task_instance.task_id,
        task_instance.run_id,
    ]
    message = "%sMarking task as %s. dag_id=%s, task_id=%s, run_id=%s, "
    if task_instance.map_index >= 0:
        params.append(task_instance.map_index)
        message += "map_index=%d, "
    message += "logical_date=%s, start_date=%s, end_date=%s"
    log.info(
        message,
        *params,
        _date_or_empty(task_instance=task_instance, attr="logical_date"),
        _date_or_empty(task_instance=task_instance, attr="start_date"),
        _date_or_empty(task_instance=task_instance, attr="end_date"),
        stacklevel=2,
    )


def _date_or_empty(*, task_instance: TaskInstance, attr: str) -> str:
    """
    Fetch a date attribute or None of it does not exist.

    :param task_instance: the task instance
    :param attr: the attribute name

    :meta private:
    """
    result: datetime | None = getattr(task_instance, attr, None)
    return result.strftime("%Y%m%dT%H%M%S") if result else ""


def uuid7() -> str:
    """Generate a new UUID7 string."""
    return str(uuid6.uuid7())


class TaskInstance(Base, LoggingMixin):
    """
    Task instances store the state of a task instance.

    This table is the authority and single source of truth around what tasks
    have run and the state they are in.

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
    id = Column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        primary_key=True,
        default=uuid7,
        nullable=False,
    )
    task_id = Column(StringID(), nullable=False)
    dag_id = Column(StringID(), nullable=False)
    run_id = Column(StringID(), nullable=False)
    map_index = Column(Integer, nullable=False, server_default=text("-1"))

    start_date = Column(UtcDateTime)
    end_date = Column(UtcDateTime)
    duration = Column(Float)
    state = Column(String(20))
    try_number = Column(Integer, default=0)
    max_tries = Column(Integer, server_default=text("-1"))
    hostname = Column(String(1000))
    unixname = Column(String(1000))
    pool = Column(String(256), nullable=False)
    pool_slots = Column(Integer, default=1, nullable=False)
    queue = Column(String(256))
    priority_weight = Column(Integer)
    operator = Column(String(1000))
    custom_operator_name = Column(String(1000))
    queued_dttm = Column(UtcDateTime)
    scheduled_dttm = Column(UtcDateTime)
    queued_by_job_id = Column(Integer)

    last_heartbeat_at = Column(UtcDateTime)
    pid = Column(Integer)
    executor = Column(String(1000))
    executor_config = Column(ExecutorConfigType(pickler=dill))
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow)
    _rendered_map_index = Column("rendered_map_index", String(250))
    context_carrier = Column(MutableDict.as_mutable(ExtendedJSON))
    span_status = Column(String(250), server_default=SpanStatus.NOT_STARTED, nullable=False)

    external_executor_id = Column(StringID())

    # The trigger to resume on if we are in state DEFERRED
    trigger_id = Column(Integer)

    # Optional timeout utcdatetime for the trigger (past this, we'll fail)
    trigger_timeout = Column(UtcDateTime)

    # The method to call next, and any extra arguments to pass to it.
    # Usually used when resuming from DEFERRED.
    next_method = Column(String(1000))
    next_kwargs = Column(MutableDict.as_mutable(ExtendedJSON))

    _task_display_property_value = Column("task_display_name", String(2000), nullable=True)
    dag_version_id = Column(UUIDType(binary=False), ForeignKey("dag_version.id", ondelete="CASCADE"))
    dag_version = relationship("DagVersion", back_populates="task_instances")

    __table_args__ = (
        Index("ti_dag_state", dag_id, state),
        Index("ti_dag_run", dag_id, run_id),
        Index("ti_state", state),
        Index("ti_state_lkp", dag_id, task_id, run_id, state),
        Index("ti_pool", pool, state, priority_weight),
        Index("ti_trigger_id", trigger_id),
        Index("ti_heartbeat", last_heartbeat_at),
        PrimaryKeyConstraint("id", name="task_instance_pkey"),
        UniqueConstraint("dag_id", "task_id", "run_id", "map_index", name="task_instance_composite_key"),
        ForeignKeyConstraint(
            [trigger_id],
            ["trigger.id"],
            name="task_instance_trigger_id_fkey",
            ondelete="CASCADE",
        ),
        ForeignKeyConstraint(
            [dag_id, run_id],
            ["dag_run.dag_id", "dag_run.run_id"],
            name="task_instance_dag_run_fkey",
            ondelete="CASCADE",
        ),
    )

    dag_model: DagModel = relationship(
        "DagModel",
        primaryjoin="TaskInstance.dag_id == DagModel.dag_id",
        foreign_keys=dag_id,
        uselist=False,
        innerjoin=True,
        viewonly=True,
    )

    trigger = relationship("Trigger", uselist=False, back_populates="task_instance")
    triggerer_job = association_proxy("trigger", "triggerer_job")
    dag_run = relationship("DagRun", back_populates="task_instances", lazy="joined", innerjoin=True)
    rendered_task_instance_fields = relationship("RenderedTaskInstanceFields", lazy="noload", uselist=False)
    run_after = association_proxy("dag_run", "run_after")
    logical_date = association_proxy("dag_run", "logical_date")
    task_instance_note = relationship(
        "TaskInstanceNote",
        back_populates="task_instance",
        uselist=False,
        cascade="all, delete, delete-orphan",
    )
    note = association_proxy("task_instance_note", "content", creator=_creator_note)

    task: Operator | None = None
    test_mode: bool = False
    is_trigger_log_context: bool = False
    run_as_user: str | None = None
    raw: bool | None = None
    """Indicate to FileTaskHandler that logging context should be set up for trigger logging.

    :meta private:
    """
    _logger_name = "airflow.task"

    def __init__(
        self,
        task: Operator,
        run_id: str | None = None,
        state: str | None = None,
        map_index: int = -1,
        dag_version_id: UUIDType | None = None,
    ):
        super().__init__()
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.map_index = map_index
        self.dag_version_id = dag_version_id
        self.refresh_from_task(task)
        if TYPE_CHECKING:
            assert self.task

        # init_on_load will config the log
        self.init_on_load()

        self.run_id = run_id
        self.try_number = 0
        self.max_tries = self.task.retries
        if not self.id:
            self.id = uuid7()
        self.unixname = getuser()
        if state:
            self.state = state
        self.hostname = ""
        # Is this TaskInstance being currently running within `airflow tasks run --raw`.
        # Not persisted to the database so only valid for the current process
        self.raw = False
        # can be changed when calling 'run'
        self.test_mode = False
        self.context_carrier = {}

    def __hash__(self):
        return hash((self.task_id, self.dag_id, self.run_id, self.map_index))

    @property
    def stats_tags(self) -> dict[str, str]:
        """Returns task instance tags."""
        return prune_dict({"dag_id": self.dag_id, "task_id": self.task_id})

    @staticmethod
    def insert_mapping(
        run_id: str, task: Operator, map_index: int, dag_version_id: UUIDType | None
    ) -> dict[str, Any]:
        """
        Insert mapping.

        :meta private:
        """
        priority_weight = task.weight_rule.get_weight(
            TaskInstance(task=task, run_id=run_id, map_index=map_index)
        )

        return {
            "dag_id": task.dag_id,
            "task_id": task.task_id,
            "run_id": run_id,
            "try_number": 0,
            "hostname": "",
            "unixname": getuser(),
            "queue": task.queue,
            "pool": task.pool,
            "pool_slots": task.pool_slots,
            "priority_weight": priority_weight,
            "run_as_user": task.run_as_user,
            "max_tries": task.retries,
            "executor": task.executor,
            "executor_config": task.executor_config,
            "operator": task.task_type,
            "custom_operator_name": getattr(task, "custom_operator_name", None),
            "map_index": map_index,
            "_task_display_property_value": task.task_display_name,
            "dag_version_id": dag_version_id,
        }

    @reconstructor
    def init_on_load(self) -> None:
        """Initialize the attributes that aren't stored in the DB."""
        self.test_mode = False  # can be changed when calling 'run'

    @property
    def operator_name(self) -> str | None:
        """@property: use a more friendly display name for the operator, if set."""
        return self.custom_operator_name or self.operator

    @hybrid_property
    def task_display_name(self) -> str:
        return self._task_display_property_value or self.task_id

    @hybrid_property
    def rendered_map_index(self) -> str | None:
        if self._rendered_map_index is not None:
            return self._rendered_map_index
        if self.map_index >= 0:
            return str(self.map_index)
        return None

    @classmethod
    def from_runtime_ti(cls, runtime_ti: RuntimeTaskInstanceProtocol) -> TaskInstance:
        if runtime_ti.map_index is None:
            runtime_ti.map_index = -1
        ti = TaskInstance(
            run_id=runtime_ti.run_id,
            task=runtime_ti.task,  # type: ignore[arg-type]
            map_index=runtime_ti.map_index,
        )

        if TYPE_CHECKING:
            assert ti
            assert isinstance(ti, TaskInstance)
        return ti

    def to_runtime_ti(self, context_from_server) -> RuntimeTaskInstanceProtocol:
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance

        runtime_ti = RuntimeTaskInstance.model_construct(
            id=self.id,
            task_id=self.task_id,
            dag_id=self.dag_id,
            run_id=self.run_id,
            try_numer=self.try_number,
            map_index=self.map_index,
            task=self.task,
            max_tries=self.max_tries,
            hostname=self.hostname,
            _ti_context_from_server=context_from_server,
            start_date=self.start_date,
        )

        return runtime_ti

    @staticmethod
    def _command_as_list(
        ti: TaskInstance,
        mark_success: bool = False,
        ignore_all_deps: bool = False,
        ignore_task_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_ti_state: bool = False,
        local: bool = False,
        raw: bool = False,
        pool: str | None = None,
        cfg_path: str | None = None,
    ) -> list[str]:
        dag: DAG | DagModel | None
        # Use the dag if we have it, else fallback to the ORM dag_model, which might not be loaded
        if hasattr(ti, "task") and getattr(ti.task, "dag", None) is not None:
            if TYPE_CHECKING:
                assert ti.task
                assert isinstance(ti.task.dag, SchedulerDAG)
            dag = ti.task.dag
        else:
            dag = ti.dag_model

        if dag is None:
            raise ValueError("DagModel is empty")

        path = None
        if dag.relative_fileloc:
            path = Path(dag.relative_fileloc)

        if path:
            if not path.is_absolute():
                path = "DAGS_FOLDER" / path

        return TaskInstance.generate_command(
            ti.dag_id,
            ti.task_id,
            run_id=ti.run_id,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            ignore_ti_state=ignore_ti_state,
            local=local,
            file_path=path,
            raw=raw,
            pool=pool,
            cfg_path=cfg_path,
            map_index=ti.map_index,
        )

    def command_as_list(
        self,
        mark_success: bool = False,
        ignore_all_deps: bool = False,
        ignore_task_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_ti_state: bool = False,
        local: bool = False,
        raw: bool = False,
        pool: str | None = None,
        cfg_path: str | None = None,
    ) -> list[str]:
        """
        Return a command that can be executed anywhere where airflow is installed.

        This command is part of the message sent to executors by the orchestrator.
        """
        return TaskInstance._command_as_list(
            ti=self,
            mark_success=mark_success,
            ignore_all_deps=ignore_all_deps,
            ignore_task_deps=ignore_task_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            ignore_ti_state=ignore_ti_state,
            local=local,
            raw=raw,
            pool=pool,
            cfg_path=cfg_path,
        )

    @staticmethod
    def generate_command(
        dag_id: str,
        task_id: str,
        run_id: str,
        mark_success: bool = False,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        local: bool = False,
        file_path: PurePath | str | None = None,
        raw: bool = False,
        pool: str | None = None,
        cfg_path: str | None = None,
        map_index: int = -1,
    ) -> list[str]:
        """
        Generate the shell command required to execute this task instance.

        :param dag_id: DAG ID
        :param task_id: Task ID
        :param run_id: The run_id of this task's DagRun
        :param mark_success: Whether to mark the task as successful
        :param ignore_all_deps: Ignore all ignorable dependencies.
            Overrides the other ignore_* parameters.
        :param ignore_depends_on_past: Ignore depends_on_past parameter of DAGs
            (e.g. for Backfills)
        :param wait_for_past_depends_before_skipping: Wait for past depends before marking the ti as skipped
        :param ignore_task_deps: Ignore task-specific dependencies such as depends_on_past
            and trigger rule
        :param ignore_ti_state: Ignore the task instance's previous failure/success
        :param local: Whether to run the task locally
        :param file_path: path to the file containing the DAG definition
        :param raw: raw mode (needs more details)
        :param pool: the Airflow pool that the task should run in
        :param cfg_path: the Path to the configuration file
        :return: shell command that can be used to run the task instance
        """
        cmd = ["airflow", "tasks", "run", dag_id, task_id, run_id]
        if mark_success:
            cmd.extend(["--mark-success"])
        if ignore_all_deps:
            cmd.extend(["--ignore-all-dependencies"])
        if ignore_task_deps:
            cmd.extend(["--ignore-dependencies"])
        if ignore_depends_on_past:
            cmd.extend(["--depends-on-past", "ignore"])
        elif wait_for_past_depends_before_skipping:
            cmd.extend(["--depends-on-past", "wait"])
        if ignore_ti_state:
            cmd.extend(["--force"])
        if local:
            cmd.extend(["--local"])
        if pool:
            cmd.extend(["--pool", pool])
        if raw:
            cmd.extend(["--raw"])
        if file_path:
            cmd.extend(["--subdir", os.fspath(file_path)])
        if cfg_path:
            cmd.extend(["--cfg-path", cfg_path])
        if map_index != -1:
            cmd.extend(["--map-index", str(map_index)])
        return cmd

    @property
    def log_url(self) -> str:
        """Log URL for TaskInstance."""
        run_id = quote(self.run_id)
        base_url = conf.get("api", "base_url", fallback="http://localhost:8080/")
        map_index = f"/mapped/{self.map_index}" if self.map_index >= 0 else ""
        try_number = f"?try_number={self.try_number}" if self.try_number > 0 else ""
        _log_uri = f"{base_url}dags/{self.dag_id}/runs/{run_id}/tasks/{self.task_id}{map_index}{try_number}"

        return _log_uri

    @property
    def mark_success_url(self) -> str:
        """URL to mark TI success."""
        return self.log_url

    @provide_session
    def error(self, session: Session = NEW_SESSION) -> None:
        """
        Force the task instance's state to FAILED in the database.

        :param session: SQLAlchemy ORM Session
        """
        self.log.error("Recording the task instance as FAILED")
        self.state = TaskInstanceState.FAILED
        session.merge(self)
        session.commit()

    @classmethod
    @provide_session
    def get_task_instance(
        cls,
        dag_id: str,
        run_id: str,
        task_id: str,
        map_index: int,
        lock_for_update: bool = False,
        session: Session = NEW_SESSION,
    ) -> TaskInstance | None:
        query = (
            session.query(TaskInstance)
            .options(lazyload(TaskInstance.dag_run))  # lazy load dag run to avoid locking it
            .filter_by(
                dag_id=dag_id,
                run_id=run_id,
                task_id=task_id,
                map_index=map_index,
            )
        )

        if lock_for_update:
            for attempt in run_with_db_retries(logger=cls.logger()):
                with attempt:
                    return query.with_for_update().one_or_none()
        else:
            return query.one_or_none()

        return None

    @provide_session
    def refresh_from_db(
        self, session: Session = NEW_SESSION, lock_for_update: bool = False, keep_local_changes: bool = False
    ) -> None:
        """
        Refresh the task instance from the database based on the primary key.

        :param session: SQLAlchemy ORM Session
        :param lock_for_update: if True, indicates that the database should
            lock the TaskInstance (issuing a FOR UPDATE clause) until the
            session is committed.
        :param keep_local_changes: Force all attributes to the values from the database if False (the default),
            or if True don't overwrite locally set attributes
        """
        query = select(
            # Select the columns, not the ORM object, to bypass any session/ORM caching layer
            *TaskInstance.__table__.columns
        ).filter_by(
            dag_id=self.dag_id,
            run_id=self.run_id,
            task_id=self.task_id,
            map_index=self.map_index,
        )

        if lock_for_update:
            query = query.with_for_update()

        source = session.execute(query).mappings().one_or_none()
        if source:
            target_state = inspect(self)
            if target_state is None:
                raise RuntimeError(f"Unable to inspect SQLAlchemy state of {type(self)}: {self}")

            # To deal with `@hybrid_property` we need to get the names from `mapper.columns`
            for attr_name, col in target_state.mapper.columns.items():
                if keep_local_changes and target_state.attrs[attr_name].history.has_changes():
                    continue

                set_committed_value(self, attr_name, source[col.name])

            # ID may have changed, update SQLAs state and object tracking
            newkey = session.identity_key(type(self), (self.id,))

            # Delete anything under the new key
            if newkey != target_state.key:
                old = session.identity_map.get(newkey)
                if old is not self and old is not None:
                    session.expunge(old)
                target_state.key = newkey

            if target_state.attrs.dag_run.loaded_value is not NO_VALUE:
                dr_key = session.identity_key(type(self.dag_run), (self.dag_run.id,))
                if (dr := session.identity_map.get(dr_key)) is not None:
                    set_committed_value(self, "dag_run", dr)

        else:
            self.state = None

    def refresh_from_task(self, task: Operator, pool_override: str | None = None) -> None:
        """
        Copy common attributes from the given task.

        :param task: The task object to copy from
        :param pool_override: Use the pool_override instead of task's pool
        """
        self.task = task
        self.queue = task.queue
        self.pool = pool_override or task.pool
        self.pool_slots = task.pool_slots
        with contextlib.suppress(Exception):
            # This method is called from the different places, and sometimes the TI is not fully initialized
            self.priority_weight = self.task.weight_rule.get_weight(self)  # type: ignore[arg-type]
        self.run_as_user = task.run_as_user
        # Do not set max_tries to task.retries here because max_tries is a cumulative
        # value that needs to be stored in the db.
        self.executor = task.executor
        self.executor_config = task.executor_config
        self.operator = task.task_type
        self.custom_operator_name = getattr(task, "custom_operator_name", None)
        # Re-apply cluster policy here so that task default do not overload previous data
        task_instance_mutation_hook(self)

    @property
    def key(self) -> TaskInstanceKey:
        """Returns a tuple that identifies the task instance uniquely."""
        return TaskInstanceKey(self.dag_id, self.task_id, self.run_id, self.try_number, self.map_index)

    @provide_session
    def set_state(self, state: str | None, session: Session = NEW_SESSION) -> bool:
        """
        Set TaskInstance state.

        :param state: State to set for the TI
        :param session: SQLAlchemy ORM Session
        :return: Was the state changed
        """
        if self.state == state:
            return False

        current_time = timezone.utcnow()
        self.log.debug("Setting task state for %s to %s", self, state)
        if self not in session:
            self.refresh_from_db(session)
        self.state = state
        self.start_date = self.start_date or current_time
        if self.state in State.finished or self.state == TaskInstanceState.UP_FOR_RETRY:
            self.end_date = self.end_date or current_time
            self.duration = (self.end_date - self.start_date).total_seconds()
        session.merge(self)
        session.flush()
        return True

    @property
    def is_premature(self) -> bool:
        """Returns whether a task is in UP_FOR_RETRY state and its retry interval has elapsed."""
        # is the task still in the retry waiting period?
        return self.state == TaskInstanceState.UP_FOR_RETRY and not self.ready_for_retry()

    def prepare_db_for_next_try(self, session: Session):
        """Update the metadata with all the records needed to put this TI in queued for the next try."""
        from airflow.models.taskinstancehistory import TaskInstanceHistory

        TaskInstanceHistory.record_ti(self, session=session)
        session.execute(delete(TaskReschedule).filter_by(ti_id=self.id))
        self.id = uuid7()

    @provide_session
    def are_dependents_done(self, session: Session = NEW_SESSION) -> bool:
        """
        Check whether the immediate dependents of this task instance have succeeded or have been skipped.

        This is meant to be used by wait_for_downstream.

        This is useful when you do not want to start processing the next
        schedule of a task until the dependents are done. For instance,
        if the task DROPs and recreates a table.

        :param session: SQLAlchemy ORM Session
        """
        task = self.task
        if TYPE_CHECKING:
            assert task

        if not task.downstream_task_ids:
            return True

        ti = session.query(func.count(TaskInstance.task_id)).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(task.downstream_task_ids),
            TaskInstance.run_id == self.run_id,
            TaskInstance.state.in_((TaskInstanceState.SKIPPED, TaskInstanceState.SUCCESS)),
        )
        count = ti[0][0]
        return count == len(task.downstream_task_ids)

    @provide_session
    def get_previous_dagrun(
        self,
        state: DagRunState | None = None,
        session: Session | None = None,
    ) -> DagRun | None:
        """
        Return the DagRun that ran before this task instance's DagRun.

        :param state: If passed, it only take into account instances of a specific state.
        :param session: SQLAlchemy ORM Session.
        """
        if TYPE_CHECKING:
            assert self.task

        dag = self.task.dag
        if dag is None:
            return None

        if TYPE_CHECKING:
            assert isinstance(dag, SchedulerDAG)
        dr = self.get_dagrun(session=session)
        dr.dag = dag

        from airflow.models.dagrun import DagRun  # Avoid circular import

        # We always ignore schedule in dagrun lookup when `state` is given
        # or the DAG is never scheduled. For legacy reasons, when
        # `catchup=True`, we use `get_previous_scheduled_dagrun` unless
        # `ignore_schedule` is `True`.
        ignore_schedule = state is not None or not dag.timetable.can_be_scheduled
        if dag.catchup is True and not ignore_schedule:
            last_dagrun = DagRun.get_previous_scheduled_dagrun(dr.id, session=session)
        else:
            last_dagrun = DagRun.get_previous_dagrun(dag_run=dr, session=session, state=state)

        if last_dagrun:
            return last_dagrun

        return None

    @provide_session
    def get_previous_ti(
        self,
        state: DagRunState | None = None,
        session: Session = NEW_SESSION,
    ) -> TaskInstance | None:
        """
        Return the task instance for the task that ran before this task instance.

        :param session: SQLAlchemy ORM Session
        :param state: If passed, it only take into account instances of a specific state.
        """
        dagrun = self.get_previous_dagrun(state, session=session)
        if dagrun is None:
            return None
        return dagrun.get_task_instance(self.task_id, session=session)

    @provide_session
    def are_dependencies_met(
        self, dep_context: DepContext | None = None, session: Session = NEW_SESSION, verbose: bool = False
    ) -> bool:
        """
        Are all conditions met for this task instance to be run given the context for the dependencies.

        (e.g. a task instance being force run from the UI will ignore some dependencies).

        :param dep_context: The execution context that determines the dependencies that should be evaluated.
        :param session: database session
        :param verbose: whether log details on failed dependencies on info or debug log level
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

        verbose_aware_logger("Dependencies all met for dep_context=%s ti=%s", dep_context.description, self)
        return True

    @provide_session
    def get_failed_dep_statuses(self, dep_context: DepContext | None = None, session: Session = NEW_SESSION):
        """Get failed Dependencies."""
        if TYPE_CHECKING:
            assert isinstance(self.task, BaseOperator)

        if not hasattr(self.task, "deps"):
            # These deps are not on BaseOperator since they are only needed and evaluated
            # in the scheduler and not needed at the Runtime.
            from airflow.serialization.serialized_objects import SerializedBaseOperator

            serialized_op = SerializedBaseOperator.deserialize_operator(
                SerializedBaseOperator.serialize_operator(self.task)
            )
            setattr(self.task, "deps", serialized_op.deps)  # type: ignore[union-attr]

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

    def __repr__(self) -> str:
        prefix = f"<TaskInstance: {self.dag_id}.{self.task_id} {self.run_id} "
        if self.map_index != -1:
            prefix += f"map_index={self.map_index} "
        return prefix + f"[{self.state}]>"

    def next_retry_datetime(self):
        """
        Get datetime of the next retry if the task instance fails.

        For exponential backoff, retry_delay is used as base and will be converted to seconds.
        """
        from airflow.sdk.definitions._internal.abstractoperator import MAX_RETRY_DELAY

        delay = self.task.retry_delay
        if self.task.retry_exponential_backoff:
            # If the min_backoff calculation is below 1, it will be converted to 0 via int. Thus,
            # we must round up prior to converting to an int, otherwise a divide by zero error
            # will occur in the modded_hash calculation.
            # this probably gives unexpected results if a task instance has previously been cleared,
            # because try_number can increase without bound
            min_backoff = math.ceil(delay.total_seconds() * (2 ** (self.try_number - 1)))

            # In the case when delay.total_seconds() is 0, min_backoff will not be rounded up to 1.
            # To address this, we impose a lower bound of 1 on min_backoff. This effectively makes
            # the ceiling function unnecessary, but the ceiling function was retained to avoid
            # introducing a breaking change.
            if min_backoff < 1:
                min_backoff = 1

            # deterministic per task instance
            ti_hash = int(
                hashlib.sha1(
                    f"{self.dag_id}#{self.task_id}#{self.logical_date}#{self.try_number}".encode(),
                    usedforsecurity=False,
                ).hexdigest(),
                16,
            )
            # between 1 and 1.0 * delay * (2^retry_number)
            modded_hash = min_backoff + ti_hash % min_backoff
            # timedelta has a maximum representable value. The exponentiation
            # here means this value can be exceeded after a certain number
            # of tries (around 50 if the initial delay is 1s, even fewer if
            # the delay is larger). Cap the value here before creating a
            # timedelta object so the operation doesn't fail with "OverflowError".
            delay_backoff_in_seconds = min(modded_hash, MAX_RETRY_DELAY)
            delay = timedelta(seconds=delay_backoff_in_seconds)
            if self.task.max_retry_delay:
                delay = min(self.task.max_retry_delay, delay)
        return self.end_date + delay

    def ready_for_retry(self) -> bool:
        """Check on whether the task instance is in the right state and timeframe to be retried."""
        return self.state == TaskInstanceState.UP_FOR_RETRY and self.next_retry_datetime() < timezone.utcnow()

    @staticmethod
    def _get_dagrun(dag_id, run_id, session) -> DagRun:
        from airflow.models.dagrun import DagRun  # Avoid circular import

        dr = session.query(DagRun).filter(DagRun.dag_id == dag_id, DagRun.run_id == run_id).one()
        return dr

    @provide_session
    def get_dagrun(self, session: Session = NEW_SESSION) -> DagRun:
        """
        Return the DagRun for this TaskInstance.

        :param session: SQLAlchemy ORM Session
        :return: DagRun
        """
        info = inspect(self)
        if info.attrs.dag_run.loaded_value is not NO_VALUE:
            if getattr(self, "task", None) is not None:
                if TYPE_CHECKING:
                    assert self.task
                self.dag_run.dag = self.task.dag
            return self.dag_run

        dr = self._get_dagrun(self.dag_id, self.run_id, session)
        if getattr(self, "task", None) is not None:
            if TYPE_CHECKING:
                assert self.task
                assert isinstance(self.task.dag, SchedulerDAG)
            dr.dag = self.task.dag
        # Record it in the instance for next time. This means that `self.logical_date` will work correctly
        set_committed_value(self, "dag_run", dr)

        return dr

    @classmethod
    @provide_session
    def _check_and_change_state_before_execution(
        cls,
        task_instance: TaskInstance,
        verbose: bool = True,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        test_mode: bool = False,
        hostname: str = "",
        pool: str | None = None,
        external_executor_id: str | None = None,
        session: Session = NEW_SESSION,
    ) -> bool:
        """
        Check dependencies and then sets state to RUNNING if they are met.

        Returns True if and only if state is set to RUNNING, which implies that task should be
        executed, in preparation for _run_raw_task.

        :param verbose: whether to turn on more verbose logging
        :param ignore_all_deps: Ignore all of the non-critical dependencies, just runs
        :param ignore_depends_on_past: Ignore depends_on_past DAG attribute
        :param wait_for_past_depends_before_skipping: Wait for past depends before mark the ti as skipped
        :param ignore_task_deps: Don't check the dependencies of this TaskInstance's task
        :param ignore_ti_state: Disregards previous task instance state
        :param mark_success: Don't run the task, mark its state as success
        :param test_mode: Doesn't record success or failure in the DB
        :param hostname: The hostname of the worker running the task instance.
        :param pool: specifies the pool to use to run the task instance
        :param external_executor_id: The identifier of the celery executor
        :param session: SQLAlchemy ORM Session
        :return: whether the state was changed to running or not
        """
        if TYPE_CHECKING:
            assert task_instance.task

        ti: TaskInstance = task_instance
        task = task_instance.task
        if TYPE_CHECKING:
            assert task
        ti.refresh_from_task(task, pool_override=pool)
        ti.test_mode = test_mode
        ti.refresh_from_db(session=session, lock_for_update=True)
        ti.hostname = hostname
        ti.pid = None

        if not ignore_all_deps and not ignore_ti_state and ti.state == TaskInstanceState.SUCCESS:
            Stats.incr("previously_succeeded", tags=ti.stats_tags)

        if not mark_success:
            # Firstly find non-runnable and non-requeueable tis.
            # Since mark_success is not set, we do nothing.
            non_requeueable_dep_context = DepContext(
                deps=RUNNING_DEPS - REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_ti_state=ignore_ti_state,
                ignore_depends_on_past=ignore_depends_on_past,
                wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
                ignore_task_deps=ignore_task_deps,
                description="non-requeueable deps",
            )
            if not ti.are_dependencies_met(
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
            ti.start_date = ti.start_date if ti.next_method else timezone.utcnow()
            if ti.state == TaskInstanceState.UP_FOR_RESCHEDULE:
                tr_start_date = session.scalar(
                    TR.stmt_for_task_instance(ti, descending=False).with_only_columns(TR.start_date).limit(1)
                )
                if tr_start_date:
                    ti.start_date = tr_start_date

            # Secondly we find non-runnable but requeueable tis. We reset its state.
            # This is because we might have hit concurrency limits,
            # e.g. because of backfilling.
            dep_context = DepContext(
                deps=REQUEUEABLE_DEPS,
                ignore_all_deps=ignore_all_deps,
                ignore_depends_on_past=ignore_depends_on_past,
                wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
                ignore_task_deps=ignore_task_deps,
                ignore_ti_state=ignore_ti_state,
                description="requeueable deps",
            )
            if not ti.are_dependencies_met(dep_context=dep_context, session=session, verbose=True):
                ti.state = None
                cls.logger().warning(
                    "Rescheduling due to concurrency limits reached "
                    "at task runtime. Attempt %s of "
                    "%s. State set to NONE.",
                    ti.try_number,
                    ti.max_tries + 1,
                )
                ti.queued_dttm = timezone.utcnow()
                session.merge(ti)
                session.commit()
                return False

        if ti.next_kwargs is not None:
            cls.logger().info("Resuming after deferral")
        else:
            cls.logger().info("Starting attempt %s of %s", ti.try_number, ti.max_tries + 1)

        if not test_mode:
            session.add(Log(TaskInstanceState.RUNNING.value, ti))

        ti.state = TaskInstanceState.RUNNING
        ti.emit_state_change_metric(TaskInstanceState.RUNNING)

        if external_executor_id:
            ti.external_executor_id = external_executor_id

        ti.end_date = None
        if not test_mode:
            session.merge(ti).task = task
        session.commit()

        # Closing all pooled connections to prevent
        # "max number of connections reached"
        settings.engine.dispose()  # type: ignore
        if verbose:
            if mark_success:
                cls.logger().info("Marking success for %s on %s", ti.task, ti.logical_date)
            else:
                cls.logger().info("Executing %s on %s", ti.task, ti.logical_date)
        return True

    @provide_session
    def check_and_change_state_before_execution(
        self,
        verbose: bool = True,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        test_mode: bool = False,
        pool: str | None = None,
        external_executor_id: str | None = None,
        session: Session = NEW_SESSION,
    ) -> bool:
        return TaskInstance._check_and_change_state_before_execution(
            task_instance=self,
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            test_mode=test_mode,
            hostname=get_hostname(),
            pool=pool,
            external_executor_id=external_executor_id,
            session=session,
        )

    def emit_state_change_metric(self, new_state: TaskInstanceState) -> None:
        """
        Send a time metric representing how much time a given state transition took.

        The previous state and metric name is deduced from the state the task was put in.

        :param new_state: The state that has just been set for this task.
            We do not use `self.state`, because sometimes the state is updated directly in the DB and not in
            the local TaskInstance object.
            Supported states: QUEUED and RUNNING
        """
        if self.end_date:
            # if the task has an end date, it means that this is not its first round.
            # we send the state transition time metric only on the first try, otherwise it gets more complex.
            return

        # switch on state and deduce which metric to send
        if new_state == TaskInstanceState.RUNNING:
            metric_name = "queued_duration"
            if self.queued_dttm is None:
                # this should not really happen except in tests or rare cases,
                # but we don't want to create errors just for a metric, so we just skip it
                self.log.warning(
                    "cannot record %s for task %s because previous state change time has not been saved",
                    metric_name,
                    self.task_id,
                )
                return
            timing = timezone.utcnow() - self.queued_dttm
        elif new_state == TaskInstanceState.QUEUED:
            metric_name = "scheduled_duration"
            if self.scheduled_dttm is None:
                self.log.warning(
                    "cannot record %s for task %s because previous state change time has not been saved",
                    metric_name,
                    self.task_id,
                )
                return
            timing = timezone.utcnow() - self.scheduled_dttm
        else:
            raise NotImplementedError("no metric emission setup for state %s", new_state)

        # send metric twice, once (legacy) with tags in the name and once with tags as tags
        Stats.timing(f"dag.{self.dag_id}.{self.task_id}.{metric_name}", timing)
        Stats.timing(
            f"task.{metric_name}",
            timing,
            tags={"task_id": self.task_id, "dag_id": self.dag_id, "queue": self.queue},
        )

    def clear_next_method_args(self) -> None:
        """Ensure we unset next_method and next_kwargs to ensure that any retries don't reuse them."""
        log.debug("Clearing next_method and next_kwargs.")

        self.next_method = None
        self.next_kwargs = None

    @provide_session
    def _run_raw_task(
        self,
        mark_success: bool = False,
        session: Session = NEW_SESSION,
        **kwargs: Any,
    ) -> None:
        """Only kept for tests."""
        from airflow.sdk.definitions.dag import _run_task

        if mark_success:
            self.set_state(TaskInstanceState.SUCCESS)
            log.info("[DAG TEST] Marking success for %s ", self.task_id)
            return None

        taskrun_result = _run_task(ti=self)
        if taskrun_result is not None and taskrun_result.error:
            raise taskrun_result.error
        return None

    @staticmethod
    @provide_session
    def register_asset_changes_in_db(
        ti: TaskInstance,
        task_outlets: list[AssetProfile],
        outlet_events: list[dict[str, Any]],
        session: Session = NEW_SESSION,
    ) -> None:
        from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetNameRef, AssetUniqueKey, AssetUriRef

        asset_keys = {
            AssetUniqueKey(o.name, o.uri)
            for o in task_outlets
            if o.type == Asset.__name__ and o.name and o.uri
        }
        asset_name_refs = {
            Asset.ref(name=o.name) for o in task_outlets if o.type == AssetNameRef.__name__ and o.name
        }
        asset_uri_refs = {
            Asset.ref(uri=o.uri) for o in task_outlets if o.type == AssetUriRef.__name__ and o.uri
        }

        asset_models: dict[AssetUniqueKey, AssetModel] = {
            AssetUniqueKey.from_asset(am): am
            for am in session.scalars(
                select(AssetModel).where(
                    AssetModel.active.has(),
                    or_(
                        tuple_(AssetModel.name, AssetModel.uri).in_(attrs.astuple(k) for k in asset_keys),
                        AssetModel.name.in_(r.name for r in asset_name_refs),
                        AssetModel.uri.in_(r.uri for r in asset_uri_refs),
                    ),
                )
            )
        }

        asset_event_extras: dict[AssetUniqueKey, dict] = {
            AssetUniqueKey(**event["dest_asset_key"]): event["extra"]
            for event in outlet_events
            if "source_alias_name" not in event
        }

        bad_asset_keys: set[AssetUniqueKey | AssetNameRef | AssetUriRef] = set()

        for key in asset_keys:
            try:
                am = asset_models[key]
            except KeyError:
                bad_asset_keys.add(key)
                continue
            ti.log.debug("register event for asset %s", am)
            asset_manager.register_asset_change(
                task_instance=ti,
                asset=am,
                extra=asset_event_extras.get(key),
                session=session,
            )

        if asset_name_refs:
            asset_models_by_name = {key.name: am for key, am in asset_models.items()}
            asset_event_extras_by_name = {key.name: extra for key, extra in asset_event_extras.items()}
            for nref in asset_name_refs:
                try:
                    am = asset_models_by_name[nref.name]
                except KeyError:
                    bad_asset_keys.add(nref)
                    continue
                ti.log.debug("register event for asset name ref %s", am)
                asset_manager.register_asset_change(
                    task_instance=ti,
                    asset=am,
                    extra=asset_event_extras_by_name.get(nref.name),
                    session=session,
                )
        if asset_uri_refs:
            asset_models_by_uri = {key.uri: am for key, am in asset_models.items()}
            asset_event_extras_by_uri = {key.uri: extra for key, extra in asset_event_extras.items()}
            for uref in asset_uri_refs:
                try:
                    am = asset_models_by_uri[uref.uri]
                except KeyError:
                    bad_asset_keys.add(uref)
                    continue
                ti.log.debug("register event for asset uri ref %s", am)
                asset_manager.register_asset_change(
                    task_instance=ti,
                    asset=am,
                    extra=asset_event_extras_by_uri.get(uref.uri),
                    session=session,
                )

        def _asset_event_extras_from_aliases() -> dict[tuple[AssetUniqueKey, frozenset], set[str]]:
            d = defaultdict(set)
            for event in outlet_events:
                try:
                    alias_name = event["source_alias_name"]
                except KeyError:
                    continue
                if alias_name not in outlet_alias_names:
                    continue
                asset_key = AssetUniqueKey(**event["dest_asset_key"])
                extra_key = frozenset(event["extra"].items())
                d[asset_key, extra_key].add(alias_name)
            return d

        outlet_alias_names = {o.name for o in task_outlets if o.type == AssetAlias.__name__ and o.name}
        if outlet_alias_names and (event_extras_from_aliases := _asset_event_extras_from_aliases()):
            for (asset_key, extra_key), event_aliase_names in event_extras_from_aliases.items():
                ti.log.debug("register event for asset %s with aliases %s", asset_key, event_aliase_names)
                event = asset_manager.register_asset_change(
                    task_instance=ti,
                    asset=asset_key,
                    source_alias_names=event_aliase_names,
                    extra=dict(extra_key),
                    session=session,
                )
                if event is None:
                    ti.log.info("Dynamically creating AssetModel %s", asset_key)
                    session.add(AssetModel(name=asset_key.name, uri=asset_key.uri))
                    session.flush()  # So event can set up its asset fk.
                    asset_manager.register_asset_change(
                        task_instance=ti,
                        asset=asset_key,
                        source_alias_names=event_aliase_names,
                        extra=dict(extra_key),
                        session=session,
                    )

        if bad_asset_keys:
            raise AirflowInactiveAssetInInletOrOutletException(bad_asset_keys)

    @provide_session
    def update_rtif(self, rendered_fields, session: Session = NEW_SESSION):
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        rtif = RenderedTaskInstanceFields(ti=self, render_templates=False, rendered_fields=rendered_fields)
        RenderedTaskInstanceFields.write(rtif, session=session)
        session.flush()
        RenderedTaskInstanceFields.delete_old_records(self.task_id, self.dag_id, session=session)

    def update_heartbeat(self):
        with create_session() as session:
            session.execute(
                update(TaskInstance)
                .where(TaskInstance.id == self.id)
                .values(last_heartbeat_at=timezone.utcnow())
            )

    @provide_session
    def defer_task(self, exception: TaskDeferred | None, session: Session = NEW_SESSION) -> None:
        """
        Mark the task as deferred and sets up the trigger that is needed to resume it when TaskDeferred is raised.

        :meta: private
        """
        from airflow.models.trigger import Trigger

        # TODO: TaskSDK add start_trigger_args to SDK definitions
        if TYPE_CHECKING:
            assert self.task is None or isinstance(self.task, BaseOperator)

        timeout: timedelta | None
        if exception is not None:
            trigger_row = Trigger.from_object(exception.trigger)
            next_method = exception.method_name
            next_kwargs = exception.kwargs
            timeout = exception.timeout
        elif self.task is not None and self.task.start_trigger_args is not None:
            context = self.get_template_context()
            start_trigger_args = self.task.expand_start_trigger_args(context=context, session=session)
            if start_trigger_args is None:
                raise TaskDeferralError(
                    "A none 'None' start_trigger_args has been change to 'None' during expandion"
                )

            trigger_kwargs = start_trigger_args.trigger_kwargs or {}
            next_kwargs = start_trigger_args.next_kwargs
            next_method = start_trigger_args.next_method
            timeout = start_trigger_args.timeout
            trigger_row = Trigger(
                classpath=self.task.start_trigger_args.trigger_cls,
                kwargs=trigger_kwargs,
            )
        else:
            raise TaskDeferralError("exception and ti.task.start_trigger_args cannot both be None")

        # First, make the trigger entry
        session.add(trigger_row)
        session.flush()

        if TYPE_CHECKING:
            assert self.task

        # Then, update ourselves so it matches the deferral request
        # Keep an eye on the logic in `check_and_change_state_before_execution()`
        # depending on self.next_method semantics
        self.state = TaskInstanceState.DEFERRED
        self.trigger_id = trigger_row.id
        self.next_method = next_method
        self.next_kwargs = next_kwargs or {}

        # Calculate timeout too if it was passed
        if timeout is not None:
            self.trigger_timeout = timezone.utcnow() + timeout
        else:
            self.trigger_timeout = None

        # If an execution_timeout is set, set the timeout to the minimum of
        # it and the trigger timeout
        execution_timeout = self.task.execution_timeout
        if execution_timeout:
            if TYPE_CHECKING:
                assert self.start_date
            if self.trigger_timeout:
                self.trigger_timeout = min(self.start_date + execution_timeout, self.trigger_timeout)
            else:
                self.trigger_timeout = self.start_date + execution_timeout
        if self.test_mode:
            _add_log(event=self.state, task_instance=self, session=session)

        if exception is not None:
            session.merge(self)
            session.commit()

    @provide_session
    def run(
        self,
        verbose: bool = True,
        ignore_all_deps: bool = False,
        ignore_depends_on_past: bool = False,
        wait_for_past_depends_before_skipping: bool = False,
        ignore_task_deps: bool = False,
        ignore_ti_state: bool = False,
        mark_success: bool = False,
        test_mode: bool = False,
        pool: str | None = None,
        session: Session = NEW_SESSION,
        raise_on_defer: bool = False,
    ) -> None:
        """Run TaskInstance (only kept for tests)."""
        res = self.check_and_change_state_before_execution(
            verbose=verbose,
            ignore_all_deps=ignore_all_deps,
            ignore_depends_on_past=ignore_depends_on_past,
            wait_for_past_depends_before_skipping=wait_for_past_depends_before_skipping,
            ignore_task_deps=ignore_task_deps,
            ignore_ti_state=ignore_ti_state,
            mark_success=mark_success,
            test_mode=test_mode,
            pool=pool,
            session=session,
        )
        if not res:
            return

        self._run_raw_task(mark_success=mark_success)

    def dry_run(self) -> None:
        """Only Renders Templates for the TI."""
        if TYPE_CHECKING:
            assert self.task

        self.task = self.task.prepare_for_execution()
        self.render_templates()
        if TYPE_CHECKING:
            assert isinstance(self.task, BaseOperator)
        self.task.dry_run()

    @classmethod
    def fetch_handle_failure_context(
        cls,
        ti: TaskInstance,
        error: None | str | BaseException,
        test_mode: bool | None = None,
        context: Context | None = None,
        force_fail: bool = False,
        *,
        session: Session,
        fail_fast: bool = False,
    ):
        """
        Fetch the context needed to handle a failure.

        :param ti: TaskInstance
        :param error: if specified, log the specific exception if thrown
        :param test_mode: doesn't record success or failure in the DB if True
        :param context: Jinja2 context
        :param force_fail: if True, task does not retry
        :param session: SQLAlchemy ORM Session
        :param fail_fast: if True, fail all downstream tasks
        """
        if error:
            cls.logger().error("%s", error)
        if not test_mode:
            ti.refresh_from_db(session)

        ti.end_date = timezone.utcnow()
        ti.set_duration()

        Stats.incr(f"operator_failures_{ti.operator}", tags=ti.stats_tags)
        # Same metric with tagging
        Stats.incr("operator_failures", tags={**ti.stats_tags, "operator": ti.operator})
        Stats.incr("ti_failures", tags=ti.stats_tags)

        if not test_mode:
            session.add(Log(TaskInstanceState.FAILED.value, ti))

        ti.clear_next_method_args()

        # In extreme cases (task instance heartbeat timeout in case of dag with parse error) we might _not_ have a Task.
        if context is None and getattr(ti, "task", None):
            context = ti.get_template_context(session)

        if context is not None:
            context["exception"] = error

        # Set state correctly and figure out how to log it and decide whether
        # to email

        # Note, callback invocation needs to be handled by caller of
        # _run_raw_task to avoid race conditions which could lead to duplicate
        # invocations or miss invocation.

        # Since this function is called only when the TaskInstance state is running,
        # try_number contains the current try_number (not the next). We
        # only mark task instance as FAILED if the next task instance
        # try_number exceeds the max_tries ... or if force_fail is truthy

        task: BaseOperator | None = None
        try:
            if getattr(ti, "task", None) and context:
                if TYPE_CHECKING:
                    assert isinstance(ti.task, BaseOperator)
                task = ti.task.unmap((context, session))
        except Exception:
            cls.logger().error("Unable to unmap task to determine if we need to send an alert email")

        if force_fail or not ti.is_eligible_to_retry():
            ti.state = TaskInstanceState.FAILED
            email_for_state = operator.attrgetter("email_on_failure")
            callbacks = task.on_failure_callback if task else None

            if task and fail_fast:
                _stop_remaining_tasks(task_instance=ti, session=session)
        else:
            if ti.state == TaskInstanceState.RUNNING:
                # If the task instance is in the running state, it means it raised an exception and
                # about to retry so we record the task instance history. For other states, the task
                # instance was cleared and already recorded in the task instance history.
                ti.prepare_db_for_next_try(session)

            ti.state = State.UP_FOR_RETRY
            email_for_state = operator.attrgetter("email_on_retry")
            callbacks = task.on_retry_callback if task else None

        try:
            get_listener_manager().hook.on_task_instance_failed(
                previous_state=TaskInstanceState.RUNNING, task_instance=ti, error=error
            )
        except Exception:
            log.exception("error calling listener")

        return {
            "ti": ti,
            "email_for_state": email_for_state,
            "task": task,
            "callbacks": callbacks,
            "context": context,
        }

    @staticmethod
    @provide_session
    def save_to_db(ti: TaskInstance, session: Session = NEW_SESSION):
        ti.updated_at = timezone.utcnow()
        session.merge(ti)
        session.flush()
        session.commit()

    @provide_session
    def handle_failure(
        self,
        error: None | str | BaseException,
        test_mode: bool | None = None,
        context: Context | None = None,
        force_fail: bool = False,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Handle Failure for a task instance.

        :param error: if specified, log the specific exception if thrown
        :param session: SQLAlchemy ORM Session
        :param test_mode: doesn't record success or failure in the DB if True
        :param context: Jinja2 context
        :param force_fail: if True, task does not retry
        """
        if TYPE_CHECKING:
            assert self.task
            assert self.task.dag
        try:
            fail_fast = self.task.dag.fail_fast
        except Exception:
            fail_fast = False
        if test_mode is None:
            test_mode = self.test_mode
        failure_context = TaskInstance.fetch_handle_failure_context(
            ti=self,  # type: ignore[arg-type]
            error=error,
            test_mode=test_mode,
            context=context,
            force_fail=force_fail,
            session=session,
            fail_fast=fail_fast,
        )

        _log_state(task_instance=self, lead_msg="Immediate failure requested. " if force_fail else "")
        if (
            failure_context["task"]
            and failure_context["email_for_state"](failure_context["task"])
            and failure_context["task"].email
        ):
            try:
                self.email_alert(error, failure_context["task"])
            except Exception:
                log.exception("Failed to send email to: %s", failure_context["task"].email)

        if failure_context["callbacks"] and failure_context["context"]:
            _run_finished_callback(
                callbacks=failure_context["callbacks"],
                context=failure_context["context"],
            )

        if not test_mode:
            TaskInstance.save_to_db(failure_context["ti"], session)

    def is_eligible_to_retry(self) -> bool:
        """Is task instance is eligible for retry."""
        if self.state == TaskInstanceState.RESTARTING:
            # If a task is cleared when running, it goes into RESTARTING state and is always
            # eligible for retry
            return True
        if not getattr(self, "task", None):
            # Couldn't load the task, don't know number of retries, guess:
            return self.try_number <= self.max_tries

        if TYPE_CHECKING:
            assert self.task
            assert self.task.retries

        return bool(self.task.retries and self.try_number <= self.max_tries)

    def get_template_context(
        self,
        session: Session | None = None,
        ignore_param_exceptions: bool = True,
    ) -> Context:
        """
        Return TI Context.

        :param session: SQLAlchemy ORM Session
        :param ignore_param_exceptions: flag to suppress value exceptions while initializing the ParamsDict
        """
        if TYPE_CHECKING:
            assert self.task
            assert isinstance(self.task.dag, SchedulerDAG)

        # Do not use provide_session here -- it expunges everything on exit!
        if not session:
            session = settings.Session()

        from airflow import macros
        from airflow.models.abstractoperator import NotMapped
        from airflow.models.baseoperator import BaseOperator
        from airflow.sdk.api.datamodels._generated import (
            DagRun as DagRunSDK,
            PrevSuccessfulDagRunResponse,
            TIRunContext,
        )
        from airflow.sdk.definitions.param import process_params
        from airflow.sdk.execution_time.context import InletEventsAccessors
        from airflow.utils.context import (
            ConnectionAccessor,
            OutletEventAccessors,
            VariableAccessor,
        )

        integrate_macros_plugins()

        task = self.task
        if TYPE_CHECKING:
            assert self.task
            assert task
            assert task.dag
            assert session

        def _get_dagrun(session: Session) -> DagRun:
            dag_run = self.get_dagrun(session)
            if dag_run in session:
                return dag_run
            # The dag_run may not be attached to the session anymore since the
            # code base is over-zealous with use of session.expunge_all().
            # Re-attach it if the relation is not loaded so we can load it when needed.
            info = inspect(dag_run)
            if info.attrs.consumed_asset_events.loaded_value is not NO_VALUE:
                return dag_run
            # If dag_run is not flushed to db at all (e.g. CLI commands using
            # in-memory objects for ad-hoc operations), just set the value manually.
            if not info.has_identity:
                dag_run.consumed_asset_events = []
                return dag_run
            return session.merge(dag_run, load=False)

        dag_run = _get_dagrun(session)

        validated_params = process_params(
            self.task.dag, task, dag_run.conf, suppress_exception=ignore_param_exceptions
        )
        ti_context_from_server = TIRunContext(
            dag_run=DagRunSDK.model_validate(dag_run, from_attributes=True),
            max_tries=self.max_tries,
            should_retry=self.is_eligible_to_retry(),
        )
        runtime_ti = self.to_runtime_ti(context_from_server=ti_context_from_server)

        context: Context = runtime_ti.get_template_context()

        @cache  # Prevent multiple database access.
        def _get_previous_dagrun_success() -> PrevSuccessfulDagRunResponse:
            dr_from_db = self.get_previous_dagrun(state=DagRunState.SUCCESS, session=session)
            if dr_from_db:
                return PrevSuccessfulDagRunResponse.model_validate(dr_from_db, from_attributes=True)
            return PrevSuccessfulDagRunResponse()

        def get_prev_data_interval_start_success() -> pendulum.DateTime | None:
            return timezone.coerce_datetime(_get_previous_dagrun_success().data_interval_start)

        def get_prev_data_interval_end_success() -> pendulum.DateTime | None:
            return timezone.coerce_datetime(_get_previous_dagrun_success().data_interval_end)

        def get_prev_start_date_success() -> pendulum.DateTime | None:
            return timezone.coerce_datetime(_get_previous_dagrun_success().start_date)

        def get_prev_end_date_success() -> pendulum.DateTime | None:
            return timezone.coerce_datetime(_get_previous_dagrun_success().end_date)

        def get_triggering_events() -> dict[str, list[AssetEvent]]:
            asset_events = dag_run.consumed_asset_events
            triggering_events: dict[str, list[AssetEvent]] = defaultdict(list)
            for event in asset_events:
                if event.asset:
                    triggering_events[event.asset.uri].append(event)

            return triggering_events

        # NOTE: If you add to this dict, make sure to also update the following:
        # * Context in task-sdk/src/airflow/sdk/definitions/context.py
        # * KNOWN_CONTEXT_KEYS in airflow/utils/context.py
        # * Table in docs/apache-airflow/templates-ref.rst

        context.update(
            {
                "outlet_events": OutletEventAccessors(),
                "inlet_events": InletEventsAccessors(task.inlets),
                "macros": macros,
                "params": validated_params,
                "prev_data_interval_start_success": get_prev_data_interval_start_success(),
                "prev_data_interval_end_success": get_prev_data_interval_end_success(),
                "prev_start_date_success": get_prev_start_date_success(),
                "prev_end_date_success": get_prev_end_date_success(),
                "test_mode": self.test_mode,
                # ti/task_instance are added here for ti.xcom_{push,pull}
                "task_instance": self,
                "ti": self,
                "triggering_asset_events": lazy_object_proxy.Proxy(get_triggering_events),
                "var": {
                    "json": VariableAccessor(deserialize_json=True),
                    "value": VariableAccessor(deserialize_json=False),
                },
                "conn": ConnectionAccessor(),
            }
        )

        try:
            expanded_ti_count: int | None = BaseOperator.get_mapped_ti_count(
                task, self.run_id, session=session
            )
            context["expanded_ti_count"] = expanded_ti_count
            if expanded_ti_count:
                setattr(
                    self,
                    "_upstream_map_indexes",
                    {
                        upstream.task_id: self.get_relevant_upstream_map_indexes(
                            upstream,
                            expanded_ti_count,
                            session=session,
                        )
                        for upstream in task.upstream_list
                    },
                )
        except NotMapped:
            pass

        return context

    def render_templates(
        self, context: Context | None = None, jinja_env: jinja2.Environment | None = None
    ) -> Operator:
        """
        Render templates in the operator fields.

        If the task was originally mapped, this may replace ``self.task`` with
        the unmapped, fully rendered BaseOperator. The original ``self.task``
        before replacement is returned.
        """
        from airflow.sdk.definitions.mappedoperator import MappedOperator

        if not context:
            context = self.get_template_context()
        original_task = self.task

        ti = context["ti"]

        if TYPE_CHECKING:
            assert original_task
            assert self.task
            assert ti.task

        # If self.task is mapped, this call replaces self.task to point to the
        # unmapped BaseOperator created by this function! This is because the
        # MappedOperator is useless for template rendering, and we need to be
        # able to access the unmapped task instead.
        original_task.render_template_fields(context, jinja_env)
        if isinstance(self.task, MappedOperator):
            self.task = context["ti"].task  # type: ignore[assignment]

        return original_task

    def get_email_subject_content(
        self, exception: BaseException, task: BaseOperator | None = None
    ) -> tuple[str, str, str]:
        """
        Get the email subject content for exceptions.

        :param exception: the exception sent in the email
        :param task:
        """
        return _get_email_subject_content(task_instance=self, exception=exception, task=task)

    def email_alert(self, exception, task: BaseOperator) -> None:
        """
        Send alert email with exception information.

        :param exception: the exception
        :param task: task related to the exception
        """
        subject, html_content, html_content_err = self.get_email_subject_content(exception, task=task)
        if TYPE_CHECKING:
            assert task.email
        try:
            send_email(task.email, subject, html_content)
        except Exception:
            send_email(task.email, subject, html_content_err)

    def set_duration(self) -> None:
        """Set task instance duration."""
        if self.end_date and self.start_date:
            self.duration = (self.end_date - self.start_date).total_seconds()
        else:
            self.duration = None
        log.debug("Task Duration set to %s", self.duration)

    @provide_session
    def xcom_push(
        self,
        key: str,
        value: Any,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Make an XCom available for tasks to pull.

        :param key: Key to store the value under.
        :param value: Value to store. Only be JSON-serializable may be used otherwise.
        """
        XComModel.set(
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
        task_ids: str | Iterable[str] | None = None,
        dag_id: str | None = None,
        key: str = XCOM_RETURN_KEY,
        include_prior_dates: bool = False,
        session: Session = NEW_SESSION,
        *,
        map_indexes: int | Iterable[int] | None = None,
        default: Any = None,
        run_id: str | None = None,
    ) -> Any:
        """:meta private:"""  # noqa: D400
        # This is only kept for compatibility in tests for now while AIP-72 is in progress.
        if dag_id is None:
            dag_id = self.dag_id
        if run_id is None:
            run_id = self.run_id

        query = XComModel.get_many(
            key=key,
            run_id=run_id,
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
                XComModel.run_id, XComModel.task_id, XComModel.dag_id, XComModel.map_index, XComModel.value
            ).first()
            if first is None:  # No matching XCom at all.
                return default
            if map_indexes is not None or first.map_index < 0:
                return XComModel.deserialize_value(first)

            # raise RuntimeError("Nothing should hit this anymore")

        # TODO: TaskSDK: We should remove this, but many tests still currently call `ti.run()`. See #45549

        # At this point either task_ids or map_indexes is explicitly multi-value.
        # Order return values to match task_ids and map_indexes ordering.
        ordering = []
        if task_ids is None or isinstance(task_ids, str):
            ordering.append(XComModel.task_id)
        elif task_id_whens := {tid: i for i, tid in enumerate(task_ids)}:
            ordering.append(case(task_id_whens, value=XComModel.task_id))
        else:
            ordering.append(XComModel.task_id)
        if map_indexes is None or isinstance(map_indexes, int):
            ordering.append(XComModel.map_index)
        elif isinstance(map_indexes, range):
            order = XComModel.map_index
            if map_indexes.step < 0:
                order = order.desc()
            ordering.append(order)
        elif map_index_whens := {map_index: i for i, map_index in enumerate(map_indexes)}:
            ordering.append(case(map_index_whens, value=XComModel.map_index))
        else:
            ordering.append(XComModel.map_index)
        return LazyXComSelectSequence.from_select(
            query.with_entities(XComModel.value).order_by(None).statement,
            order_by=ordering,
            session=session,
        )

    @provide_session
    def get_num_running_task_instances(self, session: Session, same_dagrun: bool = False) -> int:
        """Return Number of running TIs from the DB."""
        # .count() is inefficient
        num_running_task_instances_query = session.query(func.count()).filter(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id == self.task_id,
            TaskInstance.state == TaskInstanceState.RUNNING,
        )
        if same_dagrun:
            num_running_task_instances_query = num_running_task_instances_query.filter(
                TaskInstance.run_id == self.run_id
            )
        return num_running_task_instances_query.scalar()

    @staticmethod
    def filter_for_tis(tis: Iterable[TaskInstance | TaskInstanceKey]) -> BooleanClauseList | None:
        """Return SQLAlchemy filter to query selected task instances."""
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

        # pre-compute the set of dag_id, run_id, map_indices and task_ids
        dag_ids, run_ids, map_indices, task_ids = set(), set(), set(), set()
        for t in tis:
            dag_ids.add(t.dag_id)
            run_ids.add(t.run_id)
            map_indices.add(t.map_index)
            task_ids.add(t.task_id)

        # Common path optimisations: when all TIs are for the same dag_id and run_id, or same dag_id
        # and task_id -- this can be over 150x faster for huge numbers of TIs (20k+)
        if dag_ids == {dag_id} and run_ids == {run_id} and map_indices == {map_index}:
            return and_(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.map_index == map_index,
                TaskInstance.task_id.in_(task_ids),
            )
        if dag_ids == {dag_id} and task_ids == {first_task_id} and map_indices == {map_index}:
            return and_(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id.in_(run_ids),
                TaskInstance.map_index == map_index,
                TaskInstance.task_id == first_task_id,
            )
        if dag_ids == {dag_id} and run_ids == {run_id} and task_ids == {first_task_id}:
            return and_(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.map_index.in_(map_indices),
                TaskInstance.task_id == first_task_id,
            )

        filter_condition = []
        # create 2 nested groups, both primarily grouped by dag_id and run_id,
        # and in the nested group 1 grouped by task_id the other by map_index.
        task_id_groups: dict[tuple, dict[Any, list[Any]]] = defaultdict(lambda: defaultdict(list))
        map_index_groups: dict[tuple, dict[Any, list[Any]]] = defaultdict(lambda: defaultdict(list))
        for t in tis:
            task_id_groups[(t.dag_id, t.run_id)][t.task_id].append(t.map_index)
            map_index_groups[(t.dag_id, t.run_id)][t.map_index].append(t.task_id)

        # this assumes that most dags have dag_id as the largest grouping, followed by run_id. even
        # if its not, this is still  a significant optimization over querying for every single tuple key
        for cur_dag_id, cur_run_id in itertools.product(dag_ids, run_ids):
            # we compare the group size between task_id and map_index and use the smaller group
            dag_task_id_groups = task_id_groups[(cur_dag_id, cur_run_id)]
            dag_map_index_groups = map_index_groups[(cur_dag_id, cur_run_id)]

            if len(dag_task_id_groups) <= len(dag_map_index_groups):
                for cur_task_id, cur_map_indices in dag_task_id_groups.items():
                    filter_condition.append(
                        and_(
                            TaskInstance.dag_id == cur_dag_id,
                            TaskInstance.run_id == cur_run_id,
                            TaskInstance.task_id == cur_task_id,
                            TaskInstance.map_index.in_(cur_map_indices),
                        )
                    )
            else:
                for cur_map_index, cur_task_ids in dag_map_index_groups.items():
                    filter_condition.append(
                        and_(
                            TaskInstance.dag_id == cur_dag_id,
                            TaskInstance.run_id == cur_run_id,
                            TaskInstance.task_id.in_(cur_task_ids),
                            TaskInstance.map_index == cur_map_index,
                        )
                    )

        return or_(*filter_condition)

    @classmethod
    def ti_selector_condition(cls, vals: Collection[str | tuple[str, int]]) -> ColumnOperators:
        """
        Build an SQLAlchemy filter for a list of task_ids or tuples of (task_id,map_index).

        :meta private:
        """
        # Compute a filter for TI.task_id and TI.map_index based on input values
        # For each item, it will either be a task_id, or (task_id, map_index)
        task_id_only = [v for v in vals if isinstance(v, str)]
        with_map_index = [v for v in vals if not isinstance(v, str)]

        filters: list[ColumnOperators] = []
        if task_id_only:
            filters.append(cls.task_id.in_(task_id_only))
        if with_map_index:
            filters.append(tuple_(cls.task_id, cls.map_index).in_(with_map_index))

        if not filters:
            return false()
        if len(filters) == 1:
            return filters[0]
        return or_(*filters)

    def get_relevant_upstream_map_indexes(
        self,
        upstream: Operator,
        ti_count: int | None,
        *,
        session: Session,
    ) -> int | range | None:
        """
        Infer the map indexes of an upstream "relevant" to this ti.

        The bulk of the logic mainly exists to solve the problem described by
        the following example, where 'val' must resolve to different values,
        depending on where the reference is being used::

            @task
            def this_task(v):  # This is self.task.
                return v * 2


            @task_group
            def tg1(inp):
                val = upstream(inp)  # This is the upstream task.
                this_task(val)  # When inp is 1, val here should resolve to 2.
                return val


            # This val is the same object returned by tg1.
            val = tg1.expand(inp=[1, 2, 3])


            @task_group
            def tg2(inp):
                another_task(inp, val)  # val here should resolve to [2, 4, 6].


            tg2.expand(inp=["a", "b"])

        The surrounding mapped task groups of ``upstream`` and ``self.task`` are
        inspected to find a common "ancestor". If such an ancestor is found,
        we need to return specific map indexes to pull a partial value from
        upstream XCom.

        :param upstream: The referenced upstream task.
        :param ti_count: The total count of task instance this task was expanded
            by the scheduler, i.e. ``expanded_ti_count`` in the template context.
        :return: Specific map index or map indexes to pull, or ``None`` if we
            want to "whole" return value (i.e. no mapped task groups involved).
        """
        from airflow.models.baseoperator import BaseOperator

        if TYPE_CHECKING:
            assert self.task

        # This value should never be None since we already know the current task
        # is in a mapped task group, and should have been expanded, despite that,
        # we need to check that it is not None to satisfy Mypy.
        # But this value can be 0 when we expand an empty list, for that it is
        # necessary to check that ti_count is not 0 to avoid dividing by 0.
        if not ti_count:
            return None

        # Find the innermost common mapped task group between the current task
        # If the current task and the referenced task does not have a common
        # mapped task group, the two are in different task mapping contexts
        # (like another_task above), and we should use the "whole" value.
        common_ancestor = _find_common_ancestor_mapped_group(self.task, upstream)
        if common_ancestor is None:
            return None

        # At this point we know the two tasks share a mapped task group, and we
        # should use a "partial" value. Let's break down the mapped ti count
        # between the ancestor and further expansion happened inside it.

        ancestor_ti_count = BaseOperator.get_mapped_ti_count(common_ancestor, self.run_id, session=session)
        ancestor_map_index = self.map_index * ancestor_ti_count // ti_count

        # If the task is NOT further expanded inside the common ancestor, we
        # only want to reference one single ti. We must walk the actual DAG,
        # and "ti_count == ancestor_ti_count" does not work, since the further
        # expansion may be of length 1.
        if not _is_further_mapped_inside(upstream, common_ancestor):
            return ancestor_map_index

        # Otherwise we need a partial aggregation for values from selected task
        # instances in the ancestor's expansion context.
        further_count = ti_count // ancestor_ti_count
        map_index_start = ancestor_map_index * further_count
        return range(map_index_start, map_index_start + further_count)

    def clear_db_references(self, session: Session):
        """
        Clear db tables that have a reference to this instance.

        :param session: ORM Session

        :meta private:
        """
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        tables: list[type[TaskInstanceDependencies]] = [
            XComModel,
            RenderedTaskInstanceFields,
            TaskMap,
        ]
        tables_by_id: list[type[Base]] = [TaskInstanceNote, TaskReschedule]
        for table in tables:
            session.execute(
                delete(table).where(
                    table.dag_id == self.dag_id,
                    table.task_id == self.task_id,
                    table.run_id == self.run_id,
                    table.map_index == self.map_index,
                )
            )
        for table in tables_by_id:
            session.execute(delete(table).where(table.ti_id == self.id))

    @classmethod
    def duration_expression_update(
        cls, end_date: datetime, query: Update, bind: Engine | SAConnection
    ) -> Update:
        """Return a SQL expression for calculating the duration of this TI, based on the start and end date columns."""
        # TODO: Compare it with self._set_duration method

        if bind.dialect.name == "sqlite":
            return query.values(
                {
                    "end_date": end_date,
                    "duration": (
                        (func.strftime("%s", end_date) - func.strftime("%s", cls.start_date))
                        + func.round((func.strftime("%f", end_date) - func.strftime("%f", cls.start_date)), 3)
                    ),
                }
            )
        if bind.dialect.name == "postgresql":
            return query.values(
                {
                    "end_date": end_date,
                    "duration": extract("EPOCH", end_date - cls.start_date),
                }
            )

        return query.values(
            {
                "end_date": end_date,
                "duration": (
                    func.timestampdiff(text("MICROSECOND"), cls.start_date, end_date)
                    # Turn microseconds into floating point seconds.
                    / 1_000_000
                ),
            }
        )


def _find_common_ancestor_mapped_group(node1: Operator, node2: Operator) -> MappedTaskGroup | None:
    """Given two operators, find their innermost common mapped task group."""
    if node1.dag is None or node2.dag is None or node1.dag_id != node2.dag_id:
        return None
    parent_group_ids = {g.group_id for g in node1.iter_mapped_task_groups()}
    common_groups = (g for g in node2.iter_mapped_task_groups() if g.group_id in parent_group_ids)
    return next(common_groups, None)


def _is_further_mapped_inside(operator: Operator, container: TaskGroup) -> bool:
    """Whether given operator is *further* mapped inside a task group."""
    from airflow.sdk.definitions.mappedoperator import MappedOperator
    from airflow.sdk.definitions.taskgroup import MappedTaskGroup

    if isinstance(operator, MappedOperator):
        return True
    task_group = operator.task_group
    while task_group is not None and task_group.group_id != container.group_id:
        if isinstance(task_group, MappedTaskGroup):
            return True
        task_group = task_group.parent_group
    return False


# State of the task instance.
# Stores string version of the task state.
TaskInstanceStateType = tuple[TaskInstanceKey, TaskInstanceState]


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
        queued_dttm: datetime | None,
        start_date: datetime | None,
        end_date: datetime | None,
        try_number: int,
        map_index: int,
        state: str,
        executor: str | None,
        executor_config: Any,
        pool: str,
        queue: str,
        key: TaskInstanceKey,
        run_as_user: str | None = None,
        priority_weight: int | None = None,
        parent_context_carrier: dict | None = None,
        context_carrier: dict | None = None,
        span_status: str | None = None,
    ):
        self.dag_id = dag_id
        self.task_id = task_id
        self.run_id = run_id
        self.map_index = map_index
        self.queued_dttm = queued_dttm
        self.start_date = start_date
        self.end_date = end_date
        self.try_number = try_number
        self.state = state
        self.executor = executor
        self.executor_config = executor_config
        self.run_as_user = run_as_user
        self.pool = pool
        self.priority_weight = priority_weight
        self.queue = queue
        self.key = key
        self.parent_context_carrier = parent_context_carrier
        self.context_carrier = context_carrier
        self.span_status = span_status

    def __repr__(self) -> str:
        attrs = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items())
        return f"SimpleTaskInstance({attrs})"

    def __eq__(self, other) -> bool:
        if isinstance(other, self.__class__):
            return self.__dict__ == other.__dict__
        return NotImplemented

    @classmethod
    def from_ti(cls, ti: TaskInstance) -> SimpleTaskInstance:
        return cls(
            dag_id=ti.dag_id,
            task_id=ti.task_id,
            run_id=ti.run_id,
            map_index=ti.map_index,
            queued_dttm=ti.queued_dttm,
            start_date=ti.start_date,
            end_date=ti.end_date,
            try_number=ti.try_number,
            state=ti.state,
            executor=ti.executor,
            executor_config=ti.executor_config,
            pool=ti.pool,
            queue=ti.queue,
            key=ti.key,
            run_as_user=ti.run_as_user if hasattr(ti, "run_as_user") else None,
            priority_weight=ti.priority_weight if hasattr(ti, "priority_weight") else None,
            # Inspect the ti, to check if the 'dag_run' relationship is loaded.
            parent_context_carrier=ti.dag_run.context_carrier
            if "dag_run" not in inspect(ti).unloaded
            else None,
            context_carrier=ti.context_carrier if hasattr(ti, "context_carrier") else None,
            span_status=ti.span_status,
        )


class TaskInstanceNote(Base):
    """For storage of arbitrary notes concerning the task instance."""

    __tablename__ = "task_instance_note"
    ti_id = Column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        primary_key=True,
        nullable=False,
    )
    user_id = Column(String(128), nullable=True)
    content = Column(String(1000).with_variant(Text(1000), "mysql"))
    created_at = Column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at = Column(UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False)

    task_instance = relationship("TaskInstance", back_populates="task_instance_note", uselist=False)

    __table_args__ = (
        ForeignKeyConstraint(
            (ti_id,),
            [
                "task_instance.id",
            ],
            name="task_instance_note_ti_fkey",
            ondelete="CASCADE",
            onupdate="CASCADE",
        ),
    )

    def __init__(self, content, user_id=None):
        self.content = content
        self.user_id = user_id

    def __repr__(self):
        prefix = f"<{self.__class__.__name__}: {self.task_instance.dag_id}.{self.task_instance.task_id} {self.task_instance.run_id}"
        if self.task_instance.map_index != -1:
            prefix += f" map_index={self.task_instance.map_index}"
        return prefix + f" TI ID: {self.ti_id}>"


STATICA_HACK = True
globals()["kcah_acitats"[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from airflow.jobs.job import Job

    TaskInstance.queued_by_job = relationship(Job)
