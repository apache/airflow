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
import json
import logging
import math
import uuid
from collections import defaultdict
from collections.abc import Collection, Iterable
from datetime import datetime, timedelta
from functools import cache
from typing import TYPE_CHECKING, Any
from urllib.parse import quote

import attrs
import dill
import lazy_object_proxy
import uuid6
from sqlalchemy import (
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
from sqlalchemy.orm import Mapped, lazyload, reconstructor, relationship
from sqlalchemy.orm.attributes import NO_VALUE, set_committed_value
from sqlalchemy_utils import UUIDType

from airflow import settings
from airflow._shared.timezones import timezone
from airflow.assets.manager import asset_manager
from airflow.configuration import conf
from airflow.listeners.listener import get_listener_manager
from airflow.models.asset import AssetEvent, AssetModel
from airflow.models.base import Base, StringID, TaskInstanceDependencies
from airflow.models.dag_version import DagVersion

# Import HITLDetail at runtime so SQLAlchemy can resolve the relationship
from airflow.models.hitl import HITLDetail  # noqa: F401
from airflow.models.log import Log
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.models.taskmap import TaskMap
from airflow.models.taskreschedule import TaskReschedule
from airflow.models.xcom import XCOM_RETURN_KEY, LazyXComSelectSequence, XComModel
from airflow.observability.stats import Stats
from airflow.settings import task_instance_mutation_hook
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_deps import REQUEUEABLE_DEPS, RUNNING_DEPS
from airflow.utils.helpers import prune_dict
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.net import get_hostname
from airflow.utils.platform import getuser
from airflow.utils.retries import run_with_db_retries
from airflow.utils.session import NEW_SESSION, create_session, provide_session
from airflow.utils.span_status import SpanStatus
from airflow.utils.sqlalchemy import ExecutorConfigType, ExtendedJSON, UtcDateTime, mapped_column
from airflow.utils.state import DagRunState, State, TaskInstanceState

TR = TaskReschedule

log = logging.getLogger(__name__)


if TYPE_CHECKING:
    from datetime import datetime
    from typing import Literal

    import pendulum
    from sqlalchemy.engine import Connection as SAConnection, Engine
    from sqlalchemy.orm.session import Session
    from sqlalchemy.sql import Update
    from sqlalchemy.sql.elements import ColumnElement

    from airflow.api_fastapi.execution_api.datamodels.asset import AssetProfile
    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.sdk import Context
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.serialization.definitions.mappedoperator import Operator
    from airflow.serialization.definitions.taskgroup import SerializedTaskGroup


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
        assert task_instance.task.dag

    for ti in tis:
        if ti.task_id == task_instance.task_id or ti.state in (
            TaskInstanceState.SUCCESS,
            TaskInstanceState.FAILED,
        ):
            continue
        if task_teardown_map:
            teardown = task_teardown_map[ti.task_id]
        else:
            teardown = task_instance.task.dag.task_dict[ti.task_id].is_teardown
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
    run_on_latest_version: bool = False,
    prevent_running_task: bool | None = None,
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
    :param run_on_latest_version: whether to run on latest serialized DAG and Bundle version

    :meta private:
    """
    task_instance_ids: list[str] = []
    from airflow.exceptions import AirflowClearRunningTaskException
    from airflow.models.dagbag import DBDagBag

    scheduler_dagbag = DBDagBag(load_op_links=False)
    for ti in tis:
        task_instance_ids.append(ti.id)
        ti.prepare_db_for_next_try(session)

        if ti.state == TaskInstanceState.RUNNING:
            if prevent_running_task:
                raise AirflowClearRunningTaskException(
                    "AirflowClearRunningTaskException: Disable 'prevent_running_task' to proceed, or wait until the task is not running, queued, or scheduled state."
                )
                # Prevents the task from re-running and clearing when prevent_running_task from the frontend and the tas is running is True.

            ti.state = TaskInstanceState.RESTARTING
        # If a task is cleared when running and the prevent_running_task is false,
        # set its state to RESTARTING so that
        # the task is terminated and becomes eligible for retry.
        else:
            dr = ti.dag_run
            if run_on_latest_version:
                ti_dag = scheduler_dagbag.get_latest_version_of_dag(ti.dag_id, session=session)
            else:
                ti_dag = scheduler_dagbag.get_dag_for_run(dag_run=dr, session=session)
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

        drs = session.scalars(
            select(DagRun).where(
                or_(
                    *(
                        and_(DagRun.dag_id == dag_id, DagRun.run_id.in_(run_ids))
                        for dag_id, run_ids in run_ids_by_dag_id.items()
                    )
                )
            )
        ).all()
        dag_run_state = DagRunState(dag_run_state)  # Validate the state value.
        for dr in drs:
            if dr.state in State.finished_dr_states:
                dr.state = dag_run_state
                dr.start_date = timezone.utcnow()
                if run_on_latest_version:
                    dr_dag = scheduler_dagbag.get_latest_version_of_dag(dr.dag_id, session=session)
                    dag_version = DagVersion.get_latest_version(dr.dag_id, session=session)
                    if dag_version:
                        # Change the dr.created_dag_version_id so the scheduler doesn't reject this
                        # version when it sets the dag_run.dag
                        dr.created_dag_version_id = dag_version.id
                        dr.dag = dr_dag
                        dr.verify_integrity(session=session, dag_version_id=dag_version.id)
                        for ti in dr.task_instances:
                            ti.dag_version_id = dag_version.id
                else:
                    dr_dag = scheduler_dagbag.get_dag_for_run(dag_run=dr, session=session)
                if not dr_dag:
                    log.warning("No serialized dag found for dag '%s'", dr.dag_id)
                if dr_dag and not dr_dag.disable_bundle_versioning and run_on_latest_version:
                    bundle_version = dr.dag_model.bundle_version
                    if bundle_version is not None and run_on_latest_version:
                        dr.bundle_version = bundle_version
                if dag_run_state == DagRunState.QUEUED:
                    dr.last_scheduling_decision = None
                    dr.start_date = None
                    dr.clear_number += 1
                    dr.queued_at = timezone.utcnow()
    session.flush()


def _creator_note(val):
    """Creator for the ``note`` association proxy."""
    if isinstance(val, str):
        return TaskInstanceNote(content=val)
    if isinstance(val, dict):
        return TaskInstanceNote(**val)
    return TaskInstanceNote(*val)


def _log_state(*, task_instance: TaskInstance, lead_msg: str = "") -> None:
    """
    Log task state.

    :param task_instance: the task instance
    :param lead_msg: lead message

    :meta private:
    """
    params: list[str | int] = [
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
    id: Mapped[str] = mapped_column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        primary_key=True,
        default=uuid7,
        nullable=False,
    )
    task_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    run_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    map_index: Mapped[int] = mapped_column(Integer, nullable=False, server_default=text("-1"))

    start_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    end_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    duration: Mapped[float | None] = mapped_column(Float, nullable=True)
    state: Mapped[str | None] = mapped_column(String(20), nullable=True)
    try_number: Mapped[int] = mapped_column(Integer, default=0)
    max_tries: Mapped[int] = mapped_column(Integer, server_default=text("-1"))
    hostname: Mapped[str] = mapped_column(String(1000))
    unixname: Mapped[str] = mapped_column(String(1000))
    pool: Mapped[str] = mapped_column(String(256), nullable=False)
    pool_slots: Mapped[int] = mapped_column(Integer, default=1, nullable=False)
    queue: Mapped[str] = mapped_column(String(256))
    priority_weight: Mapped[int] = mapped_column(Integer)
    operator: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    custom_operator_name: Mapped[str] = mapped_column(String(1000))
    queued_dttm: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    scheduled_dttm: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    queued_by_job_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    last_heartbeat_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    pid: Mapped[int | None] = mapped_column(Integer, nullable=True)
    executor: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    executor_config: Mapped[dict] = mapped_column(ExecutorConfigType(pickler=dill))
    updated_at: Mapped[datetime | None] = mapped_column(
        UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=True
    )
    _rendered_map_index: Mapped[str | None] = mapped_column("rendered_map_index", String(250), nullable=True)
    context_carrier: Mapped[dict | None] = mapped_column(MutableDict.as_mutable(ExtendedJSON), nullable=True)
    span_status: Mapped[str] = mapped_column(
        String(250), server_default=SpanStatus.NOT_STARTED, nullable=False
    )

    external_executor_id: Mapped[str | None] = mapped_column(StringID(), nullable=True)

    # The trigger to resume on if we are in state DEFERRED
    trigger_id: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # Optional timeout utcdatetime for the trigger (past this, we'll fail)
    trigger_timeout: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    # The method to call next, and any extra arguments to pass to it.
    # Usually used when resuming from DEFERRED.
    next_method: Mapped[str | None] = mapped_column(String(1000), nullable=True)
    next_kwargs: Mapped[dict | None] = mapped_column(MutableDict.as_mutable(ExtendedJSON), nullable=True)

    _task_display_property_value: Mapped[str | None] = mapped_column(
        "task_display_name", String(2000), nullable=True
    )
    dag_version_id: Mapped[str | uuid.UUID | None] = mapped_column(
        UUIDType(binary=False),
        ForeignKey("dag_version.id", ondelete="RESTRICT"),
        nullable=True,
    )
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

    dag_model: Mapped[DagModel] = relationship(
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
    hitl_detail = relationship("HITLDetail", lazy="noload", uselist=False)

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
        dag_version_id: UUIDType | uuid.UUID,
        run_id: str | None = None,
        state: str | None = None,
        map_index: int = -1,
    ):
        super().__init__()
        self.dag_id = task.dag_id
        self.task_id = task.task_id
        self.map_index = map_index

        self.refresh_from_task(task)
        if TYPE_CHECKING:
            assert self.task
        # init_on_load will config the log
        self.init_on_load()

        if run_id is not None:
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
        self.dag_version_id = dag_version_id

    def __hash__(self):
        return hash((self.task_id, self.dag_id, self.run_id, self.map_index))

    @property
    def stats_tags(self) -> dict[str, str]:
        """Returns task instance tags."""
        return prune_dict({"dag_id": self.dag_id, "task_id": self.task_id})

    @staticmethod
    def insert_mapping(
        run_id: str, task: Operator, map_index: int, dag_version_id: UUIDType
    ) -> dict[str, Any]:
        """
        Insert mapping.

        :meta private:
        """
        priority_weight = task.weight_rule.get_weight(
            TaskInstance(task=task, run_id=run_id, map_index=map_index, dag_version_id=dag_version_id)
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
            "custom_operator_name": getattr(task, "operator_name", None),
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

    @property
    def log_url(self) -> str:
        """Log URL for TaskInstance."""
        run_id = quote(self.run_id)
        base_url = conf.get("api", "base_url", fallback="http://localhost:8080/")
        map_index = f"/mapped/{self.map_index}" if self.map_index >= 0 else ""
        try_number = f"?try_number={self.try_number}" if self.try_number > 0 else ""
        _log_uri = f"{base_url.rstrip('/')}/dags/{self.dag_id}/runs/{run_id}/tasks/{self.task_id}{map_index}{try_number}"

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
            select(TaskInstance)
            .options(lazyload(TaskInstance.dag_run))  # lazy load dag run to avoid locking it
            .filter_by(
                run_id=run_id,
                task_id=task_id,
                map_index=map_index,
            )
        )

        if lock_for_update:
            for attempt in run_with_db_retries(logger=cls.logger()):
                with attempt:
                    return session.execute(query.with_for_update()).scalar_one_or_none()
        else:
            return session.execute(query).scalar_one_or_none()

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
            target_state: Any = inspect(self)
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
            self.priority_weight = self.task.weight_rule.get_weight(self)
        self.run_as_user = task.run_as_user
        # Do not set max_tries to task.retries here because max_tries is a cumulative
        # value that needs to be stored in the db.
        self.executor = task.executor
        self.executor_config = task.executor_config
        self.operator = task.task_type
        op_name = getattr(task, "operator_name", None)
        self.custom_operator_name = op_name if isinstance(op_name, str) else ""
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

        ti = select(func.count(TaskInstance.task_id)).where(
            TaskInstance.dag_id == self.dag_id,
            TaskInstance.task_id.in_(task.downstream_task_ids),
            TaskInstance.run_id == self.run_id,
            TaskInstance.state.in_((TaskInstanceState.SKIPPED, TaskInstanceState.SUCCESS)),
        )
        count = session.scalar(ti)
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
            assert session is not None

        dag = self.task.dag
        if dag is None:
            return None

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
            assert self.task is not None
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
        multiplier = self.task.retry_exponential_backoff if self.task.retry_exponential_backoff != 0 else 1.0
        if multiplier != 1.0 and multiplier > 0:
            try:
                # If the min_backoff calculation is below 1, it will be converted to 0 via int. Thus,
                # we must round up prior to converting to an int, otherwise a divide by zero error
                # will occur in the modded_hash calculation.
                # this probably gives unexpected results if a task instance has previously been cleared,
                # because try_number can increase without bound
                min_backoff = math.ceil(delay.total_seconds() * (multiplier ** (self.try_number - 1)))
            except OverflowError:
                min_backoff = MAX_RETRY_DELAY
                self.log.warning(
                    "OverflowError occurred while calculating min_backoff, using MAX_RETRY_DELAY for min_backoff."
                )

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
            # between 1 and 1.0 * delay * (multiplier^retry_number)
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

        dr = session.execute(
            select(DagRun).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id)
        ).scalar_one()
        return dr

    @provide_session
    def get_dagrun(self, session: Session = NEW_SESSION) -> DagRun:
        """
        Return the DagRun for this TaskInstance.

        :param session: SQLAlchemy ORM Session
        :return: DagRun
        """
        info: Any = inspect(self)
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
        if settings.engine is not None:
            settings.engine.dispose()
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

        # TODO (TaskSDK): This is the old ti execution path. The only usage is
        # in TI.run(...), someone needs to analyse if it's still actually used
        # somewhere and fix it, likely by rewriting TI.run(...) to use the same
        # mechanism as Operator.test().
        taskrun_result = _run_task(ti=self, task=self.task)  # type: ignore[arg-type]
        if taskrun_result is None:
            return None
        if taskrun_result.error:
            raise taskrun_result.error
        self.task = taskrun_result.ti.task  # type: ignore[assignment]
        return None

    @staticmethod
    @provide_session
    def register_asset_changes_in_db(
        ti: TaskInstance,
        task_outlets: list[AssetProfile],
        outlet_events: list[dict[str, Any]],
        session: Session = NEW_SESSION,
    ) -> None:
        print(task_outlets, outlet_events)
        from airflow.serialization.definitions.assets import (
            SerializedAsset,
            SerializedAssetNameRef,
            SerializedAssetUniqueKey,
            SerializedAssetUriRef,
        )

        # TODO: AIP-76 should we provide an interface to override this, so that the task can
        #  tell the truth if for some reason it touches a different partition?
        #  https://github.com/apache/airflow/issues/58474
        partition_key = ti.dag_run.partition_key
        asset_keys = {
            SerializedAssetUniqueKey(o.name, o.uri)
            for o in task_outlets
            if o.type == "Asset" and o.name and o.uri
        }
        asset_name_refs = {
            SerializedAssetNameRef(o.name) for o in task_outlets if o.type == "AssetNameRef" and o.name
        }
        asset_uri_refs = {
            SerializedAssetUriRef(o.uri) for o in task_outlets if o.type == "AssetUriRef" and o.uri
        }

        asset_models: dict[SerializedAssetUniqueKey, AssetModel] = {
            SerializedAssetUniqueKey.from_asset(am): am
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

        asset_event_extras: dict[SerializedAssetUniqueKey, dict] = {
            SerializedAssetUniqueKey(**event["dest_asset_key"]): event["extra"]
            for event in outlet_events
            if "source_alias_name" not in event
        }

        for key in asset_keys:
            try:
                am = asset_models[key]
            except KeyError:
                ti.log.warning(
                    'Task has inactive assets "Asset(name=%s, uri=%s)" in inlets or outlets',
                    key.name,
                    key.uri,
                )
                continue
            ti.log.debug("register event for asset %s", am)
            asset_manager.register_asset_change(
                task_instance=ti,
                asset=am,
                extra=asset_event_extras.get(key),
                partition_key=partition_key,
                session=session,
            )

        if asset_name_refs:
            asset_models_by_name = {key.name: am for key, am in asset_models.items()}
            asset_event_extras_by_name = {key.name: extra for key, extra in asset_event_extras.items()}
            for nref in asset_name_refs:
                try:
                    am = asset_models_by_name[nref.name]
                except KeyError:
                    ti.log.warning(
                        'Task has inactive assets "Asset.ref(name=%s)" in inlets or outlets', nref.name
                    )
                    continue
                ti.log.debug("register event for asset name ref %s", am)
                asset_manager.register_asset_change(
                    task_instance=ti,
                    asset=am,
                    extra=asset_event_extras_by_name.get(nref.name),
                    partition_key=partition_key,
                    session=session,
                )
        if asset_uri_refs:
            asset_models_by_uri = {key.uri: am for key, am in asset_models.items()}
            asset_event_extras_by_uri = {key.uri: extra for key, extra in asset_event_extras.items()}
            for uref in asset_uri_refs:
                try:
                    am = asset_models_by_uri[uref.uri]
                except KeyError:
                    ti.log.warning(
                        'Task has inactive assets "Asset.ref(uri=%s)" in inlets or outlets', uref.uri
                    )
                    continue
                ti.log.debug("register event for asset uri ref %s", am)
                asset_manager.register_asset_change(
                    task_instance=ti,
                    asset=am,
                    extra=asset_event_extras_by_uri.get(uref.uri),
                    partition_key=partition_key,
                    session=session,
                )

        def _asset_event_extras_from_aliases() -> dict[tuple[SerializedAssetUniqueKey, str, str], set[str]]:
            d = defaultdict(set)
            for event in outlet_events:
                try:
                    alias_name = event["source_alias_name"]
                except KeyError:
                    continue
                if alias_name not in outlet_alias_names:
                    continue
                asset_key = SerializedAssetUniqueKey(**event["dest_asset_key"])
                # fallback for backward compatibility
                asset_extra_json = json.dumps(event.get("dest_asset_extra", {}), sort_keys=True)
                asset_event_extra_json = json.dumps(event["extra"], sort_keys=True)
                d[asset_key, asset_extra_json, asset_event_extra_json].add(alias_name)
            return d

        outlet_alias_names = {o.name for o in task_outlets if o.type == "AssetAlias" and o.name}
        if outlet_alias_names and (event_extras_from_aliases := _asset_event_extras_from_aliases()):
            for (
                asset_key,
                asset_extra_json,
                asset_event_extras_json,
            ), event_aliase_names in event_extras_from_aliases.items():
                asset_event_extra = json.loads(asset_event_extras_json)
                asset = SerializedAsset(
                    name=asset_key.name,
                    uri=asset_key.uri,
                    group="asset",
                    extra=json.loads(asset_extra_json),
                    watchers=[],
                )
                ti.log.debug("register event for asset %s with aliases %s", asset_key, event_aliase_names)
                event = asset_manager.register_asset_change(
                    task_instance=ti,
                    asset=asset,
                    source_alias_names=event_aliase_names,
                    extra=asset_event_extra,
                    partition_key=partition_key,
                    session=session,
                )
                if event is None:
                    ti.log.info("Dynamically creating AssetModel %s", asset_key)
                    session.add(AssetModel.from_serialized(asset))
                    session.flush()  # So event can set up its asset fk.
                    asset_manager.register_asset_change(
                        task_instance=ti,
                        asset=asset,
                        source_alias_names=event_aliase_names,
                        extra=asset_event_extra,
                        partition_key=partition_key,
                        session=session,
                    )

    @provide_session
    def update_rtif(self, rendered_fields, session: Session = NEW_SESSION):
        from airflow.models.renderedtifields import RenderedTaskInstanceFields

        rtif = RenderedTaskInstanceFields(ti=self, render_templates=False, rendered_fields=rendered_fields)
        rtif.write(session=session)
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
        # This method is only used in ti.run and dag.test and task.test.
        # So doing the s10n/de-s10n dance to operator on Serialized task for the scheduler dep check part.
        from airflow.serialization.definitions.dag import SerializedDAG
        from airflow.serialization.serialized_objects import DagSerialization

        original_task = self.task
        if TYPE_CHECKING:
            assert original_task is not None
            assert original_task.dag is not None

        # We don't set up all tests well...
        if not isinstance(original_task.dag, SerializedDAG):
            serialized_dag = DagSerialization.from_dict(DagSerialization.to_dict(original_task.dag))
            self.task = serialized_dag.get_task(original_task.task_id)

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
        self.task = original_task
        if not res:
            return

        self._run_raw_task(mark_success=mark_success)

    @classmethod
    def fetch_handle_failure_context(
        cls,
        ti: TaskInstance,
        error: None | str,
        test_mode: bool | None = None,
        *,
        session: Session,
        fail_fast: bool = False,
    ):
        """
        Fetch the context needed to handle a failure.

        :param ti: TaskInstance
        :param error: if specified, log the specific exception if thrown
        :param test_mode: doesn't record success or failure in the DB if True
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

        # Set state correctly and figure out how to log it and decide whether
        # to email

        # Since this function is called only when the TaskInstance state is running,
        # try_number contains the current try_number (not the next). We
        # only mark task instance as FAILED if the next task instance
        # try_number exceeds the max_tries ... or if force_fail is truthy

        # Actual callbacks are handled by the DAG processor, not the scheduler
        task = getattr(ti, "task", None)

        if not ti.is_eligible_to_retry():
            ti.state = TaskInstanceState.FAILED

            if task and fail_fast:
                _stop_remaining_tasks(task_instance=ti, session=session)
        else:
            if ti.state == TaskInstanceState.RUNNING:
                # If the task instance is in the running state, it means it raised an exception and
                # about to retry so we record the task instance history. For other states, the task
                # instance was cleared and already recorded in the task instance history.
                ti.prepare_db_for_next_try(session)

            ti.state = State.UP_FOR_RETRY

        try:
            get_listener_manager().hook.on_task_instance_failed(
                previous_state=TaskInstanceState.RUNNING, task_instance=ti, error=error
            )
        except Exception:
            log.exception("error calling listener")

        return ti

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
        error: None | str,
        test_mode: bool | None = None,
        session: Session = NEW_SESSION,
    ) -> None:
        """
        Handle Failure for a task instance.

        :param error: if specified, log the specific exception if thrown
        :param test_mode: doesn't record success or failure in the DB if True
        :param session: SQLAlchemy ORM Session
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
        ti = TaskInstance.fetch_handle_failure_context(
            ti=self,
            error=error,
            test_mode=test_mode,
            session=session,
            fail_fast=fail_fast,
        )

        _log_state(task_instance=self)

        if not test_mode:
            TaskInstance.save_to_db(ti, session)

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

    # TODO (GH-52141): We should remove this entire function (only makes sense at runtime).
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
        # Do not use provide_session here -- it expunges everything on exit!
        if not session:
            session = settings.get_session()()

        from airflow.exceptions import NotMapped
        from airflow.sdk.api.datamodels._generated import (
            DagRun as DagRunSDK,
            PrevSuccessfulDagRunResponse,
            TIRunContext,
        )
        from airflow.sdk.definitions.param import process_params
        from airflow.sdk.execution_time.context import InletEventsAccessors
        from airflow.sdk.execution_time.task_runner import RuntimeTaskInstance
        from airflow.serialization.definitions.mappedoperator import get_mapped_ti_count
        from airflow.utils.context import (
            ConnectionAccessor,
            OutletEventAccessors,
            VariableAccessor,
        )

        if TYPE_CHECKING:
            assert session

        def _get_dagrun(session: Session) -> DagRun:
            dag_run = self.get_dagrun(session)
            if dag_run in session:
                return dag_run
            # The dag_run may not be attached to the session anymore since the
            # code base is over-zealous with use of session.expunge_all().
            # Re-attach it if the relation is not loaded so we can load it when needed.
            info: Any = inspect(dag_run)
            if info.attrs.consumed_asset_events.loaded_value is not NO_VALUE:
                return dag_run
            # If dag_run is not flushed to db at all (e.g. CLI commands using
            # in-memory objects for ad-hoc operations), just set the value manually.
            if not info.has_identity:
                dag_run.consumed_asset_events = []
                return dag_run
            return session.merge(dag_run, load=False)

        task: Any = self.task
        dag = task.dag
        dag_run = _get_dagrun(session)

        validated_params = process_params(dag, task, dag_run.conf, suppress_exception=ignore_param_exceptions)
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
            _ti_context_from_server=TIRunContext(
                dag_run=DagRunSDK.model_validate(dag_run, from_attributes=True),
                max_tries=self.max_tries,
                should_retry=self.is_eligible_to_retry(),
            ),
            start_date=self.start_date,
            dag_version_id=self.dag_version_id,
        )

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
            expanded_ti_count: int | None = get_mapped_ti_count(task, self.run_id, session=session)
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

    # TODO (GH-52141): We should remove this entire function (only makes sense at runtime).
    # This is intentionally left untyped so Mypy complains less about this dead code.
    def render_templates(self, context=None, jinja_env=None):
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

        # If self.task is mapped, this call replaces self.task to point to the
        # unmapped BaseOperator created by this function! This is because the
        # MappedOperator is useless for template rendering, and we need to be
        # able to access the unmapped task instead.
        original_task.render_template_fields(context, jinja_env)
        if isinstance(self.task, MappedOperator):
            self.task = context["ti"].task

        return original_task

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
        )

        # NOTE: Since we're only fetching the value field and not the whole
        # class, the @recreate annotation does not kick in. Therefore we need to
        # call XCom.deserialize_value() manually.

        # We are only pulling one single task.
        if (task_ids is None or isinstance(task_ids, str)) and not isinstance(map_indexes, Iterable):
            first = session.execute(
                query.with_only_columns(
                    XComModel.run_id,
                    XComModel.task_id,
                    XComModel.dag_id,
                    XComModel.map_index,
                    XComModel.value,
                )
            ).first()
            if first is None:  # No matching XCom at all.
                return default
            if map_indexes is not None or first.map_index < 0:
                return XComModel.deserialize_value(first)

            # raise RuntimeError("Nothing should hit this anymore")

        # TODO: TaskSDK: We should remove this, but many tests still currently call `ti.run()`. See #45549

        # At this point either task_ids or map_indexes is explicitly multi-value.
        # Order return values to match task_ids and map_indexes ordering.
        ordering: list[Any] = []
        if task_ids is None or isinstance(task_ids, str):
            ordering.append(XComModel.task_id)
        elif task_id_whens := {tid: i for i, tid in enumerate(task_ids)}:
            ordering.append(case(task_id_whens, value=XComModel.task_id))
        else:
            ordering.append(XComModel.task_id)
        if map_indexes is None or isinstance(map_indexes, int):
            ordering.append(XComModel.map_index)
        elif isinstance(map_indexes, range):
            if map_indexes.step < 0:
                ordering.append(XComModel.map_index.desc())
            else:
                ordering.append(XComModel.map_index)
        elif map_index_whens := {map_index: i for i, map_index in enumerate(map_indexes)}:
            ordering.append(case(map_index_whens, value=XComModel.map_index))
        else:
            ordering.append(XComModel.map_index)
        return LazyXComSelectSequence.from_select(
            query.with_only_columns(XComModel.value).order_by(None),
            order_by=ordering,
            session=session,
        )

    @provide_session
    def get_num_running_task_instances(self, session: Session, same_dagrun: bool = False) -> int:
        """Return Number of running TIs from the DB."""
        # .count() is inefficient
        num_running_task_instances_query = (
            select(func.count())
            .select_from(TaskInstance)
            .where(
                TaskInstance.dag_id == self.dag_id,
                TaskInstance.task_id == self.task_id,
                TaskInstance.state == TaskInstanceState.RUNNING,
            )
        )
        if same_dagrun:
            num_running_task_instances_query = num_running_task_instances_query.where(
                TaskInstance.run_id == self.run_id
            )
        return session.scalar(num_running_task_instances_query) or 0

    @staticmethod
    def filter_for_tis(tis: Iterable[TaskInstance | TaskInstanceKey]) -> ColumnElement[bool] | None:
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
    def ti_selector_condition(cls, vals: Collection[str | tuple[str, int]]) -> ColumnElement[bool]:
        """
        Build an SQLAlchemy filter for a list of task_ids or tuples of (task_id,map_index).

        :meta private:
        """
        # Compute a filter for TI.task_id and TI.map_index based on input values
        # For each item, it will either be a task_id, or (task_id, map_index)
        task_id_only = [v for v in vals if isinstance(v, str)]
        with_map_index = [v for v in vals if not isinstance(v, str)]

        filters: list[Any] = []
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
        if TYPE_CHECKING:
            assert self.task
        return _get_relevant_map_indexes(
            run_id=self.run_id,
            map_index=self.map_index,
            ti_count=ti_count,
            task=self.task,
            relative=upstream,
            session=session,
        )

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

    @property
    def is_schedulable(self):
        """Determine if the task_instance should be scheduled or short-circuited to ``success``."""
        return self.is_task_schedulable(self.task)

    @staticmethod
    def is_task_schedulable(task: Operator) -> bool:
        """
        Determine if the task should be scheduled instead of being short-circuited to ``success``.

        A task requires scheduling if it is not a trivial EmptyOperator, i.e. one of the
        following conditions holds:

        * it does **not** inherit from ``EmptyOperator``
        * it defines an ``on_execute_callback``
        * it defines an ``on_success_callback``
        * it declares any ``outlets``
        * it declares any ``inlets``

        If none of these are true, the task is considered empty and is immediately marked
        successful without being scheduled.

        Note: keeping this check as a separate public method is important so it can also be used
        by listeners (when a task is not scheduled, listeners are never called). For example,
        the OpenLineage listener checks all tasks at DAG start, and using this method lets
        it consistently determine whether the listener will run for each task.
        """
        return bool(
            not task.inherits_from_empty_operator
            or task.has_on_execute_callback
            or task.has_on_success_callback
            or task.outlets
            or task.inlets
        )


def _find_common_ancestor_mapped_group(node1: Operator, node2: Operator) -> SerializedTaskGroup | None:
    """Given two operators, find their innermost common mapped task group."""
    if node1.dag is None or node2.dag is None or node1.dag_id != node2.dag_id:
        return None
    parent_group_ids = {g.group_id for g in node1.iter_mapped_task_groups()}
    common_groups = (g for g in node2.iter_mapped_task_groups() if g.group_id in parent_group_ids)
    return next(common_groups, None)


def _is_further_mapped_inside(operator: Operator, container: SerializedTaskGroup) -> bool:
    """Whether given operator is *further* mapped inside a task group."""
    from airflow.serialization.definitions.mappedoperator import is_mapped

    if is_mapped(operator):
        return True
    task_group = operator.task_group
    while task_group is not None and task_group.group_id != container.group_id:
        if is_mapped(task_group):
            return True
        task_group = task_group.parent_group
    return False


def _get_relevant_map_indexes(
    *,
    task: Operator,
    run_id: str,
    map_index: int,
    relative: Operator,
    ti_count: int | None,
    session: Session,
) -> int | range | None:
    """
    Infer the map indexes of a relative that's "relevant" to this ti.

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

    The surrounding mapped task groups of ``upstream`` and ``task`` are
    inspected to find a common "ancestor". If such an ancestor is found,
    we need to return specific map indexes to pull a partial value from
    upstream XCom.

    The same logic apply for finding downstream tasks.

    :param task: Current task being inspected.
    :param run_id: Current run ID.
    :param map_index: Map index of the current task instance.
    :param relative: The relative task to find relevant map indexes for.
    :param ti_count: The total count of task instance this task was expanded
        by the scheduler, i.e. ``expanded_ti_count`` in the template context.
    :return: Specific map index or map indexes to pull, or ``None`` if we
        want to "whole" return value (i.e. no mapped task groups involved).
    """
    from airflow.serialization.definitions.mappedoperator import get_mapped_ti_count

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
    if (common_ancestor := _find_common_ancestor_mapped_group(task, relative)) is None:
        return None

    # At this point we know the two tasks share a mapped task group, and we
    # should use a "partial" value. Let's break down the mapped ti count
    # between the ancestor and further expansion happened inside it.

    ancestor_ti_count = get_mapped_ti_count(common_ancestor, run_id, session=session)
    ancestor_map_index = map_index * ancestor_ti_count // ti_count

    # If the task is NOT further expanded inside the common ancestor, we
    # only want to reference one single ti. We must walk the actual DAG,
    # and "ti_count == ancestor_ti_count" does not work, since the further
    # expansion may be of length 1.
    if not _is_further_mapped_inside(relative, common_ancestor):
        return ancestor_map_index

    # Otherwise we need a partial aggregation for values from selected task
    # instances in the ancestor's expansion context.
    further_count = ti_count // ancestor_ti_count
    map_index_start = ancestor_map_index * further_count
    return range(map_index_start, map_index_start + further_count)


def find_relevant_relatives(
    normal_tasks: Iterable[str],
    mapped_tasks: Iterable[tuple[str, int]],
    *,
    direction: Literal["upstream", "downstream"],
    dag: SerializedDAG,
    run_id: str,
    session: Session,
) -> Collection[str | tuple[str, int]]:
    from airflow.serialization.definitions.mappedoperator import get_mapped_ti_count

    visited: set[str | tuple[str, int]] = set()

    def _visit_relevant_relatives_for_normal(task_ids: Iterable[str]) -> None:
        partial_dag = dag.partial_subset(
            task_ids=task_ids,
            include_downstream=direction == "downstream",
            include_upstream=direction == "upstream",
            exclude_original=True,
        )
        visited.update(partial_dag.task_dict)

    def _visit_relevant_relatives_for_mapped(mapped_tasks: Iterable[tuple[str, int]]) -> None:
        from airflow.exceptions import NotMapped

        for task_id, map_index in mapped_tasks:
            task = dag.get_task(task_id)
            try:
                ti_count = get_mapped_ti_count(task, run_id, session=session)
            except NotMapped:
                # Task is not actually mapped (not a MappedOperator and not inside a mapped task group).
                # Treat it as a normal task instead.
                _visit_relevant_relatives_for_normal([task_id])
                continue
            for relative in task.get_flat_relatives(upstream=direction == "upstream"):
                if relative.task_id in visited:
                    continue
                relative_map_indexes = _get_relevant_map_indexes(
                    task=task,
                    relative=relative,  # type: ignore[arg-type]
                    run_id=run_id,
                    map_index=map_index,
                    ti_count=ti_count,
                    session=session,
                )
                visiting_mapped: set[tuple[str, int]] = set()
                visiting_normal: set[str] = set()
                match relative_map_indexes:
                    case int():
                        if (item := (relative.task_id, relative_map_indexes)) not in visited:
                            visiting_mapped.add(item)
                    case range():
                        visiting_mapped.update((relative.task_id, i) for i in relative_map_indexes)
                    case None:
                        if (task_id := relative.task_id) not in visited:
                            visiting_normal.add(task_id)
                _visit_relevant_relatives_for_normal(visiting_normal)
                _visit_relevant_relatives_for_mapped(visiting_mapped)
                visited.update(visiting_mapped, visiting_normal)

    _visit_relevant_relatives_for_normal(normal_tasks)
    _visit_relevant_relatives_for_mapped(mapped_tasks)
    return visited


class TaskInstanceNote(Base):
    """For storage of arbitrary notes concerning the task instance."""

    __tablename__ = "task_instance_note"
    ti_id: Mapped[str] = mapped_column(
        String(36).with_variant(postgresql.UUID(as_uuid=False), "postgresql"),
        primary_key=True,
        nullable=False,
    )
    user_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    content: Mapped[str | None] = mapped_column(String(1000).with_variant(Text(1000), "mysql"))
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False
    )

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
