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

import itertools
import logging
import os
import re
from collections import defaultdict
from collections.abc import Callable, Iterable, Iterator, Sequence
from datetime import datetime
from typing import TYPE_CHECKING, Any, NamedTuple, TypeVar, cast, overload
from uuid import UUID

import structlog
from opentelemetry import trace
from opentelemetry.context import context
from opentelemetry.trace import StatusCode
from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
from sqlalchemy import (
    JSON,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    PrimaryKeyConstraint,
    String,
    Text,
    UniqueConstraint,
    Uuid,
    and_,
    case,
    func,
    not_,
    or_,
    text,
    tuple_,
    update,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.ext.mutable import MutableDict
from sqlalchemy.orm import (
    Mapped,
    declared_attr,
    joinedload,
    mapped_column,
    relationship,
    synonym,
    validates,
)
from sqlalchemy.orm.exc import StaleDataError
from sqlalchemy.sql.expression import false, select
from sqlalchemy.sql.functions import coalesce

from airflow._shared.observability.metrics import stats
from airflow._shared.observability.metrics.stats import build_dag_metric_tags
from airflow._shared.observability.traces import (
    DAGRUN_PARENT_TRACE_CONTEXT_KEY,
    TASK_SPAN_DETAIL_LEVEL_KEY,
    TRACE_SAMPLED_KEY,
    new_dagrun_trace_carrier,
    override_ids,
)
from airflow._shared.timezones import timezone
from airflow.callbacks.callback_requests import DagCallbackRequest, DagRunContext
from airflow.configuration import conf as airflow_conf
from airflow.exceptions import AirflowException, NotMapped, TaskNotFound
from airflow.listeners.listener import get_listener_manager
from airflow.models import Deadline, Log
from airflow.models.backfill import Backfill
from airflow.models.base import Base, StringID
from airflow.models.deadline_alert import DeadlineAlert as DeadlineAlertModel
from airflow.models.dynamic_region import (
    LOOP_DECISION_REASON_KEY,
    SENTINEL_REGION_ID,
    DynamicRegion,
    public_region_filter,
)
from airflow.models.taskinstance import TaskInstance as TI, _add_and_prime_mapped_ti
from airflow.models.tasklog import LogTemplate
from airflow.serialization.definitions.deadline import SerializedReferenceModels
from airflow.serialization.definitions.notset import NOTSET, ArgNotSet, is_arg_set
from airflow.ti_deps.dep_context import DepContext
from airflow.ti_deps.dependencies_states import SCHEDULEABLE_STATES
from airflow.utils.helpers import chunks, is_container, prune_dict
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.retries import retry_db_transaction
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.sqlalchemy import (
    ExtendedJSON,
    UtcDateTime,
    get_dialect_name,
    nulls_first,
    with_row_locks,
)
from airflow.utils.state import DagRunState, State, TaskInstanceState
from airflow.utils.strings import get_random_string
from airflow.utils.types import DagRunTriggeredByType, DagRunType

if TYPE_CHECKING:
    from typing import Literal, TypeAlias

    from pydantic import NonNegativeInt
    from sqlalchemy.engine import ScalarResult
    from sqlalchemy.orm import Session
    from sqlalchemy.sql.elements import ColumnElement
    from sqlalchemy.sql.selectable import Select

    from airflow.api_fastapi.execution_api.datamodels.taskinstance import (
        DagRun as DRDataModel,
        TaskInstance as TIDataModel,
    )
    from airflow.models.dag_version import DagVersion
    from airflow.sdk import DAG as SDKDAG
    from airflow.serialization.definitions.dag import SerializedDAG
    from airflow.serialization.definitions.mappedoperator import Operator
    from airflow.serialization.definitions.taskgroup import SerializedLoopTaskGroup
    from airflow.timetables.base import Timetable

    CreatedTasks = TypeVar("CreatedTasks", Iterator["dict[str, Any]"], Iterator[TI])
    AttributeValueType: TypeAlias = (
        str | bool | int | float | Sequence[str] | Sequence[bool] | Sequence[int] | Sequence[float]
    )

RUN_ID_REGEX = r"^(?:manual|scheduled|asset_triggered)__(?:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\+00:00)$"

log = structlog.get_logger(__name__)

tracer = trace.get_tracer(__name__)


class InvalidLoopDecision(ValueError):
    """A gate cannot complete successfully with the available decision."""


def validate_loop_decision(stop: bool | None, region_index: int, group: SerializedLoopTaskGroup) -> None:
    """Raise :class:`InvalidLoopDecision` unless ``stop`` is valid for the gate of pass ``region_index``."""
    at_limit = region_index >= group.max_iterations
    if stop is None:
        raise InvalidLoopDecision("Successful loop gate requires a decision")
    if not stop and at_limit:
        raise InvalidLoopDecision("Loop cannot continue beyond its iteration limit")
    if not group.has_until and stop != at_limit:
        raise InvalidLoopDecision("Fixed-count loop decision does not match its iteration limit")


class TISchedulingDecision(NamedTuple):
    """Type of return for DagRun.task_instance_scheduling_decisions."""

    tis: list[TI]
    schedulable_tis: list[TI]
    changed_tis: bool
    unfinished_tis: list[TI]
    finished_tis: list[TI]


def _default_run_after(ctx):
    params = ctx.get_current_parameters()
    return params["data_interval_end"] or params["logical_date"] or timezone.utcnow()


def _creator_note(val):
    """Creator for the ``note`` association proxy."""
    if isinstance(val, str):
        return DagRunNote(content=val)
    if isinstance(val, dict):
        return DagRunNote(**val)
    return DagRunNote(*val)


def dagrun_trace_attributes(dr) -> dict[str, str]:
    """
    Run-identity attributes for a DAG run's trace.

    Defined once and used in two places: forwarded to the sampler at carrier
    creation (so a custom sampler can differentiate the head-sampling decision by
    run kind) and set on the emitted ``dag_run`` span. ``getattr`` guards because
    the values may not be populated yet at ``__init__``/clear time.
    """
    attributes: dict[str, str] = {}
    dag_id = getattr(dr, "dag_id", None)
    if dag_id is not None:
        attributes["airflow.dag_id"] = str(dag_id)
    run_id = getattr(dr, "run_id", None)
    if run_id is not None:
        attributes["airflow.dag_run.run_id"] = str(run_id)
    run_type = getattr(dr, "run_type", None)
    if run_type is not None:
        attributes["airflow.dag_run.run_type"] = str(run_type)
    return attributes


def trace_sampled_override(conf) -> bool | None:
    """
    Head-sampling override from the ``airflow/trace_sampled`` run conf key.

    Returns the forced SAMPLED flag only when the conf value is an explicit bool
    (True = always trace this run, False = never); otherwise None, meaning no
    override and the configured sampler decides. Non-bool values are ignored
    rather than coerced, so a malformed value never silently flips sampling or
    fails run creation.
    """
    if not conf:
        return None
    raw = conf.get(TRACE_SAMPLED_KEY)
    if isinstance(raw, bool):
        return raw
    return None


def parent_trace_context(conf) -> context.Context | None:
    """
    External parent trace context from the ``airflow/dagrun_parent_trace_context`` run conf key.

    Lets a run be embedded in a trace owned by an external system (an upstream
    orchestrator, event pipeline, CI job, another Airflow) instead of being a root
    trace. The value is a W3C ``traceparent`` string, or a carrier dict carrying
    ``traceparent`` (and optionally ``tracestate``); anything else -- including a
    malformed traceparent -- is ignored so it can neither silently mis-parent the
    run nor fail run creation. Returns an OpenTelemetry ``Context`` when a valid
    parent is present, else None (root trace, the default).
    """
    if not conf:
        return None
    match conf.get(DAGRUN_PARENT_TRACE_CONTEXT_KEY):
        case str() as traceparent:
            carrier = {"traceparent": traceparent}
        case {"traceparent": str()} as raw:
            # Keep only str members: a non-str tracestate reaches TraceState.from_header
            # unvalidated and raises TypeError, which would drop the otherwise-valid parent.
            carrier = {k: raw[k] for k in ("traceparent", "tracestate") if isinstance(raw.get(k), str)}
        case _:
            return None
    try:
        ctx = TraceContextTextMapPropagator().extract(carrier)
    except Exception:
        # Never let a malformed conf value fail run creation; fall back to a root
        # trace.
        return None
    if not trace.get_current_span(ctx).get_span_context().is_valid:
        return None
    return ctx


class DagRun(Base, LoggingMixin):
    """
    Invocation instance of a DAG.

    A DAG run can be created by the scheduler (i.e. scheduled runs), or by an
    external trigger (i.e. manual runs).
    """

    __tablename__ = "dag_run"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    dag_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    queued_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    logical_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    start_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    end_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    _state: Mapped[str | None] = mapped_column("state", String(50), default=DagSrunState.QUEUED, nullable=True)
    run_id: Mapped[str] = mapped_column(StringID(), nullable=False)
    creating_job_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    run_type: Mapped[str] = mapped_column(String(50), nullable=False)
    triggered_by: Mapped[DagRunTriggeredByType | None] = mapped_column(
        Enum(DagRunTriggeredByType, native_enum=False, length=50), nullable=True
    )  # Airflow component that triggered the run.
    triggering_user_name: Mapped[str | None] = mapped_column(
        String(512),
        nullable=True,
    )  # The user that triggered the DagRun, if applicable
    conf: Mapped[dict[str, Any] | None] = mapped_column(
        JSON().with_variant(postgresql.JSONB, "postgresql"), nullable=True
    )
    # These two must be either both NULL or both datetime.
    data_interval_start: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    data_interval_end: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    # Earliest time when this DagRun can start running.
    run_after: Mapped[datetime] = mapped_column(UtcDateTime, default=_default_run_after, nullable=False)
    # When a scheduler last attempted to schedule TIs for this DagRun
    last_scheduling_decision: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)
    # Foreign key to LogTemplate. DagRun rows created prior to this column's
    # existence have this set to NULL. Later rows automatically populate this on
    # insert to point to the latest LogTemplate entry.
    log_template_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("log_template.id", name="task_instance_log_template_id_fkey", ondelete="NO ACTION"),
        default=select(func.max(LogTemplate.__table__.c.id)),
        nullable=True,
    )
    # This is nullable because it's too costly to migrate dagruns created prior
    # to this column's addition (Airflow 3.2.0). If you want a reasonable
    # meaningful non-null value, use ``dr.created_at or dr.run_after``.
    created_at: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True, default=timezone.utcnow)
    updated_at: Mapped[datetime | None] = mapped_column(
        UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=True
    )
    # Keeps track of the number of times the dagrun had been cleared.
    # This number is incremented only when the DagRun is re-Queued,
    # when the DagRun is cleared.
    clear_number: Mapped[int] = mapped_column(Integer, default=0, nullable=False, server_default="0")
    backfill_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("backfill.id"), nullable=True)
    """
    The backfill this DagRun is currently associated with.

    It's possible this could change if e.g. the dag run is cleared to be rerun, or perhaps re-backfilled.
    """
    bundle_version: Mapped[str | None] = mapped_column(StringID(), nullable=True)

    scheduled_by_job_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    # Span context carrier, used for context propagation.
    context_carrier: Mapped[dict[str, Any] | None] = mapped_column(
        MutableDict.as_mutable(ExtendedJSON), nullable=True
    )
    created_dag_version_id: Mapped[UUID | None] = mapped_column(
        Uuid(),
        ForeignKey("dag_version.id", name="created_dag_version_id_fkey", ondelete="set null"),
        nullable=True,
    )
    """The id of the dag version column that was in effect at dag run creation time.

    :meta private:
    """

    partition_key: Mapped[str | None] = mapped_column(StringID(), nullable=True)
    partition_date: Mapped[datetime | None] = mapped_column(UtcDateTime, nullable=True)

    # Remove this `if` after upgrading Sphinx-AutoAPI
    if not TYPE_CHECKING and "BUILDING_AIRFLOW_DOCS" in os.environ:
        dag: SerializedDAG | None
    else:
        dag: SerializedDAG | None = None

    __table_args__ = (
        Index("dag_id_state", dag_id, _state),
        UniqueConstraint("dag_id", "run_id", name="dag_run_dag_id_run_id_key"),
        UniqueConstraint("dag_id", "logical_date", name="dag_run_dag_id_logical_date_key"),
        Index("idx_dag_run_dag_id", dag_id),
        Index("idx_dag_run_run_after", run_after),
        Index("idx_dag_run_created_dag_version_id", created_dag_version_id),
        Index(
            "idx_dag_run_running_dags",
            "state",
            "dag_id",
            postgresql_where=text("state='running'"),
            sqlite_where=text("state='running'"),
        ),
        # since mysql lacks filtered/partial indices, this creates a
        # duplicate index on mysql. Not the end of the world
        Index(
            "idx_dag_run_queued_dags",
            "state",
            "dag_id",
            postgresql_where=text("state='queued'"),
            sqlite_where=text("state='queued'"),
        ),
    )

    task_instances = relationship(
        TI,
        back_populates="dag_run",
        primaryjoin="and_(DagRun.dag_id == TaskInstance.dag_id, DagRun.run_id == TaskInstance.run_id, TaskInstance.working_set.is_(True))",
        cascade="save-update, merge, delete, delete-orphan",
        passive_deletes=True,
    )
    historical_task_instances = relationship(
        TI,
        primaryjoin="and_(DagRun.dag_id == TaskInstance.dag_id, DagRun.run_id == TaskInstance.run_id, TaskInstance.working_set.is_(None))",
        viewonly=True,
    )
    dag_model = relationship(
        "DagModel",
        primaryjoin="foreign(DagRun.dag_id) == DagModel.dag_id",
        uselist=False,
        viewonly=True,
    )
    dag_run_note = relationship(
        "DagRunNote",
        back_populates="dag_run",
        uselist=False,
        cascade="all, delete, delete-orphan",
    )

    deadlines = relationship(
        "Deadline",
        back_populates="dagrun",
        uselist=True,
        cascade="all, delete, delete-orphan",
    )

    created_dag_version = relationship("DagVersion", uselist=False, passive_deletes=True)
    """
    The dag version that was active when the dag run was created, if available.

    :meta private:
    """

    backfill = relationship(Backfill, uselist=False)
    backfill_max_active_runs = association_proxy("backfill", "max_active_runs")
    max_active_runs = association_proxy("dag_model", "max_active_runs")

    @property
    def team_name(self) -> str | None:
        """Name of the team owning this run's Dag, or ``None`` when it is not team-owned."""
        # Gate before touching ``dag_model``: single-team deployments must not pay for the load.
        if not airflow_conf.getboolean("core", "multi_team"):
            return None
        return self.dag_model.team_name if self.dag_model else None

    note = association_proxy("dag_run_note", "content", creator=_creator_note)

    DEFAULT_DAGRUNS_TO_EXAMINE = airflow_conf.getint(
        "scheduler",
        "max_dagruns_per_loop_to_schedule",
        fallback=20,
    )
    _ti_dag_versions = association_proxy("task_instances", "dag_version")
    _historical_dag_versions = association_proxy("historical_task_instances", "dag_version")

    # Can be set by manual prefetch, such as attach_dag_versions_to_runs()
    # as an alternative to traversing all current and historical attempts. Maps version_id -> DagVersion.
    _prefetched_dag_version_ids: dict[UUID, DagVersion] | None = None

    def __init__(
        self,
        dag_id: str | None = None,
        run_id: str | None = None,
        *,
        queued_at: datetime | None | ArgNotSet = NOTSET,
        logical_date: datetime | None = None,
        run_after: datetime | None = None,
        start_date: datetime | None = None,
        conf: Any | None = None,
        state: DagRunState | None = None,
        run_type: str | None = None,
        creating_job_id: int | None = None,
        data_interval: tuple[datetime, datetime] | None = None,
        triggered_by: DagRunTriggeredByType | None = None,
        triggering_user_name: str | None = None,
        backfill_id: NonNegativeInt | None = None,
        bundle_version: str | None = None,
        partition_key: str | None = None,
        partition_date: datetime | None = None,
        note: str | None = None,
    ):
        # For manual runs where logical_date is None, ensure no data_interval is set.
        if logical_date is None and data_interval is not None:
            raise ValueError("data_interval must be None if logical_date is None")

        if data_interval is None:
            # Legacy: Only happen for runs created prior to Airflow 2.2.
            self.data_interval_start = self.data_interval_end = None
        else:
            self.data_interval_start, self.data_interval_end = data_interval
        self.bundle_version = bundle_version
        if dag_id is not None:
            self.dag_id = dag_id
        if run_id is not None:
            self.run_id = run_id
        self.logical_date = logical_date
        if run_after is not None:
            self.run_after = run_after
        self.start_date = start_date
        self.conf = conf or {}
        self.note = note
        if state is not None:
            self.state = state
        if not is_arg_set(queued_at):
            self.queued_at = timezone.utcnow() if state == DagRunState.QUEUED else None
        elif queued_at is not None:
            self.queued_at = queued_at
        if run_type is not None:
            self.run_type = run_type
        self.creating_job_id = creating_job_id
        self.backfill_id = backfill_id
        self.clear_number = 0
        self.triggered_by = triggered_by
        self.triggering_user_name = triggering_user_name
        self.scheduled_by_job_id = None
        self.context_carrier: dict[str, str] = new_dagrun_trace_carrier(
            task_span_detail_level=self.conf.get(TASK_SPAN_DETAIL_LEVEL_KEY, None),
            attributes=dagrun_trace_attributes(self),  # these are for potential use by head sampler
            force_sampled=trace_sampled_override(self.conf),
            parent_context=parent_trace_context(self.conf),
        )

        if not isinstance(partition_key, str | None):
            raise ValueError(
                f"Expected partition_key to be a `str` or `None` but got `{partition_key.__class__.__name__}`"
            )
        self.partition_key = partition_key
        self.partition_date = partition_date
        super().__init__()

    def __repr__(self):
        return (
            f"<DagRun {self.dag_id} @ {self.logical_date}: {self.run_id}, state:{self.state}, "
            f"queued_at: {self.queued_at}. run_type: {self.run_type}>"
        )

    @validates("run_id")
    def validate_run_id(self, key: str, run_id: str) -> str | None:
        if not run_id:
            return None
        if ".." in run_id and not airflow_conf.getboolean("core", "allow_double_dot_in_ids", fallback=False):
            raise ValueError(f"The run_id '{run_id}' must not contain '..' to prevent path traversal")
        if re.match(RUN_ID_REGEX, run_id):
            return run_id
        regex = airflow_conf.get("scheduler", "allowed_run_id_pattern").strip()
        if regex and re.match(regex, run_id):
            return run_id
        raise ValueError(
            f"The run_id provided '{run_id}' does not match regex pattern '{regex}' or '{RUN_ID_REGEX}'"
        )

    @property
    def dag_run_data(self) -> DRDataModel:
        from airflow.api_fastapi.execution_api.datamodels.taskinstance import DagRun as DRDataModel

        return DRDataModel(
            dag_id=self.dag_id,
            run_id=self.run_id,
            logical_date=self.logical_date,
            data_interval_start=self.data_interval_start,
            data_interval_end=self.data_interval_end,
            run_after=self.run_after,
            start_date=self.start_date or timezone.utcnow(),
            end_date=self.end_date,
            run_type=DagRunType(self.run_type),
            state=self.state,
            conf=self.conf,
            consumed_asset_events=[],
            partition_key=self.partition_key,
            partition_date=self.partition_date,
        )

    @property
    def dag_versions(self) -> list[DagVersion]:
        """Return the DAG versions associated with the TIs of this DagRun."""
        # when the dag is in a versioned bundle, we keep the dag version fixed
        if self.bundle_version:
            return [self.created_dag_version] if self.created_dag_version is not None else []

        if self._prefetched_dag_version_ids is None:
            self._prefetched_dag_version_ids = {
                v.id: v
                for v in self.created_dag_version.get_versions()
                if v.id != self.created_dag_version_id
            }
        return list(self._prefetched_dag_version_ids.values())

    @provide_session
    def get_tis(self, *, session: Session = NEW_SESSION) -> list[TI]:
        """Return all TIs associated with this DagRun."""
        return session.scalars(
            select(TI).where(TI.dag_id == self.dag_id, TI.run_id == self.run_id)
        ).all()

    @provide_session
    def get_tis_for_state(self, *, session: Session = NEW_SESSION) -> list[TI]:
        """
        Return the collection of tasks that should be considered for evaluation of terminal dag run state.
        """
        def is_effective_leaf(task):
            for down_task_id in task.downstream_task_ids:
                down_task = dag.get_task(down_task_id)

                if not down_task.is_teardown or (
                    down_task.on_failure_fail_dagrun and down_task.on_success_succeed_dagrun
                ):
                    # A downstream task that can determine the terminal DagRun state
                    # means this task is not an effective leaf.
                    return False
            # A teardown task itself only drives a failed DagRun when configured to do so.
            # Its success can suppress upstream task states only when both flags are enabled.
            return not task.is_teardown or (task.on_failure_fail_dagrun or task.on_success_succeed_dagrun)
        leaf_task_ids = {x.task_id for x in dag.tasks if is_effective_leaf(x)}
        if not leaf_task_ids:
            leaf_task_ids = {x.task_id for x in dag.tasks if not x.downstream_list}
        leaf_tis = {ti for ti in tis if ti.task_id in leaf_task_ids and ti.state != TaskInstanceState.REMOVED}
        return leaf_tis

    def _emit_dagrun_span(self, state: DagRunState):
        # just to be safe
        if not isinstance(self.context_carrier, dict):
            return

        ctx = TraceContextTextMapPropagator().extract(self.context_carrier)
        span = trace.get_current_span(context=ctx)
        span_context = span.get_span_context()

    @provide_session
    def update_state(self, *, session: Session = NEW_SESSION) -> None:
        """Update the DagRun state based on the states of its task instances."""
        # This is a simplified version of the state update logic.
        # In a real scenario, it would be much more complex.
        tis = self.get_tis(session=session)
        if not tis:
            self.state = DagRunState.FAILED
            return

        # For simplicity, we'll just check if any TI failed.
        if any(ti.state == TaskInstanceState.FAILED for ti in tis):
            self.state = DagRunState.FAILED
        elif all(ti.state == TaskInstanceState.SUCCESS for ti in tis):
            self.state = DagRunState.SUCCESS
        else:
            self.state = DagRunState.RUNNING

    @classmethod
    @provide_session
    def get_latest_runs(cls, *, session: Session = NEW_SESSION) -> list[DagRun]:
        """Return the latest DagRun for each DAG."""
        subquery = (
            select(cls.dag_id, func.max(cls.logical_date).label("logical_date"))
            .group_by(cls.dag_id)
            .subquery()
        )
        return list(
            session.scalars(
                select(cls).join(
                    subquery,
                    and_(cls.dag_id == subquery.c.dag_id, cls.logical_date == subquery.c.logical_date),
                )
            ).all()
        )

    @provide_session
    def schedule_tis(
        self,
        schedulable_tis: Iterable[TI],
        *,
        session: Session = NEW_SESSION,
        max_tis_per_query: int | None = None,
    ) -> int:
        """
        Set the given task instances in to the scheduled state.

        Each element of ``schedulable_tis`` should have its ``task`` attribute already set.

        Any EmptyOperator without ``on_execute_callback`` or ``on_success_callback`` or ``inlets`` or
        ``outlets`` is instead set straight to the success state, without execution.

        All the TIs should belong to this DagRun, but this code is in the hot-path, this is not checked --
        it is the caller's responsibility to call this function only with TIs from a single dag run.
        """
        # Get list of TI IDs that do not need to executed, these are
        # tasks using EmptyOperator and without on_execute_callback / on_success_callback
        empty_ti_ids: list[UUID] = []
        schedulable_ti_ids: list[UUID] = []
        debug_try_number_check = self.log.isEnabledFor(logging.DEBUG)
        expected_try_number_by_ti_id: dict[UUID, tuple[int, int, str | None]] = {}
        for ti in schedulable_tis:
            if ti.state == TaskInstanceState.UP_FOR_RETRY:
                if TYPE_CHECKING:
                    assert ti.task
                ti.refresh_from_task(ti.task, dag_run=self)
            if not ti.is_schedulable:
                empty_ti_ids.append(ti.id)
            # The defer_task method will check "start_trigger_args" to see whether the operator
            # start execution from triggerer. If so, we'll also check "start_from_trigger"
            # to see whether this feature is turned on and defer this task
            # to run in the worker.
            elif not ti.defer_task(session=session):
                schedulable_ti_ids.append(ti.id)
                if debug_try_number_check:
                    expected_try_number_by_ti_id[ti.id] = (
                        max(1, ti.try_number),
                        ti.try_number,
                        ti.state,
                    )

        count = 0
        # Don't only check if the TI.id is in id_chunk
        # but also check if the TI.state is in the schedulable states.
        # Plus, a scheduled empty operator should not be scheduled again.
        non_null_schedulable_states = tuple(s for s in SCHEDULEABLE_STATES if s is not None)
        schedulable_state_clause = or_(
            TI.state.is_(None),
            TI.state.in_(non_null_schedulable_states),
        )
        # Allocate the first attempt; retries and clears allocate theirs when rotating the TI id.
        next_try_number = case((TI.try_number == 0, 1), else_=TI.try_number)
        if schedulable_ti_ids:
            schedulable_ti_ids_chunks = chunks(
                schedulable_ti_ids, max_tis_per_query or len(schedulable_ti_ids)
            )
            for id_chunk in schedulable_ti_ids_chunks:
                result = session.execute(
                    update(TI)
                    .where(TI.id.in_(id_chunk), schedulable_state_clause)
                    .values(
                        state=TaskInstanceState.SCHEDULED,
                        scheduled_dttm=timezone.utcnow(),
                        try_number=next_try_number,
                        # Already archived with the finished try; the new one must not inherit it.
                        retry_reason=None,
                        retry_delay_override=None,
                    )
                    .execution_options(synchronize_session=False)
                )
                count += getattr(result, "rowcount", 0)
                if debug_try_number_check:
                    rows = session.execute(
                        select(TI.id, TI.try_number, TI.state).where(TI.id.in_(id_chunk))
                    ).all()
                    rows_by_ti_id = {
                        ti_id: (db_try_number, db_state) for ti_id, db_try_number, db_state in rows
                    }
                    for ti_id in id_chunk:
                        expected = expected_try_number_by_ti_id.get(ti_id)
                        if expected is None:
                            continue
                        db_row = rows_by_ti_id.get(ti_id)
                        if db_row is None:
                            continue
                        expected_try_number, pre_update_try_number, pre_update_state = expected
                        db_try_number, db_state = db_row
                        if db_try_number != expected_try_number:
                            self.log.warning(
                                "schedule_tis: try_number mismatch after scheduling for ti_id=%s "
                                "dag_run=%s/%s scheduler_job_id=%s "
                                "pre_state=%s pre_try_number=%d expected_try_number=%d "
                                "db_state=%s db_try_number=%d",
                                ti_id,
                                self.dag_id,
                                self.run_id,
                                self.scheduled_by_job_id,
                                pre_update_state,
                                pre_update_try_number,
                                expected_try_number,
                                db_state,
                                db_try_number,
                            )

        # Tasks using EmptyOperator should not be executed, mark them as success
        if empty_ti_ids:
            dummy_ti_ids_chunks = chunks(empty_ti_ids, max_tis_per_query or len(empty_ti_ids))
            for id_chunk in dummy_ti_ids_chunks:
                result = session.execute(
                    update(TI)
                    .where(TI.id.in_(id_chunk), schedulable_state_clause)
                    .values(
                        state=TaskInstanceState.SUCCESS,
                        start_date=timezone.utcnow(),
                        end_date=timezone.utcnow(),
                        duration=0,
                        try_number=next_try_number,
                        retry_reason=None,
                        retry_delay_override=None,
                    )
                    .execution_options(
                        synchronize_session=False,
                    )
                )
                count += getattr(result, "rowcount", 0)

        return count

    @provide_session
    def get_log_template(self, *, session: Session = NEW_SESSION) -> LogTemplate:
        return DagRun._get_log_template(log_template_id=self.log_template_id, session=session)

    @staticmethod
    @provide_session
    def _get_log_template(log_template_id: int | None, *, session: Session = NEW_SESSION) -> LogTemplate:
        template: LogTemplate | None
        if log_template_id is None:  # DagRun created before LogTemplate introduction.
            template = session.scalar(select(LogTemplate).order_by(LogTemplate.id).limit(1))
        else:
            template = session.get(LogTemplate, log_template_id)
        if template is None:
            raise AirflowException(
                f"No log_template entry found for ID {log_template_id!r}. "
                f"Please make sure you set up the metadatabase correctly."
            )
        return template

    @staticmethod
    def _get_partial_task_ids(dag: SerializedDAG | None) -> list[str] | None:
        return dag.task_ids if dag and dag.partial else None

    @staticmethod
    def apply_partition_date_window(
        stmt: Select,
        *,
        timetable: Timetable,
        start: datetime | None,
        end: datetime | None,
        end_exclusive: bool = False,
    ) -> Select:
        """
        Filter stmt to a partition_date window bounded by *start* and *end*.

        The lower bound is always inclusive. The upper bound is inclusive by default;
        pass ``end_exclusive=True`` when *end* is the open edge of the window, as with
        a date-only filter widened to the following local midnight.
        """
        if start is not None:
            lower = timetable.localize_partition_datetime(start)
            stmt = stmt.where(DagRun.partition_date >= lower)
        if end is not None:
            upper = timetable.localize_partition_datetime(end)
            if end_exclusive:
                stmt = stmt.where(DagRun.partition_date < upper)
            else:
                stmt = stmt.where(DagRun.partition_date <= upper)
        return stmt


class DagRunNote(Base):
    """For storage of arbitrary notes concerning the dagrun instance."""

    __tablename__ = "dag_run_note"

    user_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    dag_run_id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    content: Mapped[str | None] = mapped_column(String(1000).with_variant(Text(1000), "mysql"))
    created_at: Mapped[datetime] = mapped_column(UtcDateTime, default=timezone.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        UtcDateTime, default=timezone.utcnow, onupdate=timezone.utcnow, nullable=False
    )

    dag_run = relationship("DagRun", back_populates="dag_run_note")

    __table_args__ = (
        PrimaryKeyConstraint("dag_run_id", name="dag_run_note_pkey"),
        ForeignKeyConstraint(
            (dag_run_id,),
            ["dag_run.id"],
            name="dag_run_note_dr_fkey",
            ondelete="CASCADE",
        ),
    )

    def __init__(self, content, user_id=None):
        self.content = content
        self.user_id = user_id

    def __repr__(self):
        prefix = f"<{self.__class__.__name__}: {self.dag_id}.{self.dagrun_id} {self.run_id}"
        if self.map_index != -1:
            prefix += f" map_index={self.map_index}"
        return prefix + ">"


_TI_CHUNK_SIZE = 500

_LOCKED_DAG_RUNS_KEY = "airflow.locked_dag_runs"


def lock_dag_runs(session: Session, run_keys: Iterable[tuple[str, str]], *, refresh: bool = False) -> None:
    """
    Lock Dag runs ``FOR UPDATE`` in ``(dag_id, run_id)`` order.

    Every path that changes task instances under their runs' locks takes the locks in
    the same order and cannot deadlock each other. Runs this function already locked
    in the current transaction are skipped, so nested callers along one path do not
    lock them again. With ``refresh``, grep the lock and its loaded ``DagRun`` is reloaded.
    """
    keys = set(run_keys)
    held_by, held = session.info.get(_LOCKED_DAG_RUNS_KEY, (None, frozenset()))
    transaction = session.get_nested_transaction() or session.get_transaction()
    if transaction is None or held_by is not transaction:
        held = frozenset()
    wanted = sorted(keys if refresh else keys - held)
    if not wanted:
        return
    session.execute(
        select(DagRun if refresh else DagRun.id)
        .where(tuple_(DagRun.dag_id, DagRun.run_id).in_(wanted))
        .order_by(DagRun.dag_id, DagRun.run_id)
        .with_for_update()
        .execution_options(populate_existing=refresh)
    ).all()
    # A savepoint's rollback releases the locks taken inside it, so they are recorded
    # per savepoint.
    transaction = session.get_nested_transaction() or session.get_transaction()
    session.info[_LOCKED_DAG_RUNS_KEY] = (transaction, held | frozenset(wanted))


def clear_partition_runs(
    *,
    dag: SerializedDAG | None,
    dag_id: str,
    run_id: str | None,
    partition_key: str | None,
    partition_date_start: datetime | None,
    partition_date_end: datetime | None,
    clear_tis: bool,
    dry_run: bool,
    session: Session,
    on_run_matched: Callable[[DagRun, bool], None] | None = None,
) -> tuple[int, int]:
    """
    Reset partition_key and partition_date to None on matching Dag runs.

    Selector priority: run_id → partition_key → date-window (datetime-precision, localized
    through the timetable timezone). Runs already cleared (both fields None) are skipped
    unless clear_tis is True.

    The date-window selector requires a non-None ``dag`` to resolve the timetable timezone.
    The run_id and partition_key selectors do not use ``dag`` and accept ``None``.

    Returns (dag_runs_cleared, task_instances_cleared).

    ``on_run_matched`` is called once per matched run, before any mutation, with
    ``(run, had_partition_fields)`` where ``had_partition_fields`` is True when the run's
    partition fields are non-None at the time of the call (i.e. they will be reset).

    :meta private:
    """
    from airflow.models.taskinstance import clear_task_instances_for_runs

    stmt = select(DagRun).where(DagRun.dag_id == dag_id)
    if run_id is not None:
        stmt = stmt.where(DagRun.run_id == run_id)
    elif partition_key is not None:
        stmt = stmt.where(DagRun.partition_key == partition_key)
    else:
        if dag is None:
            raise ValueError(
                "The date-window selector requires a loaded Dag to resolve the timetable timezone; "
                "dag must not be None when partition_date_start or partition_date_end is used."
            )
        stmt = stmt.where(or_(DagRun.partition_key.is_not(None), DagRun.partition_date.is_not(None)))
        stmt = DagRun.apply_partition_date_window(
            stmt,
            timetable=dag.timetable,
            start=partition_date_start,
            end=partition_date_end,
        )
    stmt = stmt.order_by(DagRun.partition_date, DagRun.run_id)

    if clear_tis and not dry_run:
        lock_dag_runs(
            session,
            session.execute(
                stmt.with_only_columns(DagRun.dag_id, DagRun.run_id)
                .where(select(TI.id).where(TI.dag_id == DagRun.dag_id, TI.run_id == DagRun.run_id).exists())
                .order_by(None)
            ).tuples(),
        )

    dag_runs_cleared = 0
    ti_buffer_run_ids: list[str] = []
    tis_cleared_total = 0
    dry_run_matched_ids: list[str] = []

    def _flush_ti_buffer() -> int:
        if not ti_buffer_run_ids:
            return 0
        tis_by_run: dict[str, list[TI]] = defaultdict(list)
        for ti in session.scalars(
            select(TI).where(TI.dag_id == dag_id, TI.run_id.in_(ti_buffer_run_ids)).order_by(TI.run_id)
        ):
            tis_by_run[ti.run_id].append(ti)
        ti_buffer_run_ids.clear()
        flushed = 0
        batch: list[TI] = []

        def clear_batch() -> None:
            nonlocal flushed
            clear_task_instances_for_runs(
                list(batch),
                session=session,
                whole_task_keys={(ti.dag_id, ti.run_id, ti.task_id) for ti in batch},
            )
            flushed += len(batch)
            batch.clear()

        for run_tis in tis_by_run.values():
            batch.extend(run_tis)
            if len(batch) >= _TI_CHUNK_SIZE:
                clear_batch()
        if batch:
            clear_batch()
        return flushed

    for run in session.scalars(stmt).yield_per(100):
        fields_already_cleared = run.partition_key is None and run.partition_date is None
        if on_run_matched is not None:
            on_run_matched(run, not fields_already_cleared)
        if fields_already_cleared and not clear_tis:
            continue
        if not fields_already_cleared:
            if not dry_run:
                run.partition_key = None
                run.partition_date = None
            dag_runs_cleared += 1
        if clear_tis:
            if dry_run:
                dry_run_matched_ids.append(run.run_id)
            else:
                ti_buffer_run_ids.append(run.run_id)
                if len(ti_buffer_run_ids) >= _TI_CHUNK_SIZE:
                    tis_cleared_total += _flush_ti_buffer()

    if clear_tis and not dry_run:
        tis_cleared_total += _flush_ti_buffer()

    if dry_run and clear_tis:
        if dry_run_matched_ids:
            for i in range(0, len(dry_run_matched_ids), _TI_CHUNK_SIZE):
                chunk = dry_run_matched_ids[i : i + _TI_CHUNK_SIZE]
                tis_cleared_total += (
                    session.scalar(
                        select(func.count())
                        .select_from(TI)
                        .where(
                            TI.dag_id == dag_id,
                            TI.run_id.in_(chunk),
                        )
                    )
                    or 0
                )

    return dag_runs_cleared, tis_cleared_total


def get_or_create_dagrun(
    *,
    dag: SerializedDAG,
    run_id: str,
    logical_date: datetime | None,
    data_interval: tuple[datetime, datetime] | None,
    run_after: datetime,
    conf: dict | None,
    triggered_by: DagRunTriggeredByType,
    triggering_user_name: str | None,
    start_date: datetime,
    session: Session,
) -> DagRun:
    """
    Create a DAG run, replacing an existing instance if needed to prevent collisions.

    This function is only meant to be used by :meth:`DAG.test` and ``airflow tasks test``
    as a helper function.

    :param dag: DAG to be used to find run.
    :param conf: Configuration to pass to newly created run.
    :param start_date: Start date of new run.
    :param logical_date: Logical date for finding an existing run to replace. ``None`` skips
        the lookup, since NULL dates cannot violate the ``(dag_id, logical_date)`` unique key.
    :param run_id: Run ID for the new DAG run.
    :param triggered_by: the entity which triggers the dag_run
    :param triggering_user_name: the user name who triggers the DagRun

    :return: The newly created DAG run.
    """
    # ``== None`` compiles to ``IS NULL``, which would match an unrelated dateless run.
    if logical_date is not None:
        dr = session.scalar(
            select(DagRun).where(DagRun.dag_id == dag.dag_id, DagRun.logical_date == logical_date)
        )
        if dr:
            session.delete(dr)
            session.flush()
    dr = dag.create_dagrun(
        run_id=run_id,
        logical_date=logical_date,
        data_interval=data_interval,
        run_after=run_after,
        conf=conf,
        run_type=DagRunType.MANUAL,
        state=DagRunState.RUNNING,
        triggered_by=triggered_by,
        triggering_user_name=triggering_user_name,
        start_date=start_date or timezone.utcnow(),
        session=session,
    )
    log.info("Created dag run.", dagrun=dr)
    return dr
