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

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import uuid6
from sqlalchemy import Boolean, ForeignKey, Index, Integer, and_, func, inspect, select, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Mapped, relationship
from sqlalchemy_utils import UUIDType

from airflow._shared.observability.stats import Stats
from airflow._shared.timezones import timezone
from airflow.models.base import Base
from airflow.models.callback import Callback, CallbackDefinitionProtocol
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import UtcDateTime, get_dialect_name, mapped_column

if TYPE_CHECKING:
    from sqlalchemy.orm import Session
    from sqlalchemy.sql import ColumnElement


logger = logging.getLogger(__name__)

CALLBACK_METRICS_PREFIX = "deadline_alerts"


class classproperty:
    """
    Decorator that converts a method with a single cls argument into a property.

    Mypy won't let us use both @property and @classmethod together, this is a workaround
    to combine the two.

    Usage:

    class Circle:
        def __init__(self, radius):
            self.radius = radius

        @classproperty
        def pi(cls):
            return 3.14159

    print(Circle.pi)  # Outputs: 3.14159
    """

    def __init__(self, method):
        self.method = method

    def __get__(self, instance, cls=None):
        return self.method(cls)


class Deadline(Base):
    """A Deadline is a 'need-by' date which triggers a callback if the provided time has passed."""

    __tablename__ = "deadline"

    id: Mapped[str] = mapped_column(UUIDType(binary=False), primary_key=True, default=uuid6.uuid7)

    # If the Deadline Alert is for a DAG, store the DAG run ID from the dag_run.
    dagrun_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("dag_run.id", ondelete="CASCADE"), nullable=True
    )
    dagrun = relationship("DagRun", back_populates="deadlines")

    # The time after which the Deadline has passed and the callback should be triggered.
    deadline_time: Mapped[datetime] = mapped_column(UtcDateTime, nullable=False)

    # Whether the deadline has been marked as missed by the scheduler
    missed: Mapped[bool] = mapped_column(Boolean, nullable=False)

    # Callback that will run when this deadline is missed
    callback_id: Mapped[str] = mapped_column(
        UUIDType(binary=False), ForeignKey("callback.id", ondelete="CASCADE"), nullable=False
    )
    callback = relationship("Callback", uselist=False, cascade="all, delete-orphan", single_parent=True)

    __table_args__ = (Index("deadline_missed_deadline_time_idx", missed, deadline_time, unique=False),)

    def __init__(
        self,
        deadline_time: datetime,
        callback: CallbackDefinitionProtocol,
        dagrun_id: int,
        dag_id: str | None = None,
    ):
        super().__init__()
        self.deadline_time = deadline_time
        self.dagrun_id = dagrun_id
        self.missed = False
        self.callback = Callback.create_from_sdk_def(
            callback_def=callback, prefix=CALLBACK_METRICS_PREFIX, dag_id=dag_id
        )

    def __repr__(self):
        def _determine_resource() -> tuple[str, str]:
            """Determine the type of resource based on which values are present."""
            if self.dagrun_id:
                # The deadline is for a Dag run:
                return "DagRun", f"Dag: {self.dagrun.dag_id} Run: {self.dagrun_id}"

            return "Unknown", ""

        resource_type, resource_details = _determine_resource()

        return (
            f"[{resource_type} Deadline] {resource_details} needed by "
            f"{self.deadline_time} or run: {self.callback}"
        )

    @classmethod
    def prune_deadlines(cls, *, session: Session, conditions: dict[Mapped, Any]) -> int:
        """
        Remove deadlines from the table which match the provided conditions and return the number removed.

        NOTE: This should only be used to remove deadlines which are associated with
            successful events (DagRuns, etc). If the deadline was missed, it will be
            handled by the scheduler.

        :param conditions: Dictionary of conditions to evaluate against.
        :param session: Session to use.
        """
        from airflow.models import DagRun  # Avoids circular import

        # Assemble the filter conditions.
        filter_conditions = [column == value for column, value in conditions.items()]
        if not filter_conditions:
            return 0

        try:
            # Get deadlines which match the provided conditions and their associated DagRuns.
            deadline_dagrun_pairs = session.execute(
                select(Deadline, DagRun).join(DagRun).where(and_(*filter_conditions))
            ).all()

        except AttributeError as e:
            logger.exception("Error resolving deadlines: %s", e)
            raise

        if not deadline_dagrun_pairs:
            return 0

        deleted_count = 0
        dagruns_to_refresh = set()

        for deadline, dagrun in deadline_dagrun_pairs:
            if dagrun.end_date <= deadline.deadline_time:
                # If the DagRun finished before the Deadline:
                session.delete(deadline)
                Stats.incr(
                    "deadline_alerts.deadline_not_missed",
                    tags={"dag_id": dagrun.dag_id, "dagrun_id": dagrun.run_id},
                )
                deleted_count += 1
                dagruns_to_refresh.add(dagrun)
        session.flush()

        logger.debug("%d deadline records were deleted matching the conditions %s", deleted_count, conditions)

        # Refresh any affected DAG runs.
        for dagrun in dagruns_to_refresh:
            session.refresh(dagrun)

        return deleted_count

    def handle_miss(self, session: Session):
        """Handle a missed deadline by queueing the callback."""

        def get_simple_context():
            from airflow.api_fastapi.core_api.datamodels.dag_run import DAGRunResponse
            from airflow.models import DagRun

            # TODO: Use the TaskAPI from within Triggerer to fetch full context instead of sending this context
            #  from the scheduler

            # Fetch the DagRun from the database again to avoid errors when self.dagrun's relationship fields
            # are not in the current session.
            dagrun = session.get(DagRun, self.dagrun_id)

            return {
                "dag_run": DAGRunResponse.model_validate(dagrun).model_dump(mode="json"),
                "deadline": {"id": self.id, "deadline_time": self.deadline_time},
            }

        self.callback.data["kwargs"] = self.callback.data["kwargs"] | {"context": get_simple_context()}
        self.missed = True
        self.callback.queue()
        session.add(self)
        Stats.incr(
            "deadline_alerts.deadline_missed",
            tags={"dag_id": self.dagrun.dag_id, "dagrun_id": self.dagrun.run_id},
        )


class ReferenceModels:
    """
    Store the implementations for the different Deadline References.

    After adding the implementations here, all DeadlineReferences should be added
    to the user interface in airflow.sdk.definitions.deadline.DeadlineReference
    """

    REFERENCE_TYPE_FIELD = "reference_type"

    @classmethod
    def get_reference_class(cls, reference_name: str) -> type[BaseDeadlineReference]:
        """
        Get a reference class by its name.

        :param reference_name: The name of the reference class to find
        """
        try:
            return next(
                ref_class
                for name, ref_class in vars(cls).items()
                if isinstance(ref_class, type)
                and issubclass(ref_class, cls.BaseDeadlineReference)
                and ref_class.__name__ == reference_name
            )
        except StopIteration:
            raise ValueError(f"No reference class found with name: {reference_name}")

    class BaseDeadlineReference(LoggingMixin, ABC):
        """Base class for all Deadline implementations."""

        # Set of required kwargs - subclasses should override this.
        required_kwargs: set[str] = set()

        @classproperty
        def reference_name(cls: Any) -> str:
            return cls.__name__

        def evaluate_with(self, *, session: Session, interval: timedelta, **kwargs: Any) -> datetime | None:
            """Validate the provided kwargs and evaluate this deadline with the given conditions."""
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in self.required_kwargs}

            if missing_kwargs := self.required_kwargs - filtered_kwargs.keys():
                raise ValueError(
                    f"{self.__class__.__name__} is missing required parameters: {', '.join(missing_kwargs)}"
                )

            if extra_kwargs := kwargs.keys() - filtered_kwargs.keys():
                self.log.debug("Ignoring unexpected parameters: %s", ", ".join(extra_kwargs))

            base_time = self._evaluate_with(session=session, **filtered_kwargs)
            return base_time + interval if base_time is not None else None

        @abstractmethod
        def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
            """Must be implemented by subclasses to perform the actual evaluation."""
            raise NotImplementedError

        @classmethod
        def deserialize_reference(cls, reference_data: dict):
            """
            Deserialize a reference type from its dictionary representation.

            While the base implementation doesn't use reference_data, this parameter is required
            for subclasses that need additional data for initialization (like FixedDatetimeDeadline
            which needs a datetime value).

            :param reference_data: Dictionary containing serialized reference data.
                Always includes a 'reference_type' field, and may include additional
                fields needed by specific reference implementations.
            """
            return cls()

        def serialize_reference(self) -> dict:
            """
            Serialize this reference type into a dictionary representation.

            This method assumes that the reference doesn't require any additional data.
            Override this method in subclasses if additional data is needed for serialization.
            """
            return {ReferenceModels.REFERENCE_TYPE_FIELD: self.reference_name}

    @dataclass
    class FixedDatetimeDeadline(BaseDeadlineReference):
        """A deadline that always returns a fixed datetime."""

        _datetime: datetime

        def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
            return self._datetime

        def serialize_reference(self) -> dict:
            return {
                ReferenceModels.REFERENCE_TYPE_FIELD: self.reference_name,
                "datetime": self._datetime.timestamp(),
            }

        @classmethod
        def deserialize_reference(cls, reference_data: dict):
            return cls(_datetime=timezone.from_timestamp(reference_data["datetime"]))

    class DagRunLogicalDateDeadline(BaseDeadlineReference):
        """A deadline that returns a DagRun's logical date."""

        required_kwargs = {"dag_id", "run_id"}

        def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
            from airflow.models import DagRun

            return _fetch_from_db(DagRun.logical_date, session=session, **kwargs)

    class DagRunQueuedAtDeadline(BaseDeadlineReference):
        """A deadline that returns when a DagRun was queued."""

        required_kwargs = {"dag_id", "run_id"}

        @provide_session
        def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
            from airflow.models import DagRun

            return _fetch_from_db(DagRun.queued_at, session=session, **kwargs)

    @dataclass
    class AverageRuntimeDeadline(BaseDeadlineReference):
        """A deadline that calculates the average runtime from past DAG runs."""

        DEFAULT_LIMIT = 10
        max_runs: int
        min_runs: int | None = None
        required_kwargs = {"dag_id"}

        def __post_init__(self):
            if self.min_runs is None:
                self.min_runs = self.max_runs
            if self.min_runs < 1:
                raise ValueError("min_runs must be at least 1")

        @provide_session
        def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
            from airflow.models import DagRun

            dag_id = kwargs["dag_id"]

            # Get database dialect to use appropriate time difference calculation
            dialect = get_dialect_name(session)

            # Create database-specific expression for calculating duration in seconds
            duration_expr: ColumnElement[Any]
            if dialect == "postgresql":
                duration_expr = func.extract("epoch", DagRun.end_date - DagRun.start_date)
            elif dialect == "mysql":
                # Use TIMESTAMPDIFF to get exact seconds like PostgreSQL EXTRACT(epoch FROM ...)
                duration_expr = func.timestampdiff(text("SECOND"), DagRun.start_date, DagRun.end_date)
            elif dialect == "sqlite":
                duration_expr = (func.julianday(DagRun.end_date) - func.julianday(DagRun.start_date)) * 86400
            else:
                raise ValueError(f"Unsupported database dialect: {dialect}")

            # Query for completed DAG runs with both start and end dates
            # Order by logical_date descending to get most recent runs first
            query = (
                select(duration_expr)
                .filter(DagRun.dag_id == dag_id, DagRun.start_date.isnot(None), DagRun.end_date.isnot(None))
                .order_by(DagRun.logical_date.desc())
            )

            # Apply max_runs
            query = query.limit(self.max_runs)

            # Get all durations and calculate average
            durations = session.execute(query).scalars().all()

            if len(durations) < cast("int", self.min_runs):
                logger.info(
                    "Only %d completed DAG runs found for dag_id: %s (need %d), skipping deadline creation",
                    len(durations),
                    dag_id,
                    self.min_runs,
                )
                return None
            # Convert to float to handle Decimal types from MySQL while preserving precision
            # Use Decimal arithmetic for higher precision, then convert to float
            from decimal import Decimal

            decimal_durations = [Decimal(str(d)) for d in durations]
            avg_seconds = float(sum(decimal_durations) / len(decimal_durations))
            logger.info(
                "Average runtime for dag_id %s (from %d runs): %.2f seconds",
                dag_id,
                len(durations),
                avg_seconds,
            )
            return timezone.utcnow() + timedelta(seconds=avg_seconds)

        def serialize_reference(self) -> dict:
            return {
                ReferenceModels.REFERENCE_TYPE_FIELD: self.reference_name,
                "max_runs": self.max_runs,
                "min_runs": self.min_runs,
            }

        @classmethod
        def deserialize_reference(cls, reference_data: dict):
            max_runs = reference_data.get("max_runs", cls.DEFAULT_LIMIT)
            min_runs = reference_data.get("min_runs", max_runs)
            if min_runs < 1:
                raise ValueError("min_runs must be at least 1")
            return cls(
                max_runs=max_runs,
                min_runs=min_runs,
            )


DeadlineReferenceType = ReferenceModels.BaseDeadlineReference


@provide_session
def _fetch_from_db(model_reference: Mapped, session=None, **conditions) -> datetime | None:
    """
    Fetch a datetime value from the database using the provided model reference and filtering conditions.

    For example, to fetch a TaskInstance's start_date:
        _fetch_from_db(
            TaskInstance.start_date, dag_id='example_dag', task_id='example_task', run_id='example_run'
        )

    This generates SQL equivalent to:
        SELECT start_date
        FROM task_instance
        WHERE dag_id = 'example_dag'
            AND task_id = 'example_task'
            AND run_id = 'example_run'

    :param model_reference: SQLAlchemy Column to select (e.g., DagRun.logical_date, TaskInstance.start_date)
    :param conditions: Filtering conditions applied as equality comparisons in the WHERE clause.
                       Multiple conditions are combined with AND.
    :param session: SQLAlchemy session (auto-provided by decorator)
    """
    query = select(model_reference)

    for key, value in conditions.items():
        inspected = inspect(model_reference)
        if inspected is not None:
            query = query.where(getattr(inspected.class_, key) == value)

    compiled_query = query.compile(compile_kwargs={"literal_binds": True})
    pretty_query = "\n    ".join(str(compiled_query).splitlines())
    logger.debug(
        "Executing query:\n    %r\nAs SQL:\n    %s",
        query,
        pretty_query,
    )

    try:
        result = session.scalar(query)
    except SQLAlchemyError:
        logger.exception("Database query failed.")
        raise

    if result is None:
        message = f"No matching record found in the database for query:\n    {pretty_query}"
        logger.error(message)
        raise ValueError(message)

    return result
