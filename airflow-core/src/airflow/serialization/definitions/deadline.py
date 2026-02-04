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
from typing import TYPE_CHECKING, Any

import attrs
from sqlalchemy import select

from airflow._shared.timezones import timezone
from airflow.models.deadline import classproperty
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.session import provide_session
from airflow.utils.sqlalchemy import get_dialect_name

if TYPE_CHECKING:
    from collections.abc import Sequence

    from sqlalchemy import ColumnElement
    from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


class DeadlineAlertFields:
    """
    Define field names used in DeadlineAlert serialization/deserialization.

    These constants provide a single source of truth for the field names used when
    serializing DeadlineAlert instances to and from their dictionary representation.
    """

    REFERENCE = "reference"
    INTERVAL = "interval"
    CALLBACK = "callback"


class SerializedReferenceModels:
    """
    Store the implementations for the different Deadline References.

    :meta private:
    """

    REFERENCE_TYPE_FIELD = "reference_type"

    @classmethod
    def get_reference_class(cls, reference_name: str) -> type[SerializedBaseDeadlineReference]:
        """
        Get a reference class by its name.

        :param reference_name: The name of the reference class to find
        """
        try:
            return next(
                ref_class
                for name, ref_class in vars(cls).items()
                if isinstance(ref_class, type)
                and issubclass(ref_class, cls.SerializedBaseDeadlineReference)
                and ref_class.__name__ == reference_name
            )
        except StopIteration:
            raise ValueError(f"No reference class found with name: {reference_name}")

    class SerializedBaseDeadlineReference(LoggingMixin, ABC):
        """Base class for all serialized Deadline implementations."""

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

            :param reference_data: Dictionary containing serialized reference data.
            """
            return cls()

        def serialize_reference(self) -> dict:
            """Serialize this reference type into a dictionary representation."""
            return {SerializedReferenceModels.REFERENCE_TYPE_FIELD: self.reference_name}

        def __eq__(self, other) -> bool:
            if not isinstance(other, SerializedReferenceModels.SerializedBaseDeadlineReference):
                return NotImplemented("Comparison not implemented for other types.")
            return self.serialize_reference() == other.serialize_reference()

        def __hash__(self) -> int:
            return hash(frozenset(self.serialize_reference().items()))

    @dataclass
    class FixedDatetimeDeadline(SerializedBaseDeadlineReference):
        """A deadline that always returns a fixed datetime."""

        _datetime: datetime

        def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
            return self._datetime

        def serialize_reference(self) -> dict:
            return {
                SerializedReferenceModels.REFERENCE_TYPE_FIELD: self.reference_name,
                "datetime": self._datetime.timestamp(),
            }

        @classmethod
        def deserialize_reference(cls, reference_data: dict):
            return cls(_datetime=timezone.from_timestamp(reference_data["datetime"]))

    class DagRunLogicalDateDeadline(SerializedBaseDeadlineReference):
        """A deadline that returns a DagRun's logical date."""

        required_kwargs = {"dag_id", "run_id"}

        def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
            from airflow.models import DagRun

            return _fetch_from_db(DagRun.logical_date, session=session, **kwargs)

    class DagRunQueuedAtDeadline(SerializedBaseDeadlineReference):
        """A deadline that returns when a DagRun was queued."""

        required_kwargs = {"dag_id", "run_id"}

        @provide_session
        def _evaluate_with(self, *, session: Session, **kwargs: Any) -> datetime | None:
            from airflow.models import DagRun

            return _fetch_from_db(DagRun.queued_at, session=session, **kwargs)

    @dataclass
    class AverageRuntimeDeadline(SerializedBaseDeadlineReference):
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
            from sqlalchemy import func, select, text

            from airflow.models import DagRun

            dag_id = kwargs["dag_id"]

            dialect = get_dialect_name(session)

            duration_expr: ColumnElement[Any]
            if dialect == "postgresql":
                duration_expr = func.extract("epoch", DagRun.end_date - DagRun.start_date)
            elif dialect == "mysql":
                duration_expr = func.timestampdiff(text("SECOND"), DagRun.start_date, DagRun.end_date)
            elif dialect == "sqlite":
                duration_expr = (func.julianday(DagRun.end_date) - func.julianday(DagRun.start_date)) * 86400
            else:
                raise ValueError(f"Unsupported database dialect: {dialect}")

            query = (
                select(duration_expr)
                .filter(DagRun.dag_id == dag_id, DagRun.start_date.isnot(None), DagRun.end_date.isnot(None))
                .order_by(DagRun.logical_date.desc())
                .limit(self.max_runs)
            )

            durations: Sequence = session.execute(query).scalars().all()

            min_runs = self.min_runs or 0
            if len(durations) < min_runs:
                logger.info(
                    "Not enough completed runs to calculate average runtime for dag_id=%s "
                    "(found %d, need %d)",
                    dag_id,
                    len(durations),
                    min_runs,
                )
                return None

            avg_duration_seconds = sum(durations) / len(durations)
            return timezone.utcnow() + timedelta(seconds=avg_duration_seconds)

        def serialize_reference(self) -> dict:
            return {
                SerializedReferenceModels.REFERENCE_TYPE_FIELD: self.reference_name,
                "max_runs": self.max_runs,
                "min_runs": self.min_runs,
            }

        @classmethod
        def deserialize_reference(cls, reference_data: dict):
            return cls(max_runs=reference_data["max_runs"], min_runs=reference_data.get("min_runs"))

    class TYPES:
        """Collection of SerializedDeadlineReference types for type checking."""

        # Deadlines that should be created when the DagRun is created.
        DAGRUN_CREATED: tuple = ()

        # Deadlines that should be created when the DagRun is queued.
        DAGRUN_QUEUED: tuple = ()

        # All DagRun-related deadline types.
        DAGRUN: tuple = ()


SerializedReferenceModels.TYPES.DAGRUN_CREATED = (
    SerializedReferenceModels.DagRunLogicalDateDeadline,
    SerializedReferenceModels.FixedDatetimeDeadline,
    SerializedReferenceModels.AverageRuntimeDeadline,
)
SerializedReferenceModels.TYPES.DAGRUN_QUEUED = (SerializedReferenceModels.DagRunQueuedAtDeadline,)
SerializedReferenceModels.TYPES.DAGRUN = (
    SerializedReferenceModels.TYPES.DAGRUN_CREATED + SerializedReferenceModels.TYPES.DAGRUN_QUEUED
)


def _fetch_from_db(column, *, session: Session, dag_id: str, run_id: str) -> datetime | None:
    """
    Fetch a datetime column from the DagRun table.

    :meta private:
    """
    from airflow.models import DagRun

    result = session.execute(select(column).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id)).scalar()
    if result is None:
        logger.warning("Could not find DagRun for dag_id=%s, run_id=%s", dag_id, run_id)
    return result


@attrs.define
class SerializedDeadlineAlert:
    """Serialized representation of a deadline alert."""

    reference: SerializedReferenceModels.SerializedBaseDeadlineReference
    interval: timedelta
    callback: Any
