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
from abc import ABC
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from airflow.sdk.definitions.callback import AsyncCallback, Callback, SyncCallback

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

logger = logging.getLogger(__name__)

# Field name used in serialization - must be in sync with SerializedReferenceModels.REFERENCE_TYPE_FIELD
REFERENCE_TYPE_FIELD = "reference_type"


class BaseDeadlineReference(ABC):
    """
    Base class for all Deadline Reference implementations.

    This is a lightweight SDK class for DAG authoring. It only handles serialization.
    The actual evaluation logic (_evaluate_with) is in Core's SerializedReferenceModels.

    For custom deadline references, users should inherit from this class and implement
    _evaluate_with() with deferred Core imports (imports inside the method body).
    """

    @property
    def reference_name(self) -> str:
        """Return the class name as the reference identifier."""
        return self.__class__.__name__

    def serialize_reference(self) -> dict[str, Any]:
        """
        Serialize this reference type into a dictionary representation.

        Override this method in subclasses if additional data is needed for serialization.
        """
        return {REFERENCE_TYPE_FIELD: self.reference_name}

    @classmethod
    def deserialize_reference(cls, reference_data: dict[str, Any]) -> BaseDeadlineReference:
        """
        Deserialize a reference type from its dictionary representation.

        :param reference_data: Dictionary containing serialized reference data.
        """
        return cls()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, BaseDeadlineReference):
            return NotImplemented
        return self.serialize_reference() == other.serialize_reference()

    def __hash__(self) -> int:
        return hash(frozenset(self.serialize_reference().items()))


class DagRunLogicalDateDeadline(BaseDeadlineReference):
    """A deadline that returns a DagRun's logical date."""


class DagRunQueuedAtDeadline(BaseDeadlineReference):
    """A deadline that returns when a DagRun was queued."""


@dataclass
class FixedDatetimeDeadline(BaseDeadlineReference):
    """A deadline that always returns a fixed datetime."""

    _datetime: datetime

    def serialize_reference(self) -> dict[str, Any]:
        return {
            REFERENCE_TYPE_FIELD: self.reference_name,
            "datetime": self._datetime.timestamp(),
        }

    @classmethod
    def deserialize_reference(cls, reference_data: dict[str, Any]) -> FixedDatetimeDeadline:
        from airflow._shared.timezones import timezone

        return cls(_datetime=timezone.from_timestamp(reference_data["datetime"]))


@dataclass
class AverageRuntimeDeadline(BaseDeadlineReference):
    """A deadline that calculates the average runtime from past DAG runs."""

    DEFAULT_LIMIT = 10
    max_runs: int
    min_runs: int | None = None

    def __post_init__(self):
        if self.min_runs is None:
            self.min_runs = self.max_runs
        if self.min_runs < 1:
            raise ValueError("min_runs must be at least 1")

    def serialize_reference(self) -> dict[str, Any]:
        return {
            REFERENCE_TYPE_FIELD: self.reference_name,
            "max_runs": self.max_runs,
            "min_runs": self.min_runs,
        }

    @classmethod
    def deserialize_reference(cls, reference_data: dict[str, Any]) -> AverageRuntimeDeadline:
        max_runs = reference_data.get("max_runs", cls.DEFAULT_LIMIT)
        min_runs = reference_data.get("min_runs", max_runs)
        if min_runs < 1:
            raise ValueError("min_runs must be at least 1")
        return cls(max_runs=max_runs, min_runs=min_runs)


DeadlineReferenceType: TypeAlias = BaseDeadlineReference
DeadlineReferenceTypes: TypeAlias = tuple[type[BaseDeadlineReference], ...]


class DeadlineAlert:
    """Store Deadline values needed to calculate the need-by timestamp and the callback information."""

    def __init__(
        self,
        reference: DeadlineReferenceType,
        interval: timedelta,
        callback: Callback,
    ):
        self.reference = reference
        self.interval = interval

        if not isinstance(callback, (AsyncCallback, SyncCallback)):
            raise ValueError(f"Callbacks of type {type(callback).__name__} are not currently supported")
        self.callback = callback

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DeadlineAlert):
            return NotImplemented
        return (
            isinstance(self.reference, type(other.reference))
            and self.interval == other.interval
            and self.callback == other.callback
        )

    def __hash__(self) -> int:
        return hash(
            (
                type(self.reference).__name__,
                self.interval,
                self.callback,
            )
        )


class DeadlineReference:
    """
    The public interface class for all DeadlineReference options.

    This class provides a unified interface for working with Deadlines, supporting both
    calculated deadlines (which fetch values from the database) and fixed deadlines
    (which return a predefined datetime).

    ------
    Usage:
    ------

    1. Example deadline references:

    .. code-block:: python

       fixed = DeadlineReference.FIXED_DATETIME(datetime(2025, 5, 4))
       logical = DeadlineReference.DAGRUN_LOGICAL_DATE
       queued = DeadlineReference.DAGRUN_QUEUED_AT

    2. Using in a DAG:

    .. code-block:: python

       DAG(
           dag_id="dag_with_deadline",
           deadline=DeadlineAlert(
               reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
               interval=timedelta(hours=1),
               callback=hello_callback,
           ),
       )

    3. Evaluating deadlines will ignore unexpected parameters:

    .. code-block:: python

       # For deadlines requiring parameters:
       deadline = DeadlineReference.DAGRUN_LOGICAL_DATE
       deadline.evaluate_with(dag_id=dag.dag_id)

       # For deadlines with no required parameters:
       deadline = DeadlineReference.FIXED_DATETIME(datetime(2025, 5, 4))
       deadline.evaluate_with()
    """

    class TYPES:
        """Collection of DeadlineReference types for type checking."""

        # Deadlines that should be created when the DagRun is created.
        DAGRUN_CREATED: DeadlineReferenceTypes = (
            DagRunLogicalDateDeadline,
            FixedDatetimeDeadline,
            AverageRuntimeDeadline,
        )

        # Deadlines that should be created when the DagRun is queued.
        DAGRUN_QUEUED: DeadlineReferenceTypes = (DagRunQueuedAtDeadline,)

        # All DagRun-related deadline types.
        DAGRUN: DeadlineReferenceTypes = DAGRUN_CREATED + DAGRUN_QUEUED

    DAGRUN_LOGICAL_DATE: DeadlineReferenceType = DagRunLogicalDateDeadline()
    DAGRUN_QUEUED_AT: DeadlineReferenceType = DagRunQueuedAtDeadline()

    @classmethod
    def AVERAGE_RUNTIME(cls, max_runs: int = 0, min_runs: int | None = None) -> DeadlineReferenceType:
        if max_runs == 0:
            max_runs = AverageRuntimeDeadline.DEFAULT_LIMIT
        if min_runs is None:
            min_runs = max_runs
        return AverageRuntimeDeadline(max_runs, min_runs)

    @classmethod
    def FIXED_DATETIME(cls, dt: datetime) -> DeadlineReferenceType:
        return FixedDatetimeDeadline(dt)

    # TODO: Remove this once other deadline types exist.
    #   This is a temporary reference type used only in tests to verify that
    #   dag.has_dagrun_deadline() returns false if the dag has a non-dagrun deadline type.
    #   It should be replaced with a real non-dagrun deadline type when one is available.
    _TEMPORARY_TEST_REFERENCE = type(
        "TemporaryTestDeadlineForTypeChecking",
        (BaseDeadlineReference,),
        {"serialize_reference": lambda self: {REFERENCE_TYPE_FIELD: "TemporaryTestDeadlineForTypeChecking"}},
    )()

    @classmethod
    def register_custom_reference(
        cls,
        reference_class: type[BaseDeadlineReference],
        deadline_reference_type: DeadlineReferenceTypes | None = None,
    ) -> type[BaseDeadlineReference]:
        """
        Register a custom deadline reference class.

        :param reference_class: The custom reference class inheriting from BaseDeadlineReference
        :param deadline_reference_type: A DeadlineReference.TYPES for when the deadline should be evaluated ("DAGRUN_CREATED",
            "DAGRUN_QUEUED", etc.); defaults to DeadlineReference.TYPES.DAGRUN_CREATED
        """
        # Default to DAGRUN_CREATED if no deadline_reference_type specified
        if deadline_reference_type is None:
            deadline_reference_type = cls.TYPES.DAGRUN_CREATED

        # Validate the reference class inherits from BaseDeadlineReference
        # Accept both sdk and core base classes for backward compatibility for now
        from airflow.models.deadline import ReferenceModels

        if not issubclass(reference_class, (BaseDeadlineReference, ReferenceModels.BaseDeadlineReference)):
            raise ValueError(f"{reference_class.__name__} must inherit from BaseDeadlineReference")

        # Register the new reference with DeadlineReference for discoverability
        setattr(cls, reference_class.__name__, reference_class())
        logger.info("Registered DeadlineReference %s", reference_class.__name__)

        # Add to appropriate deadline_reference_type classification
        if deadline_reference_type is cls.TYPES.DAGRUN_CREATED:
            cls.TYPES.DAGRUN_CREATED = cls.TYPES.DAGRUN_CREATED + (reference_class,)
        elif deadline_reference_type is cls.TYPES.DAGRUN_QUEUED:
            cls.TYPES.DAGRUN_QUEUED = cls.TYPES.DAGRUN_QUEUED + (reference_class,)
        else:
            raise ValueError(
                f"Invalid deadline reference type {deadline_reference_type}; "
                "must be a valid DeadlineReference.TYPES option."
            )

        # Refresh the combined DAGRUN tuple
        cls.TYPES.DAGRUN = cls.TYPES.DAGRUN_CREATED + cls.TYPES.DAGRUN_QUEUED

        return reference_class


def deadline_reference(
    deadline_reference_type: DeadlineReferenceTypes | None = None,
) -> Callable[[type[BaseDeadlineReference]], type[BaseDeadlineReference]]:
    """
    Decorate a class to register a custom deadline reference.

    Usage:
        @deadline_reference()
        class MyCustomReference(BaseDeadlineReference):
            # By default, evaluate_with will be called when a new dagrun is created.
            def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
                # Put your business logic here (use deferred imports for Core types)
                from airflow.models import DagRun
                return some_datetime

            def serialize_reference(self) -> dict:
                return {"reference_type": self.reference_name}

        @deadline_reference(DeadlineReference.TYPES.DAGRUN_QUEUED)
        class MyQueuedRef(BaseDeadlineReference):
            # Optionally, you can specify when you want it calculated by providing a DeadlineReference.TYPES
            def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
                 # Put your business logic here
                return some_datetime

            def serialize_reference(self) -> dict:
                return {"reference_type": self.reference_name}
    """

    def decorator(
        reference_class: type[BaseDeadlineReference],
    ) -> type[BaseDeadlineReference]:
        DeadlineReference.register_custom_reference(reference_class, deadline_reference_type)
        return reference_class

    return decorator
