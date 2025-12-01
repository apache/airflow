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
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, cast

from airflow.models.deadline import DeadlineReferenceType, ReferenceModels
from airflow.sdk.definitions.callback import AsyncCallback, Callback
from airflow.sdk.serialization.serde import deserialize, serialize
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding

if TYPE_CHECKING:
    from collections.abc import Callable
    from typing import TypeAlias

logger = logging.getLogger(__name__)

DeadlineReferenceTypes: TypeAlias = tuple[type[ReferenceModels.BaseDeadlineReference], ...]


class DeadlineAlertFields:
    """
    Define field names used in DeadlineAlert serialization/deserialization.

    These constants provide a single source of truth for the field names used when
    serializing DeadlineAlert instances to and from their dictionary representation.
    """

    REFERENCE = "reference"
    INTERVAL = "interval"
    CALLBACK = "callback"


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

        if not isinstance(callback, AsyncCallback):
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

    def serialize_deadline_alert(self):
        """Return the data in a format that BaseSerialization can handle."""
        return {
            Encoding.TYPE: DAT.DEADLINE_ALERT,
            Encoding.VAR: {
                DeadlineAlertFields.REFERENCE: self.reference.serialize_reference(),
                DeadlineAlertFields.INTERVAL: self.interval.total_seconds(),
                DeadlineAlertFields.CALLBACK: serialize(self.callback),
            },
        }

    @classmethod
    def deserialize_deadline_alert(cls, encoded_data: dict) -> DeadlineAlert:
        """Deserialize a DeadlineAlert from serialized data."""
        data = encoded_data.get(Encoding.VAR, encoded_data)

        reference_data = data[DeadlineAlertFields.REFERENCE]
        reference_type = reference_data[ReferenceModels.REFERENCE_TYPE_FIELD]

        reference_class = ReferenceModels.get_reference_class(reference_type)
        reference = reference_class.deserialize_reference(reference_data)

        return cls(
            reference=reference,
            interval=timedelta(seconds=data[DeadlineAlertFields.INTERVAL]),
            callback=cast("Callback", deserialize(data[DeadlineAlertFields.CALLBACK])),
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
       fixed = DeadlineReference.FIXED_DATETIME(datetime(2025, 5, 4))
       logical = DeadlineReference.DAGRUN_LOGICAL_DATE
       queued = DeadlineReference.DAGRUN_QUEUED_AT

    2. Using in a DAG:
       DAG(
           dag_id='dag_with_deadline',
           deadline=DeadlineAlert(
               reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
               interval=timedelta(hours=1),
               callback=hello_callback,
           )
       )

    3. Evaluating deadlines will ignore unexpected parameters:
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
            ReferenceModels.DagRunLogicalDateDeadline,
            ReferenceModels.FixedDatetimeDeadline,
            ReferenceModels.AverageRuntimeDeadline,
        )

        # Deadlines that should be created when the DagRun is queued.
        DAGRUN_QUEUED: DeadlineReferenceTypes = (ReferenceModels.DagRunQueuedAtDeadline,)

        # All DagRun-related deadline types.
        DAGRUN: DeadlineReferenceTypes = DAGRUN_CREATED + DAGRUN_QUEUED

    from airflow.models.deadline import ReferenceModels

    DAGRUN_LOGICAL_DATE: DeadlineReferenceType = ReferenceModels.DagRunLogicalDateDeadline()
    DAGRUN_QUEUED_AT: DeadlineReferenceType = ReferenceModels.DagRunQueuedAtDeadline()

    @classmethod
    def AVERAGE_RUNTIME(cls, max_runs: int = 0, min_runs: int | None = None) -> DeadlineReferenceType:
        if max_runs == 0:
            max_runs = cls.ReferenceModels.AverageRuntimeDeadline.DEFAULT_LIMIT
        if min_runs is None:
            min_runs = max_runs
        return cls.ReferenceModels.AverageRuntimeDeadline(max_runs, min_runs)

    @classmethod
    def FIXED_DATETIME(cls, datetime: datetime) -> DeadlineReferenceType:
        return cls.ReferenceModels.FixedDatetimeDeadline(datetime)

    # TODO: Remove this once other deadline types exist.
    #   This is a temporary reference type used only in tests to verify that
    #   dag.has_dagrun_deadline() returns false if the dag has a non-dagrun deadline type.
    #   It should be replaced with a real non-dagrun deadline type when one is available.
    _TEMPORARY_TEST_REFERENCE = type(
        "TemporaryTestDeadlineForTypeChecking",
        (DeadlineReferenceType,),
        {"_evaluate_with": lambda self, **kwargs: datetime.now()},
    )()

    @classmethod
    def register_custom_reference(
        cls,
        reference_class: type[ReferenceModels.BaseDeadlineReference],
        deadline_reference_type: DeadlineReferenceTypes | None = None,
    ) -> type[ReferenceModels.BaseDeadlineReference]:
        """
        Register a custom deadline reference class.

        :param reference_class: The custom reference class inheriting from BaseDeadlineReference
        :param deadline_reference_type: A DeadlineReference.TYPES for when the deadline should be evaluated ("DAGRUN_CREATED",
            "DAGRUN_QUEUED", etc.); defaults to DeadlineReference.TYPES.DAGRUN_CREATED
        """
        from airflow.models.deadline import ReferenceModels

        # Default to DAGRUN_CREATED if no deadline_reference_type specified
        if deadline_reference_type is None:
            deadline_reference_type = cls.TYPES.DAGRUN_CREATED

        # Validate the reference class inherits from BaseDeadlineReference
        if not issubclass(reference_class, ReferenceModels.BaseDeadlineReference):
            raise ValueError(f"{reference_class.__name__} must inherit from BaseDeadlineReference")

        # Register the new reference with ReferenceModels and DeadlineReference for discoverability
        setattr(ReferenceModels, reference_class.__name__, reference_class)
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
) -> Callable[[type[ReferenceModels.BaseDeadlineReference]], type[ReferenceModels.BaseDeadlineReference]]:
    """
    Decorate a class to register a custom deadline reference.

    Usage:
        @deadline_reference()
        class MyCustomReference(ReferenceModels.BaseDeadlineReference):
            # By default, evaluate_with will be called when a new dagrun is created.
            def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
                # Put your business logic here
                return some_datetime

        @deadline_reference(DeadlineReference.TYPES.DAGRUN_QUEUED)
        class MyQueuedRef(ReferenceModels.BaseDeadlineReference):
            # Optionally, you can specify when you want it calculated by providing a DeadlineReference.TYPES
            def _evaluate_with(self, *, session: Session, **kwargs) -> datetime:
                 # Put your business logic here
                return some_datetime
    """

    def decorator(
        reference_class: type[ReferenceModels.BaseDeadlineReference],
    ) -> type[ReferenceModels.BaseDeadlineReference]:
        DeadlineReference.register_custom_reference(reference_class, deadline_reference_type)
        return reference_class

    return decorator
