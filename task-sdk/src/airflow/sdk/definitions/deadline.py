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

import inspect
import logging
from abc import ABC
from collections.abc import Callable
from datetime import datetime, timedelta
from typing import Any, cast

from airflow.models.deadline import DeadlineReferenceType, ReferenceModels
from airflow.sdk.module_loading import import_string, is_valid_dotpath
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.serialization.serde import deserialize, serialize

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


class Callback(ABC):
    """
    Base class for Deadline Alert callbacks.

    Callbacks are used to execute custom logic when a deadline is missed.

    The `callback_callable` can be a Python callable type or a string containing the path to the callable that
    can be used to import the callable. It must be a top-level callable in a module present on the host where
    it will run.

    It will be called with Airflow context and specified kwargs when a deadline is missed.
    """

    path: str
    kwargs: dict | None

    def __init__(self, callback_callable: Callable | str, kwargs: dict[str, Any] | None = None):
        self.path = self.get_callback_path(callback_callable)
        if kwargs and "context" in kwargs:
            raise ValueError("context is a reserved kwarg for this class")
        self.kwargs = kwargs

    @classmethod
    def get_callback_path(cls, _callback: str | Callable) -> str:
        """Convert callback to a string path that can be used to import it later."""
        if callable(_callback):
            cls.verify_callable(_callback)

            # TODO:  This implementation doesn't support using a lambda function as a callback.
            #        We should consider that in the future, but the addition is non-trivial.
            # Get the reference path to the callable in the form `airflow.models.deadline.get_from_db`
            return f"{_callback.__module__}.{_callback.__qualname__}"

        if not isinstance(_callback, str) or not is_valid_dotpath(_callback.strip()):
            raise ImportError(f"`{_callback}` doesn't look like a valid dot path.")

        stripped_callback = _callback.strip()

        try:
            # The provided callback is a string which appears to be a valid dotpath, attempt to import it.
            callback = import_string(stripped_callback)
            if not callable(callback):
                # The input is a string which can be imported, but is not callable.
                raise AttributeError(f"Provided callback {callback} is not callable.")

            cls.verify_callable(callback)

        except ImportError as e:
            # Logging here instead of failing because it is possible that the code for the callable
            # exists somewhere other than on the DAG processor. We are making a best effort to validate,
            # but can't rule out that it may be available at runtime even if it can not be imported here.
            logger.debug(
                "Callback %s is formatted like a callable dotpath, but could not be imported.\n%s",
                stripped_callback,
                e,
            )

        return stripped_callback

    @classmethod
    def verify_callable(cls, callback: Callable):
        """For additional verification of the callable during initialization in subclasses."""
        pass  # No verification needed in the base class

    @classmethod
    def deserialize(cls, data: dict, version):
        path = data.pop("path")
        return cls(callback_callable=path, **data)

    @classmethod
    def serialized_fields(cls) -> tuple[str, ...]:
        return ("path", "kwargs")

    def serialize(self) -> dict[str, Any]:
        return {f: getattr(self, f) for f in self.serialized_fields()}

    def __eq__(self, other):
        if type(self) is not type(other):
            return NotImplemented
        return self.serialize() == other.serialize()

    def __hash__(self):
        serialized = self.serialize()
        hashable_items = []
        for k, v in serialized.items():
            if isinstance(v, dict) and v:
                hashable_items.append((k, tuple(sorted(v.items()))))
            else:
                hashable_items.append((k, v))
        return hash(tuple(sorted(hashable_items)))


class AsyncCallback(Callback):
    """
    Asynchronous callback that runs in the triggerer.

    The `callback_callable` can be a Python callable type or a string containing the path to the callable that
    can be used to import the callable. It must be a top-level awaitable callable in a module present on the
    triggerer.

    It will be called with Airflow context and specified kwargs when a deadline is missed.
    """

    def __init__(self, callback_callable: Callable | str, kwargs: dict | None = None):
        super().__init__(callback_callable=callback_callable, kwargs=kwargs)

    @classmethod
    def verify_callable(cls, callback: Callable):
        if not (inspect.iscoroutinefunction(callback) or hasattr(callback, "__await__")):
            raise AttributeError(f"Provided callback {callback} is not awaitable.")


class SyncCallback(Callback):
    """
    Synchronous callback that runs in the specified or default executor.

    The `callback_callable` can be a Python callable type or a string containing the path to the callable that
    can be used to import the callable. It must be a top-level callable in a module present on the executor.

    It will be called with Airflow context and specified kwargs when a deadline is missed.
    """

    executor: str | None

    def __init__(
        self, callback_callable: Callable | str, kwargs: dict | None = None, executor: str | None = None
    ):
        super().__init__(callback_callable=callback_callable, kwargs=kwargs)
        self.executor = executor

    @classmethod
    def serialized_fields(cls) -> tuple[str, ...]:
        return super().serialized_fields() + ("executor",)


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
        DAGRUN_CREATED = (
            ReferenceModels.DagRunLogicalDateDeadline,
            ReferenceModels.FixedDatetimeDeadline,
        )

        # Deadlines that should be created when the DagRun is queued.
        DAGRUN_QUEUED = (ReferenceModels.DagRunQueuedAtDeadline,)

        # All DagRun-related deadline types.
        DAGRUN = DAGRUN_CREATED + DAGRUN_QUEUED

    from airflow.models.deadline import ReferenceModels

    DAGRUN_LOGICAL_DATE: DeadlineReferenceType = ReferenceModels.DagRunLogicalDateDeadline()
    DAGRUN_QUEUED_AT: DeadlineReferenceType = ReferenceModels.DagRunQueuedAtDeadline()

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
