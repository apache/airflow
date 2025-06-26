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
from typing import TYPE_CHECKING, Callable

from airflow.models.deadline import ReferenceModels
from airflow.serialization.enums import DagAttributeTypes as DAT, Encoding
from airflow.utils.module_loading import import_string, is_valid_dotpath

if TYPE_CHECKING:
    from airflow.models.deadline import DeadlineReferenceType

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
    CALLBACK_KWARGS = "callback_kwargs"


class DeadlineAlert:
    """Store Deadline values needed to calculate the need-by timestamp and the callback information."""

    def __init__(
        self,
        reference: DeadlineReferenceType,
        interval: timedelta,
        callback: Callable | str,
        callback_kwargs: dict | None = None,
    ):
        self.reference = reference
        self.interval = interval
        self.callback_kwargs = callback_kwargs or {}
        self.callback = self.get_callback_path(callback)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, DeadlineAlert):
            return NotImplemented
        return (
            isinstance(self.reference, type(other.reference))
            and self.interval == other.interval
            and self.callback == other.callback
            and self.callback_kwargs == other.callback_kwargs
        )

    def __hash__(self) -> int:
        return hash(
            (
                type(self.reference).__name__,
                self.interval,
                self.callback,
                tuple(sorted(self.callback_kwargs.items())) if self.callback_kwargs else None,
            )
        )

    @staticmethod
    def get_callback_path(_callback: str | Callable) -> str:
        """Convert callback to a string path that can be used to import it later."""
        if callable(_callback):
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

    def serialize_deadline_alert(self):
        """Return the data in a format that BaseSerialization can handle."""
        return {
            Encoding.TYPE: DAT.DEADLINE_ALERT,
            Encoding.VAR: {
                DeadlineAlertFields.REFERENCE: self.reference.serialize_reference(),
                DeadlineAlertFields.INTERVAL: self.interval.total_seconds(),
                DeadlineAlertFields.CALLBACK: self.callback,  # Already stored as a string path
                DeadlineAlertFields.CALLBACK_KWARGS: self.callback_kwargs,
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
            callback=data[DeadlineAlertFields.CALLBACK],  # Keep as string path
            callback_kwargs=data[DeadlineAlertFields.CALLBACK_KWARGS],
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

    from airflow.models.deadline import ReferenceModels

    DAGRUN_LOGICAL_DATE: DeadlineReferenceType = ReferenceModels.DagRunLogicalDateDeadline()
    DAGRUN_QUEUED_AT: DeadlineReferenceType = ReferenceModels.DagRunQueuedAtDeadline()

    @classmethod
    def FIXED_DATETIME(cls, datetime: datetime) -> DeadlineReferenceType:
        return cls.ReferenceModels.FixedDatetimeDeadline(datetime)
