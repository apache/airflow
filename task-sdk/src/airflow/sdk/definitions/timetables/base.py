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

from typing import TYPE_CHECKING, Any

from airflow.sdk import AssetAll

if TYPE_CHECKING:
    from collections.abc import Sequence

    from airflow.sdk.definitions.asset import BaseAsset


class BaseTimetable:
    """Base class inherited by all user-facing timetables."""

    can_be_scheduled: bool = True
    """
    Whether this timetable can actually schedule runs in an automated manner.

    This defaults to and should generally be *True* (including non periodic
    execution types like *@once* and data triggered tables), but
    ``NullTimetable`` sets this to *False*.
    """

    active_runs_limit: int | None = None
    """
    Maximum active runs that can be active at one time for a DAG.

    This is called during DAG initialization, and the return value is used as
    the DAG's default ``max_active_runs`` if not set on the DAG explicitly. This
    should generally return *None* (no limit), but some timetables may limit
    parallelism, such as :class:`~airflow.timetable.simple.ContinuousTimetable`.
    """


class Timetable(BaseTimetable):
    """
    Custom timetables should subclass this.

    Unlike built-in timetables, which only has a stub in the SDK and implements
    most logic inside Airflow Core, user-defined custom timetables contain both
    the user-facing API and the required internal logic.
    """

    periodic: bool = True
    """
    Whether this timetable runs periodically.

    This defaults to and should generally be *True*, but some special setups
    like ``schedule=None`` and ``"@once"`` set it to *False*.
    """

    run_ordering: Sequence[str] = ("data_interval_end", "logical_date")
    """
    How runs triggered from this timetable should be ordered in UI.

    This should be a list of field names on the DAG run object.
    """

    assets: BaseAsset = AssetAll()
    """The asset condition that triggers a DAG using this timetable."""

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        """
        Deserialize a timetable from data.

        This is called when a serialized DAG is deserialized. *data* will be
        whatever was returned by :func:`.serialize()` during DAG serialization.
        The default implementation creates the timetable without any arguments.
        """
        return cls()

    def serialize(self) -> dict[str, Any]:
        """
        Serialize the timetable for JSON encoding.

        This is called during DAG serialization to store timetable information
        in the database. This should return a JSON-serializable dict that will
        be fed into ``deserialize`` when the DAG is deserialized. The default
        implementation returns an empty dict.

        The serialized dict does not need to contain class identity (Airflow
        will handle this automatically), only information needed to recreate an
        object from the class.
        """
        return {}

    def validate(self) -> None:
        """
        Validate the timetable is correctly specified.

        Override this method to provide run-time validation raised when a DAG
        is put into a dagbag. The default implementation does nothing.

        This should raise ~airflow.sdk.exceptions.AirflowTimetableInvalid on a
        validation failure.
        """
        return

    @property
    def summary(self) -> str:
        """
        A short summary for the timetable.

        This is used to display the timetable in the web UI. A cron expression
        timetable, for example, can use this to display the expression. The
        default implementation returns the timetable's type name.
        """
        return type(self).__name__

    @property
    def description(self) -> str:
        """
        Human-readable description of the timetable.

        For example, this can produce something like ``'At 21:30, only on Friday'``
        from the cron expression ``'30 21 * * 5'``. This is used in the
        webserver UI.
        """
        return ""
