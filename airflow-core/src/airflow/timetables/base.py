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

from typing import TYPE_CHECKING, Any, NamedTuple, Protocol, runtime_checkable

from airflow.serialization.definitions.assets import SerializedAssetBase

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from pendulum import DateTime

    from airflow.serialization.dag_dependency import DagDependency
    from airflow.serialization.definitions.assets import (
        SerializedAsset,
        SerializedAssetAlias,
        SerializedAssetRef,
        SerializedAssetUniqueKey,
    )
    from airflow.utils.types import DagRunType


class DataInterval(NamedTuple):
    """
    A data interval for a DagRun to operate over.

    Both ``start`` and ``end`` **MUST** be "aware", i.e. contain timezone
    information.
    """

    start: DateTime
    end: DateTime

    @classmethod
    def exact(cls, at: DateTime) -> DataInterval:
        """Represent an "interval" containing only an exact time."""
        return cls(start=at, end=at)


class TimeRestriction(NamedTuple):
    """
    Restriction on when a DAG can be scheduled for a run.

    Specifically, the run must not be earlier than ``earliest``, nor later than
    ``latest``. If ``catchup`` is *False*, the run must also not be earlier than
    the current time, i.e. "missed" schedules are not backfilled.

    These values are generally set on the DAG or task's ``start_date``,
    ``end_date``, and ``catchup`` arguments.

    Both ``earliest`` and ``latest``, if not *None*, are inclusive; a DAG run
    can happen exactly at either point of time. They are guaranteed to be aware
    (i.e. contain timezone information) for ``TimeRestriction`` instances
    created by Airflow.
    """

    earliest: DateTime | None
    latest: DateTime | None
    catchup: bool


class _NullAsset(SerializedAssetBase):
    """
    Sentinel type that represents "no assets".

    This is only implemented to make typing easier in timetables, and not
    expected to be used anywhere else.

    :meta private:
    """

    def __bool__(self) -> bool:
        return False

    def as_expression(self) -> Any:
        return None

    def iter_assets(self) -> Iterator[tuple[SerializedAssetUniqueKey, SerializedAsset]]:
        return iter(())

    def iter_asset_aliases(self) -> Iterator[tuple[str, SerializedAssetAlias]]:
        return iter(())

    def iter_asset_refs(self) -> Iterator[SerializedAssetRef]:
        return iter(())

    def iter_dag_dependencies(self, source, target) -> Iterator[DagDependency]:
        return iter(())


class DagRunInfo(NamedTuple):
    """
    Information to schedule a DagRun.

    Instances of this will be returned by timetables when they are asked to
    schedule a DagRun creation.
    """

    run_after: DateTime
    """The earliest time this DagRun is created and its tasks scheduled.

    This **MUST** be "aware", i.e. contain timezone information.
    """

    data_interval: DataInterval
    """The data interval this DagRun to operate over."""

    @classmethod
    def exact(cls, at: DateTime) -> DagRunInfo:
        """Represent a run on an exact time."""
        return cls(run_after=at, data_interval=DataInterval.exact(at))

    @classmethod
    def interval(cls, start: DateTime, end: DateTime) -> DagRunInfo:
        """
        Represent a run on a continuous schedule.

        In such a schedule, each data interval starts right after the previous
        one ends, and each run is scheduled right after the interval ends. This
        applies to all schedules prior to AIP-39 except ``@once`` and ``None``.
        """
        return cls(run_after=end, data_interval=DataInterval(start, end))

    @property
    def logical_date(self: DagRunInfo) -> DateTime:
        """
        Infer the logical date to represent a DagRun.

        This replaces ``execution_date`` in Airflow 2.1 and prior. The idea is
        essentially the same, just a different name.
        """
        return self.data_interval.start


@runtime_checkable
class Timetable(Protocol):
    """Protocol that all Timetable classes are expected to implement."""

    description: str = ""
    """Human-readable description of the timetable.

    For example, this can produce something like ``'At 21:30, only on Friday'``
    from the cron expression ``'30 21 * * 5'``. This is used in the webserver UI.
    """

    periodic: bool = True
    """Whether this timetable runs periodically.

    This defaults to and should generally be *True*, but some special setups
    like ``schedule=None`` and ``"@once"`` set it to *False*.
    """

    # TODO (GH-52141): Find a way to keep this and one in Core in sync.
    can_be_scheduled: bool = True
    """
    Whether this timetable can actually schedule runs in an automated manner.

    This defaults to and should generally be *True* (including non periodic
    execution types like *@once* and data triggered tables), but
    ``NullTimetable`` sets this to *False*.
    """

    run_ordering: Sequence[str] = ("data_interval_end", "logical_date")
    """How runs triggered from this timetable should be ordered in UI.

    This should be a list of field names on the DAG run object.
    """

    # TODO (GH-52141): Find a way to keep this and one in Core in sync.
    active_runs_limit: int | None = None
    """Maximum active runs that can be active at one time for a DAG.

    This is called during DAG initialization, and the return value is used as
    the DAG's default ``max_active_runs``. This should generally return *None*,
    but there are good reasons to limit DAG run parallelism in some cases, such
    as for :class:`~airflow.timetable.simple.ContinuousTimetable`.
    """

    asset_condition: SerializedAssetBase = _NullAsset()
    """The asset condition that triggers a DAG using this timetable."""

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        """
        Deserialize a timetable from data.

        This is called when a serialized DAG is deserialized. ``data`` will be
        whatever was returned by ``serialize`` during DAG serialization. The
        default implementation constructs the timetable without any arguments.
        """
        return cls()

    def serialize(self) -> dict[str, Any]:
        """
        Serialize the timetable for JSON encoding.

        This is called during DAG serialization to store timetable information
        in the database. This should return a JSON-serializable dict that will
        be fed into ``deserialize`` when the DAG is deserialized. The default
        implementation returns an empty dict.
        """
        return {}

    def validate(self) -> None:
        """
        Validate the timetable is correctly specified.

        Override this method to provide run-time validation raised when a DAG
        is put into a dagbag. The default implementation does nothing.

        :raises: AirflowTimetableInvalid on validation failure.
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
    def type_name(self) -> str:
        """
        This is primarily intended for filtering dags based on timetable type.

        For built-in timetables (defined in airflow.timetables or
        airflow.sdk.definitions.timetables), this returns the class name only.
        For custom timetables (user-defined via plugins), this returns the full
        import path to avoid confusion between multiple implementations with the
        same class name.

        For example, built-in timetables return:
        ``"NullTimetable"`` or ``"CronDataIntervalTimetable"``
        while custom timetables return the full path:
        ``"my_company.timetables.CustomTimetable"``
        """
        module = self.__class__.__module__
        class_name = self.__class__.__name__

        # Built-in timetables from Core or SDK use class name only
        if module.startswith("airflow.timetables.") or module.startswith(
            "airflow.sdk.definitions.timetables."
        ):
            return class_name

        # Custom timetables use full import path
        return f"{module}.{class_name}"

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        """
        When a DAG run is manually triggered, infer a data interval for it.

        This is used for e.g. manually-triggered runs, where ``run_after`` would
        be when the user triggers the run. The default implementation raises
        ``NotImplementedError``.
        """
        raise NotImplementedError()

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        """
        Provide information to schedule the next DagRun.

        The default implementation raises ``NotImplementedError``.

        :param last_automated_data_interval: The data interval of the associated
            DAG's last scheduled or backfilled run (manual runs not considered).
        :param restriction: Restriction to apply when scheduling the DAG run.
            See documentation of :class:`TimeRestriction` for details.

        :return: Information on when the next DagRun can be scheduled. None
            means a DagRun will not happen. This does not mean no more runs
            will be scheduled even again for this DAG; the timetable can return
            a DagRunInfo object when asked at another time.
        """
        raise NotImplementedError()

    def generate_run_id(
        self,
        *,
        run_type: DagRunType,
        run_after: DateTime,
        data_interval: DataInterval | None,
        **extra,
    ) -> str:
        """
        Generate a unique run ID.

        :param run_type: The type of DAG run.
        :param run_after: the datetime before which to Dag cannot run.
        :param data_interval: The data interval of the DAG run.
        """
        return run_type.generate_run_id(suffix=run_after.isoformat())
