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

from typing import TYPE_CHECKING, Any, NamedTuple, Protocol, TypedDict, runtime_checkable

from typing_extensions import NotRequired

from airflow._shared.module_loading import qualname
from airflow._shared.timezones import timezone
from airflow.serialization.definitions.assets import SerializedAssetBase

if TYPE_CHECKING:
    from collections.abc import Iterator, Sequence

    from pendulum import DateTime

    from airflow.models.dag import DagModel
    from airflow.models.dagrun import DagRun
    from airflow.partition_mappers import PartitionMapper
    from airflow.serialization.dag_dependency import DagDependency
    from airflow.serialization.definitions.assets import (
        SerializedAsset,
        SerializedAssetAlias,
        SerializedAssetRef,
        SerializedAssetUniqueKey,
    )
    from airflow.utils.types import DagRunType


class PartitionMapperInfo(TypedDict):
    """
    JSON-serializable snapshot of one asset's partition mapper attributes.

    Stored as ``DagModel.partition_mapper_info`` (a list of these) so the UI can
    resolve mapper attributes without deserializing the timetable on each request.
    Either ``name``, ``uri``, or both identify the asset; ``Asset.ref(name=...)``
    omits ``uri`` and ``Asset.ref(uri=...)`` omits ``name``.
    """

    is_rollup: bool
    name: NotRequired[str]
    uri: NotRequired[str]


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

    data_interval: DataInterval | None
    """The data interval this DagRun to operate over."""

    partition_date: DateTime | None
    partition_key: str | None

    @classmethod
    def exact(cls, at: DateTime) -> DagRunInfo:
        """Represent a run on an exact time."""
        return cls(
            run_after=at,
            data_interval=DataInterval.exact(at),
            partition_key=None,
            partition_date=None,
        )

    @classmethod
    def interval(cls, start: DateTime, end: DateTime) -> DagRunInfo:
        """
        Represent a run on a continuous schedule.

        In such a schedule, each data interval starts right after the previous
        one ends, and each run is scheduled right after the interval ends. This
        applies to all schedules prior to AIP-39 except ``@once`` and ``None``.
        """
        return cls(
            run_after=end,
            data_interval=DataInterval(start, end),
            partition_key=None,
            partition_date=None,
        )

    @property
    def logical_date(self: DagRunInfo) -> DateTime | None:
        """
        Infer the logical date to represent a DagRun.

        This replaces ``execution_date`` in Airflow 2.1 and prior. The idea is
        essentially the same, just a different name.
        """
        return self.data_interval.start if self.data_interval else None


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

    partitioned: bool = False
    """Whether this timetable considers asset partitions.

    This is *True* for timetables that switch scheduling to use partitions
    instead of the traditional logic based on logical dates and data intervals.
    """

    partitioned_at_runtime: bool = False
    """Whether this timetable defers partition selection to task runtime.

    *True* for :class:`~airflow.timetables.simple.PartitionAtRuntime`;
    downstream code can branch on this flag instead of using ``isinstance``.
    """

    def get_partition_mapper(self, *, name: str = "", uri: str = "") -> PartitionMapper:
        """
        Return the partition mapper for the asset identified by *name* or *uri*.

        Only called by the scheduler when ``partitioned`` is *True*. The default
        implementation raises :exc:`NotImplementedError`; timetables that set
        ``partitioned = True`` must override this.
        """
        msg = (
            f"{type(self).__name__} is not partitioned and does not define a "
            f"partition mapper (asset name={name!r}, uri={uri!r})."
        )
        raise NotImplementedError(msg)

    @property
    def partition_mapper_info(self) -> list[PartitionMapperInfo]:
        """
        JSON-serializable per-asset partition mapper attributes.

        Empty list for timetables without asset-level partition mappers (the
        default, including non-partitioned timetables and cron-driven partitioned
        timetables). Asset-driven partitioned timetables override this with one
        entry per asset (or asset ref) — see :class:`PartitionMapperInfo`.
        """
        return []

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

        # Built-in timetables from Core or SDK use class name only
        if module.startswith("airflow.timetables.") or module.startswith(
            "airflow.sdk.definitions.timetables."
        ):
            return self.__class__.__name__

        # Custom timetables use full import path
        return qualname(self.__class__)

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

    def next_dagrun_info_v2(
        self, *, last_dagrun_info: DagRunInfo | None, restriction: TimeRestriction
    ) -> DagRunInfo | None:
        """
        Provide information to schedule the next DagRun.

        The default implementation raises ``NotImplementedError``.

        :param last_dagrun_info: The DagRunInfo object of the
            Dag's last scheduled or backfilled run.
        :param restriction: Restriction to apply when scheduling the Dag run.
            See documentation of :class:`TimeRestriction` for details.

        :return: Information on when the next DagRun can be scheduled. None
            means a DagRun should not be created. This does not mean no more runs
            will be scheduled ever again for this Dag; the timetable can return
            a DagRunInfo object when asked at another time.
        """
        return self.next_dagrun_info(
            last_automated_data_interval=last_dagrun_info and last_dagrun_info.data_interval,
            restriction=restriction,
        )

    def next_run_info_from_dag_model(self, *, dag_model: DagModel) -> DagRunInfo | None:
        from airflow.models.dag import get_next_data_interval

        if (run_after := timezone.coerce_datetime(dag_model.next_dagrun_create_after)) is None:
            return None
        return DagRunInfo(
            run_after=run_after,
            data_interval=get_next_data_interval(self, dag_model),
            partition_date=timezone.coerce_datetime(dag_model.next_dagrun_partition_date),
            partition_key=dag_model.next_dagrun_partition_key,
        )

    def run_info_from_dag_run(self, *, dag_run: DagRun) -> DagRunInfo:
        from airflow.models.dag import get_run_data_interval

        return DagRunInfo(
            run_after=timezone.coerce_datetime(dag_run.run_after),
            data_interval=get_run_data_interval(self, dag_run),
            partition_date=timezone.coerce_datetime(dag_run.partition_date),
            partition_key=dag_run.partition_key,
        )


def compute_rollup_fingerprint(timetable: Timetable) -> dict:
    """
    Return the rollup-definition fingerprint for *timetable*.

    The fingerprint is a ``dict[str, Any]`` mapping ``"{name}|{uri}"`` to the
    JSON-encoded partition mapper for each partitioned asset reachable from the
    timetable's ``asset_condition``. Keys are inserted in sorted order so the
    dict is stable across Python runs.

    Non-partitioned timetables (``timetable.partitioned is False``) return an
    empty dict. The scheduler stamps this on :class:`AssetPartitionDagRun` at
    creation time and compares it on the next tick; only mapper / window changes
    trigger cleanup of a stale partition Dag run, leaving unrelated Dag edits
    untouched.

    Both the creation side (``assets/manager.py``) and the cleanup side
    (``jobs/scheduler_job_runner.py``) call this helper to guarantee the two
    fingerprints are computed by identical logic.
    """
    if not timetable.partitioned:
        return {}

    # Local import to avoid a circular dependency: encoders.py already imports
    # Timetable from this module at the top level, so a top-level import of
    # encode_partition_mapper here would create a cycle.
    from airflow.serialization.definitions.assets import SerializedAssetNameRef, SerializedAssetUriRef
    from airflow.serialization.encoders import encode_partition_mapper

    entries: dict[str, dict[str, Any]] = {}
    for unique_key, _ in timetable.asset_condition.iter_assets():
        mapper = timetable.get_partition_mapper(name=unique_key.name, uri=unique_key.uri)
        key = f"{unique_key.name}|{unique_key.uri}"
        entries[key] = encode_partition_mapper(mapper)

    for s_asset_ref in timetable.asset_condition.iter_asset_refs():
        if isinstance(s_asset_ref, SerializedAssetNameRef):
            mapper = timetable.get_partition_mapper(name=s_asset_ref.name)
            key = f"{s_asset_ref.name}|"
            entries[key] = encode_partition_mapper(mapper)
        elif isinstance(s_asset_ref, SerializedAssetUriRef):
            mapper = timetable.get_partition_mapper(uri=s_asset_ref.uri)
            key = f"|{s_asset_ref.uri}"
            entries[key] = encode_partition_mapper(mapper)

    return dict(sorted(entries.items()))
