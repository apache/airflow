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

from contextlib import suppress
from datetime import datetime
from typing import TYPE_CHECKING, Any, TypeAlias

import structlog

from airflow._shared.timezones import timezone
from airflow.partition_mappers.identity import IdentityMapper
from airflow.serialization.definitions.assets import (
    SerializedAsset,
    SerializedAssetAlias,
    SerializedAssetAll,
    SerializedAssetBase,
    SerializedAssetNameRef,
    SerializedAssetUriRef,
)
from airflow.serialization.encoders import encode_asset_like, encode_partition_mapper
from airflow.timetables.base import DagRunInfo, DataInterval, PartitionMapperInfo, Timetable

try:
    from airflow.sdk.definitions.asset import BaseAsset
    from airflow.serialization.encoders import ensure_serialized_asset
except ModuleNotFoundError:
    BaseAsset: TypeAlias = SerializedAssetBase  # type: ignore[no-redef]

    def ensure_serialized_asset(o):  # type: ignore[misc,no-redef]
        return o


log = structlog.get_logger()

if TYPE_CHECKING:
    from collections.abc import Collection, Sequence

    from pendulum import DateTime

    from airflow.partition_mappers.base import PartitionMapper
    from airflow.timetables.base import TimeRestriction
    from airflow.utils.types import DagRunType


class _TrivialTimetable(Timetable):
    """Some code reuse for "trivial" timetables that has nothing complex."""

    periodic = False
    run_ordering: Sequence[str] = ("logical_date",)

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        return cls()

    def __eq__(self, other: object) -> bool:
        """
        As long as *other* is of the same type.

        This is only for testing purposes and should not be relied on otherwise.
        """
        from airflow.serialization.encoders import coerce_to_core_timetable

        if not isinstance(other := coerce_to_core_timetable(other), type(self)):
            return NotImplemented
        return True

    def __hash__(self):
        return hash(self.__class__.__name__)

    def infer_manual_data_interval(self, *, run_after: DateTime) -> DataInterval:
        return DataInterval.exact(run_after)


class NullTimetable(_TrivialTimetable):
    """
    Timetable that never schedules anything.

    This corresponds to ``schedule=None``.
    """

    can_be_scheduled = False  # TODO (GH-52141): Find a way to keep this and one in Core in sync.
    partitioned_at_runtime = False
    description: str = "Never, external triggers only"

    @property
    def summary(self) -> str:
        return "None"

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        return None


class OnceTimetable(_TrivialTimetable):
    """
    Timetable that schedules the execution once as soon as possible.

    This corresponds to ``schedule="@once"``.
    """

    description: str = "Once, as soon as possible"

    @property
    def summary(self) -> str:
        return "@once"

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        if last_automated_data_interval is not None:
            return None  # Already run, no more scheduling.
        # If the user does not specify an explicit start_date, the dag is ready.
        run_after = restriction.earliest or timezone.coerce_datetime(timezone.utcnow())
        # "@once" always schedule to the start_date determined by the DAG and
        # tasks, regardless of catchup or not. This has been the case since 1.10
        # and we're inheriting it.
        if restriction.latest is not None and run_after > restriction.latest:
            return None
        return DagRunInfo.exact(run_after)


class ContinuousTimetable(_TrivialTimetable):
    """
    Timetable that schedules continually, while still respecting start_date and end_date.

    This corresponds to ``schedule="@continuous"``.
    """

    description: str = "As frequently as possible, but only one run at a time."

    # TODO (GH-52141): Find a way to keep this and one in Core in sync.
    active_runs_limit = 1  # Continuous DAGRuns should be constrained to one run at a time

    @property
    def summary(self) -> str:
        return "@continuous"

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        current_time = timezone.coerce_datetime(timezone.utcnow())
        start_date = restriction.earliest or current_time

        if last_automated_data_interval is not None:  # has already run once
            if last_automated_data_interval.end > current_time:  # start date is future
                start = start_date
                elapsed = last_automated_data_interval.end - last_automated_data_interval.start

                end = start + elapsed.as_timedelta()
            else:
                start = last_automated_data_interval.end
                end = current_time
        else:  # first run
            start = start_date
            end = max(start_date, current_time)

        if restriction.latest is not None and end > restriction.latest:
            return None

        return DagRunInfo.interval(start, end)


class PartitionAtRuntime(NullTimetable):
    """
    Timetable that never schedules anything; partition keys are set at runtime.

    This corresponds to ``schedule=PartitionAtRuntime()``.

    A run's ``partition_key`` (run-level provenance) must be supplied at trigger
    time — for example via the REST API's ``partition_key`` field. Partition keys
    discovered at task runtime populate the emitted :class:`~airflow.sdk.AssetEvent`
    records but do **not** back-fill ``DagRun.partition_key`` after the run has
    been created.
    """

    description: str = "Never, partition key(s) set at runtime"
    partitioned_at_runtime = True

    @property
    def summary(self) -> str:
        return "PartitionAtRuntime"


class AssetTriggeredTimetable(_TrivialTimetable):
    """
    Timetable that never schedules anything.

    This should not be directly used anywhere, but only set if a DAG is triggered by assets.

    :meta private:
    """

    description: str = "Triggered by assets"

    def __init__(self, assets: Collection[SerializedAsset] | SerializedAssetBase) -> None:
        super().__init__()
        # Compatibility: Handle SDK assets if needed so this class works in dag files.
        if isinstance(assets, SerializedAssetBase | BaseAsset):
            self.asset_condition = ensure_serialized_asset(assets)
        else:
            self.asset_condition = SerializedAssetAll([ensure_serialized_asset(a) for a in assets])

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> Timetable:
        from airflow.serialization.decoders import decode_asset_like

        return cls(decode_asset_like(data["asset_condition"]))

    @property
    def summary(self) -> str:
        return "Asset"

    def serialize(self) -> dict[str, Any]:
        return {"asset_condition": encode_asset_like(self.asset_condition)}

    def generate_run_id(
        self,
        *,
        run_type: DagRunType,
        data_interval: DataInterval | None,
        run_after: DateTime,
        **extra,
    ) -> str:
        """
        Generate Run ID based on Run Type, run_after and logical Date.

        :param run_type: type of DagRun
        :param data_interval: the data interval
        :param run_after: the date before which dag run won't start.
        """
        from airflow.models.dagrun import DagRun

        logical_date = data_interval.start if data_interval is not None else run_after

        return DagRun.generate_run_id(run_type=run_type, logical_date=logical_date, run_after=run_after)

    def next_dagrun_info(
        self,
        *,
        last_automated_data_interval: DataInterval | None,
        restriction: TimeRestriction,
    ) -> DagRunInfo | None:
        return None


DEFAULT_PARTITION_MAPPER = IdentityMapper()


class PartitionedAssetTimetable(AssetTriggeredTimetable):
    """Asset-driven timetable that listens for partitioned assets."""

    partitioned = True

    @property
    def summary(self) -> str:
        return "Partitioned Asset"

    def __init__(
        self,
        *,
        assets: SerializedAssetBase,
        partition_mapper_config: dict[SerializedAssetBase, PartitionMapper] | None = None,
        default_partition_mapper: PartitionMapper = DEFAULT_PARTITION_MAPPER,
    ) -> None:
        super().__init__(assets=assets)
        self.partition_mapper_config = partition_mapper_config or {}
        self.default_partition_mapper = default_partition_mapper

        self._name_to_partition_mapper: dict[str, PartitionMapper] = {}
        self._uri_to_partition_mapper: dict[str, PartitionMapper] = {}
        self._build_name_uri_mapping()

    def _build_name_uri_mapping(self) -> None:
        for base_asset, partition_mapper in self.partition_mapper_config.items():
            for unique_key, _ in base_asset.iter_assets():
                self._name_to_partition_mapper[unique_key.name] = partition_mapper
                self._uri_to_partition_mapper[unique_key.uri] = partition_mapper

            for s_asset_ref in base_asset.iter_asset_refs():
                if isinstance(s_asset_ref, SerializedAssetNameRef):
                    self._name_to_partition_mapper[s_asset_ref.name] = partition_mapper
                elif isinstance(s_asset_ref, SerializedAssetUriRef):
                    self._uri_to_partition_mapper[s_asset_ref.uri] = partition_mapper
                else:
                    raise ValueError(f"{type(s_asset_ref)} is not supported")

            if isinstance(base_asset, SerializedAssetAlias):
                log.warning("Partitioned Asset Alias is not supported.")

    def get_partition_mapper(self, *, name: str = "", uri: str = "") -> PartitionMapper:
        with suppress(KeyError):
            if name:
                return self._name_to_partition_mapper[name]

            if uri:
                return self._uri_to_partition_mapper[uri]

        return self.default_partition_mapper

    @property
    def partition_mapper_info(self) -> list[PartitionMapperInfo]:
        """
        JSON-serializable snapshot of partition mapper attributes per asset.

        One :class:`~airflow.timetables.base.PartitionMapperInfo` entry per asset
        (or asset ref) reachable from ``asset_condition``. Each entry uses the
        mapper resolved by :meth:`get_partition_mapper`, so assets covered only
        by ``default_partition_mapper`` (no ``partition_mapper_config`` entry)
        still appear with the default mapper's ``is_rollup`` value. The UI reads
        this from the cached ``DagModel.partition_mapper_info`` instead of
        deserializing the timetable on each request.

        Asset aliases are intentionally skipped: ``SerializedAssetAlias.iter_assets``
        yields nothing (resolution happens at event time, not at parse time),
        and :meth:`_build_name_uri_mapping` already warns that aliases are
        unsupported as ``partition_mapper_config`` keys. The cache therefore
        matches the alias-unsupported policy enforced elsewhere in this class.
        """
        entries: list[PartitionMapperInfo] = []
        for unique_key, _ in self.asset_condition.iter_assets():
            mapper = self.get_partition_mapper(name=unique_key.name, uri=unique_key.uri)
            entries.append(
                PartitionMapperInfo(name=unique_key.name, uri=unique_key.uri, is_rollup=mapper.is_rollup)
            )
        for s_asset_ref in self.asset_condition.iter_asset_refs():
            if isinstance(s_asset_ref, SerializedAssetNameRef):
                mapper = self.get_partition_mapper(name=s_asset_ref.name)
                entries.append(PartitionMapperInfo(name=s_asset_ref.name, is_rollup=mapper.is_rollup))
            elif isinstance(s_asset_ref, SerializedAssetUriRef):
                mapper = self.get_partition_mapper(uri=s_asset_ref.uri)
                entries.append(PartitionMapperInfo(uri=s_asset_ref.uri, is_rollup=mapper.is_rollup))
        return entries

    def _decode_partition_date(self, partition_key: str) -> datetime | None:
        """
        Decode *partition_key* into the period-start datetime shared by all asset mappers.

        Iterates every asset (and asset ref) reachable from the asset condition, asks
        each mapper for the temporal anchor of *partition_key*, and returns it when all
        temporal mappers agree. Returns ``None`` when no mapper is temporal or when the
        mappers disagree — consistent with how the scheduler resolves ``partition_date``
        for asset-triggered runs.
        """
        anchors: set[datetime] = set()
        for unique_key, _ in self.asset_condition.iter_assets():
            mapper = self.get_partition_mapper(name=unique_key.name, uri=unique_key.uri)
            anchor = mapper.to_partition_date(partition_key)
            if anchor is not None:
                anchors.add(anchor)
        for s_asset_ref in self.asset_condition.iter_asset_refs():
            if isinstance(s_asset_ref, SerializedAssetNameRef):
                mapper = self.get_partition_mapper(name=s_asset_ref.name)
            elif isinstance(s_asset_ref, SerializedAssetUriRef):
                mapper = self.get_partition_mapper(uri=s_asset_ref.uri)
            else:
                continue
            anchor = mapper.to_partition_date(partition_key)
            if anchor is not None:
                anchors.add(anchor)
        if len(anchors) == 1:
            return anchors.pop()
        return None

    def serialize(self) -> dict[str, Any]:
        from airflow.serialization.serialized_objects import encode_asset_like

        return {
            "asset_condition": encode_asset_like(self.asset_condition),
            "partition_mapper_config": [
                (encode_asset_like(asset), encode_partition_mapper(partition_mapper))
                for asset, partition_mapper in self.partition_mapper_config.items()
            ],
            "default_partition_mapper": encode_partition_mapper(self.default_partition_mapper),
        }

    @classmethod
    def deserialize(cls, data: dict[str, Any]) -> PartitionedAssetTimetable:
        from airflow.serialization.decoders import decode_partition_mapper
        from airflow.serialization.serialized_objects import decode_asset_like

        default_partition_mapper_data = data["default_partition_mapper"]
        partition_mapper_mappping_data = data["partition_mapper_config"]

        timetable = cls(
            assets=decode_asset_like(data["asset_condition"]),
            default_partition_mapper=decode_partition_mapper(default_partition_mapper_data),
            partition_mapper_config={
                decode_asset_like(ser_asest): decode_partition_mapper(ser_partition_mapper)
                for ser_asest, ser_partition_mapper in partition_mapper_mappping_data
            },
        )
        return timetable
