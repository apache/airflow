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

from collections.abc import Callable, Iterable
from contextlib import ExitStack
from typing import TYPE_CHECKING
from unittest import mock

import pendulum
import pytest

from airflow._shared.module_loading import qualname
from airflow.exceptions import InvalidPartitionKeyError
from airflow.partition_mappers.base import RollupMapper
from airflow.partition_mappers.identity import IdentityMapper as IdentityMapper
from airflow.partition_mappers.temporal import StartOfDayMapper
from airflow.partition_mappers.window import DayWindow
from airflow.sdk import Asset, AssetAlias
from airflow.serialization.definitions.assets import SerializedAsset
from airflow.serialization.encoders import ensure_serialized_asset
from airflow.serialization.enums import DagAttributeTypes
from airflow.timetables.simple import PartitionedAssetTimetable

if TYPE_CHECKING:
    from airflow.partition_mappers.base import PartitionMapper


class Key1Mapper(IdentityMapper):
    """Partition Mapper that returns only key-1 as downstream key"""

    def to_downstream(self, key: str) -> str:
        return "key-1"

    def to_upstream(self, key: str) -> Iterable[str]:
        yield key


def _find_registered_custom_partition_mapper(import_string: str) -> type[PartitionMapper]:
    if import_string == qualname(Key1Mapper):
        return Key1Mapper
    raise ValueError(f"unexpected class {import_string!r}")


@pytest.fixture
def custom_partition_mapper_patch() -> Callable[[], ExitStack]:
    def _patch() -> ExitStack:
        stack = ExitStack()
        for mock_target in [
            "airflow.serialization.encoders.find_registered_custom_partition_mapper",
            "airflow.serialization.decoders.find_registered_custom_partition_mapper",
        ]:
            stack.enter_context(
                mock.patch(
                    mock_target,
                    _find_registered_custom_partition_mapper,
                )
            )
        return stack

    return _patch


class TestPartitionedAssetTimetable:
    @pytest.mark.parametrize(
        "asset_obj",
        [
            Asset("test_1"),
            Asset(name="test_1"),
            Asset(uri="test_1"),
            Asset(name="test_1", uri="test_1"),
        ],
    )
    def test_get_partition_mapper_without_mapping(self, asset_obj):
        timetable = PartitionedAssetTimetable(assets=asset_obj)
        assert timetable.partition_mapper_config == {}
        assert isinstance(timetable.default_partition_mapper, IdentityMapper)
        assert isinstance(timetable.get_partition_mapper(name="test_1", uri="test_1"), IdentityMapper)
        assert isinstance(timetable.get_partition_mapper(name="test_1"), IdentityMapper)
        assert isinstance(timetable.get_partition_mapper(uri="test_1"), IdentityMapper)

    @pytest.mark.parametrize(
        "asset_obj",
        [
            Asset("test_1"),
            Asset(name="test_1"),
            Asset(uri="test_1"),
            Asset(name="test_1", uri="test_1"),
        ],
    )
    @pytest.mark.usefixtures("custom_partition_mapper_patch")
    def test_get_partition_mapper_with_mapping(self, asset_obj):
        ser_asset = ensure_serialized_asset(asset_obj)

        timetable = PartitionedAssetTimetable(
            assets=ser_asset, partition_mapper_config={ser_asset: Key1Mapper()}
        )
        assert isinstance(timetable.default_partition_mapper, IdentityMapper)
        assert isinstance(timetable.get_partition_mapper(name="test_1", uri="test_1"), Key1Mapper)
        assert isinstance(timetable.get_partition_mapper(name="test_1"), Key1Mapper)
        assert isinstance(timetable.get_partition_mapper(uri="test_1"), Key1Mapper)

    def test_partition_mapper_info_default_identity_mapper(self):
        timetable = PartitionedAssetTimetable(assets=Asset("a"))
        assert timetable.partition_mapper_info == [{"name": "a", "uri": "a", "is_rollup": False}]

    def test_partition_mapper_info_default_rollup_mapper(self):
        """
        ``default_partition_mapper=RollupMapper(...)`` is the primary documented
        rollup usage pattern (see ``example_asset_partition.py``). Every asset
        reachable from ``asset_condition`` should be reported as rollup even
        when ``partition_mapper_config`` is empty.
        """
        asset = Asset(name="daily", uri="s3://bucket/daily")
        timetable = PartitionedAssetTimetable(
            assets=asset,
            default_partition_mapper=RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow()),
        )

        assert timetable.partition_mapper_info == [
            {"name": "daily", "uri": "s3://bucket/daily", "is_rollup": True},
        ]

    def test_partition_mapper_info_mixes_rollup_and_non_rollup(self):
        non_rollup = ensure_serialized_asset(Asset(name="non_rollup_name", uri="s3://bucket/non_rollup"))
        rollup = ensure_serialized_asset(Asset(name="rollup_name", uri="s3://bucket/rollup"))
        timetable = PartitionedAssetTimetable(
            assets=[non_rollup, rollup],
            partition_mapper_config={
                non_rollup: IdentityMapper(),
                rollup: RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow()),
            },
        )

        info = timetable.partition_mapper_info
        assert info == [
            {"name": "non_rollup_name", "uri": "s3://bucket/non_rollup", "is_rollup": False},
            {"name": "rollup_name", "uri": "s3://bucket/rollup", "is_rollup": True},
        ]

    def test_partition_mapper_info_skips_asset_aliases(self):
        """
        ``SerializedAssetAlias.iter_assets`` yields nothing (aliases are resolved
        at event time, not at parse time), and ``_build_name_uri_mapping``
        warns that aliases are unsupported as ``partition_mapper_config`` keys.
        ``partition_mapper_info`` therefore omits any alias entries — including
        when an alias is combined with a regular asset under an ``Or`` condition —
        matching the alias-unsupported policy enforced elsewhere in this class.
        """
        asset = Asset(name="real_asset", uri="s3://bucket/real_asset")
        alias = AssetAlias(name="alias_only")
        timetable = PartitionedAssetTimetable(assets=asset | alias)

        info = timetable.partition_mapper_info
        assert info == [
            {"name": "real_asset", "uri": "s3://bucket/real_asset", "is_rollup": False},
        ]

    def test_partition_mapper_info_handles_asset_refs(self):
        ref_by_name = ensure_serialized_asset(Asset.ref(name="ref_by_name"))
        ref_by_uri = ensure_serialized_asset(Asset.ref(uri="s3://ref"))
        timetable = PartitionedAssetTimetable(
            assets=[ref_by_name, ref_by_uri],
            partition_mapper_config={
                ref_by_name: RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow()),
                ref_by_uri: IdentityMapper(),
            },
        )

        info = timetable.partition_mapper_info
        assert info == [
            {"name": "ref_by_name", "is_rollup": True},
            {"uri": "s3://ref", "is_rollup": False},
        ]

    def test_serialize(self):
        ser_asset = ensure_serialized_asset(Asset("test"))
        timetable = PartitionedAssetTimetable(
            assets=ser_asset, partition_mapper_config={ser_asset: IdentityMapper()}
        )
        assert timetable.serialize() == {
            "asset_condition": {
                "__type": DagAttributeTypes.ASSET,
                "name": "test",
                "uri": "test",
                "group": "asset",
                "extra": {},
            },
            "partition_mapper_config": [
                (
                    {
                        "__type": DagAttributeTypes.ASSET,
                        "name": "test",
                        "uri": "test",
                        "group": "asset",
                        "extra": {},
                    },
                    {
                        "__type": "airflow.partition_mappers.identity.IdentityMapper",
                        "__var": {},
                    },
                )
            ],
            "default_partition_mapper": {
                "__type": "airflow.partition_mappers.identity.IdentityMapper",
                "__var": {},
            },
        }

    def test_deserialize(self):
        timetable = PartitionedAssetTimetable.deserialize(
            {
                "asset_condition": {
                    "__type": DagAttributeTypes.ASSET,
                    "name": "test",
                    "uri": "test",
                    "group": "asset",
                    "extra": {},
                },
                "partition_mapper_config": [
                    (
                        {
                            "__type": DagAttributeTypes.ASSET,
                            "name": "test",
                            "uri": "test",
                            "group": "asset",
                            "extra": {},
                        },
                        {
                            "__type": "airflow.partition_mappers.identity.IdentityMapper",
                            "__var": {},
                        },
                    )
                ],
                "default_partition_mapper": {
                    "__type": "airflow.partition_mappers.identity.IdentityMapper",
                    "__var": {},
                },
            }
        )
        ser_asset = SerializedAsset(name="test", uri="test", group="asset", extra={}, watchers=[])
        assert timetable.asset_condition == ser_asset
        assert isinstance(timetable.default_partition_mapper, IdentityMapper)
        assert isinstance(timetable.partition_mapper_config[ser_asset], IdentityMapper)

    @pytest.mark.parametrize(
        "asset_like",
        [
            Asset(name="daily", uri="s3://bucket/daily"),
            Asset.ref(name="daily"),
        ],
    )
    def test_decode_partition_date_raises_on_invalid_key(self, asset_like):
        timetable = PartitionedAssetTimetable(
            assets=asset_like,
            default_partition_mapper=StartOfDayMapper(),
        )
        with pytest.raises(InvalidPartitionKeyError, match="is invalid for this timetable's mappers"):
            timetable._decode_partition_date("not-a-date")

    def test_decode_partition_date_returns_period_start_for_valid_key(self):
        timetable = PartitionedAssetTimetable(
            assets=Asset(name="daily", uri="s3://bucket/daily"),
            default_partition_mapper=StartOfDayMapper(),
        )
        assert timetable._decode_partition_date("2025-01-01") == pendulum.datetime(2025, 1, 1, tz="UTC")
