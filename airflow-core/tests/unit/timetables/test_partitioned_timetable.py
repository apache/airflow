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

import pytest

from airflow._shared.module_loading import qualname
from airflow.partition_mappers.identity import IdentityMapper as IdentityMapper
from airflow.sdk import Asset
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
