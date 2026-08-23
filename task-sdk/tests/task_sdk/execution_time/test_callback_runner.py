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

import pytest

from airflow.sdk.definitions.asset import Asset, AssetAlias
from airflow.sdk.definitions.asset.metadata import Metadata
from airflow.sdk.execution_time.callback_runner import (
    create_async_executable_runner,
    create_executable_runner,
)
from airflow.sdk.execution_time.context import OutletEventAccessors
from airflow.sdk.execution_time.task_runner import _serialize_outlet_events

ASSET = Asset("a")
LOGGER = logging.getLogger("test_callback_runner")


def _run(func, outlet_events: OutletEventAccessors | None = None) -> OutletEventAccessors:
    if outlet_events is None:
        outlet_events = OutletEventAccessors()
    create_executable_runner(func, outlet_events, logger=LOGGER).run()
    return outlet_events


class TestCreateExecutableRunnerMetadata:
    def test_two_yields_merge_extra_and_collect_partition_keys(self):
        def gen():
            yield Metadata(ASSET, extra={"a": 1}, partition_key="us")
            yield Metadata(ASSET, extra={"b": 2}, partition_key="eu")

        outlet_events = _run(gen)
        accessor = outlet_events[ASSET]
        assert accessor.extra == {"a": 1, "b": 2}
        assert accessor.partition_keys == {"us", "eu"}

        events = list(_serialize_outlet_events(outlet_events))
        assert sorted(events, key=lambda e: e["partition_key"]) == [
            {
                "dest_asset_key": {"name": "a", "uri": "a"},
                "extra": {"a": 1, "b": 2},
                "partition_key": "eu",
            },
            {
                "dest_asset_key": {"name": "a", "uri": "a"},
                "extra": {"a": 1, "b": 2},
                "partition_key": "us",
            },
        ]

    def test_add_partitions_list_matches_two_metadata_yields(self):
        def via_metadata():
            yield Metadata(ASSET, extra={"row_count": 1}, partition_key="us")
            yield Metadata(ASSET, extra={"row_count": 1}, partition_key="eu")

        def via_add_partitions(*, outlet_events):
            outlet_events[ASSET].extra = {"row_count": 1}
            outlet_events[ASSET].add_partitions(["us", "eu"])

        metadata_events = OutletEventAccessors()
        add_events = OutletEventAccessors()
        _run(via_metadata, metadata_events)
        via_add_partitions(outlet_events=add_events)

        assert metadata_events[ASSET].extra == add_events[ASSET].extra == {"row_count": 1}
        assert metadata_events[ASSET].partition_keys == add_events[ASSET].partition_keys == {"us", "eu"}
        assert sorted(_serialize_outlet_events(metadata_events), key=lambda e: e["partition_key"]) == sorted(
            _serialize_outlet_events(add_events), key=lambda e: e["partition_key"]
        )

    def test_extra_does_not_imply_partition_key(self):
        def gen():
            yield Metadata(ASSET, extra={"section": "us"})

        outlet_events = _run(gen)
        accessor = outlet_events[ASSET]
        assert accessor.extra == {"section": "us"}
        assert accessor.partition_keys == set()
        events = list(_serialize_outlet_events(outlet_events))
        assert events == [{"dest_asset_key": {"name": "a", "uri": "a"}, "extra": {"section": "us"}}]
        assert "partition_key" not in events[0]

    @pytest.mark.parametrize(
        "key",
        ["", "a" * 251],
        ids=["empty", "too_long"],
    )
    def test_invalid_partition_key_raises(self, key):
        def gen():
            yield Metadata(ASSET, extra={"a": 1}, partition_key=key)

        with pytest.raises(ValueError, match="partition_key"):
            _run(gen)

    def test_alias_and_partition_key_records_key_on_asset(self):
        alias = AssetAlias("outputs")

        def gen():
            yield Metadata(ASSET, extra={"k": "v"}, alias=alias, partition_key="us")

        outlet_events = _run(gen)
        assert outlet_events[ASSET].extra == {"k": "v"}
        assert outlet_events[ASSET].partition_keys == {"us"}
        assert outlet_events[alias].partition_keys == set()
        assert len(outlet_events[alias].asset_alias_events) == 1
        assert outlet_events[alias].asset_alias_events[0].extra == {"k": "v"}

    def test_alias_as_metadata_asset_with_partition_key_raises_type_error(self):
        alias = AssetAlias("outputs")

        def gen():
            yield Metadata(alias, extra={"k": 1}, partition_key="us")

        with pytest.raises(TypeError, match="not supported on asset alias"):
            _run(gen)


class TestCreateAsyncExecutableRunnerMetadata:
    @pytest.mark.asyncio
    async def test_two_yields_merge_extra_and_collect_partition_keys(self):
        outlet_events = OutletEventAccessors()

        async def gen():
            yield Metadata(ASSET, extra={"a": 1}, partition_key="us")
            yield Metadata(ASSET, extra={"b": 2}, partition_key="eu")

        await create_async_executable_runner(gen, outlet_events, logger=LOGGER).run()
        accessor = outlet_events[ASSET]
        assert accessor.extra == {"a": 1, "b": 2}
        assert accessor.partition_keys == {"us", "eu"}

    @pytest.mark.asyncio
    async def test_invalid_partition_key_raises(self):
        outlet_events = OutletEventAccessors()

        async def gen():
            yield Metadata(ASSET, extra={"a": 1}, partition_key="")

        with pytest.raises(ValueError, match="partition_key"):
            await create_async_executable_runner(gen, outlet_events, logger=LOGGER).run()
