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

from contextlib import nullcontext

import pytest

from airflow.sdk import (
    DAG,
    Asset,
    AssetOrTimeSchedule,
    AssetTriggeredTimetable,
    DayWindow,
    IdentityMapper,
    PartitionedAssetTimetable,
    RollupMapper,
    StartOfDayMapper,
)
from airflow.sdk.definitions.timetables.simple import NullTimetable
from airflow.sdk.exceptions import AirflowTimetableInvalid

from tests_common.test_utils.config import conf_vars


@pytest.mark.parametrize("batch_asset_events", [True, False])
def test_partitioned_timetable_preserves_positional_mapper_arguments(batch_asset_events):
    asset = Asset("test")
    mapper = IdentityMapper()
    mapping = {asset: mapper}
    timetable = PartitionedAssetTimetable(asset, mapping, mapper, batch_asset_events=batch_asset_events)

    assert timetable.asset_condition == asset
    assert timetable.partition_mapper_config == mapping
    assert timetable.default_partition_mapper is mapper
    assert timetable.batch_asset_events is batch_asset_events


@pytest.mark.parametrize("configured", [False, True])
@pytest.mark.parametrize("explicit", [None, False, True])
@pytest.mark.parametrize(
    "timetable_type", [AssetTriggeredTimetable, PartitionedAssetTimetable, AssetOrTimeSchedule]
)
def test_batching_configuration(configured, explicit, timetable_type):
    kwargs = {"timetable": NullTimetable()} if timetable_type is AssetOrTimeSchedule else {}
    if explicit is not None:
        kwargs["batch_asset_events"] = explicit
    with conf_vars({("scheduler", "batch_asset_events"): str(configured)}):
        timetable = timetable_type(assets=Asset("test"), **kwargs)
    assert timetable.batch_asset_events is (configured if explicit is None else explicit)


def test_asset_schedules_do_not_batch_by_default():
    dag = DAG("default-asset-batching", schedule=Asset("test"))
    assert dag.timetable.batch_asset_events is False


@pytest.mark.parametrize("batch_asset_events", [False, True])
@pytest.mark.parametrize(
    "timetable_type", [AssetTriggeredTimetable, PartitionedAssetTimetable, AssetOrTimeSchedule]
)
@pytest.mark.parametrize("nested", [False, True])
def test_compound_asset_condition_requires_batching(batch_asset_events, timetable_type, nested):
    condition = Asset("a") & Asset("b")
    if nested:
        condition = Asset("c") | condition
    kwargs = {"timetable": NullTimetable()} if timetable_type is AssetOrTimeSchedule else {}
    dag = DAG(
        "compound-assets",
        schedule=timetable_type(assets=condition, batch_asset_events=batch_asset_events, **kwargs),
    )
    expected = nullcontext() if batch_asset_events else pytest.raises(AirflowTimetableInvalid, match="AND")
    with expected:
        dag.validate()


@pytest.mark.parametrize("batch_asset_events", [False, True])
@pytest.mark.parametrize("use_default_mapper", [False, True])
def test_rollup_requires_batching(batch_asset_events, use_default_mapper):
    asset = Asset("test")
    mapper = RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow())
    kwargs = (
        {"default_partition_mapper": mapper}
        if use_default_mapper
        else {"partition_mapper_config": {asset: mapper}}
    )
    timetable = PartitionedAssetTimetable(assets=asset, batch_asset_events=batch_asset_events, **kwargs)
    expected = (
        nullcontext() if batch_asset_events else pytest.raises(AirflowTimetableInvalid, match="rollups")
    )
    with expected:
        timetable.validate()


@pytest.mark.parametrize("configured", [False, True])
@pytest.mark.parametrize(
    "timetable_type", [AssetTriggeredTimetable, PartitionedAssetTimetable, AssetOrTimeSchedule]
)
def test_compound_asset_condition_defaults_to_batching(configured, timetable_type):
    kwargs = {"timetable": NullTimetable()} if timetable_type is AssetOrTimeSchedule else {}
    with conf_vars({("scheduler", "batch_asset_events"): str(configured)}):
        timetable = timetable_type(assets=Asset("a") & Asset("b"), **kwargs)
    assert timetable.batch_asset_events is True
    timetable.validate()


def test_asset_list_schedule_defaults_to_batching():
    dag = DAG("asset-list-schedule", schedule=[Asset("a"), Asset("b")])
    assert dag.timetable.batch_asset_events is True
    dag.validate()


@pytest.mark.parametrize("use_default_mapper", [False, True])
def test_rollup_defaults_to_batching(use_default_mapper):
    asset = Asset("test")
    mapper = RollupMapper(upstream_mapper=StartOfDayMapper(), window=DayWindow())
    kwargs = (
        {"default_partition_mapper": mapper}
        if use_default_mapper
        else {"partition_mapper_config": {asset: mapper}}
    )
    with conf_vars({("scheduler", "batch_asset_events"): "False"}):
        timetable = PartitionedAssetTimetable(assets=asset, **kwargs)
    assert timetable.batch_asset_events is True
    timetable.validate()


def test_or_condition_can_disable_batching():
    timetable = AssetTriggeredTimetable(assets=Asset("a") | Asset("b"), batch_asset_events=False)
    timetable.validate()


def test_asset_or_time_schedule_list_defaults_to_batching():
    timetable = AssetOrTimeSchedule(assets=[Asset("a"), Asset("b")], timetable=NullTimetable())
    assert timetable.batch_asset_events is True
    timetable.validate()


def test_custom_asset_condition_requires_batching():
    class CustomAsset(Asset):
        @property
        def requires_batching(self):
            return True

    condition = Asset("a") | CustomAsset("b")
    with conf_vars({("scheduler", "batch_asset_events"): "False"}):
        timetable = AssetTriggeredTimetable(condition)
    assert timetable.batch_asset_events is True
    timetable.validate()

    with pytest.raises(AirflowTimetableInvalid, match="batch_asset_events=True"):
        AssetTriggeredTimetable(condition, batch_asset_events=False).validate()
