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

import pytest

from airflow.sdk import Asset, IdentityMapper
from airflow.sdk.definitions.timetables.assets import PartitionedAssetTimetable


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
