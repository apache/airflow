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

from airflow._shared.module_loading import qualname


def test_builtin_timetable_type_name_returns_class_name():
    """Built-in timetables should return just the class name as type_name."""
    from airflow.timetables.simple import NullTimetable

    tt = NullTimetable()
    assert tt.type_name == "NullTimetable"


def test_custom_timetable_type_name_returns_qualname():
    """Custom/user-defined timetables should return the full import path (qualname)."""
    # Define a custom timetable class that inherits from Timetable so it uses
    # the default property implementation. Its module will not start with
    # "airflow.timetables." so type_name should be the qualname.
    from airflow.timetables.base import Timetable

    class CustomTimetable(Timetable):
        pass

    inst = CustomTimetable()
    expected = qualname(inst.__class__)

    # The Timetable.type_name logic should return qualname for custom timetables
    assert inst.type_name == expected


# ---------------------------------------------------------------------------
# compute_rollup_fingerprint
# ---------------------------------------------------------------------------


def test_compute_rollup_fingerprint_non_partitioned_returns_empty():
    """Non-partitioned timetables return an empty dict."""
    from airflow.timetables.base import compute_rollup_fingerprint
    from airflow.timetables.simple import NullTimetable

    assert compute_rollup_fingerprint(NullTimetable()) == {}


def test_compute_rollup_fingerprint_stable_across_calls():
    """Same timetable produces identical fingerprints on repeated calls."""
    from airflow.sdk import Asset, HourWindow, RollupMapper, StartOfHourMapper
    from airflow.timetables.base import compute_rollup_fingerprint
    from airflow.timetables.simple import PartitionedAssetTimetable

    tt = PartitionedAssetTimetable(
        assets=Asset(name="asset-a"),
        default_partition_mapper=RollupMapper(upstream_mapper=StartOfHourMapper(), window=HourWindow()),
    )
    assert compute_rollup_fingerprint(tt) == compute_rollup_fingerprint(tt)


def test_compute_rollup_fingerprint_keys_sorted():
    """Keys are sorted, making the dict stable regardless of asset order."""
    from airflow.sdk import Asset, HourWindow, RollupMapper, StartOfHourMapper
    from airflow.timetables.base import compute_rollup_fingerprint
    from airflow.timetables.simple import PartitionedAssetTimetable

    asset_a = Asset(name="asset-a")
    asset_b = Asset(name="asset-b")

    tt_ab = PartitionedAssetTimetable(
        assets=asset_a | asset_b,
        default_partition_mapper=RollupMapper(upstream_mapper=StartOfHourMapper(), window=HourWindow()),
    )
    tt_ba = PartitionedAssetTimetable(
        assets=asset_b | asset_a,
        default_partition_mapper=RollupMapper(upstream_mapper=StartOfHourMapper(), window=HourWindow()),
    )
    fp_ab = compute_rollup_fingerprint(tt_ab)
    fp_ba = compute_rollup_fingerprint(tt_ba)
    assert fp_ab == fp_ba
    assert list(fp_ab.keys()) == sorted(fp_ab.keys())


def test_compute_rollup_fingerprint_window_change_produces_different_fingerprint():
    """Changing window (Hour → Day) produces a different fingerprint."""
    from airflow.sdk import Asset, DayWindow, HourWindow, RollupMapper, StartOfHourMapper
    from airflow.timetables.base import compute_rollup_fingerprint
    from airflow.timetables.simple import PartitionedAssetTimetable

    asset_1 = Asset(name="asset-1")
    tt_hour = PartitionedAssetTimetable(
        assets=asset_1,
        default_partition_mapper=RollupMapper(upstream_mapper=StartOfHourMapper(), window=HourWindow()),
    )
    tt_day = PartitionedAssetTimetable(
        assets=asset_1,
        default_partition_mapper=RollupMapper(upstream_mapper=StartOfHourMapper(), window=DayWindow()),
    )
    fp_hour = compute_rollup_fingerprint(tt_hour)
    fp_day = compute_rollup_fingerprint(tt_day)
    assert fp_hour != fp_day


def test_compute_rollup_fingerprint_multi_asset_all_keys_present():
    """All assets appear as keys in the fingerprint."""
    from airflow.sdk import Asset, HourWindow, RollupMapper, StartOfHourMapper
    from airflow.timetables.base import compute_rollup_fingerprint
    from airflow.timetables.simple import PartitionedAssetTimetable

    assets = [Asset(name=f"asset-{i}") for i in range(3)]
    condition = assets[0]
    for a in assets[1:]:
        condition = condition | a
    tt = PartitionedAssetTimetable(
        assets=condition,
        default_partition_mapper=RollupMapper(upstream_mapper=StartOfHourMapper(), window=HourWindow()),
    )
    fp = compute_rollup_fingerprint(tt)
    assert len(fp) == 3
    for asset in assets:
        key = f"{asset.name}|{asset.uri}"
        assert key in fp


@pytest.mark.parametrize(
    ("asset_or_ref", "expected_key"),
    [
        pytest.param(
            ("asset", "my-asset", "s3://bucket/key"),
            "my-asset|s3://bucket/key",
            id="name-and-uri",
        ),
        pytest.param(
            ("name_ref", "only-name", None),
            "only-name|",
            id="name-only",
        ),
        pytest.param(
            ("uri_ref", None, "s3://bucket/key"),
            "|s3://bucket/key",
            id="uri-only",
        ),
    ],
)
def test_compute_rollup_fingerprint_key_format(asset_or_ref, expected_key):
    """Keys follow the ``{name}|{uri}`` format for full assets and ``{name}|`` / ``|{uri}`` for refs."""
    from airflow.sdk import Asset, HourWindow, RollupMapper, StartOfHourMapper
    from airflow.sdk.definitions.asset import AssetNameRef, AssetUriRef
    from airflow.timetables.base import compute_rollup_fingerprint
    from airflow.timetables.simple import PartitionedAssetTimetable

    kind, name, uri = asset_or_ref
    if kind == "asset":
        asset_input = Asset(name=name, uri=uri)
    elif kind == "name_ref":
        asset_input = AssetNameRef(name=name)
    else:
        asset_input = AssetUriRef(uri=uri)

    tt = PartitionedAssetTimetable(
        assets=asset_input,
        default_partition_mapper=RollupMapper(upstream_mapper=StartOfHourMapper(), window=HourWindow()),
    )
    fp = compute_rollup_fingerprint(tt)
    assert expected_key in fp
