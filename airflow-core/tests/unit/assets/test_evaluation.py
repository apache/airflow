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

from airflow.assets.evaluation import AssetEvaluator
from airflow.serialization.definitions.assets import (
    SerializedAsset,
    SerializedAssetAlias,
    SerializedAssetAll,
    SerializedAssetAny,
    SerializedAssetUniqueKey,
)

pytestmark = pytest.mark.db_test

asset1 = SerializedAsset("asset-1", "s3://bucket1/data1", "asset", {}, [])
asset2 = SerializedAsset("asset-2", "s3://bucket2/data2", "asset", {}, [])


@pytest.fixture
def evaluator(session):
    return AssetEvaluator(session)


@pytest.mark.parametrize(
    ("statuses", "result"),
    [
        ({SerializedAssetUniqueKey.from_asset(asset1): True}, True),
        ({SerializedAssetUniqueKey.from_asset(asset1): False}, False),
        ({}, False),
    ],
)
def test_asset_evaluate(evaluator, statuses, result):
    assert evaluator.run(asset1, statuses) is result


@pytest.mark.parametrize(
    ("condition", "statuses", "result"),
    [
        (
            SerializedAssetAny([asset1, asset2]),
            {
                SerializedAssetUniqueKey.from_asset(asset1): False,
                SerializedAssetUniqueKey.from_asset(asset2): True,
            },
            True,
        ),
        (
            SerializedAssetAll([asset1, asset2]),
            {
                SerializedAssetUniqueKey.from_asset(asset1): True,
                SerializedAssetUniqueKey.from_asset(asset2): False,
            },
            False,
        ),
    ],
)
def test_assset_boolean_condition_evaluate_iter(evaluator, condition, statuses, result):
    """
    Tests _AssetBooleanCondition's evaluate and iter_assets methods through AssetAny and AssetAll.

    Ensures AssetAny evaluate returns True with any true condition, AssetAll evaluate returns False if
    any condition is false, and both classes correctly iterate over assets without duplication.
    """
    assert evaluator.run(condition, statuses) is result
    assert dict(condition.iter_assets()) == {
        SerializedAssetUniqueKey("asset-1", "s3://bucket1/data1"): asset1,
        SerializedAssetUniqueKey("asset-2", "s3://bucket2/data2"): asset2,
    }


@pytest.mark.parametrize(
    ("inputs", "scenario", "expected"),
    [
        # Scenarios for "any"
        ((True, True, True), SerializedAssetAny, True),
        ((True, True, False), SerializedAssetAny, True),
        ((True, False, True), SerializedAssetAny, True),
        ((True, False, False), SerializedAssetAny, True),
        ((False, False, True), SerializedAssetAny, True),
        ((False, True, False), SerializedAssetAny, True),
        ((False, True, True), SerializedAssetAny, True),
        ((False, False, False), SerializedAssetAny, False),
        # Scenarios for "all"
        ((True, True, True), SerializedAssetAll, True),
        ((True, True, False), SerializedAssetAll, False),
        ((True, False, True), SerializedAssetAll, False),
        ((True, False, False), SerializedAssetAll, False),
        ((False, False, True), SerializedAssetAll, False),
        ((False, True, False), SerializedAssetAll, False),
        ((False, True, True), SerializedAssetAll, False),
        ((False, False, False), SerializedAssetAll, False),
    ],
)
def test_asset_logical_conditions_evaluation_and_serialization(evaluator, inputs, scenario, expected):
    assets = [SerializedAsset(f"asset_{i}", f"s3://abc/{i}", "asset", {}, []) for i in range(123, 126)]
    condition = scenario(assets)

    statuses = {SerializedAssetUniqueKey.from_asset(asset): status for asset, status in zip(assets, inputs)}
    assert evaluator.run(condition, statuses) == expected, (
        f"Condition evaluation failed for inputs {inputs} and scenario '{scenario}'"
    )


@pytest.mark.parametrize(
    ("status_values", "expected_evaluation"),
    [
        pytest.param(
            (False, True, True),
            False,
            id="f & (t | t)",
        ),  # AssetAll requires all conditions to be True, but asset1 is False
        pytest.param(
            (True, True, True),
            True,
            id="t & (t | t)",
        ),  # All conditions are True
        pytest.param(
            (True, False, True),
            True,
            id="t & (f | t)",
        ),  # asset1 is True, and AssetAny condition (asset2 or asset3 being True) is met
        pytest.param(
            (True, False, False),
            False,
            id="t & (f | f)",
        ),  # asset1 is True, but neither asset2 nor asset3 meet the AssetAny condition
    ],
)
def test_nested_asset_conditions_with_serialization(evaluator, status_values, expected_evaluation):
    # Define assets
    asset1 = SerializedAsset("123", "s3://abc/123", "asset", {}, [])
    asset2 = SerializedAsset("124", "s3://abc/124", "asset", {}, [])
    asset3 = SerializedAsset("125", "s3://abc/125", "asset", {}, [])

    # Create a nested condition: AssetAll with asset1 and AssetAny with asset2 and asset3
    nested_condition = SerializedAssetAll([asset1, SerializedAssetAny([asset2, asset3])])

    statuses = {
        SerializedAssetUniqueKey.from_asset(asset1): status_values[0],
        SerializedAssetUniqueKey.from_asset(asset2): status_values[1],
        SerializedAssetUniqueKey.from_asset(asset3): status_values[2],
    }
    assert evaluator.run(nested_condition, statuses) == expected_evaluation, "Initial evaluation mismatch"


class TestAssetAlias:
    @pytest.fixture
    def asset(self):
        """Example asset links to asset alias resolved_asset_alias_2."""
        return SerializedAsset("test_name", "test://asset1/", "asset", {}, [])

    @pytest.fixture
    def asset_alias_1(self):
        """Example asset alias links to no assets."""
        return SerializedAssetAlias("test_name", "test")

    @pytest.fixture
    def resolved_asset_alias_2(self):
        """Example asset alias links to asset."""
        return SerializedAssetAlias("test_name_2", "test")

    @pytest.fixture
    def evaluator(self, session, asset_alias_1, resolved_asset_alias_2, asset):
        class _AssetEvaluator(AssetEvaluator):  # Can't use mock because AssetEvaluator sets __slots__.
            def _resolve_asset_alias(self, o):
                if o is asset_alias_1:
                    return []
                if o is resolved_asset_alias_2:
                    return [asset]
                return super()._resolve_asset_alias(o)

        return _AssetEvaluator(session)

    def test_evaluate_empty(self, evaluator, asset_alias_1, asset):
        assert evaluator.run(asset_alias_1, {SerializedAssetUniqueKey.from_asset(asset): True}) is False

    def test_evalute_resolved(self, evaluator, resolved_asset_alias_2, asset):
        assert (
            evaluator.run(resolved_asset_alias_2, {SerializedAssetUniqueKey.from_asset(asset): True}) is True
        )
