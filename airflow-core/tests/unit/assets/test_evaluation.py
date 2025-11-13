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
from airflow.sdk.definitions.asset import Asset, AssetAlias, AssetAll, AssetAny, AssetUniqueKey
from airflow.serialization.serialized_objects import BaseSerialization

pytestmark = pytest.mark.db_test

asset1 = Asset(uri="s3://bucket1/data1", name="asset-1")
asset2 = Asset(uri="s3://bucket2/data2", name="asset-2")


@pytest.fixture
def evaluator(session):
    return AssetEvaluator(session)


@pytest.mark.parametrize(
    ("statuses", "result"),
    [
        ({AssetUniqueKey.from_asset(asset1): True}, True),
        ({AssetUniqueKey.from_asset(asset1): False}, False),
        ({}, False),
    ],
)
def test_asset_evaluate(evaluator, statuses, result):
    assert evaluator.run(asset1, statuses) is result


@pytest.mark.parametrize(
    ("condition", "statuses", "result"),
    [
        (
            AssetAny(asset1, asset2),
            {AssetUniqueKey.from_asset(asset1): False, AssetUniqueKey.from_asset(asset2): True},
            True,
        ),
        (
            AssetAll(asset1, asset2),
            {AssetUniqueKey.from_asset(asset1): True, AssetUniqueKey.from_asset(asset2): False},
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
        AssetUniqueKey("asset-1", "s3://bucket1/data1"): asset1,
        AssetUniqueKey("asset-2", "s3://bucket2/data2"): asset2,
    }


@pytest.mark.parametrize(
    ("inputs", "scenario", "expected"),
    [
        # Scenarios for AssetAny
        ((True, True, True), "any", True),
        ((True, True, False), "any", True),
        ((True, False, True), "any", True),
        ((True, False, False), "any", True),
        ((False, False, True), "any", True),
        ((False, True, False), "any", True),
        ((False, True, True), "any", True),
        ((False, False, False), "any", False),
        # Scenarios for AssetAll
        ((True, True, True), "all", True),
        ((True, True, False), "all", False),
        ((True, False, True), "all", False),
        ((True, False, False), "all", False),
        ((False, False, True), "all", False),
        ((False, True, False), "all", False),
        ((False, True, True), "all", False),
        ((False, False, False), "all", False),
    ],
)
def test_asset_logical_conditions_evaluation_and_serialization(evaluator, inputs, scenario, expected):
    class_ = AssetAny if scenario == "any" else AssetAll
    assets = [Asset(uri=f"s3://abc/{i}", name=f"asset_{i}") for i in range(123, 126)]
    condition = class_(*assets)

    statuses = {AssetUniqueKey.from_asset(asset): status for asset, status in zip(assets, inputs)}
    assert evaluator.run(condition, statuses) == expected, (
        f"Condition evaluation failed for inputs {inputs} and scenario '{scenario}'"
    )

    # Serialize and deserialize the condition to test persistence
    serialized = BaseSerialization.serialize(condition)
    deserialized = BaseSerialization.deserialize(serialized)
    assert evaluator.run(deserialized, statuses) == expected, "Serialization round-trip failed"


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
    asset1 = Asset(uri="s3://abc/123")
    asset2 = Asset(uri="s3://abc/124")
    asset3 = Asset(uri="s3://abc/125")

    # Create a nested condition: AssetAll with asset1 and AssetAny with asset2 and asset3
    nested_condition = AssetAll(asset1, AssetAny(asset2, asset3))

    statuses = {
        AssetUniqueKey.from_asset(asset1): status_values[0],
        AssetUniqueKey.from_asset(asset2): status_values[1],
        AssetUniqueKey.from_asset(asset3): status_values[2],
    }

    assert evaluator.run(nested_condition, statuses) == expected_evaluation, "Initial evaluation mismatch"

    serialized_condition = BaseSerialization.serialize(nested_condition)
    deserialized_condition = BaseSerialization.deserialize(serialized_condition)

    assert evaluator.run(deserialized_condition, statuses) == expected_evaluation, (
        "Post-serialization evaluation mismatch"
    )


class TestAssetAlias:
    @pytest.fixture
    def asset(self):
        """Example asset links to asset alias resolved_asset_alias_2."""
        return Asset(uri="test://asset1/", name="test_name", group="asset")

    @pytest.fixture
    def asset_alias_1(self):
        """Example asset alias links to no assets."""
        return AssetAlias(name="test_name", group="test")

    @pytest.fixture
    def resolved_asset_alias_2(self):
        """Example asset alias links to asset."""
        return AssetAlias(name="test_name_2")

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
        assert evaluator.run(asset_alias_1, {AssetUniqueKey.from_asset(asset): True}) is False

    def test_evalute_resolved(self, evaluator, resolved_asset_alias_2, asset):
        assert evaluator.run(resolved_asset_alias_2, {AssetUniqueKey.from_asset(asset): True}) is True
