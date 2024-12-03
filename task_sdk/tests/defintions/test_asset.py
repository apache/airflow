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

import os
from collections import defaultdict
from typing import Callable
from unittest.mock import patch

import pytest
from sqlalchemy.sql import select

from airflow.models.asset import AssetAliasModel, AssetDagRunQueue, AssetModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import (
    Asset,
    AssetAlias,
    AssetAliasCondition,
    AssetAll,
    AssetAny,
    BaseAsset,
    Dataset,
    Model,
    _get_normalized_scheme,
    _sanitize_uri,
)
from airflow.serialization.serialized_objects import BaseSerialization, SerializedDAG

ASSET_MODULE_PATH = "airflow.sdk.definitions.asset"


@pytest.fixture
def clear_assets():
    from tests_common.test_utils.db import clear_db_assets

    clear_db_assets()
    yield
    clear_db_assets()


@pytest.mark.parametrize(
    ["name"],
    [
        pytest.param("", id="empty"),
        pytest.param("\n\t", id="whitespace"),
        pytest.param("a" * 1501, id="too_long"),
        pytest.param("😊", id="non-ascii"),
    ],
)
def test_invalid_names(name):
    with pytest.raises(ValueError):
        Asset(name=name)


@pytest.mark.parametrize(
    ["uri"],
    [
        pytest.param("", id="empty"),
        pytest.param("\n\t", id="whitespace"),
        pytest.param("a" * 1501, id="too_long"),
        pytest.param("airflow://xcom/dag/task", id="reserved_scheme"),
        pytest.param("😊", id="non-ascii"),
    ],
)
def test_invalid_uris(uri):
    with pytest.raises(ValueError):
        Asset(uri=uri)


def test_only_name():
    asset = Asset(name="foobar")
    assert asset.name == "foobar"
    assert asset.uri == "foobar"


def test_only_uri():
    asset = Asset(uri="s3://bucket/key/path")
    assert asset.name == "s3://bucket/key/path"
    assert asset.uri == "s3://bucket/key/path"


@pytest.mark.parametrize("arg", ["foobar", "s3://bucket/key/path"])
def test_only_posarg(arg):
    asset = Asset(arg)
    assert asset.name == arg
    assert asset.uri == arg


def test_both_name_and_uri():
    asset = Asset("foobar", "s3://bucket/key/path")
    assert asset.name == "foobar"
    assert asset.uri == "s3://bucket/key/path"


@pytest.mark.parametrize(
    "uri, normalized",
    [
        pytest.param("foobar", "foobar", id="scheme-less"),
        pytest.param("foo:bar", "foo:bar", id="scheme-less-colon"),
        pytest.param("foo/bar", "foo/bar", id="scheme-less-slash"),
        pytest.param("s3://bucket/key/path", "s3://bucket/key/path", id="normal"),
        pytest.param("file:///123/456/", "file:///123/456", id="trailing-slash"),
    ],
)
def test_uri_with_scheme(uri: str, normalized: str) -> None:
    asset = Asset(uri)
    EmptyOperator(task_id="task1", outlets=[asset])
    assert asset.uri == normalized
    assert os.fspath(asset) == normalized


def test_uri_with_auth() -> None:
    with pytest.warns(UserWarning) as record:
        asset = Asset("ftp://user@localhost/foo.txt")
    assert len(record) == 1
    assert str(record[0].message) == (
        "An Asset URI should not contain auth info (e.g. username or "
        "password). It has been automatically dropped."
    )
    EmptyOperator(task_id="task1", outlets=[asset])
    assert asset.uri == "ftp://localhost/foo.txt"
    assert os.fspath(asset) == "ftp://localhost/foo.txt"


def test_uri_without_scheme():
    asset = Asset(uri="example_asset")
    EmptyOperator(task_id="task1", outlets=[asset])


def test_fspath():
    uri = "s3://example/asset"
    asset = Asset(uri=uri)
    assert os.fspath(asset) == uri


def test_equal_when_same_uri():
    uri = "s3://example/asset"
    asset1 = Asset(uri=uri)
    asset2 = Asset(uri=uri)
    assert asset1 == asset2


def test_not_equal_when_different_uri():
    asset1 = Asset(uri="s3://example/asset")
    asset2 = Asset(uri="s3://other/asset")
    assert asset1 != asset2


def test_asset_logic_operations():
    result_or = asset1 | asset2
    assert isinstance(result_or, AssetAny)
    result_and = asset1 & asset2
    assert isinstance(result_and, AssetAll)


def test_asset_iter_assets():
    assert list(asset1.iter_assets()) == [(("asset-1", "s3://bucket1/data1"), asset1)]


@pytest.mark.db_test
def test_asset_iter_asset_aliases():
    base_asset = AssetAll(
        AssetAlias(name="example-alias-1"),
        Asset("1"),
        AssetAny(
            Asset(name="2", uri="test://asset1"),
            AssetAlias("example-alias-2"),
            Asset(name="3"),
            AssetAll(AssetAlias("example-alias-3"), Asset("4"), AssetAlias("example-alias-4")),
        ),
        AssetAll(AssetAlias("example-alias-5"), Asset("5")),
    )
    assert list(base_asset.iter_asset_aliases()) == [
        (f"example-alias-{i}", AssetAlias(f"example-alias-{i}")) for i in range(1, 6)
    ]


def test_asset_evaluate():
    assert asset1.evaluate({"s3://bucket1/data1": True}) is True
    assert asset1.evaluate({"s3://bucket1/data1": False}) is False


def test_asset_any_operations():
    result_or = (asset1 | asset2) | asset3
    assert isinstance(result_or, AssetAny)
    assert len(result_or.objects) == 3
    result_and = (asset1 | asset2) & asset3
    assert isinstance(result_and, AssetAll)


def test_asset_all_operations():
    result_or = (asset1 & asset2) | asset3
    assert isinstance(result_or, AssetAny)
    result_and = (asset1 & asset2) & asset3
    assert isinstance(result_and, AssetAll)


def test_assset_boolean_condition_evaluate_iter():
    """
    Tests _AssetBooleanCondition's evaluate and iter_assets methods through AssetAny and AssetAll.
    Ensures AssetAny evaluate returns True with any true condition, AssetAll evaluate returns False if
    any condition is false, and both classes correctly iterate over assets without duplication.
    """
    any_condition = AssetAny(asset1, asset2)
    all_condition = AssetAll(asset1, asset2)
    assert any_condition.evaluate({"s3://bucket1/data1": False, "s3://bucket2/data2": True}) is True
    assert all_condition.evaluate({"s3://bucket1/data1": True, "s3://bucket2/data2": False}) is False

    # Testing iter_assets indirectly through the subclasses
    assets_any = dict(any_condition.iter_assets())
    assets_all = dict(all_condition.iter_assets())
    assert assets_any == {
        ("asset-1", "s3://bucket1/data1"): asset1,
        ("asset-2", "s3://bucket2/data2"): asset2,
    }
    assert assets_all == {
        ("asset-1", "s3://bucket1/data1"): asset1,
        ("asset-2", "s3://bucket2/data2"): asset2,
    }


@pytest.mark.parametrize(
    "inputs, scenario, expected",
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
def test_asset_logical_conditions_evaluation_and_serialization(inputs, scenario, expected):
    class_ = AssetAny if scenario == "any" else AssetAll
    assets = [Asset(uri=f"s3://abc/{i}", name=f"asset_{i}") for i in range(123, 126)]
    condition = class_(*assets)

    statuses = {asset.uri: status for asset, status in zip(assets, inputs)}
    assert (
        condition.evaluate(statuses) == expected
    ), f"Condition evaluation failed for inputs {inputs} and scenario '{scenario}'"

    # Serialize and deserialize the condition to test persistence
    serialized = BaseSerialization.serialize(condition)
    deserialized = BaseSerialization.deserialize(serialized)
    assert deserialized.evaluate(statuses) == expected, "Serialization round-trip failed"


@pytest.mark.parametrize(
    "status_values, expected_evaluation",
    [
        (
            (False, True, True),
            False,
        ),  # AssetAll requires all conditions to be True, but asset1 is False
        ((True, True, True), True),  # All conditions are True
        (
            (True, False, True),
            True,
        ),  # asset1 is True, and AssetAny condition (asset2 or asset3 being True) is met
        (
            (True, False, False),
            False,
        ),  # asset1 is True, but neither asset2 nor asset3 meet the AssetAny condition
    ],
)
def test_nested_asset_conditions_with_serialization(status_values, expected_evaluation):
    # Define assets
    asset1 = Asset(uri="s3://abc/123")
    asset2 = Asset(uri="s3://abc/124")
    asset3 = Asset(uri="s3://abc/125")

    # Create a nested condition: AssetAll with asset1 and AssetAny with asset2 and asset3
    nested_condition = AssetAll(asset1, AssetAny(asset2, asset3))

    statuses = {
        asset1.uri: status_values[0],
        asset2.uri: status_values[1],
        asset3.uri: status_values[2],
    }

    assert nested_condition.evaluate(statuses) == expected_evaluation, "Initial evaluation mismatch"

    serialized_condition = BaseSerialization.serialize(nested_condition)
    deserialized_condition = BaseSerialization.deserialize(serialized_condition)

    assert (
        deserialized_condition.evaluate(statuses) == expected_evaluation
    ), "Post-serialization evaluation mismatch"


@pytest.fixture
def create_test_assets(session):
    """Fixture to create test assets and corresponding models."""
    assets = [Asset(uri=f"test://asset{i}", name=f"hello{i}") for i in range(1, 3)]
    for asset in assets:
        session.add(AssetModel(uri=asset.uri))
    session.commit()
    return assets


@pytest.mark.db_test
@pytest.mark.usefixtures("clear_assets")
def test_asset_trigger_setup_and_serialization(session, dag_maker, create_test_assets):
    assets = create_test_assets

    # Create DAG with asset triggers
    with dag_maker(schedule=AssetAny(*assets)) as dag:
        EmptyOperator(task_id="hello")

    # Verify assets are set up correctly
    assert isinstance(dag.timetable.asset_condition, AssetAny), "DAG assets should be an instance of AssetAny"

    # Round-trip the DAG through serialization
    deserialized_dag = SerializedDAG.deserialize_dag(SerializedDAG.serialize_dag(dag))

    # Verify serialization and deserialization integrity
    assert isinstance(
        deserialized_dag.timetable.asset_condition, AssetAny
    ), "Deserialized assets should maintain type AssetAny"
    assert (
        deserialized_dag.timetable.asset_condition.objects == dag.timetable.asset_condition.objects
    ), "Deserialized assets should match original"


@pytest.mark.db_test
@pytest.mark.usefixtures("clear_assets")
def test_asset_dag_run_queue_processing(session, clear_assets, dag_maker, create_test_assets):
    assets = create_test_assets
    asset_models = session.query(AssetModel).all()

    with dag_maker(schedule=AssetAny(*assets)) as dag:
        EmptyOperator(task_id="hello")

    # Add AssetDagRunQueue entries to simulate asset event processing
    for am in asset_models:
        session.add(AssetDagRunQueue(asset_id=am.id, target_dag_id=dag.dag_id))
    session.commit()

    # Fetch and evaluate asset triggers for all DAGs affected by asset events
    records = session.scalars(select(AssetDagRunQueue)).all()
    dag_statuses = defaultdict(lambda: defaultdict(bool))
    for record in records:
        dag_statuses[record.target_dag_id][record.asset.uri] = True

    serialized_dags = session.execute(
        select(SerializedDagModel).where(SerializedDagModel.dag_id.in_(dag_statuses.keys()))
    ).fetchall()

    for (serialized_dag,) in serialized_dags:
        dag = SerializedDAG.deserialize(serialized_dag.data)
        for asset_uri, status in dag_statuses[dag.dag_id].items():
            cond = dag.timetable.asset_condition
            assert cond.evaluate({asset_uri: status}), "DAG trigger evaluation failed"


@pytest.mark.db_test
@pytest.mark.usefixtures("clear_assets")
def test_dag_with_complex_asset_condition(session, dag_maker):
    # Create Asset instances
    asset1 = Asset(uri="test://asset1", name="hello1")
    asset2 = Asset(uri="test://asset2", name="hello2")

    # Create and add AssetModel instances to the session
    am1 = AssetModel(uri=asset1.uri, name=asset1.name, group="asset")
    am2 = AssetModel(uri=asset2.uri, name=asset2.name, group="asset")
    session.add_all([am1, am2])
    session.commit()

    # Setup a DAG with complex asset triggers (AssetAny with AssetAll)
    with dag_maker(schedule=AssetAny(asset1, AssetAll(asset2, asset1))) as dag:
        EmptyOperator(task_id="hello")

    assert isinstance(
        dag.timetable.asset_condition, AssetAny
    ), "DAG's asset trigger should be an instance of AssetAny"
    assert any(
        isinstance(trigger, AssetAll) for trigger in dag.timetable.asset_condition.objects
    ), "DAG's asset trigger should include AssetAll"

    serialized_triggers = SerializedDAG.serialize(dag.timetable.asset_condition)

    deserialized_triggers = SerializedDAG.deserialize(serialized_triggers)

    assert isinstance(
        deserialized_triggers, AssetAny
    ), "Deserialized triggers should be an instance of AssetAny"
    assert any(
        isinstance(trigger, AssetAll) for trigger in deserialized_triggers.objects
    ), "Deserialized triggers should include AssetAll"

    serialized_timetable_dict = SerializedDAG.to_dict(dag)["dag"]["timetable"]["__var"]
    assert (
        "asset_condition" in serialized_timetable_dict
    ), "Serialized timetable should contain 'asset_condition'"
    assert isinstance(
        serialized_timetable_dict["asset_condition"], dict
    ), "Serialized 'asset_condition' should be a dict"


def assets_equal(a1: BaseAsset, a2: BaseAsset) -> bool:
    if type(a1) is not type(a2):
        return False

    if isinstance(a1, Asset) and isinstance(a2, Asset):
        return a1.uri == a2.uri

    elif isinstance(a1, (AssetAny, AssetAll)) and isinstance(a2, (AssetAny, AssetAll)):
        if len(a1.objects) != len(a2.objects):
            return False

        # Compare each pair of objects
        for obj1, obj2 in zip(a1.objects, a2.objects):
            # If obj1 or obj2 is an Asset, AssetAny, or AssetAll instance,
            # recursively call assets_equal
            if not assets_equal(obj1, obj2):
                return False
        return True

    return False


asset1 = Asset(uri="s3://bucket1/data1", name="asset-1")
asset2 = Asset(uri="s3://bucket2/data2", name="asset-2")
asset3 = Asset(uri="s3://bucket3/data3", name="asset-3")
asset4 = Asset(uri="s3://bucket4/data4", name="asset-4")
asset5 = Asset(uri="s3://bucket5/data5", name="asset-5")

test_cases = [
    (lambda: asset1, asset1),
    (lambda: asset1 & asset2, AssetAll(asset1, asset2)),
    (lambda: asset1 | asset2, AssetAny(asset1, asset2)),
    (lambda: asset1 | (asset2 & asset3), AssetAny(asset1, AssetAll(asset2, asset3))),
    (lambda: asset1 | asset2 & asset3, AssetAny(asset1, AssetAll(asset2, asset3))),
    (
        lambda: ((asset1 & asset2) | asset3) & (asset4 | asset5),
        AssetAll(AssetAny(AssetAll(asset1, asset2), asset3), AssetAny(asset4, asset5)),
    ),
    (lambda: asset1 & asset2 | asset3, AssetAny(AssetAll(asset1, asset2), asset3)),
    (
        lambda: (asset1 | asset2) & (asset3 | asset4),
        AssetAll(AssetAny(asset1, asset2), AssetAny(asset3, asset4)),
    ),
    (
        lambda: (asset1 & asset2) | (asset3 & (asset4 | asset5)),
        AssetAny(AssetAll(asset1, asset2), AssetAll(asset3, AssetAny(asset4, asset5))),
    ),
    (
        lambda: (asset1 & asset2) & (asset3 & asset4),
        AssetAll(asset1, asset2, AssetAll(asset3, asset4)),
    ),
    (lambda: asset1 | asset2 | asset3, AssetAny(asset1, asset2, asset3)),
    (lambda: asset1 & asset2 & asset3, AssetAll(asset1, asset2, asset3)),
    (
        lambda: ((asset1 & asset2) | asset3) & (asset4 | asset5),
        AssetAll(AssetAny(AssetAll(asset1, asset2), asset3), AssetAny(asset4, asset5)),
    ),
]


@pytest.mark.parametrize("expression, expected", test_cases)
def test_evaluate_assets_expression(expression, expected):
    expr = expression()
    assert assets_equal(expr, expected)


@pytest.mark.parametrize(
    "expression, error",
    [
        pytest.param(
            lambda: asset1 & 1,  # type: ignore[operator]
            "unsupported operand type(s) for &: 'Asset' and 'int'",
            id="&",
        ),
        pytest.param(
            lambda: asset1 | 1,  # type: ignore[operator]
            "unsupported operand type(s) for |: 'Asset' and 'int'",
            id="|",
        ),
        pytest.param(
            lambda: AssetAll(1, asset1),  # type: ignore[arg-type]
            "expect asset expressions in condition",
            id="AssetAll",
        ),
        pytest.param(
            lambda: AssetAny(1, asset1),  # type: ignore[arg-type]
            "expect asset expressions in condition",
            id="AssetAny",
        ),
    ],
)
def test_assets_expression_error(expression: Callable[[], None], error: str) -> None:
    with pytest.raises(TypeError) as info:
        expression()
    assert str(info.value) == error


def test_get_normalized_scheme():
    assert _get_normalized_scheme("http://example.com") == "http"
    assert _get_normalized_scheme("HTTPS://example.com") == "https"
    assert _get_normalized_scheme("ftp://example.com") == "ftp"
    assert _get_normalized_scheme("file://") == "file"

    assert _get_normalized_scheme("example.com") == ""
    assert _get_normalized_scheme("") == ""
    assert _get_normalized_scheme(" ") == ""


def _mock_get_uri_normalizer_raising_error(normalized_scheme):
    def normalizer(uri):
        raise ValueError("Incorrect URI format")

    return normalizer


def _mock_get_uri_normalizer_noop(normalized_scheme):
    def normalizer(uri):
        return uri

    return normalizer


@patch(
    "airflow.sdk.definitions.asset._get_uri_normalizer",
    _mock_get_uri_normalizer_raising_error,
)
def test_sanitize_uri_raises_exception():
    with pytest.raises(ValueError) as e_info:
        _sanitize_uri("postgres://localhost:5432/database.schema.table")
    assert isinstance(e_info.value, ValueError)
    assert str(e_info.value) == "Incorrect URI format"


@patch("airflow.sdk.definitions.asset._get_uri_normalizer", lambda x: None)
def test_normalize_uri_no_normalizer_found():
    asset = Asset(uri="any_uri_without_normalizer_defined")
    assert asset.normalized_uri is None


@patch(
    "airflow.sdk.definitions.asset._get_uri_normalizer",
    _mock_get_uri_normalizer_raising_error,
)
def test_normalize_uri_invalid_uri():
    asset = Asset(uri="any_uri_not_aip60_compliant")
    assert asset.normalized_uri is None


@patch("airflow.sdk.definitions.asset._get_uri_normalizer", _mock_get_uri_normalizer_noop)
@patch("airflow.sdk.definitions.asset._get_normalized_scheme", lambda x: "valid_scheme")
def test_normalize_uri_valid_uri():
    asset = Asset(uri="valid_aip60_uri")
    assert asset.normalized_uri == "valid_aip60_uri"


@pytest.mark.db_test
@pytest.mark.usefixtures("clear_assets")
class TestAssetAliasCondition:
    @pytest.fixture
    def asset_model(self, session):
        """Example asset links to asset alias resolved_asset_alias_2."""
        asset_model = AssetModel(
            id=1,
            uri="test://asset1/",
            name="test_name",
            group="asset",
        )

        session.add(asset_model)
        session.commit()

        return asset_model

    @pytest.fixture
    def asset_alias_1(self, session):
        """Example asset alias links to no assets."""
        asset_alias_model = AssetAliasModel(
            name="test_name",
            group="test",
        )

        session.add(asset_alias_model)
        session.commit()

        return asset_alias_model

    @pytest.fixture
    def resolved_asset_alias_2(self, session, asset_model):
        """Example asset alias links to asset asset_alias_1."""
        asset_alias_2 = AssetAliasModel(name="test_name_2")
        asset_alias_2.assets.append(asset_model)

        session.add(asset_alias_2)
        session.commit()

        return asset_alias_2

    def test_init(self, asset_alias_1, asset_model, resolved_asset_alias_2):
        cond = AssetAliasCondition.from_asset_alias(asset_alias_1)
        assert cond.objects == []

        cond = AssetAliasCondition.from_asset_alias(resolved_asset_alias_2)
        assert cond.objects == [Asset(uri=asset_model.uri, name=asset_model.name)]

    def test_as_expression(self, asset_alias_1, resolved_asset_alias_2):
        for asset_alias in (asset_alias_1, resolved_asset_alias_2):
            cond = AssetAliasCondition.from_asset_alias(asset_alias)
            assert cond.as_expression() == {"alias": {"name": asset_alias.name, "group": asset_alias.group}}

    def test_evalute(self, asset_alias_1, resolved_asset_alias_2, asset_model):
        cond = AssetAliasCondition.from_asset_alias(asset_alias_1)
        assert cond.evaluate({asset_model.uri: True}) is False

        cond = AssetAliasCondition.from_asset_alias(resolved_asset_alias_2)
        assert cond.evaluate({asset_model.uri: True}) is True


class TestAssetSubclasses:
    @pytest.mark.parametrize("subcls, group", ((Model, "model"), (Dataset, "dataset")))
    def test_only_name(self, subcls, group):
        obj = subcls(name="foobar")
        assert obj.name == "foobar"
        assert obj.uri == "foobar"
        assert obj.group == group

    @pytest.mark.parametrize("subcls, group", ((Model, "model"), (Dataset, "dataset")))
    def test_only_uri(self, subcls, group):
        obj = subcls(uri="s3://bucket/key/path")
        assert obj.name == "s3://bucket/key/path"
        assert obj.uri == "s3://bucket/key/path"
        assert obj.group == group

    @pytest.mark.parametrize("subcls, group", ((Model, "model"), (Dataset, "dataset")))
    def test_both_name_and_uri(self, subcls, group):
        obj = subcls("foobar", "s3://bucket/key/path")
        assert obj.name == "foobar"
        assert obj.uri == "s3://bucket/key/path"
        assert obj.group == group

    @pytest.mark.parametrize("arg", ["foobar", "s3://bucket/key/path"])
    @pytest.mark.parametrize("subcls, group", ((Model, "model"), (Dataset, "dataset")))
    def test_only_posarg(self, subcls, group, arg):
        obj = subcls(arg)
        assert obj.name == arg
        assert obj.uri == arg
        assert obj.group == group
