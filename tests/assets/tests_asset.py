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

from airflow.assets import (
    Asset,
    AssetAlias,
    AssetAll,
    AssetAny,
    BaseAsset,
    _AssetAliasCondition,
    _get_normalized_scheme,
    _sanitize_uri,
)
from airflow.models.asset import AssetAliasModel, AssetDagRunQueue, AssetModel
from airflow.models.serialized_dag import SerializedDagModel
from airflow.operators.empty import EmptyOperator
from airflow.serialization.serialized_objects import BaseSerialization, SerializedDAG
from tests.test_utils.config import conf_vars


@pytest.fixture
def clear_assets():
    from tests.test_utils.db import clear_db_assets

    clear_db_assets()
    yield
    clear_db_assets()


@pytest.mark.parametrize(
    ["uri"],
    [
        pytest.param("", id="empty"),
        pytest.param("\n\t", id="whitespace"),
        pytest.param("a" * 3001, id="too_long"),
        pytest.param("airflow://xcom/dag/task", id="reserved_scheme"),
        pytest.param("😊", id="non-ascii"),
    ],
)
def test_invalid_uris(uri):
    with pytest.raises(ValueError):
        Asset(uri=uri)


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
    assert list(asset1.iter_assets()) == [("s3://bucket1/data1", asset1)]


@pytest.mark.db_test
def test_asset_iter_asset_aliases():
    base_asset = AssetAll(
        AssetAlias("example-alias-1"),
        Asset("1"),
        AssetAny(
            Asset("2"),
            AssetAlias("example-alias-2"),
            Asset("3"),
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
    assert assets_any == {"s3://bucket1/data1": asset1, "s3://bucket2/data2": asset2}
    assert assets_all == {"s3://bucket1/data1": asset1, "s3://bucket2/data2": asset2}


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
    assets = [Asset(uri=f"s3://abc/{i}") for i in range(123, 126)]
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
        ((False, True, True), False),  # AssetAll requires all conditions to be True, but d1 is False
        ((True, True, True), True),  # All conditions are True
        ((True, False, True), True),  # d1 is True, and AssetAny condition (d2 or d3 being True) is met
        ((True, False, False), False),  # d1 is True, but neither d2 nor d3 meet the AssetAny condition
    ],
)
def test_nested_asset_conditions_with_serialization(status_values, expected_evaluation):
    # Define assets
    d1 = Asset(uri="s3://abc/123")
    d2 = Asset(uri="s3://abc/124")
    d3 = Asset(uri="s3://abc/125")

    # Create a nested condition: AssetAll with d1 and AssetAny with d2 and d3
    nested_condition = AssetAll(d1, AssetAny(d2, d3))

    statuses = {
        d1.uri: status_values[0],
        d2.uri: status_values[1],
        d3.uri: status_values[2],
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
    assets = [Asset(uri=f"hello{i}") for i in range(1, 3)]
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
        session.add(AssetDagRunQueue(dataset_id=am.id, target_dag_id=dag.dag_id))
    session.commit()

    # Fetch and evaluate asset triggers for all DAGs affected by asset events
    records = session.scalars(select(AssetDagRunQueue)).all()
    dag_statuses = defaultdict(lambda: defaultdict(bool))
    for record in records:
        dag_statuses[record.target_dag_id][record.dataset.uri] = True

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
    d1 = Asset(uri="hello1")
    d2 = Asset(uri="hello2")

    # Create and add AssetModel instances to the session
    am1 = AssetModel(uri=d1.uri)
    am2 = AssetModel(uri=d2.uri)
    session.add_all([am1, am2])
    session.commit()

    # Setup a DAG with complex asset triggers (AssetAny with AssetAll)
    with dag_maker(schedule=AssetAny(d1, AssetAll(d2, d1))) as dag:
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
            # If obj1 or obj2 is a Asset, AssetAny, or AssetAll instance,
            # recursively call assets_equal
            if not assets_equal(obj1, obj2):
                return False
        return True

    return False


asset1 = Asset(uri="s3://bucket1/data1")
asset2 = Asset(uri="s3://bucket2/data2")
asset3 = Asset(uri="s3://bucket3/data3")
asset4 = Asset(uri="s3://bucket4/data4")
asset5 = Asset(uri="s3://bucket5/data5")

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


@patch("airflow.assets._get_uri_normalizer", _mock_get_uri_normalizer_raising_error)
@patch("airflow.assets.warnings.warn")
def test_sanitize_uri_raises_warning(mock_warn):
    _sanitize_uri("postgres://localhost:5432/database.schema.table")
    msg = mock_warn.call_args.args[0]
    assert "The Asset URI postgres://localhost:5432/database.schema.table is not AIP-60 compliant" in msg
    assert "In Airflow 3, this will raise an exception." in msg


@patch("airflow.assets._get_uri_normalizer", _mock_get_uri_normalizer_raising_error)
@conf_vars({("core", "strict_asset_uri_validation"): "True"})
def test_sanitize_uri_raises_exception():
    with pytest.raises(ValueError) as e_info:
        _sanitize_uri("postgres://localhost:5432/database.schema.table")
    assert isinstance(e_info.value, ValueError)
    assert str(e_info.value) == "Incorrect URI format"


@patch("airflow.assets._get_uri_normalizer", lambda x: None)
def test_normalize_uri_no_normalizer_found():
    asset = Asset(uri="any_uri_without_normalizer_defined")
    assert asset.normalized_uri is None


@patch("airflow.assets._get_uri_normalizer", _mock_get_uri_normalizer_raising_error)
def test_normalize_uri_invalid_uri():
    asset = Asset(uri="any_uri_not_aip60_compliant")
    assert asset.normalized_uri is None


@patch("airflow.assets._get_uri_normalizer", _mock_get_uri_normalizer_noop)
@patch("airflow.assets._get_normalized_scheme", lambda x: "valid_scheme")
def test_normalize_uri_valid_uri():
    asset = Asset(uri="valid_aip60_uri")
    assert asset.normalized_uri == "valid_aip60_uri"


@pytest.mark.skip_if_database_isolation_mode
@pytest.mark.db_test
@pytest.mark.usefixtures("clear_assets")
class Test_AssetAliasCondition:
    @pytest.fixture
    def asset_1(self, session):
        """Example asset links to asset alias resolved_asset_alias_2."""
        asset_uri = "test_uri"
        asset_1 = AssetModel(id=1, uri=asset_uri)

        session.add(asset_1)
        session.commit()

        return asset_1

    @pytest.fixture
    def asset_alias_1(self, session):
        """Example asset alias links to no assets."""
        alias_name = "test_name"
        asset_alias_model = AssetAliasModel(name=alias_name)

        session.add(asset_alias_model)
        session.commit()

        return asset_alias_model

    @pytest.fixture
    def resolved_asset_alias_2(self, session, asset_1):
        """Example asset alias links to asset asset_alias_1."""
        asset_name = "test_name_2"
        asset_alias_2 = AssetAliasModel(name=asset_name)
        asset_alias_2.datasets.append(asset_1)

        session.add(asset_alias_2)
        session.commit()

        return asset_alias_2

    def test_init(self, asset_alias_1, asset_1, resolved_asset_alias_2):
        cond = _AssetAliasCondition(name=asset_alias_1.name)
        assert cond.objects == []

        cond = _AssetAliasCondition(name=resolved_asset_alias_2.name)
        assert cond.objects == [Asset(uri=asset_1.uri)]

    def test_as_expression(self, asset_alias_1, resolved_asset_alias_2):
        for assset_alias in (asset_alias_1, resolved_asset_alias_2):
            cond = _AssetAliasCondition(assset_alias.name)
            assert cond.as_expression() == {"alias": assset_alias.name}

    def test_evalute(self, asset_alias_1, resolved_asset_alias_2, asset_1):
        cond = _AssetAliasCondition(asset_alias_1.name)
        assert cond.evaluate({asset_1.uri: True}) is False

        cond = _AssetAliasCondition(resolved_asset_alias_2.name)
        assert cond.evaluate({asset_1.uri: True}) is True
