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
from typing import Callable
from unittest import mock

import pytest

from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk.definitions.asset import (
    Asset,
    AssetAlias,
    AssetAll,
    AssetAny,
    AssetUniqueKey,
    BaseAsset,
    Dataset,
    Model,
    _get_normalized_scheme,
    _sanitize_uri,
)
from airflow.sdk.definitions.dag import DAG
from airflow.serialization.serialized_objects import SerializedDAG

ASSET_MODULE_PATH = "airflow.sdk.definitions.asset"


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


asset1 = Asset(uri="s3://bucket1/data1", name="asset-1")
asset2 = Asset(uri="s3://bucket2/data2", name="asset-2")
asset3 = Asset(uri="s3://bucket3/data3", name="asset-3")
asset4 = Asset(uri="s3://bucket4/data4", name="asset-4")
asset5 = Asset(uri="s3://bucket5/data5", name="asset-5")


def test_asset_logic_operations():
    result_or = asset1 | asset2
    assert isinstance(result_or, AssetAny)
    result_and = asset1 & asset2
    assert isinstance(result_and, AssetAll)


def test_asset_iter_assets():
    assert list(asset1.iter_assets()) == [(AssetUniqueKey("asset-1", "s3://bucket1/data1"), asset1)]


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


@pytest.fixture
def create_test_assets():
    """Fixture to create test assets and corresponding models."""
    return [Asset(uri=f"test://asset{i}", name=f"hello{i}") for i in range(1, 3)]


def test_asset_trigger_setup_and_serialization(create_test_assets):
    assets = create_test_assets

    # Create DAG with asset triggers
    with DAG(dag_id="test", schedule=AssetAny(*assets), catchup=False) as dag:
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


@mock.patch(
    "airflow.sdk.definitions.asset._get_uri_normalizer",
    _mock_get_uri_normalizer_raising_error,
)
def test_sanitize_uri_raises_exception():
    with pytest.raises(ValueError) as e_info:
        _sanitize_uri("postgres://localhost:5432/database.schema.table")
    assert isinstance(e_info.value, ValueError)
    assert str(e_info.value) == "Incorrect URI format"


@mock.patch("airflow.sdk.definitions.asset._get_uri_normalizer", return_value=None)
def test_normalize_uri_no_normalizer_found(mock_get_uri_normalizer):
    asset = Asset(uri="any_uri_without_normalizer_defined")
    assert asset.normalized_uri is None


@mock.patch(
    "airflow.sdk.definitions.asset._get_uri_normalizer",
    _mock_get_uri_normalizer_raising_error,
)
def test_normalize_uri_invalid_uri():
    asset = Asset(uri="any_uri_not_aip60_compliant")
    assert asset.normalized_uri is None


@mock.patch("airflow.sdk.definitions.asset._get_uri_normalizer", _mock_get_uri_normalizer_noop)
@mock.patch("airflow.sdk.definitions.asset._get_normalized_scheme", return_value="valid_scheme")
def test_normalize_uri_valid_uri(mock_get_normalized_scheme):
    asset = Asset(uri="valid_aip60_uri")
    assert asset.normalized_uri == "valid_aip60_uri"


class TestAssetAlias:
    def test_as_expression(self):
        alias = AssetAlias(name="test_name", group="test")
        assert alias.as_expression() == {"alias": {"name": alias.name, "group": alias.group}}


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
