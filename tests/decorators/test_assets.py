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

from unittest import mock
from unittest.mock import ANY

import pytest

from airflow.assets import Asset
from airflow.decorators.assets import AssetRef, _AssetMainOperator, asset

pytestmark = pytest.mark.db_test


@pytest.fixture
def example_asset_func(request):
    name = "example_asset_func"
    if getattr(request, "param", None) is not None:
        name = request.param

    def _example_asset_func():
        return "This is example_asset"

    _example_asset_func.__name__ = name
    _example_asset_func.__qualname__ = name
    return _example_asset_func


@pytest.fixture
def example_asset_definition(example_asset_func):
    return asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
        example_asset_func
    )


@pytest.fixture
def example_asset_func_with_valid_arg_as_inlet_asset():
    def _example_asset_func(self, context, inlet_asset_1, inlet_asset_2):
        return "This is example_asset"

    _example_asset_func.__name__ = "example_asset_func"
    _example_asset_func.__qualname__ = "example_asset_func"
    return _example_asset_func


class TestAssetDecorator:
    def test_without_uri(self, example_asset_func):
        asset_definition = asset(schedule=None)(example_asset_func)

        assert asset_definition.name == "example_asset_func"
        assert asset_definition.uri == "example_asset_func"
        assert asset_definition.group == ""
        assert asset_definition.extra == {}
        assert asset_definition.function == example_asset_func
        assert asset_definition.schedule is None

    def test_with_uri(self, example_asset_func):
        asset_definition = asset(schedule=None, uri="s3://bucket/object")(example_asset_func)

        assert asset_definition.name == "example_asset_func"
        assert asset_definition.uri == "s3://bucket/object"
        assert asset_definition.group == ""
        assert asset_definition.extra == {}
        assert asset_definition.function == example_asset_func
        assert asset_definition.schedule is None

    def test_with_group_and_extra(self, example_asset_func):
        asset_definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            example_asset_func
        )
        assert asset_definition.name == "example_asset_func"
        assert asset_definition.uri == "s3://bucket/object"
        assert asset_definition.group == "MLModel"
        assert asset_definition.extra == {"k": "v"}
        assert asset_definition.function == example_asset_func
        assert asset_definition.schedule is None

    def test_nested_function(self):
        def root_func():
            @asset(schedule=None)
            def asset_func():
                pass

        with pytest.raises(ValueError) as err:
            root_func()

        assert err.value.args[0] == "nested function not supported"

    @pytest.mark.parametrize("example_asset_func", ("self", "context"), indirect=True)
    def test_with_invalid_asset_name(self, example_asset_func):
        with pytest.raises(ValueError) as err:
            asset(schedule=None)(example_asset_func)

        assert err.value.args[0].startswith("prohibited name for asset: ")


class TestAssetDefinition:
    def test_serialzie(self, example_asset_definition):
        assert example_asset_definition.serialize() == {
            "extra": {"k": "v"},
            "group": "MLModel",
            "name": "example_asset_func",
            "uri": "s3://bucket/object",
        }

    @mock.patch("airflow.decorators.assets._AssetMainOperator")
    @mock.patch("airflow.decorators.assets.DAG")
    def test__attrs_post_init__(
        self, DAG, _AssetMainOperator, example_asset_func_with_valid_arg_as_inlet_asset
    ):
        asset_definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            example_asset_func_with_valid_arg_as_inlet_asset
        )

        DAG.assert_called_once_with(dag_id="example_asset_func", schedule=None, auto_register=True)
        _AssetMainOperator.assert_called_once_with(
            task_id="__main__",
            inlets=[
                AssetRef(name="inlet_asset_1"),
                AssetRef(name="inlet_asset_2"),
            ],
            outlets=[asset_definition],
            python_callable=ANY,
            definition_name="example_asset_func",
            uri="s3://bucket/object",
        )

        python_callable = _AssetMainOperator.call_args.kwargs["python_callable"]
        assert python_callable.__wrapped__ == example_asset_func_with_valid_arg_as_inlet_asset


class Test_AssetMainOperator:
    def test_determine_kwargs(self, example_asset_func_with_valid_arg_as_inlet_asset):
        asset_definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            example_asset_func_with_valid_arg_as_inlet_asset
        )

        op = _AssetMainOperator(
            task_id="__main__",
            inlets=[],
            outlets=[asset_definition],
            python_callable=example_asset_func_with_valid_arg_as_inlet_asset,
            definition_name="example_asset_func",
        )
        assert op.determine_kwargs(context={"k": "v"}) == {
            "_self": Asset(name="example_asset_func"),
            "context": {"k": "v"},
            "inlet_asset_1": Asset(name="inlet_asset_1"),
            "inlet_asset_2": Asset(name="inlet_asset_2"),
        }
