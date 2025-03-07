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

import pytest

from airflow.models.asset import AssetModel
from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.asset.decorators import _AssetMainOperator, asset


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
        assert asset_definition.group == "asset"
        assert asset_definition.extra == {}
        assert asset_definition._function == example_asset_func
        assert asset_definition._source.schedule is None

    def test_with_uri(self, example_asset_func):
        asset_definition = asset(schedule=None, uri="s3://bucket/object")(example_asset_func)

        assert asset_definition.name == "example_asset_func"
        assert asset_definition.uri == "s3://bucket/object"
        assert asset_definition.group == "asset"
        assert asset_definition.extra == {}
        assert asset_definition._function == example_asset_func
        assert asset_definition._source.schedule is None

    def test_with_group_and_extra(self, example_asset_func):
        asset_definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            example_asset_func
        )
        assert asset_definition.name == "example_asset_func"
        assert asset_definition.uri == "s3://bucket/object"
        assert asset_definition.group == "MLModel"
        assert asset_definition.extra == {"k": "v"}
        assert asset_definition._function == example_asset_func
        assert asset_definition._source.schedule is None

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


class TestAssetMultiDecorator:
    def test_multi_asset(self, example_asset_func):
        definition = asset.multi(
            schedule=None,
            outlets=[Asset(name="a"), Asset(name="b")],
        )(example_asset_func)

        assert definition._function == example_asset_func
        assert definition._source.schedule is None
        assert definition._source.outlets == [Asset(name="a"), Asset(name="b")]


class TestAssetDefinition:
    @mock.patch("airflow.sdk.definitions.asset.decorators._AssetMainOperator.from_definition")
    @mock.patch("airflow.models.dag.DAG")
    def test__attrs_post_init__(self, DAG, from_definition, example_asset_func_with_valid_arg_as_inlet_asset):
        asset_definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            example_asset_func_with_valid_arg_as_inlet_asset
        )

        DAG.assert_called_once_with(
            dag_id="example_asset_func",
            dag_display_name="example_asset_func",
            description=None,
            schedule=None,
            catchup=False,
            is_paused_upon_creation=None,
            on_failure_callback=None,
            on_success_callback=None,
            params=None,
            auto_register=True,
        )
        from_definition.assert_called_once_with(asset_definition)


class TestMultiAssetDefinition:
    @mock.patch("airflow.sdk.definitions.asset.decorators._AssetMainOperator.from_definition")
    @mock.patch("airflow.models.dag.DAG")
    def test__attrs_post_init__(self, DAG, from_definition, example_asset_func_with_valid_arg_as_inlet_asset):
        definition = asset.multi(
            schedule=None,
            outlets=[Asset(name="a"), Asset(name="b")],
        )(example_asset_func_with_valid_arg_as_inlet_asset)

        DAG.assert_called_once_with(
            dag_id="example_asset_func",
            dag_display_name="example_asset_func",
            description=None,
            schedule=None,
            catchup=False,
            is_paused_upon_creation=None,
            on_failure_callback=None,
            on_success_callback=None,
            params=None,
            auto_register=True,
        )
        from_definition.assert_called_once_with(definition)


class Test_AssetMainOperator:
    def test_from_definition(self, example_asset_func_with_valid_arg_as_inlet_asset):
        definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            example_asset_func_with_valid_arg_as_inlet_asset
        )
        op = _AssetMainOperator.from_definition(definition)
        assert op.task_id == "__main__"
        assert op.inlets == [Asset.ref(name="inlet_asset_1"), Asset.ref(name="inlet_asset_2")]
        assert op.outlets == [definition]
        assert op.python_callable == example_asset_func_with_valid_arg_as_inlet_asset
        assert op._definition_name == "example_asset_func"

    def test_from_definition_multi(self, example_asset_func_with_valid_arg_as_inlet_asset):
        definition = asset.multi(
            schedule=None,
            outlets=[Asset(name="a"), Asset(name="b")],
        )(example_asset_func_with_valid_arg_as_inlet_asset)
        op = _AssetMainOperator.from_definition(definition)
        assert op.task_id == "__main__"
        assert op.inlets == [Asset.ref(name="inlet_asset_1"), Asset.ref(name="inlet_asset_2")]
        assert op.outlets == [Asset(name="a"), Asset(name="b")]
        assert op.python_callable == example_asset_func_with_valid_arg_as_inlet_asset
        assert op._definition_name == "example_asset_func"

    @mock.patch("airflow.models.asset.fetch_active_assets_by_name")
    @mock.patch("airflow.utils.session.create_session")
    def test_determine_kwargs(
        self,
        mock_create_session,
        mock_fetch_active_assets_by_name,
        example_asset_func_with_valid_arg_as_inlet_asset,
    ):
        asset_definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            example_asset_func_with_valid_arg_as_inlet_asset
        )

        class FakeSession:
            def __enter__(self):
                return self

            def __exit__(self, *args, **kwargs):
                pass

        mock_create_session.return_value = fake_session = FakeSession()
        mock_fetch_active_assets_by_name.return_value = {
            "example_asset_func": AssetModel.from_public(asset_definition),
            "inlet_asset_1": AssetModel(uri="s3://bucket/object1", name="inlet_asset_1"),
        }

        op = _AssetMainOperator(
            task_id="__main__",
            inlets=[Asset.ref(name="inlet_asset_1"), Asset.ref(name="inlet_asset_2")],
            outlets=[asset_definition],
            python_callable=example_asset_func_with_valid_arg_as_inlet_asset,
            definition_name="example_asset_func",
        )
        assert op.determine_kwargs(context={"k": "v"}) == {
            "self": Asset(
                name="example_asset_func",
                uri="s3://bucket/object",
                group="MLModel",
                extra={"k": "v"},
            ),
            "context": {"k": "v"},
            "inlet_asset_1": Asset(name="inlet_asset_1", uri="s3://bucket/object1"),
            "inlet_asset_2": Asset(name="inlet_asset_2"),
        }

        assert mock_fetch_active_assets_by_name.mock_calls == [
            mock.call({"example_asset_func", "inlet_asset_1", "inlet_asset_2"}, fake_session),
        ]
