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

from airflow.sdk.definitions.asset import Asset
from airflow.sdk.definitions.asset.decorators import _AssetMainOperator, asset
from airflow.sdk.definitions.decorators import task
from airflow.sdk.execution_time.comms import AssetResult, GetAssetByName


@pytest.fixture
def func_fixer(request):
    name = getattr(request, "param", None) or "example_asset_func"

    def fixer(f):
        """Pretend 'f' is not nested."""
        f.__name__ = f.__qualname__ = name
        return f

    fixer.fixed_name = name
    return fixer


@pytest.fixture
def example_asset_func(func_fixer):
    @func_fixer
    def _example_asset_func():
        return "This is example_asset"

    return _example_asset_func


@pytest.fixture
def example_asset_definition(example_asset_func):
    return asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
        example_asset_func
    )


@pytest.fixture
def example_asset_func_with_valid_arg_as_inlet_asset(func_fixer):
    @func_fixer
    def _example_asset_func(self, context, inlet_asset_1, inlet_asset_2):
        return "This is example_asset"

    return _example_asset_func


@pytest.fixture
def example_asset_func_with_valid_arg_as_inlet_asset_and_default(func_fixer):
    @func_fixer
    def _example_asset_func(
        inlet_asset_1,
        inlet_asset_2="default overwrites valid asset name",
        unknown_name="default supplied for non-asset argument",
    ):
        return "This is example_asset"

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

        with pytest.raises(ValueError, match="nested function not supported"):
            root_func()

    @pytest.mark.parametrize("func_fixer", ("self", "context"), indirect=True)
    def test_with_invalid_asset_name(self, func_fixer):
        @func_fixer
        def example_asset_func():
            pass

        with pytest.raises(ValueError, match=f"prohibited name for asset: {func_fixer.fixed_name}"):
            asset(schedule=None)(example_asset_func)

    def test_with_star(self, func_fixer):
        @func_fixer
        def example_asset_func(*args):
            pass

        with pytest.raises(TypeError) as err:
            asset(schedule=None)(example_asset_func)
        assert err.value.args[0] == "wildcard '*args' is not supported in @asset"

    def test_with_starstar(self, func_fixer):
        @func_fixer
        def example_asset_func(**kwargs):
            pass

        with pytest.raises(TypeError) as err:
            asset(schedule=None)(example_asset_func)
        assert err.value.args[0] == "wildcard '**kwargs' is not supported in @asset"

    def test_with_posonly(self, func_fixer):
        @func_fixer
        def example_asset_func(self, /):
            pass

        with pytest.raises(TypeError) as err:
            asset(schedule=None)(example_asset_func)
        assert (
            err.value.args[0]
            == "positional-only argument 'self' without a default is not supported in @asset"
        )

    def test_with_task_decorator(self, func_fixer):
        @task(retries=3)
        @func_fixer
        def _example_task_func():
            return "This is example_task"

        asset_definition = asset(name="asset", dag_id="dag", schedule=None)(_example_task_func)
        assert asset_definition.name == "asset"
        assert asset_definition._source.dag_id == "dag"
        assert asset_definition._function == _example_task_func

    def test_with_task_decorator_and_outlets(self, func_fixer):
        @task(retries=3, outlets=Asset(name="a"))
        @func_fixer
        def _example_task_func():
            return "This is example_task"

        with pytest.raises(TypeError) as err:
            asset(schedule=None)(_example_task_func)
        assert err.value.args[0] == "@task decorator with 'outlets' argument is not supported in @asset"

    @pytest.mark.parametrize(
        ("provided_uri", "expected_uri"),
        [
            pytest.param(None, "custom", id="default-uri"),
            pytest.param("s3://bucket/object", "s3://bucket/object", id="custom-uri"),
        ],
    )
    def test_custom_name(self, example_asset_func, provided_uri, expected_uri):
        asset_definition = asset(name="custom", uri=provided_uri, schedule=None)(example_asset_func)
        assert asset_definition.name == "custom"
        assert asset_definition.uri == expected_uri

    def test_custom_dag_id(self, example_asset_func):
        asset_definition = asset(name="asset", dag_id="dag", schedule=None)(example_asset_func)
        assert asset_definition.name == "asset"
        assert asset_definition._source.dag_id == "dag"


class TestAssetMultiDecorator:
    def test_multi_asset(self, example_asset_func):
        definition = asset.multi(
            schedule=None,
            outlets=[Asset(name="a"), Asset(name="b")],
        )(example_asset_func)

        assert definition._function == example_asset_func
        assert definition._source.schedule is None
        assert definition._source.outlets == [Asset(name="a"), Asset(name="b")]

    def test_multi_custom_dag_id(self, example_asset_func):
        definition = asset.multi(
            dag_id="custom",
            schedule=None,
            outlets=[Asset(name="a"), Asset(name="b")],
        )(example_asset_func)
        assert definition._source.dag_id == "custom"


class TestAssetDefinition:
    @mock.patch("airflow.sdk.definitions.asset.decorators._AssetMainOperator.from_definition")
    @mock.patch("airflow.sdk.definitions.dag.DAG")
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
            access_control=None,
            owner_links={},
            tags=set(),
            auto_register=True,
        )
        from_definition.assert_called_once_with(asset_definition)

    @mock.patch("airflow.sdk.bases.decorator._TaskDecorator.__call__")
    @mock.patch("airflow.sdk.definitions.dag.DAG")
    def test_with_task_decorator(self, DAG, __call__, func_fixer):
        @task(retries=3)
        @func_fixer
        def _example_task_func():
            return "This is example_task"

        asset_definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            _example_task_func
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
            access_control=None,
            owner_links={},
            tags=set(),
            auto_register=True,
        )
        __call__.assert_called_once_with()
        assert asset_definition._function.kwargs["outlets"] == [asset_definition]


class TestMultiAssetDefinition:
    @mock.patch("airflow.sdk.definitions.asset.decorators._AssetMainOperator.from_definition")
    @mock.patch("airflow.sdk.definitions.dag.DAG")
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
            access_control=None,
            owner_links={},
            tags=set(),
            auto_register=True,
        )
        from_definition.assert_called_once_with(definition)

    @mock.patch("airflow.sdk.bases.decorator._TaskDecorator.__call__")
    @mock.patch("airflow.sdk.definitions.dag.DAG")
    def test_with_task_decorator(self, DAG, __call__, func_fixer):
        @task(retries=3)
        @func_fixer
        def _example_task_func():
            return "This is example_task"

        definition = asset.multi(
            schedule=None,
            outlets=[Asset(name="a"), Asset(name="b")],
        )(_example_task_func)

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
            access_control=None,
            owner_links={},
            tags=set(),
            auto_register=True,
        )
        __call__.assert_called_once_with()
        assert definition._function.kwargs["outlets"] == [Asset(name="a"), Asset(name="b")]


class Test_AssetMainOperator:
    def test_from_definition(self, example_asset_func_with_valid_arg_as_inlet_asset):
        definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            example_asset_func_with_valid_arg_as_inlet_asset
        )
        op = _AssetMainOperator.from_definition(definition)
        assert op.task_id == "example_asset_func"
        assert op.inlets == [Asset.ref(name="inlet_asset_1"), Asset.ref(name="inlet_asset_2")]
        assert op.outlets == [definition]
        assert op.python_callable == example_asset_func_with_valid_arg_as_inlet_asset
        assert op._definition_name == "example_asset_func"

    def test_from_definition_default(self, example_asset_func_with_valid_arg_as_inlet_asset_and_default):
        definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            example_asset_func_with_valid_arg_as_inlet_asset_and_default
        )
        op = _AssetMainOperator.from_definition(definition)
        assert op.task_id == "example_asset_func"
        assert op.inlets == [Asset.ref(name="inlet_asset_1")]
        assert op.outlets == [definition]
        assert op.python_callable == example_asset_func_with_valid_arg_as_inlet_asset_and_default
        assert op._definition_name == "example_asset_func"

    def test_from_definition_multi(self, example_asset_func_with_valid_arg_as_inlet_asset):
        definition = asset.multi(
            schedule=None,
            outlets=[Asset(name="a"), Asset(name="b")],
        )(example_asset_func_with_valid_arg_as_inlet_asset)
        op = _AssetMainOperator.from_definition(definition)
        assert op.task_id == "example_asset_func"
        assert op.inlets == [Asset.ref(name="inlet_asset_1"), Asset.ref(name="inlet_asset_2")]
        assert op.outlets == [Asset(name="a"), Asset(name="b")]
        assert op.python_callable == example_asset_func_with_valid_arg_as_inlet_asset
        assert op._definition_name == "example_asset_func"

    def test_determine_kwargs(
        self,
        mock_supervisor_comms,
        example_asset_func_with_valid_arg_as_inlet_asset,
    ):
        asset_definition = asset(schedule=None, uri="s3://bucket/object", group="MLModel", extra={"k": "v"})(
            example_asset_func_with_valid_arg_as_inlet_asset
        )

        mock_supervisor_comms.send.side_effect = [
            AssetResult(
                name="example_asset_func",
                uri="s3://bucket/object",
                group="MLModel",
                extra={"k": "v"},
            ),
            AssetResult(name="inlet_asset_1", uri="s3://bucket/object1", group="asset", extra=None),
            AssetResult(name="inlet_asset_2", uri="inlet_asset_2", group="asset", extra=None),
        ]

        op = _AssetMainOperator(
            task_id="example_asset_func",
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

        assert mock_supervisor_comms.mock_calls == [
            mock.call.send(GetAssetByName(name="example_asset_func")),
            mock.call.send(GetAssetByName(name="inlet_asset_1")),
            mock.call.send(GetAssetByName(name="inlet_asset_2")),
        ]

    def test_determine_kwargs_defaults(
        self,
        mock_supervisor_comms,
        example_asset_func_with_valid_arg_as_inlet_asset_and_default,
    ):
        asset_definition = asset(schedule=None)(example_asset_func_with_valid_arg_as_inlet_asset_and_default)

        mock_supervisor_comms.send.side_effect = [
            AssetResult(name="inlet_asset_1", uri="s3://bucket/object1", group="asset", extra=None),
        ]

        op = _AssetMainOperator(
            task_id="__main__",
            inlets=[Asset.ref(name="inlet_asset_1")],
            outlets=[asset_definition],
            python_callable=example_asset_func_with_valid_arg_as_inlet_asset_and_default,
            definition_name="example_asset_func",
        )
        assert op.determine_kwargs(context={}) == {
            "inlet_asset_1": Asset(name="inlet_asset_1", uri="s3://bucket/object1"),
            "inlet_asset_2": "default overwrites valid asset name",
            "unknown_name": "default supplied for non-asset argument",
        }

        assert mock_supervisor_comms.mock_calls == [
            mock.call.send(GetAssetByName(name="inlet_asset_1")),
        ]

    def test_from_definition_custom_name(self, mock_supervisor_comms, func_fixer):
        @func_fixer
        def example_asset_func(self):
            pass

        definition = asset(schedule=None, name="custom_name")(example_asset_func)
        op = _AssetMainOperator.from_definition(definition)
        assert op.task_id == "example_asset_func"
        assert op.python_callable == example_asset_func
        assert op._definition_name == "custom_name"

        mock_supervisor_comms.send.side_effect = [
            AssetResult(name="custom_name", uri="s3://bucket/object1", group="Asset")
        ]

        assert op.determine_kwargs(context={}) == {
            "self": Asset(name="custom_name", uri="s3://bucket/object1", group="Asset")
        }

        assert mock_supervisor_comms.mock_calls == [
            mock.call.send(GetAssetByName(name="custom_name", uri="s3://bucket/object1", group="Asset"))
        ]
