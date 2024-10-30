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

from airflow.decorators.assets import asset

pytestmark = pytest.mark.db_test


@pytest.fixture
def clear_assets():
    from tests_common.test_utils.db import clear_db_assets

    clear_db_assets()
    yield
    clear_db_assets()


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
def example_asset_func_with_valid_arg_as_inlet_asset():
    def _example_asset_func(inlet_asset_1, inlet_asset_2):
        return "This is example_asset"

    _example_asset_func.__name__ = "context"
    _example_asset_func.__qualname__ = "context"
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
