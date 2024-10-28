#
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

from unittest.mock import MagicMock, patch

import pytest

from airflow import plugins_manager
from airflow.assets import Asset
from airflow.hooks.base import BaseHook
from airflow.lineage import hook
from airflow.lineage.hook import (
    AssetLineageInfo,
    HookLineage,
    HookLineageCollector,
    HookLineageReader,
    NoOpCollector,
    get_hook_lineage_collector,
)

from tests_common.test_utils.mock_plugins import mock_plugin_manager


class TestHookLineageCollector:
    def setup_method(self):
        self.collector = HookLineageCollector()

    def test_are_assets_collected(self):
        assert self.collector is not None
        assert self.collector.collected_assets == HookLineage()
        input_hook = BaseHook()
        output_hook = BaseHook()
        self.collector.add_input_asset(input_hook, uri="s3://in_bucket/file")
        self.collector.add_output_asset(
            output_hook, uri="postgres://example.com:5432/database/default/table"
        )
        assert self.collector.collected_assets == HookLineage(
            [
                AssetLineageInfo(
                    asset=Asset("s3://in_bucket/file"), count=1, context=input_hook
                )
            ],
            [
                AssetLineageInfo(
                    asset=Asset("postgres://example.com:5432/database/default/table"),
                    count=1,
                    context=output_hook,
                )
            ],
        )

    @patch("airflow.lineage.hook.Asset")
    def test_add_input_asset(self, mock_asset):
        asset = MagicMock(spec=Asset, extra={})
        mock_asset.return_value = asset

        hook = MagicMock()
        self.collector.add_input_asset(hook, uri="test_uri")

        assert next(iter(self.collector._inputs.values())) == (asset, hook)
        mock_asset.assert_called_once_with(uri="test_uri", extra=None)

    def test_grouping_assets(self):
        hook_1 = MagicMock()
        hook_2 = MagicMock()

        uri = "test://uri/"

        self.collector.add_input_asset(context=hook_1, uri=uri)
        self.collector.add_input_asset(context=hook_2, uri=uri)
        self.collector.add_input_asset(
            context=hook_1, uri=uri, asset_extra={"key": "value"}
        )

        collected_inputs = self.collector.collected_assets.inputs

        assert len(collected_inputs) == 3
        assert collected_inputs[0].asset.uri == "test://uri/"
        assert collected_inputs[0].asset == collected_inputs[1].asset
        assert collected_inputs[0].count == 1
        assert collected_inputs[0].context == collected_inputs[2].context == hook_1
        assert collected_inputs[1].count == 1
        assert collected_inputs[1].context == hook_2
        assert collected_inputs[2].count == 1
        assert collected_inputs[2].asset.extra == {"key": "value"}

    @patch("airflow.lineage.hook.ProvidersManager")
    def test_create_asset(self, mock_providers_manager):
        def create_asset(arg1, arg2="default", extra=None):
            return Asset(uri=f"myscheme://{arg1}/{arg2}", extra=extra or {})

        mock_providers_manager.return_value.asset_factories = {"myscheme": create_asset}
        assert self.collector.create_asset(
            scheme="myscheme",
            uri=None,
            asset_kwargs={"arg1": "value_1"},
            asset_extra=None,
        ) == Asset("myscheme://value_1/default")
        assert self.collector.create_asset(
            scheme="myscheme",
            uri=None,
            asset_kwargs={"arg1": "value_1", "arg2": "value_2"},
            asset_extra={"key": "value"},
        ) == Asset("myscheme://value_1/value_2", extra={"key": "value"})

    @patch("airflow.lineage.hook.ProvidersManager")
    def test_create_asset_no_factory(self, mock_providers_manager):
        test_scheme = "myscheme"
        mock_providers_manager.return_value.asset_factories = {}

        test_kwargs = {"arg1": "value_1"}

        assert (
            self.collector.create_asset(
                scheme=test_scheme, uri=None, asset_kwargs=test_kwargs, asset_extra=None
            )
            is None
        )

    @patch("airflow.lineage.hook.ProvidersManager")
    def test_create_asset_factory_exception(self, mock_providers_manager):
        def create_asset(extra=None, **kwargs):
            raise RuntimeError("Factory error")

        test_scheme = "myscheme"
        mock_providers_manager.return_value.asset_factories = {test_scheme: create_asset}

        test_kwargs = {"arg1": "value_1"}

        assert (
            self.collector.create_asset(
                scheme=test_scheme, uri=None, asset_kwargs=test_kwargs, asset_extra=None
            )
            is None
        )

    def test_collected_assets(self):
        context_input = MagicMock()
        context_output = MagicMock()

        self.collector.add_input_asset(context_input, uri="test://input")
        self.collector.add_output_asset(context_output, uri="test://output")

        hook_lineage = self.collector.collected_assets
        assert len(hook_lineage.inputs) == 1
        assert hook_lineage.inputs[0].asset.uri == "test://input/"
        assert hook_lineage.inputs[0].context == context_input

        assert len(hook_lineage.outputs) == 1
        assert hook_lineage.outputs[0].asset.uri == "test://output/"

    def test_has_collected(self):
        collector = HookLineageCollector()
        assert not collector.has_collected

        collector._inputs = {"unique_key": (MagicMock(spec=Asset), MagicMock())}
        assert collector.has_collected


class FakePlugin(plugins_manager.AirflowPlugin):
    name = "FakePluginHavingHookLineageCollector"
    hook_lineage_readers = [HookLineageReader]


@pytest.mark.parametrize(
    "has_readers, expected_class",
    [
        (True, HookLineageCollector),
        (False, NoOpCollector),
    ],
)
def test_get_hook_lineage_collector(has_readers, expected_class):
    # reset global variable
    hook._hook_lineage_collector = None
    plugins = [FakePlugin()] if has_readers else []
    with mock_plugin_manager(plugins=plugins):
        assert isinstance(get_hook_lineage_collector(), expected_class)
        assert get_hook_lineage_collector() is get_hook_lineage_collector()
