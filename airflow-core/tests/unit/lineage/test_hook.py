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
from airflow.lineage import hook
from airflow.lineage.hook import (
    AssetLineageInfo,
    HookLineage,
    HookLineageCollector,
    HookLineageReader,
    NoOpCollector,
    get_hook_lineage_collector,
)
from airflow.sdk import BaseHook
from airflow.sdk.definitions.asset import Asset

from tests_common.test_utils.mock_plugins import mock_plugin_manager


class TestHookLineageCollector:
    @pytest.fixture  # default scope is function
    def collector(self):
        return HookLineageCollector()

    def test_generate_hash_handles_non_serializable(self, collector):
        class Obj:
            def __str__(self):
                return "<obj>"

        h1 = collector._generate_hash({"a": Obj()})
        h2 = collector._generate_hash({"a": "<obj>"})
        assert isinstance(h1, str)
        assert h1 == h2

    def test_generate_hash_is_deterministic(self, collector):
        h1 = collector._generate_hash({"foo": "bar"})
        h2 = collector._generate_hash({"foo": "bar"})
        assert h1 == h2

    def test_generate_hash_changes_with_value(self, collector):
        h1 = collector._generate_hash({"foo": "bar"})
        h2 = collector._generate_hash({"foo": "different"})
        assert h1 != h2

    def test_generate_key_matches_generate_asset_entry_id(self, collector):
        """Ensure legacy _generate_key matches new _generate_asset_entry_id."""
        asset = Asset(uri="s3://bucket/a")
        ctx = BaseHook()
        assert collector._generate_key(asset, ctx) == collector._generate_asset_entry_id(asset, ctx)

    def test_generate_asset_entry_id_deterministic(self, collector):
        asset = Asset(uri="s3://bucket/file", extra={"x": 1})
        ctx = BaseHook()
        key1 = collector._generate_asset_entry_id(asset, ctx)
        key2 = collector._generate_asset_entry_id(asset, ctx)
        assert key1 == key2

    def test_generate_asset_entry_id_differs_by_context(self, collector):
        asset = Asset(uri="s3://bucket/file")
        ctx1 = BaseHook()
        ctx2 = BaseHook()
        key1 = collector._generate_asset_entry_id(asset, ctx1)
        key2 = collector._generate_asset_entry_id(asset, ctx2)
        assert key1 != key2

    def test_generate_asset_entry_id_differs_by_extra(self, collector):
        ctx = BaseHook()
        asset1 = Asset(uri="s3://bucket/file", extra={"foo": "bar"})
        asset2 = Asset(uri="s3://bucket/file", extra={"foo": "other"})
        key1 = collector._generate_asset_entry_id(asset1, ctx)
        key2 = collector._generate_asset_entry_id(asset2, ctx)
        assert key1 != key2

    def test_generate_extra_entry_id_deterministic(self, collector):
        ctx = BaseHook()
        key1 = collector._generate_extra_entry_id("k", "v", ctx)
        key2 = collector._generate_extra_entry_id("k", "v", ctx)
        assert key1 == key2

    def test_generate_extra_entry_id_differs_by_context(self, collector):
        ctx1 = BaseHook()
        ctx2 = BaseHook()
        key1 = collector._generate_extra_entry_id("k", "v", ctx1)
        key2 = collector._generate_extra_entry_id("k", "v", ctx2)
        assert key1 != key2

    def test_generate_extra_entry_id_differs_by_key_value(self, collector):
        ctx = BaseHook()
        key1 = collector._generate_extra_entry_id("k", "v1", ctx)
        key2 = collector._generate_extra_entry_id("k", "v2", ctx)
        key3 = collector._generate_extra_entry_id("k2", "v1", ctx)
        assert key1 != key2
        assert key1 != key3

    def test_are_assets_collected(self, collector):
        assert collector is not None
        assert collector.collected_assets == HookLineage()
        input_hook = BaseHook()
        output_hook = BaseHook()
        collector.add_input_asset(input_hook, uri="s3://in_bucket/file", name="asset-1", group="test")
        collector.add_output_asset(
            output_hook,
            uri="postgres://example.com:5432/database/default/table",
        )
        assert collector.collected_assets == HookLineage(
            [
                AssetLineageInfo(
                    asset=Asset(uri="s3://in_bucket/file", name="asset-1", group="test"),
                    count=1,
                    context=input_hook,
                )
            ],
            [
                AssetLineageInfo(
                    asset=Asset(
                        uri="postgres://example.com:5432/database/default/table",
                        name="postgres://example.com:5432/database/default/table",
                        group="asset",
                    ),
                    count=1,
                    context=output_hook,
                )
            ],
        )

    @patch("airflow.lineage.hook.Asset")
    def test_add_input_asset(self, mock_asset, collector):
        asset = MagicMock(spec=Asset, extra={})
        mock_asset.return_value = asset

        mock_hook = MagicMock(spec=BaseHook)
        collector.add_input_asset(mock_hook, uri="test_uri")

        assert next(iter(collector._inputs.values())) == (asset, mock_hook)
        mock_asset.assert_called_once_with(uri="test_uri")

    def test_grouping_assets(self, collector):
        hook_1 = MagicMock(spec=BaseHook)
        hook_2 = MagicMock(spec=BaseHook)

        uri = "test://uri/"

        collector.add_input_asset(context=hook_1, uri=uri)
        collector.add_input_asset(context=hook_2, uri=uri)
        collector.add_input_asset(context=hook_1, uri=uri, asset_extra={"key": "value"})

        collected_inputs = collector.collected_assets.inputs

        assert len(collected_inputs) == 3
        assert collected_inputs[0].asset.uri == "test://uri/"
        assert collected_inputs[0].asset == collected_inputs[1].asset
        assert collected_inputs[0].count == 1
        assert collected_inputs[0].context == collected_inputs[2].context == hook_1
        assert collected_inputs[1].count == 1
        assert collected_inputs[1].context == hook_2
        assert collected_inputs[2].count == 1
        assert collected_inputs[2].asset.extra == {"key": "value"}

    def test_create_asset(self, collector):
        def create_asset(arg1, arg2="default", extra=None):
            return Asset(
                uri=f"myscheme://{arg1}/{arg2}", name=f"asset-{arg1}", group="test", extra=extra or {}
            )

        collector._asset_factories = {"myscheme": create_asset}
        assert collector.create_asset(
            scheme="myscheme",
            uri=None,
            name=None,
            group=None,
            asset_kwargs={"arg1": "value_1"},
            asset_extra=None,
        ) == Asset(uri="myscheme://value_1/default", name="asset-value_1", group="test")
        assert collector.create_asset(
            scheme="myscheme",
            uri=None,
            name=None,
            group=None,
            asset_kwargs={"arg1": "value_1", "arg2": "value_2"},
            asset_extra={"key": "value"},
        ) == Asset(
            uri="myscheme://value_1/value_2", name="asset-value_1", group="test", extra={"key": "value"}
        )

    @patch("airflow.lineage.hook.ProvidersManager")
    def test_create_asset_no_factory(self, mock_providers_manager, collector):
        test_scheme = "myscheme"
        mock_providers_manager.return_value.asset_factories = {}

        test_kwargs = {"arg1": "value_1"}

        assert (
            collector.create_asset(
                scheme=test_scheme,
                uri=None,
                name=None,
                group=None,
                asset_kwargs=test_kwargs,
                asset_extra=None,
            )
            is None
        )

    @patch("airflow.lineage.hook.ProvidersManager")
    def test_create_asset_factory_exception(self, mock_providers_manager, collector):
        def create_asset(extra=None, **kwargs):
            raise RuntimeError("Factory error")

        test_scheme = "myscheme"
        mock_providers_manager.return_value.asset_factories = {test_scheme: create_asset}

        test_kwargs = {"arg1": "value_1"}

        assert (
            collector.create_asset(scheme=test_scheme, uri=None, asset_kwargs=test_kwargs, asset_extra=None)
            is None
        )

    def test_create_asset_missing_parameters_returns_none(self, collector):
        assert collector.create_asset() is None

    def test_collected_assets(self, collector):
        context_input = MagicMock(spec=BaseHook)
        context_output = MagicMock(spec=BaseHook)

        collector.add_input_asset(context_input, uri="test://input")
        collector.add_output_asset(context_output, uri="test://output")

        hook_lineage = collector.collected_assets
        assert len(hook_lineage.inputs) == 1
        assert hook_lineage.inputs[0].asset.uri == "test://input/"
        assert hook_lineage.inputs[0].context == context_input

        assert len(hook_lineage.outputs) == 1
        assert hook_lineage.outputs[0].asset.uri == "test://output/"

    def test_has_collected(self, collector):
        assert not collector.has_collected

        collector._inputs = {"unique_key": (MagicMock(spec=Asset), MagicMock(spec=BaseHook))}
        assert collector.has_collected

    def test_has_collected_only_extra(self, collector):
        assert collector.has_collected is False

        collector.add_extra(MagicMock(spec=BaseHook), "event", "trigger")

        assert collector.has_collected is True
        assert len(collector.collected_assets.inputs) == 0
        assert len(collector.collected_assets.outputs) == 0
        assert len(collector.collected_assets.extra) == 1

    def test_hooks_limit_input_output_assets(self, collector):
        assert not collector.has_collected

        for i in range(1000):
            collector.add_input_asset(MagicMock(spec=BaseHook), uri=f"test://input/{i}")
            collector.add_output_asset(MagicMock(spec=BaseHook), uri=f"test://output/{i}")

        assert collector.has_collected
        assert len(collector._inputs) == 100
        assert len(collector._outputs) == 100

    def test_add_extra(self, collector):
        ctx = MagicMock(spec=BaseHook)
        collector.add_extra(ctx, "k", "v")

        data = collector.collected_assets.extra
        assert len(data) == 1
        assert data[0].key == "k"
        assert data[0].value == "v"
        assert data[0].context == ctx
        assert data[0].count == 1

        # adding again with same values only increments count
        collector.add_extra(ctx, "k", "v")
        assert collector.collected_assets.extra[0].count == 2
        data = collector.collected_assets.extra
        assert len(data) == 1

    def test_add_extra_missing_key_or_value(self, collector):
        ctx = MagicMock(spec=BaseHook)

        collector.add_extra(ctx, "", "v")
        collector.add_extra(ctx, "k", None)

        # nothing added
        assert len(collector.collected_assets.extra) == 0

    def test_extra_limit(self, collector):
        ctx = MagicMock(spec=BaseHook)

        for i in range(501):
            collector.add_extra(ctx, f"k{i}", f"v{i}")

        assert len(collector.collected_assets.extra) == 200

    def test_noop_collector(self):
        noop = NoOpCollector()
        ctx = MagicMock(spec=BaseHook)
        noop.add_input_asset(ctx, uri="x")
        noop.add_output_asset(ctx, uri="y")
        noop.add_extra(ctx, "k", "v")

        lineage = noop.collected_assets
        assert lineage.inputs == []
        assert lineage.outputs == []
        assert lineage.extra == []


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
