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

    def test_add_asset_count_tracking(self, collector):
        """Test that duplicate assets are counted correctly."""
        ctx = MagicMock(spec=BaseHook)

        # Add same input multiple times
        collector.add_input_asset(ctx, uri="s3://bucket/input")
        collector.add_input_asset(ctx, uri="s3://bucket/input")
        collector.add_input_asset(ctx, uri="s3://bucket/input")

        # Add same output multiple times
        collector.add_output_asset(ctx, uri="s3://bucket/output")
        collector.add_output_asset(ctx, uri="s3://bucket/output")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == "s3://bucket/input"
        assert lineage.inputs[0].count == 3

        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.uri == "s3://bucket/output"
        assert lineage.outputs[0].count == 2

    def test_add_asset_different_uris(self, collector):
        """Test that different URIs are tracked separately."""
        ctx = MagicMock(spec=BaseHook)

        collector.add_input_asset(ctx, uri="s3://bucket/file1")
        collector.add_input_asset(ctx, uri="s3://bucket/file2")
        collector.add_input_asset(ctx, uri="postgres://example.com:5432/database/default/table")

        collector.add_output_asset(ctx, uri="s3://output/file1")
        collector.add_output_asset(ctx, uri="s3://output/file2")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 3
        assert lineage.inputs[0].asset.uri == "s3://bucket/file1"
        assert lineage.inputs[1].asset.uri == "s3://bucket/file2"
        assert lineage.inputs[2].asset.uri == "postgres://example.com:5432/database/default/table"

        assert len(lineage.outputs) == 2
        assert lineage.outputs[0].asset.uri == "s3://output/file1"
        assert lineage.outputs[1].asset.uri == "s3://output/file2"

    def test_add_asset_different_contexts(self, collector):
        """Test that different contexts are tracked separately."""
        ctx1 = MagicMock(spec=BaseHook)
        ctx2 = MagicMock(spec=BaseHook)

        collector.add_input_asset(ctx1, uri="s3://bucket/file")
        collector.add_input_asset(ctx2, uri="s3://bucket/file")

        collector.add_output_asset(ctx1, uri="s3://output/file")
        collector.add_output_asset(ctx2, uri="s3://output/file")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 2
        assert lineage.inputs[0].context == ctx1
        assert lineage.inputs[1].context == ctx2

        assert len(lineage.outputs) == 2
        assert lineage.outputs[0].context == ctx1
        assert lineage.outputs[1].context == ctx2

    def test_add_asset_with_extra_metadata(self, collector):
        """Test adding assets with extra metadata."""
        ctx = MagicMock(spec=BaseHook)

        collector.add_input_asset(
            ctx,
            uri="postgres://example.com:5432/database/default/table",
            asset_extra={"schema": "public", "table": "users"},
        )
        collector.add_output_asset(
            ctx,
            uri="postgres://example.com:5432/database/default/table",
            asset_extra={"schema": "public", "table": "results"},
        )

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.extra == {"schema": "public", "table": "users"}

        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.extra == {"schema": "public", "table": "results"}

    def test_add_asset_different_extra_values(self, collector):
        """Test that assets with different extra values are tracked separately."""
        ctx = MagicMock(spec=BaseHook)

        # Same URI but different extra metadata
        collector.add_input_asset(ctx, uri="s3://bucket/file", asset_extra={"version": "1"})
        collector.add_input_asset(ctx, uri="s3://bucket/file", asset_extra={"version": "2"})

        collector.add_output_asset(ctx, uri="s3://output/file", asset_extra={"format": "parquet"})
        collector.add_output_asset(ctx, uri="s3://output/file", asset_extra={"format": "csv"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 2
        assert lineage.inputs[0].asset.extra == {"version": "1"}
        assert lineage.inputs[1].asset.extra == {"version": "2"}

        assert len(lineage.outputs) == 2
        assert lineage.outputs[0].asset.extra == {"format": "parquet"}
        assert lineage.outputs[1].asset.extra == {"format": "csv"}

    def test_hooks_limit_input_output_assets(self, collector):
        assert not collector.has_collected

        for i in range(1000):
            collector.add_input_asset(MagicMock(spec=BaseHook), uri=f"test://input/{i}")
            collector.add_output_asset(MagicMock(spec=BaseHook), uri=f"test://output/{i}")

        assert collector.has_collected
        assert len(collector._inputs) == 100
        assert len(collector._outputs) == 100

    @pytest.mark.parametrize("uri", ["", None])
    def test_invalid_uri_none(self, collector, uri):
        """Test handling of None or empty URI - should not raise."""
        ctx = MagicMock(spec=BaseHook)

        # Should not raise exceptions
        collector.add_input_asset(ctx, uri=uri)
        collector.add_output_asset(ctx, uri=uri)

        # Collector should handle gracefully and not collect invalid URIs
        assert not collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 0
        assert len(lineage.outputs) == 0

    def test_malformed_uri(self, collector):
        """Test handling of malformed URIs - should not raise."""
        ctx = MagicMock(spec=BaseHook)

        # Various malformed URIs should not cause crashes
        collector.add_input_asset(ctx, uri="not-a-valid-uri")
        collector.add_input_asset(ctx, uri="://missing-scheme")
        collector.add_input_asset(ctx, uri="scheme:")
        collector.add_output_asset(ctx, uri="//no-scheme")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 3
        assert lineage.inputs[0].asset.uri == "not-a-valid-uri"
        assert lineage.inputs[1].asset.uri == "://missing-scheme"
        assert lineage.inputs[2].asset.uri == "scheme:/"

        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.uri == "//no-scheme"

    def test_very_long_uri(self, collector):
        """Test handling of very long URIs - 1000 chars OK, 2000 chars raises ValueError."""
        ctx = MagicMock(spec=BaseHook)

        # Create very long URI (1000 chars - should work)
        long_path = "a" * 1000
        long_uri = f"s3://bucket/{long_path}"

        # Create too long URI (2000 chars - should raise)
        too_long_uri = f"s3://bucket/{long_path * 2}"

        collector.add_input_asset(ctx, uri=long_uri)

        # Too long URI should raise ValueError
        with pytest.raises(ValueError, match="Asset name cannot exceed"):
            collector.add_output_asset(ctx, uri=too_long_uri)

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == long_uri

        assert len(lineage.outputs) == 0

    def test_uri_with_special_characters(self, collector):
        """Test URIs with special characters - should not raise."""
        ctx = MagicMock(spec=BaseHook)

        # URIs with various special characters
        special_uris = {
            "s3://bucket/path with spaces/file": "s3://bucket/path with spaces/file",
            "s3://bucket/path%20encoded/file": "s3://bucket/path%20encoded/file",
            "file:///path/with/üñíçødé/chars": "file:///path/with/üñíçødé/chars",
            "scheme://host/path?query=value&other=123": "scheme://host/path?other=123&query=value",
            "scheme://host/path#fragment": "scheme://host/path",
            "postgres://user:p@ss!word@host:5432/db/sche$ma/table": "postgres://host:5432/db/sche$ma/table",
        }

        for uri in special_uris:
            collector.add_input_asset(ctx, uri=uri)

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 6

        for i, expected_uri in enumerate(special_uris.values()):
            assert lineage.inputs[i].asset.uri == expected_uri

    def test_empty_asset_extra(self, collector):
        """Test that empty asset_extra is handled correctly."""
        ctx = MagicMock(spec=BaseHook)

        collector.add_input_asset(ctx, uri="s3://bucket/file", asset_extra={})
        collector.add_output_asset(ctx, uri="s3://bucket/file", asset_extra={})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.extra == {}
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.extra == {}

    def test_asset_with_all_optional_parameters(self, collector):
        """Test asset creation with all optional parameters provided."""
        ctx = MagicMock(spec=BaseHook)

        collector.add_input_asset(
            ctx,
            uri="s3://bucket/file",
            name="custom-name",
            group="custom-group",
            asset_extra={"key": "value"},
        )

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == "s3://bucket/file"
        assert lineage.inputs[0].asset.name == "custom-name"
        assert lineage.inputs[0].asset.group == "custom-group"
        assert lineage.inputs[0].asset.extra == {"key": "value"}

    def test_asset_extra_with_non_serializable(self, collector):
        """Test that asset_extra with non-JSON-serializable values is handled."""
        ctx = MagicMock(spec=BaseHook)

        class CustomObject:
            pass

        # Should not raise
        collector.add_input_asset(ctx, uri="s3://bucket/file", asset_extra={"obj": CustomObject()})

        # May or may not be collected depending on implementation
        lineage = collector.collected_assets
        # Just verify it doesn't crash and structure is intact
        assert isinstance(lineage.inputs, list)
        assert isinstance(lineage.outputs, list)

    def test_empty_name_and_group(self, collector):
        """Test that empty strings for name and group are handled."""
        ctx = MagicMock(spec=BaseHook)

        # Empty strings for optional parameters
        collector.add_input_asset(ctx, uri="s3://bucket/file", name="", group="")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == "s3://bucket/file"
        assert lineage.inputs[0].asset.name == "s3://bucket/file"
        assert lineage.inputs[0].asset.group == "asset"

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

    def test_add_extra_different_values(self, collector):
        """Test that different values are tracked separately."""
        ctx = MagicMock(spec=BaseHook)

        collector.add_extra(ctx, "key1", {"data": "value1"})
        collector.add_extra(ctx, "key2", {"data": "value2"})
        collector.add_extra(ctx, "key1", {"data": "value3"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 3
        assert lineage.extra[0].key == "key1"
        assert lineage.extra[0].value == {"data": "value1"}
        assert lineage.extra[0].count == 1
        assert lineage.extra[1].key == "key2"
        assert lineage.extra[1].value == {"data": "value2"}
        assert lineage.extra[1].count == 1
        assert lineage.extra[2].key == "key1"
        assert lineage.extra[2].value == {"data": "value3"}
        assert lineage.extra[2].count == 1

    def test_add_extra_different_contexts(self, collector):
        """Test that different contexts are tracked separately."""
        ctx1 = MagicMock(spec=BaseHook)
        ctx2 = MagicMock(spec=BaseHook)

        collector.add_extra(ctx1, "test_key", {"data": "value"})
        collector.add_extra(ctx2, "test_key", {"data": "value"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 2
        assert lineage.extra[0].context == ctx1
        assert lineage.extra[1].context == ctx2

    def test_add_extra_complex_values(self, collector):
        """Test that add_extra handles complex JSON-serializable values."""
        ctx = MagicMock(spec=BaseHook)

        collector.add_extra(ctx, "dict", {"nested": {"data": "value"}})
        collector.add_extra(ctx, "list", [1, 2, 3, "test"])
        collector.add_extra(ctx, "number", 42)
        collector.add_extra(ctx, "string", "simple string")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 4
        assert lineage.extra[0].value == {"nested": {"data": "value"}}
        assert lineage.extra[1].value == [1, 2, 3, "test"]
        assert lineage.extra[2].value == 42
        assert lineage.extra[3].value == "simple string"

    def test_special_characters_in_extra_key(self, collector):
        """Test that extra keys with special characters work."""
        ctx = MagicMock(spec=BaseHook)

        collector.add_extra(ctx, "key-with-dashes", {"data": "value"})
        collector.add_extra(ctx, "key.with.dots", {"data": "value"})
        collector.add_extra(ctx, "key_with_underscores", {"data": "value"})
        collector.add_extra(ctx, "key/with/slashes", {"data": "value"})
        collector.add_extra(ctx, "key:with:colons", {"data": "value"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 5
        assert lineage.extra[0].key == "key-with-dashes"
        assert lineage.extra[1].key == "key.with.dots"
        assert lineage.extra[2].key == "key_with_underscores"
        assert lineage.extra[3].key == "key/with/slashes"
        assert lineage.extra[4].key == "key:with:colons"

    def test_unicode_in_extra_key_and_value(self, collector):
        """Test that unicode characters in extra work correctly."""
        ctx = MagicMock(spec=BaseHook)

        collector.add_extra(ctx, "clé_française", {"données": "valeur"})
        collector.add_extra(ctx, "中文键", {"中文": "值"})
        collector.add_extra(ctx, "مفتاح", {"بيانات": "قيمة"})
        collector.add_extra(ctx, "emoji_🚀", {"status": "✅"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 4
        assert lineage.extra[0].key == "clé_française"
        assert lineage.extra[0].value == {"données": "valeur"}
        assert lineage.extra[1].key == "中文键"
        assert lineage.extra[1].value == {"中文": "值"}
        assert lineage.extra[2].key == "مفتاح"
        assert lineage.extra[2].value == {"بيانات": "قيمة"}
        assert lineage.extra[3].key == "emoji_🚀"
        assert lineage.extra[3].value == {"status": "✅"}

    def test_very_large_extra_value(self, collector):
        """Test that large extra values are handled."""
        ctx = MagicMock(spec=BaseHook)

        # Create a large value
        large_value = {"data": "x" * 1000, "list": list(range(1000))}

        collector.add_extra(ctx, "large_key", large_value)

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 1
        assert lineage.extra[0].key == "large_key"
        assert lineage.extra[0].value == large_value

    def test_deeply_nested_extra_value(self, collector):
        """Test that deeply nested data structures in extra are handled."""
        ctx = MagicMock(spec=BaseHook)

        # Create deeply nested structure
        nested_value = {"level1": {"level2": {"level3": {"level4": {"level5": {"data": "deep"}}}}}}

        collector.add_extra(ctx, "nested", nested_value)

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 1
        assert lineage.extra[0].value == nested_value

    def test_extra_value_with_various_types(self, collector):
        """Test that extra can handle various data types."""
        ctx = MagicMock(spec=BaseHook)

        collector.add_extra(ctx, "string", "text")
        collector.add_extra(ctx, "integer", 42)
        collector.add_extra(ctx, "float", 3.14)
        collector.add_extra(ctx, "boolean", True)
        collector.add_extra(ctx, "list", [1, 2, 3])
        collector.add_extra(ctx, "dict", {"key": "value"})
        collector.add_extra(ctx, "null", None)

        assert collector.has_collected

        # None value should not be collected (based on validation)
        lineage = collector.collected_assets
        assert len(lineage.extra) == 6  # None is filtered out

        assert lineage.extra[0].value == "text"
        assert lineage.extra[1].value == 42
        assert lineage.extra[2].value == 3.14
        assert lineage.extra[3].value is True
        assert lineage.extra[4].value == [1, 2, 3]
        assert lineage.extra[5].value == {"key": "value"}

    def test_non_json_serializable_value_in_extra(self, collector):
        """Test that non-JSON-serializable values are handled gracefully."""
        ctx = MagicMock(spec=BaseHook)

        # Create a non-serializable object
        class CustomObject:
            def __str__(self):
                return "custom_object"

        # Should not raise - collector should handle via str conversion or skip
        collector.add_extra(ctx, "custom_key", CustomObject())

        # May or may not be collected depending on implementation
        lineage = collector.collected_assets
        # Just verify it doesn't crash
        assert isinstance(lineage.extra, list)

    def test_extremely_long_extra_key(self, collector):
        """Test that extremely long extra keys are handled."""
        ctx = MagicMock(spec=BaseHook)

        long_key = "k" * 1000
        collector.add_extra(ctx, long_key, "value")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 1
        assert lineage.extra[0].key == long_key
        assert lineage.extra[0].value == "value"

    def test_collected_assets_called_multiple_times(self, collector):
        """Test that collected_assets property can be called multiple times."""
        ctx = MagicMock(spec=BaseHook)

        collector.add_input_asset(ctx, uri="s3://bucket/file")

        assert collector.has_collected

        # Call multiple times - should return same data
        lineage1 = collector.collected_assets
        lineage2 = collector.collected_assets
        lineage3 = collector.collected_assets

        assert lineage1.inputs == lineage2.inputs == lineage3.inputs
        assert len(lineage1.inputs) == 1

    def test_has_collected_only_extra(self, collector):
        assert collector.has_collected is False

        collector.add_extra(MagicMock(spec=BaseHook), "event", "trigger")

        assert collector.has_collected is True
        assert len(collector.collected_assets.inputs) == 0
        assert len(collector.collected_assets.outputs) == 0
        assert len(collector.collected_assets.extra) == 1

    def test_none_context(self, collector):
        """Test handling of None context - should not raise."""
        # Should not raise exceptions
        collector.add_input_asset(None, uri="s3://bucket/input")
        collector.add_output_asset(None, uri="s3://bucket/output")
        collector.add_extra(None, "key", "value")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].context is None

        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].context is None

        assert len(lineage.extra) == 1
        assert lineage.extra[0].context is None

    def test_rapid_repeated_calls(self, collector):
        """Test that rapid repeated calls don't cause issues."""
        ctx = MagicMock(spec=BaseHook)

        # Simulate rapid repeated calls
        for _ in range(50):
            collector.add_input_asset(ctx, uri="s3://bucket/file")
            collector.add_output_asset(ctx, uri="s3://bucket/output")
            collector.add_extra(ctx, "key", "value")

        assert collector.has_collected

        lineage = collector.collected_assets
        # Should have counted properly
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].count == 50
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].count == 50
        assert len(lineage.extra) == 1
        assert lineage.extra[0].count == 50

    def test_mixed_valid_invalid_operations(self, collector):
        """Test mixing valid and invalid operations."""
        ctx = MagicMock(spec=BaseHook)

        # Mix valid and invalid calls
        collector.add_input_asset(ctx, uri="s3://bucket/valid")
        collector.add_input_asset(ctx, uri=None)  # Invalid - should not be collected
        collector.add_input_asset(ctx, uri="")  # Invalid - should not be collected
        collector.add_input_asset(ctx, uri="s3://bucket/another-valid")

        collector.add_extra(ctx, "valid_key", "valid_value")
        collector.add_extra(ctx, "", "invalid_key")  # Invalid key - should not be collected
        collector.add_extra(ctx, "another_key", "another_value")

        assert collector.has_collected

        # Should collect only valid items
        lineage = collector.collected_assets
        assert len(lineage.inputs) == 2
        assert lineage.inputs[0].asset.uri == "s3://bucket/valid"
        assert lineage.inputs[1].asset.uri == "s3://bucket/another-valid"

        assert len(lineage.extra) == 2
        assert lineage.extra[0].key == "valid_key"
        assert lineage.extra[0].value == "valid_value"
        assert lineage.extra[1].key == "another_key"
        assert lineage.extra[1].value == "another_value"

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
    ("has_readers", "expected_class"),
    [
        (True, HookLineageCollector),
        (False, NoOpCollector),
    ],
)
def test_get_hook_lineage_collector(has_readers, expected_class):
    # reset cached instance
    hook.get_hook_lineage_collector.cache_clear()
    plugins = [FakePlugin()] if has_readers else []
    with mock_plugin_manager(plugins=plugins):
        assert isinstance(get_hook_lineage_collector(), expected_class)
        assert get_hook_lineage_collector() is get_hook_lineage_collector()
