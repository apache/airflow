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

from airflow.providers.common.compat.lineage.hook import _lacks_add_extra_method, _lacks_asset_methods

from tests_common.test_utils.version_compat import AIRFLOW_V_3_0_PLUS


@pytest.fixture
def collector():
    from airflow.lineage.hook import HookLineageCollector

    # Patch the "inner" function that the compat version will call
    with mock.patch(
        "airflow.lineage.hook.get_hook_lineage_collector",
        return_value=HookLineageCollector(),
    ):
        from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

        yield get_hook_lineage_collector()


@pytest.fixture
def noop_collector():
    from airflow.lineage.hook import NoOpCollector

    # Patch the "inner" function that the compat version will call
    with mock.patch(
        "airflow.lineage.hook.get_hook_lineage_collector",
        return_value=NoOpCollector(),
    ):
        from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

        yield get_hook_lineage_collector()


@pytest.fixture(params=["collector", "noop_collector"])
def any_collector(request):
    return request.getfixturevalue(request.param)


def test_lacks_asset_methods_all_present():
    class Collector:
        def add_input_asset(self):
            pass

        def add_output_asset(self):
            pass

        @property
        def collected_assets(self):
            return "<HookLineage object usually>"

        def create_asset(self):
            pass

    assert _lacks_asset_methods(Collector()) is False


def test_lacks_asset_methods_missing_few():
    class Collector:
        def add_input_asset(self):
            pass

        @property
        def collected_assets(self):
            return "<HookLineage object usually>"

    assert _lacks_asset_methods(Collector()) is True


def test_lacks_asset_methods_none_present():
    class Collector:
        def add_input_dataset(self):
            pass

        def add_output_dataset(self):
            pass

    assert _lacks_asset_methods(Collector()) is True


def test_lacks_add_extra_method_present():
    class Collector:
        def __init__(self):
            self._extra = {}

        def add_extra(self):
            pass

    assert _lacks_add_extra_method(Collector()) is False


def test_lacks_add_extra_method_missing():
    class Collector:
        pass

    assert _lacks_add_extra_method(Collector()) is True


def test_retrieval_does_not_raise():  # do not use fixture here
    from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

    # On compat tests this goes into ImportError code path
    assert get_hook_lineage_collector() is not None
    assert get_hook_lineage_collector() is not None


def test_global_collector_is_reused():  # do not use fixture here
    from airflow.providers.common.compat.lineage.hook import get_hook_lineage_collector

    c1 = get_hook_lineage_collector()
    c2 = get_hook_lineage_collector()

    assert c1 is c2


def test_all_required_methods_exist(any_collector):
    """Test that all required methods exist regardless of version."""

    # Core methods that should always exist
    assert hasattr(any_collector, "add_input_asset")
    assert hasattr(any_collector, "add_output_asset")
    assert hasattr(any_collector, "add_extra")
    assert hasattr(any_collector, "collected_assets")
    assert hasattr(any_collector, "create_asset")

    # Verify they're callable
    assert callable(any_collector.add_input_asset)
    assert callable(any_collector.add_output_asset)
    assert callable(any_collector.add_extra)
    assert callable(any_collector.create_asset)


def test_empty_collector(any_collector):
    """Test that empty collector returns empty lineage."""
    lineage = any_collector.collected_assets

    assert lineage.inputs == []
    assert lineage.outputs == []
    assert lineage.extra == []


@pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow < 3.0")
def test_af2_collector_has_dataset_methods(any_collector):
    """Test that AF 2.x also has dataset methods."""

    assert hasattr(any_collector, "add_input_dataset")
    assert hasattr(any_collector, "add_output_dataset")
    assert hasattr(any_collector, "collected_datasets")
    assert hasattr(any_collector, "create_dataset")

    assert callable(any_collector.add_input_dataset)
    assert callable(any_collector.add_output_dataset)
    assert callable(any_collector.create_dataset)


@pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+")
def test_af3_collector_do_not_have_dataset_methods(any_collector):
    with pytest.raises(AttributeError):
        any_collector.add_input_dataset
    with pytest.raises(AttributeError):
        any_collector.add_output_dataset
    with pytest.raises(AttributeError):
        any_collector.collected_datasets
    with pytest.raises(AttributeError):
        any_collector.create_dataset


class TestCollectorAddExtra:
    def test_add_extra_basic_functionality(self, collector):
        """Test basic add_extra functionality."""
        mock_context = mock.MagicMock()
        collector.add_extra(mock_context, "test_key", {"data": "value"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert hasattr(lineage, "extra")
        assert len(lineage.extra) == 1
        assert lineage.extra[0].key == "test_key"
        assert lineage.extra[0].value == {"data": "value"}
        assert lineage.extra[0].count == 1
        assert lineage.extra[0].context == mock_context

    def test_add_extra_count_tracking(self, collector):
        """Test that duplicate extra entries are counted correctly."""
        mock_context = mock.MagicMock()

        # Add same extra multiple times
        collector.add_extra(mock_context, "test_key", {"data": "value"})
        collector.add_extra(mock_context, "test_key", {"data": "value"})
        collector.add_extra(mock_context, "test_key", {"data": "value"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 1
        assert lineage.extra[0].count == 3

    def test_add_extra_different_values(self, collector):
        """Test that different values are tracked separately."""
        mock_context = mock.MagicMock()

        # Add different values
        collector.add_extra(mock_context, "key1", {"data": "value1"})
        collector.add_extra(mock_context, "key2", {"data": "value2"})
        collector.add_extra(mock_context, "key1", {"data": "value3"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 3
        assert lineage.extra[0].key == "key1"
        assert lineage.extra[0].value == {"data": "value1"}
        assert lineage.extra[0].count == 1
        assert lineage.extra[0].context == mock_context
        assert lineage.extra[1].key == "key2"
        assert lineage.extra[1].value == {"data": "value2"}
        assert lineage.extra[1].count == 1
        assert lineage.extra[1].context == mock_context
        assert lineage.extra[2].key == "key1"
        assert lineage.extra[2].value == {"data": "value3"}
        assert lineage.extra[2].count == 1
        assert lineage.extra[2].context == mock_context

    def test_add_extra_different_contexts(self, collector):
        """Test that different contexts are tracked separately."""
        mock_context1 = mock.MagicMock()
        mock_context2 = mock.MagicMock()

        # Add same key/value with different contexts
        collector.add_extra(mock_context1, "test_key", {"data": "value"})
        collector.add_extra(mock_context2, "test_key", {"data": "value"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert lineage.extra[0].key == "test_key"
        assert lineage.extra[0].value == {"data": "value"}
        assert lineage.extra[0].count == 1
        assert lineage.extra[0].context == mock_context1
        assert lineage.extra[1].key == "test_key"
        assert lineage.extra[1].value == {"data": "value"}
        assert lineage.extra[1].count == 1
        assert lineage.extra[1].context == mock_context2

    def test_add_extra_missing_key(self, collector):
        """Test that add_extra handles missing key gracefully."""
        mock_context = mock.MagicMock()

        # Try to add with empty key
        collector.add_extra(mock_context, "", {"data": "value"})
        collector.add_extra(mock_context, None, {"data": "value"})

        assert not collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 0

    def test_add_extra_missing_value(self, collector):
        """Test that add_extra handles missing value gracefully."""
        mock_context = mock.MagicMock()

        # Try to add with empty/None value
        collector.add_extra(mock_context, "key", "")
        collector.add_extra(mock_context, "key", None)

        assert not collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 0

    def test_add_extra_max_limit(self, collector):
        """Test that add_extra respects maximum limit."""
        mock_context = mock.MagicMock()
        max_limit = 200

        # Add more than max allowed
        for i in range(max_limit + 10):
            collector.add_extra(mock_context, f"key_{i}", {"data": f"value_{i}"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == max_limit

    def test_add_extra_complex_values(self, collector):
        """Test that add_extra handles complex JSON-serializable values."""
        mock_context = mock.MagicMock()

        # Add various complex types
        collector.add_extra(mock_context, "dict", {"nested": {"data": "value"}})
        collector.add_extra(mock_context, "list", [1, 2, 3, "test"])
        collector.add_extra(mock_context, "number", 42)
        collector.add_extra(mock_context, "string", "simple string")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 4
        assert lineage.extra[0].key == "dict"
        assert lineage.extra[0].value == {"nested": {"data": "value"}}
        assert lineage.extra[0].count == 1
        assert lineage.extra[0].context == mock_context
        assert lineage.extra[1].key == "list"
        assert lineage.extra[1].value == [1, 2, 3, "test"]
        assert lineage.extra[1].count == 1
        assert lineage.extra[1].context == mock_context
        assert lineage.extra[2].key == "number"
        assert lineage.extra[2].value == 42
        assert lineage.extra[2].count == 1
        assert lineage.extra[2].context == mock_context
        assert lineage.extra[3].key == "string"
        assert lineage.extra[3].value == "simple string"
        assert lineage.extra[3].count == 1
        assert lineage.extra[3].context == mock_context


class TestCollectorAddAssets:
    def test_add_asset_basic_functionality(self, collector):
        """Test basic add_input_asset and add_output_asset functionality."""
        mock_context = mock.MagicMock()

        collector.add_input_asset(mock_context, uri="s3://bucket/input-file")
        collector.add_output_asset(mock_context, uri="s3://bucket/output-file")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == "s3://bucket/input-file"
        assert lineage.inputs[0].count == 1
        assert lineage.inputs[0].context == mock_context

        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.uri == "s3://bucket/output-file"
        assert lineage.outputs[0].count == 1
        assert lineage.outputs[0].context == mock_context

    def test_add_asset_count_tracking(self, collector):
        """Test that duplicate assets are counted correctly."""
        mock_context = mock.MagicMock()

        # Add same input multiple times
        collector.add_input_asset(mock_context, uri="s3://bucket/input")
        collector.add_input_asset(mock_context, uri="s3://bucket/input")
        collector.add_input_asset(mock_context, uri="s3://bucket/input")

        # Add same output multiple times
        collector.add_output_asset(mock_context, uri="s3://bucket/output")
        collector.add_output_asset(mock_context, uri="s3://bucket/output")

        assert collector.has_collected

        lineage = collector.collected_assets

        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == "s3://bucket/input"
        assert lineage.inputs[0].count == 3
        assert lineage.inputs[0].context == mock_context

        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.uri == "s3://bucket/output"
        assert lineage.outputs[0].count == 2
        assert lineage.outputs[0].context == mock_context

    def test_add_asset_different_uris(self, collector):
        """Test that different URIs are tracked separately."""
        mock_context = mock.MagicMock()

        # Add different input URIs
        collector.add_input_asset(mock_context, uri="s3://bucket/file1")
        collector.add_input_asset(mock_context, uri="s3://bucket/file2")
        collector.add_input_asset(mock_context, uri="postgres://example.com:5432/database/default/table")

        # Add different output URIs
        collector.add_output_asset(mock_context, uri="s3://output/file1")
        collector.add_output_asset(mock_context, uri="s3://output/file2")

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
        mock_context1 = mock.MagicMock()
        mock_context2 = mock.MagicMock()

        # Add same URI with different contexts
        collector.add_input_asset(mock_context1, uri="s3://bucket/file")
        collector.add_input_asset(mock_context2, uri="s3://bucket/file")

        collector.add_output_asset(mock_context1, uri="s3://output/file")
        collector.add_output_asset(mock_context2, uri="s3://output/file")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 2
        assert lineage.inputs[0].asset.uri == "s3://bucket/file"
        assert lineage.inputs[0].context == mock_context1
        assert lineage.inputs[0].count == 1
        assert lineage.inputs[1].asset.uri == "s3://bucket/file"
        assert lineage.inputs[1].context == mock_context2
        assert lineage.inputs[1].count == 1

        assert len(lineage.outputs) == 2
        assert lineage.outputs[0].asset.uri == "s3://output/file"
        assert lineage.outputs[0].context == mock_context1
        assert lineage.outputs[0].count == 1
        assert lineage.outputs[1].asset.uri == "s3://output/file"
        assert lineage.outputs[1].context == mock_context2
        assert lineage.outputs[1].count == 1

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+")
    def test_add_asset_with_name_and_group(self, collector):
        """Test adding assets with name and group parameters."""
        mock_context = mock.MagicMock()

        collector.add_input_asset(mock_context, uri="s3://bucket/file", name="my-input", group="input-group")
        collector.add_output_asset(
            mock_context, uri="s3://output/file", name="my-output", group="output-group"
        )

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == "s3://bucket/file"
        assert lineage.inputs[0].asset.name == "my-input"
        assert lineage.inputs[0].asset.group == "input-group"
        assert lineage.inputs[0].count == 1
        assert lineage.inputs[0].context == mock_context

        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.uri == "s3://output/file"
        assert lineage.outputs[0].asset.name == "my-output"
        assert lineage.outputs[0].asset.group == "output-group"
        assert lineage.outputs[0].count == 1
        assert lineage.outputs[0].context == mock_context

    def test_add_asset_with_extra_metadata(self, collector):
        """Test adding assets with extra metadata."""
        mock_context = mock.MagicMock()

        collector.add_input_asset(
            mock_context,
            uri="postgres://example.com:5432/database/default/table",
            asset_extra={"schema": "public", "table": "users"},
        )
        collector.add_output_asset(
            mock_context,
            uri="postgres://example.com:5432/database/default/table",
            asset_extra={"schema": "public", "table": "results"},
        )

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == "postgres://example.com:5432/database/default/table"
        assert lineage.inputs[0].asset.extra == {"schema": "public", "table": "users"}
        assert lineage.inputs[0].count == 1
        assert lineage.inputs[0].context == mock_context

        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.uri == "postgres://example.com:5432/database/default/table"
        assert lineage.outputs[0].asset.extra == {"schema": "public", "table": "results"}
        assert lineage.outputs[0].count == 1
        assert lineage.outputs[0].context == mock_context

    def test_add_asset_different_extra_values(self, collector):
        """Test that assets with different extra values are tracked separately."""
        mock_context = mock.MagicMock()

        # Same URI but different extra metadata
        collector.add_input_asset(mock_context, uri="s3://bucket/file", asset_extra={"version": "1"})
        collector.add_input_asset(mock_context, uri="s3://bucket/file", asset_extra={"version": "2"})

        collector.add_output_asset(mock_context, uri="s3://output/file", asset_extra={"format": "parquet"})
        collector.add_output_asset(mock_context, uri="s3://output/file", asset_extra={"format": "csv"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 2
        assert lineage.inputs[0].asset.uri == "s3://bucket/file"
        assert lineage.inputs[0].asset.extra == {"version": "1"}
        assert lineage.inputs[0].count == 1
        assert lineage.inputs[0].context == mock_context
        assert lineage.inputs[1].asset.uri == "s3://bucket/file"
        assert lineage.inputs[1].asset.extra == {"version": "2"}
        assert lineage.inputs[1].count == 1
        assert lineage.inputs[1].context == mock_context

        assert len(lineage.outputs) == 2
        assert lineage.outputs[0].asset.uri == "s3://output/file"
        assert lineage.outputs[0].asset.extra == {"format": "parquet"}
        assert lineage.outputs[0].count == 1
        assert lineage.outputs[0].context == mock_context
        assert lineage.outputs[1].asset.uri == "s3://output/file"
        assert lineage.outputs[1].asset.extra == {"format": "csv"}
        assert lineage.outputs[1].count == 1
        assert lineage.outputs[1].context == mock_context

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+")
    def test_add_asset_max_limit_af3(self, collector):
        """Test that asset operations respect maximum limit."""
        mock_context = mock.MagicMock()
        max_limit = 100
        added_assets = max_limit + 50

        # Limitation on collected assets was added in AF3 #45798
        expected_number = max_limit

        # Add more than max allowed inputs
        for i in range(added_assets):
            collector.add_input_asset(mock_context, uri=f"s3://bucket/input-{i}")

        # Add more than max allowed outputs
        for i in range(added_assets):
            collector.add_output_asset(mock_context, uri=f"s3://bucket/output-{i}")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == expected_number
        assert len(lineage.outputs) == expected_number

    @pytest.mark.skipif(AIRFLOW_V_3_0_PLUS, reason="Test requires < Airflow 3.0")
    def test_add_asset_max_limit_af2(self, collector):
        """Test that asset operations do not respect maximum limit."""
        mock_context = mock.MagicMock()
        max_limit = 100
        added_assets = max_limit + 50

        # Limitation on collected assets was added in AF3 #45798
        expected_number = added_assets

        # Add more than max allowed inputs
        for i in range(added_assets):
            collector.add_input_asset(mock_context, uri=f"s3://bucket/input-{i}")

        # Add more than max allowed outputs
        for i in range(added_assets):
            collector.add_output_asset(mock_context, uri=f"s3://bucket/output-{i}")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == expected_number
        assert len(lineage.outputs) == expected_number


class TestEdgeCases:
    """Test edge cases and error conditions to ensure collector never fails."""

    @pytest.mark.parametrize("uri", ["", None])
    def test_invalid_uri_none(self, collector, uri):
        """Test handling of None URI - should not raise."""
        mock_context = mock.MagicMock()

        # Should not raise exceptions
        collector.add_input_asset(mock_context, uri=uri)
        collector.add_output_asset(mock_context, uri=uri)

        # Collector should handle gracefully and not collect invalid URIs
        assert not collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 0
        assert len(lineage.outputs) == 0
        assert len(lineage.extra) == 0

    def test_malformed_uri(self, collector):
        """Test handling of malformed URIs - should not raise."""
        mock_context = mock.MagicMock()

        # Various malformed URIs should not cause crashes
        collector.add_input_asset(mock_context, uri="not-a-valid-uri")
        collector.add_input_asset(mock_context, uri="://missing-scheme")
        collector.add_input_asset(mock_context, uri="scheme:")
        collector.add_output_asset(mock_context, uri="//no-scheme")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 3
        assert lineage.inputs[0].asset.uri == "not-a-valid-uri"
        assert lineage.inputs[0].count == 1
        assert lineage.inputs[0].context == mock_context
        assert lineage.inputs[1].asset.uri == "://missing-scheme"
        assert lineage.inputs[1].count == 1
        assert lineage.inputs[1].context == mock_context
        assert lineage.inputs[2].asset.uri == "scheme:/"
        assert lineage.inputs[2].count == 1
        assert lineage.inputs[2].context == mock_context

        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.uri == "//no-scheme"
        assert lineage.outputs[0].count == 1
        assert lineage.outputs[0].context == mock_context

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+")
    def test_very_long_uri(self, collector):
        """Test handling of very long URIs - 1000 chars OK, 2000 chars raises ValueError."""
        mock_context = mock.MagicMock()

        # Create very long URI (1000 chars - should work)
        long_path = "a" * 1000
        long_uri = f"s3://bucket/{long_path}"

        # Create too long URI (2000 chars - should raise)
        too_long_uri = f"s3://bucket/{long_path * 2}"

        collector.add_input_asset(mock_context, uri=long_uri)

        # Too long URI should raise ValueError
        with pytest.raises(ValueError, match="Asset name cannot exceed"):
            collector.add_output_asset(mock_context, uri=too_long_uri)

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == long_uri
        assert lineage.inputs[0].count == 1
        assert lineage.inputs[0].context == mock_context

        assert len(lineage.outputs) == 0
        assert len(lineage.extra) == 0

    def test_none_context(self, collector):
        """Test handling of None context - should not raise."""
        # Should not raise exceptions
        collector.add_input_asset(None, uri="s3://bucket/input")
        collector.add_output_asset(None, uri="s3://bucket/output")
        collector.add_extra(None, "key", "value")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == "s3://bucket/input"
        assert lineage.inputs[0].count == 1
        assert lineage.inputs[0].context is None

        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.uri == "s3://bucket/output"
        assert lineage.outputs[0].count == 1
        assert lineage.outputs[0].context is None

        assert len(lineage.extra) == 1
        assert lineage.extra[0].key == "key"
        assert lineage.extra[0].value == "value"
        assert lineage.extra[0].count == 1
        assert lineage.extra[0].context is None

    def test_special_characters_in_extra_key(self, collector):
        """Test that extra keys with special characters work."""
        mock_context = mock.MagicMock()

        collector.add_extra(mock_context, "key-with-dashes", {"data": "value"})
        collector.add_extra(mock_context, "key.with.dots", {"data": "value"})
        collector.add_extra(mock_context, "key_with_underscores", {"data": "value"})
        collector.add_extra(mock_context, "key/with/slashes", {"data": "value"})
        collector.add_extra(mock_context, "key:with:colons", {"data": "value"})

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
        mock_context = mock.MagicMock()

        collector.add_extra(mock_context, "clé_française", {"données": "valeur"})
        collector.add_extra(mock_context, "中文键", {"中文": "值"})
        collector.add_extra(mock_context, "مفتاح", {"بيانات": "قيمة"})
        collector.add_extra(mock_context, "emoji_🚀", {"status": "✅"})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 4
        assert lineage.extra[0].key == "clé_française"
        assert lineage.extra[0].value == {"données": "valeur"}
        assert lineage.extra[0].count == 1
        assert lineage.extra[0].context == mock_context

        assert lineage.extra[1].key == "中文键"
        assert lineage.extra[1].value == {"中文": "值"}
        assert lineage.extra[1].count == 1
        assert lineage.extra[1].context == mock_context

        assert lineage.extra[2].key == "مفتاح"
        assert lineage.extra[2].value == {"بيانات": "قيمة"}
        assert lineage.extra[2].count == 1
        assert lineage.extra[2].context == mock_context

        assert lineage.extra[3].key == "emoji_🚀"
        assert lineage.extra[3].value == {"status": "✅"}
        assert lineage.extra[3].count == 1
        assert lineage.extra[3].context == mock_context

    def test_very_large_extra_value(self, collector):
        """Test that large extra values are handled."""
        mock_context = mock.MagicMock()

        # Create a large value
        large_value = {"data": "x" * 10000, "list": list(range(1000))}

        collector.add_extra(mock_context, "large_key", large_value)

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 1
        assert lineage.extra[0].key == "large_key"
        assert lineage.extra[0].value == large_value
        assert lineage.extra[0].count == 1
        assert lineage.extra[0].context == mock_context

        assert len(lineage.inputs) == 0
        assert len(lineage.outputs) == 0

    def test_deeply_nested_extra_value(self, collector):
        """Test that deeply nested data structures in extra are handled."""
        mock_context = mock.MagicMock()

        # Create deeply nested structure
        nested_value = {"level1": {"level2": {"level3": {"level4": {"level5": {"data": "deep"}}}}}}

        collector.add_extra(mock_context, "nested", nested_value)

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 1
        assert lineage.extra[0].key == "nested"
        assert lineage.extra[0].value == nested_value
        assert lineage.extra[0].count == 1
        assert lineage.extra[0].context == mock_context

        assert len(lineage.inputs) == 0
        assert len(lineage.outputs) == 0

    def test_extra_value_with_various_types(self, collector):
        """Test that extra can handle various data types."""
        mock_context = mock.MagicMock()

        collector.add_extra(mock_context, "string", "text")
        collector.add_extra(mock_context, "integer", 42)
        collector.add_extra(mock_context, "float", 3.14)
        collector.add_extra(mock_context, "boolean", True)
        collector.add_extra(mock_context, "list", [1, 2, 3])
        collector.add_extra(mock_context, "dict", {"key": "value"})
        collector.add_extra(mock_context, "null", None)

        assert collector.has_collected

        # None value should not be collected (based on validation)
        lineage = collector.collected_assets
        assert len(lineage.extra) == 6  # None is filtered out

        assert lineage.extra[0].key == "string"
        assert lineage.extra[0].value == "text"
        assert lineage.extra[0].count == 1

        assert lineage.extra[1].key == "integer"
        assert lineage.extra[1].value == 42
        assert lineage.extra[1].count == 1

        assert lineage.extra[2].key == "float"
        assert lineage.extra[2].value == 3.14
        assert lineage.extra[2].count == 1

        assert lineage.extra[3].key == "boolean"
        assert lineage.extra[3].value is True
        assert lineage.extra[3].count == 1

        assert lineage.extra[4].key == "list"
        assert lineage.extra[4].value == [1, 2, 3]
        assert lineage.extra[4].count == 1

        assert lineage.extra[5].key == "dict"
        assert lineage.extra[5].value == {"key": "value"}
        assert lineage.extra[5].count == 1

        assert len(lineage.inputs) == 0
        assert len(lineage.outputs) == 0

    def test_non_json_serializable_value_in_extra(self, collector):
        """Test that non-JSON-serializable values are handled gracefully."""
        mock_context = mock.MagicMock()

        # Create a non-serializable object
        class CustomObject:
            def __str__(self):
                return "custom_object"

        # Should not raise - collector should handle via str conversion or skip
        collector.add_extra(mock_context, "custom_key", CustomObject())

        # May or may not be collected depending on implementation
        lineage = collector.collected_assets
        # Just verify it doesn't crash
        assert isinstance(lineage.extra, list)
        assert len(lineage.inputs) == 0
        assert len(lineage.outputs) == 0

    def test_empty_asset_extra(self, collector):
        """Test that empty asset_extra is handled correctly."""
        mock_context = mock.MagicMock()

        collector.add_input_asset(mock_context, uri="s3://bucket/file", asset_extra={})
        collector.add_output_asset(mock_context, uri="s3://bucket/file", asset_extra={})

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.extra == {}
        assert len(lineage.outputs) == 1
        assert lineage.outputs[0].asset.extra == {}

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+")
    def test_asset_with_all_optional_parameters(self, collector):
        """Test asset creation with all optional parameters provided."""
        mock_context = mock.MagicMock()

        collector.add_input_asset(
            mock_context,
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

    def test_rapid_repeated_calls(self, collector):
        """Test that rapid repeated calls don't cause issues."""
        mock_context = mock.MagicMock()

        # Simulate rapid repeated calls
        for _ in range(50):
            collector.add_input_asset(mock_context, uri="s3://bucket/file")
            collector.add_output_asset(mock_context, uri="s3://bucket/output")
            collector.add_extra(mock_context, "key", "value")

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
        mock_context = mock.MagicMock()

        # Mix valid and invalid calls
        collector.add_input_asset(mock_context, uri="s3://bucket/valid")
        collector.add_input_asset(mock_context, uri=None)  # Invalid - should not be collected
        collector.add_input_asset(mock_context, uri="")  # Invalid - should not be collected
        collector.add_input_asset(mock_context, uri="s3://bucket/another-valid")

        collector.add_extra(mock_context, "valid_key", "valid_value")
        collector.add_extra(mock_context, "", "invalid_key")  # Invalid key - should not be collected
        collector.add_extra(mock_context, "another_key", "another_value")

        assert collector.has_collected

        # Should collect only valid items
        lineage = collector.collected_assets
        assert len(lineage.inputs) == 2
        assert lineage.inputs[0].asset.uri == "s3://bucket/valid"
        assert lineage.inputs[0].count == 1
        assert lineage.inputs[0].context == mock_context
        assert lineage.inputs[1].asset.uri == "s3://bucket/another-valid"
        assert lineage.inputs[1].count == 1
        assert lineage.inputs[1].context == mock_context

        assert len(lineage.extra) == 2
        assert lineage.extra[0].key == "valid_key"
        assert lineage.extra[0].value == "valid_value"
        assert lineage.extra[0].count == 1
        assert lineage.extra[1].key == "another_key"
        assert lineage.extra[1].value == "another_value"
        assert lineage.extra[1].count == 1

        assert len(lineage.outputs) == 0

    def test_collector_collected_assets_called_multiple_times(self, collector):
        """Test that collected_assets property can be called multiple times."""
        mock_context = mock.MagicMock()

        collector.add_input_asset(mock_context, uri="s3://bucket/file")

        assert collector.has_collected

        # Call multiple times - should return same data
        lineage1 = collector.collected_assets
        lineage2 = collector.collected_assets
        lineage3 = collector.collected_assets

        assert lineage1.inputs == lineage2.inputs == lineage3.inputs
        assert len(lineage1.inputs) == 1
        assert lineage1.inputs[0].asset.uri == "s3://bucket/file"
        assert lineage1.inputs[0].count == 1
        assert lineage1.inputs[0].context == mock_context

    @pytest.mark.skipif(not AIRFLOW_V_3_0_PLUS, reason="Test requires Airflow 3.0+")
    def test_empty_name_and_group(self, collector):
        """Test that empty strings for name and group are handled."""
        mock_context = mock.MagicMock()

        # Empty strings for optional parameters
        collector.add_input_asset(mock_context, uri="s3://bucket/file", name="", group="")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.inputs) == 1
        assert lineage.inputs[0].asset.uri == "s3://bucket/file"
        assert lineage.inputs[0].asset.name == "s3://bucket/file"
        assert lineage.inputs[0].asset.group == "asset"
        assert lineage.inputs[0].count == 1
        assert lineage.inputs[0].context == mock_context

        assert len(lineage.outputs) == 0
        assert len(lineage.extra) == 0

    def test_extremely_long_extra_key(self, collector):
        """Test that extremely long extra keys are handled."""
        mock_context = mock.MagicMock()

        long_key = "k" * 10000
        collector.add_extra(mock_context, long_key, "value")

        assert collector.has_collected

        lineage = collector.collected_assets
        assert len(lineage.extra) == 1
        assert lineage.extra[0].key == long_key
        assert lineage.extra[0].value == "value"
        assert lineage.extra[0].count == 1
        assert lineage.extra[0].context == mock_context

        assert len(lineage.inputs) == 0
        assert len(lineage.outputs) == 0
