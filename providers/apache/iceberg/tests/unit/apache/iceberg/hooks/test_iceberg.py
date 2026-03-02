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
import requests_mock

from airflow.models import Connection
from airflow.providers.apache.iceberg.hooks.iceberg import IcebergHook

LOAD_CATALOG = "airflow.providers.apache.iceberg.hooks.iceberg.load_catalog"


@pytest.fixture
def iceberg_connection(create_connection_without_db):
    """Create a standard Iceberg connection for tests."""
    create_connection_without_db(
        Connection(
            conn_id="iceberg_default",
            conn_type="iceberg",
            host="https://api.iceberg.io/ws/v1",
            login="my_client_id",
            password="my_client_secret",
            extra='{"warehouse": "s3://my-warehouse/"}',
        )
    )


@pytest.fixture
def iceberg_connection_no_creds(create_connection_without_db):
    """Create an Iceberg connection without OAuth2 credentials."""
    create_connection_without_db(
        Connection(
            conn_id="iceberg_default",
            conn_type="iceberg",
            host="https://local-catalog.example.com",
        )
    )


def _make_mock_catalog():
    """Create a mock catalog with spec matching the real Catalog interface."""
    from pyiceberg.catalog.rest import RestCatalog

    return MagicMock(spec=RestCatalog)


def _make_mock_table():
    """Create a mock table with spec matching the real Table interface."""
    from pyiceberg.table import Table

    return MagicMock(spec=Table)


def _make_hook_with_mock_catalog(mock_catalog):
    """Create a hook with a pre-populated cached_property for catalog."""
    hook = IcebergHook()
    # Bypass cached_property by writing directly to instance __dict__
    hook.__dict__["catalog"] = mock_catalog
    return hook


class TestIcebergHookCatalogConfig:
    """Test catalog configuration from Airflow connection."""

    def test_catalog_from_connection_fields(self, iceberg_connection):
        """Catalog is configured from host, login, password."""
        hook = IcebergHook()
        with patch(LOAD_CATALOG) as mock_load:
            mock_load.return_value = MagicMock()
            catalog = hook.get_conn()

            mock_load.assert_called_once_with(
                "iceberg_default",
                warehouse="s3://my-warehouse/",
                uri="https://api.iceberg.io/ws/v1",
                type="rest",
                credential="my_client_id:my_client_secret",
            )
            assert catalog is mock_load.return_value

    def test_catalog_merges_extra_properties(self, create_connection_without_db):
        """Extra JSON properties (warehouse, S3 config) are merged into catalog config."""
        create_connection_without_db(
            Connection(
                conn_id="iceberg_default",
                conn_type="iceberg",
                host="https://api.iceberg.io/ws/v1",
                login="client",
                password="secret",
                extra='{"warehouse": "s3://bucket/", "s3.region": "us-east-1"}',
            )
        )
        hook = IcebergHook()
        with patch(LOAD_CATALOG) as mock_load:
            mock_load.return_value = MagicMock()
            hook.get_conn()

            call_kwargs = mock_load.call_args[1]
            assert call_kwargs["warehouse"] == "s3://bucket/"
            assert call_kwargs["s3.region"] == "us-east-1"

    def test_catalog_without_credentials(self, iceberg_connection_no_creds):
        """Catalog works without OAuth2 credentials (e.g., local/unsigned catalogs)."""
        hook = IcebergHook()
        with patch(LOAD_CATALOG) as mock_load:
            mock_load.return_value = MagicMock()
            hook.get_conn()

            call_kwargs = mock_load.call_args[1]
            assert "credential" not in call_kwargs
            assert call_kwargs["uri"] == "https://local-catalog.example.com"

    def test_extra_cannot_override_uri(self, create_connection_without_db):
        """Connection host always wins over uri in extra."""
        create_connection_without_db(
            Connection(
                conn_id="iceberg_default",
                conn_type="iceberg",
                host="https://correct-host.example.com",
                extra='{"uri": "https://wrong-host.example.com"}',
            )
        )
        hook = IcebergHook()
        with patch(LOAD_CATALOG) as mock_load:
            mock_load.return_value = MagicMock()
            hook.get_conn()

            call_kwargs = mock_load.call_args[1]
            assert call_kwargs["uri"] == "https://correct-host.example.com"

    def test_extra_can_override_catalog_type(self, create_connection_without_db):
        """Extra can set catalog type to non-REST (e.g., glue)."""
        create_connection_without_db(
            Connection(
                conn_id="iceberg_default",
                conn_type="iceberg",
                host="https://glue.example.com",
                extra='{"type": "glue"}',
            )
        )
        hook = IcebergHook()
        with patch(LOAD_CATALOG) as mock_load:
            mock_load.return_value = MagicMock()
            hook.get_conn()

            call_kwargs = mock_load.call_args[1]
            assert call_kwargs["type"] == "glue"

    def test_non_rest_catalog_skips_credential(self, create_connection_without_db):
        """Non-REST catalogs (glue, bigquery) don't get the credential field."""
        create_connection_without_db(
            Connection(
                conn_id="iceberg_default",
                conn_type="iceberg",
                host="https://glue.example.com",
                login="some_user",
                password="some_pass",
                extra='{"type": "glue"}',
            )
        )
        hook = IcebergHook()
        with patch(LOAD_CATALOG) as mock_load:
            mock_load.return_value = MagicMock()
            hook.get_conn()

            call_kwargs = mock_load.call_args[1]
            assert "credential" not in call_kwargs

    def test_partial_credentials_warns(self, create_connection_without_db, caplog):
        """Only login or only password logs a warning."""
        create_connection_without_db(
            Connection(
                conn_id="iceberg_default",
                conn_type="iceberg",
                host="https://api.iceberg.io/ws/v1",
                login="client_id_only",
            )
        )
        hook = IcebergHook()
        with patch(LOAD_CATALOG) as mock_load:
            mock_load.return_value = MagicMock()
            hook.get_conn()

            call_kwargs = mock_load.call_args[1]
            assert "credential" not in call_kwargs
        assert "Both are required" in caplog.text


class TestIcebergHookCatalog:
    """Test catalog introspection methods."""

    def test_list_namespaces(self):
        """list_namespaces returns dotted namespace strings."""
        mock_cat = _make_mock_catalog()
        mock_cat.list_namespaces.return_value = [("default",), ("analytics", "raw")]
        hook = _make_hook_with_mock_catalog(mock_cat)

        result = hook.list_namespaces()

        assert result == ["default", "analytics.raw"]

    def test_list_tables(self):
        """list_tables returns fully-qualified table names."""
        mock_cat = _make_mock_catalog()
        mock_cat.list_tables.return_value = [
            ("analytics", "events"),
            ("analytics", "users"),
        ]
        hook = _make_hook_with_mock_catalog(mock_cat)

        result = hook.list_tables("analytics")

        assert result == ["analytics.events", "analytics.users"]
        mock_cat.list_tables.assert_called_once_with("analytics")

    def test_table_exists_true(self):
        """table_exists returns True for existing table."""
        mock_cat = _make_mock_catalog()
        mock_cat.table_exists.return_value = True
        hook = _make_hook_with_mock_catalog(mock_cat)

        assert hook.table_exists("analytics.events") is True

    def test_table_exists_false(self):
        """table_exists returns False for missing table."""
        mock_cat = _make_mock_catalog()
        mock_cat.table_exists.return_value = False
        hook = _make_hook_with_mock_catalog(mock_cat)

        assert hook.table_exists("analytics.missing") is False

    def test_load_table(self):
        """load_table delegates to catalog.load_table."""
        mock_cat = _make_mock_catalog()
        mock_table = _make_mock_table()
        mock_cat.load_table.return_value = mock_table
        hook = _make_hook_with_mock_catalog(mock_cat)

        result = hook.load_table("db.my_table")

        assert result is mock_table
        mock_cat.load_table.assert_called_once_with("db.my_table")

    def test_load_table_rejects_unqualified_name(self):
        """load_table raises ValueError for names without a namespace."""
        hook = _make_hook_with_mock_catalog(_make_mock_catalog())

        with pytest.raises(ValueError, match="namespace.table"):
            hook.load_table("bare_table")

    def test_get_table_schema(self):
        """get_table_schema returns list of {name, type} dicts matching DbApiHook contract."""
        mock_field1 = MagicMock()
        mock_field1.name = "id"
        mock_field1.field_type = MagicMock(__str__=lambda s: "long")

        mock_field2 = MagicMock()
        mock_field2.name = "name"
        mock_field2.field_type = MagicMock(__str__=lambda s: "string")

        mock_field3 = MagicMock()
        mock_field3.name = "created_at"
        mock_field3.field_type = MagicMock(__str__=lambda s: "timestamptz")

        mock_schema = MagicMock()
        mock_schema.fields = [mock_field1, mock_field2, mock_field3]

        mock_table = _make_mock_table()
        mock_table.schema.return_value = mock_schema

        mock_cat = _make_mock_catalog()
        mock_cat.load_table.return_value = mock_table
        hook = _make_hook_with_mock_catalog(mock_cat)

        result = hook.get_table_schema("db.my_table")

        assert result == [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"},
            {"name": "created_at", "type": "timestamptz"},
        ]

    def test_get_table_schema_nested_types(self):
        """Nested types (struct, list, map) are stringified correctly."""
        mock_field1 = MagicMock()
        mock_field1.name = "tags"
        mock_field1.field_type = MagicMock(__str__=lambda s: "list<string>")

        mock_field2 = MagicMock()
        mock_field2.name = "metadata"
        mock_field2.field_type = MagicMock(__str__=lambda s: "map<string, string>")

        mock_field3 = MagicMock()
        mock_field3.name = "address"
        mock_field3.field_type = MagicMock(__str__=lambda s: "struct<city: string, zip: string>")

        mock_schema = MagicMock()
        mock_schema.fields = [mock_field1, mock_field2, mock_field3]

        mock_table = _make_mock_table()
        mock_table.schema.return_value = mock_schema

        mock_cat = _make_mock_catalog()
        mock_cat.load_table.return_value = mock_table
        hook = _make_hook_with_mock_catalog(mock_cat)

        result = hook.get_table_schema("db.nested_table")

        assert result == [
            {"name": "tags", "type": "list<string>"},
            {"name": "metadata", "type": "map<string, string>"},
            {"name": "address", "type": "struct<city: string, zip: string>"},
        ]

    def test_get_partition_spec(self):
        """get_partition_spec returns field names and transforms."""
        mock_source_field1 = MagicMock()
        mock_source_field1.name = "event_date"
        mock_source_field2 = MagicMock()
        mock_source_field2.name = "region"

        mock_partition_field1 = MagicMock()
        mock_partition_field1.source_id = 1
        mock_partition_field1.transform = MagicMock(__str__=lambda s: "day")
        mock_partition_field2 = MagicMock()
        mock_partition_field2.source_id = 2
        mock_partition_field2.transform = MagicMock(__str__=lambda s: "identity")

        mock_spec = MagicMock()
        mock_spec.fields = [mock_partition_field1, mock_partition_field2]

        mock_schema = MagicMock()
        mock_schema.find_field.side_effect = lambda sid: {
            1: mock_source_field1,
            2: mock_source_field2,
        }[sid]

        mock_table = _make_mock_table()
        mock_table.spec.return_value = mock_spec
        mock_table.schema.return_value = mock_schema

        mock_cat = _make_mock_catalog()
        mock_cat.load_table.return_value = mock_table
        hook = _make_hook_with_mock_catalog(mock_cat)

        result = hook.get_partition_spec("db.partitioned_table")

        assert result == [
            {"field": "event_date", "transform": "day"},
            {"field": "region", "transform": "identity"},
        ]

    def test_get_partition_spec_no_partitions(self):
        """Unpartitioned table returns empty list."""
        mock_spec = MagicMock()
        mock_spec.fields = []

        mock_table = _make_mock_table()
        mock_table.spec.return_value = mock_spec
        mock_table.schema.return_value = MagicMock()

        mock_cat = _make_mock_catalog()
        mock_cat.load_table.return_value = mock_table
        hook = _make_hook_with_mock_catalog(mock_cat)

        result = hook.get_partition_spec("db.unpartitioned_table")

        assert result == []

    def test_get_table_properties(self):
        """get_table_properties returns dict of string key-value pairs."""
        mock_table = _make_mock_table()
        mock_table.properties = {
            "format-version": "2",
            "write.format.default": "parquet",
        }

        mock_cat = _make_mock_catalog()
        mock_cat.load_table.return_value = mock_table
        hook = _make_hook_with_mock_catalog(mock_cat)

        result = hook.get_table_properties("db.my_table")

        assert result == {
            "format-version": "2",
            "write.format.default": "parquet",
        }

    def test_get_snapshots_returns_most_recent_first(self):
        """get_snapshots returns snapshots in reverse chronological order."""
        mock_arrow = MagicMock()
        mock_arrow.__len__ = lambda s: 3
        mock_arrow.to_pylist.return_value = [
            {"snapshot_id": 1, "committed_at": "2024-01-01"},
            {"snapshot_id": 2, "committed_at": "2024-06-01"},
            {"snapshot_id": 3, "committed_at": "2024-12-01"},
        ]

        mock_table = _make_mock_table()
        mock_table.inspect.snapshots.return_value = mock_arrow

        mock_cat = _make_mock_catalog()
        mock_cat.load_table.return_value = mock_table
        hook = _make_hook_with_mock_catalog(mock_cat)

        result = hook.get_snapshots("db.my_table")

        assert len(result) == 3
        # Most recent first
        assert result[0]["snapshot_id"] == 3
        assert result[2]["snapshot_id"] == 1

    def test_get_snapshots_respects_limit(self):
        """Limit parameter caps the number of returned snapshots, taking the most recent."""
        mock_arrow = MagicMock()
        mock_arrow.__len__ = lambda s: 20
        # When sliced to last 5 items (offset=15, length=5), return those
        mock_arrow.slice.return_value = MagicMock()
        mock_arrow.slice.return_value.to_pylist.return_value = [{"snapshot_id": i} for i in range(15, 20)]

        mock_table = _make_mock_table()
        mock_table.inspect.snapshots.return_value = mock_arrow

        mock_cat = _make_mock_catalog()
        mock_cat.load_table.return_value = mock_table
        hook = _make_hook_with_mock_catalog(mock_cat)

        result = hook.get_snapshots("db.my_table", limit=5)

        assert len(result) == 5
        # Most recent first (reversed from chronological)
        assert result[0]["snapshot_id"] == 19
        assert result[4]["snapshot_id"] == 15
        mock_arrow.slice.assert_called_once_with(offset=15, length=5)


class TestPyicebergAPICompatibility:
    """Verify the pyiceberg APIs that IcebergHook depends on still exist.

    These tests use real pyiceberg classes (no mocks) so they fail if
    pyiceberg removes or renames an API we rely on. This catches breaking
    changes from upstream that mocked unit tests would miss.
    """

    def test_catalog_has_required_methods(self):
        """Catalog ABC exposes the methods IcebergHook calls."""
        from pyiceberg.catalog import Catalog

        for method in ("list_namespaces", "list_tables", "load_table", "table_exists"):
            assert hasattr(Catalog, method), f"pyiceberg Catalog missing {method}"

    def test_load_catalog_importable(self):
        """load_catalog is importable from pyiceberg.catalog."""
        from pyiceberg.catalog import load_catalog

        assert callable(load_catalog)

    def test_table_has_required_attributes(self):
        """Table exposes schema, spec, properties, and inspect."""
        from pyiceberg.table import Table

        for attr in ("schema", "spec", "properties", "inspect"):
            assert hasattr(Table, attr), f"pyiceberg Table missing {attr}"

    def test_inspect_table_has_snapshots(self):
        """InspectTable has a snapshots method."""
        from pyiceberg.table.inspect import InspectTable

        assert hasattr(InspectTable, "snapshots")

    def test_schema_has_fields_and_find_field(self):
        """Schema instance exposes fields and find_field."""
        from pyiceberg.schema import Schema
        from pyiceberg.types import LongType, NestedField

        schema = Schema(NestedField(1, "id", LongType(), required=True))
        assert hasattr(schema, "fields")
        assert hasattr(schema, "find_field")

    def test_partition_spec_has_fields(self):
        """PartitionSpec instance exposes fields."""
        from pyiceberg.partitioning import PartitionSpec

        spec = PartitionSpec()
        assert hasattr(spec, "fields")


class TestIcebergHookToken:
    """Test backward-compatible token methods."""

    def test_get_token(self, iceberg_connection):
        """get_token fetches OAuth2 access token."""
        access_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSU"
        hook = IcebergHook()
        with requests_mock.Mocker() as m:
            m.post(
                "https://api.iceberg.io/ws/v1/oauth/tokens",
                json={
                    "access_token": access_token,
                    "token_type": "Bearer",
                    "expires_in": 86400,
                },
            )
            result = hook.get_token()

        assert result == access_token

    def test_get_token_macro(self):
        """get_token_macro returns correct Jinja2 template string."""
        hook = IcebergHook()
        result = hook.get_token_macro()
        assert result == "{{ conn.iceberg_default.get_hook().get_token() }}"

    def test_test_connection_success(self):
        """test_connection returns True when catalog is reachable."""
        mock_cat = _make_mock_catalog()
        mock_cat.list_namespaces.return_value = [("default",), ("analytics",)]
        hook = _make_hook_with_mock_catalog(mock_cat)

        success, message = hook.test_connection()

        assert success is True
        assert "2 namespace(s)" in message

    def test_test_connection_failure(self):
        """test_connection returns False with error message on failure."""
        mock_cat = _make_mock_catalog()
        mock_cat.list_namespaces.side_effect = ConnectionError("Connection refused")
        hook = _make_hook_with_mock_catalog(mock_cat)

        success, message = hook.test_connection()

        assert success is False
        assert "Connection refused" in message
