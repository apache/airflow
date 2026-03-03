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

from functools import cached_property
from typing import TYPE_CHECKING, Any

import requests
from pyiceberg.catalog import load_catalog

from airflow.providers.common.compat.sdk import BaseHook

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

TOKENS_ENDPOINT = "oauth/tokens"


class IcebergHook(BaseHook):
    """
    Hook for Apache Iceberg REST catalogs.

    Provides catalog-level operations (list namespaces, list tables, load schemas)
    using pyiceberg, plus OAuth2 token generation for external query engines.

    :param iceberg_conn_id: The :ref:`Iceberg connection id<howto/connection:iceberg>`
        which refers to the information to connect to the Iceberg catalog.
    """

    conn_name_attr = "iceberg_conn_id"
    default_conn_name = "iceberg_default"
    conn_type = "iceberg"
    hook_name = "Iceberg"

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Return custom UI field behaviour for Iceberg connection."""
        return {
            "hidden_fields": ["schema", "port"],
            "relabeling": {
                "host": "Catalog URI",
                "login": "Client ID",
                "password": "Client Secret",
            },
            "placeholders": {
                "host": "https://your-catalog.example.com/ws/v1",
                "login": "client_id (OAuth2 credentials)",
                "password": "client_secret (OAuth2 credentials)",
                "extra": '{"warehouse": "s3://my-warehouse/", "s3.region": "us-east-1"}',
            },
        }

    def __init__(self, iceberg_conn_id: str = default_conn_name) -> None:
        super().__init__()
        self.conn_id = iceberg_conn_id

    @cached_property
    def catalog(self) -> Catalog:
        """Return a pyiceberg Catalog instance for the configured connection."""
        conn = self.get_connection(self.conn_id)

        # Start with extra so connection fields take precedence
        extra = conn.extra_dejson or {}
        catalog_properties: dict[str, str] = {**extra}
        catalog_properties["uri"] = conn.host.rstrip("/") if conn.host else ""
        if "type" not in catalog_properties:
            catalog_properties["type"] = "rest"

        # credential is REST-catalog-specific; other catalogs (Glue, BigQuery)
        # use their own auth fields passed through extra.
        if catalog_properties["type"] == "rest":
            if conn.login and conn.password:
                catalog_properties["credential"] = f"{conn.login}:{conn.password}"
            elif conn.login or conn.password:
                self.log.warning(
                    "Only one of Client ID / Client Secret is set. "
                    "Both are required for OAuth2 credential authentication."
                )

        return load_catalog(self.conn_id, **catalog_properties)

    def get_conn(self) -> Catalog:
        """Return the pyiceberg Catalog."""
        return self.catalog

    def test_connection(self) -> tuple[bool, str]:
        """Test the Iceberg connection by listing namespaces."""
        try:
            namespaces = self.catalog.list_namespaces()
            return True, f"Connected. Found {len(namespaces)} namespace(s)."
        except Exception as e:
            return False, str(e)

    # ---- Token methods (backward compatibility) ----

    def get_token(self) -> str:
        """
        Obtain a short-lived OAuth2 access token.

        This preserves the legacy behavior of the pre-2.0 ``get_conn()`` method.
        Use this when you need a raw token for external engines (Spark, Trino, Flink).
        """
        conn = self.get_connection(self.conn_id)
        base_url = conn.host.rstrip("/") if conn.host else ""
        data = {
            "client_id": conn.login,
            "client_secret": conn.password,
            "grant_type": "client_credentials",
        }
        response = requests.post(f"{base_url}/{TOKENS_ENDPOINT}", data=data, timeout=30)
        response.raise_for_status()
        return response.json()["access_token"]

    def get_token_macro(self) -> str:
        """Return a Jinja2 macro that resolves to a fresh token at render time."""
        return f"{{{{ conn.{self.conn_id}.get_hook().get_token() }}}}"

    # ---- Namespace operations ----

    def list_namespaces(self) -> list[str]:
        """Return all namespace names in the catalog."""
        return [".".join(ns) for ns in self.catalog.list_namespaces()]

    # ---- Table operations ----

    def list_tables(self, namespace: str) -> list[str]:
        """
        Return all table names in the given namespace.

        :param namespace: Namespace (database/schema) to list tables from.
        :return: List of fully-qualified table names ("namespace.table").
        """
        return [".".join(ident) for ident in self.catalog.list_tables(namespace)]

    def load_table(self, table_name: str) -> Table:
        """
        Load an Iceberg table object.

        :param table_name: Fully-qualified table name ("namespace.table").
        :return: pyiceberg Table instance.
        """
        if "." not in table_name:
            raise ValueError(f"Expected fully-qualified table name (namespace.table), got: {table_name!r}")
        return self.catalog.load_table(table_name)

    def table_exists(self, table_name: str) -> bool:
        """Check whether a table exists in the catalog."""
        return self.catalog.table_exists(table_name)

    # ---- Schema introspection ----

    def get_table_schema(self, table_name: str, **kwargs: Any) -> list[dict[str, str]]:
        """
        Return column names and types for an Iceberg table.

        Compatible with the ``DbApiHook.get_table_schema()`` contract so that
        LLM operators can use this hook interchangeably for schema context.

        :param table_name: Fully-qualified table name ("namespace.table").
        :return: List of dicts with ``name`` and ``type`` keys.

        Example return value::

            [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "created_at", "type": "timestamptz"},
            ]
        """
        table = self.load_table(table_name)
        return [
            {
                "name": field.name,
                "type": str(field.field_type),
            }
            for field in table.schema().fields
        ]

    def get_partition_spec(self, table_name: str) -> list[dict[str, str]]:
        """
        Return the partition spec for an Iceberg table.

        :param table_name: Fully-qualified table name.
        :return: List of dicts with ``field`` and ``transform`` keys.

        Example::

            [
                {"field": "event_date", "transform": "day"},
                {"field": "region", "transform": "identity"},
            ]
        """
        table = self.load_table(table_name)
        spec = table.spec()
        schema = table.schema()
        result = []
        for partition_field in spec.fields:
            source_field = schema.find_field(partition_field.source_id)
            result.append(
                {
                    "field": source_field.name,
                    "transform": str(partition_field.transform),
                }
            )
        return result

    def get_table_properties(self, table_name: str) -> dict[str, str]:
        """
        Return table properties (format version, write config, etc.).

        :param table_name: Fully-qualified table name.
        """
        table = self.load_table(table_name)
        return dict(table.properties)

    def get_snapshots(self, table_name: str, limit: int = 10) -> list[dict[str, Any]]:
        """
        Return recent snapshots for an Iceberg table.

        :param table_name: Fully-qualified table name.
        :param limit: Maximum number of snapshots to return (most recent first).
        :return: List of dicts with snapshot metadata.
        """
        table = self.load_table(table_name)
        arrow_table = table.inspect.snapshots()
        num_rows = len(arrow_table)
        if num_rows <= limit:
            rows = arrow_table.to_pylist()
        else:
            rows = arrow_table.slice(offset=num_rows - limit, length=limit).to_pylist()
        rows.reverse()
        return rows
