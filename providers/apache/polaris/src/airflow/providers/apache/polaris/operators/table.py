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
"""
Operators that manage Iceberg table definitions in a Polaris catalog.

They create, drop, register, rename and set properties of tables. Reading and writing table data
is left to pyiceberg and the engines (Spark, Trino, ...).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.polaris.hooks.polaris import namespace_parts
from airflow.providers.apache.polaris.operators.base import PolarisCatalogBaseOperator

if TYPE_CHECKING:
    from airflow.providers.apache.polaris.hooks.polaris import IfExists, IfMissing
    from airflow.providers.common.compat.sdk import Context


def table_to_dict(result: dict[str, Any], namespace: str | Sequence[str], table: str) -> dict[str, Any]:
    """
    Return a summary of a table as returned by the hook (Iceberg REST ``LoadTableResult`` JSON).

    The response's ``config`` and ``storage-credentials`` are left out on purpose: they can carry
    storage credentials, which must not reach XCom.
    """
    metadata = result.get("metadata") or {}
    return {
        "namespace": namespace_parts(namespace),
        "name": table,
        "table_uuid": metadata.get("table-uuid"),
        "location": metadata.get("location"),
        "metadata_location": result.get("metadata-location"),
        "format_version": metadata.get("format-version"),
    }


class PolarisCreateTableOperator(PolarisCatalogBaseOperator):
    """
    Create an Iceberg table in a Polaris catalog.

    The table's definition uses the JSON shapes of the Iceberg REST specification.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisCreateTableOperator`

    :param namespace: Namespace of the table, as ``"a.b"`` or ``["a", "b"]``.
    :param table: Table name.
    :param schema: The schema, for example ``{"type": "struct", "fields": [{"id": 1, "name": "id",
        "type": "long", "required": False}]}``.
    :param partition_spec: Optional partition spec, for example ``{"fields": [{"source-id": 1,
        "field-id": 1000, "name": "id_bucket", "transform": "bucket[16]"}]}``.
    :param sort_order: Optional write order, for example ``{"order-id": 1, "fields":
        [{"source-id": 1, "transform": "identity", "direction": "asc", "null-order":
        "nulls-first"}]}``.
    :param location: Optional base location; defaults to one derived from the namespace's.
    :param properties: Optional table properties, for example ``{"format-version": "3"}``;
        values are strings.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param if_exists: ``"skip"`` (default) succeeds and returns the existing table unchanged;
        ``"fail"`` raises.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``{"namespace", "name", "table_uuid", "location", "metadata_location",
        "format_version"}``.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "table",
        "schema",
        "partition_spec",
        "sort_order",
        "location",
        "properties",
        "catalog",
    )
    template_fields_renderers = {
        "schema": "json",
        "partition_spec": "json",
        "sort_order": "json",
        "properties": "json",
    }

    def __init__(
        self,
        *,
        namespace: str | list[str],
        table: str,
        schema: dict[str, Any],
        partition_spec: dict[str, Any] | None = None,
        sort_order: dict[str, Any] | None = None,
        location: str | None = None,
        properties: dict[str, str] | None = None,
        catalog: str | None = None,
        if_exists: IfExists = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.table = table
        self.schema = schema
        self.partition_spec = partition_spec
        self.sort_order = sort_order
        self.location = location
        self.properties = properties
        self.catalog = catalog
        self.if_exists = if_exists

    def execute(self, context: Context) -> dict[str, Any]:
        result = self.hook.create_table(
            self.namespace,
            self.table,
            self.schema,
            partition_spec=self.partition_spec,
            sort_order=self.sort_order,
            location=self.location,
            properties=self.properties,
            catalog=self.catalog,
            if_exists=self.if_exists,
        )
        return table_to_dict(result, self.namespace, self.table)


class PolarisDropTableOperator(PolarisCatalogBaseOperator):
    """
    Drop an Iceberg table from a Polaris catalog.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisDropTableOperator`

    :param namespace: Namespace of the table, as ``"a.b"`` or ``["a", "b"]``.
    :param table: Table name.
    :param purge: Also delete the table's data and metadata files (default ``False``: only the
        catalog entry is removed). Irreversible; Polaris refuses it (HTTP 403) unless the
        catalog property ``polaris.config.drop-with-purge.enabled`` is ``"true"`` or the server
        enables the ``DROP_WITH_PURGE_ENABLED`` feature.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param if_missing: ``"skip"`` (default) succeeds when the table does not exist;
        ``"fail"`` raises.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if the table was dropped, ``False`` if it did not exist.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "table",
        "catalog",
    )

    def __init__(
        self,
        *,
        namespace: str | list[str],
        table: str,
        purge: bool = False,
        catalog: str | None = None,
        if_missing: IfMissing = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.table = table
        self.purge = purge
        self.catalog = catalog
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.drop_table(
            self.namespace, self.table, purge=self.purge, catalog=self.catalog, if_missing=self.if_missing
        )


class PolarisUpdateTablePropertiesOperator(PolarisCatalogBaseOperator):
    """
    Set and/or remove properties of an Iceberg table, committing only what would change.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisUpdateTablePropertiesOperator`

    :param namespace: Namespace of the table, as ``"a.b"`` or ``["a", "b"]``.
    :param table: Table name.
    :param updates: Properties to set, for example ``{"history.expire.max-snapshot-age-ms":
        "604800000"}``. Values are strings.
    :param removals: Property keys to remove; keys that do not exist are ignored. A key cannot be
        both updated and removed.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``{"updated": [...], "removed": [...], "properties": {...}}``: the keys this run
        changed and the table's properties afterwards.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "table",
        "updates",
        "removals",
        "catalog",
    )
    template_fields_renderers = {"updates": "json", "removals": "json"}

    def __init__(
        self,
        *,
        namespace: str | list[str],
        table: str,
        updates: dict[str, str] | None = None,
        removals: list[str] | None = None,
        catalog: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if updates is None and removals is None:
            raise ValueError("Pass updates and/or removals.")
        self.namespace = namespace
        self.table = table
        self.updates = updates
        self.removals = removals
        self.catalog = catalog

    def execute(self, context: Context) -> dict[str, Any]:
        return self.hook.update_table_properties(
            self.namespace, self.table, updates=self.updates, removals=self.removals, catalog=self.catalog
        )


class PolarisRegisterTableOperator(PolarisCatalogBaseOperator):
    """
    Register an existing Iceberg table in a Polaris catalog from its metadata file.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisRegisterTableOperator`

    :param namespace: Namespace to register the table in, as ``"a.b"`` or ``["a", "b"]``.
    :param table: Table name in Polaris.
    :param metadata_location: The table's current ``metadata.json`` file, within the catalog's
        allowed locations.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param if_exists: ``"skip"`` (default) succeeds and returns the existing table without
        comparing its metadata location; ``"fail"`` raises.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``{"namespace", "name", "table_uuid", "location", "metadata_location",
        "format_version"}``.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "table",
        "metadata_location",
        "catalog",
    )

    def __init__(
        self,
        *,
        namespace: str | list[str],
        table: str,
        metadata_location: str,
        catalog: str | None = None,
        if_exists: IfExists = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.table = table
        self.metadata_location = metadata_location
        self.catalog = catalog
        self.if_exists = if_exists

    def execute(self, context: Context) -> dict[str, Any]:
        result = self.hook.register_table(
            self.namespace,
            self.table,
            self.metadata_location,
            catalog=self.catalog,
            if_exists=self.if_exists,
        )
        return table_to_dict(result, self.namespace, self.table)


class PolarisRenameTableOperator(PolarisCatalogBaseOperator):
    """
    Rename an Iceberg table, optionally moving it to another namespace of the same catalog.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisRenameTableOperator`

    :param namespace: Current namespace of the table, as ``"a.b"`` or ``["a", "b"]``.
    :param table: Current table name.
    :param new_table: New table name.
    :param new_namespace: New namespace; defaults to the current one. Since Polaris 1.9.0 a move
        is refused (HTTP 403) unless the table's files lie within the new namespace's location
        or the catalog enables ``ALLOW_UNSTRUCTURED_TABLE_LOCATION``.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param if_missing: ``"skip"`` (default) succeeds when the table is gone but its new name
        exists, i.e. it was already renamed. A table missing under both names always fails.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if the table was renamed, ``False`` if it had already been renamed.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "table",
        "new_table",
        "new_namespace",
        "catalog",
    )

    def __init__(
        self,
        *,
        namespace: str | list[str],
        table: str,
        new_table: str,
        new_namespace: str | list[str] | None = None,
        catalog: str | None = None,
        if_missing: IfMissing = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.table = table
        self.new_table = new_table
        self.new_namespace = new_namespace
        self.catalog = catalog
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.rename_table(
            self.namespace,
            self.table,
            self.new_table,
            new_namespace=self.new_namespace,
            catalog=self.catalog,
            if_missing=self.if_missing,
        )
