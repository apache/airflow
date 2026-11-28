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
"""Operators that create, update and drop namespaces in a Polaris catalog."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.polaris.hooks.polaris import namespace_parts
from airflow.providers.apache.polaris.operators.base import PolarisCatalogBaseOperator

if TYPE_CHECKING:
    from airflow.providers.apache.polaris.hooks.polaris import IfExists, IfMissing
    from airflow.providers.common.compat.sdk import Context


class PolarisCreateNamespaceOperator(PolarisCatalogBaseOperator):
    """
    Create a namespace in a Polaris catalog.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisCreateNamespaceOperator`

    :param namespace: Namespace to create, as ``"a.b"`` or ``["a", "b"]``; its parent must exist.
    :param properties: Optional namespace properties, for example ``{"owner": "data-eng"}``;
        values are strings.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param if_exists: ``"skip"`` (default) succeeds and returns the existing namespace unchanged;
        ``"fail"`` raises.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``{"namespace": [...], "properties": {...}}``. Polaris adds a ``location`` property.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "properties",
        "catalog",
    )
    template_fields_renderers = {"properties": "json"}

    def __init__(
        self,
        *,
        namespace: str | list[str],
        properties: dict[str, str] | None = None,
        catalog: str | None = None,
        if_exists: IfExists = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.properties = properties
        self.catalog = catalog
        self.if_exists = if_exists

    def execute(self, context: Context) -> dict[str, Any]:
        properties = self.hook.create_namespace(
            self.namespace, properties=self.properties, catalog=self.catalog, if_exists=self.if_exists
        )
        return {"namespace": namespace_parts(self.namespace), "properties": properties}


class PolarisUpdateNamespacePropertiesOperator(PolarisCatalogBaseOperator):
    """
    Set and/or remove properties of a namespace. Re-running it is safe.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisUpdateNamespacePropertiesOperator`

    :param namespace: Namespace, as ``"a.b"`` or ``["a", "b"]``.
    :param updates: Properties to set; values are strings.
    :param removals: Property keys to remove; keys that do not exist are reported as missing. A
        key cannot be both updated and removed.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``{"updated": [...], "removed": [...], "missing": [...]}``: property keys.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "updates",
        "removals",
        "catalog",
    )
    template_fields_renderers = {"updates": "json", "removals": "json"}

    def __init__(
        self,
        *,
        namespace: str | list[str],
        updates: dict[str, str] | None = None,
        removals: list[str] | None = None,
        catalog: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if updates is None and removals is None:
            raise ValueError("Pass updates and/or removals.")
        self.namespace = namespace
        self.updates = updates
        self.removals = removals
        self.catalog = catalog

    def execute(self, context: Context) -> dict[str, Any]:
        return self.hook.update_namespace_properties(
            self.namespace, updates=self.updates, removals=self.removals, catalog=self.catalog
        )


class PolarisDropNamespaceOperator(PolarisCatalogBaseOperator):
    """
    Drop an empty namespace. Polaris refuses to drop one that still has contents.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisDropNamespaceOperator`

    :param namespace: Namespace to drop, as ``"a.b"`` or ``["a", "b"]``.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param if_missing: ``"skip"`` (default) succeeds when the namespace does not exist;
        ``"fail"`` raises.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if the namespace was dropped, ``False`` if it did not exist.
    """

    template_fields: Sequence[str] = (*PolarisCatalogBaseOperator.template_fields, "namespace", "catalog")

    def __init__(
        self,
        *,
        namespace: str | list[str],
        catalog: str | None = None,
        if_missing: IfMissing = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.catalog = catalog
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.drop_namespace(self.namespace, catalog=self.catalog, if_missing=self.if_missing)
