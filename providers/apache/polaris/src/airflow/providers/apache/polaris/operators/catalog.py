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
"""Operators that create, update and delete Polaris catalogs."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.polaris.operators.base import PolarisManagementBaseOperator

if TYPE_CHECKING:
    from airflow.providers.apache.polaris.hooks.polaris import IfExists, IfMissing
    from airflow.providers.common.compat.sdk import Context


class PolarisCreateCatalogOperator(PolarisManagementBaseOperator):
    """
    Create a Polaris catalog.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisCreateCatalogOperator`

    :param catalog: The catalog in Polaris's JSON shape, for example ``{"type": "INTERNAL",
        "name": "analytics", "properties": {"default-base-location": "s3://bucket/analytics"},
        "storageConfigInfo": {"storageType": "S3", "roleArn": "arn:aws:iam::...",
        "allowedLocations": ["s3://bucket/"]}}``. Use ``"type": "EXTERNAL"`` with
        ``connectionConfigInfo`` for a federated catalog.
    :param if_exists: ``"skip"`` (default) succeeds and returns the existing catalog unchanged;
        ``"fail"`` raises.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: The catalog as a dict.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "catalog",
    )
    template_fields_renderers = {"catalog": "json"}

    def __init__(self, *, catalog: dict[str, Any], if_exists: IfExists = "skip", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.catalog = catalog
        self.if_exists = if_exists

    def execute(self, context: Context) -> dict[str, Any]:
        return self.hook.create_catalog(self.catalog, if_exists=self.if_exists).to_dict()


class PolarisUpdateCatalogOperator(PolarisManagementBaseOperator):
    """
    Update a Polaris catalog's properties and/or storage configuration.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisUpdateCatalogOperator`

    :param catalog_name: Catalog to update.
    :param properties: Properties to set; merged into the existing ones unless
        ``replace_properties`` is true.
    :param storage_config_info: New storage configuration in Polaris's JSON shape, if it changes.
    :param replace_properties: Replace all properties instead of merging.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: The updated catalog as a dict.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "catalog_name",
        "properties",
        "storage_config_info",
    )
    template_fields_renderers = {"properties": "json", "storage_config_info": "json"}

    def __init__(
        self,
        *,
        catalog_name: str,
        properties: dict[str, str] | None = None,
        storage_config_info: dict[str, Any] | None = None,
        replace_properties: bool = False,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.catalog_name = catalog_name
        self.properties = properties
        self.storage_config_info = storage_config_info
        self.replace_properties = replace_properties

    def execute(self, context: Context) -> dict[str, Any]:
        return self.hook.update_catalog(
            self.catalog_name,
            properties=self.properties,
            storage_config_info=self.storage_config_info,
            replace_properties=self.replace_properties,
        ).to_dict()


class PolarisDeleteCatalogOperator(PolarisManagementBaseOperator):
    """
    Delete a Polaris catalog.

    Polaris refuses to delete a catalog that still contains namespaces or catalog roles other than
    ``catalog_admin``.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisDeleteCatalogOperator`

    :param catalog_name: Catalog to delete.
    :param if_missing: ``"skip"`` (default) succeeds when the catalog does not exist;
        ``"fail"`` raises.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if the catalog was deleted, ``False`` if it did not exist.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "catalog_name",
    )

    def __init__(self, *, catalog_name: str, if_missing: IfMissing = "skip", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.catalog_name = catalog_name
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.delete_catalog(self.catalog_name, if_missing=self.if_missing)
