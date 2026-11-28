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
Operators for Polaris role-based access control.

Polaris RBAC has two levels: principals are assigned **principal roles**; principal roles are
assigned **catalog roles**; catalog roles are granted **privileges** on a catalog, namespace,
table, view or policy.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.polaris.exceptions import PolarisInvalidArgumentError
from airflow.providers.apache.polaris.operators.base import PolarisManagementBaseOperator

if TYPE_CHECKING:
    from airflow.providers.apache.polaris.hooks.polaris import IfExists, IfMissing
    from airflow.providers.common.compat.sdk import Context


def _as_list(grants: dict[str, Any] | list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(grants, dict):
        return [grants]
    if isinstance(grants, str):  # a Jinja template rendered to text instead of a list
        raise PolarisInvalidArgumentError(
            f"grants must be a dict or a list of dicts, got the string {grants[:200]!r}. If it comes "
            "from a template, set render_template_as_native_obj=True on the Dag."
        )
    return list(grants)


class PolarisCreatePrincipalRoleOperator(PolarisManagementBaseOperator):
    """
    Create a principal role.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisCreatePrincipalRoleOperator`

    :param role_name: Name of the principal role.
    :param properties: Optional role properties.
    :param if_exists: ``"skip"`` (default) or ``"fail"``.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: The principal role as a dict.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "role_name",
        "properties",
    )

    def __init__(
        self,
        *,
        role_name: str,
        properties: dict[str, str] | None = None,
        if_exists: IfExists = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.role_name = role_name
        self.properties = properties
        self.if_exists = if_exists

    def execute(self, context: Context) -> dict[str, Any]:
        return self.hook.create_principal_role(
            self.role_name, properties=self.properties, if_exists=self.if_exists
        ).to_dict()


class PolarisDeletePrincipalRoleOperator(PolarisManagementBaseOperator):
    """
    Delete a principal role.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisDeletePrincipalRoleOperator`

    :param role_name: Principal role to delete.
    :param if_missing: ``"skip"`` (default) or ``"fail"``.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if deleted, ``False`` if it did not exist.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "role_name",
    )

    def __init__(self, *, role_name: str, if_missing: IfMissing = "skip", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.role_name = role_name
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.delete_principal_role(self.role_name, if_missing=self.if_missing)


class PolarisCreateCatalogRoleOperator(PolarisManagementBaseOperator):
    """
    Create a catalog role in a catalog.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisCreateCatalogRoleOperator`

    :param catalog_name: Catalog the role belongs to.
    :param role_name: Name of the catalog role.
    :param properties: Optional role properties.
    :param if_exists: ``"skip"`` (default) or ``"fail"``.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: The catalog role as a dict.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "catalog_name",
        "role_name",
        "properties",
    )

    def __init__(
        self,
        *,
        catalog_name: str,
        role_name: str,
        properties: dict[str, str] | None = None,
        if_exists: IfExists = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.catalog_name = catalog_name
        self.role_name = role_name
        self.properties = properties
        self.if_exists = if_exists

    def execute(self, context: Context) -> dict[str, Any]:
        return self.hook.create_catalog_role(
            self.catalog_name, self.role_name, properties=self.properties, if_exists=self.if_exists
        ).to_dict()


class PolarisDeleteCatalogRoleOperator(PolarisManagementBaseOperator):
    """
    Delete a catalog role.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisDeleteCatalogRoleOperator`

    :param catalog_name: Catalog the role belongs to.
    :param role_name: Catalog role to delete.
    :param if_missing: ``"skip"`` (default) or ``"fail"``.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if deleted, ``False`` if it did not exist.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "catalog_name",
        "role_name",
    )

    def __init__(
        self, *, catalog_name: str, role_name: str, if_missing: IfMissing = "skip", **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.catalog_name = catalog_name
        self.role_name = role_name
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.delete_catalog_role(self.catalog_name, self.role_name, if_missing=self.if_missing)


class PolarisAssignPrincipalRoleOperator(PolarisManagementBaseOperator):
    """
    Assign a principal role to a principal. Already-assigned roles are left as they are.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisAssignPrincipalRoleOperator`

    :param principal_name: Principal receiving the role.
    :param principal_role_name: Principal role to assign.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "principal_name",
        "principal_role_name",
    )

    def __init__(self, *, principal_name: str, principal_role_name: str, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.principal_name = principal_name
        self.principal_role_name = principal_role_name

    def execute(self, context: Context) -> None:
        self.hook.assign_principal_role(self.principal_name, self.principal_role_name)


class PolarisRevokePrincipalRoleOperator(PolarisManagementBaseOperator):
    """
    Revoke a principal role from a principal.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisRevokePrincipalRoleOperator`

    :param principal_name: Principal holding the role.
    :param principal_role_name: Principal role to revoke.
    :param if_missing: ``"skip"`` (default) or ``"fail"``.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if revoked, ``False`` if it was not assigned.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "principal_name",
        "principal_role_name",
    )

    def __init__(
        self, *, principal_name: str, principal_role_name: str, if_missing: IfMissing = "skip", **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.principal_name = principal_name
        self.principal_role_name = principal_role_name
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.revoke_principal_role(
            self.principal_name, self.principal_role_name, if_missing=self.if_missing
        )


class PolarisAssignCatalogRoleOperator(PolarisManagementBaseOperator):
    """
    Assign a catalog role to a principal role. Already-assigned roles are left as they are.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisAssignCatalogRoleOperator`

    :param principal_role_name: Principal role receiving the catalog role.
    :param catalog_name: Catalog the catalog role belongs to.
    :param catalog_role_name: Catalog role to assign.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "principal_role_name",
        "catalog_name",
        "catalog_role_name",
    )

    def __init__(
        self, *, principal_role_name: str, catalog_name: str, catalog_role_name: str, **kwargs: Any
    ) -> None:
        super().__init__(**kwargs)
        self.principal_role_name = principal_role_name
        self.catalog_name = catalog_name
        self.catalog_role_name = catalog_role_name

    def execute(self, context: Context) -> None:
        self.hook.assign_catalog_role(self.principal_role_name, self.catalog_name, self.catalog_role_name)


class PolarisRevokeCatalogRoleOperator(PolarisManagementBaseOperator):
    """
    Revoke a catalog role from a principal role.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisRevokeCatalogRoleOperator`

    :param principal_role_name: Principal role holding the catalog role.
    :param catalog_name: Catalog the catalog role belongs to.
    :param catalog_role_name: Catalog role to revoke.
    :param if_missing: ``"skip"`` (default) or ``"fail"``.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if revoked, ``False`` if it was not assigned.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "principal_role_name",
        "catalog_name",
        "catalog_role_name",
    )

    def __init__(
        self,
        *,
        principal_role_name: str,
        catalog_name: str,
        catalog_role_name: str,
        if_missing: IfMissing = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.principal_role_name = principal_role_name
        self.catalog_name = catalog_name
        self.catalog_role_name = catalog_role_name
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.revoke_catalog_role(
            self.principal_role_name, self.catalog_name, self.catalog_role_name, if_missing=self.if_missing
        )


class PolarisGrantPrivilegeOperator(PolarisManagementBaseOperator):
    """
    Grant one or more privileges to a catalog role. Existing grants are left as they are.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisGrantPrivilegeOperator`

    :param catalog_name: Catalog the catalog role belongs to.
    :param catalog_role_name: Catalog role receiving the privileges.
    :param grants: One grant or a list, in Polaris's JSON shape, for example
        ``{"type": "catalog", "privilege": "TABLE_READ_DATA"}`` or ``{"type": "namespace",
        "namespace": ["sales"], "privilege": "TABLE_WRITE_DATA"}``. Types: ``catalog``,
        ``namespace``, ``table`` (with ``tableName``), ``view`` (``viewName``), ``policy``
        (``policyName``).
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "catalog_name",
        "catalog_role_name",
        "grants",
    )
    template_fields_renderers = {"grants": "json"}

    def __init__(
        self,
        *,
        catalog_name: str,
        catalog_role_name: str,
        grants: dict[str, Any] | list[dict[str, Any]],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.catalog_name = catalog_name
        self.catalog_role_name = catalog_role_name
        self.grants = grants

    def execute(self, context: Context) -> None:
        for grant in _as_list(self.grants):
            self.log.info("Granting %s to catalog role %r.", grant, self.catalog_role_name)
            self.hook.add_grant(self.catalog_name, self.catalog_role_name, grant)


class PolarisRevokePrivilegeOperator(PolarisManagementBaseOperator):
    """
    Revoke one or more privileges from a catalog role.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisRevokePrivilegeOperator`

    :param catalog_name: Catalog the catalog role belongs to.
    :param catalog_role_name: Catalog role losing the privileges.
    :param grants: One grant or a list, in the same shape as for
        :class:`PolarisGrantPrivilegeOperator`.
    :param cascade: Also revoke the privilege on sub-resources.
    :param if_missing: ``"skip"`` (default) or ``"fail"`` for grants that do not exist.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "catalog_name",
        "catalog_role_name",
        "grants",
    )
    template_fields_renderers = {"grants": "json"}

    def __init__(
        self,
        *,
        catalog_name: str,
        catalog_role_name: str,
        grants: dict[str, Any] | list[dict[str, Any]],
        cascade: bool = False,
        if_missing: IfMissing = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.catalog_name = catalog_name
        self.catalog_role_name = catalog_role_name
        self.grants = grants
        self.cascade = cascade
        self.if_missing = if_missing

    def execute(self, context: Context) -> None:
        for grant in _as_list(self.grants):
            self.log.info("Revoking %s from catalog role %r.", grant, self.catalog_role_name)
            self.hook.revoke_grant(
                self.catalog_name,
                self.catalog_role_name,
                grant,
                cascade=self.cascade,
                if_missing=self.if_missing,
            )
