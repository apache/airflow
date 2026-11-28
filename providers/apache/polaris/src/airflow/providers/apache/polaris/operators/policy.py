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
Operators for Polaris policies.

Polaris stores policies (for example the table-maintenance types ``system.snapshot-expiry``,
``system.data-compaction``, ``system.metadata-compaction`` and ``system.orphan-file-removal``),
attaches them to catalogs, namespaces and tables, and resolves which ones apply to a table through
inheritance. It does not execute them; :class:`PolarisGetApplicablePoliciesOperator` hands the
resolved policies to downstream tasks that do.
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.polaris.exceptions import PolarisInvalidArgumentError
from airflow.providers.apache.polaris.hooks.polaris import IfMissing, IfPolicyExists, namespace_parts
from airflow.providers.apache.polaris.operators.base import PolarisCatalogBaseOperator

if TYPE_CHECKING:
    from apache_polaris.sdk.catalog.models.applicable_policy import ApplicablePolicy
    from apache_polaris.sdk.catalog.models.policy import Policy

    from airflow.providers.common.compat.sdk import Context

ORPHAN_FILE_REMOVAL = "system.orphan-file-removal"


def _parse_content(content: str | None) -> Any:
    if content is None:
        return None
    try:
        return json.loads(content)
    except ValueError:
        return content


def policy_to_dict(policy: Policy | ApplicablePolicy) -> dict[str, Any]:
    """Return a JSON-serializable view of a policy, with ``content`` parsed from its JSON string."""
    result: dict[str, Any] = {
        "name": policy.name,
        "type": policy.policy_type,
        "description": policy.description,
        "inheritable": policy.inheritable,
        "version": policy.version,
        "content": _parse_content(policy.content),
    }
    if hasattr(policy, "inherited"):
        result["inherited"] = policy.inherited
        result["defined_in"] = list(policy.namespace)  # namespace where the policy is defined
    return result


class PolarisCreatePolicyOperator(PolarisCatalogBaseOperator):
    """
    Create a policy in a namespace, or bring an existing one up to date.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisCreatePolicyOperator`

    :param namespace: Namespace that holds the policy, as ``"a.b"`` or ``["a", "b"]``.
    :param policy_name: Policy name, unique within the namespace.
    :param policy_type: Policy type, for example ``system.snapshot-expiry``.
    :param content: Policy content as a dict or JSON string; Polaris validates it against the
        type's schema.
    :param description: Optional description.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param if_exists: ``"update"`` (default) updates content and description when they differ,
        so the task is declarative; ``"skip"`` leaves an existing policy unchanged; ``"fail"`` raises.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: The policy as a dict, with ``content`` parsed.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "policy_name",
        "policy_type",
        "content",
        "description",
        "catalog",
    )
    template_fields_renderers = {"content": "json"}

    def __init__(
        self,
        *,
        namespace: str | list[str],
        policy_name: str,
        policy_type: str,
        content: dict[str, Any] | str | None = None,
        description: str | None = None,
        catalog: str | None = None,
        if_exists: IfPolicyExists = "update",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.policy_name = policy_name
        self.policy_type = policy_type
        self.content = content
        self.description = description
        self.catalog = catalog
        self.if_exists = if_exists

    def execute(self, context: Context) -> dict[str, Any]:
        policy = self.hook.create_policy(
            self.namespace,
            self.policy_name,
            self.policy_type,
            content=self.content,
            description=self.description,
            catalog=self.catalog,
            if_exists=self.if_exists,
        )
        return policy_to_dict(policy)


class PolarisDropPolicyOperator(PolarisCatalogBaseOperator):
    """
    Drop a policy.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisDropPolicyOperator`

    :param namespace: Namespace that holds the policy, as ``"a.b"`` or ``["a", "b"]``.
    :param policy_name: Policy to drop.
    :param detach_all: Detach the policy from all its targets first; otherwise Polaris refuses to
        drop an attached policy.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param if_missing: ``"skip"`` (default) or ``"fail"``.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if dropped, ``False`` if it did not exist.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "policy_name",
        "catalog",
    )

    def __init__(
        self,
        *,
        namespace: str | list[str],
        policy_name: str,
        detach_all: bool = False,
        catalog: str | None = None,
        if_missing: IfMissing = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.policy_name = policy_name
        self.detach_all = detach_all
        self.catalog = catalog
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.drop_policy(
            self.namespace,
            self.policy_name,
            catalog=self.catalog,
            detach_all=self.detach_all,
            if_missing=self.if_missing,
        )


class PolarisAttachPolicyOperator(PolarisCatalogBaseOperator):
    """
    Attach a policy to a catalog, namespace or table. Re-attaching the same policy is a no-op.

    A target holds at most one inheritable policy of each type. If a different policy of the same
    type is already attached, the task fails without retrying; detach the other policy first.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisAttachPolicyOperator`

    :param namespace: Namespace that holds the policy, as ``"a.b"`` or ``["a", "b"]``.
    :param policy_name: Policy to attach.
    :param target: ``{"type": "catalog"}``, ``{"type": "namespace", "path": ["sales"]}`` or
        ``{"type": "table-like", "path": ["sales", "orders"]}``.
    :param parameters: Optional attachment parameters.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "policy_name",
        "target",
        "parameters",
        "catalog",
    )
    template_fields_renderers = {"target": "json", "parameters": "json"}

    def __init__(
        self,
        *,
        namespace: str | list[str],
        policy_name: str,
        target: dict[str, Any],
        parameters: dict[str, str] | None = None,
        catalog: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.policy_name = policy_name
        self.target = target
        self.parameters = parameters
        self.catalog = catalog

    def execute(self, context: Context) -> None:
        self.hook.attach_policy(
            self.namespace, self.policy_name, self.target, parameters=self.parameters, catalog=self.catalog
        )


class PolarisDetachPolicyOperator(PolarisCatalogBaseOperator):
    """
    Detach a policy from a catalog, namespace or table.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisDetachPolicyOperator`

    :param namespace: Namespace that holds the policy, as ``"a.b"`` or ``["a", "b"]``.
    :param policy_name: Policy to detach.
    :param target: Same shape as for :class:`PolarisAttachPolicyOperator`.
    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param if_missing: ``"skip"`` (default) or ``"fail"`` when the policy is not attached there.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if detached, ``False`` if it was not attached.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "namespace",
        "policy_name",
        "target",
        "catalog",
    )
    template_fields_renderers = {"target": "json"}

    def __init__(
        self,
        *,
        namespace: str | list[str],
        policy_name: str,
        target: dict[str, Any],
        catalog: str | None = None,
        if_missing: IfMissing = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.namespace = namespace
        self.policy_name = policy_name
        self.target = target
        self.catalog = catalog
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.detach_policy(
            self.namespace, self.policy_name, self.target, catalog=self.catalog, if_missing=self.if_missing
        )


class PolarisGetApplicablePoliciesOperator(PolarisCatalogBaseOperator):
    """
    Resolve the policies in effect on a catalog, namespace or table (inheritance applied by Polaris).

    Two output shapes:

    * ``per_table=False`` (default): a list of policies for the target, each
      ``{"name", "type", "description", "inheritable", "version", "content", "inherited",
      "defined_in"}`` with ``content`` parsed from JSON.
    * ``per_table=True``: one item per (table, policy) for every table in ``namespace``, ready for
      dynamic task mapping: ``{"catalog", "namespace", "table", "policy": {...}}``. Items for
      ``system.orphan-file-removal`` policies also carry ``table_location`` (the table's base
      location), so a downstream task can check that the policy's ``locations`` stay inside it.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisGetApplicablePoliciesOperator`

    :param catalog: Catalog; defaults to the connection's ``default_catalog``.
    :param namespace: Namespace to resolve for (``"a.b"`` or ``["a", "b"]``). Required with
        ``per_table`` or ``table``; omit both for the catalog itself.
    :param table: A single table in ``namespace``. Not allowed with ``per_table``.
    :param policy_types: Only return policies of these types (a list, or one type as a string).
    :param per_table: Resolve policies for every table in ``namespace``.
    :param enabled_only: Drop policies whose content has ``"enable": false`` (default true).
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: With ``per_table=False``, a list of policy dicts; with ``per_table=True``, a list of
        ``{"catalog", "namespace", "table", "policy"}`` items (plus ``"table_location"`` for
        ``system.orphan-file-removal``), one per table and policy.
    """

    template_fields: Sequence[str] = (
        *PolarisCatalogBaseOperator.template_fields,
        "catalog",
        "namespace",
        "table",
        "policy_types",
    )

    def __init__(
        self,
        *,
        catalog: str | None = None,
        namespace: str | list[str] | None = None,
        table: str | None = None,
        policy_types: list[str] | str | None = None,
        per_table: bool = False,
        enabled_only: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        if per_table and table is not None:
            raise ValueError("Pass either table or per_table=True, not both.")
        if (per_table or table is not None) and namespace is None:
            raise ValueError("namespace is required with table or per_table=True.")
        self.catalog = catalog
        self.namespace = namespace
        self.table = table
        self.policy_types = policy_types
        self.per_table = per_table
        self.enabled_only = enabled_only

    def _policy_types(self) -> list[str]:
        types = self.policy_types or []
        if isinstance(types, str):  # one type, or a Jinja template rendered to text
            if types.lstrip().startswith("["):
                raise PolarisInvalidArgumentError(
                    f"policy_types must be a list, got the string {types[:200]!r}. If it comes from a "
                    "template, set render_template_as_native_obj=True on the Dag."
                )
            return [types]
        return list(types)

    def _resolve(self, catalog: str, table: str | None) -> list[dict[str, Any]]:
        types = self._policy_types()
        policies = self.hook.get_applicable_policies(
            catalog=catalog,
            namespace=self.namespace,
            table=table,
            policy_type=types[0] if len(types) == 1 else None,  # the API filters by one type only
        )
        result = []
        for policy in policies:
            if types and policy.policy_type not in types:
                continue
            item = policy_to_dict(policy)
            content = item["content"]
            if self.enabled_only and isinstance(content, dict) and content.get("enable") is False:
                self.log.info("Skipping disabled policy %r (%s).", item["name"], item["type"])
                continue
            result.append(item)
        return result

    def execute(self, context: Context) -> list[dict[str, Any]]:
        catalog = self.hook.resolve_catalog(self.catalog)
        if not self.per_table:
            return self._resolve(catalog, self.table)

        namespace = namespace_parts(self.namespace)  # type: ignore[arg-type]  # checked in __init__
        items: list[dict[str, Any]] = []
        tables = self.hook.list_tables(namespace, catalog=catalog)
        for identifier in tables:
            policies = self._resolve(catalog, identifier.name)
            location = None
            if any(p["type"] == ORPHAN_FILE_REMOVAL for p in policies):
                location = self.hook.get_table_location(namespace, identifier.name, catalog=catalog)
            for policy in policies:
                item: dict[str, Any] = {
                    "catalog": catalog,
                    "namespace": namespace,
                    "table": identifier.name,
                    "policy": policy,
                }
                if policy["type"] == ORPHAN_FILE_REMOVAL:
                    item["table_location"] = location
                items.append(item)
        self.log.info(
            "Resolved %d (table, policy) pairs across %d tables in %s.", len(items), len(tables), namespace
        )
        return items
