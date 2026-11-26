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
"""Operators that create and delete Polaris principals."""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from airflow.providers.apache.polaris.operators.base import PolarisManagementBaseOperator

if TYPE_CHECKING:
    from airflow.providers.apache.polaris.hooks.polaris import IfExists, IfMissing
    from airflow.providers.common.compat.sdk import Context


class PolarisCreatePrincipalOperator(PolarisManagementBaseOperator):
    """
    Create a Polaris principal (a service identity).

    The operator never returns, logs or stores the new principal's client secret, which Polaris
    sends only once. It suits principals that authenticate through an external identity provider;
    for Polaris-issued credentials, obtain them out of band (for example ``polaris principals
    reset`` in the Polaris CLI) and store them in your secrets backend.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisCreatePrincipalOperator`

    :param principal_name: Name of the principal.
    :param properties: Optional principal properties.
    :param credential_rotation_required: If true, Polaris lets the initial credentials call only
        ``rotateCredentials`` (enforced when the server enables that check).
    :param if_exists: ``"skip"`` (default) succeeds and returns the existing principal;
        ``"fail"`` raises.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: The principal as a dict (name, client id, properties); never credentials.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "principal_name",
        "properties",
    )
    template_fields_renderers = {"properties": "json"}

    def __init__(
        self,
        *,
        principal_name: str,
        properties: dict[str, str] | None = None,
        credential_rotation_required: bool = False,
        if_exists: IfExists = "skip",
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.principal_name = principal_name
        self.properties = properties
        self.credential_rotation_required = credential_rotation_required
        self.if_exists = if_exists

    def execute(self, context: Context) -> dict[str, Any]:
        principal = self.hook.create_principal(
            self.principal_name,
            properties=self.properties,
            credential_rotation_required=self.credential_rotation_required,
            if_exists=self.if_exists,
        )
        return principal.to_dict()


class PolarisDeletePrincipalOperator(PolarisManagementBaseOperator):
    """
    Delete a Polaris principal.

    .. seealso::
        For more information, take a look at the guide:
        :ref:`howto/operator:PolarisDeletePrincipalOperator`

    :param principal_name: Principal to delete.
    :param if_missing: ``"skip"`` (default) succeeds when the principal does not exist;
        ``"fail"`` raises.
    :param polaris_conn_id: :ref:`Polaris connection id <howto/connection:polaris>`.
    :return: ``True`` if deleted, ``False`` if it did not exist.
    """

    template_fields: Sequence[str] = (
        *PolarisManagementBaseOperator.template_fields,
        "principal_name",
    )

    def __init__(self, *, principal_name: str, if_missing: IfMissing = "skip", **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.principal_name = principal_name
        self.if_missing = if_missing

    def execute(self, context: Context) -> bool:
        return self.hook.delete_principal(self.principal_name, if_missing=self.if_missing)
