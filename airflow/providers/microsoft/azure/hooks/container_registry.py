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
"""Hook for Azure Container Registry."""
from __future__ import annotations

from functools import cached_property
from typing import Any

from azure.mgmt.containerinstance.models import ImageRegistryCredential
from azure.mgmt.containerregistry import ContainerRegistryManagementClient

from airflow.hooks.base import BaseHook
from airflow.providers.microsoft.azure.utils import (
    add_managed_identity_connection_widgets,
    get_field,
    get_sync_default_azure_credential,
)


class AzureContainerRegistryHook(BaseHook):
    """
    A hook to communicate with a Azure Container Registry.

    :param conn_id: :ref:`Azure Container Registry connection id<howto/connection:acr>`
        of a service principal which will be used to start the container instance

    """

    conn_name_attr = "azure_container_registry_conn_id"
    default_conn_name = "azure_container_registry_default"
    conn_type = "azure_container_registry"
    hook_name = "Azure Container Registry"

    @staticmethod
    @add_managed_identity_connection_widgets
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form."""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "subscription_id": StringField(
                lazy_gettext("Subscription ID (optional)"),
                widget=BS3TextFieldWidget(),
            ),
            "resource_group": StringField(
                lazy_gettext("Resource group name (optional)"),
                widget=BS3TextFieldWidget(),
            ),
        }

    @classmethod
    def get_ui_field_behaviour(cls) -> dict[str, Any]:
        """Returns custom field behaviour."""
        return {
            "hidden_fields": ["schema", "port", "extra"],
            "relabeling": {
                "login": "Registry Username",
                "password": "Registry Password",
                "host": "Registry Server",
            },
            "placeholders": {
                "login": "private registry username",
                "password": "private registry password",
                "host": "docker image registry server",
                "subscription_id": "Subscription id (required for Azure AD authentication)",
                "resource_group": "Resource group name (required for Azure AD authentication)",
            },
        }

    def __init__(self, conn_id: str = "azure_registry") -> None:
        super().__init__()
        self.conn_id = conn_id

    def _get_field(self, extras, name):
        return get_field(
            conn_id=self.conn_id,
            conn_type=self.conn_type,
            extras=extras,
            field_name=name,
        )

    @cached_property
    def connection(self) -> ImageRegistryCredential:
        return self.get_conn()

    def get_conn(self) -> ImageRegistryCredential:
        conn = self.get_connection(self.conn_id)
        password = conn.password
        if not password:
            extras = conn.extra_dejson
            subscription_id = self._get_field(extras, "subscription_id")
            resource_group = self._get_field(extras, "resource_group")
            managed_identity_client_id = self._get_field(extras, "managed_identity_client_id")
            workload_identity_tenant_id = self._get_field(extras, "workload_identity_tenant_id")
            credential = get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )
            client = ContainerRegistryManagementClient(
                credential=credential,
                subscription_id=subscription_id,
            )
            credentials = client.registries.list_credentials(resource_group, conn.login).as_dict()
            password = credentials["passwords"][0]["value"]

        return ImageRegistryCredential(server=conn.host, username=conn.login, password=password)
