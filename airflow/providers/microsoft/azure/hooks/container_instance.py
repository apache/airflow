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
from __future__ import annotations

import warnings
from functools import cached_property
from typing import TYPE_CHECKING, Any, cast

from azure.common.client_factory import get_client_from_auth_file, get_client_from_json_dict
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.mgmt.containerinstance import ContainerInstanceManagementClient

from airflow.exceptions import AirflowException, AirflowProviderDeprecationWarning
from airflow.providers.microsoft.azure.hooks.base_azure import AzureBaseHook
from airflow.providers.microsoft.azure.utils import get_sync_default_azure_credential

if TYPE_CHECKING:
    from azure.mgmt.containerinstance.models import (
        ContainerGroup,
        ContainerPropertiesInstanceView,
        ContainerState,
        Event,
    )


class AzureContainerInstanceHook(AzureBaseHook):
    """
    A hook to communicate with Azure Container Instances.

    This hook requires a service principal in order to work.
    After creating this service principal
    (Azure Active Directory/App Registrations), you need to fill in the
    client_id (Application ID) as login, the generated password as password,
    and tenantId and subscriptionId in the extra's field as a json.

    :param azure_conn_id: :ref:`Azure connection id<howto/connection:azure>` of
        a service principal which will be used to start the container instance.
    """

    conn_name_attr = "azure_conn_id"
    default_conn_name = "azure_default"
    conn_type = "azure_container_instance"
    hook_name = "Azure Container Instance"

    def __init__(self, azure_conn_id: str = default_conn_name) -> None:
        super().__init__(sdk_client=ContainerInstanceManagementClient, conn_id=azure_conn_id)

    @cached_property
    def connection(self):
        return self.get_conn()

    def get_conn(self) -> Any:
        """
        Authenticates the resource using the connection id passed during init.

        :return: the authenticated client.
        """
        conn = self.get_connection(self.conn_id)
        tenant = conn.extra_dejson.get("tenantId")

        key_path = conn.extra_dejson.get("key_path")
        if key_path:
            if not key_path.endswith(".json"):
                raise AirflowException("Unrecognised extension for key file.")
            self.log.info("Getting connection using a JSON key file.")
            return get_client_from_auth_file(client_class=self.sdk_client, auth_path=key_path)

        key_json = conn.extra_dejson.get("key_json")
        if key_json:
            self.log.info("Getting connection using a JSON config.")
            return get_client_from_json_dict(client_class=self.sdk_client, config_dict=key_json)

        credential: ClientSecretCredential | DefaultAzureCredential
        if all([conn.login, conn.password, tenant]):
            self.log.info("Getting connection using specific credentials and subscription_id.")
            credential = ClientSecretCredential(
                client_id=conn.login, client_secret=conn.password, tenant_id=cast(str, tenant)
            )
        else:
            self.log.info("Using DefaultAzureCredential as credential")
            managed_identity_client_id = conn.extra_dejson.get("managed_identity_client_id")
            workload_identity_tenant_id = conn.extra_dejson.get("workload_identity_tenant_id")
            credential = get_sync_default_azure_credential(
                managed_identity_client_id=managed_identity_client_id,
                workload_identity_tenant_id=workload_identity_tenant_id,
            )

        subscription_id = cast(str, conn.extra_dejson.get("subscriptionId"))
        return ContainerInstanceManagementClient(
            credential=credential,
            subscription_id=subscription_id,
        )

    def create_or_update(self, resource_group: str, name: str, container_group: ContainerGroup) -> None:
        """
        Create a new container group.

        :param resource_group: the name of the resource group
        :param name: the name of the container group
        :param container_group: the properties of the container group
        """
        self.connection.container_groups.begin_create_or_update(resource_group, name, container_group)

    def get_state_exitcode_details(self, resource_group: str, name: str) -> tuple:
        """
        Get the state and exitcode of a container group.

        :param resource_group: the name of the resource group
        :param name: the name of the container group
        :return: A tuple with the state, exitcode, and details.
            If the exitcode is unknown 0 is returned.
        """
        warnings.warn(
            "get_state_exitcode_details() is deprecated. Related method is get_state()",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        cg_state = self.get_state(resource_group, name)
        container = cg_state.containers[0]
        instance_view: ContainerPropertiesInstanceView = container.instance_view  # type: ignore[assignment]
        c_state: ContainerState = instance_view.current_state  # type: ignore[assignment]
        return c_state.state, c_state.exit_code, c_state.detail_status

    def get_messages(self, resource_group: str, name: str) -> list:
        """
        Get the messages of a container group.

        :param resource_group: the name of the resource group
        :param name: the name of the container group
        :return: A list of the event messages
        """
        warnings.warn(
            "get_messages() is deprecated. Related method is get_state()",
            AirflowProviderDeprecationWarning,
            stacklevel=2,
        )
        cg_state = self.get_state(resource_group, name)
        container = cg_state.containers[0]
        instance_view: ContainerPropertiesInstanceView = container.instance_view  # type: ignore[assignment]
        events: list[Event] = instance_view.events  # type: ignore[assignment]
        return [event.message for event in events]

    def get_state(self, resource_group: str, name: str) -> ContainerGroup:
        """
        Get the state of a container group.

        :param resource_group: the name of the resource group
        :param name: the name of the container group
        :return: ContainerGroup
        """
        return self.connection.container_groups.get(resource_group, name)

    def get_logs(self, resource_group: str, name: str, tail: int = 1000) -> list:
        """
        Get the tail from logs of a container group.

        :param resource_group: the name of the resource group
        :param name: the name of the container group
        :param tail: the size of the tail
        :return: A list of log messages
        """
        logs = self.connection.containers.list_logs(resource_group, name, name, tail=tail)
        if logs.content is None:
            return [None]
        return logs.content.splitlines(True)

    def delete(self, resource_group: str, name: str) -> None:
        """
        Delete a container group.

        :param resource_group: the name of the resource group
        :param name: the name of the container group
        """
        self.connection.container_groups.begin_delete(resource_group, name)

    def exists(self, resource_group: str, name: str) -> bool:
        """
        Test if a container group exists.

        :param resource_group: the name of the resource group
        :param name: the name of the container group
        """
        for container in self.connection.container_groups.list_by_resource_group(resource_group):
            if container.name == name:
                return True
        return False

    def test_connection(self):
        """Test a configured Azure Container Instance connection."""
        try:
            # Attempt to list existing container groups under the configured subscription and retrieve the
            # first in the returned iterator. We need to _actually_ try to retrieve an object to properly
            # test the connection.
            next(self.connection.container_groups.list(), None)
        except Exception as e:
            return False, str(e)

        return True, "Successfully connected to Azure Container Instance."
