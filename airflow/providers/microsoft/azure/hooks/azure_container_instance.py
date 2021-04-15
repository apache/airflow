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
#

import warnings
from typing import Any, Dict

from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import ContainerGroup

from airflow.providers.microsoft.azure.hooks.base_azure import AzureBaseHook


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
    :type azure_conn_id: str
    """

    conn_name_attr = 'azure_conn_id'
    default_conn_name = 'azure_default'
    conn_type = 'azure_container_instances'
    hook_name = 'Azure Container Instance'

    @staticmethod
    def get_connection_form_widgets() -> Dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "extra__azure__tenantId": StringField(
                lazy_gettext('Azure Tenant ID'), widget=BS3TextFieldWidget()
            ),
            "extra__azure__subscriptionId": StringField(
                lazy_gettext('Azure Subscription ID'), widget=BS3TextFieldWidget()
            ),
        }

    @staticmethod
    def get_ui_field_behaviour() -> Dict:
        """Returns custom field behaviour"""
        import json

        return {
            "hidden_fields": ['schema', 'port', 'host'],
            "relabeling": {
                'login': 'Azure Client ID',
                'password': 'Azure Secret',
            },
            "placeholders": {
                'extra': json.dumps(
                    {
                        "key_path": "path to json file for auth",
                        "key_json": "specifies json dict for auth",
                    },
                    indent=1,
                ),
                'login': 'client id (token credentials auth)',
                'password': 'secret (token credentials auth)',
                'extra__azure__tenantId': 'tenant id (token credentials auth)',
                'extra__azure__subscriptionId': 'subscription id (token credentials auth)',
            },
        }

    def __init__(self, conn_id: str = default_conn_name) -> None:
        super().__init__(sdk_client=ContainerInstanceManagementClient, conn_id=conn_id)
        self.connection = self.get_conn()

    def create_or_update(self, resource_group: str, name: str, container_group: ContainerGroup) -> None:
        """
        Create a new container group

        :param resource_group: the name of the resource group
        :type resource_group: str
        :param name: the name of the container group
        :type name: str
        :param container_group: the properties of the container group
        :type container_group: azure.mgmt.containerinstance.models.ContainerGroup
        """
        self.connection.container_groups.create_or_update(resource_group, name, container_group)

    def get_state_exitcode_details(self, resource_group: str, name: str) -> tuple:
        """
        Get the state and exitcode of a container group

        :param resource_group: the name of the resource group
        :type resource_group: str
        :param name: the name of the container group
        :type name: str
        :return: A tuple with the state, exitcode, and details.
            If the exitcode is unknown 0 is returned.
        :rtype: tuple(state,exitcode,details)
        """
        warnings.warn(
            "get_state_exitcode_details() is deprecated. Related method is get_state()",
            DeprecationWarning,
            stacklevel=2,
        )
        cg_state = self.get_state(resource_group, name)
        c_state = cg_state.containers[0].instance_view.current_state
        return (c_state.state, c_state.exit_code, c_state.detail_status)

    def get_messages(self, resource_group: str, name: str) -> list:
        """
        Get the messages of a container group

        :param resource_group: the name of the resource group
        :type resource_group: str
        :param name: the name of the container group
        :type name: str
        :return: A list of the event messages
        :rtype: list[str]
        """
        warnings.warn(
            "get_messages() is deprecated. Related method is get_state()", DeprecationWarning, stacklevel=2
        )
        cg_state = self.get_state(resource_group, name)
        instance_view = cg_state.containers[0].instance_view
        return [event.message for event in instance_view.events]

    def get_state(self, resource_group: str, name: str) -> Any:
        """
        Get the state of a container group

        :param resource_group: the name of the resource group
        :type resource_group: str
        :param name: the name of the container group
        :type name: str
        :return: ContainerGroup
        :rtype: ~azure.mgmt.containerinstance.models.ContainerGroup
        """
        return self.connection.container_groups.get(resource_group, name, raw=False)

    def get_logs(self, resource_group: str, name: str, tail: int = 1000) -> list:
        """
        Get the tail from logs of a container group

        :param resource_group: the name of the resource group
        :type resource_group: str
        :param name: the name of the container group
        :type name: str
        :param tail: the size of the tail
        :type tail: int
        :return: A list of log messages
        :rtype: list[str]
        """
        logs = self.connection.container.list_logs(resource_group, name, name, tail=tail)
        return logs.content.splitlines(True)

    def delete(self, resource_group: str, name: str) -> None:
        """
        Delete a container group

        :param resource_group: the name of the resource group
        :type resource_group: str
        :param name: the name of the container group
        :type name: str
        """
        self.connection.container_groups.delete(resource_group, name)

    def exists(self, resource_group: str, name: str) -> bool:
        """
        Test if a container group exists

        :param resource_group: the name of the resource group
        :type resource_group: str
        :param name: the name of the container group
        :type name: str
        """
        for container in self.connection.container_groups.list_by_resource_group(resource_group):
            if container.name == name:
                return True
        return False
