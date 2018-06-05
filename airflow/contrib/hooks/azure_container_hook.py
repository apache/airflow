
# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import os

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException

from azure.common.client_factory import get_client_from_auth_file
from azure.common.credentials import ServicePrincipalCredentials

from azure.mgmt.containerinstance import ContainerInstanceManagementClient
from azure.mgmt.containerinstance.models import (ImageRegistryCredential,
                                                 Volume,
                                                 AzureFileVolume)


class AzureContainerInstanceHook(BaseHook):

    def __init__(self, conn_id='azure_default'):
        self.conn_id = conn_id
        self.connection = self.get_conn()

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        key_path = conn.extra_dejson.get('key_path', False)
        if key_path:
            if key_path.endswith('.json'):
                self.log.info('Getting connection using a JSON key file.')
                return get_client_from_auth_file(ContainerInstanceManagementClient,
                                                 key_path)
            else:
                raise AirflowException('Unrecognised extension for key file.')

        if os.environ.get('AZURE_AUTH_LOCATION'):
            key_path = os.environ.get('AZURE_AUTH_LOCATION')
            if key_path.endswith('.json'):
                self.log.info('Getting connection using a JSON key file.')
                return get_client_from_auth_file(ContainerInstanceManagementClient,
                                                 key_path)
            else:
                raise AirflowException('Unrecognised extension for key file.')

        credentials = ServicePrincipalCredentials(
            client_id=conn.login,
            secret=conn.password,
            tenant=conn.extra_dejson['tenantId']
        )

        subscription_id = conn.extra_dejson['subscriptionId']
        return ContainerInstanceManagementClient(credentials, str(subscription_id))

    def create_or_update(self, resource_group, name, container_group):
        self.connection.container_groups.create_or_update(resource_group,
                                                          name,
                                                          container_group)

    def get_state_exitcode(self, resource_group, name):
        response = self.connection.container_groups.get(resource_group,
                                                        name,
                                                        raw=True).response.json()
        containers = response['properties']['containers']
        instance_view = containers[0]['properties'].get('instanceView', {})
        current_state = instance_view.get('currentState', {})

        return current_state.get('state'), current_state.get('exitCode', 0)

    def get_messages(self, resource_group, name):
        response = self.connection.container_groups.get(resource_group,
                                                        name,
                                                        raw=True).response.json()
        containers = response['properties']['containers']
        instance_view = containers[0]['properties'].get('instanceView', {})

        return [event['message'] for event in instance_view.get('events', [])]

    def get_logs(self, resource_group, name, tail=1000):
        logs = self.connection.container_logs.list(resource_group, name, name, tail=tail)
        return logs.content.splitlines(True)

    def delete(self, resource_group, name):
        self.connection.container_groups.delete(resource_group, name)


class AzureContainerRegistryHook(BaseHook):

    def __init__(self, conn_id='azure_registry'):
        self.conn_id = conn_id
        self.connection = self.get_conn()

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        return ImageRegistryCredential(conn.host, conn.login, conn.password)


class AzureContainerVolumeHook(BaseHook):

    def __init__(self, wasb_conn_id='wasb_default'):
        self.conn_id = wasb_conn_id

    def get_storagekey(self):
        conn = self.get_connection(self.conn_id)
        service_options = conn.extra_dejson

        if 'connection_string' in service_options:
            for keyvalue in service_options['connection_string'].split(";"):
                key, value = keyvalue.split("=", 1)
                if key == "AccountKey":
                    return value
        return conn.password

    def get_file_volume(self, mount_name, share_name,
                        storage_account_name, read_only=False):
        return Volume(mount_name,
                      AzureFileVolume(share_name, storage_account_name,
                                      read_only, self.get_storagekey()))
