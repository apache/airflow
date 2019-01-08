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

from azure.mgmt.batchai import BatchAIManagementClient


class AzureBatchAIHook(BaseHook):
    """
    Interact with Azure Batch AI

    :param azure_batchai_conn_id: Reference to the Azure Batch AI connection
    :type azure_batchai_conn_id: str
    :param config_data: JSON Object with credential and subscription information
    :type config_data: str
    """

    def __init__(self, azure_batchai_conn_id='azure_batchai_default', config_data=None):
        self.conn_id = azure_batchai_conn_id
        self.connection = self.get_conn()
        self.configData = config_data
        self.credentials = None
        self.subscription_id = None

    def get_conn(self):
        try:
            conn = self.get_connection(self.conn_id)
            key_path = conn.extra_dejson.get('key_path', False)
            if key_path:
                if key_path.endswith('.json'):
                    self.log.info('Getting connection using a JSON key file.')
                    return get_client_from_auth_file(BatchAIManagementClient,
                                                     key_path)
                else:
                    raise AirflowException('Unrecognised extension for key file.')

            elif os.environ.get('AZURE_AUTH_LOCATION'):
                key_path = os.environ.get('AZURE_AUTH_LOCATION')
                if key_path.endswith('.json'):
                    self.log.info('Getting connection using a JSON key file.')
                    return get_client_from_auth_file(BatchAIManagementClient,
                                                     key_path)
                else:
                    raise AirflowException('Unrecognised extension for key file.')

            self.credentials = ServicePrincipalCredentials(
                client_id=self.configData['clientId'],
                secret=self.configData['clientSecret'],
                tenant=self.configData['tenantId']
            )
        except AirflowException:
            # No connection found
            pass

        return BatchAIManagementClient(self.credentials, self.configData['subscriptionId'])

    def create(self, resource_group, workspace_name, cluster_name, location, parameters):
        self.log.info("creating workspace.....")
        self.connection.workspaces._create_initial(resource_group,
                                                   workspace_name,
                                                   location)

        self.log.info("creating cluster.....")
        self.connection.clusters._create_initial(resource_group,
                                                 workspace_name,
                                                 cluster_name,
                                                 parameters)

    def update(self, resource_group, workspace_name, cluster_name):
        self.log.info("updating cluster.....")
        self.connection.clusters.update(resource_group,
                                        workspace_name,
                                        cluster_name)

    def get_state_exitcode(self, resource_group, workspace_name, cluster_name):
        response = self.connection.clusters.get(resource_group,
                                                workspace_name,
                                                cluster_name,
                                                raw=True).response.json()
        current_state = response['properties']['provisioningState']
        return current_state

    def get_messages(self, resource_group, workspace_name, cluster_name):
        response = self.connection.clusters.get(resource_group,
                                                workspace_name,
                                                cluster_name,
                                                raw=True).response.json()
        cluster = response['properties']['cluster']
        instance_view = cluster[0]['properties'].get('instanceView', {})
        return [event['message'] for event in instance_view.get('events', [])]

    def delete(self, resource_group, workspace_name, cluster_name):
        self.connection.clusters.delete(resource_group, workspace_name, cluster_name)
