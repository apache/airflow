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

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.containerservice import ContainerServiceClient
from azure.mgmt.resource import ResourceManagementClient
from airflow.contrib.utils.aks_utils import load_json


class AzureKubernetesServiceHook(BaseHook):

    def __init__(self, conn_id=None):
        self.conn_id = conn_id
        self.connection = self.get_conn()
        self.configData = None
        self.credentials = None
        self.subscription_id = None
        self.clientId = None
        self.clientSecret = None

    def get_conn(self):
        if self.conn_id:
            conn = self.get_connection(self.conn_id)
            key_path = conn.extra_dejson.get('key_path', False)
            if key_path:
                if key_path.endswith('.json'):
                    self.log.info('Getting connection using a JSON key file.')

                    self.configData = load_json(self, key_path)
                else:
                    raise AirflowException('Unrecognised extension for key file.')

        if os.environ.get('AZURE_AUTH_LOCATION'):
            key_path = os.environ.get('AZURE_AUTH_LOCATION')
            if key_path.endswith('.json'):
                self.log.info('Getting connection using a JSON key file.')
                self.configData = load_json(self, key_path)
            else:
                raise AirflowException('Unrecognised extension for key file.')

        self.credentials = ServicePrincipalCredentials(
            client_id=self.configData['clientId'],
            secret=self.configData['clientSecret'],
            tenant=self.configData['tenantId']
        )

        self.subscription_id = self.configData['subscriptionId']
        self.clientId = self.configData["clientId"]
        self.clientSecret = self.configData["clientSecret"]
        return ContainerServiceClient(self.credentials, str(self.subscription_id))

    def check_resource(self, credentials, subscriptionId, resource_group):
        resource_grp = ResourceManagementClient(credentials, subscriptionId)
        return resource_grp.resource_groups.check_existence(resource_group)

    def create_resource(self, credentials, subscriptionId, resource_group, location):
        resource_grp = ResourceManagementClient(credentials, subscriptionId)
        return resource_grp.resource_groups.create_or_update(resource_group, location)
