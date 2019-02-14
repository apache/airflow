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
"""
AKS Hook that can create a cluster for Kubernetes hosted in Azure
"""

import os

from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.containerservice import ContainerServiceClient
from azure.mgmt.resource import ResourceManagementClient
from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.contrib.utils.aks_utils import load_json


class AzureKubernetesServiceHook(BaseHook):
    """
    Azure Kubernetes Service (AKS) hook simplifies the deployment and operations of Kubernetes
    and enables you to dynamically scale your application infrastructure.
    """

    def __init__(self, conn_id='azure_default'):
        self.conn_id = conn_id
        self.connection = self.get_conn()
        self.config_data = None
        self.credentials = None
        self.subscription_id = None
        self.client_id = None
        self.client_secret = None

    def get_conn(self):
        if self.conn_id:
            conn = self.get_connection(self.conn_id)
            key_path = conn.extra_dejson.get('key_path', False)
            if key_path:
                if key_path.endswith('.json'):
                    self.log.info('Getting connection using a JSON key file.')

                    self.config_data = load_json(key_path)
                else:
                    raise AirflowException('Unrecognised extension for key file.')

        if os.environ.get('AIRFLOW_CONN_AZURE_DEFAULT'):
            key_path = os.environ.get('AIRFLOW_CONN_AZURE_DEFAULT')
            if key_path.endswith('.json'):
                self.log.info('Getting connection using a JSON key file.')
                self.config_data = load_json(key_path)
            else:
                raise AirflowException('Unrecognised extension for key file.')

        self.credentials = ServicePrincipalCredentials(
            client_id=self.config_data['clientId'],
            secret=self.config_data['clientSecret'],
            tenant=self.config_data['tenantId']
        )

        self.subscription_id = self.config_data['subscriptionId']
        self.client_id = self.config_data["clientId"]
        self.client_secret = self.config_data["clientSecret"]
        return ContainerServiceClient(self.credentials, str(self.subscription_id))

    def check_resource(self, credentials, subscription_id, resource_group):
        """
        Check that the Azure Resource exists in the given subscription and resource group
        """
        resource_grp = ResourceManagementClient(credentials, subscription_id)
        return resource_grp.resource_groups.check_existence(resource_group)

    def create_resource(self, credentials, subscription_id, resource_group, location):
        """
        Create the AKS cluster
        """
        resource_grp = ResourceManagementClient(credentials, subscription_id)
        return resource_grp.resource_groups.create_or_update(resource_group, location)
