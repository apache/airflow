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
"""Hook for Azure Container Registry"""
from __future__ import annotations

from typing import Any

from azure.mgmt.containerinstance.models import ImageRegistryCredential

from airflow.hooks.base import BaseHook


class AzureContainerRegistryHook(BaseHook):
    """
    A hook to communicate with a Azure Container Registry.

    :param conn_id: :ref:`Azure Container Registry connection id<howto/connection:acr>`
        of a service principal which will be used to start the container instance

    """

    conn_name_attr = 'azure_container_registry_conn_id'
    default_conn_name = 'azure_container_registry_default'
    conn_type = 'azure_container_registry'
    hook_name = 'Azure Container Registry'

    @staticmethod
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ['schema', 'port', 'extra'],
            "relabeling": {
                'login': 'Registry Username',
                'password': 'Registry Password',
                'host': 'Registry Server',
            },
            "placeholders": {
                'login': 'private registry username',
                'password': 'private registry password',
                'host': 'docker image registry server',
            },
        }

    def __init__(self, conn_id: str = 'azure_registry') -> None:
        super().__init__()
        self.conn_id = conn_id
        self.connection = self.get_conn()

    def get_conn(self) -> ImageRegistryCredential:
        conn = self.get_connection(self.conn_id)
        return ImageRegistryCredential(server=conn.host, username=conn.login, password=conn.password)
