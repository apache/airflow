# -*- coding: utf-8 -*-
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

from airflow.hooks.base_hook import BaseHook
from azure.mgmt.containerinstance.models import ImageRegistryCredential


class AzureContainerRegistryHook(BaseHook):
    """
    A hook to communicate with a Azure Container Registry.

    :param conn_id: connection id of a service principal which will be used
        to start the container instance
    :type conn_id: str
    """

    def __init__(self, conn_id='azure_registry'):
        self.conn_id = conn_id
        self.connection = self.get_conn()

    def get_conn(self):
        conn = self.get_connection(self.conn_id)
        return ImageRegistryCredential(server=conn.host, username=conn.login, password=conn.password)
