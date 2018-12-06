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

import unittest

from airflow.contrib.operators.aks_operator import AzureKubernetesOperator
from msrestazure.azure_exceptions import CloudError

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class AzureKubernetesKubernetesOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.aks_operator.AzureKubernetesServiceHook')
    def test_execute_existing_kubernetes(self, aks_hook_mock):

        aks = AzureKubernetesOperator(ci_conn_id=None,
                                      resource_group="resource_group",
                                      name="name",
                                      ssh_key_value=None,
                                      dns_name_prefix=None,
                                      location="location",
                                      tags=None,
                                      task_id='task')

        client_hook = aks_hook_mock.return_value.get_conn.return_value

        aks.execute(None)

        client_hook.managed_clusters.get.assert_called_once_with('resource_group', 'name')
        self.assertEqual(client_hook.managed_clusters.create_or_update.call_count, 0)

    @mock.patch('airflow.contrib.operators.aks_operator.AzureKubernetesServiceHook')
    def test_execute_create_kubernetes(self, aks_hook_mock):

        aks = AzureKubernetesOperator(ci_conn_id=None,
                                      resource_group="resource_group",
                                      name="name",
                                      ssh_key_value=None,
                                      dns_name_prefix=None,
                                      location="location",
                                      tags=None,
                                      task_id='task')

        client_hook = aks_hook_mock.return_value.get_conn.return_value

        resp = mock.MagicMock()
        resp.status_code = 404
        resp.text = '{"Message": "The Resource Microsoft.ContainerService/managedClusters/name under resource group \
        resource_group was not found."}'

        client_hook.managed_clusters.get.side_effect = CloudError(resp, error="Not found")

        aks.execute(None)

        self.assertEqual(client_hook.managed_clusters.get.call_count, 1)
        self.assertEqual(client_hook.managed_clusters.create_or_update.call_count, 1)
