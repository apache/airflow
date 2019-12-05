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

import json
import unittest
from mock import patch

from airflow.models import Connection
from airflow.contrib.hooks.azure_container_instance_hook import AzureContainerInstanceHook
from airflow.utils import db

from azure.mgmt.containerinstance.models import (Container,
                                                 ContainerGroup,
                                                 Logs,
                                                 ResourceRequests,
                                                 ResourceRequirements)


class TestAzureContainerInstanceHook(unittest.TestCase):

    def setUp(self):
        db.merge_conn(
            Connection(
                conn_id='azure_container_instance_test',
                conn_type='azure_container_instances',
                login='login',
                password='key',
                extra=json.dumps({'tenantId': 'tenant_id',
                                  'subscriptionId': 'subscription_id'})
            )
        )

        self.resources = ResourceRequirements(requests=ResourceRequests(
            memory_in_gb='4',
            cpu='1'))
        with patch('azure.common.credentials.ServicePrincipalCredentials.__init__',
                   autospec=True, return_value=None):
            with patch('azure.mgmt.containerinstance.ContainerInstanceManagementClient'):
                self.testHook = AzureContainerInstanceHook(conn_id='azure_container_instance_test')

    @patch('azure.mgmt.containerinstance.models.ContainerGroup')
    @patch('azure.mgmt.containerinstance.operations.ContainerGroupsOperations.create_or_update')
    def test_create_or_update(self, create_or_update_mock, container_group_mock):
        self.testHook.create_or_update('resource_group', 'aci-test', container_group_mock)
        create_or_update_mock.assert_called_with('resource_group', 'aci-test', container_group_mock)

    @patch('azure.mgmt.containerinstance.operations.ContainerGroupsOperations.get')
    def test_get_state(self, get_state_mock):
        self.testHook.get_state('resource_group', 'aci-test')
        get_state_mock.assert_called_with('resource_group', 'aci-test', raw=False)

    @patch('azure.mgmt.containerinstance.operations.ContainerOperations.list_logs')
    def test_get_logs(self, list_logs_mock):
        expected_messages = ['log line 1\n', 'log line 2\n', 'log line 3\n']
        logs = Logs(content=''.join(expected_messages))
        list_logs_mock.return_value = logs

        logs = self.testHook.get_logs('resource_group', 'name', 'name')

        self.assertSequenceEqual(logs, expected_messages)

    @patch('azure.mgmt.containerinstance.operations.ContainerGroupsOperations.delete')
    def test_delete(self, delete_mock):
        self.testHook.delete('resource_group', 'aci-test')
        delete_mock.assert_called_with('resource_group', 'aci-test')

    @patch('azure.mgmt.containerinstance.operations.ContainerGroupsOperations.list_by_resource_group')
    def test_exists_with_existing(self, list_mock):
        list_mock.return_value = [ContainerGroup(os_type='Linux',
                                                 containers=[Container(name='test1',
                                                                       image='hello-world',
                                                                       resources=self.resources)])]
        self.assertFalse(self.testHook.exists('test', 'test1'))

    @patch('azure.mgmt.containerinstance.operations.ContainerGroupsOperations.list_by_resource_group')
    def test_exists_with_not_existing(self, list_mock):
        list_mock.return_value = [ContainerGroup(os_type='Linux',
                                                 containers=[Container(name='test1',
                                                                       image='hello-world',
                                                                       resources=self.resources)])]
        self.assertFalse(self.testHook.exists('test', 'not found'))
