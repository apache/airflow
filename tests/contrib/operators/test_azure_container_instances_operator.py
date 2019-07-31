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
#

from collections import namedtuple
from airflow.exceptions import AirflowException
from airflow.contrib.operators.azure_container_instances_operator import AzureContainerInstancesOperator

from azure.mgmt.containerinstance.models import (ContainerState,
                                                 Event)

from tests.compat import mock

import unittest


def make_mock_cg(container_state, events=[]):
    """
    Make a mock Container Group as the underlying azure Models have read-only attributes
    See https://docs.microsoft.com/en-us/rest/api/container-instances/containergroups
    """
    instance_view_dict = {"current_state": container_state,
                          "events": events}
    instance_view = namedtuple("InstanceView",
                               instance_view_dict.keys())(*instance_view_dict.values())

    container_dict = {"instance_view": instance_view}
    container = namedtuple("Container", container_dict.keys())(*container_dict.values())

    container_g_dict = {"containers": [container]}
    container_g = namedtuple("ContainerGroup",
                             container_g_dict.keys())(*container_g_dict.values())
    return container_g


class TestACIOperator(unittest.TestCase):

    @mock.patch("airflow.contrib.operators."
                "azure_container_instances_operator.AzureContainerInstanceHook")
    def test_execute(self, aci_mock):
        expected_c_state = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg = make_mock_cg(expected_c_state)

        aci_mock.return_value.get_state.return_value = expected_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(ci_conn_id=None,
                                              registry_conn_id=None,
                                              resource_group='resource-group',
                                              name='container-name',
                                              image='container-image',
                                              region='region',
                                              task_id='task')
        aci.execute(None)

        self.assertEqual(aci_mock.return_value.create_or_update.call_count, 1)
        (called_rg, called_cn, called_cg), _ = \
            aci_mock.return_value.create_or_update.call_args

        self.assertEqual(called_rg, 'resource-group')
        self.assertEqual(called_cn, 'container-name')

        self.assertEqual(called_cg.location, 'region')
        self.assertEqual(called_cg.image_registry_credentials, None)
        self.assertEqual(called_cg.restart_policy, 'Never')
        self.assertEqual(called_cg.os_type, 'Linux')

        called_cg_container = called_cg.containers[0]
        self.assertEqual(called_cg_container.name, 'container-name')
        self.assertEqual(called_cg_container.image, 'container-image')

        self.assertEqual(aci_mock.return_value.delete.call_count, 1)

    @mock.patch("airflow.contrib.operators."
                "azure_container_instances_operator.AzureContainerInstanceHook")
    def test_execute_with_failures(self, aci_mock):
        expected_c_state = ContainerState(state='Terminated', exit_code=1, detail_status='test')
        expected_cg = make_mock_cg(expected_c_state)

        aci_mock.return_value.get_state.return_value = expected_cg
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(ci_conn_id=None,
                                              registry_conn_id=None,
                                              resource_group='resource-group',
                                              name='container-name',
                                              image='container-image',
                                              region='region',
                                              task_id='task')
        with self.assertRaises(AirflowException):
            aci.execute(None)

        self.assertEqual(aci_mock.return_value.delete.call_count, 1)

    @mock.patch("airflow.contrib.operators."
                "azure_container_instances_operator.AzureContainerInstanceHook")
    def test_execute_with_messages_logs(self, aci_mock):
        events = [Event(message="test"), Event(message="messages")]
        expected_c_state1 = ContainerState(state='Running', exit_code=0, detail_status='test')
        expected_cg1 = make_mock_cg(expected_c_state1, events)
        expected_c_state2 = ContainerState(state='Terminated', exit_code=0, detail_status='test')
        expected_cg2 = make_mock_cg(expected_c_state2, events)

        aci_mock.return_value.get_state.side_effect = [expected_cg1,
                                                       expected_cg2]
        aci_mock.return_value.get_logs.return_value = ["test", "logs"]
        aci_mock.return_value.exists.return_value = False

        aci = AzureContainerInstancesOperator(ci_conn_id=None,
                                              registry_conn_id=None,
                                              resource_group='resource-group',
                                              name='container-name',
                                              image='container-image',
                                              region='region',
                                              task_id='task')
        aci.execute(None)

        self.assertEqual(aci_mock.return_value.create_or_update.call_count, 1)
        self.assertEqual(aci_mock.return_value.get_state.call_count, 2)
        self.assertEqual(aci_mock.return_value.get_logs.call_count, 2)

        self.assertEqual(aci_mock.return_value.delete.call_count, 1)

    def test_name_checker(self):
        valid_names = ['test-dash', 'name-with-length---63' * 3]

        invalid_names = ['test_underscore',
                         'name-with-length---84' * 4,
                         'name-ending-with-dash-',
                         '-name-starting-with-dash']
        for name in invalid_names:
            with self.assertRaises(AirflowException):
                AzureContainerInstancesOperator._check_name(name)

        for name in valid_names:
            checked_name = AzureContainerInstancesOperator._check_name(name)
            self.assertEqual(checked_name, name)


if __name__ == '__main__':
    unittest.main()
