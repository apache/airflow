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

from airflow.exceptions import AirflowException
from airflow.contrib.operators.azure_container_instances_operator import AzureContainerInstancesOperator

import unittest

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestACIOperator(unittest.TestCase):

    @mock.patch("airflow.contrib.operators."
                "azure_container_instances_operator.AzureContainerInstanceHook")
    def test_execute(self, aci_mock):
        aci_mock.return_value.get_state_exitcode_details.return_value = "Terminated", 0, "test"
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
        aci_mock.return_value.get_state_exitcode_details.return_value = "Terminated", 1, "test"
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
        aci_mock.return_value.get_state_exitcode_details.side_effect = [("Running", 0, "test"),
                                                                        ("Terminated", 0, "test")]
        aci_mock.return_value.get_messages.return_value = ["test", "messages"]
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
        self.assertEqual(aci_mock.return_value.get_state_exitcode_details.call_count, 2)
        self.assertEqual(aci_mock.return_value.get_messages.call_count, 2)
        self.assertEqual(aci_mock.return_value.get_logs.call_count, 2)

        self.assertEqual(aci_mock.return_value.delete.call_count, 1)


if __name__ == '__main__':
    unittest.main()
