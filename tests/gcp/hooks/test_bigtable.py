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

import google

from google.cloud.bigtable import Client
from google.cloud.bigtable.instance import Instance

from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_no_default_project_id, \
    mock_base_gcp_hook_default_project_id, GCP_PROJECT_ID_HOOK_UNIT_TEST
from tests.compat import mock, PropertyMock

from airflow import AirflowException
from airflow.gcp.hooks.bigtable import BigtableHook

CBT_INSTANCE = 'instance'
CBT_CLUSTER = 'cluster'
CBT_ZONE = 'zone'
CBT_TABLE = 'table'


class TestBigtableHookNoDefaultProjectId(unittest.TestCase):

    def setUp(self):
        with mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_no_default_project_id):
            self.bigtable_hook_no_default_project_id = BigtableHook(gcp_conn_id='test')

    @mock.patch(
        'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_get_instance_missing_project_id(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        with self.assertRaises(AirflowException) as cm:
            self.bigtable_hook_no_default_project_id.get_instance(instance_id=CBT_INSTANCE)
        instance_exists_method.assert_not_called()
        instance_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_get_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        res = self.bigtable_hook_no_default_project_id.get_instance(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=CBT_INSTANCE)
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        get_client.assert_called_once_with(project_id='example-project')
        self.assertIsNotNone(res)

    @mock.patch(
        'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_delete_instance_missing_project_id(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        delete_method = instance_method.return_value.delete
        instance_exists_method.return_value = True
        with self.assertRaises(AirflowException) as cm:
            self.bigtable_hook_no_default_project_id.delete_instance(instance_id=CBT_INSTANCE)
        instance_exists_method.assert_not_called()
        instance_method.assert_not_called()
        delete_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_delete_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        delete_method = instance_method.return_value.delete
        res = self.bigtable_hook_no_default_project_id.delete_instance(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST, instance_id=CBT_INSTANCE)
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        delete_method.assert_called_once_with()
        get_client.assert_called_once_with(project_id='example-project')
        self.assertIsNone(res)

    @mock.patch(
        'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('google.cloud.bigtable.instance.Instance.create')
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_create_instance_missing_project_id(self, get_client, instance_create, mock_project_id):
        operation = mock.Mock()
        operation.result_return_value = Instance(instance_id=CBT_INSTANCE, client=get_client)
        instance_create.return_value = operation
        with self.assertRaises(AirflowException) as cm:
            self.bigtable_hook_no_default_project_id.create_instance(
                instance_id=CBT_INSTANCE,
                main_cluster_id=CBT_CLUSTER,
                main_cluster_zone=CBT_ZONE)
        get_client.assert_not_called()
        instance_create.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))

    @mock.patch('google.cloud.bigtable.instance.Instance.create')
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_create_instance_overridden_project_id(self, get_client, instance_create):
        operation = mock.Mock()
        operation.result_return_value = Instance(instance_id=CBT_INSTANCE, client=get_client)
        instance_create.return_value = operation
        res = self.bigtable_hook_no_default_project_id.create_instance(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=CBT_INSTANCE,
            main_cluster_id=CBT_CLUSTER,
            main_cluster_zone=CBT_ZONE)
        get_client.assert_called_once_with(project_id='example-project')
        instance_create.assert_called_once_with(clusters=mock.ANY)
        self.assertEqual(res.instance_id, 'instance')

    @mock.patch(
        'airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_delete_table_missing_project_id(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        table_delete_method = instance_method.return_value.table.return_value.delete
        instance_exists_method.return_value = True
        with self.assertRaises(AirflowException) as cm:
            self.bigtable_hook_no_default_project_id.delete_table(
                instance_id=CBT_INSTANCE,
                table_id=CBT_TABLE)
        get_client.assert_not_called()
        instance_exists_method.assert_not_called()
        table_delete_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_delete_table_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        table_delete_method = instance_method.return_value.table.return_value.delete
        instance_exists_method.return_value = True
        self.bigtable_hook_no_default_project_id.delete_table(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=CBT_INSTANCE,
            table_id=CBT_TABLE)
        get_client.assert_called_once_with(project_id='example-project')
        instance_exists_method.assert_called_once_with()
        table_delete_method.assert_called_once_with()


class TestBigtableHookDefaultProjectId(unittest.TestCase):

    def setUp(self):
        with mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_default_project_id):
            self.bigtable_hook_default_project_id = BigtableHook(gcp_conn_id='test')

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_get_instance(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        res = self.bigtable_hook_default_project_id.get_instance(
            instance_id=CBT_INSTANCE)
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        get_client.assert_called_once_with(project_id='example-project')
        self.assertIsNotNone(res)

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_get_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        res = self.bigtable_hook_default_project_id.get_instance(
            project_id='new-project',
            instance_id=CBT_INSTANCE)
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        get_client.assert_called_once_with(project_id='new-project')
        self.assertIsNotNone(res)

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_get_instance_no_instance(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = False
        res = self.bigtable_hook_default_project_id.get_instance(
            instance_id=CBT_INSTANCE)
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        get_client.assert_called_once_with(project_id='example-project')
        self.assertIsNone(res)

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_delete_instance(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        delete_method = instance_method.return_value.delete
        res = self.bigtable_hook_default_project_id.delete_instance(
            instance_id=CBT_INSTANCE)
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        delete_method.assert_called_once_with()
        get_client.assert_called_once_with(project_id='example-project')
        self.assertIsNone(res)

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_delete_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        delete_method = instance_method.return_value.delete
        res = self.bigtable_hook_default_project_id.delete_instance(
            project_id='new-project', instance_id=CBT_INSTANCE)
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        delete_method.assert_called_once_with()
        get_client.assert_called_once_with(project_id='new-project')
        self.assertIsNone(res)

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_delete_instance_no_instance(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = False
        delete_method = instance_method.return_value.delete
        self.bigtable_hook_default_project_id.delete_instance(
            instance_id=CBT_INSTANCE)
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        delete_method.assert_not_called()
        get_client.assert_called_once_with(project_id='example-project')

    @mock.patch('google.cloud.bigtable.instance.Instance.create')
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_create_instance(self, get_client, instance_create):
        operation = mock.Mock()
        operation.result_return_value = Instance(instance_id=CBT_INSTANCE, client=get_client)
        instance_create.return_value = operation
        res = self.bigtable_hook_default_project_id.create_instance(
            instance_id=CBT_INSTANCE,
            main_cluster_id=CBT_CLUSTER,
            main_cluster_zone=CBT_ZONE)
        get_client.assert_called_once_with(project_id='example-project')
        instance_create.assert_called_once_with(clusters=mock.ANY)
        self.assertEqual(res.instance_id, 'instance')

    @mock.patch('google.cloud.bigtable.instance.Instance.create')
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_create_instance_overridden_project_id(self, get_client, instance_create):
        operation = mock.Mock()
        operation.result_return_value = Instance(instance_id=CBT_INSTANCE, client=get_client)
        instance_create.return_value = operation
        res = self.bigtable_hook_default_project_id.create_instance(
            project_id='new-project',
            instance_id=CBT_INSTANCE,
            main_cluster_id=CBT_CLUSTER,
            main_cluster_zone=CBT_ZONE)
        get_client.assert_called_once_with(project_id='new-project')
        instance_create.assert_called_once_with(clusters=mock.ANY)
        self.assertEqual(res.instance_id, 'instance')

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_delete_table(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        table_delete_method = instance_method.return_value.table.return_value.delete
        instance_exists_method.return_value = True
        self.bigtable_hook_default_project_id.delete_table(
            instance_id=CBT_INSTANCE,
            table_id=CBT_TABLE)
        get_client.assert_called_once_with(project_id='example-project')
        instance_exists_method.assert_called_once_with()
        table_delete_method.assert_called_once_with()

    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_delete_table_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        table_delete_method = instance_method.return_value.table.return_value.delete
        instance_exists_method.return_value = True
        self.bigtable_hook_default_project_id.delete_table(
            project_id='new-project',
            instance_id=CBT_INSTANCE,
            table_id=CBT_TABLE)
        get_client.assert_called_once_with(project_id='new-project')
        instance_exists_method.assert_called_once_with()
        table_delete_method.assert_called_once_with()

    @mock.patch('google.cloud.bigtable.table.Table.create')
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_create_table(self, get_client, create):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        client = mock.Mock(Client)
        instance = google.cloud.bigtable.instance.Instance(
            instance_id=CBT_INSTANCE,
            client=client)
        self.bigtable_hook_default_project_id.create_table(
            instance=instance,
            table_id=CBT_TABLE)
        get_client.assert_not_called()
        create.assert_called_once_with([], {})

    @mock.patch('google.cloud.bigtable.cluster.Cluster.update')
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_update_cluster(self, get_client, update):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        client = mock.Mock(Client)
        instance = google.cloud.bigtable.instance.Instance(
            instance_id=CBT_INSTANCE,
            client=client)
        self.bigtable_hook_default_project_id.update_cluster(
            instance=instance,
            cluster_id=CBT_CLUSTER,
            nodes=4)
        get_client.assert_not_called()
        update.assert_called_once_with()

    @mock.patch('google.cloud.bigtable.table.Table.list_column_families')
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_list_column_families(self, get_client, list_column_families):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        client = mock.Mock(Client)
        get_client.return_value = client
        instance = google.cloud.bigtable.instance.Instance(
            instance_id=CBT_INSTANCE,
            client=client)
        self.bigtable_hook_default_project_id.get_column_families_for_table(
            instance=instance, table_id=CBT_TABLE)
        get_client.assert_not_called()
        list_column_families.assert_called_once_with()

    @mock.patch('google.cloud.bigtable.table.Table.get_cluster_states')
    @mock.patch('airflow.gcp.hooks.bigtable.BigtableHook._get_client')
    def test_get_cluster_states(self, get_client, get_cluster_states):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        client = mock.Mock(Client)
        instance = google.cloud.bigtable.instance.Instance(
            instance_id=CBT_INSTANCE,
            client=client)
        self.bigtable_hook_default_project_id.get_cluster_states_for_table(
            instance=instance, table_id=CBT_TABLE)
        get_client.assert_not_called()
        get_cluster_states.assert_called_once_with()
