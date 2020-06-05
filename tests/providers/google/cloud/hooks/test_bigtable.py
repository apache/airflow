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
import mock
from google.cloud.bigtable import Client
from google.cloud.bigtable.instance import Instance
from mock import PropertyMock

from airflow.providers.google.cloud.hooks.bigtable import BigtableHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST, mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

CBT_INSTANCE = 'instance'
CBT_CLUSTER = 'cluster'
CBT_ZONE = 'zone'
CBT_TABLE = 'table'


class TestBigtableHookNoDefaultProjectId(unittest.TestCase):

    def setUp(self):
        with mock.patch('airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__',
                        new=mock_base_gcp_hook_no_default_project_id):
            self.bigtable_hook_no_default_project_id = BigtableHook(gcp_conn_id='test')

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigtable.BigtableHook.client_info",
        new_callable=mock.PropertyMock
    )
    @mock.patch("airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_credentials")
    @mock.patch("airflow.providers.google.cloud.hooks.bigtable.Client")
    def test_bigtable_client_creation(self, mock_client, mock_get_creds, mock_client_info):
        result = self.bigtable_hook_no_default_project_id._get_client(GCP_PROJECT_ID_HOOK_UNIT_TEST)
        mock_client.assert_called_once_with(
            project=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            credentials=mock_get_creds.return_value,
            client_info=mock_client_info.return_value,
            admin=True
        )
        self.assertEqual(mock_client.return_value, result)
        self.assertEqual(self.bigtable_hook_no_default_project_id._client, result)

    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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

    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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

    @mock.patch('google.cloud.bigtable.instance.Instance.create')
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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

    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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
        with mock.patch('airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__',
                        new=mock_base_gcp_hook_default_project_id):
            self.bigtable_hook_default_project_id = BigtableHook(gcp_conn_id='test')

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigtable.BigtableHook.client_info",
        new_callable=mock.PropertyMock
    )
    @mock.patch("airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_credentials")
    @mock.patch("airflow.providers.google.cloud.hooks.bigtable.Client")
    def test_bigtable_client_creation(self, mock_client, mock_get_creds, mock_client_info):
        result = self.bigtable_hook_default_project_id._get_client(GCP_PROJECT_ID_HOOK_UNIT_TEST)
        mock_client.assert_called_once_with(
            project=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            credentials=mock_get_creds.return_value,
            client_info=mock_client_info.return_value,
            admin=True
        )
        self.assertEqual(mock_client.return_value, result)
        self.assertEqual(self.bigtable_hook_default_project_id._client, result)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
    def test_get_instance(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        res = self.bigtable_hook_default_project_id.get_instance(
            instance_id=CBT_INSTANCE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        get_client.assert_called_once_with(project_id='example-project')
        self.assertIsNotNone(res)

    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
    def test_get_instance_no_instance(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = False
        res = self.bigtable_hook_default_project_id.get_instance(
            instance_id=CBT_INSTANCE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        get_client.assert_called_once_with(project_id='example-project')
        self.assertIsNone(res)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
    def test_delete_instance(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        delete_method = instance_method.return_value.delete
        res = self.bigtable_hook_default_project_id.delete_instance(
            instance_id=CBT_INSTANCE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        delete_method.assert_called_once_with()
        get_client.assert_called_once_with(project_id='example-project')
        self.assertIsNone(res)

    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
    def test_delete_instance_no_instance(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = False
        delete_method = instance_method.return_value.delete
        self.bigtable_hook_default_project_id.delete_instance(
            instance_id=CBT_INSTANCE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        instance_method.assert_called_once_with('instance')
        instance_exists_method.assert_called_once_with()
        delete_method.assert_not_called()
        get_client.assert_called_once_with(project_id='example-project')

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('google.cloud.bigtable.instance.Instance.create')
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
    def test_create_instance(self, get_client, instance_create, mock_project_id):
        operation = mock.Mock()
        operation.result_return_value = Instance(instance_id=CBT_INSTANCE, client=get_client)
        instance_create.return_value = operation
        res = self.bigtable_hook_default_project_id.create_instance(
            instance_id=CBT_INSTANCE,
            main_cluster_id=CBT_CLUSTER,
            main_cluster_zone=CBT_ZONE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_create.assert_called_once_with(clusters=mock.ANY)
        self.assertEqual(res.instance_id, 'instance')

    @mock.patch('google.cloud.bigtable.instance.Instance.create')
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST
    )
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
    def test_delete_table(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        table_delete_method = instance_method.return_value.table.return_value.delete
        instance_exists_method.return_value = True
        self.bigtable_hook_default_project_id.delete_table(
            instance_id=CBT_INSTANCE,
            table_id=CBT_TABLE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_exists_method.assert_called_once_with()
        table_delete_method.assert_called_once_with()

    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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
    @mock.patch('airflow.providers.google.cloud.hooks.bigtable.BigtableHook._get_client')
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
