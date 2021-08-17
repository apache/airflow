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
from unittest import mock
from unittest.mock import PropertyMock

from airflow.providers.google.cloud.hooks.spanner import SpannerHook
from tests.providers.google.cloud.utils.base_gcp_mock import (
    GCP_PROJECT_ID_HOOK_UNIT_TEST,
    mock_base_gcp_hook_default_project_id,
    mock_base_gcp_hook_no_default_project_id,
)

SPANNER_INSTANCE = 'instance'
SPANNER_CONFIGURATION = 'configuration'
SPANNER_DATABASE = 'database-name'


class TestGcpSpannerHookDefaultProjectId(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__',
            new=mock_base_gcp_hook_default_project_id,
        ):
            self.spanner_hook_default_project_id = SpannerHook(gcp_conn_id='test')

    @mock.patch(
        "airflow.providers.google.cloud.hooks.spanner.SpannerHook.client_info", new_callable=mock.PropertyMock
    )
    @mock.patch("airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_credentials")
    @mock.patch("airflow.providers.google.cloud.hooks.spanner.Client")
    def test_spanner_client_creation(self, mock_client, mock_get_creds, mock_client_info):
        result = self.spanner_hook_default_project_id._get_client(GCP_PROJECT_ID_HOOK_UNIT_TEST)
        mock_client.assert_called_once_with(
            project=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            credentials=mock_get_creds.return_value,
            client_info=mock_client_info.return_value,
        )
        assert mock_client.return_value == result
        assert self.spanner_hook_default_project_id._client == result

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_get_existing_instance(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        res = self.spanner_hook_default_project_id.get_instance(
            instance_id=SPANNER_INSTANCE, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        assert res is not None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_get_existing_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        res = self.spanner_hook_default_project_id.get_instance(
            instance_id=SPANNER_INSTANCE, project_id='new-project'
        )
        get_client.assert_called_once_with(project_id='new-project')
        instance_method.assert_called_once_with(instance_id='instance')
        assert res is not None

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_create_instance(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        create_method = instance_method.return_value.create
        create_method.return_value = False
        res = self.spanner_hook_default_project_id.create_instance(
            instance_id=SPANNER_INSTANCE,
            configuration_name=SPANNER_CONFIGURATION,
            node_count=1,
            display_name=SPANNER_DATABASE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(
            instance_id='instance',
            configuration_name='configuration',
            display_name='database-name',
            node_count=1,
        )
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_create_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        create_method = instance_method.return_value.create
        create_method.return_value = False
        res = self.spanner_hook_default_project_id.create_instance(
            project_id='new-project',
            instance_id=SPANNER_INSTANCE,
            configuration_name=SPANNER_CONFIGURATION,
            node_count=1,
            display_name=SPANNER_DATABASE,
        )
        get_client.assert_called_once_with(project_id='new-project')
        instance_method.assert_called_once_with(
            instance_id='instance',
            configuration_name='configuration',
            display_name='database-name',
            node_count=1,
        )
        assert res is None

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_update_instance(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        update_method = instance_method.return_value.update
        update_method.return_value = False
        res = self.spanner_hook_default_project_id.update_instance(
            instance_id=SPANNER_INSTANCE,
            configuration_name=SPANNER_CONFIGURATION,
            node_count=2,
            display_name=SPANNER_DATABASE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(
            instance_id='instance',
            configuration_name='configuration',
            display_name='database-name',
            node_count=2,
        )
        update_method.assert_called_once_with()
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_update_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        update_method = instance_method.return_value.update
        update_method.return_value = False
        res = self.spanner_hook_default_project_id.update_instance(
            project_id='new-project',
            instance_id=SPANNER_INSTANCE,
            configuration_name=SPANNER_CONFIGURATION,
            node_count=2,
            display_name=SPANNER_DATABASE,
        )
        get_client.assert_called_once_with(project_id='new-project')
        instance_method.assert_called_once_with(
            instance_id='instance',
            configuration_name='configuration',
            display_name='database-name',
            node_count=2,
        )
        update_method.assert_called_once_with()
        assert res is None

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_delete_instance(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        delete_method = instance_method.return_value.delete
        delete_method.return_value = False
        res = self.spanner_hook_default_project_id.delete_instance(
            instance_id=SPANNER_INSTANCE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with('instance')
        delete_method.assert_called_once_with()
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_delete_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        delete_method = instance_method.return_value.delete
        delete_method.return_value = False
        res = self.spanner_hook_default_project_id.delete_instance(
            project_id='new-project', instance_id=SPANNER_INSTANCE
        )
        get_client.assert_called_once_with(project_id='new-project')
        instance_method.assert_called_once_with('instance')
        delete_method.assert_called_once_with()
        assert res is None

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_get_database(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_exists_method = instance_method.return_value.exists
        database_exists_method.return_value = True
        res = self.spanner_hook_default_project_id.get_database(
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_exists_method.assert_called_once_with()
        assert res is not None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_get_database_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_exists_method = instance_method.return_value.exists
        database_exists_method.return_value = True
        res = self.spanner_hook_default_project_id.get_database(
            project_id='new-project', instance_id=SPANNER_INSTANCE, database_id=SPANNER_DATABASE
        )
        get_client.assert_called_once_with(project_id='new-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_exists_method.assert_called_once_with()
        assert res is not None

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_create_database(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_create_method = database_method.return_value.create
        res = self.spanner_hook_default_project_id.create_database(
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            ddl_statements=[],
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name', ddl_statements=[])
        database_create_method.assert_called_once_with()
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_create_database_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_create_method = database_method.return_value.create
        res = self.spanner_hook_default_project_id.create_database(
            project_id='new-project',
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            ddl_statements=[],
        )
        get_client.assert_called_once_with(project_id='new-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name', ddl_statements=[])
        database_create_method.assert_called_once_with()
        assert res is None

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_update_database(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_update_ddl_method = database_method.return_value.update_ddl
        res = self.spanner_hook_default_project_id.update_database(
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            ddl_statements=[],
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_update_ddl_method.assert_called_once_with(ddl_statements=[], operation_id=None)
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_update_database_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_update_ddl_method = database_method.return_value.update_ddl
        res = self.spanner_hook_default_project_id.update_database(
            project_id='new-project',
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            ddl_statements=[],
        )
        get_client.assert_called_once_with(project_id='new-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_update_ddl_method.assert_called_once_with(ddl_statements=[], operation_id=None)
        assert res is None

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_delete_database(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_drop_method = database_method.return_value.drop
        database_exists_method = database_method.return_value.exists
        database_exists_method.return_value = True
        res = self.spanner_hook_default_project_id.delete_database(
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_exists_method.assert_called_once_with()
        database_drop_method.assert_called_once_with()
        assert res

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_delete_database_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_drop_method = database_method.return_value.drop
        database_exists_method = database_method.return_value.exists
        database_exists_method.return_value = True
        res = self.spanner_hook_default_project_id.delete_database(
            project_id='new-project', instance_id=SPANNER_INSTANCE, database_id=SPANNER_DATABASE
        )
        get_client.assert_called_once_with(project_id='new-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_exists_method.assert_called_once_with()
        database_drop_method.assert_called_once_with()
        assert res

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=GCP_PROJECT_ID_HOOK_UNIT_TEST,
    )
    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_execute_dml(self, get_client, mock_project_id):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        run_in_transaction_method = database_method.return_value.run_in_transaction
        res = self.spanner_hook_default_project_id.execute_dml(
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            queries='',
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        run_in_transaction_method.assert_called_once_with(mock.ANY)
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_execute_dml_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        run_in_transaction_method = database_method.return_value.run_in_transaction
        res = self.spanner_hook_default_project_id.execute_dml(
            project_id='new-project', instance_id=SPANNER_INSTANCE, database_id=SPANNER_DATABASE, queries=''
        )
        get_client.assert_called_once_with(project_id='new-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        run_in_transaction_method.assert_called_once_with(mock.ANY)
        assert res is None


class TestGcpSpannerHookNoDefaultProjectID(unittest.TestCase):
    def setUp(self):
        with mock.patch(
            'airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__',
            new=mock_base_gcp_hook_no_default_project_id,
        ):
            self.spanner_hook_no_default_project_id = SpannerHook(gcp_conn_id='test')

    @mock.patch(
        "airflow.providers.google.cloud.hooks.spanner.SpannerHook.client_info", new_callable=mock.PropertyMock
    )
    @mock.patch(
        "airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_credentials",
        return_value="CREDENTIALS",
    )
    @mock.patch("airflow.providers.google.cloud.hooks.spanner.Client")
    def test_spanner_client_creation(self, mock_client, mock_get_creds, mock_client_info):
        result = self.spanner_hook_no_default_project_id._get_client(GCP_PROJECT_ID_HOOK_UNIT_TEST)
        mock_client.assert_called_once_with(
            project=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            credentials=mock_get_creds.return_value,
            client_info=mock_client_info.return_value,
        )
        assert mock_client.return_value == result
        assert self.spanner_hook_no_default_project_id._client == result

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_get_existing_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        res = self.spanner_hook_no_default_project_id.get_instance(
            instance_id=SPANNER_INSTANCE, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        assert res is not None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_get_non_existing_instance(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = False
        res = self.spanner_hook_no_default_project_id.get_instance(
            instance_id=SPANNER_INSTANCE, project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_create_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        create_method = instance_method.return_value.create
        create_method.return_value = False
        res = self.spanner_hook_no_default_project_id.create_instance(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=SPANNER_INSTANCE,
            configuration_name=SPANNER_CONFIGURATION,
            node_count=1,
            display_name=SPANNER_DATABASE,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(
            instance_id='instance',
            configuration_name='configuration',
            display_name='database-name',
            node_count=1,
        )
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_update_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        update_method = instance_method.return_value.update
        update_method.return_value = False
        res = self.spanner_hook_no_default_project_id.update_instance(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=SPANNER_INSTANCE,
            configuration_name=SPANNER_CONFIGURATION,
            node_count=2,
            display_name=SPANNER_DATABASE,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(
            instance_id='instance',
            configuration_name='configuration',
            display_name='database-name',
            node_count=2,
        )
        update_method.assert_called_once_with()
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_delete_instance_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        delete_method = instance_method.return_value.delete
        delete_method.return_value = False
        res = self.spanner_hook_no_default_project_id.delete_instance(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST, instance_id=SPANNER_INSTANCE
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with('instance')
        delete_method.assert_called_once_with()
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_get_database_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_exists_method = instance_method.return_value.exists
        database_exists_method.return_value = True
        res = self.spanner_hook_no_default_project_id.get_database(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_exists_method.assert_called_once_with()
        assert res is not None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_create_database_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_create_method = database_method.return_value.create
        res = self.spanner_hook_no_default_project_id.create_database(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            ddl_statements=[],
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name', ddl_statements=[])
        database_create_method.assert_called_once_with()
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_update_database_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_update_ddl_method = database_method.return_value.update_ddl
        res = self.spanner_hook_no_default_project_id.update_database(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            ddl_statements=[],
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_update_ddl_method.assert_called_once_with(ddl_statements=[], operation_id=None)
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_update_database_overridden_project_id_and_operation(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_update_ddl_method = database_method.return_value.update_ddl
        res = self.spanner_hook_no_default_project_id.update_database(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            operation_id="operation",
            ddl_statements=[],
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_update_ddl_method.assert_called_once_with(ddl_statements=[], operation_id="operation")
        assert res is None

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_delete_database_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_drop_method = database_method.return_value.drop
        database_exists_method = database_method.return_value.exists
        database_exists_method.return_value = True
        res = self.spanner_hook_no_default_project_id.delete_database(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_exists_method.assert_called_once_with()
        database_drop_method.assert_called_once_with()
        assert res

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_delete_database_missing_database(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        database_drop_method = database_method.return_value.drop
        database_exists_method = database_method.return_value.exists
        database_exists_method.return_value = False
        self.spanner_hook_no_default_project_id.delete_database(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        database_exists_method.assert_called_once_with()
        database_drop_method.assert_not_called()

    @mock.patch('airflow.providers.google.cloud.hooks.spanner.SpannerHook._get_client')
    def test_execute_dml_overridden_project_id(self, get_client):
        instance_method = get_client.return_value.instance
        instance_exists_method = instance_method.return_value.exists
        instance_exists_method.return_value = True
        database_method = instance_method.return_value.database
        run_in_transaction_method = database_method.return_value.run_in_transaction
        res = self.spanner_hook_no_default_project_id.execute_dml(
            project_id=GCP_PROJECT_ID_HOOK_UNIT_TEST,
            instance_id=SPANNER_INSTANCE,
            database_id=SPANNER_DATABASE,
            queries='',
        )
        get_client.assert_called_once_with(project_id='example-project')
        instance_method.assert_called_once_with(instance_id='instance')
        database_method.assert_called_once_with(database_id='database-name')
        run_in_transaction_method.assert_called_once_with(mock.ANY)
        assert res is None
