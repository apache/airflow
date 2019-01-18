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

from googleapiclient.errors import HttpError

from airflow.contrib.hooks.gcp_sql_hook import CloudSqlHook, CloudSqlDatabaseHook
from airflow.exceptions import AirflowException
from airflow.models.connection import Connection
from tests.contrib.utils.base_gcp_mock import mock_base_gcp_hook_default_project_id, \
    mock_base_gcp_hook_no_default_project_id

from parameterized import parameterized

try:
    from unittest import mock
except ImportError:
    try:
        import mock
    except ImportError:
        mock = None


class TestGcpSqlHookDefaultProjectId(unittest.TestCase):

    def setUp(self):
        with mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_default_project_id):
            self.cloudsql_hook = CloudSqlHook(api_version='v1', gcp_conn_id='test')

    def test_instance_import_exception(self):
        self.cloudsql_hook.get_conn = mock.Mock(
            side_effect=HttpError(resp={'status': '400'},
                                  content='Error content'.encode('utf-8'))
        )
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook.import_instance(
                instance='instance',
                body={})
        err = cm.exception
        self.assertIn("Importing instance ", str(err))

    def test_instance_export_exception(self):
        self.cloudsql_hook.get_conn = mock.Mock(
            side_effect=HttpError(resp={'status': '400'},
                                  content='Error content'.encode('utf-8'))
        )
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook.export_instance(
                instance='instance',
                body={})
        err = cm.exception
        self.assertIn("Exporting instance ", str(err))

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_import(self, wait_for_operation_to_complete, get_conn):
        import_method = get_conn.return_value.instances.return_value.import_
        execute_method = import_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.import_instance(
            instance='instance',
            body={})
        self.assertIsNone(res)
        import_method.assert_called_once_with(body={}, instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(project_id='example-project',
                                                               operation_name='operation_id')

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_import_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        import_method = get_conn.return_value.instances.return_value.import_
        execute_method = import_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.import_instance(
            project_id='new-project',
            instance='instance',
            body={})
        self.assertIsNone(res)
        import_method.assert_called_once_with(body={}, instance='instance', project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(project_id='new-project',
                                                               operation_name='operation_id')

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_export(self, wait_for_operation_to_complete, get_conn):
        export_method = get_conn.return_value.instances.return_value.export
        execute_method = export_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.export_instance(
            instance='instance',
            body={})
        self.assertIsNone(res)
        export_method.assert_called_once_with(body={}, instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(project_id='example-project',
                                                               operation_name='operation_id')

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_export_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        export_method = get_conn.return_value.instances.return_value.export
        execute_method = export_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.export_instance(
            project_id='new-project',
            instance='instance',
            body={})
        self.assertIsNone(res)
        export_method.assert_called_once_with(body={}, instance='instance', project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(project_id='new-project',
                                                               operation_name='operation_id')

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_instance(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.instances.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "instance"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.get_instance(
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEquals('instance', res['name'])
        get_method.assert_called_once_with(instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.instances.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "instance"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.get_instance(
            project_id='new-project',
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEquals('instance', res['name'])
        get_method.assert_called_once_with(instance='instance', project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_instance(self, wait_for_operation_to_complete, get_conn):
        insert_method = get_conn.return_value.instances.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.create_instance(
            body={})
        self.assertIsNone(res)
        insert_method.assert_called_once_with(body={}, project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        insert_method = get_conn.return_value.instances.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.create_instance(
            project_id='new-project',
            body={})
        self.assertIsNone(res)
        insert_method.assert_called_once_with(body={}, project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='new-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_instance(self, wait_for_operation_to_complete, get_conn):
        patch_method = get_conn.return_value.instances.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.patch_instance(
            instance='instance',
            body={})
        self.assertIsNone(res)
        patch_method.assert_called_once_with(body={}, instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        patch_method = get_conn.return_value.instances.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.patch_instance(
            project_id='new-project',
            instance='instance',
            body={})
        self.assertIsNone(res)
        patch_method.assert_called_once_with(body={}, instance='instance', project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='new-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_instance(self, wait_for_operation_to_complete, get_conn):
        delete_method = get_conn.return_value.instances.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.delete_instance(
            instance='instance')
        self.assertIsNone(res)
        delete_method.assert_called_once_with(instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        delete_method = get_conn.return_value.instances.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.delete_instance(
            project_id='new-project',
            instance='instance')
        self.assertIsNone(res)
        delete_method.assert_called_once_with(instance='instance', project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='new-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_database(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.databases.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "database"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.get_database(
            database='database',
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEquals('database', res['name'])
        get_method.assert_called_once_with(instance='instance',
                                           database='database',
                                           project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_database_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.databases.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "database"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.get_database(
            project_id='new-project',
            database='database',
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEquals('database', res['name'])
        get_method.assert_called_once_with(instance='instance',
                                           database='database',
                                           project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_database(self, wait_for_operation_to_complete, get_conn):
        insert_method = get_conn.return_value.databases.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.create_database(
            instance='instance',
            body={})
        self.assertIsNone(res)
        insert_method.assert_called_once_with(body={}, instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_database_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        insert_method = get_conn.return_value.databases.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.create_database(
            project_id='new-project',
            instance='instance',
            body={})
        self.assertIsNone(res)
        insert_method.assert_called_once_with(body={}, instance='instance', project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='new-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_database(self, wait_for_operation_to_complete, get_conn):
        patch_method = get_conn.return_value.databases.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.patch_database(
            instance='instance',
            database='database',
            body={})
        self.assertIsNone(res)
        patch_method.assert_called_once_with(body={},
                                             database='database',
                                             instance='instance',
                                             project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_database_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        patch_method = get_conn.return_value.databases.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.patch_database(
            project_id='new-project',
            instance='instance',
            database='database',
            body={})
        self.assertIsNone(res)
        patch_method.assert_called_once_with(body={},
                                             database='database',
                                             instance='instance',
                                             project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='new-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_database(self, wait_for_operation_to_complete, get_conn):
        delete_method = get_conn.return_value.databases.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.delete_database(
            instance='instance',
            database='database')
        self.assertIsNone(res)
        delete_method.assert_called_once_with(database='database',
                                              instance='instance',
                                              project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_database_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        delete_method = get_conn.return_value.databases.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.delete_database(
            project_id='new-project',
            instance='instance',
            database='database')
        self.assertIsNone(res)
        delete_method.assert_called_once_with(database='database',
                                              instance='instance',
                                              project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='new-project'
        )


class TestGcpSqlHookNoDefaultProjectID(unittest.TestCase):
    def setUp(self):
        with mock.patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__',
                        new=mock_base_gcp_hook_no_default_project_id):
            self.cloudsql_hook_no_default_project_id = CloudSqlHook(api_version='v1', gcp_conn_id='test')

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_import_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        import_method = get_conn.return_value.instances.return_value.import_
        execute_method = import_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.import_instance(
            project_id='example-project',
            instance='instance',
            body={})
        self.assertIsNone(res)
        import_method.assert_called_once_with(body={}, instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(project_id='example-project',
                                                               operation_name='operation_id')

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_import_missing_project_id(self, wait_for_operation_to_complete, get_conn):
        import_method = get_conn.return_value.instances.return_value.import_
        execute_method = import_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook_no_default_project_id.import_instance(
                instance='instance',
                body={})
        import_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_export_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        export_method = get_conn.return_value.instances.return_value.export
        execute_method = export_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.export_instance(
            project_id='example-project',
            instance='instance',
            body={})
        self.assertIsNone(res)
        export_method.assert_called_once_with(body={}, instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(project_id='example-project',
                                                               operation_name='operation_id')

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_export_missing_project_id(self, wait_for_operation_to_complete, get_conn):
        export_method = get_conn.return_value.instances.return_value.export
        execute_method = export_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook_no_default_project_id.export_instance(
                instance='instance',
                body={})
        export_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.instances.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "instance"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.get_instance(
            project_id='example-project',
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEquals('instance', res['name'])
        get_method.assert_called_once_with(instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_instance_missing_project_id(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.instances.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "instance"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook_no_default_project_id.get_instance(
                instance='instance')
        get_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        insert_method = get_conn.return_value.instances.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.create_instance(
            project_id='example-project',
            body={})
        self.assertIsNone(res)
        insert_method.assert_called_once_with(body={}, project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_instance_missing_project_id(self, wait_for_operation_to_complete, get_conn):
        insert_method = get_conn.return_value.instances.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook_no_default_project_id.create_instance(
                body={})
        insert_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        patch_method = get_conn.return_value.instances.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.patch_instance(
            project_id='example-project',
            instance='instance',
            body={})
        self.assertIsNone(res)
        patch_method.assert_called_once_with(body={}, instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_instance_missing_project_id(self, wait_for_operation_to_complete, get_conn):
        patch_method = get_conn.return_value.instances.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook_no_default_project_id.patch_instance(
                instance='instance',
                body={})
        patch_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        delete_method = get_conn.return_value.instances.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.delete_instance(
            project_id='example-project',
            instance='instance')
        self.assertIsNone(res)
        delete_method.assert_called_once_with(instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_instance_missing_project_id(self, wait_for_operation_to_complete, get_conn):
        delete_method = get_conn.return_value.instances.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook_no_default_project_id.delete_instance(
                instance='instance')
        delete_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_database_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.databases.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "database"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.get_database(
            project_id='example-project',
            database='database',
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEquals('database', res['name'])
        get_method.assert_called_once_with(instance='instance',
                                           database='database',
                                           project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_database_missing_project_id(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.databases.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "database"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook_no_default_project_id.get_database(
                database='database',
                instance='instance')
        get_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_database_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        insert_method = get_conn.return_value.databases.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.create_database(
            project_id='example-project',
            instance='instance',
            body={})
        self.assertIsNone(res)
        insert_method.assert_called_once_with(body={}, instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_database_missing_project_id(self, wait_for_operation_to_complete, get_conn):
        insert_method = get_conn.return_value.databases.return_value.insert
        execute_method = insert_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook_no_default_project_id.create_database(
                instance='instance',
                body={})
        insert_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_database_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        patch_method = get_conn.return_value.databases.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.patch_database(
            project_id='example-project',
            instance='instance',
            database='database',
            body={})
        self.assertIsNone(res)
        patch_method.assert_called_once_with(body={},
                                             database='database',
                                             instance='instance',
                                             project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_database_missing_project_id(self, wait_for_operation_to_complete, get_conn):
        patch_method = get_conn.return_value.databases.return_value.patch
        execute_method = patch_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook_no_default_project_id.patch_database(
                instance='instance',
                database='database',
                body={})
        patch_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_database_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        delete_method = get_conn.return_value.databases.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.delete_database(
            project_id='example-project',
            instance='instance',
            database='database')
        self.assertIsNone(res)
        delete_method.assert_called_once_with(database='database',
                                              instance='instance',
                                              project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_called_once_with(
            operation_name='operation_id', project_id='example-project'
        )

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook.get_conn')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_database_missing_project_id(self, wait_for_operation_to_complete, get_conn):
        delete_method = get_conn.return_value.databases.return_value.delete
        execute_method = delete_method.return_value.execute
        execute_method.return_value = {"name": "operation_id"}
        wait_for_operation_to_complete.return_value = None
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook_no_default_project_id.delete_database(
                instance='instance',
                database='database')
        delete_method.assert_not_called()
        execute_method.assert_not_called()
        err = cm.exception
        self.assertIn("The project id must be passed", str(err))
        wait_for_operation_to_complete.assert_not_called()


class TestCloudsqlDatabaseHook(unittest.TestCase):

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_validate_ssl_certs_no_ssl(self, get_connection):
        connection = Connection()
        connection.set_extra(json.dumps({
            "location": "test",
            "instance": "instance",
            "database_type": "postgres"
        }))
        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        hook.validate_ssl_certs()

    @parameterized.expand([
        [{}],
        [{"sslcert": "cert_file.pem"}],
        [{"sslkey": "cert_key.pem"}],
        [{"sslrootcert": "root_cert_file.pem"}],
        [{"sslcert": "cert_file.pem", "sslkey": "cert_key.pem"}],
        [{"sslrootcert": "root_cert_file.pem", "sslkey": "cert_key.pem"}],
        [{"sslrootcert": "root_cert_file.pem", "sslcert": "cert_file.pem"}],
    ])
    @mock.patch('os.path.isfile')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_validate_ssl_certs_missing_cert_params(
            self, cert_dict, get_connection, mock_is_file):
        mock_is_file.side_effects = True
        connection = Connection()
        extras = {
            "location": "test",
            "instance": "instance",
            "database_type": "postgres",
            "use_ssl": "True"
        }
        extras.update(cert_dict)
        connection.set_extra(json.dumps(extras))

        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        with self.assertRaises(AirflowException) as cm:
            hook.validate_ssl_certs()
        err = cm.exception
        self.assertIn("SSL connections requires", str(err))

    @mock.patch('os.path.isfile')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_validate_ssl_certs_with_ssl(self, get_connection, mock_is_file):
        connection = Connection()
        mock_is_file.return_value = True
        connection.set_extra(json.dumps({
            "location": "test",
            "instance": "instance",
            "database_type": "postgres",
            "use_ssl": "True",
            "sslcert": "cert_file.pem",
            "sslrootcert": "rootcert_file.pem",
            "sslkey": "key_file.pem",
        }))
        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        hook.validate_ssl_certs()

    @mock.patch('os.path.isfile')
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_validate_ssl_certs_with_ssl_files_not_readable(
            self, get_connection, mock_is_file):
        connection = Connection()
        mock_is_file.return_value = False
        connection.set_extra(json.dumps({
            "location": "test",
            "instance": "instance",
            "database_type": "postgres",
            "use_ssl": "True",
            "sslcert": "cert_file.pem",
            "sslrootcert": "rootcert_file.pem",
            "sslkey": "key_file.pem",
        }))
        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        with self.assertRaises(AirflowException) as cm:
            hook.validate_ssl_certs()
        err = cm.exception
        self.assertIn("must be a readable file", str(err))

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_validate_socket_path_length_too_long(self, get_connection):
        connection = Connection()
        connection.set_extra(json.dumps({
            "location": "test",
            "instance": "very_long_instance_name_that_will_be_too_long_to_build_socket_length",
            "database_type": "postgres",
            "use_proxy": "True",
            "use_tcp": "False"
        }))
        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        with self.assertRaises(AirflowException) as cm:
            hook.validate_socket_path_length()
        err = cm.exception
        self.assertIn("The UNIX socket path length cannot exceed", str(err))

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_validate_socket_path_length_not_too_long(self, get_connection):
        connection = Connection()
        connection.set_extra(json.dumps({
            "location": "test",
            "instance": "short_instance_name",
            "database_type": "postgres",
            "use_proxy": "True",
            "use_tcp": "False"
        }))
        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        hook.validate_socket_path_length()

    @parameterized.expand([
        ["http://:password@host:80/database"],
        ["http://user:@host:80/database"],
        ["http://user:password@/database"],
        ["http://user:password@host:80/"],
        ["http://user:password@/"],
        ["http://host:80/database"],
        ["http://host:80/"],
    ])
    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_create_connection_missing_fields(self, uri, get_connection):
        connection = Connection()
        connection.parse_from_uri(uri)
        params = {
            "location": "test",
            "instance": "instance",
            "database_type": "postgres",
            'use_proxy': "True",
            'use_tcp': "False"
        }
        connection.set_extra(json.dumps(params))
        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        with self.assertRaises(AirflowException) as cm:
            hook.create_connection()
        err = cm.exception
        self.assertIn("needs to be set in connection", str(err))

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_create_delete_connection(self, get_connection):
        connection = Connection()
        connection.parse_from_uri("http://user:password@host:80/database")
        connection.set_extra(json.dumps({
            "location": "test",
            "instance": "instance",
            "database_type": "postgres"
        }))
        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        hook.create_connection()
        self.assertIsNotNone(hook.retrieve_connection())
        hook.delete_connection()
        self.assertIsNone(hook.retrieve_connection())

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_get_sqlproxy_runner_no_proxy(self, get_connection):
        connection = Connection()
        connection.parse_from_uri("http://user:password@host:80/database")
        connection.set_extra(json.dumps({
            "location": "test",
            "instance": "instance",
            "database_type": "postgres",
        }))
        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        hook.create_connection()
        try:
            with self.assertRaises(AirflowException) as cm:
                hook.get_sqlproxy_runner()
            err = cm.exception
            self.assertIn('Proxy runner can only be retrieved in case of use_proxy = True', str(err))
        finally:
            hook.delete_connection()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_get_sqlproxy_runner(self, get_connection):
        connection = Connection()
        connection.parse_from_uri("http://user:password@host:80/database")
        connection.set_extra(json.dumps({
            "location": "test",
            "instance": "instance",
            "database_type": "postgres",
            'use_proxy': "True",
            'use_tcp': "False"
        }))
        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        hook.create_connection()
        try:
            proxy_runner = hook.get_sqlproxy_runner()
            self.assertIsNotNone(proxy_runner)
        finally:
            hook.delete_connection()

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def test_cloudsql_database_hook_get_database_hook(self, get_connection):
        connection = Connection()
        connection.parse_from_uri("http://user:password@host:80/database")
        connection.set_extra(json.dumps({
            "location": "test",
            "instance": "instance",
            "database_type": "postgres",
        }))
        get_connection.return_value = connection
        hook = CloudSqlDatabaseHook(gcp_cloudsql_conn_id='cloudsql_connection',
                                    default_gcp_project_id='google_connection')
        hook.create_connection()
        try:
            db_hook = hook.get_database_hook()
            self.assertIsNotNone(db_hook)
        finally:
            hook.delete_connection()
