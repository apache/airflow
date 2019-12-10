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

# pylint: disable=too-many-lines

import json
import unittest

from googleapiclient.errors import HttpError
from parameterized import parameterized

from airflow.exceptions import AirflowException
from airflow.gcp.hooks.cloud_sql import CloudSqlDatabaseHook, CloudSqlHook
from airflow.models import Connection
from tests.compat import PropertyMock, mock
from tests.gcp.utils.base_gcp_mock import (
    mock_base_gcp_hook_default_project_id, mock_base_gcp_hook_no_default_project_id,
)


class TestGcpSqlHookDefaultProjectId(unittest.TestCase):

    def setUp(self):
        with mock.patch('airflow.gcp.hooks.base.CloudBaseHook.__init__',
                        new=mock_base_gcp_hook_default_project_id):
            self.cloudsql_hook = CloudSqlHook(api_version='v1', gcp_conn_id='test')

    def test_instance_import_exception(self):
        self.cloudsql_hook.get_conn = mock.Mock(
            side_effect=HttpError(resp={'status': '400'},
                                  content=b'Error content')
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
                                  content=b'Error content')
        )
        with self.assertRaises(AirflowException) as cm:
            self.cloudsql_hook.export_instance(
                instance='instance',
                body={})
        err = cm.exception
        self.assertIn("Exporting instance ", str(err))

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_instance(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.instances.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "instance"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.get_instance(
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEqual('instance', res['name'])
        get_method.assert_called_once_with(instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_instance_overridden_project_id(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.instances.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "instance"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.get_instance(
            project_id='new-project',
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEqual('instance', res['name'])
        get_method.assert_called_once_with(instance='instance', project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_database(self, wait_for_operation_to_complete, get_conn):
        get_method = get_conn.return_value.databases.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "database"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook.get_database(
            database='database',
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEqual('database', res['name'])
        get_method.assert_called_once_with(instance='instance',
                                           database='database',
                                           project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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
        self.assertEqual('database', res['name'])
        get_method.assert_called_once_with(instance='instance',
                                           database='database',
                                           project='new-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
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
        with mock.patch('airflow.gcp.hooks.base.CloudBaseHook.__init__',
                        new=mock_base_gcp_hook_no_default_project_id):
            self.cloudsql_hook_no_default_project_id = CloudSqlHook(api_version='v1', gcp_conn_id='test')

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_import_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_import_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_export_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_instance_export_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_instance_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        get_method = get_conn.return_value.instances.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "instance"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.get_instance(
            project_id='example-project',
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEqual('instance', res['name'])
        get_method.assert_called_once_with(instance='instance', project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_instance_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_instance_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_instance_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_instance_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_instance_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_instance_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_instance_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_database_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
        get_method = get_conn.return_value.databases.return_value.get
        execute_method = get_method.return_value.execute
        execute_method.return_value = {"name": "database"}
        wait_for_operation_to_complete.return_value = None
        res = self.cloudsql_hook_no_default_project_id.get_database(
            project_id='example-project',
            database='database',
            instance='instance')
        self.assertIsNotNone(res)
        self.assertEqual('database', res['name'])
        get_method.assert_called_once_with(instance='instance',
                                           database='database',
                                           project='example-project')
        execute_method.assert_called_once_with(num_retries=5)
        wait_for_operation_to_complete.assert_not_called()

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_get_database_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_database_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_create_database_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_database_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_patch_database_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_database_overridden_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=PropertyMock,
        return_value=None
    )
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook.get_conn')
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlHook._wait_for_operation_to_complete')
    def test_delete_database_missing_project_id(
        self, wait_for_operation_to_complete, get_conn, mock_project_id
    ):
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection')
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
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection')
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
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection')
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
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection')
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
    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection')
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

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection')
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
        with self.assertRaises(ValueError) as cm:
            hook.get_sqlproxy_runner()
        err = cm.exception
        self.assertIn('Proxy runner can only be retrieved in case of use_proxy = True', str(err))

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection')
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
        proxy_runner = hook.get_sqlproxy_runner()
        self.assertIsNotNone(proxy_runner)

    @mock.patch('airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection')
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
        connection = hook.create_connection()
        db_hook = hook.get_database_hook(connection=connection)
        self.assertIsNotNone(db_hook)


class TestCloudSqlDatabaseHook(unittest.TestCase):

    @mock.patch('airflow.contrib.hooks.gcp_sql_hook.CloudSqlDatabaseHook.get_connection')
    def setUp(self, m):
        super().setUp()

        self.sql_connection = Connection(
            conn_id='my_gcp_sql_connection',
            conn_type='gcpcloudsql',
            login='login',
            password='password',
            host='host',
            schema='schema',
            extra='{"database_type":"postgres", "location":"my_location", '
                  '"instance":"my_instance", "use_proxy": true, '
                  '"project_id":"my_project"}'
        )
        self.connection = Connection(
            conn_id='my_gcp_connection',
            conn_type='google_cloud_platform',
        )
        scopes = [
            "https://www.googleapis.com/auth/pubsub",
            "https://www.googleapis.com/auth/datastore",
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/devstorage.read_write",
            "https://www.googleapis.com/auth/logging.write",
            "https://www.googleapis.com/auth/cloud-platform",
        ]
        conn_extra = {
            "extra__google_cloud_platform__scope": ",".join(scopes),
            "extra__google_cloud_platform__project": "your-gcp-project",
            "extra__google_cloud_platform__key_path":
                '/var/local/google_cloud_default.json'
        }
        conn_extra_json = json.dumps(conn_extra)
        self.connection.set_extra(conn_extra_json)

        m.side_effect = [self.sql_connection, self.connection]
        self.db_hook = CloudSqlDatabaseHook(
            gcp_cloudsql_conn_id='my_gcp_sql_connection',
            gcp_conn_id='my_gcp_connection'
        )

    def test_get_sqlproxy_runner(self):
        self.db_hook._generate_connection_uri()
        sqlproxy_runner = self.db_hook.get_sqlproxy_runner()
        self.assertEqual(sqlproxy_runner.gcp_conn_id, self.connection.conn_id)
        project = self.sql_connection.extra_dejson['project_id']
        location = self.sql_connection.extra_dejson['location']
        instance = self.sql_connection.extra_dejson['instance']
        instance_spec = "{project}:{location}:{instance}".format(
            project=project, location=location, instance=instance
        )
        self.assertEqual(sqlproxy_runner.instance_specification, instance_spec)

    @mock.patch("airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection")
    def test_hook_with_not_too_long_unix_socket_path(self, get_connection):
        uri = "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&" \
              "instance=" \
              "test_db_with_longname_but_with_limit_of_UNIX_socket&" \
              "use_proxy=True&sql_proxy_use_tcp=False"
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSqlDatabaseHook()
        connection = hook.create_connection()
        self.assertEqual('postgres', connection.conn_type)
        self.assertEqual('testdb', connection.schema)

    @mock.patch("airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres(self, get_connection):
        uri = "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=False&use_ssl=False"
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSqlDatabaseHook()
        connection = hook.create_connection()
        self.assertEqual('postgres', connection.conn_type)
        self.assertEqual('127.0.0.1', connection.host)
        self.assertEqual(3200, connection.port)
        self.assertEqual('testdb', connection.schema)

    @mock.patch("airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres_ssl(self, get_connection):
        uri = "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=False&use_ssl=True&sslcert=/bin/bash&" \
              "sslkey=/bin/bash&sslrootcert=/bin/bash"
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSqlDatabaseHook()
        connection = hook.create_connection()
        self.assertEqual('postgres', connection.conn_type)
        self.assertEqual('127.0.0.1', connection.host)
        self.assertEqual(3200, connection.port)
        self.assertEqual('testdb', connection.schema)
        self.assertEqual('/bin/bash', connection.extra_dejson['sslkey'])
        self.assertEqual('/bin/bash', connection.extra_dejson['sslcert'])
        self.assertEqual('/bin/bash', connection.extra_dejson['sslrootcert'])

    @mock.patch("airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres_proxy_socket(self, get_connection):
        uri = "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=True&sql_proxy_use_tcp=False"
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSqlDatabaseHook()
        connection = hook.create_connection()
        self.assertEqual('postgres', connection.conn_type)
        self.assertIn('/tmp', connection.host)
        self.assertIn('example-project:europe-west1:testdb', connection.host)
        self.assertIsNone(connection.port)
        self.assertEqual('testdb', connection.schema)

    @mock.patch("airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_project_id_missing(self, get_connection):
        uri = "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&" \
              "location=europe-west1&instance=testdb&" \
              "use_proxy=False&use_ssl=False"
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSqlDatabaseHook()
        connection = hook.create_connection()
        self.assertEqual('mysql', connection.conn_type)
        self.assertEqual('127.0.0.1', connection.host)
        self.assertEqual(3200, connection.port)
        self.assertEqual('testdb', connection.schema)

    @mock.patch("airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_postgres_proxy_tcp(self, get_connection):
        uri = "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=postgres&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=True&sql_proxy_use_tcp=True"
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSqlDatabaseHook()
        connection = hook.create_connection()
        self.assertEqual('postgres', connection.conn_type)
        self.assertEqual('127.0.0.1', connection.host)
        self.assertNotEqual(3200, connection.port)
        self.assertEqual('testdb', connection.schema)

    @mock.patch("airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql(self, get_connection):
        uri = "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=False&use_ssl=False"
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSqlDatabaseHook()
        connection = hook.create_connection()
        self.assertEqual('mysql', connection.conn_type)
        self.assertEqual('127.0.0.1', connection.host)
        self.assertEqual(3200, connection.port)
        self.assertEqual('testdb', connection.schema)

    @mock.patch("airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql_ssl(self, get_connection):
        uri = "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=False&use_ssl=True&sslcert=/bin/bash&" \
              "sslkey=/bin/bash&sslrootcert=/bin/bash"
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSqlDatabaseHook()
        connection = hook.create_connection()
        self.assertEqual('mysql', connection.conn_type)
        self.assertEqual('127.0.0.1', connection.host)
        self.assertEqual(3200, connection.port)
        self.assertEqual('testdb', connection.schema)
        self.assertEqual('/bin/bash', json.loads(connection.extra_dejson['ssl'])['cert'])
        self.assertEqual('/bin/bash', json.loads(connection.extra_dejson['ssl'])['key'])
        self.assertEqual('/bin/bash', json.loads(connection.extra_dejson['ssl'])['ca'])

    @mock.patch("airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql_proxy_socket(self, get_connection):
        uri = "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=True&sql_proxy_use_tcp=False"
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSqlDatabaseHook()
        connection = hook.create_connection()
        self.assertEqual('mysql', connection.conn_type)
        self.assertEqual('localhost', connection.host)
        self.assertIn('/tmp', connection.extra_dejson['unix_socket'])
        self.assertIn('example-project:europe-west1:testdb',
                      connection.extra_dejson['unix_socket'])
        self.assertIsNone(connection.port)
        self.assertEqual('testdb', connection.schema)

    @mock.patch("airflow.gcp.hooks.cloud_sql.CloudSqlDatabaseHook.get_connection")
    def test_hook_with_correct_parameters_mysql_tcp(self, get_connection):
        uri = "gcpcloudsql://user:password@127.0.0.1:3200/testdb?database_type=mysql&" \
              "project_id=example-project&location=europe-west1&instance=testdb&" \
              "use_proxy=True&sql_proxy_use_tcp=True"
        get_connection.side_effect = [Connection(uri=uri)]
        hook = CloudSqlDatabaseHook()
        connection = hook.create_connection()
        self.assertEqual('mysql', connection.conn_type)
        self.assertEqual('127.0.0.1', connection.host)
        self.assertNotEqual(3200, connection.port)
        self.assertEqual('testdb', connection.schema)
