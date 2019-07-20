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

import unittest

from airflow.contrib.hooks.datastore_hook import DatastoreHook
from unittest.mock import call, patch

from tests.compat import mock


def mock_init(unused_self, unused_gcp_conn_id, unused_delegate_to=None):
    pass


class TestDatastoreHook(unittest.TestCase):

    def setUp(self):
        with patch('airflow.contrib.hooks.gcp_api_base_hook.GoogleCloudBaseHook.__init__', new=mock_init):
            self.datastore_hook = DatastoreHook()

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook._authorize')
    @patch('airflow.contrib.hooks.datastore_hook.build')
    def test_get_conn(self, mock_build, mock_authorize):
        conn = self.datastore_hook.get_conn()

        mock_build.assert_called_once_with('datastore', 'v1', http=mock_authorize.return_value,
                                           cache_discovery=False)
        self.assertEqual(conn, mock_build.return_value)
        self.assertEqual(conn, self.datastore_hook.connection)

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    def test_allocate_ids(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        partial_keys = []

        keys = self.datastore_hook.allocate_ids(partial_keys)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        allocate_ids = projects.return_value.allocateIds
        allocate_ids.assert_called_once_with(projectId=self.datastore_hook.project_id,
                                             body={'keys': partial_keys})
        execute = allocate_ids.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(keys, execute.return_value['keys'])

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    def test_begin_transaction(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value

        transaction = self.datastore_hook.begin_transaction()

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        begin_transaction = projects.return_value.beginTransaction
        begin_transaction.assert_called_once_with(projectId=self.datastore_hook.project_id, body={})
        execute = begin_transaction.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(transaction, execute.return_value['transaction'])

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    def test_commit(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        body = {'item': 'a'}

        resp = self.datastore_hook.commit(body)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        commit = projects.return_value.commit
        commit.assert_called_once_with(projectId=self.datastore_hook.project_id, body=body)
        execute = commit.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value)

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    def test_lookup(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        keys = []
        read_consistency = 'ENUM'
        transaction = 'transaction'

        resp = self.datastore_hook.lookup(keys, read_consistency, transaction)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        lookup = projects.return_value.lookup
        lookup.assert_called_once_with(projectId=self.datastore_hook.project_id,
                                       body={
                                           'keys': keys,
                                           'readConsistency': read_consistency,
                                           'transaction': transaction
                                       })
        execute = lookup.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value)

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    def test_rollback(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        transaction = 'transaction'

        self.datastore_hook.rollback(transaction)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        rollback = projects.return_value.rollback
        rollback.assert_called_once_with(projectId=self.datastore_hook.project_id,
                                         body={'transaction': transaction})
        execute = rollback.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    def test_run_query(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        body = {'item': 'a'}

        resp = self.datastore_hook.run_query(body)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        run_query = projects.return_value.runQuery
        run_query.assert_called_once_with(projectId=self.datastore_hook.project_id, body=body)
        execute = run_query.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value['batch'])

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    def test_get_operation(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        name = 'name'

        resp = self.datastore_hook.get_operation(name)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        operations = projects.return_value.operations
        operations.assert_called_once_with()
        get = operations.return_value.get
        get.assert_called_once_with(name=name)
        execute = get.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value)

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    def test_delete_operation(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        name = 'name'

        resp = self.datastore_hook.delete_operation(name)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        operations = projects.return_value.operations
        operations.assert_called_once_with()
        delete = operations.return_value.delete
        delete.assert_called_once_with(name=name)
        execute = delete.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value)

    @patch('airflow.contrib.hooks.datastore_hook.time.sleep')
    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_operation',
           side_effect=[
               {'metadata': {'common': {'state': 'PROCESSING'}}},
               {'metadata': {'common': {'state': 'NOT PROCESSING'}}}
           ])
    def test_poll_operation_until_done(self, mock_get_operation, mock_time_sleep):
        name = 'name'
        polling_interval_in_seconds = 10

        result = self.datastore_hook.poll_operation_until_done(name, polling_interval_in_seconds)

        mock_get_operation.assert_has_calls([call(name), call(name)])
        mock_time_sleep.assert_called_once_with(polling_interval_in_seconds)
        self.assertEqual(result, {'metadata': {'common': {'state': 'NOT PROCESSING'}}})

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    def test_export_to_storage_bucket(self, mock_get_conn):
        self.datastore_hook.admin_connection = mock_get_conn.return_value
        bucket = 'bucket'
        namespace = None
        entity_filter = {}
        labels = {}

        resp = self.datastore_hook.export_to_storage_bucket(bucket, namespace, entity_filter, labels)

        projects = self.datastore_hook.admin_connection.projects
        projects.assert_called_once_with()
        export = projects.return_value.export
        export.assert_called_once_with(projectId=self.datastore_hook.project_id,
                                       body={
                                           'outputUrlPrefix': 'gs://' + '/'.join(
                                               filter(None, [bucket, namespace])
                                           ),
                                           'entityFilter': entity_filter,
                                           'labels': labels,
                                       })
        execute = export.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value)

    @patch('airflow.contrib.hooks.datastore_hook.DatastoreHook.get_conn')
    def test_import_from_storage_bucket(self, mock_get_conn):
        self.datastore_hook.admin_connection = mock_get_conn.return_value
        bucket = 'bucket'
        file = 'file'
        namespace = None
        entity_filter = {}
        labels = {}

        resp = self.datastore_hook.import_from_storage_bucket(bucket, file, namespace, entity_filter, labels)

        projects = self.datastore_hook.admin_connection.projects
        projects.assert_called_once_with()
        import_ = projects.return_value.import_
        import_.assert_called_once_with(projectId=self.datastore_hook.project_id,
                                        body={
                                            'inputUrl': 'gs://' + '/'.join(
                                                filter(None, [bucket, namespace, file])
                                            ),
                                            'entityFilter': entity_filter,
                                            'labels': labels,
                                        })
        execute = import_.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value)
