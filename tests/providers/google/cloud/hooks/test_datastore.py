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
from unittest.mock import call, patch

import mock

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.hooks.datastore import DatastoreHook

GCP_PROJECT_ID = "test"


def mock_init(self, gcp_conn_id, delegate_to=None):  # pylint: disable=unused-argument
    pass


class TestDatastoreHook(unittest.TestCase):
    def setUp(self):
        with patch('airflow.providers.google.cloud.hooks.base.CloudBaseHook.__init__', new=mock_init):
            self.datastore_hook = DatastoreHook()

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook._authorize')
    @patch('airflow.providers.google.cloud.hooks.datastore.build')
    def test_get_conn(self, mock_build, mock_authorize):
        conn = self.datastore_hook.get_conn()

        mock_build.assert_called_once_with('datastore', 'v1', http=mock_authorize.return_value,
                                           cache_discovery=False)
        self.assertEqual(conn, mock_build.return_value)
        self.assertEqual(conn, self.datastore_hook.connection)

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_allocate_ids(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        partial_keys = []

        keys = self.datastore_hook.allocate_ids(partial_keys=partial_keys, project_id=GCP_PROJECT_ID)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        allocate_ids = projects.return_value.allocateIds
        allocate_ids.assert_called_once_with(projectId=GCP_PROJECT_ID,
                                             body={'keys': partial_keys})
        execute = allocate_ids.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(keys, execute.return_value['keys'])

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.project_id',
           new_callable=mock.PropertyMock, return_value=None)
    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_allocate_ids_no_project_id(self, mock_get_conn, mock_project_id):
        self.datastore_hook.connection = mock_get_conn.return_value
        partial_keys = []

        with self.assertRaises(AirflowException) as err:
            self.datastore_hook.allocate_ids(partial_keys=partial_keys)
        self.assertIn("project_id", str(err.exception))

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_begin_transaction(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value

        transaction = self.datastore_hook.begin_transaction(project_id=GCP_PROJECT_ID)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        begin_transaction = projects.return_value.beginTransaction
        begin_transaction.assert_called_once_with(projectId=GCP_PROJECT_ID, body={})
        execute = begin_transaction.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(transaction, execute.return_value['transaction'])

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.project_id',
           new_callable=mock.PropertyMock, return_value=None)
    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_begin_transaction_no_project_id(self, mock_get_conn, mock_project_id):
        self.datastore_hook.connection = mock_get_conn.return_value
        with self.assertRaises(AirflowException) as err:
            self.datastore_hook.begin_transaction()
        self.assertIn("project_id", str(err.exception))

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_commit(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        body = {'item': 'a'}

        resp = self.datastore_hook.commit(body=body, project_id=GCP_PROJECT_ID)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        commit = projects.return_value.commit
        commit.assert_called_once_with(projectId=GCP_PROJECT_ID, body=body)
        execute = commit.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value)

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.project_id',
           new_callable=mock.PropertyMock, return_value=None)
    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_commit_no_project_id(self, mock_get_conn, mock_project_id):
        self.datastore_hook.connection = mock_get_conn.return_value
        body = {'item': 'a'}

        with self.assertRaises(AirflowException) as err:
            self.datastore_hook.commit(body=body)
        self.assertIn("project_id", str(err.exception))

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_lookup(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        keys = []
        read_consistency = 'ENUM'
        transaction = 'transaction'

        resp = self.datastore_hook.lookup(keys=keys,
                                          read_consistency=read_consistency,
                                          transaction=transaction,
                                          project_id=GCP_PROJECT_ID
                                          )

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        lookup = projects.return_value.lookup
        lookup.assert_called_once_with(projectId=GCP_PROJECT_ID,
                                       body={
                                           'keys': keys,
                                           'readConsistency': read_consistency,
                                           'transaction': transaction
                                       })
        execute = lookup.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value)

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.project_id',
           new_callable=mock.PropertyMock, return_value=None)
    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_lookup_no_project_id(self, mock_get_conn, mock_project_id):
        self.datastore_hook.connection = mock_get_conn.return_value
        keys = []
        read_consistency = 'ENUM'
        transaction = 'transaction'

        with self.assertRaises(AirflowException) as err:
            self.datastore_hook.lookup(keys=keys,
                                       read_consistency=read_consistency,
                                       transaction=transaction,
                                       )
        self.assertIn("project_id", str(err.exception))

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_rollback(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        transaction = 'transaction'

        self.datastore_hook.rollback(transaction=transaction, project_id=GCP_PROJECT_ID)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        rollback = projects.return_value.rollback
        rollback.assert_called_once_with(projectId=GCP_PROJECT_ID,
                                         body={'transaction': transaction})
        execute = rollback.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.project_id',
           new_callable=mock.PropertyMock, return_value=None)
    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_rollback_no_project_id(self, mock_get_conn, mock_project_id):
        self.datastore_hook.connection = mock_get_conn.return_value
        transaction = 'transaction'

        with self.assertRaises(AirflowException) as err:
            self.datastore_hook.rollback(transaction=transaction)
        self.assertIn("project_id", str(err.exception))

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_run_query(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        body = {'item': 'a'}

        resp = self.datastore_hook.run_query(body=body, project_id=GCP_PROJECT_ID)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        run_query = projects.return_value.runQuery
        run_query.assert_called_once_with(projectId=GCP_PROJECT_ID, body=body)
        execute = run_query.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value['batch'])

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.project_id',
           new_callable=mock.PropertyMock, return_value=None)
    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_run_query_no_project_id(self, mock_get_conn, mock_project_id):
        self.datastore_hook.connection = mock_get_conn.return_value
        body = {'item': 'a'}

        with self.assertRaises(AirflowException) as err:
            self.datastore_hook.run_query(body=body)
        self.assertIn("project_id", str(err.exception))

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_get_operation(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        name = 'name'

        resp = self.datastore_hook.get_operation(name=name)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        operations = projects.return_value.operations
        operations.assert_called_once_with()
        get = operations.return_value.get
        get.assert_called_once_with(name=name)
        execute = get.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value)

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_delete_operation(self, mock_get_conn):
        self.datastore_hook.connection = mock_get_conn.return_value
        name = 'name'

        resp = self.datastore_hook.delete_operation(name=name)

        projects = self.datastore_hook.connection.projects
        projects.assert_called_once_with()
        operations = projects.return_value.operations
        operations.assert_called_once_with()
        delete = operations.return_value.delete
        delete.assert_called_once_with(name=name)
        execute = delete.return_value.execute
        execute.assert_called_once_with(num_retries=mock.ANY)
        self.assertEqual(resp, execute.return_value)

    @patch('airflow.providers.google.cloud.hooks.datastore.time.sleep')
    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_operation',
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

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_export_to_storage_bucket(self, mock_get_conn):
        self.datastore_hook.admin_connection = mock_get_conn.return_value
        bucket = 'bucket'
        namespace = None
        entity_filter = {}
        labels = {}

        resp = self.datastore_hook.export_to_storage_bucket(bucket=bucket,
                                                            namespace=namespace,
                                                            entity_filter=entity_filter,
                                                            labels=labels,
                                                            project_id=GCP_PROJECT_ID
                                                            )

        projects = self.datastore_hook.admin_connection.projects
        projects.assert_called_once_with()
        export = projects.return_value.export
        export.assert_called_once_with(projectId=GCP_PROJECT_ID,
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

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.project_id',
           new_callable=mock.PropertyMock, return_value=None)
    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_export_to_storage_bucket_no_project_id(self, mock_get_conn, mock_project_id):
        self.datastore_hook.admin_connection = mock_get_conn.return_value
        bucket = 'bucket'
        namespace = None
        entity_filter = {}
        labels = {}

        with self.assertRaises(AirflowException) as err:
            self.datastore_hook.export_to_storage_bucket(bucket=bucket,
                                                         namespace=namespace,
                                                         entity_filter=entity_filter,
                                                         labels=labels,
                                                         )
        self.assertIn("project_id", str(err.exception))

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_import_from_storage_bucket(self, mock_get_conn):
        self.datastore_hook.admin_connection = mock_get_conn.return_value
        bucket = 'bucket'
        file = 'file'
        namespace = None
        entity_filter = {}
        labels = {}

        resp = self.datastore_hook.import_from_storage_bucket(bucket=bucket,
                                                              file=file,
                                                              namespace=namespace,
                                                              entity_filter=entity_filter,
                                                              labels=labels,
                                                              project_id=GCP_PROJECT_ID
                                                              )

        projects = self.datastore_hook.admin_connection.projects
        projects.assert_called_once_with()
        import_ = projects.return_value.import_
        import_.assert_called_once_with(projectId=GCP_PROJECT_ID,
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

    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.project_id',
           new_callable=mock.PropertyMock, return_value=None)
    @patch('airflow.providers.google.cloud.hooks.datastore.DatastoreHook.get_conn')
    def test_import_from_storage_bucket_no_project_id(self, mock_get_conn, mock_project_id):
        self.datastore_hook.admin_connection = mock_get_conn.return_value
        bucket = 'bucket'
        file = 'file'
        namespace = None
        entity_filter = {}
        labels = {}

        with self.assertRaises(AirflowException) as err:
            self.datastore_hook.import_from_storage_bucket(bucket=bucket,
                                                           file=file,
                                                           namespace=namespace,
                                                           entity_filter=entity_filter,
                                                           labels=labels,
                                                           )
        self.assertIn("project_id", str(err.exception))
