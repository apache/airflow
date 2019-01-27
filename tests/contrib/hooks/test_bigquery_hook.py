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

import os
import unittest

import mock
from google.cloud import bigquery

from airflow.utils import timezone
from airflow import AirflowException
from airflow.contrib.hooks import bigquery_hook as hook
from airflow.contrib.hooks.bigquery_hook import _validate_value

bq_available = True

if "TRAVIS" in os.environ and bool(os.environ["TRAVIS"]):
    bq_available = False

try:
    bigquery.Client()
except Exception:
    bq_available = False


class BigQueryTestBase(unittest.TestCase):
    def setUp(self):
        super(BigQueryTestBase, self).setUp()
        self.bq_client = mock.Mock()
        self.bq_hook = hook.BigQueryHook()
        patcher = mock.patch.object(hook.BigQueryHook, 'get_client')
        self.addCleanup(patcher.stop)
        patcher.start().return_value = self.bq_client


class TestPandasGbqPrivateKey(unittest.TestCase):
    def setUp(self):
        self.instance = hook.BigQueryHook()
        if not bq_available:
            self.instance.extras['extra__google_cloud_platform__project'] = 'mock_project'

    def test_key_path_provided(self):
        private_key_path = '/Fake/Path'
        self.instance.extras['extra__google_cloud_platform__key_path'] = private_key_path

        with mock.patch('airflow.contrib.hooks.bigquery_hook.read_gbq',
                        new=lambda *args, **kwargs: kwargs['private_key']):

            self.assertEqual(self.instance.get_pandas_df('select 1'), private_key_path)

    def test_key_json_provided(self):
        private_key_json = 'Fake Private Key'
        self.instance.extras['extra__google_cloud_platform__keyfile_dict'] = private_key_json

        with mock.patch('airflow.contrib.hooks.bigquery_hook.read_gbq', new=lambda *args,
                        **kwargs: kwargs['private_key']):
            self.assertEqual(self.instance.get_pandas_df('select 1'), private_key_json)

    def test_no_key_provided(self):
        with mock.patch('airflow.contrib.hooks.bigquery_hook.read_gbq', new=lambda *args,
                        **kwargs: kwargs['private_key']):
            self.assertEqual(self.instance.get_pandas_df('select 1'), None)


class TestBigQueryDataframeResults(unittest.TestCase):
    def setUp(self):
        self.instance = hook.BigQueryHook()

    @unittest.skipIf(not bq_available, 'BQ is not available to run tests')
    def test_output_is_dataframe_with_valid_query(self):
        import pandas as pd
        df = self.instance.get_pandas_df('select 1')
        self.assertIsInstance(df, pd.DataFrame)

    @unittest.skipIf(not bq_available, 'BQ is not available to run tests')
    def test_throws_exception_with_invalid_query(self):
        with self.assertRaises(Exception) as context:
            self.instance.get_pandas_df('from `1`')
        self.assertIn('Reason: ', str(context.exception), "")

    @unittest.skipIf(not bq_available, 'BQ is not available to run tests')
    def test_succeeds_with_explicit_legacy_query(self):
        df = self.instance.get_pandas_df('select 1', dialect='legacy')
        self.assertEqual(df.iloc(0)[0][0], 1)

    @unittest.skipIf(not bq_available, 'BQ is not available to run tests')
    def test_succeeds_with_explicit_std_query(self):
        df = self.instance.get_pandas_df(
            'select * except(b) from (select 1 a, 2 b)', dialect='standard')
        self.assertEqual(df.iloc(0)[0][0], 1)

    @unittest.skipIf(not bq_available, 'BQ is not available to run tests')
    def test_throws_exception_with_incompatible_syntax(self):
        with self.assertRaises(Exception) as context:
            self.instance.get_pandas_df(
                'select * except(b) from (select 1 a, 2 b)', dialect='legacy')
        self.assertIn('Reason: ', str(context.exception), "")


class TestBigQueryTableSplitter(unittest.TestCase):
    def test_internal_need_default_project(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('dataset.table', None)

        self.assertIn('INTERNAL: No default project is specified',
                      str(context.exception), "")

    def test_split_dataset_table(self):
        table_ref = hook._split_tablename('dataset.table',
                                          'project')
        self.assertEqual('project', table_ref.project)
        self.assertEqual("dataset", table_ref.dataset_id)
        self.assertEqual("table", table_ref.table_id)

    def test_split_project_dataset_table(self):
        table_ref = hook._split_tablename('alternative:dataset.table',
                                          'project')

        self.assertEqual('alternative', table_ref.project)
        self.assertEqual("dataset", table_ref.dataset_id)
        self.assertEqual("table", table_ref.table_id)

    def test_sql_split_project_dataset_table(self):
        table_ref = hook._split_tablename('alternative.dataset.table',
                                          'project')

        self.assertEqual('alternative', table_ref.project)
        self.assertEqual("dataset", table_ref.dataset_id)
        self.assertEqual("table", table_ref.table_id)

    def test_colon_in_project(self):
        table_ref = hook._split_tablename('alt1:alt.dataset.table',
                                          'project')

        self.assertEqual('alt1:alt', table_ref.project)
        self.assertEqual("dataset", table_ref.dataset_id)
        self.assertEqual("table", table_ref.table_id)

    def test_valid_double_column(self):
        table_ref = hook._split_tablename('alt1:alt:dataset.table',
                                          'project')

        self.assertEqual('alt1:alt', table_ref.project)
        self.assertEqual("dataset", table_ref.dataset_id)
        self.assertEqual("table", table_ref.table_id)

    def test_invalid_syntax_triple_colon(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1:alt2:alt3:dataset.table',
                                  'project')

        self.assertIn('Use either : or . to specify project',
                      str(context.exception), "")
        self.assertFalse('Format exception for' in str(context.exception))

    def test_invalid_syntax_triple_dot(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1.alt.dataset.table',
                                  'project')

        self.assertIn('Expect format of (<project.|<project:)<dataset>.<table>',
                      str(context.exception), "")
        self.assertFalse('Format exception for' in str(context.exception))

    def test_invalid_syntax_column_double_project_var(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1:alt2:alt.dataset.table',
                                  'project', 'var_x')

        self.assertIn('Use either : or . to specify project',
                      str(context.exception), "")
        self.assertIn('Format exception for var_x:',
                      str(context.exception), "")

    def test_invalid_syntax_triple_colon_project_var(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1:alt2:alt:dataset.table',
                                  'project', 'var_x')

        self.assertIn('Use either : or . to specify project',
                      str(context.exception), "")
        self.assertIn('Format exception for var_x:',
                      str(context.exception), "")

    def test_invalid_syntax_triple_dot_var(self):
        with self.assertRaises(Exception) as context:
            hook._split_tablename('alt1.alt.dataset.table',
                                  'project', 'var_x')

        self.assertIn('Expect format of (<project.|<project:)<dataset>.<table>',
                      str(context.exception), "")
        self.assertIn('Format exception for var_x:',
                      str(context.exception), "")


class TestBigQueryExternalTableSourceFormat(unittest.TestCase):
    def test_invalid_source_format(self):
        with self.assertRaises(Exception) as context:
            hook.BigQueryHook().create_external_table(
                external_project_dataset_table='test:test.test',
                schema_fields='test_schema.json',
                source_uris=['test_data.json'],
                source_format='json'
            )

        # since we passed 'csv' in, and it's not valid, make sure it's present in the
        # error string.
        self.assertIn("JSON", str(context.exception))


class TestQueries(BigQueryTestBase):

    def test_invalid_schema_update_options(self):
        with self.assertRaises(Exception) as context:
            self.bq_hook.run_load(
                "test.test",
                ["test_data.json"],
                project_id='project_id',
                schema_update_options=["THIS IS NOT VALID"],
                schema_fields=[],
            )
        self.assertIn("THIS IS NOT VALID", str(context.exception))

    def test_invalid_schema_update_and_write_disposition(self):
        with self.assertRaises(Exception) as context:
            self.bq_hook.run_load(
                "test.test",
                ["test_data.json"],
                project_id='project_id',
                schema_update_options=['ALLOW_FIELD_ADDITION'],
                schema_fields=[],
                write_disposition='WRITE_EMPTY',
            )
        self.assertIn("schema_update_options is only", str(context.exception))

    def test_run_query_sql_dialect_default(self):
        self.bq_hook.run_query('query')
        self.bq_client.query.assert_called
        args, kwargs = self.bq_client.query.call_args
        self.assertFalse(kwargs['job_config'].use_legacy_sql)

    def test_run_query_sql_dialect_override(self):
        for bool_val in [True, False]:
            self.bq_hook.run_query('query', use_legacy_sql=bool_val)
            args, kwargs = self.bq_client.query.call_args
            self.assertIs(kwargs['job_config'].use_legacy_sql, bool_val)

    def test_run_query_labels(self):
        self.bq_hook.run_query('query', labels={'foo': 'bar'})
        self.bq_client.query.assert_called
        args, kwargs = self.bq_client.query.call_args
        self.assertEqual(kwargs['job_config'].labels, {'foo': 'bar'})

    def test_run_query_autodetect(self):
        self.bq_hook.run_query('query', labels={'foo': 'bar'})
        self.bq_client.query.assert_called
        args, kwargs = self.bq_client.query.call_args
        self.assertEqual(kwargs['job_config'].labels, {'foo': 'bar'})

    def test_validate_value(self):
        with self.assertRaises(TypeError):
            _validate_value("case_1", "a", dict)
        self.assertIsNone(_validate_value("case_2", 0, int))

    def test_insert_all_succeed(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'
        rows = [
            {"json": {"a_key": "a_value_0"}}
        ]

        self.bq_client.insert_rows.return_value = []

        self.bq_hook.insert_all(dataset_id, table_id, rows, project_id=project_id)

        self.bq_client.insert_rows.assert_called
        args, kwargs = self.bq_client.insert_rows.call_args
        self.assertEqual(args[1:], (rows, ))
        self.assertEqual(kwargs, {
            'skip_invalid_rows': False,
            'ignore_unknown_values': False,
        })

    def test_insert_all_fail(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'
        rows = [
            {"json": {"a_key": "a_value_0"}}
        ]

        self.bq_client.insert_rows.return_value = [{'errors': [{'reason': 'invalid'}]}]

        with self.assertRaises(AirflowException):
            self.bq_hook.insert_all(dataset_id, table_id, rows,
                                    project_id=project_id, fail_on_error=True)

    def test_create_view(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table_view'
        view_query = 'SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*`'

        self.bq_client.insert_rows.return_value = [{'errors': [{'reason': 'invalid'}]}]

        self.bq_hook.create_empty_table(dataset_id, table_id, project_id=project_id, view=view_query)

        self.bq_client.create_table.assert_called
        args, kwargs = self.bq_client.create_table.call_args
        table = args[0]
        self.assertEqual(table.view_query, view_query)

    def test_patch_table(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'

        description_patched = 'Test description.'
        expiration_time_patched = timezone.datetime(2019, 4, 20)
        friendly_name_patched = 'Test friendly name.'
        labels_patched = {'label1': 'test1', 'label2': 'test2'}
        schema_patched = [
            bigquery.SchemaField('id', 'STRING', mode='REQUIRED'),
            bigquery.SchemaField('name', 'STRING', mode='NULLABLE'),
            bigquery.SchemaField('balance', 'FLOAT', mode='NULLABLE'),
            bigquery.SchemaField('new_field', 'STRING', mode='NULLABLE'),
        ]
        time_partitioning_patched = bigquery.TimePartitioning()

        self.bq_hook.patch_table(
            dataset_id, table_id, project_id,
            description=description_patched,
            expiration_time=expiration_time_patched,
            friendly_name=friendly_name_patched,
            labels=labels_patched, schema=schema_patched,
            time_partitioning=time_partitioning_patched,
        )

        self.bq_client.update_table.assert_called
        args, kwargs = self.bq_client.update_table.call_args
        table = args[0]
        self.assertEqual(table.description, description_patched)
        self.assertEqual(table.expires, expiration_time_patched)

    def test_patch_view(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        view_id = 'bq_view'
        view_patched = "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 500"

        self.bq_hook.patch_table(
            dataset_id, view_id, project_id,
            view=view_patched,
        )

        self.bq_client.update_table.assert_called
        args, kwargs = self.bq_client.update_table.call_args
        table = args[0]
        self.assertEqual(table.view_query, view_patched)
        self.assertEqual(kwargs['fields'], ['view_query'])


class TestDatasetsOperations(BigQueryTestBase):

    def test_get_dataset(self):
        dataset_id = "test_dataset"
        project_id = "project_test"

        dataset = self.bq_hook.get_dataset(dataset_id=dataset_id, project_id=project_id)

        self.assertEqual(dataset, self.bq_client.get_dataset.return_value)

    def test_get_datasets_list(self):
        project_id = "project_test"

        self.bq_client.list_datasets.return_value = []
        datasets = self.bq_hook.list_datasets(project_id=project_id)

        self.assertEqual(datasets, self.bq_client.list_datasets.return_value)


class TestLoad(BigQueryTestBase):

    def test_load(self):
        project_id = 'project'
        destination = 'dataset.table'
        source_uris = ['gs://bucket/path']
        schema = []
        self.bq_hook.run_load(destination, source_uris, project_id=project_id, schema_fields=schema)

        self.bq_client.load_table_from_uri.assert_called()
        self.bq_client.load_table_from_uri.return_value.result.assert_called()

    def test_load_time_partitioning(self):
        project_id = 'project'
        destination = 'dataset.table'
        source_uris = ['gs://bucket/path']
        schema = []
        partitioning = bigquery.TimePartitioning()
        self.bq_hook.run_load(destination, source_uris, project_id=project_id, schema_fields=schema,
                              time_partitioning=partitioning)

        self.bq_client.load_table_from_uri.assert_called()
        self.bq_client.load_table_from_uri.return_value.result.assert_called()

        args, kwargs = self.bq_client.load_table_from_uri.call_args

        self.assertEqual(kwargs['job_config'].time_partitioning, partitioning)
