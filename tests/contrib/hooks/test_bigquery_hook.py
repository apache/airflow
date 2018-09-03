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
import warnings

from google.auth.exceptions import GoogleAuthError
import mock

from airflow.contrib.hooks import bigquery_hook as hook
from airflow.contrib.hooks.bigquery_hook import _cleanse_time_partitioning, \
    _validate_value, _api_resource_configs_duplication_check

bq_available = True

try:
    hook.BigQueryHook().get_service()
except GoogleAuthError:
    bq_available = False


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
        project, dataset, table = hook._split_tablename('dataset.table',
                                                        'project')
        self.assertEqual("project", project)
        self.assertEqual("dataset", dataset)
        self.assertEqual("table", table)

    def test_split_project_dataset_table(self):
        project, dataset, table = hook._split_tablename('alternative:dataset.table',
                                                        'project')
        self.assertEqual("alternative", project)
        self.assertEqual("dataset", dataset)
        self.assertEqual("table", table)

    def test_sql_split_project_dataset_table(self):
        project, dataset, table = hook._split_tablename('alternative.dataset.table',
                                                        'project')
        self.assertEqual("alternative", project)
        self.assertEqual("dataset", dataset)
        self.assertEqual("table", table)

    def test_colon_in_project(self):
        project, dataset, table = hook._split_tablename('alt1:alt.dataset.table',
                                                        'project')

        self.assertEqual('alt1:alt', project)
        self.assertEqual("dataset", dataset)
        self.assertEqual("table", table)

    def test_valid_double_column(self):
        project, dataset, table = hook._split_tablename('alt1:alt:dataset.table',
                                                        'project')

        self.assertEqual('alt1:alt', project)
        self.assertEqual("dataset", dataset)
        self.assertEqual("table", table)

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


class TestBigQueryHookSourceFormat(unittest.TestCase):
    def test_invalid_source_format(self):
        with self.assertRaises(Exception) as context:
            hook.BigQueryBaseCursor("test", "test").run_load(
                "test.test", "test_schema.json", ["test_data.json"], source_format="json"
            )

        # since we passed 'json' in, and it's not valid, make sure it's present in the
        # error string.
        self.assertIn("JSON", str(context.exception))


class TestBigQueryExternalTableSourceFormat(unittest.TestCase):
    def test_invalid_source_format(self):
        with self.assertRaises(Exception) as context:
            hook.BigQueryBaseCursor("test", "test").create_external_table(
                external_project_dataset_table='test.test',
                schema_fields='test_schema.json',
                source_uris=['test_data.json'],
                source_format='json'
            )

        # since we passed 'csv' in, and it's not valid, make sure it's present in the
        # error string.
        self.assertIn("JSON", str(context.exception))


# Helpers to test_cancel_queries that have mock_poll_job_complete returning false,
# unless mock_job_cancel was called with the same job_id
mock_canceled_jobs = []


def mock_poll_job_complete(job_id):
    return job_id in mock_canceled_jobs


def mock_job_cancel(projectId, jobId):
    mock_canceled_jobs.append(jobId)
    return mock.Mock()


class TestBigQueryBaseCursor(unittest.TestCase):
    def test_bql_deprecation_warning(self):
        with warnings.catch_warnings(record=True) as w:
            hook.BigQueryBaseCursor("test", "test").run_query(
                bql='select * from test_table'
            )
            yield
        self.assertIn(
            'Deprecated parameter `bql`',
            w[0].message.args[0])

    def test_invalid_schema_update_options(self):
        with self.assertRaises(Exception) as context:
            hook.BigQueryBaseCursor("test", "test").run_load(
                "test.test",
                "test_schema.json",
                ["test_data.json"],
                schema_update_options=["THIS IS NOT VALID"]
            )
        self.assertIn("THIS IS NOT VALID", str(context.exception))

    def test_nobql_nosql_param_error(self):
        with self.assertRaises(TypeError) as context:
            hook.BigQueryBaseCursor("test", "test").run_query(
                sql=None,
                bql=None
            )
        self.assertIn(
            'missing 1 required positional',
            str(context.exception))

    def test_invalid_schema_update_and_write_disposition(self):
        with self.assertRaises(Exception) as context:
            hook.BigQueryBaseCursor("test", "test").run_load(
                "test.test",
                "test_schema.json",
                ["test_data.json"],
                schema_update_options=['ALLOW_FIELD_ADDITION'],
                write_disposition='WRITE_EMPTY'
            )
        self.assertIn("schema_update_options is only", str(context.exception))

    @mock.patch("airflow.contrib.hooks.bigquery_hook.LoggingMixin")
    @mock.patch("airflow.contrib.hooks.bigquery_hook.time")
    def test_cancel_queries(self, mocked_time, mocked_logging):
        project_id = 12345
        running_job_id = 3

        mock_jobs = mock.Mock()
        mock_jobs.cancel = mock.Mock(side_effect=mock_job_cancel)
        mock_service = mock.Mock()
        mock_service.jobs = mock.Mock(return_value=mock_jobs)

        bq_hook = hook.BigQueryBaseCursor(mock_service, project_id)
        bq_hook.running_job_id = running_job_id
        bq_hook.poll_job_complete = mock.Mock(side_effect=mock_poll_job_complete)

        bq_hook.cancel_query()

        mock_jobs.cancel.assert_called_with(projectId=project_id, jobId=running_job_id)

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_sql_dialect_default(self, run_with_config):
        cursor = hook.BigQueryBaseCursor(mock.Mock(), "project_id")
        cursor.run_query('query')
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], True)

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_sql_dialect_override(self, run_with_config):
        for bool_val in [True, False]:
            cursor = hook.BigQueryBaseCursor(mock.Mock(), "project_id")
            cursor.run_query('query', use_legacy_sql=bool_val)
            args, kwargs = run_with_config.call_args
            self.assertIs(args[0]['query']['useLegacySql'], bool_val)

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_api_resource_configs(self, run_with_config):
        for bool_val in [True, False]:
            cursor = hook.BigQueryBaseCursor(mock.Mock(), "project_id")
            cursor.run_query('query',
                             api_resource_configs={
                                 'query': {'useQueryCache': bool_val}})
            args, kwargs = run_with_config.call_args
            self.assertIs(args[0]['query']['useQueryCache'], bool_val)
            self.assertIs(args[0]['query']['useLegacySql'], True)

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_api_resource_configs_duplication_warning(self, run_with_config):
        with self.assertRaises(ValueError):
            cursor = hook.BigQueryBaseCursor(mock.Mock(), "project_id")
            cursor.run_query('query',
                             use_legacy_sql=True,
                             api_resource_configs={
                                 'query': {'useLegacySql': False}})

    def test_validate_value(self):
        with self.assertRaises(TypeError):
            _validate_value("case_1", "a", dict)
        self.assertIsNone(_validate_value("case_2", 0, int))

    def test_duplication_check(self):
        with self.assertRaises(ValueError):
            key_one = True
            _api_resource_configs_duplication_check(
                "key_one", key_one, {"key_one": False})
        self.assertIsNone(_api_resource_configs_duplication_check(
            "key_one", key_one, {"key_one": True}))


class TestLabelsInRunJob(unittest.TestCase):
    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_with_arg(self, mocked_rwc):
        project_id = 12345

        def run_with_config(config):
            self.assertEqual(
                config['labels'], {'label1': 'test1', 'label2': 'test2'}
            )
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        bq_hook.run_query(
            sql='select 1',
            destination_dataset_table='my_dataset.my_table',
            labels={'label1': 'test1', 'label2': 'test2'}
        )

        mocked_rwc.assert_called_once()


class TestTimePartitioningInRunJob(unittest.TestCase):

    @mock.patch("airflow.contrib.hooks.bigquery_hook.LoggingMixin")
    @mock.patch("airflow.contrib.hooks.bigquery_hook.time")
    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_load_default(self, mocked_rwc, mocked_time, mocked_logging):
        project_id = 12345

        def run_with_config(config):
            self.assertIsNone(config['load'].get('timePartitioning'))
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        bq_hook.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
        )

        mocked_rwc.assert_called_once()

    @mock.patch("airflow.contrib.hooks.bigquery_hook.LoggingMixin")
    @mock.patch("airflow.contrib.hooks.bigquery_hook.time")
    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_load_with_arg(self, mocked_rwc, mocked_time, mocked_logging):
        project_id = 12345

        def run_with_config(config):
            self.assertEqual(
                config['load']['timePartitioning'],
                {
                    'field': 'test_field',
                    'type': 'DAY',
                    'expirationMs': 1000
                }
            )
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        bq_hook.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
            time_partitioning={'type': 'DAY', 'field': 'test_field', 'expirationMs': 1000}
        )

        mocked_rwc.assert_called_once()

    @mock.patch("airflow.contrib.hooks.bigquery_hook.LoggingMixin")
    @mock.patch("airflow.contrib.hooks.bigquery_hook.time")
    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_default(self, mocked_rwc, mocked_time, mocked_logging):
        project_id = 12345

        def run_with_config(config):
            self.assertIsNone(config['query'].get('timePartitioning'))
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        bq_hook.run_query(sql='select 1')

        mocked_rwc.assert_called_once()

    @mock.patch("airflow.contrib.hooks.bigquery_hook.LoggingMixin")
    @mock.patch("airflow.contrib.hooks.bigquery_hook.time")
    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_with_arg(self, mocked_rwc, mocked_time, mocked_logging):
        project_id = 12345

        def run_with_config(config):
            self.assertEqual(
                config['query']['timePartitioning'],
                {
                    'field': 'test_field',
                    'type': 'DAY',
                    'expirationMs': 1000
                }
            )
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        bq_hook.run_query(
            sql='select 1',
            destination_dataset_table='my_dataset.my_table',
            time_partitioning={'type': 'DAY',
                               'field': 'test_field', 'expirationMs': 1000}
        )

        mocked_rwc.assert_called_once()

    def test_dollar_makes_partition(self):
        tp_out = _cleanse_time_partitioning('test.teast$20170101', {})
        expect = {
            'type': 'DAY'
        }
        self.assertEqual(tp_out, expect)

    def test_extra_time_partitioning_options(self):
        tp_out = _cleanse_time_partitioning(
            'test.teast',
            {'type': 'DAY', 'field': 'test_field', 'expirationMs': 1000}
        )

        expect = {
            'type': 'DAY',
            'field': 'test_field',
            'expirationMs': 1000
        }
        self.assertEqual(tp_out, expect)

    def test_cant_add_dollar_and_field_name(self):
        with self.assertRaises(ValueError):
            _cleanse_time_partitioning(
                'test.teast$20170101',
                {'type': 'DAY', 'field': 'test_field', 'expirationMs': 1000}
            )


class TestBigQueryHookLegacySql(unittest.TestCase):
    """Ensure `use_legacy_sql` param in `BigQueryHook` propagates properly."""

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_hook_uses_legacy_sql_by_default(self, run_with_config):
        with mock.patch.object(hook.BigQueryHook, 'get_service'):
            bq_hook = hook.BigQueryHook()
            bq_hook.get_first('query')
            args, kwargs = run_with_config.call_args
            self.assertIs(args[0]['query']['useLegacySql'], True)

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_legacy_sql_override_propagates_properly(self, run_with_config):
        with mock.patch.object(hook.BigQueryHook, 'get_service'):
            bq_hook = hook.BigQueryHook(use_legacy_sql=False)
            bq_hook.get_first('query')
            args, kwargs = run_with_config.call_args
            self.assertIs(args[0]['query']['useLegacySql'], False)


if __name__ == '__main__':
    unittest.main()
