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
from unittest import mock
from typing import List

from google.auth.exceptions import GoogleAuthError
from googleapiclient.errors import HttpError

from airflow.contrib.hooks import bigquery_hook as hook
from airflow.contrib.hooks.bigquery_hook import _cleanse_time_partitioning, \
    _validate_value, _api_resource_configs_duplication_check

bq_available = True

try:
    hook.BigQueryHook().get_service()
except GoogleAuthError:
    bq_available = False


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
mock_canceled_jobs = []  # type: List


def mock_poll_job_complete(job_id):
    return job_id in mock_canceled_jobs


def mock_job_cancel(projectId, jobId):  # pylint: disable=unused-argument
    mock_canceled_jobs.append(jobId)
    return mock.Mock()


class TestBigQueryBaseCursor(unittest.TestCase):
    def test_invalid_schema_update_options(self):
        with self.assertRaises(Exception) as context:
            hook.BigQueryBaseCursor("test", "test").run_load(
                "test.test",
                "test_schema.json",
                ["test_data.json"],
                schema_update_options=["THIS IS NOT VALID"]
            )
        self.assertIn("THIS IS NOT VALID", str(context.exception))

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

    def test_cancel_queries(self):
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
    def test_run_query_sql_dialect_legacy_with_query_params(self, run_with_config):
        cursor = hook.BigQueryBaseCursor(mock.Mock(), "project_id")
        params = [{
            'name': "param_name",
            'parameterType': {'type': "STRING"},
            'parameterValue': {'value': "param_value"}
        }]
        cursor.run_query('query', use_legacy_sql=False, query_params=params)
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], False)

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_sql_dialect_legacy_with_query_params_fails(self, mock_run_with_configuration):
        cursor = hook.BigQueryBaseCursor(mock.Mock(), "project_id")
        params = [{
            'name': "param_name",
            'parameterType': {'type': "STRING"},
            'parameterValue': {'value': "param_value"}
        }]
        with self.assertRaises(ValueError):
            cursor.run_query('query', use_legacy_sql=True, query_params=params)

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

    def test_api_resource_configs_duplication_warning(self):
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


class TestTableDataOperations(unittest.TestCase):
    def test_insert_all_succeed(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'
        rows = [
            {"json": {"a_key": "a_value_0"}}
        ]
        body = {
            "rows": rows,
            "ignoreUnknownValues": False,
            "kind": "bigquery#tableDataInsertAllRequest",
            "skipInvalidRows": False,
        }

        mock_service = mock.Mock()
        method = mock_service.tabledata.return_value.insertAll
        method.return_value.execute.return_value = {
            "kind": "bigquery#tableDataInsertAllResponse"
        }
        cursor = hook.BigQueryBaseCursor(mock_service, 'project_id')
        cursor.insert_all(project_id, dataset_id, table_id, rows)
        method.assert_called_with(projectId=project_id, datasetId=dataset_id,
                                  tableId=table_id, body=body)

    def test_insert_all_fail(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'
        rows = [
            {"json": {"a_key": "a_value_0"}}
        ]

        mock_service = mock.Mock()
        method = mock_service.tabledata.return_value.insertAll
        method.return_value.execute.return_value = {
            "kind": "bigquery#tableDataInsertAllResponse",
            "insertErrors": [
                {
                    "index": 1,
                    "errors": []
                }
            ]
        }
        cursor = hook.BigQueryBaseCursor(mock_service, 'project_id')
        with self.assertRaises(Exception):
            cursor.insert_all(project_id, dataset_id, table_id,
                              rows, fail_on_error=True)


class TestTableOperations(unittest.TestCase):
    def test_create_view_fails_on_exception(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table_view'
        view = {
            'incorrect_key': 'SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*`',
            "useLegacySql": False
        }

        mock_service = mock.Mock()
        method = mock_service.tables.return_value.insert
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Query is required for views')
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)
        with self.assertRaises(Exception):
            cursor.create_empty_table(project_id, dataset_id, table_id,
                                      view=view)

    def test_create_view(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table_view'
        view = {
            'query': 'SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*`',
            "useLegacySql": False
        }

        mock_service = mock.Mock()
        method = mock_service.tables.return_value.insert
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)
        cursor.create_empty_table(project_id, dataset_id, table_id,
                                  view=view)
        body = {
            'tableReference': {
                'tableId': table_id
            },
            'view': view
        }
        method.assert_called_once_with(projectId=project_id, datasetId=dataset_id, body=body)

    def test_patch_table(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'

        description_patched = 'Test description.'
        expiration_time_patched = 2524608000000
        friendly_name_patched = 'Test friendly name.'
        labels_patched = {'label1': 'test1', 'label2': 'test2'}
        schema_patched = [
            {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'balance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
            {'name': 'new_field', 'type': 'STRING', 'mode': 'NULLABLE'}
        ]
        time_partitioning_patched = {
            'expirationMs': 10000000
        }
        require_partition_filter_patched = True

        mock_service = mock.Mock()
        method = (mock_service.tables.return_value.patch)
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)
        cursor.patch_table(
            dataset_id, table_id, project_id,
            description=description_patched,
            expiration_time=expiration_time_patched,
            friendly_name=friendly_name_patched,
            labels=labels_patched, schema=schema_patched,
            time_partitioning=time_partitioning_patched,
            require_partition_filter=require_partition_filter_patched
        )

        body = {
            "description": description_patched,
            "expirationTime": expiration_time_patched,
            "friendlyName": friendly_name_patched,
            "labels": labels_patched,
            "schema": {
                "fields": schema_patched
            },
            "timePartitioning": time_partitioning_patched,
            "requirePartitionFilter": require_partition_filter_patched
        }
        method.assert_called_once_with(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=body
        )

    def test_patch_view(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        view_id = 'bq_view'
        view_patched = {
            'query': "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 500",
            'useLegacySql': False
        }

        mock_service = mock.Mock()
        method = (mock_service.tables.return_value.patch)
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)
        cursor.patch_table(dataset_id, view_id, project_id, view=view_patched)

        body = {
            'view': view_patched
        }
        method.assert_called_once_with(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=view_id,
            body=body
        )

    def test_create_empty_table_succeed(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'

        mock_service = mock.Mock()
        method = mock_service.tables.return_value.insert
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)
        cursor.create_empty_table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id)

        body = {
            'tableReference': {
                'tableId': table_id
            }
        }
        method.assert_called_once_with(
            projectId=project_id,
            datasetId=dataset_id,
            body=body
        )

    def test_create_empty_table_with_extras_succeed(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'
        schema_fields = [
            {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created', 'type': 'DATE', 'mode': 'REQUIRED'},
        ]
        time_partitioning = {"field": "created", "type": "DAY"}
        cluster_fields = ['name']

        mock_service = mock.Mock()
        method = mock_service.tables.return_value.insert
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)

        cursor.create_empty_table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            schema_fields=schema_fields,
            time_partitioning=time_partitioning,
            cluster_fields=cluster_fields
        )

        body = {
            'tableReference': {
                'tableId': table_id
            },
            'schema': {
                'fields': schema_fields
            },
            'timePartitioning': time_partitioning,
            'clustering': {
                'fields': cluster_fields
            }
        }
        method.assert_called_once_with(
            projectId=project_id,
            datasetId=dataset_id,
            body=body
        )

    def test_create_empty_table_on_exception(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'

        mock_service = mock.Mock()
        method = mock_service.tables.return_value.insert
        method.return_value.execute.side_effect = HttpError(
            resp={'status': '400'}, content=b'Bad request')
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)
        with self.assertRaises(Exception):
            cursor.create_empty_table(project_id, dataset_id, table_id)


class TestBigQueryCursor(unittest.TestCase):
    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_execute_with_parameters(self, mocked_rwc):
        hook.BigQueryCursor("test", "test").execute(
            "SELECT %(foo)s", {"foo": "bar"})
        assert mocked_rwc.call_count == 1


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

        assert mocked_rwc.call_count == 1


class TestDatasetsOperations(unittest.TestCase):

    def test_create_empty_dataset_no_dataset_id_err(self):

        with self.assertRaises(ValueError):
            hook.BigQueryBaseCursor(
                mock.Mock(), "test_create_empty_dataset").create_empty_dataset(
                dataset_id="", project_id="")

    def test_create_empty_dataset_duplicates_call_err(self):
        with self.assertRaises(ValueError):
            hook.BigQueryBaseCursor(
                mock.Mock(), "test_create_empty_dataset").create_empty_dataset(
                dataset_id="", project_id="project_test",
                dataset_reference={
                    "datasetReference":
                        {"datasetId": "test_dataset",
                         "projectId": "project_test2"}})

    def test_get_dataset_without_dataset_id(self):
        with mock.patch.object(hook.BigQueryHook, 'get_service'):
            with self.assertRaises(ValueError):
                hook.BigQueryBaseCursor(
                    mock.Mock(), "test_create_empty_dataset").get_dataset(
                    dataset_id="", project_id="project_test")

    def test_get_dataset(self):
        expected_result = {
            "kind": "bigquery#dataset",
            "location": "US",
            "id": "your-project:dataset_2_test",
            "datasetReference": {
                "projectId": "your-project",
                "datasetId": "dataset_2_test"
            }
        }
        dataset_id = "test_dataset"
        project_id = "project_test"

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        with mock.patch.object(bq_hook.service, 'datasets') as MockService:
            MockService.return_value.get(datasetId=dataset_id,
                                         projectId=project_id).execute.\
                return_value = expected_result
            result = bq_hook.get_dataset(dataset_id=dataset_id,
                                         project_id=project_id)
            self.assertEqual(result, expected_result)

    def test_get_datasets_list(self):
        expected_result = {'datasets': [
            {
                "kind": "bigquery#dataset",
                "location": "US",
                "id": "your-project:dataset_2_test",
                "datasetReference": {
                    "projectId": "your-project",
                    "datasetId": "dataset_2_test"
                }
            },
            {
                "kind": "bigquery#dataset",
                "location": "US",
                "id": "your-project:dataset_1_test",
                "datasetReference": {
                    "projectId": "your-project",
                    "datasetId": "dataset_1_test"
                }
            }
        ]}
        project_id = "project_test"''

        mocked = mock.Mock()
        with mock.patch.object(hook.BigQueryBaseCursor(mocked, project_id).service,
                               'datasets') as MockService:
            MockService.return_value.list(
                projectId=project_id).execute.return_value = expected_result
            result = hook.BigQueryBaseCursor(
                mocked, "test_create_empty_dataset").get_datasets_list(
                project_id=project_id)
            self.assertEqual(result, expected_result['datasets'])

    def test_delete_dataset(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        delete_contents = True

        mock_service = mock.Mock()
        method = mock_service.datasets.return_value.delete
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)
        cursor.delete_dataset(project_id, dataset_id, delete_contents)

        method.assert_called_once_with(projectId=project_id, datasetId=dataset_id,
                                       deleteContents=delete_contents)


class TestTimePartitioningInRunJob(unittest.TestCase):
    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_load_default(self, mocked_rwc):
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

        assert mocked_rwc.call_count == 1

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_with_auto_detect(self, run_with_config):
        destination_project_dataset_table = "autodetect.table"
        cursor = hook.BigQueryBaseCursor(mock.Mock(), "project_id")
        cursor.run_load(destination_project_dataset_table, [], [], autodetect=True)
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['load']['autodetect'], True)

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_load_with_arg(self, mocked_rwc):
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

        assert mocked_rwc.call_count == 1

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_default(self, mocked_rwc):
        project_id = 12345

        def run_with_config(config):
            self.assertIsNone(config['query'].get('timePartitioning'))
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        bq_hook.run_query(sql='select 1')

        assert mocked_rwc.call_count == 1

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_with_arg(self, mocked_rwc):
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

        assert mocked_rwc.call_count == 1

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


class TestClusteringInRunJob(unittest.TestCase):

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_load_default(self, mocked_rwc):
        project_id = 12345

        def run_with_config(config):
            self.assertIsNone(config['load'].get('clustering'))
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        bq_hook.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
        )

        assert mocked_rwc.call_count == 1

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_load_with_arg(self, mocked_rwc):
        project_id = 12345

        def run_with_config(config):
            self.assertEqual(
                config['load']['clustering'],
                {
                    'fields': ['field1', 'field2']
                }
            )
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        bq_hook.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
            cluster_fields=['field1', 'field2'],
            time_partitioning={'type': 'DAY'}
        )

        assert mocked_rwc.call_count == 1

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_default(self, mocked_rwc):
        project_id = 12345

        def run_with_config(config):
            self.assertIsNone(config['query'].get('clustering'))
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        bq_hook.run_query(sql='select 1')

        assert mocked_rwc.call_count == 1

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_with_arg(self, mocked_rwc):
        project_id = 12345

        def run_with_config(config):
            self.assertEqual(
                config['query']['clustering'],
                {
                    'fields': ['field1', 'field2']
                }
            )
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryBaseCursor(mock.Mock(), project_id)
        bq_hook.run_query(
            sql='select 1',
            destination_dataset_table='my_dataset.my_table',
            cluster_fields=['field1', 'field2'],
            time_partitioning={'type': 'DAY'}
        )

        assert mocked_rwc.call_count == 1


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


class TestBigQueryHookLocation(unittest.TestCase):
    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_location_propagates_properly(self, run_with_config):
        with mock.patch.object(hook.BigQueryHook, 'get_service'):
            bq_hook = hook.BigQueryHook(location=None)
            self.assertIsNone(bq_hook.location)

            bq_cursor = hook.BigQueryBaseCursor(mock.Mock(),
                                                'test-project',
                                                location=None)
            self.assertIsNone(bq_cursor.location)
            bq_cursor.run_query(sql='select 1', location='US')
            assert run_with_config.call_count == 1
            self.assertEqual(bq_cursor.location, 'US')


class TestBigQueryHookRunWithConfiguration(unittest.TestCase):
    def test_run_with_configuration_location(self):
        project_id = 'bq-project'
        running_job_id = 'job_vjdi28vskdui2onru23'
        location = 'asia-east1'

        mock_service = mock.Mock()
        method = (mock_service.jobs.return_value.get)

        mock_service.jobs.return_value.insert.return_value.execute.return_value = {
            'jobReference': {
                'jobId': running_job_id,
                'location': location
            }
        }

        mock_service.jobs.return_value.get.return_value.execute.return_value = {
            'status': {
                'state': 'DONE'
            }
        }

        cursor = hook.BigQueryBaseCursor(mock_service, project_id)
        cursor.running_job_id = running_job_id
        cursor.run_with_configuration({})

        method.assert_called_once_with(
            projectId=project_id,
            jobId=running_job_id,
            location=location
        )


class TestBigQueryWithKMS(unittest.TestCase):
    def test_create_empty_table_with_kms(self):
        project_id = "bq-project"
        dataset_id = "bq_dataset"
        table_id = "bq_table"
        schema_fields = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"}
        ]
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }

        mock_service = mock.Mock()
        method = mock_service.tables.return_value.insert
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)

        cursor.create_empty_table(
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=table_id,
            schema_fields=schema_fields,
            encryption_configuration=encryption_configuration,
        )

        body = {
            "tableReference": {"tableId": table_id},
            "schema": {"fields": schema_fields},
            "encryptionConfiguration": encryption_configuration,
        }
        method.assert_called_once_with(
            projectId=project_id, datasetId=dataset_id, body=body
        )

    def test_create_external_table_with_kms(self):
        project_id = "bq-project"
        dataset_id = "bq_dataset"
        table_id = "bq_table"
        external_project_dataset_table = "{}.{}.{}".format(
            project_id, dataset_id, table_id
        )
        source_uris = ['test_data.csv']
        source_format = 'CSV'
        autodetect = False
        compression = 'NONE'
        ignore_unknown_values = False
        max_bad_records = 10
        skip_leading_rows = 1
        field_delimiter = ','
        quote_character = None
        allow_quoted_newlines = False
        allow_jagged_rows = False
        labels = {'label1': 'test1', 'label2': 'test2'}
        schema_fields = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"}
        ]
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }

        mock_service = mock.Mock()
        method = mock_service.tables.return_value.insert
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)

        cursor.create_external_table(
            external_project_dataset_table=external_project_dataset_table,
            source_uris=source_uris,
            source_format=source_format,
            autodetect=autodetect,
            compression=compression,
            ignore_unknown_values=ignore_unknown_values,
            max_bad_records=max_bad_records,
            skip_leading_rows=skip_leading_rows,
            field_delimiter=field_delimiter,
            quote_character=quote_character,
            allow_jagged_rows=allow_jagged_rows,
            allow_quoted_newlines=allow_quoted_newlines,
            labels=labels,
            schema_fields=schema_fields,
            encryption_configuration=encryption_configuration
        )

        body = {
            'externalDataConfiguration': {
                'autodetect': autodetect,
                'sourceFormat': source_format,
                'sourceUris': source_uris,
                'compression': compression,
                'ignoreUnknownValues': ignore_unknown_values,
                'schema': {'fields': schema_fields},
                'maxBadRecords': max_bad_records,
                'csvOptions': {
                    'skipLeadingRows': skip_leading_rows,
                    'fieldDelimiter': field_delimiter,
                    'quote': quote_character,
                    'allowQuotedNewlines': allow_quoted_newlines,
                    'allowJaggedRows': allow_jagged_rows
                }
            },
            'tableReference': {
                'projectId': project_id,
                'datasetId': dataset_id,
                'tableId': table_id,
            },
            'labels': labels,
            "encryptionConfiguration": encryption_configuration,
        }
        method.assert_called_once_with(
            projectId=project_id, datasetId=dataset_id, body=body
        )

    def test_patch_table_with_kms(self):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }

        mock_service = mock.Mock()
        method = (mock_service.tables.return_value.patch)
        cursor = hook.BigQueryBaseCursor(mock_service, project_id)
        cursor.patch_table(
            dataset_id=dataset_id,
            table_id=table_id,
            project_id=project_id,
            encryption_configuration=encryption_configuration
        )

        body = {
            "encryptionConfiguration": encryption_configuration
        }

        method.assert_called_once_with(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=body
        )

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_query_with_kms(self, run_with_config):
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }
        cursor = hook.BigQueryBaseCursor(mock.Mock(), "project_id")
        cursor.run_query(
            sql='query',
            encryption_configuration=encryption_configuration
        )
        args, kwargs = run_with_config.call_args
        self.assertIs(
            args[0]['query']['destinationEncryptionConfiguration'],
            encryption_configuration
        )

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_copy_with_kms(self, run_with_config):
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }
        cursor = hook.BigQueryBaseCursor(mock.Mock(), "project_id")
        cursor.run_copy(
            source_project_dataset_tables='p.d.st',
            destination_project_dataset_table='p.d.dt',
            encryption_configuration=encryption_configuration
        )
        args, kwargs = run_with_config.call_args
        self.assertIs(
            args[0]['copy']['destinationEncryptionConfiguration'],
            encryption_configuration
        )

    @mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration')
    def test_run_load_with_kms(self, run_with_config):
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }
        cursor = hook.BigQueryBaseCursor(mock.Mock(), "project_id")
        cursor.run_load(
            destination_project_dataset_table='p.d.dt',
            source_uris=['abc.csv'],
            autodetect=True,
            encryption_configuration=encryption_configuration
        )
        args, kwargs = run_with_config.call_args
        self.assertIs(
            args[0]['load']['destinationEncryptionConfiguration'],
            encryption_configuration
        )


if __name__ == '__main__':
    unittest.main()
