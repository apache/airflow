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
from unittest import mock

from googleapiclient.errors import HttpError
from parameterized import parameterized

from airflow.gcp.hooks import bigquery as hook
from airflow.gcp.hooks.bigquery import (
    _api_resource_configs_duplication_check, _cleanse_time_partitioning, _validate_src_fmt_configs,
    _validate_value,
)


class TestBigQueryHookConnection(unittest.TestCase):

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryConnection")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook._authorize")
    @mock.patch("airflow.gcp.hooks.bigquery.build")
    def test_bigquery_client_creation(
        self, mock_build, mock_authorize, mock_bigquery_connection, mock_get_creds_and_proj_id
    ):
        bq_hook = hook.BigQueryHook()
        result = bq_hook.get_conn()
        mock_build.assert_called_once_with(
            'bigquery', 'v2', http=mock_authorize.return_value, cache_discovery=False
        )
        mock_bigquery_connection.assert_called_once_with(
            service=mock_build.return_value,
            project_id=bq_hook.project_id,
            use_legacy_sql=bq_hook.use_legacy_sql,
            location=bq_hook.location,
            num_retries=bq_hook.num_retries
        )
        self.assertEqual(mock_bigquery_connection.return_value, result)


class TestPandasGbqCredentials(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch('airflow.gcp.hooks.bigquery.read_gbq')
    def test_credentials_provided(self, mock_read_gbq, mock_get_creds_and_proj_id):
        self.instance = hook.BigQueryHook()  # pylint: disable=attribute-defined-outside-init

        self.instance.get_pandas_df('select 1')

        args, kwargs = mock_read_gbq.call_args
        self.assertEqual("CREDENTIALS", kwargs['credentials'])
        self.assertEqual("PROJECT_ID", kwargs['project_id'])


class TestBigQueryTableSplitter(unittest.TestCase):
    def test_internal_need_default_project(self):
        with self.assertRaisesRegex(Exception, "INTERNAL: No default project is specified"):
            hook._split_tablename("dataset.table", None)

    @parameterized.expand([
        ("project", "dataset", "table", "dataset.table"),
        ("alternative", "dataset", "table", "alternative:dataset.table"),
        ("alternative", "dataset", "table", "alternative.dataset.table"),
        ("alt1:alt", "dataset", "table", "alt1:alt.dataset.table"),
        ("alt1:alt", "dataset", "table", "alt1:alt:dataset.table"),
    ])
    def test_split_tablename(self, project_expected, dataset_expected, table_expected, table_input):
        default_project_id = "project"
        project, dataset, table = hook._split_tablename(table_input, default_project_id)
        self.assertEqual(project_expected, project)
        self.assertEqual(dataset_expected, dataset)
        self.assertEqual(table_expected, table)

    @parameterized.expand([
        ("alt1:alt2:alt3:dataset.table", None, "Use either : or . to specify project got {}"),
        (
            "alt1.alt.dataset.table", None,
            r"Expect format of \(<project\.\|<project\:\)<dataset>\.<table>, got {}",
        ),
        (
            "alt1:alt2:alt.dataset.table", "var_x",
            "Format exception for var_x: Use either : or . to specify project got {}",
        ),
        (
            "alt1:alt2:alt:dataset.table", "var_x",
            "Format exception for var_x: Use either : or . to specify project got {}",
        ),
        (
            "alt1.alt.dataset.table", "var_x",
            r"Format exception for var_x: Expect format of "
            r"\(<project\.\|<project:\)<dataset>.<table>, got {}",
        ),
    ])
    def test_invalid_syntax(self, table_input, var_name, exception_message):
        default_project_id = "project"
        with self.assertRaisesRegex(Exception, exception_message.format(table_input)):
            hook._split_tablename(table_input, default_project_id, var_name)


class TestBigQueryHookSourceFormat(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_invalid_source_format(self, mock_get_service, mock_get_creds_and_proj_id):
        with self.assertRaisesRegex(
            Exception,
            r"JSON is not a valid source format. Please use one of the following types: \['CSV', "
            r"'NEWLINE_DELIMITED_JSON', 'AVRO', 'GOOGLE_SHEETS', 'DATASTORE_BACKUP', 'PARQUET'\]"
        ):
            bq_hook = hook.BigQueryHook()
            cursor = bq_hook.get_cursor()
            cursor.run_load(
                "test.test", "test_schema.json", ["test_data.json"], source_format="json"
            )


class TestBigQueryExternalTableSourceFormat(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_invalid_source_format(self, mock_get_service, mock_get_creds_and_proj_id):
        with self.assertRaisesRegex(
            Exception,
            r"JSON is not a valid source format. Please use one of the following types: \['CSV', "
            r"'NEWLINE_DELIMITED_JSON', 'AVRO', 'GOOGLE_SHEETS', 'DATASTORE_BACKUP', 'PARQUET'\]"
        ):
            bq_hook = hook.BigQueryHook()
            cursor = bq_hook.get_cursor()
            cursor.create_external_table(
                external_project_dataset_table='test.test',
                schema_fields='test_schema.json',
                source_uris=['test_data.json'],
                source_format='json'
            )


class TestBigQueryBaseCursor(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_invalid_schema_update_options(self, mock_get_service, mock_get_creds_and_proj_id):
        with self.assertRaisesRegex(
            Exception,
            r"\['THIS IS NOT VALID'\] contains invalid schema update options.Please only use one or more of "
            r"the following options: \['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'\]"
        ):
            bq_hook = hook.BigQueryHook()
            cursor = bq_hook.get_cursor()
            cursor.run_load(
                "test.test",
                "test_schema.json",
                ["test_data.json"],
                schema_update_options=["THIS IS NOT VALID"]
            )

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_invalid_schema_update_and_write_disposition(self, mock_get_service, mock_get_creds_and_proj_id):
        with self.assertRaisesRegex(Exception, "schema_update_options is only allowed if"
                                               " write_disposition is 'WRITE_APPEND' or 'WRITE_TRUNCATE'."):
            bq_hook = hook.BigQueryHook()
            cursor = bq_hook.get_cursor()
            cursor.run_load(
                "test.test",
                "test_schema.json",
                ["test_data.json"],
                schema_update_options=['ALLOW_FIELD_ADDITION'],
                write_disposition='WRITE_EMPTY'
            )

    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.poll_job_complete", side_effect=[False, True])
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_cancel_queries(self, mock_get_service, mock_get_creds_and_proj_id, mock_poll_job_complete):
        project_id = 12345
        running_job_id = 3

        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.running_job_id = running_job_id
        cursor.cancel_query()

        mock_poll_job_complete.has_calls(mock.call(running_job_id), mock.call(running_job_id))
        mock_get_service.return_value.jobs.return_value.cancel.assert_called_once_with(
            projectId=project_id, jobId=running_job_id
        )

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_query_sql_dialect_default(
        self, run_with_config, mock_get_service, mock_get_creds_and_proj_id
    ):
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_query('query')
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], True)

    @parameterized.expand([(None, True), (True, True), (False, False)])
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_query_sql_dialect(
        self, bool_val, expected, run_with_config, mock_get_service, mock_get_creds_and_proj_id
    ):
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_query('query', use_legacy_sql=bool_val)
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], expected)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_query_sql_dialect_legacy_with_query_params(
        self, run_with_config, mock_get_service, mock_get_creds_and_proj_id
    ):
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        params = [{
            'name': "param_name",
            'parameterType': {'type': "STRING"},
            'parameterValue': {'value': "param_value"}
        }]
        cursor.run_query('query', use_legacy_sql=False, query_params=params)
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], False)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_run_query_sql_dialect_legacy_with_query_params_fails(
        self, mock_get_service, mock_get_creds_and_proj_id
    ):
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        params = [{
            'name': "param_name",
            'parameterType': {'type': "STRING"},
            'parameterValue': {'value': "param_value"}
        }]
        with self.assertRaisesRegex(ValueError, "Query parameters are not allowed when using legacy SQL"):
            cursor.run_query('query', use_legacy_sql=True, query_params=params)

    @parameterized.expand([(True,), (False,)])
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_api_resource_configs(
        self, bool_val, run_with_config, mock_get_service, mock_get_creds_and_proj_id
    ):
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_query('query',
                         api_resource_configs={
                             'query': {'useQueryCache': bool_val}})
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useQueryCache'], bool_val)
        self.assertIs(args[0]['query']['useLegacySql'], True)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_api_resource_configs_duplication_warning(self, mock_get_service, mock_get_creds_and_proj_id):
        with self.assertRaisesRegex(
            ValueError,
            r"Values of useLegacySql param are duplicated\. api_resource_configs contained useLegacySql "
            r"param in `query` config and useLegacySql was also provided with arg to run_query\(\) method\. "
            r"Please remove duplicates\."
        ):
            bq_hook = hook.BigQueryHook()
            cursor = bq_hook.get_cursor()
            cursor.run_query('query',
                             use_legacy_sql=True,
                             api_resource_configs={
                                 'query': {'useLegacySql': False}})

    def test_validate_value(self):
        with self.assertRaisesRegex(
            TypeError, "case_1 argument must have a type <class 'dict'> not <class 'str'>"
        ):
            _validate_value("case_1", "a", dict)
        self.assertIsNone(_validate_value("case_2", 0, int))

    def test_duplication_check(self):
        with self.assertRaisesRegex(
            ValueError,
            r"Values of key_one param are duplicated. api_resource_configs contained key_one param in"
            r" `query` config and key_one was also provided with arg to run_query\(\) method. "
            r"Please remove duplicates."
        ):
            key_one = True
            _api_resource_configs_duplication_check("key_one", key_one, {"key_one": False})
        self.assertIsNone(_api_resource_configs_duplication_check(
            "key_one", key_one, {"key_one": True}))

    def test_validate_src_fmt_configs(self):
        source_format = "test_format"
        valid_configs = ["test_config_known", "compatibility_val"]
        backward_compatibility_configs = {"compatibility_val": "val"}

        with self.assertRaisesRegex(
            ValueError, "test_config_unknown is not a valid src_fmt_configs for type test_format."
        ):
            # This config should raise a value error.
            src_fmt_configs = {"test_config_unknown": "val"}
            _validate_src_fmt_configs(source_format,
                                      src_fmt_configs,
                                      valid_configs,
                                      backward_compatibility_configs)

        src_fmt_configs = {"test_config_known": "val"}
        src_fmt_configs = _validate_src_fmt_configs(source_format, src_fmt_configs, valid_configs,
                                                    backward_compatibility_configs)
        assert "test_config_known" in src_fmt_configs, \
            "src_fmt_configs should contain al known src_fmt_configs"

        assert "compatibility_val" in src_fmt_configs, \
            "_validate_src_fmt_configs should add backward_compatibility config"

    @parameterized.expand(
        [("AVRO",), ("PARQUET",), ("NEWLINE_DELIMITED_JSON",), ("DATASTORE_BACKUP",)]
    )
    def test_run_load_with_non_csv_as_src_fmt(self, fmt):
        with mock.patch.object(hook.BigQueryBaseCursor, 'run_with_configuration'):
            bq_hook = hook.BigQueryBaseCursor(mock.Mock(), 12345)

            try:
                bq_hook.run_load(
                    destination_project_dataset_table='my_dataset.my_table',
                    source_uris=[],
                    source_format=fmt,
                    autodetect=True
                )
            except ValueError:
                self.fail("run_load() raised ValueError unexpectedly!")


class TestTableDataOperations(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_insert_all_succeed(self, mock_get_service, mock_get_creds_and_proj_id):
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

        mock_service = mock_get_service.return_value
        method = mock_service.tabledata.return_value.insertAll
        method.return_value.execute.return_value = {
            "kind": "bigquery#tableDataInsertAllResponse"
        }

        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.insert_all(project_id, dataset_id, table_id, rows)
        method.assert_called_once_with(
            projectId=project_id,
            datasetId=dataset_id,
            tableId=table_id,
            body=body
        )

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_insert_all_fail(self, mock_get_service, mock_get_creds_and_proj_id):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'
        rows = [
            {"json": {"a_key": "a_value_0"}}
        ]

        mock_service = mock_get_service.return_value
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
        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        with self.assertRaisesRegex(
            Exception,
            r"BigQuery job failed\. Error was: 1 insert error\(s\) occurred: "
            r"bq-project:bq_dataset\.bq_table\. Details: \[{'index': 1, 'errors': \[\]}\]"
        ):
            cursor.insert_all(project_id, dataset_id, table_id,
                              rows, fail_on_error=True)


class TestTableOperations(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_view_fails_on_exception(self, mock_get_service, mock_get_creds_and_proj_id):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table_view'
        view = {
            'incorrect_key': 'SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*`',
            "useLegacySql": False
        }

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.insert
        resp = type('', (object,), {"status": 500, "reason": "Query is required for views"})()
        method.return_value.execute.side_effect = HttpError(
            resp=resp, content=b'Query is required for views')
        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        with self.assertRaisesRegex(Exception, "HttpError 500 \"Query is required for views\""):
            cursor.create_empty_table(project_id, dataset_id, table_id,
                                      view=view)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_view(self, mock_get_service, mock_get_creds_and_proj_id):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table_view'
        view = {
            'query': 'SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*`',
            "useLegacySql": False
        }

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.insert
        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.create_empty_table(project_id, dataset_id, table_id,
                                  view=view)
        body = {
            'tableReference': {
                'tableId': table_id
            },
            'view': view
        }
        method.assert_called_once_with(projectId=project_id, datasetId=dataset_id, body=body)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_patch_table(self, mock_get_service, mock_get_creds_and_proj_id):
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

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.patch
        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_patch_view(self, mock_get_service, mock_get_creds_and_proj_id):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        view_id = 'bq_view'
        view_patched = {
            'query': "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 500",
            'useLegacySql': False
        }

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.patch
        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_empty_table_succeed(self, mock_get_service, mock_get_creds_and_proj_id):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.insert
        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_empty_table_with_extras_succeed(self, mock_get_service, mock_get_creds_and_proj_id):
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

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.insert
        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()

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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_empty_table_on_exception(self, mock_get_service, mock_get_creds_and_proj_id):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.insert
        resp = type('', (object,), {"status": 500, "reason": "Bad request"})()
        method.return_value.execute.side_effect = HttpError(
            resp=resp, content=b'Bad request')
        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        with self.assertRaisesRegex(Exception, "Bad request"):
            cursor.create_empty_table(project_id, dataset_id, table_id)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_get_tables_list(self, mock_get_service, mock_get_creds_and_proj_id):
        expected_result = {
            "kind": "bigquery#tableList",
            "etag": "N/b12GSqMasEfwBOXofGQ==",
            "tables": [
                {
                    "kind": "bigquery#table",
                    "id": "your-project:your_dataset.table1",
                    "tableReference": {
                        "projectId": "your-project",
                        "datasetId": "your_dataset",
                        "tableId": "table1"
                    },
                    "type": "TABLE",
                    "creationTime": "1565781859261"
                },
                {
                    "kind": "bigquery#table",
                    "id": "your-project:your_dataset.table2",
                    "tableReference": {
                        "projectId": "your-project",
                        "datasetId": "your_dataset",
                        "tableId": "table2"
                    },
                    "type": "TABLE",
                    "creationTime": "1565782713480"
                }
            ],
            "totalItems": 2
        }

        project_id = 'your-project'
        dataset_id = 'your_dataset'

        mock_service = mock_get_service.return_value
        mock_service.tables.return_value.list.return_value.execute.return_value = expected_result
        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        result = cursor.get_dataset_tables(dataset_id=dataset_id)
        mock_service.tables.return_value.list.assert_called_once_with(
            datasetId=dataset_id, projectId=project_id
        )
        self.assertEqual(result, expected_result)


class TestBigQueryCursor(unittest.TestCase):
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_execute_with_parameters(self, mocked_rwc):
        hook.BigQueryCursor("test", "test").execute(
            "SELECT %(foo)s", {"foo": "bar"})
        assert mocked_rwc.call_count == 1

    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryCursor.flush_results")
    def test_flush_cursor_in_execute(self, _, mocked_fr):
        hook.BigQueryCursor("test", "test").execute(
            "SELECT %(foo)s", {"foo": "bar"})
        assert mocked_fr.call_count == 1

    def test_flush_cursor(self):
        bq_cursor = hook.BigQueryCursor("test", "test")
        bq_cursor.page_token = '456dcea9-fcbf-4f02-b570-83f5297c685e'
        bq_cursor.job_id = 'c0a79ae4-0e72-4593-a0d0-7dbbf726f193'
        bq_cursor.all_pages_loaded = True
        bq_cursor.buffer = [('a', 100, 200), ('b', 200, 300)]
        bq_cursor.flush_results()
        self.assertIsNone(bq_cursor.page_token)
        self.assertIsNone(bq_cursor.job_id)
        self.assertFalse(bq_cursor.all_pages_loaded)
        self.assertListEqual(bq_cursor.buffer, [])


class TestLabelsInRunJob(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_query_with_arg(self, mocked_rwc, mock_get_service, mock_get_creds_and_proj_id):

        def run_with_config(config):
            self.assertEqual(
                config['labels'], {'label1': 'test1', 'label2': 'test2'}
            )
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_query(
            sql='select 1',
            destination_dataset_table='my_dataset.my_table',
            labels={'label1': 'test1', 'label2': 'test2'}
        )

        assert mocked_rwc.call_count == 1


class TestDatasetsOperations(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_empty_dataset_no_dataset_id_err(self, mock_get_service, mock_get_creds_and_proj_id):
        with self.assertRaisesRegex(ValueError, r"dataset_id not provided and datasetId not exist in the "
                                                r"datasetReference\. Impossible to create dataset"):
            bq_hook = hook.BigQueryHook()
            cursor = bq_hook.get_cursor()
            cursor.create_empty_dataset(dataset_id="", project_id="")

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_empty_dataset_duplicates_call_err(self, mock_get_service, mock_get_creds_and_proj_id):
        with self.assertRaisesRegex(
            ValueError,
            r"Values of projectId param are duplicated\. dataset_reference contained projectId param in "
            r"`query` config and projectId was also provided with arg to run_query\(\) method\. "
            r"Please remove duplicates\."
        ):
            bq_hook = hook.BigQueryHook()
            cursor = bq_hook.get_cursor()
            cursor.create_empty_dataset(
                dataset_id="", project_id="project_test",
                dataset_reference={
                    "datasetReference":
                        {"datasetId": "test_dataset",
                         "projectId": "project_test2"}})

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_empty_dataset_with_location_duplicates_call_err(
        self, mock_get_service, mock_get_creds_and_proj_id
    ):
        with self.assertRaisesRegex(
            ValueError,
            r"Values of location param are duplicated\. dataset_reference contained location param in "
            r"`query` config and location was also provided with arg to run_query\(\) method\. "
            r"Please remove duplicates\."
        ):
            bq_hook = hook.BigQueryHook()
            cursor = bq_hook.get_cursor()
            cursor.create_empty_dataset(
                dataset_id="", project_id="project_test", location="EU",
                dataset_reference={
                    "location": "US",
                    "datasetReference":
                        {"datasetId": "test_dataset",
                         "projectId": "project_test"}})

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_empty_dataset_with_location(self, mock_get_service, mock_get_creds_and_proj_id):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        location = 'EU'

        method = mock_get_service.return_value.datasets.return_value.insert
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.create_empty_dataset(project_id=project_id, dataset_id=dataset_id, location=location)

        expected_body = {
            "location": "EU",
            "datasetReference": {
                "datasetId": "bq_dataset",
                "projectId": "bq-project"
            }
        }

        method.assert_called_once_with(projectId=project_id, body=expected_body)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_empty_dataset_with_location_duplicates_call_no_err(
        self, mock_get_service, mock_get_creds_and_proj_id
    ):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        location = 'EU'
        dataset_reference = {"location": "EU"}

        method = mock_get_service.return_value.datasets.return_value.insert
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.create_empty_dataset(project_id=project_id, dataset_id=dataset_id, location=location,
                                    dataset_reference=dataset_reference)

        expected_body = {
            "location": "EU",
            "datasetReference": {
                "datasetId": "bq_dataset",
                "projectId": "bq-project"
            }
        }

        method.assert_called_once_with(projectId=project_id, body=expected_body)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_get_dataset_without_dataset_id(self, mock_get_service, mock_get_creds_and_proj_id):
        with self.assertRaisesRegex(
            ValueError,
            r"ataset_id argument must be provided and has a type 'str'\. You provided: "
        ):
            bq_hook = hook.BigQueryHook()
            cursor = bq_hook.get_cursor()
            cursor.get_dataset(dataset_id="", project_id="project_test")

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_get_dataset(self, mock_get_service, mock_get_creds_and_proj_id):
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
        mock_get_service.return_value.datasets.return_value.get.return_value.execute.return_value = (
            expected_result
        )

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()

        result = cursor.get_dataset(dataset_id=dataset_id, project_id=project_id)

        self.assertEqual(result, expected_result)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_get_datasets_list(self, mock_get_service, mock_get_creds_and_proj_id):
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

        mock_get_service.return_value.datasets.return_value.list.return_value.execute.return_value = (
            expected_result
        )
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        result = cursor.get_datasets_list(project_id=project_id)

        mock_get_service.return_value.datasets.return_value.list.assert_called_once_with(projectId=project_id)
        self.assertEqual(result, expected_result['datasets'])

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_delete_dataset(self, mock_get_service, mock_get_creds_and_proj_id):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        delete_contents = True

        method = mock_get_service.return_value.datasets.return_value.delete
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.delete_dataset(project_id, dataset_id, delete_contents)

        method.assert_called_once_with(projectId=project_id, datasetId=dataset_id,
                                       deleteContents=delete_contents)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_patch_dataset(self, mock_get_service, mock_get_creds_and_proj_id):
        dataset_resource = {
            "access": [
                {
                    "role": "WRITER",
                    "groupByEmail": "cloud-logs@google.com"
                }
            ]
        }

        dataset_id = "test_dataset"
        project_id = "project_test"

        method = mock_get_service.return_value.datasets.return_value.patch
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.patch_dataset(
            dataset_id=dataset_id,
            project_id=project_id,
            dataset_resource=dataset_resource
        )

        method.assert_called_once_with(
            projectId=project_id,
            datasetId=dataset_id,
            body=dataset_resource
        )

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_update_dataset(self, mock_get_service, mock_get_creds_and_proj_id):
        dataset_resource = {
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

        method = mock_get_service.return_value.datasets.return_value.update
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.update_dataset(
            dataset_id=dataset_id,
            project_id=project_id,
            dataset_resource=dataset_resource
        )

        method.assert_called_once_with(
            projectId=project_id,
            datasetId=dataset_id,
            body=dataset_resource
        )


class TestTimePartitioningInRunJob(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_load_default(self, mocked_rwc, mock_get_service, mock_get_creds_and_proj_id):
        def run_with_config(config):
            self.assertIsNone(config['load'].get('timePartitioning'))
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()

        cursor.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
        )

        assert mocked_rwc.call_count == 1

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_with_auto_detect(self, run_with_config, mock_get_service, mock_get_creds_and_proj_id):
        destination_project_dataset_table = "autodetect.table"
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_load(destination_project_dataset_table, [], [], autodetect=True)
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['load']['autodetect'], True)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_load_with_arg(self, mocked_rwc, mock_get_service, mock_get_creds_and_proj_id):
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

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
            time_partitioning={'type': 'DAY', 'field': 'test_field', 'expirationMs': 1000}
        )

        assert mocked_rwc.call_count == 1

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_query_default(self, mocked_rwc, mock_get_service, mock_get_creds_and_proj_id):
        def run_with_config(config):
            self.assertIsNone(config['query'].get('timePartitioning'))
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_query(sql='select 1')

        assert mocked_rwc.call_count == 1

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_query_with_arg(self, mocked_rwc, mock_get_service, mock_get_creds_and_proj_id):
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

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_query(
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_load_default(self, mocked_rwc, mock_get_service, mock_get_creds_and_proj_id):

        def run_with_config(config):
            self.assertIsNone(config['load'].get('clustering'))
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
        )

        assert mocked_rwc.call_count == 1

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_load_with_arg(self, mocked_rwc, mock_get_service, mock_get_creds_and_proj_id):

        def run_with_config(config):
            self.assertEqual(
                config['load']['clustering'],
                {
                    'fields': ['field1', 'field2']
                }
            )
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
            cluster_fields=['field1', 'field2'],
            time_partitioning={'type': 'DAY'}
        )

        assert mocked_rwc.call_count == 1

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_query_default(self, mocked_rwc, mock_get_service, mock_get_creds_and_proj_id):

        def run_with_config(config):
            self.assertIsNone(config['query'].get('clustering'))
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_query(sql='select 1')

        assert mocked_rwc.call_count == 1

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_query_with_arg(self, mocked_rwc, mock_get_service, mock_get_creds_and_proj_id):

        def run_with_config(config):
            self.assertEqual(
                config['query']['clustering'],
                {
                    'fields': ['field1', 'field2']
                }
            )
        mocked_rwc.side_effect = run_with_config

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()

        cursor.run_query(
            sql='select 1',
            destination_dataset_table='my_dataset.my_table',
            cluster_fields=['field1', 'field2'],
            time_partitioning={'type': 'DAY'}
        )

        self.assertEqual(mocked_rwc.call_count, 1, "run_with_configuration() was not called exactly once")


class TestBigQueryHookLegacySql(unittest.TestCase):
    """Ensure `use_legacy_sql` param in `BigQueryHook` propagates properly."""

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_hook_uses_legacy_sql_by_default(
        self, run_with_config, mock_get_service, mock_get_creds_and_proj_id
    ):
        bq_hook = hook.BigQueryHook()
        bq_hook.get_first('query')
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], True)

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_legacy_sql_override_propagates_properly(
        self, run_with_config, mock_get_service, mock_get_creds_and_proj_id
    ):
        bq_hook = hook.BigQueryHook(use_legacy_sql=False)
        bq_hook.get_first('query')
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], False)


class TestBigQueryHookLocation(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_location_propagates_properly(
        self, run_with_config, mock_get_service, mock_get_creds_and_proj_id
    ):
        bq_hook = hook.BigQueryHook(location=None)
        self.assertIsNone(bq_hook.location)
        cursor = bq_hook.get_cursor()
        self.assertIsNone(cursor.location)
        cursor.run_query(sql='select 1', location='US')
        assert run_with_config.call_count == 1
        self.assertEqual(cursor.location, 'US')


class TestBigQueryHookRunWithConfiguration(unittest.TestCase):
    @mock.patch('airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id')
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_run_with_configuration_location(self, mock_get_service, mock_get_creds_and_proj_id):
        project_id = 'bq-project'
        running_job_id = 'job_vjdi28vskdui2onru23'
        location = 'asia-east1'

        mock_get_creds_and_proj_id.return_value = ("CREDENTIALS", project_id)
        method = mock_get_service.return_value.jobs.return_value.get

        mock_get_service.return_value.jobs.return_value.insert.return_value.execute.return_value = {
            'jobReference': {
                'jobId': running_job_id,
                'location': location
            }
        }

        mock_get_service.return_value.jobs.return_value.get.return_value.execute.return_value = {
            'status': {
                'state': 'DONE'
            }
        }

        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.running_job_id = running_job_id
        cursor.run_with_configuration({})

        method.assert_called_once_with(
            projectId=project_id,
            jobId=running_job_id,
            location=location
        )


class TestBigQueryWithKMS(unittest.TestCase):
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_empty_table_with_kms(self, mock_get_service, mock_get_creds_and_proj_id):
        project_id = "bq-project"
        dataset_id = "bq_dataset"
        table_id = "bq_table"
        schema_fields = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"}
        ]
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.insert
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()

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

    # pylint: disable=too-many-locals
    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=mock.PropertyMock,
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_create_external_table_with_kms(self, mock_get_service, mock_project_id):
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
        encoding = "UTF-8"
        labels = {'label1': 'test1', 'label2': 'test2'}
        schema_fields = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"}
        ]
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.insert
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()

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
            encoding=encoding,
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
                    'allowJaggedRows': allow_jagged_rows,
                    'encoding': encoding
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=mock.PropertyMock,
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    def test_patch_table_with_kms(self, mock_get_service, mock_project_id):
        project_id = 'bq-project'
        dataset_id = 'bq_dataset'
        table_id = 'bq_table'
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.patch
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()

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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook._get_credentials_and_project_id',
        return_value=("CREDENTIALS", "PROJECT_ID",)
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_query_with_kms(self, run_with_config, mock_get_service, mock_get_creds_and_proj_id):
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
        cursor.run_query(
            sql='query',
            encryption_configuration=encryption_configuration
        )
        args, kwargs = run_with_config.call_args
        self.assertIs(
            args[0]['query']['destinationEncryptionConfiguration'],
            encryption_configuration
        )

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=mock.PropertyMock,
        return_value="project_id"
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_copy_with_kms(self, run_with_config, mock_get_service, mock_project_id):
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
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

    @mock.patch(
        'airflow.gcp.hooks.base.CloudBaseHook.project_id',
        new_callable=mock.PropertyMock,
        return_value="project_id"
    )
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.gcp.hooks.bigquery.BigQueryBaseCursor.run_with_configuration")
    def test_run_load_with_kms(self, run_with_config, mock_get_service, mock_project_id):
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }
        bq_hook = hook.BigQueryHook()
        cursor = bq_hook.get_cursor()
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
