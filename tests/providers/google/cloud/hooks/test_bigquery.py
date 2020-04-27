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
import re
import unittest
from unittest import mock

from google.cloud.bigquery import DEFAULT_RETRY, DatasetReference, Table, TableReference
from google.cloud.bigquery.dataset import Dataset, DatasetListItem
from google.cloud.exceptions import NotFound
from googleapiclient.errors import HttpError
from parameterized import parameterized

from airflow.providers.google.cloud.hooks.bigquery import (
    BigQueryCursor, BigQueryHook, _api_resource_configs_duplication_check, _cleanse_time_partitioning,
    _split_tablename, _validate_src_fmt_configs, _validate_value,
)

PROJECT_ID = "bq-project"
CREDENTIALS = "bq-credentials"
DATASET_ID = "bq_dataset"
TABLE_ID = "bq_table"
VIEW_ID = 'bq_view'
JOB_ID = 1234
LOCATION = 'europe-north1'
TABLE_REFERENCE_REPR = {
    'tableId': TABLE_ID,
    'datasetId': DATASET_ID,
    'projectId': PROJECT_ID,
}
TABLE_REFERENCE = TableReference.from_api_repr(TABLE_REFERENCE_REPR)


class _BigQueryBaseTestClass(unittest.TestCase):
    def setUp(self) -> None:
        class MockedBigQueryHook(BigQueryHook):
            def _get_credentials_and_project_id(self):
                return CREDENTIALS, PROJECT_ID

        self.hook = MockedBigQueryHook()


class TestBigQueryHookMethods(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryConnection")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook._authorize")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.build")
    def test_bigquery_client_creation(
        self, mock_build, mock_authorize, mock_bigquery_connection
    ):
        result = self.hook.get_conn()
        mock_build.assert_called_once_with(
            'bigquery', 'v2', http=mock_authorize.return_value, cache_discovery=False
        )
        mock_bigquery_connection.assert_called_once_with(
            service=mock_build.return_value,
            project_id=PROJECT_ID,
            hook=self.hook,
            use_legacy_sql=self.hook.use_legacy_sql,
            location=self.hook.location,
            num_retries=self.hook.num_retries
        )
        self.assertEqual(mock_bigquery_connection.return_value, result)

    @mock.patch("airflow.providers.google.common.hooks.base_google.GoogleBaseHook.__init__")
    def test_bigquery_bigquery_conn_id_deprecation_warning(
        self, mock_base_hook_init,
    ):
        bigquery_conn_id = "bigquery conn id"
        warning_message = "The bigquery_conn_id parameter has been deprecated. " \
                          "You should pass the gcp_conn_id parameter."
        with self.assertWarns(DeprecationWarning) as warn:
            BigQueryHook(bigquery_conn_id=bigquery_conn_id)
            mock_base_hook_init.assert_called_once_with(delegate_to=None, gcp_conn_id='bigquery conn id')
        self.assertEqual(warning_message, str(warn.warning))

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_location_propagates_properly(
        self, run_with_config, mock_get_service
    ):
        self.assertIsNone(self.hook.location)
        self.hook.run_query(sql='select 1', location='US')
        assert run_with_config.call_count == 1
        self.assertEqual(self.hook.location, 'US')

    def test_bigquery_insert_rows_not_implemented(self):
        with self.assertRaises(NotImplementedError):
            self.hook.insert_rows(table="table", rows=[1, 2])

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_bigquery_table_exists_true(self, mock_client):
        result = self.hook.table_exists(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)
        mock_client.return_value.get_table.assert_called_once_with(TABLE_REFERENCE)
        assert result is True

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_bigquery_table_exists_false(self, mock_client):
        mock_client.return_value.get_table.side_effect = NotFound("Dataset not found")

        result = self.hook.table_exists(project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID)
        mock_client.return_value.get_table.assert_called_once_with(TABLE_REFERENCE)
        assert result is False

    @mock.patch('airflow.providers.google.cloud.hooks.bigquery.read_gbq')
    def test_get_pandas_df(self, mock_read_gbq):
        self.hook.get_pandas_df('select 1')

        mock_read_gbq.assert_called_once_with(
            'select 1', credentials=CREDENTIALS, dialect='legacy', project_id=PROJECT_ID, verbose=False
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_invalid_schema_update_options(self, mock_get_service):
        with self.assertRaisesRegex(
            Exception,
            r"\['THIS IS NOT VALID'\] contains invalid schema update options.Please only use one or more of "
            r"the following options: \['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'\]"
        ):

            self.hook.run_load(
                "test.test",
                "test_schema.json",
                ["test_data.json"],
                schema_update_options=["THIS IS NOT VALID"]
            )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_invalid_schema_update_and_write_disposition(self, mock_get_service):
        with self.assertRaisesRegex(Exception, "schema_update_options is only allowed if"
                                               " write_disposition is 'WRITE_APPEND' or 'WRITE_TRUNCATE'."):

            self.hook.run_load(
                "test.test",
                "test_schema.json",
                ["test_data.json"],
                schema_update_options=['ALLOW_FIELD_ADDITION'],
                write_disposition='WRITE_EMPTY'
            )

    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.poll_job_complete",
        side_effect=[False, True]
    )
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_cancel_queries(self, mock_get_service, mock_poll_job_complete):
        running_job_id = 3

        self.hook.running_job_id = running_job_id
        self.hook.cancel_query()

        mock_poll_job_complete.has_calls(mock.call(running_job_id), mock.call(running_job_id))
        mock_get_service.return_value.jobs.return_value.cancel.assert_called_once_with(
            projectId=PROJECT_ID, jobId=running_job_id
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_query_sql_dialect_default(
        self, run_with_config, mock_get_service,
    ):
        self.hook.run_query('query')
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], True)

    @parameterized.expand([(None, True), (True, True), (False, False)])
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_query_sql_dialect(
        self, bool_val, expected, run_with_config, mock_get_service,
    ):
        self.hook.run_query('query', use_legacy_sql=bool_val)
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], expected)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_query_sql_dialect_legacy_with_query_params(
        self, run_with_config, mock_get_service
    ):
        params = [{
            'name': "param_name",
            'parameterType': {'type': "STRING"},
            'parameterValue': {'value': "param_value"}
        }]
        self.hook.run_query('query', use_legacy_sql=False, query_params=params)
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], False)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_run_query_sql_dialect_legacy_with_query_params_fails(
        self, mock_get_service,
    ):
        params = [{
            'name': "param_name",
            'parameterType': {'type': "STRING"},
            'parameterValue': {'value': "param_value"}
        }]
        with self.assertRaisesRegex(ValueError, "Query parameters are not allowed when using legacy SQL"):
            self.hook.run_query('query', use_legacy_sql=True, query_params=params)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_run_query_without_sql_fails(
        self, mock_get_service,
    ):
        with self.assertRaisesRegex(
            TypeError,
            r"`BigQueryBaseCursor.run_query` missing 1 required positional argument: `sql`"
        ):
            self.hook.run_query(sql=None)

    @parameterized.expand([
        (['ALLOW_FIELD_ADDITION'], 'WRITE_APPEND'),
        (['ALLOW_FIELD_RELAXATION'], 'WRITE_APPEND'),
        (['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'], 'WRITE_APPEND'),
        (['ALLOW_FIELD_ADDITION'], 'WRITE_TRUNCATE'),
        (['ALLOW_FIELD_RELAXATION'], 'WRITE_TRUNCATE'),
        (['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'], 'WRITE_TRUNCATE'),
    ])
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_query_schema_update_options(
        self,
        schema_update_options,
        write_disposition,
        run_with_config,
        mock_get_service,
    ):
        self.hook.run_query(
            sql='query',
            destination_dataset_table='my_dataset.my_table',
            schema_update_options=schema_update_options,
            write_disposition=write_disposition
        )
        args, kwargs = run_with_config.call_args
        self.assertEqual(
            args[0]['query']['schemaUpdateOptions'],
            schema_update_options
        )
        self.assertEqual(
            args[0]['query']['writeDisposition'],
            write_disposition
        )

    @parameterized.expand([
        (
            ['INCORRECT_OPTION'],
            None,
            r"\['INCORRECT_OPTION'\] contains invalid schema update options\. "
            r"Please only use one or more of the following options: "
            r"\['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'\]"
        ),
        (
            ['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION', 'INCORRECT_OPTION'],
            None,
            r"\['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION', 'INCORRECT_OPTION'\] contains invalid "
            r"schema update options\. Please only use one or more of the following options: "
            r"\['ALLOW_FIELD_ADDITION', 'ALLOW_FIELD_RELAXATION'\]"
        ),
        (
            ['ALLOW_FIELD_ADDITION'],
            None,
            r"schema_update_options is only allowed if write_disposition is "
            r"'WRITE_APPEND' or 'WRITE_TRUNCATE'"),
    ])
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_run_query_schema_update_options_incorrect(
        self,
        schema_update_options,
        write_disposition,
        expected_regex,
        mock_get_service,
    ):
        with self.assertRaisesRegex(ValueError, expected_regex):
            self.hook.run_query(
                sql='query',
                destination_dataset_table='my_dataset.my_table',
                schema_update_options=schema_update_options,
                write_disposition=write_disposition
            )

    @parameterized.expand([(True,), (False,)])
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_api_resource_configs(
        self, bool_val, run_with_config, mock_get_service,
    ):
        self.hook.run_query('query', api_resource_configs={'query': {'useQueryCache': bool_val}})
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useQueryCache'], bool_val)
        self.assertIs(args[0]['query']['useLegacySql'], True)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_api_resource_configs_duplication_warning(self, mock_get_service):
        with self.assertRaisesRegex(
            ValueError,
            r"Values of useLegacySql param are duplicated\. api_resource_configs contained useLegacySql "
            r"param in `query` config and useLegacySql was also provided with arg to run_query\(\) method\. "
            r"Please remove duplicates\."
        ):
            self.hook.run_query('query',
                                use_legacy_sql=True,
                                api_resource_configs={'query': {'useLegacySql': False}}
                                )

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
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_load_with_non_csv_as_src_fmt(self, fmt, mock_get_service, mock_rwc):

        try:
            self.hook.run_load(
                destination_project_dataset_table='my_dataset.my_table',
                source_uris=[],
                source_format=fmt,
                autodetect=True
            )
        except ValueError:
            self.fail("run_load() raised ValueError unexpectedly!")

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_extract(self, run_with_config, mock_get_service):
        source_project_dataset_table = "{}.{}.{}".format(PROJECT_ID, DATASET_ID, TABLE_ID)
        destination_cloud_storage_uris = ["gs://bucket/file.csv"]
        expected_configuration = {
            "extract": {
                "sourceTable": {
                    "projectId": PROJECT_ID,
                    "datasetId": DATASET_ID,
                    "tableId": TABLE_ID,
                },
                "compression": "NONE",
                "destinationUris": destination_cloud_storage_uris,
                "destinationFormat": "CSV",
                "fieldDelimiter": ",",
                "printHeader": True,
            }
        }

        self.hook.run_extract(
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris
        )
        run_with_config.assert_called_once_with(expected_configuration)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_get_tabledata_default_parameters(self, mock_get_service):
        method = mock_get_service.return_value.tabledata.return_value.list
        cursor = self.hook.get_cursor()
        cursor.get_tabledata(DATASET_ID, TABLE_ID)
        method.assert_called_once_with(datasetId=DATASET_ID, projectId=PROJECT_ID, tableId=TABLE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_get_tabledata_optional_parameters(self, mock_get_service):
        method = mock_get_service.return_value.tabledata.return_value.list
        cursor = self.hook.get_cursor()
        cursor.get_tabledata(
            DATASET_ID,
            TABLE_ID,
            max_results=10,
            selected_fields=["field_1", "field_2"],
            page_token="page123",
            start_index=5
        )
        method.assert_called_once_with(
            projectId=PROJECT_ID,
            datasetId=DATASET_ID,
            tableId=TABLE_ID,
            maxResults=10,
            selectedFields=['field_1', 'field_2'],
            pageToken='page123',
            startIndex=5
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_run_table_delete(self, mock_get_service,):
        source_project_dataset_table = "{}.{}.{}".format(PROJECT_ID, DATASET_ID, TABLE_ID)
        method = mock_get_service.return_value.tables.return_value.delete
        self.hook.run_table_delete(source_project_dataset_table)
        method.assert_called_once_with(datasetId=DATASET_ID, projectId=PROJECT_ID, tableId=TABLE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_run_table_delete_ignore_if_missing_fails(self, mock_get_service):
        source_project_dataset_table = "{}.{}.{}".format(PROJECT_ID, DATASET_ID, TABLE_ID)
        method = mock_get_service.return_value.tables.return_value.delete
        resp = type('', (object,), {"status": 404, "reason": "Address not found"})()
        method.return_value.execute.side_effect = HttpError(
            resp=resp, content=b'Address not found')
        with self.assertRaisesRegex(HttpError, r"Address not found"):
            self.hook.run_table_delete(source_project_dataset_table)
        method.assert_called_once_with(datasetId=DATASET_ID, projectId=PROJECT_ID, tableId=TABLE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_run_table_delete_ignore_if_missing_pass(self, mock_get_service):
        source_project_dataset_table = "{}.{}.{}".format(PROJECT_ID, DATASET_ID, TABLE_ID)
        method = mock_get_service.return_value.tables.return_value.delete
        resp = type('', (object,), {"status": 404, "reason": "Address not found"})()
        method.return_value.execute.side_effect = HttpError(
            resp=resp, content=b'Address not found')
        self.hook.run_table_delete(source_project_dataset_table, ignore_if_missing=True)
        method.assert_called_once_with(datasetId=DATASET_ID, projectId=PROJECT_ID, tableId=TABLE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_table_upsert_create_new_table(self, mock_get_service):
        table_resource = {
            "tableReference": {
                "tableId": TABLE_ID
            }
        }

        method_tables_list = mock_get_service.return_value.tables.return_value.list
        method_tables_list_execute = method_tables_list.return_value.execute
        method_tables_insert = mock_get_service.return_value.tables.return_value.insert
        method_tables_insert_execute = method_tables_insert.return_value.execute

        self.hook.run_table_upsert(dataset_id=DATASET_ID, table_resource=table_resource)

        method_tables_list.assert_called_once_with(datasetId=DATASET_ID, projectId=PROJECT_ID)
        method_tables_insert.assert_called_once_with(
            datasetId=DATASET_ID, projectId=PROJECT_ID, body=table_resource
        )
        assert method_tables_list_execute.call_count == 1
        assert method_tables_insert_execute.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_table_upsert_already_exists(self, mock_get_service):
        page_token = "token"
        table_resource = {
            "tableReference": {
                "tableId": TABLE_ID
            }
        }

        method_tables_list = mock_get_service.return_value.tables.return_value.list
        method_tables_list_execute = method_tables_list.return_value.execute
        method_tables_list_execute.side_effect = [
            {
                "tables": [{"tableReference": {"tableId": "some_id"}}],
                "nextPageToken": page_token
            },
            {
                "tables": [{"tableReference": {"tableId": TABLE_ID}}]
            }
        ]
        method_tables_update = mock_get_service.return_value.tables.return_value.update
        method_tables_update_execute = method_tables_update.return_value.execute

        self.hook.run_table_upsert(dataset_id=DATASET_ID, table_resource=table_resource)

        method_tables_list.has_calls(
            mock.call(datasetId=DATASET_ID, projectId=PROJECT_ID),
            mock.call(datasetId=DATASET_ID, projectId=PROJECT_ID, pageToken=page_token),
        )
        method_tables_update.assert_called_once_with(
            projectId=PROJECT_ID, datasetId=DATASET_ID, tableId=TABLE_ID, body=table_resource)
        assert method_tables_list_execute.call_count == 2
        assert method_tables_update_execute.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_run_grant_dataset_view_access_granting(self, mock_get_service):
        source_dataset = "source_dataset_{}".format(DATASET_ID)
        view_dataset = "view_dataset_{}".format(DATASET_ID)
        view = {"projectId": PROJECT_ID, "datasetId": view_dataset, "tableId": TABLE_ID}
        access = [{"view": view}]
        body = {"access": access}

        method_get = mock_get_service.return_value.datasets.return_value.get
        method_get_execute = method_get.return_value.execute
        method_get_execute.return_value = []
        method_patch = mock_get_service.return_value.datasets.return_value.patch
        method_patch_execute = method_patch.return_value.execute

        self.hook.run_grant_dataset_view_access(
            source_dataset=source_dataset,
            view_dataset=view_dataset,
            view_table=TABLE_ID
        )

        method_get.assert_called_once_with(datasetId=source_dataset, projectId=PROJECT_ID)
        method_patch.assert_called_once_with(datasetId=source_dataset, projectId=PROJECT_ID, body=body)
        assert method_get.call_count == 1
        assert method_get_execute.call_count == 1
        assert method_patch.call_count == 1
        assert method_patch_execute.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_run_grant_dataset_view_access_already_granted(self, mock_get_service):
        source_dataset = "source_dataset_{}".format(DATASET_ID)
        view_dataset = "view_dataset_{}".format(DATASET_ID)
        view = {"projectId": PROJECT_ID, "datasetId": view_dataset, "tableId": TABLE_ID}
        access = [{"view": view}]
        body = {"access": access}

        method_get = mock_get_service.return_value.datasets.return_value.get
        method_get_execute = method_get.return_value.execute
        method_get_execute.return_value = body
        method_patch = mock_get_service.return_value.datasets.return_value.patch

        self.hook.run_grant_dataset_view_access(
            source_dataset=source_dataset,
            view_dataset=view_dataset,
            view_table=TABLE_ID
        )

        method_get.assert_called_once_with(datasetId=source_dataset, projectId=PROJECT_ID)
        assert method_get.call_count == 1
        assert method_get_execute.call_count == 1
        method_patch.assert_not_called()

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_get_dataset_tables_list(self, mock_client):
        table_list = [
            {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": "a-1"},
            {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": "b-1"},
            {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": "a-2"},
            {"projectId": PROJECT_ID, "datasetId": DATASET_ID, "tableId": "b-2"},
        ]
        table_list_response = [Table.from_api_repr({"tableReference": t}) for t in table_list]
        mock_client.return_value.list_tables.return_value = table_list_response

        dataset_reference = DatasetReference(PROJECT_ID, DATASET_ID)
        result = self.hook.get_dataset_tables_list(dataset_id=DATASET_ID, project_id=PROJECT_ID)

        mock_client.return_value.list_tables.assert_called_once_with(
            dataset=dataset_reference,
            max_results=None
        )
        self.assertEqual(table_list, result)

    @parameterized.expand([
        ("US", None, True),
        (None, None, True),
        (
            None,
            HttpError(resp=type('', (object,), {"status": 500, })(), content=b'Internal Server Error'),
            False
        ),
        (
            None,
            HttpError(resp=type('', (object,), {"status": 503, })(), content=b'Service Unavailable'),
            False
        ),
    ])
    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID)
    )
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_poll_job_complete_pass(
        self, location, exception, expected_result, mock_get_service, mock_get_creds_and_proj_id
    ):
        method_jobs = mock_get_service.return_value.jobs
        method_get = method_jobs.return_value.get
        method_execute = method_get.return_value.execute
        method_execute.return_value = {"status": {"state": "DONE"}}
        method_execute.side_effect = exception

        hook_params = {"location": location} if location else {}
        bq_hook = BigQueryHook(**hook_params)

        result = bq_hook.poll_job_complete(JOB_ID)
        self.assertEqual(expected_result, result)
        method_get.assert_called_once_with(projectId=PROJECT_ID, jobId=JOB_ID, **hook_params)
        assert method_jobs.call_count == 1
        assert method_get.call_count == 1
        assert method_execute.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_pull_job_complete_on_fails(self, mock_get_service):
        method_jobs = mock_get_service.return_value.jobs
        method_get = method_jobs.return_value.get
        method_execute = method_get.return_value.execute
        resp = type('', (object,), {"status": 404, "reason": "Not Found"})()
        method_execute.side_effect = HttpError(resp=resp, content=b'Not Found')

        with self.assertRaisesRegex(HttpError, "HttpError 404 \"Not Found\""):
            self.hook.poll_job_complete(JOB_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.poll_job_complete")
    @mock.patch("logging.Logger.info")
    def test_cancel_query_np_jobs_to_cancel(
        self, mock_logger_info, poll_job_complete, mock_get_service,
    ):
        method_jobs = mock_get_service.return_value.jobs
        poll_job_complete.return_value = True

        self.hook.running_job_id = JOB_ID
        self.hook.cancel_query()
        assert method_jobs.call_count == 1
        assert poll_job_complete.call_count == 1
        mock_logger_info.has_call(mock.call("No running BigQuery jobs to cancel."))

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.poll_job_complete")
    @mock.patch("time.sleep")
    @mock.patch("logging.Logger.info")
    def test_cancel_query_np_cancel_timeout(
        self, mock_logger_info, mock_sleep, poll_job_complete, mock_get_service,
    ):
        method_jobs = mock_get_service.return_value.jobs
        method_jobs_cancel = method_jobs.return_value.cancel
        poll_job_complete.side_effect = [False] * 13

        self.hook.running_job_id = JOB_ID
        self.hook.cancel_query()
        assert method_jobs.call_count == 1
        assert method_jobs_cancel.call_count == 1
        assert poll_job_complete.call_count == 13
        assert mock_sleep.call_count == 11
        mock_logger_info.has_call(
            mock.call("Stopping polling due to timeout. Job with id {} "
                      "has not completed cancel and may or may not finish.".format(JOB_ID))
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.poll_job_complete")
    @mock.patch("time.sleep")
    @mock.patch("logging.Logger.info")
    def test_cancel_query_np_cancel_completed(
        self, mock_logger_info, mock_sleep, poll_job_complete, mock_get_service,
    ):
        method_jobs = mock_get_service.return_value.jobs
        method_jobs_cancel = method_jobs.return_value.cancel
        poll_job_complete.side_effect = [False] * 12 + [True]

        self.hook.running_job_id = JOB_ID
        self.hook.cancel_query()
        assert method_jobs.call_count == 1
        assert method_jobs_cancel.call_count == 1
        assert poll_job_complete.call_count == 13
        assert mock_sleep.call_count == 11
        mock_logger_info.has_call(mock.call("Job successfully canceled: {}, {}".format(PROJECT_ID, JOB_ID)))

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_get_schema(self, mock_get_service):
        schema = "SCHEMA"
        method_get = mock_get_service.return_value.tables.return_value.get
        method_execute = method_get.return_value.execute
        method_execute.return_value = {"schema": schema}

        result = self.hook.get_schema(dataset_id=DATASET_ID, table_id=TABLE_ID)

        method_get.assert_called_once_with(projectId=PROJECT_ID, datasetId=DATASET_ID, tableId=TABLE_ID)
        assert method_execute.call_count == 1
        self.assertEqual(schema, result)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_get_schema_other_project(self, mock_get_service,):
        other_project = "another-project"
        schema = "SCHEMA"
        method_get = mock_get_service.return_value.tables.return_value.get
        method_execute = method_get.return_value.execute
        method_execute.return_value = {"schema": schema}

        cursor = self.hook.get_cursor()
        result = cursor.get_schema(dataset_id=DATASET_ID, table_id=TABLE_ID, project_id=other_project)

        method_get.assert_called_once_with(projectId=other_project, datasetId=DATASET_ID, tableId=TABLE_ID)
        assert method_execute.call_count == 1
        self.assertEqual(schema, result)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_invalid_source_format(self, mock_get_service):
        with self.assertRaisesRegex(
            Exception,
            r"JSON is not a valid source format. Please use one of the following types: \['CSV', "
            r"'NEWLINE_DELIMITED_JSON', 'AVRO', 'GOOGLE_SHEETS', 'DATASTORE_BACKUP', 'PARQUET'\]"
        ):
            self.hook.run_load(
                "test.test", "test_schema.json", ["test_data.json"], source_format="json"
            )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_external_table_invalid_source_format(self, mock_get_service):
        with self.assertRaisesRegex(
            Exception,
            r"JSON is not a valid source format. Please use one of the following types: \['CSV', "
            r"'NEWLINE_DELIMITED_JSON', 'AVRO', 'GOOGLE_SHEETS', 'DATASTORE_BACKUP', 'PARQUET'\]"
        ):
            self.hook.create_external_table(
                external_project_dataset_table='test.test',
                schema_fields='test_schema.json',
                source_uris=['test_data.json'],
                source_format='json'
            )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_insert_all_succeed(self, mock_get_service):
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

        self.hook.insert_all(PROJECT_ID, DATASET_ID, TABLE_ID, rows)
        method.assert_called_once_with(
            projectId=PROJECT_ID,
            datasetId=DATASET_ID,
            tableId=TABLE_ID,
            body=body
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_insert_all_fail(self, mock_get_service):
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
        with self.assertRaisesRegex(
            Exception,
            r"BigQuery job failed\. Error was: 1 insert error\(s\) occurred: "
            r"bq-project:bq_dataset\.bq_table\. Details: \[{'index': 1, 'errors': \[\]}\]"
        ):
            self.hook.insert_all(PROJECT_ID, DATASET_ID, TABLE_ID, rows, fail_on_error=True)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_query_with_arg(self, mocked_rwc, mock_get_service):
        def run_with_config(config):
            self.assertEqual(
                config['labels'], {'label1': 'test1', 'label2': 'test2'}
            )
        mocked_rwc.side_effect = run_with_config

        self.hook.run_query(
            sql='select 1',
            destination_dataset_table='my_dataset.my_table',
            labels={'label1': 'test1', 'label2': 'test2'}
        )

        assert mocked_rwc.call_count == 1


class TestBigQueryTableSplitter(unittest.TestCase):
    def test_internal_need_default_project(self):
        with self.assertRaisesRegex(Exception, "INTERNAL: No default project is specified"):
            _split_tablename("dataset.table", None)

    @parameterized.expand([
        ("project", "dataset", "table", "dataset.table"),
        ("alternative", "dataset", "table", "alternative:dataset.table"),
        ("alternative", "dataset", "table", "alternative.dataset.table"),
        ("alt1:alt", "dataset", "table", "alt1:alt.dataset.table"),
        ("alt1:alt", "dataset", "table", "alt1:alt:dataset.table"),
    ])
    def test_split_tablename(self, project_expected, dataset_expected, table_expected, table_input):
        default_project_id = "project"
        project, dataset, table = _split_tablename(table_input, default_project_id)
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
            _split_tablename(table_input, default_project_id, var_name)


class TestTableOperations(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_view(self, mock_bq_client, mock_table):
        view = {
            'query': 'SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*`',
            "useLegacySql": False
        }

        self.hook.create_empty_table(
            project_id=PROJECT_ID, dataset_id=DATASET_ID, table_id=TABLE_ID, view=view, retry=DEFAULT_RETRY
        )
        body = {
            'tableReference': TABLE_REFERENCE_REPR,
            'view': view
        }
        mock_table.from_api_repr.assert_called_once_with(body)
        mock_bq_client.return_value.create_table.assert_called_once_with(
            table=mock_table.from_api_repr.return_value,
            exists_ok=True,
            retry=DEFAULT_RETRY,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_patch_table(self, mock_get_service):
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
        self.hook.patch_table(
            DATASET_ID, TABLE_ID, PROJECT_ID,
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
            projectId=PROJECT_ID,
            datasetId=DATASET_ID,
            tableId=TABLE_ID,
            body=body
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_patch_table_on_exception(self, mock_get_service):
        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.patch
        resp = type('', (object,), {"status": 500, "reason": "Bad request"})()
        method.return_value.execute.side_effect = HttpError(
            resp=resp, content=b'Bad request')
        with self.assertRaisesRegex(HttpError, "Bad request"):
            self.hook.patch_table(DATASET_ID, TABLE_ID)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_patch_view(self, mock_get_service):
        view_patched = {
            'query': "SELECT * FROM `test-project-id.test_dataset_id.test_table_prefix*` LIMIT 500",
            'useLegacySql': False
        }

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.patch
        self.hook.patch_table(DATASET_ID, VIEW_ID, PROJECT_ID, view=view_patched)

        body = {
            'view': view_patched
        }
        method.assert_called_once_with(
            projectId=PROJECT_ID,
            datasetId=DATASET_ID,
            tableId=VIEW_ID,
            body=body
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_empty_table_succeed(self, mock_bq_client, mock_table):
        self.hook.create_empty_table(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID)

        body = {
            'tableReference': {
                'tableId': TABLE_ID,
                'projectId': PROJECT_ID,
                'datasetId': DATASET_ID,
            }
        }
        mock_table.from_api_repr.assert_called_once_with(body)
        mock_bq_client.return_value.create_table.assert_called_once_with(
            table=mock_table.from_api_repr.return_value,
            exists_ok=True,
            retry=DEFAULT_RETRY
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_empty_table_with_extras_succeed(self, mock_bq_client, mock_table):
        schema_fields = [
            {'name': 'id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'name', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'created', 'type': 'DATE', 'mode': 'REQUIRED'},
        ]
        time_partitioning = {"field": "created", "type": "DAY"}
        cluster_fields = ['name']

        self.hook.create_empty_table(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            schema_fields=schema_fields,
            time_partitioning=time_partitioning,
            cluster_fields=cluster_fields
        )

        body = {
            'tableReference': {
                'tableId': TABLE_ID,
                'projectId': PROJECT_ID,
                'datasetId': DATASET_ID,
            },
            'schema': {
                'fields': schema_fields
            },
            'timePartitioning': time_partitioning,
            'clustering': {
                'fields': cluster_fields
            }
        }
        mock_table.from_api_repr.assert_called_once_with(body)
        mock_bq_client.return_value.create_table.assert_called_once_with(
            table=mock_table.from_api_repr.return_value,
            exists_ok=True,
            retry=DEFAULT_RETRY
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_get_tables_list(self, mock_client):
        table_list = [
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
        ]
        table_list_response = [Table.from_api_repr(t) for t in table_list]
        mock_client.return_value.list_tables.return_value = table_list_response

        dataset_reference = DatasetReference(PROJECT_ID, DATASET_ID)
        result = self.hook.get_dataset_tables(dataset_id=DATASET_ID, project_id=PROJECT_ID)

        mock_client.return_value.list_tables.assert_called_once_with(
            dataset=dataset_reference,
            max_results=None,
            page_token=None,
            retry=DEFAULT_RETRY,
        )
        for res, exp in zip(result, table_list):
            assert res.full_table_id == exp["id"]

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_table_upsert_on_insert(self, mock_get_service):
        table_resource = {
            "tableReference": {
                "tableId": "test-table-id"
            },
            "expirationTime": 123456
        }
        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.insert
        self.hook.run_table_upsert(
            dataset_id=DATASET_ID,
            table_resource=table_resource,
            project_id=PROJECT_ID
        )

        method.assert_called_once_with(
            body=table_resource,
            datasetId=DATASET_ID,
            projectId=PROJECT_ID
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_table_upsert_on_update(self, mock_get_service):
        table_resource = {
            "tableReference": {
                "tableId": "table1"
            },
            "expirationTime": 123456
        }
        table1 = "table1"
        table2 = "table2"
        expected_tables_list = {'tables': [
            {
                "creationTime": "12345678",
                "kind": "bigquery#table",
                "type": "TABLE",
                "id": "{project}:{dataset}.{table}".format(
                    project=PROJECT_ID,
                    dataset=DATASET_ID,
                    table=table1),
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "tableId": table1,
                    "datasetId": DATASET_ID
                }
            },
            {
                "creationTime": "12345678",
                "kind": "bigquery#table",
                "type": "TABLE",
                "id": "{project}:{dataset}.{table}".format(
                    project=PROJECT_ID,
                    dataset=DATASET_ID,
                    table=table2),
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "tableId": table2,
                    "datasetId": DATASET_ID
                }
            }
        ]}
        mock_service = mock_get_service.return_value
        mock_service.tables.return_value.list.return_value.execute.return_value = expected_tables_list
        method = mock_service.tables.return_value.update
        self.hook.run_table_upsert(
            dataset_id=DATASET_ID,
            table_resource=table_resource,
            project_id=PROJECT_ID
        )

        method.assert_called_once_with(
            body=table_resource,
            datasetId=DATASET_ID,
            projectId=PROJECT_ID,
            tableId=table1
        )


class TestBigQueryCursor(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_execute_with_parameters(self, mocked_rwc, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.execute("SELECT %(foo)s", {"foo": "bar"})
        assert mocked_rwc.call_count == 1
        assert mocked_rwc.has_calls(
            mock.call(
                {
                    'query': {
                        'query': "SELECT 'bar'",
                        'priority': 'INTERACTIVE',
                        'useLegacySql': True,
                        'schemaUpdateOptions': []
                    }
                }
            )
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_execute_many(self, mocked_rwc, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.executemany("SELECT %(foo)s", [{"foo": "bar"}, {"foo": "baz"}])
        assert mocked_rwc.call_count == 2
        assert mocked_rwc.has_calls(
            mock.call(
                {
                    'query': {
                        'query': "SELECT 'bar'",
                        'priority': 'INTERACTIVE',
                        'useLegacySql': True,
                        'schemaUpdateOptions': []
                    }
                }
            ),
            mock.call(
                {
                    'query': {
                        'query': "SELECT 'baz'",
                        'priority': 'INTERACTIVE',
                        'useLegacySql': True,
                        'schemaUpdateOptions': []
                    }
                }
            )
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_description(self, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        with self.assertRaises(NotImplementedError):
            bq_cursor.description

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_close(self, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        result = bq_cursor.close()  # pylint: disable=assignment-from-no-return
        self.assertIsNone(result)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_rowcunt(self, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        result = bq_cursor.rowcount
        self.assertEqual(-1, result)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor.next")
    def test_fetchone(self, mock_next, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        result = bq_cursor.fetchone()
        mock_next.call_count == 1
        self.assertEqual(mock_next.return_value, result)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch(
        "airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor.fetchone",
        side_effect=[1, 2, 3, None]
    )
    def test_fetchall(self, mock_fetchone, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        result = bq_cursor.fetchall()
        self.assertEqual([1, 2, 3], result)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor.fetchone")
    def test_fetchmany(self, mock_fetchone, mock_get_service):
        side_effect_values = [1, 2, 3, None]
        bq_cursor = self.hook.get_cursor()
        mock_fetchone.side_effect = side_effect_values
        result = bq_cursor.fetchmany()
        self.assertEqual([1], result)

        mock_fetchone.side_effect = side_effect_values
        result = bq_cursor.fetchmany(2)
        self.assertEqual([1, 2], result)

        mock_fetchone.side_effect = side_effect_values
        result = bq_cursor.fetchmany(5)
        self.assertEqual([1, 2, 3], result)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_next_no_jobid(self, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.job_id = None
        result = bq_cursor.next()
        self.assertIsNone(result)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_next_buffer(self, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.job_id = JOB_ID
        bq_cursor.buffer = [1, 2]
        result = bq_cursor.next()
        self.assertEqual(1, result)
        result = bq_cursor.next()
        self.assertEqual(2, result)
        bq_cursor.all_pages_loaded = True
        result = bq_cursor.next()
        self.assertIsNone(result)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_next(self, mock_get_service):
        mock_get_query_results = mock_get_service.return_value.jobs.return_value.getQueryResults
        mock_execute = mock_get_query_results.return_value.execute
        mock_execute.return_value = {
            "rows": [
                {"f": [{"v": "one"}, {"v": 1}]},
                {"f": [{"v": "two"}, {"v": 2}]},
            ],
            "pageToken": None,
            "schema": {
                "fields": [
                    {"name": "field_1", "type": "STRING"},
                    {"name": "field_2", "type": "INTEGER"},
                ]
            }
        }

        bq_cursor = self.hook.get_cursor()
        bq_cursor.job_id = JOB_ID
        bq_cursor.location = LOCATION

        result = bq_cursor.next()
        self.assertEqual(['one', 1], result)

        result = bq_cursor.next()
        self.assertEqual(['two', 2], result)

        mock_get_query_results.assert_called_once_with(jobId=JOB_ID, location=LOCATION, pageToken=None,
                                                       projectId='bq-project')
        mock_execute.assert_called_once_with(num_retries=bq_cursor.num_retries)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor.flush_results")
    def test_next_no_rows(self, mock_flush_results, mock_get_service):
        mock_get_query_results = mock_get_service.return_value.jobs.return_value.getQueryResults
        mock_execute = mock_get_query_results.return_value.execute
        mock_execute.return_value = {}

        bq_cursor = self.hook.get_cursor()
        bq_cursor.job_id = JOB_ID

        result = bq_cursor.next()

        self.assertIsNone(result)
        mock_get_query_results.assert_called_once_with(jobId=JOB_ID, location=None, pageToken=None,
                                                       projectId='bq-project')
        mock_execute.assert_called_once_with(num_retries=bq_cursor.num_retries)
        assert mock_flush_results.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryCursor.flush_results")
    def test_flush_cursor_in_execute(self, _, mocked_fr, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.execute("SELECT %(foo)s", {"foo": "bar"})
        assert mocked_fr.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_flush_cursor(self, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        bq_cursor.page_token = '456dcea9-fcbf-4f02-b570-83f5297c685e'
        bq_cursor.job_id = 'c0a79ae4-0e72-4593-a0d0-7dbbf726f193'
        bq_cursor.all_pages_loaded = True
        bq_cursor.buffer = [('a', 100, 200), ('b', 200, 300)]
        bq_cursor.flush_results()
        self.assertIsNone(bq_cursor.page_token)
        self.assertIsNone(bq_cursor.job_id)
        self.assertFalse(bq_cursor.all_pages_loaded)
        self.assertListEqual(bq_cursor.buffer, [])

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_arraysize(self, mock_get_service):
        bq_cursor = self.hook.get_cursor()
        self.assertIsNone(bq_cursor.buffersize)
        self.assertEqual(bq_cursor.arraysize, 1)
        bq_cursor.set_arraysize(10)
        self.assertEqual(bq_cursor.buffersize, 10)
        self.assertEqual(bq_cursor.arraysize, 10)


class TestDatasetsOperations(_BigQueryBaseTestClass):
    def test_create_empty_dataset_no_dataset_id_err(self):
        with self.assertRaisesRegex(ValueError, r"Please specify `datasetId`"):
            self.hook.create_empty_dataset(dataset_id=None, project_id=None)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_empty_dataset_with_params(self, mock_client, mock_dataset):
        self.hook.create_empty_dataset(project_id=PROJECT_ID, dataset_id=DATASET_ID, location=LOCATION)
        expected_body = {
            "location": LOCATION,
            "datasetReference": {
                "datasetId": DATASET_ID,
                "projectId": PROJECT_ID
            }
        }

        api_repr = mock_dataset.from_api_repr
        api_repr.assert_called_once_with(expected_body)
        mock_client.return_value.create_dataset.assert_called_once_with(
            dataset=api_repr.return_value,
            exists_ok=True
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_empty_dataset_with_object(self, mock_client, mock_dataset):
        dataset = {
            "location": "LOCATION",
            "datasetReference": {
                "datasetId": "DATASET_ID",
                "projectId": "PROJECT_ID"
            }
        }
        self.hook.create_empty_dataset(dataset_reference=dataset)

        api_repr = mock_dataset.from_api_repr
        api_repr.assert_called_once_with(dataset)
        mock_client.return_value.create_dataset.assert_called_once_with(
            dataset=api_repr.return_value,
            exists_ok=True
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_empty_dataset_use_values_from_object(self, mock_client, mock_dataset):
        dataset = {
            "location": "LOCATION",
            "datasetReference": {
                "datasetId": "DATASET_ID",
                "projectId": "PROJECT_ID"
            }
        }
        self.hook.create_empty_dataset(
            dataset_reference=dataset,
            location="Unknown location",
            dataset_id="Fashionable Dataset",
            project_id="Amazing Project",
        )

        api_repr = mock_dataset.from_api_repr
        api_repr.assert_called_once_with(dataset)
        mock_client.return_value.create_dataset.assert_called_once_with(
            dataset=api_repr.return_value,
            exists_ok=True
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_get_dataset(self, mock_client):
        _expected_result = {
            "kind": "bigquery#dataset",
            "location": "US",
            "id": "your-project:dataset_2_test",
            "datasetReference": {
                "projectId": "your-project",
                "datasetId": "dataset_2_test"
            }
        }
        expected_result = Dataset.from_api_repr(_expected_result)
        mock_client.return_value.get_dataset.return_value = expected_result

        result = self.hook.get_dataset(dataset_id=DATASET_ID, project_id=PROJECT_ID)
        mock_client.return_value.get_dataset.assert_called_once_with(
            dataset_ref=DatasetReference(PROJECT_ID, DATASET_ID)
        )

        self.assertEqual(result, expected_result)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_get_datasets_list(self, mock_client):
        datasets = [
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
        ]
        return_value = [DatasetListItem(d) for d in datasets]
        mock_client.return_value.list_datasets.return_value = return_value

        result = self.hook.get_datasets_list(project_id=PROJECT_ID)

        mock_client.return_value.list_datasets.assert_called_once_with(
            project=PROJECT_ID,
            include_all=False,
            filter=None,
            max_results=None,
            page_token=None,
            retry=DEFAULT_RETRY,
        )
        for exp, res in zip(datasets, result):
            assert res.full_dataset_id == exp["id"]

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_delete_dataset(self, mock_client):
        delete_contents = True
        self.hook.delete_dataset(
            project_id=PROJECT_ID, dataset_id=DATASET_ID, delete_contents=delete_contents
        )

        mock_client.return_value.delete_dataset.assert_called_once_with(
            dataset=DatasetReference(PROJECT_ID, DATASET_ID),
            delete_contents=delete_contents,
            retry=DEFAULT_RETRY,
            not_found_ok=True,
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_patch_dataset(self, mock_get_service):
        dataset_resource = {
            "access": [
                {
                    "role": "WRITER",
                    "groupByEmail": "cloud-logs@google.com"
                }
            ]
        }

        method = mock_get_service.return_value.datasets.return_value.patch
        self.hook.patch_dataset(
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            dataset_resource=dataset_resource
        )

        method.assert_called_once_with(
            projectId=PROJECT_ID,
            datasetId=DATASET_ID,
            body=dataset_resource
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Dataset")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_update_dataset(self, mock_client, mock_dataset):
        dataset_resource = {
            "kind": "bigquery#dataset",
            "location": "US",
            "id": "your-project:dataset_2_test",
            "datasetReference": {
                "projectId": "your-project",
                "datasetId": "dataset_2_test"
            }
        }

        method = mock_client.return_value.update_dataset
        dataset = Dataset.from_api_repr(dataset_resource)
        mock_dataset.from_api_repr.return_value = dataset
        method.return_value = dataset

        result = self.hook.update_dataset(
            dataset_id=DATASET_ID,
            project_id=PROJECT_ID,
            dataset_resource=dataset_resource,
            fields=["location"]
        )

        mock_dataset.from_api_repr.assert_called_once_with(dataset_resource)
        method.assert_called_once_with(
            dataset=dataset,
            fields=["location"],
            retry=DEFAULT_RETRY,
        )
        assert result == dataset


class TestTimePartitioningInRunJob(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_load_default(self, mocked_rwc, mock_get_service):
        def run_with_config(config):
            self.assertIsNone(config['load'].get('timePartitioning'))

        mocked_rwc.side_effect = run_with_config

        self.hook.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
        )

        assert mocked_rwc.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_with_auto_detect(self, run_with_config, mock_get_service):
        destination_project_dataset_table = "autodetect.table"
        self.hook.run_load(destination_project_dataset_table, [], [], autodetect=True)
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['load']['autodetect'], True)

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_load_with_arg(self, mocked_rwc, mock_get_service):
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

        self.hook.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
            time_partitioning={'type': 'DAY', 'field': 'test_field', 'expirationMs': 1000}
        )

        assert mocked_rwc.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_query_default(self, mocked_rwc, mock_get_service):
        def run_with_config(config):
            self.assertIsNone(config['query'].get('timePartitioning'))

        mocked_rwc.side_effect = run_with_config

        self.hook.run_query(sql='select 1')

        assert mocked_rwc.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_query_with_arg(self, mocked_rwc, mock_get_service):
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

        self.hook.run_query(
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


class TestClusteringInRunJob(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_load_default(self, mocked_rwc, mock_get_service):
        def run_with_config(config):
            self.assertIsNone(config['load'].get('clustering'))

        mocked_rwc.side_effect = run_with_config

        self.hook.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
        )

        assert mocked_rwc.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_load_with_arg(self, mocked_rwc, mock_get_service):
        def run_with_config(config):
            self.assertEqual(
                config['load']['clustering'],
                {
                    'fields': ['field1', 'field2']
                }
            )

        mocked_rwc.side_effect = run_with_config

        self.hook.run_load(
            destination_project_dataset_table='my_dataset.my_table',
            schema_fields=[],
            source_uris=[],
            cluster_fields=['field1', 'field2'],
            time_partitioning={'type': 'DAY'}
        )

        assert mocked_rwc.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_query_default(self, mocked_rwc, mock_get_service):
        def run_with_config(config):
            self.assertIsNone(config['query'].get('clustering'))

        mocked_rwc.side_effect = run_with_config

        self.hook.run_query(sql='select 1')

        assert mocked_rwc.call_count == 1

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_query_with_arg(self, mocked_rwc, mock_get_service):
        def run_with_config(config):
            self.assertEqual(
                config['query']['clustering'],
                {
                    'fields': ['field1', 'field2']
                }
            )

        mocked_rwc.side_effect = run_with_config

        self.hook.run_query(
            sql='select 1',
            destination_dataset_table='my_dataset.my_table',
            cluster_fields=['field1', 'field2'],
            time_partitioning={'type': 'DAY'}
        )

        self.assertEqual(mocked_rwc.call_count, 1, "run_with_configuration() was not called exactly once")


class TestBigQueryHookLegacySql(_BigQueryBaseTestClass):
    """Ensure `use_legacy_sql` param in `BigQueryHook` propagates properly."""

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_hook_uses_legacy_sql_by_default(
        self, run_with_config, mock_get_service
    ):
        self.hook.get_first('query')
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], True)

    @mock.patch(
        'airflow.providers.google.common.hooks.base_google.GoogleBaseHook._get_credentials_and_project_id',
        return_value=(CREDENTIALS, PROJECT_ID)
    )
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_legacy_sql_override_propagates_properly(
        self, run_with_config, mock_get_service, mock_get_creds_and_proj_id
    ):
        bq_hook = BigQueryHook(use_legacy_sql=False)
        bq_hook.get_first('query')
        args, kwargs = run_with_config.call_args
        self.assertIs(args[0]['query']['useLegacySql'], False)


class TestBigQueryHookRunWithConfiguration(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_run_with_configuration_location(self, mock_get_service):
        running_job_id = 'job_vjdi28vskdui2onru23'
        location = 'asia-east1'

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

        self.hook.running_job_id = running_job_id
        self.hook.run_with_configuration({})

        method.assert_called_once_with(
            projectId=PROJECT_ID,
            jobId=running_job_id,
            location=location
        )


class TestBigQueryWithKMS(_BigQueryBaseTestClass):
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Table")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.Client")
    def test_create_empty_table_with_kms(self, mock_bq_client, mock_table):
        schema_fields = [
            {"name": "id", "type": "STRING", "mode": "REQUIRED"}
        ]
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }

        self.hook.create_empty_table(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            schema_fields=schema_fields,
            encryption_configuration=encryption_configuration,
        )

        body = {
            "tableReference": {"tableId": TABLE_ID, 'projectId': PROJECT_ID, 'datasetId': DATASET_ID},
            "schema": {"fields": schema_fields},
            "encryptionConfiguration": encryption_configuration,
        }
        mock_table.from_api_repr.assert_called_once_with(body)
        mock_bq_client.return_value.create_table.assert_called_once_with(
            table=mock_table.from_api_repr.return_value,
            exists_ok=True,
            retry=DEFAULT_RETRY,
        )

    # pylint: disable=too-many-locals
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_create_external_table_with_kms(self, mock_get_service):
        external_project_dataset_table = "{}.{}.{}".format(
            PROJECT_ID, DATASET_ID, TABLE_ID
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

        self.hook.create_external_table(
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
                'projectId': PROJECT_ID,
                'datasetId': DATASET_ID,
                'tableId': TABLE_ID,
            },
            'labels': labels,
            "encryptionConfiguration": encryption_configuration,
        }
        method.assert_called_once_with(
            projectId=PROJECT_ID, datasetId=DATASET_ID, body=body
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    def test_patch_table_with_kms(self, mock_get_service):
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }

        mock_service = mock_get_service.return_value
        method = mock_service.tables.return_value.patch

        self.hook.patch_table(
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            project_id=PROJECT_ID,
            encryption_configuration=encryption_configuration
        )

        body = {
            "encryptionConfiguration": encryption_configuration
        }

        method.assert_called_once_with(
            projectId=PROJECT_ID,
            datasetId=DATASET_ID,
            tableId=TABLE_ID,
            body=body
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_query_with_kms(self, run_with_config, mock_get_service):
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }
        self.hook.run_query(
            sql='query',
            encryption_configuration=encryption_configuration
        )
        args, kwargs = run_with_config.call_args
        self.assertIs(
            args[0]['query']['destinationEncryptionConfiguration'],
            encryption_configuration
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_copy_with_kms(self, run_with_config, mock_get_service):
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }
        self.hook.run_copy(
            source_project_dataset_tables='p.d.st',
            destination_project_dataset_table='p.d.dt',
            encryption_configuration=encryption_configuration
        )
        args, kwargs = run_with_config.call_args
        self.assertIs(
            args[0]['copy']['destinationEncryptionConfiguration'],
            encryption_configuration
        )

    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.get_service")
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.run_with_configuration")
    def test_run_load_with_kms(self, run_with_config, mock_get_service):
        encryption_configuration = {
            "kms_key_name": "projects/p/locations/l/keyRings/k/cryptoKeys/c"
        }
        self.hook.run_load(
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


class TestBigQueryBaseCursorMethodsDeprecationWarning(unittest.TestCase):
    @parameterized.expand([
        ("create_empty_table",),
        ("create_empty_dataset",),
        ("get_dataset_tables",),
        ("delete_dataset",),
        ("create_external_table",),
        ("patch_table",),
        ("insert_all",),
        ("update_dataset",),
        ("patch_dataset",),
        ("get_dataset_tables_list",),
        ("get_datasets_list",),
        ("get_dataset",),
        ("run_grant_dataset_view_access",),
        ("run_table_upsert",),
        ("run_table_delete",),
        ("get_tabledata",),
        ("get_schema",),
        ("poll_job_complete",),
        ("cancel_query",),
        ("run_with_configuration",),
        ("run_load",),
        ("run_copy",),
        ("run_extract",),
        ("run_query",),
    ])
    @mock.patch("airflow.providers.google.cloud.hooks.bigquery.BigQueryHook")
    def test_deprecation_warning(self, func_name, mock_bq_hook):
        args, kwargs = [1], {"param1": "val1"}
        new_path = re.escape(f"`airflow.providers.google.cloud.hooks.bigquery.BigQueryHook.{func_name}`")
        message_pattern = r"This method is deprecated\.\s+Please use {}".format(new_path)
        message_regex = re.compile(message_pattern, re.MULTILINE)

        mocked_func = getattr(mock_bq_hook, func_name)
        bq_cursor = BigQueryCursor(mock.MagicMock(), PROJECT_ID, mock_bq_hook)
        func = getattr(bq_cursor, func_name)

        with self.assertWarnsRegex(DeprecationWarning, message_regex):
            _ = func(*args, **kwargs)

        mocked_func.assert_called_once_with(*args, **kwargs)
        self.assertRegex(func.__doc__, ".*{}.*".format(new_path))
