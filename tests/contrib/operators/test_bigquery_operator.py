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

from tests.compat import MagicMock
from datetime import datetime

import six

from airflow import models
from airflow.contrib.operators.bigquery_get_data import BigQueryGetDataOperator
from airflow.contrib.operators.bigquery_operator import \
    BigQueryCreateExternalTableOperator, BigQueryCreateEmptyTableOperator, \
    BigQueryDeleteDatasetOperator, BigQueryCreateEmptyDatasetOperator, \
    BigQueryOperator, BigQueryConsoleLink, BigQueryGetDatasetOperator, \
    BigQueryPatchDatasetOperator, BigQueryUpdateDatasetOperator
from airflow.contrib.operators.bigquery_table_delete_operator import \
    BigQueryTableDeleteOperator
from airflow.contrib.operators.bigquery_to_bigquery import \
    BigQueryToBigQueryOperator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.contrib.operators.bigquery_to_mysql_operator import BigQueryToMySqlOperator
from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskFail, TaskInstance, XCom
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.settings import Session
from airflow.utils.db import provide_session
from tests.compat import mock

if six.PY2:
    # Need `assertWarns` back-ported from unittest2
    import unittest2 as unittest
else:
    import unittest


TASK_ID = 'test-bq-create-table-operator'
TEST_DATASET = 'test-dataset'
TEST_GCP_PROJECT_ID = 'test-project'
TEST_DELETE_CONTENTS = True
TEST_TABLE_ID = 'test-table-id'
TEST_GCS_BUCKET = 'test-bucket'
TEST_GCS_DATA = ['dir1/*.csv']
TEST_SOURCE_FORMAT = 'CSV'
DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'test-bigquery-operators'


class BigQueryCreateEmptyTableOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryCreateEmptyTableOperator(task_id=TASK_ID,
                                                    dataset_id=TEST_DATASET,
                                                    project_id=TEST_GCP_PROJECT_ID,
                                                    table_id=TEST_TABLE_ID)

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .create_empty_table \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                table_id=TEST_TABLE_ID,
                schema_fields=None,
                time_partitioning={},
                labels=None,
                encryption_configuration=None
            )


class BigQueryCreateExternalTableOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryCreateExternalTableOperator(
            task_id=TASK_ID,
            destination_project_dataset_table='{}.{}'.format(
                TEST_DATASET, TEST_TABLE_ID
            ),
            schema_fields=[],
            bucket=TEST_GCS_BUCKET,
            source_objects=TEST_GCS_DATA,
            source_format=TEST_SOURCE_FORMAT
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .create_external_table \
            .assert_called_once_with(
                external_project_dataset_table='{}.{}'.format(
                    TEST_DATASET, TEST_TABLE_ID
                ),
                schema_fields=[],
                source_uris=['gs://{}/{}'.format(TEST_GCS_BUCKET, source_object)
                             for source_object in TEST_GCS_DATA],
                source_format=TEST_SOURCE_FORMAT,
                compression='NONE',
                skip_leading_rows=0,
                field_delimiter=',',
                max_bad_records=0,
                quote_character=None,
                allow_quoted_newlines=False,
                allow_jagged_rows=False,
                src_fmt_configs={},
                labels=None,
                encryption_configuration=None
            )


class BigQueryDeleteDatasetOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryDeleteDatasetOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            delete_contents=TEST_DELETE_CONTENTS
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .delete_dataset \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                delete_contents=TEST_DELETE_CONTENTS
            )


class BigQueryCreateEmptyDatasetOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryCreateEmptyDatasetOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .create_empty_dataset \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                dataset_reference={}
            )


class BigQueryGetDatasetOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryGetDatasetOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .get_dataset \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID
            )


class BigQueryPatchDatasetOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_execute(self, mock_hook):
        dataset_resource = {"friendlyName": 'Test DS'}
        operator = BigQueryPatchDatasetOperator(
            dataset_resource=dataset_resource,
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .patch_dataset \
            .assert_called_once_with(
                dataset_resource=dataset_resource,
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID
            )


class BigQueryUpdateDatasetOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_execute(self, mock_hook):
        dataset_resource = {"friendlyName": 'Test DS'}
        operator = BigQueryUpdateDatasetOperator(
            dataset_resource=dataset_resource,
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .update_dataset \
            .assert_called_once_with(
                dataset_resource=dataset_resource,
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID
            )


class BigQueryOperatorTest(unittest.TestCase):
    def test_bql_deprecation_warning(self):
        with self.assertWarns(DeprecationWarning) as cm:
            task = BigQueryOperator(
                task_id='test_deprecation_warning_for_bql',
                bql='select * from test_table'
            )
        warning = cm.warning
        assert task, "Task should be created"
        assert 'Deprecated parameter `bql` used in Task id: test_deprecation_warning_for_bql. ' \
               'Use `sql` parameter instead to pass the sql to be executed.' \
               ' `bql` parameter is deprecated and will be removed' \
               ' in a future version of Airflow.' == warning.args[0]

    def setUp(self):
        self.dagbag = models.DagBag(
            dag_folder='/dev/null', include_examples=True)
        self.args = {'owner': 'airflow', 'start_date': DEFAULT_DATE}
        self.dag = DAG(TEST_DAG_ID, default_args=self.args)

    def tearDown(self):
        session = Session()
        session.query(models.TaskInstance).filter_by(
            dag_id=TEST_DAG_ID).delete()
        session.query(TaskFail).filter_by(
            dag_id=TEST_DAG_ID).delete()
        session.commit()
        session.close()

    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_execute(self, mock_hook):
        encryption_configuration = {'key': 'kk'}

        operator = BigQueryOperator(
            task_id=TASK_ID,
            sql='Select * from test_table',
            destination_dataset_table=None,
            write_disposition='WRITE_EMPTY',
            allow_large_results=False,
            flatten_results=None,
            bigquery_conn_id='bigquery_default',
            udf_config=None,
            use_legacy_sql=True,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition='CREATE_IF_NEEDED',
            schema_update_options=(),
            query_params=None,
            labels=None,
            priority='INTERACTIVE',
            time_partitioning=None,
            api_resource_configs=None,
            cluster_fields=None,
            encryption_configuration=encryption_configuration
        )

        operator.execute(MagicMock())
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_query \
            .assert_called_once_with(
                sql='Select * from test_table',
                destination_dataset_table=None,
                write_disposition='WRITE_EMPTY',
                allow_large_results=False,
                flatten_results=None,
                udf_config=None,
                maximum_billing_tier=None,
                maximum_bytes_billed=None,
                create_disposition='CREATE_IF_NEEDED',
                schema_update_options=(),
                query_params=None,
                labels=None,
                priority='INTERACTIVE',
                time_partitioning=None,
                api_resource_configs=None,
                cluster_fields=None,
                encryption_configuration=encryption_configuration
            )

    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_execute_list(self, mock_hook):
        operator = BigQueryOperator(
            task_id=TASK_ID,
            sql=[
                'Select * from test_table',
                'Select * from other_test_table',
            ],
            destination_dataset_table=None,
            write_disposition='WRITE_EMPTY',
            allow_large_results=False,
            flatten_results=None,
            bigquery_conn_id='google_cloud_default',
            udf_config=None,
            use_legacy_sql=True,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition='CREATE_IF_NEEDED',
            schema_update_options=(),
            query_params=None,
            labels=None,
            priority='INTERACTIVE',
            time_partitioning=None,
            api_resource_configs=None,
            cluster_fields=None,
            encryption_configuration=None,
        )

        operator.execute(MagicMock())
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_query \
            .assert_has_calls([
                mock.call(
                    sql='Select * from test_table',
                    destination_dataset_table=None,
                    write_disposition='WRITE_EMPTY',
                    allow_large_results=False,
                    flatten_results=None,
                    udf_config=None,
                    maximum_billing_tier=None,
                    maximum_bytes_billed=None,
                    create_disposition='CREATE_IF_NEEDED',
                    schema_update_options=(),
                    query_params=None,
                    labels=None,
                    priority='INTERACTIVE',
                    time_partitioning=None,
                    api_resource_configs=None,
                    cluster_fields=None,
                    encryption_configuration=None,
                ),
                mock.call(
                    sql='Select * from other_test_table',
                    destination_dataset_table=None,
                    write_disposition='WRITE_EMPTY',
                    allow_large_results=False,
                    flatten_results=None,
                    udf_config=None,
                    maximum_billing_tier=None,
                    maximum_bytes_billed=None,
                    create_disposition='CREATE_IF_NEEDED',
                    schema_update_options=(),
                    query_params=None,
                    labels=None,
                    priority='INTERACTIVE',
                    time_partitioning=None,
                    api_resource_configs=None,
                    cluster_fields=None,
                    encryption_configuration=None,
                ),
            ])

    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_execute_bad_type(self, mock_hook):
        operator = BigQueryOperator(
            task_id=TASK_ID,
            sql=1,
            destination_dataset_table=None,
            write_disposition='WRITE_EMPTY',
            allow_large_results=False,
            flatten_results=None,
            bigquery_conn_id='google_cloud_default',
            udf_config=None,
            use_legacy_sql=True,
            maximum_billing_tier=None,
            maximum_bytes_billed=None,
            create_disposition='CREATE_IF_NEEDED',
            schema_update_options=(),
            query_params=None,
            labels=None,
            priority='INTERACTIVE',
            time_partitioning=None,
            api_resource_configs=None,
            cluster_fields=None,
        )

        with self.assertRaises(AirflowException):
            operator.execute(MagicMock())

    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_bigquery_operator_defaults(self, mock_hook):
        operator = BigQueryOperator(
            task_id=TASK_ID,
            sql='Select * from test_table',
            dag=self.dag,
            default_args=self.args,
            schema_update_options=None
        )

        operator.execute(MagicMock())
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_query \
            .assert_called_once_with(
                sql='Select * from test_table',
                destination_dataset_table=None,
                write_disposition='WRITE_EMPTY',
                allow_large_results=False,
                flatten_results=None,
                udf_config=None,
                maximum_billing_tier=None,
                maximum_bytes_billed=None,
                create_disposition='CREATE_IF_NEEDED',
                schema_update_options=None,
                query_params=None,
                labels=None,
                priority='INTERACTIVE',
                time_partitioning=None,
                api_resource_configs=None,
                cluster_fields=None,
                encryption_configuration=None
            )

        self.assertTrue(isinstance(operator.sql, six.string_types))
        ti = TaskInstance(task=operator, execution_date=DEFAULT_DATE)
        ti.render_templates()
        self.assertTrue(isinstance(ti.task.sql, six.string_types))

    @provide_session
    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_bigquery_operator_extra_link_when_missing_job_id(self, mock_hook, session):
        bigquery_task = BigQueryOperator(
            task_id=TASK_ID,
            sql='SELECT * FROM test_table',
            dag=self.dag,
        )
        self.dag.clear()
        session.query(XCom).delete()

        self.assertEqual(
            '',
            bigquery_task.get_extra_links(DEFAULT_DATE, BigQueryConsoleLink.name),
        )

    @provide_session
    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_bigquery_operator_extra_link_when_single_query(self, mock_hook, session):
        bigquery_task = BigQueryOperator(
            task_id=TASK_ID,
            sql='SELECT * FROM test_table',
            dag=self.dag,
        )
        self.dag.clear()
        session.query(XCom).delete()

        ti = TaskInstance(
            task=bigquery_task,
            execution_date=DEFAULT_DATE,
        )

        job_id = '12345'
        ti.xcom_push(key='job_id', value=job_id)

        self.assertEquals(
            'https://console.cloud.google.com/bigquery?j={job_id}'.format(job_id=job_id),
            bigquery_task.get_extra_links(DEFAULT_DATE, BigQueryConsoleLink.name),
        )

        self.assertEquals(
            '',
            bigquery_task.get_extra_links(datetime(2019, 1, 1), BigQueryConsoleLink.name),
        )

    @provide_session
    @mock.patch('airflow.contrib.operators.bigquery_operator.BigQueryHook')
    def test_bigquery_operator_extra_link_when_multiple_query(self, mock_hook, session):
        bigquery_task = BigQueryOperator(
            task_id=TASK_ID,
            sql=['SELECT * FROM test_table', 'SELECT * FROM test_table2'],
            dag=self.dag,
        )
        self.dag.clear()
        session.query(XCom).delete()

        ti = TaskInstance(
            task=bigquery_task,
            execution_date=DEFAULT_DATE,
        )

        job_id = ['123', '45']
        ti.xcom_push(key='job_id', value=job_id)

        six.assertCountEqual(
            self, {'BigQuery Console #1', 'BigQuery Console #2'},
            bigquery_task.operator_extra_link_dict.keys()
        )

        self.assertEqual(
            'https://console.cloud.google.com/bigquery?j=123',
            bigquery_task.get_extra_links(DEFAULT_DATE, 'BigQuery Console #1'),
        )

        self.assertEqual(
            'https://console.cloud.google.com/bigquery?j=45',
            bigquery_task.get_extra_links(DEFAULT_DATE, 'BigQuery Console #2'),
        )

    def test_bigquery_operator_extra_serialized_field_when_single_query(self):
        with self.dag:
            BigQueryOperator(
                task_id=TASK_ID,
                sql='SELECT * FROM test_table',
            )
        serialized_dag = SerializedDAG.to_dict(self.dag)
        self.assertIn("sql", serialized_dag["dag"]["tasks"][0])

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict[TASK_ID]
        self.assertEqual(getattr(simple_task, "sql"), 'SELECT * FROM test_table')

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################

        # Check Serialized version of operator link
        self.assertEqual(
            serialized_dag["dag"]["tasks"][0]["_operator_extra_links"],
            [{'airflow.contrib.operators.bigquery_operator.BigQueryConsoleLink': {}}]
        )

        # Check DeSerialized version of operator link
        self.assertIsInstance(list(simple_task.operator_extra_links)[0], BigQueryConsoleLink)

        ti = TaskInstance(task=simple_task, execution_date=DEFAULT_DATE)
        ti.xcom_push('job_id', 12345)

        # check for positive case
        url = simple_task.get_extra_links(DEFAULT_DATE, BigQueryConsoleLink.name)
        self.assertEqual(url, 'https://console.cloud.google.com/bigquery?j=12345')

        # check for negative case
        url2 = simple_task.get_extra_links(datetime(2017, 1, 2), BigQueryConsoleLink.name)
        self.assertEqual(url2, '')


class BigQueryGetDataOperatorTest(unittest.TestCase):

    @mock.patch('airflow.contrib.operators.bigquery_get_data.BigQueryHook')
    def test_execute(self, mock_hook):

        max_results = '100'
        selected_fields = 'DATE'
        operator = BigQueryGetDataOperator(task_id=TASK_ID,
                                           dataset_id=TEST_DATASET,
                                           table_id=TEST_TABLE_ID,
                                           max_results=max_results,
                                           selected_fields=selected_fields,
                                           )
        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .get_tabledata \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                table_id=TEST_TABLE_ID,
                max_results=max_results,
                selected_fields=selected_fields,
            )


class BigQueryTableDeleteOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.bigquery_table_delete_operator.BigQueryHook')
    def test_execute(self, mock_hook):
        ignore_if_missing = True
        deletion_dataset_table = '{}.{}'.format(TEST_DATASET, TEST_TABLE_ID)

        operator = BigQueryTableDeleteOperator(
            task_id=TASK_ID,
            deletion_dataset_table=deletion_dataset_table,
            ignore_if_missing=ignore_if_missing
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_table_delete \
            .assert_called_once_with(
                deletion_dataset_table=deletion_dataset_table,
                ignore_if_missing=ignore_if_missing
            )


class BigQueryToBigQueryOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.bigquery_to_bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        source_project_dataset_tables = '{}.{}'.format(
            TEST_DATASET, TEST_TABLE_ID)
        destination_project_dataset_table = '{}.{}'.format(
            TEST_DATASET + '_new', TEST_TABLE_ID)
        write_disposition = 'WRITE_EMPTY'
        create_disposition = 'CREATE_IF_NEEDED'
        labels = {'k1': 'v1'}
        encryption_configuration = {'key': 'kk'}

        operator = BigQueryToBigQueryOperator(
            task_id=TASK_ID,
            source_project_dataset_tables=source_project_dataset_tables,
            destination_project_dataset_table=destination_project_dataset_table,
            write_disposition=write_disposition,
            create_disposition=create_disposition,
            labels=labels,
            encryption_configuration=encryption_configuration
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_copy \
            .assert_called_once_with(
                source_project_dataset_tables=source_project_dataset_tables,
                destination_project_dataset_table=destination_project_dataset_table,
                write_disposition=write_disposition,
                create_disposition=create_disposition,
                labels=labels,
                encryption_configuration=encryption_configuration
            )


class BigQueryToCloudStorageOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.bigquery_to_gcs.BigQueryHook')
    def test_execute(self, mock_hook):
        source_project_dataset_table = '{}.{}'.format(
            TEST_DATASET, TEST_TABLE_ID)
        destination_cloud_storage_uris = ['gs://some-bucket/some-file.txt']
        compression = 'NONE'
        export_format = 'CSV'
        field_delimiter = ','
        print_header = True
        labels = {'k1': 'v1'}

        operator = BigQueryToCloudStorageOperator(
            task_id=TASK_ID,
            source_project_dataset_table=source_project_dataset_table,
            destination_cloud_storage_uris=destination_cloud_storage_uris,
            compression=compression,
            export_format=export_format,
            field_delimiter=field_delimiter,
            print_header=print_header,
            labels=labels
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .run_extract \
            .assert_called_once_with(
                source_project_dataset_table=source_project_dataset_table,
                destination_cloud_storage_uris=destination_cloud_storage_uris,
                compression=compression,
                export_format=export_format,
                field_delimiter=field_delimiter,
                print_header=print_header,
                labels=labels
            )


class BigQueryToMySqlOperatorTest(unittest.TestCase):
    @mock.patch('airflow.contrib.operators.bigquery_to_mysql_operator.BigQueryHook')
    def test_execute_good_request_to_bq(self, mock_hook):
        destination_table = 'table'
        operator = BigQueryToMySqlOperator(
            task_id=TASK_ID,
            dataset_table='{}.{}'.format(TEST_DATASET, TEST_TABLE_ID),
            mysql_table=destination_table,
            replace=False,
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .get_tabledata \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                table_id=TEST_TABLE_ID,
                max_results=1000,
                selected_fields=None,
                start_index=0
            )
