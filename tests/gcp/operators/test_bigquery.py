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
from datetime import datetime
from unittest.mock import MagicMock

from airflow import models
from airflow.exceptions import AirflowException
from airflow.gcp.operators.bigquery import (
    BigQueryConsoleLink, BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator, BigQueryDeleteDatasetOperator, BigQueryGetDataOperator,
    BigQueryGetDatasetOperator, BigQueryGetDatasetTablesOperator, BigQueryOperator,
    BigQueryPatchDatasetOperator, BigQueryTableDeleteOperator, BigQueryUpdateDatasetOperator,
)
from airflow.models import DAG, TaskFail, TaskInstance, XCom
from airflow.settings import Session
from airflow.utils.db import provide_session
from tests.compat import mock

TASK_ID = 'test-bq-generic-operator'
TEST_DATASET = 'test-dataset'
TEST_DATASET_LOCATION = 'EU'
TEST_GCP_PROJECT_ID = 'test-project'
TEST_DELETE_CONTENTS = True
TEST_TABLE_ID = 'test-table-id'
TEST_GCS_BUCKET = 'test-bucket'
TEST_GCS_DATA = ['dir1/*.csv']
TEST_SOURCE_FORMAT = 'CSV'
DEFAULT_DATE = datetime(2015, 1, 1)
TEST_DAG_ID = 'test-bigquery-operators'


class TestBigQueryCreateEmptyTableOperator(unittest.TestCase):

    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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


class TestBigQueryCreateExternalTableOperator(unittest.TestCase):

    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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


class TestBigQueryDeleteDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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


class TestBigQueryCreateEmptyDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryCreateEmptyDatasetOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            location=TEST_DATASET_LOCATION
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .create_empty_dataset \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                location=TEST_DATASET_LOCATION,
                dataset_reference={}
            )


class TestBigQueryGetDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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


class TestBigQueryPatchDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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


class TestBigQueryUpdateDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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


class TestBigQueryOperator(unittest.TestCase):
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

    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        encryption_configuration = {'key': 'kk'}

        operator = BigQueryOperator(
            task_id=TASK_ID,
            sql='Select * from test_table',
            destination_dataset_table=None,
            write_disposition='WRITE_EMPTY',
            allow_large_results=False,
            flatten_results=None,
            gcp_conn_id='google_cloud_default',
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

    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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
            gcp_conn_id='google_cloud_default',
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

    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
    def test_execute_bad_type(self, mock_hook):
        operator = BigQueryOperator(
            task_id=TASK_ID,
            sql=1,
            destination_dataset_table=None,
            write_disposition='WRITE_EMPTY',
            allow_large_results=False,
            flatten_results=None,
            gcp_conn_id='google_cloud_default',
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

    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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
        self.assertTrue(isinstance(operator.sql, str))
        ti = TaskInstance(task=operator, execution_date=DEFAULT_DATE)
        ti.render_templates()
        self.assertTrue(isinstance(ti.task.sql, str))

    @provide_session
    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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
    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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

        self.assertEqual(
            'https://console.cloud.google.com/bigquery?j={job_id}'.format(job_id=job_id),
            bigquery_task.get_extra_links(DEFAULT_DATE, BigQueryConsoleLink.name),
        )

        self.assertEqual(
            '',
            bigquery_task.get_extra_links(datetime(2019, 1, 1), BigQueryConsoleLink.name),
        )

    @provide_session
    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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

        self.assertEqual(
            {'BigQuery Console #1', 'BigQuery Console #2'},
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


class TestBigQueryGetDataOperator(unittest.TestCase):

    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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


class TestBigQueryTableDeleteOperator(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
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


class TestBigQueryGetDatasetTablesOperator(unittest.TestCase):
    @mock.patch('airflow.gcp.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryGetDatasetTablesOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            max_results=2
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_conn.return_value \
            .cursor.return_value \
            .get_dataset_tables \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                max_results=2,
                page_token=None
            )
