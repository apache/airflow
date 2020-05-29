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

import mock
from parameterized import parameterized

from airflow import models
from airflow.exceptions import AirflowException
from airflow.models import DAG, TaskFail, TaskInstance, XCom
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator, BigQueryConsoleIndexableLink, BigQueryConsoleLink,
    BigQueryCreateEmptyDatasetOperator, BigQueryCreateEmptyTableOperator, BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator, BigQueryDeleteTableOperator, BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator, BigQueryGetDatasetOperator, BigQueryGetDatasetTablesOperator,
    BigQueryIntervalCheckOperator, BigQueryPatchDatasetOperator, BigQueryUpdateDatasetOperator,
    BigQueryUpsertTableOperator, BigQueryValueCheckOperator,
)
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.settings import Session
from airflow.utils.session import provide_session

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
TEST_TABLE_RESOURCES = {
    "tableReference": {
        "tableId": TEST_TABLE_ID
    },
    "expirationTime": 1234567
}
VIEW_DEFINITION = {
    "query": "SELECT * FROM `{}.{}`".format(TEST_DATASET, TEST_TABLE_ID),
    "useLegacySql": False
}


class TestBigQueryCreateEmptyTableOperator(unittest.TestCase):

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryCreateEmptyTableOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID
        )

        operator.execute(None)
        mock_hook.return_value.create_empty_table \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                table_id=TEST_TABLE_ID,
                schema_fields=None,
                time_partitioning={},
                cluster_fields=None,
                labels=None,
                view=None,
                encryption_configuration=None,
                table_resource=None,
                exists_ok=False,
            )

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_create_view(self, mock_hook):
        operator = BigQueryCreateEmptyTableOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            view=VIEW_DEFINITION
        )

        operator.execute(None)
        mock_hook.return_value.create_empty_table \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                table_id=TEST_TABLE_ID,
                schema_fields=None,
                time_partitioning={},
                cluster_fields=None,
                labels=None,
                view=VIEW_DEFINITION,
                encryption_configuration=None,
                table_resource=None,
                exists_ok=False,
            )

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_create_clustered_empty_table(self, mock_hook):

        schema_fields = [
            {
                "name": "emp_name",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "date_hired",
                "type": "DATE",
                "mode": "REQUIRED"
            },
            {
                "name": "date_birth",
                "type": "DATE",
                "mode": "NULLABLE"
            }
        ]
        time_partitioning = {
            "type": "DAY",
            "field": "date_hired"
        }
        cluster_fields = ["date_birth"]
        operator = BigQueryCreateEmptyTableOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            schema_fields=schema_fields,
            time_partitioning=time_partitioning,
            cluster_fields=cluster_fields
        )

        operator.execute(None)
        mock_hook.return_value.create_empty_table \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                table_id=TEST_TABLE_ID,
                schema_fields=schema_fields,
                time_partitioning=time_partitioning,
                cluster_fields=cluster_fields,
                labels=None,
                view=None,
                encryption_configuration=None,
                table_resource=None,
                exists_ok=False,
            )


class TestBigQueryCreateExternalTableOperator(unittest.TestCase):

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
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
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryDeleteDatasetOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            delete_contents=TEST_DELETE_CONTENTS
        )

        operator.execute(None)
        mock_hook.return_value \
            .delete_dataset \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                delete_contents=TEST_DELETE_CONTENTS
            )


class TestBigQueryCreateEmptyDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryCreateEmptyDatasetOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            location=TEST_DATASET_LOCATION
        )

        operator.execute(None)
        mock_hook.return_value.create_empty_dataset.assert_called_once_with(
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            location=TEST_DATASET_LOCATION,
            dataset_reference={},
            exists_ok=False,
        )


class TestBigQueryGetDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryGetDatasetOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID
        )

        operator.execute(None)
        mock_hook.return_value \
            .get_dataset \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID
            )


class TestBigQueryPatchDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
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
            .patch_dataset \
            .assert_called_once_with(
                dataset_resource=dataset_resource,
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID
            )


class TestBigQueryUpdateDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
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
            .update_dataset \
            .assert_called_once_with(
                dataset_resource=dataset_resource,
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                fields=list(dataset_resource.keys())
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

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        encryption_configuration = {'key': 'kk'}

        operator = BigQueryExecuteQueryOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute_list(self, mock_hook):
        operator = BigQueryExecuteQueryOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute_bad_type(self, mock_hook):
        operator = BigQueryExecuteQueryOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_bigquery_operator_defaults(self, mock_hook):
        operator = BigQueryExecuteQueryOperator(
            task_id=TASK_ID,
            sql='Select * from test_table',
            dag=self.dag,
            default_args=self.args,
            schema_update_options=None
        )

        operator.execute(MagicMock())
        mock_hook.return_value \
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

    def test_bigquery_operator_extra_serialized_field_when_single_query(self):
        with self.dag:
            BigQueryExecuteQueryOperator(
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
            [{'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleLink': {}}]
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

    def test_bigquery_operator_extra_serialized_field_when_multiple_queries(self):
        with self.dag:
            BigQueryExecuteQueryOperator(
                task_id=TASK_ID,
                sql=['SELECT * FROM test_table', 'SELECT * FROM test_table2'],
            )
        serialized_dag = SerializedDAG.to_dict(self.dag)
        self.assertIn("sql", serialized_dag["dag"]["tasks"][0])

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict[TASK_ID]
        self.assertEqual(getattr(simple_task, "sql"),
                         ['SELECT * FROM test_table', 'SELECT * FROM test_table2'])

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################

        # Check Serialized version of operator link
        self.assertEqual(
            serialized_dag["dag"]["tasks"][0]["_operator_extra_links"],
            [
                {
                    'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink': {
                        'index': 0
                    }
                },
                {
                    'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink': {
                        'index': 1
                    }
                }
            ]
        )

        # Check DeSerialized version of operator link
        self.assertIsInstance(list(simple_task.operator_extra_links)[0], BigQueryConsoleIndexableLink)

        ti = TaskInstance(task=simple_task, execution_date=DEFAULT_DATE)
        job_id = ['123', '45']
        ti.xcom_push(key='job_id', value=job_id)

        self.assertEqual(
            {'BigQuery Console #1', 'BigQuery Console #2'},
            simple_task.operator_extra_link_dict.keys()
        )

        self.assertEqual(
            'https://console.cloud.google.com/bigquery?j=123',
            simple_task.get_extra_links(DEFAULT_DATE, 'BigQuery Console #1'),
        )

        self.assertEqual(
            'https://console.cloud.google.com/bigquery?j=45',
            simple_task.get_extra_links(DEFAULT_DATE, 'BigQuery Console #2'),
        )

    @provide_session
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_bigquery_operator_extra_link_when_missing_job_id(self, mock_hook, session):
        bigquery_task = BigQueryExecuteQueryOperator(
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
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_bigquery_operator_extra_link_when_single_query(self, mock_hook, session):
        bigquery_task = BigQueryExecuteQueryOperator(
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
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_bigquery_operator_extra_link_when_multiple_query(self, mock_hook, session):
        bigquery_task = BigQueryExecuteQueryOperator(
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

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):

        max_results = 100
        selected_fields = 'DATE'
        operator = BigQueryGetDataOperator(task_id=TASK_ID,
                                           dataset_id=TEST_DATASET,
                                           table_id=TEST_TABLE_ID,
                                           max_results=max_results,
                                           selected_fields=selected_fields,
                                           location=TEST_DATASET_LOCATION,
                                           )
        operator.execute(None)
        mock_hook.return_value \
            .list_rows \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                table_id=TEST_TABLE_ID,
                max_results=max_results,
                selected_fields=selected_fields,
                location=TEST_DATASET_LOCATION,
            )


class TestBigQueryTableDeleteOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        ignore_if_missing = True
        deletion_dataset_table = f'{TEST_DATASET}.{TEST_TABLE_ID}'

        operator = BigQueryDeleteTableOperator(
            task_id=TASK_ID,
            deletion_dataset_table=deletion_dataset_table,
            ignore_if_missing=ignore_if_missing
        )

        operator.execute(None)
        mock_hook.return_value.delete_table.assert_called_once_with(
            table_id=deletion_dataset_table,
            not_found_ok=ignore_if_missing
        )


class TestBigQueryGetDatasetTablesOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryGetDatasetTablesOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            max_results=2
        )

        operator.execute(None)
        mock_hook.return_value.get_dataset_tables.assert_called_once_with(
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            max_results=2,
        )


class TestBigQueryConnIdDeprecationWarning(unittest.TestCase):
    @parameterized.expand([
        (BigQueryCheckOperator, dict(sql='Select * from test_table', task_id=TASK_ID)),
        (BigQueryValueCheckOperator, dict(sql='Select * from test_table', pass_value=95, task_id=TASK_ID)),
        (BigQueryIntervalCheckOperator,
         dict(table=TEST_TABLE_ID, metrics_thresholds={'COUNT(*)': 1.5}, task_id=TASK_ID)),
        (BigQueryGetDataOperator, dict(dataset_id=TEST_DATASET, table_id=TEST_TABLE_ID, task_id=TASK_ID)),
        (BigQueryExecuteQueryOperator, dict(sql='Select * from test_table', task_id=TASK_ID)),
        (BigQueryDeleteDatasetOperator, dict(dataset_id=TEST_DATASET, task_id=TASK_ID)),
        (BigQueryCreateEmptyDatasetOperator, dict(dataset_id=TEST_DATASET, task_id=TASK_ID)),
        (BigQueryDeleteTableOperator, dict(deletion_dataset_table=TEST_DATASET, task_id=TASK_ID))
    ])
    def test_bigquery_conn_id_deprecation_warning(self, operator_class, kwargs):
        bigquery_conn_id = 'google_cloud_default'
        with self.assertWarnsRegex(
            DeprecationWarning,
            "The bigquery_conn_id parameter has been deprecated. You should pass the gcp_conn_id parameter."
        ):
            operator = operator_class(
                bigquery_conn_id=bigquery_conn_id,
                **kwargs
            )
            self.assertEqual(bigquery_conn_id, operator.gcp_conn_id)


class TestBigQueryUpsertTableOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryUpsertTableOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            table_resource=TEST_TABLE_RESOURCES,
            project_id=TEST_GCP_PROJECT_ID,
        )

        operator.execute(None)
        mock_hook.return_value \
            .run_table_upsert \
            .assert_called_once_with(
                dataset_id=TEST_DATASET,
                project_id=TEST_GCP_PROJECT_ID,
                table_resource=TEST_TABLE_RESOURCES
            )
