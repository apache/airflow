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
from unittest.mock import MagicMock

import pytest
from google.cloud.bigquery import DEFAULT_RETRY
from google.cloud.exceptions import Conflict

from airflow.exceptions import AirflowException
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryConsoleIndexableLink,
    BigQueryConsoleLink,
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryExecuteQueryOperator,
    BigQueryGetDataOperator,
    BigQueryGetDatasetOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryInsertJobOperator,
    BigQueryIntervalCheckOperator,
    BigQueryPatchDatasetOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryUpdateTableOperator,
    BigQueryUpdateTableSchemaOperator,
    BigQueryUpsertTableOperator,
    BigQueryValueCheckOperator,
)
from airflow.serialization.serialized_objects import SerializedDAG
from airflow.utils.timezone import datetime
from tests.test_utils.db import clear_db_dags, clear_db_runs, clear_db_serialized_dags, clear_db_xcom

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
TEST_TABLE_RESOURCES = {"tableReference": {"tableId": TEST_TABLE_ID}, "expirationTime": 1234567}
VIEW_DEFINITION = {
    "query": f"SELECT * FROM `{TEST_DATASET}.{TEST_TABLE_ID}`",
    "useLegacySql": False,
}
MATERIALIZED_VIEW_DEFINITION = {
    'query': f'SELECT product, SUM(amount) FROM `{TEST_DATASET}.{TEST_TABLE_ID}` GROUP BY product',
    'enableRefresh': True,
    'refreshIntervalMs': 2000000,
}


class TestBigQueryCreateEmptyTableOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryCreateEmptyTableOperator(
            task_id=TASK_ID, dataset_id=TEST_DATASET, project_id=TEST_GCP_PROJECT_ID, table_id=TEST_TABLE_ID
        )

        operator.execute(context=MagicMock())
        mock_hook.return_value.create_empty_table.assert_called_once_with(
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            schema_fields=None,
            time_partitioning={},
            cluster_fields=None,
            labels=None,
            view=None,
            materialized_view=None,
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
            view=VIEW_DEFINITION,
        )

        operator.execute(context=MagicMock())
        mock_hook.return_value.create_empty_table.assert_called_once_with(
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            schema_fields=None,
            time_partitioning={},
            cluster_fields=None,
            labels=None,
            view=VIEW_DEFINITION,
            materialized_view=None,
            encryption_configuration=None,
            table_resource=None,
            exists_ok=False,
        )

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_create_materialized_view(self, mock_hook):
        operator = BigQueryCreateEmptyTableOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            materialized_view=MATERIALIZED_VIEW_DEFINITION,
        )

        operator.execute(context=MagicMock())
        mock_hook.return_value.create_empty_table.assert_called_once_with(
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            schema_fields=None,
            time_partitioning={},
            cluster_fields=None,
            labels=None,
            view=None,
            materialized_view=MATERIALIZED_VIEW_DEFINITION,
            encryption_configuration=None,
            table_resource=None,
            exists_ok=False,
        )

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_create_clustered_empty_table(self, mock_hook):

        schema_fields = [
            {"name": "emp_name", "type": "STRING", "mode": "REQUIRED"},
            {"name": "date_hired", "type": "DATE", "mode": "REQUIRED"},
            {"name": "date_birth", "type": "DATE", "mode": "NULLABLE"},
        ]
        time_partitioning = {"type": "DAY", "field": "date_hired"}
        cluster_fields = ["date_birth"]
        operator = BigQueryCreateEmptyTableOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            schema_fields=schema_fields,
            time_partitioning=time_partitioning,
            cluster_fields=cluster_fields,
        )

        operator.execute(context=MagicMock())
        mock_hook.return_value.create_empty_table.assert_called_once_with(
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            table_id=TEST_TABLE_ID,
            schema_fields=schema_fields,
            time_partitioning=time_partitioning,
            cluster_fields=cluster_fields,
            labels=None,
            view=None,
            materialized_view=None,
            encryption_configuration=None,
            table_resource=None,
            exists_ok=False,
        )


class TestBigQueryCreateExternalTableOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryCreateExternalTableOperator(
            task_id=TASK_ID,
            destination_project_dataset_table=f'{TEST_GCP_PROJECT_ID}.{TEST_DATASET}.{TEST_TABLE_ID}',
            schema_fields=[],
            bucket=TEST_GCS_BUCKET,
            source_objects=TEST_GCS_DATA,
            source_format=TEST_SOURCE_FORMAT,
            autodetect=True,
        )

        mock_hook.return_value.split_tablename.return_value = (
            TEST_GCP_PROJECT_ID,
            TEST_DATASET,
            TEST_TABLE_ID,
        )

        operator.execute(context=MagicMock())
        mock_hook.return_value.create_empty_table.assert_called_once_with(
            table_resource={
                "tableReference": {
                    "projectId": TEST_GCP_PROJECT_ID,
                    "datasetId": TEST_DATASET,
                    "tableId": TEST_TABLE_ID,
                },
                "labels": None,
                "schema": {"fields": []},
                "externalDataConfiguration": {
                    "source_uris": [
                        f'gs://{TEST_GCS_BUCKET}/{source_object}' for source_object in TEST_GCS_DATA
                    ],
                    "source_format": TEST_SOURCE_FORMAT,
                    "maxBadRecords": 0,
                    "autodetect": True,
                    "compression": 'NONE',
                    "csvOptions": {
                        "fieldDelimeter": ',',
                        "skipLeadingRows": 0,
                        "quote": None,
                        "allowQuotedNewlines": False,
                        "allowJaggedRows": False,
                    },
                },
                "location": None,
                "encryptionConfiguration": None,
            }
        )


class TestBigQueryDeleteDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryDeleteDatasetOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            delete_contents=TEST_DELETE_CONTENTS,
        )

        operator.execute(None)
        mock_hook.return_value.delete_dataset.assert_called_once_with(
            dataset_id=TEST_DATASET, project_id=TEST_GCP_PROJECT_ID, delete_contents=TEST_DELETE_CONTENTS
        )


class TestBigQueryCreateEmptyDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryCreateEmptyDatasetOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            location=TEST_DATASET_LOCATION,
        )

        operator.execute(context=MagicMock())
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
            task_id=TASK_ID, dataset_id=TEST_DATASET, project_id=TEST_GCP_PROJECT_ID
        )

        operator.execute(context=MagicMock())
        mock_hook.return_value.get_dataset.assert_called_once_with(
            dataset_id=TEST_DATASET, project_id=TEST_GCP_PROJECT_ID
        )


class TestBigQueryUpdateTableOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        table_resource = {"friendlyName": 'Test TB'}
        operator = BigQueryUpdateTableOperator(
            table_resource=table_resource,
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            project_id=TEST_GCP_PROJECT_ID,
        )

        operator.execute(context=MagicMock())
        mock_hook.return_value.update_table.assert_called_once_with(
            table_resource=table_resource,
            fields=None,
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            project_id=TEST_GCP_PROJECT_ID,
        )


class TestBigQueryUpdateTableSchemaOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):

        schema_field_updates = [
            {
                'name': 'emp_name',
                'description': 'Name of employee',
            }
        ]

        operator = BigQueryUpdateTableSchemaOperator(
            schema_fields_updates=schema_field_updates,
            include_policy_tags=False,
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            project_id=TEST_GCP_PROJECT_ID,
        )
        operator.execute(context=MagicMock())

        mock_hook.return_value.update_table_schema.assert_called_once_with(
            schema_fields_updates=schema_field_updates,
            include_policy_tags=False,
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            project_id=TEST_GCP_PROJECT_ID,
        )


class TestBigQueryPatchDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        dataset_resource = {"friendlyName": 'Test DS'}
        operator = BigQueryPatchDatasetOperator(
            dataset_resource=dataset_resource,
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
        )

        operator.execute(None)
        mock_hook.return_value.patch_dataset.assert_called_once_with(
            dataset_resource=dataset_resource, dataset_id=TEST_DATASET, project_id=TEST_GCP_PROJECT_ID
        )


class TestBigQueryUpdateDatasetOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        dataset_resource = {"friendlyName": 'Test DS'}
        operator = BigQueryUpdateDatasetOperator(
            dataset_resource=dataset_resource,
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
        )

        operator.execute(context=MagicMock())
        mock_hook.return_value.update_dataset.assert_called_once_with(
            dataset_resource=dataset_resource,
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            fields=list(dataset_resource.keys()),
        )


class TestBigQueryOperator:
    def teardown_method(self):
        clear_db_xcom()
        clear_db_runs()
        clear_db_serialized_dags()
        clear_db_dags()

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
            encryption_configuration=encryption_configuration,
        )

        operator.execute(MagicMock())
        mock_hook.return_value.run_query.assert_called_once_with(
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
            encryption_configuration=encryption_configuration,
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
        mock_hook.return_value.run_query.assert_has_calls(
            [
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
            ]
        )

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

        with pytest.raises(AirflowException):
            operator.execute(MagicMock())

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_bigquery_operator_defaults(self, mock_hook, create_task_instance_of_operator):
        ti = create_task_instance_of_operator(
            BigQueryExecuteQueryOperator,
            dag_id=TEST_DAG_ID,
            task_id=TASK_ID,
            sql='Select * from test_table',
            schema_update_options=None,
        )
        operator = ti.task

        operator.execute(MagicMock())
        mock_hook.return_value.run_query.assert_called_once_with(
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
            encryption_configuration=None,
        )
        assert isinstance(operator.sql, str)
        ti.render_templates()
        assert isinstance(ti.task.sql, str)

    @pytest.mark.need_serialized_dag
    def test_bigquery_operator_extra_serialized_field_when_single_query(
        self,
        dag_maker,
        create_task_instance_of_operator,
    ):
        ti = create_task_instance_of_operator(
            BigQueryExecuteQueryOperator,
            dag_id=TEST_DAG_ID,
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            sql='SELECT * FROM test_table',
        )
        serialized_dag = dag_maker.get_serialized_data()
        assert "sql" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict[TASK_ID]
        assert getattr(simple_task, "sql") == 'SELECT * FROM test_table'

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################

        # Check Serialized version of operator link
        assert serialized_dag["dag"]["tasks"][0]["_operator_extra_links"] == [
            {'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleLink': {}}
        ]

        # Check DeSerialized version of operator link
        assert isinstance(list(simple_task.operator_extra_links)[0], BigQueryConsoleLink)

        ti.xcom_push('job_id', 12345)

        url = simple_task.get_extra_links(ti, BigQueryConsoleLink.name)
        assert url == 'https://console.cloud.google.com/bigquery?j=12345'

    @pytest.mark.need_serialized_dag
    def test_bigquery_operator_extra_serialized_field_when_multiple_queries(
        self,
        dag_maker,
        create_task_instance_of_operator,
    ):
        ti = create_task_instance_of_operator(
            BigQueryExecuteQueryOperator,
            dag_id=TEST_DAG_ID,
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            sql=['SELECT * FROM test_table', 'SELECT * FROM test_table2'],
        )
        serialized_dag = dag_maker.get_serialized_data()
        assert "sql" in serialized_dag["dag"]["tasks"][0]

        dag = SerializedDAG.from_dict(serialized_dag)
        simple_task = dag.task_dict[TASK_ID]
        assert getattr(simple_task, "sql") == ['SELECT * FROM test_table', 'SELECT * FROM test_table2']

        #########################################################
        # Verify Operator Links work with Serialized Operator
        #########################################################

        # Check Serialized version of operator link
        assert serialized_dag["dag"]["tasks"][0]["_operator_extra_links"] == [
            {'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink': {'index': 0}},
            {'airflow.providers.google.cloud.operators.bigquery.BigQueryConsoleIndexableLink': {'index': 1}},
        ]

        # Check DeSerialized version of operator link
        assert isinstance(list(simple_task.operator_extra_links)[0], BigQueryConsoleIndexableLink)

        job_id = ['123', '45']
        ti.xcom_push(key='job_id', value=job_id)

        assert {'BigQuery Console #1', 'BigQuery Console #2'} == simple_task.operator_extra_link_dict.keys()

        assert 'https://console.cloud.google.com/bigquery?j=123' == simple_task.get_extra_links(
            ti, 'BigQuery Console #1'
        )

        assert 'https://console.cloud.google.com/bigquery?j=45' == simple_task.get_extra_links(
            ti, 'BigQuery Console #2'
        )

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_bigquery_operator_extra_link_when_missing_job_id(
        self, mock_hook, create_task_instance_of_operator
    ):
        ti = create_task_instance_of_operator(
            BigQueryExecuteQueryOperator,
            dag_id=TEST_DAG_ID,
            task_id=TASK_ID,
            sql='SELECT * FROM test_table',
        )
        bigquery_task = ti.task

        assert '' == bigquery_task.get_extra_links(ti, BigQueryConsoleLink.name)

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_bigquery_operator_extra_link_when_single_query(
        self, mock_hook, create_task_instance_of_operator
    ):
        ti = create_task_instance_of_operator(
            BigQueryExecuteQueryOperator,
            dag_id=TEST_DAG_ID,
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            sql='SELECT * FROM test_table',
        )
        bigquery_task = ti.task

        job_id = '12345'
        ti.xcom_push(key='job_id', value=job_id)

        assert f'https://console.cloud.google.com/bigquery?j={job_id}' == bigquery_task.get_extra_links(
            ti, BigQueryConsoleLink.name
        )

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_bigquery_operator_extra_link_when_multiple_query(
        self, mock_hook, create_task_instance_of_operator
    ):
        ti = create_task_instance_of_operator(
            BigQueryExecuteQueryOperator,
            dag_id=TEST_DAG_ID,
            execution_date=DEFAULT_DATE,
            task_id=TASK_ID,
            sql=['SELECT * FROM test_table', 'SELECT * FROM test_table2'],
        )
        bigquery_task = ti.task

        job_id = ['123', '45']
        ti.xcom_push(key='job_id', value=job_id)

        assert {'BigQuery Console #1', 'BigQuery Console #2'} == bigquery_task.operator_extra_link_dict.keys()

        assert 'https://console.cloud.google.com/bigquery?j=123' == bigquery_task.get_extra_links(
            ti, 'BigQuery Console #1'
        )

        assert 'https://console.cloud.google.com/bigquery?j=45' == bigquery_task.get_extra_links(
            ti, 'BigQuery Console #2'
        )


class TestBigQueryGetDataOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):

        max_results = 100
        selected_fields = 'DATE'
        operator = BigQueryGetDataOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            table_id=TEST_TABLE_ID,
            max_results=max_results,
            selected_fields=selected_fields,
            location=TEST_DATASET_LOCATION,
        )
        operator.execute(None)
        mock_hook.return_value.list_rows.assert_called_once_with(
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
            ignore_if_missing=ignore_if_missing,
        )

        operator.execute(None)
        mock_hook.return_value.delete_table.assert_called_once_with(
            table_id=deletion_dataset_table, not_found_ok=ignore_if_missing
        )


class TestBigQueryGetDatasetTablesOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryGetDatasetTablesOperator(
            task_id=TASK_ID, dataset_id=TEST_DATASET, project_id=TEST_GCP_PROJECT_ID, max_results=2
        )

        operator.execute(None)
        mock_hook.return_value.get_dataset_tables.assert_called_once_with(
            dataset_id=TEST_DATASET,
            project_id=TEST_GCP_PROJECT_ID,
            max_results=2,
        )


@pytest.mark.parametrize(
    "operator_class, kwargs",
    [
        (BigQueryCheckOperator, dict(sql='Select * from test_table')),
        (BigQueryValueCheckOperator, dict(sql='Select * from test_table', pass_value=95)),
        (BigQueryIntervalCheckOperator, dict(table=TEST_TABLE_ID, metrics_thresholds={'COUNT(*)': 1.5})),
    ],
)
class TestBigQueryCheckOperators:
    @mock.patch("airflow.providers.google.cloud.operators.bigquery._BigQueryDbHookMixin.get_db_hook")
    def test_get_db_hook(
        self,
        mock_get_db_hook,
        operator_class,
        kwargs,
    ):
        operator = operator_class(task_id=TASK_ID, gcp_conn_id='google_cloud_default', **kwargs)
        operator.get_db_hook()
        mock_get_db_hook.assert_called_once()


class TestBigQueryUpsertTableOperator(unittest.TestCase):
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute(self, mock_hook):
        operator = BigQueryUpsertTableOperator(
            task_id=TASK_ID,
            dataset_id=TEST_DATASET,
            table_resource=TEST_TABLE_RESOURCES,
            project_id=TEST_GCP_PROJECT_ID,
        )

        operator.execute(context=MagicMock())
        mock_hook.return_value.run_table_upsert.assert_called_once_with(
            dataset_id=TEST_DATASET, project_id=TEST_GCP_PROJECT_ID, table_resource=TEST_TABLE_RESOURCES
        )


class TestBigQueryInsertJobOperator:
    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute_query_success(self, mock_hook):
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        configuration = {
            "query": {
                "query": "SELECT * FROM any",
                "useLegacySql": False,
            }
        }
        mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)
        mock_hook.return_value.generate_job_id.return_value = real_job_id

        op = BigQueryInsertJobOperator(
            task_id="insert_query_job",
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=job_id,
            project_id=TEST_GCP_PROJECT_ID,
        )
        result = op.execute(context=MagicMock())

        mock_hook.return_value.insert_job.assert_called_once_with(
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=real_job_id,
            project_id=TEST_GCP_PROJECT_ID,
            retry=DEFAULT_RETRY,
            timeout=None,
        )

        assert result == real_job_id

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute_copy_success(self, mock_hook):
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        configuration = {
            "copy": {
                "sourceTable": "aaa",
                "destinationTable": "bbb",
            }
        }
        mock_configuration = {
            "configuration": configuration,
            "jobReference": "a",
        }
        mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        mock_hook.return_value.insert_job.return_value.to_api_repr.return_value = mock_configuration

        op = BigQueryInsertJobOperator(
            task_id="copy_query_job",
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=job_id,
            project_id=TEST_GCP_PROJECT_ID,
        )
        result = op.execute(context=MagicMock())

        mock_hook.return_value.insert_job.assert_called_once_with(
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=real_job_id,
            project_id=TEST_GCP_PROJECT_ID,
            retry=DEFAULT_RETRY,
            timeout=None,
        )

        assert result == real_job_id

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_on_kill(self, mock_hook):
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        configuration = {
            "query": {
                "query": "SELECT * FROM any",
                "useLegacySql": False,
            }
        }
        mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=False)
        mock_hook.return_value.generate_job_id.return_value = real_job_id

        op = BigQueryInsertJobOperator(
            task_id="insert_query_job",
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=job_id,
            project_id=TEST_GCP_PROJECT_ID,
            cancel_on_kill=False,
        )
        op.execute(context=MagicMock())

        op.on_kill()
        mock_hook.return_value.cancel_job.assert_not_called()

        op.cancel_on_kill = True
        op.on_kill()
        mock_hook.return_value.cancel_job.assert_called_once_with(
            job_id=real_job_id,
            location=TEST_DATASET_LOCATION,
            project_id=TEST_GCP_PROJECT_ID,
        )

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute_failure(self, mock_hook):
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        configuration = {
            "query": {
                "query": "SELECT * FROM any",
                "useLegacySql": False,
            }
        }
        mock_hook.return_value.insert_job.return_value = MagicMock(job_id=real_job_id, error_result=True)
        mock_hook.return_value.generate_job_id.return_value = real_job_id

        op = BigQueryInsertJobOperator(
            task_id="insert_query_job",
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=job_id,
            project_id=TEST_GCP_PROJECT_ID,
        )
        with pytest.raises(AirflowException):
            op.execute(context=MagicMock())

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute_reattach(self, mock_hook):
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        configuration = {
            "query": {
                "query": "SELECT * FROM any",
                "useLegacySql": False,
            }
        }

        mock_hook.return_value.insert_job.side_effect = Conflict("any")
        job = MagicMock(
            job_id=real_job_id,
            error_result=False,
            state="PENDING",
            done=lambda: False,
        )
        mock_hook.return_value.get_job.return_value = job
        mock_hook.return_value.generate_job_id.return_value = real_job_id

        op = BigQueryInsertJobOperator(
            task_id="insert_query_job",
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=job_id,
            project_id=TEST_GCP_PROJECT_ID,
            reattach_states={"PENDING"},
        )
        result = op.execute(context=MagicMock())

        mock_hook.return_value.get_job.assert_called_once_with(
            location=TEST_DATASET_LOCATION,
            job_id=real_job_id,
            project_id=TEST_GCP_PROJECT_ID,
        )

        job.result.assert_called_once_with(
            retry=DEFAULT_RETRY,
            timeout=None,
        )

        assert result == real_job_id

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute_force_rerun(self, mock_hook):
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        configuration = {
            "query": {
                "query": "SELECT * FROM any",
                "useLegacySql": False,
            }
        }

        job = MagicMock(
            job_id=real_job_id,
            error_result=False,
        )
        mock_hook.return_value.insert_job.return_value = job
        mock_hook.return_value.generate_job_id.return_value = real_job_id

        op = BigQueryInsertJobOperator(
            task_id="insert_query_job",
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=job_id,
            project_id=TEST_GCP_PROJECT_ID,
            force_rerun=True,
        )
        result = op.execute(context=MagicMock())

        mock_hook.return_value.insert_job.assert_called_once_with(
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=real_job_id,
            project_id=TEST_GCP_PROJECT_ID,
            retry=DEFAULT_RETRY,
            timeout=None,
        )

        assert result == real_job_id

    @mock.patch('airflow.providers.google.cloud.operators.bigquery.BigQueryHook')
    def test_execute_no_force_rerun(self, mock_hook):
        job_id = "123456"
        hash_ = "hash"
        real_job_id = f"{job_id}_{hash_}"

        configuration = {
            "query": {
                "query": "SELECT * FROM any",
                "useLegacySql": False,
            }
        }

        mock_hook.return_value.insert_job.return_value.result.side_effect = Conflict("any")
        mock_hook.return_value.generate_job_id.return_value = real_job_id
        job = MagicMock(
            job_id=real_job_id,
            error_result=False,
            state="DONE",
            done=lambda: True,
        )
        mock_hook.return_value.get_job.return_value = job

        op = BigQueryInsertJobOperator(
            task_id="insert_query_job",
            configuration=configuration,
            location=TEST_DATASET_LOCATION,
            job_id=job_id,
            project_id=TEST_GCP_PROJECT_ID,
            reattach_states={"PENDING"},
        )
        # No force rerun
        with pytest.raises(AirflowException):
            op.execute(context=MagicMock())
